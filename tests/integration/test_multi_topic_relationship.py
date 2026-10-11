"""v7-S6 story 6.5: two topics feeding one relationship type, real Neo4j + Kafka, zero lock waits.

Type ``LIKES`` (``Person -> Item``) is fed by ``likes-a`` and ``likes-b``.  ``GRAPH_LOADER_MULTI_TOPIC_PAIRS``
(default 20,000, a multiple of 10) distinct pairs are published, half of them into ``Item`` 0 (the hub) and the rest
spread over the other items; 30% of the pairs are published to BOTH topics.  For half of the overlapping pairs the
newer ``updated_at`` sits in the EARLIER topic, for the other half in the LATER one, so neither "last topic wins"
nor "first topic wins" can pass the ``order_by`` cases by accident.

Cases (all against one query-logged Neo4j 5.26 container; every measurement window starts after node seeding):

* ``order_by: updated_at``: stream lease fleet, bulk lease, bulk classic, planned.  Final ``updated_at`` is the
  maximum over both topics for every pair, the relationship count equals the distinct pairs, the winner's
  properties are intact, no record is rejected, and (except classic, see below) the query log shows zero lock-wait
  time over at least as many completed LIKES write transactions as the run must have produced.
* ``topic_conflict: precedence`` (bulk only): bulk lease and bulk classic run through the REAL
  ``cli._run_precedence_substages`` (real ``RelationshipBulkMonitor``, control topic, boundary snapshots and
  zero-lag proof) with a fake ``DockerService`` whose "containers" are in-process loader fleets; planned orders by
  plan chain.  The later-listed topic wins every overlapping pair and the earlier topic's properties it does not
  carry survive (property-level merge).
* ``topic_conflict: arrival`` (stream lease): relationship count, lock waits, and that every stored value is one
  that was actually published for its pair; never which one.
* ``dead_letter`` (v7-S4, stream lease, ``arrival``): edges before nodes, topic b dead-lettered before topic a; one
  ``LIKES.dlq`` for both topics (as wide as the widest topic), drained by the real retry consumer; the exact graph
  (every pair ends with its last-indexed record), an empty parking lot, no per-topic DLQ, zero lock waits.

The loaders run in process (``EdgeLoader`` + ``build_edge_execution`` + an in-process ``BucketLeaseServer``,
three replicas for the lease modes) rather than as containers, like ``tests/stress``; the end-to-end container
runs live in ``plan/v7/slice-*/docker_proof``.  Classic runs ONE replica and asserts correctness only: it takes
APOC node locks by design and may wait (65 ms measured here), which is the documented difference to the
zero-wait modes (stream lease, bulk lease, planned), whose waits ARE asserted to be zero.

Lock-wait guard: the window is filtered to completed query-log entries of the right database that mention
``LIKES`` (the SHOW TRANSACTIONS poller and node seeding never count), and their number must reach the lower bound
of write transactions the run must have issued before the summed ``waiting`` is judged.

Skips cleanly without Docker (the shared fixtures skip).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import re
import threading
import time
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from neo4j import GraphDatabase

from src import cli
from src.loader.control import CONTROL_TOPIC
from src.loader.dead_letter import RejectionSink
from src.loader.edge_execution import build_edge_execution, streaming_lease_client_factory
from src.loader.edge_loader import EdgeLoader, apply_edge_outer_batch_override
from src.loader.lease_client import LEASE_ADDRESS_ENV, LEASE_BUCKETS_ENV, LEASE_KEYED_LABELS_ENV
from src.orchestrator.lease_server import BucketLeaseServer
from src.utils.schema_loader import load_schema
from tests.stress import lock_wait, planned_harness
from tests.stress import star_topology_data as std

pytestmark = pytest.mark.integration

PAIRS = int(os.environ.get("GRAPH_LOADER_MULTI_TOPIC_PAIRS", "20000"))
assert PAIRS % 10 == 0 and PAIRS >= 100, "GRAPH_LOADER_MULTI_TOPIC_PAIRS must be a multiple of 10 (>= 100)"
ITEMS = 5  # Item 0 is the hub (half of all pairs); items 1..4 share the other half
HUB_PAIRS = PAIRS // 2
PERSONS = HUB_PAIRS  # the hub's pairs use person ids 0..HUB_PAIRS-1
REPLICAS = 3
PARTITIONS = 3
EDGE_BATCH = 2000  # edge_unwind_batch_size in the schema below
NEO4J_IMAGE = "neo4j:5.26-enterprise"
DATABASE = "neo4j"  # the testcontainer's default database, as it appears in the query log


# -- dataset ----------------------------------------------------------------------------------------------


def _pair(i: int) -> tuple[int, int]:
    """Pair ``i`` -> (person id, item id): the first half goes into the hub (item 0), the rest into items 1..4."""
    if i < HUB_PAIRS:
        return i, 0
    j = i - HUB_PAIRS
    return j // (ITEMS - 1), 1 + j % (ITEMS - 1)


def _index(person: int, item: int) -> int:
    """Inverse of :func:`_pair`."""
    if item == 0:
        return person
    return HUB_PAIRS + person * (ITEMS - 1) + (item - 1)


def build_records() -> tuple[list[dict], list[dict]]:
    """Records for topic a and topic b.

    ``i % 10 < 3`` (30%) is published to both topics.  Of those, even ``(i // 10)`` carries the newer value in
    topic a (the earlier topic), odd in topic b.  Everything else is single-topic.  Topic a records carry
    ``a_only`` (= the pair index); every record carries ``src`` naming its topic.
    """
    a: list[dict] = []
    b: list[dict] = []
    for i in range(PAIRS):
        person, item = _pair(i)
        older, newer = 1_000 + i, 1_000_000 + i
        in_both = i % 10 < 3

        def record(topic: str, updated_at: int, i=i, person=person, item=item) -> dict:
            properties = {"updated_at": updated_at, "src": topic}
            if topic == "a":
                properties["a_only"] = i
            return {"source": {"id": person}, "target": {"id": item}, "properties": properties}

        if in_both:
            newer_in_a = (i // 10) % 2 == 0
            a.append(record("a", newer if newer_in_a else older))
            b.append(record("b", older if newer_in_a else newer))
        elif i % 2 == 0:
            a.append(record("a", older))
        else:
            b.append(record("b", older))
    return a, b


def expected_order_by() -> dict[tuple[int, int], tuple[int, str]]:
    """pair -> (max updated_at, topic of that value)."""
    a, b = build_records()
    best: dict[tuple[int, int], tuple[int, str]] = {}
    for rec in a + b:
        key = (rec["source"]["id"], rec["target"]["id"])
        value = (rec["properties"]["updated_at"], rec["properties"]["src"])
        if key not in best or value[0] > best[key][0]:
            best[key] = value
    return best


def expected_precedence() -> dict[tuple[int, int], dict]:
    """pair -> properties after topic a then topic b are merged (``SET r += props``)."""
    a, b = build_records()
    merged: dict[tuple[int, int], dict] = {}
    for rec in a + b:
        merged.setdefault((rec["source"]["id"], rec["target"]["id"]), {}).update(rec["properties"])
    return merged


def published_values() -> dict[tuple[int, int], set[tuple[int, str]]]:
    """pair -> every (updated_at, src) published for it (what ``arrival`` may legitimately store)."""
    a, b = build_records()
    out: dict[tuple[int, int], set[tuple[int, str]]] = {}
    for rec in a + b:
        out.setdefault((rec["source"]["id"], rec["target"]["id"]), set()).add(
            (rec["properties"]["updated_at"], rec["properties"]["src"]))
    return out


# -- fixtures ---------------------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def neo4j_logged():
    """One query-logged, APOC-enabled Neo4j for every case of this module (nodes are seeded once)."""
    import docker

    try:
        docker.from_env().ping()
    except Exception as exc:
        pytest.skip(f"Docker is required for the multi-topic integration test: {exc}")
    from testcontainers.community.neo4j import Neo4jContainer

    container = Neo4jContainer(NEO4J_IMAGE, password="test-password") \
        .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes") \
        .with_env("NEO4J_server_memory_heap_initial__size", "1G") \
        .with_env("NEO4J_server_memory_heap_max__size", "1G") \
        .with_env("NEO4J_server_memory_pagecache_size", "512M")
    container = lock_wait.with_apoc(lock_wait.with_transaction_timeout(lock_wait.with_verbose_query_log(container)))
    try:
        container.start()
    except Exception:
        try:
            container.stop()
        except Exception:
            pass
        raise
    driver = GraphDatabase.driver(container.get_connection_url(), auth=(container.username, container.password))
    try:
        driver.verify_connectivity()
        yield container, driver
    finally:
        driver.close()
        container.stop()


def schema_dict(run_id: str, *, rule: str, mode: str, replicas: int) -> dict:
    edge: dict = {
        "type": "LIKES",
        "topics": [f"mt-{run_id}-likes-a", f"mt-{run_id}-likes-b"],
        "nodes": {"source": "Person", "target": "Item"},
        "source_key_property": "id",
        "target_key_property": "id",
        "replicas": replicas,
        "properties": {
            "updated_at": {"type": "integer", "required": True},
            "src": {"type": "string", "required": True},
            "a_only": {"type": "integer", "required": False},
        },
        "execution": {"mode": "python_apoc", "worker_count": 4},
    }
    if rule == "order_by":
        edge["order_by"] = "updated_at"
    else:
        edge["topic_conflict"] = rule
    return {
        "loading": {
            "mode": mode,
            "consumer_group_id": f"mt-{run_id}",
            "unwind_batch_size": 500,
            "edge_unwind_batch_size": EDGE_BATCH,
            "flush_interval_ms": 50,
        },
        "nodes": [
            {"label": "Person", "topic": f"mt-{run_id}-person", "key_property": "id",
             "properties": {"id": {"type": "integer", "required": True, "constraint": {"type": "uniqueness"}}}},
            {"label": "Item", "topic": f"mt-{run_id}-item", "key_property": "id",
             "properties": {"id": {"type": "integer", "required": True, "constraint": {"type": "uniqueness"}}}},
        ],
        "edges": [edge],
    }


class Case:
    """One case's schema, topics and graph reset; deletes its topics afterwards."""

    def __init__(self, tmp_path: Path, driver, bootstrap: str, *, rule: str, mode: str, replicas: int = REPLICAS) -> None:
        self.run_id = uuid.uuid4().hex[:8]
        self.driver, self.bootstrap, self.tmp_path = driver, bootstrap, tmp_path
        path = tmp_path / f"schema-{self.run_id}.yaml"
        path.write_text(yaml.safe_dump(schema_dict(self.run_id, rule=rule, mode=mode, replicas=replicas)),
                        encoding="utf-8")
        self.schema = load_schema(path)
        self.edge = self.schema.edges[0]
        self.per_topic_records: dict[str, int] = {}

    @property
    def total_records(self) -> int:
        return sum(self.per_topic_records.values())

    def seed(self) -> None:
        """Nodes (once per module, idempotent) and an empty LIKES graph; publish both topics."""
        std.ensure_constraints(self.driver, self.schema)
        with self.driver.session() as session:
            session.run("UNWIND range(0, $n) AS i MERGE (:Person {id: i})", n=PERSONS - 1).consume()
            session.run("UNWIND range(0, $n) AS i MERGE (:Item {id: i})", n=ITEMS - 1).consume()
            session.run("CALL () { MATCH ()-[r:LIKES]->() DELETE r } IN TRANSACTIONS OF 5000 ROWS").consume()
        admin = AdminClient({"bootstrap.servers": self.bootstrap})
        wanted = [NewTopic(t, num_partitions=PARTITIONS, replication_factor=1) for t in self.edge.topics]
        wanted.append(NewTopic(CONTROL_TOPIC, num_partitions=1, replication_factor=1))
        existing = admin.list_topics(timeout=20).topics
        for future in admin.create_topics([t for t in wanted if t.topic not in existing]).values():
            future.result(30)
        a, b = build_records()
        producer = Producer({"bootstrap.servers": self.bootstrap, "linger.ms": 20, "compression.type": "lz4",
                             "queue.buffering.max.messages": 1_000_000})
        for topic, records in zip(self.edge.topics, (a, b)):
            self.per_topic_records[topic] = len(records)
            for rec in records:
                payload = json.dumps(rec, separators=(",", ":")).encode()
                while True:
                    try:
                        producer.produce(topic, key=str(rec["source"]["id"]).encode(), value=payload)
                        break
                    except BufferError:
                        producer.poll(0.05)
        assert producer.flush(120) == 0

    def high_watermarks(self, topics: list[str]) -> dict[tuple[str, int], int]:
        probe = Consumer({"bootstrap.servers": self.bootstrap, "group.id": f"probe-{self.run_id}"})
        try:
            out = {}
            for topic in topics:
                for partition in probe.list_topics(topic, timeout=20).topics[topic].partitions:
                    out[(topic, partition)] = probe.get_watermark_offsets(TopicPartition(topic, partition), timeout=20)[1]
            return out
        finally:
            probe.close()

    def committed(self, group: str, keys) -> int:
        probe = Consumer({"bootstrap.servers": self.bootstrap, "group.id": group, "enable.auto.commit": False})
        try:
            parts = probe.committed([TopicPartition(t, p) for t, p in keys], timeout=20)
            return sum(max(tp.offset, 0) for tp in parts)
        finally:
            probe.close()

    def cleanup(self) -> None:
        try:
            std.delete_topics(self.bootstrap, list(self.edge.topics))
        except Exception:  # best effort
            pass


# -- in-process loader fleet --------------------------------------------------------------------------------


class Fleet:
    """``replicas`` EdgeLoaders over ``topics`` of one type (threads standing in for containers)."""

    def __init__(self, case: Case, *, topics: list[str], edge, mode: str, lease: bool, replicas: int,
                 static: bool, group: str, run_id: str, lease_address: str | None = None,
                 timeout_s: float = 420) -> None:
        self.case, self.topics, self.edge, self.mode = case, topics, edge, mode
        self.lease, self.replicas, self.static, self.group, self.run_id = lease, replicas, static, group, run_id
        self.lease_address, self.timeout_s = lease_address, timeout_s
        self.stop_event = threading.Event()
        self.codes: dict[int, int] = {}
        self.threads: list[threading.Thread] = []
        self.rejections = case.tmp_path / f"rejections-{group}.jsonl"
        self._uri = self._auth = None

    def _environ(self) -> dict:
        if not self.lease:
            return {}
        return {LEASE_ADDRESS_ENV: self.lease_address, LEASE_BUCKETS_ENV: "16", LEASE_KEYED_LABELS_ENV: "Person,Item"}

    def _run_replica(self, index: int) -> None:
        schema = self.case.schema
        consumer = producer = coordinator = None
        driver = GraphDatabase.driver(self._uri, auth=self._auth)
        try:
            environ = self._environ()
            consumer = Consumer({
                "bootstrap.servers": self.case.bootstrap, "group.id": self.group, "enable.auto.commit": False,
                "auto.offset.reset": "earliest", "max.poll.interval.ms": schema.loading.max_poll_interval_ms,
                "session.timeout.ms": schema.loading.session_timeout_ms,
            })
            if self.mode == "bulk":
                producer = Producer({"bootstrap.servers": self.case.bootstrap})
            factory = streaming_lease_client_factory(environ, schema.loading.bucket_lease, self.stop_event) \
                if self.mode == "stream" else None
            writer, partitioner, lane_batcher, coordinator = build_edge_execution(
                self.edge, driver, environ=environ, lease_client_factory=factory, mode=self.mode,
                stop_event=self.stop_event,
                bucket_lease_tx_timeout_seconds=schema.loading.bucket_lease.tx_timeout_ms / 1000,
            )
            loader = EdgeLoader(
                consumer, writer, self.edge, topics=self.topics,
                loading_config=apply_edge_outer_batch_override(schema.loading, {}),
                shutdown_requested=self.stop_event, rejection_sink=RejectionSink(self.rejections),
                partitioner=partitioner, lane_batcher=lane_batcher, coordinator=coordinator, producer=producer,
                run_id=self.run_id, replica_id=str(index),
                static_assignment=(index, self.replicas) if self.static else None,
            )
            self.codes[index] = loader.run()
        except BaseException:  # noqa: BLE001 - surfaced through codes + the join below
            self.codes[index] = -1
            self.stop_event.set()
            raise
        finally:
            for closer in (consumer and consumer.close, producer and producer.flush,
                           coordinator and getattr(coordinator, "close", None), driver.close):
                if closer:
                    try:
                        closer()
                    except Exception:
                        pass

    def start(self, uri: str, auth: tuple[str, str]) -> "Fleet":
        self._uri, self._auth = uri, auth
        self.threads = [threading.Thread(target=self._run_replica, args=(i,), name=f"mt-replica-{i}", daemon=True)
                        for i in range(self.replicas)]
        for thread in self.threads:
            thread.start()
        return self

    def stop(self) -> None:
        """Ask every replica to finish (what SIGTERM does to a container), join, and check exit codes."""
        self.stop_event.set()
        for thread in self.threads:
            thread.join(60)
        assert not any(t.is_alive() for t in self.threads), "a loader thread did not stop"
        assert self.codes == {i: 0 for i in range(self.replicas)}, f"loader exit codes {self.codes}"
        assert not self.rejections.exists() or self.rejections.read_text() == "", "records were rejected"

    def wait_drained(self) -> None:
        ends = self.case.high_watermarks(self.topics)
        want = sum(ends.values())
        deadline = time.monotonic() + self.timeout_s
        while time.monotonic() < deadline and not self.stop_event.is_set():
            if self.case.committed(self.group, ends) >= want:
                return
            time.sleep(0.5)
        raise AssertionError(f"loaders did not commit {want} records within {self.timeout_s}s (or stopped early)")

    def run(self, uri: str, auth: tuple[str, str]) -> None:
        self.start(uri, auth)
        try:
            self.wait_drained()
        finally:
            self.stop()


class LeaseServer:
    def __enter__(self):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True, name="mt-lease-loop")
        self.thread.start()
        self.server = BucketLeaseServer()
        host, port = asyncio.run_coroutine_threadsafe(self.server.start("127.0.0.1", 0), self.loop).result(10)
        self.address = f"{host}:{port}"
        return self

    def __exit__(self, *exc):
        try:
            asyncio.run_coroutine_threadsafe(self.server.close(), self.loop).result(10)
        finally:
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.thread.join(10)
        return False


class FakeDocker:
    """Stands in for ``DockerService`` in ``cli._run_precedence_substages``: a "container" is an in-process loader."""

    def __init__(self, case: Case, uri: str, auth, lease_address: str | None) -> None:
        self.case, self.uri, self.auth, self.lease_address = case, uri, auth, lease_address
        self.fleets: list[Fleet] = []
        self.launches: list[tuple[str, ...]] = []

    def run_edge_loader(self, **kw):
        assert kw["mode"] == "bulk" and not kw.get("slot_gating")
        topics = list(kw["topics"])
        self.launches.append(tuple(topics))
        edge = self.case.edge.model_copy(update={"topics": topics, "topic_conflict": None})
        fleet = Fleet(
            self.case, topics=topics, edge=edge, mode="bulk", lease=bool(kw.get("lease_address")),
            replicas=kw["replicas"], static=True,
            group=f"{kw['consumer_group_prefix']}-{kw['edge_type']}-{kw['run_id']}", run_id=kw["run_id"],
            lease_address=kw.get("lease_address"),
        ).start(self.uri, self.auth)
        self.fleets.append(fleet)
        containers = []
        for _ in range(kw["replicas"]):
            container = MagicMock()
            container.attrs = {"State": {"Status": "running"}}
            container.fleet = fleet
            containers.append(container)
        return containers


# -- query-log accounting -----------------------------------------------------------------------------------

_WAIT = re.compile(r"waiting:\s*(\d+)\)")


def likes_write_waits(container, since: int) -> list[int]:
    """``waiting`` of every COMPLETED LIKES entry (a multi-line record) of the default database in the window.

    Excludes ``Query started`` lines, SHOW TRANSACTIONS and everything that does not mention LIKES (node seeding,
    constraint DDL, the poller, the cleanup).
    """
    waits = []
    # An entry starts with a timestamp line; the query text continues on the following lines.
    for entry in re.split(r"\n(?=\d{4}-\d\d-\d\d )", lock_wait._window_text(container, since)):
        if "Query started" in entry[:200] or "SHOW TRANSACTIONS" in entry or "LIKES" not in entry:
            continue
        if f" {DATABASE} - " not in entry.split("\n", 1)[0]:
            continue
        match = _WAIT.search(entry)
        if match:
            waits.append(int(match.group(1)))
    return waits


def assert_zero_waits(container, since: int, expected_min: int, *, timeout_s: float = 10.0) -> int:
    deadline = time.monotonic() + timeout_s
    waits = likes_write_waits(container, since)
    while len(waits) < expected_min and time.monotonic() < deadline:
        time.sleep(0.5)
        waits = likes_write_waits(container, since)
    assert len(waits) >= expected_min, (
        f"only {len(waits)} completed LIKES write entries in the query log, expected at least {expected_min}")
    assert sum(waits) == 0, f"expected 0 ms of lock waiting over {len(waits)} LIKES transactions, got {sum(waits)} ms"
    return len(waits)


# -- verification -------------------------------------------------------------------------------------------


def read_likes(driver) -> tuple[dict[tuple[int, int], dict], int]:
    with driver.session() as session:
        rows = session.run(
            "MATCH (s:Person)-[r:LIKES]->(t:Item) RETURN s.id AS s, t.id AS t, properties(r) AS p"
        ).data()
        total = session.run("MATCH ()-[r:LIKES]->() RETURN count(r) AS c").single()["c"]
    return {(row["s"], row["t"]): row["p"] for row in rows}, total


def assert_order_by(driver) -> None:
    stored, total = read_likes(driver)
    want = expected_order_by()
    assert total == len(want) == len(stored) == PAIRS, f"relationships {total}, distinct pairs {len(want)}"
    wrong = []
    for pair, (value, src) in want.items():
        props = stored.get(pair) or {}
        ok = props.get("updated_at") == value and props.get("src") == src
        ok = ok and set(props) <= {"updated_at", "src", "a_only"}
        if src == "a":  # topic a won: its a_only (= the pair index) must be there
            ok = ok and props.get("a_only") == _index(*pair)
        if not ok:
            wrong.append((pair, props))
    assert not wrong, f"{len(wrong)} pairs wrong, e.g. {wrong[:3]}"


def assert_precedence(driver) -> None:
    stored, total = read_likes(driver)
    want = expected_precedence()
    assert total == len(want) == len(stored) == PAIRS, f"relationships {total}, distinct pairs {len(want)}"
    wrong = [pair for pair, props in want.items() if stored.get(pair) != props]
    assert not wrong, f"{len(wrong)} pairs differ from the precedence merge, e.g. {wrong[:3]}"
    overlap = [pair for pair in want if "a_only" in want[pair] and want[pair]["src"] == "b"]
    assert len(overlap) == round(PAIRS * 0.3), "precedence dataset must contain the overlapping pairs"


def assert_arrival(driver) -> None:
    stored, total = read_likes(driver)
    assert total == len(stored) == PAIRS, f"arrival: {total} relationships for {PAIRS} distinct pairs"
    published = published_values()
    bad = [pair for pair, props in stored.items() if (props.get("updated_at"), props.get("src")) not in published[pair]]
    assert not bad, f"{len(bad)} pairs hold a value that was never published for them, e.g. {bad[:3]}"


def no_dlq_topics(bootstrap: str, case: Case) -> None:
    """No dead-letter topic of this case exists: none is configured, so nothing may have been routed to one.

    Checks the topic names a dead-lettering run of this schema would use (``<topic>.dlq`` and ``LIKES.dlq``).
    """
    names = set(AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=20).topics)
    forbidden = {f"{t}.dlq" for t in case.edge.topics} | {"LIKES.dlq"}
    assert not (names & forbidden), f"unexpected DLQ topics: {sorted(names & forbidden)}"


# -- cases --------------------------------------------------------------------------------------------------


def _drive(neo4j_logged, kafka_bootstrap, tmp_path, *, rule, mode, runner, verify, zero_wait=True, replicas=REPLICAS):
    """``runner(case, uri, auth)`` returns the lower bound of LIKES write transactions the run must have issued."""
    container, driver = neo4j_logged
    case = Case(tmp_path, driver, kafka_bootstrap, rule=rule, mode=mode, replicas=replicas)
    uri, auth = container.get_connection_url(), (container.username, container.password)
    try:
        with lock_wait.hard_timeout(900):
            case.seed()
            mark = lock_wait.query_log_mark(container)
            stop = threading.Event()
            blocked: list = []
            samples: list = []
            poller = threading.Thread(target=lock_wait.poll_show_transactions,
                                      args=(driver, stop, blocked, 0.05, samples), daemon=True)
            poller.start()
            try:
                expected_min = runner(case, uri, auth)
            finally:
                stop.set()
                poller.join(5)
            verify(driver)
            no_dlq_topics(kafka_bootstrap, case)
            if zero_wait:
                seen = assert_zero_waits(container, mark, expected_min)
                assert samples, "the SHOW TRANSACTIONS poller never completed a sample"
                assert blocked == [], f"SHOW TRANSACTIONS saw blocked transactions: {blocked[:3]}"
                print(f"zero-wait: {seen} LIKES write entries (>= {expected_min}), {len(samples)} poll samples")
            else:
                # Classic is the one mode that may wait on APOC node locks (measured 65 ms with one replica and
                # worker_count 4): report it, never assert it.
                print(f"classic lock wait (informational): {sum(likes_write_waits(container, mark))} ms")
    finally:
        case.cleanup()


def _single_stage(*, mode: str, lease: bool, replicas: int, static: bool):
    def run(case: Case, uri, auth) -> int:
        server = LeaseServer().__enter__() if lease else None
        try:
            Fleet(case, topics=list(case.edge.topics), edge=case.edge, mode=mode, lease=lease, replicas=replicas,
                  static=static, group=f"g-{case.run_id}", run_id=case.run_id,
                  lease_address=server.address if server else None).run(uri, auth)
        finally:
            if server:
                server.__exit__(None, None, None)
        return math.ceil(case.total_records / EDGE_BATCH)
    return run


def _cli_substages(*, lease: bool):
    """The real ``cli._run_precedence_substages`` with in-process loader fleets as the "containers"."""
    def run(case: Case, uri, auth) -> int:
        server = LeaseServer().__enter__() if lease else None
        docker = FakeDocker(case, uri, auth, server.address if server else None)
        args = argparse.Namespace(config="unused.yaml", network="unused", bulk_timeout_seconds=300)
        if server:
            args.lease_address, args.lease_buckets, args.lease_keyed_labels = server.address, 16, ("Person", "Item")

        def stop_fleets(containers, identities=None):
            for fleet in {id(c.fleet): c.fleet for c in containers}.values():
                if not fleet.stop_event.is_set():
                    fleet.stop()
            return []

        try:
            with pytest.MonkeyPatch.context() as mp:
                mp.setenv("KAFKA_BOOTSTRAP_SERVERS", case.bootstrap)
                mp.setattr(cli, "_stop_exact_containers", stop_fleets)
                mp.setattr(cli, "_remove_exact_containers", lambda containers: None)
                rc = cli._run_precedence_substages(
                    case.edge, docker, args, run_id=case.run_id,
                    consumer_group_prefix=case.schema.loading.consumer_group_id)
        finally:
            for fleet in docker.fleets:  # a failed stage must not leak threads
                fleet.stop_event.set()
            if server:
                server.__exit__(None, None, None)
        assert rc == 0, "cli._run_precedence_substages failed"
        assert docker.launches == [(t,) for t in case.edge.topics], "sub-stages must run one topic at a time, in order"
        return sum(math.ceil(n / EDGE_BATCH) for n in case.per_topic_records.values())
    return run


def _planned(case: Case, uri, auth) -> int:
    driver = GraphDatabase.driver(uri, auth=auth)
    try:
        result = planned_harness.run_planned_mode(
            case.schema, driver, case.bootstrap, plan_dir=case.tmp_path, writers=4, timeout_s=420,
            chunk_rows=1000,
        )
        assert result.total_attempts_over_one == 0
        assert len(result.plan.chunks) >= 2
        return len(result.plan.chunks)
    finally:
        driver.close()


def test_order_by_stream_lease_fleet(neo4j_logged, kafka_bootstrap, tmp_path):
    _drive(neo4j_logged, kafka_bootstrap, tmp_path, rule="order_by", mode="stream",
           runner=_single_stage(mode="stream", lease=True, replicas=REPLICAS, static=False), verify=assert_order_by)


def test_order_by_bulk_lease(neo4j_logged, kafka_bootstrap, tmp_path):
    _drive(neo4j_logged, kafka_bootstrap, tmp_path, rule="order_by", mode="bulk",
           runner=_single_stage(mode="bulk", lease=True, replicas=REPLICAS, static=True), verify=assert_order_by)


def test_order_by_bulk_classic_single_replica(neo4j_logged, kafka_bootstrap, tmp_path):
    _drive(neo4j_logged, kafka_bootstrap, tmp_path, rule="order_by", mode="bulk", replicas=1,
           runner=_single_stage(mode="bulk", lease=False, replicas=1, static=True), verify=assert_order_by,
           zero_wait=False)


def test_order_by_planned(neo4j_logged, kafka_bootstrap, tmp_path):
    _drive(neo4j_logged, kafka_bootstrap, tmp_path, rule="order_by", mode="bulk", runner=_planned,
           verify=assert_order_by)


def test_precedence_bulk_lease_cli_substages(neo4j_logged, kafka_bootstrap, tmp_path):
    _drive(neo4j_logged, kafka_bootstrap, tmp_path, rule="precedence", mode="bulk",
           runner=_cli_substages(lease=True), verify=assert_precedence)


def test_precedence_bulk_classic_cli_substages_single_replica(neo4j_logged, kafka_bootstrap, tmp_path):
    _drive(neo4j_logged, kafka_bootstrap, tmp_path, rule="precedence", mode="bulk", replicas=1,
           runner=_cli_substages(lease=False), verify=assert_precedence, zero_wait=False)


def test_precedence_planned(neo4j_logged, kafka_bootstrap, tmp_path):
    _drive(neo4j_logged, kafka_bootstrap, tmp_path, rule="precedence", mode="bulk", runner=_planned,
           verify=assert_precedence)


def test_arrival_stream_lease_counts_and_no_lock_waits(neo4j_logged, kafka_bootstrap, tmp_path):
    _drive(neo4j_logged, kafka_bootstrap, tmp_path, rule="arrival", mode="stream",
           runner=_single_stage(mode="stream", lease=True, replicas=REPLICAS, static=False), verify=assert_arrival)


# -- v7-S4: one DLQ per multi-topic type, edges before nodes, real retry consumer ------------------------
#
# The S4 Docker proof's ``arrival`` dataset (plan/v7/slice-4/docker_proof/proof.py): topic b is published and
# fully dead-lettered first, then topic a (names sort the other way), then the endpoints. Every row of both
# topics goes to the type's ONE dead-letter queue ``LIKES.dlq``; the real retry consumer (a subprocess:
# ``python -m src.loader.retry_consumer``, fenced by the same in-process lease server) drains it once the nodes
# exist. The sequenced parked-pair index must replay each pair b-then-a, so every pair ends with its
# last-indexed record. Own labels (DlqPerson, DlqCompany): the module's other cases seed Person/Item up front.


def _s4_arrival_dataset():
    import importlib.util

    path = Path(__file__).resolve().parents[2] / "plan" / "v7" / "slice-4" / "docker_proof" / "proof.py"
    spec = importlib.util.spec_from_file_location("v7s4_proof", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    data = module.arrival_dataset()
    return data[module.TB], data[module.TA], module.expected("arrival")


def _dlq_schema_dict(run_id: str) -> dict:
    return {
        "loading": {"mode": "stream", "consumer_group_id": f"mtdlq-{run_id}", "unwind_batch_size": 500,
                    "edge_unwind_batch_size": EDGE_BATCH, "flush_interval_ms": 50},
        "nodes": [
            {"label": "DlqPerson", "topic": f"mtdlq-{run_id}-person", "key_property": "person_id",
             "properties": {"person_id": {"type": "integer", "required": True, "constraint": {"type": "uniqueness"}}}},
            {"label": "DlqCompany", "topic": f"mtdlq-{run_id}-company", "key_property": "company_id",
             "properties": {"company_id": {"type": "string", "required": True, "constraint": {"type": "uniqueness"}}}},
        ],
        "edges": [{
            "type": "LIKES",
            "topics": [f"mtdlq-{run_id}-likes-a", f"mtdlq-{run_id}-likes-b"],
            "topic_conflict": "arrival",
            "nodes": {"source": "DlqPerson", "target": "DlqCompany"},
            "source_key_property": "person_id", "target_key_property": "company_id",
            "replicas": REPLICAS,
            "retry": {"base_delay_seconds": 1.0, "max_delay_seconds": 5.0},
            "dead_letter": {"enabled": True, "max_retry_attempts": 200},
            "properties": {"since": {"type": "date", "required": True}, "role": {"type": "string"}},
            "execution": {"mode": "python_apoc", "worker_count": 4},
        }],
    }


class DlqFleet(Fleet):
    """A stream lease fleet wired like ``edge_loader.main`` for a DLQ-enabled type: the type's DLQ sink, a
    writable sequenced parked-pair index and the admission barrier."""

    def _run_replica(self, index: int) -> None:
        from src.loader.dead_letter import build_malformed_record_sink
        from src.loader.parked_pair_index import ParkedPairIndex
        from src.models.schema import edge_parked_pair_index_topic_name

        schema = self.case.schema
        consumer = dlq_producer = coordinator = parked = None
        driver = GraphDatabase.driver(self._uri, auth=self._auth)
        try:
            environ = self._environ()
            consumer = Consumer({
                "bootstrap.servers": self.case.bootstrap, "group.id": self.group, "enable.auto.commit": False,
                "auto.offset.reset": "earliest", "max.poll.interval.ms": schema.loading.max_poll_interval_ms,
                "session.timeout.ms": schema.loading.session_timeout_ms,
            })
            sink, dlq_producer = build_malformed_record_sink(
                dead_letter=self.edge.dead_letter, entity_topic=self.edge.dead_letter_entity, entity_kind="edge",
                rejection_log_path=str(self.rejections), bootstrap_servers=self.case.bootstrap,
                run_id=self.run_id, replica_id=str(index), max_poll_interval_ms=schema.loading.max_poll_interval_ms,
            )
            parked = ParkedPairIndex(topic=edge_parked_pair_index_topic_name(self.edge),
                                     bootstrap_servers=self.case.bootstrap, writable=True,
                                     sequenced=self.edge.is_multi_topic)
            parked.start()
            factory = streaming_lease_client_factory(environ, schema.loading.bucket_lease, self.stop_event)
            writer, partitioner, lane_batcher, coordinator = build_edge_execution(
                self.edge, driver, environ=environ, lease_client_factory=factory, mode="stream",
                stop_event=self.stop_event, missing_endpoint_sink=sink, parked_pair_index=parked,
                bucket_lease_tx_timeout_seconds=schema.loading.bucket_lease.tx_timeout_ms / 1000,
            )
            loader = EdgeLoader(
                consumer, writer, self.edge, topics=self.topics,
                loading_config=apply_edge_outer_batch_override(schema.loading, {}),
                shutdown_requested=self.stop_event, rejection_sink=sink,
                partitioner=partitioner, lane_batcher=lane_batcher, coordinator=coordinator,
                run_id=self.run_id, replica_id=str(index), admission_index=parked,
            )
            self.codes[index] = loader.run()
        except BaseException:  # noqa: BLE001 - surfaced through codes + the join below
            self.codes[index] = -1
            self.stop_event.set()
            raise
        finally:
            for closer in (consumer and consumer.close, dlq_producer and dlq_producer.flush,
                           coordinator and getattr(coordinator, "close", None), parked and parked.stop,
                           driver.close):
                if closer:
                    try:
                        closer()
                    except Exception:
                        pass


def _topic_records(bootstrap: str, topic: str) -> tuple[int, int]:
    """``(partitions, records ever written)`` of a topic (0, 0 when absent)."""
    probe = Consumer({"bootstrap.servers": bootstrap, "group.id": f"probe-{uuid.uuid4().hex[:6]}"})
    try:
        meta = probe.list_topics(topic, timeout=20).topics.get(topic)
        if meta is None or meta.error is not None:
            return 0, 0
        return len(meta.partitions), sum(
            probe.get_watermark_offsets(TopicPartition(topic, p), timeout=20)[1] for p in meta.partitions)
    finally:
        probe.close()


def _publish_likes(bootstrap: str, topic: str, rows) -> None:
    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 20})
    for _t, person, company, since, role in rows:
        producer.produce(topic, key=str(person).encode(), value=json.dumps({
            "source": {"person_id": person}, "target": {"company_id": company},
            "properties": {"since": since.isoformat(), "role": role},
        }).encode())
    assert producer.flush(60) == 0


def test_dead_letter_arrival_stream_type_wide_dlq_drained_by_the_retry_consumer(neo4j_logged, kafka_bootstrap, tmp_path):
    import signal
    import subprocess
    import sys
    from datetime import date

    from src.models.schema import edge_dead_letter_topic_name, edge_parked_topic_name

    container, driver = neo4j_logged
    run_id = uuid.uuid4().hex[:8]
    path = tmp_path / f"dlq-schema-{run_id}.yaml"
    path.write_text(yaml.safe_dump(_dlq_schema_dict(run_id)), encoding="utf-8")
    case = Case.__new__(Case)
    case.run_id, case.driver, case.bootstrap, case.tmp_path = run_id, driver, kafka_bootstrap, tmp_path
    case.schema = load_schema(path)
    case.edge = case.schema.edges[0]
    case.per_topic_records = {}
    topic_a, topic_b = case.edge.topics
    dlq, parked = edge_dead_letter_topic_name(case.edge), edge_parked_topic_name(case.edge)
    assert dlq == "LIKES.dlq"
    b_rows, a_rows, want = _s4_arrival_dataset()
    uri, auth = container.get_connection_url(), (container.username, container.password)
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    family = [dlq, parked, f"{dlq}.parked-pairs"]
    retry_proc = None
    server = None
    fleet = None
    try:
        with lock_wait.hard_timeout(900):
            std.ensure_constraints(driver, case.schema)
            with driver.session() as session:
                session.run("MATCH (n) WHERE n:DlqPerson OR n:DlqCompany DETACH DELETE n").consume()
            existing = admin.list_topics(timeout=20).topics
            for future in admin.create_topics([NewTopic(t, num_partitions=n, replication_factor=1) for t, n in
                                               ((topic_a, 3), (topic_b, 4)) if t not in existing]).values():
                future.result(30)
            cli._ensure_dead_letter_topics(case.schema, kafka_bootstrap)
            cli._ensure_retry_topics(case.schema, kafka_bootstrap)
            assert _topic_records(kafka_bootstrap, dlq)[0] == 4  # the widest of the type's topics
            mark = lock_wait.query_log_mark(container)
            server = LeaseServer().__enter__()
            fleet = DlqFleet(case, topics=list(case.edge.topics), edge=case.edge, mode="stream", lease=True,
                             replicas=REPLICAS, static=False, group=f"g-{run_id}", run_id=run_id,
                             lease_address=server.address).start(uri, auth)
            env = {**os.environ, "KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap, "NEO4J_URI": uri,
                   "NEO4J_USERNAME": auth[0], "NEO4J_PASSWORD": auth[1], LEASE_BUCKETS_ENV: "16",
                   LEASE_ADDRESS_ENV: server.address,
                   LEASE_KEYED_LABELS_ENV: "DlqPerson,DlqCompany", "GRAPH_LOADER_METRICS_ENABLED": "0",
                   "KAFKA_GROUP_ID": f"retry-{run_id}"}
            env.pop("NEO4J_DATABASE", None)  # explicit: never inherit an ambient database (also scrubbed by tests/conftest.py)
            retry_proc = subprocess.Popen(
                [sys.executable, "-m", "src.loader.retry_consumer", "--config", str(path), "--run-id", run_id,
                 "--dlq-topics", dlq, "--lease-address", server.address],
                env=env, cwd=str(Path(__file__).resolve().parents[2]),
                stdout=open(tmp_path / "retry-consumer.log", "w"), stderr=subprocess.STDOUT,
            )

            def consumed(topic: str) -> None:
                ends = case.high_watermarks([topic])
                deadline = time.monotonic() + 180
                while case.committed(fleet.group, ends) < sum(ends.values()):
                    assert time.monotonic() < deadline and not fleet.stop_event.is_set(), f"{topic} not consumed"
                    time.sleep(0.5)

            _publish_likes(kafka_bootstrap, topic_b, b_rows)  # first: every row dead-lettered (no nodes yet)
            consumed(topic_b)
            _publish_likes(kafka_bootstrap, topic_a, a_rows)  # later, behind b's rows of the same pairs
            consumed(topic_a)
            with driver.session() as session:
                session.run("UNWIND $ids AS i MERGE (:DlqPerson {person_id: i})",
                            ids=sorted({p for _t, p, *_ in a_rows + b_rows})).consume()
                session.run("UNWIND $ids AS i MERGE (:DlqCompany {company_id: i})",
                            ids=sorted({c for _t, _p, c, *_ in a_rows + b_rows})).consume()
            deadline = time.monotonic() + 300
            while True:
                with driver.session() as session:
                    got = {(r["p"], r["c"]): (date.fromisoformat(str(r["since"])), r["role"]) for r in session.run(
                        "MATCH (p:DlqPerson)-[r:LIKES]->(c:DlqCompany) "
                        "RETURN p.person_id AS p, c.company_id AS c, r.since AS since, r.role AS role")}
                if got == want:
                    break
                assert retry_proc.poll() is None, (tmp_path / "retry-consumer.log").read_text()[-3000:]
                assert time.monotonic() < deadline, (
                    f"graph not exact: {len(got)}/{len(want)} pairs, "
                    f"{sum(1 for k in want if got.get(k) != want[k])} differ")
                time.sleep(2)
            both = {(p, c) for _t, p, c, *_ in a_rows} & {(p, c) for _t, p, c, *_ in b_rows}
            assert len(both) > 100, "the dataset must put many pairs in both topics"
            dlq_partitions, dlq_records = _topic_records(kafka_bootstrap, dlq)
            assert dlq_records >= len(a_rows) + len(b_rows), "every row of both topics went to the type's DLQ"
            assert _topic_records(kafka_bootstrap, parked)[1] == 0, "nothing parked"
            assert not any(n in admin.list_topics(timeout=20).topics for n in (f"{topic_a}.dlq", f"{topic_b}.dlq")), \
                "no per-topic DLQ"
            seen = assert_zero_waits(container, mark, math.ceil(len(want) / EDGE_BATCH))
            print(f"dlq: {dlq_partitions} partitions, {dlq_records} envelopes for {len(a_rows) + len(b_rows)} "
                  f"records, {len(both)} pairs in both topics, {seen} LIKES write entries all waiting 0")
    finally:
        if retry_proc is not None and retry_proc.poll() is None:
            retry_proc.send_signal(signal.SIGTERM)
            try:
                retry_proc.wait(60)
            except subprocess.TimeoutExpired:
                retry_proc.kill()
        if fleet is not None:
            fleet.stop()
        if server is not None:
            server.__exit__(None, None, None)
        try:
            std.delete_topics(kafka_bootstrap, [*case.edge.topics, *family])
        except Exception:  # best effort
            pass
