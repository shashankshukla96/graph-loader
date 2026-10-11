"""v7-S7 story 7.11: one relationship type over two label pairs, real Neo4j + Kafka, zero lock waits.

``LIKES`` reads ``likes-posts-a`` and ``likes-posts-b`` (Person->Post, the same pair, so ``order_by`` or the
conflict rule decides) and ``likes-comments`` (Person->Comment). Post and Comment ids are the SAME (0..ITEMS-1), and
the Comment likes reuse 70% of the Post likes' ``(person, id)`` keys, so a loader that ever mixed the pairs would
write wrong or extra relationships. ``GRAPH_LOADER_MULTI_PAIR_PAIRS`` (default 20,000, a multiple of 20) labelled
pairs are loaded, half per label pair. Hubs: Post 0 and Comment 0 (a quarter of each pair's likes) and Person 0
(likes every post and every comment).

Cases (one query-logged Neo4j 5.26 container for the module, in-process loaders like the multi-topic module):

* ``order_by``: stream lease fleet (3 replicas), bulk lease (3 replicas, static assignment), planned (real planner,
  dispatcher, 4 writers; the plan has 2 pairs), bulk classic (1 replica, waits reported only). The graph is exact
  per labelled pair (the newest ``updated_at`` of each pair, over both Post topics) with no cross-talk.
* ``arrival`` (stream lease): counts per pair, every stored value was published for its pair.
* ``dead_letter`` (stream lease, ``arrival``, the real retry consumer): Comment nodes are seeded late, so every
  Comment row goes to the type's one DLQ with a labelled ``[S, T, sk, tk]`` key while the Post rows are written;
  after the late seeding the graph is exact and the parking lot empty.

Every zero-wait case asserts a ``waiting`` sum of 0 over at least the expected number of LIKES write transactions.
Skips cleanly without Docker.
"""
from __future__ import annotations

import json
import math
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import pytest
import yaml
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from neo4j import GraphDatabase

from src import cli
from src.loader.control import CONTROL_TOPIC
from src.loader.dead_letter import build_malformed_record_sink, decode_envelope
from src.loader.edge_execution import build_edge_execution, streaming_lease_client_factory
from src.loader.edge_loader import EdgeLoader, apply_edge_outer_batch_override
from src.loader.lease_client import LEASE_ADDRESS_ENV, LEASE_BUCKETS_ENV, LEASE_KEYED_LABELS_ENV
from src.loader.parked_pair_index import ParkedPairIndex
from src.models.schema import edge_dead_letter_topic_name, edge_parked_pair_index_topic_name, edge_parked_topic_name
from src.utils.schema_loader import load_schema
from tests.integration import test_multi_topic_relationship as _multi_topic
from tests.integration.test_multi_topic_relationship import Fleet, LeaseServer, assert_zero_waits, likes_write_waits
from tests.stress import lock_wait, planned_harness
from tests.stress import star_topology_data as std

pytestmark = pytest.mark.integration

# The multi-topic module's query-logged Neo4j fixture (module scope: one container for this module's cases).
neo4j_logged = _multi_topic.neo4j_logged

PAIRS = int(os.environ.get("GRAPH_LOADER_MULTI_PAIR_PAIRS", "20000"))
assert PAIRS % 20 == 0 and PAIRS >= 400, "GRAPH_LOADER_MULTI_PAIR_PAIRS must be a multiple of 20 (>= 400)"
PER_PAIR = PAIRS // 2
ITEMS = 100  # Post ids == Comment ids == 0..ITEMS-1
REPLICAS = 3
PARTITIONS = 3
EDGE_BATCH = 2000
LABEL_PAIRS = (("Person", "Post"), ("Person", "Comment"))


# -- dataset ----------------------------------------------------------------------------------------------


def post_pairs() -> list[tuple[int, int]]:
    """``PER_PAIR`` distinct (person, post) pairs: a quarter into Post 0, Person 0 likes every post."""
    pairs = [(0, item) for item in range(ITEMS)]
    quarter = PER_PAIR // 4
    pairs += [(1 + k, 0) for k in range(quarter)]  # Post 0 hub
    rest = PER_PAIR - len(pairs)
    pairs += [(1 + quarter + k, 1 + k % (ITEMS - 1)) for k in range(rest)]
    assert len(set(pairs)) == PER_PAIR
    return pairs


def comment_pairs() -> list[tuple[int, int]]:
    """``PER_PAIR`` distinct (person, comment) pairs; 70% reuse a Post pair's exact keys."""
    out: list[tuple[int, int]] = []
    seen: set = set()
    for k, (person, item) in enumerate(post_pairs()):
        # Person 0 (the hub) keeps every key; every other person has one like per pair, so a shift is unique.
        pair = (person, item) if k % 10 < 7 or person == 0 else (person, (item + 37) % ITEMS)
        assert pair not in seen
        seen.add(pair)
        out.append(pair)
    assert len(set(out)) == PER_PAIR
    return out


def build_records() -> dict[str, list[dict]]:
    """Records per topic suffix ``a``/``b`` (Post) and ``c`` (Comment). Unique ``updated_at`` per record; 30% of the
    Post pairs are in both Post topics with the newer value in a for half of them and in b for the other half."""
    out: dict[str, list[dict]] = {"a": [], "b": [], "c": []}
    for k, (person, post) in enumerate(post_pairs()):
        older, newer = 1_000 + k, 1_000_000 + k

        def rec(src: str, updated_at: int, person=person, post=post) -> dict:
            return {"source": {"person_id": person}, "target": {"post_id": post},
                    "properties": {"updated_at": updated_at, "src": src}}

        if k % 10 < 3:
            newer_in_a = (k // 10) % 2 == 0
            out["a"].append(rec("a", newer if newer_in_a else older))
            out["b"].append(rec("b", older if newer_in_a else newer))
        else:
            out["a" if k % 2 == 0 else "b"].append(rec("a" if k % 2 == 0 else "b", older))
    for k, (person, comment) in enumerate(comment_pairs()):
        # Half of the Comment values are OLDER than every Post value and half newer, so a leak in either
        # direction on a colliding key leaves a wrong (updated_at, src) under order_by (review round 6 S1).
        updated_at = -(k + 1) if k % 2 == 0 else 2_000_000 + k
        out["c"].append({"source": {"person_id": person}, "target": {"comment_id": comment},
                         "properties": {"updated_at": updated_at, "src": "c"}})
    return out


def expected_order_by() -> dict[tuple[str, str], dict[tuple[int, int], tuple[int, str]]]:
    best: dict = {pair: {} for pair in LABEL_PAIRS}
    for suffix, records in build_records().items():
        label_pair = LABEL_PAIRS[1] if suffix == "c" else LABEL_PAIRS[0]
        key_name = "comment_id" if suffix == "c" else "post_id"
        for rec in records:
            key = (rec["source"]["person_id"], rec["target"][key_name])
            value = (rec["properties"]["updated_at"], rec["properties"]["src"])
            if key not in best[label_pair] or value[0] > best[label_pair][key][0]:
                best[label_pair][key] = value
    return best


def published_values() -> dict:
    out: dict = {pair: {} for pair in LABEL_PAIRS}
    for suffix, records in build_records().items():
        label_pair = LABEL_PAIRS[1] if suffix == "c" else LABEL_PAIRS[0]
        key_name = "comment_id" if suffix == "c" else "post_id"
        for rec in records:
            key = (rec["source"]["person_id"], rec["target"][key_name])
            out[label_pair].setdefault(key, set()).add((rec["properties"]["updated_at"], rec["properties"]["src"]))
    return out


def max_person() -> int:
    return max(person for person, _ in post_pairs() + comment_pairs())


# -- schema and case ----------------------------------------------------------------------------------------


def schema_dict(run_id: str, *, rule: str, mode: str, replicas: int, labels=("Person", "Post", "Comment"),
                dead_letter: dict | None = None) -> dict:
    person, post, comment = labels

    def entry(suffix: str, target: str, key: str) -> dict:
        return {"name": f"mp-{run_id}-likes-{suffix}", "nodes": {"source": person, "target": target},
                "source_key_property": "person_id", "target_key_property": key}

    edge: dict = {
        "type": "LIKES",
        "topics": [entry("posts-a", post, "post_id"), entry("posts-b", post, "post_id"),
                   entry("comments", comment, "comment_id")],
        "replicas": replicas,
        "properties": {"updated_at": {"type": "integer", "required": True}, "src": {"type": "string", "required": True}},
        "execution": {"mode": "python_apoc", "worker_count": 4},
    }
    if rule == "order_by":
        edge["order_by"] = "updated_at"
    else:
        edge["topic_conflict"] = rule
    if dead_letter is not None:
        edge["dead_letter"] = dead_letter
        edge["retry"] = {"base_delay_seconds": 1.0, "max_delay_seconds": 5.0}

    def node(label: str, key: str) -> dict:
        return {"label": label, "topic": f"mp-{run_id}-{label.lower()}", "key_property": key,
                "properties": {key: {"type": "integer", "required": True, "constraint": {"type": "uniqueness"}}}}

    return {
        "loading": {"mode": mode, "consumer_group_id": f"mp-{run_id}", "unwind_batch_size": 500,
                    "edge_unwind_batch_size": EDGE_BATCH, "flush_interval_ms": 50},
        "nodes": [node(person, "person_id"), node(post, "post_id"), node(comment, "comment_id")],
        "edges": [edge],
    }


class Case:
    """The subset of the multi-topic module's ``Case`` interface that ``Fleet`` uses."""

    def __init__(self, tmp_path: Path, driver, bootstrap: str, **schema_kwargs) -> None:
        self.run_id = uuid.uuid4().hex[:8]
        self.driver, self.bootstrap, self.tmp_path = driver, bootstrap, tmp_path
        path = tmp_path / f"schema-{self.run_id}.yaml"
        path.write_text(yaml.safe_dump(schema_dict(self.run_id, **schema_kwargs)), encoding="utf-8")
        self.path = path
        self.schema = load_schema(path)
        self.edge = self.schema.edges[0]
        assert self.edge.is_multi_pair
        self.per_topic_records: dict[str, int] = {}

    @property
    def total_records(self) -> int:
        return sum(self.per_topic_records.values())

    def create_topics(self, partitions=PARTITIONS) -> None:
        admin = AdminClient({"bootstrap.servers": self.bootstrap})
        wanted = [NewTopic(t, num_partitions=partitions, replication_factor=1) for t in self.edge.topics]
        wanted.append(NewTopic(CONTROL_TOPIC, num_partitions=1, replication_factor=1))
        existing = admin.list_topics(timeout=20).topics
        for future in admin.create_topics([t for t in wanted if t.topic not in existing]).values():
            future.result(30)

    def publish(self, suffixes=("a", "b", "c")) -> None:
        records = build_records()
        producer = Producer({"bootstrap.servers": self.bootstrap, "linger.ms": 20, "compression.type": "lz4",
                             "queue.buffering.max.messages": 1_000_000})
        for topic, suffix in zip(self.edge.topics, ("a", "b", "c")):
            if suffix not in suffixes:
                continue
            self.per_topic_records[topic] = len(records[suffix])
            for rec in records[suffix]:
                payload = json.dumps(rec, separators=(",", ":")).encode()
                while True:
                    try:
                        producer.produce(topic, key=str(rec["source"]["person_id"]).encode(), value=payload)
                        break
                    except BufferError:
                        producer.poll(0.05)
        assert producer.flush(120) == 0

    def seed_nodes(self, labels) -> None:
        std.ensure_constraints(self.driver, self.schema)
        keys = {node.label: node.key_property for node in self.schema.nodes}
        with self.driver.session() as session:
            for label in labels:
                upper = max_person() if keys[label] == "person_id" else ITEMS - 1
                session.run(f"UNWIND range(0, $n) AS i MERGE (:`{label}` {{`{keys[label]}`: i}})", n=upper).consume()

    def clear_likes(self) -> None:
        with self.driver.session() as session:
            session.run("CALL () { MATCH ()-[r:LIKES]->() DELETE r } IN TRANSACTIONS OF 5000 ROWS").consume()

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

    def cleanup(self, extra=()) -> None:
        try:
            std.delete_topics(self.bootstrap, [*self.edge.topics, *extra])
        except Exception:  # best effort
            pass


class MultiPairFleet(Fleet):
    def _environ(self) -> dict:
        if not self.lease:
            return {}
        return {LEASE_ADDRESS_ENV: self.lease_address, LEASE_BUCKETS_ENV: "16", LEASE_KEYED_LABELS_ENV: ""}


# -- verification -------------------------------------------------------------------------------------------


def read_pairs(driver, labels=("Person", "Post", "Comment")) -> tuple[dict, int]:
    person, post, comment = labels
    out: dict = {}
    with driver.session() as session:
        for (s_label, t_label), (target, key) in zip(LABEL_PAIRS, ((post, "post_id"), (comment, "comment_id"))):
            rows = session.run(
                f"MATCH (s:`{person}`)-[r:LIKES]->(t:`{target}`) RETURN s.person_id AS s, t.`{key}` AS t, "
                "properties(r) AS p").data()
            out[(s_label, t_label)] = {(row["s"], row["t"]): row["p"] for row in rows}
        # Every LIKES of this label set, whatever its target: a cross-pair write would be counted here.
        total = session.run(f"MATCH (:`{person}`)-[r:LIKES]->() RETURN count(r) AS c").single()["c"]
    return out, total


def assert_order_by(driver, labels=("Person", "Post", "Comment")) -> None:
    stored, total = read_pairs(driver, labels)
    want = expected_order_by()
    assert total == sum(len(v) for v in want.values()) == PAIRS, f"{total} LIKES for {PAIRS} labelled pairs"
    for label_pair in LABEL_PAIRS:
        got = stored[label_pair]
        assert set(got) == set(want[label_pair]), f"{label_pair}: {len(set(got) ^ set(want[label_pair]))} keys differ"
        wrong = [k for k, (value, src) in want[label_pair].items()
                 if (got[k].get("updated_at"), got[k].get("src")) != (value, src)]
        assert not wrong, f"{label_pair}: {len(wrong)} pairs wrong, e.g. {wrong[:3]}"
    assert len(set(stored[LABEL_PAIRS[0]]) & set(stored[LABEL_PAIRS[1]])) >= PER_PAIR // 2  # colliding keys


def assert_arrival(driver) -> None:
    stored, total = read_pairs(driver)
    published = published_values()
    assert total == PAIRS
    for label_pair in LABEL_PAIRS:
        assert set(stored[label_pair]) == set(published[label_pair])
        bad = [k for k, p in stored[label_pair].items() if (p.get("updated_at"), p.get("src")) not in published[label_pair][k]]
        assert not bad, f"{label_pair}: {len(bad)} pairs hold a value never published for them"


# -- cases --------------------------------------------------------------------------------------------------


def _drive(neo4j_logged, kafka_bootstrap, tmp_path, *, rule, mode, runner, verify, zero_wait=True,
           replicas=REPLICAS):
    container, driver = neo4j_logged
    case = Case(tmp_path, driver, kafka_bootstrap, rule=rule, mode=mode, replicas=replicas)
    uri, auth = container.get_connection_url(), (container.username, container.password)
    try:
        with lock_wait.hard_timeout(900):
            case.seed_nodes(("Person", "Post", "Comment"))
            case.clear_likes()
            case.create_topics()
            case.publish()
            mark = lock_wait.query_log_mark(container)
            stop, blocked, samples = threading.Event(), [], []
            poller = threading.Thread(target=lock_wait.poll_show_transactions,
                                      args=(driver, stop, blocked, 0.05, samples), daemon=True)
            poller.start()
            try:
                expected_min = runner(case, uri, auth)
            finally:
                stop.set()
                poller.join(5)
            verify(driver)
            names = set(AdminClient({"bootstrap.servers": kafka_bootstrap}).list_topics(timeout=20).topics)
            assert "LIKES.dlq" not in names and not {f"{t}.dlq" for t in case.edge.topics} & names
            if zero_wait:
                seen = assert_zero_waits(container, mark, expected_min)
                assert samples and blocked == [], f"blocked transactions: {blocked[:3]}"
                print(f"zero-wait: {seen} LIKES write entries (>= {expected_min}), {len(samples)} poll samples")
            else:
                print(f"classic lock wait (informational): {sum(likes_write_waits(container, mark))} ms")
    finally:
        case.cleanup()


def _single_stage(*, mode: str, lease: bool, replicas: int, static: bool):
    def run(case: Case, uri, auth) -> int:
        server = LeaseServer().__enter__() if lease else None
        try:
            MultiPairFleet(case, topics=list(case.edge.topics), edge=case.edge, mode=mode, lease=lease,
                           replicas=replicas, static=static, group=f"g-{case.run_id}", run_id=case.run_id,
                           lease_address=server.address if server else None).run(uri, auth)
        finally:
            if server:
                server.__exit__(None, None, None)
        return math.ceil(case.total_records / EDGE_BATCH)
    return run


def _planned(case: Case, uri, auth) -> int:
    driver = GraphDatabase.driver(uri, auth=auth)
    try:
        result = planned_harness.run_planned_mode(case.schema, driver, case.bootstrap, plan_dir=case.tmp_path,
                                                  writers=4, timeout_s=420, chunk_rows=1000, plan_hub_degree=150)
        assert result.total_attempts_over_one == 0
        item = result.plan.types["LIKES"]
        assert [pair.labels for pair in item.pairs] == list(LABEL_PAIRS)
        assert {chunk.pair for chunk in result.plan.chunks} == {0, 1}
        hubs = {result.plan.resources.describe(r) for r in range(result.plan.resources.count)}
        assert {"Person#hub=0", "Post#hub=0", "Comment#hub=0"} <= hubs, sorted(h for h in hubs if "hub" in h)
        return len(result.plan.chunks)
    finally:
        driver.close()


def test_order_by_stream_lease_fleet(neo4j_logged, kafka_bootstrap, tmp_path):
    _drive(neo4j_logged, kafka_bootstrap, tmp_path, rule="order_by", mode="stream",
           runner=_single_stage(mode="stream", lease=True, replicas=REPLICAS, static=False), verify=assert_order_by)


def test_order_by_bulk_lease_static_assignment(neo4j_logged, kafka_bootstrap, tmp_path):
    _drive(neo4j_logged, kafka_bootstrap, tmp_path, rule="order_by", mode="bulk",
           runner=_single_stage(mode="bulk", lease=True, replicas=REPLICAS, static=True), verify=assert_order_by)


def test_order_by_planned(neo4j_logged, kafka_bootstrap, tmp_path):
    _drive(neo4j_logged, kafka_bootstrap, tmp_path, rule="order_by", mode="bulk", runner=_planned,
           verify=assert_order_by)


def test_order_by_bulk_classic_single_replica(neo4j_logged, kafka_bootstrap, tmp_path):
    _drive(neo4j_logged, kafka_bootstrap, tmp_path, rule="order_by", mode="bulk", replicas=1,
           runner=_single_stage(mode="bulk", lease=False, replicas=1, static=True), verify=assert_order_by,
           zero_wait=False)


def test_arrival_stream_lease(neo4j_logged, kafka_bootstrap, tmp_path):
    _drive(neo4j_logged, kafka_bootstrap, tmp_path, rule="arrival", mode="stream",
           runner=_single_stage(mode="stream", lease=True, replicas=REPLICAS, static=False), verify=assert_arrival)


# -- dead letters: Comment nodes seeded late, the real retry consumer ---------------------------------------

DLQ_LABELS = ("MpdPerson", "MpdPost", "MpdComment")


class MultiPairDlqFleet(MultiPairFleet):
    """A stream lease fleet wired like ``edge_loader.main`` for a DLQ-enabled multi-pair type."""

    def _run_replica(self, index: int) -> None:
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
                                     sequenced=self.edge.is_multi_topic,
                                     pair_of_topic=self.edge.pair_labels_for_topic)
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


def _dlq_envelopes(bootstrap: str, topic: str) -> list:
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"dlq-read-{uuid.uuid4().hex[:6]}",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest", "enable.partition.eof": True})
    try:
        meta = consumer.list_topics(topic, timeout=20).topics[topic]
        consumer.assign([TopicPartition(topic, p, 0) for p in meta.partitions])
        remaining, out, deadline = set(meta.partitions), [], time.monotonic() + 60
        while remaining and time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error() is not None:
                remaining.discard(message.partition())
                continue
            out.append(decode_envelope(message.value()))
        return out
    finally:
        consumer.close()


def test_dead_letter_comment_nodes_seeded_late_retried_per_pair(neo4j_logged, kafka_bootstrap, tmp_path):
    container, driver = neo4j_logged
    case = Case(tmp_path, driver, kafka_bootstrap, rule="arrival", mode="stream", replicas=REPLICAS,
                labels=DLQ_LABELS, dead_letter={"enabled": True, "max_retry_attempts": 200})
    edge = case.edge
    dlq, parked = edge_dead_letter_topic_name(edge), edge_parked_topic_name(edge)
    family = [dlq, parked, f"{dlq}.parked-pairs"]
    uri, auth = container.get_connection_url(), (container.username, container.password)
    retry_proc = server = fleet = None
    try:
        with lock_wait.hard_timeout(900):
            with driver.session() as session:
                session.run("MATCH (n) WHERE n:MpdPerson OR n:MpdPost OR n:MpdComment DETACH DELETE n").consume()
            case.seed_nodes(DLQ_LABELS[:2])  # Person and Post exist; Comment arrives late
            case.create_topics()
            cli._ensure_dead_letter_topics(case.schema, kafka_bootstrap)
            cli._ensure_retry_topics(case.schema, kafka_bootstrap)
            mark = lock_wait.query_log_mark(container)
            server = LeaseServer().__enter__()
            fleet = MultiPairDlqFleet(case, topics=list(edge.topics), edge=edge, mode="stream", lease=True,
                                      replicas=REPLICAS, static=False, group=f"g-{case.run_id}", run_id=case.run_id,
                                      lease_address=server.address).start(uri, auth)
            env = {**os.environ, "KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap, "NEO4J_URI": uri,
                   "NEO4J_USERNAME": auth[0], "NEO4J_PASSWORD": auth[1], LEASE_BUCKETS_ENV: "16",
                   LEASE_ADDRESS_ENV: server.address, LEASE_KEYED_LABELS_ENV: "",
                   "GRAPH_LOADER_METRICS_ENABLED": "0", "KAFKA_GROUP_ID": f"retry-{case.run_id}"}
            env.pop("NEO4J_DATABASE", None)
            retry_proc = subprocess.Popen(
                [sys.executable, "-m", "src.loader.retry_consumer", "--config", str(case.path), "--run-id",
                 case.run_id, "--dlq-topics", dlq, "--lease-address", server.address],
                env=env, cwd=str(Path(__file__).resolve().parents[2]),
                stdout=open(tmp_path / "retry-consumer.log", "w"), stderr=subprocess.STDOUT,
            )
            case.publish()
            ends = case.high_watermarks(list(edge.topics))
            deadline = time.monotonic() + 240
            while case.committed(fleet.group, ends) < sum(ends.values()):
                assert time.monotonic() < deadline and not fleet.stop_event.is_set(), "topics not consumed"
                time.sleep(0.5)
            stored, _total = read_pairs(driver, DLQ_LABELS)
            want = published_values()
            assert stored[LABEL_PAIRS[1]] == {}  # no Comment node yet: no Comment like
            # Every Post like is written while every Comment row is parked: a parked Comment (p, i) never
            # blocks the colliding live Post (p, i) (review round 6 S2).
            assert set(stored[LABEL_PAIRS[0]]) == set(want[LABEL_PAIRS[0]])
            case.seed_nodes(DLQ_LABELS[2:])  # the Comment nodes arrive
            deadline = time.monotonic() + 300
            while True:
                stored, total = read_pairs(driver, DLQ_LABELS)
                if total == PAIRS and all(set(stored[p]) == set(want[p]) for p in LABEL_PAIRS):
                    break
                assert retry_proc.poll() is None, (tmp_path / "retry-consumer.log").read_text()[-3000:]
                assert time.monotonic() < deadline, f"graph not exact: {total}/{PAIRS} LIKES"
                time.sleep(2)
            for label_pair in LABEL_PAIRS:
                bad = [k for k, p in stored[label_pair].items()
                       if (p.get("updated_at"), p.get("src")) not in want[label_pair][k]]
                assert not bad, f"{label_pair}: {len(bad)} pairs hold a value never published for them"
            envelopes = _dlq_envelopes(kafka_bootstrap, dlq)
            assert len(envelopes) >= PER_PAIR  # every Comment row went to the type's one DLQ
            comment_topic = edge.topics[2]
            for envelope in envelopes:
                key = json.loads(envelope.key_bytes)
                assert len(key) == 4 and tuple(key[:2]) == edge.pair_labels_for_topic(envelope.original_topic)
            assert all(e.original_topic == comment_topic for e in envelopes), "a Post row entered the DLQ"
            probe = Consumer({"bootstrap.servers": kafka_bootstrap, "group.id": f"probe-{case.run_id}"})
            try:
                parked_meta = probe.list_topics(parked, timeout=20).topics[parked]
                parked_records = sum(probe.get_watermark_offsets(TopicPartition(parked, p), timeout=20)[1]
                                     for p in parked_meta.partitions)
            finally:
                probe.close()
            assert parked_records == 0, "nothing parked"
            seen = assert_zero_waits(container, mark, math.ceil(PAIRS / EDGE_BATCH))
            print(f"dlq: {len(envelopes)} envelopes, all keys labelled, {seen} LIKES write entries all waiting 0")
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
        case.cleanup(family)
