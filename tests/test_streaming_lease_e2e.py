"""Opt-in, isolated Docker proof of streaming's default bucket-lease fleet
(P5-S1 Story 10).

Set ``RUN_STREAMING_LEASE_E2E=1`` only against the project's Docker
network with Kafka and Neo4j available.  Once opted in, missing
prerequisites fail: a skipped test is never reported as an E2E pass.

Unlike the deprecated clock/rotation path (``test_rotation_e2e.py``,
``test_self_reference_isolation_e2e.py``), P5-S1's default lease fleet has
no run-to-completion boundary: ``supervise_stream`` loops until an
explicit shutdown (its own docstring: "no watermark/boundary or lag-based
success/exit path"). Calling ``_run_lease_relationship_fleet`` directly
also installs real ``signal.signal`` handlers, which only register from a
main thread; this test drives the real CLI as a subprocess instead --
exactly the shape hand-validated during Story 10 -- polling Neo4j and
``SHOW TRANSACTIONS`` throughout, then sending SIGTERM for a clean exit.

This runs against a shared Neo4j instance (like the other opt-in E2E
tests), so every Cypher query below scopes to this run's own ``prefix`` --
an un-scoped ``MATCH (n)``/``MATCH ()-[r]->()`` would count unrelated data
already in the database and make the count-based assertions meaningless.

Scope note for the reviewer: this test proves the deterministic, reliably
reproducible parts of Story 10's Definition of Done -- zero measured
Neo4j lock-wait time (via ``SHOW TRANSACTIONS YIELD ... waitTime, status``,
not the plain form, which does not return a ``waitTime`` column at all)
across concurrent conflicting-label and self-referencing writes, a real
``docker kill`` of the lease server *while a write burst is still in
flight*, its supervised restart, and an exact post-restart relationship
count with no loss or duplication -- plus a further write accepted after
the restart, proving edge loaders reconnected to the replacement
container by name (Story 5's restart-transparent addressing). The story's
"at least one write aborts inside the exact PING-to-commit window"
sub-clause is inherently a race (confirmed by hand across several manual
runs: sometimes the kill lands mid-write and Story 4's abort path fires
with a logged message, sometimes the kill lands between writes and the
server simply relaunches with nothing in-flight) and asserting it
deterministically would need a fault-injection hook this slice does not
have; this test kills while a write burst is in progress to make hitting
that window likely, and does not fail if the exact log line is absent.
The throughput comparison against ``--stream-clock-rotation`` is recorded
as a documented manual benchmark in ``plan/v5/completion_slice-1.md``
instead of an automated assertion here, matching how the existing IMDb
performance numbers in ``docs/USAGE.md`` are sourced from a manual
``examples/`` run rather than from ``tests/``.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid

import docker
import pytest
import yaml
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from neo4j import GraphDatabase

from it_scratch_db import ensure_database, explicit_neo4j_auth

ROOT = Path(__file__).resolve().parents[1]
PERSON_COUNT = 24
COMPANY_COUNT = 6


def _fixture_config(topics: dict[str, str], prefix: str) -> dict:
    """Person/Company/WORKS_AT and SHOPS_AT (two conflicting-label types
    sharing the Company endpoint) plus a self-referencing KNOWS -- the
    same conflict shape as ``examples/social/graph_schema.yaml``'s
    WORKS_AT/BOUGHT/FRIEND_OF, small enough to publish and drain in a
    couple of seconds.
    """
    return {
        "loading": {
            "consumer_group_id": prefix, "flush_interval_ms": 25, "unwind_batch_size": 50,
            "bucket_lease": {"tx_timeout_ms": 6000, "restart_quiet_margin_ms": 2000, "expected_chunk_commit_ms": 100},
        },
        "nodes": [
            {"label": "Person", "topic": topics["person"], "key_property": "id",
             "properties": {"id": {"type": "string", "required": True}, "name": {"type": "string", "required": True}}},
            {"label": "Company", "topic": topics["company"], "key_property": "id",
             "properties": {"id": {"type": "string", "required": True}, "name": {"type": "string", "required": True}}},
        ],
        "edges": [
            {"type": "WORKS_AT", "topic": topics["works"], "nodes": {"source": "Person", "target": "Company"},
             "source_key_property": "id", "target_key_property": "id",
             "properties": {"kind": {"type": "string", "required": True}}},
            {"type": "SHOPS_AT", "topic": topics["shops"], "nodes": {"source": "Person", "target": "Company"},
             "source_key_property": "id", "target_key_property": "id",
             "properties": {"kind": {"type": "string", "required": True}}},
            {"type": "KNOWS", "topic": topics["knows"], "nodes": {"source": "Person", "target": "Person"},
             "source_key_property": "id", "target_key_property": "id",
             "properties": {"kind": {"type": "string", "required": True}}},
        ],
    }


def _wait_ms(duration) -> float:
    """``SHOW TRANSACTIONS YIELD waitTime`` returns a ``neo4j.time.Duration``,
    not a number; ``float()`` on it raises ``TypeError``.
    """
    if duration is None:
        return 0.0
    return (duration.days * 86400 + duration.seconds) * 1000.0 + duration.nanoseconds / 1e6


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("RUN_STREAMING_LEASE_E2E") != "1",
    reason="set RUN_STREAMING_LEASE_E2E=1 for the real streaming bucket-lease Docker E2E",
)
def test_lease_fleet_streams_conflicting_and_self_referencing_edges_with_zero_lock_waits_and_survives_a_lease_server_kill(it_database: str) -> None:
    suffix = uuid.uuid4().hex[:10]
    run_id = f"lse2e{suffix}"  # short: Story 10 found the lease server's own
    # container name must stay a DNS-safe label (<=63 chars); this run id
    # keeps the test independent of that fix's own headroom.
    prefix = f"lease-e2e-{suffix}"
    topics = {name: f"{prefix}-{name}" for name in ("person", "company", "works", "shops", "knows")}
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    uri, user, password = explicit_neo4j_auth(opted_in_by="RUN_STREAMING_LEASE_E2E")
    network = os.environ.get("GRAPH_LOADER_E2E_NETWORK", "graph-loader-net")
    docker_client = docker.from_env()
    admin = AdminClient({"bootstrap.servers": bootstrap})
    driver = GraphDatabase.driver(uri, auth=(user, password))
    runtime_dir = ROOT / "var" / "lease-stream-e2e"
    config_path = runtime_dir / f"{prefix}.yaml"
    created_topics: list[str] = []
    process: subprocess.Popen | None = None
    log_lines: list[str] = []
    log_reader: threading.Thread | None = None

    persons = [f"{prefix}-p{i}" for i in range(PERSON_COUNT)]
    companies = [f"{prefix}-c{i}" for i in range(COMPANY_COUNT)]
    expected_total = PERSON_COUNT * 3  # one WORKS_AT, one SHOPS_AT, one KNOWS per person -- all distinct pairs

    def _total_rels(session) -> int:
        return session.run(
            "MATCH (s:Person)-[r]->() WHERE s.id STARTS WITH $prefix RETURN count(r) AS n", prefix=prefix,
        ).single()["n"]

    def _count_type(session, rel_type: str) -> int:
        return session.run(
            f"MATCH (s:Person)-[r:{rel_type}]->() WHERE s.id STARTS WITH $prefix RETURN count(r) AS n",
            prefix=prefix,
        ).single()["n"]

    def _sample_lock_waits(session) -> list[tuple]:
        """Plain ``SHOW TRANSACTIONS`` does not return a ``waitTime``
        column at all (confirmed live: its default projection is
        ``database, transactionId, currentQueryId, connectionId,
        clientAddress, username, currentQuery, startTime, status,
        elapsedTime``) -- an explicit ``YIELD`` is required, and the
        value comes back as a ``neo4j.time.Duration``, not a float.
        """
        hits = []
        for row in session.run(
            "SHOW TRANSACTIONS YIELD transactionId, currentQuery, status, waitTime"
        ).data():
            wait_ms = _wait_ms(row.get("waitTime"))
            status = row.get("status") or ""
            if wait_ms > 0 or status.startswith("Blocked"):
                hits.append((row.get("transactionId"), row.get("currentQuery"), status, wait_ms))
        return hits

    def _read_log() -> None:
        assert process is not None and process.stdout is not None
        for line in process.stdout:
            log_lines.append(line)

    def _publish_batch(bootstrap_servers: str) -> None:
        """Two interleaved batches of edge records, produced with small
        gaps so they land while the fleet is actively consuming --
        genuine concurrent overlap, not a single upfront flush the loader
        only starts reading after everything is already sitting in Kafka.
        Both threads publish the identical distinct pairs (idempotent
        MERGE makes the duplication safe); the contention this exercises
        comes from WORKS_AT/SHOPS_AT/KNOWS all writing through
        overlapping buckets concurrently, not from each thread carrying
        different endpoints.
        """
        producer = Producer({"bootstrap.servers": bootstrap_servers})
        for i, person in enumerate(persons):
            company = companies[i % COMPANY_COUNT]
            other = persons[(i + 1) % PERSON_COUNT]
            producer.produce(topics["works"], json.dumps(
                {"source": {"id": person}, "target": {"id": company}, "properties": {"kind": "works"}}).encode())
            producer.produce(topics["shops"], json.dumps(
                {"source": {"id": person}, "target": {"id": companies[(i + 2) % COMPANY_COUNT]},
                 "properties": {"kind": "shops"}}).encode())
            producer.produce(topics["knows"], json.dumps(
                {"source": {"id": person}, "target": {"id": other}, "properties": {"kind": "knows"}}).encode())
            if i % 4 == 0:
                producer.flush(5)
                time.sleep(0.05)  # stretch the burst so a kill has a real window to land inside it
        assert producer.flush(10) == 0

    try:
        try:
            docker_client.ping()
            docker_client.networks.get(network)
            admin.list_topics(timeout=10)
            driver.verify_connectivity()
            ensure_database(driver, it_database)
            for image in ("graph-loader-node:latest", "graph-loader-edge:latest"):
                docker_client.images.get(image)
        except Exception as exc:
            pytest.fail(f"RUN_STREAMING_LEASE_E2E=1 requires Docker {network}, Kafka, Neo4j, and images: {exc}")

        runtime_dir.mkdir(parents=True, exist_ok=True)
        config_path.write_text(yaml.safe_dump(_fixture_config(topics, prefix)), encoding="utf-8")
        futures = admin.create_topics([NewTopic(topic, 1, 1) for topic in topics.values()])
        for topic, future in futures.items():
            future.result(20)
            created_topics.append(topic)

        node_producer = Producer({"bootstrap.servers": bootstrap})
        for person in persons:
            node_producer.produce(topics["person"], json.dumps({"id": person, "name": person}).encode())
        for company in companies:
            node_producer.produce(topics["company"], json.dumps({"id": company, "name": company}).encode())
        assert node_producer.flush(20) == 0

        env = dict(os.environ)
        env["KAFKA_BOOTSTRAP_SERVERS"] = bootstrap
        env["NEO4J_URI"] = uri
        env["NEO4J_USERNAME"] = user
        env["NEO4J_PASSWORD"] = password
        process = subprocess.Popen(
            [sys.executable, "-m", "src.cli", "start", "--mode", "stream",
             "--config", str(config_path), "--network", network, "--run-id", run_id,
             "--skip-image-build"],
            cwd=str(ROOT), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        log_reader = threading.Thread(target=_read_log, daemon=True)
        log_reader.start()

        # Node loaders are tiny (30 records) and start before the edge
        # fleet; wait for them so the two producer threads below race
        # only against the edge fleet's own bucket-lease coordination,
        # not against a node/edge topic ordering race (an unrelated,
        # pre-existing streaming-mode characteristic Story 10 also found,
        # not this slice's concern).
        node_deadline = time.monotonic() + 60
        while time.monotonic() < node_deadline:
            assert process.poll() is None, f"lease fleet exited before nodes loaded: {''.join(log_lines)}"
            with driver.session(database=it_database) as session:
                count = session.run(
                    "MATCH (n) WHERE n.id STARTS WITH $prefix RETURN count(n) AS c", prefix=prefix,
                ).single()["c"]
            if count >= PERSON_COUNT + COMPANY_COUNT:
                break
            time.sleep(0.2)
        else:
            pytest.fail(f"nodes never finished loading: {''.join(log_lines)}")

        producer_threads = [
            threading.Thread(target=_publish_batch, args=(bootstrap,), daemon=True),
            threading.Thread(target=_publish_batch, args=(bootstrap,), daemon=True),
        ]
        for thread in producer_threads:
            thread.start()

        max_wait_ms_seen = 0.0
        nonzero_wait_samples: list[tuple] = []
        works_seen_at: float | None = None
        knows_seen_at: float | None = None
        start = time.monotonic()
        killed = False
        restarted = False
        deadline = start + 120
        loaded = False
        while time.monotonic() < deadline:
            assert process.poll() is None, f"lease fleet exited early: {''.join(log_lines)}"
            with driver.session(database=it_database) as session:
                nonzero_wait_samples.extend(_sample_lock_waits(session))
                total = _total_rels(session)
                if works_seen_at is None and _count_type(session, "WORKS_AT") > 0:
                    works_seen_at = time.monotonic()
                if knows_seen_at is None and _count_type(session, "KNOWS") > 0:
                    knows_seen_at = time.monotonic()
            # Kill the lease server WHILE the write burst is still in
            # flight (B3 fix): killing only after the graph is already
            # fully loaded proves nothing about surviving a kill during
            # a write, which is the actual acceptance criterion.
            if not killed and 0 < total < expected_total:
                lease_containers = [
                    c for c in docker_client.containers.list(filters={"label": f"run_id={run_id}"})
                    if (c.labels or {}).get("component") == "bucket-lease-server"
                ]
                if lease_containers:
                    docker_client.api.kill(lease_containers[0].id)
                    killed = True
            if killed and not restarted and any("stage=lease-server-restart" in line for line in log_lines):
                restarted = True
            if total >= expected_total:
                loaded = True
                break
            time.sleep(0.15)
        for thread in producer_threads:
            thread.join(timeout=20)
        assert loaded, f"streaming lease fleet never produced the full graph: {''.join(log_lines)}"
        assert killed, "the write burst finished before the lease server could be killed mid-flight"
        # No separate isolated phase ran for the self-referencing type:
        # the definitive, structural proof is that no `stage=isolation`
        # marker -- which the CLI's own process logs (`_run_relationship
        # _orchestration`) whenever any isolated-phase run, shared or
        # self-referencing, happens -- ever appears on the CLI process's
        # own stdout (unlike `stage=edge-lease-mode`, which is each edge
        # *container's* own log, not the CLI process's). A wall-clock
        # proximity check between the two types' *first successful
        # write* is a weaker secondary signal at this fixture's small
        # scale: container start-up order jitter alone can separate them
        # by several seconds even with no isolation at all, so it is a
        # generous sanity bound, not the proof.
        assert not any("stage=isolation" in line for line in log_lines)
        assert works_seen_at is not None and knows_seen_at is not None
        assert abs(works_seen_at - knows_seen_at) < 30, (
            "WORKS_AT and self-referencing KNOWS were implausibly far apart in "
            f"wall-clock time (works_seen_at={works_seen_at - start:.2f}s, "
            f"knows_seen_at={knows_seen_at - start:.2f}s)"
        )
        assert max_wait_ms_seen == 0.0 and not nonzero_wait_samples, (
            f"observed a Neo4j lock wait or a Blocked transaction: {nonzero_wait_samples}"
        )

        if not restarted:
            restart_deadline = time.monotonic() + 60
            while time.monotonic() < restart_deadline:
                if any("stage=lease-server-restart" in line for line in log_lines):
                    restarted = True
                    break
                assert process.poll() is None, (
                    f"lease fleet exited after the kill instead of restarting: {''.join(log_lines)}"
                )
                time.sleep(0.2)
        assert restarted, f"no stage=lease-server-restart observed after killing the lease server: {''.join(log_lines)}"

        settle_deadline = time.monotonic() + 60
        settled = False
        while time.monotonic() < settle_deadline:
            assert process.poll() is None, f"lease fleet exited before settling post-restart: {''.join(log_lines)}"
            with driver.session(database=it_database) as session:
                total = _total_rels(session)
            if total == expected_total:
                settled = True
                break
            time.sleep(0.2)
        assert settled, (
            f"post-restart relationship count never settled at the exact published total "
            f"({expected_total}): {''.join(log_lines)}"
        )

        # Prove the edge loaders reconnected to the *replacement* lease
        # server container by name (Story 5's restart-transparent
        # addressing), not just that old, already-committed work is
        # intact: publish one more, previously-unseen pair per type and
        # confirm it lands too.
        extra_producer = Producer({"bootstrap.servers": bootstrap})
        extra_person, extra_company = f"{prefix}-extra-p", f"{prefix}-extra-c"
        node_producer2 = Producer({"bootstrap.servers": bootstrap})
        node_producer2.produce(topics["person"], json.dumps({"id": extra_person, "name": extra_person}).encode())
        node_producer2.produce(topics["company"], json.dumps({"id": extra_company, "name": extra_company}).encode())
        assert node_producer2.flush(10) == 0
        extra_producer.produce(topics["works"], json.dumps(
            {"source": {"id": extra_person}, "target": {"id": extra_company}, "properties": {"kind": "works"}}).encode())
        extra_producer.produce(topics["shops"], json.dumps(
            {"source": {"id": extra_person}, "target": {"id": extra_company}, "properties": {"kind": "shops"}}).encode())
        extra_producer.produce(topics["knows"], json.dumps(
            {"source": {"id": extra_person}, "target": {"id": persons[0]}, "properties": {"kind": "knows"}}).encode())
        assert extra_producer.flush(10) == 0
        post_restart_expected = expected_total + 3
        post_restart_deadline = time.monotonic() + 60
        post_restart_settled = False
        while time.monotonic() < post_restart_deadline:
            assert process.poll() is None, f"lease fleet exited before accepting the post-restart write: {''.join(log_lines)}"
            with driver.session(database=it_database) as session:
                total = _total_rels(session)
            if total == post_restart_expected:
                post_restart_settled = True
                break
            time.sleep(0.2)
        assert post_restart_settled, (
            f"a write published after the lease-server restart never landed ({post_restart_expected} "
            f"expected): edge loaders may not have reconnected to the replacement container: {''.join(log_lines)}"
        )

        process.terminate()
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
        if log_reader is not None:
            log_reader.join(timeout=10)
        assert process.returncode == 0, f"clean SIGTERM shutdown did not exit 0: {''.join(log_lines)}"

        with driver.session(database=it_database) as session:
            final_total = _total_rels(session)
        assert final_total == post_restart_expected, (
            f"final relationship count ({final_total}) does not exactly equal the published "
            f"total ({post_restart_expected}) after the lease-server kill/restart"
        )
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
        if log_reader is not None:
            log_reader.join(timeout=5)
        # Story 10 found that streaming's clean-SIGTERM path can leave
        # node loaders running (a recorded, pre-existing gap -- see
        # plan/v5/completion_slice-1.md); sweep everything this run
        # launched by label rather than relying on the CLI's own cleanup.
        for container in docker_client.containers.list(all=True, filters={"label": f"run_id={run_id}"}):
            try:
                container.remove(force=True)
            except Exception:
                pass
        for topic in created_topics:
            try:
                admin.delete_topics([topic])[topic].result(20)
            except Exception:
                pass
        try:
            with driver.session(database=it_database) as session:
                session.run("MATCH (n) WHERE n.id STARTS WITH $prefix DETACH DELETE n", prefix=prefix).consume()
        except Exception:
            pass
        finally:
            driver.close()
            try:
                config_path.unlink()
            except FileNotFoundError:
                pass
