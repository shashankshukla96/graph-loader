"""Opt-in real acceptance proof for shared-then-isolated relationships.

Set ``RUN_SELF_REFERENCE_ISOLATION_E2E=1`` only with the project's Docker,
Kafka, Neo4j, and images available.  Once explicitly enabled, unavailable
infrastructure fails this test rather than becoming a misleading skip.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from threading import Event, Thread
import time
import uuid

import docker
from docker.errors import NotFound
import pytest
import yaml
from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from neo4j import GraphDatabase

from src.cli import _derive_relationship_phase_run_id, handle_start
from src.utils.schema_loader import load_schema


ROOT = Path(__file__).resolve().parents[1]


def _exact_edge_name(edge_type: str, run_id: str) -> str:
    return f"graph-loader-edge-{edge_type}-0-{run_id.encode().hex()}"


def _fixture_config(topics: dict[str, str], prefix: str) -> dict:
    """Return the smallest valid public-CLI schema for this finite fixture."""
    return {
        "loading": {"consumer_group_id": prefix, "flush_interval_ms": 25, "unwind_batch_size": 1,
                    "coordination": {"topic": topics["clock"], "bucket_count": 1, "slot_duration_ms": 1000, "lease_timeout_ms": 5000}},
        "nodes": [
            {"label": "Person", "topic": topics["person"], "key_property": "id", "properties": {"id": {"type": "string", "required": True}, "name": {"type": "string", "required": True}}},
            {"label": "Company", "topic": topics["company"], "key_property": "id", "properties": {"id": {"type": "string", "required": True}, "name": {"type": "string", "required": True}}},
        ],
        "edges": [
            {"type": "WORKS_AT", "topic": topics["works"], "nodes": {"source": "Person", "target": "Company"}, "source_key_property": "id", "target_key_property": "id", "properties": {"kind": {"type": "string", "required": True}}},
            {"type": "KNOWS", "topic": topics["knows"], "nodes": {"source": "Person", "target": "Person"}, "source_key_property": "id", "target_key_property": "id", "properties": {"kind": {"type": "string", "required": True}}},
        ],
    }


def test_self_reference_e2e_fixture_schema_is_valid(tmp_path: Path) -> None:
    topics = {name: f"fixture-{name}" for name in ("person", "company", "works", "knows", "clock")}
    config_path = tmp_path / "fixture.yaml"
    config_path.write_text(yaml.safe_dump(_fixture_config(topics, "fixture")), encoding="utf-8")
    assert {edge.type for edge in load_schema(config_path).edges} == {"WORKS_AT", "KNOWS"}


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("RUN_SELF_REFERENCE_ISOLATION_E2E") != "1",
    reason="set RUN_SELF_REFERENCE_ISOLATION_E2E=1 for the real self-reference isolation Docker E2E",
)
def test_public_bulk_cli_isolates_person_knows_after_shared_works_at() -> None:
    suffix = uuid.uuid4().hex[:12]
    parent_run_id = f"self-reference-e2e-{suffix}"
    prefix = f"self-reference-e2e-{suffix}"
    shared_run_id = _derive_relationship_phase_run_id(
        parent_run_id, phase_kind="shared", phase_index=0, edge_type="shared",
    )
    isolated_run_id = _derive_relationship_phase_run_id(
        parent_run_id, phase_kind="isolated", phase_index=0, edge_type="KNOWS",
    )
    topics = {name: f"{prefix}-{name}" for name in ("person", "company", "works", "knows", "clock")}
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USERNAME", os.environ.get("NEO4J_USER", "neo4j"))
    password = os.environ.get("NEO4J_PASSWORD", "changeme")
    docker_client = docker.from_env()
    admin = AdminClient({"bootstrap.servers": bootstrap})
    driver = GraphDatabase.driver(uri, auth=(user, password))
    runtime_dir = ROOT / "var" / "self-reference-e2e"
    config_path = runtime_dir / f"{prefix}.yaml"
    created_topics: list[str] = []
    exact_names = {
        "person": f"graph-loader-node-Person-0-{parent_run_id.encode().hex()}",
        "company": f"graph-loader-node-Company-0-{parent_run_id.encode().hex()}",
        "works": _exact_edge_name("WORKS_AT", shared_run_id),
        "clock": f"graph-loader-clock-{shared_run_id.encode().hex()}",
        "knows": _exact_edge_name("KNOWS", isolated_run_id),
    }
    observations: list[tuple[float, str, bool]] = []
    stop_observer = Event()
    observer: Thread | None = None

    def observe_exact_names() -> None:
        while not stop_observer.is_set():
            for name in exact_names.values():
                try:
                    docker_client.containers.get(name)
                    observations.append((time.monotonic(), name, True))
                except NotFound:
                    observations.append((time.monotonic(), name, False))
            time.sleep(0.05)

    config = _fixture_config(topics, prefix)
    try:
        try:
            docker_client.ping()
            docker_client.networks.get("graph-loader-net")
            admin.list_topics(timeout=10)
            driver.verify_connectivity()
            for image in ("graph-loader-node:latest", "graph-loader-edge:latest", "graph-loader-clock:latest"):
                docker_client.images.get(image)
        except Exception as exc:
            pytest.fail(f"RUN_SELF_REFERENCE_ISOLATION_E2E=1 requires Docker graph-loader-net, Kafka, Neo4j, and images: {exc}")

        runtime_dir.mkdir(parents=True, exist_ok=True)
        config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
        futures = admin.create_topics([NewTopic(topic, 1, 1) for topic in topics.values()])
        for topic, future in futures.items():
            future.result(20)
            created_topics.append(topic)
        producer = Producer({"bootstrap.servers": bootstrap})
        person_a, person_b, company = f"{prefix}-a", f"{prefix}-b", f"{prefix}-company"
        for person in (person_a, person_b):
            producer.produce(topics["person"], json.dumps({"id": person, "name": person}).encode())
        producer.produce(topics["company"], json.dumps({"id": company, "name": company}).encode())
        producer.produce(topics["works"], json.dumps({"source": {"id": person_a}, "target": {"id": company}, "properties": {"kind": "works"}}).encode())
        producer.produce(topics["knows"], json.dumps({"source": {"id": person_a}, "target": {"id": person_b}, "properties": {"kind": "knows"}}).encode())
        producer.produce(topics["knows"], json.dumps({"source": {"id": person_b}, "target": {"id": person_a}, "properties": {"kind": "knows"}}).encode())
        assert producer.flush(20) == 0
        observer = Thread(target=observe_exact_names, daemon=True)
        observer.start()
        args = type("Args", (), {"mode": "bulk", "config": str(config_path), "network": "graph-loader-net", "run_id": parent_run_id, "bulk_timeout_seconds": 90, "skip_image_build": True})()
        assert handle_start(args) == 0
        stop_observer.set(); observer.join(5)

        with driver.session() as session:
            assert session.run("MATCH (:Person {id: $a})-[r:WORKS_AT]->(:Company {id: $c}) RETURN count(r) AS n", a=person_a, c=company).single()["n"] == 1
            assert session.run("MATCH (:Person {id: $a})-[r:KNOWS]->(:Person) RETURN count(r) AS n", a=person_a).single()["n"] == 1
            assert session.run("MATCH (:Person {id: $b})-[r:KNOWS]->(:Person) RETURN count(r) AS n", b=person_b).single()["n"] == 1
        for edge_type, topic, run_id in (("WORKS_AT", topics["works"], shared_run_id), ("KNOWS", topics["knows"], isolated_run_id)):
            group = f"{prefix}-{edge_type}-{run_id}"
            response = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group, [TopicPartition(topic, 0)])], require_stable=True)[group].result(20)
            watermark = Consumer({"bootstrap.servers": bootstrap, "group.id": f"{prefix}-watermark-{edge_type}", "enable.auto.commit": False})
            try:
                assert response.topic_partitions[0].offset == watermark.get_watermark_offsets(TopicPartition(topic, 0), timeout=20)[1]
            finally:
                watermark.close()
        first_knows = next((at for at, name, present in observations if name == exact_names["knows"] and present), None)
        assert first_knows is not None, "exact isolated KNOWS container was never observed"
        def removed_after_observed_live(name: str) -> bool:
            saw_live = False
            for at, observed_name, present in observations:
                if at >= first_knows or observed_name != name:
                    continue
                saw_live = saw_live or present
                if saw_live and not present:
                    return True
            return False

        assert removed_after_observed_live(exact_names["works"]), "shared WORKS_AT removal was not observed before KNOWS launch"
        assert removed_after_observed_live(exact_names["clock"]), "shared clock removal was not observed before KNOWS launch"
        for name in exact_names.values():
            with pytest.raises(NotFound):
                docker_client.containers.get(name)
    finally:
        stop_observer.set()
        if observer is not None:
            observer.join(5)
        for name in exact_names.values():
            try:
                docker_client.containers.get(name).stop(timeout=1)
            except NotFound:
                pass
        for topic in created_topics:
            try: admin.delete_topics([topic])[topic].result(20)
            except Exception: pass
        try:
            with driver.session() as session:
                session.run("MATCH (n) WHERE n.id STARTS WITH $prefix DETACH DELETE n", prefix=prefix).consume()
        except Exception:
            # Preserve the explicit prerequisite failure when Neo4j was never
            # reachable; there can be no fixture nodes to remove in that case.
            pass
        finally:
            driver.close()
            try: config_path.unlink()
            except FileNotFoundError: pass
