"""Opt-in, isolated Docker proof of label-scoped relationship rotation.

Set RUN_ROTATING_RELATIONSHIP_E2E=1 only against the project's Docker network
with Kafka and Neo4j available.  Once opted in, missing prerequisites fail: a
skipped test is never reported as an E2E pass.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import time
import uuid
from types import SimpleNamespace

import docker
from docker.errors import NotFound
import pytest
import yaml
from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from neo4j import GraphDatabase

from src.cli import _run_rotating_relationship_fleet
from src.loader.mix_and_batch import endpoint_bucket
from src.orchestrator.coordination import decode_clock_lease
from src.orchestrator.dependency_manager import build_conflict_families
from src.orchestrator.docker_service import DockerService
from src.orchestrator.rotation import build_rotation_plan
from src.utils.schema_loader import load_schema

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("RUN_ROTATING_RELATIONSHIP_E2E") != "1",
    reason="set RUN_ROTATING_RELATIONSHIP_E2E=1 for the real rotating relationship Docker E2E",
)
def test_shared_person_rotation_bulk_e2e() -> None:
    """Provision finite isolated inputs and prove rotation, graph, offsets, cleanup."""
    suffix = uuid.uuid4().hex[:12]
    run_id = f"rotation-e2e-{suffix}"
    run_hex = run_id.encode("utf-8").hex()
    prefix = f"rotation-e2e-{suffix}"
    topics = {name: f"{prefix}-{name}" for name in ("person", "company", "product", "works", "bought", "clock")}
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USERNAME", os.environ.get("NEO4J_USER", "neo4j"))
    password = os.environ.get("NEO4J_PASSWORD", "changeme")
    docker_client = docker.from_env()
    admin = AdminClient({"bootstrap.servers": bootstrap})
    driver = GraphDatabase.driver(uri, auth=(user, password))
    created_topics: list[str] = []
    exact_container_names = [
        f"graph-loader-edge-WORKS_AT-0-{run_hex}",
        f"graph-loader-edge-BOUGHT-0-{run_hex}",
        f"graph-loader-clock-{run_hex}",
    ]
    person_by_bucket: dict[int, str] = {}
    candidate = 0
    while len(person_by_bucket) < 2:
        person_id = f"{prefix}-person-{candidate}"
        person_by_bucket.setdefault(endpoint_bucket(person_id, 2), person_id)
        candidate += 1
    person_ids = list(person_by_bucket.values())
    assert {endpoint_bucket(person_id, 2) for person_id in person_ids} == {0, 1}
    company_id, product_id = f"{prefix}-company", f"{prefix}-product"
    # Docker Desktop mounts the project workspace but not arbitrary pytest
    # /tmp directories.  This exact UUID-named file is removed in finally.
    runtime_config_dir = ROOT / "var" / "rotation-e2e"
    runtime_config_dir.mkdir(parents=True, exist_ok=True)
    config_path = runtime_config_dir / f"{prefix}.yaml"
    config = {
        "loading": {
            "consumer_group_id": prefix,
            "flush_interval_ms": 25,
            "unwind_batch_size": 1,
            "coordination": {
                "topic": topics["clock"], "bucket_count": 2,
                "slot_duration_ms": 10_000, "lease_timeout_ms": 30_000,
            },
        },
        "nodes": [
            {"label": "Person", "topic": topics["person"], "key_property": "id", "properties": {"id": {"type": "string", "required": True}, "name": {"type": "string", "required": True}}},
            {"label": "Company", "topic": topics["company"], "key_property": "id", "properties": {"id": {"type": "string", "required": True}, "name": {"type": "string", "required": True}}},
            {"label": "Product", "topic": topics["product"], "key_property": "id", "properties": {"id": {"type": "string", "required": True}, "name": {"type": "string", "required": True}}},
        ],
        "edges": [
            {"type": "WORKS_AT", "topic": topics["works"], "nodes": {"source": "Person", "target": "Company"}, "source_key_property": "id", "target_key_property": "id", "properties": {"kind": {"type": "string", "required": True}}},
            {"type": "BOUGHT", "topic": topics["bought"], "nodes": {"source": "Person", "target": "Product"}, "source_key_property": "id", "target_key_property": "id", "properties": {"kind": {"type": "string", "required": True}}},
        ],
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    try:
        try:
            docker_client.ping()
            docker_client.networks.get("graph-loader-net")
            admin.list_topics(timeout=10)
            driver.verify_connectivity()
        except Exception as exc:
            pytest.fail(f"RUN_ROTATING_RELATIONSHIP_E2E=1 requires Docker graph-loader-net, Kafka, and Neo4j: {exc}")

        futures = admin.create_topics([NewTopic(topic, 1, 1) for topic in topics.values()])
        for topic, future in futures.items():
            future.result(20)
            created_topics.append(topic)

        # The CLI intentionally uses --skip-image-build below.  Prove all
        # three exact runtime images already exist rather than silently
        # depending on a build side effect during this acceptance run.
        for image in ("graph-loader-node:latest", "graph-loader-edge:latest", "graph-loader-clock:latest"):
            try:
                docker_client.images.get(image)
            except Exception as exc:
                pytest.fail(f"RUN_ROTATING_RELATIONSHIP_E2E=1 requires exact image {image}: {exc}")
        with driver.session() as session:
            for person_id in person_ids:
                session.run("CREATE (:Person {id: $id, name: $id, e2e_run: $run})", id=person_id, run=run_id).consume()
            session.run("CREATE (:Company {id: $id, name: $id, e2e_run: $run})", id=company_id, run=run_id).consume()
            session.run("CREATE (:Product {id: $id, name: $id, e2e_run: $run})", id=product_id, run=run_id).consume()

        producer = Producer({"bootstrap.servers": bootstrap})
        for person_id in person_ids:
            producer.produce(topics["person"], json.dumps({"id": person_id, "name": person_id}).encode())
        producer.produce(topics["company"], json.dumps({"id": company_id, "name": company_id}).encode())
        producer.produce(topics["product"], json.dumps({"id": product_id, "name": product_id}).encode())
        for person_id in person_ids:
            producer.produce(topics["works"], json.dumps({"source": {"id": person_id}, "target": {"id": company_id}, "properties": {"kind": "works"}}).encode())
            producer.produce(topics["bought"], json.dumps({"source": {"id": person_id}, "target": {"id": product_id}, "properties": {"kind": "bought"}}).encode())
        assert producer.flush(20) == 0

        schema = load_schema(config_path)
        fleet_args = SimpleNamespace(
            mode="bulk", config=str(config_path), network="graph-loader-net",
            run_id=run_id, bulk_timeout_seconds=90,
        )
        assert _run_rotating_relationship_fleet(schema, DockerService(client=docker_client), fleet_args) == 0

        with driver.session() as session:
            assert session.run("MATCH (:Person {e2e_run: $run})-[r:WORKS_AT]->(:Company {e2e_run: $run}) RETURN count(r) AS n", run=run_id).single()["n"] == 2
            assert session.run("MATCH (:Person {e2e_run: $run})-[r:BOUGHT]->(:Product {e2e_run: $run}) RETURN count(r) AS n", run=run_id).single()["n"] == 2

        for edge_type, topic_name in (("WORKS_AT", topics["works"]), ("BOUGHT", topics["bought"])):
            group = f"{prefix}-{edge_type}-{run_id}"
            request = ConsumerGroupTopicPartitions(group, [TopicPartition(topic_name, 0)])
            response = admin.list_consumer_group_offsets([request], require_stable=True)[group].result(20)
            committed = response.topic_partitions[0].offset
            watermark = Consumer({"bootstrap.servers": bootstrap, "group.id": f"{prefix}-watermark-{edge_type}", "enable.auto.commit": False})
            try:
                assert committed == watermark.get_watermark_offsets(TopicPartition(topic_name, 0), timeout=20)[1]
            finally:
                watermark.close()

        rotation = build_rotation_plan(build_conflict_families(schema.edges), bucket_count=2)
        lease_consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"{prefix}-lease-proof", "enable.auto.commit": False, "auto.offset.reset": "earliest"})
        owner_epochs: dict[str, set[int]] = {"WORKS_AT": set(), "BOUGHT": set()}
        try:
            lease_consumer.subscribe([topics["clock"]])
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline and not all(owner_epochs.values()):
                message = lease_consumer.poll(0.5)
                if message is None or message.error():
                    continue
                raw = json.loads(message.value().decode())
                if raw.get("run_id") != run_id:
                    continue
                lease = decode_clock_lease(message.value(), expected_run_id=run_id, now_ms=raw["issued_at_ms"], rotation_plan=rotation)
                if lease is not None:
                    for owner in set(lease.shared_bucket_owners["Person"].values()):
                        owner_epochs[owner].add(lease.epoch)
        finally:
            lease_consumer.close()
        assert all(owner_epochs.values())
        assert owner_epochs["WORKS_AT"].isdisjoint(owner_epochs["BOUGHT"])

        for name in exact_container_names:
            with pytest.raises(NotFound):
                docker_client.containers.get(name)
    finally:
        for name in exact_container_names:
            try:
                docker_client.containers.get(name).stop(timeout=1)
            except NotFound:
                pass
        if created_topics:
            delete_futures = admin.delete_topics(created_topics)
            for future in delete_futures.values():
                try:
                    future.result(20)
                except Exception:
                    pass
        try:
            with driver.session() as session:
                session.run("MATCH (n {e2e_run: $run}) DETACH DELETE n", run=run_id).consume()
        finally:
            driver.close()
            try:
                config_path.unlink()
            except FileNotFoundError:
                pass
