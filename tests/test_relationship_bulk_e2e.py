"""Opt-in Docker acceptance test for the Slice 4 relationship fleet."""
from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import uuid

import docker
import pytest
import yaml
from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from neo4j import GraphDatabase

from src.cli import main


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("RUN_RELATIONSHIP_BULK_E2E") != "1",
    reason="requires running Docker Kafka/Neo4j with finite relationship fixtures",
)
def test_relationship_bulk_fleet_acceptance_contract(tmp_path: Path):
    """Exercise disjoint and Person-conflicting types against the local stack."""
    suffix = uuid.uuid4().hex[:8]
    topic = lambda name: f"slice4-{name}-{suffix}"
    topics = {name: topic(name) for name in ("person", "company", "product", "visitor", "place", "works", "bought", "visited")}
    config = {
        "loading": {"consumer_group_id": f"slice4-{suffix}", "flush_interval_ms": 100},
        "nodes": [
            {"label": label, "topic": topics[key], "key_property": "id", "properties": {"id": {"type": "string", "required": True}}}
            for label, key in (("Person", "person"), ("Company", "company"), ("Product", "product"), ("Visitor", "visitor"), ("Place", "place"))
        ],
        "edges": [
            {"type": kind, "topic": topics[key], "nodes": {"source": source, "target": target}, "source_key_property": "id", "target_key_property": "id", "properties": {"kind": {"type": "string", "required": True}}}
            for kind, key, source, target in (("WORKS_AT", "works", "Person", "Company"), ("BOUGHT", "bought", "Person", "Product"), ("VISITED", "visited", "Visitor", "Place"))
        ],
    }
    config_path = tmp_path / "slice4.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics([NewTopic(name, 1, 1) for name in topics.values()])
    for future in futures.values():
        try:
            future.result(20)
        except Exception as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise
    driver = GraphDatabase.driver(
        os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
        auth=(os.environ.get("NEO4J_USERNAME", "neo4j"), os.environ.get("NEO4J_PASSWORD", "changeme")),
    )
    try:
        with driver.session() as session:
            for label, ident in (("Person", "p"), ("Company", "c"), ("Product", "x"), ("Visitor", "v"), ("Place", "l")):
                session.run(f"MERGE (n:`{label}` {{id: $id}}) SET n.name = $name", id=ident, name=ident).consume()
        producer = Producer({"bootstrap.servers": bootstrap})
        for name, ident in (("person", "p"), ("company", "c"), ("product", "x"), ("visitor", "v"), ("place", "l")):
            producer.produce(topics[name], json.dumps({"id": ident}).encode())
        for name, source, target in (("works", "p", "c"), ("bought", "p", "x"), ("visited", "v", "l")):
            producer.produce(topics[name], json.dumps({"source": {"id": source}, "target": {"id": target}, "properties": {"kind": name}}).encode())
        assert producer.flush(20) == 0
        node_run, edge_run = "node-e2e-run", "edge-e2e-run"
        with patch("src.cli.uuid.uuid4", side_effect=[SimpleNamespace(hex=node_run), SimpleNamespace(hex=edge_run)]):
            assert main(["start", "--mode", "bulk", "--config", str(config_path), "--network", "graph-loader-net", "--bulk-timeout-seconds", "90"]) == 0
        with driver.session() as session:
            for kind in ("WORKS_AT", "BOUGHT", "VISITED"):
                assert session.run(f"MATCH ()-[r:`{kind}`]->() RETURN count(r) AS total").single()["total"] >= 1
        for kind, key in (("WORKS_AT", "works"), ("BOUGHT", "bought"), ("VISITED", "visited")):
            group = f"slice4-{suffix}-{kind}-{edge_run}"
            request = ConsumerGroupTopicPartitions(group, [TopicPartition(topics[key], 0)])
            response = admin.list_consumer_group_offsets([request], require_stable=True)[group].result(20)
            assert response.topic_partitions[0].offset == 1
            watermark = Consumer({"bootstrap.servers": bootstrap, "group.id": f"watermark-{suffix}-{kind}"})
            try:
                assert watermark.get_watermark_offsets(TopicPartition(topics[key], 0), timeout=20)[1] == 1
            finally:
                watermark.close()
        assert not docker.from_env().containers.list(all=True, filters={"label": ["component=edge-loader", f"run_id={edge_run}"]})
    finally:
        driver.close()
