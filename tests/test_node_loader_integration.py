"""Integration tests for generic schema-driven Neo4j node writes."""
from __future__ import annotations

from pathlib import Path
import uuid
import json

import pytest
import yaml
from confluent_kafka import Producer

from src.loader.node_loader import NodeWriter, main as node_loader_main, normalize_node_record
from src.orchestrator.schema_initializer import apply_schema
from src.utils.schema_loader import load_schema


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _node_config(schema, label: str):
    """Return exactly one configured node label for an integration test."""
    return next(node for node in schema.nodes if node.label == label)


@pytest.mark.integration
def test_person_upsert_is_idempotent_and_uses_patch_semantics(neo4j_driver, clean_neo4j) -> None:
    schema = load_schema(PROJECT_ROOT / "config" / "graph_schema.yaml")
    apply_schema(neo4j_driver, schema, timeout_seconds=30.0, poll_interval=0.1)
    person = _node_config(schema, "Person")
    writer = NodeWriter(neo4j_driver, person)

    writer.write(
        normalize_node_record(
            {"personId": "p-001", "name": "Ada", "age": 37}, person
        )
    )
    writer.write(normalize_node_record({"personId": "p-001", "name": "Ada Lovelace"}, person))

    with neo4j_driver.session() as session:
        record = session.run(
            "MATCH (n:Person {personId: $person_id}) "
            "RETURN count(n) AS count, n.name AS name, n.age AS age",
            person_id="p-001",
        ).single()

    assert record["count"] == 1
    assert record["name"] == "Ada Lovelace"
    assert record["age"] == 37


@pytest.mark.integration
def test_company_upsert_uses_its_configured_key(neo4j_driver, clean_neo4j) -> None:
    schema = load_schema(PROJECT_ROOT / "config" / "graph_schema.yaml")
    apply_schema(neo4j_driver, schema, timeout_seconds=30.0, poll_interval=0.1)
    company = _node_config(schema, "Company")
    writer = NodeWriter(neo4j_driver, company)

    writer.write(normalize_node_record({"companyId": "c-001", "name": "Acme"}, company))
    writer.write(normalize_node_record({"companyId": "c-001", "name": "Acme Updated"}, company))

    with neo4j_driver.session() as session:
        record = session.run(
            "MATCH (n:Company {companyId: $company_id}) "
            "RETURN count(n) AS count, n.name AS name",
            company_id="c-001",
        ).single()

    assert record["count"] == 1
    assert record["name"] == "Acme Updated"


@pytest.mark.integration
@pytest.mark.parametrize(
    ("label", "payload", "key_property"),
    [
        ("Person", {"personId": "p-kafka", "name": "Kafka Ada"}, "personId"),
        ("Company", {"companyId": "c-kafka", "name": "Kafka Co"}, "companyId"),
    ],
)
def test_direct_command_uses_temporary_topic_and_group(
    neo4j_driver, neo4j_connection, clean_neo4j, kafka_bootstrap, kafka_topic,
    tmp_path, monkeypatch, label, payload, key_property
) -> None:
    schema = load_schema(PROJECT_ROOT / "config" / "graph_schema.yaml")
    apply_schema(neo4j_driver, schema, timeout_seconds=30.0, poll_interval=0.1)
    raw = yaml.safe_load((PROJECT_ROOT / "config" / "graph_schema.yaml").read_text())
    unique_group = f"test-{uuid.uuid4().hex}"
    raw["loading"]["consumer_group_id"] = unique_group
    for node in raw["nodes"]:
        if node["label"] == label:
            node["topic"] = kafka_topic
    config_path = tmp_path / "schema.yaml"
    config_path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    producer = Producer({"bootstrap.servers": kafka_bootstrap})
    updated_payload = dict(payload)
    updated_payload["name"] = f"{payload['name']} Updated"
    producer.produce(kafka_topic, json.dumps(payload).encode("utf-8"))
    producer.produce(kafka_topic, json.dumps(updated_payload).encode("utf-8"))
    producer.flush(20)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", kafka_bootstrap)
    monkeypatch.setenv("NEO4J_URI", neo4j_connection[0])
    monkeypatch.setenv("NEO4J_USERNAME", neo4j_connection[1])
    monkeypatch.setenv("NEO4J_PASSWORD", neo4j_connection[2])
    assert node_loader_main(["--config", str(config_path), "--node-label", label, "--max-messages", "2"]) == 0
    with neo4j_driver.session() as session:
        record = session.run(
            f"MATCH (n:`{label}` {{`{key_property}`: $key}}) RETURN count(n) AS count, n.name AS name",
            key=payload[key_property],
        ).single()
    assert record["count"] == 1
    assert record["name"] == updated_payload["name"]
