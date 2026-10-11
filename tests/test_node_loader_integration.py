"""Integration tests for generic schema-driven Neo4j node writes."""
from __future__ import annotations

from pathlib import Path
import uuid
import json

import pytest
import yaml
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from neo4j.exceptions import TransientError

from src.loader.node_loader import (
    NodeLoader,
    NodeWriter,
    RejectionSink,
    main as node_loader_main,
    normalize_node_record,
)
from src.orchestrator.schema_initializer import apply_schema
from src.utils.schema_loader import load_schema


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _node_config(schema, label: str):
    """Return exactly one configured node label for an integration test."""
    return next(node for node in schema.nodes if node.label == label)


def _create_topic(admin: AdminClient, topic: str, partitions: int) -> None:
    """Create one isolated topic and wait until Kafka acknowledges it."""
    admin.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=1)]
    )[topic].result(20)


def _delete_topics(admin: AdminClient, topics: list[str]) -> None:
    """Best-effort cleanup for integration-owned Kafka topics."""
    for topic, future in admin.delete_topics(topics).items():
        try:
            future.result(20)
        except Exception:
            # The test failure is more useful than a cleanup race with Kafka.
            pass


def _write_schema(data: dict, path: Path):
    """Write an isolated YAML schema and load it through the public API."""
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return load_schema(path)


def _committed_next_offset(
    kafka_bootstrap: str, group_id: str, topic: str, partition: int
) -> int:
    """Read the actual next offset committed for one consumer-group partition."""
    consumer = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap,
            "group.id": group_id,
            "enable.auto.commit": False,
        }
    )
    try:
        committed = consumer.committed([TopicPartition(topic, partition)], timeout=20)
        return committed[0].offset
    finally:
        consumer.close()


class _FailOnceWriter:
    """Inject one retryable failure before delegating to the real Neo4j writer."""

    def __init__(self, delegate: NodeWriter) -> None:
        self._delegate = delegate
        self.calls = 0

    def write_batch(self, records) -> None:
        self.calls += 1
        if self.calls == 1:
            raise TransientError("injected retryable integration failure")
        self._delegate.write_batch(records)


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


@pytest.mark.integration
def test_batched_multi_partition_rejections_offsets_and_replay(
    neo4j_driver,
    neo4j_connection,
    clean_neo4j,
    kafka_bootstrap,
    tmp_path,
    monkeypatch,
) -> None:
    """Exercise real Kafka offsets for batching, rejection, replay, and final flush."""
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    suffix = uuid.uuid4().hex
    person_topic = f"person-batch-{suffix}"
    company_topic = f"company-batch-{suffix}"
    created_topics: list[str] = []
    try:
        _create_topic(admin, person_topic, partitions=2)
        created_topics.append(person_topic)
        _create_topic(admin, company_topic, partitions=1)
        created_topics.append(company_topic)
        schema = load_schema(PROJECT_ROOT / "config" / "graph_schema.yaml")
        apply_schema(neo4j_driver, schema, timeout_seconds=30.0, poll_interval=0.1)
        group_prefix = f"batch-{suffix}"
        rejection_path = tmp_path / "rejections.jsonl"
        raw = yaml.safe_load((PROJECT_ROOT / "config" / "graph_schema.yaml").read_text())
        raw["loading"].update(
            {
                "consumer_group_id": group_prefix,
                "unwind_batch_size": 2,
                "rejection_log_path": str(rejection_path),
            }
        )
        for node in raw["nodes"]:
            if node["label"] == "Person":
                node["topic"] = person_topic
            if node["label"] == "Company":
                node["topic"] = company_topic
        config_path = tmp_path / "schema.yaml"
        config_path.write_text(yaml.safe_dump(raw), encoding="utf-8")

        producer = Producer({"bootstrap.servers": kafka_bootstrap})
        producer.produce(
            person_topic,
            json.dumps({"personId": "p-1", "name": "Ada"}).encode(),
            partition=0,
        )
        producer.produce(person_topic, b"{malformed", partition=1)
        producer.produce(
            person_topic,
            json.dumps({"personId": "p-2", "name": "Grace"}).encode(),
            partition=0,
        )
        producer.produce(
            company_topic,
            json.dumps({"companyId": "c-1", "name": "Acme"}).encode(),
            partition=0,
        )
        producer.produce(
            company_topic,
            json.dumps({"companyId": "c-1", "name": "Acme Updated"}).encode(),
            partition=0,
        )
        assert producer.flush(20) == 0

        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", kafka_bootstrap)
        monkeypatch.setenv("NEO4J_URI", neo4j_connection[0])
        monkeypatch.setenv("NEO4J_USERNAME", neo4j_connection[1])
        monkeypatch.setenv("NEO4J_PASSWORD", neo4j_connection[2])

        assert node_loader_main(
            ["--config", str(config_path), "--node-label", "Person", "--max-messages", "3"]
        ) == 0
        assert node_loader_main(
            ["--config", str(config_path), "--node-label", "Company", "--max-messages", "2"]
        ) == 0

        assert _committed_next_offset(
            kafka_bootstrap, f"{group_prefix}-Person", person_topic, 0
        ) == 2
        assert _committed_next_offset(
            kafka_bootstrap, f"{group_prefix}-Person", person_topic, 1
        ) == 1
        assert _committed_next_offset(
            kafka_bootstrap, f"{group_prefix}-Company", company_topic, 0
        ) == 2

        rejection = json.loads(rejection_path.read_text(encoding="utf-8").splitlines()[0])
        assert rejection["topic"] == person_topic
        assert rejection["partition"] == 1
        assert rejection["offset"] == 0
        assert rejection["node_label"] == "Person"
        assert rejection["reason"]
        assert "payload" not in rejection
        assert "malformed" not in rejection_path.read_text(encoding="utf-8")

        with neo4j_driver.session() as session:
            person_count = session.run("MATCH (n:Person) RETURN count(n) AS count").single()["count"]
            company = session.run(
                "MATCH (n:Company {companyId: 'c-1'}) RETURN count(n) AS count, n.name AS name"
            ).single()
        assert person_count == 2
        assert company["count"] == 1
        assert company["name"] == "Acme Updated"

        replay_prefix = f"replay-{suffix}"
        raw["loading"].update(
            {
                "consumer_group_id": replay_prefix,
                "unwind_batch_size": 3,
                "rejection_log_path": str(tmp_path / "replay-rejections.jsonl"),
            }
        )
        replay_config_path = tmp_path / "replay-schema.yaml"
        replay_config_path.write_text(yaml.safe_dump(raw), encoding="utf-8")
        assert node_loader_main(
            [
                "--config",
                str(replay_config_path),
                "--node-label",
                "Person",
                "--max-messages",
                "3",
            ]
        ) == 0
        assert _committed_next_offset(
            kafka_bootstrap, f"{replay_prefix}-Person", person_topic, 0
        ) == 2
        assert _committed_next_offset(
            kafka_bootstrap, f"{replay_prefix}-Person", person_topic, 1
        ) == 1
        with neo4j_driver.session() as session:
            replay_person_count = session.run(
                "MATCH (n:Person) RETURN count(n) AS count"
            ).single()["count"]
        assert replay_person_count == 2
    finally:
        _delete_topics(admin, created_topics)


@pytest.mark.integration
def test_real_kafka_and_neo4j_retryable_batch_write_commits_after_success(
    neo4j_driver,
    neo4j_connection,
    clean_neo4j,
    kafka_bootstrap,
    tmp_path,
) -> None:
    """Verify bounded retry writes once to Neo4j and then advances Kafka's offset."""
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    suffix = uuid.uuid4().hex
    retry_topic = f"person-retry-{suffix}"
    created_topics: list[str] = []
    consumer = None
    control_producer = None
    try:
        _create_topic(admin, retry_topic, partitions=1)
        created_topics.append(retry_topic)
        schema = load_schema(PROJECT_ROOT / "config" / "graph_schema.yaml")
        apply_schema(neo4j_driver, schema, timeout_seconds=30.0, poll_interval=0.1)
        raw = yaml.safe_load((PROJECT_ROOT / "config" / "graph_schema.yaml").read_text())
        group_prefix = f"retry-{suffix}"
        raw["loading"].update(
            {
                "consumer_group_id": group_prefix,
                "unwind_batch_size": 1,
                "retry_max_attempts": 2,
                "retry_base_delay_ms": 1,
                "retry_max_delay_ms": 1,
            }
        )
        for node in raw["nodes"]:
            if node["label"] == "Person":
                node["topic"] = retry_topic
        retry_schema = _write_schema(raw, tmp_path / "retry-schema.yaml")
        person = _node_config(retry_schema, "Person")

        producer = Producer({"bootstrap.servers": kafka_bootstrap})
        producer.produce(
            retry_topic,
            json.dumps({"personId": "p-retry", "name": "Retry Ada"}).encode(),
        )
        assert producer.flush(20) == 0
        consumer = Consumer(
            {
                "bootstrap.servers": kafka_bootstrap,
                "group.id": f"{group_prefix}-Person",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "max.poll.interval.ms": retry_schema.loading.max_poll_interval_ms,
                "session.timeout.ms": retry_schema.loading.session_timeout_ms,
            }
        )
        control_producer = Producer({"bootstrap.servers": kafka_bootstrap})
        writer = _FailOnceWriter(NodeWriter(neo4j_driver, person))
        loader = NodeLoader(
            consumer,
            writer,
            person,
            producer=control_producer,
            loading_config=retry_schema.loading,
            rejection_sink=RejectionSink(tmp_path / "retry-rejections.jsonl"),
        )

        assert loader.run(max_messages=1) == 0
        assert writer.calls == 2
        assert _committed_next_offset(
            kafka_bootstrap, f"{group_prefix}-Person", retry_topic, 0
        ) == 1
        with neo4j_driver.session() as session:
            record = session.run(
                "MATCH (n:Person {personId: 'p-retry'}) RETURN count(n) AS count, n.name AS name"
            ).single()
        assert record["count"] == 1
        assert record["name"] == "Retry Ada"
    finally:
        if consumer is not None:
            consumer.close()
        if control_producer is not None:
            control_producer.flush(20)
        _delete_topics(admin, created_topics)
