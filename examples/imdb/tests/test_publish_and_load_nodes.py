"""Unit coverage for schema-aware JSON payload conversion in the IMDb publisher."""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from datetime import datetime, timezone

from examples.imdb.publish_and_load_nodes import (
    create_runtime_schema,
    publish_tsv,
    typed_payload,
    write_run_report,
)
from src.utils.schema_loader import load_schema


def test_typed_payload_converts_imdb_numeric_tsv_values() -> None:
    schema = load_schema(Path(__file__).resolve().parents[1] / "graph_schema.yaml")
    person = next(node for node in schema.nodes if node.label == "Person")

    payload = typed_payload(
        {"nconst": "nm0000001", "primary_name": "Ada", "birth_year": "1970", "death_year": ""},
        person.properties,
    )

    assert payload == {
        "nconst": "nm0000001",
        "name": "Ada",
        "primary_name": "Ada",
        "birth_year": 1970,
    }


def test_runtime_schema_sets_consumer_replicas_and_is_isolated() -> None:
    source = Path(__file__).resolve().parents[1] / "graph_schema.yaml"
    runtime = create_runtime_schema(source, consumer_workers=3)
    try:
        schema = load_schema(runtime)
        assert {node.replicas for node in schema.nodes} == {3}
        assert "replicas:" not in source.read_text(encoding="utf-8")
    finally:
        runtime.unlink(missing_ok=True)


def test_publish_tsv_uses_the_schema_key_as_the_kafka_message_key(tmp_path: Path) -> None:
    class FakeProducer:
        def __init__(self) -> None:
            self.messages: list[dict] = []

        def produce(self, topic: str, **kwargs) -> None:
            self.messages.append({"topic": topic, **kwargs})

        def poll(self, _timeout: float) -> None:
            return None

        def flush(self, _timeout: float) -> int:
            return 0

    schema = load_schema(Path(__file__).resolve().parents[1] / "graph_schema.yaml")
    person = next(node for node in schema.nodes if node.label == "Person")
    source = tmp_path / "person.tsv"
    source.write_text(
        "nconst\tprimary_name\tbirth_year\tdeath_year\n"
        "nm0000001\tAda\t1970\t\n",
        encoding="utf-8",
    )
    producer = FakeProducer()

    assert publish_tsv(producer, "imdb-person", source, person.properties, person.key_property) == 1
    assert producer.messages[0]["key"] == b"nm0000001"


def test_run_report_includes_all_requested_timing_categories(tmp_path: Path) -> None:
    report = write_run_report(
        tmp_path,
        started_at=datetime(2026, 9, 17, tzinfo=timezone.utc),
        total_seconds=12.5,
        partitions=4,
        publish_workers=4,
        consumer_workers=4,
        topic_setup_seconds=0.5,
        parallel_phase_seconds=3.0,
        image_build_seconds=2.0,
        topic_results=[("person", "imdb-person", 20, 1.2)],
        consumer_lifecycle_seconds=8.0,
        consumer_exit_code=0,
    )

    content = report.read_text(encoding="utf-8")
    assert "Node-loader containers started: 40" in content
    assert "Kafka consumption + Neo4j node loading + drain verification" in content
    assert "20" in content
