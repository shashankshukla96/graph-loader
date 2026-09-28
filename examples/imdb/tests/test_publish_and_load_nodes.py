"""Unit coverage for schema-aware JSON payload conversion in the IMDb publisher."""
from __future__ import annotations

from pathlib import Path
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError, KafkaException

from examples.imdb.publish_and_load_nodes import (
    create_runtime_schema,
    edge_payload,
    publish_edge_tsv,
    publish_tsv,
    read_write_timings,
    require_empty_topics,
    run_bulk_loader_with_timings,
    typed_payload,
    write_run_report,
)
from src.utils.schema_loader import load_schema


def test_topic_preflight_rejects_retained_data_before_publication() -> None:
    consumer = MagicMock()
    consumer.list_topics.return_value = SimpleNamespace(topics={
        "imdb-title": SimpleNamespace(error=None, partitions={0: object(), 1: object()}),
    })
    consumer.get_watermark_offsets.side_effect = [(0, 0), (0, 2)]
    with patch("examples.imdb.publish_and_load_nodes.Consumer", return_value=consumer):
        with pytest.raises(ValueError, match="imdb-title \\(2 record\\(s\\)\\)"):
            require_empty_topics("kafka:9092", ["imdb-title"])
    consumer.close.assert_called_once()


def test_topic_preflight_retries_new_topic_leader_election() -> None:
    consumer = MagicMock()
    consumer.list_topics.return_value = SimpleNamespace(topics={
        "imdb-title": SimpleNamespace(error=None, partitions={0: object()}),
    })
    consumer.get_watermark_offsets.side_effect = [
        KafkaException(KafkaError(KafkaError.NOT_LEADER_FOR_PARTITION)),
        (0, 0),
    ]
    with patch("examples.imdb.publish_and_load_nodes.Consumer", return_value=consumer), patch(
        "examples.imdb.publish_and_load_nodes.time.sleep"
    ) as sleep:
        require_empty_topics("kafka:9092", ["imdb-title"])
    sleep.assert_called_once_with(0.25)
    consumer.close.assert_called_once()


def test_topic_preflight_waits_for_new_topic_metadata() -> None:
    consumer = MagicMock()
    consumer.list_topics.side_effect = [
        SimpleNamespace(topics={"imdb-title": SimpleNamespace(error=RuntimeError("leader pending"))}),
        SimpleNamespace(topics={"imdb-title": SimpleNamespace(error=None, partitions={0: object()})}),
    ]
    consumer.get_watermark_offsets.return_value = (0, 0)
    with patch("examples.imdb.publish_and_load_nodes.Consumer", return_value=consumer), patch(
        "examples.imdb.publish_and_load_nodes.time.sleep"
    ) as sleep:
        require_empty_topics("kafka:9092", ["imdb-title"])
    sleep.assert_called_once_with(0.25)
    consumer.close.assert_called_once()


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
    runtime = create_runtime_schema(source, consumer_workers=3, edge_writers=4)
    try:
        schema = load_schema(runtime)
        assert {node.replicas for node in schema.nodes} == {3}
        assert {edge.replicas for edge in schema.edges} == {1}
        assert {edge.execution.worker_count for edge in schema.edges} == {4}
        assert {edge.mix_and_batch.lane_count for edge in schema.edges} == {4}
        assert schema.loading.unwind_batch_size == 500
        assert schema.loading.edge_unwind_batch_size == 2000
        assert "replicas:" not in source.read_text(encoding="utf-8")
    finally:
        runtime.unlink(missing_ok=True)


def test_runtime_schema_overrides_only_edge_outer_batch_size() -> None:
    source = Path(__file__).resolve().parents[1] / "graph_schema.yaml"
    runtime = create_runtime_schema(source, consumer_workers=4, edge_batch_size=500)
    try:
        schema = load_schema(runtime)
        assert schema.loading.unwind_batch_size == 500
        assert schema.loading.edge_unwind_batch_size == 500
        assert {edge.mix_and_batch.batch_size for edge in schema.edges} == {1000}
    finally:
        runtime.unlink(missing_ok=True)


def test_runtime_schema_overrides_only_node_outer_batch_size() -> None:
    source = Path(__file__).resolve().parents[1] / "graph_schema.yaml"
    runtime = create_runtime_schema(source, consumer_workers=4, node_batch_size=750)
    try:
        schema = load_schema(runtime)
        assert schema.loading.unwind_batch_size == 750
        assert schema.loading.edge_unwind_batch_size == 2000
        assert {edge.mix_and_batch.batch_size for edge in schema.edges} == {1000}
    finally:
        runtime.unlink(missing_ok=True)


def test_node_override_preserves_edge_fallback_without_explicit_edge_size(tmp_path: Path) -> None:
    source = Path(__file__).resolve().parents[1] / "graph_schema.yaml"
    schema_without_edge_size = tmp_path / "graph_schema.yaml"
    schema_without_edge_size.write_text(
        source.read_text(encoding="utf-8").replace("  edge_unwind_batch_size: 2000\n", ""),
        encoding="utf-8",
    )
    runtime = create_runtime_schema(schema_without_edge_size, consumer_workers=4, node_batch_size=750)
    try:
        schema = load_schema(runtime)
        assert schema.loading.unwind_batch_size == 750
        assert schema.loading.edge_unwind_batch_size == 500
    finally:
        runtime.unlink(missing_ok=True)


def test_runtime_schema_accepts_both_batch_overrides() -> None:
    source = Path(__file__).resolve().parents[1] / "graph_schema.yaml"
    runtime = create_runtime_schema(
        source, consumer_workers=4, node_batch_size=750, edge_batch_size=3000,
    )
    try:
        schema = load_schema(runtime)
        assert schema.loading.unwind_batch_size == 750
        assert schema.loading.edge_unwind_batch_size == 3000
    finally:
        runtime.unlink(missing_ok=True)


def test_runtime_schema_supports_four_edge_consumers_with_one_writer_each() -> None:
    source = Path(__file__).resolve().parents[1] / "graph_schema.yaml"
    runtime = create_runtime_schema(source, consumer_workers=4, edge_writers=1, edge_consumers=4)
    try:
        schema = load_schema(runtime)
        assert {edge.replicas for edge in schema.edges} == {4}
        assert {edge.execution.worker_count for edge in schema.edges} == {1}
        assert {edge.mix_and_batch.lane_count for edge in schema.edges} == {1}
    finally:
        runtime.unlink(missing_ok=True)


def test_bulk_timing_logs_remain_after_loader_returns(tmp_path: Path) -> None:
    def complete_with_timing(_command, *, cwd, env, check):
        assert cwd.is_dir() and check is False
        timing_directory = Path(env["GRAPH_LOADER_TIMING_DIR"])
        (timing_directory / "edge-ACTED_IN-0.log").write_text(
            "stage=edge-write edge=ACTED_IN topic=imdb-acted-in replica=0 records=3 write_ms=9.000\n",
            encoding="utf-8",
        )
        return SimpleNamespace(returncode=0)

    with patch("examples.imdb.publish_and_load_nodes.subprocess.run", side_effect=complete_with_timing):
        exit_code, summaries, timing_directory = run_bulk_loader_with_timings(
            ["loader"], {}, tmp_path / "reports",
        )
    assert exit_code == 0
    assert summaries["imdb-acted-in"].records == 3
    assert (timing_directory / "edge-ACTED_IN-0.log").is_file()


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


def test_edge_payload_uses_declared_endpoint_keys_and_only_declared_properties() -> None:
    schema = load_schema(Path(__file__).resolve().parents[1] / "graph_schema.yaml")
    acted_in = next(edge for edge in schema.edges if edge.type == "ACTED_IN")

    assert edge_payload(
        {"person_id": "nm0000001", "title_id": "tt0000001", "category": "actor", "ordering": "1"},
        acted_in, source_column="person_id", target_column="title_id",
    ) == {
        "source": {"nconst": "nm0000001"},
        "target": {"tconst": "tt0000001"},
        "properties": {"category": "actor"},
    }


def test_publish_edge_tsv_uses_source_identifier_for_kafka_key(tmp_path: Path) -> None:
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
    acted_in = next(edge for edge in schema.edges if edge.type == "ACTED_IN")
    source = tmp_path / "acted_in.tsv"
    source.write_text("person_id\ttitle_id\tcategory\tordering\n" "nm1\ttt1\tactor\t1\n", encoding="utf-8")
    producer = FakeProducer()

    assert publish_edge_tsv(producer, "imdb-acted-in", source, acted_in, source_column="person_id", target_column="title_id") == 1
    assert producer.messages == [{
        "topic": "imdb-acted-in", "key": b"nm1",
        "value": b'{"source": {"nconst": "nm1"}, "target": {"tconst": "tt1"}, "properties": {"category": "actor"}}',
    }]


def test_run_report_includes_all_requested_timing_categories(tmp_path: Path) -> None:
    timing_directory = tmp_path / "imdb-write-timings-probe"
    timing_directory.mkdir()
    (timing_directory / "stage-ACTED_IN.json").write_text(json.dumps({
        "edge_type": "ACTED_IN",
        "started_utc": "2026-09-17T00:00:01+00:00",
        "completed_utc": "2026-09-17T00:00:04+00:00",
        "elapsed_seconds": 3.0,
        "succeeded": True,
    }), encoding="utf-8")
    (tmp_path / "node-Person-0.log").write_text(
        "stage=node-write label=Person topic=imdb-person replica=0 run_id=run-1 records=12 attempts=1 write_ms=40.000\n"
        "stage=node-write label=Person topic=imdb-person replica=0 run_id=run-1 records=8 attempts=1 write_ms=20.000\n",
        encoding="utf-8",
    )
    (tmp_path / "edge-ACTED_IN-0.log").write_text(
        "stage=edge-write edge=ACTED_IN topic=imdb-acted-in replica=0 run_id=run-1 records=3 write_ms=9.000\n",
        encoding="utf-8",
    )
    write_timings = read_write_timings(tmp_path)
    assert write_timings["imdb-person"].records == 20
    assert write_timings["imdb-person"].batches == 2
    assert write_timings["imdb-person"].total_ms == 60.0
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
        topic_results=[("person", "imdb-person", 20, 1.2), ("ACTED_IN", "imdb-acted-in", 3, 0.1)],
        consumer_lifecycle_seconds=8.0,
        consumer_exit_code=0,
        write_timings=write_timings,
        edge_writers=4,
        node_batch_size=750,
        edge_batch_size=2000,
        timing_directory=timing_directory,
    )

    content = report.read_text(encoding="utf-8")
    assert "Node-loader containers started: 40" in content
    assert "Node records sent to Kafka: 20" in content
    assert "Relationship records sent to Kafka: 3" in content
    assert "Graph records sent to Kafka: 23" in content
    assert "Edge writer threads per edge type: 4" in content
    assert "Node outer batch size: 750" in content
    assert "Edge outer batch size: 2000" in content
    assert "Raw Neo4j write timing logs: `imdb-write-timings-probe/`" in content
    assert "Kafka consumption + Neo4j graph loading + drain verification" in content
    assert "20" in content
    assert "## Neo4j writes by graph type" in content
    assert "| person | `imdb-person` | 20 | 2 | 0.060s | 30.0ms | 40.0ms |" in content
    assert "| ACTED_IN | `imdb-acted-in` | 3 | 1 | 0.009s | 9.0ms | 9.0ms |" in content
    assert "| ACTED_IN | 2026-09-17T00:00:01+00:00 | 2026-09-17T00:00:04+00:00 | 3.000s | succeeded |" in content
