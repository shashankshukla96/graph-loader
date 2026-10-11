"""Unit coverage for schema-aware JSON payload conversion in the IMDb publisher."""
from __future__ import annotations

from pathlib import Path
import json
import os
import signal
import subprocess
import threading
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
    read_metrics_snapshots,
    read_write_timings,
    require_empty_topics,
    ChildSignalForwarder,
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


def test_small_node_types_get_one_replica_and_runtime_schema_applies_it() -> None:
    from examples.imdb.publish_and_load_nodes import NODE_SOURCES, plan_node_replicas

    counts = {source: 50_000 for source in NODE_SOURCES}
    counts["genre"], counts["title_type"] = 28, 10
    topic_results = [(source, topic, counts[source], 1.0) for source, (topic, _) in NODE_SOURCES.items()]
    topic_results.append(("HAS_GENRE", "imdb-has-genre", 5, 1.0))  # edges are ignored

    replicas = plan_node_replicas(topic_results, consumer_workers=4, min_records=10_000)
    assert set(replicas) == {topic for topic, _ in NODE_SOURCES.values()}
    assert replicas["imdb-genre"] == replicas["imdb-title-type"] == 1
    small = {"imdb-genre", "imdb-title-type"}
    assert {count for topic, count in replicas.items() if topic not in small} == {4}
    assert set(plan_node_replicas(topic_results, 4, min_records=0).values()) == {4}

    source = Path(__file__).resolve().parents[1] / "graph_schema.yaml"
    runtime = create_runtime_schema(source, consumer_workers=4, node_replicas=replicas)
    try:
        schema = load_schema(runtime)
        assert {node.topic: node.replicas for node in schema.nodes} == replicas
        assert {node.label: node.replicas for node in schema.nodes}["Genre"] == 1
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
    def complete_with_timing(_command, *, cwd, env):
        assert cwd.is_dir()
        timing_directory = Path(env["GRAPH_LOADER_TIMING_DIR"])
        (timing_directory / "edge-ACTED_IN-0.log").write_text(
            "stage=edge-write edge=ACTED_IN topic=imdb-acted-in replica=0 records=3 write_ms=9.000\n",
            encoding="utf-8",
        )
        return SimpleNamespace(wait=lambda timeout=None: 0)

    with patch("examples.imdb.publish_and_load_nodes.subprocess.Popen", side_effect=complete_with_timing):
        exit_code, summaries, timing_directory = run_bulk_loader_with_timings(
            ["loader"], {}, tmp_path / "reports",
        )
    assert exit_code == 0
    assert summaries["imdb-acted-in"].records == 3
    assert (timing_directory / "edge-ACTED_IN-0.log").is_file()


class FakeChild:
    """A Popen stand-in that runs until stopped by a signal or kill."""

    def __init__(self, exit_on_signal: int | None = 0) -> None:
        self.exit_on_signal = exit_on_signal
        self.returncode: int | None = None
        self.signals: list[int] = []
        self.killed = False

    def poll(self):
        return self.returncode

    def send_signal(self, signum: int) -> None:
        self.signals.append(signum)
        if self.exit_on_signal is not None and len(self.signals) >= 1:
            self.returncode = self.exit_on_signal

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9

    def wait(self, timeout=None):
        if self.returncode is None:
            if timeout is None:
                raise AssertionError("unbounded wait on a live child")
            raise subprocess.TimeoutExpired("loader", timeout)
        return self.returncode


def test_child_forwarder_forwards_sigterm_and_returns_child_exit_code() -> None:
    child = FakeChild(exit_on_signal=143)
    previous = signal.getsignal(signal.SIGTERM)
    with ChildSignalForwarder(child, timeout=5) as forwarder:
        threading.Timer(0.1, lambda: os.kill(os.getpid(), signal.SIGTERM)).start()
        assert forwarder.wait() == 143
    assert child.signals == [signal.SIGTERM] and not child.killed
    assert signal.getsignal(signal.SIGTERM) is previous


def test_child_forwarder_forwards_second_signal_without_escalating() -> None:
    child = FakeChild(exit_on_signal=None)
    with ChildSignalForwarder(child, timeout=5) as forwarder:
        forwarder._forward(signal.SIGINT)
        forwarder._forward(signal.SIGINT)
        child.returncode = 130
        assert forwarder.wait() == 130
    assert child.signals == [signal.SIGINT, signal.SIGINT] and not child.killed


def test_child_forwarder_kills_a_child_that_overruns_the_stop_timeout() -> None:
    child = FakeChild(exit_on_signal=None)
    with ChildSignalForwarder(child, timeout=0.3) as forwarder:
        forwarder._forward(signal.SIGTERM)
        assert forwarder.wait() == -9
    assert child.killed


def test_child_forwarder_does_not_kill_without_a_signal() -> None:
    child = FakeChild(exit_on_signal=None)
    with ChildSignalForwarder(child, timeout=0.0) as forwarder:
        threading.Timer(0.5, lambda: setattr(child, "returncode", 0)).start()
        assert forwarder.wait() == 0
    assert not child.killed and child.signals == []


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


def test_read_metrics_snapshots_sums_samples_across_replica_files(tmp_path: Path) -> None:
    (tmp_path / "metrics-node-Person-0.prom").write_text(
        "# HELP graph_loader_records_consumed_total x\n"
        "# TYPE graph_loader_records_consumed_total counter\n"
        'graph_loader_records_consumed_total{kind="node",name="Person",label_or_type="Person"} 12.0\n',
        encoding="utf-8",
    )
    (tmp_path / "metrics-node-Person-1.prom").write_text(
        "# HELP graph_loader_records_consumed_total x\n"
        "# TYPE graph_loader_records_consumed_total counter\n"
        'graph_loader_records_consumed_total{kind="node",name="Person",label_or_type="Person"} 8.0\n'
        "# HELP graph_loader_dlq_routed_total x\n"
        "# TYPE graph_loader_dlq_routed_total counter\n"
        'graph_loader_dlq_routed_total{kind="node",name="Person",failure_class="malformed"} 3.0\n',
        encoding="utf-8",
    )
    totals = read_metrics_snapshots(tmp_path)
    assert totals["graph_loader_records_consumed_total"] == 20.0
    assert totals["graph_loader_dlq_routed_total"] == 3.0


def test_read_metrics_snapshots_empty_when_no_files(tmp_path: Path) -> None:
    assert read_metrics_snapshots(tmp_path) == {}


def test_read_metrics_snapshots_skips_unparseable_file_and_keeps_good_totals(tmp_path: Path) -> None:
    """P6-S2 review finding S9-2: a hand-edited, foreign, or partially
    copied metrics-*.prom file must not crash the whole report over this
    optional section."""
    (tmp_path / "metrics-node-Person-0.prom").write_text(
        "# HELP graph_loader_records_consumed_total x\n"
        "# TYPE graph_loader_records_consumed_total counter\n"
        'graph_loader_records_consumed_total{kind="node",name="Person",label_or_type="Person"} 20.0\n',
        encoding="utf-8",
    )
    (tmp_path / "metrics-node-Company-0.prom").write_text(
        "this is not valid Prometheus exposition text {{{\n", encoding="utf-8",
    )
    totals = read_metrics_snapshots(tmp_path)
    assert totals["graph_loader_records_consumed_total"] == 20.0


def test_run_report_includes_metrics_totals_section_when_snapshots_exist(tmp_path: Path) -> None:
    timings = tmp_path / "timings"
    timings.mkdir()
    (timings / "metrics-node-Person-0.prom").write_text(
        "# HELP graph_loader_records_consumed_total x\n"
        "# TYPE graph_loader_records_consumed_total counter\n"
        'graph_loader_records_consumed_total{kind="node",name="Person",label_or_type="Person"} 20.0\n'
        "# HELP graph_loader_deadlocks_recovered_total x\n"
        "# TYPE graph_loader_deadlocks_recovered_total counter\n"
        'graph_loader_deadlocks_recovered_total{kind="edge",name="ACTED_IN"} 2.0\n',
        encoding="utf-8",
    )
    common = dict(
        started_at=datetime(2026, 9, 17, tzinfo=timezone.utc), total_seconds=1.0, partitions=4,
        publish_workers=4, consumer_workers=4, topic_setup_seconds=0.1, parallel_phase_seconds=0.5,
        image_build_seconds=None, topic_results=[("person", "imdb-person", 2, 0.2)],
        consumer_lifecycle_seconds=1.0, consumer_exit_code=0, edge_batch_size=32000,
    )
    content = write_run_report(tmp_path / "report", timing_directory=timings, **common).read_text(encoding="utf-8")
    assert "## Metrics totals" in content
    assert "| Records consumed | 20 |" in content
    assert "| Deadlocks recovered | 2 |" in content
    assert "| DLQ routed |" not in content  # not present in the snapshots -> omitted, not a fake 0 row.


def test_run_report_omits_metrics_totals_section_cleanly_when_no_snapshots(tmp_path: Path) -> None:
    timings = tmp_path / "timings"
    timings.mkdir()
    common = dict(
        started_at=datetime(2026, 9, 17, tzinfo=timezone.utc), total_seconds=1.0, partitions=4,
        publish_workers=4, consumer_workers=4, topic_setup_seconds=0.1, parallel_phase_seconds=0.5,
        image_build_seconds=None, topic_results=[("person", "imdb-person", 2, 0.2)],
        consumer_lifecycle_seconds=1.0, consumer_exit_code=0, edge_batch_size=32000,
    )
    content = write_run_report(tmp_path / "report", timing_directory=timings, **common).read_text(encoding="utf-8")
    assert "## Metrics totals" not in content
    content_no_dir = write_run_report(tmp_path / "report2", **common).read_text(encoding="utf-8")
    assert "## Metrics totals" not in content_no_dir


# ---------------------------------------------------------------------------
# Fast publisher path: byte-for-byte parity with the reference encoding.
# ---------------------------------------------------------------------------

import csv
import pickle
from collections import defaultdict

from examples.imdb.publish_and_load_nodes import (
    PublishTask,
    PublishTaskResult,
    merge_publish_results,
    plan_publish_tasks,
    run_publish_task,
)

SCHEMA_FILE = Path(__file__).resolve().parents[1] / "graph_schema.yaml"


class RecordingProducer:
    def __init__(self, full_after: int | None = None) -> None:
        self.messages: list[tuple[bytes, bytes]] = []
        self.polls: list[float] = []
        self.full_after = full_after

    def produce(self, topic: str, *, key: bytes, value: bytes) -> None:
        if self.full_after is not None and len(self.messages) == self.full_after:
            self.full_after = None
            raise BufferError("queue full")
        self.messages.append((key, value))

    def poll(self, timeout: float) -> int:
        self.polls.append(timeout)
        return 0

    def flush(self, _timeout: float) -> int:
        return 0


def reference_node_messages(source: Path, properties, key_property: str) -> list[tuple[bytes, bytes]]:
    """The pre-optimisation encoding: DictReader + typed_payload + json.dumps."""
    with source.open("r", encoding="utf-8", newline="") as input_file:
        return [
            (str(payload[key_property]).encode("utf-8"), json.dumps(payload).encode("utf-8"))
            for payload in (typed_payload(row, properties) for row in csv.DictReader(input_file, delimiter="\t"))
        ]


def reference_edge_messages(source: Path, edge, source_column: str, target_column: str) -> list[tuple[bytes, bytes]]:
    with source.open("r", encoding="utf-8", newline="") as input_file:
        return [
            (
                str(row[source_column]).encode("utf-8"),
                json.dumps(edge_payload(row, edge, source_column=source_column, target_column=target_column)).encode("utf-8"),
            )
            for row in csv.DictReader(input_file, delimiter="\t")
        ]


def tricky_title_tsv(path: Path) -> Path:
    rows = [
        ["tconst", "title_type", "primary_title", "original_title", "is_adult", "start_year", "end_year", "runtime_minutes"],
        ["tt1", "movie", 'He said "hi"\tthen\nleft', "Ünïcødé ☃ \\ / \x01", "0", "1999", "", "90"],
        ["tt2", "short", "", "", "1", "007", "", ""],
        ["tt3", "movie", "Short row"],
        ["tt4", "movie", "Tab\tand \"quote\"", "x", "0", "2000", "", "5"],
        ["tt5", "movie", "Plain", "Plain", "0", "", "", "-3"],
    ]
    with path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.writer(output_file, delimiter="\t", lineterminator="\n")
        writer.writerows(rows[:3])
        output_file.write("\n")  # DictReader skips blank rows; so must the fast path.
        writer.writerows(rows[3:])
    return path


def test_fast_node_encoding_is_byte_identical_to_reference(tmp_path: Path) -> None:
    schema = load_schema(SCHEMA_FILE)
    title = next(node for node in schema.nodes if node.label == "Title")
    source = tricky_title_tsv(tmp_path / "title.tsv")
    producer = RecordingProducer()

    assert publish_tsv(producer, "imdb-title", source, title.properties, title.key_property) == 5
    assert producer.messages == reference_node_messages(source, title.properties, title.key_property)

    # Over-wide rows are rejected exactly as csv.DictReader + typed_payload reject them.
    with source.open("a", encoding="utf-8") as output_file:
        output_file.write("tt6\tmovie\tLong\tx\t0\t2000\t\t5\textra\n")
    with pytest.raises(KeyError):
        reference_node_messages(source, title.properties, title.key_property)
    with pytest.raises(KeyError):
        publish_tsv(RecordingProducer(), "imdb-title", source, title.properties, title.key_property)


def test_fast_node_encoding_matches_reference_floats_and_name_alias(tmp_path: Path) -> None:
    schema = load_schema(SCHEMA_FILE)
    rating = next(node for node in schema.nodes if node.label == "Rating")
    person = next(node for node in schema.nodes if node.label == "Person")
    ratings = tmp_path / "rating.tsv"
    ratings.write_text(
        "rating_id\ttitle_id\taverage_rating\tnum_votes\n"
        "tt1\ttt1\t5.7\t10\ntt2\ttt2\t1e3\t1\ntt3\ttt3\tnan\t2\ntt4\ttt4\t-inf\t3\ntt5\ttt5\t7\t4\n",
        encoding="utf-8",
    )
    people = tmp_path / "person.tsv"
    people.write_text(
        "nconst\tprimary_name\tbirth_year\tdeath_year\n" "nm1\tAda\t1815\t1852\n" "nm2\t\t\t\n" "nm3\tZoë\t\t\n",
        encoding="utf-8",
    )
    for source, node in ((ratings, rating), (people, person)):
        producer = RecordingProducer()
        publish_tsv(producer, node.topic, source, node.properties, node.key_property)
        assert producer.messages == reference_node_messages(source, node.properties, node.key_property)
    named = tmp_path / "named.tsv"
    named.write_text("nconst\tname\tprimary_name\n" "nm1\t\tAda\n" "nm2\tExplicit\tAda\n", encoding="utf-8")
    producer = RecordingProducer()
    publish_tsv(producer, "imdb-person", named, person.properties, person.key_property)
    assert producer.messages == reference_node_messages(named, person.properties, person.key_property)
    assert json.loads(producer.messages[0][1])["name"] == "Ada"
    assert json.loads(producer.messages[1][1])["name"] == "Explicit"


def test_fast_node_encoding_reports_the_same_invalid_value_error(tmp_path: Path) -> None:
    schema = load_schema(SCHEMA_FILE)
    person = next(node for node in schema.nodes if node.label == "Person")
    source = tmp_path / "person.tsv"
    source.write_text("nconst\tprimary_name\tbirth_year\n" "nm1\tAda\t1815\n\n" "nm2\tBob\tabc\n", encoding="utf-8")

    with pytest.raises(ValueError, match=r"person\.tsv:3: invalid integer value for field 'birth_year': 'abc'"):
        publish_tsv(RecordingProducer(), "imdb-person", source, person.properties, person.key_property)


def test_fast_edge_encoding_is_byte_identical_to_reference(tmp_path: Path) -> None:
    schema = load_schema(SCHEMA_FILE)
    edges = {edge.type: edge for edge in schema.edges}
    credited = tmp_path / "credited_on.tsv"
    with credited.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.writer(output_file, delimiter="\t", lineterminator="\n")
        writer.writerows([
            ["person_id", "title_id", "category", "ordering", "job"],
            ["nm1", "tt1", "self", "1", ""],
            ["nm2", "tt1", 'dir"ector\t', "2", "Ünïcødé job"],
            ["nm3", "tt2", "actor", "3"],
            ["nm4", "tt3", "actor", "4", "", "extra"],
        ])
    played = tmp_path / "played.tsv"
    played.write_text("person_id\tcharacter\ttitle_id\n" "nm1\tSelf\ttt1\n" "nm2\tZoë \"Z\"\ttt2\n", encoding="utf-8")
    cases = [(credited, edges["CREDITED_ON"], "person_id", "title_id"), (played, edges["PLAYED"], "person_id", "character")]
    for source, edge, source_column, target_column in cases:
        producer = RecordingProducer()
        publish_edge_tsv(producer, edge.topic, source, edge, source_column=source_column, target_column=target_column)
        assert producer.messages == reference_edge_messages(source, edge, source_column, target_column)


def test_fast_edge_encoding_reports_missing_endpoints_and_properties(tmp_path: Path) -> None:
    schema = load_schema(SCHEMA_FILE)
    acted_in = next(edge for edge in schema.edges if edge.type == "ACTED_IN")
    source = tmp_path / "acted_in.tsv"
    source.write_text("person_id\ttitle_id\tcategory\n" "nm1\ttt1\tactor\n" "nm2\t\tactor\n", encoding="utf-8")
    with pytest.raises(ValueError, match=r"acted_in\.tsv:3: missing endpoint columns"):
        publish_edge_tsv(RecordingProducer(), "t", source, acted_in, source_column="person_id", target_column="title_id")
    source.write_text("person_id\ttitle_id\tcategory\n" "nm1\ttt1\t\n", encoding="utf-8")
    with pytest.raises(ValueError, match=r"acted_in\.tsv:2: missing required relationship properties: category"):
        publish_edge_tsv(RecordingProducer(), "t", source, acted_in, source_column="person_id", target_column="title_id")


def test_producer_queue_full_is_retried_and_polling_is_batched(tmp_path: Path) -> None:
    schema = load_schema(SCHEMA_FILE)
    acted_in = next(edge for edge in schema.edges if edge.type == "ACTED_IN")
    source = tmp_path / "acted_in.tsv"
    source.write_text(
        "person_id\ttitle_id\tcategory\n" + "".join(f"nm{i}\ttt{i}\tactor\n" for i in range(25)), encoding="utf-8",
    )
    producer = RecordingProducer(full_after=10)

    assert publish_edge_tsv(producer, "t", source, acted_in, source_column="person_id", target_column="title_id") == 25
    assert [key for key, _ in producer.messages] == [f"nm{i}".encode() for i in range(25)]
    assert producer.polls == [0.05]  # one back-off poll; no per-record poll(0)


def per_key(messages: list[tuple[bytes, bytes]]) -> dict[bytes, list[bytes]]:
    grouped: dict[bytes, list[bytes]] = defaultdict(list)
    for key, value in messages:
        grouped[key].append(value)
    return grouped


def test_key_hash_shards_partition_records_and_keep_per_key_order(tmp_path: Path) -> None:
    schema = load_schema(SCHEMA_FILE)
    credited = next(edge for edge in schema.edges if edge.type == "CREDITED_ON")
    title = next(node for node in schema.nodes if node.label == "Title")
    edges = tmp_path / "credited_on.tsv"
    edges.write_text(
        "person_id\ttitle_id\tcategory\n" + "".join(f"nm{i % 37}\ttt{i}\tcat{i}\n" for i in range(500)),
        encoding="utf-8",
    )
    nodes = tricky_title_tsv(tmp_path / "title.tsv")

    def publish_edges(index: int, count: int) -> list[tuple[bytes, bytes]]:
        producer = RecordingProducer()
        publish_edge_tsv(
            producer, "t", edges, credited, source_column="person_id", target_column="title_id",
            shard_index=index, shard_count=count,
        )
        return producer.messages

    whole = publish_edges(0, 1)
    shards = [publish_edges(index, 3) for index in range(3)]
    assert all(shards)
    assert sum(map(len, shards)) == len(whole) == 500
    assert per_key([message for shard in shards for message in shard]) == per_key(whole)
    key_sets = [set(per_key(shard)) for shard in shards]
    assert sum(map(len, key_sets)) == len(set().union(*key_sets))  # every key lives in exactly one shard

    node_shards = []
    for index in range(2):
        producer = RecordingProducer()
        publish_tsv(producer, "t", nodes, title.properties, title.key_property, shard_index=index, shard_count=2)
        node_shards.append(producer.messages)
    assert sorted(message for shard in node_shards for message in shard) == sorted(
        reference_node_messages(nodes, title.properties, title.key_property)
    )


def test_rows_without_a_key_are_rejected_once_by_shard_zero(tmp_path: Path) -> None:
    schema = load_schema(SCHEMA_FILE)
    acted_in = next(edge for edge in schema.edges if edge.type == "ACTED_IN")
    source = tmp_path / "acted_in.tsv"
    source.write_text("person_id\ttitle_id\tcategory\n" "nm1\ttt1\tactor\n" "\ttt2\tactor\n", encoding="utf-8")
    with pytest.raises(ValueError, match=r"acted_in\.tsv:3: missing endpoint columns"):
        publish_edge_tsv(
            RecordingProducer(), "t", source, acted_in, source_column="person_id", target_column="title_id",
            shard_index=0, shard_count=4,
        )
    for index in range(1, 4):
        publish_edge_tsv(
            RecordingProducer(), "t", source, acted_in, source_column="person_id", target_column="title_id",
            shard_index=index, shard_count=4,
        )


def make_task(label: str, size: int, **fields) -> PublishTask:
    return PublishTask(label, f"imdb-{label}", Path(f"/data/{label}.tsv"), size, **fields)


def test_plan_splits_large_topics_and_schedules_largest_first() -> None:
    mib = 1024 * 1024
    tasks = [make_task("small", 1 * mib), make_task("huge", 80 * mib), make_task("mid", 30 * mib)]

    planned = plan_publish_tasks(tasks, workers=4)
    shards = defaultdict(list)
    for task in planned:
        shards[task.topic].append((task.shard_index, task.shard_count))
    assert sorted(shards["imdb-huge"]) == [(0, 4), (1, 4), (2, 4), (3, 4)]
    assert sorted(shards["imdb-mid"]) == [(0, 3), (1, 3), (2, 3)]
    assert shards["imdb-small"] == [(0, 1)]
    per_task = [task.size_bytes / task.shard_count for task in planned]
    assert per_task == sorted(per_task, reverse=True)
    assert planned[-1].topic == "imdb-small"

    assert {task.shard_count for task in plan_publish_tasks(tasks, workers=4, max_shards=1)} == {1}
    assert max(task.shard_count for task in plan_publish_tasks(tasks, workers=8, max_shards=2)) == 2
    assert max(task.shard_count for task in plan_publish_tasks(tasks, workers=2)) <= 2


def test_publish_tasks_survive_pickling_for_worker_processes() -> None:
    schema = load_schema(SCHEMA_FILE)
    edge = next(edge for edge in schema.edges if edge.type == "ACTED_IN")
    task = make_task("ACTED_IN", 10, edge=edge, source_column="person_id", target_column="title_id")
    restored = pickle.loads(pickle.dumps(task))
    assert restored.edge.source_key_property == "nconst"
    assert restored.edge.properties["category"].required


def test_merge_publish_results_sums_shards_and_spans_their_wall_time() -> None:
    merged = merge_publish_results([
        PublishTaskResult("CREDITED_ON", "imdb-credited-on", 0, 10, 1.0, 4.0, 2),
        PublishTaskResult("CREDITED_ON", "imdb-credited-on", 1, 5, 2.0, 6.5, 2),
        PublishTaskResult("person", "imdb-person", 0, 3, 1.5, 2.0),
    ])
    assert sorted(merged) == [("CREDITED_ON", "imdb-credited-on", 15, 5.5), ("person", "imdb-person", 3, 0.5)]


def test_run_publish_task_configures_producer_and_rejects_delivery_failures(tmp_path: Path) -> None:
    schema = load_schema(SCHEMA_FILE)
    person = next(node for node in schema.nodes if node.label == "Person")
    source = tmp_path / "person.tsv"
    source.write_text("nconst\tprimary_name\n" "nm1\tAda\n" "nm2\tBob\n", encoding="utf-8")
    task = PublishTask("person", "imdb-person", source, 1, properties=person.properties, key_property="nconst")
    configs: list[dict] = []

    class ConfiguredProducer(RecordingProducer):
        fail = False

        def __init__(self, config: dict) -> None:
            super().__init__()
            configs.append(config)
            self.config = config

        def flush(self, _timeout: float) -> int:
            if self.fail:
                self.config["on_delivery"](KafkaError(KafkaError._MSG_TIMED_OUT), None)
            self.config["on_delivery"](None, None)
            return 0

    with patch("examples.imdb.publish_and_load_nodes.Producer", ConfiguredProducer):
        result = run_publish_task(task, "kafka:9092", {"compression.type": "none", "linger.ms": 7})
        assert (result.label, result.topic, result.count, result.shard_count) == ("person", "imdb-person", 2, 1)
        assert configs[0]["bootstrap.servers"] == "kafka:9092"
        assert configs[0]["delivery.report.only.error"] is True
        assert configs[0]["compression.type"] == "none" and configs[0]["linger.ms"] == 7

        ConfiguredProducer.fail = True
        with pytest.raises(RuntimeError, match="failed to deliver 1 records for imdb-person"):
            run_publish_task(task, "kafka:9092")


def test_run_report_lists_the_publisher_layout(tmp_path: Path) -> None:
    report = write_run_report(
        tmp_path,
        started_at=datetime(2026, 9, 17, tzinfo=timezone.utc),
        total_seconds=1.0, partitions=4, publish_workers=4, consumer_workers=4,
        topic_setup_seconds=0.1, parallel_phase_seconds=0.5, image_build_seconds=None,
        topic_results=[("person", "imdb-person", 2, 0.2)], consumer_lifecycle_seconds=None, consumer_exit_code=None,
        publish_summary="4 process(es), 5 task(s), sharded topics: imdb-person, compression=lz4, linger.ms=50",
    )
    content = report.read_text(encoding="utf-8")
    assert "- Publisher threads: 4\n- Publisher layout: 4 process(es), 5 task(s)" in content
    assert "| person | `imdb-person` | 2 | 0.200s |" in content


def test_publish_only_main_publishes_every_topic_once_across_shards(tmp_path: Path) -> None:
    from concurrent.futures import ThreadPoolExecutor
    import threading

    from examples.imdb import publish_and_load_nodes as module

    subset = tmp_path / "subset"
    (subset / "graph" / "nodes").mkdir(parents=True)
    (subset / "graph" / "relationships").mkdir(parents=True)
    schema = load_schema(SCHEMA_FILE)
    nodes = {node.topic: node for node in schema.nodes}
    edges = {edge.type: edge for edge in schema.edges}
    expected: dict[str, list[tuple[bytes, bytes]]] = {}
    for label, (topic, filename) in module.NODE_SOURCES.items():
        key = nodes[topic].key_property
        others = [name for name in nodes[topic].properties if name not in {key, "name"} and nodes[topic].properties[name].type == "string"]
        columns = [key, *others]
        source = subset / "graph" / "nodes" / filename
        source.write_text(
            "\t".join(columns) + "\n" + "".join("\t".join(f"{label}{i}" for _ in columns) + "\n" for i in range(3)),
            encoding="utf-8",
        )
        expected[topic] = reference_node_messages(source, nodes[topic].properties, key)
    for edge_type, (topic, filename, source_column, target_column) in module.EDGE_SOURCES.items():
        columns = list(dict.fromkeys([source_column, target_column, *edges[edge_type].properties]))
        source = subset / "graph" / "relationships" / filename
        rows = 2000 if edge_type == "CREDITED_ON" else 40  # one topic large enough to shard
        source.write_text(
            "\t".join(columns) + "\n"
            + "".join("\t".join(f"{name}{i % 97}" for name in columns) + "\n" for i in range(rows)),
            encoding="utf-8",
        )
        expected[topic] = reference_edge_messages(source, edges[edge_type], source_column, target_column)

    produced: dict[str, list[tuple[bytes, bytes]]] = defaultdict(list)
    lock = threading.Lock()

    class SharedProducer(RecordingProducer):
        def __init__(self, config: dict) -> None:
            super().__init__()

        def produce(self, topic: str, *, key: bytes, value: bytes) -> None:
            with lock:
                produced[topic].append((key, value))

    def thread_pool(max_workers: int, mp_context=None) -> ThreadPoolExecutor:
        assert mp_context.get_start_method() == "spawn"
        return ThreadPoolExecutor(max_workers=max_workers)

    argv = [
        "publish_and_load_nodes.py", "--subset-dir", str(subset), "--publish-only",
        "--publish-workers", "3", "--publish-shards", "3", "--report-dir", str(tmp_path / "reports"),
    ]
    with patch.object(sys, "argv", argv), patch.object(module, "load_dotenv"), \
            patch.object(module, "ensure_topics"), patch.object(module, "require_empty_topics"), \
            patch.object(module, "MIN_SHARD_BYTES", 1), patch.object(module, "Producer", SharedProducer), \
            patch.object(module, "ProcessPoolExecutor", thread_pool):
        assert module.main() == 0

    assert set(produced) == set(expected)
    for topic, messages in expected.items():
        assert per_key(produced[topic]) == per_key(messages), topic
    report = next((tmp_path / "reports").glob("imdb-load-*.md")).read_text(encoding="utf-8")
    assert "- Publisher layout: 3 process(es)" in report
    assert "compression=lz4, linger.ms=50" in report
    assert "| CREDITED_ON | `imdb-credited-on` | 2000 |" in report
    assert "sharded topics: imdb-credited-on" in report


def test_run_report_records_bucket_lease_settings_and_coordinator_stats(tmp_path: Path) -> None:
    timings = tmp_path / "timings"
    timings.mkdir()
    (timings / "lease-settings.json").write_text(json.dumps({
        "address": "172.17.0.9:7700", "buckets": 16, "keyed_labels": ["Genre", "Region"], "outer_batch_size": 32000,
    }), encoding="utf-8")
    (timings / "lease-server.log").write_text(
        "INFO: stage=lease-server-ready host=0.0.0.0\nINFO: stage=lease-server-stats grants=7 max_concurrent=3\n",
        encoding="utf-8",
    )
    common = dict(
        started_at=datetime(2026, 9, 17, tzinfo=timezone.utc), total_seconds=1.0, partitions=4,
        publish_workers=4, consumer_workers=4, topic_setup_seconds=0.1, parallel_phase_seconds=0.5,
        image_build_seconds=None, topic_results=[("person", "imdb-person", 2, 0.2)],
        consumer_lifecycle_seconds=1.0, consumer_exit_code=0, edge_batch_size=32000,
    )
    content = write_run_report(
        tmp_path / "leases", timing_directory=timings, edge_leases=True, lease_buckets=16,
        bulk_edge_overlap="shared", **common,
    ).read_text(encoding="utf-8")
    assert "- Edge outer batch size: 32000" in content
    assert "- Edge bucket leases: enabled (16 buckets per label;" in content
    assert "- Lease keyed labels (one resource per node): Genre, Region; lease outer batch size: 32000" in content
    assert "- Lease coordinator stats: grants=7 max_concurrent=3" in content
    plain = write_run_report(tmp_path / "plain", **common).read_text(encoding="utf-8")
    assert "- Edge bucket leases: disabled" in plain and "Lease keyed labels" not in plain


def _run_main(tmp_path: Path, *flags: str):
    """Run ``main()`` against an empty subset; return the bulk CLI command, the report and the output."""
    from examples.imdb import publish_and_load_nodes as module

    subset = tmp_path / "subset"
    (subset / "graph" / "nodes").mkdir(parents=True)
    (subset / "graph" / "relationships").mkdir(parents=True)
    for _label, (_topic, filename) in module.NODE_SOURCES.items():
        (subset / "graph" / "nodes" / filename).write_text("", encoding="utf-8")
    for _edge, (_topic, filename, _source, _target) in module.EDGE_SOURCES.items():
        (subset / "graph" / "relationships" / filename).write_text("", encoding="utf-8")
    captured = {}

    def fake_run(command, environment, report_directory):
        captured["command"] = command
        report_directory.mkdir(parents=True, exist_ok=True)
        return 0, {}, None

    def no_publish(task, bootstrap, config):
        return SimpleNamespace(label=task.label, topic=task.topic, count=0, started=0.0, completed=0.0,
                               shard_index=0, shard_count=1)

    from concurrent.futures import ThreadPoolExecutor

    argv = ["publish_and_load_nodes.py", "--subset-dir", str(subset), "--skip-image-build", *flags,
            "--report-dir", str(tmp_path / "reports")]
    with patch.object(sys, "argv", argv), patch.object(module, "load_dotenv"), \
            patch.object(module, "ensure_topics"), patch.object(module, "require_empty_topics"), \
            patch.object(module, "run_publish_task", no_publish), \
            patch.object(module, "merge_publish_results", return_value=[]), \
            patch.object(module, "ProcessPoolExecutor", lambda max_workers, mp_context=None: ThreadPoolExecutor(max_workers)), \
            patch.object(module, "run_bulk_loader_with_timings", fake_run):
        assert module.main() == 0
    report = next((tmp_path / "reports").glob("imdb-load-*.md")).read_text(encoding="utf-8")
    return captured["command"], report


def test_main_without_a_mode_flag_uses_the_cli_planned_default(tmp_path: Path, capsys) -> None:
    command, report = _run_main(tmp_path)
    assert "--edge-plan" in command and command[command.index("--edge-plan") + 1].startswith("--")
    for absent in ("--edge-classic", "--edge-leases", "--bulk-edge-overlap", "--max-concurrent-bulk-edges"):
        assert absent not in command
    out = capsys.readouterr().out
    assert "planned from Kafka by the loader" in out
    assert "edge overlap" not in out
    assert "Bulk edge overlap" not in report
    assert "Edge bucket leases: disabled" in report


def test_main_planned_mode_drops_per_type_edge_lines_and_warns_about_classic_flags(tmp_path: Path, capsys) -> None:
    _, report = _run_main(tmp_path, "--bulk-edge-overlap", "shared")
    captured = capsys.readouterr()
    assert "tune classic only; ignored (add --edge-classic to use classic)" in captured.err
    assert "edge consumer(s) per type" not in captured.out
    assert "Edge writer threads per edge type" not in report and "Edge consumer containers per edge type" not in report


def test_main_plan_source_files_needs_edge_plan(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        _run_main(tmp_path, "--plan-source", "files")


def test_main_edge_classic_passes_the_classic_tuning_flags(tmp_path: Path, capsys) -> None:
    command, report = _run_main(tmp_path, "--edge-classic", "--bulk-edge-overlap", "shared",
                                "--max-concurrent-bulk-edges", "3")
    assert "--edge-classic" in command and "--edge-plan" not in command and "--edge-leases" not in command
    assert command[command.index("--bulk-edge-overlap") + 1] == "shared"
    assert command[command.index("--max-concurrent-bulk-edges") + 1] == "3"
    assert "edge overlap=shared, max concurrent edge types=3" in capsys.readouterr().out
    assert "- Bulk edge overlap: shared (max concurrent edge types: 3)" in report


@pytest.mark.parametrize("flags", [
    ("--edge-classic", "--edge-plan"), ("--edge-classic", "--edge-leases"), ("--edge-plan", "--edge-leases"),
])
def test_main_edge_mode_flags_are_mutually_exclusive(tmp_path: Path, flags) -> None:
    with pytest.raises(SystemExit):
        _run_main(tmp_path, *flags)


def test_main_passes_edge_lease_flags_to_the_bulk_cli(tmp_path: Path) -> None:
    command, report = _run_main(tmp_path, "--edge-leases", "--lease-buckets", "8")
    assert "--edge-classic" not in command and "--bulk-edge-overlap" not in command  # the CLI implies shared overlap
    assert "--edge-leases" in command
    # P5-S1 Story 1: the constructed subprocess command uses the current
    # --bucket-lease-* graph-loader CLI flag names (not the deprecated
    # --lease-* aliases), even though this example script's own argparse
    # flag (parsed above) keeps its --lease-buckets name unchanged.
    assert command[command.index("--bucket-lease-buckets") + 1] == "8"
    assert command[command.index("--bucket-lease-outer-batch-size") + 1] == "32000"
    assert command[command.index("--bucket-lease-keyed-max-nodes") + 1] == "1000"
    assert "- Edge outer batch size: 32000" in report
    assert "- Edge bucket leases: enabled (8 buckets per label;" in report
