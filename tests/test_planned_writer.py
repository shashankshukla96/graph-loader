"""Unit tests for planned-edge writers: validation, fetching, prefetch, and the chunk loop."""
from __future__ import annotations

import asyncio
import copy
import json
import random
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from neo4j.exceptions import TransientError

from src.loader.edge_execution import ApocLockedEdgeWriter, build_apoc_locked_upsert_query
from src.loader.edge_loader import EdgeRecordValidationError, MissingRelationshipEndpointError, normalize_edge_record
from src.loader.edge_plan import (
    ChunkSpec, EdgePlanError, ResourceTable, assemble_plan, encode_chunk_message, resource_pair,
)
from src.loader.planned_writer import (
    ChunkDecoder, ChunkFetcher, DispatcherClient, DispatcherError, PlannedEdgeWriterWorker, compile_record_normalizer,
    planned_consumer_config,
)
from src.orchestrator.planned_dispatcher import PlannedDispatcher
from src.orchestrator.planned_edges import verify_ledger
from src.utils.schema_loader import load_schema

SCHEMA = load_schema(Path(__file__).resolve().parents[1] / "examples" / "imdb" / "graph_schema.yaml")
EDGES = {edge.type: edge for edge in SCHEMA.edges}
DIGEST = "c" * 64


def envelope(source, target, **properties):
    return {"source": {"nconst": source}, "target": {"tconst": target}, "properties": properties}


def _outcome(function, raw):
    try:
        return "ok", function(raw)
    except EdgeRecordValidationError as exc:
        return "error", str(exc)


def test_fast_normalizer_matches_the_reference_on_valid_and_invalid_records() -> None:
    edge = EDGES["ACTED_IN"]
    fast = compile_record_normalizer(edge)

    def reference(raw):
        record = normalize_edge_record(raw, edge)
        return record.source_key, record.target_key, record.properties

    base = envelope("nm1", "tt1", category="actor")
    variants = [
        base,
        envelope("nm1", "tt1"),  # missing required property
        envelope("nm1", "tt1", category=None),
        envelope("nm1", "tt1", category=5),
        envelope("nm1", "tt1", category="actor", extra="x"),
        envelope(1, "tt1", category="actor"),
        envelope("nm1", None, category="actor"),
        {**base, "extra": {}},
        {"source": base["source"], "target": base["target"]},
        {**base, "source": {"nconst": "nm1", "other": "x"}},
        {**base, "source": {"wrong": "nm1"}},
        {**base, "properties": []},
        [],
        "text",
    ]
    for raw in variants:
        assert _outcome(fast, copy.deepcopy(raw)) == _outcome(reference, copy.deepcopy(raw)), raw
    rng = random.Random(3)
    for _ in range(300):
        raw = copy.deepcopy(base)
        choice = rng.randrange(5)
        if choice == 0:
            raw["properties"]["category"] = rng.choice(["x", None, 3, 2.5, True])
        elif choice == 1:
            raw["source"]["nconst"] = rng.choice(["nm2", None, 7, ""])
        elif choice == 2:
            raw[rng.choice(["source", "target", "properties"])] = rng.choice([None, {}, "x"])
        elif choice == 3:
            raw["properties"][rng.choice(["category", "bogus"])] = "v"
        assert _outcome(fast, copy.deepcopy(raw)) == _outcome(reference, copy.deepcopy(raw)), raw


def make_plan(rows_by_chunk, *, run="plan-w", buckets=2):
    """One ACTED_IN unit per bucket pair holding the given records."""
    table = ResourceTable(["Person", "Title"], buckets)
    specs, records, total = [], {}, 0
    for index, rows in enumerate(rows_by_chunk):
        source = table.resource("Person", rows[0][0])
        target = table.resource("Title", rows[0][1])
        key = f"ACTED_IN:{index}"
        specs.append(ChunkSpec(key, "ACTED_IN", ("ACTED_IN", index), 0, resource_pair(source, target), len(rows),
                               0, index))
        records[key] = [json.dumps(envelope(s, t, category=c)) for s, t, c in rows]
        total += len(rows)
    types = {"ACTED_IN": ("Person", "Title", total)}
    plan = assemble_plan(plan_id=run, topic="chunks", partitions=1, resources=table, types=types, specs=specs)
    messages = {
        chunk.offset: encode_chunk_message(run, chunk.key, "ACTED_IN", records[chunk.key]) for chunk in plan.chunks
    }
    return plan, messages


def _rows_in_one_cell(count, buckets=2, start=0):
    table = ResourceTable(["Person", "Title"], buckets)
    rows, index = [], start
    while len(rows) < count:
        source, target = f"nm{index}", f"tt{index}"
        if table.resource("Person", source) == 0 and table.resource("Title", target) == buckets:
            rows.append((source, target, "actor" if index % 2 else "self"))
        index += 1
    return rows


def test_decoder_validates_header_counts_records_and_resources() -> None:
    plan, messages = make_plan([_rows_in_one_cell(3)])
    decoder = ChunkDecoder(plan, EDGES)
    chunk = plan.chunks[0]
    rows = decoder.decode(chunk, messages[0])
    assert [row["properties"] for row in rows] == [{"category": item[2]} for item in _rows_in_one_cell(3)]
    other_plan, other = make_plan([_rows_in_one_cell(3)], run="another")
    with pytest.raises(EdgePlanError, match="is not chunk"):
        decoder.decode(chunk, other[0])
    records = [json.dumps(envelope(s, t, category=c)) for s, t, c in _rows_in_one_cell(2)]
    with pytest.raises(EdgePlanError, match="holds 2"):
        decoder.decode(chunk, encode_chunk_message("plan-w", chunk.key, "ACTED_IN", records))
    # A record whose endpoint maps to a resource outside the chunk fails closed.
    table = plan.resources
    outside = next(f"nm{i}" for i in range(1000) if table.resource("Person", f"nm{i}") != 0)
    bad = records + [json.dumps(envelope(outside, _rows_in_one_cell(1)[0][1], category="x"))]
    with pytest.raises(EdgePlanError, match="outside its planned resources"):
        decoder.decode(chunk, encode_chunk_message("plan-w", chunk.key, "ACTED_IN", bad))
    invalid = records + [json.dumps(envelope("nm0", "tt0"))]
    with pytest.raises(EdgeRecordValidationError, match="is required"):
        decoder.decode(chunk, encode_chunk_message("plan-w", chunk.key, "ACTED_IN", invalid))


def test_decoder_rejects_plans_that_disagree_with_the_schema() -> None:
    plan, _ = make_plan([_rows_in_one_cell(1)])
    with pytest.raises(EdgePlanError, match="not in the schema"):
        ChunkDecoder(plan, {name: edge for name, edge in EDGES.items() if name != "ACTED_IN"})


class FakeMessage:
    def __init__(self, topic, partition, offset, value, error=None):
        self._topic, self._partition, self._offset, self._value, self._error = topic, partition, offset, value, error

    def topic(self): return self._topic
    def partition(self): return self._partition
    def offset(self): return self._offset
    def value(self): return self._value
    def error(self): return self._error


class FakeConsumer:
    """Serves ``log[partition]`` from the assigned offset onwards."""

    def __init__(self, topic, log):
        self.topic, self.log = topic, log
        self.assignments = []
        self._queue = []

    def assign(self, partitions):
        self.assignments.append([(tp.topic, tp.partition, tp.offset) for tp in partitions])
        (tp,) = partitions
        self._queue = [
            FakeMessage(self.topic, tp.partition, offset, value)
            for offset, value in sorted(self.log.get(tp.partition, {}).items()) if offset >= tp.offset - 1
        ]

    def poll(self, timeout):
        return self._queue.pop(0) if self._queue else None

    def unassign(self):
        self.assignments.append([])
        self._queue = []


def test_fetcher_seeks_the_exact_offset_and_serves_prefetched_chunks() -> None:
    plan, messages = make_plan([_rows_in_one_cell(2), _rows_in_one_cell(3, start=500)])
    consumer = FakeConsumer("chunks", {0: dict(messages)})
    fetcher = ChunkFetcher(consumer, "chunks", ChunkDecoder(plan, EDGES), fetch_timeout_seconds=1, poll_seconds=0.01)
    try:
        fetcher.prefetch(plan.chunks[1])
        second, hit = fetcher.get(plan.chunks[1])
        assert hit and len(second.rows) == plan.chunks[1].rows
        first, hit = fetcher.get(plan.chunks[0])
        assert not hit and len(first.rows) == plan.chunks[0].rows
        assert consumer.assignments[0] == [("chunks", 0, plan.chunks[1].offset)]
        assert consumer.assignments[1] == []  # unassigned right after the chunk arrived
        assert fetcher.fetches == 2 and fetcher.prefetch_hits == 1
    finally:
        fetcher.close()


def test_fetcher_fails_closed_on_missing_or_late_messages() -> None:
    plan, messages = make_plan([_rows_in_one_cell(2), _rows_in_one_cell(3, start=500)])
    low, high = sorted(plan.chunks, key=lambda chunk: chunk.offset)
    consumer = FakeConsumer("chunks", {0: {high.offset: messages[high.offset]}})
    fetcher = ChunkFetcher(consumer, "chunks", ChunkDecoder(plan, EDGES), fetch_timeout_seconds=0.2, poll_seconds=0.01)
    try:
        with pytest.raises(EdgePlanError, match="missing from Kafka"):
            fetcher.get(low)
        consumer.log = {0: {}}
        with pytest.raises(EdgePlanError, match="not fetched within"):
            fetcher.get(high)
    finally:
        fetcher.close()


def test_consumer_config_limits_read_ahead_and_disables_fetch_backoff() -> None:
    config = planned_consumer_config("kafka:29092", "g", 16 << 20)
    assert config["enable.auto.commit"] is False and config["auto.offset.reset"] == "error"
    assert config["queued.min.messages"] == 1 and config["fetch.queue.backoff.ms"] == 0
    assert config["fetch.max.bytes"] > 16 << 20


def test_planned_query_keeps_input_row_order_and_uses_no_apoc_lock() -> None:
    query = build_apoc_locked_upsert_query(EDGES["PLAYED"], explicit_locks=False, sort_rows=False)
    assert "ORDER BY" not in query and "apoc" not in query
    assert "ORDER BY elementId(s), elementId(t)" in build_apoc_locked_upsert_query(EDGES["PLAYED"], explicit_locks=False)


def _session_driver(results):
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    transactions = []

    def begin(**kwargs):
        transaction = MagicMock()
        outcome = results.pop(0)
        if isinstance(outcome, Exception):
            transaction.run.side_effect = outcome
        else:
            result = MagicMock()
            result.__iter__.return_value = iter([outcome])
            transaction.run.return_value = result
        transactions.append((kwargs, transaction))
        manager = MagicMock()
        manager.__enter__.return_value = transaction
        manager.__exit__.return_value = False
        return manager

    session.begin_transaction.side_effect = begin
    return driver, transactions


def test_write_rows_retries_transient_errors_with_metadata_and_fails_closed_on_endpoints() -> None:
    rows = [{"source_key": "nm1", "target_key": "tt1", "properties": {"category": "actor"}}]
    good = {"checked_rows": 1, "invalid_rows": 0, "merged_rows": 1}
    transient = TransientError("deadlock")
    driver, transactions = _session_driver([transient, good])
    sleeps = []
    writer = ApocLockedEdgeWriter(driver, EDGES["ACTED_IN"], explicit_locks=False, sort_rows=False,
                                  missing_endpoint_error_factory=MissingRelationshipEndpointError, sleep=sleeps.append)
    assert writer.write_rows(rows, metadata={"chunk": 7}) == 2
    assert [kwargs for kwargs, _ in transactions] == [{"metadata": {"chunk": 7}}] * 2
    transactions[-1][1].commit.assert_called_once()
    assert len(sleeps) == 1
    driver, transactions = _session_driver([{"checked_rows": 1, "invalid_rows": 1, "merged_rows": 0}])
    writer = ApocLockedEdgeWriter(driver, EDGES["ACTED_IN"], explicit_locks=False, sort_rows=False,
                                  missing_endpoint_error_factory=MissingRelationshipEndpointError)
    with pytest.raises(MissingRelationshipEndpointError):
        writer.write_rows(rows)
    transactions[0][1].commit.assert_not_called()


class FakeClient:
    def __init__(self, replies):
        self.replies = list(replies)
        self.done_calls, self.fail_calls = [], []
        self.broken = False

    def connect(self):
        return "plan-w"

    def next(self, wait_ms):
        return self.replies.pop(0)

    def done(self, chunk, rows, write_ms):
        self.done_calls.append((chunk, rows))

    def fail(self, chunk, reason):
        self.fail_calls.append((chunk, reason))


class FakeFetcher:
    def __init__(self, rows):
        self.rows, self.prefetched, self.fetches, self.prefetch_hits = rows, [], 0, 0

    def get(self, chunk):
        return SimpleNamespace(rows=self.rows[chunk.id], fetch_ms=0.5), chunk.id in self.prefetched

    def prefetch(self, chunk):
        self.prefetched.append(chunk.id)


def test_worker_writes_each_granted_chunk_once_then_exits_on_end(caplog) -> None:
    plan, _ = make_plan([_rows_in_one_cell(2), _rows_in_one_cell(1, start=500)])
    rows = {0: [{"r": 1}, {"r": 2}], 1: [{"r": 3}]}
    writer = MagicMock()
    writer.write_rows.return_value = 1
    client = FakeClient(["WAIT", (0, 1), (1, None), "END"])
    fetcher = FakeFetcher(rows)
    caplog.set_level("INFO")
    worker = PlannedEdgeWriterWorker(plan, client, fetcher, {"ACTED_IN": writer}, writer_id="w0")
    assert worker.run() == 0
    assert client.done_calls == [(0, 2), (1, 1)] and client.fail_calls == []
    assert fetcher.prefetched == [1]
    assert [call.args[0] for call in writer.write_rows.call_args_list] == [rows[0], rows[1]]
    assert writer.write_rows.call_args.kwargs["metadata"] == {"graph_loader_plan": "plan-w", "chunk": 1}
    lines = [record.getMessage() for record in caplog.records if record.getMessage().startswith("stage=edge-write ")]
    assert len(lines) == 2 and "planned=1" in lines[1] and "prefetched=1" in lines[1] and "edge=ACTED_IN" in lines[1]


# ── Metrics instrumentation (P6-S2 Story 6) ──────────────────────────────────


def _two_type_plan(rows_by_chunk_and_type):
    """Like make_plan, but each chunk names its own edge type - Story 6's
    AC needs a plan mixing at least two edge types in one writer process."""
    table = ResourceTable(["Person", "Title"], 2)
    specs, records, totals = [], {}, {}
    for index, (edge_type, rows) in enumerate(rows_by_chunk_and_type):
        source = table.resource("Person", rows[0][0])
        target = table.resource("Title", rows[0][1])
        key = f"{edge_type}:{index}"
        specs.append(ChunkSpec(key, edge_type, (edge_type, index), 0, resource_pair(source, target), len(rows),
                               0, index))
        records[key] = [json.dumps(envelope(s, t, category=c)) for s, t, c in rows]
        totals[edge_type] = totals.get(edge_type, 0) + len(rows)
    types = {edge_type: ("Person", "Title", total) for edge_type, total in totals.items()}
    plan = assemble_plan(plan_id="plan-w", topic="chunks", partitions=1, resources=table, types=types, specs=specs)
    return plan


def test_worker_increments_records_consumed_per_chunk_edge_type() -> None:
    """A plan mixing ACTED_IN and DIRECTED in one writer process: each
    chunk's records_consumed_total is labelled by *that chunk's* edge
    type, not a single fixed process-wide name (Story 6's own distinction
    from node/edge loaders' fixed-at-startup `name`)."""
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    plan = _two_type_plan([
        ("ACTED_IN", _rows_in_one_cell(2)),
        ("DIRECTED", _rows_in_one_cell(1, start=500)),
    ])
    rows = {0: [{"r": 1}, {"r": 2}], 1: [{"r": 3}]}
    acted_in_writer, directed_writer = MagicMock(), MagicMock()
    acted_in_writer.write_rows.return_value = 1
    directed_writer.write_rows.return_value = 1
    client = FakeClient([(0, 1), (1, None), "END"])
    fetcher = FakeFetcher(rows)
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    worker = PlannedEdgeWriterWorker(
        plan, client, fetcher, {"ACTED_IN": acted_in_writer, "DIRECTED": directed_writer},
        writer_id="w0", metrics=metrics,
    )

    assert worker.run() == 0

    assert metrics.registry.get_sample_value(
        "graph_loader_records_consumed_total",
        {"kind": "planned-writer", "name": "ACTED_IN", "label_or_type": "ACTED_IN"},
    ) == 2.0
    assert metrics.registry.get_sample_value(
        "graph_loader_records_consumed_total",
        {"kind": "planned-writer", "name": "DIRECTED", "label_or_type": "DIRECTED"},
    ) == 1.0
    assert metrics.registry.get_sample_value(
        "graph_loader_neo4j_write_duration_seconds_count",
        {"kind": "planned-writer", "name": "ACTED_IN", "stage": "edge-write"},
    ) == 1.0
    assert metrics.registry.get_sample_value(
        "graph_loader_neo4j_write_duration_seconds_count",
        {"kind": "planned-writer", "name": "DIRECTED", "stage": "edge-write"},
    ) == 1.0


def test_worker_metrics_calls_scale_with_chunks_not_rows() -> None:
    """P6-S2 review finding S10-1: a deterministic behavioral guard - each
    chunk here carries many rows (synthetic, via FakeFetcher - the point
    is the row *count* per emission call, not real chunk content), but
    the metrics call count must equal the chunk count (2), not the total
    row count."""
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    rows_per_chunk = 300
    plan, _ = make_plan([_rows_in_one_cell(2), _rows_in_one_cell(1, start=500)])
    rows = {
        0: [{"r": i} for i in range(rows_per_chunk)],
        1: [{"r": i} for i in range(rows_per_chunk)],
    }
    writer = MagicMock()
    writer.write_rows.return_value = 1
    client = FakeClient([(0, 1), (1, None), "END"])
    fetcher = FakeFetcher(rows)
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())

    write_duration_calls = _count_labels_calls_for_worker(metrics.write_duration)
    records_consumed_calls = _count_labels_calls_for_worker(metrics.records_consumed)

    worker = PlannedEdgeWriterWorker(plan, client, fetcher, {"ACTED_IN": writer}, writer_id="w0", metrics=metrics)
    assert worker.run() == 0

    assert write_duration_calls["n"] == 2
    assert records_consumed_calls["n"] == 2
    assert metrics.registry.get_sample_value(
        "graph_loader_records_consumed_total",
        {"kind": "planned-writer", "name": "ACTED_IN", "label_or_type": "ACTED_IN"},
    ) == 2 * rows_per_chunk


def _count_labels_calls_for_worker(metric) -> dict:
    original = metric.labels
    calls = {"n": 0}

    def counting_labels(*args, **kwargs):
        calls["n"] += 1
        return original(*args, **kwargs)

    metric.labels = counting_labels
    return calls


def test_planned_writer_metrics_failure_never_blocks_a_successful_chunk() -> None:
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    plan, _ = make_plan([_rows_in_one_cell(2)])
    rows = {0: [{"r": 1}, {"r": 2}]}
    writer = MagicMock()
    writer.write_rows.return_value = 1
    client = FakeClient([(0, None), "END"])
    fetcher = FakeFetcher(rows)
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())

    def boom(**_kwargs):
        raise ValueError("simulated metrics failure")

    metrics.records_consumed.labels = boom
    worker = PlannedEdgeWriterWorker(plan, client, fetcher, {"ACTED_IN": writer}, writer_id="w0", metrics=metrics)

    assert worker.run() == 0
    assert client.done_calls == [(0, 2)] and client.fail_calls == []


def test_main_constructs_apoc_writer_and_worker_with_planned_writer_kind_and_process_metrics(monkeypatch) -> None:
    """P6-S2 review finding S6-1: a behavioral test of main()'s own
    construction call sites (planned_writer.py:~630/~653), not a
    standalone ApocLockedEdgeWriter built directly by the test itself -
    the earlier version of this test was tautological (it built the
    object with the kwargs it then asserted on) and would not fail if
    `metrics=process_metrics, kind="planned-writer"` were ever accidentally
    deleted from main(). Drives the real main() with every I/O dependency
    faked, and a recording mock in place of both ApocLockedEdgeWriter and
    PlannedEdgeWriterWorker."""
    import src.cli as cli_module
    import src.loader.edge_execution as edge_execution_module
    import src.loader.planned_writer as planned_writer_module
    import src.orchestrator.schema_initializer as schema_initializer_module
    import src.utils.schema_loader as schema_loader_module
    from src.utils import metrics as metrics_module
    import confluent_kafka

    plan, _ = make_plan([_rows_in_one_cell(2)])  # plan.types == {"ACTED_IN": ...}

    monkeypatch.setattr(planned_writer_module, "read_plan_file", lambda _path: (plan, "d" * 64))
    monkeypatch.setattr(schema_loader_module, "load_schema", lambda _path: SCHEMA)
    monkeypatch.setattr(cli_module, "get_neo4j_credentials", lambda: ("bolt://x", "neo4j", "pw"))
    monkeypatch.setattr(schema_initializer_module, "get_neo4j_driver", lambda *a, **k: MagicMock())
    monkeypatch.setattr(confluent_kafka, "Consumer", lambda *a, **k: MagicMock())

    writer_calls: list[dict] = []

    class RecordingWriterClass:
        def __init__(self, *args, **kwargs):
            writer_calls.append(kwargs)

    monkeypatch.setattr(edge_execution_module, "ApocLockedEdgeWriter", RecordingWriterClass)

    worker_calls: list[dict] = []

    class RecordingWorker:
        def __init__(self, *args, **kwargs):
            worker_calls.append(kwargs)

        def run(self):
            return 0

    monkeypatch.setattr(planned_writer_module, "PlannedEdgeWriterWorker", RecordingWorker)

    process_metrics = metrics_module.process_metrics()
    exit_code = planned_writer_module.main([
        "--plan", "unused.json", "--dispatcher", "127.0.0.1:1", "--writer-id", "w0",
    ])

    assert exit_code == 0
    assert len(writer_calls) == len(plan.types) == 1
    for kwargs in writer_calls:
        assert kwargs["kind"] == "planned-writer"
        assert kwargs["metrics"] is process_metrics
        # P5-S1 Story 4 / P5-Q1 fencing amendment: planned mode has no
        # lease grant, so it gets a flat per-attempt T sourced from the
        # schema, not a per-call deadline.
        assert kwargs["tx_timeout_seconds"] == SCHEMA.loading.bucket_lease.tx_timeout_ms / 1000
    assert len(worker_calls) == 1
    assert worker_calls[0]["metrics"] is process_metrics


def test_close_failure_still_keeps_sigterm_handler(monkeypatch) -> None:
    """P6-S5 Story 4, Decision 10: node_loader.py and edge_loader.py already
    have a `getsignal`-based test proving the previous SIGTERM handler is
    restored on `main()` exit, including when a close/flush step raises;
    planned_writer.py had none. This is confirmation-only - the production
    `finally` at planned_writer.py already restores unconditionally - but a
    future refactor that drops that line should fail a test, not just be
    caught by code review."""
    import signal
    import src.cli as cli_module
    import src.loader.edge_execution as edge_execution_module
    import src.loader.planned_writer as planned_writer_module
    import src.orchestrator.schema_initializer as schema_initializer_module
    import src.utils.schema_loader as schema_loader_module
    import confluent_kafka

    plan, _ = make_plan([_rows_in_one_cell(2)])

    monkeypatch.setattr(planned_writer_module, "read_plan_file", lambda _path: (plan, "d" * 64))
    monkeypatch.setattr(schema_loader_module, "load_schema", lambda _path: SCHEMA)
    monkeypatch.setattr(cli_module, "get_neo4j_credentials", lambda: ("bolt://x", "neo4j", "pw"))
    driver = MagicMock()
    driver.close.side_effect = RuntimeError("close failed")
    monkeypatch.setattr(schema_initializer_module, "get_neo4j_driver", lambda *a, **k: driver)
    monkeypatch.setattr(confluent_kafka, "Consumer", lambda *a, **k: MagicMock())
    monkeypatch.setattr(edge_execution_module, "ApocLockedEdgeWriter", lambda *a, **k: MagicMock())

    class RecordingWorker:
        def __init__(self, *args, **kwargs):
            pass

        def run(self):
            return 0

    monkeypatch.setattr(planned_writer_module, "PlannedEdgeWriterWorker", RecordingWorker)

    exit_code = planned_writer_module.main([
        "--plan", "unused.json", "--dispatcher", "127.0.0.1:1", "--writer-id", "w0",
    ])

    assert exit_code == 0
    driver.close.assert_called_once()
    # F2: SIGTERM is ignored as the last shutdown step (CPython resets Python handlers
    # to SIG_DFL at interpreter exit; only SIG_IGN survives a repeated stop signal).
    assert signal.getsignal(signal.SIGTERM) == signal.SIG_IGN


def test_main_sigterm_mid_run_leaves_snapshot_with_accumulated_counters(monkeypatch, tmp_path) -> None:
    """P6-S2 Story 9/10 (review finding S9-1): a SIGTERM arriving mid-run,
    after at least one counted chunk write, still leaves a metrics-*.prom
    snapshot with that chunk's counters. Drives the real main() and the
    real PlannedEdgeWriterWorker.run() loop; only the I/O boundary
    (Consumer, DispatcherClient, ChunkFetcher, ApocLockedEdgeWriter) is
    faked."""
    import signal
    import src.loader.edge_execution as edge_execution_module
    import src.loader.planned_writer as planned_writer_module
    import src.orchestrator.schema_initializer as schema_initializer_module
    import src.utils.schema_loader as schema_loader_module
    import confluent_kafka

    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    # P6-S2 review finding S10-2: pin to a free ephemeral port instead of
    # binding the real 0.0.0.0:9464 during the unit suite.
    import socket as _socket_module
    with _socket_module.socket(_socket_module.AF_INET, _socket_module.SOCK_STREAM) as _s:
        _s.bind(("127.0.0.1", 0))
        monkeypatch.setenv("GRAPH_LOADER_METRICS_PORT", str(_s.getsockname()[1]))
    snapshot_path = tmp_path / "metrics-planned-writer-w0.prom"
    monkeypatch.setenv("GRAPH_LOADER_METRICS_SNAPSHOT_PATH", str(snapshot_path))

    plan, _ = make_plan([_rows_in_one_cell(2)])
    rows = {0: [{"r": 1}, {"r": 2}]}

    monkeypatch.setattr(planned_writer_module, "read_plan_file", lambda _path: (plan, "d" * 64))
    monkeypatch.setattr(schema_loader_module, "load_schema", lambda _path: SCHEMA)
    monkeypatch.setattr(schema_initializer_module, "get_neo4j_driver", lambda *a, **k: MagicMock())
    monkeypatch.setattr(confluent_kafka, "Consumer", lambda *a, **k: MagicMock())

    stub_writer = MagicMock()
    stub_writer.write_rows.return_value = 1
    monkeypatch.setattr(edge_execution_module, "ApocLockedEdgeWriter", lambda *a, **k: stub_writer)

    handlers: dict[str, object] = {}
    real_signal = signal.signal

    def capturing_signal(sig, handler):
        previous = real_signal(sig, handler)
        if sig == signal.SIGTERM:
            handlers["current"] = handler
        return previous

    monkeypatch.setattr(planned_writer_module.signal, "signal", capturing_signal)
    monkeypatch.setattr(planned_writer_module, "ChunkFetcher", lambda *a, **k: FakeFetcher(rows))

    call_state = {"n": 0}

    class SigtermClient(FakeClient):
        def next(self, wait_ms):
            reply = super().next(wait_ms)
            call_state["n"] += 1
            if call_state["n"] == 1:
                # One chunk granted and about to be fetched/written; fire
                # the SIGTERM handler now so the *next* loop iteration's
                # shutdown check sees it.
                handlers["current"](signal.SIGTERM, None)
            return reply

    monkeypatch.setattr(
        planned_writer_module, "DispatcherClient",
        lambda *a, **k: SigtermClient([(0, None), "END"]),
    )

    exit_code = planned_writer_module.main([
        "--plan", "unused.json", "--dispatcher", "127.0.0.1:1", "--writer-id", "w0",
    ])

    # The one granted chunk is written and DONE'd normally (the SIGTERM
    # handler fires mid-`.next()`, before the *next* chunk is requested);
    # the following loop iteration observes the shutdown and stops
    # cleanly. P6-S5 Story 4: this is a requested stop, not a failure, so
    # main() now deterministically returns 0 - no longer "either is fine."
    assert exit_code == 0
    assert snapshot_path.exists()
    from prometheus_client.parser import text_string_to_metric_families

    families = {family.name: family for family in text_string_to_metric_families(snapshot_path.read_text())}
    samples = families["graph_loader_records_consumed"].samples
    assert any(sample.value > 0 for sample in samples)


def test_worker_failure_reports_fail_and_never_done() -> None:
    plan, _ = make_plan([_rows_in_one_cell(2)])
    writer = MagicMock()
    writer.write_rows.side_effect = MissingRelationshipEndpointError("missing endpoint")
    client = FakeClient([(0, None)])
    worker = PlannedEdgeWriterWorker(plan, client, FakeFetcher({0: [{}, {}]}), {"ACTED_IN": writer}, writer_id="w0")
    assert worker.run() == 1
    assert client.done_calls == [] and client.fail_calls[0][0] == 0 and "missing endpoint" in client.fail_calls[0][1]


def test_worker_stops_between_chunks_on_shutdown_request() -> None:
    plan, _ = make_plan([_rows_in_one_cell(2)])
    client = FakeClient([(0, None)])
    worker = PlannedEdgeWriterWorker(plan, client, FakeFetcher({}), {}, writer_id="w0",
                                     shutdown_requested=lambda: True)
    # P6-S5 Story 4: a shutdown observed before any chunk is granted is a
    # clean, requested stop, not a failure - exit 0, and the dispatcher is
    # never told FAIL for a chunk that was never granted.
    assert worker.run() == 0
    assert client.fail_calls == []


def test_orchestrator_stop_abort_between_chunks_exits_zero_without_fail() -> None:
    """D5: an operator-stop ABORT answered to NEXT is a clean stop; any other abort stays a failure."""
    from src.loader.planned_writer import DispatcherClient, STOP_REASON

    plan, _ = make_plan([_rows_in_one_cell(2)])

    def worker_for(reply):
        client = DispatcherClient("127.0.0.1:1", digest="a" * 64, writer_id="w0")
        client.connect = lambda: "plan-w"
        client._request = lambda line, extra_timeout=0.0: reply
        client.fail = MagicMock()
        return PlannedEdgeWriterWorker(plan, client, FakeFetcher({}), {}, writer_id="w0"), client

    worker, client = worker_for(f"ERR failed aborted by orchestrator: {STOP_REASON}")
    assert worker.run() == 0
    client.fail.assert_not_called()

    worker, client = worker_for("ERR failed aborted by orchestrator: writer w1 exited with code 2")
    assert worker.run() == 1


def test_writer_pool_against_a_real_dispatcher_commits_every_chunk_exactly_once() -> None:
    chunks = [_rows_in_one_cell(2, start=start) for start in range(0, 4000, 400)]
    plan, messages = make_plan(chunks, buckets=2)
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    dispatcher = PlannedDispatcher(plan, DIGEST, ready_labels=["Person", "Title"])
    host, port = asyncio.run_coroutine_threadsafe(dispatcher.start("127.0.0.1", 0), loop).result(5)
    committed, lock, active = [], threading.Lock(), set()

    class RecordingWriter:
        def write_rows(self, rows, metadata=None):
            chunk = plan.chunks[metadata["chunk"]]
            with lock:
                for other in active:
                    assert set(plan.chunks[other].resources).isdisjoint(chunk.resources)
                active.add(chunk.id)
            threading.Event().wait(0.002)
            with lock:
                active.discard(chunk.id)
                committed.append(chunk.id)
            return 1

    def run_writer(index):
        client = DispatcherClient(f"{host}:{port}", digest=DIGEST, writer_id=f"w{index}")
        consumer = FakeConsumer("chunks", {0: dict(messages)})
        fetcher = ChunkFetcher(consumer, "chunks", ChunkDecoder(plan, EDGES), poll_seconds=0.01)
        try:
            return PlannedEdgeWriterWorker(plan, client, fetcher, {"ACTED_IN": RecordingWriter()},
                                           writer_id=f"w{index}", wait_ms=200).run()
        finally:
            fetcher.close()
            client.close()

    results = []
    threads = [threading.Thread(target=lambda i=i: results.append(run_writer(i))) for i in range(4)]
    for item in threads:
        item.start()
    for item in threads:
        item.join(30)
    control = DispatcherClient(f"{host}:{port}", digest=DIGEST)
    summary = control.summary()
    control.close()
    asyncio.run_coroutine_threadsafe(dispatcher.close(), loop).result(5)
    loop.call_soon_threadsafe(loop.stop)
    assert results == [0, 0, 0, 0]
    assert sorted(committed) == list(range(len(plan.chunks)))
    per_type = verify_ledger(plan, summary)
    assert per_type["ACTED_IN"]["rows"] == plan.total_rows == 20


def test_client_retries_a_connection_reset_before_the_hello_reply() -> None:
    """A published Docker port accepts TCP before the dispatcher listens, then resets it."""
    replies = [b"", b"OK plan-7\n"]
    attempts = []

    class FakeSocket:
        def __init__(self):
            self.reply = replies.pop(0)

        def setsockopt(self, *args):
            pass

        def settimeout(self, value):
            pass

        def sendall(self, data):
            attempts.append(data)

        def makefile(self, mode):
            reply = self.reply
            return SimpleNamespace(readline=lambda: reply, close=lambda: None)

        def close(self):
            pass

    sleeps = []
    client = DispatcherClient("127.0.0.1:1", digest=DIGEST, writer_id="w3",
                              connect=lambda address, timeout: FakeSocket(), sleep=sleeps.append)
    assert client.connect() == "plan-7"
    assert attempts == [f"HELLO WRITER w3 {DIGEST}\n".encode()] * 2 and len(sleeps) == 1
    refusing = DispatcherClient("127.0.0.1:1", digest=DIGEST, connect_timeout_seconds=0,
                                connect=lambda address, timeout: (_ for _ in ()).throw(ConnectionRefusedError()))
    with pytest.raises(DispatcherError, match="unreachable"):
        refusing.connect()
    assert refusing.broken
