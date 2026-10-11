"""P6-S2 Story 10: overhead benchmark and cross-cutting edge-case tests.

Only the wall-clock benchmark (``test_prometheus_client_call_cost_ceiling``)
is marked ``integration`` (this repo's existing convention for a heavier
test that should not run in the default fast unit suite, matching Slice
4's own ``test_star_topology_stress.py`` pattern): it needs no Docker/
network, but a wall-clock assertion is the kind of test that can be
noisy on a shared CI runner. Every other test in this file is cheap and
deterministic, so it stays in the default suite (P6-S2 review finding
S10-5).
"""
from __future__ import annotations

import socket
import time
from unittest.mock import MagicMock

import pytest
from prometheus_client import CollectorRegistry

from src.utils import metrics as metrics_module
from src.utils.metrics import GraphLoaderMetrics

N_CALLS = 20_000


def _real_emit(metrics: GraphLoaderMetrics) -> None:
    metrics.write_duration.labels(kind="node", name="Person", stage="node-write").observe(0.01)
    metrics.records_consumed.labels(kind="node", name="Person", label_or_type="Person").inc(500)


def _noop_emit(_metrics: GraphLoaderMetrics) -> None:
    pass


def _time_calls(emit, metrics: GraphLoaderMetrics, n: int) -> float:
    started = time.perf_counter()
    for _ in range(n):
        emit(metrics)
    return time.perf_counter() - started


@pytest.mark.integration
def test_prometheus_client_call_cost_ceiling():
    """P6-S2 review finding S10-1: renamed from the original
    'test_metrics_call_overhead_is_negligible_per_batch'. This measures
    only the marginal cost of one Counter.inc() + one Histogram.observe()
    (with a label lookup) called directly against prometheus_client - it
    proves "prometheus_client itself is cheap", NOT "this slice's
    instrumentation never runs per record". A future change that put an
    .observe() inside a per-record loop in _write_batch_with_retry would
    still pass this test at ~3us/call, since this benchmark never
    executes any loader/writer code at all. The real per-record guard is
    the deterministic call-count tests below (test_*_metrics_calls_scale_
    with_batches_not_records), which drive the actual write paths."""
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    # Warm up label-tuple caching (bounded by schema size, done once in
    # any real process) before timing either arm.
    _real_emit(metrics)

    real_elapsed = _time_calls(_real_emit, metrics, N_CALLS)
    noop_elapsed = _time_calls(_noop_emit, metrics, N_CALLS)

    per_call_overhead_seconds = (real_elapsed - noop_elapsed) / N_CALLS
    # Generous absolute ceiling (an order of magnitude above the ~3us
    # measured empirically on a shared dev host), per the story's own
    # "absolute microseconds-per-call ceiling" alternative to a percentage
    # threshold.
    assert per_call_overhead_seconds < 100e-6, (
        f"prometheus_client call cost {per_call_overhead_seconds * 1e6:.2f}us/call exceeds the 100us ceiling"
    )


def _count_labels_calls(metric) -> dict:
    """Wrap ``metric.labels`` with a counting spy that still calls
    through to the real bound method (so the returned child object's
    .observe()/.inc() keeps working) - counting .labels() invocations is
    exactly counting .observe()/.inc() call sites, since this codebase's
    own convention is always ``X.labels(...).observe(...)``/``.inc(...)``
    in one expression, never a cached call recorded separately."""
    original = metric.labels
    calls = {"n": 0}

    def counting_labels(*args, **kwargs):
        calls["n"] += 1
        return original(*args, **kwargs)

    metric.labels = counting_labels
    return calls


def test_node_loader_metrics_calls_scale_with_batches_not_records():
    """P6-S2 review finding S10-1: a deterministic, non-flaky behavioral
    guard - unlike the wall-clock benchmark above, this drives the real
    NodeLoader._write_batch_with_retry over B batches of R records each
    and asserts the metrics call count is exactly B (one per batch), not
    R*B (one per record). This is the test that would actually catch a
    future .observe()/.inc() call accidentally moved inside a per-record
    loop."""
    from src.loader.node_loader import NodeLoader, NodeRecord, PendingRecord
    from src.utils.schema_loader import load_schema
    from pathlib import Path

    schema = load_schema(Path(__file__).resolve().parent.parent / "config" / "graph_schema.yaml")
    person_config = next(node for node in schema.nodes if node.label == "Person")
    consumer, writer = MagicMock(), MagicMock()
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    loader = NodeLoader(consumer, writer, person_config, metrics=metrics)

    write_duration_calls = _count_labels_calls(metrics.write_duration)
    records_consumed_calls = _count_labels_calls(metrics.records_consumed)

    batches, records_per_batch = 50, 500
    for batch_index in range(batches):
        loader._batch = [
            PendingRecord(NodeRecord(key=f"p-{batch_index}-{i}", properties={}), "person-events", 0, i)
            for i in range(records_per_batch)
        ]
        assert loader._write_batch_with_retry() is True

    assert write_duration_calls["n"] == batches
    assert records_consumed_calls["n"] == batches
    assert metrics.registry.get_sample_value(
        "graph_loader_records_consumed_total",
        {"kind": "node", "name": "Person", "label_or_type": "Person"},
    ) == batches * records_per_batch


def test_edge_loader_metrics_calls_scale_with_batches_not_records():
    """Same guard as above, for EdgeLoader's classic/bulk _flush_batch
    write path. Buffers real messages through _buffer_message (not a
    hand-built _batch) so the partition ledger the flush path needs is
    populated the normal way."""
    import json
    from confluent_kafka import TopicPartition
    from src.loader.edge_loader import EdgeLoader
    from src.utils.schema_loader import load_schema
    from pathlib import Path

    schema = load_schema(Path(__file__).resolve().parent.parent / "config" / "graph_schema.yaml")
    works_at = next(edge for edge in schema.edges if edge.type == "WORKS_AT")
    consumer, writer = MagicMock(), MagicMock()
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    loader = EdgeLoader(consumer, writer, works_at, metrics=metrics)
    loader._on_assign(consumer, [TopicPartition("works-at-events", 0)])

    write_duration_calls = _count_labels_calls(metrics.write_duration)
    records_consumed_calls = _count_labels_calls(metrics.records_consumed)

    def message(offset: int) -> MagicMock:
        payload = json.dumps({
            "source": {"personId": f"p-{offset}"}, "target": {"companyId": f"c-{offset}"},
            "properties": {"since": "2020-01-02"},
        }).encode()
        msg = MagicMock()
        msg.error.return_value = None
        msg.value.return_value = payload
        msg.topic.return_value = "works-at-events"
        msg.partition.return_value = 0
        msg.offset.return_value = offset
        return msg

    batches, records_per_batch = 20, 200
    offset = 0
    for _batch_index in range(batches):
        for _record_index in range(records_per_batch):
            assert loader._buffer_message(message(offset))
            offset += 1
        assert loader._flush_batch() is True

    assert write_duration_calls["n"] == batches
    assert records_consumed_calls["n"] == batches
    assert metrics.registry.get_sample_value(
        "graph_loader_records_consumed_total",
        {"kind": "edge", "name": "WORKS_AT", "label_or_type": "WORKS_AT"},
    ) == batches * records_per_batch


def test_apoc_locked_edge_writer_deadlock_metric_never_scales_with_row_count():
    """Same guard, for ApocLockedEdgeWriter.write_rows: deadlocks_recovered_total
    must stay tied to the number of *batches* that recovered from a
    deadlock, never the number of rows in them."""
    from unittest.mock import MagicMock as _MM
    from neo4j.exceptions import TransientError
    from src.loader.edge_execution import ApocLockedEdgeWriter
    from src.loader.edge_loader import MissingRelationshipEndpointError
    from src.utils.schema_loader import load_schema
    from pathlib import Path

    schema = load_schema(Path(__file__).resolve().parent.parent / "config" / "graph_schema.yaml")
    works_at = next(edge for edge in schema.edges if edge.type == "WORKS_AT")
    metrics = GraphLoaderMetrics(registry=CollectorRegistry())

    driver = _MM()
    session = driver.session.return_value.__enter__.return_value
    transaction = session.begin_transaction.return_value.__enter__.return_value

    call_state = {"n": 0}

    def run_side_effect(*args, **kwargs):
        call_state["n"] += 1
        if call_state["n"] % 2 == 1:
            raise TransientError("Neo.TransientError.Transaction.DeadlockDetected")
        rows = kwargs.get("rows", args[-1] if args else [])
        result = _MM()
        result.__iter__.return_value = iter([
            {"checked_rows": len(rows), "invalid_rows": 0, "merged_rows": len(rows)}
        ])
        return result

    transaction.run.side_effect = run_side_effect
    writer = ApocLockedEdgeWriter(
        driver, works_at, missing_endpoint_error_factory=MissingRelationshipEndpointError,
        sleep=lambda _s: None, metrics=metrics, kind="edge",
    )
    deadlocks_calls = _count_labels_calls(metrics.deadlocks_recovered)

    batches, rows_per_batch = 20, 500
    for batch_index in range(batches):
        rows = [
            {"source_key": f"p-{batch_index}-{i}", "target_key": f"c-{batch_index}-{i}", "properties": {}}
            for i in range(rows_per_batch)
        ]
        writer.write_rows(rows)

    # Every batch's first attempt deadlocks and the retry succeeds - one
    # recovered deadlock per batch, never per row.
    assert deadlocks_calls["n"] == batches
    assert metrics.registry.get_sample_value(
        "graph_loader_deadlocks_recovered_total", {"kind": "edge", "name": "WORKS_AT"}
    ) == batches


def test_start_metrics_server_bind_failure_inside_one_container_never_crashes():
    """Story 10's port-conflict edge case, restated as a pure-Python unit
    test with no container/Docker involved (Story 3's own AC already
    covers 'two replicas never collide' via ephemeral host ports - this
    is the one case that isn't: two processes inside the *same*
    container/OS binding the identical fixed internal port)."""
    blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    blocker.bind(("127.0.0.1", 0))
    blocker.listen(1)
    port = blocker.getsockname()[1]
    try:
        metrics = GraphLoaderMetrics(registry=CollectorRegistry())
        server = metrics_module.start_metrics_server(
            metrics, kind="node", name="Person", addr="127.0.0.1", port=port,
        )
        assert server is None
    finally:
        blocker.close()


def test_start_metrics_server_unexpected_exception_never_crashes_any_of_six_kinds(monkeypatch):
    """Broader than Story 3's own AC (which only exercises the OSError
    branch): any exception, not only OSError, must never propagate out of
    start_metrics_server, for every one of the six kind= values a real
    process passes. Proven at the helper level, which is sufficient: every
    one of the six real main()/serve() functions calls this helper and
    nothing else that could raise sits between the call and its own
    internal try/except (confirmed by inspection of each call site)."""
    for kind, name in (
        ("node", "Person"), ("edge", "WORKS_AT"), ("planned-writer", "-"),
        ("lease-server", "-"), ("planned-dispatcher", "-"), ("clock", "-"),
    ):
        metrics = GraphLoaderMetrics(registry=CollectorRegistry())
        monkeypatch.setattr(metrics_module, "start_http_server", MagicMock(side_effect=RuntimeError("boom")))
        server = metrics_module.start_metrics_server(metrics, kind=kind, name=name)
        assert server is None


def test_three_node_loader_replicas_report_independent_non_aggregated_counters():
    """Story 10's AC: three replicas of one node label report three
    independent, non-aggregated records_consumed_total values when
    scraped individually - trivially true in production (each replica is
    its own OS process with its own registry), but worth this one
    explicit test given Story 1's registry design otherwise centers on
    avoiding *cross-test* leakage within one process, a different
    failure mode than cross-*replica* leakage. The literal end-to-end
    proof (three real containers, three real curls) is in the scratch
    Docker e2e validation, recorded in the completion doc."""
    replica_metrics = [GraphLoaderMetrics(registry=CollectorRegistry()) for _ in range(3)]
    for index, metrics in enumerate(replica_metrics):
        metrics.records_consumed.labels(
            kind="node", name="Person", label_or_type="Person"
        ).inc((index + 1) * 10)

    values = [
        metrics.registry.get_sample_value(
            "graph_loader_records_consumed_total",
            {"kind": "node", "name": "Person", "label_or_type": "Person"},
        )
        for metrics in replica_metrics
    ]
    assert values == [10.0, 20.0, 30.0]
    # Each replica's registry is genuinely its own object - no shared
    # state leaked across "replicas" (here, separate GraphLoaderMetrics
    # instances standing in for separate OS processes).
    assert len({id(metrics.registry) for metrics in replica_metrics}) == 3
