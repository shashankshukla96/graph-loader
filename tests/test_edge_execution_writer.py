"""Unit tests for the APOC-locked relationship writer."""
from datetime import date
from threading import Barrier, get_ident
from unittest.mock import MagicMock
import pytest
from neo4j.exceptions import ClientError, TransientError

from src.loader.edge_execution import ApocLockedEdgeWriter, LaneExecutionCoordinator, NativeDisjointEdgeWriter, ServerCapabilities, ExecutionModeError, build_apoc_locked_upsert_query, build_edge_execution
from src.loader.lease_client import LeaseCoordinatorError
from src.loader.mix_and_batch import RoutedEdgeRecord
from src.loader.edge_loader import EdgeRecord, MissingRelationshipEndpointError, PendingEdgeRecord
from src.models.schema import DeadLetterConfig
from src.utils.schema_loader import load_schema


def _edge():
    return next(e for e in load_schema("config/graph_schema.yaml").edges if e.type == "WORKS_AT")


def _knows():
    return next(e for e in load_schema("config/graph_schema.yaml").edges if e.type == "KNOWS")


def _record(key="p-1"):
    return EdgeRecord(key, "c-1", {"since": date(2020, 1, 2)})


def _ok(rows=1):
    result = MagicMock(); result.__iter__.return_value = iter([{"checked_rows": rows, "invalid_rows": 0, "merged_rows": rows}])
    return result


def _invalid(checked_rows, invalid_rows=1, merged_rows=0):
    result = MagicMock()
    result.__iter__.return_value = iter(
        [{"checked_rows": checked_rows, "invalid_rows": invalid_rows, "merged_rows": merged_rows}]
    )
    return result


def _classification(triples):
    """triples: [(row_index, source_matches, target_matches), ...]."""
    result = MagicMock()
    result.__iter__.return_value = iter(
        [{"row_index": i, "source_matches": s, "target_matches": t} for i, s, t in triples]
    )
    return result


def _pending(record, offset, topic="works-at-events", partition=0):
    return PendingEdgeRecord(record=record, topic=topic, partition=partition, offset=offset)


def _apoc_tx():
    driver = MagicMock(); session = driver.session.return_value.__enter__.return_value
    tx = session.begin_transaction.return_value.__enter__.return_value
    return driver, tx


def _apoc_writer(driver):
    return ApocLockedEdgeWriter(driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError)


def test_apoc_query_uses_sorted_distinct_locks_and_schema_identifiers():
    query = build_apoc_locked_upsert_query(_edge())
    assert "CALL (all_nodes) { UNWIND all_nodes AS n" in query
    assert "WITH DISTINCT n AS dist_n ORDER BY elementId(dist_n)" in query
    assert "CALL apoc.lock.nodes(sorted_nodes)" in query
    assert "WITH rel.s AS s, rel.t AS t, rel.props AS props MERGE (s)-[r:`WORKS_AT`]->(t) SET r += props" in query
    assert "MERGE (rel.s)-[r:`WORKS_AT`]->(rel.t)" not in query
    assert "CREATE" not in query
    knows = build_apoc_locked_upsert_query(_knows())
    assert "MATCH (t:`Person` {`personId`: row.target_key})" in knows
    assert "MERGE (s)-[r:`KNOWS`]->(t) SET r += props" in knows


def test_planned_query_streams_rows_into_merge_with_exact_pair_proof():
    """This exact query shape (explicit_locks=False, sort_rows=False) is
    also what P6-S4's finding fixed lease mode to use, since both are
    node-disjoint by construction and neither needs (or may safely use)
    the elementId reorder -- see
    test_build_edge_execution_lease_mode_drops_apoc_locks_and_primes's
    "ORDER BY elementId" assertion for the lease-mode call-site proof."""
    query = build_apoc_locked_upsert_query(_edge(), explicit_locks=False, sort_rows=False)
    assert query.count("MATCH (s:") == 1 and query.count("MATCH (t:") == 1
    assert "apoc.lock" not in query and "collect(" not in query and "ORDER BY" not in query
    assert "MERGE (s)-[r:`WORKS_AT`]->(t) SET r += row.properties" in query
    # The same checked_rows/matched_pairs proof, evaluated after the writes;
    # the client rolls back unless every row matched exactly one pair.
    assert query.index("MERGE") < query.index("count(DISTINCT row_index) AS checked_rows, count(*) AS matched_pairs")
    assert "(size($rows) - checked_rows) + (matched_pairs - checked_rows) AS invalid_rows" in query
    assert "matched_pairs AS merged_rows" in query
    assert "CREATE" not in query


def test_apoc_query_resolves_endpoints_once_and_guards_locks_on_validity():
    query = build_apoc_locked_upsert_query(_edge())
    # Each endpoint label is looked up exactly once, with non-locking plain MATCH.
    assert query.count("MATCH (s:") == 1 and query.count("MATCH (t:") == 1
    assert "OPTIONAL MATCH" not in query and "COLLECT {" not in query
    # Exactly-one-pair-per-row check: every row index matched and no surplus pairs.
    assert "count(DISTINCT row_index) AS checked_rows, count(*) AS matched_pairs" in query
    assert "(size($rows) - checked_rows) + (matched_pairs - checked_rows) AS invalid_rows" in query
    # Locking and MERGE happen only inside the invalid_rows = 0 guard, after it.
    guard = query.index("WHERE invalid_rows = 0")
    assert guard < query.index("apoc.lock.nodes") < query.index("MERGE")
    assert query.rstrip().endswith("RETURN checked_rows, invalid_rows, merged_rows")


def test_apoc_writer_runs_one_statement_then_commits():
    driver, tx = _apoc_tx()
    locked = _ok(2); order = []
    locked.consume.side_effect = lambda: order.append("locked")
    tx.commit.side_effect = lambda: order.append("commit")
    tx.run.side_effect = [locked]
    _apoc_writer(driver).write_batch([_record(), _record("p-2")])
    assert tx.run.call_count == 1
    assert len(tx.run.call_args.kwargs["rows"]) == 2
    assert "apoc.lock.nodes" in tx.run.call_args.args[0]
    locked.consume.assert_called_once_with(); tx.commit.assert_called_once_with()
    assert order == ["locked", "commit"]
    driver.close.assert_not_called()


@pytest.mark.parametrize("status", [
    [{"checked_rows": 1, "invalid_rows": 1, "merged_rows": 0}],
    [{"checked_rows": 0, "invalid_rows": 1, "merged_rows": 0}],
])
def test_apoc_writer_invalid_row_raises_and_rolls_back(status):
    driver, tx = _apoc_tx()
    result = MagicMock(); result.__iter__.return_value = iter(status); tx.run.return_value = result
    tx_cm = driver.session.return_value.__enter__.return_value.begin_transaction.return_value
    with pytest.raises(MissingRelationshipEndpointError):
        _apoc_writer(driver).write(_record())
    result.consume.assert_called_once_with()
    assert tx.run.call_count == 1; tx.commit.assert_not_called()
    # The transaction context exits with the error, i.e. the driver rolls back.
    exit_args = tx_cm.__exit__.call_args.args
    assert exit_args[0] is MissingRelationshipEndpointError


@pytest.mark.parametrize("status", [
    [],
    [{"checked_rows": 0, "invalid_rows": 0, "merged_rows": 0}],
    [{"checked_rows": 1, "invalid_rows": 0, "merged_rows": 0}],
    [{"checked_rows": 1, "invalid_rows": 0, "merged_rows": 1}, {"checked_rows": 1, "invalid_rows": 0, "merged_rows": 1}],
])
def test_apoc_writer_incomplete_or_nonaggregate_status_fails_closed(status):
    driver, tx = _apoc_tx()
    result = MagicMock(); result.__iter__.return_value = iter(status); tx.run.return_value = result
    with pytest.raises(MissingRelationshipEndpointError):
        _apoc_writer(driver).write(_record())
    assert tx.run.call_count == 1
    tx.commit.assert_not_called()


def test_apoc_writer_driver_failure_never_commits():
    driver, tx = _apoc_tx()
    tx.run.side_effect = [RuntimeError("apoc failed")]
    with pytest.raises(RuntimeError, match="apoc failed"):
        _apoc_writer(driver).write(_record())
    tx.commit.assert_not_called(); driver.close.assert_not_called()


def test_native_writer_preflights_then_checks_committed_status():
    driver = MagicMock(); first = driver.session.return_value.__enter__.return_value
    second = MagicMock(); driver.session.return_value.__enter__.side_effect = [first, second]
    preflight, native = MagicMock(), MagicMock(); preflight.__iter__.return_value = iter([{"checked_rows": 1, "invalid_rows": 0}])
    native.__iter__.return_value = iter([{"status": {"committed": True, "errorMessage": None}}])
    first.run.return_value = preflight; second.run.return_value = native
    writer = NativeDisjointEdgeWriter(driver, _edge(), ServerCapabilities("2026.06", True), missing_endpoint_error_factory=MissingRelationshipEndpointError)
    writer.write_batch([_record()])
    assert "ON ERROR CONTINUE REPORT STATUS AS status" in second.run.call_args.args[0]
    assert "CYPHER 25" in second.run.call_args.args[0]
    assert "DISJOINT BY (row.source_key, row.target_key)" in second.run.call_args.args[0]
    assert "RETURN 1 AS wrote" in second.run.call_args.args[0]
    assert second.run.call_args.kwargs["concurrency"] == 1
    assert second.run.call_args.kwargs["batch_size"] == 1000
    assert second.run.call_args.kwargs["rows"] == [{"source_key": "p-1", "target_key": "c-1", "properties": {"since": date(2020, 1, 2)}}]
    native.consume.assert_called_once_with()
    assert driver.session.return_value.__exit__.call_count == 2
    assert "MERGE (s)-[r:`KNOWS`]->(t)" in NativeDisjointEdgeWriter(driver, _knows(), ServerCapabilities("2026.06", True), missing_endpoint_error_factory=MissingRelationshipEndpointError)._query


def test_native_writer_refuses_unsupported_and_noncommitted_status():
    with pytest.raises(ExecutionModeError):
        NativeDisjointEdgeWriter(MagicMock(), _edge(), ServerCapabilities("5.21", False), missing_endpoint_error_factory=MissingRelationshipEndpointError)


@pytest.mark.parametrize("status", [{"committed": False, "errorMessage": None}, {"committed": True, "errorMessage": "boom"}])
def test_native_writer_failed_status_raises(status):
    driver = MagicMock(); first = MagicMock(); second = MagicMock()
    driver.session.return_value.__enter__.side_effect = [first, second]
    preflight, native = MagicMock(), MagicMock(); preflight.__iter__.return_value = iter([{"checked_rows": 1, "invalid_rows": 0}]); native.__iter__.return_value = iter([{"status": status}])
    first.run.return_value = preflight; second.run.return_value = native
    with pytest.raises(ExecutionModeError):
        NativeDisjointEdgeWriter(driver, _edge(), ServerCapabilities("2026", True), missing_endpoint_error_factory=MissingRelationshipEndpointError).write(_record())
    preflight.consume.assert_called_once_with(); native.consume.assert_called_once_with(); driver.close.assert_not_called()


def test_native_preflight_miss_skips_second_session():
    driver = MagicMock(); first = MagicMock(); driver.session.return_value.__enter__.return_value = first
    preflight = MagicMock(); preflight.__iter__.return_value = iter([{"checked_rows": 1, "invalid_rows": 1}]); first.run.return_value = preflight
    with pytest.raises(MissingRelationshipEndpointError):
        NativeDisjointEdgeWriter(driver, _edge(), ServerCapabilities("2026", True), missing_endpoint_error_factory=MissingRelationshipEndpointError).write(_record())
    preflight.consume.assert_called_once_with(); assert driver.session.call_count == 1


def test_native_runtime_error_propagates_and_write_delegates():
    writer = NativeDisjointEdgeWriter(MagicMock(), _edge(), ServerCapabilities("2026", True), missing_endpoint_error_factory=MissingRelationshipEndpointError)
    writer.write_batch = MagicMock()
    writer.write(_record())
    writer.write_batch.assert_called_once()
    driver = MagicMock(); first = MagicMock(); second = MagicMock()
    driver.session.return_value.__enter__.side_effect = [first, second]
    preflight = MagicMock(); preflight.__iter__.return_value = iter([{"checked_rows": 1, "invalid_rows": 0}]); first.run.return_value = preflight
    second.run.side_effect = RuntimeError("native boom")
    with pytest.raises(RuntimeError, match="native boom"):
        NativeDisjointEdgeWriter(driver, _edge(), ServerCapabilities("2026", True), missing_endpoint_error_factory=MissingRelationshipEndpointError).write(_record())
    driver.close.assert_not_called()


def test_native_empty_status_fails_closed():
    driver = MagicMock(); first = MagicMock(); second = MagicMock()
    driver.session.return_value.__enter__.side_effect = [first, second]
    preflight, native = MagicMock(), MagicMock(); preflight.__iter__.return_value = iter([{"checked_rows": 1, "invalid_rows": 0}]); native.__iter__.return_value = iter([])
    first.run.return_value = preflight; second.run.return_value = native
    with pytest.raises(ExecutionModeError, match="no batch status"):
        NativeDisjointEdgeWriter(driver, _edge(), ServerCapabilities("2026", True), missing_endpoint_error_factory=MissingRelationshipEndpointError).write(_record())


def test_python_coordinator_preserves_fifo_chunks_within_lane():
    # row_level_dlq_enabled=False: a bare MagicMock() auto-creates every
    # attribute (truthy), which would otherwise make the coordinator's
    # getattr(writer, "row_level_dlq_enabled", False) check misfire.
    writer = MagicMock(row_level_dlq_enabled=False)
    routed = lambda offset: RoutedEdgeRecord(_record(), "works-at-events", 0, offset, 1, "forward", 0, 0)
    result = LaneExecutionCoordinator(writer, mode="python_apoc", worker_count=2).execute([(routed(1),), (routed(2),)])
    assert result[0].success is True
    assert [call.args[0][0].source_key for call in writer.write_batch.call_args_list] == ["p-1", "p-1"]
    assert writer.write_batch.call_count == 2


def test_native_coordinator_uses_one_flattened_call():
    writer = MagicMock(); routed = lambda lane, offset: RoutedEdgeRecord(_record(), "works-at-events", 0, offset, lane, "forward", 0, 0)
    result = LaneExecutionCoordinator(writer, mode="native_disjoint", worker_count=9).execute([(routed(2, 2),), (routed(1, 1),)])
    assert result[0].lane_id is None and result[0].success
    assert len(writer.write_batch.call_args.args[0]) == 2


def test_python_coordinator_uses_configured_workers_per_distinct_lane():
    captured = {}
    class Executor:
        def __init__(self, *, max_workers): captured["workers"] = max_workers
        def __enter__(self): return self
        def __exit__(self, *_): return None
        def submit(self, func, *args):
            class Future:
                def result(self): return func(*args)
            return Future()
    writer = MagicMock(row_level_dlq_enabled=False)
    routed = lambda lane, offset: RoutedEdgeRecord(_record(), "t", 0, offset, lane, "forward", 0, 0)
    LaneExecutionCoordinator(writer, mode="python_apoc", worker_count=3, executor_factory=Executor).execute([(routed(1,1),), (routed(1,2),), (routed(2,3),)])
    assert captured["workers"] == 3


def test_python_coordinator_writes_four_lanes_concurrently():
    barrier = Barrier(4)
    worker_threads = set()

    class Writer:
        def write_batch(self, records):
            worker_threads.add(get_ident())
            barrier.wait(timeout=3)

    routed = lambda lane: RoutedEdgeRecord(_record(f"p-{lane}"), "t", lane, 0, lane, "forward", lane, 0)
    results = LaneExecutionCoordinator(Writer(), mode="python_apoc", worker_count=4).execute(
        [(routed(lane),) for lane in range(4)]
    )
    assert len(worker_threads) == 4
    assert len(results) == 4
    assert all(result.success for result in results)


def test_build_execution_refuses_native_before_constructing_components(monkeypatch):
    edge = _edge(); edge.execution.mode = "native_disjoint"
    monkeypatch.setattr("src.loader.edge_execution.probe_server_capabilities", lambda _driver: ServerCapabilities("5.21", False))
    with pytest.raises(ExecutionModeError):
        build_edge_execution(edge, MagicMock())


def test_build_native_maps_neo4j_526_cypher25_rejection_before_constructing_writer(monkeypatch):
    edge = _edge(); edge.execution.mode = "native_disjoint"
    driver = MagicMock(); driver.get_server_info.return_value.agent = "Neo4j/5.26.30"
    session = driver.session.return_value.__enter__.return_value
    session.run.side_effect = ClientError._hydrate_neo4j(
        code="Neo.ClientError.Statement.ArgumentError",
        message="25 is not a valid option for cypher version. Valid options are: 5",
    )
    native_writer = MagicMock()
    monkeypatch.setattr("src.loader.edge_execution.NativeDisjointEdgeWriter", native_writer)
    with pytest.raises(ExecutionModeError, match="native_disjoint.*5.26.30"):
        build_edge_execution(edge, driver)
    native_writer.assert_not_called()


def test_build_default_apoc_execution_does_not_probe_cypher_25():
    driver = MagicMock()
    edge = _edge()
    assert edge.execution.mode == "python_apoc"
    writer, _partitioner, _batcher, coordinator = build_edge_execution(edge, driver)
    assert isinstance(writer, ApocLockedEdgeWriter)
    assert coordinator._mode == "python_apoc"
    driver.get_server_info.assert_not_called()
    driver.session.assert_not_called()


# ── P5-S3 Story 5: classic bulk opt-in ─────────────────────────────────────


def test_build_edge_execution_rejects_unknown_mode():
    with pytest.raises(ValueError, match="mode must be 'bulk' or 'stream'"):
        build_edge_execution(_edge(), MagicMock(), mode="streaming")


def test_build_edge_execution_classic_bulk_opt_in_activates_row_level_dlq():
    edge = _edge().model_copy(update={
        "dead_letter": DeadLetterConfig(topic="dlq-works-at", enabled=True, route_missing_endpoints_in_bulk=True),
    })
    writer, _p, _b, coordinator = build_edge_execution(
        edge, MagicMock(), mode="bulk", missing_endpoint_sink=MagicMock(),
    )
    assert writer.row_level_dlq_enabled is True
    assert isinstance(coordinator, LaneExecutionCoordinator)


def test_build_edge_execution_classic_bulk_without_opt_in_stays_fail_closed():
    """Identical schema, opt-in unset: today's exact fail-closed behavior,
    unchanged -- a regression pin against the pre-slice fixture shape."""
    edge = _edge().model_copy(update={"dead_letter": DeadLetterConfig(topic="dlq-works-at", enabled=True)})
    writer, _p, _b, _c = build_edge_execution(edge, MagicMock(), mode="bulk", missing_endpoint_sink=MagicMock())
    assert writer.row_level_dlq_enabled is False


def test_build_edge_execution_classic_stream_never_activates_even_with_opt_in():
    """Decisions point 2: mode='stream' reaching the CLASSIC branch only
    happens via the deprecated --stream-clock-rotation path (no lease env
    var set) -- row-level DLQ must stay off there even with the opt-in,
    matching P5-S1's 'byte-for-byte unchanged' contract for that path."""
    edge = _edge().model_copy(update={
        "dead_letter": DeadLetterConfig(topic="dlq-works-at", enabled=True, route_missing_endpoints_in_bulk=True),
    })
    writer, _p, _b, _c = build_edge_execution(edge, MagicMock(), mode="stream", missing_endpoint_sink=MagicMock())
    assert writer.row_level_dlq_enabled is False


def test_build_edge_execution_classic_default_mode_is_bulk_no_opt_in_by_default():
    """Every pre-P5-S3 call site (mode= omitted) keeps zero behavior change."""
    edge = _edge().model_copy(update={"dead_letter": DeadLetterConfig(topic="dlq-works-at", enabled=True)})
    writer, _p, _b, _c = build_edge_execution(edge, MagicMock(), missing_endpoint_sink=MagicMock())
    assert writer.row_level_dlq_enabled is False


def test_lane_coordinator_same_pair_ordering_holds_across_sequential_lane_batches():
    """MUST-FIX parity with Story 4: LaneBatcher.drain_all() can split one
    lane into several sequential batches when it holds more rows than
    batch_size; a pair always routes to the same lane, so the same
    cross-transaction parking guard is required here too."""
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    tx = session.begin_transaction.return_value.__enter__.return_value
    tx.run.side_effect = [
        MagicMock(__iter__=lambda self: iter([{"checked_rows": 1, "invalid_rows": 1, "merged_rows": 0}])),
        MagicMock(__iter__=lambda self: iter([{"row_index": 0, "source_matches": 0, "target_matches": 1}])),
        MagicMock(__iter__=lambda self: iter([{"row_index": 0, "source_matches": 1, "target_matches": 1}])),
    ]
    sink = MagicMock()
    writer = ApocLockedEdgeWriter(
        driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError,
        missing_endpoint_sink=sink,
    )
    same_lane_batch = lambda offset: RoutedEdgeRecord(
        EdgeRecord("mixed-a", "mixed-b", {"seq": offset}), "works-at-events", 0, offset, 1, "forward", 0, 0,
    )
    result = LaneExecutionCoordinator(writer, mode="python_apoc", worker_count=1).execute(
        [(same_lane_batch(0),), (same_lane_batch(1),)],
    )
    assert result[0].success is True
    assert tx.run.call_count == 3  # never a 4th call (a matched-subset write for the 2nd row)
    assert sink.route_unresolved_endpoint.call_count == 2
    by_offset = {c.kwargs["offset"]: c.kwargs["reason_code"] for c in sink.route_unresolved_endpoint.call_args_list}
    assert by_offset[0] == "missing_source"
    assert by_offset[1] == "blocked_by_earlier_unresolved_row"


def _transient_writer(tx_runs, *, attempts=3):
    """Return an APOC writer whose transactions replay ``tx_runs`` in order."""
    driver = MagicMock(); session = driver.session.return_value.__enter__.return_value
    tx = session.begin_transaction.return_value.__enter__.return_value
    tx.run.side_effect = tx_runs
    edge = _edge().model_copy(update={"retry": _edge().retry.model_copy(update={
        "max_attempts": attempts, "base_delay_seconds": 0.5, "max_delay_seconds": 0.75,
    })})
    sleeps = []
    writer = ApocLockedEdgeWriter(
        driver, edge, missing_endpoint_error_factory=MissingRelationshipEndpointError, sleep=sleeps.append,
    )
    return writer, tx, sleeps


def test_apoc_writer_replays_whole_transaction_after_transient_deadlock():
    deadlock = TransientError("Neo.TransientError.Transaction.DeadlockDetected")
    writer, tx, sleeps = _transient_writer([deadlock, _ok()])
    writer.write_batch([_record()])
    assert tx.run.call_count == 2
    assert tx.run.call_args_list[0].kwargs["rows"] == tx.run.call_args_list[1].kwargs["rows"]
    tx.commit.assert_called_once_with()
    assert sleeps == [0.5]


def test_apoc_writer_transient_retries_are_bounded_with_capped_backoff():
    writer, tx, sleeps = _transient_writer([TransientError("busy")] * 3, attempts=3)
    with pytest.raises(TransientError):
        writer.write_batch([_record()])
    assert tx.run.call_count == 3 and sleeps == [0.5, 0.75]
    tx.commit.assert_not_called()


def test_apoc_writer_does_not_retry_nonretryable_transient_error(monkeypatch):
    error = TransientError("terminated")
    monkeypatch.setattr(error, "is_retryable", lambda: False)
    writer, tx, sleeps = _transient_writer([error])
    with pytest.raises(TransientError):
        writer.write_batch([_record()])
    assert tx.run.call_count == 1 and sleeps == []


def test_apoc_writer_never_retries_missing_endpoint_or_client_errors():
    missing = MagicMock(); missing.__iter__.return_value = iter([{"checked_rows": 1, "invalid_rows": 1, "merged_rows": 0}])
    writer, tx, sleeps = _transient_writer([missing])
    with pytest.raises(MissingRelationshipEndpointError):
        writer.write_batch([_record()])
    assert tx.run.call_count == 1 and sleeps == []
    writer, tx, sleeps = _transient_writer([ClientError("bad")])
    with pytest.raises(ClientError):
        writer.write_batch([_record()])
    assert tx.run.call_count == 1 and sleeps == []
    tx.commit.assert_not_called()


# ── P5-S1 Story 4: Neo4j tx timeout + lease-liveness abort-not-commit ───────


def _apoc_tx_with_session():
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    tx = session.begin_transaction.return_value.__enter__.return_value
    return driver, session, tx


def test_classic_mode_passes_no_timeout_kwarg_at_all():
    """Classic (no lease, no plan) mode has no lease-server-restart
    exposure to fence against; begin_transaction() must be called with no
    ``timeout`` kwarg at all, unbounded/server-default, unchanged."""
    driver, session, tx = _apoc_tx_with_session()
    tx.run.return_value = _ok()
    _apoc_writer(driver).write(_record())
    assert "timeout" not in session.begin_transaction.call_args.kwargs


def test_flat_tx_timeout_seconds_applied_on_every_attempt_without_a_deadline():
    """Planned mode has no lease grant (no deadline); it gets a flat T
    per attempt via the constructor, per ApocLockedEdgeWriter's docstring."""
    driver, session, tx = _apoc_tx_with_session()
    tx.run.return_value = _ok()
    writer = ApocLockedEdgeWriter(
        driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError,
        tx_timeout_seconds=30.0,
    )
    writer.write(_record())
    assert session.begin_transaction.call_args.kwargs["timeout"] == 30.0


def test_deadline_derived_timeout_shrinks_and_is_shared_across_retries():
    """Review finding D1: a retried write must NOT get a fresh T -- every
    attempt shares the same absolute deadline (grant time + T), so the
    per-attempt timeout passed to Neo4j shrinks attempt over attempt."""
    driver, session, tx = _apoc_tx_with_session()
    deadlock = TransientError("Neo.TransientError.Transaction.DeadlockDetected")
    tx.run.side_effect = [deadlock, _ok()]
    edge = _edge().model_copy(update={"retry": _edge().retry.model_copy(update={
        "max_attempts": 3, "base_delay_seconds": 0.001, "max_delay_seconds": 0.001,
    })})
    # Fake clock: grant at t=100.0, first attempt's remaining=20, second
    # attempt's remaining=15 (5s "elapsed" during the failed attempt+retry
    # sleep) -- strictly less, never reset to a fresh 20. Each timeout is the
    # fenced budget: remaining minus the monitor slack and BEGIN allowance (4 s).
    now = [100.0]
    writer = ApocLockedEdgeWriter(
        driver, edge, missing_endpoint_error_factory=MissingRelationshipEndpointError,
        sleep=lambda _s: now.__setitem__(0, now[0] + 5.0), clock=lambda: now[0],
    )
    writer.write_rows([{"source_key": "p-1", "target_key": "c-1", "properties": {}}], deadline=100.0 + 20.0)
    timeouts = [call.kwargs["timeout"] for call in session.begin_transaction.call_args_list]
    assert timeouts == [16.0, 11.0]
    assert timeouts[1] < timeouts[0]


def test_deadline_exceeded_before_attempt_aborts_without_opening_a_transaction():
    driver, session, _tx = _apoc_tx_with_session()
    clock_values = iter([100.0])
    writer = ApocLockedEdgeWriter(
        driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError,
        clock=lambda: next(clock_values),
    )
    # Deadline already within the floor (< 1s remaining): abort before
    # ever calling begin_transaction, let alone opening a Neo4j transaction.
    with pytest.raises(LeaseCoordinatorError, match="lease deadline exceeded"):
        writer.write_rows([{"source_key": "p-1", "target_key": "c-1", "properties": {}}], deadline=100.5)
    session.begin_transaction.assert_not_called()


def test_lease_liveness_check_failure_aborts_before_commit_never_raises_past_write_rows_as_lease_error():
    """AC: a cell write whose lease connection's liveness check fails
    immediately before the scheduled commit rolls back (no Neo4j write
    persists) and reports a failure -- never commits."""
    driver, session, tx = _apoc_tx_with_session()
    tx.run.return_value = _ok()
    tx_cm = session.begin_transaction.return_value
    writer = _apoc_writer(driver)
    with pytest.raises(LeaseCoordinatorError, match="lease connection broke before commit"):
        writer.write(_record(), lease_liveness_check=lambda: False)
    tx.commit.assert_not_called()
    exit_args = tx_cm.__exit__.call_args.args
    assert exit_args[0] is LeaseCoordinatorError


def test_lease_liveness_check_success_commits_exactly_like_today():
    driver, session, tx = _apoc_tx_with_session()
    tx.run.return_value = _ok()
    writer = _apoc_writer(driver)
    calls = {"count": 0}

    def liveness_check():
        calls["count"] += 1
        return True

    writer.write(_record(), lease_liveness_check=liveness_check)
    tx.commit.assert_called_once_with()
    assert calls["count"] == 1


def test_ping_never_raises_and_reflects_request_outcome():
    """The client's ping() is unit tested independent of the writer:
    success, LeaseCoordinatorError caught and turned into False, never
    raises to the caller."""
    from src.loader.lease_client import BucketLeaseClient

    class _OkSocket:
        def setsockopt(self, *_a, **_k):
            pass

        def sendall(self, _data):
            pass

        def makefile(self, _mode):
            class _R:
                def readline(self_inner):
                    return b"OK\n"
            return _R()

        def settimeout(self, *_a, **_k):
            pass

    client = BucketLeaseClient("fake:1", connect=lambda *_a, **_k: _OkSocket())
    assert client.ping() is True

    class _DeadSocket:
        def setsockopt(self, *_a, **_k):
            pass

        def sendall(self, _data):
            raise OSError("connection reset")

        def makefile(self, _mode):
            class _R:
                def readline(self_inner):
                    return b""

                def close(self_inner):
                    pass
            return _R()

        def settimeout(self, *_a, **_k):
            pass

        def close(self):
            pass

    broken_client = BucketLeaseClient("fake:1", connect=lambda *_a, **_k: _DeadSocket())
    assert broken_client.ping() is False
    assert broken_client.broken is True


# ── Metrics instrumentation (P6-S2 Story 5) ──────────────────────────────────


def _transient_writer_with_metrics(tx_runs, *, attempts=3, metrics=None, kind="edge"):
    driver = MagicMock(); session = driver.session.return_value.__enter__.return_value
    tx = session.begin_transaction.return_value.__enter__.return_value
    tx.run.side_effect = tx_runs
    edge = _edge().model_copy(update={"retry": _edge().retry.model_copy(update={
        "max_attempts": attempts, "base_delay_seconds": 0.001, "max_delay_seconds": 0.001,
    })})
    writer = ApocLockedEdgeWriter(
        driver, edge, missing_endpoint_error_factory=MissingRelationshipEndpointError,
        sleep=lambda _s: None, metrics=metrics, kind=kind,
    )
    return writer, tx


def test_deadlock_retried_then_successful_increments_deadlocks_recovered_once():
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    deadlock = TransientError("Neo.TransientError.Transaction.DeadlockDetected")
    writer, tx = _transient_writer_with_metrics([deadlock, _ok()], metrics=metrics)
    writer.write_batch([_record()])
    assert metrics.registry.get_sample_value(
        "graph_loader_deadlocks_recovered_total", {"kind": "edge", "name": "WORKS_AT"}
    ) == 1.0


def test_two_retries_then_success_increments_deadlocks_recovered_by_one_not_by_attempt_count():
    """P6-S2 review finding N-c: one retry can't distinguish 'count once'
    from 'count per retry'; two retries before success can."""
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    deadlock = TransientError("Neo.TransientError.Transaction.DeadlockDetected")
    writer, tx = _transient_writer_with_metrics([deadlock, deadlock, _ok()], attempts=3, metrics=metrics)
    writer.write_batch([_record()])
    assert tx.run.call_count == 3
    assert metrics.registry.get_sample_value(
        "graph_loader_deadlocks_recovered_total", {"kind": "edge", "name": "WORKS_AT"}
    ) == 1.0


def test_batch_without_transient_error_never_increments_deadlocks_recovered():
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    writer, tx = _transient_writer_with_metrics([_ok()], metrics=metrics)
    writer.write_batch([_record()])
    assert metrics.registry.get_sample_value(
        "graph_loader_deadlocks_recovered_total", {"kind": "edge", "name": "WORKS_AT"}
    ) is None


def test_exhausted_retry_never_increments_deadlocks_recovered():
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    writer, tx = _transient_writer_with_metrics([TransientError("busy")] * 3, attempts=3, metrics=metrics)
    with pytest.raises(TransientError):
        writer.write_batch([_record()])
    assert metrics.registry.get_sample_value(
        "graph_loader_deadlocks_recovered_total", {"kind": "edge", "name": "WORKS_AT"}
    ) is None


def test_planned_writer_kind_keeps_deadlocks_recovered_at_zero_construction():
    """Planned mode's writer is constructed with kind='planned-writer'
    (Story 6); confirm the label reflects that kind, still only
    incrementing on a real recovered deadlock (structurally never expected
    to happen in planned mode's disjoint-resource design)."""
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    writer, tx = _transient_writer_with_metrics([_ok()], metrics=metrics, kind="planned-writer")
    writer.write_batch([_record()])
    assert metrics.registry.get_sample_value(
        "graph_loader_deadlocks_recovered_total", {"kind": "planned-writer", "name": "WORKS_AT"}
    ) is None
    assert metrics.registry.get_sample_value(
        "graph_loader_deadlocks_recovered_total", {"kind": "edge", "name": "WORKS_AT"}
    ) is None


def test_apoc_writer_metrics_failure_never_blocks_a_successful_write(monkeypatch):
    """P6-S2 review finding S4-1's pattern, applied to Story 5: a metrics
    exception must never turn an already-successful write into a raised
    error."""
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    deadlock = TransientError("Neo.TransientError.Transaction.DeadlockDetected")
    writer, tx = _transient_writer_with_metrics([deadlock, _ok()], metrics=metrics)

    def boom(**_kwargs):
        raise ValueError("simulated metrics failure")

    monkeypatch.setattr(metrics.deadlocks_recovered, "labels", boom)
    # Must not raise despite the metrics call failing.
    writer.write_batch([_record()])
    assert tx.commit.call_count == 1


# ── P5-S3 Story 3: row-level missing-endpoint DLQ ──────────────────────────


def test_row_level_dlq_enabled_reflects_sink_presence():
    driver, _tx = _apoc_tx()
    assert ApocLockedEdgeWriter(
        driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError,
    ).row_level_dlq_enabled is False
    assert ApocLockedEdgeWriter(
        driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError,
        missing_endpoint_sink=MagicMock(),
    ).row_level_dlq_enabled is True


def test_write_provenanced_batch_all_resolved_costs_exactly_one_identical_attempt():
    """All-resolved batch: Attempt 1 only, identical query/params to write_rows
    today, zero DLQ envelopes, zero classification round trip."""
    driver, session, tx = _apoc_tx_with_session()
    tx.run.return_value = _ok(1)
    sink = MagicMock()
    writer = ApocLockedEdgeWriter(
        driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError,
        missing_endpoint_sink=sink,
    )
    item = _pending(_record("p-1"), offset=10)
    writer.write_provenanced_batch([item])
    assert tx.run.call_count == 1
    call = tx.run.call_args_list[0]
    assert call.args[0] is writer._query
    assert call.kwargs["rows"] == [{"source_key": "p-1", "target_key": "c-1", "properties": {"since": date(2020, 1, 2)}}]
    tx.commit.assert_called_once_with()
    sink.route_unresolved_endpoint.assert_not_called()


def test_write_provenanced_batch_empty_items_is_a_noop():
    driver, _tx = _apoc_tx()
    writer = ApocLockedEdgeWriter(
        driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError,
        missing_endpoint_sink=MagicMock(),
    )
    writer.write_provenanced_batch([])  # must not touch the driver at all
    driver.session.assert_not_called()


def test_write_provenanced_batch_mixed_batch_commits_matched_and_dlqs_the_rest():
    """[R1(missing source), R2(matched), R3(ambiguous source)], no shared pairs."""
    driver, session, tx = _apoc_tx_with_session()
    tx.run.side_effect = [
        _invalid(checked_rows=3, invalid_rows=2, merged_rows=0),  # attempt 1: aggregate fails
        _classification([(0, 0, 1), (1, 1, 1), (2, 2, 1)]),        # classification
        _ok(1),                                                    # matched-subset write (R2 only)
    ]
    sink = MagicMock()
    writer = ApocLockedEdgeWriter(
        driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError,
        missing_endpoint_sink=sink,
    )
    items = [
        _pending(EdgeRecord("missing-src", "c-1", {}), offset=10),
        _pending(EdgeRecord("p-2", "c-2", {}), offset=11),
        _pending(EdgeRecord("ambiguous-src", "c-3", {}), offset=12),
    ]
    writer.write_provenanced_batch(items)
    assert tx.run.call_count == 3
    matched_write_call = tx.run.call_args_list[2]
    assert matched_write_call.kwargs["rows"] == [
        {"source_key": "p-2", "target_key": "c-2", "properties": {}},
    ]
    tx.commit.assert_called_once_with()
    assert sink.route_unresolved_endpoint.call_count == 2
    by_offset = {c.kwargs["offset"]: c.kwargs for c in sink.route_unresolved_endpoint.call_args_list}
    assert by_offset[10]["failure_class"] == "missing_endpoint"
    assert by_offset[10]["reason_code"] == "missing_source"
    assert by_offset[12]["failure_class"] == "ambiguous_endpoint"
    assert by_offset[12]["reason_code"] == "ambiguous_source"
    assert by_offset[10]["topic"] == "works-at-events" and by_offset[10]["partition"] == 0
    assert by_offset[10]["source_key_property"] == "personId"
    assert by_offset[10]["target_key_property"] == "companyId"


def test_write_provenanced_batch_same_pair_ordering_parks_both_rows_and_writes_neither():
    """R1(missing) then R2(individually resolvable), same (source,target) pair:
    both parked, matched-subset write is skipped entirely (empty)."""
    driver, session, tx = _apoc_tx_with_session()
    tx.run.side_effect = [
        _invalid(checked_rows=2, invalid_rows=1, merged_rows=0),  # attempt 1 fails
        _classification([(0, 0, 1), (1, 1, 1)]),                   # R1 missing_source, R2 matched
    ]
    sink = MagicMock()
    writer = ApocLockedEdgeWriter(
        driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError,
        missing_endpoint_sink=sink,
    )
    items = [
        _pending(EdgeRecord("missing-src", "c-1", {"role": "old"}), offset=20),
        _pending(EdgeRecord("missing-src", "c-1", {"role": "new"}), offset=21),
    ]
    writer.write_provenanced_batch(items)
    # No third tx.run call: matched_rows was empty, so the write query never ran.
    assert tx.run.call_count == 2
    tx.commit.assert_called_once_with()
    assert sink.route_unresolved_endpoint.call_count == 2
    by_offset = {c.kwargs["offset"]: c.kwargs for c in sink.route_unresolved_endpoint.call_args_list}
    assert by_offset[20]["reason_code"] == "missing_source"
    assert by_offset[21]["reason_code"] == "blocked_by_earlier_unresolved_row"
    assert by_offset[21]["failure_class"] == "missing_endpoint"


def test_write_provenanced_batch_toctou_mismatch_fails_whole_batch_closed():
    """Classification said matched, but the matched-subset write's own
    aggregate check disagrees moments later -- fail EVERYTHING closed."""
    driver, session, tx = _apoc_tx_with_session()
    tx.run.side_effect = [
        _invalid(checked_rows=1, invalid_rows=1, merged_rows=0),
        _classification([(0, 1, 1)]),          # classified matched
        _invalid(checked_rows=1, invalid_rows=1, merged_rows=0),  # but the write disagrees
    ]
    sink = MagicMock()
    writer = ApocLockedEdgeWriter(
        driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError,
        missing_endpoint_sink=sink,
    )
    items = [_pending(EdgeRecord("p-1", "c-1", {}), offset=30)]
    with pytest.raises(MissingRelationshipEndpointError, match="matched-subset write mismatch"):
        writer.write_provenanced_batch(items)
    tx.commit.assert_not_called()
    sink.route_unresolved_endpoint.assert_not_called()


def test_write_provenanced_batch_transient_error_on_attempt_1_retries_without_classifying():
    driver, session, tx = _apoc_tx_with_session()
    deadlock = TransientError("Neo.TransientError.Transaction.DeadlockDetected")
    tx.run.side_effect = [deadlock, _ok(1)]
    sink = MagicMock()
    edge = _edge().model_copy(update={"retry": _edge().retry.model_copy(update={
        "max_attempts": 3, "base_delay_seconds": 0.001, "max_delay_seconds": 0.001,
    })})
    writer = ApocLockedEdgeWriter(
        driver, edge, missing_endpoint_error_factory=MissingRelationshipEndpointError,
        missing_endpoint_sink=sink, sleep=lambda _s: None,
    )
    items = [_pending(_record("p-1"), offset=40)]
    writer.write_provenanced_batch(items)
    assert tx.run.call_count == 2  # deadlock attempt + successful retry -- never a classification call
    tx.commit.assert_called_once_with()
    sink.route_unresolved_endpoint.assert_not_called()


def test_write_provenanced_batch_lease_loss_during_fallback_raises_lease_error_not_dlq():
    """Decisions point 7: a lease-loss abort during the fallback's own
    commit must stay LeaseCoordinatorError, never misclassified as a DLQ
    condition -- zero envelopes published."""
    driver, session, tx = _apoc_tx_with_session()
    tx.run.side_effect = [
        _invalid(checked_rows=1, invalid_rows=1, merged_rows=0),
        _classification([(0, 1, 1)]),
        _ok(1),
    ]
    sink = MagicMock()
    writer = ApocLockedEdgeWriter(
        driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError,
        missing_endpoint_sink=sink,
    )
    items = [_pending(EdgeRecord("p-1", "c-1", {}), offset=50)]
    with pytest.raises(LeaseCoordinatorError, match="lease connection broke before commit"):
        writer.write_provenanced_batch(items, lease_liveness_check=lambda: False)
    tx.commit.assert_not_called()
    sink.route_unresolved_endpoint.assert_not_called()


# ── P5-S4 Story 3's second half: IndexedMissingEndpointSink, _LeaseFlush's
# durable_index fallback (integrating through the existing ParkedPairs
# protocol rather than a parallel mechanism) ────────────────────────────────


def test_indexed_missing_endpoint_sink_records_before_delegating():
    from src.loader.edge_execution import IndexedMissingEndpointSink

    order = []
    index = MagicMock()
    index.add.side_effect = lambda *a, **k: order.append("add")
    sink = MagicMock()
    sink.route_unresolved_endpoint.side_effect = lambda **k: order.append("route")
    wrapped = IndexedMissingEndpointSink(sink, index, clock=lambda: 1_700_000_000_000)
    record = EdgeRecord("p-1", "c-1", {"role": "eng"})

    wrapped.route_unresolved_endpoint(
        topic="works-at-events", partition=2, offset=17, record=record, edge_type="WORKS_AT",
        source_key_property="personId", target_key_property="companyId",
        failure_class="missing_endpoint", reason_code="missing_source",
        error_text="x", retry_after_seconds=1.0,
    )

    assert order == ["add", "route"]  # add() before the DLQ publish, always
    index.add.assert_called_once_with(
        "p-1", "c-1", original_topic="works-at-events", original_partition=2,
        original_offset=17, first_failure_at_ms=1_700_000_000_000,
    )
    sink.route_unresolved_endpoint.assert_called_once_with(
        topic="works-at-events", partition=2, offset=17, record=record, edge_type="WORKS_AT",
        source_key_property="personId", target_key_property="companyId",
        failure_class="missing_endpoint", reason_code="missing_source",
        error_text="x", retry_after_seconds=1.0,
    )


def test_indexed_missing_endpoint_sink_never_publishes_when_add_raises():
    """add() failing (an unconfirmed publish, ParkedPairIndex's own
    fail-closed contract) must propagate uncaught, exactly like a DLQ
    publish failure already does -- the wrapped sink's own publish must
    never be attempted."""
    from src.loader.edge_execution import IndexedMissingEndpointSink

    index = MagicMock()
    index.add.side_effect = RuntimeError("simulated unconfirmed publish")
    sink = MagicMock()
    wrapped = IndexedMissingEndpointSink(sink, index)
    record = EdgeRecord("p-1", "c-1", {})

    with pytest.raises(RuntimeError, match="simulated unconfirmed publish"):
        wrapped.route_unresolved_endpoint(
            topic="t", partition=0, offset=1, record=record, edge_type="WORKS_AT",
            source_key_property="personId", target_key_property="companyId",
            failure_class="missing_endpoint", reason_code="missing_source",
            error_text="x", retry_after_seconds=1.0,
        )
    sink.route_unresolved_endpoint.assert_not_called()


def test_lease_flush_is_parked_checks_durable_index_as_fallback():
    from threading import Lock as _Lock

    from src.loader.edge_execution import _LeaseFlush

    index = MagicMock()
    index.is_blocked.return_value = True
    state = _LeaseFlush(
        cells={}, totals={}, resources={}, by_resource={}, nonempty=set(),
        lock=_Lock(), written={}, busy=set(), durable_index=index,
    )
    assert state.is_parked(("a", "b")) is True
    index.is_blocked.assert_called_once_with("a", "b")


def test_lease_flush_is_parked_prefers_local_set_without_consulting_index():
    from threading import Lock as _Lock

    from src.loader.edge_execution import _LeaseFlush

    index = MagicMock()
    state = _LeaseFlush(
        cells={}, totals={}, resources={}, by_resource={}, nonempty=set(),
        lock=_Lock(), written={}, busy=set(), durable_index=index,
    )
    state.mark_parked([("a", "b")])
    assert state.is_parked(("a", "b")) is True
    index.is_blocked.assert_not_called()  # already parked locally -- no need to ask the index


def test_lease_flush_is_parked_false_and_no_crash_without_a_durable_index():
    from threading import Lock as _Lock

    from src.loader.edge_execution import _LeaseFlush

    state = _LeaseFlush(
        cells={}, totals={}, resources={}, by_resource={}, nonempty=set(),
        lock=_Lock(), written={}, busy=set(),  # durable_index defaults to None
    )
    assert state.is_parked(("a", "b")) is False


# ── P5-S4 Story 4: order_by conditional SET ──────────────────────────────────


def _edge_with_order_by(prop="since"):
    return _edge().model_copy(update={"order_by": prop})


def test_order_by_query_text_unchanged_for_a_non_order_by_edge_type():
    """Regression pin: order_by=None must produce the byte-for-byte same
    query as before this story, for every shape."""
    for kwargs in ({}, {"explicit_locks": False, "sort_rows": False}, {"explicit_locks": False}):
        without = build_apoc_locked_upsert_query(_edge(), **kwargs)
        with_none = build_apoc_locked_upsert_query(_edge(), **kwargs)
        assert without == with_none
    assert "should_apply_order_by" not in build_apoc_locked_upsert_query(_edge())
    assert "should_apply_order_by" not in build_apoc_locked_upsert_query(_edge(), explicit_locks=False, sort_rows=False)


def test_order_by_lease_shape_query_has_conditional_set_and_skip_count():
    query = build_apoc_locked_upsert_query(_edge_with_order_by(), explicit_locks=False, sort_rows=False)
    assert "MERGE (s)-[r:`WORKS_AT`]->(t)\n" in query
    assert "SET r += row.properties" not in query.split("MERGE")[1].split("FOREACH")[0]  # not unconditional
    assert (
        "(row.properties[$order_by] IS NULL OR r[$order_by] IS NULL OR row.properties[$order_by] > r[$order_by])"
        in query
    )
    assert "FOREACH (_x_ IN CASE WHEN should_apply_order_by THEN [1] ELSE [] END | SET r += row.properties)" in query
    assert "sum(CASE WHEN should_apply_order_by THEN 0 ELSE 1 END) AS skipped_as_not_newer_rows" in query
    assert query.rstrip().endswith(
        "RETURN checked_rows, (size($rows) - checked_rows) + (matched_pairs - checked_rows) AS invalid_rows, "
        "matched_pairs AS merged_rows, skipped_as_not_newer_rows"
    )


def test_order_by_apoc_lock_shape_query_has_conditional_set_and_skip_count():
    query = build_apoc_locked_upsert_query(_edge_with_order_by())
    assert "MERGE (s)-[r:`WORKS_AT`]->(t)\n" in query
    assert (
        "(props[$order_by] IS NULL OR r[$order_by] IS NULL OR props[$order_by] > r[$order_by])"
        in query
    )
    assert "FOREACH (_x_ IN CASE WHEN should_apply_order_by THEN [1] ELSE [] END | SET r += props)" in query
    assert "RETURN count(*) AS merged_rows, sum(CASE WHEN should_apply_order_by THEN 0 ELSE 1 END) AS skipped_as_not_newer_rows" in query
    assert query.rstrip().endswith("RETURN checked_rows, invalid_rows, merged_rows, skipped_as_not_newer_rows")


def test_classify_rows_order_by_configured_skips_the_blocking_pass_entirely():
    from src.loader.edge_execution import ApocLockedEdgeWriter

    rows = [
        {"source_key": "missing-src", "target_key": "c-1", "properties": {}},
        {"source_key": "missing-src", "target_key": "c-1", "properties": {}},  # same pair, resolvable
    ]
    by_index = {0: (0, 1), 1: (1, 1)}  # row 0 missing_source, row 1 matched
    statuses = ApocLockedEdgeWriter._classify_rows(rows, by_index, order_by_configured=True)
    assert statuses[0][0] == "missing"
    assert statuses[1][0] == "matched"  # never forced to "blocked" -- order_by skips this pass


def test_classify_rows_order_by_configured_ignores_pre_blocked_pairs_too():
    from src.loader.edge_execution import ApocLockedEdgeWriter

    rows = [{"source_key": "a", "target_key": "b", "properties": {}}]
    by_index = {0: (1, 1)}  # individually resolvable
    statuses = ApocLockedEdgeWriter._classify_rows(
        rows, by_index, pre_blocked_pairs={("a", "b")}, order_by_configured=True,
    )
    assert statuses[0][0] == "matched"  # pre_blocked_pairs ignored for order_by types


def test_write_provenanced_batch_order_by_type_ignores_the_callers_parked_pairs():
    """Decisions §10, enforced regardless of what the coordinator passes."""
    driver, session, tx = _apoc_tx_with_session()
    tx.run.side_effect = [_ok(1)]
    sink = MagicMock()
    writer = ApocLockedEdgeWriter(
        driver, _edge_with_order_by(), missing_endpoint_error_factory=MissingRelationshipEndpointError,
        missing_endpoint_sink=sink,
    )
    caller_parked_pairs = MagicMock()
    caller_parked_pairs.is_parked.return_value = True  # would pre-block everything if consulted
    items = [_pending(_record("p-1"), offset=60)]
    writer.write_provenanced_batch(items, parked_pairs=caller_parked_pairs)
    caller_parked_pairs.is_parked.assert_not_called()  # never even asked
    assert tx.run.call_count == 1  # fast path taken -- nothing was pre-blocked
    sink.route_unresolved_endpoint.assert_not_called()


def test_missing_order_by_value_applies_unconditionally_and_logs_a_warning(caplog):
    import logging

    driver, session, tx = _apoc_tx_with_session()
    tx.run.side_effect = [_ok(1)]
    writer = ApocLockedEdgeWriter(
        driver, _edge_with_order_by(), missing_endpoint_error_factory=MissingRelationshipEndpointError,
    )
    record = EdgeRecord("p-1", "c-1", {"since": None})
    with caplog.at_level(logging.WARNING, logger="src.loader.edge_execution"):
        writer.write_batch([record])
    assert tx.run.call_count == 1  # applied unconditionally -- no special handling needed
    assert any("stage=edge-order-by-missing" in message for message in caplog.messages)
    assert any("p-1" in message and "c-1" in message for message in caplog.messages)


def test_present_order_by_value_logs_no_warning(caplog):
    import logging

    driver, session, tx = _apoc_tx_with_session()
    tx.run.side_effect = [_ok(1)]
    writer = ApocLockedEdgeWriter(
        driver, _edge_with_order_by(), missing_endpoint_error_factory=MissingRelationshipEndpointError,
    )
    record = EdgeRecord("p-1", "c-1", {"since": date(2024, 1, 1)})
    with caplog.at_level(logging.WARNING, logger="src.loader.edge_execution"):
        writer.write_batch([record])
    assert not any("stage=edge-order-by-missing" in message for message in caplog.messages)


def test_skipped_as_not_newer_rows_logged_at_info_when_nonzero(caplog):
    import logging

    driver, session, tx = _apoc_tx_with_session()
    result = MagicMock()
    result.__iter__.return_value = iter(
        [{"checked_rows": 1, "invalid_rows": 0, "merged_rows": 1, "skipped_as_not_newer_rows": 1}]
    )
    tx.run.side_effect = [result]
    writer = ApocLockedEdgeWriter(
        driver, _edge_with_order_by(), missing_endpoint_error_factory=MissingRelationshipEndpointError,
    )
    with caplog.at_level(logging.INFO, logger="src.loader.edge_execution"):
        writer.write_batch([EdgeRecord("p-1", "c-1", {"since": date(2020, 1, 1)})])
    assert any("stage=edge-order-by-superseded" in message for message in caplog.messages)


# ── Review findings M13 (missing $order_by param) / M14 (same-pair collapse) ─


def test_order_by_writer_binds_order_by_parameter_on_every_query_run():
    """M13: against real Neo4j, both query shapes reference $order_by --
    never binding it raises a non-transient ParameterMissing error on
    every write, stalling that edge type's stream forever."""
    driver, session, tx = _apoc_tx_with_session()
    tx.run.side_effect = [_ok(1)]
    writer = ApocLockedEdgeWriter(
        driver, _edge_with_order_by(), missing_endpoint_error_factory=MissingRelationshipEndpointError,
    )
    writer.write_batch([EdgeRecord("p-1", "c-1", {"since": date(2024, 1, 1)})])
    assert tx.run.call_args.kwargs.get("order_by") == "since"


def test_non_order_by_writer_never_binds_an_order_by_parameter():
    driver, session, tx = _apoc_tx_with_session()
    tx.run.side_effect = [_ok(1)]
    writer = ApocLockedEdgeWriter(driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError)
    writer.write_batch([_record("p-1")])
    assert "order_by" not in tx.run.call_args.kwargs


def test_collapse_same_pair_rows_keeps_the_strictly_newer_row_regardless_of_list_position():
    """M14: two same-pair rows in one batch -- older value LATER in the
    list -- must not win just because Neo4j's own per-row check reads a
    stale pre-batch value for every row in a flat UNWIND batch."""
    edge = _edge_with_order_by()
    writer = ApocLockedEdgeWriter(MagicMock(), edge, missing_endpoint_error_factory=MissingRelationshipEndpointError)
    rows = [
        {"source_key": "a", "target_key": "b", "properties": {"since": date(2024, 1, 5), "role": "new"}},
        {"source_key": "a", "target_key": "b", "properties": {"since": date(2024, 1, 1), "role": "old"}},
    ]
    collapsed = writer._collapse_same_pair_rows_for_order_by(rows)
    assert collapsed[0]["properties"] == {"since": date(2024, 1, 5), "role": "new"}  # winner untouched
    assert collapsed[1]["properties"] == {}  # loser's properties suppressed -- harmless no-op merge
    assert collapsed[1]["source_key"] == "a" and collapsed[1]["target_key"] == "b"  # pair/identity preserved


def test_collapse_same_pair_rows_winner_updates_when_a_later_row_is_actually_newer():
    edge = _edge_with_order_by()
    writer = ApocLockedEdgeWriter(MagicMock(), edge, missing_endpoint_error_factory=MissingRelationshipEndpointError)
    rows = [
        {"source_key": "a", "target_key": "b", "properties": {"since": date(2024, 1, 1)}},
        {"source_key": "a", "target_key": "b", "properties": {"since": date(2024, 1, 5)}},  # strictly newer
    ]
    collapsed = writer._collapse_same_pair_rows_for_order_by(rows)
    assert collapsed[0]["properties"] == {}
    assert collapsed[1]["properties"] == {"since": date(2024, 1, 5)}


def test_collapse_same_pair_rows_tie_keeps_the_earlier_occurrence():
    edge = _edge_with_order_by()
    writer = ApocLockedEdgeWriter(MagicMock(), edge, missing_endpoint_error_factory=MissingRelationshipEndpointError)
    same_day = date(2024, 1, 1)
    rows = [
        {"source_key": "a", "target_key": "b", "properties": {"since": same_day, "tag": "first"}},
        {"source_key": "a", "target_key": "b", "properties": {"since": same_day, "tag": "second"}},
    ]
    collapsed = writer._collapse_same_pair_rows_for_order_by(rows)
    assert collapsed[0]["properties"]["tag"] == "first"  # earlier occurrence keeps the slot on a tie
    assert collapsed[1]["properties"] == {}


def test_collapse_same_pair_rows_never_empties_the_max_value_row_for_a_missing_value_row():
    """M14, second review round: a missing-value row must never be
    allowed to "win" and empty a value-carrying row -- doing so would
    silently regress the stored order_by value to whatever it was before
    this batch (a value-carrying row's own value is the only thing that
    can ever legitimately raise it)."""
    edge = _edge_with_order_by()
    writer = ApocLockedEdgeWriter(MagicMock(), edge, missing_endpoint_error_factory=MissingRelationshipEndpointError)
    rows = [
        {"source_key": "a", "target_key": "b", "properties": {"since": date(2024, 1, 10), "tag": "first"}},
        {"source_key": "a", "target_key": "b", "properties": {"tag": "no-value"}},        # no "since" at all
        {"source_key": "a", "target_key": "b", "properties": {"since": date(2024, 1, 5), "tag": "older"}},
    ]
    collapsed = writer._collapse_same_pair_rows_for_order_by(rows)
    assert collapsed[0]["properties"] == {"since": date(2024, 1, 10), "tag": "first"}  # the batch max -- never emptied
    assert collapsed[1]["properties"] == {"tag": "no-value"}  # missing-value row: left completely untouched
    assert collapsed[2]["properties"] == {}  # a lesser value-carrying row still loses to the max


def test_collapse_same_pair_rows_missing_value_row_after_the_max_row_leaves_max_untouched():
    edge = _edge_with_order_by()
    writer = ApocLockedEdgeWriter(MagicMock(), edge, missing_endpoint_error_factory=MissingRelationshipEndpointError)
    rows = [
        {"source_key": "a", "target_key": "b", "properties": {"since": date(2024, 1, 10)}},  # the max
        {"source_key": "a", "target_key": "b", "properties": {"tag": "no-value"}},           # after it, no value
    ]
    collapsed = writer._collapse_same_pair_rows_for_order_by(rows)
    assert collapsed[0]["properties"] == {"since": date(2024, 1, 10)}  # untouched
    assert collapsed[1]["properties"] == {"tag": "no-value"}  # untouched


def test_collapse_same_pair_rows_regression_stored_value_never_goes_backwards():
    """The exact regression the second review round found: stored v=1;
    batch has row0 {v:10} then row1 {} (no v). Sequential per-row
    semantics would give v=10; the buggy collapse gave v=1 (a regression
    a later stale v=5 record could then pass against). The fix must
    leave row0's v=10 intact."""
    edge = _edge_with_order_by()
    writer = ApocLockedEdgeWriter(MagicMock(), edge, missing_endpoint_error_factory=MissingRelationshipEndpointError)
    rows = [
        {"source_key": "a", "target_key": "b", "properties": {"since": date(2024, 1, 10)}},
        {"source_key": "a", "target_key": "b", "properties": {}},
    ]
    collapsed = writer._collapse_same_pair_rows_for_order_by(rows)
    assert collapsed[0]["properties"] == {"since": date(2024, 1, 10)}  # never emptied


def test_collapse_same_pair_rows_is_a_noop_for_a_single_row_or_distinct_pairs():
    edge = _edge_with_order_by()
    writer = ApocLockedEdgeWriter(MagicMock(), edge, missing_endpoint_error_factory=MissingRelationshipEndpointError)
    rows = [
        {"source_key": "a", "target_key": "b", "properties": {"since": date(2024, 1, 1)}},
        {"source_key": "c", "target_key": "d", "properties": {"since": date(2024, 1, 2)}},
    ]
    assert writer._collapse_same_pair_rows_for_order_by(rows) == rows


def test_collapse_same_pair_rows_is_a_noop_for_non_order_by_writers():
    writer = ApocLockedEdgeWriter(MagicMock(), _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError)
    rows = [
        {"source_key": "a", "target_key": "b", "properties": {"role": "x"}},
        {"source_key": "a", "target_key": "b", "properties": {"role": "y"}},
    ]
    assert writer._collapse_same_pair_rows_for_order_by(rows) == rows


def test_write_batch_sends_collapsed_rows_to_neo4j_for_an_order_by_type():
    """End-to-end proof (Attempt-1 fast path): the collapsed, not the
    original, row list is what actually reaches transaction.run."""
    driver, session, tx = _apoc_tx_with_session()
    tx.run.side_effect = [_ok(2)]
    writer = ApocLockedEdgeWriter(
        driver, _edge_with_order_by(), missing_endpoint_error_factory=MissingRelationshipEndpointError,
    )
    records = [
        EdgeRecord("a", "b", {"since": date(2024, 1, 1)}),
        EdgeRecord("a", "b", {"since": date(2024, 1, 10)}),  # same pair, strictly newer, later in the list
    ]
    writer.write_batch(records)
    sent_rows = tx.run.call_args.kwargs["rows"]
    assert sent_rows[0]["properties"] == {}  # older, same-pair row collapsed away
    assert sent_rows[1]["properties"] == {"since": date(2024, 1, 10)}


def test_non_order_by_writer_never_checks_or_logs_order_by_things(caplog):
    """A writer for an edge type with no order_by must never touch these
    helpers at all -- proven by absence of any of their log lines even
    when the aggregate result happens to lack the new key entirely (the
    real byte-for-byte-unchanged query never returns it)."""
    import logging

    driver, session, tx = _apoc_tx_with_session()
    tx.run.side_effect = [_ok(1)]
    writer = ApocLockedEdgeWriter(driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError)
    with caplog.at_level(logging.INFO, logger="src.loader.edge_execution"):
        writer.write_batch([_record("p-1")])
    assert not any("order-by" in message for message in caplog.messages)


# ── P5-S4 Story 6: retry_provenanced_batch (the real RetryWriter adapter) ───


def _envelope_for_retry(source_key="p-1", target_key="c-1", properties=None, *,
                         original_topic="works-at-events", original_partition=0, original_offset=7):
    from src.loader.dead_letter import DeadLetterEnvelope, LoaderIdentity, SCHEMA_VERSION
    import json as _json

    value = _json.dumps({
        "source": {"personId": source_key}, "target": {"companyId": target_key},
        "properties": properties or {"since": "2020-01-02"},
    }).encode("utf-8")
    return DeadLetterEnvelope(
        schema_version=SCHEMA_VERSION, failure_class="missing_endpoint", entity_kind="edge",
        entity_name="WORKS_AT", original_topic=original_topic, original_partition=original_partition,
        original_offset=original_offset, key_bytes=None, value_bytes=value,
        reason_code="missing_source", error_text="x",
        loader_identity=LoaderIdentity(run_id="run-1", replica_id="0", kind="edge"),
        first_failure_at_ms=1_000, attempt_count=1, dlq_routed_at_ms=1_000, retry_after_ms=2_000,
    )


def test_retry_provenanced_batch_written_outcome():
    driver, session, tx = _apoc_tx_with_session()
    tx.run.side_effect = [_classification([(0, 1, 1)]), _ok(1)]
    writer = ApocLockedEdgeWriter(driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError)
    outcomes = writer.retry_provenanced_batch([_envelope_for_retry()])
    assert outcomes == {0: ("written", "")}
    tx.commit.assert_called_once_with()


def test_retry_provenanced_batch_unresolved_outcome_never_publishes_to_dlq():
    driver, session, tx = _apoc_tx_with_session()
    tx.run.side_effect = [_classification([(0, 0, 1)])]  # still missing_source
    sink = MagicMock()
    writer = ApocLockedEdgeWriter(
        driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError, missing_endpoint_sink=sink,
    )
    outcomes = writer.retry_provenanced_batch([_envelope_for_retry()])
    assert outcomes == {0: ("unresolved", "missing_source")}
    tx.commit.assert_called_once_with()  # still commits -- nothing to write, no failure
    sink.route_unresolved_endpoint.assert_not_called()  # this method never publishes to the DLQ itself


def test_retry_provenanced_batch_empty_list_is_a_noop():
    driver, session, tx = _apoc_tx_with_session()
    writer = ApocLockedEdgeWriter(driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError)
    assert writer.retry_provenanced_batch([]) == {}
    tx.run.assert_not_called()


def _unreadable_envelope(original_offset=1):
    from src.loader.dead_letter import DeadLetterEnvelope, LoaderIdentity, SCHEMA_VERSION

    return DeadLetterEnvelope(
        schema_version=SCHEMA_VERSION, failure_class="missing_endpoint", entity_kind="edge", entity_name="WORKS_AT",
        original_topic="works-at-events", original_partition=0, original_offset=original_offset, key_bytes=None,
        value_bytes=b"not valid json", reason_code="missing_source", error_text="x",
        loader_identity=LoaderIdentity(run_id="run-1", replica_id="0", kind="edge"),
        first_failure_at_ms=1_000, attempt_count=0, dlq_routed_at_ms=1_000, retry_after_ms=2_000,
    )


def test_retry_provenanced_batch_unreadable_value_bytes_reported_unresolved_never_raises():
    """Review finding M15: one bad envelope must not fail the whole batch

    (and, with no other envelope in the batch, must never even reach
    Neo4j) -- it is reported unresolved/invalid_on_retry so the retry
    consumer's normal bounded-attempts/park machinery handles it."""
    driver, session, tx = _apoc_tx_with_session()
    writer = ApocLockedEdgeWriter(driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError)
    outcomes = writer.retry_provenanced_batch([_unreadable_envelope()])
    assert outcomes == {0: ("unresolved", "invalid_on_retry")}
    tx.run.assert_not_called()
    tx.commit.assert_not_called()


def test_retry_provenanced_batch_mixed_good_and_bad_envelope_writes_the_good_one():
    """M15: a bad envelope batched with a healthy one must not block the

    healthy one -- it is written and committed; the bad one is reported
    unresolved for that same call, independently."""
    driver, session, tx = _apoc_tx_with_session()
    tx.run.side_effect = [_classification([(0, 1, 1)]), _ok(1)]
    writer = ApocLockedEdgeWriter(driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError)
    outcomes = writer.retry_provenanced_batch([_unreadable_envelope(original_offset=1), _envelope_for_retry()])
    assert outcomes[0] == ("unresolved", "invalid_on_retry")
    assert outcomes[1] == ("written", "")
    tx.commit.assert_called_once_with()
    # only the good row's is a real classification query on 1 row, not 2
    classify_rows = tx.run.call_args_list[0].kwargs["rows"]
    assert len(classify_rows) == 1


def test_retry_provenanced_batch_toctou_mismatch_raises_and_commits_nothing():
    driver, session, tx = _apoc_tx_with_session()
    tx.run.side_effect = [
        _classification([(0, 1, 1)]),
        _invalid(checked_rows=1, invalid_rows=1, merged_rows=0),  # write disagrees moments later
    ]
    writer = ApocLockedEdgeWriter(driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError)
    with pytest.raises(MissingRelationshipEndpointError, match="retry matched-subset write mismatch"):
        writer.retry_provenanced_batch([_envelope_for_retry()])
    tx.commit.assert_not_called()


def test_retry_provenanced_batch_order_by_binds_parameter_and_collapses_same_pair_rows():
    driver, session, tx = _apoc_tx_with_session()
    tx.run.side_effect = [_classification([(0, 1, 1), (1, 1, 1)]), _ok(2)]
    writer = ApocLockedEdgeWriter(
        driver, _edge_with_order_by(), missing_endpoint_error_factory=MissingRelationshipEndpointError,
    )
    envelopes = [
        _envelope_for_retry("a", "b", {"since": "2024-01-01"}, original_offset=1),
        _envelope_for_retry("a", "b", {"since": "2024-01-10"}, original_offset=2),
    ]
    outcomes = writer.retry_provenanced_batch(envelopes)
    assert outcomes == {0: ("written", ""), 1: ("written", "")}
    write_call = tx.run.call_args_list[1]
    assert write_call.kwargs.get("order_by") == "since"
    sent_rows = write_call.kwargs["rows"]
    assert sent_rows[0]["properties"] == {}  # collapsed: older same-pair row
    assert sent_rows[1]["properties"]["since"] == date(2024, 1, 10)


# ── Retry drain throughput: retry_provenanced_batch inside a lease flush ─────


class _ParkedSet:
    def __init__(self, parked=()):
        self.parked = set(parked)

    def is_parked(self, pair):
        return pair in self.parked

    def mark_parked(self, pairs):
        self.parked.update(pairs)


def test_retry_provenanced_batch_blocks_a_pair_parked_by_an_earlier_transaction_of_the_flush():
    driver, session, tx = _apoc_tx_with_session()
    tx.run.side_effect = [_classification([(0, 1, 1)])]  # resolvable, but its pair was left unresolved earlier
    writer = ApocLockedEdgeWriter(driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError)
    parked = _ParkedSet({("p-1", "c-1")})
    outcomes = writer.retry_provenanced_batch([_envelope_for_retry()], parked_pairs=parked)
    assert outcomes == {0: ("unresolved", "blocked_by_earlier_unresolved_row")}
    assert tx.run.call_count == 1  # nothing was written


def test_retry_provenanced_batch_records_newly_unresolved_pairs_for_later_transactions():
    driver, session, tx = _apoc_tx_with_session()
    tx.run.side_effect = [_classification([(0, 0, 1)])]
    writer = ApocLockedEdgeWriter(driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError)
    parked = _ParkedSet()
    writer.retry_provenanced_batch([_envelope_for_retry()], parked_pairs=parked)
    assert parked.parked == {("p-1", "c-1")}


def test_retry_provenanced_batch_aborts_before_commit_when_the_lease_liveness_check_fails():
    driver, session, tx = _apoc_tx_with_session()
    tx.run.side_effect = [_classification([(0, 1, 1)]), _ok(1)]
    writer = ApocLockedEdgeWriter(driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError)
    with pytest.raises(LeaseCoordinatorError, match="lease connection broke"):
        writer.retry_provenanced_batch([_envelope_for_retry()], lease_liveness_check=lambda: False)
    tx.commit.assert_not_called()


def test_retry_provenanced_batch_caps_the_transaction_timeout_at_the_lease_deadline():
    import time as _time

    driver, session, tx = _apoc_tx_with_session()
    tx.run.side_effect = [_classification([(0, 1, 1)]), _ok(1)]
    writer = ApocLockedEdgeWriter(
        driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError, tx_timeout_seconds=30.0,
    )
    writer.retry_provenanced_batch([_envelope_for_retry()], deadline=_time.monotonic() + 8.0)
    (_args, kwargs), = session.begin_transaction.call_args_list
    assert 0 < kwargs["timeout"] <= 8.0 - 4.0  # the fenced budget: minus the 3 s slack and 1 s BEGIN allowance
    driver2, session2, tx2 = _apoc_tx_with_session()
    with pytest.raises(LeaseCoordinatorError, match="deadline exceeded"):
        ApocLockedEdgeWriter(
            driver2, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError,
        ).retry_provenanced_batch([_envelope_for_retry()], deadline=_time.monotonic() - 1.0)
    tx2.run.assert_not_called()
