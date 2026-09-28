"""Unit tests for the APOC-locked relationship writer."""
from datetime import date
from threading import Barrier, get_ident
from unittest.mock import MagicMock
import pytest
from neo4j.exceptions import ClientError

from src.loader.edge_execution import ApocLockedEdgeWriter, LaneExecutionCoordinator, NativeDisjointEdgeWriter, ServerCapabilities, ExecutionModeError, build_apoc_locked_upsert_query, build_edge_execution
from src.loader.mix_and_batch import RoutedEdgeRecord
from src.loader.edge_loader import EdgeRecord, MissingRelationshipEndpointError
from src.utils.schema_loader import load_schema


def _edge():
    return next(e for e in load_schema("config/graph_schema.yaml").edges if e.type == "WORKS_AT")


def _knows():
    return next(e for e in load_schema("config/graph_schema.yaml").edges if e.type == "KNOWS")


def _record(key="p-1"):
    return EdgeRecord(key, "c-1", {"since": date(2020, 1, 2)})


def test_apoc_query_uses_sorted_distinct_locks_and_schema_identifiers():
    query = build_apoc_locked_upsert_query(_edge())
    assert "CALL (all_nodes) { UNWIND all_nodes AS n" in query
    assert "WITH DISTINCT n AS dist_n ORDER BY elementId(dist_n)" in query
    assert "CALL apoc.lock.nodes(sorted_nodes)" in query
    assert "WITH rel.s AS s, rel.t AS t, rel.props AS props MERGE (s)-[r:`WORKS_AT`]->(t) SET r += props" in query
    assert "MERGE (rel.s)-[r:`WORKS_AT`]->(rel.t)" not in query
    knows = build_apoc_locked_upsert_query(_knows())
    assert "MATCH (t:`Person` {`personId`: row.target_key})" in knows
    assert "MERGE (s)-[r:`KNOWS`]->(t) SET r += props" in knows


def test_endpoint_preflight_returns_one_aggregate_validation_row():
    writer = ApocLockedEdgeWriter(MagicMock(), _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError)
    assert "WITH row_index, source_matches, count(t) AS target_matches" in writer._preflight
    assert "RETURN count(*) AS checked_rows" in writer._preflight
    assert "AS invalid_rows" in writer._preflight
    assert "RETURN row_index, source_matches" not in writer._preflight


def test_apoc_writer_preflights_then_consumes_locks_and_commits():
    driver = MagicMock(); session = driver.session.return_value.__enter__.return_value
    tx = session.begin_transaction.return_value.__enter__.return_value
    preflight, locked = MagicMock(), MagicMock()
    order = []
    preflight.consume.side_effect = lambda: order.append("preflight")
    locked.consume.side_effect = lambda: order.append("locked")
    tx.commit.side_effect = lambda: order.append("commit")
    preflight.__iter__.return_value = iter([{"checked_rows": 2, "invalid_rows": 0}])
    tx.run.side_effect = [preflight, locked]
    ApocLockedEdgeWriter(driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError).write_batch([_record(), _record("p-2")])
    rows = tx.run.call_args_list[0].kwargs["rows"]
    assert len(rows) == 2 and tx.run.call_args_list[1].kwargs["rows"] == rows
    assert "apoc.lock.nodes" in tx.run.call_args_list[1].args[0]
    preflight.consume.assert_called_once_with(); locked.consume.assert_called_once_with(); tx.commit.assert_called_once_with()
    assert order == ["preflight", "locked", "commit"]
    driver.close.assert_not_called()


def test_apoc_writer_missing_endpoint_never_locks_or_commits():
    driver = MagicMock(); session = driver.session.return_value.__enter__.return_value
    tx = session.begin_transaction.return_value.__enter__.return_value; preflight = MagicMock()
    preflight.__iter__.return_value = iter([{"checked_rows": 1, "invalid_rows": 1}]); tx.run.return_value = preflight
    with pytest.raises(MissingRelationshipEndpointError):
        ApocLockedEdgeWriter(driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError).write(_record())
    preflight.consume.assert_called_once_with()
    assert tx.run.call_count == 1; tx.commit.assert_not_called()


@pytest.mark.parametrize("status", [
    [],
    [{"checked_rows": 0, "invalid_rows": 0}],
    [{"checked_rows": 1, "invalid_rows": 0}, {"checked_rows": 1, "invalid_rows": 0}],
])
def test_apoc_writer_incomplete_or_nonaggregate_preflight_fails_closed(status):
    driver = MagicMock(); session = driver.session.return_value.__enter__.return_value
    tx = session.begin_transaction.return_value.__enter__.return_value
    preflight = MagicMock(); preflight.__iter__.return_value = iter(status); tx.run.return_value = preflight
    with pytest.raises(MissingRelationshipEndpointError):
        ApocLockedEdgeWriter(driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError).write(_record())
    assert tx.run.call_count == 1
    tx.commit.assert_not_called()


def test_apoc_writer_driver_failure_never_commits():
    driver = MagicMock(); session = driver.session.return_value.__enter__.return_value
    tx = session.begin_transaction.return_value.__enter__.return_value; preflight = MagicMock()
    preflight.__iter__.return_value = iter([{"checked_rows": 1, "invalid_rows": 0}])
    tx.run.side_effect = [preflight, RuntimeError("apoc failed")]
    with pytest.raises(RuntimeError, match="apoc failed"):
        ApocLockedEdgeWriter(driver, _edge(), missing_endpoint_error_factory=MissingRelationshipEndpointError).write(_record())
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
    writer = MagicMock()
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
    writer = MagicMock(); routed = lambda lane, offset: RoutedEdgeRecord(_record(), "t", 0, offset, lane, "forward", 0, 0)
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
