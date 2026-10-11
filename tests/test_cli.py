"""
tests/test_cli.py
─────────────────
Unit tests for the CLI parser and routing.
"""
import argparse
import json
from threading import Condition, Event, Thread
from types import SimpleNamespace
from unittest.mock import patch, MagicMock

import pytest

from src.cli import (
    _derive_relationship_phase_run_id,
    _run_isolated_relationship_bulk_phase,
    _run_isolated_relationship_stream_phase,
    _run_lease_relationship_fleet,
    _run_relationship_orchestration,
    _run_completion_driven_bulk_edges,
    _run_relationship_bulk_stages,
    _run_rotating_relationship_fleet,
    _run_bulk_relationship_edge,
    _select_keyed_lease_labels,
    build_parser,
    get_neo4j_credentials,
    handle_start,
    handle_status,
    handle_stop,
    main,
)
from src.models.schema import DeadLetterConfig
from src.orchestrator.dependency_manager import RelationshipConflictPlan, RelationshipStage
from src.orchestrator.schema_initializer import SchemaInitializationError
from src.orchestrator.bulk_monitor import LoaderCrashError


def test_parser_start_valid():
    parser = build_parser()
    args = parser.parse_args(["start", "--mode", "bulk"])
    assert args.command == "start"
    assert args.mode == "bulk"
    assert args.config == "config/graph_schema.yaml"
    assert hasattr(args, "func")


def test_parser_accepts_optional_run_id():
    assert build_parser().parse_args(["start", "--mode", "bulk", "--run-id", "e2e-run_42"]).run_id == "e2e-run_42"


def test_parser_start_missing_mode(capsys):
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["start"])
    captured = capsys.readouterr()
    assert "the following arguments are required: --mode" in captured.err


def test_parser_stop():
    parser = build_parser()
    args = parser.parse_args(["stop", "--loader", "company_loader"])
    assert args.command == "stop"
    assert args.loader == "company_loader"


def test_parser_status():
    parser = build_parser()
    args = parser.parse_args(["status", "--config", "custom.yaml"])
    assert args.command == "status"
    assert args.config == "custom.yaml"


@patch("src.cli.handle_start")
def test_main_routes_to_start_handler(mock_handle_start):
    mock_handle_start.return_value = 0
    exit_code = main(["start", "--mode", "stream"])
    assert exit_code == 0
    mock_handle_start.assert_called_once()
    args = mock_handle_start.call_args[0][0]
    assert args.mode == "stream"


@patch("src.cli.handle_stop")
def test_main_routes_to_stop_handler(mock_handle_stop):
    mock_handle_stop.return_value = 0
    exit_code = main(["stop"])
    assert exit_code == 0
    mock_handle_stop.assert_called_once()


@patch("src.cli.handle_status")
def test_main_routes_to_status_handler(mock_handle_status):
    mock_handle_status.return_value = 0
    exit_code = main(["status"])
    assert exit_code == 0
    mock_handle_status.assert_called_once()


@patch("src.cli.DockerService")
def test_handle_stop_stub(mock_docker):
    args = argparse.Namespace(loader=None)
    mock_docker_instance = MagicMock()
    mock_docker.return_value = mock_docker_instance
    assert handle_stop(args) == 0
    mock_docker_instance.stop_node_loaders.assert_called_once_with(node_label=None)


@patch("src.cli.DockerService")
def test_handle_status_stub(mock_docker):
    args = argparse.Namespace(config="config/graph_schema.yaml")
    mock_docker_instance = MagicMock()
    mock_docker_instance.list_node_loaders.return_value = [{"name": "c1", "id": "1", "status": "running", "node_label": "P"}]
    mock_docker.return_value = mock_docker_instance
    assert handle_status(args) == 0
    mock_docker_instance.list_node_loaders.assert_called_once()


def test_neo4j_username_prefers_documented_variable(monkeypatch):
    monkeypatch.setenv("NEO4J_USERNAME", "documented")
    monkeypatch.setenv("NEO4J_USER", "legacy")
    assert get_neo4j_credentials()[1] == "documented"


def test_neo4j_username_uses_legacy_fallback(monkeypatch):
    monkeypatch.delenv("NEO4J_USERNAME", raising=False)
    monkeypatch.setenv("NEO4J_USER", "legacy")
    assert get_neo4j_credentials()[1] == "legacy"


@patch("src.cli.RelationshipBulkMonitor")
@patch("src.cli.build_relationship_conflict_plan")
def test_relationship_stage_failure_blocks_later_stage(mock_plan, monitor_cls):
    works = SimpleNamespace(type="WORKS_AT", topic="works", topics=["works"], dead_letter_entity="works", is_multi_topic=False, replicas=1)
    bought = SimpleNamespace(type="BOUGHT", topic="bought", topics=["bought"], dead_letter_entity="bought", is_multi_topic=False, replicas=1)
    schema = SimpleNamespace(edges=[works, bought], loading=SimpleNamespace(consumer_group_id="loader"))
    mock_plan.return_value = RelationshipConflictPlan(
        conflicts={}, stages=(RelationshipStage(0, ("WORKS_AT",)), RelationshipStage(1, ("BOUGHT",)))
    )
    container = MagicMock(); docker = MagicMock(); docker.run_edge_loader.return_value = [container]
    monitor = MagicMock(); monitor.wait_for_completion.side_effect = RuntimeError("write failed")
    monitor_cls.return_value = monitor
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1)

    assert _run_relationship_bulk_stages(schema, docker, args) == 1
    docker.run_edge_loader.assert_called_once()
    assert container.stop.call_count >= 1
    monitor.close.assert_called_once()


@patch("src.cli.RelationshipBulkMonitor")
@patch("src.cli.build_relationship_conflict_plan")
def test_relationship_independent_stage_launches_every_type_with_exact_scope(mock_plan, monitor_cls):
    first = SimpleNamespace(type="VISITED", topic="visits", topics=["visits"], dead_letter_entity="visits", is_multi_topic=False, replicas=1)
    second = SimpleNamespace(type="SUPPLIES", topic="supplies", topics=["supplies"], dead_letter_entity="supplies", is_multi_topic=False, replicas=1)
    schema = SimpleNamespace(edges=[first, second], loading=SimpleNamespace(consumer_group_id="loader"))
    mock_plan.return_value = RelationshipConflictPlan(conflicts={}, stages=(RelationshipStage(0, ("SUPPLIES", "VISITED")),))
    docker = MagicMock(); docker.run_edge_loader.side_effect = [[MagicMock()], [MagicMock()]]
    monitor = MagicMock(); monitor_cls.return_value = monitor
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1)

    assert _run_relationship_bulk_stages(schema, docker, args) == 0
    assert [call.kwargs["edge_type"] for call in docker.run_edge_loader.call_args_list] == ["SUPPLIES", "VISITED"]
    for call in docker.run_edge_loader.call_args_list:
        assert call.kwargs["replicas"] == 1
        assert call.kwargs["consumer_group_prefix"] == "loader"
        assert call.kwargs["run_id"]
    monitor.close.assert_called_once()


@patch("src.cli.RelationshipBulkMonitor")
@patch("src.cli.build_relationship_conflict_plan")
def test_relationship_duplicate_returned_container_is_rejected_and_stopped(mock_plan, monitor_cls):
    edge = SimpleNamespace(type="WORKS_AT", topic="works", topics=["works"], dead_letter_entity="works", is_multi_topic=False, replicas=2)
    schema = SimpleNamespace(edges=[edge], loading=SimpleNamespace(consumer_group_id="loader"))
    mock_plan.return_value = RelationshipConflictPlan(conflicts={}, stages=(RelationshipStage(0, ("WORKS_AT",)),))
    container = MagicMock(); docker = MagicMock(); docker.run_edge_loader.return_value = [container, container]
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1)

    assert _run_relationship_bulk_stages(schema, docker, args) == 1
    monitor_cls.assert_not_called()
    assert container.stop.call_count >= 1


@patch("src.cli.RelationshipBulkMonitor")
@patch("src.cli.build_relationship_conflict_plan")
def test_relationship_second_conflict_stage_waits_for_first_zero_lag(mock_plan, monitor_cls):
    first = SimpleNamespace(type="WORKS_AT", topic="works", topics=["works"], dead_letter_entity="works", is_multi_topic=False, replicas=1)
    second = SimpleNamespace(type="BOUGHT", topic="bought", topics=["bought"], dead_letter_entity="bought", is_multi_topic=False, replicas=1)
    schema = SimpleNamespace(edges=[first, second], loading=SimpleNamespace(consumer_group_id="loader"))
    mock_plan.return_value = RelationshipConflictPlan(conflicts={}, stages=(RelationshipStage(0, ("WORKS_AT",)), RelationshipStage(1, ("BOUGHT",))))
    docker = MagicMock(); docker.run_edge_loader.side_effect = [[MagicMock()], [MagicMock()]]
    first_monitor, second_monitor = MagicMock(), MagicMock()
    first_monitor.verify_zero_lag.side_effect = lambda: assert_first_stage_is_only_launch(docker)
    monitor_cls.side_effect = [first_monitor, second_monitor]
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1)

    assert _run_relationship_bulk_stages(schema, docker, args) == 0
    assert docker.run_edge_loader.call_count == 2


def assert_first_stage_is_only_launch(docker):
    assert docker.run_edge_loader.call_count == 1


def _rotating_edge(edge_type, topic, source, target, replicas=1):
    return SimpleNamespace(
        type=edge_type, topic=topic, topics=[topic], dead_letter_entity=topic, is_multi_topic=False, replicas=replicas,
        endpoint_labels=frozenset({source, target}), has_self_reference=source == target, is_multi_pair=False,
        nodes=SimpleNamespace(source=source, target=target, is_self_referencing=source == target),
    )


@patch("src.cli.RelationshipBulkMonitor")
def test_isolated_bulk_phase_uses_exact_clock_free_finite_lifecycle(monitor_cls, tmp_path, monkeypatch):
    monkeypatch.setenv("GRAPH_LOADER_TIMING_DIR", str(tmp_path))
    knows = _rotating_edge("KNOWS", "knows", "Person", "Person", replicas=2)
    first, second = MagicMock(), MagicMock()
    docker = MagicMock()
    docker.run_edge_loader.return_value = [first, second]
    monitor = MagicMock()
    monitor_cls.return_value = monitor
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1)

    assert _run_isolated_relationship_bulk_phase(
        knows, docker, args, run_id="parent-isolated-0", phase_index=0,
        consumer_group_prefix="loader",
    ) == 0

    launch = docker.run_edge_loader.call_args.kwargs
    assert launch == {
        "edge_type": "KNOWS", "topics": ["knows"], "mode": "bulk", "config_path": "config.yaml",
        "replicas": 2, "network": "test-net", "run_id": "parent-isolated-0",
        "consumer_group_prefix": "loader", "slot_gating": False,
    }
    docker.run_global_batch_clock.assert_not_called()
    assert monitor_cls.call_args.args[0] == [knows]
    assert monitor_cls.call_args.args[1] == "parent-isolated-0"
    assert "rotation_plan" not in monitor_cls.call_args.kwargs
    assert "coordination_topic" not in monitor_cls.call_args.kwargs
    monitor.wait_for_assignment_coverage.assert_called_once_with(1)
    monitor.capture_boundary.assert_called_once_with()
    monitor.wait_for_completion.assert_called_once_with(1)
    monitor.wait_for_drain_complete.assert_called_once_with(1)
    monitor.verify_zero_lag.assert_called_once_with()
    monitor.close.assert_called_once_with()
    assert first.stop.call_count >= 1 and second.stop.call_count >= 1
    timing = json.loads((tmp_path / "stage-KNOWS.json").read_text())
    assert timing["edge_type"] == "KNOWS"
    assert timing["succeeded"] is True
    assert timing["elapsed_seconds"] >= 0


@patch("src.cli.RelationshipBulkMonitor")
def test_isolated_bulk_phase_rejects_duplicate_response_and_stops_only_returned(monitor_cls, caplog):
    knows = _rotating_edge("KNOWS", "knows", "Person", "Person", replicas=2)
    duplicated = MagicMock()
    docker = MagicMock()
    docker.run_edge_loader.return_value = [duplicated, duplicated]
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1)

    assert _run_isolated_relationship_bulk_phase(
        knows, docker, args, run_id="isolated-run", phase_index=3,
        consumer_group_prefix="loader",
    ) == 1

    monitor_cls.assert_not_called()
    assert duplicated.stop.call_count >= 1
    assert "stage=isolation run_id=isolated-run phase=3 edge=KNOWS" in caplog.text
    docker.run_global_batch_clock.assert_not_called()


@patch("src.cli.RelationshipBulkMonitor")
def test_isolated_bulk_phase_monitor_failure_stops_exact_loader_and_closes_monitor(monitor_cls, caplog):
    knows = _rotating_edge("KNOWS", "knows", "Person", "Person")
    container = MagicMock()
    docker = MagicMock()
    docker.run_edge_loader.return_value = [container]
    monitor = MagicMock()
    monitor.wait_for_completion.side_effect = RuntimeError("durable write failed")
    monitor_cls.return_value = monitor
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1)

    assert _run_isolated_relationship_bulk_phase(
        knows, docker, args, run_id="isolated-run", phase_index=1,
        consumer_group_prefix="loader",
    ) == 1

    assert container.stop.call_count >= 1
    monitor.close.assert_called_once_with()
    assert "stage=isolation run_id=isolated-run phase=1 edge=KNOWS" in caplog.text
    docker.run_global_batch_clock.assert_not_called()


@patch("src.cli.RelationshipBulkMonitor")
def test_isolated_bulk_phase_drain_failure_names_exact_replica_and_stops_no_unrelated_container(monitor_cls, caplog):
    knows = _rotating_edge("KNOWS", "knows", "Person", "Person")
    container, unrelated = MagicMock(), MagicMock()
    docker = MagicMock()
    docker.run_edge_loader.return_value = [container]
    monitor = MagicMock()
    monitor.wait_for_drain_complete.side_effect = RuntimeError("edge=KNOWS replica=0 drain failed")
    monitor_cls.return_value = monitor
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1)

    assert _run_isolated_relationship_bulk_phase(
        knows, docker, args, run_id="isolated-run", phase_index=2,
        consumer_group_prefix="loader",
    ) == 1

    assert container.stop.call_count >= 1
    unrelated.stop.assert_not_called()
    assert "stage=isolation run_id=isolated-run phase=2 edge=KNOWS" in caplog.text
    assert "edge=KNOWS replica=0 drain failed" in caplog.text
    monitor.close.assert_called_once_with()


@pytest.mark.parametrize("run_id, phase_index", [("bad run", 0), ("isolated-run", True), ("isolated-run", -1)])
def test_isolated_bulk_phase_validates_inputs_before_docker(run_id, phase_index, caplog):
    knows = _rotating_edge("KNOWS", "knows", "Person", "Person")
    docker = MagicMock()
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1)

    assert _run_isolated_relationship_bulk_phase(
        knows, docker, args, run_id=run_id, phase_index=phase_index,
        consumer_group_prefix="loader",
    ) == 1

    docker.run_edge_loader.assert_not_called()
    assert "stage=isolation" in caplog.text


def test_isolated_bulk_phase_rejects_shared_edge_before_docker():
    works = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    docker = MagicMock()
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1)

    assert _run_isolated_relationship_bulk_phase(
        works, docker, args, run_id="isolated-run", phase_index=0,
        consumer_group_prefix="loader",
    ) == 1

    docker.run_edge_loader.assert_not_called()


def test_isolated_bulk_phase_rejects_truthy_forged_self_reference_before_docker():
    knows = _rotating_edge("KNOWS", "knows", "Person", "Person")
    knows.nodes.is_self_referencing = 1
    docker = MagicMock()
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1)

    assert _run_isolated_relationship_bulk_phase(
        knows, docker, args, run_id="isolated-run", phase_index=0,
        consumer_group_prefix="loader",
    ) == 1

    docker.run_edge_loader.assert_not_called()


def test_relationship_phase_run_ids_are_deterministic_bounded_and_distinct():
    parent = "p" * 128
    first = _derive_relationship_phase_run_id(parent, phase_kind="isolated", phase_index=0, edge_type="KNOWS")
    assert first == _derive_relationship_phase_run_id(parent, phase_kind="isolated", phase_index=0, edge_type="KNOWS")
    assert len(first) <= 128
    assert first != _derive_relationship_phase_run_id(parent, phase_kind="isolated", phase_index=1, edge_type="KNOWS")
    assert first != _derive_relationship_phase_run_id(parent, phase_kind="shared", phase_index=0, edge_type="shared")


@patch("src.cli._run_isolated_relationship_bulk_phase")
@patch("src.cli._run_completion_driven_bulk_edges")
def test_bulk_orchestration_runs_shared_before_each_isolated_phase(shared_fleet, isolated_phase):
    works = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    knows = _rotating_edge("KNOWS", "knows", "Person", "Person")
    schema = SimpleNamespace(edges=[knows, works], loading=SimpleNamespace(consumer_group_id="loader"))
    docker = MagicMock()
    args = argparse.Namespace(mode="bulk", config="config.yaml", network="test-net", bulk_timeout_seconds=1, edge_classic=True)
    shared_fleet.return_value = 0
    isolated_phase.side_effect = lambda *_args, **_kwargs: shared_fleet.assert_called_once() or 0

    assert _run_relationship_orchestration(schema, docker, args, parent_run_id="parent-run") == 0

    assert shared_fleet.call_args.args[0] == (works,)
    assert shared_fleet.call_args.kwargs["consumer_group_prefix"] == "loader"
    assert isolated_phase.call_args.args[0] is knows
    assert isolated_phase.call_args.kwargs["phase_index"] == 0
    assert isolated_phase.call_args.kwargs["run_id"] != shared_fleet.call_args.kwargs["parent_run_id"]


@patch("src.cli._run_isolated_relationship_bulk_phase")
@patch("src.cli._run_completion_driven_bulk_edges")
def test_bulk_orchestration_shared_failure_blocks_isolated_with_full_phase_attribution(shared_fleet, isolated_phase, caplog):
    works = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    knows = _rotating_edge("KNOWS", "knows", "Person", "Person")
    schema = SimpleNamespace(edges=[works, knows], loading=SimpleNamespace(consumer_group_id="loader"))
    shared_fleet.return_value = 1
    args = argparse.Namespace(mode="bulk", config="config.yaml", network="test-net", bulk_timeout_seconds=1, edge_classic=True)

    assert _run_relationship_orchestration(schema, MagicMock(), args, parent_run_id="parent-run") == 1

    isolated_phase.assert_not_called()
    assert "parent_run_id=parent-run shared relationship phase failed" in caplog.text


@patch("src.cli._run_bulk_relationship_edge")
def test_bulk_scheduler_starts_next_conflict_as_soon_as_labels_are_released(run_edge):
    first = _rotating_edge("A", "a", "Person", "Title")
    blocked = _rotating_edge("B", "b", "Person", "Genre")
    independent = _rotating_edge("C", "c", "Region", "Language")
    started = {edge.type: Event() for edge in (first, blocked, independent)}
    release = {edge.type: Event() for edge in (first, blocked, independent)}

    def run(edge, *_args, **_kwargs):
        started[edge.type].set()
        assert release[edge.type].wait(3)
        return 0

    run_edge.side_effect = run
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1)
    result = []
    thread = Thread(target=lambda: result.append(_run_completion_driven_bulk_edges(
        (first, blocked, independent), MagicMock(), args,
        parent_run_id="bulk-run", consumer_group_prefix="loader",
    )))
    thread.start()
    try:
        assert started["A"].wait(3)
        assert started["C"].wait(3)
        assert not started["B"].is_set()
        release["A"].set()
        assert started["B"].wait(3)
        assert not release["C"].is_set()
    finally:
        for event in release.values():
            event.set()
        thread.join(3)
    assert not thread.is_alive()
    assert result == [0]


@patch("src.cli._run_bulk_relationship_edge")
def test_bulk_scheduler_failure_blocks_conflicting_pending_edge(run_edge):
    first = _rotating_edge("A", "a", "Person", "Title")
    blocked = _rotating_edge("B", "b", "Person", "Genre")
    run_edge.return_value = 1
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1)

    assert _run_completion_driven_bulk_edges(
        (first, blocked), MagicMock(), args,
        parent_run_id="bulk-run", consumer_group_prefix="loader",
    ) == 1
    assert [call.args[0].type for call in run_edge.call_args_list] == ["A"]


@patch("src.cli._run_bulk_relationship_edge", return_value=0)
def test_bulk_scheduler_records_each_edge_lifecycle(run_edge, tmp_path, monkeypatch):
    monkeypatch.setenv("GRAPH_LOADER_TIMING_DIR", str(tmp_path))
    edge = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1)

    assert _run_completion_driven_bulk_edges(
        (edge,), MagicMock(), args,
        parent_run_id="bulk-run", consumer_group_prefix="loader",
    ) == 0

    timing = json.loads((tmp_path / "stage-WORKS_AT.json").read_text(encoding="utf-8"))
    assert timing["edge_type"] == "WORKS_AT"
    assert timing["succeeded"] is True
    assert timing["elapsed_seconds"] >= 0
    assert timing["started_utc"] <= timing["completed_utc"]


@patch("src.cli._run_bulk_relationship_edge", return_value=0)
def test_bulk_scheduler_waits_for_both_node_labels(run_edge):
    edge = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    ready: set[str] = set()
    condition = Condition()
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1)
    result = []
    thread = Thread(target=lambda: result.append(_run_completion_driven_bulk_edges(
        (edge,), MagicMock(), args,
        parent_run_id="bulk-run", consumer_group_prefix="loader",
        ready_labels=ready, ready_condition=condition,
    )))
    thread.start()
    try:
        with condition:
            ready.add("Person")
            condition.notify_all()
        assert not run_edge.called
        with condition:
            ready.add("Company")
            condition.notify_all()
        thread.join(3)
    finally:
        with condition:
            ready.update(("Person", "Company"))
            condition.notify_all()
        thread.join(3)
    assert not thread.is_alive()
    assert result == [0]
    run_edge.assert_called_once()


def _gated_scheduler(run_edge, edges, args, **kwargs):
    """Start the scheduler with per-edge started/release events."""
    started = {edge.type: Event() for edge in edges}
    release = {edge.type: Event() for edge in edges}

    def run(edge, *_args, **_kwargs):
        started[edge.type].set()
        assert release[edge.type].wait(3)
        return 0

    run_edge.side_effect = run
    result = []
    thread = Thread(target=lambda: result.append(_run_completion_driven_bulk_edges(
        tuple(edges), MagicMock(), args,
        parent_run_id="bulk-run", consumer_group_prefix="loader", **kwargs,
    )))
    thread.start()
    return thread, started, release, result


def _finish(thread, release):
    for event in release.values():
        event.set()
    thread.join(3)
    assert not thread.is_alive()


def _bulk_args(**overrides):
    return argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1, **overrides)


def test_parser_bulk_edge_overlap_defaults_and_choices(capsys):
    args = build_parser().parse_args(["start", "--mode", "bulk"])
    assert (args.bulk_edge_overlap, args.max_concurrent_bulk_edges) == (None, None)  # resolved in _handle_start (classic-only flags)
    args = build_parser().parse_args([
        "start", "--mode", "bulk", "--bulk-edge-overlap", "shared", "--max-concurrent-bulk-edges", "3",
    ])
    assert (args.bulk_edge_overlap, args.max_concurrent_bulk_edges) == ("shared", 3)
    for bad in (["--bulk-edge-overlap", "none"], ["--max-concurrent-bulk-edges", "-1"]):
        with pytest.raises(SystemExit):
            build_parser().parse_args(["start", "--mode", "bulk", *bad])


@patch("src.cli._run_bulk_relationship_edge")
def test_bulk_scheduler_shared_overlap_starts_conflicting_types_concurrently(run_edge):
    edges = (
        _rotating_edge("A", "a", "Person", "Title"),
        _rotating_edge("B", "b", "Person", "Genre"),
        _rotating_edge("C", "c", "Title", "Genre"),
    )
    thread, started, release, result = _gated_scheduler(run_edge, edges, _bulk_args(bulk_edge_overlap="shared"))
    try:
        assert all(started[edge.type].wait(3) for edge in edges)
    finally:
        _finish(thread, release)
    assert result == [0]


@patch("src.cli._run_bulk_relationship_edge")
def test_bulk_scheduler_labels_overlap_explicitly_still_serializes_conflicts(run_edge):
    edges = (_rotating_edge("A", "a", "Person", "Title"), _rotating_edge("B", "b", "Person", "Genre"))
    thread, started, release, result = _gated_scheduler(run_edge, edges, _bulk_args(bulk_edge_overlap="labels"))
    try:
        assert started["A"].wait(3)
        assert not started["B"].wait(0.6)
        release["A"].set()
        assert started["B"].wait(3)
    finally:
        _finish(thread, release)
    assert result == [0]


@patch("src.cli._run_bulk_relationship_edge")
def test_bulk_scheduler_cap_bounds_running_types_and_prefers_heavier_edges(run_edge):
    edges = tuple(_rotating_edge(name, name.lower(), "Person", "Title") for name in ("A", "B", "C", "D"))
    thread, started, release, result = _gated_scheduler(
        run_edge, edges, _bulk_args(bulk_edge_overlap="shared", max_concurrent_bulk_edges=2),
        edge_weights={"C": 10, "D": 5},
    )
    try:
        assert started["C"].wait(3) and started["D"].wait(3)
        assert not started["A"].wait(0.6) and not started["B"].is_set()
        release["C"].set()
        assert started["A"].wait(3)
        assert not started["B"].wait(0.6)
        release["D"].set()
        assert started["B"].wait(3)
    finally:
        _finish(thread, release)
    assert result == [0]
    assert [call.args[0].type for call in run_edge.call_args_list] == ["C", "D", "A", "B"]


@patch("src.cli._run_bulk_relationship_edge")
def test_bulk_scheduler_shared_overlap_still_waits_for_endpoint_labels(run_edge):
    edges = (_rotating_edge("A", "a", "Person", "Title"), _rotating_edge("B", "b", "Person", "Genre"))
    ready: set[str] = {"Person", "Title"}
    condition = Condition()
    thread, started, release, result = _gated_scheduler(
        run_edge, edges, _bulk_args(bulk_edge_overlap="shared"),
        ready_labels=ready, ready_condition=condition,
    )
    try:
        assert started["A"].wait(3)
        assert not started["B"].wait(0.6)
        with condition:
            ready.add("Genre")
            condition.notify_all()
        assert started["B"].wait(3)
        assert not release["A"].is_set()
    finally:
        _finish(thread, release)
    assert result == [0]


@patch("src.cli._stop_exact_containers", return_value=[])
@patch("src.cli._run_bulk_relationship_edge")
def test_bulk_scheduler_shared_overlap_failure_cancels_running_and_pending(run_edge, stop):
    edges = (
        _rotating_edge("A", "a", "Person", "Title"),
        _rotating_edge("B", "b", "Person", "Genre"),
        _rotating_edge("C", "c", "Person", "Region"),
    )
    b_started, launched = Event(), {}

    def run(edge, *_args, cancelled, on_launch, **_kwargs):
        containers = (MagicMock(name=f"{edge.type}-0"),)
        launched[edge.type] = containers
        on_launch(edge.type, containers)
        if edge.type == "A":
            assert b_started.wait(3)
            return 1
        b_started.set()
        assert cancelled.wait(3)
        return 1

    run_edge.side_effect = run
    assert _run_completion_driven_bulk_edges(
        edges, MagicMock(), _bulk_args(bulk_edge_overlap="shared", max_concurrent_bulk_edges=2),
        parent_run_id="bulk-run", consumer_group_prefix="loader",
    ) == 1
    assert [call.args[0].type for call in run_edge.call_args_list] == ["A", "B"]
    stopped = [container for call in stop.call_args_list for container in call.args[0]]
    assert launched["B"][0] in stopped


@pytest.mark.parametrize("overrides", [
    {"bulk_edge_overlap": "none"}, {"max_concurrent_bulk_edges": -1}, {"max_concurrent_bulk_edges": True},
])
@patch("src.cli._run_bulk_relationship_edge", return_value=0)
def test_bulk_scheduler_rejects_invalid_overlap_policy_before_launch(run_edge, overrides):
    assert _run_completion_driven_bulk_edges(
        (_rotating_edge("A", "a", "Person", "Title"),), MagicMock(), _bulk_args(**overrides),
        parent_run_id="bulk-run", consumer_group_prefix="loader",
    ) == 1
    run_edge.assert_not_called()


@patch("src.cli._run_isolated_relationship_stream_phase")
def test_stream_orchestration_allows_one_isolated_edge_without_shared_clock(isolated_stream):
    knows = _rotating_edge("KNOWS", "knows", "Person", "Person")
    schema = SimpleNamespace(edges=[knows], loading=SimpleNamespace(consumer_group_id="loader"))
    args = argparse.Namespace(mode="stream", config="config.yaml", network="test-net", bulk_timeout_seconds=1)
    docker = MagicMock()
    isolated_stream.return_value = 0

    assert _run_relationship_orchestration(schema, docker, args, parent_run_id="stream-parent") == 0

    assert isolated_stream.call_args.args[:3] == (knows, docker, args)
    assert isolated_stream.call_args.kwargs["parent_run_id"] == "stream-parent"
    docker.run_global_batch_clock.assert_not_called()


@patch("src.cli.signal.signal", return_value=object())
@patch("src.cli.RelationshipBulkMonitor")
def test_isolated_stream_phase_uses_no_slot_clock_free_nonterminal_supervision(monitor_cls, _signal):
    knows = _rotating_edge("KNOWS", "knows", "Person", "Person")
    container, unrelated = MagicMock(), MagicMock()
    docker = MagicMock()
    docker.run_edge_loader.return_value = [container]
    monitor = MagicMock()
    monitor.supervise_stream.side_effect = lambda shutdown: shutdown.set()
    monitor_cls.return_value = monitor
    args = argparse.Namespace(mode="stream", config="config.yaml", network="test-net", bulk_timeout_seconds=1)

    assert _run_isolated_relationship_stream_phase(
        knows, docker, args, run_id="phase-run", phase_index=0,
        consumer_group_prefix="loader", parent_run_id="parent-run",
    ) == 0

    assert docker.run_edge_loader.call_args.kwargs == {
        "edge_type": "KNOWS", "topics": ["knows"], "mode": "stream", "config_path": "config.yaml",
        "replicas": 1, "network": "test-net", "run_id": "phase-run",
        "consumer_group_prefix": "loader", "slot_gating": False,
    }
    docker.run_global_batch_clock.assert_not_called()
    shutdown = monitor.supervise_stream.call_args.args[0]
    assert shutdown.is_set()
    monitor.capture_boundary.assert_not_called()
    monitor.wait_for_completion.assert_not_called()
    monitor.verify_zero_lag.assert_not_called()
    assert container.stop.call_count >= 1
    unrelated.stop.assert_not_called()
    monitor.close.assert_called_once_with()


def test_stream_orchestration_rejects_mixed_and_multiple_isolated_before_relationship_launch():
    works = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    knows = _rotating_edge("KNOWS", "knows", "Person", "Person")
    follows = _rotating_edge("FOLLOWS", "follows", "User", "User")
    args = argparse.Namespace(mode="stream", config="config.yaml", network="test-net", bulk_timeout_seconds=1)
    for edges in ([works, knows], [knows, follows]):
        docker = MagicMock()
        schema = SimpleNamespace(edges=edges, loading=SimpleNamespace(consumer_group_id="loader"))
        assert _run_relationship_orchestration(schema, docker, args, parent_run_id="stream-parent") == 1
        docker.run_edge_loader.assert_not_called()
        docker.run_global_batch_clock.assert_not_called()


def test_selected_rotating_fleet_rejects_self_reference_before_docker_launch():
    knows = _rotating_edge("KNOWS", "knows", "Person", "Person")
    schema = SimpleNamespace(
        edges=[knows],
        loading=SimpleNamespace(consumer_group_id="loader", coordination=SimpleNamespace(topic="clock", bucket_count=1)),
    )
    docker = MagicMock()
    args = argparse.Namespace(mode="bulk", config="config.yaml", network="test-net", bulk_timeout_seconds=1, edge_classic=True)

    assert _run_rotating_relationship_fleet(schema, docker, args, selected_edges=(knows,)) == 1

    docker.run_edge_loader.assert_not_called()
    docker.run_global_batch_clock.assert_not_called()


@patch("src.cli._run_lease_relationship_fleet")
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_handle_start_stream_generates_relationship_parent_run_id_when_omitted(
    mock_load, mock_driver_factory, mock_apply_schema, mock_docker, lease_fleet,
):
    """P5-S1 Story 6: --mode stream's default dispatch is now
    _run_lease_relationship_fleet, not the deprecated
    _run_relationship_orchestration (see the --stream-clock-rotation
    regression pin below for that path)."""
    knows = _rotating_edge("KNOWS", "knows", "Person", "Person")
    mock_load.return_value = SimpleNamespace(nodes=(), edges=[knows])
    mock_driver_factory.return_value = MagicMock()
    mock_docker.return_value = MagicMock()
    lease_fleet.return_value = 0
    args = argparse.Namespace(mode="stream", config="config.yaml", network="test-net", skip_image_build=True)

    assert handle_start(args) == 0

    parent_run_id = lease_fleet.call_args.kwargs["parent_run_id"]
    assert isinstance(parent_run_id, str) and len(parent_run_id) == 32
    assert lease_fleet.call_args.args[:3] == (mock_load.return_value, mock_docker.return_value, args)


@patch("src.cli.RelationshipBulkMonitor")
def test_rotating_fleet_launches_every_eligible_type_before_clock_and_monitor(monitor_cls):
    works = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    bought = _rotating_edge("BOUGHT", "bought", "Person", "Product", replicas=2)
    schema = SimpleNamespace(
        edges=[works, bought],
        loading=SimpleNamespace(
            consumer_group_id="loader",
            coordination=SimpleNamespace(topic="clock-topic", bucket_count=2),
        ),
    )
    first, second, third, clock = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    docker = MagicMock()
    docker.run_edge_loader.side_effect = [[second, third], [first]]
    docker.run_global_batch_clock.return_value = clock
    monitor = MagicMock(); monitor_cls.return_value = monitor
    monitor.wait_for_drain_complete.side_effect = lambda _timeout: (
        first.stop.assert_called(), second.stop.assert_called(), third.stop.assert_called(), clock.stop.assert_not_called()
    )
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1)

    assert _run_rotating_relationship_fleet(schema, docker, args) == 0

    assert [call.kwargs["edge_type"] for call in docker.run_edge_loader.call_args_list] == ["BOUGHT", "WORKS_AT"]
    shared_run_ids = {call.kwargs["run_id"] for call in docker.run_edge_loader.call_args_list}
    assert len(shared_run_ids) == 1
    for call in docker.run_edge_loader.call_args_list:
        assert call.kwargs["slot_gating"] is True
        assert call.kwargs["fleet_edge_types"] == "BOUGHT,WORKS_AT"
        assert call.kwargs["coordination_topic"] == "clock-topic"
    clock_call = docker.run_global_batch_clock.call_args.kwargs
    assert clock_call["run_id"] == shared_run_ids.pop()
    assert clock_call["fleet_edge_types"] == "BOUGHT,WORKS_AT"
    assert clock_call["coordination_topic"] == "clock-topic"
    monitor_cls.assert_called_once()
    assert monitor.wait_for_assignment_coverage.call_args.args == (1,)
    assert monitor.wait_for_clock_lease.call_args.args == (1,)
    assert monitor.wait_for_rotating_completion.call_args.args == (1,)
    monitor.wait_for_completion.assert_not_called()
    assert first.stop.call_count >= 1 and second.stop.call_count >= 1 and third.stop.call_count >= 1
    assert clock.stop.call_count >= 1


@patch("src.cli.RelationshipBulkMonitor")
def test_rotating_fleet_clock_launch_failure_does_not_launch_edges(monitor_cls):
    works = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    schema = SimpleNamespace(
        edges=[works],
        loading=SimpleNamespace(
            consumer_group_id="loader",
            coordination=SimpleNamespace(topic="clock-topic", bucket_count=1),
        ),
    )
    edge_container = MagicMock()
    docker = MagicMock()
    docker.run_edge_loader.return_value = [edge_container]
    docker.run_global_batch_clock.side_effect = RuntimeError("clock launch failed")
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1)

    assert _run_rotating_relationship_fleet(schema, docker, args) == 1

    monitor_cls.assert_not_called()
    edge_container.stop.assert_not_called()
    assert [call[0] for call in docker.mock_calls].count("run_edge_loader") == 0


def test_rotating_fleet_defers_self_referencing_edges_without_launching_them():
    knows = _rotating_edge("KNOWS", "knows", "Person", "Person")
    schema = SimpleNamespace(
        edges=[knows],
        loading=SimpleNamespace(
            consumer_group_id="loader",
            coordination=SimpleNamespace(topic="clock-topic", bucket_count=1),
        ),
    )
    docker = MagicMock()
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1)

    assert _run_rotating_relationship_fleet(schema, docker, args) == 1
    docker.run_edge_loader.assert_not_called()
    docker.run_global_batch_clock.assert_not_called()


def test_rotating_fleet_generic_edge_launch_failure_names_stage_run_and_edge(caplog):
    works = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    schema = SimpleNamespace(
        edges=[works],
        loading=SimpleNamespace(
            consumer_group_id="loader",
            coordination=SimpleNamespace(topic="clock-topic", bucket_count=1),
        ),
    )
    docker = MagicMock()
    docker.run_edge_loader.side_effect = RuntimeError("docker unavailable")
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1)

    assert _run_rotating_relationship_fleet(schema, docker, args) == 1
    assert "stage=launch run_id=" in caplog.text
    assert "edge=WORKS_AT reason=docker unavailable" in caplog.text


def test_rotating_fleet_generic_clock_launch_failure_names_clock_stage_and_run(caplog):
    works = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    schema = SimpleNamespace(
        edges=[works],
        loading=SimpleNamespace(
            consumer_group_id="loader",
            coordination=SimpleNamespace(topic="clock-topic", bucket_count=1),
        ),
    )
    docker = MagicMock()
    docker.run_edge_loader.return_value = [MagicMock()]
    docker.run_global_batch_clock.side_effect = RuntimeError("clock unavailable")
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1)

    assert _run_rotating_relationship_fleet(schema, docker, args) == 1
    assert "stage=clock run_id=" in caplog.text
    assert "reason=launch failed: clock unavailable" in caplog.text


@patch("src.cli.RelationshipBulkMonitor")
def test_rotating_fleet_cleanup_failure_names_exact_edge_and_replica(monitor_cls, caplog):
    class FailingEdgeContainer:
        name = "opaque-container"
        attrs = {"State": {"Status": "running"}}

        def reload(self):
            return None

        def stop(self):
            raise RuntimeError("stop denied")

    works = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    schema = SimpleNamespace(
        edges=[works],
        loading=SimpleNamespace(
            consumer_group_id="loader",
            coordination=SimpleNamespace(topic="clock-topic", bucket_count=1),
        ),
    )
    edge = FailingEdgeContainer()
    docker = MagicMock()
    docker.run_edge_loader.return_value = [edge]
    docker.run_global_batch_clock.return_value = MagicMock()
    monitor = MagicMock(); monitor.wait_for_assignment_coverage.side_effect = RuntimeError("monitor failed")
    monitor_cls.return_value = monitor
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1)

    assert _run_rotating_relationship_fleet(schema, docker, args) == 1
    assert "stage=shutdown run_id=" in caplog.text
    assert "edge=WORKS_AT replica=0: stop denied" in caplog.text


@patch("src.cli.signal.signal", return_value=object())
@patch("src.cli.RelationshipBulkMonitor")
def test_stream_fleet_supervises_without_bulk_completion_or_zero_lag(monitor_cls, _signal):
    works = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    schema = SimpleNamespace(
        edges=[works],
        loading=SimpleNamespace(
            consumer_group_id="loader",
            coordination=SimpleNamespace(topic="clock-topic", bucket_count=1, lease_timeout_ms=20),
        ),
    )
    edge, clock = MagicMock(), MagicMock()
    docker = MagicMock()
    docker.run_edge_loader.return_value = [edge]
    docker.run_global_batch_clock.return_value = clock
    monitor = MagicMock(); monitor_cls.return_value = monitor
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1, mode="stream", run_id="stream-run")

    assert _run_rotating_relationship_fleet(schema, docker, args) == 0
    assert docker.run_edge_loader.call_args.kwargs["run_id"] == "stream-run"
    assert docker.run_global_batch_clock.call_args.kwargs["run_id"] == "stream-run"
    assert monitor_cls.call_args.args[1] == "stream-run"
    assert monitor.supervise_stream.call_args.kwargs["initial_lease_timeout_seconds"] == pytest.approx(0.02)
    monitor.wait_for_assignment_coverage.assert_not_called()
    monitor.capture_boundary.assert_not_called()
    monitor.wait_for_rotating_completion.assert_not_called()
    monitor.wait_for_drain_complete.assert_not_called()
    monitor.verify_zero_lag.assert_not_called()
    assert clock.stop.call_count >= 1 and edge.stop.call_count >= 1


# -- P5-S1 Story 6: unified lease fleet, streaming's default --------------

def _lease_fleet_schema(edges):
    return SimpleNamespace(
        edges=list(edges),
        nodes=(),
        loading=SimpleNamespace(
            consumer_group_id="loader",
            bucket_lease=SimpleNamespace(tx_timeout_ms=30_000, restart_quiet_margin_ms=5_000),
        ),
    )


@patch("src.cli.RelationshipBulkMonitor")
def test_lease_fleet_launches_every_type_including_self_reference_concurrently(monitor_cls):
    """AC: a mix of a conflicting-label type, a non-conflicting type, and a
    self-referencing type, run with --mode stream and no extra flags,
    launches one lease server and every edge loader concurrently, with no
    rejection and no isolated phase."""
    works = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    bought = _rotating_edge("BOUGHT", "bought", "Person", "Product")
    knows = _rotating_edge("KNOWS", "knows", "Person", "Person")  # self-referencing
    schema = _lease_fleet_schema([works, bought, knows])
    lease, edge_a, edge_b, edge_c = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    docker = MagicMock()
    docker.run_bucket_lease_server.return_value = lease
    docker.bucket_lease_address.return_value = "graph-loader-lease-x:7700"
    docker.run_edge_loader.side_effect = [[edge_a], [edge_b], [edge_c]]
    monitor = MagicMock(); monitor_cls.return_value = monitor
    monitor.tracked_lease_server = lease
    args = argparse.Namespace(
        config="config.yaml", network="test-net", lease_buckets=16,
        lease_outer_batch_size=32_000, lease_keyed_max_nodes=1_000,
    )

    assert _run_lease_relationship_fleet(schema, docker, args, parent_run_id="run-lease") == 0

    docker.run_bucket_lease_server.assert_called_once()
    assert docker.run_bucket_lease_server.call_args.kwargs["run_id"] == "run-lease"
    assert docker.bucket_lease_address.call_args.kwargs["prefer_name"] is True
    launched_types = {call.kwargs["edge_type"] for call in docker.run_edge_loader.call_args_list}
    assert launched_types == {"WORKS_AT", "BOUGHT", "KNOWS"}
    for call in docker.run_edge_loader.call_args_list:
        assert call.kwargs["mode"] == "stream"
        assert call.kwargs["lease_address"] == "graph-loader-lease-x:7700"
        assert "slot_gating" not in call.kwargs or not call.kwargs["slot_gating"]
        assert "coordination_topic" not in call.kwargs
        assert "fleet_edge_types" not in call.kwargs
    monitor_cls.assert_called_once()
    assert monitor_cls.call_args.kwargs["tracked_lease_server"] is lease
    monitor.wait_for_lease_server_ready.assert_called_once()
    monitor.supervise_stream.assert_called_once()
    assert edge_a.stop.call_count >= 1 and edge_b.stop.call_count >= 1 and edge_c.stop.call_count >= 1


# ── P5-S4 Story 6: retry-consumer fleet wiring ──────────────────────────────


def _rotating_edge_with_dead_letter(edge_type, topic, source, target, dead_letter):
    return SimpleNamespace(
        type=edge_type, topic=topic, topics=[topic], dead_letter_entity=topic, is_multi_topic=False, replicas=1,
        endpoint_labels=frozenset({source, target}), has_self_reference=source == target, is_multi_pair=False,
        nodes=SimpleNamespace(source=source, target=target, is_self_referencing=source == target),
        dead_letter=dead_letter,
    )


@patch("src.cli.RelationshipBulkMonitor")
def test_lease_fleet_launches_retry_consumer_after_lease_server_before_edge_loaders(monitor_cls):
    """A schema with at least one DLQ-and-retry-enabled edge type launches
    exactly one retry-consumer replica (Decisions §4's launch order: lease
    server, then retry consumer, then edge loaders), subscribed to every
    resolved DLQ topic."""
    works = _rotating_edge_with_dead_letter(
        "WORKS_AT", "works", "Person", "Company", DeadLetterConfig(enabled=True),
    )
    knows = _rotating_edge_with_dead_letter(
        "KNOWS", "knows", "Person", "Person", DeadLetterConfig(enabled=False),
    )
    schema = _lease_fleet_schema([works, knows])
    lease, retry, edge_a, edge_b = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    docker = MagicMock()
    docker.run_bucket_lease_server.return_value = lease
    docker.bucket_lease_address.return_value = "graph-loader-lease-x:7700"
    docker.run_retry_consumer.return_value = [retry]
    monitor = MagicMock(); monitor_cls.return_value = monitor
    monitor.tracked_lease_server = lease
    call_order: list[str] = []
    docker.run_bucket_lease_server.side_effect = lambda **_k: (call_order.append("lease"), lease)[1]
    docker.run_retry_consumer.side_effect = lambda **_k: (call_order.append("retry"), [retry])[1]
    edge_results = iter([[edge_a], [edge_b]])
    docker.run_edge_loader.side_effect = lambda **_k: (call_order.append("edge"), next(edge_results))[1]
    args = argparse.Namespace(
        config="config.yaml", network="test-net", lease_buckets=16,
        lease_outer_batch_size=32_000, lease_keyed_max_nodes=1_000, retry_consumer_replicas=1,
    )

    assert _run_lease_relationship_fleet(schema, docker, args, parent_run_id="run-lease") == 0

    docker.run_retry_consumer.assert_called_once()
    retry_call = docker.run_retry_consumer.call_args.kwargs
    assert retry_call["run_id"] == "run-lease"
    assert retry_call["lease_address"] == "graph-loader-lease-x:7700"
    assert retry_call["dlq_topics"] == ["works.dlq"]  # only the DLQ-enabled type's resolved topic
    assert retry_call["replicas"] == 1
    assert call_order[0] == "lease" and call_order[1] == "retry"
    assert retry.stop.call_count >= 1  # stopped in the shutdown pass


@patch("src.cli.RelationshipBulkMonitor")
def test_lease_fleet_supervises_retry_consumers_and_a_retry_crash_stops_the_fleet_rc_1(monitor_cls, caplog):
    """Owner decision D2: the retry containers are handed to stream
    supervision; when supervision reports one exited, every container the
    fleet started is stopped and the run returns 1 with a stage= ERROR naming
    the retry consumer."""
    from src.orchestrator.bulk_monitor import LoaderCrashError

    works = _rotating_edge_with_dead_letter(
        "WORKS_AT", "works", "Person", "Company", DeadLetterConfig(enabled=True),
    )
    schema = _lease_fleet_schema([works])
    lease, retry, edge = MagicMock(), MagicMock(), MagicMock()
    docker = MagicMock()
    docker.run_bucket_lease_server.return_value = lease
    docker.bucket_lease_address.return_value = "graph-loader-lease-x:7700"
    docker.run_retry_consumer.return_value = [retry]
    docker.run_edge_loader.return_value = [edge]
    monitor = MagicMock(); monitor_cls.return_value = monitor
    monitor.tracked_lease_server = lease
    monitor.supervise_stream.side_effect = LoaderCrashError(
        "stage=retry-consumer run_id=run-lease replica=0 retry consumer exited during stream supervision "
        "status=exited exit_code=1 -- stopping the stream fleet"
    )
    args = argparse.Namespace(
        config="config.yaml", network="test-net", lease_buckets=16,
        lease_outer_batch_size=32_000, lease_keyed_max_nodes=1_000, retry_consumer_replicas=1,
    )

    assert _run_lease_relationship_fleet(schema, docker, args, parent_run_id="run-lease") == 1

    assert monitor_cls.call_args.kwargs["tracked_retry_consumers"] == [retry]
    assert retry.stop.call_count >= 1 and edge.stop.call_count >= 1 and lease.stop.call_count >= 1
    assert "stage=retry-consumer run_id=run-lease replica=0 retry consumer exited" in caplog.text


@patch("src.cli.RelationshipBulkMonitor")
def test_lease_fleet_launches_zero_retry_consumers_when_no_edge_type_has_dlq_enabled(monitor_cls):
    works = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    schema = _lease_fleet_schema([works])
    lease, edge = MagicMock(), MagicMock()
    docker = MagicMock()
    docker.run_bucket_lease_server.return_value = lease
    docker.bucket_lease_address.return_value = "graph-loader-lease-x:7700"
    docker.run_edge_loader.return_value = [edge]
    monitor = MagicMock(); monitor_cls.return_value = monitor
    monitor.tracked_lease_server = lease
    args = argparse.Namespace(
        config="config.yaml", network="test-net", lease_buckets=16,
        lease_outer_batch_size=32_000, lease_keyed_max_nodes=1_000,
    )

    assert _run_lease_relationship_fleet(schema, docker, args, parent_run_id="run-lease") == 0

    docker.run_retry_consumer.assert_not_called()


def test_lease_fleet_stops_edges_before_the_current_lease_server(monkeypatch):
    """B6-1: on shutdown, edge loaders must be stopped (and thus receive
    their own SIGTERM) BEFORE the lease server disappears from under
    them -- the reverse order degrades a clean drain into WRITE_FAILUREs
    and Docker SIGKILLs. Also covers S6-1: the container stopped is
    whichever one the monitor currently tracks (a relaunched one), not
    the one originally launched."""
    works = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    schema = _lease_fleet_schema([works])
    original_lease, relaunched_lease, edge = MagicMock(), MagicMock(), MagicMock()
    docker = MagicMock()
    docker.run_bucket_lease_server.return_value = original_lease
    docker.bucket_lease_address.return_value = "graph-loader-lease-x:7700"
    docker.run_edge_loader.return_value = [edge]
    order: list[str] = []
    edge.stop.side_effect = lambda **_k: order.append("edge-stop")
    relaunched_lease.stop.side_effect = lambda **_k: order.append("lease-stop")
    relaunched_lease.logs.return_value = b""
    args = argparse.Namespace(
        config="config.yaml", network="test-net", lease_buckets=16,
        lease_outer_batch_size=32_000, lease_keyed_max_nodes=1_000,
    )
    with patch("src.cli.RelationshipBulkMonitor") as monitor_cls:
        monitor = MagicMock(); monitor_cls.return_value = monitor
        # The supervisor relaunched the lease server during the run -- the
        # object shutdown must target is the *current* one, not the
        # original_lease this test never expects to be stopped directly.
        monitor.tracked_lease_server = relaunched_lease
        assert _run_lease_relationship_fleet(schema, docker, args, parent_run_id="run-lease") == 0
    # Idempotent double-stops (main path + finally) are expected and
    # harmless; what matters is the *first* lease-stop never precedes the
    # *first* edge-stop.
    assert order.index("edge-stop") < order.index("lease-stop")
    original_lease.stop.assert_not_called()


def test_lease_fleet_edge_launch_failure_stops_lease_server_and_edges():
    works = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    schema = _lease_fleet_schema([works])
    lease = MagicMock()
    docker = MagicMock()
    docker.run_bucket_lease_server.return_value = lease
    docker.bucket_lease_address.return_value = "graph-loader-lease-x:7700"
    docker.run_edge_loader.side_effect = RuntimeError("no image")
    args = argparse.Namespace(
        config="config.yaml", network="test-net", lease_buckets=16,
        lease_outer_batch_size=32_000, lease_keyed_max_nodes=1_000,
    )

    assert _run_lease_relationship_fleet(schema, docker, args, parent_run_id="run-lease") == 1
    assert lease.stop.call_count >= 1


def test_lease_fleet_rejects_edge_leases_flag_combined_with_stream():
    """AC: --edge-leases combined with --mode stream (not
    --stream-clock-rotation) is rejected with a clear error distinguishing
    it from the new leases-by-default streaming path."""
    schema = _lease_fleet_schema([_rotating_edge("WORKS_AT", "works", "Person", "Company")])
    args = argparse.Namespace(mode="stream", edge_leases=True)
    from src.cli import _validate_edge_lease_settings

    with pytest.raises(ValueError, match="--edge-leases requires --mode bulk"):
        _validate_edge_lease_settings(schema, args)


def test_stream_clock_rotation_deprecated_flag_uses_old_dispatch(caplog):
    """AC regression pin: --mode stream --stream-clock-rotation reproduces
    today's dispatch (through _run_relationship_orchestration), byte for
    byte, logging the deprecation exactly once."""
    with patch("src.cli._run_relationship_orchestration", return_value=0) as orchestration, \
         patch("src.cli._run_lease_relationship_fleet") as lease_fleet, \
         patch("src.cli.DockerService") as mock_docker, \
         patch("src.cli.apply_schema"), \
         patch("src.cli.get_neo4j_driver") as mock_driver, \
         patch("src.cli.load_schema") as mock_load:
        knows = _rotating_edge("KNOWS", "knows", "Person", "Person")
        mock_load.return_value = SimpleNamespace(nodes=(), edges=[knows])
        mock_driver.return_value = MagicMock()
        mock_docker.return_value = MagicMock()
        args = argparse.Namespace(
            mode="stream", config="config.yaml", network="test-net", skip_image_build=True,
            stream_clock_rotation=True,
        )
        with caplog.at_level("WARNING"):
            assert handle_start(args) == 0
        orchestration.assert_called_once()
        lease_fleet.assert_not_called()
        deprecation_lines = [
            r for r in caplog.records if "stage=deprecation" in r.message and "--stream-clock-rotation" in r.message
        ]
        assert len(deprecation_lines) == 1


@patch("src.cli.DockerService")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_handle_start_invalid_run_id_rejects_before_docker_launch(mock_load, mock_driver, mock_docker):
    mock_load.return_value = SimpleNamespace(nodes=(), edges=())
    args = argparse.Namespace(mode="stream", config="config.yaml", network="test-net", run_id="not valid!", skip_image_build=True)

    assert handle_start(args) == 1
    mock_docker.assert_not_called()
    mock_driver.assert_not_called()


@patch("src.cli.signal.signal", return_value=object())
@patch("src.cli.RelationshipBulkMonitor")
def test_stream_supervisor_failure_stops_exact_clock_before_edge(monitor_cls, _signal):
    works = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    schema = SimpleNamespace(
        edges=[works],
        loading=SimpleNamespace(
            consumer_group_id="loader",
            coordination=SimpleNamespace(topic="clock-topic", bucket_count=1, lease_timeout_ms=20),
        ),
    )
    events = []
    edge, clock = MagicMock(), MagicMock()
    edge.stop.side_effect = lambda: events.append("edge")
    clock.stop.side_effect = lambda: events.append("clock")
    docker = MagicMock()
    docker.run_edge_loader.return_value = [edge]
    docker.run_global_batch_clock.return_value = clock
    monitor = MagicMock(); monitor.supervise_stream.side_effect = RuntimeError("clock lease expired")
    monitor_cls.return_value = monitor
    args = argparse.Namespace(config="config.yaml", network="test-net", bulk_timeout_seconds=1, mode="stream", run_id="stream-run")

    assert _run_rotating_relationship_fleet(schema, docker, args) == 1
    assert events[:2] == ["clock", "edge"]


@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_handle_start_success(mock_load, mock_get_driver, mock_apply_schema, mock_docker):
    args = argparse.Namespace(mode="stream", config="config.yaml", network="test_net")
    mock_driver = MagicMock()
    mock_get_driver.return_value = mock_driver
    mock_schema = MagicMock()
    mock_node = SimpleNamespace(label="Person", topic="person_topic", replicas=1)
    mock_schema.nodes = [mock_node]
    mock_load.return_value = mock_schema

    mock_docker_instance = MagicMock()
    mock_docker_instance.run_node_loader.return_value = [MagicMock()]
    mock_docker.return_value = mock_docker_instance

    exit_code = handle_start(args)

    assert exit_code == 0
    mock_load.assert_called_once_with("config.yaml")
    mock_get_driver.assert_called_once()
    mock_apply_schema.assert_called_once()
    mock_driver.close.assert_called_once()

    mock_docker_instance.build_image.assert_called_once()
    mock_docker_instance.run_node_loader.assert_called_once_with(
        node_label="Person",
        topic="person_topic",
        mode="stream",
        config_path="config.yaml",
        replicas=1,
        network="test_net",
    )


# ── P6-S5 Story 1/7: node-loader containers are stopped on every stream exit ─


def _stream_two_label_schema():
    works = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    return SimpleNamespace(
        nodes=[
            SimpleNamespace(label="Person", topic="person", replicas=1),
            SimpleNamespace(label="Company", topic="company", replicas=1),
        ],
        edges=[works],
        loading=SimpleNamespace(
            consumer_group_id="loader",
            bucket_lease=SimpleNamespace(tx_timeout_ms=30_000, restart_quiet_margin_ms=5_000),
        ),
    )


@pytest.mark.parametrize("fleet_result", [0, 1])
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_stream_relationship_stop_stops_node_containers_whatever_the_result(
    mock_load, mock_get_driver, mock_apply_schema, mock_docker, fleet_result,
):
    """P5-S1's default streaming path already stops its own edge/lease
    containers; `_handle_start` must also stop the node-loader containers
    *it* launched on a graceful stop (result 0) and on a failure alike."""
    mock_load.return_value = _stream_two_label_schema()
    mock_get_driver.return_value = MagicMock()
    person_container, company_container = MagicMock(), MagicMock()
    docker = MagicMock()
    docker.run_node_loader.side_effect = lambda node_label, **_kwargs: {
        "Person": [person_container], "Company": [company_container],
    }[node_label]
    mock_docker.return_value = docker
    args = argparse.Namespace(mode="stream", config="config.yaml", network="test-net")

    with patch("src.cli._run_lease_relationship_fleet", return_value=fleet_result):
        assert handle_start(args) == fleet_result
    person_container.stop.assert_called_once()
    company_container.stop.assert_called_once()


# ── Metrics instrumentation (P6-S2 Story 8) ──────────────────────────────────


def _free_metrics_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _assert_port_closed(port: int) -> None:
    import socket

    with pytest.raises(ConnectionRefusedError):
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            pass


@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_handle_start_stream_leaves_cli_metrics_port_closed_after_return(
    mock_load, mock_get_driver, mock_apply_schema, mock_docker, monkeypatch,
):
    """P6-S2 Story 8: the CLI's own metrics server does not outlive
    handle_start() - port unbound immediately after it returns, on the
    bulk-mode-free stream success path."""
    port = _free_metrics_port()
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    monkeypatch.setenv("GRAPH_LOADER_METRICS_PORT", str(port))
    args = argparse.Namespace(mode="stream", config="config.yaml", network="test_net")
    mock_get_driver.return_value = MagicMock()
    mock_schema = MagicMock()
    mock_schema.nodes = [SimpleNamespace(label="Person", topic="person_topic", replicas=1)]
    mock_load.return_value = mock_schema
    mock_docker_instance = MagicMock()
    mock_docker_instance.run_node_loader.return_value = [MagicMock()]
    mock_docker.return_value = mock_docker_instance

    assert handle_start(args) == 0
    _assert_port_closed(port)


@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_handle_start_bulk_leaves_cli_metrics_port_closed_after_return(
    mock_load, mock_get_driver, mock_apply_schema, mock_docker, monkeypatch,
):
    port = _free_metrics_port()
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    monkeypatch.setenv("GRAPH_LOADER_METRICS_PORT", str(port))
    args = argparse.Namespace(mode="bulk", config="config.yaml", network="test_net", run_id="bulk-run-1", edge_classic=True)
    mock_get_driver.return_value = MagicMock()
    mock_schema = MagicMock()
    mock_schema.nodes = [SimpleNamespace(label="Person", topic="person_topic", replicas=1)]
    mock_schema.edges = ()
    mock_load.return_value = mock_schema
    mock_docker_instance = MagicMock()
    mock_docker_instance.run_node_loader.return_value = [MagicMock()]
    mock_docker.return_value = mock_docker_instance

    # Bulk mode drives real Kafka topic-metadata lookups this fixture
    # doesn't mock, so it fails closed here - which is fine, and itself
    # doubles as another early-return-path proof: the port must close
    # either way, success or failure.
    handle_start(args)
    _assert_port_closed(port)


@patch("src.cli._run_lease_relationship_fleet", return_value=0)
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_handle_start_fleet_stream_with_edges_leaves_cli_metrics_port_closed(
    mock_load, mock_driver_factory, mock_apply_schema, mock_docker, lease_fleet, monkeypatch,
):
    """Covers the fleet-wide stream path (with edges, driving
    _run_lease_relationship_fleet -> ... -> supervise_stream, P5-S1
    Story 6's new default) - a mocked lease_fleet returning 0 stands in
    for supervise_stream returning normally after a SIGTERM sets
    `shutdown`, which is indistinguishable from this function's own
    perspective."""
    port = _free_metrics_port()
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    monkeypatch.setenv("GRAPH_LOADER_METRICS_PORT", str(port))
    knows = _rotating_edge("KNOWS", "knows", "Person", "Person")
    mock_load.return_value = SimpleNamespace(nodes=(), edges=[knows])
    mock_driver_factory.return_value = MagicMock()
    mock_docker.return_value = MagicMock()
    args = argparse.Namespace(mode="stream", config="config.yaml", network="test-net", skip_image_build=True)

    assert handle_start(args) == 0
    _assert_port_closed(port)


@patch("src.cli.load_schema")
def test_handle_start_schema_load_failure_still_leaves_cli_metrics_port_closed(mock_load, monkeypatch):
    """Early-return coverage: the metrics server is started and its cleanup
    registered before schema load even runs, so a schema-load failure (the
    very first possible early return) still leaves the port closed."""
    port = _free_metrics_port()
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    monkeypatch.setenv("GRAPH_LOADER_METRICS_PORT", str(port))
    mock_load.side_effect = Exception("boom")
    args = argparse.Namespace(mode="stream", config="bad.yaml", network="test_net")

    assert handle_start(args) == 1
    _assert_port_closed(port)


@patch("src.cli.load_schema")
def test_handle_start_cli_metrics_disabled_binds_nothing(mock_load, monkeypatch):
    monkeypatch.setenv("GRAPH_LOADER_METRICS_ENABLED", "0")
    mock_load.side_effect = Exception("boom")
    args = argparse.Namespace(mode="stream", config="bad.yaml", network="test_net")

    from src.utils import metrics as metrics_module

    with patch.object(metrics_module, "start_http_server") as start_http:
        assert handle_start(args) == 1
        start_http.assert_not_called()


@patch("src.cli.load_schema")
def test_handle_start_cli_metrics_binds_127_0_0_1_by_default(mock_load, monkeypatch):
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    monkeypatch.delenv("GRAPH_LOADER_METRICS_PORT", raising=False)
    monkeypatch.delenv("GRAPH_LOADER_METRICS_BIND_ADDR", raising=False)
    mock_load.side_effect = Exception("boom")
    args = argparse.Namespace(mode="stream", config="bad.yaml", network="test_net")

    from src.utils import metrics as metrics_module

    captured = {}

    def fake_start_http_server(port, addr, registry):
        captured["addr"] = addr
        server = MagicMock()
        server.server_port = port
        return server, MagicMock()

    with patch.object(metrics_module, "start_http_server", side_effect=fake_start_http_server):
        assert handle_start(args) == 1
    assert captured["addr"] == "127.0.0.1"


@patch("src.cli.load_schema")
def test_handle_start_cli_metrics_bind_addr_override_is_honored_and_logged(mock_load, monkeypatch, caplog):
    """P6-S5, found during P6-S3 (not P6-S5's own Story 6, a separate,
    still-open item - see plan/v6/slice-5/stories.md):
    GRAPH_LOADER_METRICS_BIND_ADDR is an explicit, off-by-default opt-in
    - unset (or "127.0.0.1") keeps today's pure loopback-only bind
    (previous test), but a non-default value is honored exactly, and
    logged at WARNING since it exposes an unauthenticated endpoint
    beyond localhost."""
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    monkeypatch.delenv("GRAPH_LOADER_METRICS_PORT", raising=False)
    monkeypatch.setenv("GRAPH_LOADER_METRICS_BIND_ADDR", "0.0.0.0")
    mock_load.side_effect = Exception("boom")
    args = argparse.Namespace(mode="stream", config="bad.yaml", network="test_net")

    from src.utils import metrics as metrics_module

    captured = {}

    def fake_start_http_server(port, addr, registry):
        captured["addr"] = addr
        server = MagicMock()
        server.server_port = port
        return server, MagicMock()

    with patch.object(metrics_module, "start_http_server", side_effect=fake_start_http_server), \
         caplog.at_level("WARNING"):
        assert handle_start(args) == 1
    assert captured["addr"] == "0.0.0.0"
    assert any(
        "reason=cli_metrics_bind_addr_overridden" in record.message and "addr=0.0.0.0" in record.message
        for record in caplog.records
    )


@patch("src.cli.load_schema")
def test_handle_start_cli_metrics_bind_addr_empty_string_treated_as_unset(mock_load, monkeypatch, caplog):
    """Review finding on the bind-address opt-in: an exported-but-empty
    GRAPH_LOADER_METRICS_BIND_ADDR="" must not silently become "bind
    every interface" - it is treated exactly like the variable being
    unset, keeping the loopback-only default, and no override warning
    is logged."""
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    monkeypatch.delenv("GRAPH_LOADER_METRICS_PORT", raising=False)
    monkeypatch.setenv("GRAPH_LOADER_METRICS_BIND_ADDR", "")
    mock_load.side_effect = Exception("boom")
    args = argparse.Namespace(mode="stream", config="bad.yaml", network="test_net")

    from src.utils import metrics as metrics_module

    captured = {}

    def fake_start_http_server(port, addr, registry):
        captured["addr"] = addr
        server = MagicMock()
        server.server_port = port
        return server, MagicMock()

    with patch.object(metrics_module, "start_http_server", side_effect=fake_start_http_server), \
         caplog.at_level("WARNING"):
        assert handle_start(args) == 1
    assert captured["addr"] == "127.0.0.1"
    assert not any("reason=cli_metrics_bind_addr_overridden" in record.message for record in caplog.records)


@patch("src.cli.load_schema")
def test_handle_start_cli_metrics_falls_back_to_ephemeral_port_and_logs_it(mock_load, monkeypatch, caplog):
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    blocker_port = _free_metrics_port()
    monkeypatch.setenv("GRAPH_LOADER_METRICS_PORT", str(blocker_port))
    mock_load.side_effect = Exception("boom")
    args = argparse.Namespace(mode="stream", config="bad.yaml", network="test_net")

    import socket

    blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    blocker.bind(("127.0.0.1", blocker_port))
    blocker.listen(1)
    try:
        with caplog.at_level("WARNING"):
            assert handle_start(args) == 1
        fallback_lines = [
            record.message for record in caplog.records
            if "reason=port_in_use" in record.message
        ]
        assert fallback_lines and f"requested_port={blocker_port}" in fallback_lines[0]
        assert "fallback_port=" in fallback_lines[0]
        assert f"fallback_port={blocker_port}" not in fallback_lines[0]
    finally:
        blocker.close()


@patch("src.cli.load_schema")
def test_handle_start_writes_cli_snapshot_when_timing_dir_set(mock_load, monkeypatch, tmp_path):
    """P6-S2 Story 9/10: the CLI's own snapshot (metrics-cli.prom) is the
    one snapshot path that bypasses GRAPH_LOADER_METRICS_SNAPSHOT_PATH -
    it goes straight to $GRAPH_LOADER_TIMING_DIR when that host env var is
    set, and is absent when it isn't."""
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    monkeypatch.setenv("GRAPH_LOADER_TIMING_DIR", str(tmp_path))
    mock_load.side_effect = Exception("boom")
    args = argparse.Namespace(mode="stream", config="bad.yaml", network="test_net")

    assert handle_start(args) == 1

    snapshot_path = tmp_path / "metrics-cli.prom"
    assert snapshot_path.exists()
    assert 'graph_loader_process_up{kind="cli",name="-"} 1.0' in snapshot_path.read_text()


@patch("src.cli.load_schema")
def test_handle_start_no_cli_snapshot_when_timing_dir_unset(mock_load, monkeypatch, tmp_path):
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    monkeypatch.delenv("GRAPH_LOADER_TIMING_DIR", raising=False)
    mock_load.side_effect = Exception("boom")
    args = argparse.Namespace(mode="stream", config="bad.yaml", network="test_net")

    assert handle_start(args) == 1
    assert not (tmp_path / "metrics-cli.prom").exists()


@patch("src.cli.load_schema")
def test_handle_start_cli_snapshot_write_failure_does_not_change_exit_code(mock_load, monkeypatch, tmp_path):
    """A read-only (or otherwise unwritable) GRAPH_LOADER_TIMING_DIR must
    not change handle_start's exit code - matching every other
    best-effort GRAPH_LOADER_TIMING_DIR writer's existing contract."""
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    unwritable = tmp_path / "does" / "not" / "exist"
    monkeypatch.setenv("GRAPH_LOADER_TIMING_DIR", str(unwritable))
    mock_load.return_value = MagicMock(nodes=[], edges=())
    args = argparse.Namespace(mode="stream", config="config.yaml", network="test_net")

    # Schema load succeeds trivially (empty schema); the run still exits
    # normally regardless of what happens next, and the snapshot write
    # (to a nonexistent directory) must not raise or change that.
    exit_code = handle_start(args)
    assert isinstance(exit_code, int)
    assert not unwritable.exists()


@patch("src.cli._run_relationship_orchestration", return_value=0)
@patch("src.cli._run_completion_driven_bulk_edges")
@patch("src.cli.BulkMonitor")
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_bulk_starts_edge_after_its_node_labels_drain(
    mock_load, mock_driver, _apply, mock_docker, monitor_cls, scheduler, orchestration,
):
    person = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    schema = SimpleNamespace(
        nodes=[
            SimpleNamespace(label="Person", topic="person", replicas=1),
            SimpleNamespace(label="Company", topic="company", replicas=1),
        ],
        edges=[person], loading=SimpleNamespace(consumer_group_id="loader"),
    )
    mock_load.return_value = schema
    mock_driver.return_value = MagicMock()
    person_container, company_container = MagicMock(), MagicMock()
    docker = MagicMock()
    # Labels launch concurrently, so map containers by label, not call order.
    docker.run_node_loader.side_effect = lambda node_label, **_kwargs: {
        "Person": [person_container], "Company": [company_container],
    }[node_label]
    mock_docker.return_value = docker
    monitor = MagicMock()
    saw_person = Event()

    def drive(labels, timeout, *, stop_label, on_ready, should_abort):
        assert labels == {"Person", "Company"} and timeout == 3
        stop_label("Person")
        on_ready("Person")
        assert saw_person.wait(3)
        assert not should_abort()
        stop_label("Company")
        on_ready("Company")

    monitor.drive_label_readiness.side_effect = drive
    monitor_cls.return_value = monitor

    def run_scheduler(*_args, ready_labels, ready_condition, **_kwargs):
        with ready_condition:
            assert ready_condition.wait_for(lambda: "Person" in ready_labels, timeout=3)
            assert person_container.stop.called
            assert not company_container.stop.called
            saw_person.set()
            assert ready_condition.wait_for(lambda: "Company" in ready_labels, timeout=3)
        return 0

    scheduler.side_effect = run_scheduler
    args = argparse.Namespace(
        mode="bulk", config="config.yaml", network="test-net",
        skip_image_build=True, bulk_timeout_seconds=3, edge_classic=True,
    )

    assert handle_start(args) == 0
    monitor.drive_label_readiness.assert_called_once()
    # Coverage and the fixed boundary are proved per label inside the driver,
    # not for the whole fleet before any label may become ready.
    monitor.wait_for_assignment_coverage.assert_not_called()
    monitor.capture_boundary.assert_not_called()
    monitor.wait_for_next_label_completion.assert_not_called()
    orchestration.assert_called_once()
    assert orchestration.call_args.kwargs["shared_already_loaded"] is True
    assert orchestration.call_args.kwargs["self_references_already_loaded"] == frozenset()


def _two_label_bulk_start(mock_load, mock_driver, mock_docker, edges, **arg_overrides):
    schema = SimpleNamespace(
        nodes=[
            SimpleNamespace(label="Person", topic="person", replicas=2),
            SimpleNamespace(label="Company", topic="company", replicas=1),
        ],
        edges=list(edges), loading=SimpleNamespace(consumer_group_id="loader"),
    )
    mock_load.return_value = schema
    mock_driver.return_value = MagicMock()
    containers = {
        "Person": [MagicMock(name="person-0"), MagicMock(name="person-1")],
        "Company": [MagicMock(name="company-0")],
    }
    docker = MagicMock()
    docker.run_node_loader.side_effect = lambda node_label, **_kwargs: containers[node_label]
    mock_docker.return_value = docker
    args = argparse.Namespace(
        mode="bulk", config="config.yaml", network="test-net",
        skip_image_build=True, bulk_timeout_seconds=3, **{"edge_classic": not (arg_overrides.get("edge_leases") or arg_overrides.get("edge_plan")), **arg_overrides},
    )
    return args, containers


@patch("src.cli._run_relationship_orchestration", return_value=0)
@patch("src.cli._run_completion_driven_bulk_edges")
@patch("src.cli.BulkMonitor")
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_bulk_label_drain_failure_cancels_scheduler_and_stops_every_container(
    mock_load, mock_driver, _apply, mock_docker, monitor_cls, scheduler, orchestration,
):
    edge = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    args, containers = _two_label_bulk_start(mock_load, mock_driver, mock_docker, [edge])
    monitor = MagicMock()
    published: list[str] = []

    def drive(_labels, _timeout, *, stop_label, on_ready, should_abort):
        stop_label("Company")
        on_ready("Company")
        stop_label("Person")
        raise RuntimeError("node label=Person drain timed out")

    monitor.drive_label_readiness.side_effect = drive
    monitor_cls.return_value = monitor

    def run_scheduler(*_args, ready_labels, ready_condition, cancelled, **_kwargs):
        with ready_condition:
            assert ready_condition.wait_for(cancelled.is_set, timeout=3)
            published.extend(sorted(ready_labels))
        return 1

    scheduler.side_effect = run_scheduler
    assert handle_start(args) == 1
    assert published == ["Company"]
    orchestration.assert_not_called()
    for replicas in containers.values():
        for container in replicas:
            assert container.stop.call_count == 1  # stopped at most once: the fail-closed stop skips it
    monitor.close.assert_called_once()


@patch("src.cli._run_relationship_orchestration", return_value=0)
@patch("src.cli._run_completion_driven_bulk_edges", return_value=0)
@patch("src.cli.BulkMonitor")
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_bulk_label_stop_uses_long_grace_and_run_removes_node_containers(
    mock_load, mock_driver, _apply, mock_docker, monitor_cls, _scheduler, _orchestration,
):
    edge = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    args, containers = _two_label_bulk_start(mock_load, mock_driver, mock_docker, [edge])
    monitor = MagicMock()

    def drive(_labels, _timeout, *, stop_label, on_ready, should_abort):
        for label in ("Company", "Person"):
            stop_label(label)
            for container in containers[label]:
                # Docker SIGKILL must not race daemon teardown after a drain.
                container.stop.assert_called_once_with(timeout=30)
                container.remove.assert_not_called()
            on_ready(label)

    monitor.drive_label_readiness.side_effect = drive
    monitor_cls.return_value = monitor

    assert handle_start(args) == 0
    for replicas in containers.values():
        for container in replicas:
            container.remove.assert_called_once_with(force=True)


@patch("src.cli._run_relationship_orchestration", return_value=0)
@patch("src.cli._run_completion_driven_bulk_edges", return_value=0)
@patch("src.cli.BulkMonitor")
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_bulk_failure_logs_crashed_loader_tail_before_removal(
    mock_load, mock_driver, _apply, mock_docker, monitor_cls, _scheduler, _orchestration, caplog,
):
    edge = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    args, containers = _two_label_bulk_start(mock_load, mock_driver, mock_docker, [edge])
    crashed = containers["Person"][1]
    crashed.name = "graph-loader-node-Person-1-run"
    crashed.attrs = {"State": {"Status": "exited", "ExitCode": 1}}
    crashed.logs.return_value = b"ERROR: Kafka partitions revoked with unresolved work"
    for container in (containers["Person"][0], containers["Company"][0]):
        container.attrs = {"State": {"Status": "running"}}
    order: list[str] = []
    crashed.logs.side_effect = lambda **_kwargs: order.append("logs") or b"ERROR: revoked with unresolved work"
    crashed.remove.side_effect = lambda **_kwargs: order.append("remove")
    monitor = MagicMock()
    monitor.drive_label_readiness.side_effect = LoaderCrashError(
        "loader exited label=Person replica=1 status=exited exit_code=1"
    )
    monitor_cls.return_value = monitor

    assert handle_start(args) == 1
    assert order == ["logs", "remove"]
    assert "container=graph-loader-node-Person-1-run exit_code=1" in caplog.text
    assert "revoked with unresolved work" in caplog.text
    containers["Person"][0].logs.assert_not_called()


@patch("src.cli._run_relationship_orchestration", return_value=0)
@patch("src.cli._run_completion_driven_bulk_edges")
@patch("src.cli.BulkMonitor")
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_bulk_label_readiness_aborts_when_edge_scheduler_fails(
    mock_load, mock_driver, _apply, mock_docker, monitor_cls, scheduler, orchestration,
):
    edge = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    args, containers = _two_label_bulk_start(mock_load, mock_driver, mock_docker, [edge])
    monitor = MagicMock()
    aborted = Event()

    def drive(_labels, _timeout, *, stop_label, on_ready, should_abort):
        for _ in range(300):
            if should_abort():
                aborted.set()
                raise RuntimeError("aborted")
            Event().wait(0.01)
        raise AssertionError("scheduler failure was never observed")

    monitor.drive_label_readiness.side_effect = drive
    monitor_cls.return_value = monitor
    scheduler.return_value = 1
    assert handle_start(args) == 1
    assert aborted.is_set()
    orchestration.assert_not_called()
    assert all(container.stop.called for replicas in containers.values() for container in replicas)


@patch("src.cli._run_relationship_orchestration", return_value=0)
@patch("src.cli._run_completion_driven_bulk_edges", return_value=0)
@patch("src.cli.BulkMonitor")
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_bulk_node_stop_failure_names_exact_replica(
    mock_load, mock_driver, _apply, mock_docker, monitor_cls, scheduler, orchestration,
):
    edge = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    args, containers = _two_label_bulk_start(mock_load, mock_driver, mock_docker, [edge])
    stuck = containers["Person"][1]
    stuck.stop.side_effect = RuntimeError("daemon unavailable")
    stuck.attrs = {"State": {"Status": "running"}}
    monitor = MagicMock()
    raised: list[str] = []

    def drive(_labels, _timeout, *, stop_label, on_ready, should_abort):
        try:
            stop_label("Person")
        except RuntimeError as exc:
            raised.append(str(exc))
            raise

    monitor.drive_label_readiness.side_effect = drive
    monitor_cls.return_value = monitor
    assert handle_start(args) == 1
    assert raised and "label=Person replica=1" in raised[0]
    # Company never reached its boundary; it is stopped only by fail-closed cleanup.
    assert containers["Company"][0].stop.call_count == 1


@pytest.mark.parametrize("overlap, expect_co_scheduled", [("shared", True), ("labels", False)])
@patch("src.cli._run_relationship_orchestration", return_value=0)
@patch("src.cli._run_completion_driven_bulk_edges", return_value=0)
@patch("src.cli.BulkMonitor")
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_bulk_shared_overlap_co_schedules_self_references_with_shared_edges(
    mock_load, mock_driver, _apply, mock_docker, monitor_cls, scheduler, orchestration,
    overlap, expect_co_scheduled,
):
    works = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    knows = _apoc_edge("KNOWS", "knows", "Person", "Person")
    args, _containers = _two_label_bulk_start(
        mock_load, mock_driver, mock_docker, [works, knows], bulk_edge_overlap=overlap,
    )
    monitor = MagicMock()
    monitor_cls.return_value = monitor
    assert handle_start(args) == 0
    scheduled = [edge.type for edge in scheduler.call_args.args[0]]
    already = orchestration.call_args.kwargs["self_references_already_loaded"]
    if expect_co_scheduled:
        assert scheduled == ["WORKS_AT", "KNOWS"] and already == frozenset({"KNOWS"})
    else:
        assert scheduled == ["WORKS_AT"] and already == frozenset()


def _apoc_edge(edge_type, topic, source, target, *, mode="python_apoc"):
    edge = _rotating_edge(edge_type, topic, source, target)
    edge.execution = SimpleNamespace(mode=mode)
    return edge


@patch("src.cli._run_bulk_relationship_edge")
def test_bulk_scheduler_shared_starts_self_reference_once_its_label_is_ready(run_edge):
    edges = (_rotating_edge("WORKS_AT", "works", "Person", "Company"), _apoc_edge("KNOWS", "knows", "Person", "Person"))
    ready: set[str] = {"Company"}
    condition = Condition()
    thread, started, release, result = _gated_scheduler(
        run_edge, edges, _bulk_args(bulk_edge_overlap="shared"),
        ready_labels=ready, ready_condition=condition,
    )
    try:
        assert not started["KNOWS"].wait(0.4)
        with condition:
            ready.add("Person")
            condition.notify_all()
        # The self-reference runs concurrently with the shared edge.
        assert started["KNOWS"].wait(3) and started["WORKS_AT"].wait(3)
    finally:
        _finish(thread, release)
    assert result == [0]
    kwargs = {call.args[0].type: call.kwargs for call in run_edge.call_args_list}
    assert kwargs["KNOWS"]["self_reference_co_scheduled"] is True
    assert kwargs["WORKS_AT"]["self_reference_co_scheduled"] is False


@pytest.mark.parametrize("overlap, mode", [("labels", "python_apoc"), ("shared", "native_disjoint")])
@patch("src.cli._run_bulk_relationship_edge", return_value=0)
def test_bulk_scheduler_rejects_self_reference_outside_apoc_shared_overlap(run_edge, overlap, mode):
    edges = (_apoc_edge("KNOWS", "knows", "Person", "Person", mode=mode),)
    assert _run_completion_driven_bulk_edges(
        edges, MagicMock(), _bulk_args(bulk_edge_overlap=overlap),
        parent_run_id="bulk-run", consumer_group_prefix="loader",
    ) == 1
    run_edge.assert_not_called()


@patch("src.cli._run_isolated_relationship_bulk_phase", return_value=0)
def test_orchestration_skips_isolated_phase_of_co_scheduled_self_reference(isolated):
    knows = _apoc_edge("KNOWS", "knows", "Person", "Person")
    likes = _apoc_edge("LIKES", "likes", "Person", "Person", mode="native_disjoint")
    schema = SimpleNamespace(edges=[knows, likes], loading=SimpleNamespace(consumer_group_id="loader"))
    args = _bulk_args(mode="bulk", bulk_edge_overlap="shared")
    assert _run_relationship_orchestration(
        schema, MagicMock(), args, parent_run_id="bulk-run",
        shared_already_loaded=True, self_references_already_loaded=frozenset({"KNOWS"}),
    ) == 0
    assert [call.args[0].type for call in isolated.call_args_list] == ["LIKES"]


@patch("src.cli.RelationshipBulkMonitor")
def test_co_scheduled_self_reference_edge_runs_bulk_lifecycle(monitor_cls):
    from src.cli import _run_bulk_relationship_edge

    knows = _apoc_edge("KNOWS", "knows", "Person", "Person")
    docker = MagicMock()
    docker.run_edge_loader.return_value = [MagicMock()]
    args = _bulk_args()
    assert _run_bulk_relationship_edge(
        knows, docker, args, run_id="bulk-run.bulk.0.abc", consumer_group_prefix="loader",
    ) == 1  # still rejected without the explicit co-schedule flag
    docker.run_edge_loader.assert_not_called()
    assert _run_bulk_relationship_edge(
        knows, docker, args, run_id="bulk-run.bulk.0.abc", consumer_group_prefix="loader",
        self_reference_co_scheduled=True,
    ) == 0
    monitor_cls.return_value.wait_for_drain_complete.assert_called_once()
    monitor_cls.return_value.verify_zero_lag.assert_called_once()
    works = _apoc_edge("WORKS_AT", "works", "Person", "Company")
    assert _run_bulk_relationship_edge(
        works, docker, args, run_id="bulk-run.bulk.0.def", consumer_group_prefix="loader",
        self_reference_co_scheduled=True,
    ) == 1


def test_stop_exact_containers_stops_one_labels_replicas_in_parallel():
    from threading import Barrier
    from src.cli import _stop_exact_containers

    barrier = Barrier(4, timeout=3)
    containers = [MagicMock(name=f"c{index}") for index in range(4)]
    for container in containers:
        # Serial stops would deadlock the barrier and raise BrokenBarrierError.
        container.stop.side_effect = lambda: barrier.wait()
    assert _stop_exact_containers(containers) == []
    assert all(container.stop.call_count == 1 for container in containers)


def test_stop_exact_containers_parallel_failures_keep_retry_and_attribution():
    from docker.errors import NotFound
    from src.cli import _stop_exact_containers

    gone, stuck, fine = MagicMock(name="gone"), MagicMock(name="stuck"), MagicMock(name="fine")
    gone.stop.side_effect = RuntimeError("already removed")
    gone.reload.side_effect = NotFound("gone")
    stuck.stop.side_effect = RuntimeError("daemon unavailable")
    stuck.attrs = {"State": {"Status": "running"}}
    failures = _stop_exact_containers([gone, stuck, fine], identities={id(stuck): "label=Person replica=1"})
    assert failures == ["label=Person replica=1: daemon unavailable"]
    assert stuck.stop.call_count == 2 and gone.stop.call_count == 1 and fine.stop.call_count == 1



@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_handle_start_uses_prebuilt_image_when_requested(mock_load, mock_get_driver, mock_apply_schema, mock_docker):
    args = argparse.Namespace(mode="stream", config="config.yaml", network="test_net", skip_image_build=True)
    mock_schema = MagicMock()
    mock_schema.nodes = [SimpleNamespace(label="Person", topic="person_topic", replicas=1)]
    mock_load.return_value = mock_schema
    mock_docker_instance = MagicMock()
    mock_docker_instance.run_node_loader.return_value = [MagicMock()]
    mock_docker.return_value = mock_docker_instance

    assert handle_start(args) == 0
    mock_docker_instance.build_image.assert_not_called()
    mock_docker_instance.run_node_loader.assert_called_once()


@patch("src.cli.load_schema")
def test_handle_start_schema_load_failure(mock_load):
    args = argparse.Namespace(mode="bulk", config="config.yaml")
    mock_load.side_effect = Exception("File not found")

    exit_code = handle_start(args)

    assert exit_code == 1


@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_handle_start_init_failure(mock_load, mock_get_driver, mock_apply_schema):
    args = argparse.Namespace(mode="bulk", config="config.yaml")
    mock_get_driver.side_effect = SchemaInitializationError("DB offline")

    exit_code = handle_start(args)

    assert exit_code == 1
    mock_load.assert_called_once()
    mock_apply_schema.assert_not_called()


@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_handle_start_docker_launch_failure(mock_load, mock_get_driver, mock_apply_schema, mock_docker):
    args = argparse.Namespace(mode="stream", config="config.yaml", network="test_net")
    mock_schema = MagicMock()
    mock_node1 = SimpleNamespace(label="Person", topic="person_topic", replicas=1)
    mock_node2 = SimpleNamespace(label="Company", topic="company_topic", replicas=1)
    mock_schema.nodes = [mock_node1, mock_node2]
    mock_load.return_value = mock_schema

    mock_docker_instance = MagicMock()
    mock_docker.return_value = mock_docker_instance
    mock_container = MagicMock()

    # First call succeeds, second fails
    mock_docker_instance.run_node_loader.side_effect = [[mock_container], Exception("Launch failed")]

    exit_code = handle_start(args)

    assert exit_code == 1
    mock_container.stop.assert_called_once()


@patch("src.cli.BulkMonitor")
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_handle_start_launches_labels_concurrently_and_stops_all_on_any_failure(
    mock_load, _driver, _apply, mock_docker, monitor_cls,
):
    from threading import Barrier

    labels = ["A", "B", "C"]
    mock_load.return_value = SimpleNamespace(
        nodes=[SimpleNamespace(label=label, topic=label.lower(), replicas=1) for label in labels],
        edges=[],
    )
    barrier = Barrier(len(labels), timeout=5)
    launched = {label: MagicMock(name=label) for label in labels}

    def run_node_loader(node_label, **_kwargs):
        barrier.wait()  # every label's launch must be in flight at once
        if node_label == "B":
            raise RuntimeError("launch failed label=B")
        return [launched[node_label]]

    docker = MagicMock()
    docker.run_node_loader.side_effect = run_node_loader
    mock_docker.return_value = docker
    args = argparse.Namespace(
        mode="bulk", config="config.yaml", network="test-net",
        skip_image_build=True, bulk_timeout_seconds=3, edge_classic=True,
    )

    assert handle_start(args) == 1
    launched["A"].stop.assert_called_once()
    launched["C"].stop.assert_called_once()
    launched["B"].stop.assert_not_called()
    monitor_cls.assert_not_called()


# -- bucket-lease mode ---------------------------------------------------------------

def test_parser_edge_lease_flags_defaults_and_values():
    # P5-S1 Story 1 (review finding S1-1): both flags of a pair default to
    # None at parse time; _merge_deprecated_lease_flags (tested separately
    # below) is what resolves the real default.
    args = build_parser().parse_args(["start", "--mode", "bulk"])
    assert (args.edge_leases, args.lease_buckets, args.lease_outer_batch_size, args.lease_keyed_max_nodes) == (
        False, None, None, None,
    )
    # --bucket-lease-* is the current name.
    args = build_parser().parse_args([
        "start", "--mode", "bulk", "--edge-leases", "--bucket-lease-buckets", "8",
        "--bucket-lease-outer-batch-size", "16000", "--bucket-lease-keyed-max-nodes", "0",
    ])
    assert (args.edge_leases, args.lease_buckets, args.lease_outer_batch_size, args.lease_keyed_max_nodes) == (
        True, 8, 16_000, 0,
    )


def test_parser_deprecated_edge_lease_flags_merge_to_identical_behavior(caplog):
    """P5-S1 Story 1 AC: the deprecated ``--lease-*`` flags, merged in
    ``_handle_start`` via ``_merge_deprecated_lease_flags``, produce
    byte-identical ``args.lease_*`` to the current ``--bucket-lease-*``
    flags, with exactly one deprecation log line per flag actually used.
    Both flags of a pair default to ``None`` at parse time (review finding
    S1-1): the merge is the only place either one's final value is
    decided."""
    import src.cli as cli_module
    from src.cli import _merge_deprecated_lease_flags

    # "Logged once per process" state lives at module scope; reset it so
    # this test is independent of whatever ran earlier in the same process.
    cli_module._deprecated_flags_logged.clear()

    current = build_parser().parse_args([
        "start", "--mode", "bulk", "--edge-leases", "--bucket-lease-buckets", "8",
        "--bucket-lease-outer-batch-size", "16000", "--bucket-lease-keyed-max-nodes", "0",
    ])
    deprecated = build_parser().parse_args([
        "start", "--mode", "bulk", "--edge-leases", "--lease-buckets", "8",
        "--lease-outer-batch-size", "16000", "--lease-keyed-max-nodes", "0",
    ])
    # Before the merge, the deprecated flags land on their own attributes;
    # args.lease_buckets/etc. are still None (real parse-time default).
    assert deprecated.lease_buckets is None
    assert deprecated.lease_buckets_deprecated == 8
    _merge_deprecated_lease_flags(current)
    with caplog.at_level("WARNING"):
        _merge_deprecated_lease_flags(deprecated)
    assert (deprecated.lease_buckets, deprecated.lease_outer_batch_size, deprecated.lease_keyed_max_nodes) == (
        current.lease_buckets, current.lease_outer_batch_size, current.lease_keyed_max_nodes,
    ) == (8, 16_000, 0)
    deprecation_lines = [r for r in caplog.records if "stage=deprecation" in r.message]
    assert len(deprecation_lines) == 3
    assert sum("flag=--lease-buckets" in r.message for r in deprecation_lines) == 1
    # A namespace that never used a deprecated flag logs nothing when merged
    # (and resolves to the current flag's own value, already proven above).
    caplog.clear()
    with caplog.at_level("WARNING"):
        _merge_deprecated_lease_flags(current)
    assert not [r for r in caplog.records if "stage=deprecation" in r.message]


def test_parser_neither_lease_flag_given_resolves_to_defaults():
    args = build_parser().parse_args(["start", "--mode", "bulk", "--edge-leases"])
    assert (args.lease_buckets, args.lease_outer_batch_size, args.lease_keyed_max_nodes) == (None, None, None)
    from src.cli import _merge_deprecated_lease_flags

    _merge_deprecated_lease_flags(args)
    assert (args.lease_buckets, args.lease_outer_batch_size, args.lease_keyed_max_nodes) == (16, 32_000, 1_000)


def test_parser_conflicting_current_and_deprecated_lease_flags_raise():
    """S1-1: a silent "one wins" would let an operator's typo go unnoticed,
    so a differing pair must be a hard configuration error."""
    from src.cli import _merge_deprecated_lease_flags

    args = build_parser().parse_args([
        "start", "--mode", "bulk", "--edge-leases",
        "--bucket-lease-buckets", "8", "--lease-buckets", "32",
    ])
    with pytest.raises(ValueError, match="conflicts with"):
        _merge_deprecated_lease_flags(args)
    # An identical pair (agreeing values) is not a conflict.
    args = build_parser().parse_args([
        "start", "--mode", "bulk", "--edge-leases",
        "--bucket-lease-buckets", "8", "--lease-buckets", "8",
    ])
    _merge_deprecated_lease_flags(args)
    assert args.lease_buckets == 8


def _lease_bulk_start(mock_load, mock_driver, mock_docker, edges, **overrides):
    defaults = dict(
        edge_leases=True, lease_buckets=16, lease_outer_batch_size=32_000, lease_keyed_max_nodes=1_000,
        bulk_edge_overlap="labels",
    )
    defaults.update(overrides)
    args, containers = _two_label_bulk_start(mock_load, mock_driver, mock_docker, edges, **defaults)
    docker = mock_docker.return_value
    lease = MagicMock(name="lease-server")
    lease.logs.return_value = (
        b"INFO: stage=lease-server-ready host=0.0.0.0 port=7700\n"
        b"INFO: stage=lease-server-stats grants=12 denials=0 max_concurrent=3\n"
    )
    docker.run_bucket_lease_server.return_value = lease
    docker.bucket_lease_address.return_value = "172.17.0.9:7700"
    return args, containers, lease


@patch("src.cli._node_topic_record_counts", return_value={"person": 50_000, "company": 40})
@patch("src.cli._run_relationship_orchestration", return_value=0)
@patch("src.cli._run_completion_driven_bulk_edges", return_value=0)
@patch("src.cli.BulkMonitor")
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_bulk_edge_leases_launch_coordinator_and_stop_it_after_the_run(
    mock_load, mock_driver, _apply, mock_docker, monitor_cls, scheduler, orchestration, counts,
    tmp_path, monkeypatch, caplog,
):
    monkeypatch.setenv("GRAPH_LOADER_TIMING_DIR", str(tmp_path))
    works = _apoc_edge("WORKS_AT", "works", "Person", "Company")
    knows = _apoc_edge("KNOWS", "knows", "Person", "Person")
    args, _containers, lease = _lease_bulk_start(mock_load, mock_driver, mock_docker, [works, knows])
    docker = mock_docker.return_value
    monitor_cls.return_value = MagicMock()
    order: list[str] = []
    docker.run_bucket_lease_server.side_effect = lambda **kwargs: order.append("lease") or lease
    scheduler.side_effect = lambda *a, **k: order.append("edges") or 0
    lease.stop.side_effect = lambda **kwargs: order.append("stop")
    with caplog.at_level("INFO", logger="src.cli"):
        assert handle_start(args) == 0
    launch = docker.run_bucket_lease_server.call_args.kwargs
    assert launch["bucket_count"] == 16 and launch["network"] == "test-net"
    assert launch["keyed_labels"] == ("Company",)
    assert args.lease_address == "172.17.0.9:7700" and args.lease_keyed_labels == ("Company",)
    # Leases imply shared overlap, so the self-reference is co-scheduled.
    assert args.bulk_edge_overlap == "shared"
    assert [edge.type for edge in scheduler.call_args.args[0]] == ["WORKS_AT", "KNOWS"]
    assert order == ["lease", "edges", "stop"]
    lease.remove.assert_called_once_with(force=True)
    assert "stage=lease-server-stats grants=12" in caplog.text
    assert (tmp_path / "lease-server.log").read_text().count("lease-server-stats") == 1
    settings = json.loads((tmp_path / "lease-settings.json").read_text())
    assert settings == {"address": "172.17.0.9:7700", "buckets": 16, "keyed_labels": ["Company"],
                        "outer_batch_size": 32000}


@patch("src.cli._node_topic_record_counts", return_value={"person": 50_000, "company": 40})
@patch("src.cli._run_relationship_orchestration", return_value=0)
@patch("src.cli._run_completion_driven_bulk_edges", return_value=0)
@patch("src.cli.BulkMonitor")
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_deprecated_lease_flag_reaches_docker_service_through_handle_start(
    mock_load, mock_driver, _apply, mock_docker, monitor_cls, scheduler, orchestration, counts,
):
    """P5-S1 Story 1 review finding S1-2: prove end to end (through
    ``handle_start``, not by calling ``_merge_deprecated_lease_flags``
    directly) that a deprecated ``--lease-buckets``-style value reaches
    every downstream reader -- here, ``DockerService.run_bucket_lease_server``
    -- not just that the merge function itself works in isolation."""
    works = _apoc_edge("WORKS_AT", "works", "Person", "Company")
    args, _containers, lease = _lease_bulk_start(
        mock_load, mock_driver, mock_docker, [works],
        # Simulate real argparse output: the deprecated flag was typed, the
        # current one was not, exactly as build_parser() would produce it.
        lease_buckets=None, lease_buckets_deprecated=8,
    )
    docker = mock_docker.return_value
    monitor_cls.return_value = MagicMock()
    assert handle_start(args) == 0
    launch = docker.run_bucket_lease_server.call_args.kwargs
    assert launch["bucket_count"] == 8
    assert args.lease_buckets == 8


@patch("src.cli._run_completion_driven_bulk_edges", return_value=0)
@patch("src.cli.BulkMonitor")
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_bulk_edge_lease_launch_failure_stops_nodes_and_fails(
    mock_load, mock_driver, _apply, mock_docker, monitor_cls, scheduler,
):
    works = _apoc_edge("WORKS_AT", "works", "Person", "Company")
    args, containers, _lease = _lease_bulk_start(
        mock_load, mock_driver, mock_docker, [works], lease_keyed_max_nodes=0,
    )
    mock_docker.return_value.run_bucket_lease_server.side_effect = RuntimeError("no image")
    assert handle_start(args) == 1
    scheduler.assert_not_called()
    assert all(container.stop.called for replicas in containers.values() for container in replicas)


@patch("src.cli._run_completion_driven_bulk_edges", return_value=0)
@patch("src.cli.BulkMonitor")
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_bulk_edge_lease_address_failure_removes_coordinator(
    mock_load, mock_driver, _apply, mock_docker, monitor_cls, scheduler,
):
    works = _apoc_edge("WORKS_AT", "works", "Person", "Company")
    args, _containers, lease = _lease_bulk_start(mock_load, mock_driver, mock_docker, [works], lease_keyed_max_nodes=0)
    mock_docker.return_value.bucket_lease_address.side_effect = RuntimeError("no ip")
    assert handle_start(args) == 1
    lease.stop.assert_called()
    lease.remove.assert_called_once_with(force=True)


@pytest.mark.parametrize("overrides, edge_mode", [
    ({"mode": "stream"}, "python_apoc"),
    ({}, "native_disjoint"),
    ({"lease_buckets": 0}, "python_apoc"),
    ({"lease_outer_batch_size": 0}, "python_apoc"),
    ({"lease_keyed_max_nodes": -1}, "python_apoc"),
])
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_edge_lease_preconditions_fail_before_touching_neo4j(
    mock_load, mock_driver, apply, mock_docker, overrides, edge_mode,
):
    works = _apoc_edge("WORKS_AT", "works", "Person", "Company", mode=edge_mode)
    args, _containers, _lease = _lease_bulk_start(mock_load, mock_driver, mock_docker, [works])
    for key, value in overrides.items():
        setattr(args, key, value)
    assert handle_start(args) == 1
    apply.assert_not_called()
    mock_docker.return_value.run_node_loader.assert_not_called()


@patch("src.cli.RelationshipBulkMonitor")
def test_bulk_edge_launch_passes_lease_settings(monitor_cls):
    works = _rotating_edge("WORKS_AT", "works", "Person", "Company")
    docker = MagicMock()
    docker.run_edge_loader.return_value = [MagicMock()]
    monitor_cls.return_value = MagicMock()
    args = _bulk_args(
        lease_address="172.17.0.9:7700", lease_buckets=8, lease_outer_batch_size=20_000,
        lease_keyed_labels=("Company",),
    )
    assert _run_bulk_relationship_edge(works, docker, args, run_id="run-1", consumer_group_prefix="loader") == 0
    launch = docker.run_edge_loader.call_args.kwargs
    assert (launch["lease_address"], launch["lease_buckets"], launch["outer_batch_size"],
            launch["lease_keyed_labels"]) == ("172.17.0.9:7700", 8, 20_000, ("Company",))


def test_keyed_lease_labels_use_small_endpoint_topics(monkeypatch):
    schema = SimpleNamespace(
        nodes=[SimpleNamespace(label="Title", topic="title"), SimpleNamespace(label="Genre", topic="genre"),
               SimpleNamespace(label="Empty", topic="empty"), SimpleNamespace(label="Unused", topic="unused")],
        edges=[_rotating_edge("HAS_GENRE", "g", "Title", "Genre"), _rotating_edge("X", "x", "Title", "Empty")],
    )
    seen = {}

    def counts(_bootstrap, topics):
        seen["topics"] = list(topics)
        return {"title": 2_000_000, "genre": 28, "empty": 0}

    monkeypatch.setattr("src.cli._node_topic_record_counts", counts)
    args = argparse.Namespace(lease_keyed_max_nodes=1_000)
    assert _select_keyed_lease_labels(schema, args) == ("Genre",)
    assert seen["topics"] == ["empty", "genre", "title"]
    assert _select_keyed_lease_labels(schema, argparse.Namespace(lease_keyed_max_nodes=0)) == ()

    def broken(_bootstrap, _topics):
        raise RuntimeError("kafka down")

    monkeypatch.setattr("src.cli._node_topic_record_counts", broken)
    assert _select_keyed_lease_labels(schema, args) == ()
