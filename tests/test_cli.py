"""
tests/test_cli.py
─────────────────
Unit tests for the CLI parser and routing.
"""
import argparse
from threading import Condition, Event, Thread
from types import SimpleNamespace
from unittest.mock import patch, MagicMock

import pytest

from src.cli import (
    _derive_relationship_phase_run_id,
    _run_isolated_relationship_bulk_phase,
    _run_isolated_relationship_stream_phase,
    _run_relationship_orchestration,
    _run_completion_driven_bulk_edges,
    _run_relationship_bulk_stages,
    _run_rotating_relationship_fleet,
    build_parser,
    get_neo4j_credentials,
    handle_start,
    handle_status,
    handle_stop,
    main,
)
from src.orchestrator.dependency_manager import RelationshipConflictPlan, RelationshipStage
from src.orchestrator.schema_initializer import SchemaInitializationError


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
    works = SimpleNamespace(type="WORKS_AT", topic="works", replicas=1)
    bought = SimpleNamespace(type="BOUGHT", topic="bought", replicas=1)
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
    first = SimpleNamespace(type="VISITED", topic="visits", replicas=1)
    second = SimpleNamespace(type="SUPPLIES", topic="supplies", replicas=1)
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
    edge = SimpleNamespace(type="WORKS_AT", topic="works", replicas=2)
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
    first = SimpleNamespace(type="WORKS_AT", topic="works", replicas=1)
    second = SimpleNamespace(type="BOUGHT", topic="bought", replicas=1)
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
        type=edge_type, topic=topic, replicas=replicas,
        nodes=SimpleNamespace(source=source, target=target, is_self_referencing=source == target),
    )


@patch("src.cli.RelationshipBulkMonitor")
def test_isolated_bulk_phase_uses_exact_clock_free_finite_lifecycle(monitor_cls):
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
        "edge_type": "KNOWS", "topic": "knows", "mode": "bulk", "config_path": "config.yaml",
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
    args = argparse.Namespace(mode="bulk", config="config.yaml", network="test-net", bulk_timeout_seconds=1)
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
    args = argparse.Namespace(mode="bulk", config="config.yaml", network="test-net", bulk_timeout_seconds=1)

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
        "edge_type": "KNOWS", "topic": "knows", "mode": "stream", "config_path": "config.yaml",
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
    args = argparse.Namespace(mode="bulk", config="config.yaml", network="test-net", bulk_timeout_seconds=1)

    assert _run_rotating_relationship_fleet(schema, docker, args, selected_edges=(knows,)) == 1

    docker.run_edge_loader.assert_not_called()
    docker.run_global_batch_clock.assert_not_called()


@patch("src.cli._run_relationship_orchestration")
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_handle_start_stream_generates_relationship_parent_run_id_when_omitted(
    mock_load, mock_driver_factory, mock_apply_schema, mock_docker, orchestration,
):
    knows = _rotating_edge("KNOWS", "knows", "Person", "Person")
    mock_load.return_value = SimpleNamespace(nodes=(), edges=[knows])
    mock_driver_factory.return_value = MagicMock()
    mock_docker.return_value = MagicMock()
    orchestration.return_value = 0
    args = argparse.Namespace(mode="stream", config="config.yaml", network="test-net", skip_image_build=True)

    assert handle_start(args) == 0

    parent_run_id = orchestration.call_args.kwargs["parent_run_id"]
    assert isinstance(parent_run_id, str) and len(parent_run_id) == 32
    assert orchestration.call_args.args[:3] == (mock_load.return_value, mock_docker.return_value, args)


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
    docker.run_node_loader.side_effect = [[person_container], [company_container]]
    mock_docker.return_value = docker
    monitor = MagicMock()
    saw_person = Event()
    calls = iter(("Person", "Company"))

    def next_label(*_args):
        label = next(calls)
        if label == "Company":
            assert saw_person.wait(3)
        return label

    monitor.wait_for_next_label_completion.side_effect = next_label
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
        skip_image_build=True, bulk_timeout_seconds=3,
    )

    assert handle_start(args) == 0
    assert [call.args[0] for call in monitor.wait_for_label_drain_complete.call_args_list] == ["Person", "Company"]
    assert [call.args[0] for call in monitor.verify_label_zero_lag.call_args_list] == ["Person", "Company"]
    orchestration.assert_called_once()
    assert orchestration.call_args.kwargs["shared_already_loaded"] is True


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
