"""
tests/test_cli.py
─────────────────
Unit tests for the CLI parser and routing.
"""
import argparse
from types import SimpleNamespace
from unittest.mock import patch, MagicMock

import pytest

from src.cli import (
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
def test_rotating_fleet_clock_launch_failure_stops_only_returned_edges(monitor_cls):
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
    edge_container.stop.assert_called()
    assert [call[0] for call in docker.mock_calls].count("run_edge_loader") == 1


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
