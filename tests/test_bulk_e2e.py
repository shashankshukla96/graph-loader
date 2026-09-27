"""CLI lifecycle contract tests for finite, run-scoped bulk loading.

Kafka boundary semantics are unit-tested in ``test_bulk_monitor``.  These tests
exercise the orchestration boundary: which objects are started and stopped, and
the exact ordering of lifecycle calls.
"""
import argparse
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import src.cli as cli
from src.orchestrator.bulk_monitor import QuiescentViolationError


def _args() -> argparse.Namespace:
    return argparse.Namespace(
        mode="bulk",
        config="config.yaml",
        network="test_net",
        bulk_timeout_seconds=12.5,
    )


def _container(name: str) -> MagicMock:
    container = MagicMock(name=name)
    container.name = name
    container.attrs = {"State": {"Status": "running"}}
    return container


def _wire_start(monkeypatch, schema, service, monitor):
    """Replace external infrastructure while retaining the real CLI flow."""
    driver = MagicMock()
    monkeypatch.setattr(cli, "load_schema", MagicMock(return_value=schema))
    monkeypatch.setattr(cli, "get_neo4j_driver", MagicMock(return_value=driver))
    monkeypatch.setattr(cli, "apply_schema", MagicMock())
    monkeypatch.setattr(cli, "DockerService", MagicMock(return_value=service))
    monitor_factory = MagicMock(return_value=monitor)
    monkeypatch.setattr(cli, "BulkMonitor", monitor_factory)
    monkeypatch.setattr(cli.uuid, "uuid4", MagicMock(return_value=SimpleNamespace(hex="run-42")))
    return monitor_factory


def test_bulk_happy_path_uses_one_run_scope_and_orders_lifecycle(monkeypatch):
    schema = SimpleNamespace(
        nodes=[
            SimpleNamespace(label="Person", topic="people", replicas=2),
            SimpleNamespace(label="Company", topic="companies", replicas=1),
        ]
    )
    person_0, person_1, company_0 = (_container("person-0"), _container("person-1"), _container("company-0"))
    service = MagicMock()
    service.run_node_loader.side_effect = [[person_0, person_1], [company_0]]
    monitor = MagicMock()
    events = []
    monitor.wait_for_assignment_coverage.side_effect = lambda timeout: events.append("coverage")
    monitor.capture_boundary.side_effect = lambda: events.append("boundary")
    monitor.wait_for_completion.side_effect = lambda timeout: events.append("completion")
    monitor.wait_for_drain_complete.side_effect = lambda timeout: events.append("drain")
    monitor.verify_zero_lag.side_effect = lambda: events.append("zero-lag")
    for container in (person_0, person_1, company_0):
        container.stop.side_effect = lambda target=container: events.append(f"stop:{target.name}")
    monitor_factory = _wire_start(monkeypatch, schema, service, monitor)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "host-kafka:19092")

    assert cli.handle_start(_args()) == 0

    monitor_factory.assert_called_once_with(
        schema,
        "run-42",
        {("Person", "0"), ("Person", "1"), ("Company", "0")},
        {("Person", "0"): person_0, ("Person", "1"): person_1, ("Company", "0"): company_0},
        bootstrap_servers="host-kafka:19092",
    )
    assert service.run_node_loader.call_args_list == [
        call(node_label="Person", topic="people", mode="bulk", config_path="config.yaml", replicas=2, network="test_net", run_id="run-42"),
        call(node_label="Company", topic="companies", mode="bulk", config_path="config.yaml", replicas=1, network="test_net", run_id="run-42"),
    ]
    assert monitor.method_calls == [
        call.wait_for_assignment_coverage(12.5),
        call.capture_boundary(),
        call.wait_for_completion(12.5),
        call.wait_for_drain_complete(12.5),
        call.verify_zero_lag(),
        call.close(),
    ]
    for container in (person_0, person_1, company_0):
        container.stop.assert_called_once_with()
    assert events == [
        "coverage", "boundary", "completion",
        "stop:person-0", "stop:person-1", "stop:company-0",
        "drain", "zero-lag",
    ]


def test_bulk_quiescent_violation_stops_only_its_exact_fleet(monkeypatch):
    schema = SimpleNamespace(nodes=[SimpleNamespace(label="Person", topic="people", replicas=2)])
    first, second, older_run_container = _container("first"), _container("second"), _container("older")
    service = MagicMock()
    service.run_node_loader.return_value = [first, second]
    monitor = MagicMock()
    monitor.wait_for_completion.side_effect = QuiescentViolationError("late record")
    _wire_start(monkeypatch, schema, service, monitor)

    assert cli.handle_start(_args()) == 1

    first.stop.assert_called_once_with()
    second.stop.assert_called_once_with()
    older_run_container.stop.assert_not_called()
    monitor.wait_for_drain_complete.assert_not_called()
    monitor.verify_zero_lag.assert_not_called()
    monitor.close.assert_called_once_with()


def test_bulk_stop_attempts_every_target_and_retries_only_failed_target(monkeypatch):
    schema = SimpleNamespace(nodes=[SimpleNamespace(label="Person", topic="people", replicas=2)])
    failing, other, older_run_container = _container("failing"), _container("other"), _container("older")
    failing.stop.side_effect = [RuntimeError("first stop failed"), None]
    service = MagicMock()
    service.run_node_loader.return_value = [failing, other]
    monitor = MagicMock()
    _wire_start(monkeypatch, schema, service, monitor)

    assert cli.handle_start(_args()) == 1

    # The successful replica is still signalled even though its peer failed.
    other.stop.assert_called_once_with()
    # A still-running failed target receives one bounded follow-up on itself.
    assert failing.stop.call_count == 2
    older_run_container.stop.assert_not_called()
    monitor.wait_for_drain_complete.assert_not_called()
    monitor.close.assert_called_once_with()


def test_bulk_post_drain_verification_failure_does_not_touch_other_runs(monkeypatch):
    schema = SimpleNamespace(nodes=[SimpleNamespace(label="Person", topic="people", replicas=1)])
    current, older_run_container = _container("current"), _container("older")
    service = MagicMock()
    service.run_node_loader.return_value = [current]
    monitor = MagicMock()
    monitor.verify_zero_lag.side_effect = RuntimeError("lag remained")
    _wire_start(monkeypatch, schema, service, monitor)

    assert cli.handle_start(_args()) == 1

    current.stop.assert_called_once_with()
    older_run_container.stop.assert_not_called()
    monitor.wait_for_drain_complete.assert_called_once_with(12.5)
    monitor.close.assert_called_once_with()


def test_bulk_partial_launch_response_is_cleaned_up_and_never_monitored(monkeypatch):
    schema = SimpleNamespace(nodes=[SimpleNamespace(label="Person", topic="people", replicas=2)])
    returned = _container("returned")
    service = MagicMock()
    service.run_node_loader.return_value = [returned]
    monitor = MagicMock()
    monitor_factory = _wire_start(monkeypatch, schema, service, monitor)

    assert cli.handle_start(_args()) == 1

    returned.stop.assert_called_once_with()
    monitor_factory.assert_not_called()
