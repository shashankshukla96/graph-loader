"""Unit tests for the run-scoped Slice 4 bulk monitor."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from confluent_kafka import TopicPartition
from docker.errors import NotFound

from src.orchestrator.bulk_monitor import (
    AssignmentCoverageError,
    BulkMonitor,
    BulkMonitorError,
    LoaderCrashError,
    OffsetResolutionError,
    QuiescentViolationError,
)
import src.orchestrator.bulk_monitor as bulk_monitor_module
from src.utils.schema_loader import load_schema


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class FakeTime:
    """Deterministic monotonic time for bounded monitor loops."""

    def __init__(self) -> None:
        self.now = 0.0

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture(scope="module")
def schema():
    return load_schema(PROJECT_ROOT / "config" / "graph_schema.yaml")


def _control(payload: dict[str, object]) -> MagicMock:
    message = MagicMock()
    message.error.return_value = None
    import json
    message.value.return_value = json.dumps(payload).encode()
    return message


def _metadata(schema, person_partitions: int = 2):
    return SimpleNamespace(
        topics={
            "person-events": SimpleNamespace(
                error=None,
                partitions={index: SimpleNamespace() for index in range(person_partitions)},
            ),
            "company-events": SimpleNamespace(
                error=None,
                partitions={0: SimpleNamespace()},
            ),
        }
    )


def _monitor(schema, *, expected=None, containers=None, watermarks=None, offsets=None):
    expected = expected or {("Person", "0"), ("Person", "1"), ("Company", "0")}
    containers = containers or {replica: MagicMock(attrs={"State": {"Status": "running"}}) for replica in expected}
    watermarks = watermarks or {
        ("person-events", 0): (0, 3),
        ("person-events", 1): (0, 2),
        ("company-events", 0): (0, 4),
    }
    offsets = offsets or {
        "graph-loader-Person": {("person-events", 0): 3, ("person-events", 1): 2},
        "graph-loader-Company": {("company-events", 0): 4},
    }
    admin, watermark, control = MagicMock(), MagicMock(), MagicMock()
    admin.list_topics.return_value = _metadata(schema)
    watermark.get_watermark_offsets.side_effect = lambda partition, timeout: watermarks[
        (partition.topic, partition.partition)
    ]

    def group_offsets(requests, require_stable):
        request = requests[0]
        entries = [
            TopicPartition(topic, partition, offset)
            for (topic, partition), offset in offsets[request.group_id].items()
        ]
        future = MagicMock()
        future.result.return_value = SimpleNamespace(topic_partitions=entries)
        return {request.group_id: future}

    admin.list_consumer_group_offsets.side_effect = group_offsets
    control.poll.return_value = None
    clock = FakeTime()
    monitor = BulkMonitor(
        schema,
        "run-1",
        expected,
        containers,
        bootstrap_servers="kafka:9092",
        admin_client=admin,
        watermark_consumer=watermark,
        control_consumer=control,
        poll_interval_seconds=0.1,
        clock=clock.clock,
        sleep=clock.sleep,
    )
    return monitor, admin, watermark, control, clock, containers


def _assignment(label: str, replica_id: str, epoch: int, partitions: list[dict[str, object]]) -> dict[str, object]:
    return {
        "run_id": "run-1",
        "node_label": label,
        "replica_id": replica_id,
        "type": "ASSIGNMENT",
        "assignment_epoch": epoch,
        "assigned_partitions": partitions,
    }


def _cover_all_partitions(monitor: BulkMonitor) -> None:
    monitor._decode_control(_control(_assignment("Person", "0", 1, [{"topic": "person-events", "partition": 0}])))
    monitor._decode_control(_control(_assignment("Person", "1", 1, [{"topic": "person-events", "partition": 1}])))
    monitor._decode_control(_control(_assignment("Company", "0", 1, [{"topic": "company-events", "partition": 0}])))


def test_assignment_coverage_is_run_and_epoch_scoped(schema):
    monitor, *_ = _monitor(schema)
    monitor._decode_control(_control({**_assignment("Person", "0", 5, [{"topic": "person-events", "partition": 0}]), "run_id": "old"}))
    monitor._decode_control(_control(_assignment("Person", "0", 2, [{"topic": "person-events", "partition": 0}])))
    monitor._decode_control(_control(_assignment("Person", "0", 1, [{"topic": "person-events", "partition": 1}])))
    monitor._decode_control(_control(_assignment("Person", "1", 1, [{"topic": "person-events", "partition": 1}])))
    monitor._decode_control(_control(_assignment("Company", "0", 1, [{"topic": "company-events", "partition": 0}])))

    assert monitor._has_assignment_coverage() is True
    assert monitor._acks[("Person", "0")].epoch == 2


@pytest.mark.parametrize("bad_label,bad_replica", [([], "0"), ("Person", {}), ("", "0"), ("Person", "")])
def test_malformed_control_envelopes_are_ignored(schema, bad_label, bad_replica):
    monitor, *_ = _monitor(schema)

    monitor._decode_control(_control({
        "run_id": "run-1", "node_label": bad_label, "replica_id": bad_replica,
        "type": "ASSIGNMENT", "assignment_epoch": 1,
        "assigned_partitions": [{"topic": "person-events", "partition": 0}],
    }))

    assert monitor._acks == {}


def test_duplicate_assignment_is_transient_but_duplicate_in_one_ack_fails(schema):
    monitor, *_ = _monitor(schema)
    monitor._decode_control(_control(_assignment("Person", "0", 1, [{"topic": "person-events", "partition": 0}])))
    monitor._decode_control(_control(_assignment("Person", "1", 1, [{"topic": "person-events", "partition": 0}])))
    monitor._decode_control(_control(_assignment("Company", "0", 1, [{"topic": "company-events", "partition": 0}])))
    assert monitor._has_assignment_coverage() is False

    with pytest.raises(AssignmentCoverageError, match="duplicates"):
        monitor._decode_control(_control(_assignment("Person", "1", 2, [
            {"topic": "person-events", "partition": 1},
            {"topic": "person-events", "partition": 1},
        ])))


def test_replica_cannot_claim_another_label_topic(schema):
    monitor, *_ = _monitor(schema)

    with pytest.raises(AssignmentCoverageError, match="another node label"):
        monitor._decode_control(_control(_assignment("Person", "0", 1, [
            {"topic": "company-events", "partition": 0},
        ])))


def test_idle_surplus_is_accepted_only_with_exact_partition_cover(schema):
    expected = {("Person", "0"), ("Person", "1"), ("Company", "0")}
    monitor, *_ = _monitor(schema, expected=expected)
    monitor._decode_control(_control(_assignment("Person", "0", 1, [
        {"topic": "person-events", "partition": 0},
        {"topic": "person-events", "partition": 1},
    ])))
    monitor._decode_control(_control({
        "run_id": "run-1", "node_label": "Person", "replica_id": "1",
        "type": "IDLE_SURPLUS", "assignment_epoch": 1,
    }))
    monitor._decode_control(_control(_assignment("Company", "0", 1, [{"topic": "company-events", "partition": 0}])))

    assert monitor._has_assignment_coverage() is True


def test_capture_boundary_uses_qualified_partitions_and_real_watermarks(schema):
    monitor, _, watermark, _, _, _ = _monitor(schema)
    _cover_all_partitions(monitor)

    assert monitor.capture_boundary() == {
        ("person-events", 0): 3,
        ("person-events", 1): 2,
        ("company-events", 0): 4,
    }
    assert watermark.get_watermark_offsets.call_count == 3


def test_wait_for_completion_uses_group_offset_request_api(schema):
    monitor, admin, _, _, _, _ = _monitor(schema)
    _cover_all_partitions(monitor)
    monitor.capture_boundary()

    monitor.wait_for_completion(timeout_seconds=1)

    assert admin.list_consumer_group_offsets.call_count == 2
    requests = [call.args[0][0] for call in admin.list_consumer_group_offsets.call_args_list]
    assert {request.group_id for request in requests} == {"graph-loader-Person", "graph-loader-Company"}
    person_request = next(request for request in requests if request.group_id == "graph-loader-Person")
    assert {(item.topic, item.partition) for item in person_request.topic_partitions} == {
        ("person-events", 0), ("person-events", 1)
    }


def test_completion_rejects_late_arrival_and_missing_offsets(schema):
    monitor, _, watermark, _, _, _ = _monitor(schema)
    _cover_all_partitions(monitor)
    monitor.capture_boundary()
    watermark.get_watermark_offsets.side_effect = lambda partition, timeout: (0, 4) if (
        partition.topic, partition.partition
    ) == ("person-events", 0) else {("person-events", 1): (0, 2), ("company-events", 0): (0, 4)}[
        (partition.topic, partition.partition)
    ]
    with pytest.raises(QuiescentViolationError):
        monitor.wait_for_completion(timeout_seconds=1)

    unresolved, *_ = _monitor(schema, offsets={
        "graph-loader-Person": {("person-events", 0): 3, ("person-events", 1): -1001},
        "graph-loader-Company": {("company-events", 0): 4},
    })
    _cover_all_partitions(unresolved)
    unresolved.capture_boundary()
    assert unresolved._at_boundary() is False


def test_completion_clips_stalled_admin_future_to_stage_deadline(schema):
    monitor, admin, _, _, _, _ = _monitor(schema)
    _cover_all_partitions(monitor)
    monitor.capture_boundary()
    future = MagicMock()
    future.result.side_effect = TimeoutError("broker stalled")
    admin.list_consumer_group_offsets.side_effect = None
    admin.list_consumer_group_offsets.return_value = {"graph-loader-Person": future}

    with pytest.raises(OffsetResolutionError, match="committed offsets"):
        monitor.wait_for_completion(timeout_seconds=0.5)

    future.result.assert_called_once_with(timeout=0.5)


def test_crash_during_coverage_is_attributed(schema):
    containers = {replica: MagicMock(attrs={"State": {"Status": "running"}}) for replica in {
        ("Person", "0"), ("Person", "1"), ("Company", "0")
    }}
    containers[("Person", "1")].attrs = {"State": {"Status": "exited", "ExitCode": 1}}
    monitor, *_ = _monitor(schema, containers=containers)

    with pytest.raises(LoaderCrashError, match="label=Person replica=1"):
        monitor.wait_for_assignment_coverage(timeout_seconds=1)


def test_drain_polls_ack_before_accepting_auto_removed_container(schema):
    monitor, _, _, _, _, containers = _monitor(schema)
    _cover_all_partitions(monitor)
    containers[("Person", "0")].reload.side_effect = NotFound("gone")
    # A missing acknowledgement never counts as successful drain. Attribution is
    # retained, but only after the bounded control loop had time to receive it.
    with pytest.raises(LoaderCrashError, match="label=Person replica=0"):
        monitor.wait_for_drain_complete(timeout_seconds=1)

    acknowledged, _, _, control, _, containers = _monitor(schema)
    _cover_all_partitions(acknowledged)
    acknowledgements = []
    for label, replica_id in [("Person", "0"), ("Person", "1"), ("Company", "0")]:
        acknowledgements.append(_control({
            "run_id": "run-1", "node_label": label, "replica_id": replica_id,
            "type": "DRAIN_COMPLETE", "assignment_epoch": 1,
        }))
    # Control records arrive after one exact container has already vanished.
    control.poll.side_effect = acknowledgements
    containers[("Person", "0")].reload.side_effect = NotFound("gone")
    # An exited object has the same delayed-control race as an auto-removed one.
    containers[("Company", "0")].attrs = {"State": {"Status": "exited", "ExitCode": 0}}

    acknowledged.wait_for_drain_complete(timeout_seconds=1)


def test_idle_surplus_must_ack_drain_before_disappearing(schema):
    monitor, _, _, _, _, containers = _monitor(schema)
    _cover_all_partitions(monitor)
    monitor._decode_control(_control({
        "run_id": "run-1", "node_label": "Person", "replica_id": "1",
        "type": "IDLE_SURPLUS", "assignment_epoch": 2,
    }))
    containers[("Person", "1")].reload.side_effect = NotFound("gone")

    with pytest.raises(LoaderCrashError, match="label=Person replica=1"):
        monitor.wait_for_drain_complete(timeout_seconds=1)


def test_verify_zero_lag_rejects_remaining_lag(schema):
    monitor, *_ = _monitor(schema, offsets={
        "graph-loader-Person": {("person-events", 0): 2, ("person-events", 1): 2},
        "graph-loader-Company": {("company-events", 0): 4},
    })
    _cover_all_partitions(monitor)
    monitor.capture_boundary()

    with pytest.raises(OffsetResolutionError, match="nonzero lag"):
        monitor.verify_zero_lag()


def test_duplicate_configured_topics_are_rejected(schema):
    duplicate = schema.model_copy(deep=True)
    duplicate.nodes[1].topic = duplicate.nodes[0].topic
    with pytest.raises(BulkMonitorError, match="distinct Kafka topic"):
        _monitor(duplicate)


def test_constructor_closes_owned_consumers_when_topology_discovery_fails(schema, monkeypatch):
    watermark, control = MagicMock(), MagicMock()
    monkeypatch.setattr(bulk_monitor_module, "Consumer", MagicMock(side_effect=[watermark, control]))
    admin = MagicMock()
    admin.list_topics.side_effect = RuntimeError("metadata unavailable")
    expected = {("Person", "0"), ("Person", "1"), ("Company", "0")}
    containers = {replica: MagicMock() for replica in expected}

    with pytest.raises(OffsetResolutionError):
        BulkMonitor(
            schema,
            "run-1",
            expected,
            containers,
            bootstrap_servers="kafka:9092",
            admin_client=admin,
        )

    watermark.close.assert_called_once()
    control.close.assert_called_once()
