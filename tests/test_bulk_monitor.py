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


def test_completed_node_label_can_drain_before_other_labels(schema):
    offsets = {
        "graph-loader-Person": {("person-events", 0): 3, ("person-events", 1): 2},
        "graph-loader-Company": {("company-events", 0): 0},
    }
    monitor, _, _, _, _, containers = _monitor(schema, offsets=offsets)
    _cover_all_partitions(monitor)
    monitor.capture_boundary()

    assert monitor.wait_for_next_label_completion({"Person", "Company"}, 1) == "Person"
    for replica, container in containers.items():
        if replica[0] == "Person":
            container.attrs["State"]["Status"] = "exited"
            monitor._decode_control(_control({
                "run_id": "run-1", "node_label": "Person", "replica_id": replica[1],
                "type": "DRAIN_COMPLETE", "assignment_epoch": 1,
            }))
    monitor.wait_for_label_drain_complete("Person", 1)
    monitor.verify_label_zero_lag("Person")

    # Stopping one replica can rebalance its finished consumer group. Its
    # retained assignment no longer forms a global exact cover, but Company
    # still has a valid live cover and must be allowed to finish.
    monitor._decode_control(_control(_assignment("Person", "1", 2, [
        {"topic": "person-events", "partition": 0},
        {"topic": "person-events", "partition": 1},
    ])))
    monitor._decode_control(_control({
        "run_id": "run-1", "node_label": "Person", "replica_id": "1",
        "type": "DRAIN_COMPLETE", "assignment_epoch": 2,
    }))
    assert not monitor._has_assignment_coverage()

    offsets["graph-loader-Company"][("company-events", 0)] = 4
    assert monitor.wait_for_next_label_completion({"Company"}, 1) == "Company"


def test_assignment_coverage_is_run_and_epoch_scoped(schema):
    monitor, *_ = _monitor(schema)
    monitor._decode_control(_control({**_assignment("Person", "0", 5, [{"topic": "person-events", "partition": 0}]), "run_id": "old"}))
    monitor._decode_control(_control(_assignment("Person", "0", 2, [{"topic": "person-events", "partition": 0}])))
    monitor._decode_control(_control(_assignment("Person", "0", 1, [{"topic": "person-events", "partition": 1}])))
    monitor._decode_control(_control(_assignment("Person", "1", 1, [{"topic": "person-events", "partition": 1}])))
    monitor._decode_control(_control(_assignment("Company", "0", 1, [{"topic": "company-events", "partition": 0}])))

    assert monitor._has_assignment_coverage() is True
    assert monitor._acks[("Person", "0")].epoch == 2


def test_assignment_coverage_drains_foreign_control_backlog_in_one_iteration(schema):
    monitor, _, _, control, _, _ = _monitor(schema)
    foreign = _control({**_assignment("Person", "0", 1, [{"topic": "person-events", "partition": 0}]), "run_id": "prior-run"})
    control.poll.side_effect = [
        *([foreign] * 50),
        _control(_assignment("Person", "0", 1, [{"topic": "person-events", "partition": 0}])),
        _control(_assignment("Person", "1", 1, [{"topic": "person-events", "partition": 1}])),
        _control(_assignment("Company", "0", 1, [{"topic": "company-events", "partition": 0}])),
    ]

    monitor._poll_control()

    assert monitor._has_assignment_coverage()
    assert control.poll.call_count == 53
    assert all(call.args[0] in {0.01, 0.1} for call in control.poll.call_args_list)


def test_control_backlog_drain_is_bounded_before_the_next_health_check(schema):
    monitor, _, _, control, _, containers = _monitor(schema)
    foreign = _control({**_assignment("Person", "0", 1, [{"topic": "person-events", "partition": 0}]), "run_id": "prior-run"})
    control.poll.side_effect = [foreign] * 1_002

    monitor._poll_control()

    assert control.poll.call_count == 1_001
    monitor._check_container_health()
    next(iter(containers.values())).reload.assert_called_once_with()


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


def test_empty_partition_needs_no_offset_but_nonempty_partition_still_does(schema):
    monitor, _, _, _, _, _ = _monitor(schema, offsets={
        "graph-loader-Person": {("person-events", 0): 3, ("person-events", 1): 2},
        "graph-loader-Company": {},
    })
    _cover_all_partitions(monitor)
    monitor._boundary = {
        ("person-events", 0): 3,
        ("person-events", 1): 2,
        ("company-events", 0): 0,
    }
    monitor._watermarks = lambda *_a: (dict(monitor._boundary), {})

    assert monitor._at_boundary() is True
    monitor.verify_zero_lag()


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
    pending_acknowledgements = iter(acknowledgements)
    control.poll.side_effect = lambda *_args: next(pending_acknowledgements, None)
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


# --- Concurrent per-label readiness -------------------------------------------------


def _drain_ack(label: str, replica_id: str, epoch: int = 1) -> MagicMock:
    return _control({
        "run_id": "run-1", "node_label": label, "replica_id": replica_id,
        "type": "DRAIN_COMPLETE", "assignment_epoch": epoch,
    })


def _readiness_monitor(schema, offsets=None):
    """Return a covered, bounded monitor whose control poll reads a thread-safe queue."""
    import queue
    import time as real_time

    monitor, admin, watermark, control, clock, containers = _monitor(schema, offsets=offsets)
    _cover_all_partitions(monitor)
    monitor.capture_boundary()
    inbox: "queue.Queue[MagicMock]" = queue.Queue()

    def poll(_timeout):
        try:
            return inbox.get_nowait()
        except queue.Empty:
            return None

    control.poll.side_effect = poll

    def sleep(seconds: float) -> None:
        clock.sleep(seconds)
        real_time.sleep(0.001)  # let Docker-stop worker threads make progress

    monitor._sleep = sleep
    return monitor, admin, inbox, clock, containers


def _exit(containers, label: str) -> None:
    for (node_label, _), container in containers.items():
        if node_label == label:
            container.attrs["State"]["Status"] = "exited"


def _ack_all(inbox, containers, label: str) -> None:
    for node_label, replica_id in containers:
        if node_label == label:
            inbox.put(_drain_ack(label, replica_id))


def test_drive_label_readiness_stops_every_label_concurrently(schema):
    """P6-S5 Story 5 (Decision 3): `drive_label_readiness`'s own docstring
    promises "one worker per label by default, so no label's SIGTERM
    queues behind another's Docker stop" (`workers = len(labels) if
    max_concurrent_stops is None else max_concurrent_stops`). Git history
    shows a real queuing bug this comment used to describe was already
    fixed before this audit; this guards the fix with a synchronization
    primitive, not a sleep/timing race.

    A two-party `threading.Barrier` inside `stop_label` only releases once
    BOTH labels' stops have actually started executing. If a future
    regression ever sized the executor to fewer workers than labels (e.g.
    a stray default of 1), the second label's stop could never begin
    until the first returned, so the first call's `barrier.wait()` would
    deterministically time out and raise - not merely run slower.
    """
    import threading

    monitor, _, inbox, _, containers = _readiness_monitor(schema)
    barrier = threading.Barrier(2, timeout=5)

    def stop_label(label: str) -> None:
        barrier.wait()
        _exit(containers, label)
        _ack_all(inbox, containers, label)

    ready: list[str] = []
    monitor.drive_label_readiness(
        {"Person", "Company"}, 30, stop_label=stop_label, on_ready=ready.append,
    )
    assert set(ready) == {"Person", "Company"}


def test_label_ready_while_earlier_stopped_label_is_still_draining(schema):
    offsets = {
        "graph-loader-Person": {("person-events", 0): 3, ("person-events", 1): 2},
        "graph-loader-Company": {("company-events", 0): 1},
    }
    monitor, _, inbox, _, containers = _readiness_monitor(schema, offsets)
    events: list[str] = []

    def stop_label(label: str) -> None:
        events.append(f"stop:{label}")
        _exit(containers, label)
        if label == "Person":
            # Person stops first but its drain acknowledgement is slow; only
            # now does Company reach its boundary.
            offsets["graph-loader-Company"][("company-events", 0)] = 4
        else:
            _ack_all(inbox, containers, "Company")

    def on_ready(label: str) -> None:
        events.append(f"ready:{label}")
        if label == "Company":
            _ack_all(inbox, containers, "Person")

    monitor.drive_label_readiness({"Person", "Company"}, 30, stop_label=stop_label, on_ready=on_ready)
    assert events == ["stop:Person", "stop:Company", "ready:Company", "ready:Person"]


def test_label_is_not_ready_unless_offsets_read_after_drain_equal_boundary(schema):
    monitor, admin, inbox, _, containers = _readiness_monitor(schema)
    ready: list[str] = []

    def stop_label(label: str) -> None:
        _exit(containers, label)
        _ack_all(inbox, containers, label)

    original = admin.list_consumer_group_offsets.side_effect
    drained: set[str] = set()

    def drifting(requests, require_stable):
        group_id = requests[0].group_id
        if group_id == "graph-loader-Company" and "Company" in drained:
            future = MagicMock()
            future.result.return_value = SimpleNamespace(
                topic_partitions=[TopicPartition("company-events", 0, 5)]
            )
            return {group_id: future}
        return original(requests, require_stable)

    def stop_then_drift(label: str) -> None:
        stop_label(label)
        drained.add(label)

    admin.list_consumer_group_offsets.side_effect = drifting
    with pytest.raises(OffsetResolutionError, match="nonzero lag label=Company"):
        monitor.drive_label_readiness(
            {"Person", "Company"}, 30, stop_label=stop_then_drift, on_ready=ready.append,
        )
    assert "Company" not in ready


def test_drain_failure_of_one_label_fails_without_waiting_for_others(schema):
    offsets = {
        "graph-loader-Person": {("person-events", 0): 3, ("person-events", 1): 2},
        "graph-loader-Company": {("company-events", 0): 0},
    }
    monitor, _, _, clock, containers = _readiness_monitor(schema, offsets)
    stopped: list[str] = []
    ready: list[str] = []

    def stop_label(label: str) -> None:
        stopped.append(label)
        for (node_label, _), container in containers.items():
            if node_label == label:
                container.reload.side_effect = NotFound("removed")

    with pytest.raises(LoaderCrashError, match="label=Person replica=0"):
        monitor.drive_label_readiness(
            {"Person", "Company"}, 5, stop_label=stop_label, on_ready=ready.append,
        )
    assert stopped == ["Person"] and ready == []
    assert clock.now < 12  # bounded by the per-label drain timeout


def test_live_label_crash_during_another_labels_drain_fails_immediately(schema):
    offsets = {
        "graph-loader-Person": {("person-events", 0): 3, ("person-events", 1): 2},
        "graph-loader-Company": {("company-events", 0): 0},
    }
    monitor, _, _, clock, containers = _readiness_monitor(schema, offsets)

    def stop_label(label: str) -> None:
        _exit(containers, label)
        containers[("Company", "0")].attrs["State"] = {"Status": "exited", "ExitCode": 1}

    with pytest.raises(LoaderCrashError, match="label=Company replica=0 status=exited exit_code=1"):
        monitor.drive_label_readiness(
            {"Person", "Company"}, 30, stop_label=stop_label, on_ready=lambda _label: None,
        )
    assert clock.now < 30


def test_stop_failure_and_abort_propagate(schema):
    monitor, *_ = _readiness_monitor(schema)

    def failing_stop(label: str) -> None:
        raise RuntimeError(f"unable to stop node label={label}")

    with pytest.raises(RuntimeError, match="unable to stop node label="):
        monitor.drive_label_readiness(
            {"Person", "Company"}, 30, stop_label=failing_stop, on_ready=lambda _label: None,
        )

    monitor, *_ = _readiness_monitor(schema)
    with pytest.raises(BulkMonitorError, match="aborted"):
        monitor.drive_label_readiness(
            {"Person", "Company"}, 30, stop_label=lambda _label: None,
            on_ready=lambda _label: None, should_abort=lambda: True,
        )


def test_completion_timeout_is_preserved(schema):
    offsets = {
        "graph-loader-Person": {("person-events", 0): 0, ("person-events", 1): 0},
        "graph-loader-Company": {("company-events", 0): 0},
    }
    monitor, _, _, clock, _ = _readiness_monitor(schema, offsets)
    with pytest.raises(bulk_monitor_module.BulkTimeoutError, match="node label completion"):
        monitor.drive_label_readiness(
            {"Person", "Company"}, 2, stop_label=lambda _label: None, on_ready=lambda _label: None,
        )
    assert 2 <= clock.now < 3


def test_label_stops_run_concurrently(schema):
    from threading import Barrier

    monitor, _, inbox, _, containers = _readiness_monitor(schema)
    barrier = Barrier(2, timeout=3)

    def stop_label(label: str) -> None:
        barrier.wait()  # both labels' stops must be in flight together
        _exit(containers, label)
        _ack_all(inbox, containers, label)

    ready: list[str] = []
    monitor.drive_label_readiness({"Person", "Company"}, 30, stop_label=stop_label, on_ready=ready.append)
    assert sorted(ready) == ["Company", "Person"]


# --- Per-label coverage / boundary and non-blocking stops ---------------------------


def _uncovered_readiness_monitor(schema, offsets=None, watermarks=None):
    """A readiness monitor with no assignment acks and no captured boundary."""
    import queue
    import time as real_time

    monitor, admin, watermark, control, clock, containers = _monitor(
        schema, offsets=offsets, watermarks=watermarks,
    )
    inbox: "queue.Queue[MagicMock]" = queue.Queue()

    def poll(_timeout):
        try:
            return inbox.get_nowait()
        except queue.Empty:
            return None

    control.poll.side_effect = poll

    def sleep(seconds: float) -> None:
        clock.sleep(seconds)
        real_time.sleep(0.001)

    monitor._sleep = sleep
    return monitor, admin, watermark, inbox, clock, containers


def _person_assignments(inbox) -> None:
    inbox.put(_control(_assignment("Person", "0", 1, [{"topic": "person-events", "partition": 0}])))
    inbox.put(_control(_assignment("Person", "1", 1, [{"topic": "person-events", "partition": 1}])))


def test_label_boundary_requires_that_labels_exact_coverage(schema):
    monitor, *_ = _monitor(schema)
    monitor._decode_control(_control(_assignment("Company", "0", 1, [{"topic": "company-events", "partition": 0}])))
    monitor._decode_control(_control(_assignment("Person", "0", 1, [{"topic": "person-events", "partition": 0}])))

    with pytest.raises(AssignmentCoverageError, match="label=Person"):
        monitor.capture_label_boundary("Person")
    assert monitor.capture_label_boundary("Company") == {("company-events", 0): 4}
    # The global boundary is untouched: whole-fleet stages still require it.
    assert monitor.boundary is None


def test_small_label_is_ready_before_another_label_proves_coverage(schema):
    monitor, _, watermark, inbox, _, containers = _uncovered_readiness_monitor(schema)
    inbox.put(_control(_assignment("Company", "0", 1, [{"topic": "company-events", "partition": 0}])))
    events: list[str] = []

    def stop_label(label: str) -> None:
        events.append(f"stop:{label}")
        _exit(containers, label)
        _ack_all(inbox, containers, label)

    def on_ready(label: str) -> None:
        events.append(f"ready:{label}")
        if label == "Company":
            # Person's group only settles after Company was already published.
            assert monitor._label_boundary("Person") is None
            _person_assignments(inbox)

    monitor.drive_label_readiness({"Person", "Company"}, 30, stop_label=stop_label, on_ready=on_ready)

    assert events == ["stop:Company", "ready:Company", "stop:Person", "ready:Person"]
    assert monitor._label_boundaries == {
        "Company": {("company-events", 0): 4},
        "Person": {("person-events", 0): 3, ("person-events", 1): 2},
    }


def test_label_is_not_stopped_until_its_own_coverage_and_boundary(schema):
    monitor, _, _, inbox, _, containers = _uncovered_readiness_monitor(schema)
    # Only one of Person's two replicas has acknowledged; offsets are already
    # at the watermarks, but without exact coverage Person must not stop.
    inbox.put(_control(_assignment("Person", "0", 1, [{"topic": "person-events", "partition": 0}])))
    inbox.put(_control(_assignment("Company", "0", 1, [{"topic": "company-events", "partition": 0}])))
    stopped: list[str] = []

    def stop_label(label: str) -> None:
        stopped.append(label)
        _exit(containers, label)
        _ack_all(inbox, containers, label)

    with pytest.raises(bulk_monitor_module.BulkTimeoutError, match="node label completion"):
        monitor.drive_label_readiness(
            {"Person", "Company"}, 2, stop_label=stop_label, on_ready=lambda _label: None,
        )
    assert stopped == ["Company"]


def test_label_boundary_input_change_is_a_quiescence_violation(schema):
    watermarks = {
        ("person-events", 0): (0, 3),
        ("person-events", 1): (0, 2),
        ("company-events", 0): (0, 4),
    }
    offsets = {
        "graph-loader-Person": {("person-events", 0): 0, ("person-events", 1): 0},
        "graph-loader-Company": {("company-events", 0): 0},
    }
    monitor, _, _, inbox, _, _ = _uncovered_readiness_monitor(schema, offsets=offsets, watermarks=watermarks)
    inbox.put(_control(_assignment("Company", "0", 1, [{"topic": "company-events", "partition": 0}])))
    original = monitor.capture_label_boundary

    def capture_then_publish(label: str):
        boundary = original(label)
        watermarks[("company-events", 0)] = (0, 5)
        return boundary

    monitor.capture_label_boundary = capture_then_publish
    with pytest.raises(QuiescentViolationError, match="company-events"):
        monitor.drive_label_readiness(
            {"Person", "Company"}, 30, stop_label=lambda _label: None, on_ready=lambda _label: None,
        )


def test_readiness_does_not_wait_for_docker_teardown_but_waits_before_returning(schema):
    from threading import Event

    monitor, _, inbox, _, containers = _readiness_monitor(schema)
    release = Event()
    ready: list[str] = []
    returned: list[str] = []

    def stop_label(label: str) -> None:
        # SIGTERM delivered: the replica drains and acknowledges, but Docker's
        # network/container teardown is still in progress.
        _ack_all(inbox, containers, label)
        assert release.wait(5)
        returned.append(label)

    def on_ready(label: str) -> None:
        ready.append(label)
        assert label not in returned
        if len(ready) == 2:
            release.set()

    monitor.drive_label_readiness({"Person", "Company"}, 30, stop_label=stop_label, on_ready=on_ready)
    assert sorted(ready) == ["Company", "Person"]
    assert sorted(returned) == ["Company", "Person"]


def test_stop_failure_after_readiness_still_fails(schema):
    import time as real_time

    monitor, _, inbox, _, containers = _readiness_monitor(schema)
    ready: list[str] = []

    def stop_label(label: str) -> None:
        _ack_all(inbox, containers, label)
        if label == "Company":
            deadline = real_time.monotonic() + 5
            while "Company" not in ready and real_time.monotonic() < deadline:
                real_time.sleep(0.001)
            raise RuntimeError("unable to stop node label=Company")

    with pytest.raises(RuntimeError, match="unable to stop node label=Company"):
        monitor.drive_label_readiness({"Person", "Company"}, 30, stop_label=stop_label, on_ready=ready.append)
    assert "Company" in ready


def test_hung_docker_stop_is_bounded(schema):
    from threading import Event

    monitor, _, inbox, _, containers = _readiness_monitor(schema)
    never = Event()

    def stop_label(label: str) -> None:
        _ack_all(inbox, containers, label)
        never.wait(5)

    try:
        with pytest.raises(bulk_monitor_module.BulkTimeoutError, match="container stop"):
            monitor.drive_label_readiness(
                {"Person", "Company"}, 1, stop_label=stop_label, on_ready=lambda _label: None,
            )
    finally:
        never.set()



# -- P6-S5 Story 2: an operator stop aborts a wait promptly --------------------

_BEHIND = {
    "graph-loader-Person": {("person-events", 0): 0, ("person-events", 1): 0},
    "graph-loader-Company": {("company-events", 0): 0},
}


def test_wait_aborts_on_shutdown_request_long_before_the_deadline(schema):
    monitor, _, _, clock, _ = _readiness_monitor(schema, _BEHIND)
    polls = []

    def abort_requested() -> bool:
        polls.append(clock.now)
        return len(polls) > 3  # the operator's signal lands on the 4th poll

    monitor._abort_requested = abort_requested
    with pytest.raises(bulk_monitor_module.BulkShutdownRequested, match="shutdown requested"):
        monitor.wait_for_completion(300)  # nothing ever completes
    assert clock.now < 1  # a handful of poll intervals, not the 300s stage deadline
    # A BulkTimeoutError subclass: existing fail-closed call sites keep working.
    assert issubclass(bulk_monitor_module.BulkShutdownRequested, bulk_monitor_module.BulkTimeoutError)


def test_wait_never_aborts_when_shutdown_is_not_requested(schema):
    monitor, *_ = _readiness_monitor(schema, _BEHIND)
    monitor._abort_requested = lambda: False
    with pytest.raises(bulk_monitor_module.BulkTimeoutError) as excinfo:
        monitor.wait_for_completion(1)
    assert not isinstance(excinfo.value, bulk_monitor_module.BulkShutdownRequested)
