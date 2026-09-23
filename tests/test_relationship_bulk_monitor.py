"""Unit tests for one run-scoped relationship bulk stage monitor."""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from confluent_kafka import TopicPartition
from docker.errors import NotFound

from src.orchestrator.bulk_monitor import AssignmentCoverageError, BulkTimeoutError, QuiescentViolationError
from src.orchestrator.coordination import ClockLease, encode_clock_lease
from src.orchestrator.dependency_manager import build_conflict_families
from src.orchestrator.relationship_bulk_monitor import EdgeControlAck, RelationshipBulkMonitor
from src.orchestrator.rotation import build_rotation_plan
from src.utils.schema_loader import load_schema


ROOT = Path(__file__).resolve().parents[1]


def _message(payload):
    message = MagicMock(); message.error.return_value = None
    message.value.return_value = json.dumps(payload).encode()
    return message


def _monitor():
    schema = load_schema(ROOT / "config" / "graph_schema.yaml")
    edge = next(edge for edge in schema.edges if edge.type == "WORKS_AT")
    admin, watermark, control = MagicMock(), MagicMock(), MagicMock()
    admin.list_topics.return_value = SimpleNamespace(topics={
        "works-at-events": SimpleNamespace(error=None, partitions={0: SimpleNamespace()}),
    })
    watermark.get_watermark_offsets.return_value = (0, 3)
    future = MagicMock(); future.result.return_value = SimpleNamespace(topic_partitions=[TopicPartition("works-at-events", 0, 3)])
    admin.list_consumer_group_offsets.return_value = {"graph-loader-WORKS_AT-run-1": future}
    control.poll.return_value = None
    container = MagicMock(); container.attrs = {"State": {"Status": "running"}}
    monitor = RelationshipBulkMonitor(
        [edge], "run-1", {("WORKS_AT", "0")}, {("WORKS_AT", "0"): container},
        bootstrap_servers="kafka:9092", consumer_group_prefix="graph-loader",
        admin_client=admin, watermark_consumer=watermark, control_consumer=control,
        poll_interval_seconds=0.01,
    )
    return monitor, admin, watermark, control, container


def _assignment(epoch=1, *, partitions=None, run_id="run-1", edge_type="WORKS_AT"):
    if partitions is None:
        partitions = [{"topic": "works-at-events", "partition": 0}]
    return {"run_id": run_id, "edge_type": edge_type, "replica_id": "0", "type": "ASSIGNMENT", "assignment_epoch": epoch, "assigned_partitions": partitions}


def test_stage_requires_current_run_and_edge_then_tracks_exact_group():
    monitor, admin, _, _, _ = _monitor()
    monitor._decode_control(_message(_assignment(run_id="old")))
    monitor._decode_control(_message(_assignment(edge_type="OTHER")))
    assert not monitor._coverage()
    monitor._decode_control(_message(_assignment()))
    assert monitor._coverage()
    monitor.capture_boundary()
    monitor.wait_for_completion(1)
    assert admin.list_consumer_group_offsets.call_args.args[0][0].group_id == "graph-loader-WORKS_AT-run-1"


def test_conflicting_same_epoch_and_future_drain_are_rejected_or_ignored():
    monitor, *_ = _monitor()
    monitor._decode_control(_message(_assignment()))
    idle = {**_assignment(), "type": "IDLE_SURPLUS"}
    idle.pop("assigned_partitions")
    with pytest.raises(AssignmentCoverageError, match="conflicting same-epoch"):
        monitor._decode_control(_message(idle))
    monitor._decode_control(_message({**_assignment(), "type": "DRAIN_COMPLETE", "assignment_epoch": 2}))
    assert monitor._drains == {}


def test_late_input_after_boundary_is_rejected():
    monitor, _, watermark, _, _ = _monitor()
    monitor._decode_control(_message(_assignment()))
    monitor.capture_boundary()
    watermark.get_watermark_offsets.return_value = (0, 4)
    with pytest.raises(QuiescentViolationError):
        monitor._assert_quiescent()


def test_stage_monitor_uses_one_commit_snapshot_and_clipped_deadline():
    monitor, admin, _, _, _ = _monitor()
    monitor._decode_control(_message(_assignment()))
    monitor.capture_boundary()
    monitor._operation_deadline = monitor._clock() + 0.01
    assert monitor._request_timeout() <= 0.01
    assert monitor._at_boundary()
    assert admin.list_consumer_group_offsets.call_count == 1


def test_control_poll_timeout_is_clipped_to_stage_deadline():
    monitor, _, _, control, _ = _monitor()
    monitor._operation_deadline = monitor._clock() + 0.001
    monitor._poll()
    assert control.poll.call_args.args[0] <= 0.001


def test_rotation_monitor_drains_foreign_control_backlog_to_current_coverage():
    monitor, _, coordination, _, _ = _rotation_monitor()
    control = monitor._control
    foreign = _message(_assignment(run_id="prior-run"))
    control.poll.side_effect = [
        *([foreign] * 50),
        _message(_assignment(run_id="run-rotation", edge_type="WORKS_AT")),
        _message(_assignment(
            run_id="run-rotation", edge_type="BOUGHT",
            partitions=[{"topic": "bought-events", "partition": 0}],
        )),
        None,
    ]
    coordination.poll.return_value = None
    monitor._poll()
    assert monitor._coverage()
    assert control.poll.call_count == 53


def test_control_backlog_drain_is_bounded_and_returns_to_health_checks():
    monitor, _, _, control, container = _monitor()
    foreign = _message(_assignment(run_id="prior-run"))
    control.poll.side_effect = [foreign] * 1_002
    monitor._poll()
    assert control.poll.call_count == 1_001
    assert not monitor._coverage()
    monitor._check_health()
    container.reload.assert_called_once_with()


def test_exited_and_removed_edge_container_require_current_drain_ack():
    monitor, _, _, _, container = _monitor()
    monitor._decode_control(_message(_assignment()))
    container.attrs = {"State": {"Status": "exited"}}
    with pytest.raises(Exception, match="exited before drain"):
        monitor._check_health()
    monitor._decode_control(_message({**_assignment(), "type": "DRAIN_COMPLETE"}))
    monitor._check_health(True)
    container.reload.side_effect = NotFound("removed")
    monitor._check_health(True)


def test_missing_committed_offset_fails_zero_lag():
    monitor, admin, _, _, _ = _monitor()
    monitor._decode_control(_message(_assignment()))
    monitor.capture_boundary()
    future = MagicMock(); future.result.return_value = SimpleNamespace(topic_partitions=[])
    admin.list_consumer_group_offsets.return_value = {"graph-loader-WORKS_AT-run-1": future}
    with pytest.raises(Exception, match="nonzero lag"):
        monitor.verify_zero_lag()


def _rotation_monitor():
    works = SimpleNamespace(
        type="WORKS_AT", topic="works-at-events", replicas=1,
        nodes=SimpleNamespace(source="Person", target="Company", is_self_referencing=False),
    )
    bought = SimpleNamespace(
        type="BOUGHT", topic="bought-events", replicas=1,
        nodes=SimpleNamespace(source="Person", target="Product", is_self_referencing=False),
    )
    plan = build_rotation_plan(build_conflict_families((works, bought)), bucket_count=1)
    admin, watermark, control, coordination = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    admin.list_topics.return_value = SimpleNamespace(topics={
        "works-at-events": SimpleNamespace(error=None, partitions={0: SimpleNamespace()}),
        "bought-events": SimpleNamespace(error=None, partitions={0: SimpleNamespace()}),
    })
    watermark.get_watermark_offsets.return_value = (0, 0)
    control.poll.return_value = None
    coordination.poll.return_value = None
    first, second, clock = MagicMock(), MagicMock(), MagicMock()
    for container in (first, second, clock):
        container.attrs = {"State": {"Status": "running"}}
    now = [100]
    monitor = RelationshipBulkMonitor(
        (works, bought), "run-rotation",
        {("WORKS_AT", "0"), ("BOUGHT", "0")},
        {("WORKS_AT", "0"): first, ("BOUGHT", "0"): second},
        bootstrap_servers="kafka:9092", consumer_group_prefix="loader",
        admin_client=admin, watermark_consumer=watermark, control_consumer=control,
        coordination_consumer=coordination, rotation_plan=plan,
        coordination_topic="clock-topic", tracked_clock=clock,
        poll_interval_seconds=0.01, wall_clock_ms=lambda: now[0],
    )
    return monitor, plan, coordination, clock, now


def _rotation_lease(plan, *, epoch=0, slot_id=0, run_id="run-rotation", expires=200):
    return encode_clock_lease(ClockLease(
        run_id=run_id, epoch=epoch, slot_id=slot_id, issued_at_ms=10,
        expires_at_ms=expires, active_edge_types=("BOUGHT", "WORKS_AT"),
        bucket_owners={0: "BOUGHT"}, shared_bucket_owners=plan.owners_for_epoch(epoch),
    ), rotation_plan=plan)


def test_rotation_monitor_uses_distinct_clock_group_and_tracks_current_lease():
    monitor, plan, coordination, _, _ = _rotation_monitor()
    coordination.subscribe.assert_called_once_with(["clock-topic"])
    monitor._decode_clock(_message(json.loads(_rotation_lease(plan).decode())))
    assert monitor.clock_lease is not None
    assert monitor.clock_lease.epoch == 0
    assert monitor._has_current_clock_lease()


def test_rotation_monitor_rejects_conflicting_or_expired_current_lease():
    monitor, plan, _, _, now = _rotation_monitor()
    monitor._decode_clock(_message(json.loads(_rotation_lease(plan).decode())))
    conflicting = json.loads(_rotation_lease(plan).decode())
    conflicting["bucket_owners"] = {"0": "WORKS_AT"}
    with pytest.raises(Exception, match="conflicting clock lease"):
        monitor._decode_clock(_message(conflicting))
    now[0] = 200
    with pytest.raises(Exception, match="clock lease expired"):
        monitor._has_current_clock_lease()

    now[0] = 100
    same_epoch_other_slot = json.loads(_rotation_lease(plan).decode())
    same_epoch_other_slot["slot_id"] = 1
    with pytest.raises(Exception, match="conflicting clock lease epoch=0 slot=1"):
        monitor._decode_clock(_message(same_epoch_other_slot))
    assert monitor.clock_lease is not None
    assert monitor.clock_lease.slot_id == 0


def test_rotation_monitor_rejects_duplicate_and_stale_lease_without_replacing_current():
    monitor, plan, _, _, _ = _rotation_monitor()
    monitor._decode_clock(_message(json.loads(_rotation_lease(plan, epoch=1).decode())))
    with pytest.raises(Exception, match="duplicate clock lease epoch=1 slot=0 last_epoch=1 slot=0"):
        monitor._decode_clock(_message(json.loads(_rotation_lease(plan, epoch=1).decode())))
    with pytest.raises(Exception, match="stale clock lease epoch=0 slot=0 last_epoch=1 slot=0"):
        monitor._decode_clock(_message(json.loads(_rotation_lease(plan, epoch=0).decode())))
    assert monitor.clock_lease is not None
    assert monitor.clock_lease.epoch == 1


def test_rotation_monitor_rejects_malformed_foreign_run_lease_before_filtering():
    monitor, plan, _, _, now = _rotation_monitor()
    now[0] = 1_000
    foreign = json.loads(_rotation_lease(plan, run_id="finished-run", expires=20).decode())
    with pytest.raises(Exception, match="invalid clock lease.*expired"):
        monitor._decode_clock(_message(foreign))
    assert monitor.clock_lease is None


def test_rotation_monitor_clock_health_is_exact_and_attributed():
    monitor, _, _, clock, _ = _rotation_monitor()
    clock.attrs = {"State": {"Status": "exited"}}
    with pytest.raises(Exception, match="stage=monitor run_id=run-rotation clock exited.*epoch=unknown"):
        monitor._check_clock_health()


def test_rotation_monitor_edge_health_has_run_edge_replica_and_clock_context():
    monitor, _, _, _, _ = _rotation_monitor()
    monitor._tracked_containers[("WORKS_AT", "0")].attrs = {"State": {"Status": "exited"}}
    with pytest.raises(
        Exception,
        match="stage=monitor run_id=run-rotation epoch=unknown slot=unknown .*edge=WORKS_AT replica=0 exited",
    ):
        monitor._check_health()


def test_rotating_completion_requires_all_boundaries_across_multiple_accepted_epochs():
    monitor, plan, _, _, _ = _rotation_monitor()
    monitor._decode_control(_message(_assignment(edge_type="WORKS_AT", run_id="run-rotation")))
    monitor._decode_control(_message(_assignment(
        edge_type="BOUGHT", run_id="run-rotation",
        partitions=[{"topic": "bought-events", "partition": 0}],
    )))
    boundary = {
        ("WORKS_AT", "works-at-events", 0): 3,
        ("BOUGHT", "bought-events", 0): 3,
    }
    monitor._boundary = boundary
    monitor._watermarks = lambda: dict(boundary)
    commits = [{key: 0 for key in boundary}, dict(boundary)]
    monitor._commits = lambda: commits.pop(0)
    monitor._decode_clock(_message(json.loads(_rotation_lease(plan, epoch=0).decode())))

    def wait(timeout, name, predicate, *, drain=False):
        assert (timeout, name, drain) == (1, "rotating completion", False)
        assert not predicate()  # One live slot's zero progress is non-terminal.
        monitor._decode_clock(_message(json.loads(_rotation_lease(plan, epoch=1).decode())))
        assert predicate()

    monitor._wait = wait
    monitor.wait_for_rotating_completion(1)
    assert monitor._accepted_clock_epochs == {0, 1}


def test_rotating_completion_fails_closed_without_current_lease_or_on_moved_watermark():
    monitor, plan, _, _, _ = _rotation_monitor()
    monitor._boundary = {
        ("WORKS_AT", "works-at-events", 0): 3,
        ("BOUGHT", "bought-events", 0): 3,
    }
    assert not monitor._at_rotating_boundary()
    monitor._decode_clock(_message(json.loads(_rotation_lease(plan).decode())))
    monitor._watermarks = lambda: {**monitor._boundary, ("WORKS_AT", "works-at-events", 0): 4}
    with pytest.raises(QuiescentViolationError, match="stage=monitor run_id=run-rotation"):
        monitor._at_rotating_boundary()


def test_rotating_drain_timeout_names_missing_edge_replica_epoch_and_slot():
    monitor, plan, _, _, _ = _rotation_monitor()
    monitor._decode_clock(_message(json.loads(_rotation_lease(plan, epoch=1).decode())))
    monitor._acks = {
        ("WORKS_AT", "0"): EdgeControlAck("ASSIGNMENT", 1, frozenset()),
        ("BOUGHT", "0"): EdgeControlAck("ASSIGNMENT", 1, frozenset()),
    }
    monitor._drains = {("WORKS_AT", "0"): 1}

    def timeout(*_args, **_kwargs):
        raise BulkTimeoutError("timed out")

    monitor._wait = timeout
    with pytest.raises(
        BulkTimeoutError,
        match="stage=monitor run_id=run-rotation epoch=1 slot=0 .*edge=BOUGHT replica=0",
    ):
        monitor.wait_for_drain_complete(1)


def test_rotation_monitor_rejects_stale_and_future_drain_acknowledgements():
    monitor, plan, _, _, _ = _rotation_monitor()
    monitor._decode_clock(_message(json.loads(_rotation_lease(plan, epoch=2).decode())))
    monitor._decode_control(_message(_assignment(
        epoch=2, run_id="run-rotation", edge_type="WORKS_AT",
    )))
    for epoch in (1, 3):
        with pytest.raises(
            AssignmentCoverageError,
            match=rf"stage=monitor run_id=run-rotation epoch=2 slot=0 .*edge=WORKS_AT replica=0 drain epoch={epoch} does not match assignment epoch=2",
        ):
            monitor._decode_control(_message({
                "run_id": "run-rotation", "edge_type": "WORKS_AT", "replica_id": "0",
                "type": "DRAIN_COMPLETE", "assignment_epoch": epoch,
            }))
    assert monitor._drains == {}


def test_stream_supervisor_stays_live_until_injected_shutdown_without_lag_checks():
    monitor, _, _, _, _ = _rotation_monitor()
    monitor.wait_for_clock_lease = MagicMock()
    monitor._has_current_clock_lease = MagicMock(return_value=True)

    class Shutdown:
        requested = False

        def is_set(self):
            return self.requested

    shutdown = Shutdown()
    monitor._sleep = lambda _seconds: setattr(shutdown, "requested", True)
    monitor.supervise_stream(shutdown, initial_lease_timeout_seconds=1)
    monitor.wait_for_clock_lease.assert_called_once_with(1)
    monitor._has_current_clock_lease.assert_called_once()


def test_stream_supervisor_propagates_exact_edge_exit_with_monitor_context():
    monitor, _, _, _, _ = _rotation_monitor()
    monitor.wait_for_clock_lease = MagicMock()
    monitor._tracked_containers[("WORKS_AT", "0")].attrs = {"State": {"Status": "exited"}}

    class NeverShutdown:
        def is_set(self):
            return False

    with pytest.raises(Exception, match="stage=monitor run_id=run-rotation .*edge=WORKS_AT replica=0 exited"):
        monitor.supervise_stream(NeverShutdown(), initial_lease_timeout_seconds=1)


def test_stream_supervisor_attributes_initial_lease_timeout():
    monitor, _, _, _, _ = _rotation_monitor()
    monitor.wait_for_clock_lease = MagicMock(side_effect=BulkTimeoutError("timed out"))

    class NeverShutdown:
        def is_set(self):
            return False

    with pytest.raises(BulkTimeoutError, match="stage=monitor run_id=run-rotation .*initial stream clock lease timed out"):
        monitor.supervise_stream(NeverShutdown(), initial_lease_timeout_seconds=1)


def test_stream_supervisor_fails_closed_when_current_lease_expires():
    monitor, plan, _, _, now = _rotation_monitor()
    monitor._decode_clock(_message(json.loads(_rotation_lease(plan, expires=200).decode())))
    monitor.wait_for_clock_lease = MagicMock()
    now[0] = 200

    class NeverShutdown:
        def is_set(self):
            return False

    with pytest.raises(Exception, match="stage=monitor run_id=run-rotation clock lease expired epoch=0 slot=0"):
        monitor.supervise_stream(NeverShutdown(), initial_lease_timeout_seconds=1)


def test_stream_supervisor_propagates_exact_clock_exit_with_monitor_context():
    monitor, _, _, clock, _ = _rotation_monitor()
    monitor.wait_for_clock_lease = MagicMock()
    clock.attrs = {"State": {"Status": "exited"}}

    class NeverShutdown:
        def is_set(self):
            return False

    with pytest.raises(Exception, match="stage=monitor run_id=run-rotation clock exited"):
        monitor.supervise_stream(NeverShutdown(), initial_lease_timeout_seconds=1)


def test_nonrotation_stream_supervisor_stays_live_without_clock_or_lag_completion():
    monitor, _, _, _, _ = _monitor()

    class Shutdown:
        requested = False

        def is_set(self):
            return self.requested

    shutdown = Shutdown()
    monitor._sleep = lambda _seconds: setattr(shutdown, "requested", True)
    monitor.supervise_stream(shutdown)

    assert monitor._coordination is None
    assert monitor._rotation_plan is None
