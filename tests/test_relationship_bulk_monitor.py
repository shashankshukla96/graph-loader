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


def _monitor(*, metrics=None, clock=None, retry_consumers=()):
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
    kwargs = {}
    if metrics is not None:
        kwargs["metrics"] = metrics
    if clock is not None:
        kwargs["clock"] = clock
    if retry_consumers:
        kwargs["tracked_retry_consumers"] = retry_consumers
    monitor = RelationshipBulkMonitor(
        [edge], "run-1", {("WORKS_AT", "0")}, {("WORKS_AT", "0"): container},
        bootstrap_servers="kafka:9092", consumer_group_prefix="graph-loader",
        admin_client=admin, watermark_consumer=watermark, control_consumer=control,
        poll_interval_seconds=0.01, **kwargs,
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


def test_crashed_edge_reports_exit_code_and_log_tail():
    monitor, _, _, _, container = _monitor()
    container.attrs = {"State": {"Status": "exited", "ExitCode": 1}}
    container.logs.return_value = b"Relationship slot lease failure: slot buffer add failed"
    with pytest.raises(Exception, match="exit_code=1.*slot buffer add failed"):
        monitor._check_health()
    container.logs.assert_called_once_with(tail=30, stdout=True, stderr=True)


def test_missing_committed_offset_fails_zero_lag():
    monitor, admin, _, _, _ = _monitor()
    monitor._decode_control(_message(_assignment()))
    monitor.capture_boundary()
    future = MagicMock(); future.result.return_value = SimpleNamespace(topic_partitions=[])
    admin.list_consumer_group_offsets.return_value = {"graph-loader-WORKS_AT-run-1": future}
    with pytest.raises(Exception, match="nonzero lag"):
        monitor.verify_zero_lag()


def test_empty_partition_without_a_kafka_commit_reaches_and_verifies_zero_boundary():
    """Kafka creates no group offset when an assigned bulk topic has no data."""
    monitor, admin, watermark, _, _ = _monitor()
    monitor._decode_control(_message(_assignment()))
    watermark.get_watermark_offsets.return_value = (0, 0)
    monitor.capture_boundary()
    future = MagicMock(); future.result.return_value = SimpleNamespace(topic_partitions=[])
    admin.list_consumer_group_offsets.return_value = {"graph-loader-WORKS_AT-run-1": future}

    assert monitor._at_boundary()
    monitor.verify_zero_lag()


def _rotation_monitor():
    works = SimpleNamespace(
        type="WORKS_AT", topic="works-at-events", topics=["works-at-events"], dead_letter_entity="works-at-events", is_multi_topic=False, replicas=1,
        nodes=SimpleNamespace(source="Person", target="Company", is_self_referencing=False),
    )
    bought = SimpleNamespace(
        type="BOUGHT", topic="bought-events", topics=["bought-events"], dead_letter_entity="bought-events", is_multi_topic=False, replicas=1,
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


def test_rotation_monitor_ignores_expired_foreign_run_lease():
    monitor, plan, _, _, now = _rotation_monitor()
    now[0] = 1_000
    foreign = json.loads(_rotation_lease(plan, run_id="finished-run", expires=20).decode())
    monitor._decode_clock(_message(foreign))
    assert monitor.clock_lease is None


def test_rotation_monitor_clock_health_is_exact_and_attributed():
    monitor, _, _, clock, _ = _rotation_monitor()
    clock.attrs = {"State": {"Status": "exited", "ExitCode": 2}}
    clock.logs.return_value = b"clock publish failed"
    with pytest.raises(Exception, match="clock exited status=exited exit_code=2.*clock publish failed.*epoch=unknown"):
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


# ── Metrics instrumentation (P6-S2 Story 8) ──────────────────────────────────


class _FakeClock:
    """A monotonic-like clock the test can advance explicitly, so the lag
    refresh's own 10s throttle (P6-S2 review S8-1) doesn't have to wait on
    wall-clock time."""

    def __init__(self, start: float = 0.0) -> None:
        self.value = start

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def test_supervise_stream_refreshes_consumer_lag_and_drains_to_zero():
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    clock = _FakeClock()
    monitor, admin, watermark, control, _ = _monitor(metrics=metrics, clock=clock)
    watermark.get_watermark_offsets.return_value = (0, 10)  # low=0, high=10, nothing committed yet.
    admin.list_consumer_group_offsets.return_value = {
        "graph-loader-WORKS_AT-run-1": MagicMock(result=MagicMock(return_value=SimpleNamespace(topic_partitions=[])))
    }

    class Shutdown:
        requested = False

        def is_set(self):
            return self.requested

    shutdown = Shutdown()
    monitor._sleep = lambda _seconds: setattr(shutdown, "requested", True)
    monitor.supervise_stream(shutdown)

    assert metrics.registry.get_sample_value(
        "graph_loader_consumer_lag_records",
        {"kind": "cli", "name": "WORKS_AT", "topic": "works-at-events"},
    ) == 10.0  # nothing committed -> falls back to low=0, full backlog counts as lag.

    # Immediately re-supervising must NOT re-refresh (throttled) even
    # though the underlying data changed - proves the throttle, not just
    # the metric.
    future = MagicMock()
    future.result.return_value = SimpleNamespace(topic_partitions=[TopicPartition("works-at-events", 0, 10)])
    admin.list_consumer_group_offsets.return_value = {"graph-loader-WORKS_AT-run-1": future}
    shutdown.requested = False
    monitor.supervise_stream(shutdown)
    assert metrics.registry.get_sample_value(
        "graph_loader_consumer_lag_records",
        {"kind": "cli", "name": "WORKS_AT", "topic": "works-at-events"},
    ) == 10.0  # still the stale value: throttle window hasn't elapsed.

    # Now the partition is fully drained (committed caught up to high) AND
    # the throttle window has elapsed.
    clock.advance(monitor._LAG_REFRESH_INTERVAL_SECONDS + 1)
    shutdown.requested = False
    monitor.supervise_stream(shutdown)
    assert metrics.registry.get_sample_value(
        "graph_loader_consumer_lag_records",
        {"kind": "cli", "name": "WORKS_AT", "topic": "works-at-events"},
    ) == 0.0


def test_consumer_lag_uses_low_watermark_not_zero_when_nothing_committed():
    """P6-S2 review finding S8-2: an absolute committed=0 fallback would
    wildly overstate lag on a topic aged past retention. Falling back to
    the *low* watermark (Kafka's own convention) instead reports the true
    backlog: 10 records, not 1,000,010."""
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    monitor, admin, watermark, control, _ = _monitor(metrics=metrics)
    watermark.get_watermark_offsets.return_value = (1_000_000, 1_000_010)
    admin.list_consumer_group_offsets.return_value = {
        "graph-loader-WORKS_AT-run-1": MagicMock(result=MagicMock(return_value=SimpleNamespace(topic_partitions=[])))
    }

    class Shutdown:
        requested = False

        def is_set(self):
            return self.requested

    shutdown = Shutdown()
    monitor._sleep = lambda _seconds: setattr(shutdown, "requested", True)
    monitor.supervise_stream(shutdown)

    assert metrics.registry.get_sample_value(
        "graph_loader_consumer_lag_records",
        {"kind": "cli", "name": "WORKS_AT", "topic": "works-at-events"},
    ) == 10.0


def test_lag_refresh_rate_is_independent_of_the_supervision_tick_rate():
    """P6-S2 review finding S8-1: with poll_interval_seconds=0.01 (100x
    faster than production) and a fake clock advancing 1s per tick, N
    ticks must produce ceil(elapsed / _LAG_REFRESH_INTERVAL_SECONDS)
    refreshes, never N."""
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    clock = _FakeClock()
    monitor, admin, watermark, control, _ = _monitor(metrics=metrics, clock=clock)
    refresh_calls = {"n": 0}

    def counting_get_watermark_offsets(*args, **kwargs):
        refresh_calls["n"] += 1
        return (0, 10)

    watermark.get_watermark_offsets.side_effect = counting_get_watermark_offsets

    ticks = {"n": 0}

    def sleep_and_advance(_seconds):
        ticks["n"] += 1
        clock.advance(1.0)  # 1 simulated second per tick.
        if ticks["n"] >= 25:
            shutdown.requested = True

    class Shutdown:
        requested = False

        def is_set(self):
            return self.requested

    shutdown = Shutdown()
    monitor._sleep = sleep_and_advance
    monitor.supervise_stream(shutdown)

    assert ticks["n"] == 25
    # 25 ticks at 1s each = 25s elapsed; refreshes happen at t=0, t=10,
    # t=20 -> 3 refreshes, not 25.
    assert refresh_calls["n"] == 3


def test_supervise_stream_lag_refresh_failure_never_breaks_supervision():
    metrics_module_import = __import__("src.utils.metrics", fromlist=["GraphLoaderMetrics"])
    from prometheus_client import CollectorRegistry

    metrics = metrics_module_import.GraphLoaderMetrics(registry=CollectorRegistry())
    monitor, admin, _, _, _ = _monitor(metrics=metrics)
    admin.list_consumer_group_offsets.side_effect = RuntimeError("admin API unreachable")

    class Shutdown:
        requested = False

        def is_set(self):
            return self.requested

    shutdown = Shutdown()
    monitor._sleep = lambda _seconds: setattr(shutdown, "requested", True)
    # Must not raise despite the admin API failure inside the lag refresh.
    monitor.supervise_stream(shutdown)


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


# -- P5-S1 Story 5: supervised, restartable streaming lease server ---------

def _lease_config(**overrides):
    from src.models.schema import BucketLeaseStreamConfig
    return BucketLeaseStreamConfig(**overrides)


def test_check_lease_server_health_restarts_a_dead_container_with_backoff():
    monitor, _, _, _, _ = _monitor()
    dead = MagicMock()
    dead.reload.side_effect = None
    dead.attrs = {"State": {"Status": "exited", "ExitCode": 1}}
    dead.logs.return_value = b""
    monitor._tracked_lease_server = dead
    monitor._bucket_lease_config = _lease_config(restart_backoff_base_ms=1, restart_backoff_max_ms=1)
    fresh = MagicMock()
    docker_service = MagicMock()
    docker_service.run_bucket_lease_server.return_value = fresh
    monitor._docker_service = docker_service
    monitor._lease_bucket_count = 16

    class Shutdown:
        def is_set(self):
            return False

    monitor._check_lease_server_health(Shutdown())

    dead.remove.assert_called_once_with(force=True)
    docker_service.run_bucket_lease_server.assert_called_once()
    kwargs = docker_service.run_bucket_lease_server.call_args.kwargs
    assert kwargs["run_id"] == monitor._run_id
    assert kwargs["restart_attempt"] == 1
    assert monitor._tracked_lease_server is fresh


def test_check_lease_server_health_raises_after_exceeding_max_restarts():
    monitor, _, _, _, _ = _monitor()
    dead = MagicMock()
    dead.attrs = {"State": {"Status": "exited"}}
    dead.logs.return_value = b""
    monitor._tracked_lease_server = dead
    monitor._bucket_lease_config = _lease_config(
        max_consecutive_restarts=1, restart_backoff_base_ms=1, restart_backoff_max_ms=1,
    )
    monitor._docker_service = MagicMock()
    monitor._lease_restart_count = 1  # already at the limit

    class Shutdown:
        def is_set(self):
            return False

    with pytest.raises(Exception, match="exceeded"):
        monitor._check_lease_server_health(Shutdown())


def test_check_lease_server_health_honors_shutdown_mid_backoff_without_restarting():
    monitor, _, _, _, _ = _monitor()
    dead = MagicMock()
    dead.attrs = {"State": {"Status": "exited"}}
    monitor._tracked_lease_server = dead
    monitor._bucket_lease_config = _lease_config(restart_backoff_base_ms=60_000, restart_backoff_max_ms=60_000)
    docker_service = MagicMock()
    monitor._docker_service = docker_service

    class ShutdownAfterFirstCheck:
        def __init__(self):
            self.calls = 0

        def is_set(self):
            self.calls += 1
            return self.calls > 1  # live during the loop's first check, then requested

    monitor._sleep = lambda _s: None
    monitor._check_lease_server_health(ShutdownAfterFirstCheck())
    docker_service.run_bucket_lease_server.assert_not_called()
    dead.remove.assert_not_called()
    assert monitor._tracked_lease_server is dead  # unchanged: restart abandoned


class _NeverShutdown:
    def is_set(self):
        return False


def _restart_harness(**config):
    """A monitor whose lease server dies on demand, on a fake clock."""
    monitor, _, _, _, _ = _monitor()
    now = [1_000.0]
    monitor._clock = lambda: now[0]
    monitor._sleep = lambda seconds: now.__setitem__(0, now[0] + seconds)
    monitor._bucket_lease_config = _lease_config(
        restart_backoff_base_ms=1_000, restart_backoff_max_ms=8_000, **config,
    )
    servers = []

    def launch(**kwargs):
        server = MagicMock()
        server.attrs = {"State": {"Status": "running"}}
        servers.append((server, kwargs))
        return server

    monitor._docker_service = MagicMock()
    monitor._docker_service.run_bucket_lease_server.side_effect = launch
    first = MagicMock()
    first.attrs = {"State": {"Status": "running"}}
    monitor._tracked_lease_server = first

    def kill():
        monitor._tracked_lease_server.attrs = {"State": {"Status": "exited", "ExitCode": 137}}

    def check():
        monitor._check_lease_server_health(_NeverShutdown())

    return monitor, now, servers, kill, check


def test_lease_restart_count_resets_after_a_healthy_interval():
    """Deaths far apart never reach the cap of a long-lived stream fleet."""
    monitor, now, servers, kill, check = _restart_harness(max_consecutive_restarts=2)
    config = monitor._bucket_lease_config
    healthy_s = (config.restart_quiet_period_ms + config.restart_count_reset_ms) / 1000
    backoffs = []
    for death in range(10):  # ten deaths, each after a healthy stretch: never fatal
        kill()
        before = now[0]
        check()
        backoffs.append(now[0] - before)  # the fake time the restart backoff slept
        assert monitor._lease_restart_count == 1
        now[0] += healthy_s
        check()  # running and healthy long enough: the counter resets
        assert monitor._lease_restart_count == 0
    assert len(servers) == 10
    # Every generation keeps its own metrics snapshot name (restart_attempt).
    assert [kwargs["restart_attempt"] for _server, kwargs in servers] == list(range(1, 11))
    # The backoff restarts from its base after each reset: every restart slept only the base delay
    # (without the reset it would have grown 1, 2, 4, 8 s ... up to the 8 s cap).
    assert backoffs == pytest.approx([config.restart_backoff_base_ms / 1000] * 10)


def test_lease_restart_count_is_not_reset_before_the_healthy_interval():
    """Within the interval the cap and the exponential backoff behave exactly as before."""
    monitor, now, servers, kill, check = _restart_harness(max_consecutive_restarts=3)
    config = monitor._bucket_lease_config
    almost_s = (config.restart_quiet_period_ms + config.restart_count_reset_ms) / 1000 - 1
    started = now[0]
    for expected in (1, 2, 3):
        kill()
        check()
        assert monitor._lease_restart_count == expected
        now[0] += almost_s
        check()  # healthy, but not for long enough
        assert monitor._lease_restart_count == expected
    # The backoffs were 1 s, 2 s, 4 s: unchanged exponential growth.
    assert now[0] - started == pytest.approx(3 * almost_s + 1 + 2 + 4)
    kill()
    with pytest.raises(Exception, match="exceeded 3 restarts"):
        check()
    assert len(servers) == 3


def test_lease_restart_count_reset_needs_a_running_server():
    """A server that is only ``created``/``restarting`` does not count as healthy."""
    monitor, now, _servers, kill, check = _restart_harness()
    kill()
    check()
    monitor._tracked_lease_server.attrs = {"State": {"Status": "restarting"}}
    now[0] += 10_000
    check()
    assert monitor._lease_restart_count == 1
    monitor._tracked_lease_server.attrs = {"State": {"Status": "running"}}
    check()
    assert monitor._lease_restart_count == 0


def test_lease_restart_count_reset_setting_is_validated():
    from pydantic import ValidationError

    assert _lease_config().restart_count_reset_ms == 600_000
    for bad in (999, 86_400_001, True):
        with pytest.raises(ValidationError):
            _lease_config(restart_count_reset_ms=bad)


def test_bulk_lease_path_has_no_restart_supervision():
    """Bulk ``--edge-leases`` monitors never get a tracked lease server, so the reset cannot affect them."""
    import inspect

    from src import cli

    source = inspect.getsource(cli._run_bulk_relationship_edge) + inspect.getsource(cli._run_relationship_bulk_stages)
    assert "tracked_lease_server" not in source
    monitor, _, _, _, _ = _monitor()
    assert monitor._tracked_lease_server is None


def test_wait_for_lease_server_ready_uses_a_fresh_client_per_attempt():
    """B5-1: a reused client whose first connect attempt fails is marked
    permanently broken, so every later attempt would fail immediately
    without really retrying. A fresh client per attempt must eventually
    succeed once the server comes up."""
    monitor, _, _, _, _ = _monitor()
    monitor._docker_service = MagicMock()
    monitor._docker_service.bucket_lease_address.return_value = "lease-x:7700"
    monitor._tracked_lease_server = MagicMock()
    monitor._sleep = lambda _s: None

    attempts = {"count": 0}

    class _FlakyThenOkClient:
        def __init__(self, *_a, **_k):
            attempts["count"] += 1
            self._ok = attempts["count"] > 3

        def ping(self):
            return self._ok

        def close(self):
            pass

    monitor.wait_for_lease_server_ready(5.0, client_factory=_FlakyThenOkClient)
    assert attempts["count"] == 4


def test_wait_for_lease_server_ready_honors_shutdown():
    monitor, _, _, _, _ = _monitor()
    monitor._docker_service = MagicMock()
    monitor._docker_service.bucket_lease_address.return_value = "lease-x:7700"
    monitor._tracked_lease_server = MagicMock()
    monitor._sleep = lambda _s: None

    class _NeverOkClient:
        def __init__(self, *_a, **_k):
            pass

        def ping(self):
            return False

        def close(self):
            pass

    class ShutdownNow:
        def is_set(self):
            return True

    # Must return (not raise BulkTimeoutError) once shutdown is requested.
    monitor.wait_for_lease_server_ready(
        5.0, shutdown_requested=ShutdownNow(), client_factory=_NeverOkClient,
    )


# -- P6-S5 Story 2: an operator stop aborts a wait promptly --------------------


def test_wait_aborts_on_shutdown_request_before_the_stage_deadline():
    from src.orchestrator.bulk_monitor import BulkShutdownRequested

    now = [0.0]
    monitor, *_ = _monitor(clock=lambda: now[0])
    monitor._sleep = lambda seconds: now.__setitem__(0, now[0] + seconds)
    polls = []

    def abort_requested() -> bool:
        polls.append(now[0])
        return len(polls) > 3

    monitor._abort_requested = abort_requested
    with pytest.raises(BulkShutdownRequested, match="shutdown requested"):
        monitor.wait_for_assignment_coverage(300)  # no assignment ever arrives
    assert now[0] < 1


def test_wait_without_abort_hook_still_times_out_normally():
    from src.orchestrator.bulk_monitor import BulkShutdownRequested

    now = [0.0]
    monitor, *_ = _monitor(clock=lambda: now[0])
    monitor._sleep = lambda seconds: now.__setitem__(0, now[0] + seconds)
    with pytest.raises(BulkTimeoutError) as excinfo:
        monitor.wait_for_assignment_coverage(1)
    assert not isinstance(excinfo.value, BulkShutdownRequested)


# ── P5-S4 owner decision D2: retry consumers are supervised like edge loaders ─


class _NeverShutdown:
    def is_set(self):
        return False


@pytest.mark.parametrize("status, exit_code", [("exited", 1), ("exited", 0), ("dead", 137)])
def test_stream_supervisor_stops_the_fleet_when_a_retry_consumer_exits(status, exit_code, caplog):
    """A crash or a fatal exit (RetryConsumerFatalError -> rc 1) of any
    retry-consumer replica fails stream supervision with a clear
    stage=retry-consumer error naming the replica, exactly like an edge crash;
    the CLI then stops the fleet and returns 1."""
    from src.orchestrator.bulk_monitor import LoaderCrashError

    healthy, crashed = MagicMock(), MagicMock()
    healthy.attrs = {"State": {"Status": "running"}}
    crashed.attrs = {"State": {"Status": status, "ExitCode": exit_code}}
    crashed.logs.return_value = b"stage=retry-consumer reason=run_loop_failed error=writes failing for 600s"
    monitor, *_ = _monitor(retry_consumers=[healthy, crashed])
    with pytest.raises(LoaderCrashError, match=(
        rf"stage=retry-consumer run_id=run-1 replica=1 retry consumer exited during stream supervision "
        rf"status={status} exit_code={exit_code} log_tail=.*writes failing for 600s"
    )):
        monitor.supervise_stream(_NeverShutdown())
    assert "stage=retry-consumer run_id=run-1 replica=1" in caplog.text


def test_stream_supervisor_treats_a_removed_retry_consumer_as_a_crash():
    from src.orchestrator.bulk_monitor import LoaderCrashError

    gone = MagicMock()
    gone.reload.side_effect = NotFound("gone")
    monitor, *_ = _monitor(retry_consumers=[gone])
    with pytest.raises(LoaderCrashError, match="stage=retry-consumer run_id=run-1 replica=0 .*status=removed"):
        monitor.supervise_stream(_NeverShutdown())


def test_stream_supervisor_keeps_running_while_retry_consumers_run():
    running = MagicMock()
    running.attrs = {"State": {"Status": "running"}}
    monitor, *_ = _monitor(retry_consumers=[running])

    class Shutdown:
        requested = False

        def is_set(self):
            return self.requested

    shutdown = Shutdown()
    monitor._sleep = lambda _seconds: setattr(shutdown, "requested", True)
    monitor.supervise_stream(shutdown)
    running.reload.assert_called()
