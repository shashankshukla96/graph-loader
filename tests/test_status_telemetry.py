"""E15.F1.S2: telemetry acquisition. Role status snapshots -> status fields, with null instead of 0 or stale.

Snapshots are written by the real writer (``utils.metrics.write_status_snapshot``) from real metric registries;
roles are recorded in a real ``StatusTracker``. No Kafka, Neo4j or Docker.
"""
from __future__ import annotations

import os
import stat
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from prometheus_client import CollectorRegistry

from neo4j_graph_loader.orchestrator import relationship_bulk_monitor as rbm
from neo4j_graph_loader.status import StatusTracker, model, schemas, telemetry, validate
from neo4j_graph_loader.utils import metrics

INTERVAL = 5.0


@pytest.fixture
def root(tmp_path):
    return tmp_path / "timings"


@pytest.fixture
def execution():
    return StatusTracker(mode="stream", runtime="docker", run_id="r1")


def write_snapshot(monkeypatch, root: Path, execution_id: str, incarnation: str, build, *, final: bool = True,
                   metrics_enabled: bool = True) -> metrics.GraphLoaderMetrics:
    """What a role does: its registry, written through the real status snapshot writer."""
    registry = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    build(registry)
    monkeypatch.setenv(metrics.STATUS_SNAPSHOT_DIR_ENV, str(telemetry.incarnation_dir(root, execution_id, incarnation)))
    if not metrics_enabled:
        monkeypatch.setenv("GRAPH_LOADER_METRICS_ENABLED", "0")
    assert metrics.write_status_snapshot(registry, final=final)
    monkeypatch.delenv(metrics.STATUS_SNAPSHOT_DIR_ENV)
    return registry


def role(execution, kind, name, replica=0):
    incarnation = model.new_id()
    execution.role_started(kind, name, replica, incarnation=incarnation, container_id=f"c-{incarnation[:6]}")
    return incarnation


def acquire(execution, root, **kwargs):
    return telemetry.acquire(execution.snapshot()["roles"], root, execution.execution_id, interval=INTERVAL, **kwargs)


def consumed(registry, kind, label, count):
    registry.records_consumed.labels(kind=kind, name=label, label_or_type=label).inc(count)


# -- transport: the role-side writer -------------------------------------------------------------------------------
def test_the_writer_makes_the_incarnation_directory_a_snapshot_and_a_final_marker(monkeypatch, root, execution):
    inc = model.new_id()
    write_snapshot(monkeypatch, root, execution.execution_id, inc, lambda m: consumed(m, "node", "Person", 3), final=False)
    directory = telemetry.incarnation_dir(root, execution.execution_id, inc)
    assert (directory / "metrics.prom").exists() and not (directory / "final.json").exists()
    assert telemetry.read_incarnation(root, execution.execution_id, inc).final is False
    write_snapshot(monkeypatch, root, execution.execution_id, inc, lambda m: consumed(m, "node", "Person", 4))
    reading = telemetry.read_incarnation(root, execution.execution_id, inc)
    assert reading.final is True and reading.problem is None
    assert not [p for p in directory.iterdir() if p.name.endswith(".tmp")]


def test_without_the_env_key_nothing_is_written(monkeypatch, tmp_path):
    monkeypatch.delenv(metrics.STATUS_SNAPSHOT_DIR_ENV, raising=False)
    registry = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    assert metrics.write_status_snapshot(registry) is False and metrics.start_status_snapshots(registry) is False


def test_the_periodic_writer_refreshes_the_snapshot_and_the_final_write_stops_it(monkeypatch, tmp_path):
    monkeypatch.setenv(metrics.STATUS_SNAPSHOT_DIR_ENV, str(tmp_path / "d"))
    monkeypatch.setenv(metrics.STATUS_INTERVAL_ENV, "0.5")
    registry = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    assert metrics.start_status_snapshots(registry)
    assert metrics.start_status_snapshots(registry)                      # once per registry
    target = tmp_path / "d" / "metrics.prom"
    first = target.stat().st_mtime_ns
    deadline = time.monotonic() + 5
    while target.stat().st_mtime_ns == first and time.monotonic() < deadline:
        time.sleep(0.05)
    assert target.stat().st_mtime_ns != first
    metrics.write_metrics_snapshot(registry, None)                       # what every role does at exit
    assert (tmp_path / "d" / "final.json").exists()
    settled = target.stat().st_mtime_ns
    time.sleep(1.2)
    assert target.stat().st_mtime_ns == settled                           # the timer is stopped


def test_the_status_interval_is_validated(monkeypatch):
    for raw, expected in ((None, 5.0), ("", 5.0), ("abc", 5.0), ("-1", 5.0), ("nan", 5.0), ("0.01", 0.5), ("2", 2.0)):
        monkeypatch.delenv(metrics.STATUS_INTERVAL_ENV, raising=False)
        if raw is not None:
            monkeypatch.setenv(metrics.STATUS_INTERVAL_ENV, raw)
        assert metrics.status_interval_seconds() == expected, raw


def test_an_unwritable_directory_does_not_raise_and_is_reported_by_the_return_value(monkeypatch, tmp_path, caplog):
    blocked = tmp_path / "ro"
    blocked.mkdir()
    blocked.chmod(stat.S_IRUSR | stat.S_IXUSR)
    try:
        if os.access(blocked, os.W_OK):
            pytest.skip("running as a user that ignores directory permissions")
        monkeypatch.setenv(metrics.STATUS_SNAPSHOT_DIR_ENV, str(blocked / "child"))
        assert metrics.write_status_snapshot(metrics.GraphLoaderMetrics(registry=CollectorRegistry())) is False
        assert "stage=status-snapshot" in caplog.text
    finally:
        blocked.chmod(stat.S_IRWXU)


def test_status_snapshots_do_not_depend_on_the_http_metrics_setting(monkeypatch, root, execution):
    inc = role(execution, "node-loader", "Person")
    write_snapshot(monkeypatch, root, execution.execution_id, inc, lambda m: consumed(m, "node", "Person", 7),
                   metrics_enabled=False)                                # GRAPH_LOADER_METRICS_ENABLED=0
    result = acquire(execution, root)
    assert result.types[("node", "Person")]["records_written"]["value"] == 7


# -- counters ---------------------------------------------------------------------------------------------------------------
def test_records_written_sums_replicas_and_restarted_incarnations(monkeypatch, root, execution):
    first, second = role(execution, "node-loader", "Person", 0), role(execution, "node-loader", "Person", 1)
    write_snapshot(monkeypatch, root, execution.execution_id, first, lambda m: consumed(m, "node", "Person", 10))
    write_snapshot(monkeypatch, root, execution.execution_id, second, lambda m: consumed(m, "node", "Person", 5))
    execution.role_exited(first, exit_code=1)
    third = role(execution, "node-loader", "Person", 0)                  # the restart of replica 0
    write_snapshot(monkeypatch, root, execution.execution_id, third, lambda m: consumed(m, "node", "Person", 4))
    field = acquire(execution, root).types[("node", "Person")]["records_written"]
    assert field["value"] == 19 and field["source"] == "role-snapshots" and "lower_bound" not in field
    assert field["observed_at"] <= model.timestamp(time.time() + 1)


def test_a_missing_snapshot_is_null_not_zero(monkeypatch, root, execution):
    role(execution, "node-loader", "Person")
    types = acquire(execution, root, entities=[("node", "Person")]).types
    assert types[("node", "Person")]["records_written"] is None and types[("node", "Person")]["rejected"] is None


@pytest.mark.parametrize("damage", [b"", b"\x00\xff\xfe garbage", b"graph_loader_records_consumed_total{kind=\"node\" 3\n",
                                    b"# TYPE x counter\nx_total notanumber\n"])
def test_a_corrupt_snapshot_is_null_not_zero(monkeypatch, root, execution, damage):
    inc = role(execution, "node-loader", "Person")
    write_snapshot(monkeypatch, root, execution.execution_id, inc, lambda m: consumed(m, "node", "Person", 9))
    (telemetry.incarnation_dir(root, execution.execution_id, inc) / "metrics.prom").write_bytes(damage)
    assert telemetry.read_incarnation(root, execution.execution_id, inc).problem == "corrupt"
    assert acquire(execution, root).types[("node", "Person")]["records_written"] is None


def test_one_unreadable_incarnation_makes_the_total_a_lower_bound(monkeypatch, root, execution):
    good, lost = role(execution, "node-loader", "Person", 0), role(execution, "node-loader", "Person", 1)
    write_snapshot(monkeypatch, root, execution.execution_id, good, lambda m: consumed(m, "node", "Person", 6))
    field = acquire(execution, root).types[("node", "Person")]["records_written"]           # `lost` never wrote one
    assert field["value"] == 6 and field["lower_bound"] is True
    assert lost                                                                            # (the second replica)


def test_a_sigkilled_incarnation_without_a_final_snapshot_is_a_lower_bound(monkeypatch, root, execution):
    killed, survivor = role(execution, "node-loader", "Person", 0), role(execution, "node-loader", "Person", 1)
    write_snapshot(monkeypatch, root, execution.execution_id, killed, lambda m: consumed(m, "node", "Person", 100), final=False)
    write_snapshot(monkeypatch, root, execution.execution_id, survivor, lambda m: consumed(m, "node", "Person", 1))
    execution.role_exited(killed, exit_code=137, exit_reason="signal 9")     # exited, and no final.json was ever written
    field = acquire(execution, root).types[("node", "Person")]["records_written"]
    assert field["value"] == 101 and field["lower_bound"] is True
    # the same incarnation with its final snapshot is exact
    write_snapshot(monkeypatch, root, execution.execution_id, killed, lambda m: consumed(m, "node", "Person", 120))
    exact = acquire(execution, root).types[("node", "Person")]["records_written"]
    assert exact["value"] == 121 and "lower_bound" not in exact


def test_a_running_role_with_only_a_periodic_snapshot_is_not_a_lower_bound(monkeypatch, root, execution):
    inc = role(execution, "node-loader", "Person")
    write_snapshot(monkeypatch, root, execution.execution_id, inc, lambda m: consumed(m, "node", "Person", 2), final=False)
    assert "lower_bound" not in acquire(execution, root).types[("node", "Person")]["records_written"]


def test_two_executions_sharing_one_timing_directory_do_not_collide(monkeypatch, root):
    one, two = StatusTracker(mode="stream", run_id="same"), StatusTracker(mode="stream", run_id="same")
    for tracker, count in ((one, 11), (two, 22)):
        inc = role(tracker, "node-loader", "Person")
        write_snapshot(monkeypatch, root, tracker.execution_id, inc, lambda m, count=count: consumed(m, "node", "Person", count))
    assert acquire(one, root).types[("node", "Person")]["records_written"]["value"] == 11
    assert acquire(two, root).types[("node", "Person")]["records_written"]["value"] == 22
    assert sorted(p.name for p in root.iterdir()) == sorted([one.execution_id, two.execution_id])


def test_edge_writes_come_from_loaders_and_planned_writers_but_not_the_retry_consumer(monkeypatch, root):
    execution = StatusTracker(mode="stream", run_id="r")
    loader, planned, retry = (role(execution, "edge-loader", "KNOWS"), role(execution, "planned-edge-writer", "-"),
                              role(execution, "retry-consumer", "-"))
    write_snapshot(monkeypatch, root, execution.execution_id, loader, lambda m: consumed(m, "edge", "KNOWS", 10))
    write_snapshot(monkeypatch, root, execution.execution_id, planned,
                   lambda m: m.records_consumed.labels(kind="planned-writer", name="KNOWS", label_or_type="KNOWS").inc(5))

    def retry_build(m):
        consumed(m, "edge", "KNOWS", 1000)                     # the retry consumer writes under kind=edge too
        for outcome, count in (("written", 3), ("re_dlq", 2), ("parked", 1)):
            m.retry_outcome.labels(kind="edge", name="KNOWS", outcome=outcome).inc(count)

    write_snapshot(monkeypatch, root, execution.execution_id, retry, retry_build)
    fields = acquire(execution, root).types[("edge", "KNOWS")]
    assert fields["records_written"]["value"] == 15
    assert [fields[name]["value"] for name in ("retry_written", "retry_redlq", "retry_parked")] == [3, 2, 1]


def test_rejected_counts_node_and_edge_rejections_and_planned_runs_leave_the_planner_total_alone(monkeypatch, root):
    execution = StatusTracker(mode="stream", run_id="r")
    node, edge = role(execution, "node-loader", "Person"), role(execution, "edge-loader", "KNOWS")
    write_snapshot(monkeypatch, root, execution.execution_id, node, lambda m: m.rejected.labels(kind="node", name="Person").inc(2))
    write_snapshot(monkeypatch, root, execution.execution_id, edge, lambda m: m.rejected.labels(kind="edge", name="KNOWS").inc(3))
    result = acquire(execution, root)
    assert result.types[("node", "Person")]["rejected"]["value"] == 2
    assert result.types[("edge", "KNOWS")]["rejected"]["value"] == 3
    assert result.totals["rejected"]["value"] == 5

    planned = StatusTracker(mode="bulk", run_id="r")
    writer = role(planned, "planned-edge-writer", "-")
    write_snapshot(monkeypatch, root, planned.execution_id, writer,
                   lambda m: m.records_consumed.labels(kind="planned-writer", name="KNOWS", label_or_type="KNOWS").inc(8))
    result = acquire(planned, root)
    assert "rejected" not in result.types[("edge", "KNOWS")] and "rejected" not in result.totals


def test_totals_are_null_when_any_type_is_unknown(monkeypatch, root):
    execution = StatusTracker(mode="stream", run_id="r")
    person = role(execution, "node-loader", "Person")
    role(execution, "node-loader", "Company")                     # no snapshot
    write_snapshot(monkeypatch, root, execution.execution_id, person, lambda m: consumed(m, "node", "Person", 4))
    result = acquire(execution, root)
    assert result.totals["records_written"] is None


# -- gauges -------------------------------------------------------------------------------------------------------------------
def lag(registry, name, value, observed_at, topic="people"):
    registry.consumer_lag.labels(kind="node", name=name, topic=topic).set(value)
    registry.consumer_lag_observed_at.labels(kind="node", name=name, topic=topic).set(observed_at)


def test_node_lag_is_summed_over_replicas_that_each_observed_it_recently(monkeypatch, root, execution):
    now = time.time()
    a, b = role(execution, "node-loader", "Person", 0), role(execution, "node-loader", "Person", 1)
    write_snapshot(monkeypatch, root, execution.execution_id, a, lambda m: lag(m, "Person", 30, now - 2))
    write_snapshot(monkeypatch, root, execution.execution_id, b, lambda m: lag(m, "Person", 12, now - 4), final=False)
    field = acquire(execution, root, now=now).types[("node", "Person")]["lag"]
    assert field["value"] == 42 and field["source"] == "node-loader"
    assert field["observed_at"] == model.timestamp(now - 4)            # as fresh as its stalest term


def test_a_fresh_file_carrying_a_stale_observation_is_null(monkeypatch, root, execution):
    now = time.time()
    inc = role(execution, "node-loader", "Person")
    write_snapshot(monkeypatch, root, execution.execution_id, inc, lambda m: lag(m, "Person", 30, now - 120), final=False)
    path = telemetry.incarnation_dir(root, execution.execution_id, inc) / "metrics.prom"
    assert time.time() - path.stat().st_mtime < 5                         # the FILE is fresh
    assert acquire(execution, root, now=now).types[("node", "Person")]["lag"] is None


def test_node_lag_is_null_until_first_observed_and_when_one_replica_has_not_observed(monkeypatch, root, execution):
    now = time.time()
    a, b = role(execution, "node-loader", "Person", 0), role(execution, "node-loader", "Person", 1)
    write_snapshot(monkeypatch, root, execution.execution_id, a, lambda m: None, final=False)         # no lag series yet
    write_snapshot(monkeypatch, root, execution.execution_id, b, lambda m: lag(m, "Person", 12, now), final=False)
    assert acquire(execution, root, now=now).types[("node", "Person")]["lag"] is None                  # understate: no
    write_snapshot(monkeypatch, root, execution.execution_id, a, lambda m: lag(m, "Person", 1, now), final=False)
    assert acquire(execution, root, now=now).types[("node", "Person")]["lag"]["value"] == 13


def test_the_lag_of_a_role_that_has_exited_is_not_current(monkeypatch, root, execution):
    now = time.time()
    inc = role(execution, "node-loader", "Person")
    write_snapshot(monkeypatch, root, execution.execution_id, inc, lambda m: lag(m, "Person", 30, now))
    execution.role_exited(inc, exit_code=0)
    assert acquire(execution, root, now=now).types[("node", "Person")]["lag"] is None


def backlog(registry, name, dlq, parked, dlq_at, parked_at):
    for queue, value, at in (("dlq", dlq, dlq_at), ("parked", parked, parked_at)):
        registry.dlq_backlog.labels(kind="edge", name=name, queue=queue).set(value)
        registry.dlq_backlog_observed_at.labels(kind="edge", name=name, queue=queue).set(at)


def test_dlq_lag_and_parked_size_carry_their_own_observation_time(monkeypatch, root):
    now = time.time()
    execution = StatusTracker(mode="stream", run_id="r")
    retry = role(execution, "retry-consumer", "-")
    write_snapshot(monkeypatch, root, execution.execution_id, retry, lambda m: backlog(m, "KNOWS", 40, 7, now - 10, now - 30),
                   final=False)
    fields = acquire(execution, root, now=now).types[("edge", "KNOWS")]
    assert (fields["dlq_lag"]["value"], fields["dlq_lag"]["observed_at"]) == (40, model.timestamp(now - 10))
    assert (fields["parked_size"]["value"], fields["parked_size"]["observed_at"]) == (7, model.timestamp(now - 30))


def test_a_failed_backlog_computation_leaves_an_old_observation_and_the_value_becomes_null(monkeypatch, root):
    """The gauge keeps its previous value when a computation fails; the companion is not touched, so it ages out."""
    from neo4j_graph_loader.loader.retry_consumer import backlog_listener

    registry = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    listener = backlog_listener(registry)
    listener("KNOWS", "dlq", 40)
    before = registry.registry.get_sample_value(
        "graph_loader_dlq_backlog_observed_at_seconds", {"kind": "edge", "name": "KNOWS", "queue": "dlq"})
    assert before is not None and abs(before - time.time()) < 5

    # a failed computation never reaches the listener (compute_and_notify_backlog returns None)
    from test_retry_consumer import _make_consumer
    consumer, fakes = _make_consumer()
    consumer._on_backlog_computed = listener
    consumer._backlog_source_by_edge_type = {"KNOWS": MagicMock(dlq_high_watermark_and_committed=MagicMock(side_effect=RuntimeError("broker down")))}
    consumer._dlq_topic_by_edge_type = {"KNOWS": "dlq"}
    consumer._parked_topic_by_edge_type = {"KNOWS": "parked"}
    assert consumer.compute_and_notify_backlog("KNOWS") is None
    after = registry.registry.get_sample_value(
        "graph_loader_dlq_backlog_observed_at_seconds", {"kind": "edge", "name": "KNOWS", "queue": "dlq"})
    assert after == before
    assert registry.registry.get_sample_value("graph_loader_dlq_backlog", {"kind": "edge", "name": "KNOWS", "queue": "dlq"}) == 40

    execution = StatusTracker(mode="stream", run_id="r")
    retry = role(execution, "retry-consumer", "-")
    write_snapshot(monkeypatch, root, execution.execution_id, retry, lambda m: backlog(m, "KNOWS", 40, 7, before, before),
                   final=False)
    stale = acquire(execution, root, now=before + 3600).types[("edge", "KNOWS")]
    assert stale["dlq_lag"] is None and stale["parked_size"] is None


def test_the_retry_row_listener_counts_the_three_outcomes_and_nothing_else():
    from neo4j_graph_loader.loader.retry_consumer import row_retried_listener

    registry = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    listener = row_retried_listener(registry)
    for outcome in ("written", "written", "re_dlq", "parked", "blocked", "duplicate_skipped"):
        listener("KNOWS", outcome)
    sample = lambda outcome: registry.registry.get_sample_value(
        "graph_loader_retry_outcome_total", {"kind": "edge", "name": "KNOWS", "outcome": outcome})
    assert [sample(o) for o in metrics.RETRY_OUTCOMES] == [2, 1, 1]
    assert registry.registry.get_sample_value(
        "graph_loader_retry_total", {"kind": "edge", "name": "KNOWS", "reason": "missing-endpoint"}) == 4


# -- availability, exits ------------------------------------------------------------------------------------------------------------
def test_when_no_role_ever_writes_a_snapshot_telemetry_is_unavailable_with_a_reason(root):
    execution = StatusTracker(mode="stream", run_id="r", clock=lambda: 1_000_000.0)
    role(execution, "node-loader", "Person")
    early = telemetry.acquire(execution.snapshot()["roles"], root, execution.execution_id, now=1_000_005.0, interval=INTERVAL)
    assert early.available is True                                            # still inside the start-up grace
    late = telemetry.acquire(execution.snapshot()["roles"], root, execution.execution_id, now=1_000_100.0, interval=INTERVAL)
    assert late.available is False and "no role has written a readable status snapshot" in late.reason


def test_the_poller_fills_the_tracker_and_the_result_validates(monkeypatch, root):
    execution = StatusTracker(mode="stream", run_id="r")
    inc = role(execution, "node-loader", "Person")
    write_snapshot(monkeypatch, root, execution.execution_id, inc, lambda m: consumed(m, "node", "Person", 12))
    poller = telemetry.TelemetryPoller(execution, root, interval=INTERVAL, entities=[("node", "Person"), ("edge", "KNOWS")])
    poller.add_source(lambda tracker, now: tracker.update_totals(rejected=model.observed(1, model.timestamp(now), "planner")))
    poller.add_source(lambda tracker, now: 1 / 0)                              # a broken source is skipped
    assert poller.poll_once() is not None
    snapshot = execution.snapshot()
    validate(snapshot, schemas.STRICT_STATUS_SCHEMA)
    types = {(t["kind"], t["name"]): t for t in snapshot["types"]}
    assert types[("node", "Person")]["records_written"]["value"] == 12
    assert types[("edge", "KNOWS")]["records_written"] is None                 # declared, nobody wrote: unknown
    assert snapshot["totals"]["rejected"]["value"] == 1


def test_an_unchanged_poll_publishes_only_the_heartbeat(monkeypatch, root):
    execution = StatusTracker(mode="stream", run_id="r")
    inc = role(execution, "node-loader", "Person")
    write_snapshot(monkeypatch, root, execution.execution_id, inc, lambda m: consumed(m, "node", "Person", 12))
    poller = telemetry.TelemetryPoller(execution, root, interval=INTERVAL, entities=[("node", "Person")])
    poller.poll_once()
    published: list = []
    execution.subscribe(on_snapshot=published.append)
    published.clear()
    poller.poll_once()
    assert len(published) == 1                                                  # identical values: only the heartbeat


def test_the_poller_survives_a_broken_acquisition(root, caplog):
    execution = StatusTracker(mode="stream", run_id="r")
    poller = telemetry.TelemetryPoller(execution, root, interval=INTERVAL)
    execution.snapshot = MagicMock(side_effect=RuntimeError("boom"))
    assert poller.poll_once() is None and poller.poll_once() is None
    assert caplog.text.count("telemetry acquisition failed") == 1


class FakeHandle:
    def __init__(self, status="running", code=0, oom=False, gone=False):
        self._state, self._gone = {"State": {"Status": status, "ExitCode": code, "OOMKilled": oom}}, gone
        self.reloads = 0

    @property
    def attrs(self):
        if self._gone:
            raise RuntimeError("gone")
        return self._state

    def reload(self):
        self.reloads += 1


def test_exits_are_observed_with_code_oom_and_unknown_is_left_alone(execution):
    ok, crashed, oomed, running, vanished = (role(execution, "node-loader", f"L{i}") for i in range(5))
    handles = {
        ok: FakeHandle("exited", 0), crashed: FakeHandle("exited", 1), oomed: FakeHandle("exited", 137, oom=True),
        running: FakeHandle("running"), vanished: FakeHandle("exited", 1, gone=True),
    }
    telemetry.observe_roles(execution, handles)
    assert sum(handle.reloads for handle in handles.values()) == 0               # no extra container API traffic
    roles = {r["incarnation"]: r for r in execution.snapshot()["roles"]}
    assert (roles[ok]["state"], roles[ok]["exit_code"]) == ("exited", 0)
    assert (roles[crashed]["state"], roles[crashed]["exit_code"], roles[crashed]["last_error"]["message"]) == (
        "failed", 1, "exited with code 1")
    assert (roles[oomed]["state"], roles[oomed]["exit_reason"]) == ("failed", "oom")
    assert roles[running]["state"] == "running" and roles[vanished]["state"] == "running"
    assert "role.oom" in [event["type"] for event in execution.events()]
    execution.request_stop()
    assert execution.finish(0) == "failed"                                     # failures before the stop decide


def test_a_group_oom_is_an_event_and_a_failure_before_a_stop(execution):
    execution.group_oom(0, 1)
    assert [e["type"] for e in execution.events()][-1] == "execution.oom"
    for event in execution.events():
        validate(event, schemas.STRICT_EVENT_SCHEMA)
    execution.request_stop()
    assert execution.finish(143) == "failed"


# -- the process-runtime OOM report reaches status ------------------------------------------------------------------------------------
def test_the_runtime_oom_report_calls_its_listeners(tmp_path):
    from neo4j_graph_loader.orchestrator.process_runtime import ProcessRuntime

    class Watch:
        def __init__(self):
            self.values = [(0, 2)]

        def rose(self):
            return self.values.pop() if self.values else None

    runtime = ProcessRuntime(tmp_path / "run", oom_watch=Watch(), oom_poll_seconds=0)
    seen: list = []
    runtime.add_oom_listener(lambda before, after: seen.append((before, after)))
    runtime.add_oom_listener(lambda before, after: 1 / 0)                     # a failing listener changes nothing
    try:
        runtime.report_cgroup_oom()
    finally:
        runtime.close()
    assert seen == [(0, 2)]


# -- node loader and relationship lag ----------------------------------------------------------------------------------------------------------
def test_the_node_loader_observes_lag_from_cached_watermarks_and_committed_offsets():
    from confluent_kafka import TopicPartition
    from neo4j_graph_loader.loader import node_loader

    registry = metrics.GraphLoaderMetrics(registry=CollectorRegistry())
    consumer = MagicMock()
    consumer.get_watermark_offsets.side_effect = lambda tp, timeout, cached: (0, {0: 100, 1: 50}[tp.partition])
    consumer.position.side_effect = lambda tps: [TopicPartition(tps[0].topic, tps[0].partition, 40)]
    stub = MagicMock(spec=node_loader.NodeLoader)
    stub._consumer, stub._metrics, stub._logger = consumer, registry, MagicMock()
    stub._node_config, stub._topic = MagicMock(label="Person"), "people"
    stub._assigned_partitions = {("people", 0), ("people", 1)}
    stub._committed_next_offsets = {("people", 0): 90}                       # partition 1 has no commit: position 40
    stub._next_lag_observation = None
    node_loader.NodeLoader._observe_lag(stub)
    sample = lambda name: registry.registry.get_sample_value(name, {"kind": "node", "name": "Person", "topic": "people"})
    assert sample("graph_loader_consumer_lag_records") == (100 - 90) + (50 - 40)
    assert abs(sample("graph_loader_consumer_lag_observed_at_seconds") - time.time()) < 5

    # failures leave both gauges exactly as they were
    stub._next_lag_observation = None
    consumer.get_watermark_offsets.side_effect = RuntimeError("no metadata")
    node_loader.NodeLoader._observe_lag(stub)
    assert sample("graph_loader_consumer_lag_records") == 20
    # no cached watermark yet (-1001) is also not an observation
    stub._next_lag_observation = None
    consumer.get_watermark_offsets.side_effect = lambda tp, timeout, cached: (-1001, -1001)
    previous = sample("graph_loader_consumer_lag_observed_at_seconds")
    node_loader.NodeLoader._observe_lag(stub)
    assert sample("graph_loader_consumer_lag_observed_at_seconds") == previous
    # throttled
    consumer.get_watermark_offsets.reset_mock()
    node_loader.NodeLoader._observe_lag(stub)
    consumer.get_watermark_offsets.assert_not_called()


def test_a_failed_relationship_lag_poll_reports_null_never_the_previous_value():
    from test_relationship_bulk_monitor import _monitor

    seen: list = []
    rbm.set_lag_listener(seen.append)
    try:
        monitor, *_ = _monitor()
        monitor._watermarks_low_high = lambda timeout: {(t, "topic", 0): (0, 50) for t in monitor._edges}
        monitor._commits = lambda timeout=None: {(t, "topic", 0): 20 for t in monitor._edges}
        monitor._next_lag_refresh = None
        monitor._refresh_consumer_lag()
        monitor._next_lag_refresh = None
        monitor._commits = MagicMock(side_effect=RuntimeError("broker down"))
        monitor._refresh_consumer_lag()
    finally:
        rbm.set_lag_listener(None)
    edge_types = set(monitor._edges)
    assert seen[0] == {t: 30 for t in edge_types}
    assert seen[1] == {t: None for t in edge_types}
    rbm.set_lag_listener(lambda lag: 1 / 0)                                  # a failing listener never reaches supervision
    try:
        monitor._next_lag_refresh = None
        monitor._refresh_consumer_lag()
    finally:
        rbm.set_lag_listener(None)


def test_rejection_counters_are_incremented_by_the_node_and_edge_loaders():
    from neo4j_graph_loader.loader import edge_loader, node_loader

    assert hasattr(node_loader.NodeLoader, "_observe_lag")
    source = Path(node_loader.__file__).read_text() + Path(edge_loader.__file__).read_text()
    assert source.count("rejected.labels(") == 2


# -- the dispatcher's STATUS carries per-type progress in O(types) ------------------------------------------------------------------------------
def test_dispatcher_status_reports_chunks_done_and_planned_per_type():
    from neo4j_graph_loader.orchestrator.planned_dispatcher import DispatchState
    from test_planned_dispatcher import make_plan

    plan = make_plan([("T", 0, 0, [10, 10]), ("U", 1, 1, [5]), ("T", 0, 1, [10])], types={"T": ("A", "B"), "U": ("A", "B")})
    state = DispatchState(plan, ready_labels=["A", "B"])
    assert state.status()["by_type"] == {"T": {"done": 0, "total": 3}, "U": {"done": 0, "total": 1}}
    chunk, _ = state.grant("w1")
    state.done("w1", chunk, plan.chunks[chunk].rows)
    done = {name: item["done"] for name, item in state.status()["by_type"].items()}
    assert sum(done.values()) == 1 and state.status()["done"] == 1
    assert state.summary()["types"] is not state.status()["by_type"]          # the summary's own `types` is unchanged
    assert set(state.summary()["types"]) <= {"T", "U"}


def test_the_status_command_stays_cheap_on_a_25k_chunk_plan():
    from neo4j_graph_loader.orchestrator.planned_dispatcher import DispatchState
    from test_planned_dispatcher import make_plan

    big = make_plan([("T", i % 4, (i + 1) % 4, [1]) for i in range(25_000)], buckets=4)
    small = make_plan([("T", 0, 0, [1])])
    assert len(big.chunks) == 25_000

    def per_call(plan, calls=300):
        state = DispatchState(plan, ready_labels=["A", "B"])
        state.status()
        started = time.perf_counter()
        for _ in range(calls):
            state.status()
        return (time.perf_counter() - started) / calls

    large, tiny = per_call(big), per_call(small)
    print(f"STATUS per call: 25k chunks {large * 1e6:.1f} us, 1 chunk {tiny * 1e6:.1f} us")
    assert large < 0.002 and large < tiny * 20 + 0.0005                         # O(types), not O(chunks)


# -- end to end with real child processes (process runtime) -----------------------------------------------------------------------------------------
@pytest.fixture
def process_service(tmp_path, monkeypatch):
    from neo4j_graph_loader import _runtime
    from neo4j_graph_loader.orchestrator.process_service import ProcessService

    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    monkeypatch.setenv("GRAPH_LOADER_STATUS_INTERVAL_S", "0.5")
    base = tmp_path / "attempt"
    for name in ("rejections", "plans", "spill", "timings"):
        (base / name).mkdir(parents=True)
    schema = base / "runtime_schema.yaml"
    schema.write_text("x: 1\n")
    paths = _runtime.RunPaths(base, schema, base / "rejections", base / "plans", base / "spill", base / "timings")
    service = ProcessService(paths, ready_timeout=60)
    yield service, paths
    service.close()


def test_a_real_child_writes_its_snapshots_where_the_status_model_expects_them(process_service):
    service, paths = process_service
    execution = StatusTracker(mode="stream", run_id="e2e", runtime="process")
    service.status = execution
    handle = service.run_bucket_lease_server(run_id="e2e", bucket_count=16, network="n", keyed_labels=[])
    (role_record,) = execution.snapshot()["roles"]
    assert role_record["incarnation"] == handle.id
    directory = telemetry.incarnation_dir(paths.timings, execution.execution_id, handle.id)
    deadline = time.monotonic() + 15
    while not (directory / "metrics.prom").exists() and time.monotonic() < deadline:
        time.sleep(0.1)
    reading = telemetry.read_incarnation(paths.timings, execution.execution_id, handle.id)
    assert reading.samples and not reading.final
    assert any(name == "graph_loader_process_up" for name, _labels, _value in reading.samples)

    poller = telemetry.TelemetryPoller(execution, paths.timings, interval=0.5, handles=service.status_handles)
    poller.poll_once()
    assert execution.snapshot()["roles"][0]["state"] == "running"

    handle.stop(timeout=10)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and not (directory / "final.json").exists():
        time.sleep(0.1)
    assert (directory / "final.json").exists()                              # the role's exit path wrote the marker
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        poller.poll_once()
        if execution.snapshot()["roles"][0]["state"] != "running":
            break
        time.sleep(0.2)
    exited = execution.snapshot()["roles"][0]
    assert exited["state"] in ("exited", "failed") and exited["exit_code"] is not None and exited["finished_at"]
    validate(execution.snapshot(), schemas.STRICT_STATUS_SCHEMA)


def test_children_get_the_identity_environment_and_two_executions_get_separate_directories(process_service):
    service, paths = process_service
    first, second = StatusTracker(mode="stream", run_id="same", runtime="process"), StatusTracker(mode="stream", run_id="same", runtime="process")
    for tracker in (first, second):
        service.status = tracker
        handle = service.run_bucket_lease_server(run_id="same", bucket_count=16, network="n", keyed_labels=[],
                                                 quiet_period_seconds=0.0, restart_attempt=len(os.listdir(paths.timings)))
        deadline = time.monotonic() + 15
        directory = telemetry.incarnation_dir(paths.timings, tracker.execution_id, handle.id)
        while not (directory / "metrics.prom").exists() and time.monotonic() < deadline:
            time.sleep(0.1)
        assert (directory / "metrics.prom").exists()
        handle.stop(timeout=10)
    assert {p.name for p in paths.timings.iterdir() if p.is_dir()} >= {first.execution_id, second.execution_id}


# -- the CLI wiring ------------------------------------------------------------------------------------------------------------------------------
def _cli_args(tmp_path, execution):
    from types import SimpleNamespace
    from neo4j_graph_loader import _runtime

    base = tmp_path / "attempt"
    for name in ("rejections", "plans", "spill", "timings"):
        (base / name).mkdir(parents=True)
    paths = _runtime.RunPaths(base, base / "s.yaml", base / "rejections", base / "plans", base / "spill", base / "timings")
    return SimpleNamespace(execution=execution, runtime=paths)


def test_the_cli_starts_and_stops_the_poller_and_its_listeners(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from neo4j_graph_loader import cli
    from neo4j_graph_loader.utils.schema_loader import load_schema

    monkeypatch.setenv(metrics.STATUS_INTERVAL_ENV, "0.5")
    execution = cli.StatusTracker(mode="stream", run_id="r", runtime="process")
    args = _cli_args(tmp_path, execution)
    oom_listeners: list = []
    runtime = SimpleNamespace(status_handles=lambda: {}, lifecycle=SimpleNamespace(add_oom_listener=oom_listeners.append))
    schema = load_schema(Path(__file__).resolve().parents[1] / "config" / "graph_schema.yaml")
    cleanups: list = []
    cli._start_status_telemetry(args, schema, runtime, cleanups)
    try:
        poller = args._status_poller
        assert poller._thread.is_alive() and poller._interval == 0.5 and rbm._lag_listener is not None
        entities = {tuple(item) for item in poller._entities}
        assert {("node", node.label) for node in schema.nodes} <= entities and {("edge", e.type) for e in schema.edges} <= entities

        rbm._notify_lag({"WORKS_AT": 12})                                       # what the lag poll does
        rbm._notify_lag({"WORKS_AT": None})
        lag = [t for t in execution.snapshot()["types"] if t["name"] == "WORKS_AT"][0]["lag"]
        assert lag is None
        rbm._notify_lag({"WORKS_AT": 7})
        lag = [t for t in execution.snapshot()["types"] if t["name"] == "WORKS_AT"][0]["lag"]
        assert lag["value"] == 7 and lag["source"] == "cli-lag-poll"

        oom_listeners[0](1, 3)
        assert execution.events()[-1]["type"] == "execution.oom"
    finally:
        for cleanup in reversed(cleanups):
            cleanup()
    assert not poller._thread.is_alive() and rbm._lag_listener is None


def test_the_cli_wiring_is_inert_without_an_execution_or_a_run_directory(tmp_path):
    from types import SimpleNamespace
    from neo4j_graph_loader import cli

    cleanups: list = []
    cli._start_status_telemetry(SimpleNamespace(), None, None, cleanups)
    cli._start_status_telemetry(SimpleNamespace(execution=cli.StatusTracker(mode="stream"), runtime=None), None, None, cleanups)
    assert cleanups == []


def test_plan_progress_is_published_per_type_and_for_the_run():
    from types import SimpleNamespace
    from neo4j_graph_loader.orchestrator.planned_edges import _publish_plan_progress

    execution = StatusTracker(mode="bulk", run_id="r")
    status = {"done": 3, "total": 8, "by_type": {"KNOWS": {"done": 2, "total": 5}, "WORKS_AT": {"done": 1, "total": 3}}}
    _publish_plan_progress(SimpleNamespace(execution=execution), status)
    _publish_plan_progress(SimpleNamespace(), status)                      # no execution: nothing
    _publish_plan_progress(SimpleNamespace(execution=execution), {"state": "failed"})   # no counts: nothing, no error
    snapshot = execution.snapshot()
    types = {t["name"]: t for t in snapshot["types"]}
    assert (types["KNOWS"]["plan_chunks_done"]["value"], types["KNOWS"]["plan_chunks_total"]["value"]) == (2, 5)
    assert snapshot["totals"]["plan_chunks_done"]["value"] == 3 and snapshot["totals"]["plan_chunks_total"]["source"] == "dispatcher-status"
    validate(snapshot, schemas.STRICT_STATUS_SCHEMA)


def test_a_docker_launch_with_the_timing_mount_gets_exactly_three_new_env_keys_and_the_same_incarnation(tmp_path):
    from docker_support import configure_images
    from neo4j_graph_loader.orchestrator.docker_service import DockerService

    args = _cli_args(tmp_path, StatusTracker(mode="stream", run_id="d1"))
    client = configure_images(MagicMock())
    client.containers.run.side_effect = lambda **kwargs: MagicMock(id="f" * 64, labels=kwargs["labels"])
    service = DockerService(client=client, runtime=args.runtime)
    service.status = args.execution
    service.run_node_loader(node_label="Person", topic="t", mode="stream", config_path="c.yaml", replicas=2, network="n",
                            rejection_dir=str(tmp_path))
    (call_one, call_two) = [c.kwargs for c in client.containers.run.call_args_list]
    for call in (call_one, call_two):
        env = call["environment"]
        mount = [spec["bind"] for spec in call["volumes"].values()]
        assert mount.count("/app/write-timings") == 1                           # the existing mount, not a new one
        assert env["GRAPH_LOADER_EXECUTION_ID"] == args.execution.execution_id
        assert env["GRAPH_LOADER_STATUS_SNAPSHOT_DIR"] == f"/app/write-timings/{args.execution.execution_id}/{env['GRAPH_LOADER_INCARNATION']}"
    roles = {r["incarnation"]: r for r in args.execution.snapshot()["roles"]}
    assert set(roles) == {call_one["environment"]["GRAPH_LOADER_INCARNATION"], call_two["environment"]["GRAPH_LOADER_INCARNATION"]}
    assert all(r["container_id"] == "f" * 64 for r in roles.values())
    assert set(service.status_handles()) == set(roles)


def test_a_docker_launch_without_a_timing_mount_or_a_tracker_adds_nothing(tmp_path):
    from docker_support import configure_images
    from neo4j_graph_loader.orchestrator.docker_service import DockerService

    client = configure_images(MagicMock())
    client.containers.run.side_effect = lambda **kwargs: MagicMock(id="e" * 64, labels=kwargs["labels"])
    service = DockerService(client=client)                                       # no run directory: no timing mount
    service.run_node_loader(node_label="Person", topic="t", mode="stream", config_path="c.yaml", replicas=1, network="n",
                            rejection_dir=str(tmp_path))
    service.status = StatusTracker(mode="stream", run_id="d2")                    # tracker but still no mount
    service.run_node_loader(node_label="Person", topic="t", mode="stream", config_path="c.yaml", replicas=1, network="n",
                            rejection_dir=str(tmp_path))
    for call in client.containers.run.call_args_list:
        assert not [key for key in call.kwargs["environment"] if key.startswith(("GRAPH_LOADER_EXECUTION", "GRAPH_LOADER_INCARNATION", "GRAPH_LOADER_STATUS"))]
    assert len(service.status.snapshot()["roles"]) == 1                          # still recorded, by container id
