"""P6-S5 Stories 1, 2, 6, 7 (unit level): CLI shutdown handling and the orphan matrix.

Everything runs against fakes: no Docker, no Kafka, no Neo4j.  Signals are
real (``os.kill`` to this very process) so the handlers ``_handle_start``
installs are the ones under test; every wait in the fakes is condition-based
with a bounded iteration guard, never a fixed sleep racing anything.

The reconciliation is symbolic: every container the fake Docker recorded as
started must end up stopped exactly once (a Docker ``stop`` of an already
stopped container is a no-op, so it is not counted), and not running.
"""
from __future__ import annotations

import argparse
import os
import signal
import time
from threading import Thread
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src import cli
from src.cli import build_parser, handle_start
from src.orchestrator.bulk_monitor import BulkMonitorError, BulkShutdownRequested

from test_planned_edges_cli import FakeControl, SCHEMA as PLANNED_SCHEMA, make_plan
from src.loader.edge_plan import write_plan_file


class NotPrompt(BaseException):
    """A BaseException so the code under test's ``except Exception`` cannot swallow the failure."""


_GUARD = 3000  # bounded spin iterations (x 1 ms) before a wait is declared not prompt


class FakeContainer:
    def __init__(self, name: str) -> None:
        self.name = name
        self.running = True
        self.effective_stops = 0
        self.removes = 0

    @property
    def attrs(self) -> dict:
        return {"State": {"Status": "running" if self.running else "exited", "ExitCode": 0}}

    def reload(self) -> None:
        pass

    def stop(self, timeout=None) -> None:
        if self.running:
            self.running = False
            self.effective_stops += 1

    def remove(self, force=False) -> None:
        self.removes += 1
        self.running = False

    def logs(self, tail=None, **_kwargs) -> bytes:
        return b""


class Fleet:
    """Everything the fake Docker started, by creation order."""

    def __init__(self) -> None:
        self.started: list[FakeContainer] = []

    def new(self, name: str) -> FakeContainer:
        container = FakeContainer(name)
        self.started.append(container)
        return container

    def names(self, prefix: str) -> list[str]:
        return [c.name for c in self.started if c.name.startswith(prefix)]

    def assert_no_orphans(self) -> None:
        assert self.started, "the scenario must have started something"
        for container in self.started:
            assert not container.running, f"orphan: {container.name}"
            assert container.effective_stops == 1, f"{container.name} stopped {container.effective_stops}x"


def _signal_and_wait(signum: int = signal.SIGTERM) -> None:
    """Deliver a real signal to this process and wait until the CLI Event saw it."""
    os.kill(os.getpid(), signum)
    for _ in range(_GUARD):
        if cli._shutdown_requested():
            return
        time.sleep(0.001)
    raise NotPrompt("signal was not observed by the CLI shutdown Event")


def _spin_until(predicate, what: str) -> None:
    for _ in range(_GUARD):
        if predicate():
            return
        time.sleep(0.001)
    raise NotPrompt(f"shutdown not prompt: {what} never observed the abort")


def _fake_docker(fleet: Fleet, *, on_node_launch=None) -> MagicMock:
    docker = MagicMock()

    def run_node_loader(node_label, replicas=1, **_kwargs):
        containers = [fleet.new(f"node-{node_label}-{i}") for i in range(replicas)]
        if on_node_launch is not None:
            on_node_launch(node_label)
        return containers

    docker.run_node_loader.side_effect = run_node_loader
    docker.run_edge_loader.side_effect = lambda edge_type, replicas=1, **_k: [
        fleet.new(f"edge-{edge_type}-{i}") for i in range(replicas)
    ]
    docker.run_bucket_lease_server.side_effect = lambda **_k: fleet.new("lease-server")
    docker.bucket_lease_address.return_value = "lease:7700"
    docker.run_planned_dispatcher.side_effect = lambda **_k: fleet.new("dispatcher")
    docker.planned_dispatcher_addresses.return_value = ("172.18.0.5:7701", "127.0.0.1:40000")
    docker.run_planned_edge_writers.side_effect = lambda writers=1, **_k: [
        fleet.new(f"writer-{i}") for i in range(writers)
    ]
    docker.client.containers.list.return_value = []
    return docker


def _schema(*, edges=(), labels=("Person", "Company")):
    return SimpleNamespace(
        nodes=[SimpleNamespace(label=label, topic=label.lower(), replicas=1) for label in labels],
        edges=list(edges),
        loading=SimpleNamespace(
            consumer_group_id="loader",
            bucket_lease=SimpleNamespace(tx_timeout_ms=30_000, restart_quiet_margin_ms=5_000),
        ),
    )


def _edge(edge_type, source, target):
    return SimpleNamespace(
        type=edge_type, topic=edge_type.lower(), topics=[edge_type.lower()], dead_letter_entity=edge_type.lower(),
        dead_letter=None, is_multi_topic=False, replicas=1,
        endpoint_labels=frozenset({source, target}), has_self_reference=source == target, is_multi_pair=False,
        nodes=SimpleNamespace(source=source, target=target, is_self_referencing=source == target),
        execution=SimpleNamespace(mode="python_apoc"),
    )


@pytest.fixture
def harness(monkeypatch, tmp_path):
    """Patch every external collaborator of ``_handle_start``; yield a runner."""
    monkeypatch.setenv("GRAPH_LOADER_METRICS_ENABLED", "0")
    monkeypatch.delenv("GRAPH_LOADER_TIMING_DIR", raising=False)
    fleet = Fleet()
    state = SimpleNamespace(fleet=fleet, docker=None, schema=None)
    patches = [
        patch("src.cli.get_neo4j_driver", return_value=MagicMock()),
        patch("src.cli.apply_schema"),
        patch("src.cli._ensure_control_topic"),
        patch("src.cli._ensure_dead_letter_topics"),
        patch("src.cli._ensure_retry_topics"),
    ]
    for p in patches:
        p.start()

    def run(schema, args, *, docker=None):
        state.docker = docker or _fake_docker(fleet)
        state.schema = schema
        before = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
        with patch("src.cli.load_schema", return_value=schema), patch("src.cli.DockerService", return_value=state.docker):
            try:
                result = handle_start(args)
            finally:
                after = {s: signal.getsignal(s) for s in before}
                assert after == before, "SIGINT/SIGTERM handlers were not restored"
                assert cli._cli_shutdown is None, "shutdown Event leaked past handle_start"
        return result

    yield SimpleNamespace(run=run, fleet=fleet, state=state, tmp_path=tmp_path)
    for p in patches:
        p.stop()


def _bulk_args(**overrides):
    values = dict(mode="bulk", config="config.yaml", network="net", skip_image_build=True, bulk_timeout_seconds=300.0)
    values["edge_classic"] = not (overrides.get("edge_leases") or overrides.get("edge_plan"))
    values.update(overrides)
    return argparse.Namespace(**values)


def _stream_args(**overrides):
    args = build_parser().parse_args(["start", "--mode", "stream", "--skip-image-build"])
    args.config, args.network = "config.yaml", "net"
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


class FakeBulkMonitor:
    """Stands in for BulkMonitor; behaviour is injected per scenario."""

    behaviour = None

    def __init__(self, schema, run_id, expected, tracked, **kwargs):
        self.abort_requested = kwargs.get("abort_requested")
        self.tracked = tracked

    def __getattr__(self, name):  # any wait the scenario does not script succeeds
        return lambda *a, **k: None

    def close(self):
        pass


# ── Story 1: handlers, restoration, every exit path ─────────────────────────


def test_handlers_are_installed_for_bulk_and_restored(harness):
    """Bulk used to install nothing: SIGTERM killed the CLI outright."""
    seen = {}

    class Monitor(FakeBulkMonitor):
        def wait_for_assignment_coverage(self, timeout):
            seen["term"] = signal.getsignal(signal.SIGTERM)
            seen["int"] = signal.getsignal(signal.SIGINT)

    with patch("src.cli.BulkMonitor", Monitor):
        assert harness.run(_schema(), _bulk_args()) == 0
    assert seen["term"] not in (signal.SIG_DFL, signal.default_int_handler)
    assert seen["int"] is not signal.default_int_handler
    assert all(c.removes == 1 for c in harness.fleet.started)


def test_double_sigint_and_sigterm_are_idempotent(harness):
    class Monitor(FakeBulkMonitor):
        def wait_for_assignment_coverage(self, timeout):
            _signal_and_wait(signal.SIGINT)
            os.kill(os.getpid(), signal.SIGINT)
            os.kill(os.getpid(), signal.SIGTERM)
            _spin_until(self.abort_requested, "coverage wait")
            raise BulkShutdownRequested("stop")

    with patch("src.cli.BulkMonitor", Monitor):
        assert harness.run(_schema(), _bulk_args()) == 1
    harness.fleet.assert_no_orphans()


def test_stop_requested_before_launch_starts_nothing(harness, monkeypatch):
    def build_then_signal(*_a, **_k):
        _signal_and_wait()

    docker = _fake_docker(harness.fleet)
    docker.build_image.side_effect = build_then_signal
    args = _bulk_args(skip_image_build=False)
    assert harness.run(_schema(), args, docker=docker) == 1
    docker.run_node_loader.assert_not_called()


def test_isolation_failure_stops_the_node_containers_it_started(harness):
    """Pre-flagged item (b): mixed shared + self-referencing edges, stream mode."""
    edges = [_edge("WORKS_AT", "Person", "Company"), _edge("KNOWS", "Person", "Person")]
    args = _stream_args(stream_clock_rotation=True)
    assert harness.run(_schema(edges=edges), args) == 1
    harness.state.docker.run_edge_loader.assert_not_called()
    harness.fleet.assert_no_orphans()
    assert len(harness.fleet.started) == 2


def test_an_exception_escaping_stream_orchestration_still_stops_node_containers(harness):
    with patch("src.cli._run_lease_relationship_fleet", side_effect=RuntimeError("boom")):
        with pytest.raises(RuntimeError, match="boom"):
            harness.run(_schema(edges=[_edge("WORKS_AT", "Person", "Company")]), _stream_args())
    harness.fleet.assert_no_orphans()


def test_stream_without_edges_leaves_its_detached_node_loaders_running(harness):
    """Unchanged contract: nothing supervises them, so the CLI does not stop them."""
    assert harness.run(_schema(), _stream_args()) == 0
    assert harness.fleet.started and all(c.running for c in harness.fleet.started)


# ── Story 7 (unit): the five-phase orphan matrix ────────────────────────────


def _planned_args(tmp_path):
    return _bulk_args(edge_plan=str(tmp_path / "edge-plan.json"), plan_writers=2)


def _launch_phase(harness, mode):
    """SIGTERM while the node-loader launch executor is in flight."""
    fired = []

    def on_launch(label):
        if not fired:
            fired.append(label)
            _signal_and_wait()

    docker = _fake_docker(harness.fleet, on_node_launch=on_launch)
    schema = _schema(edges=[_edge("WORKS_AT", "Person", "Company")])
    args = _bulk_args() if mode == "bulk" else _stream_args()
    with patch("src.cli._run_lease_relationship_fleet") as fleet_run, patch("src.cli.BulkMonitor") as monitor_cls:
        assert harness.run(schema, args, docker=docker) == 1
        fleet_run.assert_not_called()
        monitor_cls.assert_not_called()


def _node_drain_phase(harness, shared):
    class Monitor(FakeBulkMonitor):
        def wait_for_assignment_coverage(self, timeout):
            _signal_and_wait()
            _spin_until(self.abort_requested, "node drain wait")
            raise BulkShutdownRequested("stop")

        def drive_label_readiness(self, labels, timeout, *, stop_label, on_ready, should_abort):
            _signal_and_wait()
            _spin_until(should_abort, "label readiness")
            raise BulkMonitorError("aborted")

    edges = [_edge("WORKS_AT", "Person", "Company")] if shared else []
    args = _bulk_args(bulk_edge_overlap="shared") if shared else _bulk_args()
    with patch("src.cli.BulkMonitor", Monitor):
        assert harness.run(_schema(edges=edges), args) == 1


def _plan_wait_phase(harness):
    class Monitor(FakeBulkMonitor):
        def drive_label_readiness(self, labels, timeout, *, stop_label, on_ready, should_abort):
            _signal_and_wait()
            _spin_until(should_abort, "label readiness")
            raise BulkMonitorError("aborted")

    args = _planned_args(harness.tmp_path)  # the plan file never appears
    with patch("src.cli.BulkMonitor", Monitor):
        assert harness.run(PLANNED_SCHEMA, args) == 1
    harness.state.docker.run_planned_dispatcher.assert_not_called()


def _dispatcher_phase(harness):
    plan = make_plan()
    write_plan_file(harness.tmp_path / "edge-plan.json", plan)
    control = FakeControl(plan, [{"state": "running", "done": 0}])
    real = cli.run_planned_edge_phase

    def planned_phase(*args, **kwargs):
        return real(*args, control_factory=lambda address, digest: control,
                    verify_topic=lambda plan_, bootstrap: None, **kwargs)

    class Monitor(FakeBulkMonitor):
        def drive_label_readiness(self, labels, timeout, *, stop_label, on_ready, should_abort):
            for label in sorted(labels):
                on_ready(label)
            fleet = harness.fleet
            _spin_until(lambda: len(fleet.names("dispatcher")) == 1 and len(fleet.names("writer")) == 2,
                        "dispatcher/writer launch")
            _signal_and_wait()
            _spin_until(should_abort, "dispatcher phase")
            raise BulkMonitorError("aborted")

    with patch("src.cli.BulkMonitor", Monitor), patch("src.cli.run_planned_edge_phase", planned_phase):
        assert harness.run(PLANNED_SCHEMA, _planned_args(harness.tmp_path)) == 1
    assert control.aborted, "the dispatcher must be told to abort"
    assert len(harness.fleet.names("dispatcher")) == 1 and len(harness.fleet.names("writer")) == 2


def _dispatcher_after_readiness_phase(harness):
    """The normal shape: nodes are long done, only the dispatch phase is running."""
    plan = make_plan()
    write_plan_file(harness.tmp_path / "edge-plan.json", plan)
    control = FakeControl(plan, [{"state": "running", "done": 0}])
    real = cli.run_planned_edge_phase

    def planned_phase(*args, **kwargs):
        return real(*args, control_factory=lambda address, digest: control,
                    verify_topic=lambda plan_, bootstrap: None, **kwargs)

    class Monitor(FakeBulkMonitor):
        def drive_label_readiness(self, labels, timeout, *, stop_label, on_ready, should_abort):
            for label in sorted(labels):
                on_ready(label)
            # readiness returns; a helper thread signals once the writers run
            fleet = harness.fleet

            def signal_later():
                _spin_until(lambda: len(fleet.names("writer")) == 2, "writer launch")
                os.kill(os.getpid(), signal.SIGTERM)

            Thread(target=signal_later, daemon=True).start()

    args = _planned_args(harness.tmp_path)
    args.bulk_timeout_seconds = 300.0  # the progress deadline must not be what ends the run
    started = time.monotonic()
    with patch("src.cli.BulkMonitor", Monitor), patch("src.cli.run_planned_edge_phase", planned_phase):
        assert harness.run(PLANNED_SCHEMA, args) == 1
    assert time.monotonic() - started < 30
    assert control.aborted, "the dispatcher must be told to abort"


def _stream_supervision_phase(harness):
    class Monitor:
        def __init__(self, *args, **kwargs):
            self.tracked_lease_server = kwargs["tracked_lease_server"]

        def wait_for_lease_server_ready(self, timeout, shutdown_requested=None, **_k):
            pass

        def supervise_stream(self, shutdown_requested, **_k):
            _signal_and_wait()
            if not shutdown_requested.is_set():
                raise NotPrompt("supervise_stream must share the CLI-wide Event")

        def close(self):
            pass

    schema = _schema(edges=[_edge("WORKS_AT", "Person", "Company")])
    with patch("src.cli.RelationshipBulkMonitor", Monitor):
        assert harness.run(schema, _stream_args()) == 0
    assert harness.fleet.names("lease-server") and harness.fleet.names("edge-")


@pytest.mark.parametrize("phase", [
    "launch-bulk", "launch-stream", "node-drain", "node-drain-shared-scheduler", "plan-wait",
    "dispatcher", "dispatcher-after-readiness", "stream-supervision",
])
def test_sigterm_at_each_phase_leaves_no_orphan_container(harness, phase):
    {
        "launch-bulk": lambda: _launch_phase(harness, "bulk"),
        "launch-stream": lambda: _launch_phase(harness, "stream"),
        "node-drain": lambda: _node_drain_phase(harness, shared=False),
        "node-drain-shared-scheduler": lambda: _node_drain_phase(harness, shared=True),
        "plan-wait": lambda: _plan_wait_phase(harness),
        "dispatcher": lambda: _dispatcher_phase(harness),
        "dispatcher-after-readiness": lambda: _dispatcher_after_readiness_phase(harness),
        "stream-supervision": lambda: _stream_supervision_phase(harness),
    }[phase]()
    harness.fleet.assert_no_orphans()


# ── Story 6: a SIGTERM-driven stream run leaves a parseable CLI snapshot ────


def _free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_sigterm_driven_stream_run_leaves_a_parseable_cli_metrics_snapshot(harness, monkeypatch, tmp_path):
    from prometheus_client.parser import text_string_to_metric_families

    port = _free_port()
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    monkeypatch.setenv("GRAPH_LOADER_METRICS_PORT", str(port))
    monkeypatch.setenv("GRAPH_LOADER_TIMING_DIR", str(tmp_path))

    with patch("src.cli._run_lease_relationship_fleet", side_effect=lambda *a, **k: (_signal_and_wait(), 0)[1]):
        assert harness.run(_schema(edges=[_edge("WORKS_AT", "Person", "Company")]), _stream_args()) == 0

    snapshot = tmp_path / "metrics-cli.prom"
    text = snapshot.read_text()
    assert text.strip()
    families = {family.name: family for family in text_string_to_metric_families(text)}
    assert "graph_loader_process_up" in families
    harness.fleet.assert_no_orphans()
    import socket

    with pytest.raises(ConnectionRefusedError):
        socket.create_connection(("127.0.0.1", port), timeout=1)


def test_removal_already_in_progress_is_success_not_an_error(caplog):
    """D3: auto-remove containers answer 409 'removal ... already in progress'."""
    busy = MagicMock()
    busy.remove.side_effect = Exception("409 Client Error: Conflict (removal of container abc is already in progress)")
    broken = MagicMock()
    broken.remove.side_effect = Exception("500 boom")
    with caplog.at_level("DEBUG", logger=cli.logger.name):
        cli._remove_exact_containers([busy])
        assert not [r for r in caplog.records if r.levelname == "ERROR"]
        cli._remove_exact_containers([broken])
    assert [r for r in caplog.records if r.levelname == "ERROR"]


def test_node_loader_watcher_flags_a_missing_node_loader_and_requests_shutdown(caplog) -> None:
    """Stream supervision: a node loader that vanished stops the run (rc 1 by the caller)."""
    running = MagicMock()
    running.attrs = {"State": {"Status": "running"}}
    gone = MagicMock()
    gone.name = "graph-loader-node-Person-0"
    gone.reload.side_effect = cli.NotFound("gone")
    shutdown = cli.Event()
    with caplog.at_level("ERROR"):
        watcher = cli._NodeLoaderWatcher([running, gone], shutdown, interval=0.01).start()
        assert shutdown.wait(3)
        watcher.stop()
    assert watcher.failed
    assert any("graph-loader-node-Person-0" in r.getMessage() for r in caplog.records)


def test_node_loader_watcher_ignores_exits_during_an_operator_stop() -> None:
    gone = MagicMock()
    gone.reload.side_effect = cli.NotFound("gone")
    shutdown = cli.Event()
    shutdown.set()
    watcher = cli._NodeLoaderWatcher([gone], shutdown, interval=0.01).start()
    watcher.stop()
    assert not watcher.failed


def test_each_container_is_stopped_at_most_once_across_overlapping_cleanups() -> None:
    """F2: the fleet's stop passes and the stream node cleanup all name the same
    node containers; a second stop() while the first drains sends a second SIGTERM."""
    import threading

    first_stop_entered, release = threading.Event(), threading.Event()
    node = MagicMock()

    def slow_stop(**_kwargs):
        first_stop_entered.set()
        assert release.wait(5)

    node.stop.side_effect = slow_stop
    other = MagicMock()
    results = []
    first = threading.Thread(target=lambda: results.append(cli._stop_exact_containers([node, other])))
    first.start()
    assert first_stop_entered.wait(5)
    second = threading.Thread(target=lambda: results.append(cli._stop_exact_containers([node])))
    second.start()  # concurrent duplicate: must wait for the first, then skip
    release.set()
    first.join(5)
    second.join(5)
    assert results == [[], []]
    # A later sequential pass (e.g. the final sweep) is a no-op as well.
    assert cli._stop_exact_containers([node, other], stop_timeout=3) == []
    assert node.stop.call_count == 1 and other.stop.call_count == 1


def test_a_failed_stop_is_not_recorded_so_the_retry_still_happens() -> None:
    stuck = MagicMock()
    stuck.stop.side_effect = [RuntimeError("daemon unavailable"), None]
    stuck.attrs = {"State": {"Status": "running"}}
    cli._stop_exact_containers([stuck])
    assert stuck.stop.call_count == 2


def test_node_loader_watcher_ignores_containers_the_cli_itself_is_stopping(caplog) -> None:
    """F1c: the fleet's cleanup stops node loaders while the watcher still runs."""
    import time

    node = MagicMock()
    node.name = "graph-loader-node-Person-0"
    node.reload.side_effect = cli.NotFound("gone")
    shutdown = cli.Event()
    cli._stop_exact_containers([node])
    with caplog.at_level("ERROR"):
        watcher = cli._NodeLoaderWatcher([node], shutdown, interval=0.01).start()
        time.sleep(0.2)
        watcher.stop()
    assert not watcher.failed and not shutdown.is_set()
    assert not [r for r in caplog.records if "node-loader-supervision" in r.getMessage()]
