"""Tests for the exact shared-fleet clock container entry point."""
import signal as signal_module
import urllib.request
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.orchestrator.clock_runner import main
from src.utils import metrics as metrics_module


def _bind_metrics_to_an_ephemeral_port(monkeypatch):
    """Make any metrics server this test starts bind an OS-assigned port (0).

    The default 9464 may already be taken on the host (e.g. by a live CLI
    metrics endpoint); asking the OS at bind time has no probe/bind race.
    """
    real_start_http_server = metrics_module.start_http_server
    monkeypatch.setattr(
        metrics_module, "start_http_server",
        lambda port, addr="0.0.0.0", registry=None: real_start_http_server(0, addr=addr, registry=registry),
    )


def _schema(topic="clock-topic"):
    edge = SimpleNamespace(
        type="WORKS_AT", topic="works", replicas=1,
        nodes=SimpleNamespace(source="Person", target="Company", is_self_referencing=False),
    )
    return SimpleNamespace(
        edges=(edge,),
        loading=SimpleNamespace(
            coordination=SimpleNamespace(topic=topic, bucket_count=1, slot_duration_ms=10, lease_timeout_ms=20),
        ),
    )


def test_clock_runner_rejects_noncanonical_fleet_before_schema_or_kafka():
    with patch("src.orchestrator.clock_runner.load_schema") as load, \
         patch("src.orchestrator.clock_runner.Producer") as producer:
        assert main(["--config", "schema.yaml", "--run-id", "run", "--fleet-edge-types", "WORKS_AT,BOUGHT", "--coordination-topic", "clock-topic"]) == 1
    load.assert_not_called()
    producer.assert_not_called()


def test_clock_runner_rejects_topic_split_before_kafka():
    with patch("src.orchestrator.clock_runner.load_schema", return_value=_schema()) as load, \
         patch("src.orchestrator.clock_runner.Producer") as producer:
        assert main(["--config", "schema.yaml", "--run-id", "run", "--fleet-edge-types", "WORKS_AT", "--coordination-topic", "other-topic"]) == 1
    load.assert_called_once_with("schema.yaml")
    producer.assert_not_called()


def test_clock_runner_rejects_self_reference_fleet_before_kafka():
    schema = _schema()
    knows = SimpleNamespace(
        type="KNOWS", topic="knows", replicas=1,
        nodes=SimpleNamespace(source="Person", target="Person", is_self_referencing=True),
    )
    schema.edges = (*schema.edges, knows)
    with patch("src.orchestrator.clock_runner.load_schema", return_value=schema), \
         patch("src.orchestrator.clock_runner.Producer") as producer:
        assert main(["--config", "schema.yaml", "--run-id", "run", "--fleet-edge-types", "KNOWS", "--coordination-topic", "clock-topic"]) == 1
    producer.assert_not_called()


def test_clock_runner_passes_exact_rotation_plan_to_global_clock():
    producer, global_clock = MagicMock(), MagicMock()
    global_clock.run.return_value = 0
    with patch("src.orchestrator.clock_runner.load_schema", return_value=_schema()), \
         patch("src.orchestrator.clock_runner.Producer", return_value=producer), \
         patch("src.orchestrator.clock_runner.GlobalBatchClock", return_value=global_clock) as clock_cls, \
         patch("src.orchestrator.clock_runner.signal.signal", return_value=object()):
        assert main(["--config", "schema.yaml", "--run-id", "run", "--fleet-edge-types", "WORKS_AT", "--coordination-topic", "clock-topic"]) == 0
    assert clock_cls.call_args.kwargs["run_id"] == "run"
    assert [edge.type for edge in clock_cls.call_args.kwargs["edges"]] == ["WORKS_AT"]
    assert clock_cls.call_args.kwargs["rotation_plan"].bucket_count == 1
    producer.flush.assert_called_once()


def test_clock_serves_process_up_but_no_write_duration_series(monkeypatch):
    """P6-S2 Story 7's minimal scope: the clock gets the process-info triad
    (up/start_time/info) like every other process kind, but no hot-path
    write-latency histogram - the clock never writes to Neo4j.

    P6-S5 Story 3 (flaky-test fix): the previous version of this test probed
    a "free" port with a throwaway socket, closed it, and then trusted it
    would still be free by the time `start_http_server` bound it - a genuine
    TOCTOU race against any other process on the host, and the documented
    cause of this test failing once and passing on re-run. There is no
    sleep/retry/bigger-timeout fix for a port race; the fix is to never
    guess a port at all. `start_http_server` is asked for an OS-assigned
    ephemeral port (`port=0`) at the moment it actually binds, and the test
    reads back the port the OS chose from the real, already-bound server -
    no gap between "probe" and "bind" exists for anything else to win.
    """
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    monkeypatch.delenv("GRAPH_LOADER_METRICS_PORT", raising=False)

    real_start_http_server = metrics_module.start_http_server
    bound: dict[str, int] = {}

    def capturing_start_http_server(port, addr="0.0.0.0", registry=None):
        server, thread = real_start_http_server(0, addr=addr, registry=registry)
        bound["port"] = server.server_port
        return server, thread

    producer, global_clock = MagicMock(), MagicMock()
    captured: dict[str, str] = {}

    def blocking_run(_shutdown):
        # P6-S2 review finding S7-4: capture the body and return normally
        # instead of asserting inside the mocked run() - an assertion
        # raised in here would be swallowed by main()'s own `except
        # Exception` (-> return 1), losing the real failure message behind
        # a bare "assert 1 == 0".
        captured["body"] = urllib.request.urlopen(
            f"http://127.0.0.1:{bound['port']}/metrics", timeout=5
        ).read().decode()
        return 0

    global_clock.run.side_effect = blocking_run
    try:
        with patch("src.orchestrator.clock_runner.load_schema", return_value=_schema()), \
             patch("src.orchestrator.clock_runner.Producer", return_value=producer), \
             patch("src.orchestrator.clock_runner.GlobalBatchClock", return_value=global_clock), \
             patch("src.orchestrator.clock_runner.signal.signal", return_value=object()), \
             patch("src.utils.metrics.start_http_server", side_effect=capturing_start_http_server):
            assert main([
                "--config", "schema.yaml", "--run-id", "run",
                "--fleet-edge-types", "WORKS_AT", "--coordination-topic", "clock-topic",
            ]) == 0
    finally:
        metrics_module._reset_process_metrics_for_tests()

    body = captured["body"]
    assert 'graph_loader_process_up{kind="clock",name="-"} 1.0' in body
    # The metric *family*'s HELP/TYPE header always exists (every process
    # kind's registry declares the full catalogue); "no write-latency
    # series exists for this process kind" means no actual *sample* line
    # - the clock never calls .observe() on it.
    assert "graph_loader_neo4j_write_duration_seconds_bucket{" not in body
    assert "graph_loader_neo4j_write_duration_seconds_count{" not in body


def test_clock_sigterm_mid_run_leaves_snapshot(tmp_path, monkeypatch):
    """P6-S2 Story 9/10 (review finding S9-1): a SIGTERM arriving mid-run
    still leaves a metrics-*.prom snapshot with the process-info triad
    (process_up=1) - the clock's whole instrumentation scope."""
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    _bind_metrics_to_an_ephemeral_port(monkeypatch)
    snapshot_path = tmp_path / "metrics-clock.prom"
    monkeypatch.setenv("GRAPH_LOADER_METRICS_SNAPSHOT_PATH", str(snapshot_path))

    producer, global_clock = MagicMock(), MagicMock()
    handlers: dict[str, object] = {}
    real_signal = signal_module.signal

    def capturing_signal(sig, handler):
        previous = real_signal(sig, handler)
        if sig == signal_module.SIGTERM:
            handlers["current"] = handler
        return previous

    def blocking_run(shutdown):
        # Simulate the real SIGTERM handler firing mid-run, then the
        # clock's own run() observing the shutdown Event and returning.
        handlers["current"](signal_module.SIGTERM, None)
        assert shutdown.is_set()
        return 0

    global_clock.run.side_effect = blocking_run
    try:
        with patch("src.orchestrator.clock_runner.load_schema", return_value=_schema()), \
             patch("src.orchestrator.clock_runner.Producer", return_value=producer), \
             patch("src.orchestrator.clock_runner.GlobalBatchClock", return_value=global_clock), \
             patch("src.orchestrator.clock_runner.signal.signal", side_effect=capturing_signal):
            assert main([
                "--config", "schema.yaml", "--run-id", "run",
                "--fleet-edge-types", "WORKS_AT", "--coordination-topic", "clock-topic",
            ]) == 0
    finally:
        metrics_module._reset_process_metrics_for_tests()

    assert snapshot_path.exists()
    assert 'graph_loader_process_up{kind="clock",name="-"} 1.0' in snapshot_path.read_text()


def test_clock_runner_handler_installed_before_load_schema_and_producer(tmp_path, monkeypatch):
    """P6-S5 Story 3: the SIGTERM handler must be live before `load_schema`
    (and therefore before `Producer(...)` construction), not after -
    closing the avoidable pre-handler-installation window
    `clock_runner.py`'s own comment named as this slice's to fix.

    An actual OS-level pre-emption at that exact instant isn't reliably
    reproducible in a unit test, so this fires the signal from inside a
    `load_schema` stub instead - proving the handler main() installs is
    already the live SIGTERM disposition by the time `load_schema` runs,
    and that firing it there still drives a clean, `finally`-executed
    shutdown (handler restored, snapshot written, producer flushed) rather
    than an untraceable process kill.
    """
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    _bind_metrics_to_an_ephemeral_port(monkeypatch)
    snapshot_path = tmp_path / "metrics-clock.prom"
    monkeypatch.setenv("GRAPH_LOADER_METRICS_SNAPSHOT_PATH", str(snapshot_path))

    previous_handler = signal_module.getsignal(signal_module.SIGTERM)
    observed: dict[str, object] = {}

    def load_schema_fires_signal_first(_config_path):
        handler = signal_module.getsignal(signal_module.SIGTERM)
        observed["handler_was_installed"] = handler is not previous_handler
        handler(signal_module.SIGTERM, None)
        return _schema()

    producer, global_clock = MagicMock(), MagicMock()

    def run_observes_shutdown(shutdown):
        observed["shutdown_set_before_run"] = shutdown.is_set()
        return 0

    global_clock.run.side_effect = run_observes_shutdown
    try:
        with patch("src.orchestrator.clock_runner.load_schema", side_effect=load_schema_fires_signal_first), \
             patch("src.orchestrator.clock_runner.Producer", return_value=producer), \
             patch("src.orchestrator.clock_runner.GlobalBatchClock", return_value=global_clock):
            assert main([
                "--config", "schema.yaml", "--run-id", "run",
                "--fleet-edge-types", "WORKS_AT", "--coordination-topic", "clock-topic",
            ]) == 0
    finally:
        metrics_module._reset_process_metrics_for_tests()

    assert observed["handler_was_installed"] is True
    assert observed["shutdown_set_before_run"] is True
    assert signal_module.getsignal(signal_module.SIGTERM) == signal_module.SIG_IGN  # F2
    producer.flush.assert_called_once()
    assert snapshot_path.exists()
