"""E14.F2.S5: metrics endpoints, Prometheus file_sd, health endpoints and the child log relay (process runtime).

Real child processes throughout (the lease server and fixture roles); no Kafka or Neo4j. Linux repeats the loopback-only
check with ``ss``/``lsof`` on the host.
"""
from __future__ import annotations

import http.client
import json
import socket
import threading
import time
import urllib.request
from pathlib import Path

import pytest

from neo4j_graph_loader import _runtime
from neo4j_graph_loader.orchestrator.health import parse_address
from neo4j_graph_loader.orchestrator.process_runtime import LogRelay, ProcessRuntime
from neo4j_graph_loader.orchestrator.process_service import ProcessService

FIXTURES = Path(__file__).resolve().parent / "fixtures"
ROLE = "script:" + str(FIXTURES / "e14_role.py")
ALLOW = {"GRAPH_LOADER_CHILD_ALLOW_SCRIPT": "1"}


@pytest.fixture
def paths(tmp_path):
    root = tmp_path / "attempt"
    for name in ("rejections", "plans", "spill", "timings"):
        (root / name).mkdir(parents=True)
    schema = root / "runtime_schema.yaml"
    schema.write_text("x: 1\n")
    return _runtime.RunPaths(root, schema, root / "rejections", root / "plans", root / "spill", root / "timings")


@pytest.fixture(autouse=True)
def metrics_on(monkeypatch):
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)     # the suite default is "0"; these tests want endpoints
    monkeypatch.delenv("GRAPH_LOADER_METRICS_PORT", raising=False)


def targets(service):
    return json.loads(service.lifecycle.targets_path.read_text())


def scrape(port: int) -> str:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
        return response.read().decode()


def lease(service, run_id="run-obs"):
    return service.run_bucket_lease_server(run_id=run_id, bucket_count=16, network="n", keyed_labels=[])


def wait_until(predicate, seconds=10):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


# -- metrics endpoints and file_sd ------------------------------------------------------------------------------------------------
def test_every_role_gets_its_own_loopback_metrics_port_listed_in_file_sd(paths) -> None:
    service = ProcessService(paths, ready_timeout=60)
    try:
        first, second = lease(service, "run-a"), lease(service, "run-b")
        ports = [first.metrics["port"], second.metrics["port"]]
        assert first.metrics["state"] == "listening" and len(set(ports)) == 2 and 9464 not in ports
        groups = targets(service)
        listed = {g["labels"]["role"]: g for g in groups}                      # (S8: `exporter` is now unique per incarnation; `role` keeps the handle name)
        assert listed[first.name]["targets"] == [f"127.0.0.1:{first.metrics['port']}"]
        assert listed[first.name]["labels"]["component"] == "bucket-lease-server" and listed[first.name]["labels"]["runtime"] == "process"
        assert listed[first.name]["labels"]["run_id"] == "run-a" and listed[second.name]["labels"]["run_id"] == "run-b"
        assert not any("." in key for group in groups for key in group["labels"]), "file_sd label names cannot contain dots"
        body = scrape(first.metrics["port"])                                 # scrapeable, and it is this role's registry
        assert "graph_loader_process_up" in body and 'kind="lease-server"' in body
    finally:
        service.close()


def test_file_sd_is_rewritten_on_replacement_and_on_exit(paths) -> None:
    service = ProcessService(paths, ready_timeout=60)
    try:
        first = lease(service)
        old_target = targets(service)[0]["targets"][0]
        first.stop(timeout=10)
        assert wait_until(lambda: targets(service) == [])                    # exited: the target disappears
        first.remove(force=True)
        replacement = service.run_bucket_lease_server(run_id="run-obs", bucket_count=16, network="n", keyed_labels=[],
                                                      quiet_period_seconds=0.0, restart_attempt=1)
        new_target = targets(service)[0]["targets"][0]
        assert new_target == f"127.0.0.1:{replacement.metrics['port']}" and old_target != new_target
        replacement.stop(timeout=10)
        assert wait_until(lambda: targets(service) == [])
        raw = service.lifecycle.targets_path.read_text()
        json.loads(raw)                                                       # always valid JSON, never a partial file
    finally:
        service.close()


def test_file_sd_never_shows_a_partial_file(paths) -> None:
    """A 1 ms polling reader sees complete JSON on every read while roles start and stop."""
    service = ProcessService(paths, ready_timeout=60)
    stop, bad = threading.Event(), []

    def poll():
        while not stop.is_set():
            try:
                json.loads(service.lifecycle.targets_path.read_text())
            except FileNotFoundError:
                pass
            except ValueError as exc:
                bad.append(str(exc))
            time.sleep(0.001)

    thread = threading.Thread(target=poll)
    thread.start()
    try:
        for index in range(6):
            handle = lease(service, f"run-{index}")
            handle.stop(timeout=10)
    finally:
        stop.set()
        thread.join()
        service.close()
    assert bad == []


def test_disabled_metrics_leave_a_group_without_targets(paths, monkeypatch) -> None:
    monkeypatch.setenv("GRAPH_LOADER_METRICS_ENABLED", "0")                  # the builders pass the operator's switch through
    service = ProcessService(paths, ready_timeout=60)
    try:
        handle = lease(service)
        assert handle.metrics == {"state": "disabled"}
        (group,) = targets(service)
        assert group["targets"] == [] and group["labels"]["metrics_state"] == "disabled" and group["labels"]["role"] == handle.name and group["labels"]["exporter"].startswith("lease-server-")
        with socket.create_connection(("127.0.0.1", handle.ports["control"]), timeout=5):
            pass                                                             # the role itself is up and serving
    finally:
        service.close()


def test_an_occupied_metrics_port_is_recorded_and_the_role_keeps_running(paths) -> None:
    squatter = socket.socket()
    squatter.bind(("127.0.0.1", 0))
    squatter.listen()
    occupied = squatter.getsockname()[1]
    service = ProcessService(paths, metrics_addr=f"127.0.0.1:{occupied}", ready_timeout=60)
    try:
        handle = lease(service)
        assert handle.metrics == {"state": "failed"}
        (group,) = targets(service)
        assert group["targets"] == [] and group["labels"]["metrics_state"] == "failed"
        with socket.create_connection(("127.0.0.1", handle.ports["control"]), timeout=5):
            pass                                                             # it keeps doing its job
        assert handle.status == "running"
    finally:
        squatter.close()
        service.close()


def test_the_cli_endpoint_is_listed_too(paths) -> None:
    service = ProcessService(paths, ready_timeout=60)
    try:
        service.set_cli_metrics(type("Server", (), {"server_port": 41234})())
        assert targets(service)[0] == {"targets": ["127.0.0.1:41234"], "labels": {
            "component": "cli", "exporter": "cli", "kind": "cli", "run_id": "", "runtime": "process"}}
    finally:
        service.close()


def test_nothing_listens_on_a_non_loopback_address(paths) -> None:
    service = ProcessService(paths, ready_timeout=60)
    try:
        handle = lease(service)
        listening = _listening(handle.pid)
        assert len(listening) == 2, listening                                 # control port + metrics port, nothing else
        assert all(address == "127.0.0.1" for address, _port in listening), listening
    finally:
        service.close()


def _listening(pid: int) -> list[tuple[str, int]]:
    """(address, port) of every TCP socket ``pid`` listens on (psutil; the installed runner allows only Python child processes)."""
    import psutil
    return sorted((c.laddr.ip, c.laddr.port) for c in psutil.Process(pid).net_connections(kind="tcp") if c.status == psutil.CONN_LISTEN)


# -- health ---------------------------------------------------------------------------------------------------------------------
def get(health, path):
    connection = http.client.HTTPConnection(health.host, health.port, timeout=5)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        return response.status, json.loads(response.read() or b"{}")
    finally:
        connection.close()


def test_parse_address() -> None:
    assert parse_address("127.0.0.1:8080") == ("127.0.0.1", 8080)
    assert parse_address(":0") == ("127.0.0.1", 0) and parse_address("9") == ("127.0.0.1", 9)
    for bad in ("host:notaport", "host:70000"):
        with pytest.raises(ValueError):
            parse_address(bad)


def test_healthz_and_readyz_transitions(paths, tmp_path) -> None:
    service = ProcessService(paths, ready_timeout=60)
    stopping = {"now": False}
    service.set_stopping_check(lambda: stopping["now"])
    health = service.start_health("127.0.0.1:0")
    try:
        assert get(health, "/healthz")[0] == 200
        status, body = get(health, "/readyz")
        assert status == 503 and body["roles"] == 0                           # not ready control: nothing launched yet
        slow = service.lifecycle.spawn("role", ROLE, ["slow-ready", "1.0", str(tmp_path / "m")], ALLOW, wait_ready=False)
        status, body = get(health, "/readyz")
        assert status == 503 and body["pending"] == [slow.name]               # not ready control: a role has not reported ready
        assert slow.wait_ready is not None
        slow.wait_ready(30)
        status, body = get(health, "/readyz")
        assert status == 200 and body["status"] == "ready" and body["failed"] == []
        stopping["now"] = True
        assert get(health, "/readyz")[0] == 503 and get(health, "/readyz")[1]["stopping"] is True
        stopping["now"] = False
        assert get(health, "/readyz")[0] == 200
        broken = service.lifecycle.spawn("role", ROLE, ["exit-after-ready", "3"], ALLOW)
        assert broken.wait(20) == 3
        status, body = get(health, "/readyz")
        assert status == 503 and body["failed"] == [broken.name]              # unrecovered failure
        assert get(health, "/healthz")[0] == 200                              # the CLI itself is still alive
        service.lifecycle._forget(broken)
        assert get(health, "/readyz")[0] == 200                               # replaced/forgotten: recovered
        assert get(health, "/nope")[0] == 404
        slow.stop(timeout=10)
    finally:
        service.close()


def test_a_role_that_finished_its_work_with_exit_zero_is_not_a_failure(paths) -> None:
    service = ProcessService(paths, ready_timeout=60)
    health = service.start_health("127.0.0.1:0")
    try:
        done = service.lifecycle.spawn("role", ROLE, ["exit-after-ready", "0"], ALLOW)
        done.wait(20)
        status, body = get(health, "/readyz")
        assert body["failed"] == [] and body["running"] == 0 and status == 503  # nothing running any more: not ready, not failed
    finally:
        service.close()


def test_healthz_reports_a_broken_registry(paths) -> None:
    service = ProcessService(paths, ready_timeout=60)
    health = service.start_health("127.0.0.1:0")
    try:
        service.lifecycle.registry._broken = True
        assert get(health, "/healthz")[0] == 503
    finally:
        service.lifecycle.registry._broken = False
        service.close()


# -- log relay -----------------------------------------------------------------------------------------------------------------
class BlockedStream:
    """A stdout that is stuck (a full pipe nobody reads)."""

    def __init__(self):
        self.release = threading.Event()
        self.lines = []

    def write(self, text):
        self.release.wait()
        self.lines.append(text)

    def flush(self):
        pass


def test_relay_prefixes_lines_and_files_stay_complete(tmp_path) -> None:
    class Collect:
        def __init__(self):
            self.lines = []
        def write(self, text):
            self.lines.append(text)
        def flush(self):
            pass
    sink = Collect()
    relay = LogRelay(stream=sink)
    runtime = ProcessRuntime(tmp_path / "run", relay=relay, ready_timeout=60)
    try:
        handle = runtime.spawn("role", ROLE, ["chatty", "5"], ALLOW)
        assert wait_until(lambda: any("line 4" in line for line in sink.lines))
        assert "[role-0] line 0\n" in sink.lines and "[role-0] err 1\n" in sink.lines
        assert b"line 4" in handle.logs()
        handle.kill()
    finally:
        runtime.close()


def test_a_burst_never_blocks_a_child_when_stdout_is_stuck_and_the_file_is_complete(tmp_path) -> None:
    lines = 200_000
    stream = BlockedStream()
    relay = LogRelay(stream=stream, capacity=500)
    runtime = ProcessRuntime(tmp_path / "run", relay=relay, ready_timeout=60, log_cap_bytes=1 << 30)
    try:
        started = time.monotonic()
        handle = runtime.spawn("role", ROLE, ["flood", str(lines)], ALLOW)
        assert handle.wait(120) == 0, "a child blocked on logging while stdout was stuck"
        elapsed = time.monotonic() - started
        assert elapsed < 60
        text = handle.log_path.read_text()
        assert text.count("\n") == lines and text.endswith(f"flood {lines - 1}\n"), "the log file must hold every line"
        assert relay.dropped > 0, "with a stuck stdout the relay must drop, not wait"
    finally:
        stream.release.set()
        runtime.close()


def test_relay_can_be_turned_off_so_only_files_are_written(paths) -> None:
    service = ProcessService(paths, child_logs="files", ready_timeout=60)
    try:
        assert service.lifecycle.relay is None
        handle = service.lifecycle.spawn("role", ROLE, ["chatty", "3"], ALLOW)
        assert wait_until(lambda: b"line 2" in handle.logs())
        handle.kill()
    finally:
        service.close()
    with pytest.raises(ValueError):
        ProcessService(paths, child_logs="journal")


# -- CLI options ----------------------------------------------------------------------------------------------------------------
@pytest.mark.parametrize("flag,value", [("--metrics-addr", "127.0.0.1"), ("--health-addr", "127.0.0.1:0"), ("--child-logs", "files")])
def test_process_only_options_are_refused_in_docker_mode(flag, value, caplog) -> None:
    from neo4j_graph_loader import cli
    with caplog.at_level("ERROR"):
        assert cli.main(["start", "--mode", "bulk", flag, value]) == 1
    assert f"{flag} applies to --runtime process only" in caplog.text


def test_metrics_listen_env_gives_an_ephemeral_port_and_the_docker_path_is_unchanged(monkeypatch) -> None:
    from neo4j_graph_loader.utils import metrics as metrics_module
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    monkeypatch.setenv(metrics_module.METRICS_LISTEN_ENV, "127.0.0.1:0")
    server = metrics_module.start_metrics_server(metrics_module.GraphLoaderMetrics(), kind="t", name="a")
    try:
        status = metrics_module.metrics_status(server)
        assert status["state"] == "listening" and status["port"] not in (0, 9464)
    finally:
        metrics_module.stop_metrics_server(server)
    monkeypatch.setenv("GRAPH_LOADER_METRICS_ENABLED", "0")
    assert metrics_module.metrics_status(metrics_module.start_metrics_server(metrics_module.GraphLoaderMetrics(), kind="t", name="a")) == {"state": "disabled"}
    monkeypatch.delenv("GRAPH_LOADER_METRICS_ENABLED", raising=False)
    monkeypatch.delenv(metrics_module.METRICS_LISTEN_ENV)
    occupied = socket.socket()
    occupied.bind(("127.0.0.1", 0))
    occupied.listen()
    try:
        failed = metrics_module.start_metrics_server(metrics_module.GraphLoaderMetrics(), kind="t", name="a", addr="127.0.0.1",
                                                     port=occupied.getsockname()[1])
        assert failed is None and metrics_module.metrics_status(failed) == {"state": "failed"}
    finally:
        occupied.close()


# -- /healthz means "the supervision loop is alive" ---------------------------------------------------------------------------------
def test_healthz_follows_the_supervision_loop_heartbeat(paths) -> None:
    from neo4j_graph_loader.orchestrator import heartbeat
    heartbeat.reset()
    service = ProcessService(paths, ready_timeout=60, health_stale_after=0.6)
    health = service.start_health("127.0.0.1:0")
    try:
        assert get(health, "/healthz")[0] == 200                              # no loop has ticked yet: starting, nothing to judge
        heartbeat.beat()
        assert get(health, "/healthz")[0] == 200
        time.sleep(1.0)                                                       # the loop stalled: no beat for longer than the threshold
        status, body = get(health, "/healthz")
        assert status == 503 and body["status"] == "unhealthy"                # the stalled-loop control
        heartbeat.beat()
        assert get(health, "/healthz")[0] == 200                              # the loop resumed
    finally:
        service.close()
        heartbeat.reset()


def test_a_loop_that_keeps_beating_stays_healthy_and_one_that_dies_turns_unhealthy(paths) -> None:
    from neo4j_graph_loader.orchestrator import heartbeat
    heartbeat.reset()
    service = ProcessService(paths, ready_timeout=60, health_stale_after=0.5)
    health = service.start_health("127.0.0.1:0")
    stop = threading.Event()

    def loop():
        while not stop.is_set():
            heartbeat.beat()
            time.sleep(0.05)

    thread = threading.Thread(target=loop)
    thread.start()
    try:
        time.sleep(0.8)
        assert get(health, "/healthz")[0] == 200                              # beating for longer than the threshold: healthy
        stop.set()
        thread.join()
        assert wait_until(lambda: get(health, "/healthz")[0] == 503, 5)        # the loop died: unhealthy within the threshold
    finally:
        stop.set()
        service.close()
        heartbeat.reset()


def test_the_real_supervision_loops_beat() -> None:
    """The stream supervisor, the monitors' wait loop and the planned phase tick the heartbeat."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    import docker_parity_support as support
    from neo4j_graph_loader.orchestrator import heartbeat
    heartbeat.reset()
    container = MagicMock()
    container.attrs = {"State": {"Status": "running"}}
    monitor = support._relationship_monitor(SimpleNamespace(), container)
    shutdown = threading.Event()
    thread = threading.Thread(target=lambda: monitor.supervise_stream(shutdown))
    thread.start()
    try:
        assert wait_until(lambda: heartbeat.age() is not None, 10), "supervise_stream never beat"
        first = heartbeat.age()
        time.sleep(0.3)
        assert heartbeat.age() < first + 0.3                                  # still ticking
    finally:
        shutdown.set()
        thread.join(10)
        monitor.close()
    heartbeat.reset()
    waiting = support._relationship_monitor(SimpleNamespace(), container)
    try:
        with pytest.raises(Exception):
            waiting._wait(0.2, "coverage", lambda: False)
        assert heartbeat.age() is not None, "the wait loop never beat"
    finally:
        waiting.close()
        heartbeat.reset()


def test_health_stale_after_is_a_process_only_option(caplog) -> None:
    from neo4j_graph_loader import cli
    with caplog.at_level("ERROR"):
        assert cli.main(["start", "--mode", "bulk", "--health-stale-after", "5"]) == 1
    assert "--health-stale-after applies to --runtime process only" in caplog.text


# -- review fix 1: every long supervision wait keeps /healthz fresh ----------------------------------------------------------------
@pytest.mark.parametrize("module_name, function", [("neo4j_graph_loader.orchestrator.bulk_monitor", "drive_label_readiness"),
                                                   ("neo4j_graph_loader.orchestrator.planned_edges", "wait_for_plan"),
                                                   ("neo4j_graph_loader.cli", "_run_completion_driven_bulk_edges")])
def test_every_long_supervision_loop_beats_the_heartbeat(module_name, function) -> None:
    """Structural guard: each of these waits is a top-level supervision loop of a bulk run and must beat (the behavioural check of the
    node-phase wait is test_bulk_monitor.test_drive_label_readiness_beats_the_heartbeat_while_it_waits)."""
    import ast
    import importlib
    import inspect
    module = importlib.import_module(module_name)
    tree = ast.parse(inspect.getsource(module))
    node = next(n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == function)
    beats = [c for c in ast.walk(node) if isinstance(c, ast.Call)
             and (getattr(c.func, "id", None) == "_beat" or getattr(c.func, "attr", None) == "beat")]
    assert beats, f"{module_name}.{function} has no heartbeat.beat()"


def test_the_main_thread_wait_for_the_dependent_phase_beats() -> None:
    import inspect
    import re
    from neo4j_graph_loader import cli
    assert re.search(r"except FuturesTimeoutError:\s+_beat\(\)", inspect.getsource(cli)), "the edge_future wait loop must beat"
