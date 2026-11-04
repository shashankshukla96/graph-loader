"""E15.F2.S12: regressions for what the live matrix found (a stream SIGTERM recorded as ``succeeded``; a loader that dies before it
is ready, or an auto-removed stream node loader, never recorded as a failed role).

The unit forms of the three findings; the live cells are the proof that they happen in a real run.
"""
from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from neo4j_graph_loader import _runtime, cli
from neo4j_graph_loader.orchestrator.process_runtime import LaunchFailed
from neo4j_graph_loader.orchestrator.process_service import ProcessService
from neo4j_graph_loader.status import StatusTracker

SOURCE = Path(cli.__file__).read_text()


def test_no_stream_supervisor_replaces_the_stop_handler_by_a_bare_event_set() -> None:
    """The three stream supervisors used to install ``lambda: shutdown.set()``, which dropped the status stop flag: a SIGTERM stop
    of a stream run then ended ``succeeded`` instead of ``stopped``."""
    assert "lambda *_args: shutdown.set()" not in SOURCE
    assert SOURCE.count("_stop_handler(shutdown, _cli_execution)") == 6


def test_the_installed_handlers_remember_the_execution_for_the_supervisors_and_forget_it_after() -> None:
    tracker = StatusTracker(mode="stream", runtime="process")
    cleanups: list = []
    shutdown = cli._install_shutdown_handlers(cleanups, tracker)
    try:
        assert cli._cli_execution is tracker
        cli._stop_handler(shutdown, cli._cli_execution)()
        assert shutdown.is_set() and tracker.stop_requested
    finally:
        for cleanup in reversed(cleanups):
            cleanup()
    assert cli._cli_execution is None
    assert tracker.finish(0) == "stopped"


class DeadContainer:
    name, id = "graph-loader-node-Employer-0", "c" * 64


def test_the_node_loader_watcher_tells_the_status_which_container_died(monkeypatch) -> None:
    from threading import Event
    monkeypatch.setattr(cli, "_is_running", lambda container: False)
    monkeypatch.setattr(cli, "_stop_initiated_by_cli", lambda container: False)
    seen = []
    shutdown = Event()
    watcher = cli._NodeLoaderWatcher([DeadContainer()], shutdown, interval=0.01, on_exit=seen.append).start()
    assert shutdown.wait(5)
    watcher.stop()
    assert watcher.failed and [c.name for c in seen] == ["graph-loader-node-Employer-0"]


def test_a_dead_stream_node_loader_becomes_a_failed_role_and_the_execution_names_it() -> None:
    tracker = StatusTracker(mode="stream", runtime="docker")
    container = DeadContainer()
    incarnation = tracker.role_started("node-loader", "Employer", None, container_id=container.id)
    args = SimpleNamespace(execution=tracker, runtime_backend="docker", _docker_service=SimpleNamespace(status_handles=lambda: {incarnation: container}))
    record = cli._container_exit_recorder(args)
    record(container)
    (role,) = tracker.snapshot()["roles"]
    assert role["state"] == "failed" and role["exit_reason"] == "exited while the run was active" and "exit code is unavailable" in role["last_error"]["message"]
    assert tracker.finish(1) == "failed"
    assert tracker.snapshot()["outcome"]["reason"] == "role failure"


def test_the_recorder_is_off_without_a_tracker_and_in_the_process_runtime() -> None:
    assert cli._container_exit_recorder(SimpleNamespace(execution=None, runtime_backend="docker")) is None
    assert cli._container_exit_recorder(SimpleNamespace(execution=StatusTracker(runtime="process"), runtime_backend="process")) is None


@pytest.fixture
def paths(tmp_path):
    root = tmp_path / "attempt"
    for name in ("rejections", "plans", "spill", "timings"):
        (root / name).mkdir(parents=True)
    schema = root / "runtime_schema.yaml"
    schema.write_text("x: 1\n")
    return _runtime.RunPaths(root, schema, root / "rejections", root / "plans", root / "spill", root / "timings")


def test_a_process_role_that_dies_before_it_is_ready_is_a_recorded_failure(paths, monkeypatch) -> None:
    service = ProcessService(paths, ready_timeout=5)
    tracker = StatusTracker(mode="bulk", runtime="process")
    service.status = tracker
    try:
        def fail(*args, **kwargs):
            raise LaunchFailed("node-loader exited with code -9 before it was ready")
        monkeypatch.setattr(service.lifecycle, "spawn", fail)
        with pytest.raises(LaunchFailed):
            service.run_bucket_lease_server(run_id="run-x", bucket_count=16, network="n", keyed_labels=[])
        (role,) = tracker.snapshot()["roles"]
        assert role["role"] == "bucket-lease-server" and role["state"] == "failed" and role["exit_reason"] == "launch failed"
        assert re.search(r"before it was ready", role["last_error"]["message"])
        assert tracker.finish(1) == "failed" and tracker.snapshot()["outcome"]["reason"] == "role failure"
    finally:
        service.close()


# -- the stream's internal run id is the execution's run id (orchestrator decision on the S12 file_sd red) ------------------------------
def _run_stream(tmp_path, *flags, schema=None):
    import os
    from unittest.mock import patch
    import docker_parity_support as support
    from neo4j_graph_loader.status import read_status
    support._reset_module_state()
    tmp_path.mkdir(parents=True, exist_ok=True)
    context = support.Context(tmp_path)
    argv = ["start", "--run-dir", str(tmp_path / "runs"), "--mode", "stream", *flags, *(["--config", str(schema)] if schema else [])]
    previous = os.getcwd()
    os.chdir(tmp_path)
    try:
        with patch.dict(os.environ, {"PATH": os.environ.get("PATH", ""), "HOME": os.environ.get("HOME", ""), "GRAPH_LOADER_METRICS_ENABLED": "0"}, clear=True):
            code = context.main(argv)
    finally:
        os.chdir(previous)
    (attempt,) = (tmp_path / "runs").glob("graph-loader-*")
    launches = [event["args"] for event in context.recorder.events if event["op"] == "containers.run"]
    return code, read_status(attempt), launches


def test_a_stream_with_a_fleet_and_no_run_id_reports_the_fleets_run_id_to_the_status(tmp_path) -> None:
    code, status, launches = _run_stream(tmp_path)
    assert code == 0
    run_ids = {launch["labels"]["run_id"] for launch in launches if (launch.get("labels") or {}).get("run_id")}
    assert len(run_ids) == 1 and status["run_id"] in run_ids and re.fullmatch(r"[0-9a-f]{32}", status["run_id"])


def test_an_explicit_run_id_is_untouched_and_a_node_only_stream_stays_null(tmp_path) -> None:
    code, status, launches = _run_stream(tmp_path / "explicit", "--run-id", "mine-1")
    assert code == 0 and status["run_id"] == "mine-1"
    schema = tmp_path / "nodes.yaml"
    schema.write_text("loading: {consumer_group_id: g}\nnodes:\n  - label: Worker\n    topic: w\n    key_property: id\n    properties:\n      id: {type: integer, required: true}\n")
    code, status, launches = _run_stream(tmp_path / "nodes", schema=schema)
    assert status["run_id"] is None


# -- the discovery file of a finished run (orchestrator-approved fix of the live false TargetDown) -------------------------------------------
def test_targets_json_lists_the_cli_during_the_run_is_empty_after_an_orderly_end_and_stays_empty(paths) -> None:
    import json
    from neo4j_graph_loader.orchestrator.process_runtime import ProcessRuntime
    runtime = ProcessRuntime(Path(paths.root), ready_timeout=5)
    runtime.cli_metrics_port = 9464
    runtime._targets_changed()
    path = runtime.targets_path
    assert [g["labels"]["component"] for g in json.loads(path.read_text())] == ["cli"]
    runtime.close()
    assert json.loads(path.read_text()) == []
    runtime._targets_changed()                                   # a late exit callback must not repopulate it
    assert json.loads(path.read_text()) == [] and runtime.cli_metrics_port is None


def test_a_killed_cli_leaves_its_targets_so_a_dead_cli_still_alerts(tmp_path) -> None:
    import json
    import os
    import signal
    import subprocess
    import sys
    import time
    root = tmp_path / "attempt"
    code = ("import sys, time\nfrom pathlib import Path\nfrom neo4j_graph_loader.orchestrator.process_runtime import ProcessRuntime\n"
            "r = ProcessRuntime(Path(sys.argv[1]), ready_timeout=5)\nr.cli_metrics_port = 9464\nr._targets_changed()\nprint('ready', flush=True)\ntime.sleep(60)\n")
    child = subprocess.Popen([sys.executable, "-c", code, str(root)], stdout=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == "ready"
        os.kill(child.pid, signal.SIGKILL)
        child.wait(10)
        time.sleep(0.2)
        assert [g["targets"] for g in json.loads((root / "metrics" / "targets.json").read_text())] == [["127.0.0.1:9464"]]
    finally:
        if child.poll() is None:
            child.kill()

