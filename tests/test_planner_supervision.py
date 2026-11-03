"""E14.F2.S3: planner worker processes under supervision (process runtime; Docker mode keeps the old behaviour).

Real spawned planner workers run a scan that never finishes (a consumer that never delivers a record).
The Linux host repeats the kill -9 test; everything here also runs on a development host.
"""
from __future__ import annotations

import json
import multiprocessing
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from neo4j_graph_loader.loader import edge_planner
from neo4j_graph_loader.loader.edge_planner import KafkaReadTask, ParallelEdgePlanner, PlanSettings, PlannerSupervision, _ProcessWorker
from neo4j_graph_loader.orchestrator import process_runtime as pr
from neo4j_graph_loader.orchestrator.planned_edges import start_kafka_edge_planner
from neo4j_graph_loader.orchestrator.process_runtime import ProcessRuntime
from neo4j_graph_loader.orchestrator.run_registry import RunRegistry
from neo4j_graph_loader.utils.schema_loader import load_schema

from tests.fixtures.e14_planner_fake import BlockingConsumerFactory

ROOT = Path(__file__).resolve().parents[1]
STANDIN = ROOT / "tests" / "fixtures" / "e14_planner_standin.py"


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def wait_gone(pids, seconds):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline and any(alive(pid) for pid in pids):
        time.sleep(0.02)
    return [pid for pid in pids if alive(pid)]


def start_standin(tmp_path, watch):
    process = subprocess.Popen([sys.executable, str(STANDIN), str(tmp_path), "1" if watch else "0"],
                               stdout=subprocess.PIPE, text=True)
    info = json.loads(process.stdout.readline())
    return process, info


def test_kill_dash_9_during_an_active_scan_stops_every_planner_worker_and_helper(tmp_path) -> None:
    cli, info = start_standin(tmp_path, watch=True)
    pids = info["workers"] + ([info["helper"]] if info["helper"] else [])
    try:
        assert len(info["workers"]) == 2 and all(alive(pid) for pid in pids)
        time.sleep(1.0)                                    # the scan is running, not just starting
        assert all(alive(pid) for pid in pids)
        os.kill(info["pid"], signal.SIGKILL)               # no handler, no cleanup: the planner is mid-scan
        killed = time.monotonic()
        cli.wait()
        survivors = wait_gone(pids, 5.0)
        assert survivors == [], "workers and the resource-tracker helper must follow the CLI"
        assert time.monotonic() - killed < 5.0
    finally:
        for pid in pids:
            if alive(pid):
                os.kill(pid, signal.SIGKILL)


def test_control_without_supervision_the_workers_are_orphaned(tmp_path) -> None:
    """Docker mode's (unchanged) default: no parent watch, so a killed CLI leaves its workers running."""
    cli, info = start_standin(tmp_path, watch=False)
    try:
        os.kill(info["pid"], signal.SIGKILL)
        cli.wait()
        time.sleep(1.5)
        assert [alive(pid) for pid in info["workers"]] == [True, True]
    finally:
        for pid in info["workers"] + ([info["helper"]] if info["helper"] else []):
            if alive(pid):
                os.kill(pid, signal.SIGKILL)


def test_workers_and_helper_are_registered_for_recovery(tmp_path) -> None:
    cli, info = start_standin(tmp_path, watch=True)
    try:
        started = [r for r in RunRegistry.read(tmp_path / "registry.jsonl") if r["kind"] == "started" and r.get("external")]
        assert sorted((r["role"], r["pid"]) for r in started) == sorted(
            [("planner-worker", pid) for pid in info["workers"]] + [("planner-helper", info["helper"])])
        assert all(r["start_time"] and r["boot_id"] for r in started)
    finally:
        os.kill(info["pid"], signal.SIGKILL)
        cli.wait()
        wait_gone(info["workers"], 5.0)


def _planner(watch, notify=None):
    schema = load_schema(ROOT / "config" / "graph_schema.yaml")
    edge = next(item for item in schema.edges if item.type == "WORKS_AT")
    tasks = [KafkaReadTask(edge, edge.topics[0], ((0, 0, 100),), "kafka:9092", "e14-test", read_timeout_seconds=3600.0)]
    return ParallelEdgePlanner(
        tasks, PlanSettings(topic="chunks", workers=1, memory_mb=64), "kafka:9092",
        types={edge.type: (edge.nodes.source, edge.nodes.target)}, plan_id="supervised",
        context=multiprocessing.get_context("spawn"), consumer_factory=BlockingConsumerFactory(),
        supervision=PlannerSupervision(parent_watch=watch, on_process=notify) if (watch or notify) else None,
    )


def test_ordered_planner_cleanup_does_not_trip_the_parent_watch(tmp_path, monkeypatch) -> None:
    """A normal close stops the workers and only then lets go of the lifeline: no worker dies of 'parent gone' (143)."""
    seen = []
    planner = _planner(True, seen.append)
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    planner.start()
    try:
        worker = planner._workers[0]
        assert worker._death_parent is not None and worker._death_child is None
        time.sleep(0.5)
        planner.close()
        assert worker._death_parent is None                          # released only after the join
        assert worker.process.exitcode not in (None, edge_planner.PARENT_GONE_EXIT_CODE)
        assert [item["role"] for item in seen][:1] == ["planner-worker"]
    finally:
        planner.close(terminate=True)


def test_supervision_is_off_by_default_so_docker_mode_is_unchanged() -> None:
    context = multiprocessing.get_context("spawn")
    worker = _ProcessWorker(context, {}, "kafka:9092", {}, None, None)
    try:
        assert worker._death_parent is None and worker._death_child is None
        assert len(worker.process._args) == 8                   # the original arguments only: no lifeline sockets
    finally:
        worker.close_endpoints()
    calls = []

    class Recorder:
        def __init__(self, *args, **kwargs):
            calls.append(kwargs)
            raise RuntimeError("stop here")

    args = MagicMock(plan_dir=None, runtime=None, plan_workers=None, plan_memory_mb=None, plan_buckets=None,
                     plan_topic=None, plan_partitions=None, plan_spill_dir=None, plan_hub_degree=None, plan_chunk_rows=None)
    schema = MagicMock()
    with pytest.raises(RuntimeError, match="stop here"):
        start_kafka_edge_planner(schema, args, run_id="r", writers=2, bootstrap_servers="k:1", planner_factory=Recorder)
    assert "supervision" not in calls[0]
    marker = PlannerSupervision(parent_watch=True)
    with pytest.raises(RuntimeError, match="stop here"):
        start_kafka_edge_planner(schema, args, run_id="r", writers=2, bootstrap_servers="k:1", planner_factory=Recorder,
                                 supervision=marker)
    assert calls[1]["supervision"] is marker


# -- ordering against the process groups the runtime signals -----------------------------------------------------------------
ROLE = "script:" + str(ROOT / "tests" / "fixtures" / "e14_role.py")
ALLOW = {"GRAPH_LOADER_CHILD_ALLOW_SCRIPT": "1"}


def test_pre_signal_hooks_run_once_before_any_group_signal(tmp_path, monkeypatch) -> None:
    runtime = ProcessRuntime(tmp_path / "run", ready_timeout=30)
    events = []
    runtime.add_pre_signal_hook(lambda: events.append("planner-cleanup"))
    real_killpg = os.killpg
    monkeypatch.setattr(pr.os, "killpg", lambda pid, sig: (events.append("killpg"), real_killpg(pid, sig))[1])
    try:
        first = runtime.spawn("role", ROLE, ["ignore-term"], ALLOW)
        second = runtime.spawn("role", ROLE, ["ignore-term"], ALLOW)
        first.remove(force=True)
        second.remove(force=True)
        assert events == ["planner-cleanup", "killpg", "killpg"]       # once, and first
    finally:
        runtime.close()


def test_closing_the_runtime_runs_the_hooks_before_killing_anything(tmp_path) -> None:
    runtime = ProcessRuntime(tmp_path / "run", ready_timeout=30)
    handle = runtime.spawn("role", ROLE, ["ignore-term"], ALLOW)
    order = []
    runtime.add_pre_signal_hook(lambda: order.append(("hook", handle.status)))
    runtime.close()
    assert order == [("hook", "running")]                               # the child was still alive when the hook ran
    assert handle.wait(10) == 137


def test_a_failing_hook_does_not_block_the_signalling(tmp_path) -> None:
    runtime = ProcessRuntime(tmp_path / "run", ready_timeout=30)
    runtime.add_pre_signal_hook(lambda: (_ for _ in ()).throw(RuntimeError("cleanup failed")))
    handle = runtime.spawn("role", ROLE, ["ignore-term"], ALLOW)
    handle.remove(force=True)
    assert handle.wait(10) == 137
    runtime.close()
