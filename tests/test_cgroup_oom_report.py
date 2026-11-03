"""E14.F3.S8: the process runtime reports a cgroup OOM kill at group level and never names a victim."""
from __future__ import annotations

import signal
from pathlib import Path

import pytest

from neo4j_graph_loader.orchestrator.cgroup_budget import OOM_REPORT, OomWatch
from neo4j_graph_loader.orchestrator.process_runtime import ProcessRuntime
from neo4j_graph_loader.orchestrator.run_registry import RunRegistry

FIXTURES = Path(__file__).resolve().parent / "fixtures"
ROLE = "script:" + str(FIXTURES / "e14_role.py")
ALLOW = {"GRAPH_LOADER_CHILD_ALLOW_SCRIPT": "1"}


def cgroup(tmp_path, oom_kill):
    root = tmp_path / "cg"
    (root / "svc").mkdir(parents=True, exist_ok=True)
    (tmp_path / "proc_cgroup").write_text("0::/svc\n")
    events = root / "svc" / "memory.events"
    events.write_text(f"low 0\nhigh 0\nmax 3\noom 1\noom_kill {oom_kill}\n")
    return root, tmp_path / "proc_cgroup", events


def test_watch_reports_each_rise_once(tmp_path) -> None:
    root, proc, events = cgroup(tmp_path, 2)
    watch = OomWatch(root=root, proc_cgroup=proc)
    assert watch.rose() is None
    events.write_text("oom_kill 3\n")
    assert watch.rose() == (2, 3) and watch.rose() is None
    events.write_text("oom_kill 5\n")
    assert watch.rose() == (3, 5)


@pytest.mark.parametrize("setup", ["missing-file", "no-cgroup", "garbage"])
def test_watch_is_silent_without_a_readable_counter(tmp_path, setup) -> None:
    root, proc, events = cgroup(tmp_path, 0)
    if setup == "missing-file":
        events.unlink()
    elif setup == "no-cgroup":
        proc.write_text("")
    else:
        events.write_text("oom_kill many\n")
    assert OomWatch(root=root, proc_cgroup=proc).rose() is None


class Counter:
    def __init__(self):
        self.value = 0
        self.watch = None

    def rose(self):
        return self.watch.rose()


def test_sigkill_exit_after_a_counter_rise_is_reported_without_a_victim(tmp_path, capfd) -> None:
    root, proc, events = cgroup(tmp_path, 0)
    runtime = ProcessRuntime(tmp_path / "run", ready_timeout=30, lifeline_deadline=5, oom_watch=OomWatch(root=root, proc_cgroup=proc))
    try:
        quiet = runtime.spawn("role", ROLE, ["ignore-term"], dict(ALLOW))
        quiet.kill()                                            # a manual SIGKILL while the counter has not moved
        assert quiet.wait(10) == 137
        assert OOM_REPORT not in capfd.readouterr().err
        first = runtime.spawn("role", ROLE, ["ignore-term"], dict(ALLOW))
        second = runtime.spawn("role", ROLE, ["ignore-term"], dict(ALLOW))
        events.write_text("oom_kill 1\n")                       # the kernel kills one process of the group...
        first.kill()
        second.kill()                                            # ...and a manual kill lands with it
        assert first.wait(10) == 137 and second.wait(10) == 137
        err = capfd.readouterr().err
    finally:
        runtime.close()
    lines = [line for line in err.splitlines() if "stage=oom" in line]
    assert len(lines) == 1 and OOM_REPORT in lines[0] and "oom_kill 0 -> 1" in lines[0], err
    assert not any(name in lines[0] for name in (first.name, second.name, quiet.name))
    assert first.attrs["State"]["OOMKilled"] is None and second.attrs["State"]["OOMKilled"] is None
    records = RunRegistry.read(tmp_path / "run" / "registry.jsonl")
    assert [(r["oom_kill_before"], r["oom_kill_after"]) for r in records if r["kind"] == "oom"] == [(0, 1)]


def test_a_clean_exit_or_sigterm_never_reads_the_counter(tmp_path) -> None:
    class Boom:
        def rose(self):
            raise AssertionError("only a SIGKILL exit consults the counter")
    runtime = ProcessRuntime(tmp_path / "run", ready_timeout=30, lifeline_deadline=5, oom_watch=Boom(), oom_poll_seconds=0)
    try:
        handle = runtime.spawn("role", ROLE, ["exit-after-ready", "0"], dict(ALLOW))
        assert handle.wait(20) == 0
        other = runtime.spawn("role", ROLE, ["ready", str(tmp_path / "m")], dict(ALLOW))
        other.stop(timeout=5)
    finally:
        runtime.close()
    assert signal.SIGKILL == 9


def test_a_kill_of_a_process_the_runtime_did_not_spawn_is_reported_within_a_poll_period(tmp_path, capfd) -> None:
    """Review fix 3: planner workers are recorded externals; nothing reaps them through a handle, so a role-exit hook never sees their
    OOM kill. The runtime watches the counter itself."""
    import time
    root, proc, events = cgroup(tmp_path, 0)
    runtime = ProcessRuntime(tmp_path / "run", ready_timeout=30, lifeline_deadline=5, oom_watch=OomWatch(root=root, proc_cgroup=proc),
                             oom_poll_seconds=0.1)
    try:
        events.write_text("oom_kill 1\n")                     # a planner worker is OOM-killed; no role ever exits
        seen = ""
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and OOM_REPORT not in seen:
            seen += capfd.readouterr().err
            time.sleep(0.05)
        time.sleep(0.5)                                          # and it is reported once, not on every poll
        seen += capfd.readouterr().err
    finally:
        runtime.close()
    seen += capfd.readouterr().err
    records = RunRegistry.read(tmp_path / "run" / "registry.jsonl")
    assert [(r["oom_kill_before"], r["oom_kill_after"]) for r in records if r["kind"] == "oom"] == [(0, 1)], (seen, records)
    assert seen.count(OOM_REPORT) == 1 and "oom_kill 0 -> 1" in seen, seen


def test_a_rise_after_the_last_poll_is_reported_when_the_run_ends(tmp_path, capfd) -> None:
    root, proc, events = cgroup(tmp_path, 0)
    runtime = ProcessRuntime(tmp_path / "run", ready_timeout=30, lifeline_deadline=5, oom_watch=OomWatch(root=root, proc_cgroup=proc),
                             oom_poll_seconds=3600)
    events.write_text("oom_kill 2\n")
    runtime.close()
    err = capfd.readouterr().err
    assert err.count(OOM_REPORT) == 1 and "oom_kill 0 -> 2" in err, err
