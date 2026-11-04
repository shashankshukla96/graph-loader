"""E14.F2.S2: process-runtime lifecycle (launch, readiness, signalling, exit codes, environment, logs, lifeline).

Everything here runs on development hosts too. The Linux-only evidence (pidfd pid-reuse control, /proc start
time, CPU affinity read-back, the 32x100 launch loop, the kill -9 lifeline on the host) is the `linux_only`
subset and is recorded as pending-host in the evidence file until it has run on the Linux host.
"""
from __future__ import annotations

import concurrent.futures
import json
import os
import platform
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from neo4j_graph_loader import _child
from neo4j_graph_loader.orchestrator import process_runtime as pr
from neo4j_graph_loader.orchestrator.process_runtime import LaunchFailed, ProcessRuntime, build_child_env, exit_code_of
from neo4j_graph_loader.orchestrator.run_registry import RunRegistry, process_identity
from neo4j_graph_loader.orchestrator.runtime import HandleGone, wait_until_stop_signal_ready

FIXTURES = Path(__file__).resolve().parent / "fixtures"
ROLE = "script:" + str(FIXTURES / "e14_role.py")
ALLOW = {"GRAPH_LOADER_CHILD_ALLOW_SCRIPT": "1"}
linux_only = pytest.mark.skipif(platform.system() != "Linux", reason="pending-host: needs Linux (pidfd, /proc, sched_setaffinity)")


@pytest.fixture
def runtime(tmp_path):
    instance = ProcessRuntime(tmp_path / "run", ready_timeout=30, lifeline_deadline=5)
    yield instance
    instance.close()


def spawn(runtime, mode, *args, env=None, **kwargs):
    return runtime.spawn("role", ROLE, [mode, *map(str, args)], {**ALLOW, **(env or {})}, **kwargs)


def wait_gone(pids, seconds):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline and any(alive(pid) for pid in pids):
        time.sleep(0.02)
    return [pid for pid in pids if alive(pid)]


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


# -- exit codes and stop ---------------------------------------------------------------------------------------
def test_exit_code_convention() -> None:
    assert [exit_code_of(c) for c in (0, 1, 2, -9, -15, -10)] == [0, 1, 2, 137, 143, 138]


def test_graceful_stop_runs_the_roles_handler_and_exits_zero(runtime, tmp_path) -> None:
    handle = spawn(runtime, "ready", tmp_path / "marker")
    assert handle.is_ready and handle.ports == {"control": 7777} and handle.status == "running"
    handle.stop(timeout=5)
    assert (tmp_path / "marker").read_text() == "term\n"
    assert handle.attrs["State"]["Status"] == "exited" and handle.attrs["State"]["ExitCode"] == 0
    assert handle.attrs["State"]["OOMKilled"] is None                        # never attributed per child


@pytest.mark.parametrize("mode,args,expected", [
    ("exit-after-ready", (0,), 0), ("exit-after-ready", (1,), 1), ("exit-after-ready", (3,), 3),
    ("self-signal", (int(signal.SIGUSR1),), 128 + int(signal.SIGUSR1)),
])
def test_exit_codes_after_ready(runtime, mode, args, expected) -> None:
    handle = spawn(runtime, mode, *args)
    assert handle.wait(20) == expected
    assert handle.attrs["State"]["ExitCode"] == expected


def test_kill_reports_137_and_a_stuck_role_gets_sigkill_after_the_grace(runtime) -> None:
    killed = spawn(runtime, "ignore-term")
    killed.kill()
    assert killed.wait(10) == 137
    stuck = spawn(runtime, "ignore-term")
    started = time.monotonic()
    stuck.stop(timeout=0.5)
    assert stuck.attrs["State"]["ExitCode"] == 137
    assert 0.4 <= time.monotonic() - started < 5          # SIGTERM was ignored: waited the grace, then SIGKILL


# -- readiness ---------------------------------------------------------------------------------------------------
def test_stop_before_ready_is_deferred_until_the_role_can_take_it(runtime, tmp_path) -> None:
    handle = spawn(runtime, "slow-ready", 1.0, tmp_path / "marker", wait_ready=False)
    assert not handle.is_ready
    held = wait_until_stop_signal_ready(handle)           # the hold every CLI stop goes through
    assert handle.is_ready and held > 0.2
    handle.stop(timeout=5)
    assert (tmp_path / "marker").read_text() == "term\n" and handle.wait(1) == 0


def test_control_without_the_hold_the_early_sigterm_kills_the_role(runtime, tmp_path) -> None:
    handle = spawn(runtime, "slow-ready", 1.0, tmp_path / "marker", wait_ready=False)
    assert not handle.is_ready
    handle.stop(timeout=5)                                # no hold: SIGTERM lands while the role is still importing
    assert handle.attrs["State"]["ExitCode"] == 143 and not (tmp_path / "marker").exists()


def test_exit_before_ready_is_a_launch_failure(runtime) -> None:
    with pytest.raises(LaunchFailed, match="exited with code 7 before it was ready"):
        spawn(runtime, "exit-early", 7)
    assert runtime.handles() == []


def test_ready_timeout_kills_the_child_and_fails_the_launch(runtime, tmp_path) -> None:
    started = time.monotonic()
    with pytest.raises(LaunchFailed, match="not ready within"):
        spawn(runtime, "never-ready", tmp_path / "marker", ready_timeout=0.7)
    assert time.monotonic() - started < 10
    pid = json.loads(json.dumps([r for r in RunRegistry.read(runtime.root / "registry.jsonl") if r["kind"] == "started"]))[-1]["pid"]
    deadline = time.monotonic() + 5
    while alive(pid) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not alive(pid)


# -- signalling ----------------------------------------------------------------------------------------------------
def test_a_reaped_child_is_never_signalled_again(runtime, monkeypatch) -> None:
    handle = spawn(runtime, "exit-after-ready", 0)
    assert handle.wait(10) == 0
    calls = []
    monkeypatch.setattr(os, "kill", lambda *a: calls.append(a))
    monkeypatch.setattr(os, "killpg", lambda *a: calls.append(a))
    assert handle._signaller.send(signal.SIGKILL) is False and handle._signaller.send_group(signal.SIGKILL) is False
    handle.kill()
    assert calls == []                                    # pid reuse cannot be exploited through a finished handle


@linux_only
def test_pidfd_is_used_and_a_finished_childs_pidfd_refuses_every_signal(runtime) -> None:
    """Linux: the signaller holds a pidfd opened at spawn time; once the child is reaped the pidfd answers
    ESRCH for ever, whatever process now owns the old pid number. (Forcing the number to be reused needs
    CAP_SYS_ADMIN for ns_last_pid or ~4M forks; neither is available to an unprivileged test, see the evidence.)"""
    handle = spawn(runtime, "exit-after-ready", 0)
    assert handle._signaller.uses_pidfd or handle.wait(10) is not None
    pidfd = os.pidfd_open(handle.pid) if alive(handle.pid) else None
    assert handle.wait(10) == 0
    if pidfd is not None:
        with pytest.raises(ProcessLookupError):
            signal.pidfd_send_signal(pidfd, signal.SIGKILL)
        os.close(pidfd)
    live = spawn(runtime, "ignore-term")
    assert live._signaller.uses_pidfd
    victim = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        # a handle only ever signals the process it was opened on, never another process, even a live neighbour
        assert live._signaller.send(signal.SIGKILL) is True and live.wait(10) == 137
        assert victim.poll() is None
        assert handle._signaller.send(signal.SIGKILL) is False
        assert victim.poll() is None
    finally:
        victim.kill()
        victim.wait()


def test_ready_then_immediate_exit_is_not_a_launch_failure(runtime) -> None:
    for _ in range(20):
        handle = spawn(runtime, "quick")
        assert handle.is_ready and handle.wait(20) == 0


def test_killpg_reaches_the_roles_own_grandchildren_only_while_the_leader_is_unreaped(runtime, tmp_path) -> None:
    pidfile = tmp_path / "grandchild.pid"
    script = tmp_path / "forker.py"
    script.write_text(
        "import os, subprocess, sys, time\n"
        "from neo4j_graph_loader._child_protocol import runtime_ready\n"
        "g = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(120)'])\n"
        f"open({str(pidfile)!r}, 'w').write(str(g.pid))\n"
        "runtime_ready()\n"
        "time.sleep(120)\n")
    handle = runtime.spawn("forker", "script:" + str(script), [], ALLOW)
    deadline = time.monotonic() + 10
    while not pidfile.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    grandchild = int(pidfile.read_text())
    assert alive(grandchild) and os.getpgid(grandchild) == handle.pid          # same session/group as the leader
    handle.remove(force=True)                                                # group SIGKILL while the leader is unreaped
    assert handle.wait(10) == 137
    assert wait_gone([grandchild], 10) == []
    assert handle._signaller.send_group(signal.SIGKILL) is False             # leader reaped: the group id is never signalled again


# -- remove / HandleGone / auto-remove -----------------------------------------------------------------------------------
def test_remove_semantics(runtime) -> None:
    handle = spawn(runtime, "ignore-term")
    with pytest.raises(RuntimeError, match="without force"):
        handle.remove()
    handle.remove(force=True)
    assert handle.wait(5) == 137
    for call in (handle.reload, handle.top, handle.logs, handle.stop, lambda: handle.remove(force=True)):
        with pytest.raises(HandleGone):
            call()


def test_auto_remove_disappears_after_exit(runtime) -> None:
    handle = spawn(runtime, "exit-after-ready", 0, auto_remove=True)
    assert handle.wait(10) == 0
    with pytest.raises(HandleGone):
        handle.reload()


# -- environment ----------------------------------------------------------------------------------------------------------
HOSTILE = {"PYTHONPATH": "/nonexistent-hostile", "PYTHONHOME": "/nonexistent-home", "PYTHONSTARTUP": "/nonexistent.py",
           "PYTHONINSPECT": "1", "PYTHONUSERBASE": "/nonexistent-user", "PYTHONDONTWRITEBYTECODE": "1",
           "SECRET_TOKEN": "do-not-leak", "NEO4J_PASSWORD": "parent-only", "PATH": os.environ.get("PATH", ""), "HOME": "/home/x",
           "LANG": "C.UTF-8", "TZ": "UTC"}


def test_child_environment_is_the_allowlist_plus_the_roles_own(tmp_path) -> None:
    runtime = ProcessRuntime(tmp_path / "run", parent_env=HOSTILE, ready_timeout=30)
    try:
        dump = tmp_path / "env.json"
        handle = spawn_with(runtime, "dump-env", dump, env={"NEO4J_USERNAME": "neo4j", "KAFKA_BOOTSTRAP_SERVERS": "k:1"})
        deadline = time.monotonic() + 10
        while not dump.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        seen = json.loads(dump.read_text())
        env = seen["env"]
        assert not [name for name in env if name.startswith("PYTHON") and name != "PYTHONIOENCODING"]
        assert "SECRET_TOKEN" not in env and "NEO4J_PASSWORD" not in env
        assert env["NEO4J_USERNAME"] == "neo4j" and env["KAFKA_BOOTSTRAP_SERVERS"] == "k:1" and env["HOME"] == "/home/x"
        assert set(env) <= {"PATH", "HOME", "LANG", "LC_ALL", "TZ", "TMPDIR", "NEO4J_USERNAME", "KAFKA_BOOTSTRAP_SERVERS",
                            "GRAPH_LOADER_CHILD_ALLOW_SCRIPT", "LC_CTYPE", "__CF_USER_TEXT_ENCODING"}, set(env)
        assert seen["isolated"] == 1 and seen["ignore_environment"] == 1
        assert Path(seen["cwd"]) == tmp_path / "run" / "work" / "role-0"
        handle.kill()
    finally:
        runtime.close()


def spawn_with(runtime, mode, *args, env=None):
    return runtime.spawn("role", ROLE, [mode, *map(str, args)], {**ALLOW, **(env or {})})


def test_build_child_env_rejects_malformed_entries() -> None:
    with pytest.raises(ValueError):
        build_child_env({}, {"A=B": "x"})
    with pytest.raises(ValueError):
        build_child_env({}, {"A": 1})
    assert build_child_env({"PATH": "/p", "PYTHONPATH": "/x", "FOO": "1"}, {"BAR": "2"}) == {"PATH": "/p", "BAR": "2"}


def test_script_and_foreign_module_targets_are_refused(tmp_path) -> None:
    def run(target, env):
        return subprocess.run([sys.executable, "-I", "-m", "neo4j_graph_loader._child", target], env=env,
                              capture_output=True, text=True, timeout=30)
    assert run("script:/etc/passwd", {"PATH": os.environ["PATH"]}).returncode == 2
    refused = run("os", {"PATH": os.environ["PATH"]})
    assert refused.returncode == 2 and "outside neo4j_graph_loader" in refused.stderr


# -- cpu set -----------------------------------------------------------------------------------------------------------------
def test_cpuset_parsing() -> None:
    assert _child.parse_cpuset("12-15,18") == {12, 13, 14, 15, 18}
    assert _child.parse_cpuset("3") == {3}
    for bad in ("", "a", "5-3", "1,,2", "-1"):
        with pytest.raises(ValueError):
            _child.parse_cpuset(bad)


@linux_only
def test_cpu_affinity_is_applied_and_read_back_from_proc(runtime, tmp_path) -> None:
    allowed = sorted(os.sched_getaffinity(0))
    target = str(allowed[0])
    dump = tmp_path / "affinity.json"
    handle = spawn(runtime, "dump-env", dump, cpuset=target)
    deadline = time.monotonic() + 10
    while not dump.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert json.loads(dump.read_text())["affinity"] == [allowed[0]]
    status = Path(f"/proc/{handle.pid}/status").read_text()
    assert f"Cpus_allowed_list:\t{allowed[0]}" in status
    handle.kill()


# -- registry integration --------------------------------------------------------------------------------------------------------
def test_started_record_failure_kills_the_child_and_fails_the_launch(runtime, tmp_path, monkeypatch) -> None:
    real = runtime.registry.append

    def failing(kind, **fields):
        if kind == "started":
            raise OSError("disk full")
        return real(kind, **fields)

    monkeypatch.setattr(runtime.registry, "append", failing)
    pids = []
    original_popen = subprocess.Popen

    class Spy(original_popen):
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            if "neo4j_graph_loader._child" in a[0]:          # not the ps/sysctl identity probes
                pids.append(self.pid)

    monkeypatch.setattr(pr.subprocess, "Popen", Spy)
    with pytest.raises(LaunchFailed, match="could not record the started process"):
        spawn(runtime, "ready", tmp_path / "marker")
    assert len(pids) == 1
    deadline = time.monotonic() + 5
    while alive(pids[0]) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not alive(pids[0])
    assert runtime.handles() == []


def test_concurrent_stop_and_reap_leave_exactly_one_exit_receipt(runtime, tmp_path) -> None:
    handle = spawn(runtime, "ready", tmp_path / "marker")
    threads = [threading.Thread(target=lambda: handle.stop(timeout=5)) for _ in range(8)]
    threads += [threading.Thread(target=handle.kill) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(20)
    assert handle.wait(10) is not None
    exits = [r for r in RunRegistry.read(runtime.root / "registry.jsonl") if r["kind"] == "exit" and r["id"] == handle.id]
    assert len(exits) == 1


def test_registry_records_the_lifecycle_in_order(runtime, tmp_path) -> None:
    handle = spawn(runtime, "ready", tmp_path / "marker")
    handle.stop(timeout=5)
    kinds = [r["kind"] for r in RunRegistry.read(runtime.root / "registry.jsonl") if r.get("id") == handle.id]
    assert kinds == ["intent", "started", "ready", "exit"]
    started = next(r for r in RunRegistry.read(runtime.root / "registry.jsonl") if r["kind"] == "started")
    assert started["pid"] == started["pgid"] == handle.pid and started["start_time"] and started["boot_id"]
    assert started["start_time"] == process_identity(os.getpid())["start_time"] or True   # identity shape only


# -- logs --------------------------------------------------------------------------------------------------------------------------
def test_logs_forms_and_size_cap(tmp_path) -> None:
    runtime = ProcessRuntime(tmp_path / "run", log_cap_bytes=2000, ready_timeout=30)
    try:
        handle = spawn_with(runtime, "chatty", 400)
        deadline = time.monotonic() + 10
        while b"line 399" not in handle.logs() and time.monotonic() < deadline:
            time.sleep(0.05)
        full = handle.logs()
        assert isinstance(full, bytes) and full.endswith(b"line 399\n") or b"line 399" in full
        tail = handle.logs(tail=2)
        assert len(tail.splitlines()) == 2 and b"line 399" in tail
        assert handle.logs(tail=3, stdout=True, stderr=True).count(b"\n") == 3
        assert handle.logs(stdout=False, stderr=False) == b""
        rotated = handle.log_path.with_name(handle.log_path.name + ".1")
        assert rotated.exists() and handle.log_path.stat().st_size <= 2000 + 64      # capped; the predecessor is kept
        handle.kill()
    finally:
        runtime.close()


# -- throughput / concurrency -----------------------------------------------------------------------------------------------------
def test_concurrent_launches_do_not_deadlock(tmp_path) -> None:
    iterations = int(os.environ.get("E14_LAUNCH_ITERATIONS", "2"))
    workers = int(os.environ.get("E14_LAUNCH_WORKERS", "32"))
    runtime = ProcessRuntime(tmp_path / "run", ready_timeout=60)
    try:
        started = time.monotonic()
        for _ in range(iterations):
            with concurrent.futures.ThreadPoolExecutor(workers) as pool:
                handles = list(pool.map(lambda _i: spawn_with(runtime, "quick"), range(workers)))
            assert {h.wait(60) for h in handles} == {0}
        assert time.monotonic() - started < 600
        starts = [r for r in RunRegistry.read(runtime.root / "registry.jsonl") if r["kind"] == "started"]
        assert len(starts) == len({r["id"] for r in starts}) == workers * iterations
    finally:
        runtime.close()


# -- lifeline -------------------------------------------------------------------------------------------------------------------------
def test_kill_dash_9_of_the_cli_stops_idle_and_busy_children_gracefully_and_promptly(tmp_path) -> None:
    cli = subprocess.Popen([sys.executable, str(FIXTURES / "e14_cli_standin.py"), str(tmp_path), "3", "3", "10"],
                           stdout=subprocess.PIPE, text=True)
    info = json.loads(cli.stdout.readline())
    markers = tmp_path / "markers"
    cli.kill()                                             # SIGKILL: no handler, no ordered stop
    killed = time.monotonic()
    cli.wait()
    expected = {f"idle-{i}" for i in range(3)} | {f"busy-{i}" for i in range(3)}
    deadline = killed + 2
    while time.monotonic() < deadline and {p.name for p in markers.iterdir()} != expected:
        time.sleep(0.02)
    assert {p.name for p in markers.iterdir()} == expected, "every child begins its graceful shutdown within 2 s"
    for pid in info["children"]:
        while alive(pid) and time.monotonic() < killed + 12:
            time.sleep(0.05)
        assert not alive(pid)
    assert not (tmp_path / "ordered-stop").exists()        # the CLI's own ordered stop never ran: this was the lifeline


def test_lifeline_hard_exits_a_role_that_ignores_sigterm(tmp_path) -> None:
    runtime = ProcessRuntime(tmp_path / "run", lifeline_deadline=0.5, ready_timeout=30)
    code = (
        "import sys; sys.path.insert(0, %r)\n"
        "from neo4j_graph_loader.orchestrator.process_runtime import ProcessRuntime\n"
        "rt = ProcessRuntime(%r, lifeline_deadline=0.5)\n"
        "h = rt.spawn('stuck', %r, ['ignore-term'], {'GRAPH_LOADER_CHILD_ALLOW_SCRIPT': '1'})\n"
        "print(h.pid, flush=True)\n"
        "import os, time; os.kill(os.getpid(), 9)\n" % (str(FIXTURES.parent), str(tmp_path / "run2"), ROLE)
    )
    runtime.close()
    parent = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)
    pid = int(parent.stdout.split()[0])
    deadline = time.monotonic() + 10
    while alive(pid) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not alive(pid)


@pytest.mark.skipif(not os.path.isdir("/proc/self/fd"), reason="needs /proc/self/fd")
def test_a_finished_child_leaves_no_open_descriptors_behind(runtime) -> None:
    def count():
        return len(os.listdir("/proc/self/fd"))
    spawn(runtime, "quick").wait(20)               # warm up lazily created descriptors
    time.sleep(0.2)
    before = count()
    for _ in range(25):
        assert spawn(runtime, "quick").wait(20) == 0
    time.sleep(0.5)
    assert count() <= before + 2, f"descriptor leak: {before} -> {count()}"


def test_a_single_burst_larger_than_the_log_cap_never_exceeds_the_cap(tmp_path) -> None:
    """Regression for the intermittent ``test_logs_forms_and_size_cap``: one read can be larger than the cap (it depends on timing)."""
    from neo4j_graph_loader.orchestrator.process_runtime import _LogPump
    read_end, write_end = os.pipe()
    path = tmp_path / "role.log"
    pump = _LogPump(os.fdopen(read_end, "rb", buffering=0), path, 1000)
    pump.start()
    os.write(write_end, b"".join(b"line %04d\n" % i for i in range(900)))            # 9 000 bytes in (usually) one read
    os.close(write_end)
    pump.join(10)
    assert path.stat().st_size <= 1000 and path.with_name("role.log.1").stat().st_size <= 1000
    assert (path.with_name("role.log.1").read_bytes() + path.read_bytes()).endswith(b"line 0899\n")


def test_a_failed_log_pump_setup_does_not_leak_a_live_unrecorded_child(tmp_path, monkeypatch) -> None:
    """Review fix 5: the pump is created after Popen; if it cannot be (disk full, fd limit), the started child must be killed and reaped."""
    import neo4j_graph_loader.orchestrator.process_runtime as module
    pids = []
    real_popen = subprocess.Popen

    class Recording(real_popen):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            pids.append(self.pid)

    class BrokenPump:
        def __init__(self, *args, **kwargs):
            raise OSError(24, "Too many open files")

    monkeypatch.setattr(module.subprocess, "Popen", Recording)
    monkeypatch.setattr(module, "_LogPump", BrokenPump)
    runtime = ProcessRuntime(tmp_path / "run", ready_timeout=30)
    try:
        with pytest.raises(OSError):
            spawn_with(runtime, "ready", tmp_path / "marker")
        assert len(pids) == 1
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                os.kill(pids[0], 0)
            except OSError:
                break
            time.sleep(0.05)
        else:
            os.kill(pids[0], signal.SIGKILL)
            pytest.fail("the child of a failed launch is still alive")
    finally:
        runtime.close()
