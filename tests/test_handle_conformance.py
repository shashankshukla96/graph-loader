"""E14.F2.S2: one conformance suite, two ``Handle`` implementations (Docker over a fake client, and a real child process).

Every behaviour the CLI relies on is asserted identically for both: states, exit codes (0, 1, 128+n), the
stop grace then SIGKILL, every ``logs`` form, ``remove`` idempotence and ``HandleGone``.
"""
from __future__ import annotations

import signal
from pathlib import Path

import pytest

import docker_parity_support as support
from neo4j_graph_loader.orchestrator.docker_service import DockerHandle
from neo4j_graph_loader.orchestrator.process_runtime import ProcessRuntime
from neo4j_graph_loader.orchestrator.runtime import Handle, HandleGone

FIXTURES = Path(__file__).resolve().parent / "fixtures"
ROLE = "script:" + str(FIXTURES / "e14_role.py")
ALLOW = {"GRAPH_LOADER_CHILD_ALLOW_SCRIPT": "1"}


class DockerBackend:
    """A DockerHandle over the recording fake; the fake models the daemon's stop/exit semantics."""

    name = "docker"

    def __init__(self, tmp_path):
        self.client = support.FakeDockerClient(support.Recorder(), version="0")

    def launch(self, behaviour: str, *, log: bytes = b""):
        container = self.client.containers.run(name=f"c-{behaviour}-{len(self.client.containers_by_name)}", labels={"app": "graph-loader"})
        container.log_bytes = log
        if behaviour == "stuck":                      # ignores SIGTERM: the daemon waits the grace, then SIGKILLs
            self.client.exit_codes[container.name] = 137
        elif behaviour == "fail":
            self.client.exit_codes[container.name] = 1
        return DockerHandle(container)

    def finish(self, handle, code):                   # the role exits by itself
        handle.container.attrs["State"].update(Status="exited", ExitCode=code)

    def die_by_signal(self, handle, signum):
        self.finish(handle, 128 + int(signum))

    def close(self):
        pass


class ProcessBackend:
    name = "process"

    def __init__(self, tmp_path):
        self.tmp = tmp_path
        self.runtime = ProcessRuntime(tmp_path / "run", ready_timeout=30)

    def launch(self, behaviour: str, *, log: bytes = b""):
        marker = str(self.tmp / f"marker-{len(self.runtime.handles())}")
        mode = {"graceful": ["ready", marker], "stuck": ["ignore-term"], "fail": ["exit-after-ready", "1"],
                "chatty": ["chatty", "6"], "ok": ["exit-after-ready", "0"]}[behaviour if not log else "chatty"]
        return self.runtime.spawn("role", ROLE, mode, ALLOW)

    def finish(self, handle, code):
        assert handle.wait(20) == code

    def die_by_signal(self, handle, signum):
        raise NotImplementedError

    def close(self):
        self.runtime.close()


@pytest.fixture(params=["docker", "process"])
def backend(request, tmp_path):
    instance = {"docker": DockerBackend, "process": ProcessBackend}[request.param](tmp_path)
    yield instance
    instance.close()


def test_handle_has_the_protocol_surface(backend) -> None:
    handle = backend.launch("graceful")
    assert isinstance(handle, Handle)
    assert isinstance(handle.name, str) and isinstance(handle.id, str) and handle.labels is not None
    assert handle.status == "running"
    handle.reload()
    state = handle.attrs["State"]
    assert state["Status"] == "running" and state["OOMKilled"] in (False, None)
    assert isinstance(handle.top()["Processes"], list)
    assert isinstance(handle.hold_for_stop(), float)


def test_graceful_stop_with_and_without_a_timeout(backend) -> None:
    first, second = backend.launch("graceful"), backend.launch("graceful")
    first.stop()
    second.stop(timeout=5)
    for handle in (first, second):
        handle.reload()
        assert handle.status == "exited" and handle.attrs["State"]["ExitCode"] == 0


def test_a_role_that_ignores_sigterm_ends_in_sigkill_after_the_grace(backend) -> None:
    handle = backend.launch("stuck")
    handle.stop(timeout=1)
    handle.reload()
    assert handle.status == "exited" and handle.attrs["State"]["ExitCode"] == 137


@pytest.mark.parametrize("behaviour,code", [("ok", 0), ("fail", 1)])
def test_exit_codes_zero_and_one(backend, behaviour, code) -> None:
    handle = backend.launch(behaviour)
    backend.finish(handle, code)
    handle.reload()
    assert handle.status == "exited" and handle.attrs["State"]["ExitCode"] == code


def test_exit_code_after_a_signal_is_128_plus_n(backend, tmp_path) -> None:
    if backend.name == "docker":
        handle = backend.launch("graceful")
        backend.die_by_signal(handle, signal.SIGKILL)
    else:
        handle = backend.runtime.spawn("role", ROLE, ["self-signal", str(int(signal.SIGKILL))], ALLOW)
        assert handle.wait(20) == 137
    assert handle.attrs["State"]["ExitCode"] == 137


def test_every_logs_form(backend) -> None:
    handle = backend.launch("graceful", log=b"l1\nl2\nl3\nl4\n")
    if backend.name == "process":
        import time
        deadline = time.monotonic() + 10
        while b"line 5" not in handle.logs() and time.monotonic() < deadline:
            time.sleep(0.05)
    whole = handle.logs()
    assert isinstance(whole, bytes) and whole
    assert isinstance(handle.logs(tail=2), bytes)
    assert isinstance(handle.logs(tail=2, stdout=True, stderr=True), bytes)
    if backend.name == "process":
        assert len(handle.logs(tail=2).splitlines()) == 2
    handle.stop(timeout=5) if backend.name == "docker" else handle.kill()


def test_remove_then_everything_is_gone_and_remove_is_not_silently_repeatable(backend) -> None:
    handle = backend.launch("graceful")
    handle.stop(timeout=5)
    handle.remove(force=True)
    for call in (handle.reload, handle.top, lambda: handle.logs(tail=1), handle.stop, lambda: handle.remove(force=True)):
        with pytest.raises(HandleGone):
            call()


def test_force_remove_of_a_running_role_kills_it(backend) -> None:
    handle = backend.launch("stuck")
    handle.remove(force=True)
    with pytest.raises(HandleGone):
        handle.reload()
