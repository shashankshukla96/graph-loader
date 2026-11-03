"""Process runtime: roles as child processes of the CLI (E14.F2.S2 lifecycle layer).

This module owns launch, readiness, signalling, exit accounting, logs and the registry for *one* child at a
time (:meth:`ProcessRuntime.spawn` returns a :class:`ProcessHandle`). The role-specific launchers that turn
``run_node_loader`` & co. into spawns arrive with S4.

Design points (see plan/library-implementation/epics/e14.md):

* children start as ``python -I -u -m neo4j_graph_loader._child <module> ...`` in their own session, with a
  control socketpair end passed through ``pass_fds`` and an allow-listed environment;
* a signal goes to a process only through a handle that still owns it: a pidfd opened right after the spawn
  (Linux >= 5.3) or, on development hosts without pidfds, the unreaped ``Popen`` (which refuses to signal a
  reaped pid). Group signals are only sent while the group leader has not been reaped;
* one reaper thread per child is the single owner of the exit: it records the terminal ``exit`` receipt once;
* a negative return code (signal *n*) is reported as ``128 + n`` like Docker, and ``OOMKilled`` is ``None``
  (unknown): the process runtime never attributes an OOM kill to a single child.
"""
from __future__ import annotations

import hashlib
import itertools
import json
import os
import queue
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Mapping, Sequence

from neo4j_graph_loader import _child_protocol as protocol
from neo4j_graph_loader.orchestrator.cgroup_budget import OOM_REPORT, OomWatch
from neo4j_graph_loader.orchestrator.run_registry import IdentityProvider, RunRegistry, process_identity
from neo4j_graph_loader.orchestrator.runtime import HandleGone

# The only variables a child inherits from the CLI's own environment.
ENV_ALLOWLIST = ("PATH", "HOME", "LANG", "LC_ALL", "TZ", "TMPDIR")
DEFAULT_READY_TIMEOUT_S = 60.0
DEFAULT_LIFELINE_DEADLINE_S = 30.0
STOP_READY_MAX_SECONDS = 5.0
LOG_CAP_BYTES = 8 * 1024 * 1024


class LaunchFailed(RuntimeError):
    """The child exited, or did not become ready in time."""


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def exit_code_of(returncode: int) -> int:
    """Docker's convention: death by signal *n* is ``128 + n``."""
    return 128 + -returncode if returncode < 0 else returncode


def build_child_env(parent: Mapping[str, str], role_env: Mapping[str, str]) -> dict[str, str]:
    """Allow-listed parent variables plus exactly the role's own; ``PYTHON*`` never comes from the parent."""
    env = {name: parent[name] for name in ENV_ALLOWLIST if name in parent}
    for name, value in role_env.items():
        if not isinstance(name, str) or not isinstance(value, str) or "=" in name or not name:
            raise ValueError(f"invalid child environment entry {name!r}")
        env[name] = value
    return env


class _Signaller:
    """Signals one child without ever touching a recycled pid."""

    def __init__(self, process: subprocess.Popen):
        self._process = process
        self._lock = threading.Lock()
        self._pidfd: int | None = None
        if hasattr(os, "pidfd_open"):
            try:
                self._pidfd = os.pidfd_open(process.pid)     # before anything can reap the child
            except OSError:
                self._pidfd = None

    @property
    def uses_pidfd(self) -> bool:
        return self._pidfd is not None

    def send(self, signum: int) -> bool:
        """Deliver ``signum`` to the child; False when it has already been reaped (nothing is signalled)."""
        with self._lock:
            if self._pidfd is not None:
                try:
                    signal.pidfd_send_signal(self._pidfd, signum)
                    return True
                except ProcessLookupError:
                    return False
            if self._process.returncode is not None:
                return False
            try:
                self._process.send_signal(signum)
                return True
            except ProcessLookupError:
                return False

    def send_group(self, signum: int) -> bool:
        """Signal the child's process group, only while its leader is unreaped (the group id cannot be reused)."""
        with self._lock:
            if self._process.returncode is not None:
                return False
            try:
                os.killpg(self._process.pid, signum)
                return True
            except (ProcessLookupError, PermissionError):
                return False

    def close(self) -> None:
        with self._lock:
            if self._pidfd is not None:
                os.close(self._pidfd)
                self._pidfd = None


class LogRelay:
    """Forwards children's output lines to the CLI's stdout with a ``[name]`` prefix, without ever blocking a child.

    The per-child log files are the source of truth. Lines go through a bounded queue to one writer thread; when stdout
    is slow or blocked the queue fills and further relay lines are *dropped* (and counted), never waited for: the pump
    that feeds the queue keeps draining the child's pipe, so the child never blocks on logging."""

    def __init__(self, stream=None, capacity: int = 50_000):
        self._stream = stream
        self._queue: queue.Queue = queue.Queue(maxsize=capacity)
        self.dropped = 0
        self.relayed = 0
        self._closed = threading.Event()
        self._thread = threading.Thread(target=self._run, name="log-relay", daemon=True)
        self._thread.start()

    def submit(self, prefix: str, lines) -> None:
        for line in lines:
            try:
                self._queue.put_nowait((prefix, line))
            except queue.Full:
                self.dropped += 1

    def _out(self):
        return self._stream if self._stream is not None else sys.stdout

    def _run(self) -> None:
        while True:
            try:
                prefix, line = self._queue.get(timeout=0.2)
            except queue.Empty:
                if self._closed.is_set():
                    return
                try:
                    self._out().flush()
                except Exception:
                    pass
                continue
            try:
                self._out().write(f"[{prefix}] {line.decode('utf-8', 'replace')}")
                self.relayed += 1
            except Exception:
                self.dropped += 1

    def close(self) -> None:
        self._closed.set()
        self._thread.join(timeout=5)
        if self.dropped:
            sys.stderr.write(f"WARNING: stage=child-logs relay dropped {self.dropped} line(s); the per-child log files are complete\n")


class _LogPump(threading.Thread):
    """Copies a child's combined output into ``<log>`` (size-capped, one rotated predecessor ``<log>.1``) and,
    when a relay is configured, hands each complete line to it (non-blocking)."""

    def __init__(self, stream, path: Path, cap: int, relay: "LogRelay | None" = None, prefix: str = ""):
        super().__init__(name=f"log-pump-{path.stem}", daemon=True)
        self._stream, self._path, self._cap = stream, path, cap
        self._relay, self._prefix, self._partial = relay, prefix, b""
        self._written = 0
        self._file = open(path, "ab", buffering=0)

    def run(self) -> None:
        try:
            while True:
                chunk = os.read(self._stream.fileno(), 65536)
                if not chunk:
                    break
                rest = chunk
                while rest:                      # a read can be larger than the cap: slice it, so the file never exceeds the cap
                    if self._written >= self._cap:
                        self._file.close()
                        os.replace(self._path, self._path.with_name(self._path.name + ".1"))
                        self._file = open(self._path, "ab", buffering=0)
                        self._written = 0
                    piece, rest = rest[: self._cap - self._written], rest[self._cap - self._written:]
                    self._file.write(piece)
                    self._written += len(piece)
                if self._relay is not None:
                    pieces = (self._partial + chunk).split(b"\n")
                    self._partial = pieces.pop()
                    self._relay.submit(self._prefix, (piece + b"\n" for piece in pieces))
        except (OSError, ValueError):
            pass
        finally:
            if self._relay is not None and self._partial:
                self._relay.submit(self._prefix, [self._partial + b"\n"])
            for closable in (self._file, self._stream):      # the child's output pipe too, or every child leaks one fd
                try:
                    closable.close()
                except OSError:
                    pass


class ProcessHandle:
    """A child process seen through the ``Handle`` protocol."""

    def __init__(self, runtime: "ProcessRuntime", *, name: str, module: str, labels: Mapping[str, str],
                 process: subprocess.Popen, control: socket.socket, signaller: _Signaller, log_path: Path,
                 pump: _LogPump, auto_remove: bool, incarnation: str):
        self._runtime = runtime
        self.name = name
        self.id = incarnation
        self.labels = dict(labels)
        self.module = module
        self.pid = process.pid
        self.log_path = log_path
        self.auto_remove = auto_remove
        self._process, self._control, self._signaller, self._pump = process, control, signaller, pump
        self._lock = threading.RLock()
        self._ready = threading.Event()
        self._exited = threading.Event()
        self._removed = False
        self._ports: dict[str, int] = {}
        self.metrics: dict = {}                  # what the role reported about its metrics endpoint (state, port)
        self.stopping = False                    # a stop was requested through this handle
        self._announced = False                  # the ready report was processed (metrics state known)
        self._returncode: int | None = None
        self._exit_code: int | None = None
        self.launched_at = time.monotonic()
        self._finalized = False
        self._reader = threading.Thread(target=self._read_control, name=f"control-{name}", daemon=True)
        self._reaper = threading.Thread(target=self._reap, name=f"reaper-{name}", daemon=True)

    def _start_threads(self) -> None:
        self._reader.start()
        self._reaper.start()

    # -- background owners ------------------------------------------------------------------------------------
    def _read_control(self) -> None:
        buffer = b""
        try:
            while True:
                chunk = self._control.recv(4096)
                if not chunk:
                    return
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    try:
                        message = json.loads(line)
                    except ValueError:
                        continue
                    if isinstance(message, dict) and message.get("type") == "ready":
                        ports = message.get("ports") or {}
                        with self._lock:
                            self._ports = {str(k): int(v) for k, v in ports.items()}
                            self.metrics = dict(message.get("metrics") or {})
                        self._announced = True
                        self._runtime._record("ready", id=self.id, name=self.name, ports=self._ports, metrics=self.metrics)
                        self._runtime._targets_changed()       # the file lists this role before spawn() can return
                        self._ready.set()
        except (OSError, ValueError):
            return

    def _reap(self) -> None:
        returncode = self._process.wait()
        # A role may report ready and exit at once. Let the control reader drain what the child sent (it ends at
        # EOF, which the exit guarantees unless a grandchild kept the socket) before the exit becomes visible,
        # or "ready, then exited" would be misread as "exited before ready".
        self._reader.join(timeout=5)
        with self._lock:
            if self._finalized:
                return
            self._finalized = True
            self._returncode = returncode
            self._exit_code = exit_code_of(returncode)
        self._signaller.close()
        self._pump.join(timeout=5)
        try:
            self._control.close()
        except OSError:
            pass
        self._runtime._record("exit", id=self.id, name=self.name, code=self._exit_code, returncode=returncode)
        if returncode == -signal.SIGKILL:
            self._runtime.report_cgroup_oom()
        self._exited.set()
        self._runtime._targets_changed()

    # -- readiness ----------------------------------------------------------------------------------------------
    @property
    def ports(self) -> dict[str, int]:
        return dict(self._ports)

    @property
    def is_ready(self) -> bool:
        return self._ready.is_set()

    def wait_ready(self, timeout: float) -> None:
        """Block until the role reported ready. Exit before ready, or no report in ``timeout``, fails the launch."""
        deadline = time.monotonic() + timeout
        while not self._ready.is_set():
            if self._exited.is_set():
                raise LaunchFailed(f"{self.name} exited with code {self._exit_code} before it was ready{self._tail_text()}")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._signaller.send(signal.SIGKILL)
                self._exited.wait(5)
                raise LaunchFailed(f"{self.name} was not ready within {timeout:g}s; killed{self._tail_text()}")
            self._ready.wait(min(0.05, remaining))

    def _tail_text(self) -> str:
        try:
            text = self.logs(tail=10).decode("utf-8", "replace").strip()
        except Exception:
            return ""
        return f"; last output: {text}" if text else ""

    @property
    def failed(self) -> bool:
        """Exited without being asked to and not with code 0 (a role that finished its bulk work exits 0)."""
        return self._exited.is_set() and not self.stopping and self._exit_code != 0

    def stop_ready(self) -> bool:
        return self._ready.is_set() or self._exited.is_set()

    def hold_for_stop(self) -> float:
        """Defer a stop until the role can take its SIGTERM (readiness), at most ``STOP_READY_MAX_SECONDS`` after launch."""
        started = time.monotonic()
        while not self.stop_ready():
            remaining = STOP_READY_MAX_SECONDS - (time.monotonic() - self.launched_at)
            if remaining <= 0:
                break
            self._ready.wait(min(0.05, remaining))
        return time.monotonic() - started

    # -- Handle protocol ------------------------------------------------------------------------------------------
    def _check(self) -> None:
        if self._removed or (self.auto_remove and self._exited.is_set()):
            raise HandleGone(f"no such process: {self.name}")

    @property
    def status(self) -> str:
        return "exited" if self._exited.is_set() else "running"

    @property
    def attrs(self) -> dict[str, Any]:
        state = {"Status": self.status, "ExitCode": self._exit_code if self._exited.is_set() else 0,
                 "OOMKilled": None, "Pid": self.pid}
        return {"State": state, "NetworkSettings": {"Networks": {}, "Ports": {}}}

    def reload(self) -> None:
        self._check()

    def top(self) -> dict[str, Any]:
        self._check()
        rows = [] if self._exited.is_set() else [[str(self.pid), f"python -m {self.module}"]]
        return {"Titles": ["PID", "CMD"], "Processes": rows}

    def logs(self, *args: Any, tail: int | str = "all", stdout: bool = True, stderr: bool = True, **_ignored: Any) -> bytes:
        self._check()
        if not (stdout or stderr):
            return b""
        data = b""
        for path in (self.log_path.with_name(self.log_path.name + ".1"), self.log_path):      # the kept predecessor, then the live file
            try:
                data += path.read_bytes()
            except OSError:
                continue
        if tail != "all":
            lines = data.splitlines(keepends=True)
            data = b"".join(lines[-int(tail):]) if int(tail) > 0 else b""
        return data

    def stop(self, *args: Any, timeout: float | None = 10, **_ignored: Any) -> None:
        """SIGTERM, wait ``timeout`` seconds, then SIGKILL (Docker's stop semantics). Returns once the child is gone."""
        self._check()
        self.stopping = True
        grace = 10 if timeout is None else timeout
        if not self._exited.is_set():
            self._signaller.send(signal.SIGTERM)
            if not self._exited.wait(grace):
                self._signaller.send(signal.SIGKILL)
                self._exited.wait()
        if self.auto_remove:
            return

    def kill(self) -> None:
        self._signaller.send(signal.SIGKILL)

    def remove(self, *args: Any, force: bool = False, **_ignored: Any) -> None:
        with self._lock:
            if self._removed or (self.auto_remove and self._exited.is_set()):
                raise HandleGone(f"no such process: {self.name}")
            if not self._exited.is_set():
                if not force:
                    raise RuntimeError(f"cannot remove running process {self.name} without force=True")
                self._runtime.run_pre_signal_hooks()
                self._signaller.send_group(signal.SIGKILL)
                self._signaller.send(signal.SIGKILL)
            self._removed = True
        self._exited.wait()
        self._runtime._forget(self)

    def wait(self, timeout: float | None = None) -> int | None:
        """The exit code once the child has exited (None on timeout)."""
        return self._exit_code if self._exited.wait(timeout) else None


class ProcessRuntime:
    """Spawns and tracks role children for one run."""

    def __init__(self, root: Path | str, *, registry: RunRegistry | None = None, python: str | None = None,
                 identity: IdentityProvider = process_identity, ready_timeout: float = DEFAULT_READY_TIMEOUT_S,
                 lifeline_deadline: float = DEFAULT_LIFELINE_DEADLINE_S, log_cap_bytes: int = LOG_CAP_BYTES,
                 parent_env: Mapping[str, str] | None = None, relay: "LogRelay | None" = None,
                 metrics_host: str = "127.0.0.1", oom_watch: OomWatch | None = None, oom_poll_seconds: float = 1.0):
        self.root = Path(root)
        self._oom = oom_watch if oom_watch is not None else OomWatch()
        self._oom_poll = oom_poll_seconds
        self._oom_stop = threading.Event()
        self._oom_listeners: list = []
        (self.root / "logs").mkdir(parents=True, exist_ok=True)
        (self.root / "work").mkdir(parents=True, exist_ok=True)
        self.registry = registry if registry is not None else RunRegistry(self.root / "registry.jsonl")
        self._python = python or sys.executable
        self._identity = identity
        self.ready_timeout = ready_timeout
        self.lifeline_deadline = lifeline_deadline
        self._log_cap = log_cap_bytes
        self._parent_env = dict(os.environ if parent_env is None else parent_env)
        self._counters: dict[str, itertools.count] = {}
        self._counter_lock = threading.Lock()
        self._handles: dict[str, ProcessHandle] = {}
        self._handles_lock = threading.Lock()
        self.relay = relay
        self.metrics_host = metrics_host
        if oom_poll_seconds > 0:
            threading.Thread(target=self._watch_oom, name="oom-watch", daemon=True).start()
        self.targets_path = self.root / "metrics" / "targets.json"
        self.cli_metrics_port: int | None = None
        self._targets_lock = threading.Lock()
        self._pre_signal_hooks: list = []
        self._pre_signal_lock = threading.Lock()
        self._pre_signal_done = False

    # -- registry helpers ----------------------------------------------------------------------------------------
    def _record(self, kind: str, **fields: Any) -> None:
        try:
            self.registry.append(kind, **fields)
        except Exception:
            # intent/started failures are handled in spawn(); later receipts are best effort, never fatal to a role
            if kind in {"intent", "started"}:
                raise

    def _watch_oom(self) -> None:
        """The kernel can kill a process the runtime holds no handle for (a planner worker, the multiprocessing helper): look at the
        group's counter on a timer as well as when a role dies, so the report is neither missed nor attached to a later unrelated kill."""
        while not self._oom_stop.wait(self._oom_poll):
            try:
                self.report_cgroup_oom()
            except Exception:                       # reporting must never take the run down
                pass

    def report_cgroup_oom(self) -> None:
        """A child died by SIGKILL: when the group's ``oom_kill`` counter rose since the last report, say so once, at group level.
        No child is named as the victim (a manual ``kill -9`` is indistinguishable from the kernel's, and several may die at once)."""
        rose = self._oom.rose()
        if rose is None:
            return
        sys.stderr.write(f"ERROR stage=oom {OOM_REPORT} (memory.events oom_kill {rose[0]} -> {rose[1]})\n")
        sys.stderr.flush()
        self._record("oom", oom_kill_before=rose[0], oom_kill_after=rose[1])
        for listener in list(self._oom_listeners):
            try:
                listener(rose[0], rose[1])
            except Exception as exc:                  # a status listener must never disturb the runtime
                sys.stderr.write(f"WARNING: stage=runtime oom listener failed: {exc}\n")

    def add_oom_listener(self, listener) -> None:
        """Call ``listener(oom_kill_before, oom_kill_after)`` each time a group OOM is reported (run status)."""
        self._oom_listeners.append(listener)

    def register_external(self, role: str, pid: int, index: int = 0) -> None:
        """Record a process this run owns but did not spawn through :meth:`spawn` (planner workers and helpers),
        so registry recovery after an abrupt CLI death can find and verify it."""
        ident = self._identity(pid) or {}
        self.registry.append("started", id=f"{role}-{index}-{pid}", name=f"{role}-{index}", role=role, pid=pid,
                             pgid=os.getpgid(pid) if _alive(pid) else None, external=True, **ident)

    def add_pre_signal_hook(self, hook) -> None:
        """Run ``hook`` once, before the runtime sends its first *group* signal or kills anything at close.

        The CLI registers its ordered planner cleanup here, so a signal to a process group can never
        pre-empt the planner workers' own orderly shutdown."""
        self._pre_signal_hooks.append(hook)

    def run_pre_signal_hooks(self) -> None:
        with self._pre_signal_lock:
            if self._pre_signal_done:
                return
            self._pre_signal_done = True
            hooks = list(self._pre_signal_hooks)
        for hook in hooks:
            try:
                hook()
            except Exception as exc:                  # a failing cleanup must not stop the signalling that follows
                sys.stderr.write(f"WARNING: stage=runtime pre-signal hook failed: {exc}\n")

    def register_cli(self) -> None:
        """Record the controlling process so a second shell can find and stop it."""
        ident = self._identity(os.getpid()) or {}
        self.registry.append("cli", pid=os.getpid(), pgid=os.getpgid(0), **ident)

    # -- launch ------------------------------------------------------------------------------------------------------
    def _next_index(self, role: str) -> int:
        with self._counter_lock:
            return next(self._counters.setdefault(role, itertools.count()))

    def spawn(self, role: str, module: str, args: Sequence[str] = (), env: Mapping[str, str] | None = None, *,
              labels: Mapping[str, str] | None = None, auto_remove: bool = False, cpuset: str | None = None,
              wait_ready: bool = True, ready_timeout: float | None = None, name: str | None = None,
              incarnation: str | None = None) -> ProcessHandle:
        """Start ``module`` as a child. With ``wait_ready`` the call returns once the role reported ready.

        ``name`` (default ``<role>-<n>``) names the handle, its working directory and its log file. ``incarnation``
        (32 lowercase hex digits; default a fresh one) is the id the registry and the handle carry; a caller that
        needs it before the launch (run status puts it in the child's environment) passes it in."""
        index = self._next_index(role)
        name = name or f"{role}-{index}"
        incarnation = incarnation or uuid.uuid4().hex
        role_env = dict(env or {})
        if cpuset:
            role_env["LOADER_CPUSET"] = cpuset
        child_env = build_child_env(self._parent_env, role_env)
        parent_end, child_end = socket.socketpair()
        child_env[protocol.CONTROL_FD_ENV] = str(child_end.fileno())
        child_env[protocol.LIFELINE_DEADLINE_ENV] = str(self.lifeline_deadline)
        os.set_inheritable(child_end.fileno(), True)
        command = [self._python, "-I", "-u", "-m", "neo4j_graph_loader._child", module, *args]
        digest = hashlib.sha256(json.dumps(command).encode()).hexdigest()
        work = self.root / "work" / name
        work.mkdir(parents=True, exist_ok=True)
        log_path = self.root / "logs" / f"{name}.log"
        process = None
        try:
            self._record("intent", id=incarnation, name=name, role=role, command_sha256=digest)
            process = subprocess.Popen(
                command, env=child_env, cwd=work, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, start_new_session=True, close_fds=True, pass_fds=(child_end.fileno(),),
            )
        except BaseException:
            parent_end.close()
            child_end.close()
            raise
        child_end.close()
        signaller = _Signaller(process)
        try:
            pump = _LogPump(process.stdout, log_path, self._log_cap, self.relay, name)
            pump.start()
        except BaseException:
            # The child is running but nothing records it yet: it must not outlive the failed launch.
            signaller.send_group(signal.SIGKILL)
            signaller.send(signal.SIGKILL)
            process.wait()
            signaller.close()
            parent_end.close()
            try:
                process.stdout.close()
            except OSError:
                pass
            raise
        handle = ProcessHandle(self, name=name, module=module, labels=labels or {}, process=process, control=parent_end,
                               signaller=signaller, log_path=log_path, pump=pump, auto_remove=auto_remove,
                               incarnation=incarnation)
        try:
            ident = self._identity(process.pid) or {}
            self._record("started", id=incarnation, name=name, role=role, pid=process.pid, pgid=process.pid,
                         command_sha256=digest, labels=dict(labels or {}), **ident)
        except BaseException as exc:
            # No durable record of this child: it must not outlive the failed launch.
            signaller.send_group(signal.SIGKILL)
            signaller.send(signal.SIGKILL)
            process.wait()
            signaller.close()
            parent_end.close()
            pump.join(timeout=5)
            raise LaunchFailed(f"{name}: could not record the started process: {exc}") from exc
        with self._handles_lock:
            self._handles[incarnation] = handle
        handle._start_threads()
        if wait_ready:
            try:
                handle.wait_ready(self.ready_timeout if ready_timeout is None else ready_timeout)
            except LaunchFailed:
                self._forget(handle)
                raise
        return handle

    def _forget(self, handle: ProcessHandle) -> None:
        with self._handles_lock:
            self._handles.pop(handle.id, None)
        self._targets_changed()

    # -- Prometheus file_sd -----------------------------------------------------------------------------------------
    @staticmethod
    def _target_labels(handle: ProcessHandle) -> dict[str, str]:
        labels = {"exporter": handle.name, "runtime": "process"}
        for key, value in handle.labels.items():
            if key.startswith("graph_loader.metrics"):
                continue
            labels[key.replace("graph_loader.", "").replace(".", "_")] = str(value)
        return labels

    def build_targets(self) -> list[dict]:
        """One target group per live role with a listening metrics endpoint; roles whose endpoint is disabled or failed get a
        group with no targets and ``metrics_state`` so the gap is visible in the file; exited roles disappear."""
        groups = []
        if self.cli_metrics_port:
            groups.append({"targets": [f"{self.metrics_host}:{self.cli_metrics_port}"],
                           "labels": {"exporter": "cli", "runtime": "process", "component": "cli", "kind": "cli"}})
        for handle in self.handles():
            if handle._exited.is_set() or not handle._announced:
                continue
            metrics = handle.metrics or {}
            labels = self._target_labels(handle)
            if metrics.get("state") == "listening" and metrics.get("port"):
                groups.append({"targets": [f"{self.metrics_host}:{int(metrics['port'])}"], "labels": labels})
            else:
                groups.append({"targets": [], "labels": {**labels, "metrics_state": metrics.get("state", "unknown")}})
        return groups

    def _targets_changed(self) -> None:
        """Rewrite ``metrics/targets.json`` atomically (temp file, fsync, rename). Never raises into a role's path."""
        try:
            with self._targets_lock:
                self.targets_path.parent.mkdir(parents=True, exist_ok=True)
                temporary = self.targets_path.with_name(f".{self.targets_path.name}.{os.getpid()}.tmp")
                with open(temporary, "w") as out:
                    json.dump(self.build_targets(), out, indent=1, sort_keys=True)
                    out.write("\n")
                    out.flush()
                    os.fsync(out.fileno())
                os.replace(temporary, self.targets_path)
        except Exception as exc:
            sys.stderr.write(f"WARNING: stage=metrics-targets could not write {self.targets_path}: {exc}\n")

    def handles(self) -> list[ProcessHandle]:
        with self._handles_lock:
            return list(self._handles.values())

    def close(self) -> None:
        self.run_pre_signal_hooks()
        if self.relay is not None:
            self.relay.close()
        for handle in self.handles():
            if not handle._exited.is_set():
                handle.kill()
        self._oom_stop.set()
        if self._oom_poll > 0:
            try:
                self.report_cgroup_oom()            # a rise after the last poll still gets reported once, at the end of the run
            except Exception:
                pass
        self.registry.close()
