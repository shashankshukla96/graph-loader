"""The process backend of the ``Runtime`` protocol: every role is a child process of the CLI (E14.F2.S4).

``ProcessService`` reuses ``RoleLauncher`` - the same builders that decide each role's command, environment,
labels and volumes in Docker mode - and implements the one hook that differs: instead of ``containers.run`` it
turns the launch parameters into a :meth:`ProcessRuntime.spawn`. The translation is deliberately mechanical:

* a Docker bind mount ``host -> /app/...`` becomes a path substitution of ``/app/...`` by ``host`` in the
  command and the environment (the child sees the same files, at their host paths);
* the Docker-network service defaults (``bolt://neo4j:7687``, ``kafka:29092``) become the CLI's own endpoints
  (``NEO4J_URI`` / ``KAFKA_BOOTSTRAP_SERVERS``) unless the operator set ``LOADER_*`` explicitly;
* lease server and dispatcher bind ``127.0.0.1`` on a port reserved per run; a replacement lease server
  reuses the same port, so ``GRAPH_LOADER_BUCKET_LEASE_ADDR`` stays valid;
* ``init``, ``network`` and image selection have no meaning here and are dropped.
"""
from __future__ import annotations

import errno
import logging
import os
import platform
import signal
import socket
import subprocess
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Sequence

from neo4j_graph_loader.orchestrator import heartbeat
from neo4j_graph_loader.orchestrator.cgroup_budget import PlannerBudget  # noqa: F401  (annotation)
from neo4j_graph_loader.orchestrator.health import HealthServer
from neo4j_graph_loader.orchestrator.process_runtime import LogRelay, ProcessHandle, ProcessRuntime
from neo4j_graph_loader.orchestrator.role_launch import RoleLauncher
from neo4j_graph_loader.orchestrator.run_registry import (
    RunRegistry, StaleRegistry, fold, process_identity, signal_registered_cli,
)

logger = logging.getLogger(__name__)

LOOPBACK = "127.0.0.1"
# /healthz is unhealthy once the supervision loop has not ticked for this long: 3 x 10 s, where 10 s is the longest a
# healthy tick can take (a slow broker answers lag and group requests within their 2 s timeouts, several per tick).
DEFAULT_HEALTH_STALE_AFTER_S = 30.0
DOCKER_NEO4J_DEFAULT = "bolt://neo4j:7687"
DOCKER_KAFKA_DEFAULT = "kafka:29092"

# Image role (what the builders ask for) -> the module a role's image runs by default.
DEFAULT_MODULES = {
    "node": "neo4j_graph_loader.loader.node_loader",
    "edge": "neo4j_graph_loader.loader.edge_loader",
    "clock": "neo4j_graph_loader.orchestrator.clock_runner",
}
CONTROL_SERVER_MODULES = {
    "neo4j_graph_loader.orchestrator.lease_server", "neo4j_graph_loader.orchestrator.planned_dispatcher",
}


class ProcessPreflightError(RuntimeError):
    pass


def check_platform(system: str | None = None, kernel: str | None = None) -> None:
    """Process mode is supported on Linux >= 5.3 (pidfd). Anything else is refused up front."""
    system = platform.system() if system is None else system
    if system != "Linux":
        raise ProcessPreflightError(
            f"the process runtime is supported on Linux only (this is {system}); use --runtime docker")
    release = platform.release() if kernel is None else kernel
    try:
        major, minor = (int(part) for part in release.split(".")[:2])
    except ValueError:
        return                                              # an unparseable release string: the pidfd check below decides
    if (major, minor) < (5, 3):
        raise ProcessPreflightError(f"the process runtime needs Linux 5.3 or newer (pidfd); this kernel is {release}")


def _reserve_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((LOOPBACK, 0))
        return probe.getsockname()[1]


def _module_of(entrypoint: Sequence[str]) -> str:
    if len(entrypoint) >= 3 and entrypoint[1] == "-m":
        return entrypoint[2]
    raise ValueError(f"cannot run entrypoint {list(entrypoint)} as a process role")


class ProcessService(RoleLauncher):
    """``Runtime`` backend that runs roles as child processes."""

    def __init__(self, paths=None, *, root: Path | str | None = None, lifecycle: ProcessRuntime | None = None,
                 metrics_addr: str = "127.0.0.1", child_logs: str = "relay", health_stale_after: float = DEFAULT_HEALTH_STALE_AFTER_S,
                 **options):
        self.runtime = paths
        self.health_stale_after = float(health_stale_after)
        base = Path(root) if root is not None else (paths.root if paths is not None else Path(tempfile.mkdtemp(prefix="graph-loader-")))
        self.metrics_host, _, port = metrics_addr.partition(":") if metrics_addr.count(":") == 1 else (metrics_addr, "", "")
        self.metrics_port = port or "0"            # "0": every role asks the OS for its own port
        if child_logs not in ("relay", "files"):
            raise ValueError("child_logs must be 'relay' or 'files'")
        if lifecycle is None:
            relay = LogRelay() if child_logs == "relay" else None
            lifecycle = ProcessRuntime(base, relay=relay, metrics_host=self.metrics_host if self.metrics_host not in ("0.0.0.0", "") else "127.0.0.1",
                                       **options)
        self.lifecycle = lifecycle
        self.health: HealthServer | None = None
        self._stopping = lambda: False
        self._reserved: dict[tuple[str, str], int] = {}
        self._cli_registered = False

    # -- lifecycle ------------------------------------------------------------------------------------------------
    def register_cli(self) -> None:
        if not self._cli_registered:
            self.lifecycle.register_cli()
            self._cli_registered = True

    # -- health and metrics registration -------------------------------------------------------------------------------
    def set_stopping_check(self, check) -> None:
        self._stopping = check

    def set_cli_metrics(self, server) -> None:
        """Add the CLI's own metrics endpoint to the target file."""
        port = getattr(server, "server_port", None)
        if port:
            self.lifecycle.cli_metrics_port = int(port)
            self.lifecycle._targets_changed()

    def health_state(self) -> tuple[bool, dict]:
        """Readiness: every launched role has reported ready, none has failed, and the run is not stopping.

        A role that exited 0 on its own (bulk work finished) is neither ready nor failed; one that exited otherwise
        without being asked to is a failure; a replaced role (a restarted lease server) is no longer tracked."""
        handles = self.lifecycle.handles()
        failed = sorted(h.name for h in handles if h.failed)
        pending = sorted(h.name for h in handles if not h.is_ready and not h._exited.is_set())
        stopping = bool(self._stopping())
        running = [h for h in handles if not h._exited.is_set()]
        detail = {"roles": len(handles), "running": len(running), "pending": pending, "failed": failed, "stopping": stopping}
        return bool(running) and not pending and not failed and not stopping, detail

    def alive(self) -> bool:
        """Liveness: the registry is writable and the supervision loop is beating (or has not started beating yet)."""
        if self.lifecycle.registry.broken:
            return False
        age = heartbeat.age()
        return age is None or age <= self.health_stale_after

    def start_health(self, address: str) -> HealthServer:
        self.health = HealthServer(address, alive=self.alive, ready=self.health_state)
        logger.info("stage=health addr=%s port=%s", self.health.host, self.health.port)
        return self.health

    def close(self) -> None:
        if self.health is not None:
            self.health.close()
        try:
            if self._cli_registered:
                self.lifecycle.registry.append("cli-exit", pid=os.getpid())
        except Exception:
            pass
        self.lifecycle.close()

    def add_pre_signal_hook(self, hook) -> None:
        self.lifecycle.add_pre_signal_hook(hook)

    def apply_plan_budget(self, args, schema, *, view=None) -> "PlannerBudget":
        """Size the planner workers inside the shared memory limit (process backend only) and store the result on ``args``.

        Raises :class:`cgroup_budget.SizingError` when the run cannot be sized safely; nothing has been launched yet."""
        from neo4j_graph_loader.loader.edge_planner import available_memory_bytes  # noqa: F401  (documents the Docker-mode source)
        from neo4j_graph_loader.orchestrator import auto_settings, cgroup_budget, planned_edges
        nodes = [role for node in schema.nodes for role in ["node-loader"] * max(1, int(getattr(node, "replicas", 1) or 1))]
        roles = nodes + ["planned-dispatcher"] + ["planned-edge-writer"] * int(getattr(args, "plan_writers", None) or planned_edges.DEFAULT_PLAN_WRITERS)
        explicit_workers = getattr(args, "plan_workers", None) is not None
        workers = args.plan_workers if explicit_workers else auto_settings.default_plan_workers()
        budget = cgroup_budget.plan_budget(
            view if view is not None else cgroup_budget.read_view(), roles, workers=workers, explicit_workers=explicit_workers,
            explicit_memory_mb=getattr(args, "plan_memory_mb", None) or None)
        args.plan_workers, args.plan_memory_mb = budget.workers, budget.memory_mb
        logger.info("stage=sizing planner_workers=%s planner_memory_mb=%s headroom_mb=%s reserved_mb=%s planner_mb=%s roles=%s%s",
                    budget.workers, budget.memory_mb, budget.headroom_mb, budget.reserved_mb, budget.planner_mb, len(roles),
                    "".join(f" note={note!r}" for note in budget.notes))
        return budget

    def planner_supervision(self):
        """Planner workers follow the CLI and are recorded in the registry (see S3)."""
        from neo4j_graph_loader.loader.edge_planner import PlannerSupervision
        return PlannerSupervision(
            parent_watch=True,
            on_process=lambda info: self.lifecycle.register_external(info["role"], info["pid"], info["index"]),
        )

    # -- RoleLauncher hooks -----------------------------------------------------------------------------------------
    def _image(self, role: str) -> str:
        return f"role:{role}"

    def _mount_timings(self, volumes: dict) -> bool:
        directory = str(self.runtime.timings) if self.runtime is not None else os.environ.get("GRAPH_LOADER_TIMING_DIR")
        if not directory:
            return False
        directory = os.path.abspath(directory)
        os.makedirs(directory, exist_ok=True)
        volumes[directory] = {"bind": "/app/write-timings", "mode": "rw"}
        return True

    def _rejection_directory(self, explicit):
        from neo4j_graph_loader.orchestrator.runtime import REJECTION_DIR
        return explicit if explicit is not None else (str(self.runtime.rejections) if self.runtime is not None else REJECTION_DIR)

    def _rollback(self, auto_remove: bool):
        def undo(handle) -> None:
            try:
                handle.hold_for_stop()
                handle.stop()
            except Exception:
                pass
            if not auto_remove:
                try:
                    handle.remove(force=True)
                except Exception:
                    pass
        return undo

    def _run_container(self, **kwargs: Any) -> ProcessHandle:
        """Spawn the process equivalent of a ``containers.run(**kwargs)`` call."""
        role = str(kwargs["image"]).split(":", 1)[1]
        labels = dict(kwargs.get("labels") or {})
        module = _module_of(kwargs["entrypoint"]) if kwargs.get("entrypoint") else DEFAULT_MODULES[role]
        mapping = {spec["bind"]: host for host, spec in (kwargs.get("volumes") or {}).items()}

        def translate(text: str) -> str:
            for container_path in sorted(mapping, key=len, reverse=True):
                if container_path in text:
                    text = text.replace(container_path, mapping[container_path])
            return text

        args = [translate(str(item)) for item in kwargs.get("command") or ()]
        env = {name: translate(str(value)) for name, value in (kwargs.get("environment") or {}).items()}
        if env.get("NEO4J_URI") == DOCKER_NEO4J_DEFAULT and not os.environ.get("LOADER_NEO4J_URI"):
            env["NEO4J_URI"] = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
        if env.get("KAFKA_BOOTSTRAP_SERVERS") == DOCKER_KAFKA_DEFAULT and not os.environ.get("LOADER_KAFKA_BOOTSTRAP_SERVERS"):
            env["KAFKA_BOOTSTRAP_SERVERS"] = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        # Every role asks the OS for its own metrics port on the metrics address (never the shared 9464); the actual port
        # comes back in the ready message and goes into the Prometheus file_sd file. The operator's
        # GRAPH_LOADER_METRICS_ENABLED=0 still disables the endpoint (passed through by the builders).
        env["GRAPH_LOADER_METRICS_LISTEN"] = f"{self.metrics_host}:{self.metrics_port}"
        if module in CONTROL_SERVER_MODULES:
            args = self._bind_control_server(module, labels.get("run_id", ""), args)
        incarnation = self._new_incarnation()
        if incarnation is not None and self.runtime is not None:
            env.update(self._status_environment(incarnation, str(self.runtime.timings)))
        return self._note_role_started(self.lifecycle.spawn(
            labels.get("component", role), module, args, env, labels=labels, auto_remove=bool(kwargs.get("remove")),
            cpuset=kwargs.get("cpuset_cpus"), name=kwargs.get("name"), **({"incarnation": incarnation} if incarnation else {}),
        ), labels)

    def _bind_control_server(self, module: str, run_id: str, args: list[str]) -> list[str]:
        port = self._reserved.setdefault((module, run_id), _reserve_loopback_port())
        rewritten = list(args)
        for flag, value in (("--host", LOOPBACK), ("--port", str(port))):
            if flag in rewritten:
                rewritten[rewritten.index(flag) + 1] = value
            else:
                rewritten += [flag, value]
        return rewritten

    # -- Runtime protocol: images and preflight --------------------------------------------------------------------------
    def prepare_images(self, roles, *, local_only: bool = False) -> dict:
        return {role: self._image(role) for role in roles}

    def build_image(self, tag=None, dockerfile=None):
        return self._image("node")

    def build_edge_image(self, tag=None, dockerfile=None):
        return self._image("edge")

    def build_clock_image(self, tag=None, dockerfile=None):
        return self._image("clock")

    def preflight_topology(self, network, *, plan_path=None) -> None:
        """Bounded local checks; nothing here contacts a Docker daemon."""
        check_platform()
        if not hasattr(os, "pidfd_open"):
            raise ProcessPreflightError("this Python has no os.pidfd_open; the process runtime needs Linux >= 5.3")
        if not Path("/proc/self/stat").exists():
            raise ProcessPreflightError("/proc is not mounted")
        directories = [self.lifecycle.root]
        if self.runtime is not None:
            directories += [self.runtime.rejections, self.runtime.plans, self.runtime.spill, self.runtime.timings]
        for directory in directories:
            if not os.access(directory, os.W_OK):
                raise ProcessPreflightError(f"run directory {directory} is not writable")
        probe = subprocess.run([self.lifecycle._python, "-I", "-c", "import neo4j_graph_loader._child"], capture_output=True,
                               text=True, timeout=60)
        if probe.returncode != 0:
            raise ProcessPreflightError(
                f"a child interpreter cannot import neo4j_graph_loader in isolated mode: {probe.stderr.strip()[-300:]}")
        logger.info("stage=process-preflight platform=%s pidfd=ok run_dir=%s", platform.system(), self.lifecycle.root)

    # -- addresses -------------------------------------------------------------------------------------------------------
    @staticmethod
    def bucket_lease_address(handle, *, network=None, port=None, prefer_name: bool = False, host: bool = False) -> str:
        return f"{LOOPBACK}:{handle.ports['control']}"

    @staticmethod
    def planned_dispatcher_addresses(handle, *, network=None, port=None) -> tuple[str, str]:
        address = f"{LOOPBACK}:{handle.ports['control']}"
        return address, address

    # -- discovery in this CLI ------------------------------------------------------------------------------------------------
    def _live(self) -> list[ProcessHandle]:
        return [handle for handle in self.lifecycle.handles() if not (handle.auto_remove and handle._exited.is_set())]

    def list_run_containers(self, *, label_filters: Sequence[str] = ("app=graph-loader",)) -> list:
        wanted = [item.partition("=")[::2] for item in label_filters]
        found = [h for h in self._live() if all(h.labels.get(key) == value for key, value in wanted)]
        run_id = dict(wanted).get("run_id")
        if run_id:
            found += self.other_attempts_of_run(run_id)
        return found

    def other_attempts_of_run(self, run_id: str) -> list:
        """Live, identity-checked children of ``run_id`` that belong to *another command* of the same ``--run-dir`` base.

        A command only knows its own roles; a second command started with the same ``--run-id`` (or one whose CLI is gone but whose
        children are still running) is visible only through the registries of the sibling attempt directories."""
        root = getattr(self.lifecycle, "root", None)
        if root is None:
            return []
        found = []
        for registry in sorted(Path(root).parent.glob("*/registry.jsonl")):
            if registry.parent == Path(root):
                continue
            try:
                states = registered_children(registry)
            except Exception:
                continue
            found += [SimpleNamespace(name=s["name"], status="running", labels=s.get("labels") or {}, id=s["id"], attempt=str(registry.parent))
                      for s in states if (s.get("labels") or {}).get("run_id") == run_id]
        return found

    @staticmethod
    def stop_hint(run_id: str) -> str:
        return (f"stop that command (graph-loader stop --runtime process --run-dir <its attempt directory>), or after a crash "
                f"graph-loader stop --recover --runtime process --run-dir <its attempt directory>; run id {run_id}")

    def stop_node_loaders(self, node_label: str | None = None) -> None:
        for handle in self.list_run_containers(label_filters=("component=node-loader",)):
            if node_label and handle.labels.get("node_label") != node_label:
                continue
            handle.hold_for_stop()
            handle.stop()

    def list_node_loaders(self) -> list[dict]:
        return [{"id": handle.id[:12], "name": handle.name, "status": handle.status, "node_label": handle.labels.get("node_label")}
                for handle in self.list_run_containers(label_filters=("component=node-loader",))]


# -- second-shell commands: everything goes through the run directory's registry ---------------------------------------------------
def resolve_registry(run_dir: str | os.PathLike) -> Path:
    """``run_dir`` is an attempt directory (holds ``registry.jsonl``) or the base directory of attempts."""
    path = Path(run_dir)
    direct = path / "registry.jsonl"
    if direct.is_file():
        return direct
    candidates = sorted((p for p in path.glob("*/registry.jsonl") if p.is_file()), key=lambda p: p.stat().st_mtime, reverse=True)
    live = [p for p in candidates if _cli_alive(p)]
    if len(live) == 1:
        return live[0]
    if not live and len(candidates) == 1:
        return candidates[0]
    if not candidates:
        raise StaleRegistry(f"no run registry under {path}; pass --run-dir <attempt directory>")
    raise StaleRegistry(f"{len(live) or len(candidates)} runs under {path}; pass --run-dir <attempt directory>")


def _cli_alive(registry: Path) -> bool:
    try:
        records = RunRegistry.read(registry)
    except Exception:
        return False
    if any(r.get("kind") == "cli-exit" for r in records):
        return False
    cli = [r for r in records if r.get("kind") == "cli"]
    if not cli:
        return False
    current = process_identity(int(cli[-1]["pid"]))
    return current is not None and (current.get("start_time"), current.get("boot_id")) == (cli[-1].get("start_time"), cli[-1].get("boot_id"))


def registered_children(registry: Path) -> list[dict]:
    """Children the registry shows as started and not exited, with their identity re-verified."""
    records = RunRegistry.read(registry)
    alive = []
    for state in fold(records).values():
        if state.get("phase") == "exit" or "pid" not in state or state.get("external"):
            continue
        current = process_identity(int(state["pid"]))
        if current is None or (current.get("start_time"), current.get("boot_id")) != (state.get("start_time"), state.get("boot_id")):
            continue
        alive.append(state)
    return alive


def stop_registered(run_dir: str, loader: str | None = None) -> list[str]:
    """``graph-loader stop`` from a second shell. Returns what was signalled (names), raises StaleRegistry if nothing can be."""
    registry = resolve_registry(run_dir)
    if loader is None:
        pid = signal_registered_cli(registry, signal.SIGTERM)
        return [f"cli (pid {pid})"]
    stopped = []
    for state in registered_children(registry):
        labels = state.get("labels") or {}
        if labels.get("component") == "node-loader" and labels.get("node_label") == loader:
            _signal_child(state, signal.SIGTERM)
            stopped.append(state["name"])
    return stopped


def _signal_child(state: dict, signum: int) -> None:
    pid = int(state["pid"])
    descriptor = os.pidfd_open(pid) if hasattr(os, "pidfd_open") else None
    try:
        current = process_identity(pid)
        if current is None or (current.get("start_time"), current.get("boot_id")) != (state.get("start_time"), state.get("boot_id")):
            raise StaleRegistry(f"pid {pid} is no longer {state.get('name')}")
        if descriptor is not None:
            signal.pidfd_send_signal(descriptor, signum)
        else:
            os.kill(pid, signum)
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _signal_if_alive(state: dict, signum: int) -> bool:
    """Signal a recorded process; one that exited (or whose pid was reused) between the check and the signal is simply gone."""
    try:
        _signal_child(state, signum)
        return True
    except (StaleRegistry, ProcessLookupError):
        return False
    except OSError as exc:
        if getattr(exc, "errno", None) == errno.ESRCH:
            return False
        raise


def recover_registered(run_dir: str, *, grace: float = 15.0) -> dict:
    """``graph-loader stop --recover``: after the CLI died abruptly, stop whatever it left behind and say what it retained.

    Refuses while the CLI is alive (use a plain ``stop``). Every registered child and every registered planner process whose
    identity (pid, start time, boot id) still matches is sent SIGTERM through a pidfd, then SIGKILL after ``grace`` seconds.
    The attempt directory itself (spill, plan lock, progress and failure markers, logs) is retained by contract and only reported."""
    import time
    registry = resolve_registry(run_dir)
    if _cli_alive(registry):
        raise StaleRegistry("the run's CLI is still alive; use `graph-loader stop` (without --recover)")
    records = RunRegistry.read(registry)
    leftovers = []
    for state in fold(records).values():
        if state.get("phase") == "exit" or "pid" not in state:
            continue
        current = process_identity(int(state["pid"]))
        if current is not None and (current.get("start_time"), current.get("boot_id")) == (state.get("start_time"), state.get("boot_id")):
            leftovers.append(state)
    for state in leftovers:
        _signal_if_alive(state, signal.SIGTERM)
    deadline = time.monotonic() + grace
    pending = list(leftovers)
    while pending and time.monotonic() < deadline:
        pending = [s for s in pending if _still_that_process(s)]
        if pending:
            time.sleep(0.1)
    killed = [s["name"] for s in pending]
    for state in pending:
        _signal_if_alive(state, signal.SIGKILL)
    deadline = time.monotonic() + 5
    while any(_still_that_process(s) for s in leftovers) and time.monotonic() < deadline:
        time.sleep(0.05)
    remaining = [s["name"] for s in leftovers if _still_that_process(s)]
    attempt = registry.parent
    writer = RunRegistry(registry)
    try:
        writer.append("recovered", signalled=[s["name"] for s in leftovers], killed=killed, remaining=remaining)
    finally:
        writer.close()
    retained = sorted({str(p.relative_to(attempt)) for sub in ("plans", "spill", "rejections") for p in (attempt / sub).rglob("*") if p.is_file()}
                      | {str(p.relative_to(attempt)) for p in attempt.rglob("*.lock") if p.is_file()})
    return {"attempt_dir": str(attempt), "signalled": [s["name"] for s in leftovers], "killed": killed, "remaining": remaining,
            "retained": retained}


def _still_that_process(state: dict) -> bool:
    current = process_identity(int(state["pid"]))
    return current is not None and (current.get("start_time"), current.get("boot_id")) == (state.get("start_time"), state.get("boot_id"))


def describe_registered(run_dir: str) -> list[dict]:
    """``graph-loader status`` from a second shell: node loaders like Docker mode lists them, from the registry."""
    registry = resolve_registry(run_dir)
    rows = []
    for state in registered_children(registry):
        labels = state.get("labels") or {}
        if labels.get("component") == "node-loader":
            rows.append({"id": str(state["id"])[:12], "name": state["name"], "status": "running",
                         "node_label": labels.get("node_label")})
    return rows


__all__ = ["ProcessService", "ProcessPreflightError", "check_platform", "stop_registered", "describe_registered", "resolve_registry",
           "recover_registered"]
