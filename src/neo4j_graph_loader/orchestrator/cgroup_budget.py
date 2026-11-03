"""Memory budget for the process runtime inside a shared cgroup limit (E14.F2.S6).

In a container (``docker run --memory``) or a systemd service (``MemoryMax=``) the CLI, every role and the planner
workers share one limit. Docker mode never does: each role is its own container. The planner's automatic budget
(``edge_planner.default_plan_memory_mb``) reads host ``MemAvailable`` and stays exactly as it is for Docker mode;
this module is used by the **process backend only**.

Sizing formula (all sizes in MiB)::

    headroom   = min( MemAvailable,
                      min over the CLI's cgroup and every visible ancestor with a finite ``memory.max`` of
                          max(0, memory.max - memory.current) )
    reserved   = CLI_PEAK + sum of ROLE_PEAK_MB[role] over the roles the run will launch
    planner    = max(0, headroom - reserved) * PLANNER_SHARE
    workers    = requested workers, reduced to floor(planner / MIN_WORKER_MB) when their floor does not fit
    per worker = min(MAX_WORKER_MB, planner / workers)

A run is refused (``SizingError``) when not even one worker of ``MIN_WORKER_MB`` fits, when explicit
``--plan-workers`` / ``--plan-memory-mb`` need more than ``planner``, or when no limit can be read and the
planner settings were not given explicitly.
"""
from __future__ import annotations

import math
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Mapping

MIB = 1 << 20
MIN_WORKER_MB = 256                    # edge_planner.MIN_AUTO_PLAN_MEMORY_MB
MAX_WORKER_MB = 4096                   # edge_planner.MAX_AUTO_PLAN_MEMORY_MB
PLANNER_SHARE = 0.8                    # the rest of the headroom is slack for bursts and the kernel
CLI_PEAK_MB = 200
# Measured peak resident sizes of the roles (live runs, see the S6 evidence), rounded up.
ROLE_PEAK_MB: Mapping[str, int] = {
    "node-loader": 150, "edge-loader": 150, "planned-edge-writer": 150, "planned-dispatcher": 120,
    "bucket-lease-server": 100, "retry-consumer": 150, "global-batch-clock": 100,
}
CGROUP_ROOT = Path("/sys/fs/cgroup")
PROC_CGROUP = Path("/proc/self/cgroup")


class SizingError(RuntimeError):
    """The run cannot be sized safely inside its memory limit."""


@dataclass(frozen=True)
class Level:
    path: str
    limit: int | None            # None: unlimited ("max") or no memory controller at this level
    current: int | None


@dataclass(frozen=True)
class CgroupView:
    levels: tuple[Level, ...] = ()
    unreadable: tuple[str, ...] = ()       # files that exist but could not be read or parsed
    mem_available: int | None = None
    visible: bool = False                  # a cgroup v2 hierarchy with a memory controller was seen (``memory.max`` readable somewhere)


def _read(path: Path) -> str | None:
    try:
        return path.read_text(encoding="ascii").strip()
    except FileNotFoundError:
        return None
    except (OSError, ValueError):
        raise


def _cgroup_path(proc_cgroup: Path) -> str | None:
    try:
        for line in proc_cgroup.read_text(encoding="ascii").splitlines():
            hierarchy, _, rest = line.partition(":")
            if hierarchy == "0":                # cgroup v2: "0::/path"
                return rest.partition(":")[2] or "/"
    except OSError:
        return None
    return None


def read_view(*, root: Path = CGROUP_ROOT, proc_cgroup: Path = PROC_CGROUP,
              mem_available: Callable[[], int | None] | None = None) -> CgroupView:
    """The CLI's cgroup v2 chain, leaf first, with each level's ``memory.max`` / ``memory.current``."""
    from neo4j_graph_loader.loader.edge_planner import available_memory_bytes
    available = (mem_available or available_memory_bytes)()
    leaf = _cgroup_path(proc_cgroup)
    if leaf is None:
        return CgroupView(mem_available=available)           # no cgroup v2 (a v1 host, no /proc): there is no limit we can see
    parts = [part for part in leaf.split("/") if part]
    levels: list[Level] = []
    unreadable: list[str] = []
    seen = False
    for depth in range(len(parts), -1, -1):                     # leaf first, then every ancestor up to the root
        directory = root.joinpath(*parts[:depth])
        label = "/" + "/".join(parts[:depth])
        limit = current = None
        try:
            text = _read(directory / "memory.max")
            seen = seen or text is not None
            if text is not None and text != "max":
                limit = int(text)
            raw = _read(directory / "memory.current")
            if raw is not None:
                current = int(raw)
        except (OSError, ValueError):
            unreadable.append(label)
        levels.append(Level(label, limit, current))
    try:
        seen = seen or "memory" in (root / "cgroup.controllers").read_text().split()
    except OSError:
        pass
    return CgroupView(tuple(levels), tuple(unreadable), available, visible=seen)


def headroom_bytes(view: CgroupView) -> int | None:
    """Bytes the run may still use, or None when that cannot be determined."""
    if view.unreadable or not view.visible:
        return None                                              # a limit may exist that we cannot see
    candidates = [view.mem_available] if view.mem_available else []
    for level in view.levels:
        if level.limit is not None:
            candidates.append(max(0, level.limit - (level.current or 0)))
    return min(candidates) if candidates else None


def reserved_mb(roles: Iterable[str]) -> int:
    return CLI_PEAK_MB + sum(ROLE_PEAK_MB[role] for role in roles)


@dataclass(frozen=True)
class PlannerBudget:
    workers: int
    memory_mb: int
    headroom_mb: int | None
    reserved_mb: int
    planner_mb: int
    reduced_from: int | None = None
    notes: tuple[str, ...] = field(default_factory=tuple)


def plan_budget(view: CgroupView, roles: Iterable[str], *, workers: int, explicit_workers: bool = False,
                explicit_memory_mb: int | None = None) -> PlannerBudget:
    """Planner concurrency and per-worker memory for a run that will launch ``roles`` beside the planner."""
    roles = list(roles)
    reserved = reserved_mb(roles)
    headroom = headroom_bytes(view)
    if headroom is None:
        if explicit_memory_mb and explicit_workers:
            return PlannerBudget(workers, explicit_memory_mb, None, reserved, workers * explicit_memory_mb,
                                 notes=("no memory limit could be read; using the explicit planner settings",))
        if view.unreadable:
            reason = "a memory limit file exists but cannot be read: " + ", ".join(view.unreadable)
        elif not view.visible:
            reason = ("no cgroup v2 memory limit is visible (cgroup v1, or no memory controller for this process): host MemAvailable "
                      "would size the planner for the whole machine")
        else:
            reason = "no memory limit or MemAvailable could be read"
        raise SizingError(f"{reason}; give both --plan-workers and --plan-memory-mb explicitly")
    headroom_mb = headroom // MIB
    planner_mb = int(max(0, headroom_mb - reserved) * PLANNER_SHARE)
    if explicit_memory_mb:
        need = workers * explicit_memory_mb
        if need > planner_mb:
            raise SizingError(
                f"--plan-workers {workers} x --plan-memory-mb {explicit_memory_mb} needs {need} MiB but only {planner_mb} MiB fit "
                f"(headroom {headroom_mb} MiB - {reserved} MiB for the CLI and {len(roles)} roles, x{PLANNER_SHARE}); "
                "raise the memory limit or lower the planner settings")
        return PlannerBudget(workers, explicit_memory_mb, headroom_mb, reserved, planner_mb)
    if planner_mb < MIN_WORKER_MB:
        raise SizingError(
            f"the memory limit leaves {planner_mb} MiB for the planner (headroom {headroom_mb} MiB - {reserved} MiB for the CLI and "
            f"{len(roles)} roles, x{PLANNER_SHARE}); one planner worker needs at least {MIN_WORKER_MB} MiB; raise the limit")
    fit = max(1, math.floor(planner_mb / MIN_WORKER_MB))
    used = min(workers, fit)
    notes = ()
    if used < workers:
        notes = (f"planner concurrency reduced from {workers} to {used} workers so that {used} x {MIN_WORKER_MB} MiB fit",)
    per_worker = int(min(MAX_WORKER_MB, planner_mb // used))
    return PlannerBudget(used, per_worker, headroom_mb, reserved, planner_mb, reduced_from=workers if used < workers else None,
                         notes=notes)


def peak_bytes(root: Path = CGROUP_ROOT, proc_cgroup: Path = PROC_CGROUP) -> int | None:
    """``memory.peak`` of the CLI's own cgroup (Linux 5.19+), for the sizing evidence."""
    leaf = _cgroup_path(proc_cgroup)
    if leaf is None:
        return None
    try:
        return int((root.joinpath(*[p for p in leaf.split("/") if p]) / "memory.peak").read_text().strip())
    except (OSError, ValueError):
        return None



OOM_REPORT = "cgroup OOM kill observed; victim unknown"


class OomWatch:
    """Run-level view of the CLI's cgroup ``memory.events`` ``oom_kill`` counter (E14.F3.S8).

    The process runtime never attributes an OOM kill to one child: the kernel kills one process of the shared cgroup and
    every role looks the same afterwards (SIGKILL, exit 137), exactly as a manual ``kill -9`` does. What it can say is that
    the group's counter rose. ``rose()`` returns ``(before, after)`` once per rise and ``None`` otherwise (no rise, or no
    readable counter: no cgroup v2, no memory controller)."""

    def __init__(self, *, root: Path = CGROUP_ROOT, proc_cgroup: Path = PROC_CGROUP):
        leaf = _cgroup_path(proc_cgroup)
        self._file = root.joinpath(*[p for p in leaf.split("/") if p]) / "memory.events" if leaf is not None else None
        self._lock = threading.Lock()
        self._seen = self._read()

    def _read(self) -> int | None:
        if self._file is None:
            return None
        try:
            for line in self._file.read_text(encoding="ascii").splitlines():
                key, _, value = line.partition(" ")
                if key == "oom_kill":
                    return int(value)
        except (OSError, ValueError):
            return None
        return None

    def rose(self) -> tuple[int, int] | None:
        with self._lock:
            now = self._read()
            if now is None or self._seen is None or now <= self._seen:
                return None
            before, self._seen = self._seen, now
            return before, now
