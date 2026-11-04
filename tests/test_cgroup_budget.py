"""E14.F2.S6: the process backend's memory budget inside a shared cgroup limit (Docker mode keeps its planner defaults)."""
from __future__ import annotations

import argparse
import os
import platform
from pathlib import Path
from types import SimpleNamespace

import pytest

from neo4j_graph_loader.loader import edge_planner
from neo4j_graph_loader.orchestrator import cgroup_budget as cb
from neo4j_graph_loader.orchestrator.process_service import ProcessService

MIB = 1 << 20
ROLES = ["node-loader", "node-loader", "planned-dispatcher", "planned-edge-writer", "planned-edge-writer"]


def tree(tmp_path, leaf, levels):
    """A fake cgroup v2 hierarchy: ``levels`` maps a path ('/a/b') to (memory.max text or None, memory.current text or None)."""
    root = tmp_path / "cgroup"
    for path, (limit, current) in levels.items():
        directory = root.joinpath(*[p for p in path.split("/") if p])
        directory.mkdir(parents=True, exist_ok=True)
        if limit is not None:
            (directory / "memory.max").write_text(limit + "\n")
        if current is not None:
            (directory / "memory.current").write_text(current + "\n")
    proc = tmp_path / "proc_cgroup"
    proc.write_text(f"0::{leaf}\n")
    return root, proc


def view(tmp_path, leaf, levels, available=64 * 1024 * MIB):
    root, proc = tree(tmp_path, leaf, levels)
    return cb.read_view(root=root, proc_cgroup=proc, mem_available=lambda: available)


# -- headroom -------------------------------------------------------------------------------------------------------------
def test_unlimited_leaf_with_a_finite_ancestor_uses_the_ancestor(tmp_path) -> None:
    v = view(tmp_path, "/svc/app", {"/svc/app": ("max", str(100 * MIB)), "/svc": (str(2048 * MIB), str(300 * MIB)), "/": (None, None)})
    assert [level.limit for level in v.levels] == [None, 2048 * MIB, None]
    assert cb.headroom_bytes(v) == (2048 - 300) * MIB


def test_finite_leaf_smaller_than_its_ancestor_wins(tmp_path) -> None:
    v = view(tmp_path, "/svc/app", {"/svc/app": (str(1024 * MIB), str(100 * MIB)), "/svc": (str(4096 * MIB), str(300 * MIB))})
    assert cb.headroom_bytes(v) == 924 * MIB


def test_ancestor_with_less_room_than_the_leaf_wins(tmp_path) -> None:
    v = view(tmp_path, "/svc/app", {"/svc/app": (str(4096 * MIB), str(100 * MIB)), "/svc": (str(1024 * MIB), str(900 * MIB))})
    assert cb.headroom_bytes(v) == 124 * MIB


def test_headroom_is_bounded_by_mem_available(tmp_path) -> None:
    v = view(tmp_path, "/app", {"/app": (str(8192 * MIB), str(0))}, available=500 * MIB)
    assert cb.headroom_bytes(v) == 500 * MIB


def test_exhausted_headroom_clamps_to_zero(tmp_path) -> None:
    v = view(tmp_path, "/app", {"/app": (str(512 * MIB), str(700 * MIB))})
    assert cb.headroom_bytes(v) == 0
    with pytest.raises(cb.SizingError, match="leaves 0 MiB for the planner"):
        cb.plan_budget(v, ROLES, workers=4)


def test_unreadable_limit_file_means_unknown(tmp_path) -> None:
    root, proc = tree(tmp_path, "/app", {"/app": (None, None)})
    (root / "app" / "memory.max").mkdir()                          # exists, cannot be read as a file
    v = cb.read_view(root=root, proc_cgroup=proc, mem_available=lambda: 8192 * MIB)
    assert v.unreadable == ("/app",) and cb.headroom_bytes(v) is None
    with pytest.raises(cb.SizingError, match="cannot be read.*--plan-workers and --plan-memory-mb"):
        cb.plan_budget(v, ROLES, workers=4)
    with pytest.raises(cb.SizingError):
        cb.plan_budget(v, ROLES, workers=4, explicit_workers=True)       # memory still missing
    budget = cb.plan_budget(v, ROLES, workers=3, explicit_workers=True, explicit_memory_mb=512)
    assert (budget.workers, budget.memory_mb) == (3, 512) and "no memory limit could be read" in budget.notes[0]


def test_no_cgroup_v2_information_means_no_visible_limit_not_mem_available(tmp_path) -> None:
    """Review fix 2: on cgroup v1, or where no memory controller is visible, the container's limit cannot be read; host MemAvailable
    would size the planner for the whole machine and the container would be OOM-killed. Automatic sizing must refuse instead."""
    v = cb.read_view(root=tmp_path / "none", proc_cgroup=tmp_path / "missing", mem_available=lambda: 6000 * MIB)
    assert v.levels == () and cb.headroom_bytes(v) is None
    with pytest.raises(cb.SizingError, match="--plan-workers and --plan-memory-mb"):
        cb.plan_budget(v, ROLES, workers=4)
    explicit = cb.plan_budget(v, ROLES, workers=3, explicit_workers=True, explicit_memory_mb=512)
    assert (explicit.workers, explicit.memory_mb) == (3, 512)


def test_cgroup_v1_layout_is_not_mistaken_for_v2(tmp_path) -> None:
    proc = tmp_path / "proc_cgroup"
    proc.write_text("12:memory:/docker/abc\n11:cpu,cpuacct:/docker/abc\n")                     # no "0::" line: a v1 host
    root = tmp_path / "cgroup"
    (root / "memory" / "docker" / "abc").mkdir(parents=True)
    (root / "memory" / "docker" / "abc" / "memory.limit_in_bytes").write_text("1073741824\n")
    v = cb.read_view(root=root, proc_cgroup=proc, mem_available=lambda: 8192 * MIB)
    assert cb.headroom_bytes(v) is None
    with pytest.raises(cb.SizingError, match="no cgroup v2 memory limit is visible"):
        cb.plan_budget(v, ROLES, workers=4)


def test_v2_path_without_any_memory_controller_file_is_not_a_limit_either(tmp_path) -> None:
    root, proc = tree(tmp_path, "/app", {"/app": (None, None), "/": (None, None)})
    v = cb.read_view(root=root, proc_cgroup=proc, mem_available=lambda: 8192 * MIB)
    assert cb.headroom_bytes(v) is None


def test_an_unlimited_v2_leaf_on_a_bare_host_still_uses_mem_available(tmp_path) -> None:
    v = view(tmp_path, "/user.slice/app.scope", {"/user.slice/app.scope": ("max", "1000"), "/user.slice": ("max", "1000"), "/": (None, None)},
             available=7000 * MIB)
    assert cb.headroom_bytes(v) == 7000 * MIB


def test_workers_that_fit_keep_their_concurrency_and_share_the_planner_budget(tmp_path) -> None:
    v = view(tmp_path, "/app", {"/app": (str(8192 * MIB), str(200 * MIB))})
    budget = cb.plan_budget(v, ROLES, workers=4)
    reserved = cb.CLI_PEAK_MB + 2 * 150 + 120 + 2 * 150
    assert budget.reserved_mb == reserved
    planner = int((8192 - 200 - reserved) * cb.PLANNER_SHARE)
    assert budget.planner_mb == planner and budget.workers == 4 and budget.memory_mb == planner // 4
    assert budget.reduced_from is None


def test_a_worker_floor_that_does_not_fit_reduces_concurrency(tmp_path) -> None:
    reserved = cb.reserved_mb(ROLES)
    limit = reserved + 700                                              # planner = 560 MiB: room for two 256 MiB workers, not four
    v = view(tmp_path, "/app", {"/app": (str(limit * MIB), "0")})
    budget = cb.plan_budget(v, ROLES, workers=4)
    assert budget.workers == 2 and budget.reduced_from == 4 and budget.memory_mb == budget.planner_mb // 2 >= cb.MIN_WORKER_MB
    assert "reduced from 4 to 2" in budget.notes[0]


def test_a_single_worker_that_does_not_fit_refuses_the_launch(tmp_path) -> None:
    reserved = cb.reserved_mb(ROLES)
    v = view(tmp_path, "/app", {"/app": (str((reserved + 200) * MIB), "0")})   # planner = 160 MiB < 256
    with pytest.raises(cb.SizingError, match="one planner worker needs at least 256 MiB"):
        cb.plan_budget(v, ROLES, workers=1)


def test_explicit_settings_that_exceed_the_budget_are_refused_not_oom_killed(tmp_path) -> None:
    v = view(tmp_path, "/app", {"/app": (str(4096 * MIB), "0")})
    with pytest.raises(cb.SizingError, match="needs 8192 MiB but only"):
        cb.plan_budget(v, ROLES, workers=4, explicit_workers=True, explicit_memory_mb=2048)
    fitting = cb.plan_budget(v, ROLES, workers=2, explicit_workers=True, explicit_memory_mb=512)
    assert (fitting.workers, fitting.memory_mb) == (2, 512)


def test_the_per_worker_budget_is_capped(tmp_path) -> None:
    v = view(tmp_path, "/app", {"/app": (str(64 * 1024 * MIB), "0")})
    assert cb.plan_budget(v, ROLES, workers=1).memory_mb == cb.MAX_WORKER_MB


# -- ProcessService plumbing -----------------------------------------------------------------------------------------------------
def test_apply_plan_budget_stores_the_sizing_on_args_and_logs_it(tmp_path, caplog) -> None:
    service = ProcessService(root=tmp_path / "run")
    try:
        schema = SimpleNamespace(nodes=[SimpleNamespace(label="A", replicas=1), SimpleNamespace(label="B", replicas=2)])
        args = argparse.Namespace(plan_workers=None, plan_memory_mb=None, plan_writers=3)
        v = view(tmp_path, "/app", {"/app": (str(6000 * MIB), "0")})
        with caplog.at_level("INFO"):
            budget = service.apply_plan_budget(args, schema, view=v)
        assert (args.plan_workers, args.plan_memory_mb) == (budget.workers, budget.memory_mb)
        assert budget.reserved_mb == cb.reserved_mb(["node-loader"] * 3 + ["planned-dispatcher"] + ["planned-edge-writer"] * 3)
        assert "stage=sizing planner_workers=" in caplog.text
        explicit = argparse.Namespace(plan_workers=2, plan_memory_mb=100000, plan_writers=3)
        with pytest.raises(cb.SizingError):
            service.apply_plan_budget(explicit, schema, view=v)
    finally:
        service.close()


# -- Docker mode keeps its planner defaults ---------------------------------------------------------------------------------------------
# Computed from the frozen 0.1.0a1 source (git archive 60ea29f) with edge_planner.default_plan_memory_mb(workers, available).
FROZEN_DEFAULT_PLAN_MEMORY_MB = {
    (0, 1): 1024, (0, 2): 1024, (0, 4): 1024, (0, 8): 1024,
    (100 << 20, 1): 256, (100 << 20, 2): 256, (100 << 20, 4): 256, (100 << 20, 8): 256,
    (1 << 30, 1): 256, (1 << 30, 2): 256, (1 << 30, 4): 256, (1 << 30, 8): 256,
    (3 << 30, 1): 768, (3 << 30, 2): 384, (3 << 30, 4): 256, (3 << 30, 8): 256,
    (9 << 30, 1): 2304, (9 << 30, 2): 1152, (9 << 30, 4): 576, (9 << 30, 8): 288,
    (64 << 30, 1): 4096, (64 << 30, 2): 4096, (64 << 30, 4): 4096, (64 << 30, 8): 2048,
}


def test_docker_mode_planner_budget_is_identical_to_0_1_0a1_even_under_a_finite_cgroup_limit(tmp_path, monkeypatch) -> None:
    """The automatic planner budget reads host MemAvailable only. A finite limit on the CLI's own cgroup (a CLI running in a
    bounded container or service in Docker mode) must not change it: here a 100 MiB cgroup limit is in place and the values
    still equal the frozen release's."""
    root, proc = tree(tmp_path, "/app", {"/app": (str(100 * MIB), str(90 * MIB))})
    monkeypatch.setattr(cb, "CGROUP_ROOT", root)
    monkeypatch.setattr(cb, "PROC_CGROUP", proc)
    for (available, workers), expected in FROZEN_DEFAULT_PLAN_MEMORY_MB.items():
        assert edge_planner.default_plan_memory_mb(workers, available) == expected
    source = Path(edge_planner.__file__).read_text()
    assert "memory.max" not in source and "cgroup" not in source, "Docker mode's planner must not look at cgroups"
    assert edge_planner.available_memory_bytes.__module__ == "neo4j_graph_loader.loader.edge_planner"


def test_docker_mode_start_does_not_size_the_planner(monkeypatch) -> None:
    from neo4j_graph_loader.orchestrator import docker_service
    assert not hasattr(docker_service.DockerService, "apply_plan_budget")
    assert "cgroup_budget" not in Path(docker_service.__file__).read_text()


@pytest.mark.skipif(platform.system() != "Linux" or not os.path.exists("/sys/fs/cgroup/cgroup.controllers"), reason="needs cgroup v2")
def test_the_real_cgroup_of_this_process_can_be_read() -> None:
    v = cb.read_view()
    assert v.levels and v.mem_available
    assert cb.headroom_bytes(v) is None or cb.headroom_bytes(v) >= 0
