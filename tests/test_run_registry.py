"""E14.F2.S2 registry: append-only JSONL, torn tail, short-write rollback, second-shell stop."""
from __future__ import annotations

import errno
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from neo4j_graph_loader.orchestrator import run_registry
from neo4j_graph_loader.orchestrator.run_registry import (
    RegistryBroken, RegistryCorrupt, RegistryError, RunRegistry, StaleRegistry, cli_record, fold, process_identity,
    signal_registered_cli,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_round_trip_and_fold(tmp_path) -> None:
    registry = RunRegistry(tmp_path / "r.jsonl")
    registry.append("intent", id="a", name="node-0")
    registry.append("started", id="a", pid=5)
    registry.append("ready", id="a", ports={"control": 1})
    registry.append("exit", id="a", code=0)
    records = RunRegistry.read(tmp_path / "r.jsonl")
    assert [r["kind"] for r in records] == ["intent", "started", "ready", "exit"]
    assert fold(records)["a"] == {"id": "a", "name": "node-0", "pid": 5, "ports": {"control": 1}, "code": 0, "phase": "exit"}


def test_torn_final_line_is_ignored_and_repaired_by_the_next_writer(tmp_path) -> None:
    path = tmp_path / "r.jsonl"
    registry = RunRegistry(path)
    registry.append("intent", id="a")
    registry.close()
    good = path.read_bytes()
    for torn in (b"deadbeef {\"kind\":\"sta", good.splitlines()[0][:20], b"zzzz"):
        path.write_bytes(good + torn)
        assert [r["kind"] for r in RunRegistry.read(path)] == ["intent"]
        writer = RunRegistry(path)             # opening repairs the tail
        assert path.read_bytes() == good
        writer.append("started", id="a")
        writer.close()
        assert [r["kind"] for r in RunRegistry.read(path)] == ["intent", "started"]
        path.write_bytes(good)


def test_damage_before_the_last_line_is_an_error_not_silently_skipped(tmp_path) -> None:
    path = tmp_path / "r.jsonl"
    registry = RunRegistry(path)
    for kind in ("intent", "started", "ready"):
        registry.append(kind, id="a")
    registry.close()
    lines = path.read_bytes().splitlines(keepends=True)
    lines[1] = lines[1].replace(b"started", b"startee")          # checksum no longer matches
    path.write_bytes(b"".join(lines))
    with pytest.raises(RegistryCorrupt):
        RunRegistry.read(path)


def test_short_write_is_rolled_back_so_it_never_becomes_interior_damage(tmp_path, monkeypatch) -> None:
    path = tmp_path / "r.jsonl"
    registry = RunRegistry(path)
    registry.append("intent", id="a")
    real_write = os.write
    calls = {"n": 0}

    def short_then_fail(fd, data):
        calls["n"] += 1
        if calls["n"] == 1:
            return real_write(fd, bytes(data[:7]))        # a partial record reaches the file ...
        raise OSError(errno.ENOSPC, "No space left on device")   # ... and then the disk is full

    monkeypatch.setattr(run_registry.os, "write", short_then_fail)
    with pytest.raises(RegistryError, match="could not append"):
        registry.append("started", id="a", pid=1)
    monkeypatch.setattr(run_registry.os, "write", real_write)
    assert not registry.broken
    registry.append("ready", id="a")
    assert [r["kind"] for r in RunRegistry.read(path)] == ["intent", "ready"]       # no torn record in the middle


def test_writer_that_cannot_restore_its_tail_refuses_further_appends(tmp_path, monkeypatch) -> None:
    registry = RunRegistry(tmp_path / "r.jsonl")
    monkeypatch.setattr(run_registry.os, "write", lambda fd, data: (_ for _ in ()).throw(OSError(errno.EIO, "io")))
    monkeypatch.setattr(run_registry.os, "ftruncate", lambda fd, size: (_ for _ in ()).throw(OSError(errno.EIO, "io")))
    with pytest.raises(RegistryError):
        registry.append("intent", id="a")
    assert registry.broken
    monkeypatch.undo()
    with pytest.raises(RegistryBroken):
        registry.append("intent", id="b")


def test_process_identity_distinguishes_a_live_pid_from_a_missing_one() -> None:
    mine = process_identity(os.getpid())
    assert mine and mine["start_time"] and mine["boot_id"]
    assert process_identity(os.getpid()) == mine                        # stable for the life of the process
    gone = subprocess.Popen([sys.executable, "-c", "pass"])
    gone.wait()
    assert process_identity(gone.pid) is None


def _start_standin(tmp_path, idle=1, busy=1, deadline=5.0):
    process = subprocess.Popen([sys.executable, str(FIXTURES / "e14_cli_standin.py"), str(tmp_path), str(idle), str(busy), str(deadline)],
                               stdout=subprocess.PIPE, text=True)
    return process, json.loads(process.stdout.readline())


def test_second_shell_stop_reaches_the_cli_and_it_performs_the_ordered_stop(tmp_path) -> None:
    cli, info = _start_standin(tmp_path)
    try:
        pid = signal_registered_cli(tmp_path / "registry.jsonl", signal.SIGTERM)
        assert pid == info["pid"] == cli.pid
        assert cli.wait(timeout=20) == 0
        assert (tmp_path / "ordered-stop").read_text() == "done"                    # the CLI, not a role, handled it
        assert sorted(p.name for p in (tmp_path / "markers").iterdir()) == ["busy-0", "idle-0"]    # roles drained gracefully
    finally:
        if cli.poll() is None:
            cli.kill()
            cli.wait()


def test_stop_refuses_a_recycled_pid_and_does_not_signal_it(tmp_path) -> None:
    cli, _ = _start_standin(tmp_path, idle=0, busy=0)
    try:
        real = process_identity
        with pytest.raises(StaleRegistry, match="different process"):
            signal_registered_cli(tmp_path / "registry.jsonl", signal.SIGTERM,
                                  identity=lambda pid: {**real(pid), "start_time": "an earlier start"})
        time.sleep(0.3)
        assert cli.poll() is None                                                    # the control: it was not signalled
        with pytest.raises(StaleRegistry, match="different process"):
            signal_registered_cli(tmp_path / "registry.jsonl", signal.SIGTERM,
                                  identity=lambda pid: {**real(pid), "boot_id": "another boot"})
        assert cli.poll() is None
    finally:
        cli.kill()
        cli.wait()


def test_stop_of_a_dead_cli_is_reported_stale(tmp_path) -> None:
    cli, _ = _start_standin(tmp_path, idle=0, busy=0)
    cli.kill()
    cli.wait()
    with pytest.raises(StaleRegistry):
        signal_registered_cli(tmp_path / "registry.jsonl", signal.SIGTERM)
    with pytest.raises(StaleRegistry, match="no CLI record"):
        RunRegistry(tmp_path / "empty.jsonl").close() or signal_registered_cli(tmp_path / "empty.jsonl", signal.SIGTERM)


def test_cli_record_is_the_latest(tmp_path) -> None:
    registry = RunRegistry(tmp_path / "r.jsonl")
    registry.append("cli", pid=1)
    registry.append("cli", pid=2)
    assert cli_record(RunRegistry.read(tmp_path / "r.jsonl"))["pid"] == 2
