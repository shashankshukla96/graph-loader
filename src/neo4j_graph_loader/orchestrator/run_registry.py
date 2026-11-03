"""Append-only, crash-safe registry of the processes a run started (process runtime).

One JSON record per line, each prefixed by the CRC-32 of its JSON text and ``fsync``ed. Record kinds:
``cli`` (the run's controlling process), ``intent`` (before a spawn), ``started`` (pid, pgid, start time,
boot id, command digest), ``ready`` (with the ports), ``exit`` (terminal). A reader ignores a torn final
line; damage anywhere else is an error. Before any append after a failed or short write the writer truncates
back to the last complete record, or marks itself broken and refuses further appends, so a torn record
can never become interior damage.
"""
from __future__ import annotations

import json
import os
import platform
import subprocess
import threading
import zlib
from pathlib import Path
from typing import Any, Callable, Iterator

REGISTRY_NAME = "registry.jsonl"


class RegistryError(RuntimeError):
    pass


class RegistryBroken(RegistryError):
    """The writer could not restore a clean tail; appends are refused."""


class RegistryCorrupt(RegistryError):
    """A record other than the final line failed its checksum or did not parse."""


def _encode(record: dict[str, Any]) -> bytes:
    body = json.dumps(record, sort_keys=True, separators=(",", ":")).encode()
    return b"%08x " % zlib.crc32(body) + body + b"\n"


def _decode(line: bytes) -> dict[str, Any] | None:
    if len(line) < 10 or line[8:9] != b" ":
        return None
    try:
        if int(line[:8], 16) != zlib.crc32(line[9:]):
            return None
        record = json.loads(line[9:])
    except ValueError:
        return None
    return record if isinstance(record, dict) else None


class RunRegistry:
    """Writer (and reader) for ``<run dir>/registry.jsonl``. Thread-safe."""

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self._lock = threading.Lock()
        self._broken = False
        self._fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        # A previous writer may have died mid-line: cut the file back to its last complete record first.
        self._committed = self._repair_tail()

    # -- writing -------------------------------------------------------------------------------------------
    def _repair_tail(self) -> int:
        data = self.path.read_bytes()
        good = 0
        for line in data.splitlines(keepends=True):
            if not line.endswith(b"\n") or _decode(line.rstrip(b"\n")) is None:
                break
            good += len(line)
        if good != len(data):
            os.ftruncate(self._fd, good)
            os.fsync(self._fd)
        return good

    def append(self, kind: str, **fields: Any) -> None:
        payload = _encode({"kind": kind, **fields})
        with self._lock:
            if self._broken:
                raise RegistryBroken("registry writer is broken; refusing to append")
            try:
                written = self._write_all(payload)
                os.fsync(self._fd)
            except OSError as exc:
                self._restore_tail(exc)
                raise RegistryError(f"could not append {kind!r} record: {exc}") from exc
            self._committed += written

    def _write_all(self, payload: bytes) -> int:
        # A short write is retried by os.write's caller; any OSError leaves a partial record for _restore_tail.
        view, total = memoryview(payload), 0
        while total < len(payload):
            total += os.write(self._fd, view[total:])
        return total

    def _restore_tail(self, cause: BaseException) -> None:
        try:
            os.ftruncate(self._fd, self._committed)
            os.fsync(self._fd)
        except OSError:
            self._broken = True

    @property
    def broken(self) -> bool:
        return self._broken

    def close(self) -> None:
        with self._lock:
            if self._fd >= 0:
                os.close(self._fd)
                self._fd = -1

    # -- reading ---------------------------------------------------------------------------------------------
    @staticmethod
    def read(path: Path | str) -> list[dict[str, Any]]:
        data = Path(path).read_bytes()
        lines = data.split(b"\n")
        records: list[dict[str, Any]] = []
        for index, line in enumerate(lines):
            last = index == len(lines) - 1
            if last and not line:
                break                      # file ends with a newline: nothing torn
            record = _decode(line)
            if record is None or (last and not data.endswith(b"\n")):
                if last:
                    break                  # a torn final line is ignored
                raise RegistryCorrupt(f"{path}: record {index + 1} is damaged")
            records.append(record)
        return records


def fold(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Latest known state per child ``id`` from ``intent``/``started``/``ready``/``exit`` records."""
    children: dict[str, dict[str, Any]] = {}
    for record in records:
        if record.get("kind") in {"intent", "started", "ready", "exit"} and "id" in record:
            state = children.setdefault(record["id"], {})
            state.update({k: v for k, v in record.items() if k != "kind"})
            state["phase"] = record["kind"]
    return children


def cli_record(records: list[dict[str, Any]]) -> dict[str, Any] | None:
    found = [record for record in records if record.get("kind") == "cli"]
    return found[-1] if found else None


# -- process identity (pid + start time + boot id) -------------------------------------------------------------
def _linux_identity(pid: int) -> dict[str, str] | None:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
        # the command name may contain spaces and parentheses: split after the last ')'
        fields = stat[stat.rindex(")") + 2:].split()
        start = fields[19]               # field 22 of /proc/<pid>/stat: start time in clock ticks since boot
        boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    except (OSError, ValueError, IndexError):
        return None
    return {"start_time": start, "boot_id": boot}


def _portable_identity(pid: int) -> dict[str, str] | None:
    """Development hosts without /proc (macOS): ``ps`` start time and the kernel boot time."""
    try:
        start = subprocess.run(["ps", "-o", "lstart=", "-p", str(pid)], capture_output=True, text=True, timeout=5).stdout.strip()
        boot = subprocess.run(["sysctl", "-n", "kern.boottime"], capture_output=True, text=True, timeout=5).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None
    return {"start_time": start, "boot_id": boot} if start else None


def process_identity(pid: int) -> dict[str, str] | None:
    """Start time and boot id of ``pid``, or None when it does not exist (or cannot be read)."""
    if platform.system() == "Linux":
        return _linux_identity(pid)
    return _portable_identity(pid)


IdentityProvider = Callable[[int], "dict[str, str] | None"]


def iter_children(records: list[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    yield from fold(records).values()


class StaleRegistry(RegistryError):
    """The registered CLI is gone, or its pid now belongs to a different process."""


def signal_registered_cli(registry_path: Path | str, signum: int, *, identity: IdentityProvider = process_identity) -> int:
    """Send ``signum`` to the CLI recorded in ``registry_path`` and return its pid.

    The identity (start time and boot id) is verified *after* the process is pinned: on Linux through a
    pidfd opened first, so a pid recycled between the check and the signal cannot be hit. Hosts without
    pidfds (development machines) verify and then use ``os.kill``, which is racy and not a supported mode.
    """
    import signal as _signal
    record = cli_record(RunRegistry.read(registry_path))
    if record is None:
        raise StaleRegistry("the registry has no CLI record")
    pid = int(record["pid"])
    descriptor = None
    if hasattr(os, "pidfd_open"):
        try:
            descriptor = os.pidfd_open(pid)
        except ProcessLookupError as exc:
            raise StaleRegistry(f"CLI pid {pid} no longer exists") from exc
    try:
        current = identity(pid)
        if current is None:
            raise StaleRegistry(f"CLI pid {pid} no longer exists")
        if (current.get("start_time"), current.get("boot_id")) != (record.get("start_time"), record.get("boot_id")):
            raise StaleRegistry(f"pid {pid} is a different process than the registered CLI (start time or boot id differs)")
        if descriptor is not None:
            _signal.pidfd_send_signal(descriptor, signum)
        else:
            os.kill(pid, signum)
        return pid
    except ProcessLookupError as exc:
        raise StaleRegistry(f"CLI pid {pid} exited before it could be signalled") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
