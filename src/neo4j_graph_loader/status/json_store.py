"""The JSON status store (E15.F1.S3): ``status.json`` and rotating ``events.<first seq>.jsonl`` in the attempt directory.

* ``status.json`` is replaced atomically (temp file, ``fsync``, ``rename``), at most once per ``snapshot_interval``
  while changes keep arriving, and once more, synchronously, by :meth:`JsonStatusStore.close`.
* Events are appended one JSON object per line and ``fsync``ed; the ``seq`` of an event is assigned *by the store, at
  the moment the line is durably written* (the journal protocol below), so ``seq`` is gap-free in the file. The
  same protocol is what the SQL store's outbox implements (E15.F1.S5).
* Segments are named by the first ``seq`` they hold. When the current one would exceed ``max_file_bytes`` a new one
  starts; only the newest ``max_files`` are kept. A reader that finds the oldest kept segment starting after
  ``seq`` 1 reports the deleted range as a ``gap``.
* A failed write (read-only directory, full disk) disables the store: one log line, the exception goes back to the
  caller (the tracker detaches the store and flags ``history_incomplete``), and the load is never affected.

Journal protocol (what :class:`~neo4j_graph_loader.status.tracker.StatusTracker` calls): ``append(make_event)`` where
``make_event(seq) -> dict`` builds the complete event for the ``seq`` the journal chose; it returns that event.
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from pathlib import Path
from typing import Callable

from neo4j_graph_loader.status import locator as locator_module

logger = logging.getLogger(__name__)

STATUS_NAME = "status.json"
EVENTS_PREFIX = "events."
EVENTS_SUFFIX = ".jsonl"
SEGMENT = re.compile(r"^events\.(\d{12})\.jsonl$")

DEFAULT_SNAPSHOT_INTERVAL_SECONDS = 1.0
DEFAULT_MAX_FILE_BYTES = 8 * 1024 * 1024
DEFAULT_MAX_FILES = 8

FILE_MB_ENV = "GRAPH_LOADER_STATUS_EVENT_FILE_MB"
FILES_ENV = "GRAPH_LOADER_STATUS_EVENT_FILES"
SNAPSHOT_INTERVAL_ENV = "GRAPH_LOADER_STATUS_SNAPSHOT_INTERVAL_S"


def segment_name(first_seq: int) -> str:
    return f"{EVENTS_PREFIX}{first_seq:012d}{EVENTS_SUFFIX}"


def list_segments(directory: Path | str) -> list[tuple[int, Path]]:
    """``(first_seq, path)`` of every event segment in ``directory``, oldest first."""
    found = []
    try:
        for entry in os.scandir(directory):
            match = SEGMENT.match(entry.name)
            if match:
                found.append((int(match.group(1)), Path(entry.path)))
    except FileNotFoundError:
        return []
    return sorted(found)


def _number(name: str, default, convert, minimum):
    raw = os.environ.get(name)
    if not raw or not raw.strip():
        return default
    try:
        value = convert(raw)
    except ValueError:
        return default
    return value if value >= minimum else default


def settings_from_environment() -> dict:
    """Store settings from the environment (invalid or out-of-range values fall back to the defaults)."""
    return {
        "snapshot_interval": _number(SNAPSHOT_INTERVAL_ENV, DEFAULT_SNAPSHOT_INTERVAL_SECONDS, float, 0.0),
        "max_file_bytes": int(_number(FILE_MB_ENV, DEFAULT_MAX_FILE_BYTES / (1024 * 1024), float, 0.001) * 1024 * 1024),
        "max_files": _number(FILES_ENV, DEFAULT_MAX_FILES, int, 1),
    }


class JsonStatusStore:
    """Writer of one execution's JSON status files. Thread-safe."""

    def __init__(self, directory: Path | str, *, snapshot_interval: float = DEFAULT_SNAPSHOT_INTERVAL_SECONDS,
                 max_file_bytes: int = DEFAULT_MAX_FILE_BYTES, max_files: int = DEFAULT_MAX_FILES, fsync: bool = True,
                 clock: Callable[[], float] = time.monotonic):
        if max_files < 1 or max_file_bytes < 1:
            raise ValueError("max_files and max_file_bytes must be positive")
        self.directory = Path(directory)
        self._interval, self._max_bytes, self._max_files = snapshot_interval, max_file_bytes, max_files
        self._fsync, self._clock = fsync, clock
        self._lock = threading.Lock()
        self._next_seq = 1
        self._fd: int | None = None
        self._size = 0
        self._segments: list[Path] = []
        self.disabled: str | None = None            # why the store stopped writing, once it has
        self._latest: dict | None = None
        self._written_revision = 0
        self._last_write = float("-inf")
        self._wake = threading.Event()
        self._closed = threading.Event()
        self._writer = threading.Thread(target=self._run, name="status-json-writer", daemon=True)
        self._writer.start()

    # -- failure ------------------------------------------------------------------------------------------------
    def _disable(self, exc: BaseException) -> None:
        if self.disabled is None:
            self.disabled = f"{type(exc).__name__}: {exc}"
            logger.warning("stage=status reason=the JSON status store stopped writing in %s and is disabled for this run "
                           "(the load continues): %s", self.directory, self.disabled)

    # -- events (journal protocol) ------------------------------------------------------------------------------------
    def append(self, make_event: Callable[[int], dict]) -> dict:
        with self._lock:
            if self.disabled is not None:
                raise OSError(f"status store disabled: {self.disabled}")
            try:
                event = make_event(self._next_seq)
                if event.get("seq") != self._next_seq:
                    raise ValueError(f"event carries seq {event.get('seq')!r}, expected {self._next_seq}")
                data = (json.dumps(event, separators=(",", ":")) + "\n").encode()
                self._ensure_segment(self._next_seq, len(data))
                view = memoryview(data)
                while view:
                    view = view[os.write(self._fd, view):]
                if self._fsync:
                    os.fsync(self._fd)
            except Exception as exc:
                self._disable(exc)
                raise
            self._size += len(data)
            self._next_seq += 1
            return event

    def _ensure_segment(self, seq: int, incoming: int) -> None:
        if self._fd is not None and self._size > 0 and self._size + incoming > self._max_bytes:
            os.close(self._fd)
            self._fd = None
        if self._fd is None:
            path = self.directory / segment_name(seq)
            self._fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_APPEND, 0o600)
            self._size = 0
            self._segments.append(path)
            self._sync_directory()
            while len(self._segments) > self._max_files:
                self._segments.pop(0).unlink(missing_ok=True)

    def _sync_directory(self) -> None:
        if not self._fsync:
            return
        try:
            descriptor = os.open(self.directory, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(descriptor)
        except OSError:
            pass
        finally:
            os.close(descriptor)

    # -- snapshot ----------------------------------------------------------------------------------------------------
    def offer_snapshot(self, snapshot: dict) -> None:
        """The newest snapshot; it is written within ``snapshot_interval`` (a later one replaces it unwritten)."""
        with self._lock:
            if self._latest is None or snapshot["revision"] > self._latest["revision"]:
                self._latest = snapshot
        self._wake.set()

    def _run(self) -> None:
        while True:
            self._wake.wait()
            if self._closed.is_set():
                return
            delay = self._last_write + self._interval - self._clock()
            if delay > 0 and self._closed.wait(min(delay, self._interval)):
                return
            self._wake.clear()
            self.write_snapshot()

    def write_snapshot(self) -> bool:
        """Write the newest offered snapshot now (if there is one that is not on disk yet)."""
        with self._lock:
            snapshot, disabled = self._latest, self.disabled is not None
            if snapshot is None or disabled or snapshot["revision"] <= self._written_revision:
                return False
            self._last_write = self._clock()
            try:
                locator_module.atomic_write(self.directory / STATUS_NAME, (json.dumps(snapshot, indent=1) + "\n").encode(),
                                            fsync=self._fsync)
            except Exception as exc:
                self._disable(exc)
                return False
            self._written_revision = snapshot["revision"]
            return True

    def close(self) -> None:
        """Write the newest snapshot synchronously and release the files. Idempotent; never raises."""
        try:
            self.write_snapshot()
        finally:
            self._closed.set()
            self._wake.set()
            if self._writer.is_alive() and self._writer is not threading.current_thread():
                self._writer.join(timeout=5)
            with self._lock:
                if self._fd is not None:
                    try:
                        os.close(self._fd)
                    except OSError:
                        pass
                    self._fd = None
