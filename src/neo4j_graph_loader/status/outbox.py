"""The durable local outbox in front of the SQL status store (E15.F1.S5).

Every event is written to ``<attempt>/status-outbox/`` and ``fsync``ed BEFORE ``append`` returns; that is its durable
acknowledgement and the moment its ``seq`` is assigned. A delivery thread copies the outbox to the database in order,
at least once (the database deduplicates by ``(execution_id, seq)``), and then trims what it delivered. Database
latency or an outage therefore never reaches the code that emits events, and nothing durably acknowledged is lost
unless the outbox itself overflows.

Layout of ``status-outbox/``: ``events.<first seq>.jsonl`` segments, ``gaps.jsonl`` (durable gap records),
``ack.json`` (the highest ``seq`` the database holds, and the snapshot revision it holds), ``snapshot.json`` (the newest
snapshot only: snapshots are never queued, so overflow has nothing to drop there). Beside it, in the attempt
directory: ``status-outbox.lock`` (an ``flock`` that serializes the live writer, recovery at the next ``start`` and
``graph-loader status-store flush``) and, if the outbox itself could not be written, ``status-degraded.json``.

Bounds. When the undelivered events exceed ``max_bytes`` the oldest whole segment is dropped, but only after a gap
record (its ``seq`` range and count) is durably written to ``gaps.jsonl``; the database receives it as an event
(``type: gap``, ``seq`` = first missing ``seq``). If the outbox cannot be written at all, the journal degrades to a
bounded in-memory queue, says so once (log line and the ``outbox_unwritable`` store-error counter), writes a degraded
marker if it can, and flags the execution ``history_incomplete``; it never claims an exact gap it cannot prove.

What is lost on ``SIGKILL``: events not yet appended (nothing is buffered in memory on the healthy path, so none) and
whatever the degraded in-memory queue held. Recovery truncates a torn final line to the last complete record.
"""
from __future__ import annotations

import fcntl
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Callable

from neo4j_graph_loader.status import json_store, locator as locator_module, model

logger = logging.getLogger(__name__)

OUTBOX_DIR = "status-outbox"
LOCK_NAME = "status-outbox.lock"
DEGRADED_NAME = "status-degraded.json"
ACK_NAME = "ack.json"
SNAPSHOT_NAME = "snapshot.json"
GAPS_NAME = "gaps.jsonl"

MAX_BYTES_ENV = "GRAPH_LOADER_STATUS_OUTBOX_MB"
SEGMENT_BYTES_ENV = "GRAPH_LOADER_STATUS_OUTBOX_SEGMENT_MB"
DEFAULT_MAX_BYTES = 64 * 1024 * 1024
DEFAULT_SEGMENT_BYTES = 4 * 1024 * 1024
MAX_MEMORY_EVENTS = 20_000


class OutboxLocked(RuntimeError):
    """Another process (a live CLI, a recovery, a flush) holds this attempt's outbox lock."""


def settings_from_environment() -> dict:
    def mb(name: str, default: int) -> int:
        try:
            value = float(os.environ.get(name, ""))
        except ValueError:
            return default
        return int(value * 1024 * 1024) if value > 0 else default
    return {"max_bytes": mb(MAX_BYTES_ENV, DEFAULT_MAX_BYTES), "segment_bytes": mb(SEGMENT_BYTES_ENV, DEFAULT_SEGMENT_BYTES)}


class AttemptLock:
    """``flock`` on ``<attempt>/status-outbox.lock``. Released when the process exits, however it exits."""

    def __init__(self, attempt: Path | str):
        self.path = Path(attempt) / LOCK_NAME
        self._fd: int | None = None

    def acquire(self, *, timeout: float = 0.0) -> None:
        descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        deadline = time.monotonic() + timeout
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    os.close(descriptor)
                    raise OutboxLocked(f"the status outbox in {self.path.parent} is in use by another process") from None
                time.sleep(0.05)
        self._fd = descriptor
        os.ftruncate(descriptor, 0)
        os.write(descriptor, f"{os.getpid()}\n".encode())

    def release(self) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            finally:
                os.close(self._fd)
                self._fd = None


class Outbox:
    """The files of one attempt's outbox. Not thread-safe by itself (the journal serializes access)."""

    def __init__(self, attempt: Path | str, *, max_bytes: int = DEFAULT_MAX_BYTES, segment_bytes: int = DEFAULT_SEGMENT_BYTES,
                 fsync: bool = True, create: bool = True):
        self.attempt = Path(attempt)
        self.directory = self.attempt / OUTBOX_DIR
        self.max_bytes, self.segment_bytes, self._fsync = max_bytes, segment_bytes, fsync
        if create:
            self.directory.mkdir(mode=0o700, exist_ok=True)
        self._fd: int | None = None
        self._size = 0
        self.next_seq = 1
        self._memory_ack = {"through_seq": 0, "snapshot_revision": 0}   # what is acknowledged even if ack.json cannot be written
        self.on_gap: Callable[[dict], None] | None = None

    # -- recovery -----------------------------------------------------------------------------------------------
    def recover(self) -> int:
        """Cut a torn final line off the newest segment, and set ``next_seq`` after the last complete record. Returns
        how many bytes were cut."""
        cut = 0
        segments = json_store.list_segments(self.directory)
        for _first, path in reversed(segments):
            data = path.read_bytes()
            keep = data.rfind(b"\n") + 1
            if keep != len(data):
                with open(path, "r+b") as handle:
                    handle.truncate(keep)
                    handle.flush()
                    os.fsync(handle.fileno())
                cut += len(data) - keep
            last = self._last_seq(path)
            if last is not None:
                self.next_seq = last + 1
                break
        for gap in self._read_gaps():
            self.next_seq = max(self.next_seq, gap["to_seq"] + 1)
        self.next_seq = max(self.next_seq, self.ack()["through_seq"] + 1)
        return cut

    @staticmethod
    def _last_seq(path: Path) -> int | None:
        last = None
        for line in path.read_bytes().splitlines():
            try:
                last = json.loads(line)["seq"]
            except (ValueError, KeyError, TypeError):
                break
        return last

    # -- append (durable acknowledgement) ------------------------------------------------------------------------------
    def append(self, make_event: Callable[[int], dict]) -> dict:
        seq = self.next_seq
        event = make_event(seq)
        if event.get("seq") != seq:
            raise ValueError(f"event carries seq {event.get('seq')!r}, expected {seq}")
        data = (json.dumps(event, separators=(",", ":")) + "\n").encode()
        if self._fd is not None and self._size > 0 and self._size + len(data) > self.segment_bytes:
            os.close(self._fd)
            self._fd = None
        if self._fd is None:
            self._fd = os.open(self.directory / json_store.segment_name(seq), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_APPEND, 0o600)
            self._size = 0
            self._sync_directory()
        view = memoryview(data)
        while view:
            view = view[os.write(self._fd, view):]
        if self._fsync:
            os.fsync(self._fd)                                    # the durable acknowledgement
        self._size += len(data)
        self.next_seq = seq + 1
        self._enforce_bound()
        return event

    def close_segment(self) -> None:
        """Close the segment being written; the next append starts a new one."""
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None

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

    def _enforce_bound(self) -> None:
        """Over the bound: replace the oldest whole non-current segment by a durable gap record, then delete it."""
        while True:
            segments = json_store.list_segments(self.directory)
            if len(segments) < 2 or sum(path.stat().st_size for _f, path in segments) <= self.max_bytes:
                return
            _first, oldest = segments[0]
            through = self.ack()["through_seq"]
            seqs = []
            for line in oldest.read_bytes().splitlines():
                try:
                    seq = json.loads(line)["seq"]
                except (ValueError, KeyError, TypeError):
                    continue
                if seq > through:
                    seqs.append(seq)
            if seqs:
                gap = {"schema_version": model.SCHEMA_VERSION, "execution_id": None, "seq": seqs[0], "ts": model.timestamp(time.time()),
                       "type": "gap", "from_seq": seqs[0], "to_seq": seqs[-1], "count": len(seqs), "reason": "outbox-overflow"}
                if self.on_gap is not None:
                    self.on_gap(gap)                               # (fills in the execution id, counts, logs)
                with open(self.directory / GAPS_NAME, "ab") as handle:       # durable BEFORE the covered records go
                    handle.write((json.dumps(gap, separators=(",", ":")) + "\n").encode())
                    handle.flush()
                    os.fsync(handle.fileno())
            oldest.unlink()
            self._sync_directory()

    def _read_gaps(self) -> list[dict]:
        path = self.directory / GAPS_NAME
        gaps = []
        if path.exists():
            for line in path.read_bytes().split(b"\n")[:-1]:
                try:
                    gaps.append(json.loads(line))
                except ValueError:
                    continue
        return gaps

    # -- delivery side --------------------------------------------------------------------------------------------------
    def ack(self) -> dict:
        stored = {"through_seq": 0, "snapshot_revision": 0}
        try:
            data = json.loads((self.directory / ACK_NAME).read_text())
            stored = {"through_seq": int(data.get("through_seq", 0)), "snapshot_revision": int(data.get("snapshot_revision", 0))}
        except (OSError, ValueError, TypeError):
            pass
        return {key: max(stored[key], self._memory_ack[key]) for key in stored}

    def write_ack(self, *, through_seq: int | None = None, snapshot_revision: int | None = None) -> None:
        """Advance the watermark. When the file cannot be written (a read-only outbox that is still being drained) the
        watermark is kept in memory only: a crash then re-delivers, which the database deduplicates."""
        current = self.ack()
        updated = {"through_seq": max(current["through_seq"], through_seq or 0),
                   "snapshot_revision": max(current["snapshot_revision"], snapshot_revision or 0)}
        self._memory_ack = updated
        try:
            locator_module.atomic_write(self.directory / ACK_NAME, (json.dumps(updated) + "\n").encode(), fsync=self._fsync)
        except OSError:
            pass

    def pending(self, execution_id: str, limit: int = 200) -> list[dict]:
        """Undelivered records (events and gap records) in ``seq`` order, at most ``limit``."""
        through = self.ack()["through_seq"]
        records: dict[int, dict] = {}
        for gap in self._read_gaps():
            if gap["to_seq"] > through:
                records[gap["from_seq"]] = {**gap, "execution_id": execution_id}
        covered = [(g["from_seq"], g["to_seq"]) for g in records.values()]
        for first, path in json_store.list_segments(self.directory):
            try:
                data = path.read_bytes()
            except FileNotFoundError:                                  # trimmed or dropped while we were reading
                continue
            for line in data[:data.rfind(b"\n") + 1].splitlines():
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                seq = event.get("seq")
                if isinstance(seq, int) and seq > through and not any(a <= seq <= b and seq != a for a, b in covered):
                    records.setdefault(seq, event)
            if len(records) >= limit * 4:
                break
        return [records[seq] for seq in sorted(records)][:limit]

    def trim(self) -> None:
        """Delete segments (and gap records) the database already holds."""
        through = self.ack()["through_seq"]
        segments = json_store.list_segments(self.directory)
        for index, (_first, path) in enumerate(segments[:-1]):
            following = segments[index + 1][0]
            if following - 1 <= through:
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    return                                             # read-only: nothing to trim, the watermark still holds
        remaining = [gap for gap in self._read_gaps() if gap["to_seq"] > through]
        gaps_path = self.directory / GAPS_NAME
        if gaps_path.exists():
            try:
                locator_module.atomic_write(gaps_path, b"".join((json.dumps(gap, separators=(",", ":")) + "\n").encode() for gap in remaining),
                                            fsync=self._fsync)
            except OSError:
                pass

    def has_pending(self) -> bool:
        return bool(self.pending("x", limit=1)) or self.snapshot_pending()

    def write_snapshot(self, snapshot: dict) -> None:
        locator_module.atomic_write(self.directory / SNAPSHOT_NAME, (json.dumps(snapshot) + "\n").encode(), fsync=self._fsync)

    def read_snapshot(self) -> dict | None:
        try:
            return json.loads((self.directory / SNAPSHOT_NAME).read_text())
        except (OSError, ValueError):
            return None

    def snapshot_pending(self) -> bool:
        snapshot = self.read_snapshot()
        return snapshot is not None and snapshot.get("revision", 0) > self.ack()["snapshot_revision"]

    def bytes_used(self) -> int:
        return sum(path.stat().st_size for _f, path in json_store.list_segments(self.directory))


def write_degraded_marker(attempt: Path | str, reason: str) -> bool:
    """Best effort: record that this run's history may be incomplete (read by recovery). Never raises."""
    try:
        locator_module.atomic_write(Path(attempt) / DEGRADED_NAME, (json.dumps({"reason": reason, "at": model.timestamp(time.time())}) + "\n").encode())
        return True
    except OSError:
        return False


def deliver(outbox: Outbox, store, execution_id: str, *, batch: int = 200, deadline: float | None = None,
            history_incomplete: bool = False) -> dict:
    """Deliver everything pending to ``store`` (at least once; the store deduplicates). Returns counts. Raises the
    store's exception when the database refuses; progress made before that is kept (the ack watermark)."""
    delivered = {"events": 0, "snapshot": False}
    snapshot = outbox.read_snapshot()
    if snapshot is not None and snapshot.get("revision", 0) > outbox.ack()["snapshot_revision"]:
        if history_incomplete:
            snapshot = {**snapshot, "history_incomplete": True}
        store.write_snapshot(snapshot)                       # False (an older revision is there already) is fine too
        outbox.write_ack(snapshot_revision=snapshot["revision"])
        delivered["snapshot"] = True
    while True:
        if deadline is not None and time.monotonic() >= deadline:
            break
        records = outbox.pending(execution_id, batch)
        if not records:
            break
        store.write_events(records)
        outbox.write_ack(through_seq=max(max(r["seq"], r.get("to_seq", r["seq"])) for r in records))
        outbox.trim()
        delivered["events"] += len(records)
    return delivered


class OutboxJournal:
    """Tracker journal and snapshot sink: events go to the outbox; a thread delivers them to ``store``."""

    def __init__(self, attempt: Path | str, store, execution_id: str, *, snapshot_interval: float = 2.0, retry_seconds: float = 1.0,
                 max_retry_seconds: float = 15.0, on_degraded: Callable[[], None] | None = None,
                 on_error: Callable[[str], None] | None = None, fsync: bool = True, lock: AttemptLock | None = None,
                 settings: dict | None = None):
        self.attempt = Path(attempt)
        self._store, self._execution_id = store, execution_id
        self._interval, self._retry, self._max_retry = snapshot_interval, retry_seconds, max_retry_seconds
        self._on_degraded, self._on_error = on_degraded, on_error
        self._lock = threading.RLock()                       # re-entered when degrading calls back into the tracker
        self._lockfile = lock
        self._memory: list[dict] = []                        # only when the outbox is unusable
        self._next_seq = 1
        self.degraded: str | None = None
        self.dropped = 0
        self.failures = 0
        self._snapshot: dict | None = None
        self._persisted_revision = 0
        self._delivered_revision = 0
        self._last_persist = float("-inf")
        self._warned = False
        self._deadline = float("inf")
        self._wake, self._closed = threading.Event(), threading.Event()
        try:
            self._outbox: Outbox | None = Outbox(self.attempt, fsync=fsync, **(settings or settings_from_environment()))
            self._outbox.recover()
            self._next_seq = self._outbox.next_seq
            self._outbox.on_gap = self._count_overflow
        except OSError as exc:
            self._outbox = None
            self._degrade(exc)
        self._thread = threading.Thread(target=self._run, name="status-outbox-delivery", daemon=True)
        self._thread.start()

    # -- degradation -------------------------------------------------------------------------------------------------
    def _count_overflow(self, gap: dict) -> None:
        gap["execution_id"] = self._execution_id
        if self._on_error:
            self._on_error("outbox_overflow")
        logger.warning("stage=status reason=the status outbox is over its bound: events %s-%s (%s) were replaced by a gap record",
                       gap["from_seq"], gap["to_seq"], gap["count"])

    def _degrade(self, exc: BaseException) -> None:
        if self.degraded is None:
            self.degraded = f"{type(exc).__name__}: {exc}"
            logger.warning("stage=status reason=the status outbox cannot be written (%s); events are queued in memory only and "
                           "history is flagged incomplete (a kill now loses them)", self.degraded)
            write_degraded_marker(self.attempt, self.degraded)
            if self._on_error:
                self._on_error("outbox_unwritable")
            if self._on_degraded:
                try:
                    self._on_degraded()
                except Exception:                                     # noqa: BLE001
                    pass

    # -- journal protocol ---------------------------------------------------------------------------------------------
    def append(self, make_event: Callable[[int], dict]) -> dict:
        with self._lock:
            if self._outbox is not None and self.degraded is None:
                try:
                    event = self._outbox.append(make_event)
                    self._next_seq = event["seq"] + 1
                    self._wake.set()
                    return event
                except OSError as exc:
                    self._degrade(exc)
                    self._next_seq = self._outbox.next_seq
            event = make_event(self._next_seq)
            self._next_seq += 1
            if len(self._memory) >= MAX_MEMORY_EVENTS:
                self._memory.pop(0)
                self.dropped += 1
            self._memory.append(event)
        self._wake.set()
        return event

    def offer_snapshot(self, snapshot: dict) -> None:
        with self._lock:
            if self._snapshot is None or snapshot["revision"] > self._snapshot["revision"]:
                self._snapshot = snapshot
        self._wake.set()

    # -- the delivery thread ---------------------------------------------------------------------------------------------
    def _persist_snapshot(self, force: bool = False) -> None:
        with self._lock:
            snapshot = self._snapshot
        if snapshot is None or snapshot["revision"] <= self._persisted_revision:
            return
        if not force and time.monotonic() - self._last_persist < self._interval:
            return
        if self._outbox is not None and self.degraded is None:
            try:
                self._outbox.write_snapshot(snapshot)
            except OSError as exc:
                self._degrade(exc)                                    # (outside the journal lock: it calls into the tracker)
        self._last_persist = time.monotonic()
        self._persisted_revision = snapshot["revision"]

    def _deliver_once(self) -> None:
        self._persist_snapshot(force=self._closed.is_set())
        with self._lock:
            snapshot, memory = self._snapshot, list(self._memory)
        if self._outbox is not None:                                 # still readable even when it can no longer be written
            deliver(self._outbox, self._store, self._execution_id, deadline=self._deadline if self._closed.is_set() else None)
        if snapshot is not None and snapshot["revision"] > self._delivered_revision:
            document = {**snapshot, "history_incomplete": True} if (self.degraded or self.dropped) else snapshot
            self._store.write_snapshot(document)
            self._delivered_revision = snapshot["revision"]
            if self._outbox is not None and self.degraded is None:
                try:
                    self._outbox.write_ack(snapshot_revision=snapshot["revision"])
                except OSError:
                    pass
        if memory:
            self._store.write_events(memory)
            with self._lock:
                del self._memory[:len(memory)]

    def _drained(self) -> bool:
        with self._lock:
            snapshot_done = self._snapshot is None or self._snapshot["revision"] <= self._delivered_revision
            if self._memory or not snapshot_done:
                return False
        return self._outbox is None or not self._outbox.has_pending()

    def _run(self) -> None:
        delay = self._retry
        while True:
            self._wake.wait(timeout=self._interval)
            self._closed.wait(0.05)                                   # let a burst of events become one batch
            self._wake.clear()
            try:
                self._deliver_once()
                delay = self._retry
            except Exception as exc:                                  # noqa: BLE001 - the database refused: retry later
                self.failures += 1
                if self._on_error:
                    self._on_error("write_failed")
                if not self._warned:
                    self._warned = True
                    logger.warning("stage=status reason=the status database is not accepting writes; events wait in the local outbox "
                                   "and are delivered when it does (the load continues): %s", exc)
                if self._closed.is_set():
                    if time.monotonic() >= self._deadline:
                        return
                    time.sleep(min(delay, 0.1))
                else:
                    self._closed.wait(delay)
                delay = min(delay * 2, self._max_retry)
                self._wake.set()
                continue
            if self._closed.is_set() and self._drained():
                return

    def close(self, deadline_seconds: float | None = None) -> bool:
        """Persist the newest snapshot, wait up to the deadline for delivery, release the lock. Returns whether
        everything was delivered; otherwise the outbox stays on disk for ``status-store flush`` / the next start."""
        if deadline_seconds is None:
            try:
                deadline_seconds = float(os.environ.get("GRAPH_LOADER_STATUS_SHUTDOWN_DEADLINE_S", "10"))
            except ValueError:
                deadline_seconds = 10.0
        self._deadline = time.monotonic() + deadline_seconds
        self._closed.set()
        self._wake.set()
        self._thread.join(timeout=deadline_seconds + 1)
        self._persist_snapshot(force=True)
        done = self._drained() and not self._thread.is_alive()
        if not done and self._outbox is not None and self.degraded is None:
            logger.warning("stage=status reason=the status database did not take everything before the shutdown deadline; the local "
                           "outbox keeps the rest, deliver it with `graph-loader status-store flush --run-dir %s`", self.attempt)
        if done:
            try:
                self._store.close()
            except Exception:                                         # noqa: BLE001
                pass
        if self._lockfile is not None:
            self._lockfile.release()
        return done


# ----------------------------------------------------------------------------------------------------------------------
# Recovery: deliver what a finished, killed or crashed run left in its outbox (``status-store flush`` and the next start)
# ----------------------------------------------------------------------------------------------------------------------
def recover_attempt(attempt: Path | str, *, store=None, lock_timeout: float = 0.0) -> dict:
    """Deliver the outbox of ``attempt`` to the database its locator names. Returns
    ``{"attempt", "execution_id", "events", "snapshot", "cut_bytes", "history_incomplete"}``.

    Takes the attempt's lock (waiting up to ``lock_timeout``; :class:`OutboxLocked` when a live process owns it),
    truncates a torn tail, resolves the destination from the locator and the credentials from the environment (never
    from the outbox), and replays idempotently: an interrupted replay resumes from the ack watermark and anything
    delivered twice is deduplicated by the database.
    """
    from neo4j_graph_loader.status import sql_store
    attempt = Path(attempt)
    locator = locator_module.read_locator(attempt)
    if locator["store"] != "sql":
        raise ValueError(f"{attempt} records its status in the {locator['store']!r} store: there is no outbox to deliver")
    lock = AttemptLock(attempt)
    lock.acquire(timeout=lock_timeout)
    own_store = store is None
    try:
        outbox = Outbox(attempt, create=False)
        if not outbox.directory.is_dir():
            return {"attempt": str(attempt), "execution_id": locator["execution_id"], "events": 0, "snapshot": False,
                    "cut_bytes": 0, "history_incomplete": False}
        cut = outbox.recover()
        store = store or sql_store._store_for(locator)
        degraded = (attempt / DEGRADED_NAME).exists()
        result = deliver(outbox, store, locator["execution_id"], history_incomplete=degraded)
        return {"attempt": str(attempt), "execution_id": locator["execution_id"], **result, "cut_bytes": cut,
                "history_incomplete": degraded}
    finally:
        if own_store and store is not None:
            store.close()
        lock.release()


def pending_attempts(base: Path | str, *, exclude: Path | str | None = None) -> list[Path]:
    """Attempt directories under ``base`` whose outbox still holds undelivered records (locator store ``sql``)."""
    found = []
    for attempt in sorted(Path(base).glob("graph-loader-*")):
        if exclude is not None and attempt.resolve() == Path(exclude).resolve():
            continue
        try:
            locator = locator_module.read_locator(attempt)
        except locator_module.LocatorError:
            continue
        if locator["store"] != "sql" or not (attempt / OUTBOX_DIR).is_dir():
            continue
        outbox = Outbox(attempt, create=False)
        if outbox.has_pending() or (attempt / DEGRADED_NAME).exists() and outbox.ack()["snapshot_revision"] == 0:
            found.append(attempt)
    return found
