"""Run-scoped dispatcher for a globally planned bulk relationship load.

The dispatcher loads the plan (:mod:`src.loader.edge_plan`) and hands whole
chunks to writers at run time: ``NEXT`` grants the highest-ranked chunk whose
resources are all free, whose predecessor in its unit committed, and whose
two endpoint labels are ready (their node loaders drained).  The granted
chunk's resources stay held until its writer reports ``DONE`` after the
Neo4j commit, so concurrently running transactions are node-disjoint and
never wait on each other's locks.  Each chunk is granted at most once; the
completion ledger records every ``DONE``.

Failure is closed: ``FAIL`` from a writer, ``ABORT`` from the orchestrator,
a protocol violation, or a writer disconnecting while it holds a chunk moves
the dispatcher to ``failed``.  A failed dispatcher never grants again and
never releases held resources (the disconnected writer's transaction may
still be running in Neo4j), so no later transaction can wait on a zombie's
locks; the run fails and MERGE makes a full replay safe.

A writer that is asked to stop (``SIGTERM``) is only ever stopped *between*
chunks, holding nothing: it finishes and reports ``DONE`` for any granted
chunk first, then exits 0 without sending ``FAIL`` (P6-S5 Story 4).  A
``FAIL`` therefore always means a genuine write failure, never a shutdown.

Prefetch hints: each grant carries one *hint*, the chunk this writer will
most likely get next (its unit's successor, else the best chunk that is free
apart from the writer's own resources).  The writer prefetches the hinted
message while its transaction runs.  When the writer asks again, its hint is
preferred if it is eligible and ranked within ``hint_slack`` of the best
eligible chunk; chunks hinted to other writers are passed over unless
nothing else is eligible.

Line protocol (ASCII, one request and one ``\\n``-terminated reply at a time
per persistent TCP connection):

``HELLO WRITER <writer_id> <plan_digest>`` / ``HELLO CONTROL <plan_digest>``
    ``OK <plan_id>``, or ``ERR`` on a digest mismatch (fail closed).
``NEXT <wait_ms>`` (writer)
    ``OK <chunk_id> <hint_id|->``, ``WAIT`` (nothing eligible within
    ``wait_ms``), ``END`` (every chunk is done), or ``ERR failed <reason>``.
``DONE <chunk_id> <rows> <write_ms>`` (writer)
    ``OK``; the chunk must be this writer's grant with its planned rows.
``FAIL <chunk_id|-> <reason>`` (writer) / ``ABORT <reason>`` (control)
    ``OK``; the dispatcher is failed.
``READY <label> [<label> ...]`` (control)
    ``OK``; chunks of those labels become eligible.
``STATUS`` / ``SUMMARY``
    ``OK <json>`` -- progress counters / the full completion ledger.
``PING``
    ``OK``.
"""
from __future__ import annotations

import argparse
import asyncio
import bisect
from dataclasses import dataclass
import itertools
import json
import logging
import re
import os
import signal
import time
from typing import Callable, Iterable

from src.loader.edge_plan import EdgePlan, read_plan_file
from src.utils import metrics as metrics_module


logger = logging.getLogger(__name__)

DEFAULT_DISPATCHER_PORT = 7701
DEFAULT_HINT_SLACK = 32
MAX_WAIT_MS = 60_000
MAX_LINE_BYTES = 1 << 16
_TOKEN = re.compile(r"[A-Za-z0-9_.-]{1,64}")
_LABEL = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

PENDING, GRANTED, DONE = 0, 1, 2
END = "END"


class DispatchError(RuntimeError):
    """A dispatcher request violated the protocol or the plan."""


@dataclass
class DispatchStats:
    grants: int = 0
    hint_grants: int = 0
    hinted: int = 0
    stolen_hints: int = 0
    waits: int = 0
    wait_timeouts: int = 0
    total_wait_ms: float = 0.0
    active: int = 0
    max_active: int = 0
    completed: int = 0
    node_phase_deferrals: int = 0

    def as_dict(self) -> dict:
        return dict(self.__dict__)


@dataclass
class LedgerEntry:
    chunk: int
    writer: str
    rows: int
    granted_at: float
    done_at: float
    write_ms: float


class DispatchState:
    """Single-threaded chunk grant/complete state machine (no I/O)."""

    def __init__(self, plan: EdgePlan, *, ready_labels: Iterable[str] = (),
                 hint_slack: int = DEFAULT_HINT_SLACK, clock: Callable[[], float] = time.time,
                 node_phase_limit: int | None = None,
                 metrics: "metrics_module.GraphLoaderMetrics | None" = None) -> None:
        if isinstance(hint_slack, bool) or not isinstance(hint_slack, int) or hint_slack < 0:
            raise ValueError("hint slack must be a nonnegative integer")
        if node_phase_limit is None:
            node_phase_limit = int(os.environ.get("GRAPH_LOADER_PLAN_NODE_PHASE_WRITERS", "0") or 0)
        if isinstance(node_phase_limit, bool) or not isinstance(node_phase_limit, int) or node_phase_limit < 0:
            raise ValueError("node phase limit must be a nonnegative integer")
        # While any planned endpoint label still loads, cap active chunks so node
        # writes keep Neo4j CPU (0 = unlimited); the dependent edges wait on them.
        self.node_phase_limit = node_phase_limit
        self._plan_labels = {label for chunk in plan.chunks for label in plan.labels_of(chunk)}
        self.plan = plan
        self.hint_slack = hint_slack
        self._clock = clock
        chunks = plan.chunks
        self._rank_to_chunk = [0] * len(chunks)
        self._rank = [0] * len(chunks)
        self._mask = [0] * len(chunks)
        self._labels = [plan.labels_of(chunk) for chunk in chunks]
        self._successor = [-1] * len(chunks)
        for chunk in chunks:
            self._rank_to_chunk[chunk.rank] = chunk.id
            self._rank[chunk.id] = chunk.rank
            mask = 0
            for resource in chunk.resources:
                mask |= 1 << resource
            self._mask[chunk.id] = mask
            if chunk.after != -1:
                self._successor[chunk.after] = chunk.id
        self._state = [PENDING] * len(chunks)
        self._held = 0
        self.ready_labels: set[str] = set()
        # Ranks of pending chunks whose labels are ready and predecessor done.
        self._available: list[int] = []
        self.grants: dict[str, int] = {}
        self._granted_at: dict[int, float] = {}
        self._hint_of: dict[str, int] = {}
        self._hint_owner: dict[int, str] = {}
        self.ledger: list[LedgerEntry] = []
        self.failure: str | None = None
        self.stats = DispatchStats()
        self.rows_done = 0
        self.started_at = clock()
        self._metrics = metrics if metrics is not None else metrics_module.process_metrics()
        # Set once, at dispatcher start, per P6-S2 Story 6's AC - never
        # changes for the life of the run. Routed through safe_emit like
        # every other hot-path emission in this slice.
        metrics_module.safe_emit(lambda: self._metrics.plan_chunks_planned.labels(
            kind="planned-dispatcher"
        ).set(len(plan.chunks)))
        self.mark_ready(ready_labels)

    # -- queries -----------------------------------------------------------------

    @property
    def total(self) -> int:
        return len(self.plan.chunks)

    @property
    def complete(self) -> bool:
        return self.failure is None and self.stats.completed == self.total

    @property
    def state(self) -> str:
        if self.failure is not None:
            return "failed"
        return "complete" if self.complete else "running"

    def held_resources(self) -> set[int]:
        return {index for index in range(self._held.bit_length()) if self._held >> index & 1}

    def status(self) -> dict:
        return {
            "state": self.state, "plan_id": self.plan.plan_id, "done": self.stats.completed,
            "total": self.total, "rows_done": self.rows_done, "rows_total": self.plan.total_rows,
            "granted": sorted(self.grants.values()), "ready_labels": sorted(self.ready_labels),
            "failure": self.failure, "stats": self.stats.as_dict(),
        }

    def summary(self) -> dict:
        types: dict[str, dict] = {}
        for entry in self.ledger:
            chunk = self.plan.chunks[entry.chunk]
            item = types.setdefault(chunk.edge_type, {
                "rows": 0, "chunks": 0, "write_ms": 0.0, "first_grant": entry.granted_at, "last_done": entry.done_at,
            })
            item["rows"] += entry.rows
            item["chunks"] += 1
            item["write_ms"] += entry.write_ms
            item["first_grant"] = min(item["first_grant"], entry.granted_at)
            item["last_done"] = max(item["last_done"], entry.done_at)
        return {
            **self.status(),
            "started_at": self.started_at,
            "ledger": [
                [entry.chunk, entry.writer, entry.rows, round(entry.granted_at, 6), round(entry.done_at, 6),
                 round(entry.write_ms, 3)]
                for entry in self.ledger
            ],
            "types": types,
        }

    # -- transitions ---------------------------------------------------------------

    def _is_available(self, chunk_id: int) -> bool:
        chunk = self.plan.chunks[chunk_id]
        source, target = self._labels[chunk_id]
        return (
            self._state[chunk_id] == PENDING
            and source in self.ready_labels and target in self.ready_labels
            and (chunk.after == -1 or self._state[chunk.after] == DONE)
        )

    def _make_available(self, chunk_id: int) -> None:
        rank = self._rank[chunk_id]
        index = bisect.bisect_left(self._available, rank)
        if index == len(self._available) or self._available[index] != rank:
            self._available.insert(index, rank)

    def mark_ready(self, labels: Iterable[str]) -> bool:
        """Make chunks whose endpoint labels are now all ready eligible."""
        new = {label for label in labels} - self.ready_labels
        for label in new:
            if not isinstance(label, str) or not _LABEL.fullmatch(label):
                raise DispatchError(f"invalid label {label!r}")
        if not new:
            return False
        self.ready_labels |= new
        for chunk_id in range(self.total):
            if self._is_available(chunk_id) and any(label in new for label in self._labels[chunk_id]):
                self._make_available(chunk_id)
        return True

    def fail(self, reason: str) -> None:
        if self.failure is None:
            self.failure = reason or "failed"
            logger.error("stage=planned-dispatch-failed reason=%s", self.failure)

    def _select(self, writer: str) -> int | None:
        held = self._held
        best = fallback = None
        for rank in self._available:
            chunk_id = self._rank_to_chunk[rank]
            if self._mask[chunk_id] & held:
                continue
            owner = self._hint_owner.get(chunk_id)
            if owner is None or owner == writer:
                best = chunk_id
                break
            if fallback is None:
                fallback = chunk_id
        own = self._hint_of.get(writer)
        if (
            own is not None and self._state[own] == PENDING and self._is_available(own)
            and not self._mask[own] & held
            and (best is None or self._rank[own] <= self._rank[best] + self.hint_slack)
        ):
            return own
        if best is not None:
            return best
        return fallback

    def _choose_hint(self, writer: str, granted: int) -> int | None:
        successor = self._successor[granted]
        if successor != -1 and self._hint_owner.get(successor, writer) == writer:
            return successor
        others = self._held & ~self._mask[granted]
        for rank in self._available:
            chunk_id = self._rank_to_chunk[rank]
            if self._mask[chunk_id] & others:
                continue
            if self._hint_owner.get(chunk_id, writer) == writer:
                return chunk_id
        return None

    def _set_hint(self, writer: str, chunk_id: int | None) -> None:
        previous = self._hint_of.pop(writer, None)
        if previous is not None and self._hint_owner.get(previous) == writer:
            del self._hint_owner[previous]
        if chunk_id is not None:
            self._hint_of[writer] = chunk_id
            self._hint_owner[chunk_id] = writer
            self.stats.hinted += 1

    def grant(self, writer: str) -> tuple[int, int | None] | str | None:
        """Grant ``writer`` its next chunk: ``(chunk, hint)``, ``END``, or ``None`` (nothing eligible)."""
        if self.failure is not None:
            raise DispatchError(f"failed {self.failure}")
        if writer in self.grants:
            raise DispatchError(f"writer {writer} already holds chunk {self.grants[writer]}")
        if self.stats.completed == self.total:
            return END
        if (
            self.node_phase_limit and self.stats.active >= self.node_phase_limit
            and not self._plan_labels <= self.ready_labels
        ):
            self.stats.node_phase_deferrals += 1
            return None
        chunk_id = self._select(writer)
        if chunk_id is None:
            return None
        mask = self._mask[chunk_id]
        if mask & self._held:
            raise AssertionError("grant of a held resource")
        owner = self._hint_owner.get(chunk_id)
        if owner == writer:
            self.stats.hint_grants += 1
        elif owner is not None:
            self.stats.stolen_hints += 1
            del self._hint_owner[chunk_id]
            self._hint_of.pop(owner, None)
        self._held |= mask
        self._state[chunk_id] = GRANTED
        del self._available[bisect.bisect_left(self._available, self._rank[chunk_id])]
        self.grants[writer] = chunk_id
        self._granted_at[chunk_id] = self._clock()
        stats = self.stats
        stats.grants += 1
        stats.active += 1
        stats.max_active = max(stats.max_active, stats.active)
        metrics_module.safe_emit(lambda: self._metrics.plan_active_writers.labels(
            kind="planned-dispatcher"
        ).set(stats.active))
        hint = self._choose_hint(writer, chunk_id)
        self._set_hint(writer, hint)
        return chunk_id, hint

    def done(self, writer: str, chunk_id: int, rows: int, write_ms: float = 0.0) -> None:
        """Record ``writer``'s committed chunk and free its resources."""
        held = self.grants.get(writer)
        if held is None or held != chunk_id:
            reason = f"writer {writer} reported DONE for chunk {chunk_id} it does not hold"
            self.fail(reason)
            raise DispatchError(reason)
        chunk = self.plan.chunks[chunk_id]
        if rows != chunk.rows:
            reason = f"writer {writer} committed {rows} rows for chunk {chunk_id}; plan has {chunk.rows}"
            self.fail(reason)
            raise DispatchError(reason)
        del self.grants[writer]
        self._state[chunk_id] = DONE
        self._held &= ~self._mask[chunk_id]
        self.stats.active -= 1
        self.stats.completed += 1
        stats = self.stats
        metrics_module.safe_emit(lambda: self._metrics.plan_active_writers.labels(
            kind="planned-dispatcher"
        ).set(stats.active))
        metrics_module.safe_emit(lambda: self._metrics.plan_chunks_committed.labels(
            kind="planned-dispatcher"
        ).set(stats.completed))
        self.rows_done += rows
        self.ledger.append(LedgerEntry(chunk_id, writer, rows, self._granted_at.pop(chunk_id), self._clock(),
                                       float(write_ms)))
        successor = self._successor[chunk_id]
        if successor != -1 and self._is_available(successor):
            self._make_available(successor)

    def disconnect(self, writer: str) -> None:
        """Forget a writer; one that still holds a chunk fails the run closed."""
        held = self.grants.get(writer)
        self._set_hint(writer, None)
        if held is not None:
            self.fail(f"writer {writer} disconnected while holding chunk {held}")


@dataclass
class _Waiter:
    seq: int
    writer: str
    future: asyncio.Future
    started: float
    timer: asyncio.TimerHandle | None = None


@dataclass
class _Connection:
    role: str | None = None
    writer_id: str | None = None


class PlannedDispatcher:
    """Asyncio TCP front end of :class:`DispatchState`; all state on one loop."""

    def __init__(self, plan: EdgePlan, digest: str, *, ready_labels: Iterable[str] = (),
                 hint_slack: int = DEFAULT_HINT_SLACK, max_wait_ms: int = MAX_WAIT_MS,
                 clock: Callable[[], float] = time.time,
                 metrics: "metrics_module.GraphLoaderMetrics | None" = None) -> None:
        self._metrics = metrics if metrics is not None else metrics_module.process_metrics()
        self.state = DispatchState(
            plan, ready_labels=ready_labels, hint_slack=hint_slack, clock=clock,
            metrics=self._metrics,
        )
        self.digest = digest
        self._max_wait_ms = max_wait_ms
        self._seq = itertools.count(1)
        self._waiters: dict[int, _Waiter] = {}
        self._writers: set[str] = set()
        self._server: asyncio.base_events.Server | None = None
        self._connections: set[asyncio.StreamWriter] = set()
        self.finished = asyncio.Event()

    async def start(self, host: str = "127.0.0.1", port: int = 0) -> tuple[str, int]:
        self._server = await asyncio.start_server(self._handle, host, port, limit=MAX_LINE_BYTES)
        sockname = self._server.sockets[0].getsockname()
        return sockname[0], sockname[1]

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            for waiter in tuple(self._waiters.values()):
                self._resolve(waiter, "ERR dispatcher closed\n")
            for writer in tuple(self._connections):
                writer.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        connection = _Connection()
        self._connections.add(writer)
        try:
            while True:
                try:
                    line = await reader.readline()
                except (ValueError, asyncio.LimitOverrunError):
                    writer.write(b"ERR request too long\n")
                    break
                if not line or not line.endswith(b"\n"):
                    break
                response = await self._dispatch(connection, line)
                writer.write(response.encode("ascii"))
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError, OSError):
            pass
        finally:
            if connection.role == "WRITER" and connection.writer_id is not None:
                self._writers.discard(connection.writer_id)
                for waiter in tuple(self._waiters.values()):
                    if waiter.writer == connection.writer_id:
                        self._remove(waiter)
                        if not waiter.future.done():
                            waiter.future.cancel()
                self.state.disconnect(connection.writer_id)
                self._changed()
            self._connections.discard(writer)
            try:
                writer.close()
            except Exception:  # pragma: no cover - best effort
                pass

    async def _dispatch(self, connection: _Connection, line: bytes) -> str:
        try:
            parts = line.decode("ascii").split()
        except UnicodeDecodeError:
            return "ERR request is not ascii\n"
        if not parts:
            return "ERR empty request\n"
        command, args = parts[0], parts[1:]
        state = self.state
        try:
            if command == "PING":
                return "OK\n"
            if command == "HELLO":
                return self._hello(connection, args)
            if connection.role is None:
                return "ERR HELLO required\n"
            if command == "STATUS":
                return "OK " + json.dumps(state.status(), separators=(",", ":")) + "\n"
            if command == "SUMMARY":
                return "OK " + json.dumps(state.summary(), separators=(",", ":")) + "\n"
            if connection.role == "CONTROL":
                if command == "READY":
                    if not args:
                        raise DispatchError("READY requires labels")
                    if state.mark_ready(args):
                        logger.info("stage=planned-ready labels=%s", ",".join(sorted(args)))
                        self._changed()
                    return "OK\n"
                if command == "ABORT":
                    state.fail("aborted by orchestrator: " + (" ".join(args) or "-"))
                    self._changed()
                    return "OK\n"
                raise DispatchError("unknown control command")
            writer_id = connection.writer_id
            if command == "NEXT":
                if len(args) != 1 or not args[0].isdigit() or int(args[0]) > self._max_wait_ms:
                    raise DispatchError("NEXT requires wait_ms")
                return await self._next(writer_id, int(args[0]))
            if command == "DONE":
                if len(args) != 3 or not args[0].isdigit() or not args[1].isdigit():
                    raise DispatchError("DONE requires chunk, rows and write_ms")
                try:
                    write_ms = float(args[2])
                except ValueError:
                    raise DispatchError("invalid write_ms") from None
                chunk_id, rows = int(args[0]), int(args[1])
                state.done(writer_id, chunk_id, rows, write_ms)
                chunk = state.plan.chunks[chunk_id]
                logger.info(
                    "stage=planned-done chunk=%s edge=%s rows=%s writer=%s write_ms=%.3f done=%s/%s",
                    chunk_id, chunk.edge_type, rows, writer_id, write_ms, state.stats.completed, state.total,
                )
                self._changed()
                return "OK\n"
            if command == "FAIL":
                state.fail(f"writer {writer_id} failed chunk {args[0] if args else '-'}: "
                           + (" ".join(args[1:]) or "-"))
                self._changed()
                return "OK\n"
            raise DispatchError("unknown writer command")
        except DispatchError as exc:
            if connection.role == "WRITER" and command in {"NEXT", "DONE"}:
                state.fail(str(exc))
                self._changed()
            return f"ERR {exc}\n"

    def _hello(self, connection: _Connection, args: list[str]) -> str:
        if connection.role is not None:
            raise DispatchError("HELLO already accepted")
        if len(args) == 3 and args[0] == "WRITER":
            writer_id, digest = args[1], args[2]
            if not _TOKEN.fullmatch(writer_id):
                raise DispatchError("invalid writer id")
            if writer_id in self._writers:
                raise DispatchError(f"writer {writer_id} is already connected")
            role = "WRITER"
        elif len(args) == 2 and args[0] == "CONTROL":
            writer_id, digest = None, args[1]
            role = "CONTROL"
        else:
            raise DispatchError("HELLO requires WRITER <id> <digest> or CONTROL <digest>")
        if digest != self.digest:
            return f"ERR plan digest mismatch server={self.digest} client={digest}\n"
        connection.role, connection.writer_id = role, writer_id
        if writer_id is not None:
            self._writers.add(writer_id)
        return f"OK {self.state.plan.plan_id}\n"

    def _grant_reply(self, writer_id: str) -> str | None:
        state = self.state
        if state.failure is not None:
            return f"ERR failed {state.failure}\n"
        result = state.grant(writer_id)
        if result is None:
            return None
        if result == END:
            return "END\n"
        chunk_id, hint = result
        return f"OK {chunk_id} {'-' if hint is None else hint}\n"

    async def _next(self, writer_id: str, wait_ms: int) -> str:
        if any(waiter.writer == writer_id for waiter in self._waiters.values()):
            raise DispatchError("writer already waits")
        reply = self._grant_reply(writer_id)
        if reply is not None:
            return reply
        if wait_ms == 0:
            return "WAIT\n"
        loop = asyncio.get_running_loop()
        waiter = _Waiter(next(self._seq), writer_id, loop.create_future(), time.monotonic())
        self._waiters[waiter.seq] = waiter
        self.state.stats.waits += 1
        waiter.timer = loop.call_later(wait_ms / 1000, self._expire, waiter.seq)
        return await waiter.future

    def _remove(self, waiter: _Waiter) -> None:
        self._waiters.pop(waiter.seq, None)
        if waiter.timer is not None:
            waiter.timer.cancel()

    def _resolve(self, waiter: _Waiter, reply: str) -> None:
        self._remove(waiter)
        wait_seconds = time.monotonic() - waiter.started
        # P6-S2 review finding S6-2: close() also resolves every still-
        # parked waiter with "ERR dispatcher closed" - deliberately left
        # counted here exactly like total_wait_ms already counts it
        # (right above), rather than skipped only for this metric, so the
        # log's total_wait_ms and this histogram never disagree about
        # what "one wait" means. This means a shutdown mid-run can add one
        # or more long observations that are shutdown latency, not real
        # contention - documented, not treated as a bug, matching
        # P5-Q5's "at-least-once, don't over-index on exact values"
        # precedent for this catalogue's counters.
        self.state.stats.total_wait_ms += wait_seconds * 1000
        # One observation per wait *resolution* (P6-S2 Story 6), not
        # accumulated into a running total the way total_wait_ms already
        # is - this is the histogram-per-event shape used everywhere else
        # in this catalogue.
        metrics_module.safe_emit(lambda: self._metrics.plan_resource_wait.labels(
            kind="planned-dispatcher"
        ).observe(wait_seconds))
        if not waiter.future.done():
            waiter.future.set_result(reply)

    def _expire(self, seq: int) -> None:
        waiter = self._waiters.get(seq)
        if waiter is not None:
            self.state.stats.wait_timeouts += 1
            self._resolve(waiter, "WAIT\n")

    def _changed(self) -> None:
        """Serve parked writers in arrival order after any state change."""
        for waiter in sorted(self._waiters.values(), key=lambda item: item.seq):
            try:
                reply = self._grant_reply(waiter.writer)
            except DispatchError as exc:
                reply = f"ERR {exc}\n"
            if reply is not None:
                self._resolve(waiter, reply)
        if self.state.state != "running":
            self.finished.set()


async def serve(plan_path: str, host: str, port: int, *, stop: asyncio.Event,
                ready_labels: Iterable[str] = (), hint_slack: int = DEFAULT_HINT_SLACK) -> DispatchState:
    plan, digest = read_plan_file(plan_path)
    process_metrics = metrics_module.process_metrics()
    dispatcher = PlannedDispatcher(
        plan, digest, ready_labels=ready_labels, hint_slack=hint_slack, metrics=process_metrics,
    )
    bound_host, bound_port = await dispatcher.start(host, port)
    logger.info(
        "stage=planned-dispatcher-ready host=%s port=%s plan_id=%s digest=%s chunks=%s rows=%s resources=%s",
        bound_host, bound_port, plan.plan_id, digest, len(plan.chunks), plan.total_rows, plan.resources.count,
    )
    metrics_server = metrics_module.start_metrics_server(
        process_metrics, kind="planned-dispatcher", name="-"
    )
    metrics_module.initialize_zero_series(process_metrics, kind="planned-dispatcher", name="-")
    try:
        await stop.wait()
    finally:
        # Nested finally (P6-S2 review R5): a raise from `dispatcher.close()`
        # must never skip the snapshot/metrics-server shutdown below.
        try:
            await dispatcher.close()
            state = dispatcher.state
            logger.info(
                "stage=planned-dispatcher-stats state=%s done=%s/%s rows=%s/%s %s",
                state.state, state.stats.completed, state.total, state.rows_done, plan.total_rows,
                " ".join(f"{key}={value:.1f}" if isinstance(value, float) else f"{key}={value}"
                         for key, value in state.stats.as_dict().items()),
            )
        finally:
            metrics_module.write_metrics_snapshot(
                process_metrics, os.environ.get("GRAPH_LOADER_METRICS_SNAPSHOT_PATH")
            )
            metrics_module.stop_metrics_server(metrics_server)
    return dispatcher.state


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the planned bulk relationship dispatcher")
    parser.add_argument("--plan", required=True)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=DEFAULT_DISPATCHER_PORT)
    parser.add_argument("--ready-labels", default="", help="comma-separated labels already ready")
    parser.add_argument("--hint-slack", type=int, default=DEFAULT_HINT_SLACK)
    args = parser.parse_args(argv)
    ready = [label for label in args.ready_labels.split(",") if label]

    async def run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, stop.set)
        # SIGTERM uses a plain handler, not loop.add_signal_handler: closing the loop would
        # reset an add_signal_handler disposition to SIG_DFL, and a repeated SIGTERM during
        # teardown would exit 143. The handler is replaced by SIG_IGN once main() is done.
        signal.signal(signal.SIGTERM, lambda *_args: loop.call_soon_threadsafe(stop.set))
        await serve(args.plan, args.host, args.port, stop=stop, ready_labels=ready, hint_slack=args.hint_slack)

    try:
        asyncio.run(run())
    except Exception as exc:
        logger.error("stage=planned-dispatcher reason=%s", exc)
        return 1
    finally:
        # CPython resets Python-level handlers to SIG_DFL at interpreter exit; only SIG_IGN
        # survives, so a repeated stop signal cannot turn a clean exit into 143.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    raise SystemExit(main())
