"""Run-scoped global bucket-lease coordinator for bulk relationship writes.

Every node label has ``B`` stable buckets (``endpoint_bucket(key, B)``), and
a *resource* is one ``Label:bucket`` token -- or, for small "keyed" labels,
one ``Label#<key digest>`` token per node.  An edge loader writes a
relationship transaction only while it holds an exclusive lease on every
resource the transaction's endpoints map to.  Because each node maps to
exactly one resource and concurrently granted resource sets are disjoint,
concurrently running transactions -- across all edge types and containers --
are node-disjoint, so they never wait on Neo4j locks and cannot deadlock.

Line protocol (ASCII, one request and one ``\\n``-terminated reply at a time
per persistent TCP connection):

``HELLO <bucket_count> [<scheme>]``
    ``OK <generation>`` when the server was started with the same
    ``--buckets`` and ``--scheme`` (the keyed-label list; ``-`` for none), or
    with neither; ``ERR ...`` otherwise.  Every client of a run must map
    nodes to resources identically, so a mismatch fails closed.
    ``<generation>`` is a random hex token chosen once per server process
    (P5-S1 Story 2): stable for that process's entire lifetime, and never
    reused after a restart.  It lets a client (via ``docker logs``'
    ``stage=lease-server-ready``/``stage=lease-server-stats`` lines, or a
    ``STATS`` request) tell "the same coordinator instance that granted my
    prior lease" from "the coordinator restarted and remembers nothing" --
    useful for diagnosing a restart transition, but it is not itself the
    fencing mechanism: the binding guarantee against a restarted server
    re-granting a resource a prior holder may still be committing is the
    time-based fencing in ``plan/v5-v6-orchestration.md``'s P5-Q1 amendment
    (a Neo4j transaction timeout on every write, plus a post-restart quiet
    period the new generation waits out before its first grant), not this
    token.  (Review finding S2-1: the generation does *not* appear in the
    exit-time Prometheus metrics snapshot -- adding it there would be a new
    label/cardinality dimension, which the metrics catalogue forbids.)
``ACQ <lease_id> <res> [<res> ...]``
    Non-blocking all-or-nothing acquire: ``OK``, ``NO``, or ``NO quiet``
    (P5-S1 Story 5: the server is inside its post-restart quiet period ``Q``
    and is refusing every grant regardless of what looks free -- see
    ``quiet_period_seconds``, below).
``ANY <lease_id> <wait_ms> <set> [<set> ...]``
    Each ``<set>`` is comma-joined resources.  Atomically grants the first
    (lowest-index) set whose resources are all free: ``OK <index>``.  If none
    is free, waits up to ``wait_ms`` (0 = do not wait) for releases, which
    are handed to waiters in arrival order before any new request, then
    replies ``NO``.  During the quiet period ``Q``, holds the reply for
    ``min(wait_ms, remaining Q)`` and always replies ``NO quiet`` instead --
    never grants, and never counts as a denial or a wait timeout, since it
    means "still quiescing," not "contended."
``EXT <lease_id> <budget> <weight>@<set> [<weight>@<set> ...]``
    Non-blocking extension of a held lease (to merge small cells into one
    transaction): in order, adds every set whose resources are free or
    already in this lease while the summed weights stay within ``budget``.
    Replies ``OK`` followed by the comma-joined added indexes (``OK`` alone
    when none).
``REL <lease_id>``
    ``OK``, or ``ERR unknown lease``.  Releases every resource of the lease.
``PING`` / ``STATS``
    ``OK`` / ``OK key=value ...``.

Lease ids are chosen by the client and are scoped to its connection.  When a
connection closes, every lease it holds is released.  Malformed requests get
``ERR <reason>``.
"""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
import itertools
import logging
import os
import re
import signal
import time
import uuid
from typing import Iterable, Sequence

from src.utils import metrics as metrics_module


logger = logging.getLogger(__name__)

DEFAULT_BUCKET_LEASE_PORT = 7700
# Deprecated alias (removed one release after plan/v5/slice-1): kept for any
# external importer of the old, undocumented-but-importable name.
DEFAULT_LEASE_PORT = DEFAULT_BUCKET_LEASE_PORT
MAX_WAIT_MS = 60_000
MAX_LINE_BYTES = 1 << 20
_LEASE_ID = re.compile(r"[A-Za-z0-9_.-]{1,64}")
_SET_CACHE_LIMIT = 1 << 16

Owner = int
LeaseKey = tuple[Owner, str]


class LeaseProtocolError(ValueError):
    """A request violated the lease line protocol."""


@dataclass
class LeaseStats:
    """Monotonic server counters, logged at shutdown."""

    connections: int = 0
    grants: int = 0
    extensions: int = 0
    waited_grants: int = 0
    denials: int = 0
    wait_timeouts: int = 0
    releases: int = 0
    disconnect_releases: int = 0
    active: int = 0
    max_concurrent: int = 0
    max_waiters: int = 0
    total_wait_ms: float = 0.0

    def render(self) -> str:
        return (
            f"connections={self.connections} grants={self.grants} "
            f"extensions={self.extensions} "
            f"waited_grants={self.waited_grants} denials={self.denials} "
            f"wait_timeouts={self.wait_timeouts} releases={self.releases} "
            f"disconnect_releases={self.disconnect_releases} active={self.active} "
            f"max_concurrent={self.max_concurrent} max_waiters={self.max_waiters} "
            f"total_wait_ms={self.total_wait_ms:.1f}"
        )


class LeaseTable:
    """Pure single-threaded exclusive ownership of resource sets."""

    def __init__(
        self, stats: LeaseStats | None = None,
        metrics: "metrics_module.GraphLoaderMetrics | None" = None,
    ) -> None:
        self.stats = stats or LeaseStats()
        self._holder: dict[str, LeaseKey] = {}
        self._leases: dict[LeaseKey, frozenset[str]] = {}
        self._by_owner: dict[Owner, set[str]] = {}
        self._metrics = metrics if metrics is not None else metrics_module.process_metrics()

    def is_free(self, resources: Iterable[str]) -> bool:
        return self._holder.keys().isdisjoint(resources)

    def is_held(self, resource: str) -> bool:
        return resource in self._holder

    def holder(self, resource: str) -> LeaseKey | None:
        return self._holder.get(resource)

    def has_lease(self, owner: Owner, lease_id: str) -> bool:
        return (owner, lease_id) in self._leases

    def grant(self, owner: Owner, lease_id: str, resources: frozenset[str]) -> None:
        """Record a lease on currently free resources (caller checked)."""
        key = (owner, lease_id)
        if key in self._leases:
            raise LeaseProtocolError("lease id already held on this connection")
        if not self.is_free(resources):
            raise AssertionError("grant of a held resource")
        for resource in resources:
            self._holder[resource] = key
        self._leases[key] = resources
        self._by_owner.setdefault(owner, set()).add(lease_id)
        stats = self.stats
        stats.grants += 1
        stats.active += 1
        stats.max_concurrent = max(stats.max_concurrent, stats.active)
        # Covers both the immediate ANY-grant path and the wait-then-grant
        # path (_wake below calls this same method) - P6-S2 Story 7's AC:
        # "increases by exactly 1 per granted ANY request, whether granted
        # immediately or after a wait". Routed through safe_emit.
        held = len(self._holder)
        metrics_module.safe_emit(lambda: self._metrics.lease_grants.labels(kind="lease-server").inc())
        metrics_module.safe_emit(lambda: self._metrics.lease_held_resources.labels(
            kind="lease-server"
        ).set(held))

    def extend(self, owner: Owner, lease_id: str, resources: frozenset[str]) -> bool:
        """Add ``resources`` to a held lease if each is free or already its own."""
        key = (owner, lease_id)
        current = self._leases.get(key)
        if current is None:
            raise LeaseProtocolError("unknown lease")
        holder = self._holder
        for resource in resources:
            other = holder.get(resource)
            if other is not None and other != key:
                return False
        added = resources - current
        for resource in added:
            holder[resource] = key
        self._leases[key] = current | added
        self.stats.extensions += 1
        if added:
            # P6-S2 review finding S7-1: EXT is a real hot-path mutation of
            # self._holder (merged-cell extension, lease_client.py's own
            # EXT command), not just grant()/release() - without this, the
            # gauge under-reports between an extension and the next
            # grant/release, breaking "exact at every scrape".
            held = len(self._holder)
            metrics_module.safe_emit(lambda: self._metrics.lease_held_resources.labels(
                kind="lease-server"
            ).set(held))
        return True

    def try_acquire(self, owner: Owner, lease_id: str, resources: frozenset[str]) -> bool:
        if (owner, lease_id) in self._leases:
            raise LeaseProtocolError("lease id already held on this connection")
        if not self.is_free(resources):
            return False
        self.grant(owner, lease_id, resources)
        return True

    def release(self, owner: Owner, lease_id: str) -> frozenset[str]:
        key = (owner, lease_id)
        resources = self._leases.pop(key, None)
        if resources is None:
            raise KeyError(lease_id)
        for resource in resources:
            del self._holder[resource]
        ids = self._by_owner[owner]
        ids.discard(lease_id)
        if not ids:
            del self._by_owner[owner]
        self.stats.releases += 1
        self.stats.active -= 1
        held = len(self._holder)
        metrics_module.safe_emit(lambda: self._metrics.lease_held_resources.labels(
            kind="lease-server"
        ).set(held))
        return resources

    def release_owner(self, owner: Owner) -> frozenset[str]:
        released: set[str] = set()
        for lease_id in tuple(self._by_owner.get(owner, ())):
            released |= self.release(owner, lease_id)
            self.stats.disconnect_releases += 1
        return frozenset(released)


@dataclass
class _Waiter:
    seq: int
    owner: Owner
    lease_id: str
    sets: tuple[frozenset[str], ...]
    future: asyncio.Future
    started: float
    timer: asyncio.TimerHandle | None = None


def _parse_lease_id(token: str) -> str:
    if not _LEASE_ID.fullmatch(token):
        raise LeaseProtocolError("invalid lease id")
    return token


def _parse_resources(tokens: Sequence[str]) -> frozenset[str]:
    if not tokens:
        raise LeaseProtocolError("empty resource set")
    for token in tokens:
        # ``split`` already excludes whitespace from a token.
        if not token or len(token) > 256 or "," in token:
            raise LeaseProtocolError("invalid resource")
    return frozenset(tokens)


class BucketLeaseServer:
    """Asyncio TCP lease coordinator; all state changes run on one loop."""

    def __init__(self, *, bucket_count: int | None = None, scheme: str | None = None,
                 max_wait_ms: int = MAX_WAIT_MS, clock=time.monotonic,
                 metrics: "metrics_module.GraphLoaderMetrics | None" = None,
                 generation: str | None = None, quiet_period_seconds: float = 0.0,
                 disconnect_hold_seconds: float = 0.0) -> None:
        # P5-S1 Story 2: one random token per process start, never
        # persisted -- a fresh process is definitionally a new generation,
        # so there is nothing to recover across a restart. ``generation``
        # is injectable only for deterministic tests (a monotonic-counter
        # test double, not uuid4's statistical uniqueness); validated
        # against the same charset as a lease id (review finding N2-2) so
        # a test-supplied value with whitespace/newlines can never corrupt
        # the line protocol's single-line HELLO/STATS replies.
        if generation is not None and not _LEASE_ID.fullmatch(generation):
            raise ValueError("invalid generation token")
        self.generation = generation if generation is not None else uuid.uuid4().hex
        self._metrics = metrics if metrics is not None else metrics_module.process_metrics()
        self.stats = LeaseStats()
        self.table = LeaseTable(self.stats, metrics=self._metrics)
        self._bucket_count = bucket_count
        self._scheme = scheme
        self._max_wait_ms = max_wait_ms
        self._clock = clock
        # P5-S1 Story 5 / P5-Q1 fencing amendment (review finding C2-2):
        # the quiet period Q is enforced HERE, inside the server, not only
        # by a delay in whatever supervisor relaunches it -- only the
        # server itself can guarantee no grant happens before Q elapses,
        # regardless of who launched it or when a supervisor happened to
        # notice a crash. Measured from this instance's own construction
        # (=~ process start), which is safe because a prior instance is
        # dead (and, for the streaming supervisor, removed) before a new
        # one binds. 0 (the default) preserves today's behavior exactly
        # for bulk mode and every existing test, which never restart this
        # server mid-run.
        self._quiet_until = self._clock() + quiet_period_seconds if quiet_period_seconds > 0 else None
        # P5-S1 Story 4 review finding B4-1: a disconnect (a TCP EOF, not
        # an explicit REL) must NOT free a lease's resources immediately
        # -- the writer's Neo4j transaction may still be open or
        # committing, and a real BucketLeaseClient closes its socket in
        # `_fail()` *before* the driver has a chance to roll back. This is
        # a general correctness requirement, not restart-specific: it
        # applies from the very first grant of any run that sets
        # ``disconnect_hold_seconds`` (both bulk and streaming lease
        # mode), independent of ``quiet_period_seconds``/restarts. The
        # disconnect time plus this many seconds is when a
        # disconnected owner's lease is actually freed; an explicit REL on
        # a still-open connection is unaffected and stays immediate. 0
        # (the default) preserves today's behavior exactly.
        self._disconnect_hold_seconds = disconnect_hold_seconds
        self._owners = itertools.count(1)
        self._seq = itertools.count(1)
        self._waiters: dict[int, _Waiter] = {}
        self._waiter_by_owner: dict[Owner, int] = {}
        # waited-on set -> {waiter seq: set index} (FIFO), and the reverse
        # index resource -> waited-on sets containing it.
        self._set_waiters: dict[frozenset[str], dict[int, int]] = {}
        self._sets_by_resource: dict[str, dict[frozenset[str], None]] = {}
        # Clients resend the same cell sets constantly; parse each once.
        self._set_cache: dict[str, frozenset[str]] = {}
        self._server: asyncio.base_events.Server | None = None
        self._connections: set[asyncio.StreamWriter] = set()
        # P6-S5 Story 5: lets close() send one best-effort "ERR shutting
        # down" line to a parked ANY waiter's own connection before force-
        # cancelling its future - a small polish on top of an already-
        # correct outcome (per the P5-Q1 fencing amendment, a writer
        # already treats any lease-connection loss as "fence, roll back,"
        # so a bare disconnect was never a correctness gap).
        self._writer_by_owner: dict[Owner, asyncio.StreamWriter] = {}

    async def start(self, host: str = "127.0.0.1", port: int = 0) -> tuple[str, int]:
        self._server = await asyncio.start_server(
            self._handle_connection, host, port, limit=MAX_LINE_BYTES,
        )
        sockname = self._server.sockets[0].getsockname()
        return sockname[0], sockname[1]

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            for waiter in tuple(self._waiters.values()):
                self._remove_waiter(waiter)
                if not waiter.future.done():
                    # Best-effort clean reply before the force-close below.
                    # Not awaited (no `.drain()`): `transport.close()`
                    # already flushes whatever this synchronous `.write()`
                    # buffered, and draining against a stalled/malicious
                    # peer could make close() itself hang - breaking the
                    # "close() returns promptly no matter how many waiters
                    # were parked" property this has a regression test for.
                    writer = self._writer_by_owner.get(waiter.owner)
                    if writer is not None:
                        try:
                            writer.write(b"ERR shutting down\n")
                        except Exception:
                            pass  # best-effort only; the client-side fence-on-disconnect path below is already correct without this
                    waiter.future.cancel()
            for writer in tuple(self._connections):
                writer.close()
            await self._server.wait_closed()
            self._server = None

    # -- connection handling -------------------------------------------------

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        owner = next(self._owners)
        self.stats.connections += 1
        self._connections.add(writer)
        self._writer_by_owner[owner] = writer
        try:
            while True:
                try:
                    line = await reader.readline()
                except (ValueError, asyncio.LimitOverrunError):
                    writer.write(b"ERR request too long\n")
                    break
                if not line or not line.endswith(b"\n"):
                    break
                response = await self._dispatch(owner, line)
                writer.write(response.encode("ascii"))
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError, OSError):
            pass
        finally:
            self._drop_owner(owner)
            self._connections.discard(writer)
            self._writer_by_owner.pop(owner, None)
            try:
                writer.close()
            except Exception:  # pragma: no cover - best effort
                pass

    def _drop_owner(self, owner: Owner) -> None:
        seq = self._waiter_by_owner.get(owner)
        if seq is not None:
            waiter = self._waiters.get(seq)
            if waiter is not None:
                self._remove_waiter(waiter)
                if not waiter.future.done():
                    waiter.future.cancel()
        self._quarantine_owner(owner)

    def _quarantine_owner(self, owner: Owner) -> None:
        """Free a disconnected owner's leases, but not before
        ``disconnect_hold_seconds`` after the disconnect itself (review
        finding B4-1): a bare TCP EOF is not proof the writer's Neo4j
        transaction (if any was open) has finished -- a real
        ``BucketLeaseClient`` closes its socket in ``_fail()`` *before* the
        driver has a chance to roll back. Every transaction the holder
        opened had timeout ``T`` and was opened before the disconnect, so
        it is over by disconnect + ``T``. (A hold measured from the grant
        was already safe for writes -- every write's deadline is grant +
        ``T`` and EXT never extends it; measuring from the disconnect is
        hardening that keeps the fence independent of how long the lease
        was held.) An explicit ``REL`` (handled separately, in
        ``_dispatch``) stays immediate; only this disconnect path defers.
        """
        lease_ids = tuple(self.table._by_owner.get(owner, ()))
        if not lease_ids:
            return
        loop = asyncio.get_running_loop()
        immediate: set[str] = set()
        for lease_id in lease_ids:
            if self._disconnect_hold_seconds > 0:
                loop.call_later(self._disconnect_hold_seconds, self._release_quarantined, owner, lease_id)
            else:
                immediate.add(lease_id)
        if immediate:
            released: set[str] = set()
            for lease_id in immediate:
                try:
                    released |= self.table.release(owner, lease_id)
                except KeyError:  # pragma: no cover - defensive
                    continue
                self.stats.disconnect_releases += 1
            self._wake(frozenset(released))

    def _release_quarantined(self, owner: Owner, lease_id: str) -> None:
        """Timer callback: free one quarantined lease once its hold elapses."""
        try:
            released = self.table.release(owner, lease_id)
        except KeyError:  # pragma: no cover - already released some other way
            return
        self.stats.disconnect_releases += 1
        self._wake(released)

    def _quiet_remaining(self) -> float:
        """Seconds left in the post-restart quiet period Q, or 0 once it
        has elapsed (or none was configured)."""
        if self._quiet_until is None:
            return 0.0
        return max(0.0, self._quiet_until - self._clock())

    async def _dispatch(self, owner: Owner, line: bytes) -> str:
        try:
            parts = line.decode("ascii").split()
        except UnicodeDecodeError:
            return "ERR request is not ascii\n"
        if not parts:
            return "ERR empty request\n"
        command, args = parts[0], parts[1:]
        try:
            if command == "ACQ":
                if len(args) < 2:
                    raise LeaseProtocolError("ACQ requires a lease id and resources")
                lease_id = _parse_lease_id(args[0])
                if self._quiet_remaining() > 0:
                    # P5-Q1 amendment (review finding C2-2): never grant
                    # during the quiet period, no matter how free the
                    # resource looks -- an old-generation transaction may
                    # still be running. A distinct reply (not "NO") so a
                    # well-behaved client backs off without polluting
                    # lease_denials_total, which means "genuinely
                    # contended," not "server still quiescing."
                    return "NO quiet\n"
                if self.table.try_acquire(owner, lease_id, _parse_resources(args[1:])):
                    return "OK\n"
                self.stats.denials += 1
                metrics_module.safe_emit(lambda: self._metrics.lease_denials.labels(
                    kind="lease-server"
                ).inc())
                return "NO\n"
            if command == "ANY":
                return await self._any(owner, args)
            if command == "REL":
                if len(args) != 1:
                    raise LeaseProtocolError("REL requires one lease id")
                lease_id = _parse_lease_id(args[0])
                try:
                    released = self.table.release(owner, lease_id)
                except KeyError:
                    return "ERR unknown lease\n"
                # An explicit REL on a still-open connection is unaffected
                # by the disconnect hold -- it is immediate proof the
                # holder is done, unlike a bare TCP EOF.
                self._wake(released)
                return "OK\n"
            if command == "EXT":
                return self._extend(owner, args)
            if command == "HELLO":
                if len(args) not in (1, 2) or not args[0].isdigit():
                    raise LeaseProtocolError("HELLO requires a bucket count and optional scheme")
                if self._bucket_count is not None and int(args[0]) != self._bucket_count:
                    return f"ERR bucket count mismatch server={self._bucket_count} client={int(args[0])}\n"
                scheme = args[1] if len(args) == 2 else "-"
                if self._scheme is not None and scheme != self._scheme:
                    return f"ERR scheme mismatch server={self._scheme} client={scheme}\n"
                return f"OK {self.generation}\n"
            if command == "PING":
                return "OK\n"
            if command == "STATS":
                return f"OK {self.stats.render()} waiters={len(self._waiters)} generation={self.generation}\n"
            raise LeaseProtocolError("unknown command")
        except LeaseProtocolError as exc:
            return f"ERR {exc}\n"

    def _parse_set(self, token: str) -> frozenset[str]:
        resources = self._set_cache.get(token)
        if resources is None:
            resources = _parse_resources(token.split(","))
            if len(self._set_cache) >= _SET_CACHE_LIMIT:
                self._set_cache.clear()
            self._set_cache[token] = resources
        return resources

    def _extend(self, owner: Owner, args: list[str]) -> str:
        if len(args) < 3:
            raise LeaseProtocolError("EXT requires a lease id, budget and weighted sets")
        lease_id = _parse_lease_id(args[0])
        if not args[1].isdigit():
            raise LeaseProtocolError("invalid budget")
        budget = int(args[1])
        items = []
        for token in args[2:]:
            weight, separator, encoded = token.partition("@")
            if not separator or not weight.isdigit():
                raise LeaseProtocolError("EXT sets must be <weight>@<set>")
            items.append((int(weight), self._parse_set(encoded)))
        if not self.table.has_lease(owner, lease_id):
            raise LeaseProtocolError("unknown lease")
        used = 0
        added = []
        for index, (weight, resources) in enumerate(items):
            if used + weight > budget:
                continue
            if self.table.extend(owner, lease_id, resources):
                used += weight
                added.append(str(index))
        return f"OK {','.join(added)}\n" if added else "OK\n"

    async def _any(self, owner: Owner, args: list[str]) -> str:
        if len(args) < 3:
            raise LeaseProtocolError("ANY requires a lease id, wait_ms and resource sets")
        lease_id = _parse_lease_id(args[0])
        if not args[1].isdigit() or int(args[1]) > self._max_wait_ms:
            raise LeaseProtocolError("invalid wait_ms")
        wait_ms = int(args[1])
        sets = tuple(self._parse_set(token) for token in args[2:])
        if self.table.has_lease(owner, lease_id):
            raise LeaseProtocolError("lease id already held on this connection")
        quiet_remaining = self._quiet_remaining()
        if quiet_remaining > 0:
            # P5-Q1 amendment (review finding C2-2): never grant during the
            # quiet period, no matter how free a set looks. Still honor
            # the caller's own wait_ms (capped at the remaining quiet
            # time) by holding the reply, so a repeatedly-polling worker
            # is naturally throttled exactly as a real contended wait
            # would throttle it -- not a tight client-side retry loop for
            # the whole quiet window. Never touches denials/wait_timeouts:
            # this means "the server is still quiescing," not "genuinely
            # contended."
            await asyncio.sleep(min(wait_ms / 1000, quiet_remaining))
            return "NO quiet\n"
        for index, resources in enumerate(sets):
            if self.table.is_free(resources):
                self.table.grant(owner, lease_id, resources)
                return f"OK {index}\n"
        if wait_ms == 0:
            self.stats.denials += 1
            metrics_module.safe_emit(lambda: self._metrics.lease_denials.labels(
                kind="lease-server"
            ).inc())
            return "NO\n"
        loop = asyncio.get_running_loop()
        waiter = _Waiter(next(self._seq), owner, lease_id, sets, loop.create_future(), self._clock())
        seq = waiter.seq
        set_waiters, sets_by_resource = self._set_waiters, self._sets_by_resource
        for index, resources in enumerate(sets):
            members = set_waiters.get(resources)
            if members is None:
                members = set_waiters[resources] = {}
                for resource in resources:
                    by_resource = sets_by_resource.get(resource)
                    if by_resource is None:
                        by_resource = sets_by_resource[resource] = {}
                    by_resource[resources] = None
            if seq not in members:
                # Registration order is arrival order, so each member dict is
                # FIFO: its first key is the oldest waiter for that set.
                members[seq] = index
        self._waiters[seq] = waiter
        self._waiter_by_owner[owner] = waiter.seq
        self.stats.max_waiters = max(self.stats.max_waiters, len(self._waiters))
        waiter_count = len(self._waiters)
        metrics_module.safe_emit(lambda: self._metrics.lease_waiters.labels(
            kind="lease-server"
        ).set(waiter_count))
        waiter.timer = loop.call_later(wait_ms / 1000, self._expire, waiter.seq)
        return await waiter.future

    def _remove_waiter(self, waiter: _Waiter) -> None:
        self._waiters.pop(waiter.seq, None)
        # Centralized here (not duplicated at each of this method's several
        # call sites: _expire, _wake's grant path, close(), disconnect
        # cleanup) so the gauge is exact after every removal path, per
        # P6-S2 Story 7's AC ("returns to 0 once every outstanding request
        # has been either granted or timed out").
        waiter_count = len(self._waiters)
        metrics_module.safe_emit(lambda: self._metrics.lease_waiters.labels(
            kind="lease-server"
        ).set(waiter_count))
        if self._waiter_by_owner.get(waiter.owner) == waiter.seq:
            del self._waiter_by_owner[waiter.owner]
        set_waiters, sets_by_resource = self._set_waiters, self._sets_by_resource
        seq = waiter.seq
        for resources in waiter.sets:
            members = set_waiters.get(resources)
            if members is None:
                continue
            members.pop(seq, None)
            if not members:
                del set_waiters[resources]
                for resource in resources:
                    by_resource = sets_by_resource[resource]
                    del by_resource[resources]
                    if not by_resource:
                        del sets_by_resource[resource]
        waiter.sets = ()
        if waiter.timer is not None:
            waiter.timer.cancel()

    def _expire(self, seq: int) -> None:
        waiter = self._waiters.get(seq)
        if waiter is None:
            return
        self._remove_waiter(waiter)
        self.stats.wait_timeouts += 1
        metrics_module.safe_emit(lambda: self._metrics.lease_wait_timeouts.labels(
            kind="lease-server"
        ).inc())
        if not waiter.future.done():
            waiter.future.set_result("NO\n")

    def _wake(self, released: frozenset[str]) -> None:
        """Hand freshly released resources to parked waiters in arrival order.

        Invariant: no parked waiter has a free set.  Only a released resource
        can make a set free, so for each one this checks just the distinct
        waited-on *sets* containing it (not every waiter or every set of a
        waiter), and grants the oldest waiter of any such set that is now
        free.  A granted set holds the resource again, so each released
        resource yields at most one grant.
        """
        if not released or not self._waiters:
            return
        table = self.table
        set_waiters = self._set_waiters
        for resource in sorted(released):
            if table.is_held(resource):
                continue
            candidates = self._sets_by_resource.get(resource)
            if not candidates:
                continue
            best_seq = None
            free_sets = []
            for resources in candidates:
                if table.is_free(resources):
                    free_sets.append(resources)
                    head = next(iter(set_waiters[resources]))
                    if best_seq is None or head < best_seq:
                        best_seq = head
            if best_seq is None:
                continue
            waiter = self._waiters[best_seq]
            # Grant that waiter's lowest-index free set containing ``resource``.
            index = min(
                set_waiters[resources][best_seq] for resources in free_sets
                if best_seq in set_waiters[resources]
            )
            resources = waiter.sets[index]
            self._remove_waiter(waiter)
            table.grant(waiter.owner, waiter.lease_id, resources)
            self.stats.waited_grants += 1
            self.stats.total_wait_ms += (self._clock() - waiter.started) * 1000
            if not waiter.future.done():
                waiter.future.set_result(f"OK {index}\n")


async def serve(host: str, port: int, *, bucket_count: int | None, stop: asyncio.Event,
                scheme: str | None = None, quiet_period_seconds: float = 0.0,
                disconnect_hold_seconds: float = 0.0) -> LeaseStats:
    """Serve until ``stop`` is set, then log and return the final counters."""
    process_metrics = metrics_module.process_metrics()
    server = BucketLeaseServer(
        bucket_count=bucket_count, scheme=scheme, metrics=process_metrics,
        quiet_period_seconds=quiet_period_seconds, disconnect_hold_seconds=disconnect_hold_seconds,
    )
    bound_host, bound_port = await server.start(host, port)
    logger.info(
        "stage=lease-server-ready host=%s port=%s buckets=%s scheme=%s generation=%s",
        bound_host, bound_port, bucket_count, scheme, server.generation,
    )
    metrics_server = metrics_module.start_metrics_server(process_metrics, kind="lease-server", name="-")
    metrics_module.initialize_zero_series(process_metrics, kind="lease-server", name="-")
    try:
        await stop.wait()
    finally:
        # Snapshot + metrics-server shutdown run in their own nested
        # finally so a raise from `server.close()` (P6-S2 review R5) can
        # never skip them — the snapshot only reads in-memory counter
        # state, so it is safe (and correct) to capture it regardless of
        # whether the lease server's own close succeeded.
        try:
            await server.close()
            logger.info(
                "stage=lease-server-stats generation=%s %s", server.generation, server.stats.render(),
            )
        finally:
            metrics_module.write_metrics_snapshot(
                process_metrics, os.environ.get("GRAPH_LOADER_METRICS_SNAPSHOT_PATH")
            )
            metrics_module.stop_metrics_server(metrics_server)
    return server.stats


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the bulk relationship bucket-lease coordinator")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=DEFAULT_BUCKET_LEASE_PORT)
    parser.add_argument("--buckets", type=int, help="expected per-label bucket count (checked on HELLO)")
    parser.add_argument("--scheme", help="expected keyed-label scheme token, '-' for none (checked on HELLO)")
    parser.add_argument(
        "--quiet-period-seconds", type=float, default=0.0,
        help="P5-Q1 fencing amendment: refuse every grant for this many seconds after start "
        "(0 = none, the default -- a fresh run's first launch has no prior generation to wait "
        "out; the streaming supervisor passes Q > 0 only when relaunching after a restart)",
    )
    parser.add_argument(
        "--fence-seconds", type=float, default=0.0,
        help="P5-Q1 fencing amendment / review finding B4-1: hold a disconnected owner's leases "
        "for this many seconds after the disconnect before freeing them (0 = today's "
        "behavior, free immediately on disconnect) -- normally T + margin, the same value used "
        "to derive --quiet-period-seconds on a restart, but passed on every launch including "
        "the first, since a mid-run disconnect is not restart-specific",
    )
    args = parser.parse_args(argv)
    if args.buckets is not None and not 1 <= args.buckets <= 4096:
        parser.error("--buckets must be 1-4096")
    if args.quiet_period_seconds < 0:
        parser.error("--quiet-period-seconds must be nonnegative")
    if args.fence_seconds < 0:
        parser.error("--fence-seconds must be nonnegative")

    async def run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, stop.set)
        # SIGTERM uses a plain handler, not loop.add_signal_handler: closing the loop would
        # reset an add_signal_handler disposition to SIG_DFL, and a repeated SIGTERM during
        # teardown would exit 143. The handler is replaced by SIG_IGN once main() is done.
        signal.signal(signal.SIGTERM, lambda *_args: loop.call_soon_threadsafe(stop.set))
        await serve(
            args.host, args.port, bucket_count=args.buckets, stop=stop, scheme=args.scheme,
            quiet_period_seconds=args.quiet_period_seconds, disconnect_hold_seconds=args.fence_seconds,
        )

    try:
        asyncio.run(run())
    except Exception as exc:
        logger.error("stage=lease-server reason=%s", exc)
        return 1
    finally:
        # CPython resets Python-level handlers to SIG_DFL at interpreter exit; only SIG_IGN
        # survives, so a repeated stop signal cannot turn a clean exit into 143.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    raise SystemExit(main())
