"""Blocking client and cell routing for the global bucket-lease coordinator.

See :mod:`src.orchestrator.lease_server` for the line protocol.  Every client
error is a :class:`LeaseCoordinatorError`, so lease mode fails closed: a
loader that cannot prove it holds a lease never writes.

Granularity.  ``B`` buckets per label cap the fleet-wide number of
concurrent transactions touching one large label at ``B`` (a self-reference
may hold two buckets), while one outer flush of ``N`` records spreads over up
to ``B*B`` cells of about ``N / B**2`` rows.  With Neo4j on ~8 cores, 8-16
concurrent node-disjoint transactions saturate it, so the default is
``B = 16`` with ``N = 32000`` (~125-row cells for two large labels).  Small
cells are merged under one lease with cells sharing a held resource (up to
``GRAPH_LOADER_LEASE_MERGE_ROWS``, default 200), so per-transaction overhead
stays amortised without holding many resources; ``B = 8`` would give
~500-row cells but only 8 concurrent Title transactions.  Small labels
(hub-dense targets such as genres or languages) are *keyed*: one resource
per node, so unrelated hub nodes never serialize on a shared bucket; their
busiest node remains the inherent serial bottleneck of any design in which
concurrent transactions never share a node.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import itertools
import logging
import os
import re
import socket
import threading
import time
from typing import Callable, Iterable, NoReturn, Sequence

from src.loader.mix_and_batch import endpoint_bucket, endpoint_digest


# P5-S1 Story 1: the coordinator's settings are renamed with a
# ``bucket_lease_``/``BUCKET_LEASE`` prefix so "lease" unambiguously means
# this TCP coordinator, never ``CoordinationConfig.lease_timeout_ms`` (the
# unrelated Phase 4 clock slot lease). ``GRAPH_LOADER_BUCKET_LEASE_ADDR`` is
# the current name; ``GRAPH_LOADER_LEASE_ADDR`` (below) is kept as a
# deprecated fallback for one release -- every ``docker_service.py``
# launcher sets both to the same value so a mixed-version fleet (an
# edge-loader image built from a slightly older commit) never silently
# loses its lease address.
BUCKET_LEASE_ADDRESS_ENV = "GRAPH_LOADER_BUCKET_LEASE_ADDR"
LEASE_ADDRESS_ENV = "GRAPH_LOADER_LEASE_ADDR"
# LEASE_BUCKETS_ENV/LEASE_KEYED_LABELS_ENV (and edge_execution.py's
# LEASE_MERGE_ROWS_ENV) intentionally keep their existing names (review
# finding N2): these are container-internal wiring the orchestrator sets and
# only this codebase's own containers ever read (never an operator-facing
# setting an external script sets directly, unlike LEASE_ADDRESS_ENV, which
# an operator or a mixed-version fleet member can plausibly still export by
# hand). Renaming them would add alias plumbing with no back-compat need.
LEASE_BUCKETS_ENV = "GRAPH_LOADER_LEASE_BUCKETS"
LEASE_KEYED_LABELS_ENV = "GRAPH_LOADER_LEASE_KEYED_LABELS"
DEFAULT_BUCKET_LEASE_BUCKETS = 16
# Deprecated alias (removed one release after plan/v5/slice-1).
DEFAULT_LEASE_BUCKETS = DEFAULT_BUCKET_LEASE_BUCKETS
_LABEL = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
# Edge outer flush override set by the orchestrator (lease mode needs large
# flushes so each of the up to B*B cells still makes a sizeable transaction).
EDGE_OUTER_BATCH_ENV = "GRAPH_LOADER_EDGE_OUTER_BATCH_SIZE"
MAX_EDGE_OUTER_BATCH_SIZE = 1_000_000
_bucket_lease_addr_fallback_logged = False


def resolve_bucket_lease_address(environ=None) -> str:
    """Return the configured lease address, preferring the current env var.

    Falls back to the deprecated ``GRAPH_LOADER_LEASE_ADDR`` and logs the
    fallback exactly once per process (not once per reconnect attempt). A
    duplicate log line under a race on the module-global flag is harmless
    (best-effort de-duplication, not a correctness requirement).
    Returns ``""`` when neither is set (bucket-lease mode is off).
    """
    global _bucket_lease_addr_fallback_logged

    env = environ if environ is not None else os.environ
    address = env.get(BUCKET_LEASE_ADDRESS_ENV, "").strip()
    if address:
        return address
    address = env.get(LEASE_ADDRESS_ENV, "").strip()
    if address and not _bucket_lease_addr_fallback_logged:
        _bucket_lease_addr_fallback_logged = True
        logging.getLogger(__name__).warning(
            "stage=deprecation flag=%s superseded_by=%s", LEASE_ADDRESS_ENV, BUCKET_LEASE_ADDRESS_ENV,
        )
    return address


class LeaseCoordinatorError(RuntimeError):
    """The lease coordinator is unreachable, closed, or replied unexpectedly."""


def parse_lease_address(address: str) -> tuple[str, int]:
    """Parse ``host:port`` into its parts, rejecting anything ambiguous."""
    if not isinstance(address, str) or ":" not in address:
        raise ValueError("lease address must be host:port")
    host, _, port_text = address.rpartition(":")
    if not host.strip() or not port_text.isdigit() or not 1 <= int(port_text) <= 65535:
        raise ValueError("lease address must be host:port")
    return host.strip("[]"), int(port_text)


def parse_lease_bucket_count(value: str | int | None) -> int:
    """Return a validated per-label bucket count (default 16)."""
    if value is None or value == "":
        return DEFAULT_BUCKET_LEASE_BUCKETS
    if isinstance(value, bool):
        raise ValueError("lease bucket count must be an integer from 1 through 4096")
    try:
        count = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("lease bucket count must be an integer from 1 through 4096") from exc
    if not 1 <= count <= 4096:
        raise ValueError("lease bucket count must be an integer from 1 through 4096")
    return count


def parse_keyed_labels(value: str | Iterable[str] | None) -> frozenset[str]:
    """Parse the comma-separated keyed-label list (labels leased per node key)."""
    if value is None:
        return frozenset()
    items = value.split(",") if isinstance(value, str) else list(value)
    labels = frozenset(item.strip() for item in items if item.strip())
    for label in labels:
        if not _LABEL.fullmatch(label):
            raise ValueError(f"invalid keyed lease label: {label!r}")
    return labels


def lease_scheme_token(keyed_labels: Iterable[str]) -> str:
    """Return the canonical HELLO scheme token for a keyed-label set."""
    labels = sorted(parse_keyed_labels(keyed_labels))
    return ",".join(labels) if labels else "-"


# One cell is identified by its (source resource, target resource) tokens.
CellKey = tuple[str, str]
_RESOURCE_CACHE_LIMIT = 1 << 19


@dataclass
class BucketCellRouter:
    """Map an edge to its cell and the cell's lease resources.

    A node's resource is ``Label:bucket`` with ``bucket = endpoint_bucket(key,
    B)``, or ``Label#<key digest>`` when ``Label`` is *keyed* (a small label
    such as IMDb genres, whose hub nodes should not block each other merely
    by sharing a hash bucket).  Every run-wide client must use the same ``B``
    and keyed set (checked by the coordinator's HELLO).  A cell of a type
    ``S -> L`` needs ``{S resource, L resource}``; for a self-reference that
    collapses to one resource when both endpoints share it.  Records of one
    (source, target) pair always fall in one cell.
    """

    source_label: str
    target_label: str
    bucket_count: int
    keyed_labels: frozenset[str] = frozenset()
    _cache: dict = field(default_factory=dict, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        parse_lease_bucket_count(self.bucket_count)
        for label in (self.source_label, self.target_label):
            if not isinstance(label, str) or not _LABEL.fullmatch(label):
                raise ValueError("lease labels must be Cypher identifiers")
        self.keyed_labels = parse_keyed_labels(self.keyed_labels)

    def resource(self, label: str, key: object) -> str:
        """Return the stable lease resource token of one node."""
        cache_key = (label, type(key), key)
        try:
            token = self._cache.get(cache_key)
        except TypeError:  # unhashable key: compute without caching
            return self._compute(label, key)
        if token is None:
            token = self._compute(label, key)
            if len(self._cache) >= _RESOURCE_CACHE_LIMIT:
                self._cache.clear()
            self._cache[cache_key] = token
        return token

    def _compute(self, label: str, key: object) -> str:
        if label in self.keyed_labels:
            return f"{label}#{endpoint_digest(key).hex()}"
        return f"{label}:{endpoint_bucket(key, self.bucket_count)}"

    def cell(self, source_key: object, target_key: object) -> CellKey:
        return (self.resource(self.source_label, source_key), self.resource(self.target_label, target_key))

    @staticmethod
    def resources(cell: CellKey) -> tuple[str, ...]:
        """Return the cell's sorted, de-duplicated resource tokens."""
        return tuple(sorted(set(cell)))


class BucketLeaseClient:
    """One persistent, strictly request/response coordinator connection.

    Not thread-safe: give each writer thread its own client.

    ``connect_timeout_seconds``/``backoff_ms`` have two distinct shapes for
    two distinct callers (P5-S1 Story 3):

    * **Bulk** (the default): ``connect_timeout_seconds`` is a fixed 30s
      deadline and ``backoff_ms=None`` keeps today's fixed 0.2s poll,
      byte-for-byte unchanged -- bulk assumes the coordinator is already up
      before any writer starts, so a short bounded window with no backoff
      growth is correct.
    * **Streaming**: pass ``connect_timeout_seconds=None`` (unbounded -- a
      stream must outlast a supervised coordinator restart cycle, Story 5)
      and ``backoff_ms=(base_ms, max_ms)`` for capped exponential backoff
      between reconnect attempts (``min(base * 2**(attempt-1), max)``, the
      same shape ``ApocLockedEdgeWriter.write_rows`` already uses for its
      own retry delay, so this codebase has one backoff formula, not two).

    A generation change is not tracked or logged by *this* class (review
    finding S3-1): a ``BucketLeaseClient`` that hits any request failure
    is marked permanently ``broken`` and never reconnects itself -- the
    connection *pool* (``BucketLeaseExecutionCoordinator._checkout``)
    discards it and builds a fresh client via ``client_factory()``, which
    starts with no remembered generation, so a same-instance "previous vs.
    new generation" comparison inside this class could never fire in
    production. Where a coordinator restart is actually observed and
    logged (across pooled clients) is Story 4's job, once the pool itself
    is restructured to survive a restart (see the module docstring's
    pointer to ``plan/v5-v6-orchestration.md``'s P5-Q1 amendment for why:
    the binding fencing guarantee is the Neo4j transaction timeout plus the
    lease server's post-restart quiet period, never a generation
    comparison or a PING). This class only exposes ``generation`` (the
    token from the most recent successful HELLO) as a read-only diagnostic.
    """

    def __init__(
        self, address: str, *, bucket_count: int | None = None, keyed_labels: Iterable[str] = (),
        connect_timeout_seconds: float | None = 30.0, op_timeout_seconds: float = 10.0,
        connect: Callable[..., socket.socket] = socket.create_connection,
        sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic,
        backoff_ms: tuple[int, int] | None = None,
        stop_event: threading.Event | None = None,
    ) -> None:
        if backoff_ms is not None:
            base_ms, max_ms = backoff_ms
            if base_ms < 1 or max_ms < base_ms:
                raise ValueError("backoff_ms must be (base_ms >= 1, max_ms >= base_ms)")
        self._host, self._port = parse_lease_address(address)
        self._address = address
        self._bucket_count = bucket_count
        self._scheme = lease_scheme_token(keyed_labels)
        self._connect_timeout = connect_timeout_seconds
        self._op_timeout = op_timeout_seconds
        self._connect = connect
        self._sleep = sleep
        self._clock = clock
        self._backoff_ms = backoff_ms
        # Story 3, review finding S3-2: a streaming client's reconnect loop
        # is unbounded (connect_timeout_seconds=None) by design, so it must
        # be interruptible by shutdown -- otherwise a worker thread blocks
        # in time.sleep() until Docker's stop grace expires and SIGKILLs
        # the container, breaking "SIGTERM honored promptly even mid
        # backoff". Story 7 passes the edge loader's shutdown event here.
        self._stop_event = stop_event
        self._socket: socket.socket | None = None
        self._reader = None
        self._broken = False
        self._ids = itertools.count(1)
        self._generation: str | None = None

    @property
    def broken(self) -> bool:
        return self._broken

    @property
    def generation(self) -> str | None:
        """The coordinator's generation token observed at the last HELLO,
        or ``None`` before any HELLO has succeeded (``bucket_count`` unset,
        or never connected yet). Diagnostic only -- see the class docstring."""
        return self._generation

    def connect(self) -> None:
        """Connect (retrying until the connect deadline) and verify buckets."""
        if self._socket is not None:
            return
        if self._broken:
            raise LeaseCoordinatorError(f"lease coordinator connection to {self._address} is broken")
        deadline = None if self._connect_timeout is None else self._clock() + self._connect_timeout
        attempt = 0
        while True:
            if self._stop_event is not None and self._stop_event.is_set():
                raise LeaseCoordinatorError("lease reconnect abandoned: shutdown requested")
            try:
                sock = self._connect((self._host, self._port), timeout=self._op_timeout)
                break
            except OSError as exc:
                if deadline is not None and self._clock() >= deadline:
                    self._broken = True
                    raise LeaseCoordinatorError(
                        f"lease coordinator {self._address} unreachable: {exc}"
                    ) from exc
                attempt += 1
                if self._backoff_ms is None:
                    # Bulk's unchanged fixed poll -- byte-for-byte identical
                    # to pre-Story-3 behavior when backoff_ms is not set.
                    delay = 0.2
                else:
                    base_ms, max_ms = self._backoff_ms
                    # N3-1: cap the exponent, not just the resulting delay,
                    # so an unbounded outage never computes a giant int.
                    delay_ms = min(base_ms * 2 ** min(attempt - 1, 30), max_ms)
                    delay = delay_ms / 1000
                    # N3-2: an operator must see *something* during an
                    # unbounded outage, not a silent stall -- log the first
                    # attempt and every attempt that has reached the cap.
                    if attempt == 1 or delay_ms >= max_ms:
                        logging.getLogger(__name__).warning(
                            "stage=lease-reconnect-wait address=%s attempt=%s delay_ms=%s reason=%s",
                            self._address, attempt, delay_ms, exc,
                        )
                if self._stop_event is not None:
                    if self._stop_event.wait(delay):
                        raise LeaseCoordinatorError("lease reconnect abandoned: shutdown requested")
                else:
                    self._sleep(delay)
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass
        self._socket = sock
        self._reader = sock.makefile("rb")
        if self._bucket_count is not None:
            reply = self._request(f"HELLO {self._bucket_count} {self._scheme}")
            # P5-S1 Story 2: a successful HELLO now replies "OK <generation>".
            # Exact "OK" or "OK "-prefixed only (review finding N2-1): a
            # bare startswith("OK") would also accept garbage like "OKAY".
            if reply != "OK" and not reply.startswith("OK "):
                self._fail(f"HELLO rejected: {reply}")
            _, _, token = reply.partition(" ")
            if token:
                self._generation = token
        else:
            self._expect_ok(self._request("PING"), "PING")

    def acquire(self, resources: Sequence[str]) -> str | None:
        """Non-blocking all-or-nothing acquire; return a lease id or ``None``."""
        lease_id = f"l{next(self._ids)}"
        reply = self._request(f"ACQ {lease_id} {' '.join(resources)}")
        if reply == "OK":
            return lease_id
        # "NO quiet" (P5-S1 Story 5): the server is inside its post-restart
        # quiet period Q and is refusing every grant, not "genuinely
        # contended" -- a denial either way, from this client's point of
        # view, but never treated as a protocol error.
        if reply == "NO" or reply == "NO quiet":
            return None
        self._fail(f"unexpected ACQ reply: {reply}")

    def acquire_any(self, resource_sets: Sequence[Sequence[str]], *, wait_ms: int) -> tuple[str, int] | None:
        """Acquire the first free set, waiting up to ``wait_ms`` server-side."""
        if not resource_sets:
            raise ValueError("acquire_any requires at least one resource set")
        lease_id = f"l{next(self._ids)}"
        encoded = " ".join(",".join(resources) for resources in resource_sets)
        reply = self._request(f"ANY {lease_id} {int(wait_ms)} {encoded}", extra_timeout=wait_ms / 1000)
        if reply == "NO" or reply == "NO quiet":
            return None
        head, _, index_text = reply.partition(" ")
        if head == "OK" and index_text.isdigit() and int(index_text) < len(resource_sets):
            return lease_id, int(index_text)
        self._fail(f"unexpected ANY reply: {reply}")

    def extend(self, lease_id: str, weighted_sets: Sequence[tuple[int, Sequence[str]]], *, budget: int) -> list[int]:
        """Add free sets to a held lease within ``budget``; return added indexes."""
        if not weighted_sets:
            return []
        encoded = " ".join(f"{int(weight)}@{','.join(resources)}" for weight, resources in weighted_sets)
        reply = self._request(f"EXT {lease_id} {int(budget)} {encoded}")
        head, _, indexes = reply.partition(" ")
        if head != "OK":
            self._fail(f"unexpected EXT reply: {reply}")
        if not indexes:
            return []
        try:
            added = [int(item) for item in indexes.split(",")]
        except ValueError:
            self._fail(f"unexpected EXT reply: {reply}")
        if any(not 0 <= item < len(weighted_sets) for item in added) or len(set(added)) != len(added):
            self._fail(f"unexpected EXT reply: {reply}")
        return added

    def release(self, lease_id: str) -> None:
        self._expect_ok(self._request(f"REL {lease_id}"), "REL")

    def stats(self) -> str:
        reply = self._request("STATS")
        self._expect_ok(reply.split(" ", 1)[0], "STATS")
        return reply[3:]

    def ping(self) -> bool:
        """Best-effort liveness probe; never raises (P5-S1 Story 4).

        A failing liveness check is an expected outcome at the call site
        (a writer deciding whether to commit), not a bug, so this catches
        ``LeaseCoordinatorError`` and returns ``False`` instead of
        propagating it -- unlike every other method on this class, which
        fails closed by raising.
        """
        try:
            self._expect_ok(self._request("PING"), "PING")
            return True
        except LeaseCoordinatorError:
            return False

    def close(self) -> None:
        sock, reader = self._socket, self._reader
        self._socket = self._reader = None
        for item in (reader, sock):
            if item is not None:
                try:
                    item.close()
                except OSError:
                    pass

    def _expect_ok(self, reply: str, command: str) -> None:
        if reply != "OK":
            self._fail(f"unexpected {command} reply: {reply}")

    def _fail(self, reason: str) -> NoReturn:
        self._broken = True
        self.close()
        raise LeaseCoordinatorError(f"lease coordinator {self._address}: {reason}")

    def _request(self, line: str, *, extra_timeout: float = 0.0) -> str:
        if self._socket is None:
            self.connect()
        assert self._socket is not None and self._reader is not None
        try:
            self._socket.settimeout(self._op_timeout + extra_timeout)
            self._socket.sendall(line.encode("ascii") + b"\n")
            raw = self._reader.readline()
        except (OSError, ValueError) as exc:
            self._fail(f"request failed: {exc}")
        if not raw.endswith(b"\n"):
            self._fail("connection closed")
        return raw.decode("ascii").strip()
