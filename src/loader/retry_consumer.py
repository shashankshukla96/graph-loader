"""The DLQ retry consumer: pause/resume scheduling and the retry-or-park
write path (P5-S4 Story 5).

Reads every DLQ-and-retry-enabled edge type's missing/ambiguous-endpoint
DLQ topic (P5-S3), holds each record locally until its ``retry_after_ms``
passes (woken from a heap, never a blocking sleep in the poll loop; a
partition is paused only when it reaches ``max_records_per_partition``
outstanding records), and re-attempts the write through the same lease-fenced
writer the live stream uses. On a further failure the record's attempt
count increments and its ``retry_after_ms`` backs off exponentially, up to
a configured maximum; past that, the record is parked (never dropped).

This module is a **new, self-contained component** (Phase A of this
slice): it depends on :class:`src.loader.parked_pair_index.ParkedPairIndex`
and :class:`src.loader.dead_letter.DeadLetterRecordSink`, both already
merged, and on a small duck-typed :class:`RetryWriter` protocol rather than
on ``src.loader.edge_execution.ApocLockedEdgeWriter`` directly — P5-S3's
row-level DLQ writer (``write_provenanced_batch``, the sibling this
module's retry path needs) has not merged into this branch yet. Phase B
wires a real adapter around it once it has; every test in
``tests/test_retry_consumer.py`` exercises this module's own scheduling,
offset-commit, and retry-or-park logic against a fake writer/sink/index,
independent of Kafka or Neo4j.

Per-record blocking check (P5-S4 Story 3, review finding M2): this module
consults :meth:`ParkedPairIndex.is_blocked_by_earlier` — never a per-pair
counter — before writing a due record, so a record is blocked only by a
genuinely *earlier* still-outstanding record of the same pair, never by
its own presence in the index. A record found blocked this way is
re-DLQ'd at its **unchanged** ``attempt_count`` (via
``DeadLetterRecordSink.reroute_or_park(..., increment_attempt=False)``)
with a small fixed delay — never the exponential backoff formula, since
waiting on another record's own resolution must never consume this
record's own retry budget.

Frozen per-batch read barrier (review finding M8): a due batch's
membership is frozen and its :meth:`ParkedPairIndex.watermark_snapshot`
taken exactly once, at freeze time — never re-taken on every tick while
waiting for :meth:`ParkedPairIndex.caught_up`. Re-taking it on every tick
would (a) let a moving target starve the batch forever under steady index
traffic, since the snapshot would always be at least as new as this
instance's own applied position, and (b) never actually happen. A record
that becomes due *after* a batch has already been frozen joins the
*next* batch (with its own, later snapshot) rather than retroactively
attaching to the frozen one.

Per-record offset-commit discipline (review finding M9): a DLQ record
counts as resolved, and stops holding back its partition's commit, as soon
as *that record's own* outcome (written/re-DLQ'd/parked) is durable —
never withheld until its batch-mates finish too. A failure on record *k*
therefore never re-publishes (a duplicate re-DLQ/park) records ``0..k-1``
on the very next tick, even though they already succeeded — and a
duplicate publish for an already-resolved record can silently reopen the
exact cross-flush ordering bug this slice exists to close (a stale
duplicate, still holding the old record identity, can later overwrite a
newer write that the first, already-committed copy's resolution unblocked).

Batched draining (retry drain throughput): a partition no longer holds
just one record. Up to ``max_records_per_partition`` fetched records per
partition are outstanding at once; all due records of an edge type are
written as one batch (through the bucket-lease coordinator, so the batch
is cut into node-disjoint cell transactions exactly like the edge loader's
flushes), per-row results are routed with one DLQ publish flush and one
parked-pair-index flush, and offsets are committed once per partition and
batch at the highest offset below that partition's lowest *unresolved*
record (a commit never passes an unresolved row). A crash re-reads the
rows above that position: the M12 duplicate check skips only rows that were
*written or parked* (their index entry is gone). A re-DLQ'd or blocked row
keeps its index entry, so its old copy is retried again alongside the newer
re-published copy -- a safe duplicate, because both copies terminate
(written, parked, or blocked behind the same earlier row). A not-yet-due
record is held and woken from a heap -- never a sleep -- while due records
behind it keep flowing; a partition pauses only at ``max_records_per_partition``
outstanding records.

Write failures and the evidence model (owner decision D1, binding: the retry
consumer never parks a row because of a system-wide condition). A failed write
is classified (:func:`_classify_write_error`). Connectivity errors (including a
read-only database) are never counted and never bisected; other failures
(including server-wide memory pressure) bisect the slice. A row
that fails *alone* is counted toward ``WRITE_FAILURE_LIMIT`` (then routed as
unresolved with ``write_failed``, so the normal attempts-then-park path
applies) only with evidence the failure is its own: an allow-listed row error,
or a WRITTEN outcome of the same edge type after the row's first uncounted
failure (UNRESOLVED outcomes are no evidence: they may have run only a read).
Without evidence the row waits indefinitely. Per edge type the state is:

* *healthy*: nothing failing;
* *stalled*: writes failing with nothing written since the stall began. One
  failing flush per capped-exponential backoff period (``_StallState``), solo
  probe writes first once it persists, a ``reason=retry_stalled`` ERROR and the
  ``graph_loader_retry_stalled_seconds`` gauge; it ends at the first WRITTEN
  outcome or once no failing row is left;
* *fatal*: only authentication / missing-database errors, continuously, for
  ``_FATAL_OUTAGE_SECONDS``: the process exits non-zero and stream supervision
  stops the fleet (owner decision D2).

Rows that failed alone (*suspects*) are only written alone, after the other
rows, least recently tried first; a pair's rows always keep their order.

Poison-pill validation at read time (review finding M10): an
unrecoverable envelope shape (unreadable, or for an edge type this
consumer has no configuration for) is validated in :meth:`_on_message`,
next to the existing malformed/unreadable checks — logged and committed
past, never raised out of :meth:`tick`. A record that crashed this
consumer's run loop would otherwise crash it again, identically, on every
restart (the offset never committed), starving every *other* edge type's
retries too.

Duplicate-copy detection (review finding M12): idempotent Neo4j ``MERGE``
and idempotent index ``add``/``remove`` are **not** sufficient on their
own to make an at-least-once duplicate DLQ copy safe. P5-S3's own
contract republishes every already-published envelope in a batch when
even one row's DLQ publish fails partway through — so two copies of the
same record can genuinely exist with no crash at all. If both copies
independently cycle through backoff (neither blocks the other; each
excludes its own identity from :meth:`ParkedPairIndex.is_blocked_by_earlier`),
the first copy to resolve calls ``remove()``, which can unblock a *live*,
newer record of the same pair — and the second, now-stale copy then
overwrites that newer write. For every edge type with a
:class:`ParkedPairIndex`, this module now checks
:meth:`ParkedPairIndex.contains` (after the read barrier, M5) for each
due record: a record whose own entry is no longer present has already
been durably resolved by an earlier copy of itself (or, for a bulk
opt-in run, was never indexed at all, Decisions §1) — either way it is
committed past, never written or re-DLQ'd/parked again.

Superseded copies (final follow-ups, Part B): ``contains`` only detects a
copy whose record already left the index. A copy of a record that is still
indexed -- a P5-S3 replay duplicate, an at-least-once publish that reported
a failure but landed, or every older copy re-read after a restart or a
rebalance from a commit floor that a long-blocked row holds low -- used to
start its own re-DLQ lineage: each lineage republished a copy per cycle (the
DLQ grew with the number of lineages) and, one copy of a record being
written per batch, the lineages shared the record's attempts, so a row whose
endpoint never arrives took many times its budget to park. Now a copy is
skipped (committed past, ``duplicate_skipped``) when another copy of the
same record is kept in the same batch, or when this consumer already
re-DLQ'd a copy of the same lineage (same ``first_failure_at_ms``) at that
attempt or a later one: the newer copy carries the record. A fresh DLQ of
the record by the live loader starts a new lineage and is never skipped by
the memory; a revoke forgets its partitions' entries.

Replay resurrection (orchestrator decision under the P5-S3 -> P5-S4 handoff, option B): a crash, a
stop-drain abandon or a failed revoke settlement of a live edge loader redelivers a partly committed
flush, and the redelivered flush re-adds (``IndexedMissingEndpointSink.add`` is unconditional) and
republishes rows this consumer may already have written or parked and tombstoned. ``contains`` then
sees the older row as outstanding again, and it used to be applied after a newer row of its pair. This
consumer now remembers, per (edge type, canonical pair ``key_bytes``, original topic, original
partition), the highest source offset it wrote or parked (``_resolved_high``, LRU-bounded by
``_RESOLVED_TRACK_LIMIT``). It resolves a pair's records in order, so a still-indexed copy at or below
that offset can only be a resurrection (or a written / parked copy whose tombstone failed): its entry is
tombstoned and it is committed past (``stale_resurrected_copy_skipped``), never written. The memory is per process (a restart between the
two resolutions is not covered), offsets of different topics or partitions are never compared, and a
revoke forgets its partitions' entries, like the superseded memory. A replay (``replay-dlq``,
``--parked`` too) produces the record again at a NEW source offset, above the memory, so it is retried
as usual.

Due-batch fairness (final follow-ups, Part B): a frozen batch takes the
records that have been due the longest first (by the tick they became
due), then lowest offsets first as before. Offset order alone let followers
(original copies at low DLQ offsets, woken every few seconds) take every
capped batch ahead of their roots (re-DLQ'd copies at higher offsets): the
roots never ran, the commit position stood still and no stall was reported.
"""
from __future__ import annotations

import argparse
from collections import OrderedDict, defaultdict
import dataclasses
from dataclasses import dataclass, field
import heapq
import json
import logging
import os
import signal
import threading
import time
from typing import Callable, Mapping, Protocol, Sequence, runtime_checkable

from src.loader.dead_letter import (
    DeadLetterEnvelope,
    DeadLetterEnvelopeError,
    DeadLetterRecordSink,
    build_malformed_record_sink,
    decode_envelope,
)
from neo4j.exceptions import (  # noqa: F401 - module namespace kept; the taxonomy moved to write_errors
    ClientError, ConnectionPoolError, ServiceUnavailable, SessionExpired, TransientError,
)

from src.loader.lease_client import LeaseCoordinatorError  # noqa: F401 - module namespace kept
from src.loader import write_errors as _write_errors
from src.loader.parked_pair_index import ParkedPairIndex, warn_index_leftovers
from src.models.schema import (
    edge_dead_letter_topic_name, edge_parked_pair_index_topic_name, edge_parked_topic_name,
)
from src.utils import metrics as metrics_module
from src.utils.consumer_groups import legacy_retry_group
from src.utils.schema_loader import load_schema

logger = logging.getLogger(__name__)

BLOCKED_DELAY_CEILING_SECONDS = 3600.0
"""v7-S4 review S-a: a blocked copy that has to be published (over the hold budget, or
its blocker not fetched here) waits at least as long as the record has been blocked
(``now - first_failure_at_ms``), between the type's max delay and this ceiling, so a
long-blocked follower costs O(log t) copies, not one per root cycle."""

RETRY_OUTCOME_WRITTEN = "written"
RETRY_OUTCOME_UNRESOLVED = "unresolved"
"""The only two outcomes a :class:`RetryWriter` may report. "Blocked" is
never one of them -- blocking is decided by this module, via the parked-
pair index, before the writer is ever called for that record."""


WRITE_FAILURE_LIMIT = 3
"""Counted isolated write failures of one record before it is routed as
unresolved with ``reason_code="write_failed"`` (so the ordinary
attempts-then-park path applies instead of failing every flush forever). A
failure is only counted with evidence that it is the row's own (owner decision
D1): an allow-listed row error, or a WRITTEN outcome of the same edge type
after the row's first uncounted failure."""
WRITE_FAILURE_BACKOFF_SECONDS = (0.5, 5.0)
_ISOLATION_CALL_BUDGET = 32
"""Extra ``retry_batch`` calls one edge-type batch may spend per tick on
bisection sub-slices and solo tries (only non-empty sub-slices are charged)."""
_STALLED_STREAK = 2
"""An edge type whose last this-many batches resolved nothing is *stalled*: its
next batch starts with solo probe writes (one row at a time, least recently
tried first) instead of bisecting, and resumes normal writing as soon as a probe
gets an outcome. A failing probe ends the batch, so a stalled edge type costs
one failing call per backoff period."""
_MAX_STREAK = 32
"""Cap on the no-progress streak (and so on the backoff exponent): the delay
itself is capped at ``WRITE_FAILURE_BACKOFF_SECONDS[1]`` long before this."""
DEFAULT_FLUSH_BUDGET_SECONDS = 60.0
_SUPERSEDED_TRACK_LIMIT = 100_000
"""Records whose re-DLQ this consumer remembers in order to skip their older copies
(LRU). A forgotten entry only lets one stale copy start an extra lineage."""
_RESOLVED_TRACK_LIMIT = 100_000
"""(pair, original topic, original partition) keys whose highest resolved (written or parked)
source offset this consumer remembers, to skip a resurrected copy (LRU). A forgotten key
falls back to the behaviour without the memory: a resurrected copy is retried like any other."""
# The write-error taxonomy is shared with the live stream loaders' outage
# ride-out (src/loader/write_outage.py). It lives in src/loader/write_errors.py;
# these private aliases keep this module's names and behaviour unchanged.
_ROW_ERROR_CODES = _write_errors.ROW_ERROR_CODES
_SERVER_CODE_PREFIXES = _write_errors.SERVER_CODE_PREFIXES
_CAPACITY_CODE_PREFIXES = _write_errors.CAPACITY_CODE_PREFIXES
_CONNECTIVITY_CODE_PREFIXES = _write_errors.CONNECTIVITY_CODE_PREFIXES
_FATAL_CODE_PREFIXES = _write_errors.FATAL_CODE_PREFIXES
_STALL_LOG_AFTER_SECONDS = 120.0
_STALL_LOG_INTERVAL_SECONDS = 300.0
_FATAL_OUTAGE_SECONDS = _write_errors.FATAL_OUTAGE_SECONDS
_FATAL_GAP_SECONDS = _write_errors.FATAL_GAP_SECONDS
_classify_write_error = _write_errors.classify_write_error
_is_fatal_error = _write_errors.is_fatal_error


def _capped_backoff(failures: int) -> float:
    """``0.5 s * 2**(failures-1)`` capped at 5 s, 0 for no failures; never
    overflows however large ``failures`` grows."""
    base, cap = WRITE_FAILURE_BACKOFF_SECONDS
    if failures <= 0:
        return 0.0
    return min(base * 2 ** min(failures - 1, 16), cap)


class _PartialOutcomes(dict):
    """A writer's partial ``{index: outcome}`` result that also carries the
    error that stopped the rest, so the consumer classifies the rows left
    without an outcome by that error instead of treating them as an unknown
    failure."""

    def __init__(self, outcomes, error: BaseException) -> None:
        super().__init__(outcomes)
        self.error = error


class RetryConsumerError(RuntimeError):
    """Raised on a RetryConsumer construction or envelope-shape error."""


class RetryConsumerFatalError(RetryConsumerError):
    """Raised out of :meth:`RetryConsumer.tick` when an edge type's writes
    have failed for ``_FATAL_OUTAGE_SECONDS`` with nothing but errors that
    will not heal by waiting (authentication, a missing database). The process
    exits non-zero; in ``--mode stream`` the CLI's stream supervisor
    (``RelationshipBulkMonitor.supervise_stream``) sees the exited retry
    container and stops the fleet with rc 1 (owner decision D2). A standalone
    run just exits 1."""


@runtime_checkable
class RetryWriter(Protocol):
    """Duck-typed write path this module retries through.

    Phase B's real implementation is ``ApocLockedEdgeWriter.
    retry_provenanced_batch`` (P5-S4 Story 6), driven either directly
    (:class:`ApocRetryWriterAdapter`, standalone mode) or through the live
    bucket-lease coordinator (:class:`LeaseCoordinatedRetryWriter`, P5-Q1
    fencing: retry writes share the run's live lease server, are cut into
    node-disjoint cell transactions exactly like the edge loader's own
    flushes, and never use a private connection).
    """

    def retry_batch(
        self, envelopes: Sequence[DeadLetterEnvelope], *, budget_seconds: float | None = None,
    ) -> dict[int, tuple[str, str]]:
        """Return ``{index: (outcome, reason_code)}`` for every envelope in
        ``envelopes`` whose outcome is durable: written, or determined
        still-unresolved. ``reason_code`` is only meaningful when
        ``outcome == RETRY_OUTCOME_UNRESOLVED`` (ignored otherwise).

        A large batch is written as several transactions, so the result may
        be *partial*: an index absent from it was not attempted (or its
        transaction did not commit) and the caller leaves that record
        outstanding for the next tick. ``budget_seconds`` bounds the call:
        once it is spent no further write starts and the rest is left
        without an outcome. Raises only when nothing at all is
        durable; the whole batch is then safe to retry from scratch
        (idempotent ``MERGE``).
        """
        ...


class ApocRetryWriterAdapter:
    """Explicit :class:`RetryWriter` adapter over the real
    ``ApocLockedEdgeWriter``, whose retry entry point is named
    ``retry_provenanced_batch`` (BUG-1: main() used to wire the bare writer,
    which has no ``retry_batch``, so every tick raised AttributeError).
    Arguments and results map one-to-one: envelopes in, ``{index: (outcome,
    reason_code)}`` out. Used without a lease server (standalone mode),
    where a batch is written as sequential transactions of at most
    ``chunk_size`` envelopes; ``order_by`` and same-pair semantics live
    inside the writer itself.
    """

    def __init__(self, writer, *, chunk_size: int | None = None) -> None:
        self._writer = writer
        self._chunk_size = chunk_size

    def retry_batch(
        self, envelopes: Sequence[DeadLetterEnvelope], *, budget_seconds: float | None = None,
    ) -> dict[int, tuple[str, str]]:
        from src.loader.edge_execution import LeaseDrainAbandoned, _ParkedPairSet

        if budget_seconds is not None and budget_seconds <= 0:
            # Nothing may start once the budget is spent (the same contract as
            # the lease-coordinated writer); the consumer backs off, uncounted.
            raise LeaseDrainAbandoned("retry flush time budget spent before the first chunk")
        if not self._chunk_size or len(envelopes) <= self._chunk_size:
            return self._writer.retry_provenanced_batch(envelopes)
        # One flush-local set across the chunks, so a pair left unresolved by
        # an earlier chunk blocks its later rows (the lease path gets the same
        # guard from the coordinator's per-flush state).
        parked_pairs = _ParkedPairSet()
        outcomes: dict[int, tuple[str, str]] = {}
        expires_at = None if budget_seconds is None else time.monotonic() + budget_seconds
        for start in range(0, len(envelopes), self._chunk_size):
            if expires_at is not None and outcomes and time.monotonic() >= expires_at:
                # Budget spent: the remaining chunks stay outstanding, classified
                # as a drain abandon (never counted against them).
                return _PartialOutcomes(outcomes, LeaseDrainAbandoned("retry flush time budget spent"))
            chunk = envelopes[start:start + self._chunk_size]
            try:
                chunk_outcomes = self._writer.retry_provenanced_batch(chunk, parked_pairs=parked_pairs)
            except Exception as exc:
                if not outcomes:
                    raise
                logger.exception(
                    "stage=retry-consumer reason=chunk_write_failed -- keeping the %d durable "
                    "outcome(s) of the earlier chunk(s)", len(outcomes),
                )
                return _PartialOutcomes(outcomes, exc)
            outcomes.update({start + index: outcome for index, outcome in chunk_outcomes.items()})
        return outcomes


@runtime_checkable
class BacklogSource(Protocol):
    """Duck-typed per-edge-type Kafka metadata source for the DLQ backlog
    gauge (P5-S4 Story 7, the P5-Q4 follow-up). Phase B wires a real
    implementation backed by a dedicated watermark-only connection
    (mirroring :class:`src.loader.parked_pair_index.ParkedPairIndex`'s own
    ``watermark_snapshot`` precedent: metadata-only broker round trips,
    never touching the main poll-loop consumer)."""

    def dlq_high_watermark_and_committed(self, dlq_topic: str) -> tuple[int, int]:
        """Return ``(high_watermark, committed_offset)`` summed across
        every partition of ``dlq_topic`` -- the committed offset is this
        retry consumer's own consumer group's position."""
        ...

    def parked_watermarks(self, parked_topic: str) -> tuple[int, int]:
        """Return ``(high_watermark, low_watermark)`` summed across every
        partition of ``parked_topic``."""
        ...


def compute_dlq_backlog(
    *, dlq_high_watermark: int, dlq_committed_offset: int,
    parked_high_watermark: int, parked_low_watermark: int,
) -> dict[str, int]:
    """The two queues of the P5-Q4 follow-up's backlog gauge
    (``graph_loader_dlq_backlog{queue=...}``, P5-S4 Story 7):
    ``dlq`` = DLQ topic high watermark minus this retry consumer's own
    committed offset (retry work that should drain) and ``parked`` = the
    parking-lot topic's current size (high minus low watermark; terminal
    and cumulative). Never negative in either term: a momentarily stale
    watermark read (the two calls are not atomic with each other) is
    clamped rather than reported as a negative backlog.
    """
    return {
        "dlq": max(0, dlq_high_watermark - dlq_committed_offset),
        "parked": max(0, parked_high_watermark - parked_low_watermark),
    }


@dataclass(frozen=True)
class _PendingRecord:
    envelope: DeadLetterEnvelope
    topic: str
    partition: int
    offset: int

    @property
    def key(self) -> tuple[str, int, int]:
        return (self.topic, self.partition, self.offset)


@dataclass
class _FrozenBatch:
    """One edge type's due records, membership fixed at the moment this
    batch was frozen, alongside the single read-barrier snapshot taken for
    it (review finding M8). ``None`` when this edge type has no
    ``ParkedPairIndex`` at all (an ``order_by``-configured type, Phase B).
    Keys are ``(topic, partition, offset)``."""

    keyed_pendings: list[tuple[tuple[str, int, int], _PendingRecord]] = field(default_factory=list)
    snapshot: dict[int, int] | None = None


def _pair_from_envelope(envelope: DeadLetterEnvelope) -> tuple[object, object]:
    """Return ``(source_key, target_key)`` from the envelope's own
    ``key_bytes`` -- ``DeadLetterRecordSink.route_unresolved_endpoint``
    already encodes exactly ``[source_key, target_key]`` there (P5-S3), so
    this module reuses it as the pair identity rather than re-deriving it
    from ``value_bytes`` (which would need this edge type's own
    ``source_key_property``/``target_key_property`` to decode)."""
    if envelope.key_bytes is None:
        raise RetryConsumerError(
            f"envelope for edge '{envelope.entity_name}' has no key_bytes -- cannot "
            "determine its (source_key, target_key) pair identity"
        )
    try:
        decoded = json.loads(envelope.key_bytes.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise RetryConsumerError(
            f"envelope for edge '{envelope.entity_name}' has unreadable key_bytes: {exc}"
        ) from exc
    if isinstance(decoded, list) and len(decoded) == 4 and all(isinstance(label, str) for label in decoded[:2]):
        # v7-S7: a type of several label pairs keys its rows [S, T, source_key, target_key].
        return decoded[2], decoded[3]
    if not isinstance(decoded, list) or len(decoded) != 2:
        raise RetryConsumerError(
            f"envelope for edge '{envelope.entity_name}' key_bytes must decode to a "
            f"[source_key, target_key] or [source_label, target_label, source_key, target_key] list, "
            f"got {decoded!r}"
        )
    return decoded[0], decoded[1]


def _pair_labels_from_envelope(envelope: DeadLetterEnvelope) -> tuple[str, str] | None:
    """``(S, T)`` of a labelled key (v7-S7), or ``None`` for a ``[source_key, target_key]`` key."""
    try:
        decoded = json.loads(envelope.key_bytes.decode("utf-8"))
    except (AttributeError, UnicodeDecodeError, ValueError):
        return None
    if isinstance(decoded, list) and len(decoded) == 4 and all(isinstance(label, str) for label in decoded[:2]):
        return decoded[0], decoded[1]
    return None


def canonical_pair_key(envelope: DeadLetterEnvelope, labels_by_topic: Mapping[str, tuple[str, str]]):
    """A multi-pair type's envelope with its legacy ``[sk, tk]`` key rewritten to ``[S, T, sk, tk]``
    of its ``original_topic``'s pair (v7-S7), so every ``key_bytes`` grouping site sees one identity per
    labelled pair. Labelled keys, unknown topics and single-pair types are returned unchanged."""
    labels = labels_by_topic.get(envelope.original_topic)
    if labels is None or envelope.key_bytes is None:
        return envelope
    try:
        decoded = json.loads(envelope.key_bytes.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return envelope
    if not isinstance(decoded, list) or len(decoded) != 2:
        return envelope
    key = json.dumps([labels[0], labels[1], decoded[0], decoded[1]], separators=(",", ":")).encode("utf-8")
    return dataclasses.replace(envelope, key_bytes=key)


def single_pair_key(envelope: DeadLetterEnvelope, labels: tuple[str, str]):
    """A single-pair type's envelope with a labelled key OF ITS OWN PAIR, ``[S, T, sk, tk]`` (left in its
    DLQ when the type had several label pairs), rewritten to the legacy ``[sk, tk]`` (final follow-ups
    S2): the mirror of :func:`canonical_pair_key`, so one relationship never has two ``key_bytes``
    identities. A key labelled with another pair is left as it is (the writer refuses it, so it parks);
    legacy keys and unreadable keys are returned unchanged."""
    if envelope.key_bytes is None:
        return envelope
    try:
        decoded = json.loads(envelope.key_bytes.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return envelope
    if not (isinstance(decoded, list) and len(decoded) == 4 and tuple(decoded[:2]) == tuple(labels)):
        return envelope
    key = json.dumps(decoded[2:], separators=(",", ":")).encode("utf-8")
    return dataclasses.replace(envelope, key_bytes=key)


class PairSplitRetryWriter:
    """:class:`RetryWriter` of a type whose topics bind several label pairs (v7-S7).

    Splits a due batch by the pair of each envelope's ``original_topic`` (stable) and retries each
    pair through its own writer (built from that pair's view: its own queries, router and lease
    coordinator) with the same ``budget_seconds``. No transaction mixes pairs. ``concurrent=True``
    (lease mode only: the lease server serialises shared endpoint buckets) runs the pairs in parallel;
    otherwise (standalone mode, explicit APOC locks, no leases) one after another, so two pairs never
    wait on each other's Neo4j locks. A mismatched envelope never reaches a pair writer (its
    lease routing would use the raw wire keys).

    An envelope whose ``original_topic`` is not a topic of the type, or whose key labels differ from
    that topic's pair, is reported ``("unresolved", "invalid_on_retry")`` without touching Neo4j: the
    row's own condition (D1), so the bounded attempts then park it. If no envelope got an outcome and
    a pair writer raised, the first error is re-raised (a real error before a drain abandon, in pair
    order); with some outcomes the result is ``_PartialOutcomes(outcomes, that error)``.

    ``observe_lease_wait``: one ``lease_wait`` observation per call (``kind="retry-consumer"``) with
    the sum of the pairs' ``last_lease_wait_seconds`` (pair coordinators built not to observe), as
    :class:`~src.loader.edge_execution.PairSplitLeaseCoordinator` does for the live loader."""

    def __init__(self, writers_by_pair: Mapping[int, RetryWriter],
                 pair_of_envelope: Callable[[DeadLetterEnvelope], int | None], *, edge_type: str = "",
                 concurrent: bool = False, metrics=None, observe_lease_wait: bool = False) -> None:
        self._writers = dict(sorted(writers_by_pair.items()))
        self._pair_of = pair_of_envelope
        self._edge_type = edge_type
        self._concurrent = concurrent
        self._metrics = metrics
        self._observe_lease_wait = observe_lease_wait
        self.last_lease_wait_seconds = 0.0

    def retry_batch(
        self, envelopes: Sequence[DeadLetterEnvelope], *, budget_seconds: float | None = None,
    ) -> dict[int, tuple[str, str]]:
        from concurrent.futures import ThreadPoolExecutor

        from src.loader.edge_execution import LeaseDrainAbandoned

        outcomes: dict[int, tuple[str, str]] = {}
        groups: dict[int, list[int]] = {}
        for index, envelope in enumerate(envelopes):
            pair = self._pair_of(envelope)
            if pair is None or pair not in self._writers:
                logger.error(
                    "stage=retry edge=%s reason=invalid_on_retry topic=%s partition=%s offset=%s -- the "
                    "envelope's topic or key labels bind no label pair of the type", self._edge_type,
                    envelope.original_topic, envelope.original_partition, envelope.original_offset,
                )
                outcomes[index] = ("unresolved", "invalid_on_retry")
                continue
            groups.setdefault(pair, []).append(index)
        errors: dict[int, BaseException] = {}

        def run(pair: int):
            return self._writers[pair].retry_batch([envelopes[i] for i in groups[pair]], budget_seconds=budget_seconds)

        ordered = sorted(groups)
        results: dict[int, dict] = {}
        if len(ordered) == 1 or not self._concurrent:
            for pair in ordered:
                try:
                    results[pair] = run(pair)
                except Exception as exc:
                    errors[pair] = exc
        elif ordered:
            with ThreadPoolExecutor(max_workers=len(ordered), thread_name_prefix="retry-pair") as executor:
                futures = {pair: executor.submit(run, pair) for pair in ordered}
                for pair, future in futures.items():
                    try:
                        results[pair] = future.result()
                    except Exception as exc:  # every pair finishes before anything is reported
                        errors[pair] = exc
        if ordered:
            wait = sum(getattr(self._writers[pair], "last_lease_wait_seconds", 0.0) for pair in results)
            self.last_lease_wait_seconds = wait
            if self._observe_lease_wait:
                metrics = self._metrics if self._metrics is not None else metrics_module.process_metrics()
                metrics_module.safe_emit(lambda: metrics.lease_wait.labels(
                    kind="retry-consumer", name=self._edge_type,
                ).observe(wait))
        for pair, sub in results.items():
            for local, outcome in sub.items():
                outcomes[groups[pair][local]] = outcome
            error = getattr(sub, "error", None)
            if error is not None:
                errors.setdefault(pair, error)
        if errors:
            ranked = [errors[pair] for pair in sorted(errors)]
            failure = next((exc for exc in ranked if not isinstance(exc, LeaseDrainAbandoned)), ranked[0])
            if not outcomes:
                raise failure
            return _PartialOutcomes(outcomes, failure)
        return outcomes

    def close(self) -> None:
        for writer in self._writers.values():
            close = getattr(writer, "close", None)
            if callable(close):
                close()


@dataclass
class _EdgeBatchResult:
    """What one edge type's batch left behind. ``remaining`` did not get a
    durable outcome. ``followers`` are ``(row, blocker)`` pairs: rows blocked
    (per the index) behind a row of this same batch that ended without an
    outcome -- held locally at the blocker's backoff, never re-published.
    ``requeued`` were blocked only behind rows this batch just wrote or parked
    (or behind other requeued rows): they are due again locally, never
    re-published. ``stalled`` means nothing was resolved by a write, a re-DLQ
    or a park (republishing a row as merely "blocked" is not progress)."""

    remaining: list = field(default_factory=list)
    followers: list = field(default_factory=list)
    requeued: list = field(default_factory=list)
    stalled: bool = False


@dataclass
class _FlushState:
    """What one ``_write_candidates`` call observed, for the health trackers."""

    attempted: bool = False
    wrote: bool = False
    any_outcome: bool = False
    neo4j_outcome: bool = False  # an outcome Neo4j produced (not invalid_on_retry)
    errors: list = field(default_factory=list)  # (error class, exception or None) per failed call


@dataclass
class _StallState:
    """An edge type whose writes fail with nothing written since ``since``.
    ``flushes`` counts its consecutive failing flushes (capped); no write of
    the edge type starts before ``retry_at`` (capped exponential backoff), so
    a stalled edge type costs one failing flush per backoff period however many
    rows it holds and however much its other rows progress (``retry_at_ms``)."""

    since: float
    flushes: int = 0
    retry_at_ms: int = 0  # wall clock, like every hold
    last_log: float | None = None
    error_class: str = "unknown"
    error: str = ""


def _identity_of(pending: _PendingRecord) -> tuple[str, int, int]:
    """The record identity (original topic, partition, offset)."""
    envelope = pending.envelope
    return (envelope.original_topic, envelope.original_partition, envelope.original_offset)


def _order_of(envelope: DeadLetterEnvelope, identity: tuple[str, int, int], index) -> tuple:
    """Position of the record ``identity`` (a record of ``envelope``'s pair) in
    its pair's order: the parked-pair index's ``order_key`` (v7-S4: a causal
    per-pair sequence leads it for a multi-topic type, so records of one pair
    from several topics keep their arrival order), else ``(0, *identity)``.
    Every ordering comparison here goes through this, so it always agrees
    with :meth:`ParkedPairIndex.is_blocked_by_earlier`."""
    order_key = getattr(index, "order_key", None) if index is not None else None
    if order_key is None:
        return (0, *identity)
    source_key, target_key = _pair_from_envelope(envelope)
    return tuple(order_key(
        source_key, target_key, original_topic=identity[0], original_partition=identity[1],
        original_offset=identity[2],
    ))


def _topic_partition(topic: str, partition: int, offset: int | None = None):
    from confluent_kafka import TopicPartition

    if offset is None:
        return TopicPartition(topic, partition)
    return TopicPartition(topic, partition, offset)


@dataclass(frozen=True)
class _PairRecord:
    """The ``record`` shape the lease coordinator routes by (source/target key)."""

    source_key: object
    target_key: object


class _RetryItem:
    """One retried envelope as a lease-coordinator work item.

    ``outcome`` is filled in by the bridge writer once the envelope's own
    transaction has committed; it stays ``None`` for a row whose cell was
    never written (a failed/abandoned flush).
    """

    __slots__ = ("record", "envelope", "outcome")

    def __init__(self, envelope: DeadLetterEnvelope, record_from_envelope=None) -> None:
        # The lease cell must be computed from the SAME normalized keys the live
        # edge loader routes by (a date target is a ``date`` there, but a JSON
        # object/string in the envelope's key_bytes): hashing the raw wire form
        # would lease a different bucket and leave the real nodes unfenced.
        keys = None
        if record_from_envelope is not None:
            try:
                decoded = record_from_envelope(envelope)
                keys = (decoded.source_key, decoded.target_key)
            except Exception:
                keys = None  # undecodable: the writer reports it unresolved without touching Neo4j
        if keys is None:
            keys = _pair_from_envelope(envelope)
        self.record = _PairRecord(*keys)
        self.envelope = envelope
        self.outcome: tuple[str, str] | None = None


class RetryCoordinatorBridge:
    """The 'edge writer' handed to ``BucketLeaseExecutionCoordinator`` so it
    can drive retry traffic through the same cell routing the live edge
    loader uses.

    The coordinator leases one cell (plus merged neighbouring cells) at a
    time, takes its oldest rows first (so a pair's rows keep their order), and
    calls :meth:`write_provenanced_batch` once per node-disjoint transaction
    with the lease deadline and liveness check. This bridge forwards that to
    ``ApocLockedEdgeWriter.retry_provenanced_batch`` and stores each
    envelope's outcome on its work item; it publishes nothing (re-DLQ/park is
    the consumer's decision, made only after the write is durable).
    """

    row_level_dlq_enabled = True

    def __init__(self, writer) -> None:
        self._writer = writer

    def write_provenanced_batch(
        self, items: Sequence[_RetryItem], *, deadline: float | None = None,
        lease_liveness_check: Callable[[], bool] | None = None, parked_pairs=None,
    ) -> None:
        outcomes = self._writer.retry_provenanced_batch(
            [item.envelope for item in items], deadline=deadline,
            lease_liveness_check=lease_liveness_check, parked_pairs=parked_pairs,
        )
        for index, item in enumerate(items):
            item.outcome = outcomes.get(index)

    def write_batch(self, records, **_kwargs) -> None:  # pragma: no cover - defensive
        raise RetryConsumerError("the retry bridge only writes provenanced batches")


class LeaseCoordinatedRetryWriter:
    """:class:`RetryWriter` over the live bucket-lease coordinator (P5-Q1).

    A whole due batch is handed to the coordinator in one
    ``execute_pending`` call: rows are grouped into (source bucket, target
    bucket) cells, worker threads lease cells (one ``ANY`` request over the
    largest pending cells, merging small neighbours), and each leased group is
    written in ONE node-disjoint transaction whose timeout is capped at the
    lease deadline. So every concurrently running transaction in the fleet,
    retry or live, stays node-disjoint (zero Neo4j lock waits), and a pair's
    rows are written in FIFO order. Thousands of rows per call cost a handful
    of lease round trips instead of one giant union lease.

    A failure mid-flush (Neo4j error, lease loss, stop-drain budget spent)
    keeps the outcomes of every transaction that already committed; the
    failure itself is raised only when nothing committed. Rows without an
    outcome stay outstanding in the consumer. ``budget_seconds`` (the tick's
    remaining flush budget) makes the coordinator stop waiting for leases
    once it is spent, abandoning the rest of the call like the stop drain.
    """

    def __init__(self, coordinator, *, record_from_envelope=None) -> None:
        self._coordinator = coordinator
        self._record_from_envelope = record_from_envelope

    @property
    def last_lease_wait_seconds(self) -> float:
        """The coordinator's lease wait in its last ``execute_pending`` (0 before any)."""
        return float(getattr(self._coordinator, "last_lease_wait_seconds", 0.0) or 0.0)

    def retry_batch(
        self, envelopes: Sequence[DeadLetterEnvelope], *, budget_seconds: float | None = None,
    ) -> dict[int, tuple[str, str]]:
        if not envelopes:
            return {}
        items = [_RetryItem(envelope, self._record_from_envelope) for envelope in envelopes]
        results = self._coordinator.execute_pending(items, budget_seconds=budget_seconds)
        outcomes = {index: item.outcome for index, item in enumerate(items) if item.outcome is not None}
        failure = next((result.exception for result in results if result.exception is not None), None)
        if failure is not None:
            if not outcomes:
                raise failure
            logger.warning(
                "stage=retry-consumer reason=partial_flush durable=%d of %d error=%s -- the rest stays "
                "outstanding", len(outcomes), len(items), failure,
            )
            return _PartialOutcomes(outcomes, failure)
        return outcomes

    def close(self) -> None:
        self._coordinator.close()


def build_retry_base_writer(driver, edge, *, tx_timeout_seconds, metrics=None, leased: bool = False):
    """Construct the ``ApocLockedEdgeWriter`` main() uses for one edge type
    (BUG-2: the single construction path, so tests exercise the real one).
    Fails fast if the writer cannot run ``retry_provenanced_batch``.

    ``leased=True`` mirrors the live edge loader's lease-mode writer:
    leases already make every transaction node-disjoint, so the explicit APOC
    lock step is skipped and rows are not re-sorted (the sort is not stable
    for repeated pairs and would break last-write-wins)."""
    from src.loader.edge_execution import ApocLockedEdgeWriter
    from src.loader.edge_loader import MissingRelationshipEndpointError

    # sort_rows=False always: the sort is not stable for repeated (source,
    # target) pairs within a transaction, which would break replay order.
    lease_kwargs = {"explicit_locks": False, "sort_rows": False} if leased else {"sort_rows": False}
    writer = ApocLockedEdgeWriter(
        driver, edge, missing_endpoint_error_factory=MissingRelationshipEndpointError,
        tx_timeout_seconds=tx_timeout_seconds, kind="edge", metrics=metrics, **lease_kwargs,
    )
    query = getattr(writer, "_classification_query", None)
    if not isinstance(query, str) or not query.strip():
        raise RuntimeError(f"retry writer for edge {edge.type} has no endpoint classification query")
    return writer


def build_retry_writer(
    base_writer, *, coordinator=None, chunk_size: int | None = None,
) -> RetryWriter:
    """Build the exact writer main() hands to :class:`RetryConsumer` for one
    edge type: lease-coordinated when a coordinator is present (the
    coordinator's own writer must be a :class:`RetryCoordinatorBridge` around
    ``base_writer``), otherwise the plain adapter writing ``chunk_size``-row
    transactions."""
    if coordinator is not None:
        record_from_envelope = getattr(base_writer, "_record_from_envelope", None)
        if record_from_envelope is None:
            # Routing by the raw wire keys would lease different buckets than
            # the live loader for typed keys (dates), silently unfencing nodes.
            raise RetryConsumerError(
                "a lease-coordinated retry writer needs base_writer._record_from_envelope to route by "
                "the normalized record keys"
            )
        return LeaseCoordinatedRetryWriter(coordinator, record_from_envelope=record_from_envelope)
    return ApocRetryWriterAdapter(base_writer, chunk_size=chunk_size)


def build_retry_lease_coordinator(
    base_writer, edge, *, router, client_factory, worker_count: int, tx_timeout_seconds: float | None,
    stop_event: threading.Event | None = None, metrics=None, drain_deadline=None,
    observe_lease_wait: bool = True,
):
    """Build (and prime) the live-fleet lease coordinator one edge type's
    retry writer runs through: the same ``BucketLeaseExecutionCoordinator``,
    batch size and merge budget as the edge loader's own lease mode.

    ``observe_lease_wait=False`` for one label pair of a multi-pair type
    (final follow-ups S1): its :class:`PairSplitRetryWriter` observes the
    sum of its pairs' waits once per call instead, as a single-pair type's
    coordinator does, so the histogram counts the same calls for both."""
    from src.loader.edge_execution import (
        DEFAULT_LEASE_MERGE_ROWS,
        LEASE_MERGE_ROWS_ENV,
        BucketLeaseExecutionCoordinator,
    )

    merge_raw = os.environ.get(LEASE_MERGE_ROWS_ENV)
    coordinator = BucketLeaseExecutionCoordinator(
        RetryCoordinatorBridge(base_writer), router=router, worker_count=worker_count,
        batch_size=edge.mix_and_batch.batch_size,
        merge_target_rows=int(merge_raw) if merge_raw else DEFAULT_LEASE_MERGE_ROWS,
        client_factory=client_factory, edge_type=edge.type, metrics=metrics,
        # Distinct ``kind`` label (closed process-kind enum, same series name)
        # so retry lease waits never merge with the live edge loader's.
        kind="retry-consumer",
        tx_timeout_seconds=tx_timeout_seconds, stop_event=stop_event, drain_deadline=drain_deadline,
        observe_lease_wait=observe_lease_wait,
    )
    coordinator.prime()
    return coordinator


def build_pair_retry_writers(
    edge, make_writer, *, concurrent: bool = False, metrics=None, observe_lease_wait: bool = False,
) -> RetryWriter:
    """One type's retry writer: ``make_writer(edge, None)`` for a single-pair type (unchanged); for a
    type of several label pairs (v7-S7), a :class:`PairSplitRetryWriter` over
    ``make_writer(edge.pair_view(pair), drain_deadline)`` per pair, with one shared stop-drain deadline.
    ``concurrent`` only with lease-coordinated pair writers. ``observe_lease_wait`` (lease mode, pair
    coordinators built with ``observe_lease_wait=False``): the split writer observes ``lease_wait``
    once per call, ``kind="retry-consumer"``."""
    if not edge.is_multi_pair:
        return make_writer(edge, None)
    from src.loader.edge_execution import _DrainDeadline

    shared = _DrainDeadline()
    writers = {pair.index: make_writer(edge.pair_view(pair), shared) for pair in edge.endpoint_pairs}
    by_topic = {topic: pair for pair in edge.endpoint_pairs for topic in pair.topics}

    def pair_of(envelope) -> int | None:
        pair = by_topic.get(envelope.original_topic)
        if pair is None:
            return None
        labels = _pair_labels_from_envelope(envelope)
        return pair.index if labels is None or labels == pair.labels else None

    return PairSplitRetryWriter(writers, pair_of, edge_type=edge.type, concurrent=concurrent, metrics=metrics,
                                observe_lease_wait=observe_lease_wait)


class RetryConsumer:
    """Pause/resume DLQ retry scheduler and lease-fenced retry-or-park writer.

    One instance per streaming run (fleet-wide, not per edge type —
    P5-S4 Decisions §2), subscribed to every DLQ-and-retry-enabled edge
    type's DLQ topic as a single Kafka consumer group. ``consumer`` is
    assumed already constructed and subscribed by the caller (mirrors
    every other loader in this codebase: ``main()`` owns Kafka client
    construction, this class owns only the run loop).

    Batched draining: each partition may hold up to
    ``max_records_per_partition`` outstanding records (held, due or in
    flight; it is paused only at that bound -- a record whose
    ``retry_after_ms`` is still in the future is held locally and never
    pauses its partition by itself). Every due record of an edge type, across its partitions, is
    written as ONE batch through the lease-fenced writer; per-row results are
    then routed (written / re-DLQ / park / blocked) with one DLQ publish
    flush and one parked-pair-index flush per batch, and each partition's
    offset is committed ONCE, at the highest offset below its lowest
    still-unresolved record.
    """

    def __init__(
        self,
        *,
        consumer,
        writer_by_edge_type: dict[str, RetryWriter],
        sink_by_edge_type: dict[str, DeadLetterRecordSink],
        parked_pair_index_by_edge_type: dict[str, ParkedPairIndex] | None = None,
        max_retry_attempts_by_edge_type: dict[str, int],
        parked_topic_by_edge_type: dict[str, str],
        base_delay_seconds_by_edge_type: dict[str, float],
        max_delay_seconds_by_edge_type: dict[str, float],
        blocked_retry_delay_seconds: float = 0.5,
        poll_timeout_seconds: float = 0.5,
        due_batch_coalesce_seconds: float = 0.2,
        max_batch_size: int = 2000,
        max_records_per_partition: int = 1000,
        max_overflow_held_per_partition: int | None = None,
        write_slice_size: int = 1000,
        flush_budget_seconds: float = DEFAULT_FLUSH_BUDGET_SECONDS,
        wall_clock: Callable[[], int] | None = None,
        monotonic_clock: Callable[[], float] | None = None,
        on_row_retried: Callable[[str, str], None] | None = None,
        backlog_source_by_edge_type: dict[str, BacklogSource] | None = None,
        dlq_topic_by_edge_type: dict[str, str] | None = None,
        backlog_check_interval_seconds: float = 10.0,
        on_backlog_computed: Callable[[str, str, int], None] | None = None,
        on_stall_seconds: Callable[[str, float], None] | None = None,
        endpoint_pairs_by_edge_type: Mapping[str, Mapping[str, tuple[str, str]]] | None = None,
        single_pair_labels_by_edge_type: Mapping[str, tuple[str, str]] | None = None,
    ) -> None:
        if max_batch_size < 1 or max_records_per_partition < 1:
            raise RetryConsumerError("max_batch_size and max_records_per_partition must be >= 1")
        self._consumer = consumer
        # v7-S7: {edge type: {topic: (S, T)}} for types whose topics bind several label pairs.
        self._endpoint_pairs_by_edge_type = dict(endpoint_pairs_by_edge_type or {})
        # Final follow-ups S2: {edge type: (S, T)} of single-pair types; a labelled key of that
        # pair (left from the type's multi-pair days) is read back as [sk, tk].
        self._single_pair_labels_by_edge_type = dict(single_pair_labels_by_edge_type or {})
        self._writer_by_edge_type = writer_by_edge_type
        self._sink_by_edge_type = sink_by_edge_type
        self._parked_pair_index_by_edge_type = parked_pair_index_by_edge_type or {}
        self._max_retry_attempts_by_edge_type = max_retry_attempts_by_edge_type
        self._parked_topic_by_edge_type = parked_topic_by_edge_type
        self._base_delay_seconds_by_edge_type = base_delay_seconds_by_edge_type
        self._max_delay_seconds_by_edge_type = max_delay_seconds_by_edge_type
        self._blocked_retry_delay_seconds = blocked_retry_delay_seconds
        self._poll_timeout_seconds = poll_timeout_seconds
        self._due_batch_coalesce_seconds = due_batch_coalesce_seconds
        self._max_batch_size = max_batch_size
        self._max_records_per_partition = max_records_per_partition
        # v7-S4: blocked rows held beyond the hold budget, per partition (offsets). They stay
        # held and outstanding (the commit never passes them) but are not counted towards
        # the pause or the hold budget, so the partition keeps fetching; bounded separately.
        self._overflow_held: dict[tuple[str, int], set[int]] = {}
        self._last_overflow_sweep = float("-inf")
        self._max_overflow_held_per_partition = (
            10 * max_records_per_partition if max_overflow_held_per_partition is None
            else max_overflow_held_per_partition
        )
        if write_slice_size < 1:
            raise RetryConsumerError("write_slice_size must be >= 1")
        self._write_slice_size = write_slice_size
        # Wall-time cap on everything one tick writes (kept well below
        # max.poll.interval.ms so the consumer is never evicted mid-flush):
        # no new write slice / edge-type batch starts once it is spent.
        self._flush_budget_seconds = flush_budget_seconds
        self._flush_deadline: float | None = None
        self._write_failures: dict[tuple[str, int, int], int] = {}
        # Consecutive batches per edge type that resolved nothing at all (an
        # outage, a poison row alone, a failing publish): drives the edge
        # type's capped exponential backoff so a dead dependency is never
        # hammered once per tick.
        self._no_progress_streak: dict[str, int] = {}
        # (edge type, pair key_bytes) -> {record identity: wake_ms} of the
        # earlier rows of a pair held locally (wake time clamped as held).
        self._held_by_pair: dict[tuple[str, bytes], dict[tuple[str, int, int], int]] = {}
        # Same shape, for rows this consumer re-DLQ'd with a delay: a later
        # row of the pair that is blocked behind one waits out its delay
        # instead of re-publishing itself every blocked_retry_delay_seconds.
        self._rerouted_wake: dict[tuple[str, bytes], dict[tuple[str, int, int], int]] = {}
        self._rerouted_heap: list[tuple[int, tuple[str, bytes], tuple[str, int, int]]] = []
        # Evidence model (owner decision D1). Every retry_batch call gets a
        # sequence number; _last_written_seq[edge type] is the number of the
        # latest call that produced a WRITTEN outcome for that edge type (only
        # WRITTEN: an UNRESOLVED outcome may have run only a read).
        # _solo_uncounted[key] is the call number of the row's first uncounted
        # failure; a later solo unknown/capacity failure counts only if a
        # WRITTEN call of the same edge type came after it. For a type of
        # several label pairs the evidence is per (edge type, label pair)
        # (orchestrator decision under D1, final follow-ups S4): a written
        # Person->Post row says nothing about a failing Person->Comment row.
        self._write_seq = 0
        self._last_written_seq: dict[object, int] = {}
        self._last_written_at: dict[str, float] = {}
        self._solo_uncounted: dict[tuple[str, int, int], int] = {}
        # key -> consecutive flushes in which the row's write failed (any
        # class, counted or not, alone or in a slice): the row's own capped
        # backoff, so it is never retried once per tick just because other rows
        # of its edge type make progress. _suspects: rows that failed *alone*
        # with a countable class; they are only ever written alone. key ->
        # write-call number of its last solo try: probes and suspects rotate,
        # least recently tried first.
        self._fail_streak: dict[tuple[str, int, int], int] = {}
        self._suspects: set[tuple[str, int, int]] = set()
        self._last_solo_try: dict[tuple[str, int, int], int] = {}
        # edge type -> keys of outstanding rows whose last write attempt failed.
        # Empty means the edge type is idle as far as write health goes.
        self._failing: dict[str, set[tuple[str, int, int]]] = {}
        # edge type -> sustained-stall state (writes failing, nothing written).
        self._stall: dict[str, _StallState] = {}
        # Final follow-ups S4 (multi-pair types): edge type -> {(edge type, label pair): monotonic time its
        # rows started failing with none of them written}; edge type -> last ERROR log of such a stall.
        self._pair_stall_since: dict[str, dict[object, float]] = {}
        self._pair_stall_last_log: dict[str, float] = {}
        # edge type -> (first, last) monotonic time of the current continuous
        # run of batches that failed only with fatal-class errors.
        self._fatal: dict[str, tuple[float, float]] = {}
        self._wall_clock = wall_clock or (lambda: int(time.time() * 1000))
        self._monotonic_clock = monotonic_clock or time.monotonic
        self._on_row_retried = on_row_retried
        self._on_stall_seconds = on_stall_seconds
        self._stop_event: threading.Event | None = None

        # DLQ backlog gauge (P5-S4 Story 7, P5-Q4 follow-up): computed on
        # its own dedicated background thread, on a periodic interval,
        # never on this consumer's own poll-loop thread/hot path (a brief
        # staleness window between computations is expected, documented,
        # not a bug). Absent entries (no backlog_source for an edge type)
        # are simply never computed -- e.g. an order_by-configured type
        # still gets a DLQ topic but no ParkedPairIndex; the backlog gauge
        # itself is independent of that and works for either.
        self._backlog_source_by_edge_type = backlog_source_by_edge_type or {}
        self._dlq_topic_by_edge_type = dlq_topic_by_edge_type or {}
        self._backlog_check_interval_seconds = backlog_check_interval_seconds
        self._on_backlog_computed = on_backlog_computed
        self._backlog_thread: threading.Thread | None = None
        self._backlog_stop_event = threading.Event()

        # Offset bookkeeping, per (topic, partition). A fetched record is
        # *outstanding* until its outcome (written / re-DLQ'd / parked /
        # blocked / duplicate-skipped) is durable. The committable position
        # is the lowest outstanding offset (or, with none outstanding, one
        # past the highest fetched offset): a commit can therefore never pass
        # an unresolved record, however out of order records resolve.
        self._outstanding: dict[tuple[str, int], set[int]] = {}
        self._fetched_hi: dict[tuple[str, int], int] = {}
        self._committed: dict[tuple[str, int], int] = {}
        # Not-yet-due records: held until their retry_after_ms passes, woken
        # through a heap -- never a sleep. Due records behind them keep
        # flowing; the commit simply stops at the lowest unresolved offset.
        self._held: dict[tuple[str, int, int], _PendingRecord] = {}
        self._wake_heap: list[tuple[int, str, int, int]] = []  # (retry_after_ms, topic, partition, offset)
        self._paused: set[tuple[str, int]] = set()
        self._due_this_tick: dict[tuple[str, int, int], _PendingRecord] = {}
        self._first_due_at: float | None = None
        # edge_type -> the one frozen, not-yet-fully-resolved due batch for
        # it (M8): at most one in flight per edge type at a time. A record
        # that becomes due while its edge type already has a frozen batch
        # simply waits in _due_this_tick for the *next* batch.
        self._pending_batches: dict[str, _FrozenBatch] = {}
        # Due-batch fairness (Part B): the tick in which each due record became due;
        # a frozen batch takes the longest-due records first.
        self._tick_number = 0
        self._due_since: dict[tuple[str, int, int], int] = {}
        # Superseded copies (Part B): (edge type, record identity, first_failure_at_ms) -- one lineage
        # of copies; a re-DLQ keeps first_failure_at_ms, a fresh DLQ by the live loader gets a new one --
        # -> (highest attempt_count of a copy of it this consumer re-DLQ'd (acknowledged), that copy's
        # DLQ (topic, partition)). Older copies of that lineage are skipped; a revoke drops the entries
        # of its partitions.
        self._superseded: OrderedDict[tuple, tuple[int, tuple[str, int]]] = OrderedDict()
        # Replay resurrection: (edge type, canonical pair key_bytes, original topic, original partition)
        # -> (highest original offset of a record of that pair this consumer wrote or parked, the DLQ
        # (topic, partition) of that copy). A still-indexed copy at or below it is a resurrection (a
        # redelivered live flush re-added a record already resolved here) or a written / parked copy whose
        # tombstone failed: it is tombstoned and committed past, never written (again). LRU-bounded; a
        # revoke drops the entries of its partitions.
        self._resolved_high: OrderedDict[tuple, tuple[int, tuple[str, int]]] = OrderedDict()
        # DLQ keys (topic, partition, offset) of multi-pair envelopes whose key labels bind another pair
        # than their topic: parked by their next batch, never grouped (final follow-ups nit).
        self._invalid_at_read: set[tuple[str, int, int]] = set()

    # ── Main loop ────────────────────────────────────────────────────────

    def run_forever(self, shutdown_requested: threading.Event) -> None:
        """Run until ``shutdown_requested`` is set. Best-effort on shutdown:
        the batch being written when the event is set finishes (or is
        abandoned by the lease coordinator's own bounded stop drain, see
        ``stop_event``), what it resolved is committed, and everything else
        -- held, buffered or unwritten records -- is simply not committed and
        is re-read from its last committed offset on restart."""
        self._stop_event = shutdown_requested
        while not shutdown_requested.is_set():
            self.tick()
        self._commit_resolved()



    def tick(self) -> None:
        """One iteration: wake now-due records, poll (and drain whatever is
        immediately available, up to ``max_batch_size``), flush a due batch
        if one is ready, and commit every partition's resolved prefix.
        Exposed directly (not only via :meth:`run_forever`) so tests can
        drive it deterministically."""
        self._tick_number += 1
        self._resume_due_partitions()
        self._sweep_overflow_holds()
        message = self._consumer.poll(self._poll_timeout())
        fetched = 0
        while message is not None:
            if message.error() is None:
                self._on_message(message)
            fetched += 1
            if fetched >= self._max_batch_size or self._stopping():
                break
            message = self._consumer.poll(0)
        self._maybe_flush_due_batch()
        self._commit_resolved()
        self._report_stalls()

    def _poll_timeout(self) -> float:
        """Never sleep in poll() while work is ready or about to be: a full
        (paused) partition would otherwise turn every coalesce window into a
        whole poll timeout."""
        if self._pending_batches:
            return min(self._poll_timeout_seconds, 0.05)
        if self._due_this_tick:
            if len(self._due_this_tick) >= self._max_batch_size:
                return 0.0
            elapsed = self._monotonic_clock() - (self._first_due_at or 0.0)
            return max(0.0, min(self._poll_timeout_seconds, self._due_batch_coalesce_seconds - elapsed))
        return self._poll_timeout_seconds

    def _on_message(self, message) -> None:
        topic, partition, offset = message.topic(), message.partition(), message.offset()
        tp = (topic, partition)
        if offset in self._outstanding.get(tp, ()) or offset < self._fetched_hi.get(tp, 0):
            # Redelivered while still held / buffered / in a frozen batch, or
            # already resolved but not yet committed (e.g. a seek after a
            # pause/resume race): it is already tracked or already done, and
            # tracking it again would double-process it. ``_fetched_hi`` is
            # reset on revoke, so a reassigned partition starts clean.
            return
        if tp not in self._committed:
            # The consumer's starting position for this partition: nothing
            # below it needs (or may be) committed again.
            self._committed[tp] = offset
        self._fetched_hi[tp] = max(self._fetched_hi.get(tp, 0), offset + 1)
        try:
            envelope = decode_envelope(message.value())
        except DeadLetterEnvelopeError as exc:
            logger.error(
                "stage=retry-consumer unreadable envelope topic=%s partition=%s offset=%s "
                "error=%s -- committing past it, it can never be retried",
                topic, partition, offset, exc,
            )
            return
        if envelope.failure_class == "malformed":
            # P5-Q2: never retried. Commit past it; it stays in the DLQ
            # topic (retention.ms=-1) for replay-dlq, untouched otherwise.
            return
        problem = self._unretryable_reason(envelope)
        if problem is not None:
            # Review finding M10: validated here, at read time, so a
            # poison-pill record (bad key_bytes, or an edge type this
            # consumer carries no configuration for -- e.g. renamed or
            # removed since this record was DLQ'd) can never raise out of
            # tick()/run_forever and crash-loop the whole fleet-wide
            # consumer on every restart. The record stays in the DLQ topic
            # (retention.ms=-1) -- reachable via replay-dlq, never dropped.
            logger.error(
                "stage=retry-consumer unretryable envelope topic=%s partition=%s offset=%s "
                "reason=%s -- committing past it",
                topic, partition, offset, problem,
            )
            return
        labels_by_topic = self._endpoint_pairs_by_edge_type.get(envelope.entity_name)
        mismatched = False
        if labels_by_topic is not None:
            # v7-S7: one identity per labelled pair at every key_bytes grouping site; a re-DLQ
            # or park republishes this canonical copy.
            envelope = canonical_pair_key(envelope, labels_by_topic)
            topic_labels = labels_by_topic.get(envelope.original_topic)
            key_labels = _pair_labels_from_envelope(envelope)
            mismatched = topic_labels is not None and key_labels is not None and tuple(key_labels) != tuple(topic_labels)
        elif envelope.entity_name in self._single_pair_labels_by_edge_type:
            # Final follow-ups S2: the mirror for a type collapsed back to one pair.
            envelope = single_pair_key(envelope, self._single_pair_labels_by_edge_type[envelope.entity_name])
        pending = _PendingRecord(envelope=envelope, topic=topic, partition=partition, offset=offset)
        self._outstanding.setdefault(tp, set()).add(offset)
        if mismatched:
            # Final follow-ups nit: a key labelled with another pair than its topic's is never grouped
            # with either pair's rows (held, ordered or blocking by key_bytes): it is parked by its
            # next batch, without a write (the writer would refuse it anyway, invalid_on_retry).
            logger.error(
                "stage=retry-consumer edge_type=%s reason=invalid_on_retry topic=%s partition=%s offset=%s -- the "
                "key's label pair %s is not the pair its topic %s binds (%s); parking it",
                envelope.entity_name, topic, partition, offset, ">".join(key_labels), envelope.original_topic,
                ">".join(topic_labels),
            )
            self._invalid_at_read.add(pending.key)
            self._mark_due(pending)
            self._sync_pause(tp)
            return
        now = self._wall_clock()
        if envelope.retry_after_ms <= now or self._blocker_already_gone(pending):
            self._mark_due(pending)
        elif not (
            envelope.reason_code == "blocked_by_earlier_unresolved_row"
            and self._counted_outstanding(tp) >= self._hold_budget()
            and self._overflow_hold(pending, envelope.retry_after_ms, now)
        ):
            self._hold(pending, envelope.retry_after_ms, now)
        self._sync_pause(tp)

    def _blocker_already_gone(self, pending: _PendingRecord) -> bool:
        """A published blocked copy (possibly with an age-scaled delay, S-a) whose earlier
        rows have all left the local index view is due at once instead of sleeping out its
        delay. Only an early retry: its batch re-checks everything behind the read barrier."""
        envelope = pending.envelope
        if envelope.reason_code != "blocked_by_earlier_unresolved_row":
            return False
        index = self._parked_pair_index_by_edge_type.get(envelope.entity_name)
        if index is None:
            return False
        try:
            source_key, target_key = _pair_from_envelope(envelope)
            return not index.is_blocked_by_earlier(
                source_key, target_key, original_topic=envelope.original_topic,
                original_partition=envelope.original_partition, original_offset=envelope.original_offset,
                first_failure_at_ms=envelope.first_failure_at_ms,
            )
        except Exception:
            return False

    def _hold_budget(self) -> int:
        """Outstanding records per partition below which blocked rows may be held for long:
        90% of max_records_per_partition, keeping room to fetch (v7-S4 review M1)."""
        return max(1, self._max_records_per_partition - max(1, self._max_records_per_partition // 10))

    def _counted_outstanding(self, tp: tuple[str, int]) -> int:
        """Outstanding records of ``tp`` that count towards the pause and the hold
        budget: all of them except the over-budget blocked holds (v7-S4)."""
        return len(self._outstanding.get(tp, ())) - len(self._overflow_held.get(tp, ()))

    def _overflow_hold(self, pending: _PendingRecord, wake_at_ms: int, now_ms: int) -> bool:
        """Hold a blocked row beyond the hold budget, up to its age-scaled delay, if the
        partition's overflow budget has room (else the caller publishes it)."""
        tp = (pending.topic, pending.partition)
        if len(self._overflow_held.get(tp, ())) >= self._max_overflow_held_per_partition:
            return False
        self._hold(pending, wake_at_ms, now_ms, overflow=True)
        return True

    def _sweep_overflow_holds(self) -> None:
        """At most every 5 s: an over-budget hold whose earlier rows have all left the
        local index view is due now, whatever resolved them (v7-S4). Only an early
        retry: its batch re-checks everything behind the read barrier."""
        now = self._monotonic_clock()
        if not self._overflow_held or now - self._last_overflow_sweep < 5.0:
            return
        self._last_overflow_sweep = now
        woken = set()
        for tp, offsets in list(self._overflow_held.items()):
            for offset in list(offsets):
                pending = self._held.get((tp[0], tp[1], offset))
                if pending is None:
                    continue
                index = self._parked_pair_index_by_edge_type.get(pending.envelope.entity_name)
                if index is None:
                    continue
                try:
                    source_key, target_key = _pair_from_envelope(pending.envelope)
                    envelope = pending.envelope
                    still = index.is_blocked_by_earlier(
                        source_key, target_key, original_topic=envelope.original_topic,
                        original_partition=envelope.original_partition, original_offset=envelope.original_offset,
                        first_failure_at_ms=envelope.first_failure_at_ms,
                    )
                except Exception:
                    continue
                if not still:
                    del self._held[pending.key]  # its heap entry is skipped once it pops
                    self._unhold(pending)
                    self._mark_due(pending)
                    woken.add(tp)
        for tp in woken:
            self._sync_pause(tp)

    def _hold_for(self, pending: _PendingRecord, delay_seconds: float, now_ms: int) -> None:
        self._hold(pending, now_ms + int(delay_seconds * 1000), now_ms)

    def _hold(self, pending: _PendingRecord, wake_at_ms: int, now_ms: int, *, overflow: bool = False) -> None:
        """Hold a not-yet-due record until ``wake_at_ms``, clamped to
        ``now + max_delay`` so a producer clock running ahead of this one can
        never park a record for longer than the edge type's own max backoff.
        ``overflow``: an over-budget blocked hold (not counted towards the pause)."""
        max_delay = self._max_delay_seconds_by_edge_type.get(pending.envelope.entity_name, 0.0)
        tp = (pending.topic, pending.partition)
        if overflow or (
            pending.envelope.reason_code == "blocked_by_earlier_unresolved_row"
            and self._counted_outstanding(tp) < self._hold_budget()
        ):
            # A blocked row may carry an age-scaled delay (S-a), honoured up to the
            # ceiling: within the hold budget, or as an overflow hold, which does not
            # count towards the pause, so it can never stall the partition's fetch.
            max_delay = max(max_delay, BLOCKED_DELAY_CEILING_SECONDS)
        if overflow:
            self._overflow_held.setdefault(tp, set()).add(pending.offset)
        wake_at_ms = min(wake_at_ms, now_ms + int(max_delay * 1000))
        heapq.heappush(self._wake_heap, (wake_at_ms, pending.topic, pending.partition, pending.offset))
        self._held[pending.key] = pending
        pair = (pending.envelope.entity_name, pending.envelope.key_bytes)
        self._held_by_pair.setdefault(pair, {})[_identity_of(pending)] = wake_at_ms

    def _unhold(self, pending: _PendingRecord) -> None:
        """Forget a held record's pair bookkeeping (it woke, or was revoked)."""
        overflow = self._overflow_held.get((pending.topic, pending.partition))
        if overflow is not None:
            overflow.discard(pending.offset)
            if not overflow:
                del self._overflow_held[(pending.topic, pending.partition)]
        pair = (pending.envelope.entity_name, pending.envelope.key_bytes)
        entries = self._held_by_pair.get(pair)
        if entries is not None:
            entries.pop(_identity_of(pending), None)
            if not entries:
                del self._held_by_pair[pair]

    def _held_blocker_wake_ms(self, pending: _PendingRecord, index: ParkedPairIndex | None = None) -> int | None:
        """Wake time of the earliest *earlier* record of ``pending``'s pair that
        is held locally (and, with an index, still indexed: a stale duplicate
        copy whose entry is gone blocks nothing), or ``None``."""
        entries = self._held_by_pair.get((pending.envelope.entity_name, pending.envelope.key_bytes))
        if not entries:
            return None
        identity = _identity_of(pending)
        source_key, target_key = _pair_from_envelope(pending.envelope)
        own = _order_of(pending.envelope, identity, index)
        wakes = [
            wake for other, wake in entries.items()
            if _order_of(pending.envelope, other, index) < own and (index is None or index.contains(
                source_key, target_key, original_topic=other[0], original_partition=other[1], original_offset=other[2],
            ))
        ]
        return min(wakes) if wakes else None

    def _local_blocker_wake_ms(self, pending: _PendingRecord, now_ms: int, requests: list, order, own_order) -> int | None:
        """When this consumer next tries an earlier row of ``pending``'s pair, plus
        1 ms (v7-S4 review M1), or ``None`` if it does not know: the latest of
        this batch's re-DLQ requests for earlier rows (now + their delay), their
        still-pending re-DLQ wakes, and earlier rows held or due locally."""
        index = self._parked_pair_index_by_edge_type.get(pending.envelope.entity_name)
        pair = (pending.envelope.entity_name, pending.envelope.key_bytes)
        wakes = [
            now_ms + int(kwargs["retry_after_seconds_for_reattempt"] * 1000)
            for other, notify_outcome, kwargs in requests
            if notify_outcome == "re_dlq" and other.envelope.key_bytes == pending.envelope.key_bytes
            and order(other) < own_order
        ]
        wakes.extend(
            wake for other, wake in (self._rerouted_wake.get(pair) or {}).items()
            if wake > now_ms and _order_of(pending.envelope, other, index) < own_order
        )
        wakes.extend(
            wake for other, wake in (self._held_by_pair.get(pair) or {}).items()
            if _order_of(pending.envelope, other, index) < own_order
        )
        if any(
            other.envelope.key_bytes == pending.envelope.key_bytes and other.envelope.entity_name == pair[0]
            and order(other) < own_order
            for other in self._due_this_tick.values()
        ):
            # The earlier row is due here but not in this batch: wait for it, not
            # for a short timer (a short hold re-enters ahead of it, lower DLQ
            # offsets first, and starves it). Its write or park wakes this row
            # early (_wake_held_followers); otherwise the type's max delay.
            max_delay = self._max_delay_seconds_by_edge_type.get(pair[0], self._blocked_retry_delay_seconds)
            wakes.append(now_ms + int(max(max_delay, self._blocked_retry_delay_seconds) * 1000))
        return max(wakes) + 1 if wakes else None

    def _wake_held_followers(self, edge_type: str, pairs: set, keep: set) -> None:
        """Rows held locally behind an earlier row of their pair become due again
        as soon as a row of that pair is written or parked (v7-S4 review M1),
        instead of sleeping out their hold; ``keep`` (held in this very batch)
        stay held. They are re-checked from scratch in their next batch."""
        if not pairs:
            return
        for key, pending in list(self._held.items()):
            if key in keep or pending.envelope.entity_name != edge_type or pending.envelope.key_bytes not in pairs:
                continue
            del self._held[key]  # its heap entry is skipped once it pops
            self._unhold(pending)
            self._mark_due(pending)

    def _blocked_delay_seconds(
        self, pending: _PendingRecord, now_ms: int, requests: list | None = None, *, local_wake_ms: int | None = None,
    ) -> float:
        """Delay for re-DLQ'ing a blocked record: the remaining wait of an earlier
        row of its pair this consumer re-DLQ'd (recorded once that publish was
        acknowledged, or being re-DLQ'd in the same publish flush, ``requests``)
        or holds (``local_wake_ms``), at least the fixed blocked delay; with no
        known wait, the edge type's max delay (v7-S4 review M1: the short fixed
        delay re-published every blocked follower twice a second). Capped at
        the edge type's max delay."""
        delay = self._blocked_retry_delay_seconds
        max_delay = self._max_delay_seconds_by_edge_type.get(pending.envelope.entity_name, delay)
        pair = (pending.envelope.entity_name, pending.envelope.key_bytes)
        index = self._parked_pair_index_by_edge_type.get(pending.envelope.entity_name)
        own = _order_of(pending.envelope, _identity_of(pending), index)
        entries = self._rerouted_wake.get(pair) or {}
        waits = [
            (wake - now_ms) / 1000.0 for other, wake in entries.items()
            if wake > now_ms and _order_of(pending.envelope, other, index) < own
        ]
        waits.extend(
            kwargs["retry_after_seconds_for_reattempt"] for other, notify_outcome, kwargs in (requests or ())
            if notify_outcome == "re_dlq" and other.envelope.key_bytes == pending.envelope.key_bytes
            and _order_of(other.envelope, _identity_of(other), index) < own
        )
        if local_wake_ms is not None and local_wake_ms > now_ms:
            waits.append((local_wake_ms - now_ms) / 1000.0)
        known = max(delay, min(max(waits), max_delay)) if waits else max(delay, max_delay)
        blocked_for = max(0.0, (now_ms - pending.envelope.first_failure_at_ms) / 1000.0)
        return max(known, min(max(blocked_for, max_delay), max(max_delay, BLOCKED_DELAY_CEILING_SECONDS)))

    def _record_rerouted_wake(self, edge_type: str, pending: _PendingRecord, wake_ms: int) -> None:
        pair = (edge_type, pending.envelope.key_bytes)
        identity = _identity_of(pending)
        self._rerouted_wake.setdefault(pair, {})[identity] = wake_ms
        heapq.heappush(self._rerouted_heap, (wake_ms, pair, identity))

    def _prune_rerouted_wakes(self, now_ms: int) -> None:
        """Drop expired wakes (heap: only the expired entries are touched)."""
        while self._rerouted_heap and self._rerouted_heap[0][0] <= now_ms:
            wake, pair, identity = heapq.heappop(self._rerouted_heap)
            entries = self._rerouted_wake.get(pair)
            if entries is not None and entries.get(identity) == wake:
                del entries[identity]
                if not entries:
                    del self._rerouted_wake[pair]

    def _unretryable_reason(self, envelope: DeadLetterEnvelope) -> str | None:
        """Return a description of why ``envelope`` can never be retried by
        this consumer, or ``None`` if it can (review finding M10)."""
        if envelope.entity_kind != "edge":
            return f"entity_kind={envelope.entity_kind!r} is not 'edge'"
        edge_type = envelope.entity_name
        for label, config in (
            ("writer", self._writer_by_edge_type),
            ("sink", self._sink_by_edge_type),
            ("max_retry_attempts", self._max_retry_attempts_by_edge_type),
            ("parked_topic", self._parked_topic_by_edge_type),
            ("base_delay_seconds", self._base_delay_seconds_by_edge_type),
            ("max_delay_seconds", self._max_delay_seconds_by_edge_type),
        ):
            if edge_type not in config:
                return f"edge type {edge_type!r} has no configured {label} (renamed/removed edge type?)"
        try:
            _pair_from_envelope(envelope)
        except RetryConsumerError as exc:
            return str(exc)
        return None

    def _mark_due(self, pending: _PendingRecord) -> None:
        if not self._due_this_tick:
            self._first_due_at = self._monotonic_clock()
        if pending.key not in self._due_this_tick:
            self._due_since[pending.key] = self._tick_number
        self._due_this_tick[pending.key] = pending

    def _sync_pause(self, tp: tuple[str, int]) -> None:
        """Pause ``tp`` while it has ``max_records_per_partition`` outstanding
        (held, due or in-flight) records; resume it otherwise. Not-yet-due
        records do not stall the partition: due records behind them flow and
        the commit stops at the lowest unresolved offset. Only transitions
        touch the consumer."""
        want_paused = self._counted_outstanding(tp) >= self._max_records_per_partition
        if want_paused and tp not in self._paused:
            self._paused.add(tp)
            self._consumer.pause([_topic_partition(*tp)])
        elif not want_paused and tp in self._paused:
            self._paused.discard(tp)
            self._consumer.resume([_topic_partition(*tp)])

    def _resume_due_partitions(self) -> None:
        now = self._wall_clock()
        woken: set[tuple[str, int]] = set()
        while self._wake_heap and self._wake_heap[0][0] <= now:
            _, topic, partition, offset = heapq.heappop(self._wake_heap)
            pending = self._held.pop((topic, partition, offset), None)
            if pending is not None:
                self._unhold(pending)
                tp = (topic, partition)
                woken.add(tp)
                self._mark_due(pending)
        for tp in woken:
            self._sync_pause(tp)

    def _stopping(self) -> bool:
        return self._stop_event is not None and self._stop_event.is_set()

    def _flush_budget_spent(self) -> bool:
        return self._flush_deadline is not None and self._monotonic_clock() >= self._flush_deadline

    def _maybe_flush_due_batch(self) -> None:
        if self._stopping():
            # Shutdown: never freeze or start a new batch; unwritten records
            # are simply re-read from the last committed offset on restart.
            return
        self._flush_deadline = self._monotonic_clock() + self._flush_budget_seconds
        self._process_pending_batches()
        if self._due_this_tick:
            elapsed = self._monotonic_clock() - (self._first_due_at or 0.0)
            if elapsed >= self._due_batch_coalesce_seconds or len(self._due_this_tick) >= self._max_batch_size:
                self._freeze_due_batches()
                self._process_pending_batches()
        if not self._due_this_tick:
            self._first_due_at = None

    def _freeze_due_batches(self) -> None:
        """Group _due_this_tick by edge type and freeze one new
        :class:`_FrozenBatch` per edge type that doesn't already have one
        in flight, taking that batch's single read-barrier snapshot now
        (review finding M8) -- never re-taken later while waiting for
        :meth:`ParkedPairIndex.caught_up`."""
        by_edge_type: dict[str, list[tuple[tuple[str, int, int], _PendingRecord]]] = defaultdict(list)
        for key, pending in self._due_this_tick.items():
            by_edge_type[pending.envelope.entity_name].append((key, pending))

        for edge_type, keyed_pendings in by_edge_type.items():
            # At most max_batch_size records per frozen batch: the records due
            # the longest first (Part B: by the tick they became due, so rows
            # woken again and again at low offsets cannot take every batch
            # ahead of rows due for longer), then lowest offsets first within
            # each partition, and partitions interleaved by rank so a
            # partition with high offsets is never starved by one with low
            # offsets; the rest stay due for the next batch.
            keyed_pendings.sort(key=lambda item: item[0])
            ranks: dict[tuple[str, int], int] = {}
            ranked = []
            for item in keyed_pendings:
                tp = item[0][:2]
                rank = ranks.get(tp, 0)
                ranks[tp] = rank + 1
                ranked.append((self._due_age_rank(item[0]), rank, item[0][0], item[0][1], item))
            ranked.sort(key=lambda entry: entry[:4])
            keyed_pendings = [entry[4] for entry in ranked[:self._max_batch_size]]
            if edge_type in self._pending_batches:
                # Already a frozen batch in flight for this edge type --
                # these records wait for the *next* batch, never
                # retroactively join an already-snapshotted one.
                continue
            index = self._parked_pair_index_by_edge_type.get(edge_type)
            snapshot = None
            if index is not None:
                try:
                    snapshot = index.watermark_snapshot()
                except Exception:
                    # M7: bounded, raises rather than hangs. Defer
                    # freezing this edge type's batch to a later tick
                    # instead of freezing it with no snapshot at all.
                    logger.warning(
                        "stage=retry-consumer edge_type=%s reason=watermark_snapshot_failed -- "
                        "deferring this batch's freeze to a later tick", edge_type,
                    )
                    continue
            for key, _pending in keyed_pendings:
                del self._due_this_tick[key]
                self._due_since.pop(key, None)
            self._pending_batches[edge_type] = _FrozenBatch(keyed_pendings=keyed_pendings, snapshot=snapshot)

    def _process_pending_batches(self) -> None:
        for edge_type in list(self._pending_batches):
            if self._stopping() or self._flush_budget_spent():
                return  # the rest of the frozen batches wait for the next tick
            batch = self._pending_batches[edge_type]
            index = self._parked_pair_index_by_edge_type.get(edge_type)
            if index is not None and batch.snapshot is not None and not index.caught_up(batch.snapshot):
                continue  # M8: still waiting on this batch's own frozen snapshot
            # Original order: a pair's records share a partition, so offset
            # order within a partition is the replay order the lease
            # coordinator's FIFO cells then preserve.
            pendings = sorted(
                (pending for _key, pending in batch.keyed_pendings), key=lambda p: p.key,
            )
            stall = self._stall.get(edge_type)
            now = self._wall_clock()
            if stall is not None:
                # A backward wall-clock jump must never freeze the edge type.
                stall.retry_at_ms = min(stall.retry_at_ms, now + int(WRITE_FAILURE_BACKOFF_SECONDS[1] * 1000))
            if stall is not None and now < stall.retry_at_ms:
                # The edge type's writes are failing with nothing written: wait
                # out its stall backoff (a failing flush per period, no storm).
                del self._pending_batches[edge_type]
                for pending in pendings:
                    self._hold(pending, stall.retry_at_ms, now)
                for tp in {(p.topic, p.partition) for p in pendings}:
                    self._sync_pause(tp)
                continue
            result = self._retry_or_park_one_edge_type(edge_type, pendings)
            del self._pending_batches[edge_type]
            # M9: only the records not yet durably resolved go back to
            # being due -- resolved records in this same batch are never
            # touched again. They get a fresh classification and a fresh
            # snapshot on their next batch.
            now = self._wall_clock()
            if result.stalled:
                # Nothing was resolved (an outage, a lone poison row, a
                # failing publish): back the whole edge type off, capped
                # exponentially, so it is never retried once per tick. The
                # streak itself is capped (M2: an unbounded exponent overflows
                # after ~85 minutes of outage).
                streak = min(self._no_progress_streak.get(edge_type, 0) + 1, _MAX_STREAK)
                self._no_progress_streak[edge_type] = streak
            else:
                streak = 0
                self._no_progress_streak.pop(edge_type, None)
            edge_delay = _capped_backoff(streak)
            delays: dict[tuple[str, int, int], float] = {}
            for pending in result.remaining:
                # A record that just failed to write alone backs off before
                # its next attempt instead of spinning.
                delay = max(edge_delay, _capped_backoff(self._fail_streak.get(pending.key, 0)))
                delays[pending.key] = delay
                if delay:
                    self._hold_for(pending, delay, now)
                else:
                    self._mark_due(pending)
            for follower, blocker in result.followers:
                # Wait at the blocker's backoff (at least the blocked delay),
                # locally: a published copy would sit in the DLQ forever.
                self._hold_for(follower, max(delays.get(blocker.key, 0.0), self._blocked_retry_delay_seconds), now)
            for pending in result.requeued:
                # F4: its blockers were resolved by this very batch -- due again
                # locally (a fresh snapshot and index check), never re-published.
                self._mark_due(pending)
            for tp in {(p.topic, p.partition) for p in pendings}:
                self._sync_pause(tp)

    # ── Retry-or-park write path ─────────────────────────────────────────

    def _resolve(self, pending: _PendingRecord) -> None:
        """Mark one record's outcome durable: it no longer holds back its
        partition's commit position."""
        key = pending.key
        self._invalid_at_read.discard(key)
        self._write_failures.pop(key, None)
        self._solo_uncounted.pop(key, None)
        self._fail_streak.pop(key, None)
        self._suspects.discard(key)
        self._last_solo_try.pop(key, None)
        self._forget_failing(pending.envelope.entity_name, (key,))
        outstanding = self._outstanding.get((pending.topic, pending.partition))
        if outstanding is not None:
            outstanding.discard(pending.offset)

    def _retry_or_park_one_edge_type(
        self, edge_type: str, pendings: list[_PendingRecord],
    ) -> _EdgeBatchResult:
        """Attempt to durably resolve every pending record of one edge type.

        One write batch for all candidates; then per-row results are routed
        with one DLQ publish flush (re-DLQ / park) and one parked-pair-index
        flush, followed -- only if needed -- by one publish flush for rows that
        stay blocked. A record's offset becomes committable once *its own*
        outcome is durable (review finding M9), independently of its
        batch-mates. Returns the rows that did **not** get durably resolved,
        the rows to hold behind them, and the rows to requeue locally; the
        caller re-schedules only those.
        """
        index = self._parked_pair_index_by_edge_type.get(edge_type)
        earlier_of = getattr(index, "earlier_outstanding", None) if index is not None else None
        sink = self._sink_by_edge_type[edge_type]
        max_attempts = self._max_retry_attempts_by_edge_type[edge_type]
        base_delay = self._base_delay_seconds_by_edge_type[edge_type]
        max_delay = self._max_delay_seconds_by_edge_type[edge_type]
        parked_topic = self._parked_topic_by_edge_type[edge_type]

        remaining: list[_PendingRecord] = []
        notifications: list[tuple[_PendingRecord, str]] = []
        order_cache: dict[tuple[str, int, int], tuple] = {}

        def order(p: _PendingRecord) -> tuple:
            """A pair-order key (v7-S4): compare a pair's records ONLY through this,
            never by raw identity (topic names have no order). Once per record and
            batch, so every comparison of the batch agrees."""
            if p.key not in order_cache:
                order_cache[p.key] = _order_of(p.envelope, _identity_of(p), index)
            return order_cache[p.key]

        def identity(pending: _PendingRecord) -> dict:
            return dict(
                original_topic=pending.envelope.original_topic,
                original_partition=pending.envelope.original_partition,
                original_offset=pending.envelope.original_offset,
            )

        blocked: list[_PendingRecord] = []
        index_blocked: list[_PendingRecord] = []
        candidates: list[_PendingRecord] = []
        candidate_ids: dict[bytes | None, set[tuple[str, int, int]]] = {}
        progress = 0  # rows resolved by a write / re-DLQ / park / duplicate skip (not "blocked" republishes)
        now_ms = self._wall_clock()
        self._prune_rerouted_wakes(now_ms)
        # Part B: one copy per record and batch -- the newest lineage (first_failure_at_ms), then the
        # highest attempt, then the latest DLQ position. The kept copy is in hand, so skipping the
        # others never leaves the record without an outstanding copy.
        newest_copy: dict[tuple[str, int, int], tuple] = {}
        rejected: list[_PendingRecord] = []
        stale: list[_PendingRecord] = []  # resurrected copies: tombstoned, never written
        for pending in pendings:
            ident = _identity_of(pending)
            newest_copy[ident] = max(newest_copy.get(ident, (-1,)), (
                pending.envelope.first_failure_at_ms, pending.envelope.attempt_count, pending.key))
        for pending in pendings:
            source_key, target_key = _pair_from_envelope(pending.envelope)
            if self._is_superseded_copy(edge_type, pending, newest_copy[_identity_of(pending)][-1]):
                # Part B: another copy of this very record (newer, or the same attempt
                # later in the DLQ) is in this batch, or this consumer already
                # re-DLQ'd a copy of it at this attempt or later. That
                # copy carries the record (outstanding here, or in the DLQ above every
                # offset fetched when it was published, so never committed past
                # unresolved). Retrying this one too would start a parallel lineage.
                logger.info(
                    "stage=retry-consumer edge_type=%s reason=superseded_copy_skipped topic=%s partition=%s "
                    "offset=%s attempt=%s", edge_type, pending.envelope.original_topic,
                    pending.envelope.original_partition, pending.envelope.original_offset,
                    pending.envelope.attempt_count,
                )
                self._resolve(pending)
                notifications.append((pending, "duplicate_skipped"))
                progress += 1
                continue
            if index is not None and not index.contains(source_key, target_key, **identity(pending)):
                # Review finding M12: this record's own entry is gone --
                # already durably resolved (written or parked) by an
                # earlier duplicate copy of this exact record (P5-S3's
                # own at-least-once contract can republish an
                # already-recorded row when a DLQ publish fails partway
                # through a multi-row batch and the whole batch replays),
                # or -- for a bulk opt-in run of this edge type, Decisions
                # §1 -- this edge type's rows were never indexed at all.
                # Either way: never write or re-DLQ/park it again, only
                # commit past it. Idempotent MERGE alone does not cover
                # this -- the hazard is a stale duplicate overwriting
                # newer data, not a duplicate relationship.
                logger.info(
                    "stage=retry-consumer edge_type=%s reason=duplicate_or_unindexed_skipped "
                    "topic=%s partition=%s offset=%s",
                    edge_type, pending.envelope.original_topic,
                    pending.envelope.original_partition, pending.envelope.original_offset,
                )
                self._resolve(pending)
                notifications.append((pending, "duplicate_skipped"))
                progress += 1
                continue
            if pending.key in self._invalid_at_read:
                # Final follow-ups nit: its key labels bind another pair than its topic: park it
                # now, outside every pair's grouping (no write, no hold, no blocked copy).
                rejected.append(pending)
                continue
            resolved_offset = self._resolved_offset_at_or_above(edge_type, pending) if index is not None else None
            if resolved_offset is not None:
                # Replay resurrection: this consumer already wrote or parked this record, or a LATER
                # record of its pair from the same source partition. It resolves a pair's records in
                # order, so a copy still indexed after that was either re-added by a redelivered live
                # flush (a crash, a stop-drain abandon, a failed revoke settlement) after its resolution
                # here, or kept by a failed tombstone after its own write or park. Either way it is in
                # Neo4j or the parking lot already, and writing it could put the older value over a
                # newer one: tombstone its entry (so it blocks nothing) and commit past it, never write
                # it. The same offset counts, including the copy that resolved it: a sequenced index
                # (several topics) gives a re-added record a NEW seq, after every outstanding record of
                # the pair, so a rewrite could land after a newer, still outstanding row (also once its
                # entry was removed and re-added between a failed tombstone and the next batch).
                logger.info(
                    "stage=retry-consumer edge_type=%s reason=stale_resurrected_copy_skipped topic=%s "
                    "partition=%s offset=%s resolved_offset=%s", edge_type, pending.envelope.original_topic,
                    pending.envelope.original_partition, pending.envelope.original_offset, resolved_offset,
                )
                stale.append(pending)
                continue
            if _identity_of(pending) in candidate_ids.get(pending.envelope.key_bytes, ()):
                # Another copy of this very record is already a candidate of this
                # batch (an at-least-once duplicate): writing both could put the
                # copy after a later row of the pair. Hold it; once the first
                # copy is resolved, M12's contains() check skips it.
                self._hold_for(pending, self._blocked_retry_delay_seconds, now_ms)
                continue
            if self._counted_outstanding((pending.topic, pending.partition)) < self._hold_budget() and (
                blocker_wake_ms := self._held_blocker_wake_ms(pending, index)
            ) is not None:
                # (v7-S4 S-a: only within the hold budget. A partition full of rows held
                # behind each other while their blocker sits unfetched behind the pause
                # would never free a slot; over the budget the row takes the blocked path
                # below and is published, which resolves its slot.)
                # An earlier record of this pair is held locally, waiting out
                # its own backoff: hold this one here until that wake-up
                # instead of publishing a fresh DLQ copy every blocked delay
                # (the DLQ keeps every copy forever). Also the only ordering
                # guard a standalone run (no index) has across ticks.
                self._hold(pending, blocker_wake_ms, now_ms)
                continue
            if index is not None and index.is_blocked_by_earlier(
                source_key, target_key, first_failure_at_ms=pending.envelope.first_failure_at_ms,
                **identity(pending),
            ):
                earlier = earlier_of(source_key, target_key, **identity(pending)) if earlier_of else None
                if earlier is None or not set(earlier) <= candidate_ids.get(pending.envelope.key_bytes, set()):
                    index_blocked.append(pending)
                    continue
                # Blocked only behind earlier rows of this same batch that are
                # written ahead of it in this flush (the flush keeps a pair's
                # rows in order and drops a pair's later rows as soon as an
                # earlier one ends without being written): a candidate too, so
                # one pair's chain drains in one batch.
            candidates.append(pending)
            candidate_ids.setdefault(pending.envelope.key_bytes, set()).add(_identity_of(pending))

        outcomes = self._write_candidates(
            edge_type, candidates, order_keys=[order(c) for c in candidates],
        ) if candidates else {}
        # A record without an outcome (failed write, or a partially written
        # flush) was not durably resolved: it stays outstanding, so the
        # commit position cannot pass it.
        written: list[_PendingRecord] = []
        requests: list[tuple[_PendingRecord, str, dict]] = []  # (pending, notify_outcome, reroute kwargs)
        for pending in rejected:
            requests.append((pending, "parked", dict(
                envelope=pending.envelope, reason_code="invalid_on_retry",
                error_text="the key's label pair is not the pair its original topic binds",
                retry_after_seconds_for_reattempt=0.0, parked_topic=parked_topic,
            )))
        for i, pending in enumerate(candidates):
            write_result = outcomes.get(i)
            if write_result is None:
                remaining.append(pending)
                continue
            outcome, reason_code = write_result
            if outcome == RETRY_OUTCOME_WRITTEN:
                written.append(pending)
                if index is not None:
                    self._note_resolved(edge_type, pending)  # the graph holds it now, tombstone or not
            elif reason_code == "blocked_by_earlier_unresolved_row":
                # Blocked behind an earlier unresolved row of its pair in
                # this very flush: not its own resolution failure.
                blocked.append(pending)
            else:
                next_attempt = pending.envelope.attempt_count + 1
                delay = min(base_delay * 2 ** (next_attempt - 1), max_delay)
                will_park = next_attempt >= max_attempts
                requests.append((
                    pending, "parked" if will_park else "re_dlq",
                    dict(
                        envelope=pending.envelope, reason_code=reason_code,
                        error_text=f"still unresolved after {next_attempt} attempt(s)",
                        retry_after_seconds_for_reattempt=delay,
                        parked_topic=parked_topic if will_park else None,
                    ),
                ))

        # Rows blocked behind an earlier row of their pair (by the writer in
        # this flush, or by the index), decided before the one publish flush,
        # oldest first (finding F4):
        #   * behind a row of this batch still without an outcome: held locally
        #     at that row's backoff;
        #   * behind nothing but rows this batch writes or parks (or rows
        #     requeued just before): due again locally, never re-published. If
        #     a removal below fails, the requeued row simply finds itself
        #     blocked again on its next (fully re-checked) batch;
        #   * otherwise (behind a row re-DLQ'd here, or one held or outstanding
        #     elsewhere): one blocked copy, with the blocker's remaining delay.
        resolving_ids = {_identity_of(p) for p in written} | {_identity_of(p) for p in stale} | {
            _identity_of(p) for p, notify_outcome, _kwargs in requests if notify_outcome == "parked"
        }
        rerouting = {
            (p.envelope.key_bytes, _identity_of(p)) for p, notify_outcome, _kwargs in requests
            if notify_outcome == "re_dlq"
        }
        followers: list[tuple[_PendingRecord, _PendingRecord]] = []
        requeued: list[_PendingRecord] = []
        requeued_ids: set[tuple[str, int, int]] = set()
        held_here: set[tuple[str, int, int]] = set()  # followers held locally in this batch (v7-S4 M1)
        for pending in sorted(blocked + index_blocked, key=order):
            pair_bytes = pending.envelope.key_bytes
            own = _identity_of(pending)
            own_order = order(pending)
            blockers = [r for r in remaining if r.envelope.key_bytes == pair_bytes and order(r) < own_order]
            if blockers:
                followers.append((pending, min(blockers, key=order)))
                continue
            if index is None:
                # Standalone: only this batch's rows can block it here.
                still_blocked = any(key_bytes == pair_bytes and other < own for key_bytes, other in rerouting)
            elif earlier_of is not None:
                source_key, target_key = _pair_from_envelope(pending.envelope)
                still_blocked = not set(earlier_of(source_key, target_key, **identity(pending))) <= (
                    resolving_ids | requeued_ids
                )
            else:
                still_blocked = True  # an index that cannot list blockers: publish, as always
            if not still_blocked:
                requeued.append(pending)
                requeued_ids.add(own)
                continue
            # v7-S4 review M1 (DLQ amplification): blocked behind an earlier row
            # of its pair whose next attempt THIS consumer knows (re-DLQ'd in this
            # batch, re-DLQ'd before with a pending wake, or held / due here):
            # hold it locally until just after that wake instead of publishing a
            # blocked copy on every root cycle. Holding costs no memory (the row
            # is fetched and outstanding either way); the budget only keeps 10%
            # of max_records_per_partition free so the partition keeps fetching
            # (its roots' re-DLQ'd copies) instead of pausing.
            wake_ms = self._local_blocker_wake_ms(pending, now_ms, requests, order, own_order)
            tp = (pending.topic, pending.partition)
            if wake_ms is not None and self._counted_outstanding(tp) < self._hold_budget():
                self._hold(pending, wake_ms, now_ms)
                held_here.add(pending.key)
                continue
            blocked_delay = self._blocked_delay_seconds(pending, now_ms, requests, local_wake_ms=wake_ms)
            if self._counted_outstanding(tp) >= self._hold_budget() and self._overflow_hold(
                pending, now_ms + int(blocked_delay * 1000), now_ms,
            ):
                # Over the hold budget: held for the delay its published copy would wait
                # (woken early when a row of its pair is written or parked), without
                # counting towards the pause -- not re-published (v7-S4).
                held_here.add(pending.key)
                continue
            rerouting.add((pair_bytes, own))
            # P5-S4 Story 3 M2 / Story 5: blocked purely behind an earlier
            # outstanding record of this pair -- not this row's own
            # resolution failure. Non-exponential delay: the blocker's remaining
            # wait, else the type's max delay (never the short blocked delay,
            # which re-published every follower twice a second); attempt_count
            # unchanged.
            requests.append((
                pending, "blocked",
                dict(
                    envelope=pending.envelope, reason_code="blocked_by_earlier_unresolved_row",
                    error_text="an earlier outstanding record of this pair is still unresolved",
                    retry_after_seconds_for_reattempt=blocked_delay,
                    parked_topic=None, increment_attempt=False,
                ),
            ))

        # Re-DLQ / park / blocked: one producer flush for the whole batch.
        needs_index_removal: list[tuple[_PendingRecord, str]] = [(p, "written") for p in written]
        for pending, notify_outcome, kwargs, result in self._publish_requests(edge_type, sink, requests):
            if isinstance(result, Exception):
                remaining.append(pending)
            elif notify_outcome == "parked":
                needs_index_removal.append((pending, notify_outcome))
                if index is not None and pending.key not in self._invalid_at_read:
                    self._note_resolved(edge_type, pending)  # in the parking lot now
            else:
                self._resolve(pending)
                notifications.append((pending, notify_outcome))
                if notify_outcome == "re_dlq":
                    # Recorded only once the publish is acknowledged: a later
                    # row of the pair blocked behind it waits out this delay.
                    self._record_rerouted_wake(
                        edge_type, pending, now_ms + int(kwargs["retry_after_seconds_for_reattempt"] * 1000),
                    )
                    self._note_superseded(edge_type, pending)
                    progress += 1

        if stale:
            # Resurrected copies: only their re-added entry leaves the index. Committed past once the
            # tombstone is acknowledged; a failed tombstone keeps the copy outstanding (still stale
            # next time, so it is tombstoned again, never written).
            try:
                stale_removals = index.remove_many([
                    (*_pair_from_envelope(p.envelope), p.envelope.original_topic,
                     p.envelope.original_partition, p.envelope.original_offset)
                    for p in stale
                ])
            except Exception as exc:
                logger.exception(
                    "stage=retry-consumer edge_type=%s reason=index_remove_failed -- %d resurrected copies stay "
                    "outstanding", edge_type, len(stale),
                )
                stale_removals = [exc] * len(stale)
            for pending, error in zip(stale, stale_removals):
                if error is not None:
                    logger.error(
                        "stage=retry-consumer edge_type=%s reason=resolve_failed topic=%s partition=%s offset=%s "
                        "error=%s -- resurrected copy stays outstanding for the next tick",
                        edge_type, pending.topic, pending.partition, pending.offset, error,
                    )
                    remaining.append(pending)
                    continue
                self._resolve(pending)
                notifications.append((pending, "stale_resurrected_copy_skipped"))
                progress += 1

        # Written / parked rows leave the parked-pair index -- only now that
        # their write / park publish is durable -- in pair order: one flush per
        # chain level (the earliest resolved row of every pair, then the next
        # ...). Once a row's tombstone fails, its pair's later rows are not
        # tombstoned either: they stay indexed and outstanding, so the earlier
        # row (re-written next tick) can never land after them unblocked.
        chains: dict[bytes | None, list[tuple[_PendingRecord, str]]] = defaultdict(list)
        for item in needs_index_removal:
            chains[item[0].envelope.key_bytes].append(item)
        for chain in chains.values():
            chain.sort(key=lambda item: order(item[0]))
        broken: set[bytes | None] = set()
        done_in_flush, self._flush_tombstoned = getattr(self, "_flush_tombstoned", set()), set()
        level = 0
        while True:
            batch = [chain[level] for chain in chains.values() if level < len(chain)]
            if not batch:
                break
            for item in batch:
                if item[0].key in done_in_flush:
                    # Already left the index inside the write flush (acknowledged).
                    self._resolve(item[0])
                    notifications.append(item)
                    progress += 1
            batch = [item for item in batch if item[0].key not in done_in_flush]
            todo = [item for item in batch if item[0].envelope.key_bytes not in broken]
            for pending, _o in batch:
                if pending.envelope.key_bytes in broken:
                    logger.error(
                        "stage=retry-consumer edge_type=%s reason=resolve_deferred topic=%s partition=%s offset=%s "
                        "-- an earlier row of its pair could not leave the index; stays outstanding",
                        edge_type, pending.topic, pending.partition, pending.offset,
                    )
                    remaining.append(pending)
            level += 1
            if not todo:
                continue
            if index is not None:
                try:
                    removals = index.remove_many([
                        (*_pair_from_envelope(p.envelope), p.envelope.original_topic,
                         p.envelope.original_partition, p.envelope.original_offset)
                        for p, _o in todo
                    ])
                except Exception as exc:
                    logger.exception(
                        "stage=retry-consumer edge_type=%s reason=index_remove_failed -- %d record(s) stay "
                        "outstanding", edge_type, len(todo),
                    )
                    removals = [exc] * len(todo)
            else:
                removals = [None] * len(todo)
            for (pending, notify_outcome), error in zip(todo, removals):
                if error is not None:
                    logger.error(
                        "stage=retry-consumer edge_type=%s reason=resolve_failed topic=%s partition=%s "
                        "offset=%s error=%s -- record stays outstanding for the next tick",
                        edge_type, pending.topic, pending.partition, pending.offset, error,
                    )
                    broken.add(pending.envelope.key_bytes)
                    remaining.append(pending)
                    continue
                self._resolve(pending)
                notifications.append((pending, notify_outcome))
                progress += 1

        for pending, notify_outcome in notifications:
            self._notify(edge_type, notify_outcome)
            if notify_outcome in ("written", "parked"):
                self._superseded.pop(self._lineage(edge_type, pending), None)
        self._wake_held_followers(
            edge_type,
            {p.envelope.key_bytes for p, notify_outcome in notifications
             if notify_outcome in ("written", "parked", "stale_resurrected_copy_skipped")},
            held_here,
        )
        return _EdgeBatchResult(
            remaining=remaining, followers=followers, requeued=requeued,
            stalled=bool(remaining) and not progress,
        )

    def _evidence_key(self, edge_type: str, envelope: DeadLetterEnvelope) -> object:
        """Whose WRITTEN outcomes are D1 evidence for a row: its edge type (single-pair types, unchanged), or
        (edge type, label pair of its original topic) for a type of several label pairs (final follow-ups S4)."""
        labels_by_topic = self._endpoint_pairs_by_edge_type.get(edge_type)
        if labels_by_topic is None:
            return edge_type
        return (edge_type, labels_by_topic.get(envelope.original_topic))

    def _is_superseded_copy(self, edge_type: str, pending: _PendingRecord, kept_in_batch: tuple) -> bool:
        """A copy of a record other than the one copy of it this batch keeps (``kept_in_batch``, its
        DLQ position), or one at or below the attempt of a copy this consumer already re-DLQ'd (Part B)."""
        attempt = pending.envelope.attempt_count
        superseded = self._superseded.get(self._lineage(edge_type, pending))
        return pending.key != kept_in_batch or (superseded is not None and attempt <= superseded[0])

    @staticmethod
    def _lineage(edge_type: str, pending: _PendingRecord) -> tuple:
        """One lineage of copies of a record: a re-DLQ keeps ``first_failure_at_ms``, a fresh DLQ of the
        same source record by the live loader (a P5-S3 replay after the record was resolved elsewhere)
        starts a new one, which an older lineage's memory must never skip."""
        return (edge_type, _identity_of(pending), pending.envelope.first_failure_at_ms)

    def _due_age_rank(self, key: tuple[str, int, int]) -> int:
        """The tick in which a due record became due: lower is frozen first (Part B)."""
        return self._due_since.get(key, 0)

    def _note_superseded(self, edge_type: str, pending: _PendingRecord) -> None:
        """This copy of a record was re-DLQ'd (acknowledged) as attempt ``attempt_count + 1``:
        a copy of the record at ``attempt_count`` or below is stale from now on (Part B)."""
        key = self._lineage(edge_type, pending)
        attempt = max(self._superseded.get(key, (-1, None))[0], pending.envelope.attempt_count)
        self._superseded[key] = (attempt, (pending.topic, pending.partition))
        self._superseded.move_to_end(key)
        while len(self._superseded) > _SUPERSEDED_TRACK_LIMIT:
            self._superseded.popitem(last=False)

    @staticmethod
    def _resolved_key(edge_type: str, pending: _PendingRecord) -> tuple:
        """The pair (its canonical ``key_bytes``, labelled for a type of several label pairs, see
        :func:`canonical_pair_key`) and the one source partition whose offsets are comparable: offsets
        of different topics or partitions say nothing about a pair's order."""
        envelope = pending.envelope
        return (edge_type, envelope.key_bytes, envelope.original_topic, envelope.original_partition)

    def _note_resolved(self, edge_type: str, pending: _PendingRecord) -> None:
        """This copy's record was written or parked: remember the highest such source offset of its pair
        and source partition, with the DLQ (topic, partition) of the copy (LRU, ``_RESOLVED_TRACK_LIMIT``)."""
        key = self._resolved_key(edge_type, pending)
        offset = max(self._resolved_high.get(key, (-1, None))[0], pending.envelope.original_offset)
        self._resolved_high[key] = (offset, (pending.topic, pending.partition))
        self._resolved_high.move_to_end(key)
        while len(self._resolved_high) > _RESOLVED_TRACK_LIMIT:
            self._resolved_high.popitem(last=False)

    def _resolved_offset_at_or_above(self, edge_type: str, pending: _PendingRecord) -> int | None:
        """The highest resolved source offset of ``pending``'s pair and source partition when it is at or
        above ``pending``'s own (``pending``'s record, or a later one, is already written or parked), else
        ``None``."""
        entry = self._resolved_high.get(self._resolved_key(edge_type, pending))
        if entry is None or entry[0] < pending.envelope.original_offset:
            return None
        return entry[0]

    def _publish_requests(self, edge_type: str, sink, requests: list):
        """Publish re-DLQ / park / blocked requests with ONE producer flush and
        return ``(pending, notify_outcome, kwargs, result)``; ``result`` is an exception
        for a publish that was not acknowledged (that row stays outstanding)."""
        if not requests:
            return []
        try:
            results = sink.reroute_or_park_many([kwargs for _p, _o, kwargs in requests])
        except Exception as exc:
            logger.exception(
                "stage=retry-consumer edge_type=%s reason=batch_publish_failed -- %d record(s) stay outstanding",
                edge_type, len(requests),
            )
            results = [exc] * len(requests)
        published = []
        for (pending, notify_outcome, kwargs), result in zip(requests, results):
            if isinstance(result, Exception):
                logger.error(
                    "stage=retry-consumer edge_type=%s reason=%s_failed topic=%s partition=%s "
                    "offset=%s error=%s -- record stays outstanding",
                    edge_type, "blocked_re_dlq" if notify_outcome == "blocked" else "resolve",
                    pending.topic, pending.partition, pending.offset, result,
                )
            published.append((pending, notify_outcome, kwargs, result))
        return published

    def _write_candidates(
        self, edge_type: str, candidates: list[_PendingRecord], *, order_keys: list | None = None,
    ) -> dict[int, tuple[str, str]]:
        """Write ``candidates`` and return ``{candidate index: outcome}`` for
        every one whose outcome is durable.

        No new write starts once the tick's flush budget is spent or a stop
        was requested. A failed write is classified
        (:func:`_classify_write_error`):

        * ``connectivity``: says nothing about the rows. Nothing more is written
          for this edge type this tick, nothing is counted, and the caller backs
          the edge type off.
        * ``row``, ``capacity``, ``server`` and ``unknown``: the failing slice is
          bisected (at most ``_ISOLATION_CALL_BUDGET`` extra calls per tick, only
          non-empty sub-slices charged) so one bad row cannot fail its
          neighbours' writes forever.

        Rows that failed alone before (*suspects*) are only written alone, after
        the other rows, least recently tried first; without evidence of health
        in this flush (a WRITTEN outcome, or a row error) the first failing
        suspect ends the flush. A *stalled* edge type (``_STALLED_STREAK``
        batches without progress) starts with solo probe writes instead of
        bisecting and resumes normal writing at the first outcome; a failing
        probe ends the flush.

        A row that fails alone is counted (owner decision D1) only for a
        ``row`` error, or for ``unknown``/``capacity``/``server`` when a WRITTEN
        outcome of this edge type came after the row's first uncounted failure;
        ``connectivity`` failures never count (and restart the row's evidence
        window). At ``WRITE_FAILURE_LIMIT`` counted failures the row is
        reported unresolved with ``reason_code="write_failed"``, so the ordinary
        attempts-then-park path applies. Without evidence rows wait; the stall
        is surfaced by :meth:`_report_stalls`.

        Once any row of a pair ends the flush without a written outcome, the
        pair's later rows are left out of every later write of this flush (they
        stay outstanding): a later row must never be written ahead of an
        earlier one, including in standalone mode where no index blocks it.
        """
        writer = self._writer_by_edge_type[edge_type]
        index = self._parked_pair_index_by_edge_type.get(edge_type)
        # Candidates whose index entry was removed (acknowledged) inside this
        # flush; the caller resolves them without a second tombstone.
        self._flush_tombstoned = set()
        n = len(candidates)
        keys = [candidate.key for candidate in candidates]
        pairs = [(candidate.envelope.key_bytes,) for candidate in candidates]
        outcomes: dict[int, tuple[str, str]] = {}
        stalled_pairs: set[tuple] = set()  # pairs with a row that ended this flush without a written outcome
        state = _FlushState()
        first_fail_seq: dict[int, int] = {}  # candidate index -> call number of its first failure here
        solo_failures: list[tuple[int, str]] = []  # (candidate index, error class)
        isolation_calls = 0
        pair_rows: dict[tuple, list[int]] = defaultdict(list)
        for i in range(n):
            pair_rows[pairs[i]].append(i)
        if order_keys is None:
            order_keys = [_order_of(c.envelope, _identity_of(c), index) for c in candidates]
        for rows in pair_rows.values():
            # Pair order is the parked-pair index order (record identity, led by a
            # causal per-pair sequence for a multi-topic type, v7-S4), not DLQ-offset
            # order (a re-DLQ'd copy of an earlier row has a later DLQ offset).
            rows.sort(key=lambda i: (order_keys[i], i))
        written_hi: dict[tuple, tuple] = {}  # pair -> highest pair-order key WRITTEN this flush
        for key in keys:
            # Rotation age: a row never tried alone ranks by when it was first
            # seen, so a stream of new rows cannot starve older failed ones.
            self._last_solo_try.setdefault(key, self._write_seq)
        head_position: dict[tuple, int] = {}

        def halted() -> bool:
            return self._stopping() or self._flush_budget_spent()

        def write(indices: list[int]) -> tuple[list[int], str]:
            self._write_seq += 1
            seq = self._write_seq
            state.attempted = True
            # The call may not outlast the tick's flush budget: the writer
            # abandons it (rows stay outstanding) once the budget is spent.
            budget = (
                None if self._flush_deadline is None
                else max(0.0, self._flush_deadline - self._monotonic_clock())
            )
            error: BaseException | None = None
            try:
                result = writer.retry_batch([candidates[i].envelope for i in indices], budget_seconds=budget)
            except Exception as exc:
                logger.exception(
                    "stage=retry-consumer edge_type=%s reason=write_failed -- %d record(s) of the slice "
                    "left outstanding", edge_type, len(indices),
                )
                result, error = {}, exc
            else:
                error = getattr(result, "error", None)
            done = {indices[j]: outcome for j, outcome in result.items() if 0 <= j < len(indices)}
            outcomes.update(done)
            if done:
                state.any_outcome = True
                if any(reason != "invalid_on_retry" for _outcome, reason in done.values()):
                    state.neo4j_outcome = True
                if any(outcome == RETRY_OUTCOME_WRITTEN for outcome, _reason in done.values()):
                    # Only WRITTEN is evidence (M1): an UNRESOLVED outcome may
                    # have run only the classification read, or not touched
                    # Neo4j at all (invalid_on_retry).
                    state.wrote = True
                    for i, (outcome, _reason) in done.items():
                        if outcome == RETRY_OUTCOME_WRITTEN:
                            self._last_written_seq[self._evidence_key(edge_type, candidates[i].envelope)] = seq
                    self._last_written_at[edge_type] = self._monotonic_clock()
            for i, (outcome, _reason) in done.items():
                if outcome != RETRY_OUTCOME_WRITTEN:
                    stalled_pairs.add(pairs[i])
                elif pairs[i] not in written_hi or written_hi[pairs[i]] < order_keys[i]:
                    written_hi[pairs[i]] = order_keys[i]
            missing = [i for i in indices if i not in done]
            error_class = _classify_write_error(error) if error is not None else "unknown"
            if missing:
                state.errors.append((error_class, error))
                for i in missing:
                    first_fail_seq.setdefault(i, seq)
                    if error_class == "connectivity":
                        # A system-wide signal: the row's evidence window restarts.
                        self._solo_uncounted.pop(keys[i], None)
            return missing, error_class

        def solo(i: int) -> tuple[list[int], str]:
            self._last_solo_try[keys[i]] = self._write_seq + 1
            missing, error_class = write([i])
            if missing:
                stalled_pairs.add(pairs[i])
                if error_class != "connectivity":
                    solo_failures.append((i, error_class))
            return missing, error_class

        tombstoned: set[int] = set()  # candidate indices whose index entry is acknowledged removed

        def flush_tombstones() -> None:
            """Before a pair's next row may be written, the WRITTEN rows ahead
            of it must have left the index (acknowledged): otherwise a failed
            tombstone, a crash or an outage could leave the earlier row to be
            rewritten over the later one. One flush for every such pair; a
            failed tombstone stalls its pair (the later rows stay unwritten,
            indexed and outstanding)."""
            if index is None:
                return
            todo: list[int] = []
            for pair, rows in pair_rows.items():
                if pair in stalled_pairs:
                    continue
                for position, i in enumerate(rows):
                    if outcomes.get(i, ("",))[0] != RETRY_OUTCOME_WRITTEN:
                        break
                    if i not in tombstoned and any(j not in outcomes for j in rows[position + 1:]):
                        todo.append(i)
            if not todo:
                return
            try:
                results = index.remove_many([
                    (*_pair_from_envelope(candidates[i].envelope), candidates[i].envelope.original_topic,
                     candidates[i].envelope.original_partition, candidates[i].envelope.original_offset)
                    for i in todo
                ])
            except Exception as exc:
                logger.exception(
                    "stage=retry-consumer edge_type=%s reason=index_remove_failed -- %d pair(s) wait",
                    edge_type, len(todo),
                )
                results = [exc] * len(todo)
            for i, error in zip(todo, results):
                if error is None:
                    tombstoned.add(i)
                    self._flush_tombstoned.add(keys[i])
                else:
                    logger.error(
                        "stage=retry-consumer edge_type=%s reason=chain_tombstone_failed topic=%s partition=%s "
                        "offset=%s error=%s -- the pair's later rows wait", edge_type, candidates[i].topic,
                        candidates[i].partition, candidates[i].offset, error,
                    )
                    stalled_pairs.add(pairs[i])

        def pair_head(pair: tuple) -> int | None:
            """The pair's earliest candidate without a WRITTEN outcome (the only
            row of the pair that may be written next), or None -- also None
            while a WRITTEN row ahead of it has not yet left the index."""
            rows = pair_rows[pair]
            position = head_position.get(pair, 0)
            while position < len(rows) and outcomes.get(rows[position], ("",))[0] == RETRY_OUTCOME_WRITTEN:
                if index is not None and rows[position] not in tombstoned and position + 1 < len(rows):
                    head_position[pair] = position
                    return None
                position += 1
            head_position[pair] = position
            if position >= len(rows):
                return None
            head = rows[position]
            if pair in written_hi and order_keys[head] <= written_hi[pair]:
                # Never write a row at or below one already WRITTEN for its pair.
                stalled_pairs.add(pair)
                return None
            return head

        def next_solo(eligible) -> int | None:
            heads = []
            for pair in pair_rows:
                if pair in stalled_pairs:
                    continue
                head = pair_head(pair)
                if head is not None and head not in outcomes and eligible(head):
                    heads.append(head)
            if not heads:
                return None
            return min(heads, key=lambda head: (self._last_solo_try.get(keys[head], -1), head))

        # Suspects (rows that failed alone before) and every later row of their
        # pair are only ever written alone, after the others.
        first_suspect: dict[tuple, int] = {}
        for i in range(n):
            if keys[i] in self._suspects:
                first_suspect.setdefault(pairs[i], i)
        solo_only = {i for i in range(n) if pairs[i] in first_suspect and i >= first_suspect[pairs[i]]}
        stop = False

        stall = self._stall.get(edge_type)
        live_streak = max(self._no_progress_streak.get(edge_type, 0), stall.flushes if stall is not None else 0)
        if live_streak >= _STALLED_STREAK:
            # Stalled: probe one row at a time (least recently tried first, so
            # every row gets its turn) before bisecting anything; the first
            # outcome means the server answers, and the flush goes on normally.
            probing = True
            while probing and not halted() and isolation_calls < _ISOLATION_CALL_BUDGET:
                flush_tombstones()
                probe = next_solo(lambda _head: True)
                if probe is None:
                    break
                isolation_calls += 1
                missing, error_class = solo(probe)
                if not missing:
                    probing = False
                elif error_class != "row":
                    break  # the row's own error says the server is healthy; anything else ends the flush
            stop = probing

        # Normal writing, in rounds: every call carries at most ONE row per
        # pair -- the pair's earliest row not yet written -- so a later row of a
        # pair is only ever written once its earlier row came back WRITTEN in
        # an earlier call (the writer's in-transaction pair guard does not see
        # rows it cannot decode, and lease cells may run concurrently). A pair
        # with k rows in the batch drains in k rounds of this one flush.
        normal = {i for i in range(n) if i not in solo_only}
        while not stop and not halted():
            flush_tombstones()
            round_rows = []
            for pair in pair_rows:
                if pair in stalled_pairs:
                    continue
                head = pair_head(pair)
                if head is not None and head in normal and head not in outcomes:
                    round_rows.append(head)
            if not round_rows:
                break
            round_rows.sort()
            settled_before = (len(outcomes), len(stalled_pairs))
            size = self._write_slice_size
            work: list[tuple[list[int], bool]] = [  # (candidate indices, is a bisection sub-slice)
                (round_rows[start:start + size], False) for start in range(0, len(round_rows), size)
            ]
            work.reverse()  # pop() takes the lowest offsets first
            while work and not stop and not halted():
                indices, is_sub_slice = work.pop()
                indices = [i for i in indices if i not in outcomes and pairs[i] not in stalled_pairs]
                if not indices:
                    continue
                if is_sub_slice:
                    if isolation_calls >= _ISOLATION_CALL_BUDGET:
                        stalled_pairs.update([pairs[i] for i in indices])
                        continue
                    isolation_calls += 1
                if len(indices) == 1:
                    missing, error_class = solo(indices[0])
                    stop = bool(missing) and error_class == "connectivity"
                    continue
                missing, error_class = write(indices)
                if not missing:
                    continue
                if error_class == "connectivity":
                    # Bisecting a dead dependency only spends calls on it.
                    stalled_pairs.update([pairs[i] for i in missing])
                    stop = True
                elif len(missing) == 1:
                    work.append((missing, True))
                else:
                    half = len(missing) // 2
                    work.append((missing[half:], True))
                    work.append((missing[:half], True))
            if (len(outcomes), len(stalled_pairs)) == settled_before:
                break  # defensive: a round that settled nothing never repeats

        suspect_calls = 0
        while not stop and not halted() and (isolation_calls < _ISOLATION_CALL_BUDGET or suspect_calls == 0):
            flush_tombstones()
            suspect = next_solo(lambda head: head in solo_only)
            if suspect is None:
                break
            isolation_calls += 1
            suspect_calls += 1
            missing, error_class = solo(suspect)
            if missing and (error_class == "connectivity" or (error_class != "row" and not state.wrote)):
                break  # no evidence the server writes: the other suspects wait for the next flush

        for i, error_class in solo_failures:
            key = keys[i]
            self._suspects.add(key)
            if error_class != "row":
                marker = self._solo_uncounted.get(key, first_fail_seq[i])
                last_written = self._last_written_seq.get(self._evidence_key(edge_type, candidates[i].envelope), -1)
                if last_written <= marker:
                    # D1: no WRITTEN of this edge type (of this label pair, for a
                    # multi-pair type) since the row's first uncounted failure --
                    # no evidence it is the row's own.
                    self._solo_uncounted[key] = marker
                    continue
            self._solo_uncounted.pop(key, None)
            count = self._write_failures.get(key, 0) + 1
            if count >= WRITE_FAILURE_LIMIT:
                self._write_failures.pop(key, None)
                logger.error(
                    "stage=retry-consumer edge_type=%s reason=write_failed_escalated topic=%s partition=%s "
                    "offset=%s failures=%d class=%s -- routing as unresolved", edge_type, candidates[i].topic,
                    candidates[i].partition, candidates[i].offset, count, error_class,
                )
                outcomes[i] = (RETRY_OUTCOME_UNRESOLVED, "write_failed")
            else:
                self._write_failures[key] = count
        for i in first_fail_seq:
            if i not in outcomes:
                self._fail_streak[keys[i]] = min(self._fail_streak.get(keys[i], 0) + 1, _MAX_STREAK)
        for rows in pair_rows.values():
            # A pair whose earlier row ended this flush with an UNRESOLVED
            # outcome (re-DLQ / park ahead): its later rows are blocked behind
            # it, exactly what the writer's own pair guard reports in one call.
            blocker_seen = False
            for i in rows:
                if i in outcomes:
                    blocker_seen = blocker_seen or outcomes[i][0] != RETRY_OUTCOME_WRITTEN
                elif blocker_seen:
                    outcomes[i] = (RETRY_OUTCOME_UNRESOLVED, "blocked_by_earlier_unresolved_row")
                else:
                    break  # behind a row without an outcome: it simply waits
        if state.attempted:
            if edge_type in self._endpoint_pairs_by_edge_type:
                self._track_pair_stalls(edge_type, candidates, outcomes, first_fail_seq)
            self._track_write_health(edge_type, keys, outcomes, first_fail_seq, state)
        return outcomes

    def _track_pair_stalls(self, edge_type: str, candidates: list, outcomes: dict, first_fail_seq: dict) -> None:
        """A label pair of a multi-pair type whose rows fail with none of its rows written (final follow-ups
        S4). Its failures are not counted (per-pair D1 evidence), so it must still be signalled while another
        pair's writes succeed and keep the type-wide stall clear: :meth:`_report_stalls` reports the type as
        stalled for as long as any of its pairs is (the gauge stays per type, no pair label)."""
        now = self._monotonic_clock()
        settled = {self._evidence_key(edge_type, candidates[i].envelope) for i in outcomes}
        written = {
            self._evidence_key(edge_type, candidates[i].envelope)
            for i, (outcome, _reason) in outcomes.items() if outcome == RETRY_OUTCOME_WRITTEN
        }
        failed = {self._evidence_key(edge_type, candidates[i].envelope) for i in first_fail_seq if i not in outcomes}
        since = self._pair_stall_since.get(edge_type, {})
        had = bool(since)
        for key in written | (settled - failed):
            since.pop(key, None)
        for key in failed - written:
            since.setdefault(key, now)
        if since:
            self._pair_stall_since[edge_type] = since
        else:
            self._pair_stall_since.pop(edge_type, None)
            self._pair_stall_last_log.pop(edge_type, None)
            if had and edge_type not in self._stall:
                self._emit_stall(edge_type, 0.0)

    # ── Write health: stall (D1) and fatal (F2) tracking ─────────────────

    def _track_write_health(
        self, edge_type: str, keys: list, outcomes: dict, first_fail_seq: dict, state: _FlushState,
    ) -> None:
        """Update the edge type's failing rows, sustained-stall state and fatal
        timer after a flush that wrote something.

        * Any WRITTEN outcome ends a stall. Rows that failed without anything
          being written start (or continue) one: :meth:`_report_stalls` turns a
          long one into an ERROR and the ``graph_loader_retry_stalled_seconds``
          gauge (D1: the rows themselves only wait).
        * The fatal timer measures *continuous* fatal-class failures
          (authentication, a missing database): any outcome or any other error
          resets it, and so does a gap of more than ``_FATAL_GAP_SECONDS``
          (F2). After ``_FATAL_OUTAGE_SECONDS`` the process exits non-zero.
        """
        now = self._monotonic_clock()
        failed = {keys[i] for i in first_fail_seq if i not in outcomes}
        failing = self._failing.setdefault(edge_type, set())
        failing.difference_update(keys[i] for i in outcomes)
        failing.update(failed)
        if state.wrote:
            self._clear_stall(edge_type)
        elif failed and state.errors:
            error_class, error = state.errors[-1]
            stall = self._stall.get(edge_type)
            if stall is None:
                stall = self._stall[edge_type] = _StallState(since=now)
            stall.flushes = min(stall.flushes + 1, _MAX_STREAK)
            stall.retry_at_ms = self._wall_clock() + int(_capped_backoff(stall.flushes) * 1000)
            stall.error_class = error_class
            stall.error = f"{type(error).__name__}: {error}" if error is not None else "no outcome"
        # Only Neo4j's own answer resets the fatal run: an outcome that touched
        # Neo4j, or an error Neo4j classified (a status code) that is not
        # fatal-class. A driver/lease error without a Neo4j code says nothing
        # about authentication and neither resets nor extends it.
        fatal_errors = [error for _class, error in state.errors if _is_fatal_error(error)]
        resets = state.neo4j_outcome or any(
            error is not None and (getattr(error, "code", None) or "").startswith("Neo.")
            and not _is_fatal_error(error)
            for _class, error in state.errors
        )
        if resets:
            self._fatal.pop(edge_type, None)
        elif fatal_errors:
            first, last = self._fatal.get(edge_type, (now, now))
            if now - last > _FATAL_GAP_SECONDS:
                first = now  # a gap: not one continuous run
            self._fatal[edge_type] = (first, now)
            if now - first >= _FATAL_OUTAGE_SECONDS:
                error = fatal_errors[-1]
                # Commit what this and earlier flushes resolved before exiting:
                # those rows are never re-processed after a restart.
                self._commit_resolved()
                raise RetryConsumerFatalError(
                    f"edge type {edge_type}: writes failing for {now - first:.0f}s with only "
                    f"{type(error).__name__}: {error} -- exiting"
                )
        if not failing:
            self._forget_failing(edge_type, ())

    def _forget_failing(self, edge_type: str, keys, *, keep_fatal: bool = False) -> None:
        """Drop resolved/revoked rows from the edge type's failing set; once it
        is empty the edge type is healthy-idle: no stall, and (unless the rows
        were only revoked) no fatal timer."""
        failing = self._failing.get(edge_type)
        if failing is None:
            return
        failing.difference_update(keys)
        if not failing:
            del self._failing[edge_type]
            if not keep_fatal:
                self._fatal.pop(edge_type, None)
            self._clear_pair_stalls(edge_type)
            self._clear_stall(edge_type)

    def _clear_pair_stalls(self, edge_type: str) -> None:
        """Forget a multi-pair type's per-pair stalls (S4); they re-arm at its next failing flush."""
        self._pair_stall_last_log.pop(edge_type, None)
        if self._pair_stall_since.pop(edge_type, None) and edge_type not in self._stall:
            self._emit_stall(edge_type, 0.0)

    def _clear_stall(self, edge_type: str) -> None:
        if self._stall.pop(edge_type, None) is not None:
            self._emit_stall(edge_type, 0.0)

    def _emit_stall(self, edge_type: str, seconds: float) -> None:
        if self._on_stall_seconds is None:
            return
        try:
            self._on_stall_seconds(edge_type, seconds)
        except Exception:
            logger.exception("stage=retry-consumer on_stall_seconds listener raised")

    def _report_stalls(self) -> None:
        """Once per tick: report every stalled edge type's stall duration
        (gauge), and log a sustained one as an ERROR every
        ``_STALL_LOG_INTERVAL_SECONDS`` once it passes
        ``_STALL_LOG_AFTER_SECONDS``. A multi-pair type none of whose writes fail
        type-wide but one of whose label pairs writes nothing is reported the same
        way, for that pair (final follow-ups S4)."""
        if not self._stall and not self._pair_stall_since:
            return
        now = self._monotonic_clock()
        for edge_type, since in self._pair_stall_since.items():
            if edge_type in self._stall:
                continue  # the type-wide stall is the older, wider signal
            key, started = min(since.items(), key=lambda item: item[1])
            seconds = max(0.0, now - started)
            self._emit_stall(edge_type, seconds)
            last = self._pair_stall_last_log.get(edge_type)
            if seconds < _STALL_LOG_AFTER_SECONDS or (last is not None and now - last < _STALL_LOG_INTERVAL_SECONDS):
                continue
            self._pair_stall_last_log[edge_type] = now
            labels = key[1] if isinstance(key, tuple) and len(key) == 2 else None
            logger.error(
                "stage=retry-consumer edge_type=%s pair=%s reason=retry_stalled seconds=%.0f -- no write of this "
                "label pair has succeeded (other pairs of the type write); failures without evidence are not "
                "counted, the rows wait and are never parked for it",
                edge_type, ">".join(labels) if labels else "?", seconds,
            )
        for edge_type, stall in self._stall.items():
            # A pair stalled since before the type-wide stall began keeps its (longer) age (S4).
            pair_since = min(self._pair_stall_since.get(edge_type, {}).values(), default=stall.since)
            seconds = max(0.0, now - min(stall.since, pair_since))
            self._emit_stall(edge_type, seconds)
            if seconds < _STALL_LOG_AFTER_SECONDS:
                continue
            if stall.last_log is not None and now - stall.last_log < _STALL_LOG_INTERVAL_SECONDS:
                continue
            stall.last_log = now
            logger.error(
                "stage=retry-consumer edge_type=%s reason=retry_stalled seconds=%.0f failing_rows=%d "
                "error_class=%s error=%s -- no write of this edge type has succeeded; failures without "
                "evidence are not counted, the rows wait and are never parked for it",
                edge_type, seconds, len(self._failing.get(edge_type, ())), stall.error_class, stall.error,
            )

    def _commit_resolved(self) -> None:
        """Commit, per partition and in one call, the highest offset below
        that partition's lowest unresolved record. A failed commit is logged
        and simply retried on the next tick (nothing depends on it: resolved
        records are never rewritten)."""
        offsets = []
        targets: dict[tuple[str, int], int] = {}
        for tp, fetched_hi in self._fetched_hi.items():
            outstanding = self._outstanding.get(tp)
            position = min(outstanding) if outstanding else fetched_hi
            if position > self._committed.get(tp, position):
                targets[tp] = position
                offsets.append(_topic_partition(tp[0], tp[1], position))
        if not offsets:
            return
        try:
            self._consumer.commit(offsets=offsets, asynchronous=False)
        except Exception:
            logger.exception(
                "stage=retry-consumer reason=commit_failed partitions=%d -- will retry next tick", len(offsets),
            )
            return
        self._committed.update(targets)

    def _notify(self, edge_type: str, outcome: str) -> None:
        if self._on_row_retried is None:
            return
        try:
            self._on_row_retried(edge_type, outcome)
        except Exception:
            logger.exception("stage=retry-consumer on_row_retried listener raised")

    # ── Rebalance ────────────────────────────────────────────────────────

    def on_partitions_revoked(self, partitions: Sequence[tuple[str, int]]) -> None:
        """Drop any held state for a revoked ``(topic, partition)`` without
        committing its offset -- the reassigned group member re-reads it
        from the last committed offset and independently re-decides
        pause/resume; the record's envelope (and its ``retry_after_ms``) is
        self-describing on the wire, so no cross-replica coordination is
        needed.

        Also strips revoked keys out of every :class:`_FrozenBatch` (review
        finding M11): after M8's per-batch snapshot, a record awaiting its
        batch's own ``caught_up`` barrier lives in neither ``_held`` nor
        ``_due_this_tick`` -- only in ``self._pending_batches`` -- so a
        revoke that didn't also check there would let this member later
        retry/re-DLQ/park (and commit) a record on a partition it no
        longer owns, at the same moment the new owner re-reads and
        processes that same record from its last committed offset. That
        duplicate is exactly the ordering hazard M9 closed.
        """
        revoked = set(partitions)
        for tp in revoked:
            self._outstanding.pop(tp, None)
            self._fetched_hi.pop(tp, None)
            self._committed.pop(tp, None)
            self._paused.discard(tp)
        if revoked:
            for key in [k for k in self._write_failures if k[:2] in revoked]:
                del self._write_failures[key]
            for per_row in (self._solo_uncounted, self._fail_streak, self._last_solo_try):
                for key in [k for k in per_row if k[:2] in revoked]:
                    del per_row[key]
            self._suspects = {k for k in self._suspects if k[:2] not in revoked}
            self._invalid_at_read = {k for k in self._invalid_at_read if k[:2] not in revoked}
            for key in [k for k, (_attempt, tp) in self._superseded.items() if tp in revoked]:
                del self._superseded[key]  # the new owner decides about those copies
            for key in [k for k, (_offset, tp) in self._resolved_high.items() if tp in revoked]:
                del self._resolved_high[key]  # likewise: the new owner resolves that pair's records now
            for edge_type in list(self._failing):
                gone = [k for k in self._failing[edge_type] if k[:2] in revoked]
                if gone:
                    # F6: only state tied to the revoked rows goes; an edge type
                    # left with no failing row loses its streak and stall. The
                    # fatal clock is kept across rebalances (its gap rule ages it).
                    self._forget_failing(edge_type, gone, keep_fatal=True)
                    if edge_type not in self._failing:
                        self._no_progress_streak.pop(edge_type, None)
            for edge_type in list(self._pair_stall_since):
                self._clear_pair_stalls(edge_type)  # S4: re-armed by the next failing flush
            # Only an optimisation (a blocked row's delay): forget it all.
            self._rerouted_wake.clear()
            self._rerouted_heap.clear()
            for key in [k for k in self._held if k[:2] in revoked]:
                self._unhold(self._held.pop(key))
            for key in [k for k in self._due_this_tick if k[:2] in revoked]:
                del self._due_this_tick[key]
                self._due_since.pop(key, None)
            self._wake_heap = [
                entry for entry in self._wake_heap if (entry[1], entry[2]) not in revoked
            ]
            heapq.heapify(self._wake_heap)
            for edge_type in list(self._pending_batches):
                batch = self._pending_batches[edge_type]
                batch.keyed_pendings = [
                    (key, pending) for key, pending in batch.keyed_pendings if key[:2] not in revoked
                ]
                if not batch.keyed_pendings:
                    del self._pending_batches[edge_type]

    def on_partitions_assigned(self, partitions: Sequence[tuple[str, int]]) -> None:
        """Defensively resume newly assigned partitions and reset their pause
        bookkeeping: a pause issued before a rebalance must never leave an
        assigned partition silently paused with nothing tracking it."""
        assigned = [tp for tp in partitions if tp not in self._outstanding or not self._outstanding[tp]]
        for tp in assigned:
            self._paused.discard(tp)
            # Nothing outstanding here: start from the consumer's own position.
            self._fetched_hi.pop(tp, None)
            self._committed.pop(tp, None)
        if assigned:
            try:
                self._consumer.resume([_topic_partition(*tp) for tp in assigned])
            except Exception:
                logger.exception("stage=retry-consumer reason=resume_on_assign_failed")

    # ── DLQ backlog gauge (Story 7) ──────────────────────────────────────

    def start_backlog_thread(self) -> None:
        """Start the periodic backlog-computation thread, if any edge type
        has a :class:`BacklogSource` configured. A no-op otherwise (no
        thread, no cost) -- mirrors this codebase's existing "don't run a
        process/thread with structurally nothing to do" convention."""
        if not self._backlog_source_by_edge_type or self._backlog_thread is not None:
            return
        self._backlog_stop_event.clear()
        self._backlog_thread = threading.Thread(
            target=self._backlog_loop, name="retry-consumer-backlog", daemon=True,
        )
        self._backlog_thread.start()

    def stop_backlog_thread(self) -> None:
        """Stop the backlog thread. Idempotent; safe even if it was never started."""
        self._backlog_stop_event.set()
        if self._backlog_thread is not None:
            self._backlog_thread.join(timeout=10.0)
            self._backlog_thread = None

    def _backlog_loop(self) -> None:
        # threading.Event.wait(timeout) both sleeps and can be woken
        # immediately by stop_backlog_thread() -- no busy polling, no
        # unbounded blocking, and never touches this consumer's own
        # poll-loop thread or its Kafka client.
        while not self._backlog_stop_event.wait(self._backlog_check_interval_seconds):
            for edge_type in self._backlog_source_by_edge_type:
                self.compute_and_notify_backlog(edge_type)

    def compute_and_notify_backlog(self, edge_type: str) -> dict[str, int] | None:
        """Compute one edge type's current DLQ backlog and report it via
        ``on_backlog_computed`` (Phase B wires this to
        ``metrics.dlq_backlog.labels(kind="edge", name=edge_type, queue=...).set(...)``
        through ``safe_emit``). Returns the computed value, or ``None`` on
        any failure -- never raises: a metadata-lookup failure must leave
        the gauge at its last-known value, not crash this background
        thread or report a misleading ``0``.
        """
        source = self._backlog_source_by_edge_type.get(edge_type)
        dlq_topic = self._dlq_topic_by_edge_type.get(edge_type)
        parked_topic = self._parked_topic_by_edge_type.get(edge_type)
        if source is None or dlq_topic is None or parked_topic is None:
            return None
        try:
            dlq_high, dlq_committed = source.dlq_high_watermark_and_committed(dlq_topic)
            parked_high, parked_low = source.parked_watermarks(parked_topic)
        except Exception:
            logger.warning(
                "stage=retry-consumer edge_type=%s reason=backlog_computation_failed -- "
                "leaving graph_loader_dlq_backlog at its last-known value", edge_type,
            )
            return None
        backlog = compute_dlq_backlog(
            dlq_high_watermark=dlq_high, dlq_committed_offset=dlq_committed,
            parked_high_watermark=parked_high, parked_low_watermark=parked_low,
        )
        if self._on_backlog_computed is not None:
            for queue, value in backlog.items():
                try:
                    self._on_backlog_computed(edge_type, queue, value)
                except Exception:
                    logger.exception("stage=retry-consumer on_backlog_computed listener raised")
        return backlog


# ── Real BacklogSource (Story 6, N15) ────────────────────────────────────────


def _topic_partitions(consumer, topic: str, timeout: float) -> list[int]:
    """Return the topic's partition ids from broker metadata (the same
    small, generic pattern ``parked_pair_index._topic_partitions`` uses --
    not imported from there; not worth an inter-module dependency for a
    few lines)."""
    metadata = consumer.list_topics(topic=topic, timeout=timeout)
    item = metadata.topics.get(topic) if metadata is not None else None
    if item is None or getattr(item, "error", None) is not None or not getattr(item, "partitions", None):
        raise RetryConsumerError(f"topic {topic} has no partition metadata")
    return sorted(int(partition) for partition in item.partitions)


class KafkaBacklogSource:
    """Real :class:`BacklogSource`: metadata-only broker round trips via a
    dedicated consumer connection (never the main poll-loop consumer),
    sharing the retry consumer's own consumer group so a committed-offset
    read reflects this group's actual position.

    Review finding N15: a partition with no committed offset yet
    (confluent_kafka reports ``OFFSET_INVALID``, ``-1001``) is mapped to
    that partition's own low watermark -- "everything from the start of
    the currently retained log is still outstanding" -- never treated as
    offset ``0`` (which would silently inflate the backlog for a
    ``retention.ms=-1`` topic whose low watermark is far from ``0`` after
    any compaction/deletion) and never treated as "no backlog" either.
    """

    def __init__(
        self, *, bootstrap_servers: str, group_id: str, consumer_factory=None, timeout: float = 10.0,
    ) -> None:
        if consumer_factory is None:
            from confluent_kafka import Consumer as consumer_factory
        self._consumer = consumer_factory({
            "bootstrap.servers": bootstrap_servers, "group.id": group_id, "enable.auto.commit": False,
        })
        self._timeout = timeout

    def _remaining(self, deadline: float) -> float:
        """Timeout for the next broker round trip of a call that has one
        overall ``timeout`` (not one per partition). Raises once the call's
        deadline has passed, so the gauge keeps its last value instead of a
        many-partition topic stalling the backlog thread for
        ``partitions x timeout``."""
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RetryConsumerError(f"backlog lookup exceeded its {self._timeout:g}s deadline")
        return max(0.1, remaining)

    def dlq_high_watermark_and_committed(self, dlq_topic: str) -> tuple[int, int]:
        from confluent_kafka import TopicPartition

        deadline = time.monotonic() + self._timeout
        partitions = _topic_partitions(self._consumer, dlq_topic, self._remaining(deadline))
        committed = self._consumer.committed(
            [TopicPartition(dlq_topic, partition) for partition in partitions],
            timeout=self._remaining(deadline),
        )
        committed_by_partition = {tp.partition: tp.offset for tp in committed}
        high_total = 0
        committed_total = 0
        for partition in partitions:
            low, high = self._consumer.get_watermark_offsets(
                TopicPartition(dlq_topic, partition), timeout=self._remaining(deadline),
            )
            high_total += high
            committed_offset = committed_by_partition.get(partition)
            if committed_offset is None or committed_offset < 0:
                committed_offset = low  # N15: OFFSET_INVALID -> this partition's own low watermark
            committed_total += committed_offset
        return high_total, committed_total

    def parked_watermarks(self, parked_topic: str) -> tuple[int, int]:
        from confluent_kafka import TopicPartition

        deadline = time.monotonic() + self._timeout
        partitions = _topic_partitions(self._consumer, parked_topic, self._remaining(deadline))
        high_total = 0
        low_total = 0
        for partition in partitions:
            low, high = self._consumer.get_watermark_offsets(
                TopicPartition(parked_topic, partition), timeout=self._remaining(deadline),
            )
            high_total += high
            low_total += low
        return high_total, low_total

    def close(self) -> None:
        try:
            self._consumer.close()
        except Exception:  # pragma: no cover - defensive, never raise on shutdown
            logger.exception("KafkaBacklogSource: error closing consumer")


def _revoked_topic_partitions(partitions: Sequence) -> list[tuple[str, int]]:
    """Adapt confluent_kafka's ``on_revoke`` callback argument (a list of
    ``TopicPartition`` objects) to the ``(topic, partition)`` tuples
    :meth:`RetryConsumer.on_partitions_revoked` expects (review finding
    N11)."""
    return [(tp.topic, tp.partition) for tp in partitions]


# ── CLI entrypoint ───────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    """Build the retry-consumer command parser."""
    parser = argparse.ArgumentParser(description="Run the fleet-wide DLQ retry consumer")
    parser.add_argument("--config", default="config/graph_schema.yaml")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--dlq-topics", required=True, help="comma-separated DLQ topics to subscribe to")
    parser.add_argument(
        "--lease-address", default=None,
        help="host:port of the run's live bucket-lease server. Omit for a standalone run against a "
             "finished bulk run's DLQ backlog (Decisions §1) -- writes then use the classic, "
             "non-leased Neo4j session path instead.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the retry consumer until SIGTERM, closing every Kafka/Neo4j/lease
    resource on every exit path. Never raises (review finding N13): any
    unexpected failure is logged and reported via a non-zero exit code,
    never left to crash-loop silently or hang."""
    parser = build_parser()
    args = parser.parse_args(argv)
    requested_topics = sorted({topic.strip() for topic in args.dlq_topics.split(",") if topic.strip()})
    if not requested_topics:
        raise SystemExit("--dlq-topics must name at least one topic")

    shutdown_requested = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_a: shutdown_requested.set())

    consumer = None
    driver = None
    dlq_producers: list = []
    parked_pair_indexes: list[ParkedPairIndex] = []
    retry_writers: list = []
    backlog_source = None
    retry_consumer = None
    process_metrics = metrics_module.process_metrics()
    metrics_module.install_dlq_routed_listener()
    metrics_module.install_parked_listener()
    metrics_server = metrics_module.start_metrics_server(process_metrics, kind="retry-consumer", name="-")
    exit_code = 1
    try:
        from confluent_kafka import Consumer

        from src.loader.edge_execution import lease_mode_settings
        from src.loader.lease_client import BucketCellRouter, BucketLeaseClient, parse_lease_address

        schema = load_schema(args.config)
        run_id = args.run_id if args.run_id is not None else "default_run"
        bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        # The CLI always sets the stable ``<prefix>-retry-consumer``; a
        # standalone run without KAFKA_GROUP_ID keeps the run-scoped default.
        group_id = os.environ.get("KAFKA_GROUP_ID", legacy_retry_group(run_id))

        # One DLQ per relationship type (v7-S4): every name derives from the type's
        # resolved DLQ (`<topic>.dlq` for one topic, `<type>.dlq` for several), never edge.topic.
        edge_by_dlq_topic = {}
        for edge in schema.edges:
            dlq_topic = edge_dead_letter_topic_name(edge)
            if dlq_topic in requested_topics:
                edge_by_dlq_topic[dlq_topic] = edge
        missing = sorted(set(requested_topics) - set(edge_by_dlq_topic))
        if missing:
            raise ValueError(f"--dlq-topics names topic(s) with no matching schema edge: {missing}")

        lease_env_settings = lease_mode_settings(os.environ)
        lease_address = args.lease_address or (lease_env_settings[0] if lease_env_settings else None)
        bucket_count = None
        keyed_labels: frozenset = frozenset()
        if lease_address is not None:
            parse_lease_address(lease_address)
            bucket_count = lease_env_settings[1] if lease_env_settings else None
            keyed_labels = lease_env_settings[2] if lease_env_settings else frozenset()
            if bucket_count is None:
                raise ValueError(
                    "--lease-address given but the bucket-lease bucket count is not configured "
                    "(GRAPH_LOADER_LEASE_BUCKETS) -- cannot compute lease resources"
                )
            logger.info("stage=retry-consumer-lease address=%s buckets=%s", lease_address, bucket_count)

        from neo4j import GraphDatabase

        driver_kwargs: dict = {}
        database = os.environ.get("NEO4J_DATABASE")
        if database:
            driver_kwargs["database"] = database
        driver = GraphDatabase.driver(
            os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
            auth=(os.environ.get("NEO4J_USERNAME", "neo4j"), os.environ.get("NEO4J_PASSWORD", "changeme")),
        )

        backlog_source = KafkaBacklogSource(bootstrap_servers=bootstrap_servers, group_id=group_id)
        retry_settings = schema.loading.retry_consumer
        lease_config = schema.loading.bucket_lease
        lease_backoff_ms = (lease_config.restart_backoff_base_ms, lease_config.restart_backoff_max_ms)
        lease_connect_timeout_seconds = (
            None if lease_config.reconnect_timeout_ms == 0 else lease_config.reconnect_timeout_ms / 1000
        )

        writer_by_edge_type: dict = {}
        sink_by_edge_type: dict = {}
        parked_pair_index_by_edge_type: dict = {}
        max_retry_attempts_by_edge_type: dict = {}
        parked_topic_by_edge_type: dict = {}
        base_delay_seconds_by_edge_type: dict = {}
        max_delay_seconds_by_edge_type: dict = {}
        dlq_topic_by_edge_type: dict = {}
        backlog_source_by_edge_type: dict = {}
        endpoint_pairs_by_edge_type: dict = {}
        single_pair_labels_by_edge_type: dict = {}

        for dlq_topic, edge in edge_by_dlq_topic.items():
            metrics_module.initialize_zero_series(process_metrics, kind="edge", name=edge.type)
            # D1: a real 0 from the start, so a stall alert never waits on a first sample.
            metrics_module.safe_emit(
                lambda edge_type=edge.type: process_metrics.retry_stalled.labels(kind="edge", name=edge_type).set(0)
            )
            dlq_topic_by_edge_type[edge.type] = dlq_topic
            backlog_source_by_edge_type[edge.type] = backlog_source
            max_retry_attempts_by_edge_type[edge.type] = edge.dead_letter.max_retry_attempts
            base_delay_seconds_by_edge_type[edge.type] = edge.retry.base_delay_seconds
            max_delay_seconds_by_edge_type[edge.type] = edge.retry.max_delay_seconds
            parked_topic_by_edge_type[edge.type] = edge_parked_topic_name(edge)

            sink, producer = build_malformed_record_sink(
                dead_letter=edge.dead_letter, entity_topic=edge.dead_letter_entity, entity_kind="edge",
                rejection_log_path="var/rejections/retry-consumer.jsonl",
                bootstrap_servers=bootstrap_servers, run_id=run_id, replica_id="retry",
            )
            sink_by_edge_type[edge.type] = sink
            if producer is not None:
                dlq_producers.append(producer)

            tx_timeout_seconds = schema.loading.bucket_lease.tx_timeout_ms / 1000

            # Review finding M18: standalone mode (no --lease-address) is
            # documented as the escape hatch for a *finished* bulk run's
            # opt-in DLQ backlog (Decisions §1) -- bulk-routed rows never
            # get a ParkedPairIndex entry at all (no live writer replica
            # ever calls add() for them), so building one here regardless
            # made every M12 contains() check see "not indexed" and skip
            # every candidate as a false "duplicate", retrying nothing
            # while still advancing this consumer group's offsets. No
            # index is built at all in standalone mode; this means
            # standalone mode gives no same-pair cross-record ordering
            # guarantee (documented in docs/USAGE.md) -- acceptable
            # because there is no live fleet left to protect against once
            # the bulk run that produced this backlog has finished.
            if edge.order_by is None and lease_address is not None:
                index_topic = edge_parked_pair_index_topic_name(edge)
                index = ParkedPairIndex(
                    topic=index_topic, bootstrap_servers=bootstrap_servers, writable=True,
                    sequenced=edge.is_multi_topic,
                    # v7-S7: the same label-pair keys as the live writer's index.
                    pair_of_topic=edge.pair_labels_for_topic if edge.is_multi_pair else None,
                )
                index.start()
                warn_index_leftovers(index, edge, stage="retry-consumer")
                parked_pair_indexes.append(index)
                parked_pair_index_by_edge_type[edge.type] = index

            def make_writer(config, drain_deadline, tx_timeout_seconds=tx_timeout_seconds,
                            multi_pair=edge.is_multi_pair):
                """One (pair's) retry writer; v7-S7 builds one per label pair from its view. A pair's
                coordinator does not observe lease_wait: the split writer observes the sum per call."""
                base_writer = build_retry_base_writer(
                    driver, config, tx_timeout_seconds=tx_timeout_seconds, metrics=process_metrics,
                    leased=lease_address is not None,
                )
                if lease_address is None:
                    return build_retry_writer(base_writer, chunk_size=config.mix_and_batch.batch_size)
                # P5-Q1: the same cell routing and lease coordinator the live
                # edge loader flushes through -- node-disjoint transactions.
                router = BucketCellRouter(config.nodes.source, config.nodes.target, bucket_count, keyed_labels)
                coordinator = build_retry_lease_coordinator(
                    base_writer, config, router=router,
                    client_factory=lambda: BucketLeaseClient(
                        lease_address, bucket_count=bucket_count, keyed_labels=keyed_labels,
                        connect_timeout_seconds=lease_connect_timeout_seconds, backoff_ms=lease_backoff_ms,
                        stop_event=shutdown_requested,
                    ),
                    worker_count=retry_settings.write_workers, tx_timeout_seconds=tx_timeout_seconds,
                    stop_event=shutdown_requested, metrics=process_metrics, drain_deadline=drain_deadline,
                    observe_lease_wait=not multi_pair,
                )
                return build_retry_writer(base_writer, coordinator=coordinator)

            writer_by_edge_type[edge.type] = build_pair_retry_writers(
                edge, make_writer, concurrent=lease_address is not None,
                metrics=process_metrics, observe_lease_wait=lease_address is not None,
            )
            if lease_address is not None:
                retry_writers.append(writer_by_edge_type[edge.type])
            if edge.is_multi_pair:
                endpoint_pairs_by_edge_type[edge.type] = {
                    topic: pair.labels for pair in edge.endpoint_pairs for topic in pair.topics
                }
            else:
                single_pair_labels_by_edge_type[edge.type] = (edge.nodes.source, edge.nodes.target)

        def _on_row_retried(edge_type: str, outcome: str) -> None:
            # N10: "blocked"/"duplicate_skipped" are not real retry attempts.
            if outcome in ("written", "re_dlq", "parked"):
                metrics_module.safe_emit(
                    lambda: process_metrics.retry.labels(
                        kind="edge", name=edge_type, reason="missing-endpoint",
                    ).inc()
                )

        def _on_stall_seconds(edge_type: str, seconds: float) -> None:
            metrics_module.safe_emit(
                lambda: process_metrics.retry_stalled.labels(kind="edge", name=edge_type).set(seconds)
            )

        def _on_backlog_computed(edge_type: str, queue: str, backlog: int) -> None:
            metrics_module.safe_emit(
                lambda: process_metrics.dlq_backlog.labels(kind="edge", name=edge_type, queue=queue).set(backlog)
            )

        consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": schema.loading.max_poll_interval_ms,
            "session.timeout.ms": schema.loading.session_timeout_ms,
            "heartbeat.interval.ms": schema.loading.heartbeat_interval_ms,
        })
        retry_consumer = RetryConsumer(
            consumer=consumer,
            writer_by_edge_type=writer_by_edge_type,
            sink_by_edge_type=sink_by_edge_type,
            parked_pair_index_by_edge_type=parked_pair_index_by_edge_type,
            max_retry_attempts_by_edge_type=max_retry_attempts_by_edge_type,
            parked_topic_by_edge_type=parked_topic_by_edge_type,
            base_delay_seconds_by_edge_type=base_delay_seconds_by_edge_type,
            max_delay_seconds_by_edge_type=max_delay_seconds_by_edge_type,
            poll_timeout_seconds=schema.loading.retry_consumer.poll_timeout_ms / 1000,
            due_batch_coalesce_seconds=schema.loading.retry_consumer.due_batch_coalesce_ms / 1000,
            max_batch_size=retry_settings.max_batch_records,
            max_records_per_partition=retry_settings.max_records_per_partition,
            max_overflow_held_per_partition=retry_settings.max_overflow_held_per_partition,
            flush_budget_seconds=min(
                DEFAULT_FLUSH_BUDGET_SECONDS, schema.loading.max_poll_interval_ms / 1000 / 4,
            ),
            backlog_source_by_edge_type=backlog_source_by_edge_type,
            dlq_topic_by_edge_type=dlq_topic_by_edge_type,
            backlog_check_interval_seconds=schema.loading.retry_consumer.backlog_check_interval_ms / 1000,
            on_row_retried=_on_row_retried,
            on_backlog_computed=_on_backlog_computed,
            on_stall_seconds=_on_stall_seconds,
            endpoint_pairs_by_edge_type=endpoint_pairs_by_edge_type,
            single_pair_labels_by_edge_type=single_pair_labels_by_edge_type,
        )

        def _on_revoke(_consumer, partitions) -> None:
            retry_consumer.on_partitions_revoked(_revoked_topic_partitions(partitions))

        def _on_assign(_consumer, partitions) -> None:
            retry_consumer.on_partitions_assigned(_revoked_topic_partitions(partitions))

        consumer.subscribe(list(edge_by_dlq_topic), on_revoke=_on_revoke, on_assign=_on_assign)
        retry_consumer.start_backlog_thread()
        logger.info(
            "stage=retry-consumer-ready run_id=%s dlq_topics=%s lease_fenced=%s",
            run_id, ",".join(sorted(edge_by_dlq_topic)), lease_address is not None,
        )
        try:
            retry_consumer.run_forever(shutdown_requested)
            exit_code = 0
        except Exception as exc:  # N13: never spin, never crash-loop silently
            logger.error("stage=retry-consumer reason=run_loop_failed error=%s", exc)
            exit_code = 1
    except Exception as exc:
        if shutdown_requested.is_set() and "shutdown requested" in str(exc):
            # N3: a stop while still starting surfaces as the lease client's
            # connect giving up ("lease reconnect abandoned: shutdown
            # requested"): the requested outcome, nothing consumed or committed.
            # Any other startup error stays an ERROR and exit 1, even when a
            # stop arrived at the same time (same rule as edge_loader.main).
            logger.warning("stage=retry-consumer reason=stopped_during_startup error=%s", exc)
            exit_code = 0
        else:
            logger.error("stage=retry-consumer reason=startup_failed error=%s", exc)
            exit_code = 1
    finally:
        if retry_consumer is not None:
            try:
                retry_consumer.stop_backlog_thread()
            except Exception:
                logger.exception("stage=retry-consumer error stopping backlog thread")
        for index in parked_pair_indexes:
            try:
                index.stop()
            except Exception:
                logger.exception("stage=retry-consumer error stopping a parked-pair index")
        for retry_writer in retry_writers:
            try:
                retry_writer.close()
            except Exception:
                logger.exception("stage=retry-consumer error closing a lease coordinator")
        if backlog_source is not None:
            backlog_source.close()
        if consumer is not None:
            try:
                consumer.close()
            except Exception:
                logger.exception("stage=retry-consumer error closing the consumer")
        for producer in dlq_producers:
            try:
                producer.flush()
            except Exception:
                logger.exception("stage=retry-consumer error flushing a DLQ producer")
        if driver is not None:
            try:
                driver.close()
            except Exception:
                logger.exception("stage=retry-consumer error closing the Neo4j driver")
        metrics_module.write_metrics_snapshot(
            process_metrics, os.environ.get("GRAPH_LOADER_METRICS_SNAPSHOT_PATH"),
        )
        metrics_module.stop_metrics_server(metrics_server)
        # F2: last statement of the shutdown path: CPython resets every Python-level handler to
        # SIG_DFL at interpreter exit (only SIG_IGN survives), so a repeated SIGTERM from a second
        # container stop would otherwise exit 143 during teardown.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    return exit_code


if __name__ == "__main__":
    import sys

    sys.exit(main())
