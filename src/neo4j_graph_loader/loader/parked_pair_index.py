"""In-memory, cross-flush parked-pair index (P5-S4 Story 3).

Closes the gap P5-S3's own cartographer flagged: P5-S3's row-level DLQ only
blocks a later record of the same ``(source_key, target_key)`` pair *within
one flush* (its ``_classify_rows`` same-flush blocking pass). Across
flushes, a parked older record can be retried after a newer record of the
same pair has already committed, and would then overwrite newer properties
with older ones.

Design (revised twice from the original per-pair-counter sketch in
``plan/v5/slice-4/stories.md`` Story 3, after two blocking review passes —
M2, then M3-M5 — see ``plan/v5/completion_slice-4.md`` for the full
rationale of each): a per-pair *counter*, incremented only when the retry
consumer first reads a DLQ envelope, cannot give the required guarantees:

* A record sitting behind a paused head record (a long backoff) is never
  indexed until the retry consumer actually reads it — a live writer's
  later flush for the same pair can race past ``is_blocked`` in that
  window and commit, and a stale retry can later overwrite it (the exact
  bug this index exists to prevent).
* Excluding a retried record's own count from its own ``is_blocked`` check
  (to avoid the record blocking itself) reintroduces the ordering bug: the
  root record can be re-DLQ'd to the *back* of its DLQ partition, and a
  later record for the same pair, now unblocked by any check, is written
  and then overwritten when the root's retry eventually succeeds.
* A read-modify-write counter is not idempotent: a re-read after a crash
  or rebalance double-increments, and the pair then never reaches zero,
  never parks, and cycles forever.

This module instead indexes **per record**, not per pair:

* Every outstanding (still unresolved) row is tracked by its own identity
  — ``(source_key, target_key, original_topic, original_partition,
  original_offset)`` — not by a shared per-pair count. Adding or removing
  the same record id twice is a no-op-safe overwrite/tombstone (no
  read-modify-write, so a duplicate add/remove from a crash or a
  re-delivered message is always safe).
* **Live path** (:meth:`is_blocked`): a pair is blocked if it has *any*
  outstanding record at all.
* **Retry path** (:meth:`is_blocked_by_earlier`): a specific record is
  blocked only if an *earlier* outstanding record of the same pair exists
  (excluding itself) — so the record currently being retried, including
  the pair's own root cause, never blocks on its own presence, and a
  re-DLQ (which keeps the same record identity) does not need any index
  write at all.
* An entry is added when a record first becomes an outstanding DLQ'd row
  (called by the live writer, synchronously, before its DLQ publish and
  before the source offset commits — Phase B's integration point) and
  removed only when that record is durably written or parked (called by
  the retry consumer) — never on a re-DLQ, which keeps the same identity
  and therefore needs no index write.
* "Earlier" is defined as: a lower ``original_offset`` within the same
  ``original_partition``; across *different* source partitions (possible
  when an edge type's source topic has more than one partition), the
  tie-break is ``(original_topic, original_partition)`` -- arbitrary but a
  strict total order, so blocking can never cycle. There is no shared
  wall-clock order across partitions (and comparing ``first_failure_at_ms``
  there was not transitive under clock skew). A pair normally lands on one
  partition (the source topic's key partitioning), so the cross-partition
  case is unusual.
* **Several topics per type (v7-S4).** A multi-topic relationship type has
  ONE index for all its topics (pair keys never include the topic), so a
  pair outstanding from topic a also blocks its later records arriving via
  topic b. Across topics a name tie-break would be wrong: a pair DLQ'd from
  topic ``b`` and then, later, blocked behind it on topic ``a`` would be
  retried a-first, and the older record would overwrite the newer one. An
  index built with ``sequenced=True`` therefore stamps every :meth:`add` with
  a per-pair causal sequence, ``1 + max(seq)`` over the pair's outstanding
  records in this instance's view (an identity already present keeps its
  seq). The order is the strict lexicographic key ``(seq, original_topic,
  original_partition, original_offset)`` (:meth:`order_key`), so it never
  cycles. A record that was blocked behind another one always sees it, so it
  always sorts after it. Unsequenced indexes (single-topic types) store no
  seq (wire value unchanged) and every seq is 0, so their order is exactly
  the ``(topic, partition, offset)`` order above. A live loader never adds
  from a view that lags the index as of its partition assignment (the
  edge loader's admission barrier holds new partitions until
  :meth:`caught_up`), so the previous owner's records are always seen.
  Residual, the cross-replica race documented below: two replicas adding
  records of one pair within the index's propagation delay of each other.

Confirmed, fail-closed publish (review finding M3): :meth:`add`/
:meth:`remove` publish with the same delivery-confirmation discipline as
``dead_letter.DeadLetterProducer.publish`` (``acks=all``,
``enable.idempotence``, a delivery-error callback, and the undelivered-
count check) and **raise** on any failure, *before* touching the local
in-memory view — an unconfirmed ``add`` must never let the live writer
believe a pair is indexed when it silently is not (which would let a
same-pair record elsewhere race past ``is_blocked``), and an unconfirmed
``remove`` must never unblock a pair locally that other replicas still see
as blocked (the safe failure direction: staying blocked too long, never
unblocking too early because of a publish that didn't actually happen).

EOF-based rebuild completion (review finding M4): ``start()`` decides a
partition's rebuild is complete from the broker's own end-of-partition
signal (``enable.partition.eof`` / ``KafkaError._PARTITION_EOF``, the same
idiom ``edge_planner.py``'s bounded scan already uses), never from
"a message whose offset matches a high watermark captured earlier" — on a
compacted topic, the record that used to sit at that exact offset can be
gone (a tombstone removed after ``delete.retention.ms``, or a key
super­seded and compacted away), so waiting for it would stall forever.

Non-blocking read barrier (review finding M5): the retry path's view of
this index is filled by this instance's own background poller, which can
lag behind whatever DLQ record the retry consumer just fetched — even
though the corresponding ``add()`` was acknowledged *before* that DLQ
publish (M3), this instance may not have polled it yet. :meth:`is_blocked_by_earlier`
called against a lagging view can therefore wrongly say "not blocked".
:meth:`watermark_snapshot` (taken once per due batch, after the batch's
records were fetched) plus :meth:`caught_up` (a cheap, local,
non-blocking check) let the retry consumer (Story 5) hold a due batch
paused — never sleeping inside ``poll()`` — until this instance's poller
has genuinely caught up to that snapshot, before trusting any
``is_blocked_by_earlier`` result for it.

Both the live writer and the retry consumer are, therefore, legitimate
writers of this index (a deliberate departure from this story's original
sketch, which envisioned the retry consumer as the *sole* writer) — gated
by one ``writable`` construction flag shared by both call sites, since
both now only ever add (live writer) or remove (retry consumer) entries,
never read-modify-write a shared counter.

Wire shape: key = canonical JSON ``[source_key, target_key, original_topic,
original_partition, original_offset]``; value = canonical JSON
``{"first_failure_at_ms": <int>}``, or a tombstone (``value=None``) once the
record is resolved. No envelope versioning (this is an internal accounting
structure, never read by an operator or ``replay-dlq``).
"""
from __future__ import annotations

from dataclasses import dataclass
import datetime as _datetime
import json
import logging
import threading
import time
from typing import Callable, Hashable, Sequence
import uuid

logger = logging.getLogger(__name__)

# Mirrors dead_letter.DEAD_LETTER_PRODUCER_CONFIG's discipline (review
# finding M3): a lost add()/remove() publish is worse here than an
# ordinary DLQ publish failure would be -- it silently breaks the
# ordering guarantee this whole module exists for, with no downstream
# signal (unlike a lost DLQ publish, which at least fails the caller's own
# offset-commit).
PARKED_PAIR_INDEX_PRODUCER_CONFIG: dict[str, object] = {
    "acks": "all",
    "enable.idempotence": True,
    "max.in.flight.requests.per.connection": 5,
    "delivery.timeout.ms": 30_000,
    "message.max.bytes": 65536 + 4096,
    "allow.auto.create.topics": False,
}


_POLL_ERROR_WAIT_BASE_SECONDS = 0.5
_POLL_ERROR_WAIT_MAX_SECONDS = 30.0


def _poll_error_wait_seconds(failures: int) -> float:
    """Capped exponential wait after ``failures`` consecutive poll errors
    (0.5 s doubling to 30 s); reset by the first successful poll."""
    return min(_POLL_ERROR_WAIT_BASE_SECONDS * 2 ** max(0, failures - 1), _POLL_ERROR_WAIT_MAX_SECONDS)


class ParkedPairIndexError(RuntimeError):
    """Raised on a misuse of :class:`ParkedPairIndex` (e.g. a read-only
    instance asked to mutate the index) or a failed/unconfirmed publish."""


def _json_safe(value: object) -> object:
    """Return a JSON-encodable form of one key-property value.

    Mirrors ``dead_letter._dlq_json_safe``'s ISO-8601 handling for
    ``date``/``datetime`` (not imported from there — this module has no
    dependency on ``dead_letter.py``; the two only need to agree on
    *shape*). Anything else (``str``/``int``/``float``/``bool``/``None``)
    passes through unchanged.
    """
    if isinstance(value, _datetime.datetime):
        return value.isoformat()
    if isinstance(value, _datetime.date):
        return value.isoformat()
    return value


def _pair_key(source_key: object, target_key: object) -> str:
    """Return the canonical string identifying one ``(source_key, target_key)`` pair."""
    return json.dumps([_json_safe(source_key), _json_safe(target_key)], separators=(",", ":"))


def _stamped_labels(value: object) -> "tuple[str, str] | None":
    """A ``[S, T]`` label stamp (final follow-ups S2), or ``None`` (absent, malformed or a pseudo pair)."""
    if isinstance(value, (list, tuple)) and len(value) == 2 and all(isinstance(label, str) for label in value):
        if value[0] != "?":
            return (value[0], value[1])
    return None


def _record_wire_key(
    source_key: object, target_key: object, original_topic: str, original_partition: int, original_offset: int,
) -> str:
    """Return the canonical string identifying one specific outstanding record."""
    return json.dumps(
        [_json_safe(source_key), _json_safe(target_key), original_topic, original_partition, original_offset],
        separators=(",", ":"),
    )


@dataclass(frozen=True)
class _OutstandingRecord:
    original_topic: str
    original_partition: int
    original_offset: int
    first_failure_at_ms: int
    seq: int = 0
    # Final follow-ups S2: the label pair the record was written under (multi-pair indexes stamp it;
    # None for single-pair and pre-stamp entries). Never part of the order.
    labels: "tuple[str, str] | None" = None

    @property
    def order(self) -> tuple[int, str, int, int]:
        return (self.seq, self.original_topic, self.original_partition, self.original_offset)

    def is_earlier_than(self, other: "_OutstandingRecord") -> bool:
        """Strict total order: ``(original_topic, original_partition,
        original_offset)``.

        Within a source partition this is exactly offset order (authoritative:
        a pair always lands on one partition). Across partitions it is an
        arbitrary but *consistent* tie-break. The earlier fallback to
        ``first_failure_at_ms`` across partitions was not transitive (offset
        order within a partition, wall-clock across), so with clock skew three
        records could each be "earlier" than the next and block each other
        forever, re-DLQ'd without spending an attempt. ``first_failure_at_ms``
        is deliberately not used.

        ``seq`` (v7-S4, 0 unless the index is ``sequenced``) leads the key, so
        records of one pair from several topics sort in causal order."""
        return self.order < other.order


class ParkedPairIndex:
    """Per-record, cross-flush index of outstanding DLQ'd relationship rows.

    Rebuilt from the beginning of the compacted ``.parked-pairs`` topic
    (bounded by compaction) at :meth:`start`, then kept current by a
    background thread that polls indefinitely until :meth:`stop` — never
    closes, never commits (no consumer group; a dedicated, non-committing
    reader, exactly like ``dead_letter_replay.py``'s own reader, reused
    conceptually, not by import).

    ``writable=False`` (the default) is the shape every lookup-only replica
    uses (both a live-writer replica calling :meth:`is_blocked` and a
    retry-consumer replica calling :meth:`is_blocked_by_earlier` may be
    read-only, as long as some other, ``writable=True`` instance owns the
    actual :meth:`add`/:meth:`remove` calls for that edge type).
    """

    def __init__(
        self,
        *,
        topic: str,
        bootstrap_servers: str,
        writable: bool = False,
        sequenced: bool = False,
        pair_of_topic: "Callable[[str], Hashable] | None" = None,
        consumer_factory=None,
        producer_factory=None,
        clock: Callable[[], int] | None = None,
        poll_timeout_seconds: float = 1.0,
        watermark_timeout_seconds: float = 5.0,
    ) -> None:
        if not topic or not topic.strip():
            raise ParkedPairIndexError("topic must not be blank")
        if not bootstrap_servers or not bootstrap_servers.strip():
            raise ParkedPairIndexError("bootstrap_servers must not be blank")
        self._topic = topic
        self._bootstrap_servers = bootstrap_servers
        self._writable = writable
        # v7-S4: stamp adds with a per-pair causal sequence (multi-topic types).
        self._sequenced = sequenced
        # v7-S7: for a type whose topics bind several label pairs, the in-memory
        # pair key is (pair_of_topic(original_topic), source_key, target_key), so
        # Person1->Post1 and Person1->Comment1 never block or order each other.
        # The wire key is unchanged (it already carries original_topic), so entries
        # written before S7 rebuild into their topic's pair. Final follow-ups S2: the
        # value also stamps "labels": [S, T] (ignored by older readers), so a start
        # can tell entries of a rebound topic. None: one constant pair, today's
        # behaviour and today's value bytes (single-pair types).
        self._pair_of_topic = pair_of_topic
        self._consumer_factory = consumer_factory
        self._producer_factory = producer_factory
        self._clock = clock or (lambda: int(time.time() * 1000))
        self._poll_timeout_seconds = poll_timeout_seconds
        # Review finding M7: confluent_kafka's get_watermark_offsets blocks
        # forever with no timeout given. watermark_snapshot() is called
        # from the retry consumer's own poll-loop thread (Story 5) -- an
        # unbounded wait there would violate the "never block poll()" rule
        # and risk a max.poll.interval.ms eviction on a broker/leader
        # outage. Bounded here; a timeout raises (never hangs), and the
        # caller treats that the same as "not caught up this tick".
        self._watermark_timeout_seconds = watermark_timeout_seconds

        # pair_key -> {record_wire_key: _OutstandingRecord}
        self._by_pair: dict[str, dict[str, _OutstandingRecord]] = {}
        # partition -> highest position (offset+1 for a real record, or the
        # EOF marker's own offset) this instance's poller has applied.
        # Read/written under _lock; drives caught_up()'s non-blocking check.
        self._applied_position: dict[int, int] = {}
        self._lock = threading.Lock()
        self._consumer = None
        self._watermark_consumer = None
        self._producer = None
        self._partitions: list[int] = []
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._started = False

    def _key(self, source_key: object, target_key: object, original_topic: str):
        """The in-memory pair key of a record (v7-S7: label-pair qualified when ``pair_of_topic`` is set)."""
        if self._pair_of_topic is None:
            return _pair_key(source_key, target_key)
        return (self._pair_of_topic(original_topic), _pair_key(source_key, target_key))

    def pair_of(self, original_topic: str):
        """The label pair ``original_topic`` binds, or ``None`` for a single-pair index (v7-S7)."""
        return None if self._pair_of_topic is None else self._pair_of_topic(original_topic)

    def outstanding_by_topic(self) -> "dict[tuple[str, tuple[str, str] | None], int]":
        """Outstanding records per ``(original_topic, stamped label pair or None)`` (final follow-ups S2)."""
        counts: dict = {}
        with self._lock:
            for records in self._by_pair.values():
                for record in records.values():
                    key = (record.original_topic, record.labels)
                    counts[key] = counts.get(key, 0) + 1
        return counts

    def scoped(self, endpoint_pair: "tuple[str, str]") -> "ScopedParkedPairIndex":
        """A :class:`ScopedParkedPairIndex` for one label pair (the per-pair live writer's view)."""
        if self._pair_of_topic is None:
            raise ParkedPairIndexError("ParkedPairIndex.scoped() needs an index built with pair_of_topic")
        return ScopedParkedPairIndex(self, tuple(endpoint_pair))

    # ── Lifecycle ──────────────────────────────────────────────────────

    def start(self) -> None:
        """Rebuild from the topic's beginning, then start the background poller.

        Blocks until every partition has reached the broker's own
        end-of-partition marker (review finding M4 — never "a message at
        a previously captured high-watermark offset", which can stall
        forever on a compacted topic once the record that used to sit at
        that exact offset has been compacted or tombstone-expired away).
        """
        if self._started:
            raise ParkedPairIndexError("ParkedPairIndex.start() called twice")
        consumer_factory = self._consumer_factory
        if consumer_factory is None:
            from confluent_kafka import Consumer as consumer_factory
        from confluent_kafka import KafkaError, OFFSET_BEGINNING, TopicPartition

        # Review finding M6: a real confluent_kafka Consumer refuses to
        # construct at all without group.id ("group.id must be set"),
        # which a fake-client unit test can't catch. Never subscribed,
        # never committed -- assign() is used exclusively -- but librdkafka
        # still requires the field to be present, so a unique, throwaway
        # id is generated per consumer instance (mirrors
        # dead_letter_replay.py / edge_planner.py's own dedicated-reader
        # convention).
        base_consumer_config = {
            "bootstrap.servers": self._bootstrap_servers,
            "enable.auto.commit": False,
            "enable.partition.eof": True,
        }
        self._consumer = consumer_factory({
            **base_consumer_config, "group.id": f"graph-loader-parked-pairs-{uuid.uuid4().hex}",
        })
        self._watermark_consumer = consumer_factory({
            **base_consumer_config, "group.id": f"graph-loader-parked-pairs-{uuid.uuid4().hex}",
        })
        self._partitions = _topic_partitions(self._consumer, self._topic)
        self._consumer.assign(
            [TopicPartition(self._topic, partition, OFFSET_BEGINNING) for partition in self._partitions]
        )

        pending_partitions = set()
        for partition in self._partitions:
            low, high = self._get_watermark_offsets(self._consumer, partition)
            if low < high:
                pending_partitions.add(partition)
            else:
                with self._lock:
                    self._applied_position[partition] = high

        eof_code = KafkaError._PARTITION_EOF
        stall_deadline = time.monotonic() + 120.0
        while pending_partitions:
            message = self._consumer.poll(self._poll_timeout_seconds)
            if message is None:
                if time.monotonic() > stall_deadline:
                    raise ParkedPairIndexError(
                        f"ParkedPairIndex rebuild for topic {self._topic} stalled before "
                        "reaching every partition's end -- broker unreachable?"
                    )
                continue
            stall_deadline = time.monotonic() + 120.0
            error = message.error()
            if error is not None:
                if error.code() == eof_code:
                    partition = message.partition()
                    self._advance_position(partition, message.offset())
                    pending_partitions.discard(partition)
                continue
            self._apply(message)

        if self._writable:
            producer_factory = self._producer_factory
            if producer_factory is None:
                from confluent_kafka import Producer as producer_factory
            self._producer = producer_factory({
                **PARKED_PAIR_INDEX_PRODUCER_CONFIG, "bootstrap.servers": self._bootstrap_servers,
            })

        self._started = True
        self._thread = threading.Thread(
            target=self._poll_forever, name="parked-pair-index-poller", daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop the background poller and release the consumer/producer.

        Idempotent and safe to call even if :meth:`start` was never
        called or already failed partway through.
        """
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=10.0)
            self._thread = None
        for attr in ("_consumer", "_watermark_consumer"):
            client = getattr(self, attr)
            if client is not None:
                try:
                    client.close()
                except Exception:  # pragma: no cover - defensive, never raise on shutdown
                    logger.exception("ParkedPairIndex: error closing %s for topic %s", attr, self._topic)
                setattr(self, attr, None)
        if self._producer is not None:
            try:
                self._producer.flush()
            except Exception:  # pragma: no cover - defensive, never raise on shutdown
                logger.exception("ParkedPairIndex: error flushing producer for topic %s", self._topic)
            self._producer = None

    def _poll_forever(self) -> None:
        from confluent_kafka import KafkaError

        eof_code = KafkaError._PARTITION_EOF
        failures = 0
        while not self._stop_event.is_set():
            try:
                message = self._consumer.poll(self._poll_timeout_seconds)
            except Exception:  # defensive: a poll error must never kill this thread
                logger.exception("ParkedPairIndex: poll failed for topic %s", self._topic)
                failures += 1
                self._stop_event.wait(_poll_error_wait_seconds(failures))
                continue
            if message is None:
                failures = 0
                continue
            error = message.error()
            if error is not None:
                if error.code() == eof_code:
                    failures = 0
                    self._advance_position(message.partition(), message.offset())
                else:
                    # A persistent broker error would otherwise spin this
                    # thread (log flood, a busy core): wait, capped.
                    logger.warning("ParkedPairIndex: poll error for topic %s: %s", self._topic, error)
                    failures += 1
                    self._stop_event.wait(_poll_error_wait_seconds(failures))
                continue
            failures = 0
            self._apply(message)

    def _advance_position(self, partition: int, position: int) -> None:
        with self._lock:
            if position > self._applied_position.get(partition, -1):
                self._applied_position[partition] = position

    def _apply(self, message) -> None:
        raw_key = message.key()
        wire_key = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else raw_key
        value = message.value()
        try:
            decoded_key = json.loads(wire_key)
            source_key, target_key, original_topic, original_partition, original_offset = decoded_key
        except (TypeError, ValueError):
            logger.warning(
                "ParkedPairIndex: unreadable key on topic %s -- ignoring", self._topic,
            )
            self._advance_position(message.partition(), message.offset() + 1)
            return
        pair_key = self._key(source_key, target_key, original_topic)
        with self._lock:
            if value is None:
                records = self._by_pair.get(pair_key)
                if records is not None:
                    records.pop(wire_key, None)
                    if not records:
                        del self._by_pair[pair_key]
            else:
                try:
                    data = json.loads(value)
                    first_failure_at_ms = int(data["first_failure_at_ms"])
                    seq = int(data.get("seq", 0))
                    labels = _stamped_labels(data.get("labels"))
                except (TypeError, ValueError, KeyError, AttributeError):
                    logger.warning(
                        "ParkedPairIndex: unreadable value on topic %s, key=%s -- ignoring",
                        self._topic, wire_key,
                    )
                    self._applied_position[message.partition()] = max(
                        self._applied_position.get(message.partition(), -1), message.offset() + 1,
                    )
                    return
                self._by_pair.setdefault(pair_key, {})[wire_key] = _OutstandingRecord(
                    original_topic=original_topic, original_partition=original_partition,
                    original_offset=original_offset, first_failure_at_ms=first_failure_at_ms, seq=seq,
                    labels=labels,
                )
            self._applied_position[message.partition()] = max(
                self._applied_position.get(message.partition(), -1), message.offset() + 1,
            )

    # ── Live path (every live-writer replica) ───────────────────────────

    def is_blocked(self, source_key: object, target_key: object, *,
                   endpoint_pair: "tuple[str, str] | None" = None) -> bool:
        """Return whether this pair currently has any outstanding record at all.

        v7-S7: an index built with ``pair_of_topic`` needs ``endpoint_pair`` (the
        live row's label pair); without it the question is ambiguous."""
        if self._pair_of_topic is None:
            pair_key = _pair_key(source_key, target_key)
        elif endpoint_pair is None:
            raise ParkedPairIndexError(
                "ParkedPairIndex.is_blocked() on a multi-pair index needs endpoint_pair=(source, target)"
            )
        else:
            pair_key = (tuple(endpoint_pair), _pair_key(source_key, target_key))
        with self._lock:
            return bool(self._by_pair.get(pair_key))

    # ── Retry path (the retry consumer only) ─────────────────────────────

    def contains(
        self, source_key: object, target_key: object, *,
        original_topic: str, original_partition: int, original_offset: int,
    ) -> bool:
        """Return whether this *specific* record is currently indexed as
        outstanding (review finding M12).

        A record whose own entry is missing has already been durably
        resolved (written or parked) by an earlier duplicate copy of
        itself -- P5-S3's own at-least-once contract can republish an
        already-recorded row when a DLQ publish fails partway through a
        multi-row batch and the whole batch replays -- or, for a bulk
        opt-in run of this edge type (P5-S4 Decisions §1), was never
        indexed at all. Either way, the caller (the retry consumer) must
        never write or re-DLQ/park it again: doing so for a genuine
        duplicate would re-publish (and eventually re-apply) a stale copy
        after a newer record of the same pair has already been unblocked
        and written, silently overwriting it -- idempotent ``MERGE``
        alone does not prevent this, since the hazard is stale *data*, not
        a duplicate *relationship*.

        Callers must first confirm :meth:`caught_up` for a snapshot taken
        after this record was fetched (M5), exactly like
        :meth:`is_blocked_by_earlier` — this method itself does not check
        that.
        """
        pair_key = self._key(source_key, target_key, original_topic)
        wire_key = _record_wire_key(source_key, target_key, original_topic, original_partition, original_offset)
        with self._lock:
            records = self._by_pair.get(pair_key)
            return records is not None and wire_key in records

    def watermark_snapshot(self) -> dict[int, int]:
        """Return ``{partition: current_high_watermark}`` for this index's
        topic, captured now (one broker metadata round-trip). Intended to
        be taken once per due batch (Story 5), not per record — see
        :meth:`caught_up`.

        Uses a dedicated consumer connection (constructed once, in
        :meth:`start`) never touched by the background poller, so this
        call is safe to make from a different thread than the poller's
        own, with no risk of concurrent use of one ``Consumer`` instance.

        Bounded by ``watermark_timeout_seconds`` (review finding M7): a
        broker/leader outage must raise, never hang -- this is called from
        the retry consumer's own poll-loop thread (Story 5), where an
        unbounded wait would break the "never block poll()" rule and risk
        a ``max.poll.interval.ms`` eviction. Raises
        :class:`ParkedPairIndexError` on a timeout or any other failure;
        the caller (Story 5) treats that identically to "not caught up
        this tick" — the due batch stays paused and the poll loop keeps
        going.
        """
        if self._watermark_consumer is None:
            raise ParkedPairIndexError("ParkedPairIndex.watermark_snapshot() requires start() to have been called")
        return {
            partition: self._get_watermark_offsets(self._watermark_consumer, partition)[1]
            for partition in self._partitions
        }

    def _get_watermark_offsets(self, consumer, partition: int) -> tuple[int, int]:
        from confluent_kafka import TopicPartition

        try:
            return consumer.get_watermark_offsets(
                TopicPartition(self._topic, partition), timeout=self._watermark_timeout_seconds,
            )
        except Exception as exc:
            raise ParkedPairIndexError(
                f"parked-pair index watermark lookup for topic {self._topic} partition {partition} "
                f"failed or timed out after {self._watermark_timeout_seconds}s: {exc}"
            ) from exc

    def caught_up(self, snapshot: dict[int, int]) -> bool:
        """Return whether this instance's background poller has applied
        every record up to ``snapshot``'s captured high watermarks, on
        every partition. Non-blocking: reads only this instance's own
        locally tracked position, no broker call (review finding M5).

        The retry consumer (Story 5) must hold a due batch paused —
        without sleeping inside ``poll()`` — until this returns ``True``
        for a snapshot taken after that batch's records were fetched,
        before trusting any :meth:`is_blocked_by_earlier` result for it:
        an ``add()`` is always acknowledged before its corresponding DLQ
        publish (M3), so any index entry that matters for a just-fetched
        DLQ record is guaranteed to sit at or below a snapshot taken
        after that fetch — but this instance's own poller may not have
        applied it yet.
        """
        with self._lock:
            applied = dict(self._applied_position)
        return all(applied.get(partition, -1) >= high for partition, high in snapshot.items())

    def is_blocked_by_earlier(
        self, source_key: object, target_key: object, *,
        original_topic: str, original_partition: int, original_offset: int, first_failure_at_ms: int,
    ) -> bool:
        """Return whether an *earlier* outstanding record of this pair exists,
        excluding the record identified by ``original_topic``/
        ``original_partition``/``original_offset`` itself.

        ``first_failure_at_ms`` is informational only: ordering uses the
        record identity (see :meth:`_OutstandingRecord.is_earlier_than`).

        Used only by the retry consumer, on the record it is about to
        retry — a record never blocks on its own presence in the index
        (unlike :meth:`is_blocked`, which the live path uses and which
        deliberately does not exclude anything). Callers must first
        confirm :meth:`caught_up` for a snapshot taken after this record
        was fetched (M5) — this method itself does not check that; it
        only reads whatever the local view currently holds.
        """
        pair_key = self._key(source_key, target_key, original_topic)
        self_key = _record_wire_key(source_key, target_key, original_topic, original_partition, original_offset)
        with self._lock:
            records = self._by_pair.get(pair_key, {})
            own = self._order_locked(records, self_key, original_topic, original_partition, original_offset)
            return any(
                other_record.order < own
                for other_key, other_record in records.items()
                if other_key != self_key
            )

    @staticmethod
    def _order_locked(
        records: dict, self_key: str, original_topic: str, original_partition: int, original_offset: int,
    ) -> tuple[int, str, int, int]:
        record = records.get(self_key)
        if record is not None:
            return record.order
        return (0, original_topic, original_partition, original_offset)

    def order_key(
        self, source_key: object, target_key: object, *,
        original_topic: str, original_partition: int, original_offset: int,
    ) -> tuple[int, str, int, int]:
        """The record's position in its pair's order: ``(seq, original_topic,
        original_partition, original_offset)``, the key :meth:`is_blocked_by_earlier`
        and :meth:`earlier_outstanding` compare (v7-S4). ``seq`` is 0 for a
        record that is not indexed, and always 0 in an unsequenced index, so the
        key then orders exactly like the identity tuple."""
        pair_key = self._key(source_key, target_key, original_topic)
        self_key = _record_wire_key(source_key, target_key, original_topic, original_partition, original_offset)
        with self._lock:
            return self._order_locked(
                self._by_pair.get(pair_key, {}), self_key, original_topic, original_partition, original_offset,
            )

    def earlier_outstanding(
        self, source_key: object, target_key: object, *,
        original_topic: str, original_partition: int, original_offset: int,
    ) -> list[tuple[str, int, int]]:
        """Return the identities ``(original_topic, original_partition,
        original_offset)`` of every outstanding record of this pair that is
        *earlier* than the given record (the same order as
        :meth:`is_blocked_by_earlier`, :meth:`order_key`), oldest first.

        Lets the retry consumer tell a record blocked only behind rows of its
        own batch (which it writes first, in order) from one blocked behind a
        row outstanding elsewhere. Same :meth:`caught_up` precondition as
        :meth:`is_blocked_by_earlier`."""
        pair_key = self._key(source_key, target_key, original_topic)
        self_key = _record_wire_key(source_key, target_key, original_topic, original_partition, original_offset)
        with self._lock:
            records = self._by_pair.get(pair_key, {})
            own = self._order_locked(records, self_key, original_topic, original_partition, original_offset)
            earlier = sorted(
                record.order for key, record in records.items() if key != self_key and record.order < own
            )
        return [order[1:] for order in earlier]

    # ── Mutation (add: live writer; remove: retry consumer) ─────────────

    def _require_writable(self) -> None:
        if not self._writable:
            raise ParkedPairIndexError(
                "ParkedPairIndex.add()/remove() require writable=True -- a read-only "
                "instance may only call is_blocked()/is_blocked_by_earlier()"
            )

    def add(
        self, source_key: object, target_key: object, *,
        original_topic: str, original_partition: int, original_offset: int, first_failure_at_ms: int,
    ) -> None:
        """Record one outstanding DLQ'd row.

        Called by the live writer, synchronously, before its DLQ publish
        and before the source offset commits (Phase B's integration
        point). Idempotent: adding the same record identity twice
        (e.g. after a crash-and-retry of the DLQ publish itself) simply
        republishes the same key/value — a harmless overwrite, never a
        double-count, because this index tracks record identities in a
        set-like structure, not a counter.

        Raises :class:`ParkedPairIndexError` (review finding M3) if the
        publish is not confirmed by Kafka — the local view is updated
        only *after* a confirmed publish, so a failed ``add`` never lets
        this replica believe the pair is indexed when every other replica
        still sees it as unblocked.
        """
        self._require_writable()
        wire_key = _record_wire_key(source_key, target_key, original_topic, original_partition, original_offset)
        pair_key = self._key(source_key, target_key, original_topic)
        payload: dict = {"first_failure_at_ms": first_failure_at_ms}
        # v7-S4 seq. Computed under the lock but published outside it: safe
        # because one pair's adds are serialized by their caller (a pair always
        # routes to one lane / lease cell, whose rows are routed in order).
        with self._lock:
            records = self._by_pair.get(pair_key, {})
            existing = records.get(wire_key)
            highest = max((record.seq for record in records.values()), default=0)
        if existing is not None:
            seq = existing.seq  # idempotent re-add keeps its place
        elif self._sequenced:
            seq = highest + 1  # after every outstanding record of the pair this instance sees
        else:
            # Unsequenced (one topic): seq 0, so the wire value is unchanged. If
            # the pair still has sequenced entries (the type had several topics
            # before), sort after all of them, as a sequenced add would: a tie
            # would let the identity order put this later record first.
            seq = highest + 1 if highest else 0
        if seq:
            payload["seq"] = seq
        labels = None
        if self._pair_of_topic is not None:
            # Final follow-ups S2: a multi-pair index stamps the label pair the record was written
            # under, so a start can tell entries of a topic since rebound to another pair (or of a
            # type collapsed back to one pair). Single-pair values are unchanged.
            labels = _stamped_labels(list(self._pair_of_topic(original_topic)))
            if labels is not None:
                payload["labels"] = list(labels)
        value = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        self._publish(wire_key, value)
        record = _OutstandingRecord(
            original_topic=original_topic, original_partition=original_partition,
            original_offset=original_offset, first_failure_at_ms=first_failure_at_ms, seq=seq, labels=labels,
        )
        with self._lock:
            self._by_pair.setdefault(pair_key, {})[wire_key] = record

    def remove(
        self, source_key: object, target_key: object, *,
        original_topic: str, original_partition: int, original_offset: int,
    ) -> None:
        """Record that this specific record is now durably resolved (written
        or parked) — never called for a re-DLQ, which keeps the same
        record identity and therefore needs no index write at all.

        Idempotent: removing an already-removed (or never-added) record
        identity is a harmless no-op tombstone republish.

        Raises :class:`ParkedPairIndexError` (review finding M3) if the
        publish is not confirmed — the local view is updated only *after*
        a confirmed publish, so a failed ``remove`` leaves the pair
        blocked locally too (the safe failure direction) rather than
        silently unblocking it while every other replica still sees it as
        blocked.
        """
        self._require_writable()
        wire_key = _record_wire_key(source_key, target_key, original_topic, original_partition, original_offset)
        self._publish(wire_key, None)
        pair_key = self._key(source_key, target_key, original_topic)
        with self._lock:
            records = self._by_pair.get(pair_key)
            if records is not None:
                records.pop(wire_key, None)
                if not records:
                    del self._by_pair[pair_key]

    def remove_many(
        self, records: "Sequence[tuple[object, object, str, int, int]]",
    ) -> "list[Exception | None]":
        """:meth:`remove` for many records with ONE producer flush.

        ``records``: ``(source_key, target_key, original_topic,
        original_partition, original_offset)``. Returns one positional
        result: ``None`` once that record's tombstone is acknowledged (and
        only then is the local view updated, exactly like :meth:`remove`),
        otherwise the :class:`ParkedPairIndexError` it would have raised.
        """
        self._require_writable()
        assert self._producer is not None, "ParkedPairIndex: writable instance has no producer (start() not called?)"
        results: list[Exception | None] = [None] * len(records)
        delivered: set[int] = set()
        failed: dict[int, Exception] = {}
        keys: list[tuple[str, str]] = []
        produced: list[int] = []
        for position, (source_key, target_key, topic, partition, offset) in enumerate(records):
            wire_key = _record_wire_key(source_key, target_key, topic, partition, offset)
            keys.append((wire_key, self._key(source_key, target_key, topic)))

            def on_delivery(error, _message, position=position) -> None:
                if error is None:
                    delivered.add(position)
                else:
                    failed[position] = ParkedPairIndexError(
                        f"parked-pair index publish to {self._topic} failed: {error}"
                    )

            try:
                self._producer.produce(
                    self._topic, key=wire_key.encode("utf-8"), value=None, on_delivery=on_delivery,
                )
                produced.append(position)
            except Exception as exc:
                results[position] = ParkedPairIndexError(
                    f"parked-pair index publish to {self._topic} failed: {exc}"
                )
        if produced:
            flush_error: Exception | None = None
            try:
                self._producer.flush()
            except Exception as exc:
                flush_error = ParkedPairIndexError(f"parked-pair index publish to {self._topic} failed: {exc}")
            for position in produced:
                if position in failed:
                    results[position] = failed[position]
                elif position not in delivered:
                    results[position] = flush_error or ParkedPairIndexError(
                        f"parked-pair index publish to {self._topic} has an undelivered message"
                    )
        with self._lock:
            for position, (wire_key, pair_key) in enumerate(keys):
                if results[position] is not None:
                    continue
                pair_records = self._by_pair.get(pair_key)
                if pair_records is not None:
                    pair_records.pop(wire_key, None)
                    if not pair_records:
                        del self._by_pair[pair_key]
        return results

    def _publish(self, wire_key: str, value: bytes | None) -> None:
        """Synchronously publish and block until Kafka acknowledges, raising
        on any failure (review finding M3 — mirrors
        ``dead_letter.DeadLetterProducer.publish``'s discipline exactly)."""
        assert self._producer is not None, "ParkedPairIndex: writable instance has no producer (start() not called?)"
        delivery_error: Exception | None = None

        def on_delivery(error, _message) -> None:
            nonlocal delivery_error
            if error is not None:
                delivery_error = error

        try:
            self._producer.produce(
                self._topic, key=wire_key.encode("utf-8"), value=value, on_delivery=on_delivery,
            )
            undelivered_count = self._producer.flush()
        except Exception as exc:
            raise ParkedPairIndexError(f"parked-pair index publish to {self._topic} failed: {exc}") from exc
        if undelivered_count:
            raise ParkedPairIndexError(
                f"parked-pair index publish to {self._topic} has {undelivered_count} undelivered message(s)"
            )
        if delivery_error is not None:
            raise ParkedPairIndexError(f"parked-pair index publish to {self._topic} failed: {delivery_error}")


def _topic_partitions(consumer, topic: str, timeout: float = 30.0) -> list[int]:
    """Return the topic's partition ids from broker metadata (mirrors
    ``dead_letter_replay._dlq_partitions``' shape, not imported from it —
    a small, generic Kafka pattern, not worth an inter-module dependency)."""
    metadata = consumer.list_topics(topic=topic, timeout=timeout)
    item = metadata.topics.get(topic) if metadata is not None else None
    if item is None or getattr(item, "error", None) is not None or not getattr(item, "partitions", None):
        error = getattr(item, "error", None) if item is not None else None
        raise ParkedPairIndexError(
            f"parked-pair index topic {topic} has no partition metadata"
            + (f": {error}" if error is not None else "")
        )
    return sorted(int(partition) for partition in item.partitions)


class ScopedParkedPairIndex:
    """One label pair's view of a multi-pair :class:`ParkedPairIndex` (v7-S7).

    Satisfies ``edge_execution.DurableParkedPairIndex``: the per-pair lease coordinator's live
    ``is_blocked(sk, tk)`` asks only about its own pair, and ``add`` refuses a record whose
    topic binds another pair (a routing bug, never data)."""

    def __init__(self, index: ParkedPairIndex, endpoint_pair: "tuple[str, str]") -> None:
        self._index = index
        self.endpoint_pair = tuple(endpoint_pair)

    def is_blocked(self, source_key: object, target_key: object) -> bool:
        return self._index.is_blocked(source_key, target_key, endpoint_pair=self.endpoint_pair)

    def add(
        self, source_key: object, target_key: object, *,
        original_topic: str, original_partition: int, original_offset: int, first_failure_at_ms: int,
    ) -> None:
        bound = self._index.pair_of(original_topic)
        if bound is None or tuple(bound) != self.endpoint_pair:
            raise ParkedPairIndexError(
                f"parked-pair index scoped to {self.endpoint_pair} was handed a record of topic "
                f"{original_topic!r}, which binds {bound}"
            )
        self._index.add(
            source_key, target_key, original_topic=original_topic, original_partition=original_partition,
            original_offset=original_offset, first_failure_at_ms=first_failure_at_ms,
        )


def index_leftovers(index: ParkedPairIndex, edge) -> "list[tuple[str, tuple[str, str] | None, int, str]]":
    """Outstanding entries of ``edge``'s parked-pair index that the type no longer binds (final follow-ups S2):
    ``(original topic, stamped label pair or None, records, reason)`` with reason

    * ``removed``: the topic is not one of ``edge.topics`` any more;
    * ``rebound``: the entry was written under a label pair (stamped by a multi-pair index) that is not the
      pair the topic binds now -- the topic moved to another pair, or the type collapsed back to one pair.

    The index does not stamp labels for single-pair types or on entries written before this change, so a
    rebound topic of those is not detected here. ``edge`` needs ``topics`` and ``pair_labels_for_topic``."""
    topics = set(edge.topics)
    found = []
    for (topic, labels), records in sorted(index.outstanding_by_topic().items(), key=lambda item: str(item[0])):
        if topic not in topics:
            found.append((topic, labels, records, "removed"))
        elif labels is not None and tuple(labels) != tuple(edge.pair_labels_for_topic(topic)):
            found.append((topic, labels, records, "rebound"))
    return found


def leftover_refuses_start(edge, leftover) -> bool:
    """Whether a leftover group makes ``start`` refuse (S2): its rows cannot be written through the type as it
    is now -- a removed topic of a multi-pair type (its rows park), or an entry stamped with a label pair other
    than the one its rows would now be retried as (a rebound topic, or a type collapsed back to one pair; the
    rows park and meanwhile block records of the same keys). A single-pair type's unstamped leftovers of a
    removed topic are retried through its one pair, so they only warn."""
    topic, labels, _records, reason = leftover
    if getattr(edge, "is_multi_pair", False) is True and reason == "removed":
        return True
    if labels is None:
        return False
    retried_as = edge.pair_labels_for_topic(topic) if reason == "rebound" else (
        (edge.nodes.source, edge.nodes.target) if not getattr(edge, "is_multi_pair", False) else None)
    return retried_as is None or tuple(labels) != tuple(retried_as)


def describe_index_leftovers(edge, leftovers) -> str:
    """One line per leftover group, with what will happen to its rows, for logs and the start preflight."""
    parts = []
    for leftover in leftovers:
        topic, labels, records, reason = leftover
        written = f" written as {labels[0]}->{labels[1]}" if labels else ""
        if reason == "removed":
            what = f"{records} record(s) of topic {topic}{written}, which {edge.type} no longer reads"
        else:
            now = edge.pair_labels_for_topic(topic)
            what = f"{records} record(s) of topic {topic}{written}, which now binds {now[0]}->{now[1]}"
        if not leftover_refuses_start(edge, leftover):
            pair = f"{edge.nodes.source}->{edge.nodes.target}"
            what += f" (their DLQ rows are retried through the type's one pair {pair})"
        elif reason == "removed" and getattr(edge, "is_multi_pair", False) is True:
            what += " (their DLQ rows park as invalid_on_retry after max_retry_attempts)"
        else:
            what += (" (their DLQ rows park as invalid_on_retry and until then block records of the same keys "
                     "of the pair their topic maps to now)")
        parts.append(what)
    return "; ".join(parts)


def warn_index_leftovers(index: ParkedPairIndex, edge, *, stage: str) -> list:
    """Log a WARNING when ``edge``'s freshly rebuilt index holds entries the type no longer binds (S2), naming
    for each group what happens to its rows (see :func:`describe_index_leftovers`)."""
    try:
        leftovers = index_leftovers(index, edge)
    except Exception:  # a diagnostic only: never fail a start on it
        logger.exception("stage=%s edge=%s reason=index_leftover_check_failed", stage, edge.type)
        return []
    if leftovers:
        logger.warning(
            "stage=%s edge=%s reason=index_holds_entries_the_type_no_longer_binds %s -- drain the DLQ before "
            "changing a type's topics or pairs", stage, edge.type, describe_index_leftovers(edge, leftovers),
        )
    return leftovers
