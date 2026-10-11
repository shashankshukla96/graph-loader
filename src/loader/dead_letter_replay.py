"""Read a DLQ topic and republish matching envelopes onto their original topic.

Malformed records are terminal (P5-Q2): this slice's DLQ routing never
auto-retries them. This module is the "separate replay tool" the owner
decision requires -- a human fixes the producer or the schema, then re-injects
the backlog with this command (wired to ``python -m src.cli replay-dlq`` in
``src/cli.py``) instead of a bespoke per-incident script.

Idempotency and ordering: every downstream write this library performs is
``MERGE``-based, so replaying an envelope whose underlying issue was already
fixed and independently re-published by the original producer is safe, and
running this command twice over the same DLQ range is equally safe
(at-least-once, matching P5-Q5's existing precedent for counters -- no
de-duplication bookkeeping is implemented here). ``MERGE`` makes a replay
idempotent, **not order-safe**: replaying an old envelope for a key that a
newer, valid record has since updated will overwrite that newer state back
to the old values (last write wins is topic order, and a replay from the
DLQ is not in that order relative to the source topic's own subsequent
traffic). Prefer ``--dry-run`` first, then a staging ``--target-topic``, or
replay only when you know the affected keys have not been legitimately
updated since the failure. A record that is still malformed after a replay
is DLQ'd again as a new envelope with a fresh ``first_failure_at_ms`` and
``attempt_count=0`` -- this affects any later ``--since`` window.

The DLQ topic's own contents and any consumer-group offsets on it are never
touched by a replay, dry-run or live: this module's reader is a dedicated,
non-group consumer that never commits, and reads only up to each partition's
high watermark captured once at start (the same "bulk boundary" pattern
``docs/USAGE.md`` section 6 already uses for the same reason -- a topic
still receiving new failures during the run must not turn into an unbounded
tail).
"""
from __future__ import annotations

from dataclasses import dataclass
import logging
import time
import uuid

from src.loader.dead_letter import (
    DEAD_LETTER_MAX_ENVELOPE_BYTES,
    DeadLetterEnvelope,
    DeadLetterEnvelopeError,
    decode_envelope,
)

logger = logging.getLogger(__name__)

# How long a scan may go without reading a single new message before it is
# treated as stalled (broker unreachable, or an offset gap that can never be
# filled -- see _iter_matching_envelopes). A fixed, generous ceiling rather
# than a multiple of the per-poll timeout, since operators may pass a short
# per-poll timeout for responsiveness without intending a short overall
# patience budget.
DEFAULT_STALL_TIMEOUT_SECONDS = 120.0


class DeadLetterReplayError(RuntimeError):
    """Raised when a DLQ replay cannot proceed or a live replay is not fully acknowledged."""


@dataclass(frozen=True)
class ReplayFilter:
    """Selection criteria applied to every envelope in the captured DLQ range."""

    reason_code: str | None = None
    since_ms: int | None = None
    until_ms: int | None = None
    limit: int | None = None
    entity_kind: str | None = None
    entity_name: str | None = None
    # v7-S4 ``--source-topic``: only envelopes whose ``original_topic`` is one of these.
    original_topics: frozenset[str] | None = None

    def matches(self, envelope: DeadLetterEnvelope) -> bool:
        """Return whether ``envelope`` passes every configured criterion."""
        if self.reason_code is not None and envelope.reason_code != self.reason_code:
            return False
        return self.matches_record(envelope)

    def matches_record(self, envelope: DeadLetterEnvelope) -> bool:
        """The criteria every DLQ copy of one record shares (all but ``reason_code``,
        which the retry consumer updates per attempt)."""
        if self.since_ms is not None and envelope.first_failure_at_ms < self.since_ms:
            return False
        if self.until_ms is not None and envelope.first_failure_at_ms > self.until_ms:
            return False
        if self.entity_kind is not None and envelope.entity_kind != self.entity_kind:
            return False
        if self.entity_name is not None and envelope.entity_name != self.entity_name:
            return False
        if self.original_topics is not None and envelope.original_topic not in self.original_topics:
            return False
        return True


@dataclass(frozen=True)
class ReplaySummary:
    """The outcome of one replay invocation, dry-run or live."""

    matched: int
    replayed: int
    dry_run: bool


def _dlq_partitions(consumer, topic: str, timeout: float) -> list[int]:
    """Return the DLQ topic's partition ids from broker metadata."""
    metadata = consumer.list_topics(topic=topic, timeout=timeout)
    item = metadata.topics.get(topic) if metadata is not None else None
    if item is None or getattr(item, "error", None) is not None or not getattr(item, "partitions", None):
        error = getattr(item, "error", None) if item is not None else None
        raise DeadLetterReplayError(
            f"DLQ topic {topic} has no partition metadata"
            + (f": {error}" if error is not None else "")
        )
    return sorted(int(partition) for partition in item.partitions)


def _partition_start_offsets(
    consumer, topic: str, partitions: list[int], *, since_ms: int | None, timeout: float,
):
    """Return each partition's start offset: the low watermark, or ``--since``'s resolved offset.

    ``offsets_for_times`` seeks by the broker record timestamp, not by the
    envelope's own ``first_failure_at_ms`` -- conservative, since a
    message's broker timestamp is always at least its envelope's
    ``first_failure_at_ms`` (the envelope is built, then published), so this
    can only include a few extra messages before ``since_ms`` for the
    ``ReplayFilter.matches`` time check to then exclude, never skip one that
    should match.
    """
    from confluent_kafka import TopicPartition

    if since_ms is None:
        return {
            partition: consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=timeout)[0]
            for partition in partitions
        }
    requested = [TopicPartition(topic, partition, since_ms) for partition in partitions]
    resolved = consumer.offsets_for_times(requested, timeout=timeout)
    starts: dict[int, int] = {}
    for item in resolved:
        if item.offset is not None and item.offset >= 0:
            starts[item.partition] = item.offset
        else:
            # No message at or after since_ms on this partition: nothing to
            # read there. Use the high watermark as the start so the read
            # range [start, high) is empty, not the low watermark (which
            # would incorrectly re-scan everything before since_ms).
            _, high = consumer.get_watermark_offsets(TopicPartition(topic, item.partition), timeout=timeout)
            starts[item.partition] = high
    return starts


def _capture_bounds(consumer, topic: str, *, since_ms: int | None, timeout: float) -> dict[int, tuple[int, int]]:
    """``{partition: (start, end)}`` for the scan, captured ONCE per replay (v7-S4 review S3).

    ``start`` is the low watermark, or ``--since``'s resolved offset; ``end`` is
    the high watermark at the moment of capture. Both the selection pass and a
    live pass read exactly these bounds, so records published to the DLQ topic
    in between (the retry consumer keeps re-DLQing) can never change what is
    replayed."""
    from confluent_kafka import TopicPartition

    partitions = _dlq_partitions(consumer, topic, timeout)
    if not partitions:
        return {}
    starts = _partition_start_offsets(consumer, topic, partitions, since_ms=since_ms, timeout=timeout)
    ends = {
        partition: consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=timeout)[1]
        for partition in partitions
    }
    return {partition: (starts[partition], ends[partition]) for partition in partitions}


def _scan(
    consumer, topic: str, bounds: dict[int, tuple[int, int]], *,
    timeout: float, stall_timeout: float = DEFAULT_STALL_TIMEOUT_SECONDS, clock=time.monotonic,
):
    """Yield ``(partition, offset, envelope)`` for every record in ``bounds``.

    Progress is tracked by each partition's next-expected offset (not a
    per-partition message *count*), so an offset gap (a transactional
    control marker from an unrelated producer, a ``DeleteRecords`` call, or
    a moved low watermark) still lets a partition's range complete once its
    position reaches its bound. A scan that goes ``stall_timeout`` seconds
    without reading a single new message -- broker unreachable mid-scan, or
    a gap that can genuinely never be filled -- raises
    :class:`DeadLetterReplayError` rather than looping forever.
    """
    from confluent_kafka import TopicPartition

    assignment = [
        TopicPartition(topic, partition, start) for partition, (start, end) in sorted(bounds.items()) if start < end
    ]
    if not assignment:
        return
    consumer.assign(assignment)
    ends = {partition: end for partition, (_start, end) in bounds.items()}
    positions = {item.partition: item.offset for item in assignment}
    remaining_partitions = set(positions)
    last_progress_at = clock()
    while remaining_partitions:
        message = consumer.poll(timeout)
        if message is None:
            if clock() - last_progress_at > stall_timeout:
                raise DeadLetterReplayError(
                    f"DLQ scan of {topic} made no progress for over {stall_timeout:.0f}s; "
                    f"partitions still incomplete: {sorted(remaining_partitions)}"
                )
            continue
        last_progress_at = clock()
        if message.error():
            raise DeadLetterReplayError(f"DLQ read error on {topic}: {message.error()}")
        partition = message.partition()
        if partition not in remaining_partitions:
            continue
        offset = message.offset()
        if offset < positions[partition]:
            continue  # already accounted for; defensive, should not happen
        if offset >= ends[partition]:
            # An offset gap right before the captured boundary can make the
            # broker's next delivered message land at or past the bound; that
            # message arrived after the capture and must not be read.
            remaining_partitions.discard(partition)
            continue
        positions[partition] = offset + 1
        if positions[partition] >= ends[partition]:
            remaining_partitions.discard(partition)
        try:
            envelope = decode_envelope(message.value())
        except DeadLetterEnvelopeError as exc:
            raise DeadLetterReplayError(
                f"unreadable envelope on {topic} partition={partition} offset={offset}: {exc}"
            ) from exc
        yield partition, offset, envelope


@dataclass(frozen=True)
class _Selected:
    partition: int
    offset: int
    reason_code: str
    error_text: str
    original_topic: str
    has_payload: bool


def _select(consumer, topic: str, replay_filter: ReplayFilter, bounds, *, timeout, stall_timeout, clock):
    """Selection pass (v7-S4 review S3, S-b): one entry per record identity
    ``(original_topic, original_partition, original_offset)``. The retry consumer
    re-publishes a record many times (a root once per attempt); replaying every copy
    would re-apply an old value after newer ones. A record is selected when its LATEST
    copy matches the filter (its current reason code), and it is replayed from its FIRST
    copy's position (the copies carry the same key and value), so the replay follows the
    order the records first reached the DLQ: a pair's root before its followers. Then
    ``limit``. Holds one small entry per distinct MATCHING record, never a payload."""
    # One small entry per MATCHING record (review nit): the copy-invariant criteria are checked on
    # every copy; only reason_code differs between copies and is checked on the latest one.
    first: dict[tuple, tuple[int, int, int, bool]] = {}  # identity -> (scan index, partition, offset, payload)
    latest: dict[tuple, tuple[tuple, str, str]] = {}  # identity -> (recency, reason_code, error_text)
    for scan_index, (partition, offset, envelope) in enumerate(
        _scan(consumer, topic, bounds, timeout=timeout, stall_timeout=stall_timeout, clock=clock)
    ):
        if not replay_filter.matches_record(envelope):
            continue
        identity = (envelope.original_topic, envelope.original_partition, envelope.original_offset)
        if identity not in first:
            first[identity] = (scan_index, partition, offset, envelope.value_bytes is not None)
        recency = (envelope.dlq_routed_at_ms, partition, offset)
        current = latest.get(identity)
        if current is None or recency > current[0]:
            latest[identity] = (recency, envelope.reason_code, envelope.error_text[:200])
    selected = []
    for identity, (_scan_index, partition, offset, has_payload) in sorted(first.items(), key=lambda item: item[1][0]):
        _recency, reason_code, error_text = latest[identity]
        if replay_filter.reason_code is not None and reason_code != replay_filter.reason_code:
            continue
        selected.append(_Selected(
            partition=partition, offset=offset, reason_code=reason_code, error_text=error_text,
            original_topic=identity[0], has_payload=has_payload,
        ))
    if replay_filter.limit is not None:
        selected = selected[:replay_filter.limit]
    return selected


def _synchronous_produce(producer, topic: str, key: bytes | None, value: bytes) -> None:
    """Produce and block until Kafka acknowledges; raise on any failure.

    Mirrors ``DeadLetterProducer.publish``'s produce-then-flush-then-check
    shape exactly, reused here rather than reimplemented.
    """
    delivery_error: Exception | None = None

    def on_delivery(error, _message) -> None:
        nonlocal delivery_error
        if error is not None:
            delivery_error = error

    try:
        producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
        undelivered_count = producer.flush()
    except Exception as exc:
        raise DeadLetterReplayError(f"replay publish to {topic} failed: {exc}") from exc
    if undelivered_count:
        raise DeadLetterReplayError(f"replay publish to {topic} has {undelivered_count} undelivered message(s)")
    if delivery_error is not None:
        raise DeadLetterReplayError(f"replay publish to {topic} failed: {delivery_error}")


def _require_target_topic(consumer, topic: str, timeout: float) -> None:
    """Fail closed before any publish if the broker does not know ``topic``."""
    try:
        metadata = consumer.list_topics(topic=topic, timeout=timeout)
    except Exception as exc:
        raise DeadLetterReplayError(f"could not read metadata of replay target topic {topic}: {exc}") from exc
    item = metadata.topics.get(topic) if metadata is not None else None
    if item is None or getattr(item, "error", None) is not None or not getattr(item, "partitions", None):
        error = getattr(item, "error", None) if item is not None else None
        raise DeadLetterReplayError(
            f"replay target topic {topic} does not exist" + (f": {error}" if error is not None else "")
        )


def run_replay(
    *,
    dlq_topic: str,
    target_topic: str | None,
    bootstrap_servers: str,
    replay_filter: ReplayFilter,
    dry_run: bool,
    consumer_factory=None,
    producer_factory=None,
    timeout: float = 30.0,
    stall_timeout: float = DEFAULT_STALL_TIMEOUT_SECONDS,
    original_topic_targets: "frozenset[str] | set[str] | list[str] | None" = None,
    clock=time.monotonic,
) -> ReplaySummary:
    """Read ``dlq_topic``, filter, and (unless ``dry_run``) republish to ``target_topic``.

    Two reads of one captured range (v7-S4 review S3): the end offsets are
    captured once; a selection pass picks the matching envelopes -- one per
    record identity, its latest DLQ copy, then ``limit`` -- and validates them;
    a live run then re-reads the same bounds and republishes exactly those
    positions, in scan order. Records published to the DLQ in between are
    never read. ``matched`` counts distinct records.

    ``target_topic=None`` (v7-S4, a relationship type with several topics):
    each envelope is republished to its own ``original_topic``, which must be
    one of ``original_topic_targets`` (the type's topics). An envelope naming
    any other topic aborts the run before anything is published (a dry run
    reports the same error), and each target topic must exist.

    Never holds an envelope in memory beyond the one being published, and
    never commits any offset on ``dlq_topic``. Fails closed on the batch: the
    first unacknowledged live replay stops immediately and raises
    :class:`DeadLetterReplayError` -- every envelope replayed successfully
    before the failure was already logged (``replayed=N`` on each line).
    """
    if target_topic is None and not original_topic_targets:
        raise DeadLetterReplayError("no target topic: give target_topic or original_topic_targets")
    allowed = frozenset(original_topic_targets or ())
    if consumer_factory is None:
        from confluent_kafka import Consumer as consumer_factory

    consumer = consumer_factory({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"graph-loader-dlq-replay-{uuid.uuid4().hex}",
        "enable.auto.commit": False,
    })
    producer = None
    replayed = 0
    selected: list[_Selected] = []
    try:
        bounds = _capture_bounds(consumer, dlq_topic, since_ms=replay_filter.since_ms, timeout=timeout)
        selected = _select(
            consumer, dlq_topic, replay_filter, bounds, timeout=timeout, stall_timeout=stall_timeout, clock=clock,
        )
        destinations: dict[tuple[int, int], str] = {}
        for item in selected:
            if not item.has_payload:
                continue  # skipped below, needs no target
            if target_topic is not None:
                destinations[(item.partition, item.offset)] = target_topic
            elif item.original_topic in allowed:
                destinations[(item.partition, item.offset)] = item.original_topic
            else:
                raise DeadLetterReplayError(
                    f"envelope at {dlq_topic} partition={item.partition} offset={item.offset} has "
                    f"original_topic={item.original_topic!r}, which is not one of the type's topics "
                    f"{sorted(allowed)} (0 envelope(s) replayed); replay it with --target-topic, or leave it "
                    "out with --source-topic"
                )
        if dry_run:
            for item in selected:
                logger.info(
                    "stage=replay-dlq dry_run=true topic=%s partition=%s offset=%s reason_code=%s error_text=%s"
                    + ("" if target_topic is not None else " target_topic=%s"),
                    dlq_topic, item.partition, item.offset, item.reason_code, item.error_text,
                    *(() if target_topic is not None else (destinations.get((item.partition, item.offset)),)),
                )
        else:
            if target_topic is None:
                for target in sorted(set(destinations.values())):
                    _require_target_topic(consumer, target, timeout)
            wanted = {(item.partition, item.offset) for item in selected}
            if wanted:
                if producer_factory is None:
                    from confluent_kafka import Producer as producer_factory
                producer = producer_factory({
                    "bootstrap.servers": bootstrap_servers,
                    "acks": "all",
                    "enable.idempotence": True,
                    # A mistyped --target-topic must fail loudly, not silently
                    # auto-create a throwaway topic and "succeed" into it.
                    "allow.auto.create.topics": False,
                    # librdkafka's own default (1,000,000 bytes) sits just under
                    # the source broker's common 1,048,588-byte default, so a
                    # legitimately large original value could otherwise fail to
                    # replay even though it was accepted by the source topic.
                    "message.max.bytes": DEAD_LETTER_MAX_ENVELOPE_BYTES + 65536,
                })
                # The same bounds, cut at the last selected position per partition
                # (the limit's stop positions).
                live_bounds = {}
                for partition, (start, _end) in bounds.items():
                    last = max((o for p, o in wanted if p == partition), default=None)
                    if last is not None:
                        live_bounds[partition] = (start, last + 1)
                for partition, offset, envelope in _scan(
                    consumer, dlq_topic, live_bounds, timeout=timeout, stall_timeout=stall_timeout, clock=clock,
                ):
                    if (partition, offset) not in wanted:
                        continue
                    if envelope.value_bytes is None:
                        logger.warning(
                            "stage=replay-dlq skipping topic=%s partition=%s offset=%s: envelope has no "
                            "payload (reason_code=%s) -- replaying it would only produce an empty message",
                            dlq_topic, partition, offset, envelope.reason_code,
                        )
                        continue
                    destination = destinations[(partition, offset)]
                    _synchronous_produce(producer, destination, envelope.key_bytes, envelope.value_bytes)
                    replayed += 1
                    logger.info(
                        "stage=replay-dlq dry_run=false topic=%s partition=%s offset=%s reason_code=%s "
                        "target_topic=%s replayed=%s",
                        dlq_topic, partition, offset, envelope.reason_code, destination, replayed,
                    )
    except DeadLetterReplayError:
        if not dry_run:
            logger.error(
                "stage=replay-dlq replay failed after %s matching envelope(s) replayed; stopping",
                replayed,
            )
        raise
    finally:
        consumer.close()
        if producer is not None:
            producer.flush()
    logger.info(
        "stage=replay-dlq dry_run=%s matched=%s replayed=%s target_topic=%s",
        dry_run, len(selected), replayed,
        target_topic if target_topic is not None else "<original_topic of each envelope>",
    )
    return ReplaySummary(matched=len(selected), replayed=replayed, dry_run=dry_run)
