"""Unit tests for src/loader/retry_consumer.py, against fake Kafka
consumer/writer/sink/parked-pair-index doubles -- no real Kafka or Neo4j,
and no edge_execution.py hook (P5-S3's row-level DLQ writer hasn't merged
into this branch yet; Phase B wires a real RetryWriter adapter around it).
"""
from __future__ import annotations

from contextlib import ExitStack
import json
import os
from pathlib import Path
import threading
from unittest.mock import MagicMock, patch

from neo4j.exceptions import ClientError, ConnectionAcquisitionTimeoutError, ServiceUnavailable, TransientError
import pytest

from src.loader.dead_letter import (
    SCHEMA_VERSION,
    DeadLetterEnvelope,
    DeadLetterRecordSink,
    LoaderIdentity,
    encode_envelope,
)
from src.loader.parked_pair_index import (
    ParkedPairIndex,
    _OutstandingRecord,
    _pair_key,
    _record_wire_key,
)
from src.loader.retry_consumer import (
    RETRY_OUTCOME_UNRESOLVED,
    RETRY_OUTCOME_WRITTEN,
    KafkaBacklogSource,
    LeaseCoordinatedRetryWriter,
    RetryCoordinatorBridge,
    RetryConsumer,
    RetryConsumerError,
    _revoked_topic_partitions,
    compute_dlq_backlog,
    main as retry_consumer_main,
)


def _neo_error(code: str, message: str = "boom") -> ClientError:
    return ClientError._hydrate_neo4j(code=code, message=message)


def _row_error(message: str = "bad row") -> ClientError:
    """An allow-listed, row-specific Neo4j error."""
    return _neo_error("Neo.ClientError.Statement.TypeError", message)


# ── Envelope/message construction helpers ───────────────────────────────────


def _envelope(
    *, entity_name="KNOWS", source_key="p1", target_key="p2",
    original_topic="knows-events", original_partition=0, original_offset=0,
    attempt_count=0, retry_after_ms=1_000, failure_class="missing_endpoint",
) -> DeadLetterEnvelope:
    return DeadLetterEnvelope(
        schema_version=SCHEMA_VERSION,
        failure_class=failure_class,
        entity_kind="edge",
        entity_name=entity_name,
        original_topic=original_topic,
        original_partition=original_partition,
        original_offset=original_offset,
        key_bytes=json.dumps([source_key, target_key], separators=(",", ":")).encode("utf-8"),
        value_bytes=b"{}",
        reason_code="missing_target",
        error_text="target not found",
        loader_identity=LoaderIdentity(run_id="run-1", replica_id="0", kind="edge"),
        first_failure_at_ms=1_000,
        attempt_count=attempt_count,
        dlq_routed_at_ms=1_000,
        retry_after_ms=None if failure_class == "malformed" else retry_after_ms,
    )


class _FakeDlqMessage:
    def __init__(self, topic: str, partition: int, offset: int, envelope: DeadLetterEnvelope | None, raw_value=None):
        self._topic, self._partition, self._offset = topic, partition, offset
        self._value = raw_value if raw_value is not None else encode_envelope(envelope)

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def value(self):
        return self._value

    def error(self):
        return None


def _dlq_message(envelope: DeadLetterEnvelope, *, partition: int | None = None, offset: int | None = None) -> _FakeDlqMessage:
    return _FakeDlqMessage(
        f"{envelope.entity_name.lower()}.dlq",
        envelope.original_partition if partition is None else partition,
        envelope.original_offset if offset is None else offset,
        envelope,
    )


# ── Fakes ────────────────────────────────────────────────────────────────


class _FakeConsumer:
    def __init__(self) -> None:
        self._queue: list = []
        self.poll_calls = 0
        self.poll_timeouts: list = []
        self.paused_now: set = set()
        self.paused: list = []
        self.resumed: list = []
        self.committed: list = []

    def push(self, message) -> None:
        self._queue.append(message)

    def poll(self, timeout=None):
        self.poll_calls += 1
        self.poll_timeouts.append(timeout)
        # Like librdkafka, never deliver from a paused partition.
        for position, message in enumerate(self._queue):
            if (message.topic(), message.partition()) not in self.paused_now:
                return self._queue.pop(position)
        return None

    def pause(self, partitions) -> None:
        self.paused.append(list(partitions))
        self.paused_now.update((tp.topic, tp.partition) for tp in partitions)

    def resume(self, partitions) -> None:
        self.resumed.append(list(partitions))
        self.paused_now.difference_update((tp.topic, tp.partition) for tp in partitions)

    def commit(self, *, message=None, offsets=None, asynchronous=False) -> None:
        self.committed.append({"message": message, "offsets": offsets})


class _FakeWriter:
    """Returns one queued outcome dict per retry_batch() call, in order."""

    def __init__(self, outcomes_sequence: list[dict[int, tuple[str, str]]] | None = None, raises: bool = False) -> None:
        self.calls: list[list[DeadLetterEnvelope]] = []
        self._outcomes_sequence = outcomes_sequence or []
        self._raises = raises

    def retry_batch(self, envelopes, budget_seconds=None):
        self.calls.append(list(envelopes))
        if self._raises:
            raise RuntimeError("simulated Neo4j/lease failure")
        return self._outcomes_sequence.pop(0)


class _FakeSink:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def reroute_or_park(self, **kwargs):
        self.calls.append(kwargs)
        return "parked" if kwargs.get("parked_topic") else "re_dlq"

    def reroute_or_park_many(self, requests):
        self.many_calls = getattr(self, "many_calls", 0) + 1
        results = []
        for request in requests:
            try:
                results.append(self.reroute_or_park(**request))
            except Exception as exc:
                results.append(exc)
        return results


class _FakeIndex:
    """``caught_up`` (a fixed bool, the default) short-circuits the
    position comparison for the simple tests. Passing ``caught_up=None``
    switches to a realistic position-based comparison against
    ``watermark_value``/``poller_position`` (both mutable after
    construction), used by the M8 "frozen snapshot" tests below.

    ``contains`` defaults to "every record is present" (``None`` ->
    always ``True``), matching every pre-M12 test's assumption that the
    record being retried is genuinely indexed; pass ``missing_ids`` (a
    set of ``(original_topic, original_partition, original_offset)``) to
    exercise the M12 duplicate-skip path for specific records.
    """

    def __init__(
        self, *, blocked_ids=None, caught_up: bool | None = True, watermark_raises: bool = False,
        watermark_value: int = 1, poller_position: int = 0, missing_ids=None,
    ) -> None:
        self._blocked_ids = set(blocked_ids or [])
        self._missing_ids = set(missing_ids or [])
        self._caught_up_override = caught_up
        self._watermark_raises = watermark_raises
        self.watermark_value = watermark_value
        self.poller_position = poller_position
        self.removed: list[tuple] = []
        self.watermark_snapshot_calls = 0
        self.caught_up_calls = 0
        self.contains_calls: list[tuple] = []

    def watermark_snapshot(self):
        self.watermark_snapshot_calls += 1
        if self._watermark_raises:
            raise RuntimeError("simulated broker timeout")
        return {0: self.watermark_value}

    def caught_up(self, snapshot) -> bool:
        self.caught_up_calls += 1
        if self._caught_up_override is not None:
            return self._caught_up_override
        return self.poller_position >= snapshot.get(0, 0)

    def contains(self, source_key, target_key, *, original_topic, original_partition, original_offset) -> bool:
        identity = (original_topic, original_partition, original_offset)
        self.contains_calls.append(identity)
        return identity not in self._missing_ids

    def is_blocked_by_earlier(self, source_key, target_key, *, original_topic, original_partition, original_offset, first_failure_at_ms) -> bool:
        return (original_topic, original_partition, original_offset) in self._blocked_ids

    def remove(self, source_key, target_key, *, original_topic, original_partition, original_offset) -> None:
        self.removed.append((original_topic, original_partition, original_offset))

    def remove_many(self, records):
        self.remove_many_calls = getattr(self, "remove_many_calls", 0) + 1
        for source_key, target_key, topic, partition, offset in records:
            self.remove(source_key, target_key, original_topic=topic, original_partition=partition, original_offset=offset)
        return [None] * len(records)


class _Clock:
    """A controllable monotonic/wall clock: advances only when told to,
    never a real sleep."""

    def __init__(self, start: float = 0.0) -> None:
        self._now = start

    def advance(self, delta: float) -> None:
        self._now += delta

    def __call__(self) -> float:
        return self._now


def _make_consumer(
    *, consumer=None, writer=None, sink=None, index=None,
    max_retry_attempts=3, parked_topic="knows.dlq.parked",
    base_delay_seconds=1.0, max_delay_seconds=60.0,
    blocked_retry_delay_seconds=0.5, due_batch_coalesce_seconds=0.0,
    wall_clock=None, monotonic_clock=None, on_row_retried=None,
    entity_name="KNOWS", max_records_per_partition=1000, max_batch_size=2000,
) -> tuple[RetryConsumer, dict]:
    consumer = consumer or _FakeConsumer()
    writer = writer or _FakeWriter()
    sink = sink or _FakeSink()
    fakes = {"consumer": consumer, "writer": writer, "sink": sink, "index": index}
    retry_consumer = RetryConsumer(
        consumer=consumer,
        writer_by_edge_type={entity_name: writer},
        sink_by_edge_type={entity_name: sink},
        parked_pair_index_by_edge_type={entity_name: index} if index is not None else {},
        max_retry_attempts_by_edge_type={entity_name: max_retry_attempts},
        parked_topic_by_edge_type={entity_name: parked_topic},
        base_delay_seconds_by_edge_type={entity_name: base_delay_seconds},
        max_delay_seconds_by_edge_type={entity_name: max_delay_seconds},
        blocked_retry_delay_seconds=blocked_retry_delay_seconds,
        due_batch_coalesce_seconds=due_batch_coalesce_seconds,
        max_records_per_partition=max_records_per_partition, max_batch_size=max_batch_size,
        wall_clock=wall_clock or (lambda: 10_000),
        monotonic_clock=monotonic_clock or (lambda: 0.0),
        on_row_retried=on_row_retried,
    )
    return retry_consumer, fakes


# ── Pause/resume scheduling: never blocks poll() ────────────────────────────


def test_not_yet_due_record_is_held_and_never_marked_due() -> None:
    wall = _Clock(start=10_000)
    envelope = _envelope(retry_after_ms=20_000)  # in the future
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(envelope))
    rc, fakes = _make_consumer(consumer=consumer, wall_clock=wall)
    rc.tick()
    assert consumer.paused == []  # a held record alone never pauses its partition
    assert len(rc._held) == 1
    assert consumer.committed == []
    assert fakes["writer"].calls == []
    assert not rc._due_this_tick  # not due yet


def test_at_most_one_blocking_poll_per_tick_regardless_of_pending_state() -> None:
    wall = _Clock(start=10_000)
    envelope = _envelope(retry_after_ms=999_999)  # far future, stays paused
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(envelope))
    rc, _fakes = _make_consumer(consumer=consumer, wall_clock=wall)
    for _ in range(5):
        rc.tick()
    # One blocking poll per tick, unaffected by how many records are
    # paused/waiting; the only other polls are the non-blocking drain after
    # a message arrived.
    assert len([t for t in consumer.poll_timeouts if t]) == 5


def test_wake_heap_resumes_a_paused_record_once_its_retry_after_elapses() -> None:
    wall = _Clock(start=10_000)
    envelope = _envelope(retry_after_ms=10_500)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(envelope))
    outcomes = [{0: (RETRY_OUTCOME_WRITTEN, "")}]
    writer = _FakeWriter(outcomes_sequence=outcomes)
    rc, fakes = _make_consumer(consumer=consumer, writer=writer, wall_clock=wall)
    rc.tick()  # observes the record, not yet due -- paused
    assert not rc._due_this_tick
    wall.advance(600)  # now = 10_600, past retry_after_ms
    rc.tick()  # resumes it into _due_this_tick and flushes (coalesce=0)
    assert writer.calls == [[envelope]]


# ── Malformed / unreadable envelopes ────────────────────────────────────────


def test_malformed_envelope_committed_past_immediately_never_paused() -> None:
    envelope = _envelope(failure_class="malformed", retry_after_ms=1_000)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(envelope))
    rc, fakes = _make_consumer(consumer=consumer)
    rc.tick()
    assert consumer.paused == []
    assert len(consumer.committed) == 1
    assert fakes["writer"].calls == []


def test_unreadable_envelope_committed_past_as_poison_pill() -> None:
    consumer = _FakeConsumer()
    consumer.push(_FakeDlqMessage("knows.dlq", 0, 3, envelope=None, raw_value=b"not a valid envelope"))
    rc, fakes = _make_consumer(consumer=consumer)
    rc.tick()
    assert consumer.paused == []
    assert len(consumer.committed) == 1
    assert fakes["writer"].calls == []


# ── Retry-or-park write path outcomes ────────────────────────────────────────


def test_due_record_written_commits_resumes_and_notifies() -> None:
    envelope = _envelope(retry_after_ms=1_000, original_offset=7)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(envelope))
    writer = _FakeWriter(outcomes_sequence=[{0: (RETRY_OUTCOME_WRITTEN, "")}])
    notified = []
    rc, fakes = _make_consumer(consumer=consumer, writer=writer, on_row_retried=lambda t, o: notified.append((t, o)))
    rc.tick()
    assert writer.calls == [[envelope]]
    assert len(consumer.committed) == 1
    assert consumer.committed[0]["offsets"][0].offset == 8  # original_offset + 1
    assert consumer.paused == [] and consumer.resumed == []  # never held back: no pause/resume churn
    assert notified == [("KNOWS", "written")]


def test_unresolved_record_below_max_attempts_re_dlqd_with_exponential_backoff() -> None:
    envelope = _envelope(retry_after_ms=1_000, attempt_count=1)  # next_attempt=2
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(envelope))
    writer = _FakeWriter(outcomes_sequence=[{0: (RETRY_OUTCOME_UNRESOLVED, "missing_target")}])
    rc, fakes = _make_consumer(
        consumer=consumer, writer=writer, max_retry_attempts=5, base_delay_seconds=1.0, max_delay_seconds=60.0,
    )
    rc.tick()
    sink_call = fakes["sink"].calls[0]
    assert sink_call["parked_topic"] is None  # next_attempt=2 < max_retry_attempts=5
    assert sink_call["retry_after_seconds_for_reattempt"] == pytest.approx(2.0)  # base*2**(2-1)
    assert sink_call.get("increment_attempt", True) is not False  # default True: a real resolution failure
    assert len(consumer.committed) == 1  # offset still commits -- the re-DLQ publish is the durable resolution


def test_unresolved_record_reaching_max_attempts_is_parked_and_index_entry_removed() -> None:
    envelope = _envelope(retry_after_ms=1_000, attempt_count=2, original_topic="knows-events", original_partition=0, original_offset=9)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(envelope))
    writer = _FakeWriter(outcomes_sequence=[{0: (RETRY_OUTCOME_UNRESOLVED, "missing_target")}])
    index = _FakeIndex()
    notified = []
    rc, fakes = _make_consumer(
        consumer=consumer, writer=writer, index=index, max_retry_attempts=3,
        on_row_retried=lambda t, o: notified.append((t, o)),
    )
    rc.tick()  # next_attempt = 3 >= max_retry_attempts = 3 -> park
    sink_call = fakes["sink"].calls[0]
    assert sink_call["parked_topic"] == "knows.dlq.parked"
    assert index.removed == [("knows-events", 0, 9)]
    assert notified == [("KNOWS", "parked")]
    assert len(consumer.committed) == 1


def test_blocked_record_re_dlqd_at_unchanged_attempt_with_max_delay_and_writer_never_called() -> None:
    envelope = _envelope(retry_after_ms=1_000, attempt_count=1, original_topic="knows-events", original_partition=0, original_offset=5)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(envelope))
    writer = _FakeWriter()  # no outcomes queued -- must never be called
    index = _FakeIndex(blocked_ids={("knows-events", 0, 5)})
    notified = []
    rc, fakes = _make_consumer(
        consumer=consumer, writer=writer, index=index, blocked_retry_delay_seconds=0.5,
        on_row_retried=lambda t, o: notified.append((t, o)),
    )
    rc.tick()
    assert writer.calls == []  # never attempted -- blocked before reaching the writer
    sink_call = fakes["sink"].calls[0]
    assert sink_call["parked_topic"] is None  # a blocked row is never parked purely for being blocked
    assert sink_call["increment_attempt"] is False  # attempt_count must stay unchanged
    # v7-S4 M1: the blocker is outstanding elsewhere and its wait unknown -> the type's max
    # delay, never the 0.5 s blocked delay that re-published every follower twice a second
    assert sink_call["retry_after_seconds_for_reattempt"] == 60.0
    assert index.removed == []  # still outstanding -- not resolved
    assert notified == [("KNOWS", "blocked")]
    assert len(consumer.committed) == 1  # this DLQ record's own offset still commits


# ── Offset-commit discipline under failure ──────────────────────────────────


def test_writer_failure_commits_nothing_and_the_identical_batch_succeeds_next_tick() -> None:
    wall = _Clock(start=10_000)
    envelope = _envelope(retry_after_ms=1_000)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(envelope))
    failing_writer = _FakeWriter(raises=True)
    rc, fakes = _make_consumer(consumer=consumer, writer=failing_writer, wall_clock=wall)
    rc.tick()
    assert consumer.committed == []
    assert consumer.resumed == []
    assert rc._held  # the record was NOT silently dropped -- still scheduled, backed off

    wall.advance(1_000)  # past the edge type's first backoff

    # Swap in a working writer (simulating the underlying failure clearing)
    # and let the next tick's flush pick the identical batch back up.
    fakes["consumer"]._queue.clear()
    rc._writer_by_edge_type["KNOWS"] = _FakeWriter(outcomes_sequence=[{0: (RETRY_OUTCOME_WRITTEN, "")}])
    rc.tick()
    assert len(consumer.committed) == 1
    assert not rc._due_this_tick


# ── Non-blocking read barrier (M5) ──────────────────────────────────────────


def test_batch_stays_paused_when_index_not_caught_up_then_proceeds_once_caught_up() -> None:
    envelope = _envelope(retry_after_ms=1_000)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(envelope))
    writer = _FakeWriter(outcomes_sequence=[{0: (RETRY_OUTCOME_WRITTEN, "")}])
    index = _FakeIndex(caught_up=False)
    rc, fakes = _make_consumer(consumer=consumer, writer=writer, index=index)
    rc.tick()
    assert writer.calls == []  # held -- never even attempted the write
    assert consumer.committed == []
    assert rc._pending_batches  # stayed pending (frozen batch), not lost

    index._caught_up_override = True  # simulate the poller catching up
    rc.tick()
    assert writer.calls == [[envelope]]
    assert len(consumer.committed) == 1


def test_watermark_snapshot_failure_is_treated_as_not_caught_up() -> None:
    envelope = _envelope(retry_after_ms=1_000)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(envelope))
    writer = _FakeWriter(outcomes_sequence=[{0: (RETRY_OUTCOME_WRITTEN, "")}])
    index = _FakeIndex(watermark_raises=True)
    rc, fakes = _make_consumer(consumer=consumer, writer=writer, index=index)
    rc.tick()
    assert writer.calls == []
    assert consumer.committed == []
    # The batch was never even frozen (watermark_snapshot() raised at
    # freeze time) -- it stays in _due_this_tick, retried on a later tick.
    assert rc._due_this_tick
    assert not rc._pending_batches


# ── Frozen per-batch snapshot: not re-taken, doesn't starve (review M8) ─────


def test_frozen_batch_snapshot_is_taken_once_and_never_re_taken_while_waiting() -> None:
    envelope = _envelope(retry_after_ms=1_000)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(envelope))
    writer = _FakeWriter(outcomes_sequence=[{0: (RETRY_OUTCOME_WRITTEN, "")}])
    index = _FakeIndex(caught_up=None, watermark_value=5, poller_position=0)
    rc, fakes = _make_consumer(consumer=consumer, writer=writer, index=index)

    rc.tick()  # freezes the batch; snapshot={0: 5} taken exactly once
    assert index.watermark_snapshot_calls == 1
    assert writer.calls == []  # poller_position(0) < snapshot(5) -- not caught up yet

    # Steady live-writer traffic keeps advancing the index's real
    # watermark -- this must NOT move the already-frozen batch's target,
    # and must NOT cost another watermark lookup while merely waiting.
    index.watermark_value = 50
    for _ in range(3):
        rc.tick()
    assert index.watermark_snapshot_calls == 1  # never re-taken
    assert writer.calls == []  # still waiting on the ORIGINAL snapshot (5), not 50

    # The poller catches up to the ORIGINAL frozen snapshot (5) -- the
    # batch flushes even though the index's "live" watermark moved on.
    index.poller_position = 5
    rc.tick()
    assert writer.calls == [[envelope]]


def test_a_record_due_after_freeze_joins_the_next_batch_not_the_frozen_one() -> None:
    envelope1 = _envelope(original_partition=0, original_offset=1, retry_after_ms=1_000)
    envelope2 = _envelope(original_partition=1, original_offset=1, retry_after_ms=1_000)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(envelope1, partition=0, offset=0))
    writer = _FakeWriter(outcomes_sequence=[
        {0: (RETRY_OUTCOME_WRITTEN, "")}, {0: (RETRY_OUTCOME_WRITTEN, "")},
    ])
    index = _FakeIndex(caught_up=None, watermark_value=1, poller_position=0)
    rc, fakes = _make_consumer(consumer=consumer, writer=writer, index=index)

    rc.tick()  # freezes batch #1 (envelope1 alone); snapshot={0: 1}; not caught up
    assert writer.calls == []
    assert "KNOWS" in rc._pending_batches
    assert len(rc._pending_batches["KNOWS"].keyed_pendings) == 1

    # A second record for the SAME edge type (a different source
    # partition, so it doesn't collide with envelope1's still-paused one)
    # becomes due while batch #1 is still frozen and waiting.
    consumer.push(_dlq_message(envelope2, partition=1, offset=0))
    rc.tick()
    assert writer.calls == []  # batch #1 still not caught up
    assert len(rc._pending_batches["KNOWS"].keyed_pendings) == 1  # still just envelope1
    assert rc._due_this_tick  # envelope2 waits here, not retroactively in batch #1

    # Batch #1's snapshot is satisfied; envelope2 gets its OWN, later
    # snapshot once it is frozen -- proven by bumping the watermark
    # in between, so envelope2's batch is provably not yet caught up.
    index.poller_position = 1
    index.watermark_value = 2
    rc.tick()
    assert writer.calls == [[envelope1]]  # only batch #1 flushed
    assert "KNOWS" in rc._pending_batches  # envelope2's own fresh batch
    assert rc._pending_batches["KNOWS"].snapshot == {0: 2}


# ── Per-record commit discipline under a partial batch failure (M9) ────────


def _held_wake_ms(rc, offset: int, partition: int = 0) -> int | None:
    """v7-S4 M1: the wake time of a record held locally (not re-published), or None."""
    wakes = [wake for wake, _t, p, o in rc._wake_heap if o == offset and p == partition and (_t, p, o) in rc._held]
    return max(wakes) if wakes else None


def _committed_positions(consumer) -> list[list[tuple[str, int, int]]]:
    """Each commit() call as a list of (topic, partition, committed offset)."""
    return [
        [(tp.topic, tp.partition, tp.offset) for tp in call["offsets"]]
        for call in consumer.committed
    ]


class _FailingOnSecondCallSink(_FakeSink):
    def reroute_or_park(self, **kwargs):
        self.calls.append(kwargs)
        if len(self.calls) == 2:
            raise RuntimeError("simulated publish failure")
        return "parked" if kwargs.get("parked_topic") else "re_dlq"


def test_second_records_publish_failure_commits_the_first_once_and_never_replays_it() -> None:
    envelope1 = _envelope(original_partition=0, original_offset=1, retry_after_ms=1_000)
    envelope2 = _envelope(source_key="p9", original_partition=1, original_offset=1, retry_after_ms=1_000)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(envelope1, partition=0, offset=0))
    consumer.push(_dlq_message(envelope2, partition=1, offset=0))
    writer = _FakeWriter(outcomes_sequence=[
        {0: (RETRY_OUTCOME_UNRESOLVED, "missing_target"), 1: (RETRY_OUTCOME_UNRESOLVED, "missing_target")},
        {0: (RETRY_OUTCOME_UNRESOLVED, "missing_target")},  # envelope2's own retry, next batch
    ])
    sink = _FailingOnSecondCallSink()
    rc, fakes = _make_consumer(consumer=consumer, writer=writer, sink=sink)

    # Both records fetched in the same tick, so they land in one batch.
    rc._resume_due_partitions()
    for _ in range(2):
        rc._on_message(consumer.poll(0.1))
    rc._maybe_flush_due_batch()
    rc._commit_resolved()

    assert len(sink.calls) == 2  # both attempted; the second raised
    assert _committed_positions(consumer) == [[("knows.dlq", 0, 1)]]  # ONLY the first record's partition
    assert rc._due_this_tick  # the second record alone stays due for its own next batch

    # Next tick retries only the second record.
    rc.tick()
    assert len(sink.calls) == 3  # the first record's sink call is never repeated
    assert _committed_positions(consumer)[-1] == [("knows.dlq", 1, 1)]


# ── Poison-pill validation at read time (review M10) ────────────────────────


def test_poison_pill_bad_key_bytes_is_committed_past_without_crashing() -> None:
    import dataclasses

    envelope = _envelope(retry_after_ms=1_000)
    bad_envelope = dataclasses.replace(envelope, key_bytes=b"not valid json")
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(bad_envelope))
    rc, fakes = _make_consumer(consumer=consumer)
    rc.tick()  # must not raise
    assert len(consumer.committed) == 1
    assert consumer.paused == []
    assert fakes["writer"].calls == []


def test_poison_pill_unconfigured_edge_type_is_committed_past_without_crashing() -> None:
    envelope = _envelope(entity_name="UNCONFIGURED_TYPE", retry_after_ms=1_000)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(envelope))
    rc, fakes = _make_consumer(consumer=consumer)  # only "KNOWS" is configured
    rc.tick()  # must not raise
    assert len(consumer.committed) == 1
    assert consumer.paused == []


def test_poison_pill_non_edge_entity_kind_is_committed_past_without_crashing() -> None:
    import dataclasses

    envelope = _envelope(retry_after_ms=1_000)
    node_identity = dataclasses.replace(envelope.loader_identity, kind="node")
    node_shaped_envelope = dataclasses.replace(envelope, entity_kind="node", loader_identity=node_identity)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(node_shaped_envelope))
    rc, fakes = _make_consumer(consumer=consumer)
    rc.tick()  # must not raise
    assert len(consumer.committed) == 1
    assert consumer.paused == []


# ── Rebalance ────────────────────────────────────────────────────────────────


def test_revoke_drops_held_state_without_committing() -> None:
    envelope = _envelope(retry_after_ms=999_999)  # paused, far future
    consumer = _FakeConsumer()
    message = _dlq_message(envelope)
    consumer.push(message)
    rc, _fakes = _make_consumer(consumer=consumer)
    rc.tick()
    dlq_partition_key = (message.topic(), message.partition())
    assert len(rc._held) == 1

    rc.on_partitions_revoked([dlq_partition_key])
    assert not rc._held and not rc._outstanding
    assert consumer.committed == []
    assert all(entry[1:] != dlq_partition_key for entry in rc._wake_heap)


def test_revoke_strips_a_frozen_batchs_revoked_partition_and_keeps_its_batch_mate() -> None:
    """Review finding M11: after M8's per-batch snapshot, a record
    awaiting its own batch's caught_up() barrier lives only in
    self._pending_batches -- a revoke that didn't check there would let
    this member later retry/commit a record on a partition it no longer
    owns, racing the new owner's independent re-read of the same record."""
    envelope1 = _envelope(original_partition=0, original_offset=1, retry_after_ms=1_000)
    envelope2 = _envelope(original_partition=1, original_offset=1, retry_after_ms=1_000)
    message1 = _dlq_message(envelope1, partition=0, offset=0)
    message2 = _dlq_message(envelope2, partition=1, offset=0)
    consumer = _FakeConsumer()
    consumer.push(message1)
    consumer.push(message2)
    writer = _FakeWriter(outcomes_sequence=[{0: (RETRY_OUTCOME_WRITTEN, "")}])
    index = _FakeIndex(caught_up=False)  # neither record's batch is caught up yet
    rc, fakes = _make_consumer(consumer=consumer, writer=writer, index=index)

    # Both records fetched (and paused) before the batch is flushed, so
    # they land in one frozen batch together.
    rc._resume_due_partitions()
    for _ in range(2):
        rc._on_message(consumer.poll(0.1))
    rc._maybe_flush_due_batch()
    assert "KNOWS" in rc._pending_batches
    assert len(rc._pending_batches["KNOWS"].keyed_pendings) == 2
    assert writer.calls == []

    # This member loses envelope1's partition to a rebalance.
    revoked_key = (message1.topic(), message1.partition())
    rc.on_partitions_revoked([revoked_key])
    assert len(rc._pending_batches["KNOWS"].keyed_pendings) == 1
    assert rc._pending_batches["KNOWS"].keyed_pendings[0][1].envelope == envelope2

    # The barrier clears; only envelope2 (still owned) is retried/committed.
    index._caught_up_override = True
    rc.tick()
    assert writer.calls == [[envelope2]]
    assert len(consumer.committed) == 1


# ── Duplicate-copy detection via ParkedPairIndex.contains (review M12) ─────


def test_duplicate_copy_with_a_gone_index_entry_is_skipped_never_written() -> None:
    """The exact M12 scenario: two DLQ copies of the identical record
    (same original_topic/partition/offset) exist because a prior batch's
    DLQ publish failed partway through and replayed. By the time this
    (second) copy is retried, its own index entry is already gone
    (resolved by the first copy) -- it must be committed past, never
    written or re-DLQ'd/parked again."""
    envelope = _envelope(original_topic="knows-events", original_partition=0, original_offset=5, retry_after_ms=1_000)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(envelope))
    writer = _FakeWriter()  # no outcomes queued -- must never be called
    sink = _FakeSink()      # must never be called either
    index = _FakeIndex(missing_ids={("knows-events", 0, 5)})
    notified = []
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, index=index,
        on_row_retried=lambda t, o: notified.append((t, o)),
    )
    rc.tick()
    assert writer.calls == []
    assert sink.calls == []
    assert index.removed == []  # already removed by the first copy -- not this one's job
    assert len(consumer.committed) == 1
    assert notified == [("KNOWS", "duplicate_skipped")]


def test_a_genuinely_indexed_record_is_not_treated_as_a_duplicate() -> None:
    """Regression guard: contains() must not spuriously skip a record that
    IS still indexed (the overwhelming common case)."""
    envelope = _envelope(original_topic="knows-events", original_partition=0, original_offset=5, retry_after_ms=1_000)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(envelope))
    writer = _FakeWriter(outcomes_sequence=[{0: (RETRY_OUTCOME_WRITTEN, "")}])
    index = _FakeIndex()  # missing_ids empty -- every record is "present"
    rc, fakes = _make_consumer(consumer=consumer, writer=writer, index=index)
    rc.tick()
    assert writer.calls == [[envelope]]
    assert len(consumer.committed) == 1


def test_commit_failure_is_retried_next_tick_and_resolved_records_are_never_rewritten() -> None:
    """A failing commit never drops or re-processes a resolved record: the
    commit position is simply recomputed and committed on the next tick."""
    envelope = _envelope(original_topic="knows-events", original_partition=0, original_offset=5, retry_after_ms=1_000)
    consumer = _FakeConsumer()
    real_commit = consumer.commit
    failures = {"left": 1}

    def _flaky_commit(*, message=None, offsets=None, asynchronous=False):
        if failures["left"]:
            failures["left"] -= 1
            raise RuntimeError("simulated commit failure")
        real_commit(message=message, offsets=offsets, asynchronous=asynchronous)

    consumer.commit = _flaky_commit
    consumer.push(_dlq_message(envelope))
    writer = _FakeWriter()
    index = _FakeIndex(missing_ids={("knows-events", 0, 5)})
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, index=index)
    rc.tick()
    assert writer.calls == []
    assert not rc._due_this_tick  # resolved (duplicate), not re-queued
    assert consumer.committed == []  # the commit failed ...
    rc.tick()
    assert len(consumer.committed) == 1  # ... and was retried
    assert writer.calls == []


# ── LeaseCoordinatedRetryWriter (Story 6, P5-Q1 fencing) ────────────────────


class _FakeLeaseClient:
    """Enough of BucketLeaseClient for BucketLeaseExecutionCoordinator."""

    broken = False

    def __init__(self, *, grants=True):
        self._grants = grants
        self.acquire_calls: list[tuple] = []
        self.released: list[str] = []
        self._next_id = 0
        self._lock = threading.Lock()

    def connect(self):
        pass

    def close(self):
        pass

    def ping(self):
        return True

    def extend(self, lease_id, weighted_sets, *, budget):
        return []

    def acquire_any(self, resource_sets, *, wait_ms):
        with self._lock:
            self.acquire_calls.append((resource_sets, wait_ms))
            if not self._grants:
                return None
            self._next_id += 1
            return (f"lease-{self._next_id}", 0)

    def release(self, lease_id):
        with self._lock:
            self.released.append(lease_id)


class _RecordingBaseWriter:
    """Stands in for ApocLockedEdgeWriter.retry_provenanced_batch."""

    def __init__(self, *, fail_on_call: int | None = None, unresolved_sources=()):
        self.calls: list[list[DeadLetterEnvelope]] = []
        self.kwargs: list[dict] = []
        self._fail_on_call = fail_on_call
        self._unresolved_sources = set(unresolved_sources)
        self._lock = threading.Lock()

    def retry_provenanced_batch(self, envelopes, **kwargs):
        with self._lock:
            self.calls.append(list(envelopes))
            self.kwargs.append(kwargs)
            call_number = len(self.calls)
        if self._fail_on_call == call_number:
            raise RuntimeError("simulated Neo4j failure")
        outcomes = {}
        for index, envelope in enumerate(envelopes):
            source_key, _target = json.loads(envelope.key_bytes)
            outcomes[index] = (
                (RETRY_OUTCOME_UNRESOLVED, "missing_source") if source_key in self._unresolved_sources
                else (RETRY_OUTCOME_WRITTEN, "")
            )
        return outcomes


def _leased_writer(
    base, lease_client, *, batch_size=500, worker_count=1, bucket_count=4, stop_event=None, clock=None,
):
    from src.loader.edge_execution import BucketLeaseExecutionCoordinator
    from src.loader.lease_client import BucketCellRouter

    coordinator = BucketLeaseExecutionCoordinator(
        RetryCoordinatorBridge(base), router=BucketCellRouter("Person", "Company", bucket_count, frozenset()),
        worker_count=worker_count, batch_size=batch_size, client_factory=lambda: lease_client,
        edge_type="KNOWS", tx_timeout_seconds=30.0, stop_event=stop_event,
        **({"clock": clock} if clock is not None else {}),
    )
    return LeaseCoordinatedRetryWriter(coordinator)


def test_leased_retry_writer_writes_a_whole_batch_in_leased_cell_transactions():
    envelopes = [
        _envelope(source_key=f"p{i}", target_key=f"c{i % 3}", original_offset=i) for i in range(40)
    ]
    base = _RecordingBaseWriter()
    lease_client = _FakeLeaseClient()
    writer = _leased_writer(base, lease_client)

    outcomes = writer.retry_batch(envelopes)

    assert outcomes == {i: (RETRY_OUTCOME_WRITTEN, "") for i in range(40)}
    assert sum(len(call) for call in base.calls) == 40  # every envelope written exactly once
    # every transaction ran under a lease with a deadline and a liveness check
    assert all(kw["deadline"] is not None and kw["lease_liveness_check"] is not None for kw in base.kwargs)
    # one lease per transaction, all released
    assert sorted(lease_client.released) == sorted(f"lease-{i}" for i in range(1, len(base.calls) + 1))
    assert lease_client.acquire_calls


def test_leased_retry_writer_keeps_a_pairs_rows_in_order_across_transactions():
    # One pair, batch_size 2 -> several sequential transactions of one cell.
    envelopes = [_envelope(source_key="a", target_key="b", original_offset=i) for i in range(5)]
    base = _RecordingBaseWriter()
    writer = _leased_writer(base, _FakeLeaseClient(), batch_size=2)

    outcomes = writer.retry_batch(envelopes)

    assert len(outcomes) == 5
    written_order = [e.original_offset for call in base.calls for e in call]
    assert written_order == [0, 1, 2, 3, 4]


def test_leased_retry_writer_partial_failure_keeps_durable_outcomes_and_omits_the_rest():
    envelopes = [_envelope(source_key="a", target_key="b", original_offset=i) for i in range(6)]
    base = _RecordingBaseWriter(fail_on_call=2)
    lease_client = _FakeLeaseClient()
    writer = _leased_writer(base, lease_client, batch_size=2)

    outcomes = writer.retry_batch(envelopes)

    assert sorted(outcomes) == [0, 1]  # only the first transaction committed
    # the committed tx's lease is released; the ambiguous failure's lease is left to the
    # coordinator's own fencing (released only for confirmed outcomes)
    assert lease_client.released == ["lease-1"]


def test_leased_retry_writer_raises_when_nothing_was_written():
    base = _RecordingBaseWriter(fail_on_call=1)
    writer = _leased_writer(base, _FakeLeaseClient())
    with pytest.raises(RuntimeError, match="simulated Neo4j"):
        writer.retry_batch([_envelope(source_key="a", target_key="b")])


def test_leased_retry_writer_empty_batch_never_touches_the_lease_client():
    base = _RecordingBaseWriter()
    lease_client = _FakeLeaseClient()
    writer = _leased_writer(base, lease_client)
    assert writer.retry_batch([]) == {}
    assert lease_client.acquire_calls == []
    assert base.calls == []


def test_leased_retry_writer_stop_drain_abandons_when_leases_are_never_granted():
    """Shutdown mid-flush: a stop request bounds the wait for leases (nothing
    is written without one) and surfaces as a failure with no outcomes."""
    from src.loader.edge_execution import LeaseDrainAbandoned

    stop = threading.Event()
    stop.set()
    base = _RecordingBaseWriter()
    lease_client = _FakeLeaseClient(grants=False)
    writer = _leased_writer(base, lease_client, stop_event=stop)
    writer._coordinator._drain_budget = 0.0
    with pytest.raises(LeaseDrainAbandoned):
        writer.retry_batch([_envelope(source_key="a", target_key="b")])
    assert base.calls == []


def test_adapter_chunks_a_standalone_batch_and_keeps_earlier_chunks_on_a_later_failure():
    from src.loader.retry_consumer import ApocRetryWriterAdapter

    base = _RecordingBaseWriter(fail_on_call=3)
    adapter = ApocRetryWriterAdapter(base, chunk_size=2)
    envelopes = [_envelope(source_key=f"p{i}", target_key="c", original_offset=i) for i in range(6)]
    outcomes = adapter.retry_batch(envelopes)
    assert sorted(outcomes) == [0, 1, 2, 3]  # chunks 1 and 2 durable, chunk 3 failed
    assert [len(call) for call in base.calls] == [2, 2, 2]


# ── DLQ backlog gauge (Story 7, P5-Q4 follow-up) ────────────────────────────


def test_compute_dlq_backlog_arithmetic() -> None:
    assert compute_dlq_backlog(
        dlq_high_watermark=10, dlq_committed_offset=7,
        parked_high_watermark=3, parked_low_watermark=1,
    ) == {"dlq": 3, "parked": 2}  # (10-7), (3-1)


def test_compute_dlq_backlog_clamps_negative_terms_to_zero() -> None:
    # A momentarily stale read (the two watermark calls aren't atomic with
    # each other) must never report a negative backlog.
    assert compute_dlq_backlog(
        dlq_high_watermark=5, dlq_committed_offset=9,
        parked_high_watermark=2, parked_low_watermark=2,
    ) == {"dlq": 0, "parked": 0}


class _FakeBacklogSource:
    def __init__(self, dlq=(5, 2), parked=(3, 1)) -> None:
        self._dlq = dlq
        self._parked = parked
        self.calls: list[tuple[str, str]] = []

    def dlq_high_watermark_and_committed(self, dlq_topic: str):
        self.calls.append(("dlq", dlq_topic))
        return self._dlq

    def parked_watermarks(self, parked_topic: str):
        self.calls.append(("parked", parked_topic))
        return self._parked


class _FailingBacklogSource:
    def dlq_high_watermark_and_committed(self, dlq_topic: str):
        raise RuntimeError("simulated broker failure")

    def parked_watermarks(self, parked_topic: str):
        raise RuntimeError("unused")


def test_compute_and_notify_backlog_reports_the_computed_value() -> None:
    source = _FakeBacklogSource(dlq=(5, 2), parked=(3, 1))
    reported = []
    rc, _fakes = _make_consumer()
    rc._backlog_source_by_edge_type = {"KNOWS": source}
    rc._dlq_topic_by_edge_type = {"KNOWS": "knows.dlq"}
    rc._on_backlog_computed = lambda edge_type, queue, value: reported.append((edge_type, queue, value))

    value = rc.compute_and_notify_backlog("KNOWS")
    assert value == {"dlq": 3, "parked": 2}  # (5-2), (3-1)
    assert reported == [("KNOWS", "dlq", 3), ("KNOWS", "parked", 2)]
    assert source.calls == [("dlq", "knows.dlq"), ("parked", "knows.dlq.parked")]


def test_compute_and_notify_backlog_returns_none_and_never_raises_on_failure() -> None:
    reported = []
    rc, _fakes = _make_consumer()
    rc._backlog_source_by_edge_type = {"KNOWS": _FailingBacklogSource()}
    rc._dlq_topic_by_edge_type = {"KNOWS": "knows.dlq"}
    rc._on_backlog_computed = lambda edge_type, queue, value: reported.append((edge_type, queue, value))

    value = rc.compute_and_notify_backlog("KNOWS")  # must not raise
    assert value is None
    assert reported == []  # never fires the listener with a bogus value


def test_compute_and_notify_backlog_returns_none_for_an_unconfigured_edge_type() -> None:
    rc, _fakes = _make_consumer()
    assert rc.compute_and_notify_backlog("SOME_OTHER_TYPE") is None


def test_start_backlog_thread_is_a_noop_without_any_backlog_source() -> None:
    rc, _fakes = _make_consumer()
    rc.start_backlog_thread()
    assert rc._backlog_thread is None
    rc.stop_backlog_thread()  # must not raise even though never started


def test_backlog_thread_runs_periodically_and_stops_cleanly() -> None:
    source = _FakeBacklogSource()
    computed = threading.Event()
    reported = []

    def on_backlog_computed(edge_type, queue, value):
        reported.append((edge_type, queue, value))
        computed.set()

    rc, _fakes = _make_consumer()
    rc._backlog_source_by_edge_type = {"KNOWS": source}
    rc._dlq_topic_by_edge_type = {"KNOWS": "knows.dlq"}
    rc._backlog_check_interval_seconds = 0.01
    rc._on_backlog_computed = on_backlog_computed

    rc.start_backlog_thread()
    try:
        assert computed.wait(timeout=2.0)  # bounded wait, never a fixed sleep
        assert reported
    finally:
        rc.stop_backlog_thread()
    assert rc._backlog_thread is None


# ── KafkaBacklogSource (Story 6, N15) ───────────────────────────────────────


class _FakeAdminConsumer:
    def __init__(self, watermarks: dict[int, tuple[int, int]], committed: dict[int, int]) -> None:
        self._watermarks = watermarks
        self._committed = committed

    def list_topics(self, topic=None, timeout=None):
        class _Item:
            error = None

        item = _Item()
        item.partitions = {partition: object() for partition in self._watermarks}

        class _Metadata:
            topics = {topic: item}

        return _Metadata()

    def get_watermark_offsets(self, topic_partition, timeout=None):
        return self._watermarks[topic_partition.partition]

    def committed(self, partitions, timeout=None):
        results = []
        for tp in partitions:
            offset = self._committed.get(tp.partition, -1001)  # OFFSET_INVALID default
            result = MagicMock()
            result.partition = tp.partition
            result.offset = offset
            results.append(result)
        return results

    def close(self):
        pass


def test_kafka_backlog_source_dlq_high_watermark_and_committed():
    fake = _FakeAdminConsumer(watermarks={0: (0, 10)}, committed={0: 7})
    source = KafkaBacklogSource(
        bootstrap_servers="kafka:9092", group_id="g", consumer_factory=lambda config: fake,
    )
    assert source.dlq_high_watermark_and_committed("t.dlq") == (10, 7)


def test_kafka_backlog_source_falls_back_to_low_watermark_for_offset_invalid():
    """Review finding N15: a partition with no committed offset yet
    (OFFSET_INVALID, -1001) must fall back to its own low watermark, never
    to 0 (which would inflate the backlog on a retention.ms=-1 topic
    whose low watermark has already advanced) and never treat it as no
    backlog either."""
    fake = _FakeAdminConsumer(watermarks={0: (4, 10)}, committed={})  # no committed offset at all
    source = KafkaBacklogSource(
        bootstrap_servers="kafka:9092", group_id="g", consumer_factory=lambda config: fake,
    )
    assert source.dlq_high_watermark_and_committed("t.dlq") == (10, 4)


def test_kafka_backlog_source_parked_watermarks():
    fake = _FakeAdminConsumer(watermarks={0: (2, 5)}, committed={})
    source = KafkaBacklogSource(
        bootstrap_servers="kafka:9092", group_id="g", consumer_factory=lambda config: fake,
    )
    assert source.parked_watermarks("t.dlq.parked") == (5, 2)


def test_kafka_backlog_source_sums_across_multiple_partitions():
    fake = _FakeAdminConsumer(watermarks={0: (0, 10), 1: (0, 5)}, committed={0: 3, 1: 2})
    source = KafkaBacklogSource(
        bootstrap_servers="kafka:9092", group_id="g", consumer_factory=lambda config: fake,
    )
    assert source.dlq_high_watermark_and_committed("t.dlq") == (15, 5)


# ── on_revoke TopicPartition adapter (review finding N11) ───────────────────


def test_revoked_topic_partitions_adapts_confluent_kafka_objects():
    tp1, tp2 = MagicMock(topic="a.dlq", partition=0), MagicMock(topic="b.dlq", partition=1)
    assert _revoked_topic_partitions([tp1, tp2]) == [("a.dlq", 0), ("b.dlq", 1)]


def test_revoked_topic_partitions_empty_list():
    assert _revoked_topic_partitions([]) == []


# ── main() argument/config validation (Story 6) ─────────────────────────────


def test_main_rejects_blank_dlq_topics():
    with pytest.raises(SystemExit):
        retry_consumer_main(["--dlq-topics", "  ,  ", "--config", "config/graph_schema.yaml"])


@patch("src.loader.retry_consumer.load_schema")
def test_main_reports_failure_for_a_dlq_topic_with_no_matching_edge(mock_load_schema):
    mock_load_schema.return_value = MagicMock(edges=[])
    exit_code = retry_consumer_main(["--dlq-topics", "unknown.dlq"])
    assert exit_code == 1


# ── main() ParkedPairIndex construction (review finding M18) ───────────────


def _dlq_schema_for_main():
    """A real schema (not a loose double) with WORKS_AT's dead_letter

    enabled -- every nested config field main() actually reads
    (loading.retry_consumer, loading.bucket_lease, edge.retry, etc.) is
    the genuine pydantic model, not a MagicMock stand-in that could mask
    a real attribute-access bug.
    """
    from src.models.schema import DeadLetterConfig
    from src.utils.schema_loader import load_schema

    project_root = Path(__file__).resolve().parents[1]
    schema = load_schema(project_root / "config" / "graph_schema.yaml")
    edge_index = next(i for i, edge in enumerate(schema.edges) if edge.type == "WORKS_AT")
    schema.edges[edge_index] = schema.edges[edge_index].model_copy(
        update={"dead_letter": DeadLetterConfig(enabled=True, topic="dlq-works-at", max_retry_attempts=3)},
    )
    return schema


def _enter_shared_main_patches(stack, dlq_schema):
    """Enter, via the given ExitStack, every patch shared by both M18

    tests: every module-level or local (inside main()) import that would
    otherwise reach a real Kafka/Neo4j connection is replaced with a
    bookkeeping double, so only the ParkedPairIndex-construction decision
    under test is real. Returns the ``RetryConsumer`` class mock so the
    caller can inspect its call kwargs.
    """
    real_sink = MagicMock(spec=DeadLetterRecordSink)
    consumer, driver = MagicMock(), MagicMock()
    retry_consumer_instance = MagicMock()
    retry_consumer_instance.run_forever.return_value = None
    stack.enter_context(patch("src.loader.retry_consumer.load_schema", return_value=dlq_schema))
    stack.enter_context(patch("confluent_kafka.Consumer", return_value=consumer))
    stack.enter_context(patch("neo4j.GraphDatabase.driver", return_value=driver))
    stack.enter_context(
        patch("src.loader.retry_consumer.build_malformed_record_sink", return_value=(real_sink, None))
    )
    # BUG-2: the REAL ApocLockedEdgeWriter is constructed (via
    # build_retry_base_writer) -- no hand-built or spec'd stand-in.
    return stack.enter_context(
        patch("src.loader.retry_consumer.RetryConsumer", return_value=retry_consumer_instance)
    )


def _assert_main_wired_writer_delegates(rc_cls, *, leased: bool) -> None:
    """BUG-1: the writer main() hands RetryConsumer must be a real RetryWriter
    that delegates retry_batch -> the base writer's retry_provenanced_batch."""
    from src.loader.retry_consumer import ApocRetryWriterAdapter, RetryWriter

    wired = rc_cls.call_args.kwargs["writer_by_edge_type"]["WORKS_AT"]
    assert isinstance(wired, RetryWriter)
    from src.loader.edge_execution import ApocLockedEdgeWriter

    if leased:
        assert isinstance(wired, LeaseCoordinatedRetryWriter)
        bridge = wired._coordinator._writer
        assert isinstance(bridge, RetryCoordinatorBridge)
        assert type(bridge._writer) is ApocLockedEdgeWriter  # real, not a double
        assert bridge._writer._explicit_locks is False  # lease-mode writer, like the edge loader's
        return
    adapter = wired
    assert isinstance(adapter, ApocRetryWriterAdapter)
    base = adapter._writer
    assert type(base) is ApocLockedEdgeWriter  # real, not a double
    assert not hasattr(base, "retry_batch")
    assert isinstance(base._classification_query, str) and base._classification_query  # BUG-2
    with patch.object(ApocLockedEdgeWriter, "retry_provenanced_batch",
                      return_value={0: (RETRY_OUTCOME_WRITTEN, "")}) as mocked:
        assert adapter.retry_batch(["env"]) == {0: (RETRY_OUTCOME_WRITTEN, "")}
    mocked.assert_called_once_with(["env"])


def test_main_standalone_mode_builds_no_parked_pair_index():
    """M18: no --lease-address (the documented escape hatch for a

    finished bulk run's opt-in DLQ backlog) must build no
    ParkedPairIndex at all -- bulk-routed rows never get an index entry,
    so building one made every candidate look like an unindexed
    duplicate and retried nothing."""
    dlq_schema = _dlq_schema_for_main()
    with ExitStack() as stack:
        rc_cls = _enter_shared_main_patches(stack, dlq_schema)
        index_cls = stack.enter_context(patch("src.loader.retry_consumer.ParkedPairIndex"))
        exit_code = retry_consumer_main(["--dlq-topics", "dlq-works-at"])

    assert exit_code == 0
    index_cls.assert_not_called()
    assert rc_cls.call_args.kwargs["parked_pair_index_by_edge_type"] == {}
    _assert_main_wired_writer_delegates(rc_cls, leased=False)


def test_main_lease_mode_still_builds_a_parked_pair_index():
    """Corollary: with a live --lease-address, the ordinary fleet path

    (Story 3/6) still builds a writable index for a non-order_by,
    DLQ-enabled edge type -- M18's fix is standalone-only, not a
    regression of the fleet-launched case."""
    dlq_schema = _dlq_schema_for_main()
    fake_index = MagicMock()
    fake_lease_client = MagicMock()
    with ExitStack() as stack:
        rc_cls = _enter_shared_main_patches(stack, dlq_schema)
        index_cls = stack.enter_context(
            patch("src.loader.retry_consumer.ParkedPairIndex", return_value=fake_index)
        )
        stack.enter_context(patch("src.loader.lease_client.BucketLeaseClient", return_value=fake_lease_client))
        stack.enter_context(patch("src.loader.lease_client.BucketCellRouter", return_value=MagicMock()))
        stack.enter_context(patch.dict(os.environ, {
            "GRAPH_LOADER_BUCKET_LEASE_ADDR": "lease-host:7700", "GRAPH_LOADER_LEASE_BUCKETS": "16",
        }, clear=False))
        exit_code = retry_consumer_main([
            "--dlq-topics", "dlq-works-at", "--lease-address", "lease-host:7700",
        ])

    assert exit_code == 0
    index_cls.assert_called_once()
    fake_index.start.assert_called_once()
    assert rc_cls.call_args.kwargs["parked_pair_index_by_edge_type"] == {"WORKS_AT": fake_index}
    _assert_main_wired_writer_delegates(rc_cls, leased=True)


# ── P6-S5 Story 9: shutdown re-audit of the retry consumer ─────────────────


def test_run_forever_exits_on_shutdown_and_never_commits_a_paused_record() -> None:
    """At-least-once on SIGTERM: a not-yet-due record is neither committed nor
    dropped - its partition's offset stays uncommitted, so a restart re-reads it."""
    import threading

    shutdown = threading.Event()
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(_envelope(retry_after_ms=20_000)))  # due in the future
    real_poll = consumer.poll

    def poll(timeout=None):
        message = real_poll(timeout)
        if consumer.poll_calls >= 2:
            shutdown.set()  # the operator's SIGTERM lands during the 2nd poll
        return message

    consumer.poll = poll
    rc, fakes = _make_consumer(consumer=consumer, wall_clock=_Clock(start=10_000))

    rc.run_forever(shutdown)  # returns instead of spinning

    assert consumer.poll_calls == 2
    assert consumer.committed == []
    assert fakes["writer"].calls == []
    assert len(rc._held) == 1  # held in memory only; redelivered after a restart


def _run_main_with_run_forever(effect):
    """Run main() with everything faked; returns (exit_code, consumer, driver)."""
    import confluent_kafka
    import neo4j

    with ExitStack() as stack:
        rc_cls = _enter_shared_main_patches(stack, _dlq_schema_for_main())
        rc_cls.return_value.run_forever.side_effect = effect
        consumer = confluent_kafka.Consumer.return_value
        driver = neo4j.GraphDatabase.driver.return_value
        exit_code = retry_consumer_main(["--dlq-topics", "dlq-works-at"])
    return exit_code, consumer, driver


def test_main_sigterm_exit_zero_closes_everything_and_keeps_the_handler() -> None:
    import signal

    seen = {}

    def run_forever(shutdown_requested):
        handler = signal.getsignal(signal.SIGTERM)
        seen["installed"] = handler
        handler(signal.SIGTERM, None)  # what Docker's stop delivers
        seen["shutdown"] = shutdown_requested.is_set()

    before = signal.getsignal(signal.SIGTERM)
    exit_code, consumer, driver = _run_main_with_run_forever(run_forever)

    assert exit_code == 0 and seen["shutdown"] is True
    assert seen["installed"] is not before
    assert signal.getsignal(signal.SIGTERM) == signal.SIG_IGN  # F2: ignored as the last step
    assert consumer.close.called and driver.close.called


def test_main_run_loop_failure_exits_nonzero_but_still_closes_everything() -> None:
    """Review finding N13: a failing tick never spins and never leaks a resource."""
    import signal

    def run_forever(shutdown_requested):
        raise RuntimeError("tick blew up")

    exit_code, consumer, driver = _run_main_with_run_forever(run_forever)

    assert exit_code == 1
    assert signal.getsignal(signal.SIGTERM) == signal.SIG_IGN
    assert consumer.close.called and driver.close.called


# ── BUG-1 regression: real-object wiring (no fake writer) ───────────────────


def _real_wired_writer(driver, *, lease_client=None):
    """Build the writer exactly as main() does: real ApocLockedEdgeWriter
    -> build_retry_writer (adapter, or lease-coordinated when a client is given)."""
    from src.loader.lease_client import BucketCellRouter
    from src.loader.retry_consumer import build_retry_base_writer, build_retry_lease_coordinator, build_retry_writer
    from src.utils.schema_loader import load_schema

    edge = next(e for e in load_schema("config/graph_schema.yaml").edges if e.type == "WORKS_AT")
    # The same factories main() calls -- NOT a hand-built writer (BUG-2).
    base = build_retry_base_writer(driver, edge, tx_timeout_seconds=30.0, leased=lease_client is not None)
    if lease_client is None:
        return build_retry_writer(base)
    coordinator = build_retry_lease_coordinator(
        base, edge, router=BucketCellRouter(edge.nodes.source, edge.nodes.target, 16, frozenset()),
        client_factory=lambda: lease_client, worker_count=1, tx_timeout_seconds=30.0,
    )
    return build_retry_writer(base, coordinator=coordinator)


def _works_at_envelope():
    import dataclasses

    value = json.dumps({
        "source": {"personId": "p-1"}, "target": {"companyId": "c-1"},
        "properties": {"since": "2020-01-02"},
    }).encode("utf-8")
    return dataclasses.replace(_envelope(entity_name="WORKS_AT", source_key="p-1", target_key="c-1"), value_bytes=value)


def _rows_result(rows):
    result = MagicMock()
    result.__iter__.side_effect = lambda: iter(rows)
    return result


@pytest.mark.parametrize("leased", [False, True])
def test_real_wired_writer_satisfies_retry_writer_protocol(leased) -> None:
    from src.loader.retry_consumer import RetryWriter

    writer = _real_wired_writer(MagicMock(), lease_client=_FakeLeaseClient() if leased else None)
    assert isinstance(writer, RetryWriter)


@pytest.mark.parametrize("leased", [False, True])
def test_retry_consumer_tick_with_real_writer_writes_the_edge(leased) -> None:
    queries: list[str] = []
    driver = _strict_driver(queries)
    tx = driver.session.return_value.__enter__.return_value.begin_transaction.return_value.__enter__.return_value
    lease_client = _FakeLeaseClient() if leased else None
    writer = _real_wired_writer(driver, lease_client=lease_client)
    envelope = _works_at_envelope()
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(envelope))
    index = _FakeIndex()
    rc, fakes = _make_consumer(consumer=consumer, writer=writer, index=index, entity_name="WORKS_AT")
    rc.tick()
    rc.tick()

    assert tx.run.call_count == 2  # classification + the actual MERGE write
    assert len(queries) == 2 and queries[0] != queries[1]  # both non-empty (strict driver)
    assert "source_matches" in queries[0] and "MERGE" in queries[1]
    tx.commit.assert_called_once()
    assert index.removed == [(envelope.original_topic, envelope.original_partition, envelope.original_offset)]
    assert fakes["sink"].calls == []  # written, not re-DLQ'd/parked
    assert len(consumer.committed) == 1
    if leased:
        assert lease_client.released == ["lease-1"]


def test_retry_consumer_tick_with_real_writer_unresolved_reroutes() -> None:
    driver = MagicMock()
    tx = driver.session.return_value.__enter__.return_value.begin_transaction.return_value.__enter__.return_value
    tx.run.side_effect = [_rows_result([{"row_index": 0, "source_matches": 0, "target_matches": 1}])]
    writer = _real_wired_writer(driver)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(_works_at_envelope()))
    rc, fakes = _make_consumer(consumer=consumer, writer=writer, entity_name="WORKS_AT")
    rc.tick()
    rc.tick()
    (call,) = fakes["sink"].calls
    assert call["reason_code"] == "missing_source"


def test_other_retry_consumer_seams_match_real_signatures() -> None:
    """Guard the remaining duck-typed seams against fake-only drift: every
    call RetryConsumer makes must bind against the real class's signature."""
    import inspect

    from src.loader.dead_letter import DeadLetterRecordSink
    from src.loader.parked_pair_index import ParkedPairIndex
    from src.loader.retry_consumer import BacklogSource

    key = dict(original_topic="t", original_partition=0, original_offset=1)
    inspect.signature(ParkedPairIndex.contains).bind(None, "a", "b", **key)
    inspect.signature(ParkedPairIndex.remove).bind(None, "a", "b", **key)
    inspect.signature(ParkedPairIndex.is_blocked_by_earlier).bind(None, "a", "b", first_failure_at_ms=1, **key)
    inspect.signature(ParkedPairIndex.caught_up).bind(None, {})
    inspect.signature(ParkedPairIndex.watermark_snapshot).bind(None)
    inspect.signature(DeadLetterRecordSink.reroute_or_park).bind(
        None, envelope=None, reason_code="r", error_text="e",
        retry_after_seconds_for_reattempt=1.0, parked_topic=None, increment_attempt=False,
    )
    assert issubclass(KafkaBacklogSource, BacklogSource)


def test_revoke_adapter_accepts_real_confluent_topic_partitions() -> None:
    from confluent_kafka import TopicPartition

    assert _revoked_topic_partitions([TopicPartition("a.dlq", 3), TopicPartition("b.dlq", 0)]) == [
        ("a.dlq", 3), ("b.dlq", 0),
    ]


# ── BUG-2 regression: the retry path must never run a None/empty query ──────


def _strict_driver(queries):
    """Fake driver asserting every query passed to run() is a non-empty str."""
    driver = MagicMock()
    tx = driver.session.return_value.__enter__.return_value.begin_transaction.return_value.__enter__.return_value

    def _run(query, **kwargs):
        assert isinstance(query, str) and query.strip(), f"invalid query: {query!r}"
        queries.append(query)
        if len(queries) == 1:
            return _rows_result([{"row_index": 0, "source_matches": 1, "target_matches": 1}])
        return _rows_result([{"checked_rows": 1, "invalid_rows": 0, "merged_rows": 1}])

    tx.run.side_effect = _run
    return driver


def test_retry_provenanced_batch_via_main_factory_runs_only_real_queries() -> None:
    from src.loader.edge_execution import _endpoint_classification_query
    from src.loader.retry_consumer import build_retry_base_writer
    from src.utils.schema_loader import load_schema

    edge = next(e for e in load_schema("config/graph_schema.yaml").edges if e.type == "WORKS_AT")
    queries: list = []
    base = build_retry_base_writer(_strict_driver(queries), edge, tx_timeout_seconds=30.0)
    outcomes = base.retry_provenanced_batch([_works_at_envelope()])
    assert outcomes == {0: (RETRY_OUTCOME_WRITTEN, "")}
    assert queries[0] == _endpoint_classification_query(edge)
    assert queries[1] == base._query
    assert len(queries) == 2


def test_build_retry_base_writer_fails_fast_without_classification_query() -> None:
    from src.loader.edge_execution import ApocLockedEdgeWriter
    from src.loader.retry_consumer import build_retry_base_writer
    from src.utils.schema_loader import load_schema

    edge = next(e for e in load_schema("config/graph_schema.yaml").edges if e.type == "WORKS_AT")
    with patch("src.loader.edge_execution._endpoint_classification_query", return_value=None):
        with pytest.raises((ValueError, RuntimeError)):
            build_retry_base_writer(MagicMock(), edge, tx_timeout_seconds=30.0)
    # and the plain constructor (no sink) carries the query too
    plain = ApocLockedEdgeWriter(MagicMock(), edge, missing_endpoint_error_factory=RuntimeError)
    assert isinstance(plain._classification_query, str) and plain._classification_query
    assert plain.row_level_dlq_enabled is False


# ── Batched drain: many due records per partition, one write, one commit ────


class _ScriptedWriter:
    """Decides every envelope's outcome by a function of the envelope; an
    envelope whose original_offset is in ``drop`` gets no outcome at all (a
    row whose transaction never committed)."""

    def __init__(self, decide=None, drop=(), pair_guard=False) -> None:
        self.calls: list[list[DeadLetterEnvelope]] = []
        self._decide = decide or (lambda envelope: (RETRY_OUTCOME_WRITTEN, ""))
        self._drop = set(drop)
        self._pair_guard = pair_guard
        self.on_call = None

    def retry_batch(self, envelopes, budget_seconds=None):
        self.calls.append(list(envelopes))
        if self.on_call is not None:
            self.on_call()
        outcomes = {}
        unresolved_pairs = set()
        for index, envelope in enumerate(envelopes):
            if envelope.original_offset in self._drop:
                continue
            if self._pair_guard and envelope.key_bytes in unresolved_pairs:
                # The real writer's in-flush guard: a later row behind an
                # earlier unresolved row of its pair is reported blocked.
                outcomes[index] = (RETRY_OUTCOME_UNRESOLVED, "blocked_by_earlier_unresolved_row")
                continue
            outcomes[index] = self._decide(envelope)
            if outcomes[index][0] != RETRY_OUTCOME_WRITTEN:
                unresolved_pairs.add(envelope.key_bytes)
        return outcomes


_UNRESOLVED = (RETRY_OUTCOME_UNRESOLVED, "missing_target")


def _offsets(envelopes) -> list[tuple[int, int]]:
    return [(e.original_partition, e.original_offset) for e in envelopes]


def test_many_records_across_partitions_are_one_batch_one_publish_flush_and_one_commit() -> None:
    consumer = _FakeConsumer()
    for partition in range(3):
        for offset in range(50):
            consumer.push(_dlq_message(_envelope(
                source_key=f"s{partition}-{offset}", original_partition=partition, original_offset=offset,
            )))
    writer, sink, index = _ScriptedWriter(), _FakeSink(), _FakeIndex()
    notified = []
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, index=index,
        on_row_retried=lambda t, o: notified.append(o),
    )

    rc.tick()

    assert len(writer.calls) == 1 and len(writer.calls[0]) == 150  # one write batch, not 150 (or 3)
    assert index.remove_many_calls == 1 and len(index.removed) == 150  # one index flush
    assert sink.calls == [] and notified == ["written"] * 150
    # ONE commit call, carrying every partition's highest resolved offset + 1
    assert _committed_positions(consumer) == [
        [("knows.dlq", 0, 50), ("knows.dlq", 1, 50), ("knows.dlq", 2, 50)],
    ]
    assert not rc._outstanding["knows.dlq", 0]


def test_a_partition_is_paused_at_the_per_partition_bound_and_resumed_after_the_flush() -> None:
    consumer = _FakeConsumer()
    for offset in range(25):
        consumer.push(_dlq_message(_envelope(source_key=f"s{offset}", original_offset=offset)))
    writer = _ScriptedWriter()
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, max_records_per_partition=10)

    for _ in range(3):
        rc.tick()

    assert [len(call) for call in writer.calls] == [10, 10, 5]  # never more than the bound in flight
    assert len(consumer.paused) == 2 and len(consumer.resumed) == 2
    assert _committed_positions(consumer)[-1] == [("knows.dlq", 0, 25)]


def test_batch_is_written_in_original_partition_offset_order() -> None:
    consumer = _FakeConsumer()
    for partition, offset in [(1, 0), (0, 0), (0, 1), (1, 1)]:
        consumer.push(_dlq_message(_envelope(
            source_key=f"s{partition}{offset}", original_partition=partition, original_offset=offset,
        )))
    writer = _ScriptedWriter()
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer)
    rc.tick()
    assert _offsets(writer.calls[0]) == [(0, 0), (0, 1), (1, 0), (1, 1)]


def test_later_row_of_a_pair_is_blocked_behind_the_earlier_one_and_both_commit() -> None:
    first = _envelope(source_key="a", target_key="b", original_offset=0)
    second = _envelope(source_key="a", target_key="b", original_offset=1)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(first))
    consumer.push(_dlq_message(second))
    writer, sink = _ScriptedWriter(), _FakeSink()
    index = _FakeIndex(blocked_ids={("knows-events", 0, 1)})  # the index: row 1 is behind outstanding row 0
    notified = []
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, index=index,
        on_row_retried=lambda t, o: notified.append(o),
    )

    rc.tick()

    assert writer.calls == [[first]]  # the later row never reaches the writer ahead of the earlier one
    (blocked_call,) = sink.calls
    assert blocked_call["envelope"] == second and blocked_call["increment_attempt"] is False
    assert index.removed == [("knows-events", 0, 0)]  # only the written row leaves the index
    assert sorted(notified) == ["blocked", "written"]
    assert _committed_positions(consumer) == [[("knows.dlq", 0, 2)]]


def test_unresolved_earlier_row_keeps_the_later_one_blocked_without_spending_its_budget() -> None:
    first = _envelope(source_key="a", target_key="b", original_offset=0, attempt_count=0)
    second = _envelope(source_key="a", target_key="b", original_offset=1, attempt_count=1)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(first))
    consumer.push(_dlq_message(second))
    writer, sink = _ScriptedWriter(decide=lambda e: _UNRESOLVED), _FakeSink()
    index = _FakeIndex(blocked_ids={("knows-events", 0, 1)})
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, sink=sink, index=index, max_retry_attempts=5)

    rc.tick()

    by_offset = {call["envelope"].original_offset: call for call in sink.calls}
    assert by_offset[0].get("increment_attempt", True) is True  # its own resolution failure: backoff
    assert by_offset[0]["retry_after_seconds_for_reattempt"] == pytest.approx(1.0)
    # v7-S4 M1: merely blocked behind a row this consumer just re-DLQ'd -> held locally just
    # after that row's wake (attempt and budget untouched), never re-published
    assert 1 not in by_offset
    assert _held_wake_ms(rc, 1) == 10_000 + 1_000 + 1
    assert index.removed == []  # both still outstanding in the index
    assert _committed_positions(consumer) == [[("knows.dlq", 0, 1)]]  # never past the held row


def test_same_flush_block_reported_by_the_writer_is_treated_as_blocked_not_a_failed_attempt() -> None:
    first = _envelope(source_key="a", target_key="b", original_offset=0)
    second = _envelope(source_key="a", target_key="b", original_offset=1)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(first))
    consumer.push(_dlq_message(second))
    writer = _ScriptedWriter(decide=lambda e: (
        _UNRESOLVED if e.original_offset == 0 else (RETRY_OUTCOME_UNRESOLVED, "blocked_by_earlier_unresolved_row")
    ))
    sink, notified = _FakeSink(), []
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, on_row_retried=lambda t, o: notified.append(o),
    )
    rc.tick()
    by_offset = {call["envelope"].original_offset: call for call in sink.calls}
    # v7-S4 M1: held locally behind the re-DLQ'd earlier row, not re-published, not counted
    assert 1 not in by_offset and _held_wake_ms(rc, 1) is not None
    assert rc._write_failures == {}
    assert sorted(notified) == ["re_dlq"]


def test_mixed_due_and_not_due_records_flow_around_the_held_one_and_the_commit_stops_below_it() -> None:
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    for offset, retry_after in [(0, 1_000), (1, 1_000), (2, 20_000), (3, 1_000)]:
        consumer.push(_dlq_message(_envelope(source_key=f"s{offset}", original_offset=offset, retry_after_ms=retry_after)))
    writer = _ScriptedWriter()
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, wall_clock=wall)

    rc.tick()
    # the not-due row neither blocked poll() nor stalled the due rows behind it
    assert [_offsets(call) for call in writer.calls] == [[(0, 0), (0, 1), (0, 3)]]
    assert _committed_positions(consumer) == [[("knows.dlq", 0, 2)]]  # stops below the held row
    assert len(rc._held) == 1 and consumer.paused == []

    wall.advance(11_000)  # the held row's backoff elapses
    rc.tick()
    assert [_offsets(call) for call in writer.calls][1] == [(0, 2)]
    assert _committed_positions(consumer)[-1] == [("knows.dlq", 0, 4)]
    assert not rc._held and not rc._paused


def test_a_commit_never_passes_an_unresolved_row_even_when_later_rows_resolve() -> None:
    wall = _Clock(start=10_000)
    held = _envelope(source_key="s2", original_offset=2, retry_after_ms=20_000)
    later = _envelope(source_key="s3", original_offset=3, retry_after_ms=1_000)
    consumer = _FakeConsumer()
    writer = _ScriptedWriter()
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, wall_clock=wall)
    # Both delivered (the pause raced the fetch): the later row is due and writes.
    rc._on_message(_dlq_message(held))
    rc._on_message(_dlq_message(later))
    rc._maybe_flush_due_batch()
    rc._commit_resolved()
    assert _offsets(writer.calls[0]) == [(0, 3)]
    assert consumer.committed == []  # offset 2 is unresolved: nothing at or past it may commit

    wall.advance(11_000)
    rc.tick()
    assert _committed_positions(consumer) == [[("knows.dlq", 0, 4)]]  # now both are resolved


def test_rows_without_a_durable_outcome_stop_the_commit_and_are_retried_alone() -> None:
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    for offset in range(3):
        consumer.push(_dlq_message(_envelope(source_key=f"s{offset}", original_offset=offset)))
    writer = _ScriptedWriter(drop={1})  # offset 1's transaction never committed
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, wall_clock=wall)

    rc.tick()
    assert _committed_positions(consumer) == [[("knows.dlq", 0, 1)]]  # below the unresolved row, not past it
    # the slice, then the failing row bisected out and tried alone
    assert [_offsets(call) for call in writer.calls] == [[(0, 0), (0, 1), (0, 2)], [(0, 1)]]

    writer._drop = set()  # the failure clears
    wall.advance(1_000)  # past the record's short write-failure backoff
    rc.tick()
    assert [_offsets(call) for call in writer.calls][2] == [(0, 1)]  # only the unresolved row is rewritten
    assert _committed_positions(consumer)[-1] == [("knows.dlq", 0, 3)]


def test_one_batch_mixes_written_re_dlq_parked_and_blocked_rows() -> None:
    written = _envelope(source_key="w", original_offset=0)
    retried = _envelope(source_key="r", original_offset=1, attempt_count=0)
    parked = _envelope(source_key="p", original_offset=2, attempt_count=2)
    blocked = _envelope(source_key="b", original_offset=3)
    consumer = _FakeConsumer()
    for envelope in (written, retried, parked, blocked):
        consumer.push(_dlq_message(envelope))
    decide = lambda e: (RETRY_OUTCOME_WRITTEN, "") if e.original_offset == 0 else _UNRESOLVED  # noqa: E731
    writer, sink = _ScriptedWriter(decide=decide), _FakeSink()
    index = _FakeIndex(blocked_ids={("knows-events", 0, 3)})
    notified = []
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, index=index, max_retry_attempts=3,
        on_row_retried=lambda t, o: notified.append(o),
    )

    rc.tick()

    assert _offsets(writer.calls[0]) == [(0, 0), (0, 1), (0, 2)]  # the blocked row never reaches the writer
    assert sink.many_calls == 1  # one publish flush for re-DLQ + park + blocked
    kinds = {c["envelope"].original_offset: (c["parked_topic"], c.get("increment_attempt", True)) for c in sink.calls}
    assert kinds == {1: (None, True), 2: ("knows.dlq.parked", True), 3: (None, False)}
    assert sorted(index.removed) == [("knows-events", 0, 0), ("knows-events", 0, 2)]  # written + parked leave the index
    assert index.remove_many_calls == 1
    assert sorted(notified) == ["blocked", "parked", "re_dlq", "written"]
    assert _committed_positions(consumer) == [[("knows.dlq", 0, 4)]]


class _FailingForOffsetSink(_FakeSink):
    def __init__(self, offset: int) -> None:
        super().__init__()
        self._offset = offset

    def reroute_or_park(self, **kwargs):
        if kwargs["envelope"].original_offset == self._offset:
            raise RuntimeError("simulated publish failure")
        return super().reroute_or_park(**kwargs)


def test_a_failed_re_dlq_publish_leaves_only_that_row_unresolved_and_stops_the_commit_there() -> None:
    consumer = _FakeConsumer()
    for offset in range(3):
        consumer.push(_dlq_message(_envelope(source_key=f"s{offset}", original_offset=offset)))
    writer = _ScriptedWriter(decide=lambda e: _UNRESOLVED)
    sink = _FailingForOffsetSink(offset=1)
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, sink=sink, max_retry_attempts=5)

    rc.tick()

    assert _committed_positions(consumer) == [[("knows.dlq", 0, 1)]]  # row 0 resolved, row 1 not: stop there
    assert [p.offset for p in rc._due_this_tick.values()] == [1]  # only the failed row is re-queued


def test_shutdown_mid_batch_commits_only_what_was_durable_and_stops_polling() -> None:
    shutdown = threading.Event()
    consumer = _FakeConsumer()
    for offset in range(5):
        consumer.push(_dlq_message(_envelope(source_key=f"s{offset}", original_offset=offset)))
    writer = _ScriptedWriter(drop={2, 3, 4})  # the stop abandoned the rest of the flush
    writer.on_call = shutdown.set  # SIGTERM lands while the batch is being written
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer)

    rc.run_forever(shutdown)

    assert len(writer.calls) == 1  # no second flush after the stop
    assert _committed_positions(consumer) == [[("knows.dlq", 0, 2)]]  # rows 0-1 only; 2-4 re-read on restart
    polls_at_exit = consumer.poll_calls
    rc.run_forever(shutdown)  # already stopped: returns without polling again
    assert consumer.poll_calls == polls_at_exit


def test_poll_never_waits_out_a_poll_timeout_while_a_coalescing_batch_is_ready() -> None:
    monotonic = _Clock(start=0.0)
    consumer = _FakeConsumer()
    rc, _fakes = _make_consumer(consumer=consumer, due_batch_coalesce_seconds=0.2, monotonic_clock=monotonic)
    rc.tick()
    assert consumer.poll_timeouts == [0.5]  # idle: the normal poll timeout
    rc._mark_due(_PendingRecordForTest())
    monotonic.advance(0.05)
    assert rc._poll_timeout() == pytest.approx(0.15)  # only the rest of the coalescing window
    monotonic.advance(1.0)
    assert rc._poll_timeout() == 0.0


def _PendingRecordForTest():
    from src.loader.retry_consumer import _PendingRecord

    return _PendingRecord(envelope=_envelope(), topic="knows.dlq", partition=0, offset=0)


def test_revoked_partition_forgets_its_offset_bookkeeping_without_committing() -> None:
    consumer = _FakeConsumer()
    for offset in range(3):
        consumer.push(_dlq_message(_envelope(source_key=f"s{offset}", original_offset=offset)))
    writer = _ScriptedWriter(drop={1})
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer)
    rc.tick()
    committed_before = len(consumer.committed)
    rc.on_partitions_revoked([("knows.dlq", 0)])
    assert not rc._outstanding and not rc._fetched_hi and not rc._committed and not rc._due_this_tick
    rc._commit_resolved()
    assert len(consumer.committed) == committed_before


def test_lease_cell_is_routed_by_the_normalized_record_keys_not_the_wire_keys() -> None:
    """A typed key (a date target) is a JSON value in the envelope but a date in the
    live edge loader; the retry must lease the bucket the live loader would."""
    import datetime
    from types import SimpleNamespace

    from src.loader.lease_client import BucketCellRouter
    from src.loader.retry_consumer import _RetryItem, build_retry_writer

    envelope = _envelope(source_key="p1", target_key="2024-02-14")
    decoded = SimpleNamespace(source_key="p1", target_key=datetime.date(2024, 2, 14))

    class _Base(_RecordingBaseWriter):
        def _record_from_envelope(self, _envelope):
            return decoded

    item = _RetryItem(envelope, _Base()._record_from_envelope)
    assert item.record.target_key == datetime.date(2024, 2, 14)
    router = BucketCellRouter("Person", "Day", 64, frozenset())
    assert router.cell(item.record.source_key, item.record.target_key) == router.cell("p1", decoded.target_key)
    # an undecodable envelope still gets a (wire-key) cell; the writer reports it unresolved
    def _boom(_envelope):
        raise ValueError("bad")
    assert _RetryItem(envelope, _boom).record.source_key == "p1"
    writer = build_retry_writer(_Base(), coordinator=object())
    assert writer._record_from_envelope is not None


# ── Review round: bounds, held records, failures, shutdown, cells ───────────


def _held_and_due_messages(consumer, count, *, first_offset=0, retry_after_ms=1_000):
    for offset in range(first_offset, first_offset + count):
        consumer.push(_dlq_message(_envelope(
            source_key=f"s{offset}", original_offset=offset, retry_after_ms=retry_after_ms,
        )))


def test_a_redelivered_held_offset_is_tracked_once_and_never_leaves_the_partition_paused() -> None:
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    rc, _fakes = _make_consumer(consumer=consumer, wall_clock=wall, max_records_per_partition=2)
    message = _dlq_message(_envelope(original_offset=4, retry_after_ms=20_000))
    rc._on_message(message)
    rc._on_message(message)  # redelivered (e.g. a seek around a pause/resume)
    assert len(rc._held) == 1 and len(rc._wake_heap) == 1 and rc._outstanding[("knows.dlq", 0)] == {4}
    assert consumer.paused == []  # 1 outstanding < the bound of 2: a double count must not reach it
    wall.advance(11_000)
    rc._resume_due_partitions()
    assert len(rc._due_this_tick) == 1 and not rc._held and ("knows.dlq", 0) not in rc._paused


def test_a_redelivered_offset_inside_a_frozen_batch_is_not_processed_twice() -> None:
    consumer = _FakeConsumer()
    writer = _ScriptedWriter()
    index = _FakeIndex(caught_up=False)  # keeps the batch frozen
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, index=index)
    message = _dlq_message(_envelope(original_offset=0))
    rc._on_message(message)
    rc._maybe_flush_due_batch()
    assert rc._pending_batches and not rc._due_this_tick
    rc._on_message(message)
    assert not rc._due_this_tick  # still only the one copy, inside the frozen batch
    index._caught_up_override = True
    rc.tick()
    assert [_offsets(call) for call in writer.calls] == [[(0, 0)]]


def test_a_frozen_batch_holds_at_most_max_batch_size_lowest_offsets_first() -> None:
    consumer = _FakeConsumer()
    rc, _fakes = _make_consumer(consumer=consumer, max_batch_size=10, index=_FakeIndex(caught_up=False))
    for offset in range(25):
        rc._on_message(_dlq_message(_envelope(source_key=f"s{offset}", original_offset=offset)))
    rc._freeze_due_batches()
    frozen = sorted(p.offset for _k, p in rc._pending_batches["KNOWS"].keyed_pendings)
    assert frozen == list(range(10))
    assert sorted(key[2] for key in rc._due_this_tick) == list(range(10, 25))  # the rest wait their turn


def test_a_spent_flush_budget_starts_no_further_write_slice() -> None:
    monotonic = _Clock(start=0.0)
    consumer = _FakeConsumer()
    _held_and_due_messages(consumer, 5)
    writer = _ScriptedWriter()
    writer.on_call = lambda: monotonic.advance(100.0)  # one slice outlasts the whole budget
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, monotonic_clock=monotonic)
    rc._write_slice_size = 2
    rc._flush_budget_seconds = 60.0
    rc.tick()
    assert [len(call) for call in writer.calls] == [2]  # the other slices were not started
    assert _committed_positions(consumer) == [[("knows.dlq", 0, 2)]]
    assert len(rc._due_this_tick) == 3  # still outstanding, due again next tick


def test_a_not_yet_due_record_pauses_the_partition_only_at_the_outstanding_bound() -> None:
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    rc, _fakes = _make_consumer(consumer=consumer, wall_clock=wall, max_records_per_partition=3)
    for offset in range(3):
        rc._on_message(_dlq_message(_envelope(source_key=f"s{offset}", original_offset=offset, retry_after_ms=20_000)))
        assert bool(consumer.paused) == (offset == 2)


def test_held_wake_time_is_clamped_to_now_plus_max_delay_against_clock_skew() -> None:
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    rc, _fakes = _make_consumer(consumer=consumer, wall_clock=wall, max_delay_seconds=60.0)
    rc._on_message(_dlq_message(_envelope(retry_after_ms=10**15)))  # a producer clock far ahead
    assert rc._wake_heap[0][0] == 10_000 + 60_000


def test_a_batch_publish_failure_leaves_every_record_outstanding_and_does_not_crash_the_tick() -> None:
    class _RaisingManySink(_FakeSink):
        def reroute_or_park_many(self, requests):
            raise RuntimeError("producer down")

    consumer = _FakeConsumer()
    _held_and_due_messages(consumer, 3)
    writer = _ScriptedWriter(decide=lambda e: _UNRESOLVED)
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, sink=_RaisingManySink(), max_retry_attempts=5)
    rc.tick()  # must not raise
    assert consumer.committed == []
    assert len(rc._held) == 3  # outstanding, backed off


def test_an_index_removal_failure_leaves_the_written_rows_outstanding_and_does_not_crash() -> None:
    class _RaisingRemoveIndex(_FakeIndex):
        def remove_many(self, records):
            raise RuntimeError("index producer down")

    consumer = _FakeConsumer()
    _held_and_due_messages(consumer, 2)
    rc, _fakes = _make_consumer(consumer=consumer, writer=_ScriptedWriter(), index=_RaisingRemoveIndex())
    rc.tick()  # must not raise
    assert consumer.committed == []
    assert len(rc._held) == 2  # outstanding, backed off


class _PoisonRowWriter:
    """Fails (raises) for any call that includes the poison offset."""

    def __init__(self, poison_offset: int) -> None:
        self.calls: list[list[int]] = []
        self._poison = poison_offset

    def retry_batch(self, envelopes, budget_seconds=None):
        self.calls.append([e.original_offset for e in envelopes])
        if any(e.original_offset == self._poison for e in envelopes):
            raise _row_error("persistent ClientError")
        return {i: (RETRY_OUTCOME_WRITTEN, "") for i in range(len(envelopes))}


def test_a_row_that_always_fails_is_isolated_written_around_and_finally_escalated_to_write_failed() -> None:
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    _held_and_due_messages(consumer, 8)
    writer, sink = _PoisonRowWriter(poison_offset=5), _FakeSink()
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, max_retry_attempts=5, wall_clock=wall,
    )

    rc.tick()
    # the other seven rows were written despite the poison row sharing their slice
    assert _committed_positions(consumer) == [[("knows.dlq", 0, 5)]]  # stops below the poison row
    assert sink.calls == []  # one isolated failure is not yet an escalation

    for _ in range(6):
        wall.advance(6_000)  # past the write-failure backoff
        rc.tick()

    (call,) = sink.calls
    assert call["reason_code"] == "write_failed" and call["envelope"].original_offset == 5
    assert call["parked_topic"] is None  # the normal attempts-then-park path (attempt 1 of 5)
    assert _committed_positions(consumer)[-1] == [("knows.dlq", 0, 8)]
    assert not rc._write_failures


def test_no_new_batch_is_frozen_or_started_once_the_stop_event_is_set() -> None:
    stop = threading.Event()
    consumer = _FakeConsumer()
    _held_and_due_messages(consumer, 3)
    writer = _ScriptedWriter()
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer)
    rc._stop_event = stop
    stop.set()
    rc.tick()
    assert writer.calls == [] and not rc._pending_batches
    rc._on_message(_dlq_message(_envelope(original_offset=9)))
    rc._maybe_flush_due_batch()
    assert writer.calls == [] and not rc._pending_batches


def test_partition_assignment_defensively_resumes_and_resets_the_pause_bookkeeping() -> None:
    consumer = _FakeConsumer()
    rc, _fakes = _make_consumer(consumer=consumer, max_records_per_partition=2)
    rc._paused.add(("knows.dlq", 3))  # stale bookkeeping from before a rebalance
    rc.on_partitions_assigned([("knows.dlq", 3)])
    assert ("knows.dlq", 3) not in rc._paused
    assert [(tp.topic, tp.partition) for call in consumer.resumed for tp in call] == [("knows.dlq", 3)]


def test_a_lease_coordinated_writer_requires_normalized_key_routing() -> None:
    from src.loader.retry_consumer import build_retry_writer

    with pytest.raises(RetryConsumerError, match="_record_from_envelope"):
        build_retry_writer(object(), coordinator=object())


def test_standalone_adapter_shares_one_parked_pair_set_across_chunks_and_never_sorts_rows() -> None:
    from src.loader.retry_consumer import ApocRetryWriterAdapter, build_retry_base_writer
    from src.utils.schema_loader import load_schema

    base = _RecordingBaseWriter()
    ApocRetryWriterAdapter(base, chunk_size=2).retry_batch(
        [_envelope(source_key=f"p{i}", original_offset=i) for i in range(5)]
    )
    sets = {id(kw["parked_pairs"]) for kw in base.kwargs}
    assert len(base.kwargs) == 3 and len(sets) == 1 and None not in {kw["parked_pairs"] for kw in base.kwargs}

    edge = next(e for e in load_schema("config/graph_schema.yaml").edges if e.type == "WORKS_AT")
    assert "ORDER BY elementId(s), elementId(t)" not in build_retry_base_writer(MagicMock(), edge, tx_timeout_seconds=30.0)._query


def test_lease_cell_is_routed_by_the_normalized_keys_of_a_real_writer_not_the_wire_keys() -> None:
    """A date target is a JSON string in the envelope but a ``date`` in the live edge loader."""
    import dataclasses

    from src.loader.edge_execution import ApocLockedEdgeWriter
    from src.loader.edge_loader import MissingRelationshipEndpointError
    from src.loader.edge_records import normalize_edge_record
    from src.loader.lease_client import BucketCellRouter
    from src.loader.retry_consumer import _RetryItem, build_retry_writer
    from src.utils.schema_loader import load_schema

    edge = next(e for e in load_schema("examples/social/graph_schema.yaml").edges if e.type == "ACTIVE_ON")
    raw = {"source": {"person_id": 5}, "target": {"day": "2024-02-14"}, "properties": {"sessions": 3}}
    writer = ApocLockedEdgeWriter(MagicMock(), edge, missing_endpoint_error_factory=MissingRelationshipEndpointError)
    envelope = dataclasses.replace(
        _envelope(entity_name="ACTIVE_ON", source_key=5, target_key="2024-02-14"),
        value_bytes=json.dumps(raw).encode("utf-8"),
    )
    live = normalize_edge_record(raw, edge)  # what the live loader routes by
    router = BucketCellRouter(edge.nodes.source, edge.nodes.target, 64, frozenset())
    item = _RetryItem(envelope, writer._record_from_envelope)
    assert router.cell(item.record.source_key, item.record.target_key) == router.cell(live.source_key, live.target_key)
    assert router.cell(item.record.source_key, item.record.target_key) != router.cell(5, "2024-02-14")
    assert build_retry_writer(writer, coordinator=object())._record_from_envelope == writer._record_from_envelope


class _OverlapLeaseClient:
    """A lease server stand-in that really refuses overlapping resource sets."""

    broken = False
    _shared = None

    def __init__(self, shared):
        self._s = shared

    def connect(self):
        pass

    def close(self):
        pass

    def ping(self):
        return True

    def extend(self, lease_id, weighted_sets, *, budget):
        return []

    def acquire_any(self, resource_sets, *, wait_ms):
        import time as _time

        deadline = _time.monotonic() + wait_ms / 1000
        while True:
            with self._s["lock"]:
                for index, resources in enumerate(resource_sets):
                    if not self._s["held"].intersection(resources):
                        self._s["seq"] += 1
                        lease_id = f"l{self._s['seq']}"
                        self._s["held"].update(resources)
                        self._s["leases"][lease_id] = set(resources)
                        return lease_id, index
            if _time.monotonic() >= deadline:
                return None
            _time.sleep(0.001)

    def release(self, lease_id):
        with self._s["lock"]:
            self._s["held"].difference_update(self._s["leases"].pop(lease_id))


def test_multi_worker_retry_flush_keeps_transactions_node_disjoint_and_pairs_fifo() -> None:
    import time as _time

    shared = {"lock": threading.Lock(), "held": set(), "leases": {}, "seq": 0}
    active: dict[int, set] = {}
    state = {"violations": 0, "order": {}}
    lock = threading.Lock()

    class _Base:
        def retry_provenanced_batch(self, envelopes, **kwargs):
            nodes = set()
            for envelope in envelopes:
                source_key, target_key = json.loads(envelope.key_bytes)
                nodes.update({("s", source_key), ("t", target_key)})
            me = threading.get_ident()
            with lock:
                if any(nodes & other for other in active.values()):
                    state["violations"] += 1
                active[me] = nodes
                for envelope in envelopes:
                    state["order"].setdefault(envelope.key_bytes, []).append(envelope.original_offset)
            _time.sleep(0.002)
            with lock:
                del active[me]
            return {i: (RETRY_OUTCOME_WRITTEN, "") for i in range(len(envelopes))}

    from src.loader.edge_execution import BucketLeaseExecutionCoordinator
    from src.loader.lease_client import BucketCellRouter

    coordinator = BucketLeaseExecutionCoordinator(
        RetryCoordinatorBridge(_Base()), router=BucketCellRouter("Person", "Company", 4, frozenset()),
        worker_count=4, batch_size=3, client_factory=lambda: _OverlapLeaseClient(shared),
        edge_type="KNOWS", tx_timeout_seconds=30.0,
    )
    writer = LeaseCoordinatedRetryWriter(coordinator)
    envelopes = [
        _envelope(source_key=f"p{i % 7}", target_key=f"c{i % 5}", original_offset=i) for i in range(120)
    ]

    outcomes = writer.retry_batch(envelopes)

    assert len(outcomes) == 120
    assert state["violations"] == 0  # no two concurrent transactions touched the same node
    for offsets in state["order"].values():
        assert offsets == sorted(offsets)  # a pair's rows were written in original order
    assert shared["held"] == set()  # every lease released


# ── Outage vs poison row: a dead dependency never parks good rows ───────────


class _ToggleWriter:
    """Raises ``error`` (when set) for every call, else writes everything;
    records the time of every call."""

    def __init__(self, clock, error=None) -> None:
        self._clock, self.error = clock, error
        self.call_times: list[float] = []
        self.written: list[int] = []

    def retry_batch(self, envelopes, budget_seconds=None):
        self.call_times.append(self._clock())
        if self.error is not None:
            # A fresh traceback each time: re-raising one instance grows it forever.
            raise self.error.with_traceback(None)
        self.written.extend(e.original_offset for e in envelopes)
        return {i: (RETRY_OUTCOME_WRITTEN, "") for i in range(len(envelopes))}


_TIMED_OUT = _neo_error("Neo.ClientError.Transaction.TransactionTimedOut", "timed out")


def _infra_errors():
    from src.loader.edge_execution import LeaseDrainAbandoned
    from src.loader.lease_client import LeaseCoordinatorError

    return [
        ServiceUnavailable("neo4j down"), TransientError("deadlock"), TimeoutError("lease"),
        ConnectionAcquisitionTimeoutError("pool exhausted"), LeaseCoordinatorError("lease server down"),
        LeaseDrainAbandoned("budget spent"), _neo_error("Neo.ClientError.Database.DatabaseNotFound"),
        _neo_error("Neo.ClientError.Security.Unauthorized"),
    ]


@pytest.mark.parametrize("error", _infra_errors(), ids=lambda e: type(e).__name__)
def test_an_outage_neither_escalates_nor_parks_and_backs_off_then_recovers(error) -> None:
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    for offset in range(200):
        consumer.push(_dlq_message(_envelope(
            source_key=f"s{offset}", original_offset=offset, attempt_count=4, retry_after_ms=1_000,
        )))
    writer, sink = _ToggleWriter(lambda: wall() / 1000.0, error), _FakeSink()
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, max_retry_attempts=5, wall_clock=wall,
        monotonic_clock=lambda: wall() / 1000.0,
    )
    for _ in range(40):  # a 40 s outage, ticked once per second
        rc.tick()
        wall.advance(1_000)
    assert sink.calls == []  # nothing re-DLQ'd, nothing parked
    assert not rc._write_failures
    assert len(rc._outstanding[("knows.dlq", 0)]) == 200 and consumer.committed == []
    gaps = [b - a for a, b in zip(writer.call_times, writer.call_times[1:])]
    assert 2 <= len(writer.call_times) <= 12  # backed off (a retry storm would be ~40 calls)
    assert gaps == sorted(gaps) and max(gaps) <= 5.0 + 1.0  # capped exponential

    writer.error = None  # the dependency recovers
    for _ in range(10):
        rc.tick()
        wall.advance(1_000)
    assert sorted(writer.written) == list(range(200))
    assert sink.calls == []
    assert _committed_positions(consumer)[-1] == [("knows.dlq", 0, 200)]


def test_a_lone_poison_row_backs_off_and_terminates_via_write_failed_then_park() -> None:
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    sink = _FakeSink()
    writer = _ToggleWriter(lambda: wall() / 1000.0, _row_error("constraint violated"))
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, max_retry_attempts=3, wall_clock=wall,
        monotonic_clock=lambda: wall() / 1000.0,
    )
    consumer.push(_dlq_message(_envelope(original_offset=0, attempt_count=2, retry_after_ms=1_000)))
    for _ in range(30):
        rc.tick()
        wall.advance(500)
        if sink.calls:
            break
    (call,) = sink.calls
    assert call["reason_code"] == "write_failed" and call["parked_topic"] == "knows.dlq.parked"
    # three solo failures, each after a longer wait -- never one call per tick
    gaps = [b - a for a, b in zip(writer.call_times, writer.call_times[1:])]
    assert len(writer.call_times) == 3 and gaps == sorted(gaps) and gaps[0] >= 0.5 and gaps[1] >= 1.0
    assert _committed_positions(consumer)[-1] == [("knows.dlq", 0, 1)]


def test_a_lone_unclassified_failure_is_never_counted_without_evidence_and_surfaces_a_stall(caplog) -> None:
    """D1: no WRITTEN of the edge type ever happens, so the lone row's unknown
    failures are never counted -- it waits (backing off) for two hours, never
    parks, and the stall is logged as an ERROR and reported on the gauge."""
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(_envelope(original_offset=0)))
    sink = _FakeSink()
    stalls: list[tuple[str, float]] = []
    writer = _ToggleWriter(lambda: wall() / 1000.0, RuntimeError("who knows"))
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, max_retry_attempts=5, wall_clock=wall,
        monotonic_clock=lambda: wall() / 1000.0,
    )
    rc._on_stall_seconds = lambda edge_type, seconds: stalls.append((edge_type, seconds))
    for _ in range(7_200):  # 2 h at 1 s ticks: far longer than any old floor or cap
        rc.tick()
        wall.advance(1_000)
    assert sink.calls == [] and not rc._write_failures
    assert len(writer.call_times) <= 7_200 / 5 + 10  # at most one call per 5 s backoff
    assert stalls[-1][0] == "KNOWS" and stalls[-1][1] >= 7_000
    assert "reason=retry_stalled" in caplog.text


@pytest.mark.parametrize("poison_error", [RuntimeError("unknown"), _TIMED_OUT, _neo_error(
    "Neo.TransientError.General.TransactionMemoryLimit")], ids=["unknown", "timeout", "memory"])
def test_a_multi_row_first_slice_with_one_poison_terminates_while_same_type_writes_happen(poison_error) -> None:
    class _Poison:
        def __init__(self) -> None:
            self.written: list[int] = []

        def retry_batch(self, envelopes, budget_seconds=None):
            if any(e.original_offset == 4 for e in envelopes):
                raise poison_error
            self.written.extend(e.original_offset for e in envelopes)
            return {i: (RETRY_OUTCOME_WRITTEN, "") for i in range(len(envelopes))}

    wall = _Clock(start=10_000)
    consumer, sink, writer = _FakeConsumer(), _FakeSink(), _Poison()
    _held_and_due_messages(consumer, 10)
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, max_retry_attempts=5, wall_clock=wall,
        monotonic_clock=lambda: wall() / 1000.0,
    )
    for tick in range(70):
        if tick % 3 == 2:  # same-type traffic keeps being written: evidence the server is healthy
            consumer.push(_dlq_message(_envelope(source_key=f"live{tick}", original_offset=100 + tick)))
        rc.tick()
        wall.advance(6_000)
    assert {0, 1, 2, 3, 5, 6, 7, 8, 9} <= set(writer.written)  # the good rows are never starved
    (call,) = sink.calls  # the poison row, counted because the server demonstrably writes
    assert call["envelope"].original_offset == 4 and call["reason_code"] == "write_failed"
    assert not rc._outstanding[("knows.dlq", 0)]


def test_a_mixed_row_and_connectivity_batch_counts_only_the_row_and_stops_at_the_outage() -> None:
    class _Mixed:
        def retry_batch(self, envelopes, budget_seconds=None):
            offsets = {e.original_offset for e in envelopes}
            if 2 in offsets:
                raise _row_error("bad row")
            if 6 in offsets:
                raise ServiceUnavailable("neo4j went away")
            return {i: (RETRY_OUTCOME_WRITTEN, "") for i in range(len(envelopes))}

    consumer = _FakeConsumer()
    _held_and_due_messages(consumer, 10)
    rc, _fakes = _make_consumer(consumer=consumer, writer=_Mixed(), sink=_FakeSink(), max_retry_attempts=5)
    rc.tick()
    assert rc._write_failures == {("knows.dlq", 0, 2): 1}  # the row error counts; the outage does not
    assert sorted(rc._outstanding[("knows.dlq", 0)]) == [2, 5, 6, 7, 8, 9]


@pytest.mark.parametrize("exc, expected", [
    (_neo_error("Neo.ClientError.Statement.TypeError"), "row"),
    (_neo_error("Neo.ClientError.Statement.ArgumentError"), "row"),
    (_neo_error("Neo.ClientError.Statement.ArithmeticError"), "row"),
    (_neo_error("Neo.ClientError.Statement.EntityNotFound"), "row"),
    (_neo_error("Neo.ClientError.Statement.PropertyNotFound"), "row"),
    (_neo_error("Neo.ClientError.Schema.ConstraintValidationFailed"), "row"),
    (_neo_error("Neo.ClientError.Schema.ConstraintViolation"), "row"),
    (_neo_error("Neo.ClientError.Schema.ConstraintVerificationFailed"), "row"),
    (_neo_error("Neo.ClientError.General.WriteOnReadOnlyAccessDatabase"), "connectivity"),
    (_neo_error("Neo.ClientError.General.ForbiddenOnReadOnlyDatabase"), "connectivity"),
    (_neo_error("Neo.ClientError.General.ReadOnly"), "connectivity"),
    (_neo_error("Neo.ClientError.Procedure.ProcedureNotFound"), "unknown"),
    (_neo_error("Neo.ClientError.Statement.SyntaxError"), "unknown"),
    (ClientError("plain"), "unknown"),
    (RuntimeError("bug"), "unknown"),
    (_neo_error("Neo.ClientError.Database.DatabaseNotFound"), "connectivity"),
    (_neo_error("Neo.ClientError.Security.Unauthorized"), "connectivity"),
    (_neo_error("Neo.ClientError.Cluster.NotALeader"), "connectivity"),
    (ServiceUnavailable("x"), "connectivity"),
    (TransientError("deadlock"), "connectivity"),
    (ConnectionAcquisitionTimeoutError("pool"), "connectivity"),
    (TimeoutError("x"), "connectivity"),
    (_neo_error("Neo.ClientError.Transaction.TransactionTimedOut"), "capacity"),
    (_neo_error("Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration"), "capacity"),
    (_neo_error("Neo.TransientError.General.TransactionMemoryLimit"), "capacity"),
    (_neo_error("Neo.TransientError.General.MemoryPoolOutOfMemoryError"), "server"),
])
def test_write_error_classification_table(exc, expected) -> None:
    from src.loader.retry_consumer import _classify_write_error

    assert _classify_write_error(exc) == expected


def test_lease_errors_classify_as_connectivity() -> None:
    from src.loader.edge_execution import LeaseDrainAbandoned
    from src.loader.lease_client import LeaseCoordinatorError
    from src.loader.retry_consumer import _classify_write_error

    assert _classify_write_error(LeaseCoordinatorError("x")) == "connectivity"
    assert _classify_write_error(LeaseDrainAbandoned("x")) == "connectivity"


_READ_ONLY = _neo_error("Neo.ClientError.General.WriteOnReadOnlyAccessDatabase", "read only")


class _RepublishingSink:
    """Re-publishes a re-DLQ'd (or blocked) envelope back to the consumer at a
    new DLQ offset, like the real topic; records parks."""

    def __init__(self, consumer, wall, *, next_offset=10_000) -> None:
        self.consumer, self.wall, self.next_offset = consumer, wall, next_offset
        self.parked: list[tuple[bytes, str, int]] = []
        self.calls: list[dict] = []

    def reroute_or_park_many(self, requests):
        import dataclasses

        out = []
        for request in requests:
            self.calls.append(request)
            envelope = request["envelope"]
            if request.get("parked_topic"):
                self.parked.append((envelope.key_bytes, request["reason_code"], envelope.attempt_count))
                out.append("parked")
                continue
            attempt = envelope.attempt_count + (1 if request.get("increment_attempt", True) else 0)
            new = dataclasses.replace(
                envelope, attempt_count=attempt,
                retry_after_ms=int(self.wall() + request["retry_after_seconds_for_reattempt"] * 1000),
            )
            self.consumer.push(_dlq_message(new, offset=self.next_offset))
            self.next_offset += 1
            out.append("re_dlq")
        return out

    def push_new(self, envelope) -> None:
        self.consumer.push(_dlq_message(envelope, offset=self.next_offset))
        self.next_offset += 1


class _ReadOnlyAwareWriter:
    """Rows whose source key starts with "g" have both endpoints; the others
    are missing an endpoint. Like the real writer, a transaction with only
    missing rows runs only the classification read and commits (UNRESOLVED),
    while one with a matched row runs the write query -- which a read-only
    database rejects."""

    def __init__(self) -> None:
        self.read_only = True
        self.calls: list[list[str]] = []
        self.written: list[str] = []

    def retry_batch(self, envelopes, budget_seconds=None):
        sources = [json.loads(e.key_bytes)[0] for e in envelopes]
        self.calls.append(sources)
        if self.read_only and any(source.startswith("g") for source in sources):
            raise _READ_ONLY.with_traceback(None)
        outcomes = {}
        for index, source in enumerate(sources):
            if source.startswith("g"):
                outcomes[index] = (RETRY_OUTCOME_WRITTEN, "")
                self.written.append(source)
            else:
                outcomes[index] = (RETRY_OUTCOME_UNRESOLVED, "missing_target")
        return outcomes


def test_read_only_database_does_not_park_good_rows_for_a_long_maintenance_window(caplog) -> None:
    """M1/M4/D1: a 40-minute read-only window -- far longer than any old floor
    -- with a few good rows (one attempt from parking, so solo tries happen) and
    steady missing-endpoint traffic whose UNRESOLVED outcomes ran only a read:
    nothing parks, nothing is counted, no storm; the stall is surfaced; the
    good rows are written once the database is writable again."""
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    sink = _RepublishingSink(consumer, wall)
    writer = _ReadOnlyAwareWriter()
    stalls: dict[str, float] = {}
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, max_retry_attempts=100, max_delay_seconds=60.0,
        wall_clock=wall, monotonic_clock=lambda: wall() / 1000.0,
    )
    rc._on_stall_seconds = lambda edge_type, seconds: stalls.__setitem__(edge_type, seconds)
    for offset in range(3):
        sink.push_new(_envelope(source_key=f"g{offset}", original_offset=offset, attempt_count=99))
    window = 40 * 60
    for second in range(window):
        if second % 2 == 0:
            sink.push_new(_envelope(source_key=f"m{second}", original_offset=1_000 + second))
        rc.tick()
        wall.advance(1_000)
    assert sink.parked == []
    assert not any(call["reason_code"] == "write_failed" for call in sink.calls)
    assert not any(json.loads(call["envelope"].key_bytes)[0].startswith("g") for call in sink.calls)
    assert not rc._write_failures
    good_calls = [call for call in writer.calls if any(source.startswith("g") for source in call)]
    assert len(good_calls) <= window / 5 + 10  # one failing write per 5 s backoff at most: no storm
    assert any(len(call) == 1 and call[0].startswith("g") for call in good_calls)  # solo tries happened
    assert stalls["KNOWS"] >= window - 30 and "reason=retry_stalled" in caplog.text

    writer.read_only = False  # ALTER DATABASE ... SET ACCESS READ WRITE
    for _ in range(30):
        rc.tick()
        wall.advance(1_000)
    assert sorted(writer.written) == ["g0", "g1", "g2"] and sink.parked == []
    assert stalls["KNOWS"] == 0.0 and "KNOWS" not in rc._stall


def test_a_sustained_authentication_failure_logs_then_exits_non_zero(caplog) -> None:
    from src.loader.retry_consumer import RetryConsumerFatalError

    wall = _Clock(start=10_000)
    consumer, sink = _FakeConsumer(), _FakeSink()
    _held_and_due_messages(consumer, 3)
    writer = _ToggleWriter(lambda: wall() / 1000.0, _neo_error("Neo.ClientError.Security.Unauthorized"))
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, wall_clock=wall, monotonic_clock=lambda: wall() / 1000.0,
    )
    with pytest.raises(RetryConsumerFatalError):
        for _ in range(200):
            rc.tick()
            wall.advance(6_000)
    assert sink.calls == []
    assert "reason=retry_stalled" in caplog.text


def test_a_recovered_write_resets_the_outage_clock() -> None:
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    _held_and_due_messages(consumer, 2)
    writer = _ToggleWriter(lambda: wall() / 1000.0, _neo_error("Neo.ClientError.Security.Unauthorized"))
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, wall_clock=wall, monotonic_clock=lambda: wall() / 1000.0)
    rc.tick()
    assert "KNOWS" in rc._fatal and "KNOWS" in rc._stall
    writer.error = None
    wall.advance(6_000)
    rc.tick()
    assert "KNOWS" not in rc._fatal and "KNOWS" not in rc._stall and "KNOWS" not in rc._failing


def test_an_exhausted_isolation_budget_leaves_the_rest_outstanding_without_crashing() -> None:
    from src.loader.retry_consumer import _ISOLATION_CALL_BUDGET

    class _AllPoison:
        calls = 0

        def retry_batch(self, envelopes, budget_seconds=None):
            self.calls += 1
            raise _row_error("bad row")

    consumer = _FakeConsumer()
    _held_and_due_messages(consumer, 200)
    writer, sink = _AllPoison(), _FakeSink()
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, sink=sink, max_retry_attempts=5)
    rc.tick()
    assert writer.calls <= 1 + _ISOLATION_CALL_BUDGET + 1
    assert sink.calls == [] and consumer.committed == []
    assert len(rc._outstanding[("knows.dlq", 0)]) == 200


# ── Same-pair ordering: bisection, holding, real index semantics ────────────


class _RealSemanticsIndex(ParkedPairIndex):
    """The real ParkedPairIndex ``contains``/``is_blocked_by_earlier``
    semantics over an in-memory view (no Kafka)."""

    def __init__(self) -> None:
        super().__init__(topic="knows.idx", bootstrap_servers="unused", writable=True)

    def seed(self, envelope: DeadLetterEnvelope) -> None:
        source_key, target_key = json.loads(envelope.key_bytes)
        wire_key = _record_wire_key(
            source_key, target_key, envelope.original_topic, envelope.original_partition, envelope.original_offset,
        )
        self._by_pair.setdefault(_pair_key(source_key, target_key), {})[wire_key] = _OutstandingRecord(
            envelope.original_topic, envelope.original_partition, envelope.original_offset,
            envelope.first_failure_at_ms,
        )

    def watermark_snapshot(self):
        return {}

    def caught_up(self, snapshot) -> bool:
        return True

    def remove_many(self, records):
        for source_key, target_key, topic, partition, offset in records:
            self._by_pair.get(_pair_key(source_key, target_key), {}).pop(
                _record_wire_key(source_key, target_key, topic, partition, offset), None,
            )
        return [None] * len(records)


class _PairPoisonWriter:
    """Offset ``poison`` always fails with a row-specific error; everything
    else is written, in call order."""

    def __init__(self, poison: int | None) -> None:
        self.poison, self.written, self.calls = poison, [], []

    def retry_batch(self, envelopes, budget_seconds=None):
        self.calls.append([e.original_offset for e in envelopes])
        if any(e.original_offset == self.poison for e in envelopes):
            raise _row_error("bad row")
        self.written.extend(e.original_offset for e in envelopes)
        return {i: (RETRY_OUTCOME_WRITTEN, "") for i in range(len(envelopes))}


def _pair_rows(consumer, index=None):
    """Offsets 0 and 1 share a pair; offsets 2..5 are independent."""
    envelopes = [_envelope(source_key="a", target_key="b", original_offset=i) for i in (0, 1)] + [
        _envelope(source_key=f"s{i}", target_key="t", original_offset=i) for i in range(2, 6)
    ]
    for envelope in envelopes:
        if index is not None:
            index.seed(envelope)
        consumer.push(_dlq_message(envelope))
    return envelopes


def test_with_the_real_index_a_later_row_of_a_pair_is_never_written_ahead_of_a_failing_earlier_row() -> None:
    wall = _Clock(start=10_000)
    consumer, index, sink = _FakeConsumer(), _RealSemanticsIndex(), _FakeSink()
    _pair_rows(consumer, index)
    writer = _PairPoisonWriter(poison=0)
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, index=index, max_retry_attempts=9, wall_clock=wall,
    )
    for _ in range(6):
        rc.tick()
        wall.advance(2_000)
    assert 1 not in writer.written  # row 1 sits behind poison row 0 for the whole run
    assert {2, 3, 4, 5} <= set(writer.written)
    assert any(call["envelope"].original_offset == 1 and call["increment_attempt"] is False for call in sink.calls)


def test_standalone_bisection_never_writes_a_later_row_of_a_pair_past_its_failing_earlier_row() -> None:
    consumer = _FakeConsumer()
    _pair_rows(consumer)
    writer = _PairPoisonWriter(poison=0)
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer)
    rc.tick()
    assert 1 not in writer.written and {2, 3, 4, 5} <= set(writer.written)
    assert consumer.committed == []  # offset 0 is unresolved: nothing can commit


def test_standalone_a_later_row_is_held_locally_behind_a_held_earlier_row_and_written_after_it() -> None:
    wall = _Clock(start=10_000)
    consumer, sink = _FakeConsumer(), _FakeSink()
    first = _envelope(source_key="a", target_key="b", original_offset=0)
    second = _envelope(source_key="a", target_key="b", original_offset=1)
    writer = _PairPoisonWriter(poison=0)
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, sink=sink, max_retry_attempts=9, wall_clock=wall)
    consumer.push(_dlq_message(first))
    rc.tick()  # row 0 fails alone and is held for its backoff
    assert list(rc._held) == [("knows.dlq", 0, 0)]
    consumer.push(_dlq_message(second))
    rc.tick()  # row 1 arrives due: held locally behind row 0, nothing written, nothing published
    assert sorted(rc._held) == [("knows.dlq", 0, 0), ("knows.dlq", 0, 1)]
    assert writer.written == [] and sink.calls == []
    writer.poison = None  # row 0 recovers
    wall.advance(1_000)
    rc.tick()
    assert writer.written == [0, 1] and sink.calls == []
    assert _committed_positions(consumer)[-1] == [("knows.dlq", 0, 2)]


def test_a_blocked_row_waits_out_the_blockers_remaining_backoff_instead_of_republishing_every_half_second() -> None:
    wall = _Clock(start=10_000)
    consumer, index, sink = _FakeConsumer(), _RealSemanticsIndex(), _FakeSink()
    first = _envelope(source_key="a", target_key="b", original_offset=0, attempt_count=2)
    second = _envelope(source_key="a", target_key="b", original_offset=1)
    for envelope in (first, second):
        index.seed(envelope)
        consumer.push(_dlq_message(envelope))
    writer = _ScriptedWriter(decide=lambda e: _UNRESOLVED, pair_guard=True)
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, index=index, max_retry_attempts=9, wall_clock=wall,
        base_delay_seconds=1.0, max_delay_seconds=60.0,
    )
    rc.tick()
    by_offset = {call["envelope"].original_offset: call for call in sink.calls}
    assert by_offset[0]["retry_after_seconds_for_reattempt"] == pytest.approx(4.0)  # attempt 3: 1 * 2**2
    # v7-S4 M1: the blocked row waits out the blocker's remaining backoff locally
    assert 1 not in by_offset
    assert _held_wake_ms(rc, 1) == 10_000 + 4_000 + 1


# ── Fairness, redelivery, publish-failure recovery ──────────────────────────


def test_the_frozen_batch_cap_interleaves_partitions_instead_of_starving_high_offsets() -> None:
    consumer = _FakeConsumer()
    rc, _fakes = _make_consumer(consumer=consumer, max_batch_size=10, index=_FakeIndex(caught_up=False))
    for i in range(25):
        rc._on_message(_dlq_message(_envelope(source_key=f"a{i}", original_partition=0, original_offset=i)))
        rc._on_message(_dlq_message(_envelope(source_key=f"b{i}", original_partition=1, original_offset=1_000 + i)))
    rc._freeze_due_batches()
    frozen = sorted((p.partition, p.offset) for _k, p in rc._pending_batches["KNOWS"].keyed_pendings)
    assert frozen == [(0, i) for i in range(5)] + [(1, 1_000 + i) for i in range(5)]


def test_a_resolved_but_uncommitted_offset_redelivered_is_ignored() -> None:
    class _CommitFailsConsumer(_FakeConsumer):
        def commit(self, **kwargs):
            raise RuntimeError("broker unavailable")

    consumer = _CommitFailsConsumer()
    message = _dlq_message(_envelope(original_offset=3))
    consumer.push(message)
    writer = _ScriptedWriter()
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer)
    rc.tick()
    assert len(writer.calls) == 1 and not rc._outstanding.get(("knows.dlq", 0))  # resolved, commit failed
    rc._on_message(message)  # redelivered
    assert not rc._due_this_tick and not rc._outstanding.get(("knows.dlq", 0))
    rc.tick()
    assert len(writer.calls) == 1  # never written twice


def test_a_later_tick_recovers_after_a_publish_failure() -> None:
    class _FlakySink(_FakeSink):
        def __init__(self) -> None:
            super().__init__()
            self.fail = True

        def reroute_or_park_many(self, requests):
            if self.fail:
                raise RuntimeError("producer down")
            return super().reroute_or_park_many(requests)

    wall = _Clock(start=10_000)
    consumer, sink = _FakeConsumer(), _FlakySink()
    _held_and_due_messages(consumer, 3)
    writer = _ScriptedWriter(decide=lambda e: _UNRESOLVED)
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, sink=sink, max_retry_attempts=5, wall_clock=wall)
    rc.tick()
    assert consumer.committed == [] and sink.calls == []
    sink.fail = False
    wall.advance(1_000)
    rc.tick()
    assert sorted(call["envelope"].original_offset for call in sink.calls) == [0, 1, 2]
    assert _committed_positions(consumer)[-1] == [("knows.dlq", 0, 3)]


# ── Flush budget reaches the lease coordinator (S5) and the backlog source (N12) ─


def test_leased_retry_writer_abandons_when_the_flush_budget_is_spent_before_a_lease_is_granted():
    from src.loader.edge_execution import LeaseDrainAbandoned

    base = _RecordingBaseWriter()
    lease_client = _FakeLeaseClient(grants=False)
    writer = _leased_writer(base, lease_client)
    with pytest.raises(LeaseDrainAbandoned, match="flush time budget"):
        writer.retry_batch([_envelope(source_key="a", target_key="b")], budget_seconds=0.0)
    assert base.calls == [] and lease_client.acquire_calls == []


def test_the_consumer_passes_its_remaining_flush_budget_to_the_writer() -> None:
    monotonic = _Clock(start=100.0)
    consumer = _FakeConsumer()
    _held_and_due_messages(consumer, 1)
    budgets = []

    class _Recording(_ScriptedWriter):
        def retry_batch(self, envelopes, budget_seconds=None):
            budgets.append(budget_seconds)
            return super().retry_batch(envelopes)

    rc, _fakes = _make_consumer(consumer=consumer, writer=_Recording(), monotonic_clock=monotonic)
    rc._flush_budget_seconds = 45.0
    rc.tick()
    assert budgets == [pytest.approx(45.0)]


def test_adapter_stops_starting_chunks_once_the_budget_is_spent(monkeypatch):
    from types import SimpleNamespace

    from src.loader.retry_consumer import ApocRetryWriterAdapter, _classify_write_error

    now = [0.0]
    monkeypatch.setattr("src.loader.retry_consumer.time", SimpleNamespace(monotonic=lambda: now[0]))

    class _Slow(_RecordingBaseWriter):
        def retry_provenanced_batch(self, envelopes, **kwargs):
            now[0] += 2.0  # every chunk takes 2 s
            return super().retry_provenanced_batch(envelopes, **kwargs)

    base = _Slow()
    adapter = ApocRetryWriterAdapter(base, chunk_size=2)
    envelopes = [_envelope(source_key=f"p{i}", target_key="c", original_offset=i) for i in range(6)]
    outcomes = adapter.retry_batch(envelopes, budget_seconds=1.0)
    assert sorted(outcomes) == [0, 1] and [len(call) for call in base.calls] == [2]
    # the rows left without an outcome are classified by why: a budget abandon, never counted
    assert _classify_write_error(outcomes.error) == "connectivity"


@pytest.mark.parametrize("chunk_size", [None, 2])
def test_adapter_starts_nothing_once_the_budget_is_already_spent(chunk_size):
    from src.loader.edge_execution import LeaseDrainAbandoned
    from src.loader.retry_consumer import ApocRetryWriterAdapter

    base = _RecordingBaseWriter()
    adapter = ApocRetryWriterAdapter(base, chunk_size=chunk_size)  # None: the single-chunk path (F5)
    with pytest.raises(LeaseDrainAbandoned):
        adapter.retry_batch([_envelope(source_key="a", target_key="b")], budget_seconds=0.0)
    assert base.calls == []


def test_kafka_backlog_source_bounds_a_many_partition_call_by_one_overall_deadline(monkeypatch):
    from types import SimpleNamespace

    now = [0.0]
    monkeypatch.setattr(
        "src.loader.retry_consumer.time", SimpleNamespace(monotonic=lambda: now[0], time=lambda: now[0]),
    )

    class _SlowAdmin(_FakeAdminConsumer):
        timeouts: list = []

        def get_watermark_offsets(self, topic_partition, timeout=None):
            self.timeouts.append(timeout)
            now[0] += 4.0  # every broker round trip burns 4 s
            return super().get_watermark_offsets(topic_partition, timeout=timeout)

    fake = _SlowAdmin(watermarks={p: (0, 10) for p in range(8)}, committed={})
    source = KafkaBacklogSource(
        bootstrap_servers="kafka:9092", group_id="g", consumer_factory=lambda config: fake, timeout=10.0,
    )
    with pytest.raises(RetryConsumerError, match="deadline"):
        source.parked_watermarks("t.dlq.parked")
    assert fake.timeouts == [pytest.approx(10.0), pytest.approx(6.0), pytest.approx(2.0)]  # shrinking, never 8 x 10 s


# ── Outage with an index-blocked pair, stalled pairs, held blockers, bookkeeping ──


def test_an_outage_with_an_index_blocked_pair_neither_storms_nor_republishes_blocked_copies() -> None:
    wall = _Clock(start=10_000)
    consumer, index, sink = _FakeConsumer(), _RealSemanticsIndex(), _FakeSink()
    for offset in (0, 1):  # one pair with two DLQ rows: row 1 is index-blocked behind row 0
        envelope = _envelope(source_key="a", target_key="b", original_offset=offset)
        index.seed(envelope)
        consumer.push(_dlq_message(envelope))
    writer = _ToggleWriter(lambda: wall() / 1000.0, ServiceUnavailable("neo4j down"))
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, index=index, max_retry_attempts=9, wall_clock=wall,
        monotonic_clock=lambda: wall() / 1000.0,
    )
    for _ in range(40):
        rc.tick()
        wall.advance(1_000)
    assert sink.calls == []  # no "blocked" republish, nothing parked
    gaps = [b - a for a, b in zip(writer.call_times, writer.call_times[1:])]
    assert 2 <= len(writer.call_times) <= 12 and gaps == sorted(gaps)  # the streak backoff is honoured
    writer.error = None
    wall.advance(6_000)
    for _ in range(15):
        rc.tick()
        wall.advance(1_000)
    # once the dependency is back the pair drains in order in one flush: row 1 is blocked only
    # behind row 0 of the same batch, so it is written right after it -- never ahead of it, and no
    # blocked copy is ever published (finding F4)
    assert writer.written == [0, 1]
    assert sink.calls == []


def test_standalone_a_pair_with_an_unresolved_row_keeps_its_later_rows_out_of_later_slices() -> None:
    consumer = _FakeConsumer()
    for offset in (0, 1):
        consumer.push(_dlq_message(_envelope(source_key="a", target_key="b", original_offset=offset)))
    writer = _ScriptedWriter(decide=lambda e: _UNRESOLVED)
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, sink=_FakeSink(), max_retry_attempts=5)
    rc._write_slice_size = 1
    rc.tick()
    assert [_offsets(call) for call in writer.calls] == [[(0, 0)]]  # row 1 never written ahead of row 0


def test_a_stale_held_blocker_whose_index_entry_is_gone_does_not_delay_a_later_row() -> None:
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    index = _FakeIndex(missing_ids={("knows-events", 0, 0)})  # row 0 was already resolved by a duplicate copy
    writer = _ScriptedWriter()
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, index=index, wall_clock=wall)
    rc._on_message(_dlq_message(_envelope(source_key="a", target_key="b", original_offset=0, retry_after_ms=20_000)))
    rc._on_message(_dlq_message(_envelope(source_key="a", target_key="b", original_offset=1, retry_after_ms=1_000)))
    rc.tick()
    assert [_offsets(call) for call in writer.calls] == [[(0, 1)]]


def test_rerouted_wake_is_recorded_only_after_an_acknowledged_publish_and_pruned_by_expiry() -> None:
    class _FlakySink(_FakeSink):
        fail = True

        def reroute_or_park_many(self, requests):
            if self.fail:
                raise RuntimeError("producer down")
            return super().reroute_or_park_many(requests)

    wall = _Clock(start=10_000)
    consumer, sink = _FakeConsumer(), _FlakySink()
    _held_and_due_messages(consumer, 1)
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=_ScriptedWriter(decide=lambda e: _UNRESOLVED), sink=sink,
        max_retry_attempts=5, wall_clock=wall,
    )
    rc.tick()
    assert not rc._rerouted_wake  # the publish failed: nothing was re-queued
    sink.fail = False
    wall.advance(1_000)
    rc.tick()
    assert rc._rerouted_wake and rc._rerouted_heap
    wall.advance(60_000)
    rc._prune_rerouted_wakes(wall())
    assert not rc._rerouted_wake and not rc._rerouted_heap


def test_revoke_clears_the_held_pair_bookkeeping_and_rerouted_wakes() -> None:
    wall = _Clock(start=10_000)
    rc, _fakes = _make_consumer(wall_clock=wall)
    message = _dlq_message(_envelope(original_offset=0, retry_after_ms=20_000))
    rc._on_message(message)
    rc._record_rerouted_wake("KNOWS", rc._held[("knows.dlq", 0, 0)], 30_000)
    assert rc._held_by_pair and rc._rerouted_wake
    rc.on_partitions_revoked([("knows.dlq", 0)])
    assert not rc._held and not rc._held_by_pair and not rc._rerouted_wake and not rc._rerouted_heap


def test_assignment_resets_fetch_bookkeeping_for_partitions_with_nothing_outstanding() -> None:
    consumer = _FakeConsumer()
    _held_and_due_messages(consumer, 2)
    rc, _fakes = _make_consumer(consumer=consumer, writer=_ScriptedWriter())
    rc.tick()
    tp = ("knows.dlq", 0)
    assert tp in rc._fetched_hi and not rc._outstanding.get(tp)
    rc.on_partitions_assigned([tp])
    assert tp not in rc._fetched_hi and tp not in rc._committed


def test_a_partial_lease_abandon_keeps_the_committed_transaction_and_omits_the_rest():
    from src.loader.retry_consumer import _classify_write_error

    clock = _Clock(start=100.0)

    class _SlowGrantClient(_FakeLeaseClient):
        def acquire_any(self, resource_sets, *, wait_ms):
            clock.advance(0.08)  # the first grant outlasts the whole budget (a fake clock: no sleep)
            return super().acquire_any(resource_sets, wait_ms=wait_ms)

    envelopes = [_envelope(source_key="a", target_key="b", original_offset=i) for i in range(6)]
    base = _RecordingBaseWriter()
    writer = _leased_writer(base, _SlowGrantClient(), batch_size=2, clock=clock)
    outcomes = writer.retry_batch(envelopes, budget_seconds=0.05)
    assert sorted(outcomes) == [0, 1] and len(base.calls) == 1
    assert _classify_write_error(outcomes.error) == "connectivity"  # the abandon travels with the result


# ── Round 4: evidence model (D1), long outages, stalls, fatal timer, F4/F5/F6 ─


class _AlwaysFailing:
    """Raises ``error`` for every call (a fresh traceback each time)."""

    def __init__(self, error) -> None:
        self.error, self.calls = error, []

    def retry_batch(self, envelopes, budget_seconds=None):
        self.calls.append([e.original_offset for e in envelopes])
        raise self.error.with_traceback(None)


def _run(rc, wall, seconds, *, step_ms=1_000, each=None):
    for second in range(seconds):
        if each is not None:
            each(second)
        rc.tick()
        wall.advance(step_ms)


def test_an_outage_longer_than_85_minutes_neither_crashes_nor_parks_nor_storms() -> None:
    """M2: the no-progress streak is capped, so the backoff exponent never
    overflows (the old code raised OverflowError after ~85 minutes)."""
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    sink = _RepublishingSink(consumer, wall)
    sink.push_new(_envelope(original_offset=0, attempt_count=4))
    writer = _ToggleWriter(lambda: wall() / 1000.0, ServiceUnavailable("neo4j down"))
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, max_retry_attempts=5, wall_clock=wall,
        monotonic_clock=lambda: wall() / 1000.0,
    )
    _run(rc, wall, 3 * 3600)  # 3 h at 1 s ticks
    assert sink.calls == [] and len(writer.call_times) <= 3 * 3600 / 5 + 10
    from src.loader.retry_consumer import _MAX_STREAK

    assert max(rc._no_progress_streak.values()) <= _MAX_STREAK and rc._stall["KNOWS"].flushes <= _MAX_STREAK
    writer.error = None
    _run(rc, wall, 10)
    assert writer.written == [0] and sink.calls == [] and "KNOWS" not in rc._stall


@pytest.mark.parametrize("error", [
    RuntimeError("unknown"), _neo_error("Neo.ClientError.Transaction.TransactionTimedOut"),
    _neo_error("Neo.TransientError.General.TransactionMemoryLimit"),
], ids=["unknown", "timeout", "memory-limit"])
@pytest.mark.parametrize("count", [4, 6])
def test_four_or_more_rows_failing_alike_never_park_without_writes_and_terminate_once_writes_happen(error, count) -> None:
    """M3/M4: every one of the failing rows gets solo tries while the edge type
    is stalled (rotation, one failing call per backoff period); none is counted
    without a same-type WRITTEN; once same-type writes happen they terminate
    (write_failed, then the ordinary attempts-then-park path)."""
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    sink = _RepublishingSink(consumer, wall)
    poison = {f"s{offset}" for offset in range(count)}

    class _Writer:
        def __init__(self) -> None:
            self.calls: list[list[str]] = []

        def retry_batch(self, envelopes, budget_seconds=None):
            sources = [json.loads(e.key_bytes)[0] for e in envelopes]
            self.calls.append(sources)
            if poison & set(sources):
                raise error.with_traceback(None)
            return {i: (RETRY_OUTCOME_WRITTEN, "") for i in range(len(envelopes))}

    writer = _Writer()
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, max_retry_attempts=2, wall_clock=wall,
        monotonic_clock=lambda: wall() / 1000.0,
    )
    for offset in range(count):
        sink.push_new(_envelope(source_key=f"s{offset}", original_offset=offset))
    _run(rc, wall, 3600)
    assert sink.calls == [] and not rc._write_failures  # an hour, nothing counted, nothing parked
    assert len(writer.calls) <= 3600 / 5 + 20
    assert {call[0] for call in writer.calls if len(call) == 1} == poison  # every row was tried alone

    def live_traffic(second):
        if second % 3 == 0:  # same-type rows that write: evidence the server is healthy
            sink.push_new(_envelope(source_key=f"live{second}", original_offset=10_000 + second))

    _run(rc, wall, 1800, each=live_traffic)
    parked = {json.loads(key)[0] for key, reason, _attempt in sink.parked}
    assert parked == poison and all(reason == "write_failed" for _k, reason, _a in sink.parked)
    assert not any(json.loads(c["envelope"].key_bytes)[0].startswith("live") for c in sink.calls)


@pytest.mark.parametrize("unknown_error", [
    _READ_ONLY, _neo_error("Neo.ClientError.Procedure.ProcedureNotFound", "apoc missing"),
], ids=["read-only", "unknown"])
def test_split_maintenance_windows_never_park_a_row_one_attempt_from_parking(unknown_error) -> None:
    """p7: read-only (or an unknown system-wide error), then a restart, then the
    same again -- split windows never add up to a park, because nothing is
    counted without a same-type WRITTEN (the old floor/cap summed them)."""
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    sink = _RepublishingSink(consumer, wall)
    sink.push_new(_envelope(source_key="good", original_offset=0, attempt_count=4))
    writer = _ToggleWriter(lambda: wall() / 1000.0, unknown_error)
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, max_retry_attempts=5, wall_clock=wall,
        monotonic_clock=lambda: wall() / 1000.0,
    )
    for error, seconds in ((unknown_error, 400), (ServiceUnavailable("restart"), 600), (unknown_error, 400),
                           (ServiceUnavailable("restart"), 60), (unknown_error, 400)):
        writer.error = error
        _run(rc, wall, seconds)
        assert sink.calls == []
    writer.error = None
    _run(rc, wall, 10)
    assert writer.written == [0] and sink.calls == []


def test_a_security_error_after_a_long_non_fatal_outage_waits_the_full_fatal_window() -> None:
    """F2/P4: the fatal timer measures continuous fatal-class errors only; an
    11-minute ServiceUnavailable outage before them does not count."""
    from src.loader.retry_consumer import _FATAL_OUTAGE_SECONDS, RetryConsumerFatalError

    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(_envelope(original_offset=0)))
    writer = _ToggleWriter(lambda: wall() / 1000.0, ServiceUnavailable("neo4j restarting"))
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, wall_clock=wall, monotonic_clock=lambda: wall() / 1000.0)
    _run(rc, wall, 660)
    writer.error = _neo_error("Neo.ClientError.Security.AuthProviderFailed", "ldap not up yet")
    _run(rc, wall, int(_FATAL_OUTAGE_SECONDS) - 30)  # not yet: well inside its own window
    with pytest.raises(RetryConsumerFatalError, match="AuthProviderFailed"):
        _run(rc, wall, 60)


def test_the_fatal_timer_restarts_after_a_non_fatal_error_or_a_gap() -> None:
    from src.loader.retry_consumer import RetryConsumerFatalError

    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(_envelope(original_offset=0)))
    security = _neo_error("Neo.ClientError.Security.Unauthorized")
    writer = _ToggleWriter(lambda: wall() / 1000.0, security)
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, wall_clock=wall, monotonic_clock=lambda: wall() / 1000.0)
    _run(rc, wall, 400)
    writer.error = ServiceUnavailable("blip")  # a driver error without a Neo4j code: neutral
    _run(rc, wall, 20)
    assert "KNOWS" in rc._fatal
    writer.error = _neo_error("Neo.ClientError.Procedure.ProcedureNotFound")  # Neo4j answered: reset
    _run(rc, wall, 20)
    assert "KNOWS" not in rc._fatal
    writer.error = security
    _run(rc, wall, 400)  # 400 s of its own: under the 600 s window
    first, _last = rc._fatal["KNOWS"]
    rc._fatal["KNOWS"] = (first, first - 120)  # a gap of >60 s since the last fatal error
    _run(rc, wall, 300)  # would have crossed 600 s without the gap reset
    with pytest.raises(RetryConsumerFatalError):
        _run(rc, wall, 400)


_OOM = _neo_error("Neo.TransientError.General.MemoryPoolOutOfMemoryError", "pool full")


def test_memory_pool_pressure_counts_only_under_the_evidence_rule_and_a_huge_row_terminates() -> None:
    """r5 M3 (orchestrator decision under D1, superseding F1): a row that keeps
    exhausting the memory pool by itself is counted once same-type writes
    succeed after its first uncounted failure, so it terminates (write_failed,
    then parked) instead of holding its partition's commit forever; rows that
    only met intermittent pool pressure are all written."""
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    sink = _RepublishingSink(consumer, wall)

    class _Writer:
        calls = 0

        def retry_batch(self, envelopes, budget_seconds=None):
            _Writer.calls += 1
            sources = {json.loads(e.key_bytes)[0] for e in envelopes}
            if "huge" in sources or _Writer.calls % 2 == 0:
                raise _OOM.with_traceback(None)
            return {i: (RETRY_OUTCOME_WRITTEN, "") for i in range(len(envelopes))}

    rc, _fakes = _make_consumer(
        consumer=consumer, writer=_Writer(), sink=sink, max_retry_attempts=2, wall_clock=wall,
        monotonic_clock=lambda: wall() / 1000.0,
    )
    sink.push_new(_envelope(source_key="huge", original_offset=0))

    def traffic(second):
        if second < 1200:
            for k in range(3):
                sink.push_new(_envelope(source_key=f"s{second}-{k}", original_offset=1 + 3 * second + k))

    _run(rc, wall, 1800, each=traffic)
    assert [json.loads(key)[0] for key, _reason, _attempt in sink.parked] == ["huge"]
    assert {json.loads(c["envelope"].key_bytes)[0] for c in sink.calls} == {"huge"}
    assert not any(rc._outstanding.values())  # everything terminated; the commit is free


def test_a_lone_memory_pool_failure_is_never_counted_without_same_type_writes() -> None:
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(_envelope(original_offset=0, attempt_count=4)))
    sink, stalls = _FakeSink(), {}
    writer = _ToggleWriter(lambda: wall() / 1000.0, _OOM)
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, max_retry_attempts=5, wall_clock=wall,
        monotonic_clock=lambda: wall() / 1000.0,
    )
    rc._on_stall_seconds = lambda edge_type, seconds: stalls.__setitem__(edge_type, seconds)
    _run(rc, wall, 3600)
    assert sink.calls == [] and not rc._write_failures and stalls["KNOWS"] > 3000


@pytest.mark.parametrize("error", [ServiceUnavailable("down"), _READ_ONLY], ids=["outage", "read-only"])
@pytest.mark.parametrize("rows, other_pair", [(3, False), (4, True)])
def test_a_pair_chain_drains_in_order_after_an_outage_without_blocked_republishes(error, rows, other_pair) -> None:
    """F4/p8: rows blocked only behind earlier rows of their own batch are
    written in order in the same flush -- never one blocked copy per tick."""
    wall = _Clock(start=10_000)
    consumer, index = _FakeConsumer(), _RealSemanticsIndex()
    sink = _RepublishingSink(consumer, wall)
    writer = _ToggleWriter(lambda: wall() / 1000.0, error)
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, index=index, max_retry_attempts=9, wall_clock=wall,
        monotonic_clock=lambda: wall() / 1000.0,
    )
    envelopes = [_envelope(source_key="a", target_key="b", original_offset=o) for o in range(rows)]
    if other_pair:
        envelopes.append(_envelope(source_key="c", target_key="d", original_offset=100))
    for envelope in envelopes:
        index.seed(envelope)
        consumer.push(_dlq_message(envelope))
    _run(rc, wall, 120)
    calls_during = len(writer.call_times)
    writer.error = None
    _run(rc, wall, 30)
    assert calls_during <= 120 / 5 + 6 and sink.calls == []
    assert [o for o in writer.written if o < 100] == list(range(rows))
    assert not rc._outstanding[("knows.dlq", 0)]


def test_a_row_blocked_behind_a_row_parked_in_the_same_batch_is_requeued_locally_not_republished() -> None:
    """F4: row 1 is blocked (in-flush) behind row 0; row 0 parks in this very
    batch, so row 1 is due again locally and written next -- no blocked copy."""
    wall = _Clock(start=10_000)
    consumer, index, sink = _FakeConsumer(), _RealSemanticsIndex(), _FakeSink()
    first = _envelope(source_key="a", target_key="b", original_offset=0, attempt_count=2)
    second = _envelope(source_key="a", target_key="b", original_offset=1)
    for envelope in (first, second):
        index.seed(envelope)
        consumer.push(_dlq_message(envelope))
    decide = lambda e: _UNRESOLVED if e.original_offset == 0 else (RETRY_OUTCOME_WRITTEN, "")  # noqa: E731
    writer = _ScriptedWriter(decide=decide, pair_guard=True)
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, index=index, max_retry_attempts=3, wall_clock=wall,
    )
    rc.tick()
    (park,) = sink.calls
    assert park["envelope"].original_offset == 0 and park["parked_topic"] == "knows.dlq.parked"
    assert ("knows.dlq", 0, 1) in rc._due_this_tick  # requeued locally
    wall.advance(1_000)
    rc.tick()
    # one row per pair per call: row 1 never shares a call with row 0
    assert [_offsets(call) for call in writer.calls] == [[(0, 0)], [(0, 1)]]
    assert len(sink.calls) == 1 and _committed_positions(consumer)[-1] == [("knows.dlq", 0, 2)]


def test_isolation_budget_is_charged_per_attempted_sub_slice_not_per_split(monkeypatch) -> None:
    """F5: a sub-slice is charged when it is actually written (and only if it
    still holds rows), not both halves up front at the split -- so a small
    budget still reaches the first failing row alone."""
    monkeypatch.setattr("src.loader.retry_consumer._ISOLATION_CALL_BUDGET", 2)

    class _AllPoison:
        def __init__(self) -> None:
            self.calls: list[list[int]] = []

        def retry_batch(self, envelopes, budget_seconds=None):
            self.calls.append([e.original_offset for e in envelopes])
            raise _row_error("bad row")

    consumer = _FakeConsumer()
    _held_and_due_messages(consumer, 4)
    writer = _AllPoison()
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, sink=_FakeSink(), max_retry_attempts=5)
    rc.tick()
    assert writer.calls == [[0, 1, 2, 3], [0, 1], [0]]  # the old up-front charge stopped after [0, 1]
    assert rc._write_failures == {("knows.dlq", 0, 0): 1}  # isolated (and counted: a row error)


def test_a_stalled_edge_type_probes_one_row_per_flush_and_a_new_good_row_is_written_first() -> None:
    """M3/F5: once stalled, a flush starts with a solo probe (least recently
    tried first; a row never tried alone ranks by when it was first seen)
    instead of bisecting; a good row arriving behind poison rows gets its turn
    in the rotation, is written, and the flush then resumes normal writing."""
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    _held_and_due_messages(consumer, 4)
    poison = {0, 1, 2, 3}

    class _Writer:
        def __init__(self) -> None:
            self.calls: list[list[int]] = []

        def retry_batch(self, envelopes, budget_seconds=None):
            offsets = [e.original_offset for e in envelopes]
            self.calls.append(offsets)
            if poison & set(offsets):
                raise RuntimeError("unknown")
            return {i: (RETRY_OUTCOME_WRITTEN, "") for i in range(len(envelopes))}

    writer = _Writer()
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=_FakeSink(), max_retry_attempts=5, wall_clock=wall,
        monotonic_clock=lambda: wall() / 1000.0,
    )
    _run(rc, wall, 60)
    solo = [call[0] for call in writer.calls if len(call) == 1]
    assert set(solo[-8:]) == poison  # rotation: every row probed in turn
    assert all(len(call) == 1 for call in writer.calls[-8:])  # no bisection while stalled
    calls_before = len(writer.calls)
    consumer.push(_dlq_message(_envelope(source_key="g", original_offset=50)))
    _run(rc, wall, 30)
    assert [50] in writer.calls[calls_before:]  # probed in its turn and written
    assert writer.calls[calls_before:].index([50]) <= len(poison)  # after at most one probe per older row
    assert 50 not in rc._outstanding[("knows.dlq", 0)]


def test_a_partial_result_carries_its_error_class_so_untried_rows_are_not_counted() -> None:
    """A lease flush abandoned for its budget after one committed transaction:
    the rows without an outcome are classified as a drain abandon (never
    counted, never bisected), not as an unknown failure."""
    from src.loader.edge_execution import LeaseDrainAbandoned
    from src.loader.retry_consumer import _PartialOutcomes

    class _Partial:
        def __init__(self) -> None:
            self.calls = []

        def retry_batch(self, envelopes, budget_seconds=None):
            self.calls.append(len(envelopes))
            return _PartialOutcomes({0: (RETRY_OUTCOME_WRITTEN, "")}, LeaseDrainAbandoned("budget"))

    consumer = _FakeConsumer()
    _held_and_due_messages(consumer, 3)
    writer = _Partial()
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, sink=_FakeSink())
    rc.tick()
    assert writer.calls == [3]  # no bisection of the abandoned remainder
    assert not rc._write_failures and not rc._suspects and not rc._solo_uncounted


def test_revoke_resets_the_streak_fatal_timer_and_stall_of_rows_it_no_longer_owns() -> None:
    """F6 (narrowed by r5): a revoke drops the state of the rows it took away --
    their failing set, and for an edge type left with no failing row its streak
    and stall -- but keeps the fatal clock across the rebalance."""
    stalls = []
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    _held_and_due_messages(consumer, 2)
    writer = _ToggleWriter(lambda: wall() / 1000.0, _neo_error("Neo.ClientError.Security.Unauthorized"))
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, wall_clock=wall, monotonic_clock=lambda: wall() / 1000.0)
    rc._on_stall_seconds = lambda edge_type, seconds: stalls.append(seconds)
    _run(rc, wall, 30)
    assert rc._no_progress_streak and rc._fatal and rc._stall and rc._failing
    fatal_before = rc._fatal["KNOWS"]
    rc.on_partitions_revoked([("knows.dlq", 0)])
    assert not rc._no_progress_streak and not rc._stall and not rc._failing
    assert not rc._fail_streak and not rc._suspects and stalls[-1] == 0.0
    assert rc._fatal["KNOWS"] == fatal_before  # the fatal clock survives the rebalance


def test_revoke_keeps_the_state_of_edge_types_whose_rows_it_did_not_take() -> None:
    from src.loader.retry_consumer import RetryConsumer

    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    for entity in ("KNOWS", "LIKES"):
        consumer.push(_dlq_message(_envelope(entity_name=entity, original_offset=0)))
    writer = _ToggleWriter(lambda: wall() / 1000.0, ServiceUnavailable("down"))
    types = ("KNOWS", "LIKES")
    rc = RetryConsumer(
        consumer=consumer, writer_by_edge_type={t: writer for t in types},
        sink_by_edge_type={t: _FakeSink() for t in types}, max_retry_attempts_by_edge_type={t: 3 for t in types},
        parked_topic_by_edge_type={t: "p" for t in types}, base_delay_seconds_by_edge_type={t: 1.0 for t in types},
        max_delay_seconds_by_edge_type={t: 60.0 for t in types}, due_batch_coalesce_seconds=0.0,
        wall_clock=wall, monotonic_clock=lambda: wall() / 1000.0,
    )
    _run(rc, wall, 20)
    assert set(rc._no_progress_streak) == {"KNOWS", "LIKES"} and set(rc._stall) == {"KNOWS", "LIKES"}
    rc.on_partitions_revoked([("knows.dlq", 0)])
    assert set(rc._no_progress_streak) == {"LIKES"} and set(rc._stall) == {"LIKES"}


def test_an_edge_type_whose_failing_rows_all_resolve_goes_idle_and_forgets_its_stall() -> None:
    """F2: the stall and fatal timer end when the edge type has no failing row
    left, even if the last of them was resolved without a write (a duplicate)."""
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    index = _FakeIndex()
    consumer.push(_dlq_message(_envelope(original_offset=0)))
    writer = _ToggleWriter(lambda: wall() / 1000.0, _neo_error("Neo.ClientError.Security.Unauthorized"))
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, index=index, wall_clock=wall, monotonic_clock=lambda: wall() / 1000.0,
    )
    _run(rc, wall, 20)
    assert "KNOWS" in rc._fatal and "KNOWS" in rc._stall
    index._missing_ids.add(("knows-events", 0, 0))  # another copy resolved it meanwhile
    _run(rc, wall, 10)
    assert "KNOWS" not in rc._fatal and "KNOWS" not in rc._stall and "KNOWS" not in rc._failing


def test_a_lone_poison_row_with_an_unknown_error_counts_only_after_same_type_writes() -> None:
    """D1, the evidence rule itself: uncounted while nothing writes; one count
    per same-type WRITTEN after the row's last uncounted failure."""
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    sink = _RepublishingSink(consumer, wall)
    sink.push_new(_envelope(source_key="bad", original_offset=0))

    class _Writer:
        def retry_batch(self, envelopes, budget_seconds=None):
            if any(json.loads(e.key_bytes)[0] == "bad" for e in envelopes):
                raise RuntimeError("unknown")
            return {i: (RETRY_OUTCOME_WRITTEN, "") for i in range(len(envelopes))}

    rc, _fakes = _make_consumer(
        consumer=consumer, writer=_Writer(), sink=sink, max_retry_attempts=5, wall_clock=wall,
        monotonic_clock=lambda: wall() / 1000.0,
    )
    _run(rc, wall, 600)
    assert not rc._write_failures and sink.calls == []
    key = next(iter(rc._outstanding[("knows.dlq", 0)]))
    sink.push_new(_envelope(source_key="good", original_offset=1))
    _run(rc, wall, 10)  # one write of the edge type, then the bad row fails again: one count
    assert rc._write_failures == {("knows.dlq", 0, key): 1}
    _run(rc, wall, 120)  # nothing else writes: no further count
    assert rc._write_failures == {("knows.dlq", 0, key): 1}


def test_main_wires_the_stall_gauge_at_zero_and_to_the_consumer():
    """D1: graph_loader_retry_stalled_seconds{kind=edge,name=<type>} reads a
    real 0 from start-up and follows the consumer's on_stall_seconds."""
    from src.utils import metrics as metrics_module

    dlq_schema = _dlq_schema_for_main()
    with ExitStack() as stack:
        rc_cls = _enter_shared_main_patches(stack, dlq_schema)
        stack.enter_context(patch("src.loader.retry_consumer.ParkedPairIndex"))
        assert retry_consumer_main(["--dlq-topics", "dlq-works-at"]) == 0
    registry = metrics_module.process_metrics().registry
    labels = {"kind": "edge", "name": "WORKS_AT"}
    assert registry.get_sample_value("graph_loader_retry_stalled_seconds", labels) == 0.0
    rc_cls.call_args.kwargs["on_stall_seconds"]("WORKS_AT", 42.0)
    assert registry.get_sample_value("graph_loader_retry_stalled_seconds", labels) == 42.0


def test_a_later_row_never_shares_a_write_with_an_earlier_row_the_writer_cannot_decode() -> None:
    """Self-review finding: the writer's in-transaction pair guard only sees
    rows it could decode, so an earlier row reported invalid_on_retry would not
    block a later row of its pair in the same call. One row per pair per call:
    the later row waits behind it (blocked, its own budget untouched) and is
    never written ahead of it."""
    consumer, index, sink = _FakeConsumer(), _RealSemanticsIndex(), _FakeSink()
    earlier = _envelope(source_key="a", target_key="b", original_offset=0)
    later = _envelope(source_key="a", target_key="b", original_offset=1)
    for envelope in (earlier, later):
        index.seed(envelope)
        consumer.push(_dlq_message(envelope))
    decide = lambda e: (  # noqa: E731 - no pair guard, like the real writer for an undecodable row
        (RETRY_OUTCOME_UNRESOLVED, "invalid_on_retry") if e.original_offset == 0 else (RETRY_OUTCOME_WRITTEN, "")
    )
    writer = _ScriptedWriter(decide=decide)
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, sink=sink, index=index, max_retry_attempts=5)
    rc.tick()
    assert [_offsets(call) for call in writer.calls] == [[(0, 0)]]  # the later row was never written
    by_offset = {call["envelope"].original_offset: call for call in sink.calls}
    assert by_offset[0]["reason_code"] == "invalid_on_retry"
    assert 1 not in by_offset and _held_wake_ms(rc, 1) is not None  # held behind it (v7-S4 M1)


def test_parked_pair_index_lists_only_the_earlier_outstanding_records_of_the_pair() -> None:
    index = _RealSemanticsIndex()
    for envelope in (
        _envelope(source_key="a", target_key="b", original_offset=0),
        _envelope(source_key="a", target_key="b", original_offset=2),
        _envelope(source_key="a", target_key="b", original_offset=5),
        _envelope(source_key="x", target_key="y", original_offset=1),
    ):
        index.seed(envelope)
    earlier = index.earlier_outstanding("a", "b", original_topic="knows-events", original_partition=0, original_offset=5)
    assert earlier == [("knows-events", 0, 0), ("knows-events", 0, 2)]
    assert index.earlier_outstanding("a", "b", original_topic="knows-events", original_partition=0, original_offset=0) == []


# ── Round 5 review: duplicates, tombstone order, fairness, fatal commit ─────


class _OrderRecordingWriter:
    """Writes everything; records (original_offset) in write order."""

    def __init__(self) -> None:
        self.order: list[int] = []

    def retry_batch(self, envelopes, budget_seconds=None):
        self.order.extend(e.original_offset for e in envelopes)
        return {i: (RETRY_OUTCOME_WRITTEN, "") for i in range(len(envelopes))}


def _never_older_after_newer(order: list[int]) -> bool:
    return all(not (later < earlier) for i, earlier in enumerate(order) for later in order[i + 1:])


@pytest.mark.parametrize("max_batch_size, layout", [
    (2000, [(100, 10), (101, 20), (102, 10), (103, 20)]),  # P5-S3 republish of a whole flush
    (3, [(100, 10), (101, 20), (102, 10), (103, 20)]),     # the batch cap cuts between the copies
    (3, [(100, 10), (101, 20), (300, 10), (301, 20)]),     # crash after a re-DLQ ack, before the commit
], ids=["republish", "republish-batch-3", "crash-copy-batch-3"])
def test_a_duplicate_copy_of_an_earlier_row_is_never_written_after_a_later_row(max_batch_size, layout) -> None:
    """r5 M1: a second copy of r1 in the batch is held (M12 skips it once the
    first copy is resolved), and a pair's rows are written in identity order --
    never r1, r2, r1-copy."""
    wall = _Clock(start=10_000)
    consumer, index = _FakeConsumer(), _RealSemanticsIndex()
    for original in (10, 20):
        index.seed(_envelope(original_offset=original))
    writer = _OrderRecordingWriter()
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=_FakeSink(), index=index, wall_clock=wall,
        monotonic_clock=lambda: wall() / 1000.0, max_batch_size=max_batch_size,
    )
    for dlq_offset, original in layout:
        consumer.push(_dlq_message(_envelope(original_offset=original), offset=dlq_offset))
    _run(rc, wall, 10)
    assert writer.order[-1] == 20 and _never_older_after_newer(writer.order), writer.order
    assert not rc._outstanding[("knows.dlq", 0)]


class _FailingTombstoneIndex(_RealSemanticsIndex):
    """The first ``times`` tombstones of the given identities are not delivered."""

    def __init__(self, fail_once, times: int = 1) -> None:
        super().__init__()
        self.fail_left = {identity: times for identity in fail_once}

    def remove_many(self, records):
        results, ok = [], []
        for record in records:
            identity = tuple(record[2:])
            if self.fail_left.get(identity, 0) > 0:
                self.fail_left[identity] -= 1
                results.append(RuntimeError("tombstone undelivered"))
            else:
                ok.append(record)
                results.append(None)
        super().remove_many(ok)
        return results


def test_a_failed_tombstone_of_an_earlier_row_never_lets_it_be_rewritten_over_a_later_row() -> None:
    """r5 M2 (tightened): a pair's next row is written only once the rows ahead
    of it have left the index; when r1's tombstone fails, r2 is not written at
    all -- it stays indexed and outstanding. Next tick r1, already in Neo4j, is
    only tombstoned (replay resurrection: a written copy at or below the pair's
    highest resolved offset is never written again; it used to be rewritten),
    and only then r2 is written."""
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    index = _FailingTombstoneIndex(fail_once={("knows-events", 0, 10)}, times=2)  # in the flush and after it
    for original in (10, 20):
        index.seed(_envelope(original_offset=original))
    writer = _OrderRecordingWriter()
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=_FakeSink(), index=index, wall_clock=wall,
        monotonic_clock=lambda: wall() / 1000.0,
    )
    consumer.push(_dlq_message(_envelope(original_offset=10), offset=100))
    consumer.push(_dlq_message(_envelope(original_offset=20), offset=101))
    rc.tick()
    assert writer.order == [10]  # r2 never written while r1 is still indexed
    assert index.contains("p1", "p2", original_topic="knows-events", original_partition=0, original_offset=20)
    _run(rc, wall, 20)
    assert writer.order == [10, 20]
    assert not any(index._by_pair.values()) and not rc._outstanding[("knows.dlq", 0)]


def test_steady_new_bad_rows_never_starve_older_rows_of_their_probe_turn() -> None:
    """r5 should-fix 1 (pF): after an outage the edge type is stalled; new lone
    bad rows keep arriving faster than the backoff, yet the good rows that
    failed during the outage are probed in their turn and written."""
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    sink = _RepublishingSink(consumer, wall, next_offset=0)
    state = {"down": True}

    class _Writer:
        written: list[str] = []

        def retry_batch(self, envelopes, budget_seconds=None):
            names = [json.loads(e.key_bytes)[0] for e in envelopes]
            if state["down"]:
                raise ServiceUnavailable("down").with_traceback(None)
            if any(name.startswith("bad") for name in names):
                raise _neo_error("Neo.ClientError.General.UnknownError", "bad row").with_traceback(None)
            self.written.extend(names)
            return {i: (RETRY_OUTCOME_WRITTEN, "") for i in range(len(envelopes))}

    writer = _Writer()
    rc, _fakes = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, max_retry_attempts=3, max_delay_seconds=30.0,
        wall_clock=wall, monotonic_clock=lambda: wall() / 1000.0,
    )
    for i in range(10):
        sink.push_new(_envelope(source_key=f"good{i}", target_key="t", original_offset=i))
    _run(rc, wall, 120, step_ms=500)
    state["down"] = False
    arrivals = iter(range(100, 10_000))

    def bad_every_2s(tick):
        if tick % 4 == 0:
            offset = next(arrivals)
            sink.push_new(_envelope(source_key=f"bad{offset}", target_key="t", original_offset=offset))

    _run(rc, wall, 1200, step_ms=500, each=bad_every_2s)  # 10 minutes
    assert sorted(writer.written) == sorted(f"good{i}" for i in range(10))


def test_a_fatal_exit_first_commits_what_other_edge_types_resolved() -> None:
    """r5 should-fix 2 (pE case 2): the RetryConsumerFatalError of one edge
    type must not lose the commit of a row another type resolved earlier in
    the same tick."""
    from src.loader.retry_consumer import RetryConsumer, RetryConsumerFatalError

    clock = [5_000.0]
    consumer = _FakeConsumer()
    auth = _neo_error("Neo.ClientError.Security.Unauthorized")
    types = ("AA", "BB")
    rc = RetryConsumer(
        consumer=consumer, writer_by_edge_type={"AA": _ScriptedWriter(), "BB": _ToggleWriter(lambda: clock[0], auth)},
        sink_by_edge_type={t: _FakeSink() for t in types}, max_retry_attempts_by_edge_type={t: 3 for t in types},
        parked_topic_by_edge_type={t: "p" for t in types}, base_delay_seconds_by_edge_type={t: 1.0 for t in types},
        max_delay_seconds_by_edge_type={t: 30.0 for t in types}, due_batch_coalesce_seconds=0.0,
        wall_clock=lambda: int(10_000 + clock[0] * 1000), monotonic_clock=lambda: clock[0],
    )
    rc._fatal["BB"] = (clock[0] - 700.0, clock[0] - 1.0)  # BB has failed fatally for 700 s
    consumer.push(_dlq_message(_envelope(entity_name="AA", original_offset=0)))  # AA's batch goes first
    consumer.push(_dlq_message(_envelope(entity_name="BB", original_offset=0)))
    with pytest.raises(RetryConsumerFatalError):
        rc.tick()
    assert _committed_positions(consumer)[-1] == [("aa.dlq", 0, 1)]  # AA's write was committed before the exit


def test_an_outcome_that_never_touched_neo4j_does_not_reset_the_fatal_clock() -> None:
    """r5 should-fix 3: an invalid_on_retry row (decoded locally, never sent to
    Neo4j) must not keep an authentication failure from ever turning fatal."""
    from src.loader.retry_consumer import RetryConsumerFatalError

    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    sink = _RepublishingSink(consumer, wall)
    auth = _neo_error("Neo.ClientError.Security.Unauthorized")

    class _Writer:
        def retry_batch(self, envelopes, budget_seconds=None):
            invalid = {i: (RETRY_OUTCOME_UNRESOLVED, "invalid_on_retry") for i, e in enumerate(envelopes)
                       if json.loads(e.key_bytes)[0].startswith("bad")}
            if len(invalid) == len(envelopes):
                return invalid
            if invalid:
                return _PartialOutcomesForTest(invalid, auth.with_traceback(None))
            raise auth.with_traceback(None)

    from src.loader.retry_consumer import _PartialOutcomes as _PartialOutcomesForTest

    rc, _fakes = _make_consumer(
        consumer=consumer, writer=_Writer(), sink=sink, max_retry_attempts=10_000, max_delay_seconds=2.0,
        wall_clock=wall, monotonic_clock=lambda: wall() / 1000.0,
    )
    sink.push_new(_envelope(source_key="good", original_offset=0))

    def invalid_traffic(second):
        if second % 3 == 0:
            sink.push_new(_envelope(source_key=f"bad{second}", original_offset=1 + second))

    with pytest.raises(RetryConsumerFatalError):
        _run(rc, wall, 1200, each=invalid_traffic)


def test_a_backward_wall_clock_jump_never_freezes_a_stalled_edge_type() -> None:
    """r5 should-fix 5: the stall gate is clamped to now + the backoff cap."""
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    consumer.push(_dlq_message(_envelope(original_offset=0)))
    writer = _ToggleWriter(lambda: wall() / 1000.0, ServiceUnavailable("down"))
    rc, _fakes = _make_consumer(consumer=consumer, writer=writer, wall_clock=wall, monotonic_clock=lambda: wall() / 1000.0)
    _run(rc, wall, 20)
    writer.error = None
    # As if the wall clock jumped back an hour right after the gate was set:
    rc._stall["KNOWS"].retry_at_ms = wall() + 3_600_000
    _run(rc, wall, 15)
    assert writer.written == [0]


def _chain_index_and_consumer():
    consumer, index = _FakeConsumer(), _RealSemanticsIndex()
    for original in (10, 20):
        index.seed(_envelope(original_offset=original))
    consumer.push(_dlq_message(_envelope(original_offset=10), offset=100))
    consumer.push(_dlq_message(_envelope(original_offset=20), offset=101))
    return consumer, index


def test_a_failed_tombstone_then_an_outage_never_leaves_the_older_row_in_neo4j() -> None:
    """r5 re-review window 1 (pL): r1's tombstone fails, then the dependency
    fails -- Neo4j must still hold r2 (newest) for the whole outage. Since the
    replay-resurrection memory, the next flush only tombstones r1 (already in
    Neo4j) instead of rewriting it, so the outage now starts right after r2."""
    wall = _Clock(start=10_000)
    consumer, _plain = _chain_index_and_consumer()
    index = _FailingTombstoneIndex(fail_once={("knows-events", 0, 10)}, times=2)
    for original in (10, 20):
        index.seed(_envelope(original_offset=original))
    state = {"writes": 0, "down": False}
    current: dict[bytes, int] = {}

    class _Writer:
        def retry_batch(self, envelopes, budget_seconds=None):
            if state["down"]:
                raise ServiceUnavailable("down").with_traceback(None)
            for e in envelopes:
                current[e.key_bytes] = e.original_offset
            state["writes"] += 1
            if state["writes"] == 2:  # r2 just committed; then the outage starts
                state["down"] = True
            return {i: (RETRY_OUTCOME_WRITTEN, "") for i in range(len(envelopes))}

    rc, _fakes = _make_consumer(
        consumer=consumer, writer=_Writer(), index=index, wall_clock=wall, monotonic_clock=lambda: wall() / 1000.0,
    )
    _run(rc, wall, 1800)
    assert state["writes"] == 2 and state["down"]  # r1, r2 (r1 is never rewritten) -- then the outage
    assert current == {_envelope().key_bytes: 20}  # newest row, for the whole outage


def test_a_crash_between_a_chain_write_and_its_tombstone_never_rewrites_the_older_row_last() -> None:
    """r5 re-review window 2 (pM): the consumer dies between writing r1 and
    acknowledging r1's tombstone; r2 was not written yet, so the restarted
    consumer writes r1 again and then r2 -- never r1 after r2."""

    class _Crash(BaseException):
        pass

    wall = _Clock(start=10_000)
    index = _RealSemanticsIndex()
    for original in (10, 20):
        index.seed(_envelope(original_offset=original))
    real_remove = index.remove_many
    crashed = {"done": False}

    def remove_many(records):
        if not crashed["done"]:
            crashed["done"] = True
            raise _Crash()
        return real_remove(records)

    index.remove_many = remove_many
    writer = _OrderRecordingWriter()

    def start():
        consumer = _FakeConsumer()
        consumer.push(_dlq_message(_envelope(original_offset=10), offset=100))
        consumer.push(_dlq_message(_envelope(original_offset=20), offset=101))
        rc, _fakes = _make_consumer(
            consumer=consumer, writer=writer, index=index, wall_clock=wall, monotonic_clock=lambda: wall() / 1000.0,
        )
        return rc

    with pytest.raises(_Crash):
        _run(start(), wall, 5)
    assert writer.order == [10]  # r2 is never written before r1's tombstone is acknowledged
    _run(start(), wall, 5)  # restart from the last committed offset
    assert writer.order == [10, 10, 20] and _never_older_after_newer(writer.order[1:])
    assert not any(index._by_pair.values())
