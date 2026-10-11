"""Final follow-ups, Part B: retry-consumer liveness (pre-existing, reproduced on 540fe71).

Two causes, both in the retry consumer, not in the fuzz model:

* Stale copies of a still-indexed record each started their own re-DLQ lineage. Copies come from
  P5-S3 replay duplicates, at-least-once publishes that reported a failure but landed, and above
  all from a restart or rebalance: the consumer re-reads from a commit floor held low by a
  long-blocked row, so every older copy of a cycling root is fetched again. Each lineage republished
  a copy per cycle, and (one copy of a record written per batch) the lineages shared the record's
  attempts: 1,916 copies of one record, not parked after 1,600 s simulated.
* A capped batch took the lowest DLQ offsets first. Followers (original copies at low offsets,
  woken every few seconds) took every batch ahead of their roots (re-DLQ'd copies at high offsets):
  the roots never ran, the committed offset stood still, and no stall was reported.
"""
from __future__ import annotations

import dataclasses

from src.loader.retry_consumer import RETRY_OUTCOME_UNRESOLVED, RETRY_OUTCOME_WRITTEN, _PendingRecord
from tests.test_retry_consumer import _Clock, _dlq_message, _envelope, _FakeConsumer, _make_consumer
from tests.test_retry_pair_order_fuzz import RecordingWriter, RepublishingSink, SeqIndex, StatefulIndex


class _UnresolvedWriter:
    """The endpoint never arrives: every envelope comes back unresolved."""

    def __init__(self) -> None:
        self.calls: list[list[tuple[int, int]]] = []

    def retry_batch(self, envelopes, budget_seconds=None):
        self.calls.append([(e.original_offset, e.attempt_count) for e in envelopes])
        return {i: (RETRY_OUTCOME_UNRESOLVED, "missing_target") for i in range(len(envelopes))}


def _copy(attempt: int, *, offset: int = 5):
    return dataclasses.replace(_envelope(original_offset=offset, retry_after_ms=1_000), attempt_count=attempt)


def _consumer(writer, *, max_batch_size=2000, max_retry_attempts=50):
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    index = SeqIndex()
    index.add("p1", "p2", ("knows-events", 0, 5))
    sink = RepublishingSink(consumer, wall)
    rc, _ = _make_consumer(
        consumer=consumer, writer=writer, sink=sink, index=index, wall_clock=wall,
        monotonic_clock=lambda: wall() / 1000.0, max_batch_size=max_batch_size,
        max_retry_attempts=max_retry_attempts, base_delay_seconds=1.0, max_delay_seconds=2.0,
    )
    return rc, consumer, sink, wall


def test_only_the_newest_copy_of_a_record_in_a_batch_is_written() -> None:
    writer = RecordingWriter()
    rc, consumer, sink, _wall = _consumer(writer)
    consumer.push(_dlq_message(_copy(1), offset=100))
    consumer.push(_dlq_message(_copy(3), offset=101))
    rc.tick()
    assert [(offset, attempt) for _key, offset, attempt in writer.writes] == [(5, 3)]
    assert consumer.committed[-1]["offsets"][0].offset == 102  # both copies committed past


def test_a_restart_re_read_of_every_older_copy_continues_one_lineage() -> None:
    """After a restart from a low commit floor all six copies of a cycling root are fetched again
    in one poll: only the newest (attempt 5) is retried and re-DLQ'd, once."""
    writer = _UnresolvedWriter()
    rc, consumer, sink, _wall = _consumer(writer)
    for attempt in range(6):
        consumer.push(_dlq_message(_copy(attempt), offset=100 + attempt))
    rc.tick()
    assert writer.calls == [[(5, 5)]]
    assert [(r["envelope"].attempt_count, r["reason_code"]) for r in sink.calls] == [(5, "missing_target")]


def test_a_stale_copy_fetched_after_the_re_dlq_is_skipped() -> None:
    """A copy at or below the attempt this consumer already re-DLQ'd is skipped without a write,
    also one batch at a time (batch cap 1); the re-published copy is retried as usual."""
    writer = _UnresolvedWriter()
    rc, consumer, sink, wall = _consumer(writer, max_batch_size=1)
    consumer.push(_dlq_message(_copy(0), offset=100))
    rc.tick()  # attempt 0 re-DLQ'd as attempt 1 (DLQ offset 10_000)
    consumer._queue.insert(0, _dlq_message(_copy(0), offset=101))  # an at-least-once duplicate
    for _ in range(10):
        wall.advance(1_000)
        rc.tick()
    attempts = [attempt for call in writer.calls for _offset, attempt in call]
    assert attempts == sorted(attempts) and len(set(attempts)) == len(attempts)  # one lineage: 0, 1, 2, ...
    assert attempts[:2] == [0, 1]


class _ToggleWriter:
    def __init__(self) -> None:
        self.calls: list[list[tuple[int, int]]] = []
        self.resolve = False

    def retry_batch(self, envelopes, budget_seconds=None):
        self.calls.append([(e.original_offset, e.attempt_count) for e in envelopes])
        outcome = (RETRY_OUTCOME_WRITTEN, "") if self.resolve else (RETRY_OUTCOME_UNRESOLVED, "missing_target")
        return {i: outcome for i in range(len(envelopes))}


class _ElsewhereSink:
    """Acknowledged re-DLQs whose copies land where this consumer does not read."""

    def reroute_or_park_many(self, requests):
        return ["parked" if r.get("parked_topic") else "re_dlq" for r in requests]


def test_a_fresh_dlq_of_a_record_resolved_elsewhere_is_never_skipped_by_the_memory() -> None:
    """Review must-fix (p_superseded): R1 re-DLQs record I (attempt 0 -> 1); the attempt-1 copy is written
    elsewhere (another replica, another DLQ partition) and leaves the index; the live loader then DLQs I
    again (index entry re-added, a fresh attempt-0 copy with a new first_failure_at_ms). That copy is a new
    lineage: it must be retried and written, never committed past while I stays indexed."""
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    index = StatefulIndex()
    ident = ("knows-events", 0, 5)
    index.add("p1", "p2", ident)
    writer = _ToggleWriter()
    rc, _ = _make_consumer(consumer=consumer, writer=writer, sink=_ElsewhereSink(), index=index, wall_clock=wall,
                           monotonic_clock=lambda: wall() / 1000.0, max_retry_attempts=10)
    consumer.push(_dlq_message(_envelope(original_offset=5), offset=100))
    for _ in range(5):
        rc.tick()
        wall.advance(1000)
    assert writer.calls == [[(5, 0)]] and rc._superseded
    index.by_pair.clear()  # written elsewhere
    index.add("p1", "p2", ident)  # the loader DLQs the same source record again
    writer.resolve = True
    fresh = dataclasses.replace(_envelope(original_offset=5), first_failure_at_ms=9_000, dlq_routed_at_ms=9_000,
                                retry_after_ms=9_000)
    consumer.push(_dlq_message(fresh, offset=101))
    for _ in range(10):
        rc.tick()
        wall.advance(1000)
    assert writer.calls == [[(5, 0)], [(5, 0)]]  # the fresh copy was written
    assert ident not in index.by_pair.get(index._pk("p1", "p2", ident[0]), set())
    assert consumer.committed[-1]["offsets"][0].offset == 102


def test_a_revoke_forgets_the_superseded_memory_of_its_partitions() -> None:
    writer = _UnresolvedWriter()
    rc, consumer, _sink, _wall = _consumer(writer)
    consumer.push(_dlq_message(_copy(0), offset=100))
    rc.tick()
    assert [tp for _attempt, tp in rc._superseded.values()] == [("knows.dlq", 0)]
    rc.on_partitions_revoked([("knows.dlq", 1)])
    assert rc._superseded
    rc.on_partitions_revoked([("knows.dlq", 0)])
    assert not rc._superseded


def test_a_capped_batch_takes_the_longest_due_record_first() -> None:
    rc, _fakes = _make_consumer(max_batch_size=1)
    root = _PendingRecord(envelope=_envelope(original_offset=1), topic="knows.dlq", partition=0, offset=500)
    follower = _PendingRecord(envelope=_envelope(original_offset=2), topic="knows.dlq", partition=0, offset=3)
    rc._tick_number = 1
    rc._mark_due(root)
    rc._tick_number = 2
    rc._mark_due(follower)  # due later, lower DLQ offset
    rc._mark_due(root)  # marking it again keeps its age
    rc._freeze_due_batches()
    assert [key for key, _pending in rc._pending_batches["KNOWS"].keyed_pendings] == [("knows.dlq", 0, 500)]
    assert list(rc._due_this_tick) == [("knows.dlq", 0, 3)]


def test_records_due_in_the_same_tick_keep_lowest_offsets_first() -> None:
    rc, _fakes = _make_consumer(max_batch_size=1)
    for offset in (500, 3):
        rc._mark_due(_PendingRecord(envelope=_envelope(original_offset=offset), topic="knows.dlq", partition=0,
                                    offset=offset))
    rc._freeze_due_batches()
    assert [key for key, _pending in rc._pending_batches["KNOWS"].keyed_pendings] == [("knows.dlq", 0, 3)]
