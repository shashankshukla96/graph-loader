"""Replay resurrection (pre-existing, found by the stream-outage M1 trace, probe_replay_resurrection.py).

A crash, a stop-drain abandon or a failed revoke settlement redelivers a partly committed live flush. If
the retry consumer has meanwhile written (or parked) a DLQ'd row of that flush and tombstoned it, the
redelivered flush re-adds and republishes it (``IndexedMissingEndpointSink.add`` re-adds unconditionally),
and the older row then looked outstanding again:

1. the consumer writes 5115 and removes it from the index;
2. it checks 6221: not blocked;
3. the redelivered flush re-adds 5115 (and 6221);
4. the consumer writes 6221 and removes it;
5. the re-added 5115 is "contained", so it was applied again: the final value was the older one.

Fix (option B): the consumer remembers, per (pair, original topic, original partition), the highest source
offset it wrote or parked. It resolves a pair's records in order, so a still-indexed copy below that offset
can only be a resurrection: it is tombstoned and committed past (``stale_resurrected_copy_skipped``), never
written.
"""
from __future__ import annotations

import dataclasses
import json
import logging
from types import SimpleNamespace

import pytest

import src.loader.retry_consumer as retry_consumer_module
from src.loader.edge_execution import IndexedMissingEndpointSink, LabelledEndpointSink
from src.loader.retry_consumer import RETRY_OUTCOME_UNRESOLVED, RETRY_OUTCOME_WRITTEN, RetryConsumer
from tests.test_retry_consumer import _Clock, _dlq_message, _envelope, _FakeConsumer, _make_consumer
from tests.test_retry_pair_order_fuzz import PAIRS, RepublishingSink, SeqIndex, StatefulIndex

TOPIC, PARTITION = "social-works-at", 3


@pytest.fixture(autouse=True)
def _quiet_logs():
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


class _LiveIndex(StatefulIndex):
    """The fuzz's local index view, with ``ParkedPairIndex.add``'s keyword signature for the live sink."""

    def add(self, s, t, ident=None, *, original_topic=None, original_partition=None, original_offset=None,
            first_failure_at_ms=None):
        super().add(s, t, ident if ident is not None else (original_topic, original_partition, original_offset))

    def outstanding(self):
        return sorted(ident for idents in self.by_pair.values() for ident in idents)


class _LiveSeqIndex(SeqIndex):
    """The fuzz's SEQUENCED index (several topics, v7-S4): a re-added record gets a new seq, after every
    outstanding record of its pair, like ``ParkedPairIndex.add``."""

    outstanding = _LiveIndex.outstanding

    def add(self, s, t, ident=None, *, original_topic=None, original_partition=None, original_offset=None,
            first_failure_at_ms=None):
        super().add(s, t, ident if ident is not None else (original_topic, original_partition, original_offset))


class _LiveDlq:
    """The live loader's DLQ sink: each routed row becomes a fresh DLQ envelope in the consumer's queue."""

    def __init__(self, consumer, wall, *, start_offset=100) -> None:
        self.consumer, self.wall, self.next_offset = consumer, wall, start_offset

    def route_unresolved_endpoint(self, *, topic, partition, offset, record, reason_code, endpoint_labels=None,
                                  **_kwargs):
        key = [record.source_key, record.target_key]
        if endpoint_labels is not None:
            key = [*endpoint_labels, *key]
        envelope = dataclasses.replace(
            _envelope(source_key=record.source_key, target_key=record.target_key, original_topic=topic,
                      original_partition=partition, original_offset=offset, retry_after_ms=int(self.wall())),
            key_bytes=json.dumps(key, separators=(",", ":")).encode("utf-8"), reason_code=reason_code,
            first_failure_at_ms=int(self.wall()), dlq_routed_at_ms=int(self.wall()),
        )
        self.consumer.push(_dlq_message(envelope, partition=0, offset=self.next_offset))
        self.next_offset += 1


def _route(live, offset, *, topic=TOPIC, partition=PARTITION, source="41817753", target="C00363",
           reason="missing_source"):
    live.route_unresolved_endpoint(
        topic=topic, partition=partition, offset=offset,
        record=SimpleNamespace(source_key=source, target_key=target), edge_type="KNOWS",
        source_key_property="s", target_key_property="t", failure_class="missing_endpoint",
        reason_code=reason, error_text="x", retry_after_seconds=0.0,
    )


class _GraphWriter:
    """Writes every envelope (or reports ``missing`` offsets unresolved) and records the applied order;
    ``on_write`` runs before each row is applied (the redelivered flush lands mid-write)."""

    def __init__(self, on_write=None, missing=()) -> None:
        self.applied: list[tuple[str, int]] = []
        self.on_write = on_write
        self.missing = set(missing)

    def retry_batch(self, envelopes, budget_seconds=None):
        out = {}
        for i, envelope in enumerate(envelopes):
            if self.on_write is not None:
                self.on_write(envelope)
            if envelope.original_offset in self.missing:
                out[i] = (RETRY_OUTCOME_UNRESOLVED, "missing_source")
                continue
            self.applied.append((envelope.original_topic, envelope.original_offset))
            out[i] = (RETRY_OUTCOME_WRITTEN, "")
        return out


class _ParkingSink:
    def __init__(self) -> None:
        self.parked = []

    def reroute_or_park_many(self, requests):
        out = []
        for request in requests:
            if request.get("parked_topic"):
                self.parked.append(request["envelope"].original_offset)
                out.append("parked")
            else:
                out.append("re_dlq")
        return out


def _race(*, after_write=(), max_batch_size=1, ticks=12, index=None, before_c2=None):
    """The probe's interleaving through the real consumer: flush F dead-letters 5115 and 6221 (one pair);
    the consumer writes 5115, then -- while it writes 6221 -- the redelivered flush re-routes both (the pair
    is blocked by 6221) through the plain live sink, plus ``after_write`` rows of the pair."""
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    index = index or _LiveIndex()
    live = IndexedMissingEndpointSink(_LiveDlq(consumer, wall), index, clock=lambda: int(wall()))
    _route(live, 5115)
    _route(live, 6221)
    redelivered = []

    def on_write(envelope):
        if envelope.original_offset == 6221 and not redelivered:
            redelivered.append(True)
            if before_c2 is not None:
                before_c2(live)
            _route(live, 5115, reason="blocked_by_earlier_unresolved_row")
            _route(live, 6221, reason="blocked_by_earlier_unresolved_row")
            for offset in after_write:
                _route(live, offset, reason="blocked_by_earlier_unresolved_row")

    writer = _GraphWriter(on_write)
    outcomes = []
    rc, _ = _make_consumer(consumer=consumer, writer=writer, index=index, wall_clock=wall,
                           monotonic_clock=lambda: wall() / 1000.0, max_batch_size=max_batch_size,
                           on_row_retried=lambda _edge_type, outcome: outcomes.append(outcome))
    for _ in range(ticks):
        rc.tick()
        wall.advance(1_000)
    return SimpleNamespace(rc=rc, consumer=consumer, index=index, writer=writer, outcomes=outcomes,
                           applied=[offset for _topic, offset in writer.applied], redelivered=bool(redelivered))


def _committed(consumer) -> int:
    return consumer.committed[-1]["offsets"][0].offset


# ── The probe, as a test, and its control ───────────────────────────────────


def test_a_resurrected_older_copy_is_tombstoned_and_never_written() -> None:
    """probe_replay_resurrection.py: was ``applied order [5115, 6221, 5115] -> final 5115 (older)``."""
    run = _race()
    assert run.redelivered
    assert run.applied == [5115, 6221]  # the newer value is last; 5115 is never applied again
    assert run.index.outstanding() == []  # the re-added entry was tombstoned: nothing blocks the pair
    assert "stale_resurrected_copy_skipped" in run.outcomes
    assert run.outcomes.count("written") == 2
    assert _committed(run.consumer) == 104  # every copy, both c1 and both c2, committed past
    assert not any(run.rc._outstanding.values())


def test_control_without_the_resolved_memory_the_older_value_lands_last(monkeypatch) -> None:
    """Sensitivity: with the memory disabled, the same interleaving reproduces the bug exactly."""
    monkeypatch.setattr(RetryConsumer, "_resolved_offset_at_or_above", lambda self, edge_type, pending: None)
    run = _race()
    assert run.applied == [5115, 6221, 5115]


def test_a_later_row_behind_the_resurrected_copy_is_not_blocked_by_it() -> None:
    """The redelivered flush also routes a newer row 7000 behind the (re-added) pair: the stale copy's
    entry is tombstoned, so 7000 is written, last, and nothing stays indexed."""
    run = _race(after_write=(7000,))
    assert run.applied == [5115, 6221, 7000]
    assert run.index.outstanding() == []
    assert _committed(run.consumer) == 105


def test_the_same_race_inside_one_batch_is_caught_too() -> None:
    """With both first copies in one batch the chain is still written level by level, 5115 tombstoned
    before 6221 is written (``flush_tombstones``), so the redelivery can land in between there as well
    (without the fix: ``[5115, 6221, 5115]``)."""
    run = _race(max_batch_size=2000)
    assert run.applied == [5115, 6221]
    assert run.index.outstanding() == []


def test_a_failed_tombstone_of_the_resurrected_copy_keeps_it_outstanding_and_unwritten() -> None:
    index = _LiveIndex()
    fail_once = {"armed": False}
    original = index.remove_many

    def remove_many(records):
        if fail_once["armed"] and any(offset == 5115 for *_rest, offset in records):
            fail_once["armed"] = False
            return [RuntimeError("tombstone undelivered")] * len(records)
        return original(records)

    index.remove_many = remove_many

    def arm(_live):
        fail_once["armed"] = True

    run = _race(index=index, before_c2=arm, ticks=20)
    assert run.applied == [5115, 6221]  # never written, also not on the retry of the tombstone
    assert run.index.outstanding() == []
    assert run.outcomes.count("stale_resurrected_copy_skipped") == 1
    assert _committed(run.consumer) == 104


def test_a_copy_of_the_resolved_record_itself_re_added_after_its_resolution_is_tombstoned() -> None:
    """The same offset counts as resolved: the re-added 6221 is tombstoned and committed past, not
    rewritten (its value is already the graph's)."""
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    index = _LiveIndex()
    live = IndexedMissingEndpointSink(_LiveDlq(consumer, wall), index, clock=lambda: int(wall()))
    _route(live, 6221)
    writer = _GraphWriter()
    outcomes = []
    rc, _ = _make_consumer(consumer=consumer, writer=writer, index=index, wall_clock=wall,
                           monotonic_clock=lambda: wall() / 1000.0,
                           on_row_retried=lambda _edge_type, outcome: outcomes.append(outcome))
    rc.tick()
    _route(live, 6221)  # redelivered after the resolution
    for _ in range(3):
        wall.advance(1_000)
        rc.tick()
    assert [offset for _topic, offset in writer.applied] == [6221]
    assert outcomes == ["written", "stale_resurrected_copy_skipped"]
    assert index.outstanding() == []
    assert _committed(consumer) == 102


@pytest.mark.parametrize("control", [False, True], ids=["fixed", "control"])
def test_sequenced_index_a_re_added_record_is_never_written_after_a_newer_outstanding_row(
    control, monkeypatch,
) -> None:
    """Why the same offset counts (found by the resurrection fuzz, p_resurrect_fuzz.py): in a sequenced index
    (a type of several topics) a re-added record gets a NEW seq, after every outstanding record of its pair.
    5115 is written; 6221 is still outstanding (its endpoint is missing, re-DLQ'd); the redelivered flush
    re-adds 5115, which now sorts AFTER 6221. Without the memory 6221 is written, then 5115 behind it: older
    over newer."""
    if control:
        monkeypatch.setattr(RetryConsumer, "_resolved_offset_at_or_above", lambda self, edge_type, pending: None)
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    index = _LiveSeqIndex()
    live = IndexedMissingEndpointSink(_LiveDlq(consumer, wall), index, clock=lambda: int(wall()))
    _route(live, 5115)
    _route(live, 6221)
    writer = _GraphWriter(missing={6221})
    rc, _ = _make_consumer(consumer=consumer, writer=writer, sink=RepublishingSink(consumer, wall), index=index,
                           wall_clock=wall, monotonic_clock=lambda: wall() / 1000.0, max_retry_attempts=10)
    rc.tick()
    assert [o for _t, o in writer.applied] == [5115] and index.outstanding() == [(TOPIC, PARTITION, 6221)]
    _route(live, 5115, reason="blocked_by_earlier_unresolved_row")  # the redelivered flush: seq 3, after 6221
    writer.missing.clear()
    for _ in range(10):
        wall.advance(1_000)
        rc.tick()
    applied = [o for _t, o in writer.applied]
    if control:
        assert applied == [5115, 6221, 5115]
    else:
        assert applied == [5115, 6221]
        assert index.outstanding() == []


# ── Parked rows, parking-lot replay and replay-dlq ──────────────────────────


def _park_then_write(max_batch_size=1):
    """5115's endpoint never arrives (one attempt allowed: parked), then 6221 is written."""
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    index = _LiveIndex()
    live = IndexedMissingEndpointSink(_LiveDlq(consumer, wall), index, clock=lambda: int(wall()))
    _route(live, 5115)
    _route(live, 6221)
    writer = _GraphWriter(missing={5115})
    sink = _ParkingSink()
    rc, _ = _make_consumer(consumer=consumer, writer=writer, sink=sink, index=index, wall_clock=wall,
                           monotonic_clock=lambda: wall() / 1000.0, max_retry_attempts=1,
                           max_batch_size=max_batch_size)
    for _ in range(6):
        rc.tick()
        wall.advance(1_000)
    assert sink.parked == [5115] and [o for _t, o in writer.applied] == [6221]
    return SimpleNamespace(rc=rc, consumer=consumer, index=index, writer=writer, sink=sink, live=live, wall=wall)


def test_a_resurrected_copy_of_a_parked_row_is_tombstoned_too() -> None:
    """Parked counts as resolved: a redelivered flush re-adding the parked 5115 after 6221 was written
    must not write it (the parking lot keeps it; replay it with replay-dlq --parked)."""
    run = _park_then_write()
    _route(run.live, 5115)
    run.writer.missing.clear()  # its endpoint has arrived meanwhile: without the fix it would be written
    for _ in range(4):
        run.rc.tick()
        run.wall.advance(1_000)
    assert [o for _t, o in run.writer.applied] == [6221]
    assert run.index.outstanding() == []
    assert run.sink.parked == [5115]


@pytest.mark.parametrize("partition", [PARTITION, 1], ids=["same-partition", "other-partition"])
def test_parking_lot_replay_republishes_at_a_new_offset_and_is_written(partition) -> None:
    """``replay-dlq --parked`` (and ``replay-dlq`` of the ordinary DLQ) produces the record onto its original
    topic again: a NEW source offset, above every offset of that partition, so the record is never below the
    memory. The live loader dead-letters it again (endpoint still missing when it is read); the consumer
    retries and writes it after its endpoint arrives. The producer may pick another partition (the replayed
    key): offsets of another partition are never compared."""
    run = _park_then_write()
    _route(run.live, 9000, partition=partition)  # the replayed record, at its new identity
    run.writer.missing.clear()
    for _ in range(4):
        run.rc.tick()
        run.wall.advance(1_000)
    assert [o for _t, o in run.writer.applied] == [6221, 9000]
    assert run.index.outstanding() == []


def test_an_ordinary_dlq_replay_of_a_resolved_row_is_retried_at_its_new_offset() -> None:
    """replay-dlq of the ordinary DLQ (for example a blocked_by_earlier_unresolved_row envelope replayed by
    hand after its row was resolved): a new source offset, retried as a fresh record."""
    run = _race()
    live = IndexedMissingEndpointSink(_LiveDlq(run.consumer, _Clock(20_000), start_offset=500), run.index)
    _route(live, 9500)
    for _ in range(3):
        run.rc.tick()
    assert [o for _t, o in run.writer.applied] == [5115, 6221, 9500]
    assert run.index.outstanding() == []


# ── A type of several label pairs ────────────────────────────────────────────


def _multi_pair_consumer(consumer, index, writer, wall, *, max_batch_size=1):
    return RetryConsumer(
        consumer=consumer, writer_by_edge_type={"KNOWS": writer}, sink_by_edge_type={"KNOWS": _ParkingSink()},
        parked_pair_index_by_edge_type={"KNOWS": index}, max_retry_attempts_by_edge_type={"KNOWS": 5},
        parked_topic_by_edge_type={"KNOWS": "p"}, base_delay_seconds_by_edge_type={"KNOWS": 0.5},
        max_delay_seconds_by_edge_type={"KNOWS": 2.0}, due_batch_coalesce_seconds=0.0, wall_clock=wall,
        monotonic_clock=lambda: wall() / 1000.0, max_batch_size=max_batch_size,
        endpoint_pairs_by_edge_type={"KNOWS": PAIRS},
    )


def test_multi_pair_resurrection_is_caught_across_legacy_and_labelled_keys() -> None:
    """v7-S7 type (works-at-a/b: Person->Company, likes-c: Person->Comment, colliding keys). The first copies
    on likes-c carry legacy [sk, tk] keys, the redelivered flush's copies labelled [S, T, sk, tk] keys (the
    labelled sink): the read-time canonical key makes them one pair, so the resurrected 5115 is skipped.
    Person->Company rows of the same keys at LOWER offsets on works-at-a are written: another pair, and
    another topic."""
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    index = _LiveIndex(pair_of=lambda topic: list(PAIRS[topic]))
    dlq = _LiveDlq(consumer, wall)
    legacy = IndexedMissingEndpointSink(dlq, index, clock=lambda: int(wall()))
    labelled = IndexedMissingEndpointSink(LabelledEndpointSink(dlq, PAIRS["likes-c"]), index,
                                          clock=lambda: int(wall()))
    _route(legacy, 5115, topic="likes-c", partition=0)
    _route(legacy, 6221, topic="likes-c", partition=0)
    redelivered = []

    def on_write(envelope):
        if envelope.original_offset == 6221 and not redelivered:
            redelivered.append(True)
            _route(labelled, 5115, topic="likes-c", partition=0, reason="blocked_by_earlier_unresolved_row")
            _route(labelled, 6221, topic="likes-c", partition=0, reason="blocked_by_earlier_unresolved_row")
            company = IndexedMissingEndpointSink(LabelledEndpointSink(dlq, PAIRS["works-at-a"]), index)
            _route(company, 10, topic="works-at-a", partition=0)
            _route(company, 11, topic="works-at-a", partition=0)

    writer = _GraphWriter(on_write)
    rc = _multi_pair_consumer(consumer, index, writer, wall)
    for _ in range(12):
        rc.tick()
        wall.advance(1_000)
    assert redelivered
    assert writer.applied == [("likes-c", 5115), ("likes-c", 6221), ("works-at-a", 10), ("works-at-a", 11)]
    assert index.outstanding() == []
    assert all(json.loads(key[1])[:2] in (["Person", "Comment"], ["Person", "Company"])
               for key in rc._resolved_high)


def test_offsets_of_another_topic_of_the_same_pair_are_never_compared() -> None:
    """works-at-a and works-at-b both bind Person->Company: a record of b at offset 10 after a's 6221 was
    written is NOT stale (offsets of two topics say nothing about order; a documented gap, see USAGE §12)."""
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    index = _LiveIndex(pair_of=lambda topic: list(PAIRS[topic]))
    live = IndexedMissingEndpointSink(LabelledEndpointSink(_LiveDlq(consumer, wall), PAIRS["works-at-a"]), index)
    _route(live, 6221, topic="works-at-a", partition=0)
    writer = _GraphWriter()
    rc = _multi_pair_consumer(consumer, index, writer, wall)
    rc.tick()
    _route(live, 10, topic="works-at-b", partition=0)
    for _ in range(3):
        wall.advance(1_000)
        rc.tick()
    assert writer.applied == [("works-at-a", 6221), ("works-at-b", 10)]


# ── Bounds and rebalances ────────────────────────────────────────────────────


def test_lru_eviction_falls_back_to_the_behaviour_without_the_memory(monkeypatch) -> None:
    """A pair whose entry was evicted (``_RESOLVED_TRACK_LIMIT``) is retried as before the fix: the memory is
    an optimisation of safety, never needed for liveness. With the default bound the same run is exact."""
    def other_pair_first(live):
        _route(live, 1, source="p9", target="t9")  # resolved before the c2 copies: evicts the pair's entry

    run = _race(before_c2=other_pair_first)
    assert [o for t, o in run.writer.applied if o != 1] == [5115, 6221]
    monkeypatch.setattr(retry_consumer_module, "_RESOLVED_TRACK_LIMIT", 1)
    run = _race(before_c2=other_pair_first)
    assert len(run.rc._resolved_high) == 1
    assert [o for t, o in run.writer.applied if o != 1] == [5115, 6221, 5115]  # today's behaviour
    assert run.index.outstanding() == []


def test_a_revoke_forgets_the_resolved_memory_of_its_dlq_partitions() -> None:
    run = _race()
    assert [tp for _offset, tp in run.rc._resolved_high.values()] == [("knows.dlq", 0)]
    run.rc.on_partitions_revoked([("knows.dlq", 1)])
    assert run.rc._resolved_high
    run.rc.on_partitions_revoked([("knows.dlq", 0)])
    assert not run.rc._resolved_high


def test_no_memory_is_kept_for_a_type_without_a_parked_pair_index() -> None:
    """order_by and standalone types (no index) are unchanged: nothing is remembered or skipped."""
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    writer = _GraphWriter()
    rc, _ = _make_consumer(consumer=consumer, writer=writer, wall_clock=wall)
    for dlq_offset, offset in enumerate((6221, 5115)):
        consumer.push(_dlq_message(_envelope(original_topic=TOPIC, original_partition=PARTITION,
                                             original_offset=offset, retry_after_ms=1_000), partition=0,
                                   offset=100 + dlq_offset))
        rc.tick()
        wall.advance(1_000)
    assert [o for _t, o in writer.applied] == [6221, 5115]
    assert not rc._resolved_high


def test_a_written_copy_whose_tombstone_failed_is_tombstoned_next_time_not_rewritten() -> None:
    """The copy that wrote the record stays outstanding when its tombstone fails. Its record is in Neo4j
    already: next time it is only tombstoned and committed past (before this change it was rewritten)."""
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    index = _LiveIndex(fail_remove={(TOPIC, PARTITION, 6221)})
    live = IndexedMissingEndpointSink(_LiveDlq(consumer, wall), index, clock=lambda: int(wall()))
    _route(live, 6221)
    writer = _GraphWriter()
    outcomes = []
    rc, _ = _make_consumer(consumer=consumer, writer=writer, index=index, wall_clock=wall,
                           monotonic_clock=lambda: wall() / 1000.0,
                           on_row_retried=lambda _edge_type, outcome: outcomes.append(outcome))
    for _ in range(10):
        rc.tick()
        wall.advance(1_000)
    assert [offset for _topic, offset in writer.applied] == [6221]
    assert outcomes == ["stale_resurrected_copy_skipped"]  # "written" is notified only with its tombstone
    assert index.outstanding() == [] and _committed(consumer) == 101


@pytest.mark.parametrize("control", [False, True], ids=["fixed", "control"])
def test_sequenced_index_a_failed_tombstone_then_a_re_add_never_rewrites_the_row_after_a_newer_one(
    control, monkeypatch,
) -> None:
    """Found by the resurrection fuzz (seed 64, two topics): a0 (seq 1) is written but its tombstone fails,
    so its copy stays outstanding. Its entry is then removed (another copy's tombstone) and re-added by a
    redelivered flush with a NEW seq, after b1 (seq 2). Rewriting the outstanding copy would land after b1."""
    if control:
        monkeypatch.setattr(RetryConsumer, "_resolved_offset_at_or_above", lambda self, edge_type, pending: None)
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    index = _LiveSeqIndex()
    live = IndexedMissingEndpointSink(_LiveDlq(consumer, wall), index, clock=lambda: int(wall()))
    _route(live, 0, topic="works-at-a", partition=0)
    index.add("41817753", "C00363", ("works-at-b", 0, 1))  # b1, seq 2; its DLQ copy comes later
    index.fail_remove = {("works-at-a", 0, 0)}
    writer = _GraphWriter()
    rc, _ = _make_consumer(consumer=consumer, writer=writer, index=index, wall_clock=wall,
                           monotonic_clock=lambda: wall() / 1000.0, max_batch_size=1)
    rc.tick()
    assert writer.applied == [("works-at-a", 0)] and ("works-at-a", 0, 0) in index.outstanding()
    pk = index._pk("41817753", "C00363", "works-at-a")
    index.by_pair[pk].discard(("works-at-a", 0, 0))  # removed by another copy's tombstone
    index.add("41817753", "C00363", ("works-at-a", 0, 0))  # re-added by a redelivered flush: seq 3
    _route(live, 1, topic="works-at-b", partition=0)  # b1's DLQ copy (index entry kept, seq 2)
    for _ in range(10):
        wall.advance(1_000)
        rc.tick()
    if control:
        assert writer.applied == [("works-at-a", 0), ("works-at-b", 1), ("works-at-a", 0)]
    else:
        assert writer.applied == [("works-at-a", 0), ("works-at-b", 1)]
        assert index.outstanding() == []
