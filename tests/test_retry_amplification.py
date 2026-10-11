"""v7-S4 review M1: the retry consumer's DLQ growth is O(roots x cycles), never O(followers x cycles).

A pair whose first (root) row cannot resolve used to have every later row of the pair re-published as
a ``blocked_by_earlier_unresolved_row`` copy on every root cycle (and every 0.5 s once the root's wake
had passed): k followers cost about k copies per cycle, k^2/2 per pair when the endpoint never comes,
into an unlimited-retention topic. Followers are now held locally behind their root.
"""
from __future__ import annotations

import collections
import dataclasses
import json

import pytest

from src.loader.retry_consumer import RETRY_OUTCOME_UNRESOLVED, RETRY_OUTCOME_WRITTEN
from tests.test_retry_consumer import _Clock, _dlq_message, _envelope, _FakeConsumer, _make_consumer


class _StatefulIndex:
    """Per pair, the set of outstanding identities (identity order, single topic)."""

    def __init__(self) -> None:
        self.by_pair: dict[str, set] = {}

    @staticmethod
    def _pk(s, t) -> str:
        return json.dumps([s, t])

    def add(self, s, t, ident) -> None:
        self.by_pair.setdefault(self._pk(s, t), set()).add(ident)

    def watermark_snapshot(self):
        return {0: 0}

    def caught_up(self, _snapshot) -> bool:
        return True

    def contains(self, s, t, *, original_topic, original_partition, original_offset) -> bool:
        return (original_topic, original_partition, original_offset) in self.by_pair.get(self._pk(s, t), set())

    def is_blocked_by_earlier(self, s, t, *, original_topic, original_partition, original_offset, first_failure_at_ms):
        own = (original_topic, original_partition, original_offset)
        return any(i < own for i in self.by_pair.get(self._pk(s, t), set()))

    def earlier_outstanding(self, s, t, *, original_topic, original_partition, original_offset):
        own = (original_topic, original_partition, original_offset)
        return sorted(i for i in self.by_pair.get(self._pk(s, t), set()) if i < own)

    def remove_many(self, records):
        for s, t, topic, partition, offset in records:
            self.by_pair.get(self._pk(s, t), set()).discard((topic, partition, offset))
        return [None] * len(records)


class _RepublishingSink:
    """Re-DLQ'd / blocked envelopes come back at the end of the DLQ partition, like Kafka."""

    def __init__(self, consumer, wall) -> None:
        self.consumer, self.wall, self.next_offset = consumer, wall, 1_000_000
        self.published: list[str] = []
        self.parked = 0

    def reroute_or_park_many(self, requests):
        out = []
        for r in requests:
            env = r["envelope"]
            if r.get("parked_topic"):
                self.parked += 1
                out.append("parked")
                continue
            new = dataclasses.replace(
                env, attempt_count=env.attempt_count + (1 if r.get("increment_attempt", True) else 0),
                reason_code=r["reason_code"],
                retry_after_ms=int(self.wall() + r["retry_after_seconds_for_reattempt"] * 1000),
            )
            self.published.append(r["reason_code"])
            self.consumer.push(_dlq_message(new, offset=self.next_offset))
            self.next_offset += 1
            out.append("re_dlq")
        return out


def _run(pairs: int, followers: int, missing_s: float, *, attempts=200, total_s=None, max_batch_size=2000,
         max_records_per_partition=1000, max_overflow_held=None):
    wall = _Clock(start=10_000)
    consumer, index = _FakeConsumer(), _StatefulIndex()
    offset = 0
    for p in range(pairs):
        for j in range(followers + 1):
            index.add(f"s{p}", "t", ("knows-events", 0, offset))
            consumer.push(_dlq_message(_envelope(
                source_key=f"s{p}", target_key="t", original_offset=offset,
                retry_after_ms=10_000 + (0 if j == 0 else 500),
            ), offset=offset))
            offset += 1
    arrives_at = 10_000 + missing_s * 1000
    written: dict[str, list[int]] = collections.defaultdict(list)

    class _Writer:
        def retry_batch(self, envelopes, budget_seconds=None):
            out = {}
            for i, e in enumerate(envelopes):
                if wall() < arrives_at:
                    out[i] = (RETRY_OUTCOME_UNRESOLVED, "missing_target")
                else:
                    out[i] = (RETRY_OUTCOME_WRITTEN, "")
                    written[json.loads(e.key_bytes)[0]].append(e.original_offset)
            return out

    sink = _RepublishingSink(consumer, wall)
    rc, _ = _make_consumer(
        consumer=consumer, writer=_Writer(), sink=sink, index=index, wall_clock=wall,
        monotonic_clock=lambda: wall() / 1000.0, max_retry_attempts=attempts,
        base_delay_seconds=0.5, max_delay_seconds=5.0, max_batch_size=max_batch_size,
        max_records_per_partition=max_records_per_partition,
    )
    if max_overflow_held is not None:
        rc._max_overflow_held_per_partition = max_overflow_held
    for _ in range(int((total_s or missing_s + 30) * 10)):
        rc.tick()
        wall.advance(100)
    left = sum(len(v) for v in index.by_pair.values())
    return collections.Counter(sink.published), sink.parked, left, written


@pytest.mark.parametrize("followers", [3, 10, 30])
def test_dlq_growth_is_one_copy_per_root_cycle_however_many_followers(followers) -> None:
    published, parked, left, written = _run(1, followers, 120)
    assert published["blocked_by_earlier_unresolved_row"] == 0  # followers held locally, never re-published
    root_cycles = published["missing_target"]
    assert 0 < root_cycles <= 30  # 120 s of backoff 0.5 s doubling to 5 s
    assert parked == 0 and left == 0
    assert written["s0"] == list(range(followers + 1))  # every row once, in pair order


def test_dlq_growth_scales_with_roots_not_records() -> None:
    one, *_ = _run(1, 3, 120)
    many, _parked, left, written = _run(100, 3, 120)
    assert many["blocked_by_earlier_unresolved_row"] == 0
    assert many["missing_target"] == 100 * one["missing_target"]
    assert left == 0 and all(rows == sorted(rows) for rows in written.values())


def test_a_never_arriving_endpoint_parks_the_chain_at_attempts_per_row_without_blocked_copies() -> None:
    attempts, followers = 10, 10
    published, parked, left, _written = _run(1, followers, 10**6, attempts=attempts, total_s=4000)
    assert parked == followers + 1 and left == 0
    assert published == {"missing_target": (followers + 1) * (attempts - 1)}  # O(rows x attempts), not k^2


@pytest.mark.parametrize("max_batch_size", [1, 2, 3])
def test_held_followers_never_starve_their_roots_under_a_small_batch_cap(max_batch_size) -> None:
    """Followers with low DLQ offsets must not occupy every batch slot while their roots wait."""
    published, parked, left, written = _run(4, 3, 20, max_batch_size=max_batch_size, total_s=200)
    assert left == 0 and parked == 0
    assert published["blocked_by_earlier_unresolved_row"] <= 4 * 4  # at most about one per row
    assert all(rows == sorted(rows) and len(rows) == 4 for rows in written.values())


@pytest.mark.parametrize("overflow_budget", [None, 0])
@pytest.mark.parametrize("blocked_for_s,expected_delay", [(9, 60.0), (2 * 3600, 3600.0)])
def test_over_the_hold_budget_a_follower_is_overflow_held_or_published_with_an_age_scaled_delay(
    blocked_for_s, expected_delay, overflow_budget,
) -> None:
    """Holding keeps 10% of max_records_per_partition free so the partition keeps fetching. A
    follower decided while the partition is at that budget is held anyway, as an overflow hold, for
    the delay its published copy would wait (at least the type's max delay, at least as long as it has
    been blocked, up to a 1 h ceiling); it does not count towards the pause and stays outstanding (the
    commit never passes it). Only beyond the overflow budget (here 0) is it published, with that
    delay, never the 0.5 s blocked delay."""
    from tests.test_retry_consumer import _FakeIndex, _FakeSink, _ScriptedWriter, _UNRESOLVED

    import dataclasses

    consumer = _FakeConsumer()
    for i in range(7):
        envelope = _envelope(source_key="a", target_key="b", original_offset=i)
        envelope = dataclasses.replace(envelope, first_failure_at_ms=1_000, dlq_routed_at_ms=1_000)
        consumer.push(_dlq_message(envelope))
    index = _FakeIndex(blocked_ids={("knows-events", 0, i) for i in range(1, 7)})
    sink = _FakeSink()
    now = 1_000 + blocked_for_s * 1000
    rc, _ = _make_consumer(
        consumer=consumer, writer=_ScriptedWriter(decide=lambda e: _UNRESOLVED), sink=sink, index=index,
        max_records_per_partition=5, max_retry_attempts=9, wall_clock=lambda: now,
    )
    if overflow_budget is not None:
        rc._max_overflow_held_per_partition = overflow_budget
    for _ in range(3):
        rc.tick()
    calls = [(c["envelope"].original_offset, c["reason_code"], c["retry_after_seconds_for_reattempt"]) for c in sink.calls]
    assert calls[0][:2] == (0, "missing_target")
    if overflow_budget == 0:
        assert calls[1:] == [(i, "blocked_by_earlier_unresolved_row", pytest.approx(expected_delay)) for i in range(1, 5)]
        assert sorted(key[2] for key in rc._held) == [5, 6]  # fetched later, under the budget: held locally
        return
    assert calls[1:] == []  # nothing re-published
    held = sorted(key[2] for key in rc._held)
    assert held == [1, 2, 3, 4, 5, 6]
    # The first over-budget rows become overflow holds; once they no longer count, later ones fit
    # under the hold budget again and get ordinary holds (just after the root's next attempt).
    overflow = rc._overflow_held[("knows.dlq", 0)]
    assert overflow and overflow <= {1, 2, 3, 4, 5, 6}
    wakes = {o: w for w, _t, _p, o in rc._wake_heap if o in overflow}
    assert all(w == pytest.approx(now + expected_delay * 1000, abs=2) for w in wakes.values())
    assert ("knows.dlq", 0) not in rc._paused  # overflow holds do not count towards the pause
    committed = [tp.offset for c in consumer.committed for tp in c["offsets"] or ()]
    assert all(offset <= 1 for offset in committed)  # never past the lowest held row


@pytest.mark.parametrize("cap,previous_copies", [(5, 3921), (10, 3389), (20, 2348)])
def test_over_the_budget_a_never_resolving_chain_still_parks_every_row_with_fewer_copies(cap, previous_copies) -> None:
    """pX_budget_park: a 31-row chain whose endpoint never arrives, a partition cap below the chain.
    Every row still parks (no partition full of rows held behind an unfetched blocker), and the age-
    scaled delay keeps blocked copies below the previous growth (``previous_copies``, before S-a)."""
    attempts = 10
    published, parked, left, _written = _run(
        1, 30, 10**8, attempts=attempts, total_s=16_000, max_records_per_partition=cap,
    )
    assert parked == 31 and left == 0
    assert published["missing_target"] == 31 * (attempts - 1)
    assert published["blocked_by_earlier_unresolved_row"] < previous_copies
    if cap == 20:
        assert published["blocked_by_earlier_unresolved_row"] <= 1000


@pytest.mark.parametrize("cap", [5, 10, 20])
def test_over_the_budget_a_backlog_drains_once_the_endpoint_arrives(cap) -> None:
    published, parked, left, written = _run(4, 30, 120, max_records_per_partition=cap, total_s=900)
    assert parked == 0 and left == 0
    assert all(rows == sorted(rows) and len(rows) == 31 for rows in written.values())


@pytest.mark.parametrize("cap,old_copies", [(100, 10_395), (20, 2_728)])
def test_backlog_above_the_cap_costs_root_cycles_not_blocked_copies(cap, old_copies) -> None:
    """pX_ratio: 20 pairs x 10 rows on one DLQ partition, endpoints missing for 600 s. What matters
    is backlog/cap, not the cap: before the overflow hold, cap 100 (backlog 2x the cap) re-published
    10,395 blocked copies (63x the input). Now the over-budget followers are held locally."""
    published, parked, left, written = _run(20, 9, 600, attempts=200, total_s=1500, max_records_per_partition=cap)
    assert parked == 0 and left == 0
    assert all(rows == sorted(rows) and len(rows) == 10 for rows in written.values())
    blocked = published["blocked_by_earlier_unresolved_row"]
    assert blocked <= 200 and blocked < old_copies // 50  # at most about one per row
    assert published["missing_target"] <= 20 * 130  # the roots' own cycles (about 123 each)


def test_beyond_the_overflow_budget_blocked_rows_are_published_again() -> None:
    held, *_ = _run(20, 9, 600, attempts=200, total_s=1500, max_records_per_partition=100)
    none, parked, left, _written = _run(
        20, 9, 600, attempts=200, total_s=1500, max_records_per_partition=100, max_overflow_held=0,
    )
    assert parked == 0 and left == 0
    assert none["blocked_by_earlier_unresolved_row"] > 10 * held["blocked_by_earlier_unresolved_row"]


def test_overflow_budget_setting_is_validated_and_defaults_to_ten_caps() -> None:
    from src.models.schema import RetryConsumerConfig

    assert RetryConsumerConfig().max_overflow_held_per_partition is None
    assert RetryConsumerConfig(max_overflow_held_per_partition=0).max_overflow_held_per_partition == 0
    for bad in (True, -1, "x"):
        with pytest.raises(ValueError):
            RetryConsumerConfig(max_overflow_held_per_partition=bad)
    with pytest.raises(ValueError):
        RetryConsumerConfig(max_overflow_held=5)  # extra="forbid"
    rc, _ = _make_consumer(max_records_per_partition=7)
    assert rc._max_overflow_held_per_partition == 70
