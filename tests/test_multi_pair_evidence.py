"""Final follow-ups S4 (orchestrator decision under D1): for a type of several label pairs, the WRITTEN evidence that
lets a row's own failure count is per (type, label pair). A written Person->Post row says nothing about a
Person->Comment row that keeps failing alone; single-pair types are unchanged (any write of the type counts).

A pair whose rows fail with none of them written is reported as a stall of its type (gauge per type, no pair label;
an ERROR naming the pair) even while the other pair's writes keep the type-wide stall clear.
"""
from __future__ import annotations

import dataclasses
import json
import logging

from src.loader.retry_consumer import RETRY_OUTCOME_WRITTEN, RetryConsumer
from tests.test_retry_consumer import _OOM, _Clock, _envelope, _FakeConsumer, _RepublishingSink, _run

PAIRS = {"posts": ("Person", "Post"), "comments": ("Person", "Comment")}


def _row(topic: str, source: str, offset: int):
    envelope = _envelope(source_key=source, target_key="x", original_topic=topic, original_offset=offset)
    if topic in PAIRS:
        key = json.dumps([*PAIRS[topic], source, "x"], separators=(",", ":")).encode()
        envelope = dataclasses.replace(envelope, key_bytes=key)
    return envelope


class _HugeRowWriter:
    """The row from source ``huge`` always exhausts the memory pool, alone or not; every other row is written."""

    def retry_batch(self, envelopes, budget_seconds=None):
        if any(json.loads(e.key_bytes)[-2] == "huge" for e in envelopes):
            raise _OOM.with_traceback(None)
        return {i: (RETRY_OUTCOME_WRITTEN, "") for i in range(len(envelopes))}


def _run_huge_row(huge_topic: str, traffic_topic: str, *, multi_pair: bool = True, seconds: int = 1800):
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    sink = _RepublishingSink(consumer, wall)
    stalls: dict[str, float] = {}
    rc = RetryConsumer(
        consumer=consumer, writer_by_edge_type={"KNOWS": _HugeRowWriter()}, sink_by_edge_type={"KNOWS": sink},
        max_retry_attempts_by_edge_type={"KNOWS": 2}, parked_topic_by_edge_type={"KNOWS": "knows.dlq.parked"},
        base_delay_seconds_by_edge_type={"KNOWS": 1.0}, max_delay_seconds_by_edge_type={"KNOWS": 60.0},
        due_batch_coalesce_seconds=0.0, wall_clock=wall, monotonic_clock=lambda: wall() / 1000.0,
        on_stall_seconds=lambda edge_type, seconds: stalls.__setitem__(edge_type, seconds),
        endpoint_pairs_by_edge_type={"KNOWS": PAIRS} if multi_pair else None,
    )
    sink.push_new(_row(huge_topic, "huge", 0))

    def traffic(second):
        if second < 1200:
            for k in range(3):
                sink.push_new(_row(traffic_topic, f"s{second}-{k}", 1 + 3 * second + k))

    _run(rc, wall, seconds, each=traffic)
    return rc, sink, stalls


def test_a_comment_row_is_not_counted_on_the_evidence_of_written_post_rows(caplog) -> None:
    with caplog.at_level(logging.ERROR):
        rc, sink, stalls = _run_huge_row("comments", "posts")
    assert sink.parked == []
    assert {json.loads(c["envelope"].key_bytes)[2] for c in sink.calls} == set()  # never re-DLQ'd as write_failed
    assert not rc._write_failures
    huge = [key for key in rc._failing.get("KNOWS", set())]
    assert len(huge) == 1  # waits, outstanding
    assert stalls["KNOWS"] > 1000  # signalled although the Post rows keep writing
    assert "pair=Person>Comment reason=retry_stalled" in caplog.text


def test_written_rows_of_the_same_pair_are_evidence_and_the_row_terminates() -> None:
    rc, sink, _stalls = _run_huge_row("comments", "comments")
    assert [json.loads(key)[2] for key, _reason, _attempt in sink.parked] == ["huge"]
    assert not any(rc._outstanding.values())


def test_a_single_pair_type_still_counts_any_write_of_the_type() -> None:
    """Unchanged for single-pair types: the same scenario without label pairs parks the row."""
    rc, sink, _stalls = _run_huge_row("knows-events", "knows-events", multi_pair=False)
    assert [json.loads(key)[0] for key, _reason, _attempt in sink.parked] == ["huge"]
    assert not any(rc._outstanding.values())


def test_the_pair_stall_clears_once_the_pair_writes() -> None:
    """The huge Comment row is resolved another way (a duplicate skip after its own entry left), the gauge
    returns to 0 for the type."""
    rc, _sink, stalls = _run_huge_row("comments", "posts", seconds=300)
    assert stalls["KNOWS"] > 200
    rc._forget_failing("KNOWS", list(rc._failing["KNOWS"]))
    assert stalls["KNOWS"] == 0.0 and not rc._pair_stall_since
