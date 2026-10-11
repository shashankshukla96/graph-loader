"""v7-S4 review: randomised pair-order fuzz of the retry consumer with a SEQUENCED parked-pair
index (records of one pair from two topics, causal seq first in the order key), ported from the review
probes pK_seq_fuzz / pK_seq_control / pB_seq.

Chains, P5-S3 replay duplicates, crash/restart from the committed offset, tombstone failures, publish
failures, write failures of every class, small batches. Invariants: (1) per pair the written records
never go backwards in the pair order within one process epoch, and the last write is the newest;
(2) once healthy, every record terminates (nothing left in the index). The control (no order_key, so
topic-name order) must fail, which shows the fuzz is sensitive.
"""
from __future__ import annotations

import dataclasses
import json
import logging
import random

import pytest
from neo4j.exceptions import ServiceUnavailable

from src.loader.retry_consumer import RETRY_OUTCOME_UNRESOLVED, RETRY_OUTCOME_WRITTEN, RetryConsumer, _PartialOutcomes
from tests.test_retry_consumer import (
    _Clock, _dlq_message, _envelope, _FakeConsumer, _FakeDlqMessage, _make_consumer, _neo_error,
)


@pytest.fixture(autouse=True)
def _quiet_logs():
    """The fuzz drives thousands of logged failures; capturing them dominates the runtime."""
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


class StatefulIndex:
    """Realistic local view: per pair, a set of outstanding identities.

    v7-S7: ``pair_of`` maps an original topic to its label pair (a constant for a single-pair type),
    and the pair key is ``(label pair of the topic, s, t)``, like ``ParkedPairIndex(pair_of_topic=...)``."""

    def __init__(self, fail_remove=None, pair_of=None):
        self.by_pair = {}
        self.fail_remove = set(fail_remove or ())  # identities whose tombstone 'fails' (once)
        self.removed = []
        self.pair_of = pair_of or (lambda topic: "-")

    def _pk(self, s, t, topic):
        return json.dumps([self.pair_of(topic), s, t])

    def add(self, s, t, ident):
        self.by_pair.setdefault(self._pk(s, t, ident[0]), set()).add(ident)

    def watermark_snapshot(self):
        return {0: 0}

    def caught_up(self, snapshot):
        return True

    def contains(self, s, t, *, original_topic, original_partition, original_offset):
        return (original_topic, original_partition, original_offset) in self.by_pair.get(
            self._pk(s, t, original_topic), set())

    def is_blocked_by_earlier(self, s, t, *, original_topic, original_partition, original_offset, first_failure_at_ms):
        own = (original_topic, original_partition, original_offset)
        return any(i < own for i in self.by_pair.get(self._pk(s, t, original_topic), set()))

    def earlier_outstanding(self, s, t, *, original_topic, original_partition, original_offset):
        own = (original_topic, original_partition, original_offset)
        return sorted(i for i in self.by_pair.get(self._pk(s, t, original_topic), set()) if i < own)

    def remove_many(self, records):
        out = []
        for s, t, topic, partition, offset in records:
            ident = (topic, partition, offset)
            if ident in self.fail_remove:
                self.fail_remove.discard(ident)
                out.append(RuntimeError("tombstone undelivered"))
                continue
            self.by_pair.get(self._pk(s, t, topic), set()).discard(ident)
            self.removed.append(ident)
            out.append(None)
        return out


class RecordingWriter:
    """Writes every envelope (WRITTEN) and logs the write order per pair."""

    def __init__(self, fail=None):
        self.writes = []  # (key_bytes, original_offset, attempt)
        self.calls = []
        self.fail = fail  # callable(envelopes) -> exception or None

    def retry_batch(self, envelopes, budget_seconds=None):
        self.calls.append([(e.key_bytes, e.original_offset) for e in envelopes])
        if self.fail is not None:
            exc = self.fail(envelopes)
            if exc is not None:
                raise exc
        for e in envelopes:
            self.writes.append((e.key_bytes, e.original_offset, e.attempt_count))
        return {i: (RETRY_OUTCOME_WRITTEN, "") for i in range(len(envelopes))}


class RepublishingSink:
    """Re-publishes re-DLQ'd/blocked envelopes back into the consumer at a new DLQ offset."""

    def __init__(self, consumer, wall, start_offset=10_000):
        self.consumer, self.wall, self.next_offset = consumer, wall, start_offset
        self.parked, self.calls, self.published = [], [], []

    def reroute_or_park_many(self, requests):
        out = []
        for r in requests:
            self.calls.append(r)
            env = r["envelope"]
            if r.get("parked_topic"):
                self.parked.append((env.key_bytes, env.original_offset, r["reason_code"], env.attempt_count))
                out.append("parked")
                continue
            attempt = env.attempt_count + (1 if r.get("increment_attempt", True) else 0)
            new = dataclasses.replace(
                env, attempt_count=attempt, reason_code=r["reason_code"],
                retry_after_ms=int(self.wall() + r["retry_after_seconds_for_reattempt"] * 1000),
            )
            self.published.append((self.next_offset, new.original_offset, r["reason_code"]))
            self.consumer.push(_dlq_message(new, offset=self.next_offset))
            self.next_offset += 1
            out.append("re_dlq")
        return out

class SeqIndex(StatefulIndex):
    def __init__(self, fail_remove=None, pair_of=None):
        super().__init__(fail_remove, pair_of); self.seq = {}
    def add(self, s, t, ident):
        pk = self._pk(s, t, ident[0])
        if ident not in self.by_pair.get(pk, set()):
            self.seq[(pk, ident)] = 1 + max((self.seq[(pk, i)] for i in self.by_pair.get(pk, set())), default=0)
        super().add(s, t, ident)
    def _ok(self, pk, ident):
        return (self.seq.get((pk, ident), 0), *ident) if ident in self.by_pair.get(pk, set()) else (0, *ident)
    def order_key(self, s, t, *, original_topic, original_partition, original_offset):
        return self._ok(self._pk(s, t, original_topic), (original_topic, original_partition, original_offset))
    def is_blocked_by_earlier(self, s, t, *, original_topic, original_partition, original_offset, first_failure_at_ms):
        pk = self._pk(s, t, original_topic); own = self._ok(pk, (original_topic, original_partition, original_offset))
        return any(self._ok(pk, i) < own for i in self.by_pair.get(pk, set()) if i != own[1:])
    def earlier_outstanding(self, s, t, *, original_topic, original_partition, original_offset):
        pk = self._pk(s, t, original_topic); own = self._ok(pk, (original_topic, original_partition, original_offset))
        return [o[1:] for o in sorted(self._ok(pk, i) for i in self.by_pair.get(pk, set()) if i != own[1:] and self._ok(pk, i) < own)]

ERRS = [ServiceUnavailable("down"), _neo_error("Neo.ClientError.General.UnknownError", "u"),
        _neo_error("Neo.TransientError.General.MemoryPoolOutOfMemoryError", "m"),
        _neo_error("Neo.ClientError.Transaction.TransactionTimedOut", "t")]

class Log:
    def __init__(self): self.msgs = []  # partition 0 only, offsets = index
    def append(self, env):
        self.msgs.append(_FakeDlqMessage("knows.dlq", 0, len(self.msgs), env)); return len(self.msgs) - 1

class Consumer(_FakeConsumer):
    def __init__(self, log, start):
        super().__init__(); self.log, self.pos = log, start
    def poll(self, timeout=None):
        if ("knows.dlq", 0) in self.paused_now or self.pos >= len(self.log.msgs): return None
        m = self.log.msgs[self.pos]; self.pos += 1; return m
    def commit(self, *, message=None, offsets=None, asynchronous=False):
        for tp in offsets: self.committed_pos = tp.offset

# v7-S7: topics a and b bind Person->Company, topic c binds Person->Comment (colliding keys).
PAIRS = {'works-at-a': ("Person", "Company"), 'works-at-b': ("Person", "Company"), 'likes-c': ("Person", "Comment")}

REAL_SINK = True
"""Final follow-ups, Part B: the model's re-published copy carries the request's ``reason_code`` (and its
routing time), as the real sink's does. Before, a blocked copy kept its old reason, so the consumer treated
it as an ordinary retry (held at most ``max_delay``) instead of a blocked one."""


def _pair_id(envelope):
    """``(label pair, source key)`` of a (canonical) key: what one relationship is."""
    key = json.loads(envelope.key_bytes)
    return (tuple(key[:2]), key[2]) if len(key) == 4 else (("-", "-"), key[0])


def run(seed, verbose=False, multi_pair=False, *, steps=400, dead=False, collapsed=False, stats=None,
        resurrect=0.0, crashes=True):
    """``dead``: pair p0's endpoint never arrives (its records must park), and once healthy the
    sink stops failing; ``collapsed``: a single-pair type (topics a, b) whose DLQ still holds keys
    labelled with its own pair from its multi-pair days, mixed with legacy keys (final follow-ups S2);
    ``stats`` (a dict) receives the DLQ log and the parked envelopes. ``resurrect``: per-step chance
    (first 300 steps) that a live flush is redelivered: each of its rows whose pair is blocked at that
    moment is re-added to the index (a new seq once it was removed) and re-published as a fresh DLQ copy
    (replay resurrection, own RNG). ``crashes=False``: no retry-consumer restarts (the resolved memory
    is per process)."""
    rnd = random.Random(seed)
    rnd_redeliver = random.Random(seed + 7919)
    live_flushes = []
    wall = _Clock(start=10_000)
    log = Log()
    index = SeqIndex(pair_of=(lambda topic: list(PAIRS.get(topic, ("?", topic)))) if multi_pair else None)
    seqof = {}
    topics = ['works-at-a', 'works-at-b'] + (['likes-c'] if multi_pair or collapsed else [])
    topic_off = {topic: 0 for topic in topics}
    writes = {}
    violations = []
    healthy = {"on": False}
    arrived = {}  # pair -> time endpoints arrive
    # P5-S3 live flushes: rows of several pairs, with replays
    pairs = [f"p{i}" for i in range(rnd.randint(2, 6))]
    for _flush in range(rnd.randint(2, 5)):
        flush = []
        for _ in range(rnd.randint(1, 6)):
            pair = rnd.choice(pairs)
            topic = rnd.choice(topics); o = topic_off[topic]; topic_off[topic] += 1
            env = _envelope(source_key=pair, target_key="t", original_topic=topic, original_offset=o, retry_after_ms=1_000)
            leftover = collapsed and topic == 'likes-c'  # a removed pair's row, still in the DLQ
            if (multi_pair or collapsed) and (leftover or rnd.random() < 0.7):  # the rest keep a legacy [sk, tk] key
                env = dataclasses.replace(env, key_bytes=json.dumps([*PAIRS[topic], pair, "t"],
                                                                     separators=(",", ":")).encode())
            index.add(pair, "t", (topic, 0, o))
            pid = ((PAIRS[topic] if multi_pair or leftover else ("-", "-")), pair)
            seqof[(pid, topic, o)] = index.seq[(index._pk(pair, "t", topic), (topic, 0, o))]
            flush.append(env)
        live_flushes.append(list(flush))
        for e in flush: log.append(e)
        if rnd.random() < 0.5:  # partial publish failure -> whole flush replayed
            for e in flush: log.append(e)
    for p in pairs:
        arrived[(("-", "-"), p)] = rnd.randint(0, 40) * 1000
        if multi_pair:
            for labels in set(PAIRS.values()):
                arrived[(labels, p)] = rnd.randint(0, 40) * 1000
    if dead:
        for pair in list(arrived):
            if pair[1] == "p0":
                arrived[pair] = 10**12
    parked = []
    if stats is not None:
        stats.update(log=log, first_copies=len(log.msgs), parked=parked)
    class Sink:
        def reroute_or_park_many(self, requests):
            out = []
            for r in requests:
                if (not dead or not healthy["on"]) and rnd.random() < 0.1:
                    if rnd.random() < 0.5:  # publish 'failed' but actually landed (at-least-once)
                        self._pub(r)
                    out.append(RuntimeError("publish failed")); continue
                if r.get("parked_topic"):
                    parked.append(r["envelope"]); out.append("parked"); continue
                self._pub(r); out.append("re_dlq")
            return out
        def _pub(self, r):
            if r.get("parked_topic"): return
            env = r["envelope"]
            new = dataclasses.replace(env, attempt_count=env.attempt_count + (1 if r.get("increment_attempt", True) else 0),
                                      retry_after_ms=int(wall() + r["retry_after_seconds_for_reattempt"] * 1000))
            if REAL_SINK:
                # Like DeadLetterRecordSink._build_reroute: the copy carries the request's reason
                # (a blocked copy says so) and its routing time.
                new = dataclasses.replace(new, reason_code=r["reason_code"], dlq_routed_at_ms=int(wall()))
            log.append(new)
    class Writer:
        def retry_batch(self, envs, budget_seconds=None):
            if not healthy["on"] and rnd.random() < 0.4:
                raise rnd.choice(ERRS)
            out = {}
            for i, e in enumerate(envs):
                if multi_pair and len(json.loads(e.key_bytes)) != 4:  # review round 5 N3
                    violations.append(("legacy key reached the writer", e.original_topic, e.original_offset))
                if collapsed and len(json.loads(e.key_bytes)) != 2:  # final follow-ups S2
                    if e.original_topic != 'likes-c':  # its own pair's labelled key: read back as [sk, tk]
                        violations.append(("own-pair labelled key reached the single-pair writer",
                                           e.original_topic, e.original_offset))
                    # The single-pair writer refuses another pair's key (the real one reports this).
                    out[i] = (RETRY_OUTCOME_UNRESOLVED, "invalid_on_retry"); continue
                pair = _pair_id(e)
                if wall() < 10_000 + arrived[pair]:
                    out[i] = (RETRY_OUTCOME_UNRESOLVED, "missing_target"); continue
                out[i] = (RETRY_OUTCOME_WRITTEN, "")
            err = None
            if not healthy["on"] and len(envs) > 1 and rnd.random() < 0.2:
                keep = rnd.randint(0, len(envs) - 1)
                out = {k: v for k, v in out.items() if k <= keep}
                err = rnd.choice(ERRS)
            for i, (o, _r) in out.items():
                if o != RETRY_OUTCOME_WRITTEN: continue
                e = envs[i]; pair = _pair_id(e); ident = seqof[(pair, e.original_topic, e.original_offset)]
                seq = writes.setdefault(pair, [])
                if seq and ident < seq[-1][0] and seq[-1][1] == epoch[0]:
                    violations.append(("same-epoch regression", pair, [x for x, _ in seq], ident))
                seq.append((ident, epoch[0]))
            if err is not None:
                return _PartialOutcomes(out, err)
            return out
    def tomb_fail(records):
        res = []
        for s, t, topic, partition, offset in records:
            ident = (topic, partition, offset)
            if not healthy["on"] and rnd.random() < 0.25:
                res.append(RuntimeError("tombstone undelivered")); continue
            index.by_pair.get(index._pk(s, t, topic), set()).discard(ident); res.append(None)
        return res
    index.remove_many = tomb_fail
    committed = 0
    epoch = [0]
    def new_rc():
        c = Consumer(log, committed); c.committed_pos = committed
        rc = RetryConsumer(
            consumer=c, writer_by_edge_type={"KNOWS": Writer()}, sink_by_edge_type={"KNOWS": Sink()},
            parked_pair_index_by_edge_type={"KNOWS": index}, max_retry_attempts_by_edge_type={"KNOWS": 50},
            parked_topic_by_edge_type={"KNOWS": "p"}, base_delay_seconds_by_edge_type={"KNOWS": 0.5},
            max_delay_seconds_by_edge_type={"KNOWS": 2.0}, due_batch_coalesce_seconds=0.0,
            wall_clock=wall, monotonic_clock=lambda: wall() / 1000.0, max_batch_size=rnd.choice([1, 2, 3, 5, 2000]),
            endpoint_pairs_by_edge_type={"KNOWS": PAIRS} if multi_pair else None,
            single_pair_labels_by_edge_type={"KNOWS": PAIRS["works-at-a"]} if collapsed else None,
        )
        return rc, c
    rc, c = new_rc()
    for step in range(steps):
        if step == 250: healthy["on"] = True
        if not healthy["on"] and rnd.random() < 0.03 and crashes:  # crash: restart from committed offset
            committed = c.committed_pos; rc, c = new_rc(); epoch[0] += 1
        if resurrect and step < 300 and rnd_redeliver.random() < resurrect:
            for e in rnd_redeliver.choice(live_flushes):
                s, t = json.loads(e.key_bytes)[-2:]
                if index.by_pair.get(index._pk(s, t, e.original_topic)):  # blocked: the live loader re-routes it
                    index.add(s, t, (e.original_topic, 0, e.original_offset))
                    log.append(dataclasses.replace(
                        e, first_failure_at_ms=int(wall()), dlq_routed_at_ms=int(wall()), retry_after_ms=int(wall()),
                        reason_code="blocked_by_earlier_unresolved_row",
                    ))
        rc.tick(); wall.advance(rnd.choice([100, 500, 1000]))
    left = {k: v for k, v in index.by_pair.items() if v}
    for pair, seq in writes.items():
        if seq and seq[-1][0] != max(x for x, _ in seq):
            violations.append(('FINAL older', pair, [x for x, _ in seq]))
    return violations, left


@pytest.mark.parametrize("seed", range(80))
def test_sequenced_pair_order_survives_every_failure_and_terminates(seed) -> None:
    violations, left = run(seed)
    assert violations == [] and left == {}


def test_the_fuzz_is_sensitive_without_the_order_key(monkeypatch) -> None:
    monkeypatch.setattr(SeqIndex, "order_key", None)  # retry consumer falls back to topic-name order
    assert sum(1 for seed in range(80) if run(seed)[0]) >= 10


@pytest.mark.parametrize("seed", range(80))
def test_two_label_pairs_with_colliding_keys_keep_each_pairs_order(seed) -> None:
    """v7-S7: a third topic binds another label pair with the same keys; legacy keys mixed in."""
    violations, left = run(seed, multi_pair=True)
    assert violations == [] and left == {}


def test_the_two_pair_fuzz_is_sensitive_without_the_order_key(monkeypatch) -> None:
    monkeypatch.setattr(SeqIndex, "order_key", None)
    assert sum(1 for seed in range(80) if run(seed, multi_pair=True)[0]) >= 10


def test_without_the_read_time_rewrite_legacy_keys_reach_the_writer_but_order_still_holds(monkeypatch) -> None:
    """Final follow-ups S3: what the multi-pair read-time key rewrite (``canonical_pair_key``) is for.

    Without it, legacy ``[sk, tk]`` keys reach the writer in most seeds (the fuzz flags each one; its model
    writer cannot attribute an unlabelled row, so those seeds also stall) -- but no seed shows an order
    violation: a pair's order rests on the parked-pair index, not on the key form. So the fuzz proves the
    rewrite is applied and keeps the consumer live; it does not show that order depends on it. The
    order-sensitive control of the two-pair fuzz is the one without the order key (above)."""
    import src.loader.retry_consumer as retry_consumer_module

    monkeypatch.setattr(retry_consumer_module, "canonical_pair_key", lambda envelope, labels: envelope)
    results = [run(seed, multi_pair=True)[0] for seed in range(80)]
    assert sum(1 for v in results if any(kind == "legacy key reached the writer" for kind, *_ in v)) >= 60
    assert [v for v in results if any(kind != "legacy key reached the writer" for kind, *_ in v)] == []


@pytest.mark.parametrize("failing_tombstone", [True, False])
def test_cross_topic_chain_with_a_failing_tombstone_keeps_pair_order(failing_tombstone) -> None:
    """pB_seq: b's row (seq 1) is ahead of a's (seq 2, the reverse of name order) and was re-DLQ'd
    behind it. b's tombstone fails once: a is never written past it, b never rewritten after a."""
    wall = _Clock(start=10_000)
    consumer = _FakeConsumer()
    index = SeqIndex(fail_remove={("works-at-b", 0, 40)} if failing_tombstone else set())
    index.add("p1", "p2", ("works-at-b", 0, 40))
    index.add("p1", "p2", ("works-at-a", 0, 2))
    writer = RecordingWriter()
    sink = RepublishingSink(consumer, wall)
    rc, _ = _make_consumer(consumer=consumer, writer=writer, sink=sink, index=index, wall_clock=wall,
                           monotonic_clock=lambda: wall() / 1000.0)
    for dlq_offset, (topic, offset) in enumerate([("works-at-a", 2), ("works-at-b", 40)]):
        consumer.push(_dlq_message(_envelope(original_topic=topic, original_offset=offset), offset=100 + dlq_offset))
    for _ in range(20):
        rc.tick()
        wall.advance(1_000)
    order = [offset for _key, offset, _attempt in writer.writes]
    assert order[-1] == 2 and 2 not in order[:-1]  # a's row once, last
    assert set(order[:-1]) == {40}
    assert not any(index.by_pair.values())


# ── Final follow-ups S2: a type collapsed back to one label pair ─────────────


@pytest.mark.parametrize("seed", range(40))
def test_a_collapsed_type_reads_its_own_labelled_keys_back_and_parks_the_removed_pairs_rows(seed) -> None:
    """LIKES was {a, b: Person->Company; c: Person->Comment} and is now single-pair {a, b} with its DLQ not
    drained (p_collapse_fuzz / p_collapse2): a/b keys are mixed [S, T, sk, tk] / [sk, tk], c's rows keep
    their Person->Comment keys. Own-pair keys never reach the writer labelled (one identity per
    relationship), every relationship keeps its order, and c's rows park (invalid_on_retry)."""
    stats = {}
    violations, left = run(seed, collapsed=True, steps=6000, stats=stats)
    assert violations == [] and left == {}


def test_the_collapsed_fuzz_is_sensitive_without_the_read_time_rewrite(monkeypatch) -> None:
    import src.loader.retry_consumer as retry_consumer_module

    monkeypatch.setattr(retry_consumer_module, "single_pair_key", lambda envelope, labels: envelope)
    assert sum(1 for seed in range(40) if run(seed, collapsed=True, steps=400)[0]) >= 30


# ── Final follow-ups, Part B: liveness of the retry consumer ────────────────


def _dead_pair_copies(stats) -> tuple[int, int, int]:
    """(DLQ copies of p0's records, p0's distinct records, p0 records parked)."""
    from src.loader.dead_letter import decode_envelope

    def is_p0(envelope):
        return "p0" in json.loads(envelope.key_bytes)

    p0 = [e for e in (decode_envelope(m.value()) for m in stats["log"].msgs) if is_p0(e)]
    rows = {(e.original_topic, e.original_offset) for e in p0}
    parked = {(e.original_topic, e.original_offset) for e in stats["parked"] if is_p0(e)}
    return len(p0), len(rows), len(parked)


@pytest.mark.parametrize("multi_pair", [False, True], ids=["one-pair", "two-pairs"])
@pytest.mark.parametrize("seed", range(30))
def test_a_pair_whose_endpoint_never_arrives_parks_with_bounded_copies(seed, multi_pair) -> None:
    """Part B: p0's endpoint never arrives. Every p0 record parks (max_retry_attempts=50) within
    6,000 steps (about 3,200 s simulated) and the DLQ holds at most 2 x (attempts + 1) copies per
    record. Before the fix 11/30 one-pair seeds had not parked after 3,000 steps, one record had
    1,916 copies (parallel re-DLQ lineages of stale copies, re-read after each restart), and with
    a batch cap of 1-3 the roots were starved by their own followers."""
    stats = {}
    violations, left = run(seed, multi_pair=multi_pair, steps=6000, dead=True, stats=stats)
    assert violations == [] and left == {}
    copies, rows, parked = _dead_pair_copies(stats)
    assert parked == rows
    assert copies <= 2 * rows * 51


def test_the_dead_pair_fuzz_is_sensitive_without_the_liveness_fixes(monkeypatch) -> None:
    """Control: without the superseded-copy skip and the longest-due-first batch order (the
    pre-fix behaviour), the same model leaves records unparked or lets copies multiply."""
    monkeypatch.setattr(RetryConsumer, "_is_superseded_copy", lambda self, edge_type, pending, newest: False)
    monkeypatch.setattr(RetryConsumer, "_due_age_rank", lambda self, key: 0)
    bad = 0
    for seed in range(30):
        stats = {}
        _violations, left = run(seed, steps=3000, dead=True, stats=stats)
        copies, rows, _parked = _dead_pair_copies(stats)
        bad += bool(left) or copies > 2 * rows * 51
    assert bad >= 5


# ── Replay resurrection: redelivered live flushes re-add resolved rows ───────


@pytest.mark.parametrize("multi_pair", [False, True], ids=["one-pair", "two-pairs"])
@pytest.mark.parametrize("seed", range(60))
def test_redelivered_flushes_never_reorder_a_pair_within_one_retry_consumer_process(seed, multi_pair) -> None:
    """Live flushes are redelivered at random (a crash, a stop-drain abandon, a failed revoke settlement
    of the live loader) and re-route their blocked rows, including rows the retry consumer already wrote
    or parked. Every other failure of the fuzz stays on; retry-consumer restarts are off, because the
    resolved memory is per process (documented gap, USAGE §12)."""
    violations, left = run(seed, multi_pair=multi_pair, resurrect=0.05, crashes=False)
    assert violations == [] and left == {}


def test_the_redelivery_fuzz_is_sensitive_without_the_resolved_memory(monkeypatch) -> None:
    monkeypatch.setattr(RetryConsumer, "_resolved_offset_at_or_above", lambda self, edge_type, pending: None)
    assert sum(1 for seed in range(60) if run(seed, resurrect=0.05, crashes=False)[0]) >= 10
