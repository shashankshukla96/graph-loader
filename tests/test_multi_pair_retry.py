"""v7-S7 story 7.8: the retry consumer retries each DLQ'd row through the label pair of its original_topic."""
from __future__ import annotations

import argparse
import logging
import dataclasses
import json
from unittest.mock import MagicMock, patch

import pytest

from multi_pair_support import BACKFILL, COMMENTS, POSTS, GraphStore, likes, likes_schema, record
from src import cli
from src.loader.dead_letter import DeadLetterRecordSink
from src.loader.edge_execution import IndexedMissingEndpointSink, LabelledEndpointSink, LeaseDrainAbandoned
from src.loader.edge_records import normalize_edge_record
from src.loader.lease_client import BucketCellRouter, BucketLeaseClient
from src.loader.retry_consumer import (
    RETRY_OUTCOME_WRITTEN,
    PairSplitRetryWriter,
    RetryConsumer,
    _pair_from_envelope,
    _PartialOutcomes,
    build_pair_retry_writers,
    build_retry_base_writer,
    build_retry_lease_coordinator,
    build_retry_writer,
    canonical_pair_key,
)
from test_multi_pair_leases import _LeaseServer
from test_parked_pair_index import _FakeTopicLog, _wait_caught_up
from test_retry_consumer import _Clock, _FakeConsumer, _FakeDlqMessage
from tests.test_multi_topic_dlq import _AckingIndexProducer
from tests.test_parked_pair_index import _FakeConsumer as _IndexConsumer
from src.loader.parked_pair_index import ParkedPairIndex

DLQ = "LIKES.dlq"
PARKED = "LIKES.dlq.parked"


def _edge(**kwargs):
    return likes(order_by=False, topic_conflict="arrival",
                 dead_letter={"enabled": True, "max_retry_attempts": kwargs.pop("attempts", 50)}, **kwargs)


def _index(edge, log: _FakeTopicLog) -> ParkedPairIndex:
    index = ParkedPairIndex(
        topic="LIKES.dlq.parked-pairs", bootstrap_servers="kafka:9092", writable=True, sequenced=True,
        pair_of_topic=edge.pair_labels_for_topic,
        consumer_factory=lambda config: _IndexConsumer(config, log, "LIKES.dlq.parked-pairs"),
        producer_factory=lambda config: _AckingIndexProducer(config, log), poll_timeout_seconds=0.01,
    )
    index.start()
    return index


class _Loopback:
    """A DLQ producer that re-delivers DLQ publishes to the retry consumer and keeps parked ones."""

    def __init__(self, consumer: _FakeConsumer) -> None:
        self.consumer = consumer
        self.offset = 0
        self.legacy_offset = 0
        self.legacy_partition: int | None = None  # put [sk, tk] keys on another DLQ partition
        self.dlq: list = []
        self.parked: list = []

    def publish(self, topic, envelope, **_kwargs) -> None:
        if topic == PARKED:
            self.parked.append(envelope)
            return
        self.dlq.append(envelope)
        if self.legacy_partition is not None and len(json.loads(envelope.key_bytes)) == 2:
            self.consumer.push(_FakeDlqMessage(DLQ, self.legacy_partition, self.legacy_offset, envelope))
            self.legacy_offset += 1
            return
        self.consumer.push(_FakeDlqMessage(DLQ, 0, self.offset, envelope))
        self.offset += 1

    def publish_many(self, items):
        for topic, envelope, _hook in items:
            self.publish(topic, envelope)
        return [None] * len(items)


class _Harness:
    """The live writer's DLQ hook plus a retry consumer wired like ``retry_consumer.main`` (standalone writers
    on an in-memory graph, a real label-pair index)."""

    def __init__(self, edge, store: GraphStore, *, writer=None, max_batch_size: int = 2000) -> None:
        self.edge, self.store = edge, store
        self.wall = _Clock(start=10_000)
        self.log = _FakeTopicLog()
        self.index = _index(edge, self.log)
        self.consumer = _FakeConsumer()
        self.producer = _Loopback(self.consumer)
        self.sink = DeadLetterRecordSink(producer=self.producer, topic=DLQ, run_id="r", replica_id="0", kind="edge",
                                         clock=lambda: int(self.wall()))
        self.writer = writer or build_pair_retry_writers(edge, lambda config, _deadline: build_retry_writer(
            build_retry_base_writer(store.driver(), config, tx_timeout_seconds=None), chunk_size=1000))
        self.rc = RetryConsumer(
            consumer=self.consumer, writer_by_edge_type={"LIKES": self.writer}, sink_by_edge_type={"LIKES": self.sink},
            parked_pair_index_by_edge_type={"LIKES": self.index},
            max_retry_attempts_by_edge_type={"LIKES": edge.dead_letter.max_retry_attempts},
            parked_topic_by_edge_type={"LIKES": PARKED}, base_delay_seconds_by_edge_type={"LIKES": 0.1},
            max_delay_seconds_by_edge_type={"LIKES": 0.5}, due_batch_coalesce_seconds=0.0,
            wall_clock=lambda: int(self.wall()), monotonic_clock=lambda: self.wall() / 1000.0,
            max_batch_size=max_batch_size,
            endpoint_pairs_by_edge_type={"LIKES": {t: p.labels for p in edge.endpoint_pairs for t in p.topics}},
        )

    def live_dlq(self, topic: str, person, other, offset: int, *, partition: int = 0, legacy_key: bool = False):
        view = self.edge.topic_view(topic)
        pair = self.edge.pair_for_topic(topic)
        target = self.sink if legacy_key else LabelledEndpointSink(self.sink, pair.labels)
        hook = IndexedMissingEndpointSink(target, self.index.scoped(pair.labels), clock=lambda: int(self.wall()))
        hook.route_unresolved_endpoint(
            topic=topic, partition=partition, offset=offset,
            record=normalize_edge_record(record(topic, person, other, f"2024-01-01T00:00:{offset:02d}+00:00",
                                                note=f"{topic}@{offset}"), view),
            edge_type="LIKES", source_key_property=view.source_key_property,
            target_key_property=view.target_key_property, failure_class="missing_endpoint",
            reason_code="missing_target", error_text="x", retry_after_seconds=0.0,
        )

    def run(self, ticks: int = 60) -> None:
        for _ in range(ticks):
            _wait_caught_up(self.index)
            self.rc.tick()
            self.wall.advance(600)

    def close(self) -> None:
        self.index.stop()


# ── AC 1, 2: independent pairs, sequenced order within a pair ─────────────────


def test_colliding_post_and_comment_rows_retry_independently_and_exactly() -> None:
    edge = _edge()
    store = GraphStore({"Person": {1}})
    h = _Harness(edge, store)
    try:
        h.live_dlq(POSTS, 1, 1, offset=1)
        h.live_dlq(COMMENTS, 1, 1, offset=1)
        h.run(10)
        assert store.rels == {} and not h.producer.parked
        store.nodes["Post"] = {1}  # publishing the Post nodes releases only the Post row
        h.run(10)
        assert set(store.rels) == {("LIKES", "Person", 1, "Post", 1)}
        assert h.index.is_blocked(1, 1, endpoint_pair=("Person", "Comment"))
        store.nodes["Comment"] = {1}
        h.run(10)
        assert set(store.rels) == {("LIKES", "Person", 1, "Post", 1), ("LIKES", "Person", 1, "Comment", 1)}
        assert not h.index.is_blocked(1, 1, endpoint_pair=("Person", "Comment"))
        assert not h.index.is_blocked(1, 1, endpoint_pair=("Person", "Post"))
        assert not h.producer.parked
        assert {e.key_bytes for e in h.producer.dlq} == {b'["Person","Post",1,1]', b'["Person","Comment",1,1]'}
        assert all(len(tx["pairs"]) == 1 for tx in store.transactions)
    finally:
        h.close()


def test_a_pairs_records_from_two_topics_retry_in_arrival_order_beside_colliding_comments() -> None:
    edge = _edge()
    store = GraphStore({"Person": {1}, "Post": set(), "Comment": {1}})
    h = _Harness(edge, store)
    try:
        h.live_dlq(BACKFILL, 1, 1, offset=40)  # b first (names sort the other way)
        h.live_dlq(COMMENTS, 1, 1, offset=3)
        h.live_dlq(POSTS, 1, 1, offset=2)  # then a, sequenced behind b
        h.live_dlq(COMMENTS, 1, 1, offset=4)
        h.run(10)
        assert set(store.rels) == {("LIKES", "Person", 1, "Comment", 1)}
        assert store.rels[("LIKES", "Person", 1, "Comment", 1)]["note"] == f"{COMMENTS}@4"
        store.nodes["Post"] = {1}
        h.run(10)
        # The last-indexed Post row wins: a's (posts@2) was sequenced after b's (backfill@40).
        assert store.rels[("LIKES", "Person", 1, "Post", 1)]["note"] == f"{POSTS}@2"
        post_writes = [row["properties"]["note"] for tx in store.committed_writes() for row in tx["rows"]
                       if ("Person", "Post") in tx["pairs"]]
        assert post_writes == [f"{BACKFILL}@40", f"{POSTS}@2"]
    finally:
        h.close()


# ── AC 3: legacy keys ─────────────────────────────────────────────────────────


def test_a_legacy_two_element_key_joins_its_pairs_chain() -> None:
    edge = _edge()
    store = GraphStore({"Person": {1}, "Comment": {1}})
    h = _Harness(edge, store)
    try:
        h.live_dlq(POSTS, 1, 1, offset=1, legacy_key=True)  # written before S7: [sk, tk]
        h.live_dlq(BACKFILL, 1, 1, offset=1)
        assert [e.key_bytes for e in h.producer.dlq] == [b"[1,1]", b'["Person","Post",1,1]']
        h.run(5)
        # Canonicalised at read: the re-DLQ'd copies carry the labelled key, one chain for the pair.
        assert {e.key_bytes for e in h.producer.dlq[2:]} == {b'["Person","Post",1,1]'}
        store.nodes["Post"] = {1}
        h.run(10)
        assert store.rels[("LIKES", "Person", 1, "Post", 1)]["note"] == f"{BACKFILL}@1"
    finally:
        h.close()


def test_legacy_and_labelled_copies_on_different_dlq_partitions_keep_the_index_order() -> None:
    """The legacy copy lands on DLQ partition 1, the labelled ones on 0: only the index sequence orders
    them, and it is per labelled pair (a colliding Comment row sits in the same partitions)."""
    edge = _edge()
    store = GraphStore({"Person": {1}, "Comment": {1}})
    h = _Harness(edge, store)
    h.producer.legacy_partition = 1
    try:
        h.live_dlq(BACKFILL, 1, 1, offset=7, legacy_key=True)  # seq 1, partition 1 (its name sorts last)
        h.live_dlq(COMMENTS, 1, 1, offset=1)
        h.live_dlq(POSTS, 1, 1, offset=3)  # seq 2, partition 0
        h.run(5)
        store.nodes["Post"] = {1}
        h.run(10)
        post_writes = [row["properties"]["note"] for tx in store.committed_writes() for row in tx["rows"]
                       if ("Person", "Post") in tx["pairs"]]
        assert post_writes == [f"{BACKFILL}@7", f"{POSTS}@3"]
        assert set(store.rels) == {("LIKES", "Person", 1, "Post", 1), ("LIKES", "Person", 1, "Comment", 1)}
    finally:
        h.close()


def test_canonical_pair_key_only_rewrites_legacy_keys_of_known_topics() -> None:
    from test_retry_consumer import _envelope

    labels = {POSTS: ("Person", "Post")}
    legacy = _envelope(entity_name="LIKES", source_key=1, target_key=2, original_topic=POSTS)
    assert canonical_pair_key(legacy, labels).key_bytes == b'["Person","Post",1,2]'
    assert _pair_from_envelope(canonical_pair_key(legacy, labels)) == (1, 2)
    unknown = dataclasses.replace(legacy, original_topic="gone")
    assert canonical_pair_key(unknown, labels) is unknown
    labelled = canonical_pair_key(legacy, labels)
    assert canonical_pair_key(labelled, labels) is labelled


# ── AC 4: unknown topic, mismatched labels ────────────────────────────────────


@pytest.mark.parametrize("case", ["unknown-topic", "mismatched-labels"])
def test_an_envelope_that_binds_no_pair_is_parked_after_its_attempts_and_never_written(case: str) -> None:
    edge = _edge(attempts=3)
    store = GraphStore({"Person": {1}, "Post": {1}, "Comment": {1}})
    h = _Harness(edge, store)
    try:
        h.live_dlq(COMMENTS, 1, 1, offset=1)
        [envelope] = h.producer.dlq
        h.consumer._queue.clear()
        if case == "unknown-topic":
            bad = dataclasses.replace(envelope, original_topic="likes-removed")
            h.index.add(1, 1, original_topic="likes-removed", original_partition=0, original_offset=1,
                        first_failure_at_ms=1)
            h.index.remove(1, 1, original_topic=COMMENTS, original_partition=0, original_offset=1)
        else:
            bad = dataclasses.replace(envelope, key_bytes=b'["Person","Post",1,1]')
        h.consumer.push(_FakeDlqMessage(DLQ, 0, h.producer.offset, bad))
        h.producer.offset += 1
        h.run(30)
        assert store.rels == {}
        assert len(h.producer.parked) == 1 and h.producer.parked[0].reason_code == "invalid_on_retry"
        _wait_caught_up(h.index)
        assert not h.index.contains(1, 1, original_topic=bad.original_topic, original_partition=0, original_offset=1)
    finally:
        h.close()


def test_a_key_labelled_with_another_pair_than_its_topic_is_parked_at_once_outside_every_pair() -> None:
    """Final follow-ups nit: a Comment-topic envelope keyed [Person, Post, 1, 1] is never grouped with the
    Post pair (1, 1) by its key_bytes: it is parked by its first batch without a write, and the genuine Post
    row of the same keys is written in that batch."""
    edge = _edge(attempts=50)
    store = GraphStore({"Person": {1}, "Post": {1}, "Comment": {1}})
    h = _Harness(edge, store)
    try:
        h.live_dlq(COMMENTS, 1, 1, offset=1)
        h.live_dlq(POSTS, 1, 1, offset=2)
        comment, post = h.producer.dlq
        h.consumer._queue.clear()
        bad = dataclasses.replace(comment, key_bytes=post.key_bytes)
        for envelope in (bad, post):
            h.consumer.push(_FakeDlqMessage(DLQ, 0, h.producer.offset, envelope))
            h.producer.offset += 1
        h.run(1)
        assert [(p.original_topic, p.attempt_count, p.reason_code) for p in h.producer.parked] == [
            (COMMENTS, 1, "invalid_on_retry")]
        assert set(store.rels) == {("LIKES", "Person", 1, "Post", 1)}
        assert len(h.producer.dlq) == 2  # no re-DLQ'd copy of either
    finally:
        h.close()


def test_the_pair_writer_refuses_a_labelled_key_of_another_pair() -> None:
    edge = _edge()
    writer = build_retry_base_writer(GraphStore({"Person": {1}, "Post": {1}}).driver(), edge.pair_view(0),
                                     tx_timeout_seconds=None)
    from test_retry_consumer import _envelope

    envelope = dataclasses.replace(
        _envelope(entity_name="LIKES", original_topic=POSTS), key_bytes=b'["Person","Comment",1,1]',
        value_bytes=json.dumps(record(POSTS, 1, 1)).encode())
    assert writer.retry_provenanced_batch([envelope]) == {0: ("unresolved", "invalid_on_retry")}
    assert writer.retry_provenanced_batch([dataclasses.replace(envelope, key_bytes=b'["Person","Post",1,1]')]) == {
        0: ("written", "")}


# ── PairSplitRetryWriter semantics ────────────────────────────────────────────


class _Sub:
    def __init__(self, result=None, error=None) -> None:
        self.result, self.error, self.calls = result, error, []

    def retry_batch(self, envelopes, budget_seconds=None):
        self.calls.append(([e.original_offset for e in envelopes], budget_seconds))
        if self.error is not None and self.result is None:
            raise self.error
        if self.error is not None:
            return _PartialOutcomes(self.result(envelopes), self.error)
        return self.result(envelopes)


def _written(envelopes):
    return {i: (RETRY_OUTCOME_WRITTEN, "") for i in range(len(envelopes))}


def test_the_split_writer_remaps_indices_and_shares_the_budget() -> None:
    from test_retry_consumer import _envelope

    envelopes = [_envelope(entity_name="LIKES", original_topic=t, original_offset=i)
                 for i, t in enumerate((POSTS, COMMENTS, BACKFILL, "gone"))]
    posts, comments = _Sub(_written), _Sub(_written)
    writer = PairSplitRetryWriter({0: posts, 1: comments}, lambda e: {POSTS: 0, BACKFILL: 0, COMMENTS: 1}.get(
        e.original_topic))
    out = writer.retry_batch(envelopes, budget_seconds=2.5)
    assert out == {0: ("written", ""), 1: ("written", ""), 2: ("written", ""), 3: ("unresolved", "invalid_on_retry")}
    assert posts.calls == [([0, 2], 2.5)] and comments.calls == [([1], 2.5)]


def test_the_split_writer_reports_a_real_error_before_an_abandon() -> None:
    from test_retry_consumer import _envelope

    envelopes = [_envelope(entity_name="LIKES", original_topic=POSTS, original_offset=0),
                 _envelope(entity_name="LIKES", original_topic=COMMENTS, original_offset=1)]
    real = RuntimeError("neo4j down")
    writer = PairSplitRetryWriter({0: _Sub(error=LeaseDrainAbandoned("budget")), 1: _Sub(error=real)},
                                  lambda e: 0 if e.original_topic == POSTS else 1)
    with pytest.raises(RuntimeError, match="neo4j down"):
        writer.retry_batch(envelopes)
    writer = PairSplitRetryWriter({0: _Sub(_written), 1: _Sub(error=real)}, lambda e: 0 if e.original_topic == POSTS else 1)
    out = writer.retry_batch(envelopes)
    assert isinstance(out, _PartialOutcomes) and out.error is real and dict(out) == {0: ("written", "")}


# ── AC 5: lease mode ─────────────────────────────────────────────────────────


def test_lease_mode_retries_never_mix_pairs_in_a_transaction() -> None:
    edge = _edge()
    store = GraphStore({"Person": set(range(10)), "Post": set(range(10)), "Comment": set(range(10))})
    store.resource_of = BucketCellRouter("Person", "Post", 4).resource
    with _LeaseServer(4) as address:
        def make(config, deadline):
            base = build_retry_base_writer(store.driver(), config, tx_timeout_seconds=None, leased=True)
            coordinator = build_retry_lease_coordinator(
                base, config, router=BucketCellRouter(config.nodes.source, config.nodes.target, 4),
                client_factory=lambda: BucketLeaseClient(address, bucket_count=4), worker_count=3,
                tx_timeout_seconds=None, drain_deadline=deadline)
            return build_retry_writer(base, coordinator=coordinator)

        writer = build_pair_retry_writers(edge, make, concurrent=True)
        subs = writer._writers
        assert subs[0]._coordinator._drain is subs[1]._coordinator._drain  # one stop-drain budget
        h = _Harness(edge, store, writer=writer)
        try:
            for i in range(30):
                h.live_dlq((POSTS, COMMENTS, BACKFILL)[i % 3], i % 10, (i * 7) % 10, offset=i)
            h.run(10)
        finally:
            h.close()
            writer.close()
    expected = {("LIKES", "Person", i % 10, "Comment" if i % 3 == 1 else "Post", (i * 7) % 10) for i in range(30)}
    assert set(store.rels) == expected
    assert all(len(tx["pairs"]) == 1 for tx in store.transactions)
    assert not store.overlaps


# ── AC 6: replay-dlq ─────────────────────────────────────────────────────────


def _replay_args(**kwargs) -> argparse.Namespace:
    defaults = dict(config="c.yaml", node_label=None, edge_type="LIKES", source_topic=None, dlq_topic=None,
                    parked=False, target_topic=None, limit=None, since=None, until=None, dry_run=True,
                    reason_code=None, failure_class=None)
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


@pytest.mark.parametrize("kwargs, ok", [
    ({"target_topic": POSTS, "source_topic": [COMMENTS]}, False),
    ({"target_topic": POSTS, "source_topic": [POSTS, COMMENTS]}, False),
    ({"target_topic": "elsewhere"}, False),
    ({"target_topic": POSTS, "source_topic": [BACKFILL]}, True),
    ({"target_topic": "elsewhere", "source_topic": [COMMENTS]}, True),
    ({"source_topic": [COMMENTS, POSTS]}, True),  # default: each envelope back to its original_topic
])
def test_replay_refuses_target_topics_that_would_change_the_pair(kwargs, ok, caplog) -> None:
    schema = likes_schema(order_by=False, topic_conflict="arrival", dead_letter={"enabled": True})
    with patch.object(cli, "load_schema", return_value=schema), \
            patch("src.loader.dead_letter_replay.run_replay", return_value=MagicMock(matched=0, published=0)) as run:
        rc = cli.handle_replay_dlq(_replay_args(**kwargs))
    if ok:
        assert run.called
    else:
        assert rc == 1 and not run.called
    if kwargs == {"target_topic": POSTS, "source_topic": [COMMENTS]}:
        assert "replaying likes-comments records into likes-posts would write them as Person->Post" in caplog.text


@pytest.mark.parametrize("schema_kwargs, kwargs", [
    ({"order_by": False, "topic_conflict": "arrival"}, {"target_topic": "elsewhere", "source_topic": [COMMENTS]}),
    ({"order_by": True, "topics": (POSTS, BACKFILL)}, {"target_topic": "elsewhere"}),  # a single-pair type too
])
def test_replay_warns_loudly_when_the_target_topic_is_outside_the_type(schema_kwargs, kwargs, caplog) -> None:
    """Final follow-ups nit: a staging topic stays allowed, but a WARNING says no loader of the type reads it."""
    schema = likes_schema(dead_letter={"enabled": True}, **schema_kwargs)
    with caplog.at_level(logging.WARNING), patch.object(cli, "load_schema", return_value=schema), \
            patch("src.loader.dead_letter_replay.run_replay", return_value=MagicMock(matched=0, published=0)) as run:
        assert cli.handle_replay_dlq(_replay_args(**kwargs)) == 0
    assert run.called
    warning = [r for r in caplog.records if "reason=target_outside_entity" in r.getMessage()]
    assert warning and warning[0].levelno == logging.WARNING
    assert "edge LIKES does not read --target-topic elsewhere" in warning[0].getMessage()


def test_one_retry_writer_per_pair_with_one_drain_deadline() -> None:
    edge = _edge()
    built = []

    def make(config, deadline):
        built.append((config.pair_index, config.nodes.target, deadline))
        return MagicMock()

    writer = build_pair_retry_writers(edge, make)
    assert isinstance(writer, PairSplitRetryWriter)
    assert [(index, target) for index, target, _ in built] == [(0, "Post"), (1, "Comment")]
    assert built[0][2] is built[1][2] is not None
    single = likes(topics=(POSTS, BACKFILL))
    assert build_pair_retry_writers(single, lambda config, deadline: ("single", config is single, deadline)) == (
        "single", True, None)


def test_standalone_pair_writers_never_run_at_the_same_time() -> None:
    """Review round 5 S1: without leases two pairs' transactions would lock shared Person nodes."""
    import threading
    from test_retry_consumer import _envelope

    active, overlaps, lock = [0], [0], threading.Lock()

    class _Slow:
        def retry_batch(self, envelopes, budget_seconds=None):
            with lock:
                active[0] += 1
                overlaps[0] = max(overlaps[0], active[0])
            threading.Event().wait(0.05)
            with lock:
                active[0] -= 1
            return _written(envelopes)

    edge = _edge()
    envelopes = [_envelope(entity_name="LIKES", original_topic=t, original_offset=i)
                 for i, t in enumerate((POSTS, COMMENTS))]
    standalone = build_pair_retry_writers(edge, lambda config, deadline: _Slow())
    assert len(standalone.retry_batch(envelopes)) == 2 and overlaps[0] == 1
    leased = build_pair_retry_writers(edge, lambda config, deadline: _Slow(), concurrent=True)
    overlaps[0] = 0
    assert len(leased.retry_batch(envelopes)) == 2 and overlaps[0] == 2
