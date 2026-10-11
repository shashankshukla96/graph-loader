"""v7-S7 story 7.7: the label pair is part of DLQ and parked-pair identity."""
from __future__ import annotations

import json

import pytest

from multi_pair_support import BACKFILL, COMMENTS, POSTS, GraphStore, likes
from src.loader.dead_letter import DeadLetterRecordSink
from src.loader.edge_execution import LabelledEndpointSink, build_edge_execution
from src.loader.edge_loader import PendingEdgeRecord
from src.loader.edge_records import EdgeRecord, normalize_edge_record
from src.loader.lease_client import BucketLeaseClient
from src.loader.parked_pair_index import ParkedPairIndex, ParkedPairIndexError, _record_wire_key
from test_multi_pair_leases import _LeaseServer
from test_parked_pair_index import _FakeConsumer, _FakeProducer, _FakeTopicLog, _wait_caught_up
from multi_pair_support import record

DLQ = {"enabled": True, "route_missing_endpoints_in_bulk": True, "max_retry_attempts": 5}


class _Producer:
    def __init__(self) -> None:
        self.published: list = []

    def publish(self, topic, envelope, **_kwargs) -> None:
        self.published.append((topic, envelope))


def _sink() -> tuple[DeadLetterRecordSink, _Producer]:
    producer = _Producer()
    return DeadLetterRecordSink(producer=producer, topic="LIKES.dlq", run_id="r", replica_id="0", kind="edge",
                                clock=lambda: 1_700_000_000_000), producer


def _route(sink, topic: str, **extra) -> None:
    sink.route_unresolved_endpoint(
        topic=topic, partition=0, offset=1, record=EdgeRecord(source_key=1, target_key=1, properties={}),
        edge_type="LIKES", source_key_property="person_id",
        target_key_property="comment_id" if topic == COMMENTS else "post_id", failure_class="missing_endpoint",
        reason_code="missing_target", error_text="x", retry_after_seconds=0.0, **extra)


def _index(edge, log: _FakeTopicLog, *, writable: bool = True) -> ParkedPairIndex:
    index = ParkedPairIndex(
        topic="LIKES.dlq.parked-pairs", bootstrap_servers="kafka:9092", writable=writable, sequenced=True,
        pair_of_topic=edge.pair_labels_for_topic,
        consumer_factory=lambda config: _FakeConsumer(config, log, "LIKES.dlq.parked-pairs"),
        producer_factory=lambda config: _FakeProducer(config, log), clock=lambda: 1_700_000_000_000,
        poll_timeout_seconds=0.01,
    )
    index.start()
    return index


def _ident(topic: str, offset: int, partition: int = 0) -> dict:
    return {"original_topic": topic, "original_partition": partition, "original_offset": offset}


def _pending(edge, topic: str, person, other, offset: int) -> PendingEdgeRecord:
    return PendingEdgeRecord(normalize_edge_record(record(topic, person, other), edge.topic_view(topic)),
                             topic, 0, offset)


# ── AC 1, 2: the DLQ key ──────────────────────────────────────────────────────


def test_the_dlq_key_carries_the_label_pair_only_when_given() -> None:
    sink, producer = _sink()
    _route(sink, POSTS)
    _route(sink, COMMENTS, endpoint_labels=("Person", "Comment"))
    LabelledEndpointSink(sink, ("Person", "Post")).route_unresolved_endpoint(
        topic=POSTS, partition=0, offset=2, record=EdgeRecord(source_key=1, target_key=1, properties={}),
        edge_type="LIKES", source_key_property="person_id", target_key_property="post_id",
        failure_class="missing_endpoint", reason_code="missing_target", error_text="x", retry_after_seconds=0.0)
    keys = [envelope.key_bytes for _topic, envelope in producer.published]
    assert keys == [b"[1,1]", b'["Person","Comment",1,1]', b'["Person","Post",1,1]']
    values = [json.loads(envelope.value_bytes) for _topic, envelope in producer.published]
    assert values[1]["target"] == {"comment_id": 1} and values[2]["target"] == {"post_id": 1}
    assert {topic for topic, _ in producer.published} == {"LIKES.dlq"}  # still one DLQ per type


def test_classic_bulk_dlq_keys_are_labelled_per_pair() -> None:
    edge = likes(dead_letter=DLQ)
    sink, producer = _sink()
    store = GraphStore({"Person": {1}})
    _w, _p, _b, coordinator = build_edge_execution(edge, store.driver(), mode="bulk", missing_endpoint_sink=sink)
    results = coordinator.execute_pending((_pending(edge, POSTS, 1, 1, 1), _pending(edge, COMMENTS, 1, 1, 1)))
    assert all(result.success for result in results)
    assert sorted(envelope.key_bytes for _t, envelope in producer.published) == [
        b'["Person","Comment",1,1]', b'["Person","Post",1,1]']


# ── AC 3, 4, 5: the parked-pair index ─────────────────────────────────────────


def test_a_post_pair_blocks_its_own_topics_but_never_the_comment_pair() -> None:
    edge = likes(order_by=False, topic_conflict="arrival", dead_letter=DLQ)
    log = _FakeTopicLog()
    index = _index(edge, log)
    try:
        index.add(1, 1, **_ident(POSTS, 5), first_failure_at_ms=1)
        index.add(1, 1, **_ident(BACKFILL, 2), first_failure_at_ms=2)
        index.add(1, 1, **_ident(COMMENTS, 9), first_failure_at_ms=3)
        _wait_caught_up(index)
        assert index.is_blocked_by_earlier(1, 1, **_ident(BACKFILL, 2), first_failure_at_ms=2) is True
        assert index.order_key(1, 1, **_ident(BACKFILL, 2))[0] == 2  # sequenced after the likes-posts row
        assert index.order_key(1, 1, **_ident(COMMENTS, 9))[0] == 1  # its own pair starts at 1
        assert index.is_blocked_by_earlier(1, 1, **_ident(COMMENTS, 9), first_failure_at_ms=3) is False
        assert index.earlier_outstanding(1, 1, **_ident(COMMENTS, 9)) == []
        index.remove(1, 1, **_ident(COMMENTS, 9))
        assert index.is_blocked(1, 1, endpoint_pair=("Person", "Comment")) is False  # live path
        assert index.is_blocked(1, 1, endpoint_pair=("Person", "Post")) is True
        assert index.scoped(("Person", "Comment")).is_blocked(1, 1) is False
        with pytest.raises(ParkedPairIndexError, match="needs endpoint_pair"):
            index.is_blocked(1, 1)
        rebuilt = _index(edge, log, writable=False)
        try:
            _wait_caught_up(rebuilt)
            for ident in (_ident(POSTS, 5), _ident(BACKFILL, 2)):
                assert rebuilt.order_key(1, 1, **ident) == index.order_key(1, 1, **ident)
            assert rebuilt.is_blocked(1, 1, endpoint_pair=("Person", "Comment")) is False
        finally:
            rebuilt.stop()
        # The wire bytes are the pre-S7 ones: no label in the key.
        assert json.loads(log.records[0][0]) == [1, 1, POSTS, 0, 5]
    finally:
        index.stop()


def test_entries_written_before_s7_rebuild_into_their_topics_pair() -> None:
    edge = likes(order_by=False, topic_conflict="arrival", dead_letter=DLQ)
    log = _FakeTopicLog()
    for topic, offset in ((POSTS, 1), (COMMENTS, 1)):
        log.append(_record_wire_key(7, 7, topic, 0, offset).encode(), b'{"first_failure_at_ms":5}')
    log.append(_record_wire_key(7, 7, "removed-topic", 0, 1).encode(), b'{"first_failure_at_ms":5}')
    index = _index(edge, log, writable=False)
    try:
        _wait_caught_up(index)
        assert index.contains(7, 7, **_ident(POSTS, 1)) and index.contains(7, 7, **_ident(COMMENTS, 1))
        assert index.is_blocked(7, 7, endpoint_pair=("Person", "Post"))
        assert index.is_blocked(7, 7, endpoint_pair=("Person", "Comment"))
        # A topic the type no longer reads is its own pseudo pair: it blocks no real pair.
        assert index.is_blocked_by_earlier(7, 7, **_ident(POSTS, 1), first_failure_at_ms=5) is False
        assert index.contains(7, 7, **_ident("removed-topic", 1))
    finally:
        index.stop()


def test_a_scoped_index_refuses_a_record_of_another_pair() -> None:
    edge = likes(order_by=False, topic_conflict="arrival", dead_letter=DLQ)
    index = _index(edge, _FakeTopicLog())
    try:
        with pytest.raises(ParkedPairIndexError, match="binds"):
            index.scoped(("Person", "Post")).add(1, 1, **_ident(COMMENTS, 1), first_failure_at_ms=1)
    finally:
        index.stop()
    single = ParkedPairIndex(topic="t", bootstrap_servers="k:1")
    with pytest.raises(ParkedPairIndexError, match="pair_of_topic"):
        single.scoped(("Person", "Post"))


# ── AC 6 and review round 2 S2: colliding keys of two pairs, live path ────────


def _lease_flushes(edge, store, index, sink, flushes) -> list:
    out = []
    with _LeaseServer(4) as address:
        _w, _p, _b, coordinator = build_edge_execution(
            edge, store.driver(), mode="stream", missing_endpoint_sink=sink, parked_pair_index=index,
            environ={"GRAPH_LOADER_LEASE_ADDR": address, "GRAPH_LOADER_LEASE_BUCKETS": "4"},
            lease_client_factory=lambda: BucketLeaseClient(address, bucket_count=4),
        )
        try:
            for pending in flushes:
                out.append(coordinator.execute_pending(tuple(pending)))
        finally:
            coordinator.close()
    return out


def test_a_parked_comment_never_blocks_a_live_post_with_the_same_keys_lease() -> None:
    edge = likes(order_by=False, topic_conflict="arrival", dead_letter=DLQ)
    store = GraphStore({"Person": {7}, "Post": {7}})  # Comment 7 is missing
    log = _FakeTopicLog()
    index = _index(edge, log)
    sink, producer = _sink()
    try:
        first, second = _lease_flushes(edge, store, index, sink, [
            [_pending(edge, COMMENTS, 7, 7, 1), _pending(edge, POSTS, 7, 7, 1)],  # same flush
            [_pending(edge, POSTS, 7, 7, 2)],  # next flush, the Comment row is still parked
        ])
        _wait_caught_up(index)
    finally:
        index.stop()
    assert all(r.success for r in first + second)
    assert ("LIKES", "Person", 7, "Post", 7) in store.rels and len(store.rels) == 1
    assert [envelope.key_bytes for _t, envelope in producer.published] == [b'["Person","Comment",7,7]']
    assert index.is_blocked(7, 7, endpoint_pair=("Person", "Comment"))
    assert not index.is_blocked(7, 7, endpoint_pair=("Person", "Post"))


def test_a_parked_post_still_blocks_its_later_rows_from_the_other_topic_lease() -> None:
    edge = likes(order_by=False, topic_conflict="arrival", dead_letter=DLQ)
    store = GraphStore({"Person": {7}, "Comment": {7}})  # Post 7 is missing
    index = _index(edge, _FakeTopicLog())
    sink, producer = _sink()
    try:
        _lease_flushes(edge, store, index, sink, [[_pending(edge, POSTS, 7, 7, 1)]])
        store.nodes["Post"] = {7}
        [results] = _lease_flushes(edge, store, index, sink, [[_pending(edge, BACKFILL, 7, 7, 1),
                                                               _pending(edge, COMMENTS, 7, 7, 1)]])
    finally:
        index.stop()
    assert all(r.success for r in results)
    # The backfill row waits behind its pair's outstanding likes-posts row; the Comment row is written.
    assert [envelope.key_bytes for _t, envelope in producer.published] == [b'["Person","Post",7,7]'] * 2
    assert set(store.rels) == {("LIKES", "Person", 7, "Comment", 7)}


def test_a_parked_comment_never_blocks_a_live_post_in_classic_bulk() -> None:
    edge = likes(order_by=False, topic_conflict="arrival", dead_letter=DLQ)
    store = GraphStore({"Person": {7}, "Post": {7}})
    sink, producer = _sink()
    _w, _p, _b, coordinator = build_edge_execution(edge, store.driver(), mode="bulk", missing_endpoint_sink=sink)
    results = coordinator.execute_pending((_pending(edge, COMMENTS, 7, 7, 1), _pending(edge, POSTS, 7, 7, 1)))
    assert all(r.success for r in results)
    assert set(store.rels) == {("LIKES", "Person", 7, "Post", 7)}
    assert [envelope.key_bytes for _t, envelope in producer.published] == [b'["Person","Comment",7,7]']


def test_a_single_pair_index_keeps_plain_string_pair_keys() -> None:
    """Review round 3 N1: without pair_of_topic the in-memory keys are today's JSON strings."""
    from test_parked_pair_index import _make_index, _seed

    log = _FakeTopicLog()
    _seed(log, "a", "b", original_topic="knows", original_partition=0, original_offset=5)
    index = _make_index(log, writable=True)
    index.start()
    try:
        index.add("c", "d", original_topic="knows", original_partition=0, original_offset=6, first_failure_at_ms=1)
        _wait_caught_up(index)
        assert index._by_pair and all(isinstance(key, str) for key in index._by_pair)
        assert index.pair_of("knows") is None
    finally:
        index.stop()
