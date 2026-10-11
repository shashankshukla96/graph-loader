"""v7-S7 story 7.3: each record is normalised and written with the label pair its topic binds."""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
from confluent_kafka import TopicPartition

from multi_pair_support import BACKFILL, COMMENTS, POSTS, GraphStore, likes, record
from src.loader import edge_execution, edge_loader, planned_writer
from src.loader.edge_execution import (
    ApocLockedEdgeWriter,
    ExecutionModeError,
    LaneExecutionCoordinator,
    PairSplitLaneExecution,
    build_edge_execution,
)
from src.loader.edge_loader import EdgeLoader, PendingEdgeRecord
from src.loader.edge_records import normalize_edge_record
from src.loader.mix_and_batch import LaneBatcher, MixAndBatchPartitioner
from src.models.schema import LoadingConfig, MultiPairEdgeError
from src.utils.schema_loader import load_schema


class _Sink:
    def __init__(self) -> None:
        self.routed: list = []
        self.unresolved: list[dict] = []

    def route(self, **kwargs) -> None:
        self.routed.append(kwargs)

    def route_unresolved_endpoint(self, **kwargs) -> None:
        self.unresolved.append(kwargs)


def _message(payload: dict, *, topic: str, partition: int = 0, offset: int = 0):
    message = MagicMock()
    message.value.return_value = json.dumps(payload).encode()
    message.key.return_value = None
    message.error.return_value = None
    message.topic.return_value = topic
    message.partition.return_value = partition
    message.offset.return_value = offset
    return message


def _pending(edge, topic: str, person: object, other: object, offset: int, partition: int = 0, **kwargs):
    raw = record(topic, person, other, **kwargs)
    return PendingEdgeRecord(normalize_edge_record(raw, edge.topic_view(topic)), topic, partition, offset)


# ── AC 1: normalisation follows the topic ─────────────────────────────────────


def test_records_are_normalised_with_the_pair_their_topic_binds() -> None:
    edge = likes(comment_key_type="string")
    sink = _Sink()
    loader = EdgeLoader(MagicMock(), MagicMock(), edge, rejection_sink=sink,
                        loading_config=LoadingConfig(unwind_batch_size=100))
    assert loader._buffer_message(_message(record(COMMENTS, 1, "7"), topic=COMMENTS, offset=1))
    assert loader._buffer_message(_message(record(POSTS, 1, 7), topic=POSTS, offset=1))
    comment, post = loader._batch
    assert comment.record.target_key == "7" and comment.topic == COMMENTS
    assert post.record.target_key == 7 and post.topic == POSTS
    # A likes-comments record carrying post_id is malformed: the pair is never inferred from the record.
    bad = {"source": {"person_id": 1}, "target": {"post_id": 7}, "properties": {"liked_at": "2024-01-01T00:00:00Z"}}
    loader._buffer_message(_message(bad, topic=COMMENTS, offset=2))
    [rejected] = sink.routed
    assert rejected["topic"] == COMMENTS and rejected["offset"] == 2
    assert "comment_id" in rejected["reason"] or "post_id" in rejected["reason"]
    assert len(loader._batch) == 2


def test_an_integer_comment_id_is_not_a_post_id() -> None:
    edge = likes(comment_key_type="string")
    with pytest.raises(ValueError):
        normalize_edge_record(record(COMMENTS, 1, 7), edge.topic_view(COMMENTS))  # Comment's key is a string


# ── AC 2, 3: one label pair per transaction ───────────────────────────────────


def test_a_mixed_batch_with_colliding_keys_writes_each_pair_in_its_own_transactions() -> None:
    edge = likes()
    store = GraphStore({"Person": {1, 2}, "Post": {7}, "Comment": {7}})
    writer, partitioner, batcher, coordinator = build_edge_execution(edge, store.driver(), mode="bulk")
    assert partitioner is None and batcher is None and isinstance(coordinator, PairSplitLaneExecution)
    pending = [
        _pending(edge, POSTS, 1, 7, offset=5),
        _pending(edge, COMMENTS, 1, 7, offset=5),
        _pending(edge, BACKFILL, 2, 7, offset=5),
        _pending(edge, COMMENTS, 2, 7, offset=6),
    ]
    results = coordinator.execute_pending(tuple(pending))
    assert all(result.success for result in results)
    assert sorted(store.rels) == sorted([
        ("LIKES", "Person", 1, "Post", 7), ("LIKES", "Person", 1, "Comment", 7),
        ("LIKES", "Person", 2, "Post", 7), ("LIKES", "Person", 2, "Comment", 7),
    ])
    assert store.transactions and all(len(tx["pairs"]) == 1 for tx in store.transactions)
    assert {next(iter(tx["pairs"])) for tx in store.committed_writes()} == {("Person", "Post"), ("Person", "Comment")}
    assert sum(len(result.routed_records) for result in results) == 4
    assert "pair=Person>Post records=2 | pair=Person>Comment records=2" == coordinator.describe_last_flush()
    with pytest.raises(ExecutionModeError, match="binds several label pairs"):
        writer.write_batch([pending[0].record])


def test_classic_bulk_dlq_rows_carry_their_pairs_key_property_names() -> None:
    edge = likes(dead_letter={"enabled": True, "route_missing_endpoints_in_bulk": True})
    store = GraphStore({"Person": {1}, "Post": {7}, "Comment": {7}})
    sink = _Sink()
    _writer, _p, _b, coordinator = build_edge_execution(edge, store.driver(), mode="bulk",
                                                        missing_endpoint_sink=sink)
    pending = [_pending(edge, POSTS, 1, 9, offset=1), _pending(edge, COMMENTS, 1, 8, offset=1),
               _pending(edge, COMMENTS, 1, 7, offset=2)]
    assert all(result.success for result in coordinator.execute_pending(tuple(pending)))
    assert sorted(store.rels) == [("LIKES", "Person", 1, "Comment", 7)]
    by_topic = {item["topic"]: item for item in sink.unresolved}
    assert (by_topic[POSTS]["source_key_property"], by_topic[POSTS]["target_key_property"]) == ("person_id", "post_id")
    assert by_topic[COMMENTS]["target_key_property"] == "comment_id"
    assert all(len(tx["pairs"]) == 1 for tx in store.transactions)


# ── AC 4, 5: single-pair identity and guards ──────────────────────────────────


def test_single_pair_types_get_exactly_todays_objects() -> None:
    schema = load_schema("examples/social/graph_schema.yaml")
    for edge in schema.edges:
        writer, partitioner, batcher, coordinator = build_edge_execution(edge, MagicMock(), mode="bulk")
        assert type(writer) is ApocLockedEdgeWriter
        assert type(partitioner) is MixAndBatchPartitioner and type(batcher) is LaneBatcher
        assert type(coordinator) is LaneExecutionCoordinator
        assert writer._query == edge_execution.build_apoc_locked_upsert_query(edge)


@pytest.mark.parametrize("build", [
    lambda e: edge_execution.build_apoc_locked_upsert_query(e),
    lambda e: edge_execution.build_apoc_locked_upsert_query(e, explicit_locks=False, sort_rows=False),
    lambda e: edge_execution._preflight_query(e),
    lambda e: edge_execution._endpoint_classification_query(e),
    lambda e: ApocLockedEdgeWriter(MagicMock(), e, missing_endpoint_error_factory=RuntimeError),
    lambda e: edge_execution.NativeDisjointEdgeWriter(
        MagicMock(), e, edge_execution.ServerCapabilities(cypher_25=True), missing_endpoint_error_factory=RuntimeError),
    lambda e: edge_loader.build_edge_upsert_query(e),
    lambda e: edge_loader._build_endpoint_preflight_query(e),
    lambda e: planned_writer.compile_record_normalizer(e),
    lambda e: normalize_edge_record(record(POSTS, 1, 7), e),
])
def test_every_single_pair_builder_refuses_a_multi_pair_parent(build) -> None:
    with pytest.raises(MultiPairEdgeError, match="Edge 'LIKES' binds 2 label pairs"):
        build(likes())


def test_slot_gating_refuses_a_multi_pair_type_before_subscribing(tmp_path, monkeypatch, caplog) -> None:
    import yaml

    from multi_pair_support import likes_schema_dict

    config = tmp_path / "schema.yaml"
    config.write_text(yaml.safe_dump(likes_schema_dict()))
    consumer = MagicMock()
    monkeypatch.setattr(edge_loader, "Consumer", lambda *_a, **_k: consumer)
    monkeypatch.setattr(edge_loader.metrics_module, "start_metrics_server", lambda *a, **k: None)
    code = edge_loader.main(["--config", str(config), "--edge-type", "LIKES", "--mode", "stream", "--slot-gating",
                             "--coordination-topic", "graph-loader-coordination", "--run-id", "r1",
                             "--fleet-edge-types", "LIKES"])
    assert code == 1
    consumer.subscribe.assert_not_called()
    assert "binds several label pairs; --slot-gating" in caplog.text


# ── AC 6: one pair fails, the whole flush fails ───────────────────────────────


def test_a_failing_pair_fails_the_flush_and_no_offset_of_either_pair_commits() -> None:
    edge = likes()
    store = GraphStore({"Person": {1}, "Post": {7}})  # Comment 7 is missing: that pair fails (no DLQ)
    writer, partitioner, batcher, coordinator = build_edge_execution(edge, store.driver(), mode="bulk")
    consumer = MagicMock()
    commits: list = []
    consumer.commit.side_effect = lambda **kwargs: commits.append(kwargs["offsets"]) or kwargs["offsets"]
    loader = EdgeLoader(consumer, writer, edge, partitioner=partitioner, lane_batcher=batcher,
                        coordinator=coordinator, loading_config=LoadingConfig(unwind_batch_size=100))
    loader._running = True
    loader._on_assign(consumer, [TopicPartition(POSTS, 0), TopicPartition(COMMENTS, 0)])
    assert loader._buffer_message(_message(record(POSTS, 1, 7), topic=POSTS, offset=3))
    assert loader._buffer_message(_message(record(COMMENTS, 1, 7), topic=COMMENTS, offset=3))
    assert loader._flush_batch() is False
    assert commits == []
    store.nodes["Comment"] = {7}
    assert loader._flush_batch() is True  # the replay rewrites both pairs (idempotent MERGE)
    assert sorted(store.rels) == [("LIKES", "Person", 1, "Comment", 7), ("LIKES", "Person", 1, "Post", 7)]
    [offsets] = commits
    assert sorted((o.topic, o.partition, o.offset) for o in offsets) == [(COMMENTS, 0, 4), (POSTS, 0, 4)]


def test_a_failed_pair_skips_the_later_pairs_of_the_flush() -> None:
    edge = likes()
    store = GraphStore({"Person": {1}, "Comment": {7}})  # Post 7 missing: pair 0 fails first
    _writer, _p, _b, coordinator = build_edge_execution(edge, store.driver(), mode="bulk")
    results = coordinator.execute_pending((_pending(edge, POSTS, 1, 7, offset=1),
                                           _pending(edge, COMMENTS, 1, 7, offset=1)))
    assert [result.success for result in results][-1] is False
    assert "not written: pair Person>Post failed" in str(results[-1].exception)
    assert store.rels == {}
