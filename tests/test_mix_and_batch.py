"""Unit tests for pure relationship routing work records."""
from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import date

import pytest

from src.loader.edge_loader import EdgeRecord
from src.loader.mix_and_batch import (
    LaneBatcher, MixAndBatchPartitioner, RoutedEdgeRecord, canonical_endpoint_token, endpoint_bucket,
    endpoint_buckets, endpoint_digest,
)
from src.utils.schema_loader import load_schema
from pathlib import Path


def _routed(**overrides: object) -> RoutedEdgeRecord:
    values: dict[str, object] = {
        "record": EdgeRecord("p-1", "c-1", {"since": date(2020, 1, 2)}),
        "topic": "works-at-events", "partition": 2, "offset": 7,
        "lane_id": 1, "direction": "forward", "source_bucket": 1,
        "target_bucket": 0,
    }
    values.update(overrides)
    return RoutedEdgeRecord(**values)  # type: ignore[arg-type]


def test_routed_record_preserves_edge_and_kafka_provenance() -> None:
    record = _routed()
    assert record.record.source_key == "p-1"
    assert (record.topic, record.partition, record.offset) == ("works-at-events", 2, 7)
    assert record.direction == "forward"
    with pytest.raises(FrozenInstanceError):
        record.offset = 8  # type: ignore[misc]


@pytest.mark.parametrize(
    "overrides",
    [
        {"topic": " "}, {"topic": 1}, {"partition": -1}, {"offset": True},
        {"lane_id": -1}, {"source_bucket": False}, {"target_bucket": -1},
        {"direction": "sideways"},
    ],
)
def test_routed_record_rejects_invalid_runtime_metadata(overrides: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        _routed(**overrides)


def test_typed_endpoint_tokens_distinguish_integer_and_string() -> None:
    assert canonical_endpoint_token(1) != canonical_endpoint_token("1")
    assert endpoint_digest(1) != endpoint_digest("1")
    assert endpoint_bucket(1, 17) != endpoint_bucket("1", 17)


def test_endpoint_buckets_are_typed_deterministic_and_validate_count() -> None:
    record = EdgeRecord("p-1", "c-1", {})
    assert endpoint_buckets(record, 7) == endpoint_buckets(record, 7)
    assert 0 <= endpoint_buckets(record, 7).source < 7
    with pytest.raises(ValueError, match="bucket_count"):
        endpoint_bucket("p-1", True)
    with pytest.raises(ValueError, match="bucket_count"):
        endpoint_bucket("p-1", 0)
    with pytest.raises(ValueError, match="bucket_count"):
        endpoint_bucket("p-1", 4097)


def test_partitioner_is_deterministic_and_directional() -> None:
    schema = load_schema(Path(__file__).resolve().parents[1] / "config/graph_schema.yaml")
    edge = next(item for item in schema.edges if item.type == "WORKS_AT")
    edge.mix_and_batch.lane_count = 7
    partitioner = MixAndBatchPartitioner(edge)
    record = EdgeRecord("p-1", "c-1", {"since": date(2020, 1, 2)})
    first = partitioner.route(record, topic="works-at-events", partition=2, offset=7)
    second = MixAndBatchPartitioner(edge).route(record, topic="works-at-events", partition=2, offset=7)
    reverse = partitioner.route(EdgeRecord("z", "a", {}), topic="works-at-events", partition=2, offset=8)
    assert first == second
    assert 0 <= first.source_bucket < 7 and 0 <= first.target_bucket < 7
    assert 0 <= first.lane_id < 14
    assert reverse.direction == "reverse" and 7 <= reverse.lane_id < 14
    assert (first.topic, first.partition, first.offset) == ("works-at-events", 2, 7)


def test_partitioner_separates_swapped_pairs_and_keeps_record_identity() -> None:
    schema = load_schema(Path(__file__).resolve().parents[1] / "config/graph_schema.yaml")
    edge = next(item for item in schema.edges if item.type == "WORKS_AT")
    edge.mix_and_batch.lane_count = 5
    partitioner = MixAndBatchPartitioner(edge)
    forward_record = EdgeRecord("a", "z", {})
    forward = partitioner.route(forward_record, topic="works-at-events", partition=0, offset=1)
    reverse = partitioner.route(EdgeRecord("z", "a", {}), topic="works-at-events", partition=0, offset=2)
    self_edge = partitioner.route(EdgeRecord("same", "same", {}), topic="works-at-events", partition=0, offset=3)
    assert forward.direction == "forward" and reverse.direction == "reverse"
    assert forward.lane_id < 5 <= reverse.lane_id
    assert forward.lane_id == forward.source_bucket
    assert reverse.lane_id == 5 + reverse.source_bucket
    assert forward.record is forward_record
    assert self_edge.direction == "forward" and self_edge.lane_id < 5


def _batcher_config():
    schema = load_schema(Path(__file__).resolve().parents[1] / "config/graph_schema.yaml")
    edge = next(item for item in schema.edges if item.type == "WORKS_AT")
    edge.mix_and_batch.batch_size = 2
    edge.mix_and_batch.forward_slot_ms = 100
    edge.mix_and_batch.backward_slot_ms = 200
    return edge


def test_lane_batcher_full_expired_and_final_drains_are_fifo_exactly_once() -> None:
    now = [0.0]
    batcher = LaneBatcher(_batcher_config(), clock=lambda: now[0])
    records = [_routed(offset=index, lane_id=1, direction="forward") for index in range(4)]
    other = _routed(offset=9, lane_id=3, partition=2, direction="forward")
    for record in records + [other]:
        batcher.add(record)
    full = batcher.drain_full()
    assert [[item.offset for item in batch] for batch in full] == [[0, 1], [2, 3]]
    assert batcher.drain_expired() == ()
    now[0] = 0.1
    expired = batcher.drain_expired()
    assert [[item.lane_id for item in batch] for batch in expired] == [[3]]
    assert [item.offset for batch in full + expired + batcher.drain_all() for item in batch] == [0, 1, 2, 3, 9]


def test_lane_batcher_rejects_direction_mix_in_same_nonempty_lane() -> None:
    batcher = LaneBatcher(_batcher_config(), clock=lambda: 0.0)
    batcher.add(_routed(lane_id=1, direction="forward"))
    with pytest.raises(ValueError, match="cannot mix directions"):
        batcher.add(_routed(lane_id=1, direction="reverse", offset=8))


def test_expired_and_final_drains_chunk_and_sort_lanes() -> None:
    now = [0.0]
    batcher = LaneBatcher(_batcher_config(), clock=lambda: now[0])
    for offset in range(5):
        batcher.add(_routed(lane_id=4, offset=offset))
    for offset in range(5, 8):
        batcher.add(_routed(lane_id=1, offset=offset))
    now[0] = 0.1
    expired = batcher.drain_expired()
    assert [len(batch) for batch in expired] == [2, 1, 2, 2, 1]
    assert [batch[0].lane_id for batch in expired] == [1, 1, 4, 4, 4]
    assert sorted(item.offset for batch in expired for item in batch) == list(range(8))


def test_final_drain_chunks_fifo_records_by_ascending_lane() -> None:
    batcher = LaneBatcher(_batcher_config(), clock=lambda: 0.0)
    for offset in range(4):
        batcher.add(_routed(lane_id=3, offset=offset))
    for offset in range(4, 7):
        batcher.add(_routed(lane_id=1, offset=offset))
    final = batcher.drain_all()
    assert [len(batch) for batch in final] == [2, 1, 2, 2]
    assert [batch[0].lane_id for batch in final] == [1, 1, 3, 3]
    assert [item.offset for batch in final for item in batch] == [4, 5, 6, 0, 1, 2, 3]
