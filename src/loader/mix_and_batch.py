"""Pure deterministic routing primitives for relationship Mix-and-Batch work."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from collections import deque
from typing import Callable, Deque
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from src.loader.edge_loader import EdgeRecord
    from src.models.schema import EdgeConfig


Direction = Literal["forward", "reverse"]


@dataclass(frozen=True)
class EndpointBuckets:
    """Stable configured bucket resources for one relationship endpoint pair."""

    source: int
    target: int

    def __post_init__(self) -> None:
        for name in ("source", "target"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"endpoint bucket {name} must be a nonnegative integer")


@dataclass(frozen=True)
class RoutedEdgeRecord:
    """An immutable edge event coupled to its lane and Kafka provenance."""

    record: EdgeRecord
    topic: str
    partition: int
    offset: int
    lane_id: int
    direction: Direction
    source_bucket: int
    target_bucket: int

    def __post_init__(self) -> None:
        """Reject malformed routing metadata before a future executor sees it."""
        if not isinstance(self.topic, str) or not self.topic.strip():
            raise ValueError("routed edge topic must be a nonblank string")
        for name in ("partition", "offset", "lane_id", "source_bucket", "target_bucket"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"routed edge {name} must be a nonnegative integer")
        if self.direction not in ("forward", "reverse"):
            raise ValueError("routed edge direction must be 'forward' or 'reverse'")


def canonical_endpoint_token(value: object) -> str:
    """Return a stable typed endpoint token independent of Python hash randomization."""
    return json.dumps(
        [type(value).__qualname__, repr(value)], ensure_ascii=True, separators=(",", ":")
    )


def endpoint_digest(value: object) -> bytes:
    """Return the fixed-size BLAKE2b digest for a canonical endpoint token."""
    return hashlib.blake2b(canonical_endpoint_token(value).encode("utf-8"), digest_size=16).digest()


def endpoint_bucket(value: object, bucket_count: int) -> int:
    """Return a stable typed endpoint bucket in ``[0, bucket_count)``."""
    if (
        isinstance(bucket_count, bool)
        or not isinstance(bucket_count, int)
        or not 1 <= bucket_count <= 4096
    ):
        raise ValueError("bucket_count must be an integer from 1 through 4096")
    return int.from_bytes(endpoint_digest(value), byteorder="big", signed=False) % bucket_count


def endpoint_buckets(record: "EdgeRecord", bucket_count: int) -> EndpointBuckets:
    """Return the configured stable bucket for each endpoint of ``record``."""
    return EndpointBuckets(
        source=endpoint_bucket(record.source_key, bucket_count),
        target=endpoint_bucket(record.target_key, bucket_count),
    )


class MixAndBatchPartitioner:
    """Route normalized edges deterministically without Kafka or Neo4j I/O."""

    def __init__(self, edge_config: "EdgeConfig") -> None:
        self._lane_count = edge_config.mix_and_batch.lane_count

    def _bucket(self, value: object) -> int:
        return endpoint_bucket(value, self._lane_count)

    def route(
        self, record: "EdgeRecord", *, topic: str, partition: int, offset: int
    ) -> RoutedEdgeRecord:
        """Return exactly one stable directional work lane for a normalized edge."""
        source_token = canonical_endpoint_token(record.source_key)
        target_token = canonical_endpoint_token(record.target_key)
        source_bucket = self._bucket(record.source_key)
        target_bucket = self._bucket(record.target_key)
        direction: Direction = "forward" if source_token <= target_token else "reverse"
        lane_id = (0 if direction == "forward" else self._lane_count) + source_bucket
        return RoutedEdgeRecord(
            record=record, topic=topic, partition=partition, offset=offset,
            lane_id=lane_id, direction=direction, source_bucket=source_bucket,
            target_bucket=target_bucket,
        )


class LaneBatcher:
    """Pure FIFO per-lane batching with injected monotonic time."""

    def __init__(self, edge_config: "EdgeConfig", *, clock: Callable[[], float]) -> None:
        self._size = edge_config.mix_and_batch.batch_size
        self._slots = {"forward": edge_config.mix_and_batch.forward_slot_ms / 1000,
                       "reverse": edge_config.mix_and_batch.backward_slot_ms / 1000}
        self._clock = clock
        self._lanes: dict[int, Deque[RoutedEdgeRecord]] = {}
        self._started: dict[int, float] = {}

    def add(self, routed: RoutedEdgeRecord) -> None:
        queue = self._lanes.setdefault(routed.lane_id, deque())
        if queue and queue[0].direction != routed.direction:
            raise ValueError("a nonempty lane cannot mix directions")
        if not queue:
            self._started[routed.lane_id] = self._clock()
        queue.append(routed)

    def _drain(self, lane_id: int, count: int | None = None) -> tuple[RoutedEdgeRecord, ...]:
        queue = self._lanes[lane_id]
        amount = len(queue) if count is None else count
        result = tuple(queue.popleft() for _ in range(amount))
        if not queue:
            del self._lanes[lane_id]
            del self._started[lane_id]
        return result

    def drain_full(self) -> tuple[tuple[RoutedEdgeRecord, ...], ...]:
        batches = []
        for lane_id in sorted(tuple(self._lanes)):
            while lane_id in self._lanes and len(self._lanes[lane_id]) >= self._size:
                batches.append(self._drain(lane_id, self._size))
        return tuple(batches)

    def drain_expired(self) -> tuple[tuple[RoutedEdgeRecord, ...], ...]:
        now = self._clock()
        batches = []
        for lane_id in sorted(tuple(self._lanes)):
            queue = self._lanes[lane_id]
            if now - self._started[lane_id] >= self._slots[queue[0].direction]:
                while lane_id in self._lanes:
                    batches.append(self._drain(lane_id, min(self._size, len(self._lanes[lane_id]))))
        return tuple(batches)

    def drain_all(self) -> tuple[tuple[RoutedEdgeRecord, ...], ...]:
        batches = []
        for lane_id in sorted(tuple(self._lanes)):
            while lane_id in self._lanes:
                batches.append(self._drain(lane_id, min(self._size, len(self._lanes[lane_id]))))
        return tuple(batches)
