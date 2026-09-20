"""Validated, run-scoped wire protocol for Phase 4 clock leases."""
from __future__ import annotations

from dataclasses import dataclass
import json
import re
from types import MappingProxyType
from typing import Mapping
from threading import Event

from src.models.schema import CoordinationConfig, EdgeConfig

COORDINATION_TOPIC = "graph.loader.coordination"


class ClockProtocolError(ValueError):
    """Raised when a clock lease is malformed, unsafe, or expired."""


@dataclass(frozen=True)
class ClockLease:
    """One immutable run-scoped slot lease and complete bucket ownership map."""
    run_id: str
    epoch: int
    slot_id: int
    issued_at_ms: int
    expires_at_ms: int
    active_edge_types: tuple[str, ...]
    bucket_owners: Mapping[int, str]

    def __post_init__(self) -> None:
        if not isinstance(self.active_edge_types, (tuple, list)):
            raise ClockProtocolError("active_edge_types must be a sequence")
        if not isinstance(self.bucket_owners, Mapping):
            raise ClockProtocolError("bucket_owners must be a mapping")
        object.__setattr__(self, "active_edge_types", tuple(self.active_edge_types))
        object.__setattr__(self, "bucket_owners", MappingProxyType(dict(self.bucket_owners)))


def _integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ClockProtocolError(f"{field} must be a nonnegative integer")
    return value


def _validated(
    run_id: object, epoch: object, slot_id: object, issued_at_ms: object,
    expires_at_ms: object, active_edge_types: object, bucket_owners: object,
) -> ClockLease:
    if not isinstance(run_id, str) or not run_id.strip():
        raise ClockProtocolError("run_id must be nonblank")
    epoch, slot_id = _integer(epoch, "epoch"), _integer(slot_id, "slot_id")
    issued, expires = _integer(issued_at_ms, "issued_at_ms"), _integer(expires_at_ms, "expires_at_ms")
    if expires <= issued:
        raise ClockProtocolError("expires_at_ms must be after issued_at_ms")
    if not isinstance(active_edge_types, (list, tuple)) or not active_edge_types:
        raise ClockProtocolError("active_edge_types must be nonempty")
    types = tuple(active_edge_types)
    if any(not isinstance(item, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", item) for item in types) or tuple(sorted(types)) != types or len(set(types)) != len(types):
        raise ClockProtocolError("active_edge_types must be sorted unique identifiers")
    if not isinstance(bucket_owners, Mapping) or not bucket_owners:
        raise ClockProtocolError("bucket_owners must be nonempty")
    owners: dict[int, str] = {}
    for raw_bucket, owner in bucket_owners.items():
        bucket = int(raw_bucket) if isinstance(raw_bucket, str) and raw_bucket.isdigit() and str(int(raw_bucket)) == raw_bucket else raw_bucket
        if isinstance(bucket, bool) or not isinstance(bucket, int) or bucket < 0 or bucket in owners:
            raise ClockProtocolError("bucket_owners has invalid bucket")
        if not isinstance(owner, str) or owner not in types:
            raise ClockProtocolError("bucket owner must be active")
        owners[bucket] = owner
    if set(owners) != set(range(len(owners))):
        raise ClockProtocolError("bucket_owners must be contiguous from zero")
    return ClockLease(run_id, epoch, slot_id, issued, expires, types, MappingProxyType(dict(owners)))


def encode_clock_lease(lease: ClockLease) -> bytes:
    """Return a canonical UTF-8 JSON lease after full validation."""
    valid = _validated(lease.run_id, lease.epoch, lease.slot_id, lease.issued_at_ms, lease.expires_at_ms, lease.active_edge_types, lease.bucket_owners)
    return json.dumps({"run_id": valid.run_id, "epoch": valid.epoch, "slot_id": valid.slot_id, "issued_at_ms": valid.issued_at_ms, "expires_at_ms": valid.expires_at_ms, "active_edge_types": list(valid.active_edge_types), "bucket_owners": {str(key): valid.bucket_owners[key] for key in sorted(valid.bucket_owners)}}, sort_keys=True, separators=(",", ":")).encode()


def decode_clock_lease(payload: bytes, *, expected_run_id: str, now_ms: int) -> ClockLease | None:
    """Validate a lease, returning None only for a fully valid foreign run."""
    if not isinstance(expected_run_id, str) or not expected_run_id.strip():
        raise ClockProtocolError("expected_run_id must be nonblank")
    _integer(now_ms, "now_ms")
    try:
        def no_duplicates(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ClockProtocolError("clock payload has duplicate key")
                result[key] = value
            return result
        data = json.loads(payload.decode("utf-8"), object_pairs_hook=no_duplicates)
    except (AttributeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ClockProtocolError("clock payload is not valid JSON") from exc
    if not isinstance(data, dict):
        raise ClockProtocolError("clock payload must be an object")
    required_fields = {
        "run_id", "epoch", "slot_id", "issued_at_ms", "expires_at_ms",
        "active_edge_types", "bucket_owners",
    }
    if set(data) != required_fields:
        raise ClockProtocolError("clock payload has missing or unexpected field")
    try:
        lease = _validated(**{key: data[key] for key in required_fields})
    except KeyError as exc:
        raise ClockProtocolError("clock payload has missing field") from exc
    if lease.expires_at_ms <= now_ms:
        raise ClockProtocolError("clock lease has expired")
    return lease if lease.run_id == expected_run_id else None


class ClockPublishError(RuntimeError):
    """Raised when a clock lease cannot be durably published."""


class ClockHealthError(RuntimeError):
    """Raised when exact clock-fleet health cannot be proven."""


class GlobalBatchClock:
    """Publish acknowledged, expiring run-scoped coordination leases."""

    def __init__(self, *, run_id: str, edges: list[EdgeConfig], config: CoordinationConfig,
                 producer, health_probe, wall_clock_ms, sleeper) -> None:
        if not isinstance(run_id, str) or not run_id.strip() or not edges:
            raise ClockPublishError("clock requires a run id and eligible edges")
        if any(edge.nodes.is_self_referencing for edge in edges):
            raise ClockPublishError("self-referencing edges cannot join shared clock")
        types = tuple(sorted(edge.type for edge in edges))
        if len(types) != len(set(types)):
            raise ClockPublishError("clock edges must have unique types")
        self._run_id, self._types, self._config = run_id, types, config
        self._producer, self._health, self._wall, self._sleep = producer, health_probe, wall_clock_ms, sleeper
        self._lease: ClockLease | None = None

    def _publish(self, epoch: int, slot: int) -> ClockLease:
        try:
            self._health()
        except Exception as exc:
            raise ClockHealthError("clock health probe failed") from exc
        try:
            now = _integer(self._wall(), "wall_clock_ms")
        except ClockProtocolError as exc:
            raise ClockPublishError("clock wall time is invalid") from exc
        if self._lease is not None and now < self._lease.issued_at_ms:
            raise ClockPublishError("clock wall time regressed")
        if self._lease is not None and now >= self._lease.expires_at_ms:
            raise ClockPublishError("clock lease expired before renewal")
        lease = ClockLease(self._run_id, epoch, slot, now, now + self._config.lease_timeout_ms,
                           self._types, {bucket: self._types[0] for bucket in range(self._config.bucket_count)})
        delivered = []
        try:
            self._producer.produce(self._config.topic, key=self._run_id, value=encode_clock_lease(lease), on_delivery=lambda error, _message: delivered.append(error))
            outstanding = self._producer.flush()
        except Exception as exc:
            raise ClockPublishError("clock publish failed") from exc
        if isinstance(outstanding, bool) or not isinstance(outstanding, int) or outstanding != 0 or len(delivered) != 1 or delivered[0] is not None:
            raise ClockPublishError("clock publish was not acknowledged")
        self._lease = lease
        return lease

    def publish_initial(self) -> ClockLease:
        return self._lease or self._publish(0, 0)

    def advance(self) -> ClockLease:
        if self._lease is None:
            raise ClockPublishError("clock cannot advance before initial publish")
        return self._publish(self._lease.epoch + 1, (self._lease.slot_id + 1) % self._config.bucket_count)

    def run(self, shutdown_requested: Event) -> int:
        try:
            self.publish_initial()
            while not shutdown_requested.is_set():
                self._sleep(self._config.slot_duration_ms / 1000)
                if not shutdown_requested.is_set(): self.advance()
            return 0
        except (ClockPublishError, ClockHealthError):
            return 1
