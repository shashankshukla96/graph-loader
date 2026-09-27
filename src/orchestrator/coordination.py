"""Validated, run-scoped wire protocol for Phase 4 clock leases."""
from __future__ import annotations

from dataclasses import dataclass, field
import json
import re
from types import MappingProxyType
from typing import Mapping
from threading import Event

from src.models.schema import CoordinationConfig, EdgeConfig
from src.orchestrator.rotation import RotationPlan

COORDINATION_TOPIC = "graph.loader.coordination"


class ClockProtocolError(ValueError):
    """Raised when a clock lease is malformed, unsafe, or expired."""


@dataclass(frozen=True)
class ClockLease:
    """One immutable run-scoped slot lease and complete ownership maps."""
    run_id: str
    epoch: int
    slot_id: int
    issued_at_ms: int
    expires_at_ms: int
    active_edge_types: tuple[str, ...]
    bucket_owners: Mapping[int, str]
    shared_bucket_owners: Mapping[str, Mapping[int, str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.active_edge_types, (tuple, list)):
            raise ClockProtocolError("active_edge_types must be a sequence")
        if not isinstance(self.bucket_owners, Mapping):
            raise ClockProtocolError("bucket_owners must be a mapping")
        if not isinstance(self.shared_bucket_owners, Mapping):
            raise ClockProtocolError("shared_bucket_owners must be a mapping")
        object.__setattr__(self, "active_edge_types", tuple(self.active_edge_types))
        object.__setattr__(self, "bucket_owners", MappingProxyType(dict(self.bucket_owners)))
        object.__setattr__(self, "shared_bucket_owners", MappingProxyType({
            label: MappingProxyType(dict(owners))
            for label, owners in self.shared_bucket_owners.items()
        }))


def _integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ClockProtocolError(f"{field} must be a nonnegative integer")
    return value


def _validated(
    run_id: object, epoch: object, slot_id: object, issued_at_ms: object,
    expires_at_ms: object, active_edge_types: object, bucket_owners: object,
    shared_bucket_owners: object = None,
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
    if shared_bucket_owners is None:
        shared_bucket_owners = {}
    if not isinstance(shared_bucket_owners, Mapping):
        raise ClockProtocolError("shared_bucket_owners must be a mapping")
    shared: dict[str, Mapping[int, str]] = {}
    for label, raw_owners in shared_bucket_owners.items():
        if not isinstance(label, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", label):
            raise ClockProtocolError("shared bucket owner label is invalid")
        if label in shared or not isinstance(raw_owners, Mapping):
            raise ClockProtocolError("shared bucket owners has invalid label map")
        label_owners: dict[int, str] = {}
        for raw_bucket, owner in raw_owners.items():
            bucket = int(raw_bucket) if isinstance(raw_bucket, str) and raw_bucket.isdigit() and str(int(raw_bucket)) == raw_bucket else raw_bucket
            if isinstance(bucket, bool) or not isinstance(bucket, int) or bucket < 0 or bucket in label_owners:
                raise ClockProtocolError("shared bucket owners has invalid bucket")
            if not isinstance(owner, str) or owner not in types:
                raise ClockProtocolError("shared bucket owner must be active")
            label_owners[bucket] = owner
        if set(label_owners) != set(range(len(owners))):
            raise ClockProtocolError("shared bucket owners must cover every bucket")
        shared[label] = MappingProxyType(label_owners)
    return ClockLease(run_id, epoch, slot_id, issued, expires, types, MappingProxyType(dict(owners)), MappingProxyType(shared))


def _validate_rotation_plan(lease: ClockLease, rotation_plan: RotationPlan | None) -> None:
    """Require a current-run label map to equal the immutable planned epoch."""
    if rotation_plan is None:
        return
    if not isinstance(rotation_plan, RotationPlan):
        raise ClockProtocolError("rotation plan is invalid")
    planned_types = {edge_type for family in rotation_plan.families for edge_type in family.edge_types}
    if set(lease.active_edge_types) != planned_types:
        raise ClockProtocolError("clock lease active edge types do not match rotation plan")
    expected = rotation_plan.owners_for_epoch(lease.epoch)
    actual = {label: dict(owners) for label, owners in lease.shared_bucket_owners.items()}
    if actual != {label: dict(owners) for label, owners in expected.items()}:
        raise ClockProtocolError("clock lease shared ownership does not match rotation plan")


def encode_clock_lease(lease: ClockLease, *, rotation_plan: RotationPlan | None = None) -> bytes:
    """Return a canonical UTF-8 JSON lease after full validation."""
    valid = _validated(lease.run_id, lease.epoch, lease.slot_id, lease.issued_at_ms, lease.expires_at_ms, lease.active_edge_types, lease.bucket_owners, lease.shared_bucket_owners)
    _validate_rotation_plan(valid, rotation_plan)
    return json.dumps({"run_id": valid.run_id, "epoch": valid.epoch, "slot_id": valid.slot_id, "issued_at_ms": valid.issued_at_ms, "expires_at_ms": valid.expires_at_ms, "active_edge_types": list(valid.active_edge_types), "bucket_owners": {str(key): valid.bucket_owners[key] for key in sorted(valid.bucket_owners)}, "shared_bucket_owners": {label: {str(bucket): valid.shared_bucket_owners[label][bucket] for bucket in sorted(valid.shared_bucket_owners[label])} for label in sorted(valid.shared_bucket_owners)}}, sort_keys=True, separators=(",", ":")).encode()


def decode_clock_lease(payload: bytes, *, expected_run_id: str, now_ms: int, rotation_plan: RotationPlan | None = None) -> ClockLease | None:
    """Validate structure, then ignore foreign runs even after their expiry."""
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
        "active_edge_types", "bucket_owners", "shared_bucket_owners",
    }
    if set(data) != required_fields:
        raise ClockProtocolError("clock payload has missing or unexpected field")
    try:
        lease = _validated(**{key: data[key] for key in required_fields})
    except KeyError as exc:
        raise ClockProtocolError("clock payload has missing field") from exc
    if lease.run_id != expected_run_id:
        return None
    if lease.expires_at_ms <= now_ms:
        raise ClockProtocolError("clock lease has expired")
    _validate_rotation_plan(lease, rotation_plan)
    return lease


class ClockPublishError(RuntimeError):
    """Raised when a clock lease cannot be durably published."""


class ClockHealthError(RuntimeError):
    """Raised when exact clock-fleet health cannot be proven."""


class GlobalBatchClock:
    """Publish acknowledged, expiring run-scoped coordination leases."""

    def __init__(self, *, run_id: str, edges: list[EdgeConfig], config: CoordinationConfig,
                 producer, health_probe, wall_clock_ms, sleeper,
                 rotation_plan: RotationPlan | None = None) -> None:
        if not isinstance(run_id, str) or not run_id.strip() or not edges:
            raise ClockPublishError("stage=clock reason=requires a run id and eligible edges")
        if any(edge.nodes.is_self_referencing for edge in edges):
            raise ClockPublishError(f"stage=clock run_id={run_id} reason=self-referencing edges cannot join shared clock")
        types = tuple(sorted(edge.type for edge in edges))
        if len(types) != len(set(types)):
            raise ClockPublishError(f"stage=clock run_id={run_id} reason=clock edges must have unique types")
        if rotation_plan is not None:
            planned_types = {edge_type for family in rotation_plan.families for edge_type in family.edge_types}
            if set(types) != planned_types or rotation_plan.bucket_count != config.bucket_count:
                raise ClockPublishError(f"stage=clock run_id={run_id} reason=rotation plan does not match eligible edges or bucket count")
        self._run_id, self._types, self._config = run_id, types, config
        self._producer, self._health, self._wall, self._sleep = producer, health_probe, wall_clock_ms, sleeper
        self._rotation_plan = rotation_plan
        self._lease: ClockLease | None = None

    def _failure(self, reason: str, epoch: int | None = None, slot: int | None = None,
                 *, health: bool = False) -> ClockPublishError | ClockHealthError:
        """Create a payload-free clock failure with the attempted slot context."""
        suffix = ""
        if epoch is not None and slot is not None:
            suffix = f" epoch={epoch} slot={slot}"
        error_type = ClockHealthError if health else ClockPublishError
        return error_type(f"stage=clock run_id={self._run_id}{suffix} reason={reason}")

    def _publish(self, epoch: int, slot: int) -> ClockLease:
        try:
            self._health()
        except Exception as exc:
            raise self._failure("health probe failed", epoch, slot, health=True) from exc
        try:
            now = _integer(self._wall(), "wall_clock_ms")
        except ClockProtocolError as exc:
            raise self._failure("wall clock is invalid", epoch, slot) from exc
        if self._lease is not None and now < self._lease.issued_at_ms:
            raise self._failure("wall clock regressed", epoch, slot)
        if self._lease is not None and now >= self._lease.expires_at_ms:
            raise self._failure("lease expired before renewal", epoch, slot)
        try:
            shared_owners = {} if self._rotation_plan is None else self._rotation_plan.owners_for_epoch(epoch)
            lease = ClockLease(self._run_id, epoch, slot, now, now + self._config.lease_timeout_ms,
                               self._types, {bucket: self._types[0] for bucket in range(self._config.bucket_count)}, shared_owners)
            encoded = encode_clock_lease(lease, rotation_plan=self._rotation_plan)
        except Exception as exc:
            raise self._failure("rotation allocation is invalid", epoch, slot) from exc
        delivered = []
        try:
            self._producer.produce(self._config.topic, key=self._run_id, value=encoded, on_delivery=lambda error, _message: delivered.append(error))
            outstanding = self._producer.flush()
        except Exception as exc:
            raise self._failure("publish failed", epoch, slot) from exc
        if isinstance(outstanding, bool) or not isinstance(outstanding, int) or outstanding != 0 or len(delivered) != 1 or delivered[0] is not None:
            raise self._failure("publish was not acknowledged", epoch, slot)
        self._lease = lease
        return lease

    def publish_initial(self) -> ClockLease:
        return self._lease or self._publish(0, 0)

    def advance(self) -> ClockLease:
        if self._lease is None:
            raise self._failure("cannot advance before initial publish")
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
