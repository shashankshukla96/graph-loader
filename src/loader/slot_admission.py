"""Pure current-lease validation and endpoint ownership decisions."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, TYPE_CHECKING

from src.loader.mix_and_batch import EndpointBuckets
from src.orchestrator.coordination import ClockLease, ClockProtocolError, decode_clock_lease

if TYPE_CHECKING:
    from src.loader.edge_loader import PendingEdgeRecord


class LeaseStateError(RuntimeError):
    """Raised when an active relationship lease is unsafe to use."""


@dataclass(frozen=True)
class ActiveSlotLease:
    """A validated current lease retained by one edge-loader replica."""

    lease: ClockLease

    @property
    def epoch(self) -> int:
        """Return the current clock epoch."""
        return self.lease.epoch

    @property
    def slot_id(self) -> int:
        """Return the current clock slot."""
        return self.lease.slot_id


@dataclass(frozen=True)
class GatedPendingRecord:
    """One uncommitted edge record with its lease resources and arrival order."""

    pending: "PendingEdgeRecord"
    buckets: EndpointBuckets
    arrival_sequence: int

    def __post_init__(self) -> None:
        if isinstance(self.arrival_sequence, bool) or not isinstance(self.arrival_sequence, int) or self.arrival_sequence < 0:
            raise ValueError("gated pending arrival sequence must be a nonnegative integer")

    @property
    def provenance(self) -> tuple[str, int, int]:
        """Return the unique Kafka origin of this uncommitted record."""
        return (self.pending.topic, self.pending.partition, self.pending.offset)


class SlotAwareAdmissionBuffer:
    """Bound valid but currently unleased records without resolving offsets."""

    def __init__(self, *, max_records: int) -> None:
        if isinstance(max_records, bool) or not isinstance(max_records, int) or max_records <= 0:
            raise LeaseStateError("slot admission buffer requires a positive maximum")
        self._max_records = max_records
        self._items: list[GatedPendingRecord] = []
        self._provenance: set[tuple[str, int, int]] = set()
        self._next_sequence = 0

    def __len__(self) -> int:
        """Return the number of unresolved retained records."""
        return len(self._items)

    @property
    def unresolved_partitions(self) -> set[tuple[str, int]]:
        """Return every partition with retained valid unleased work."""
        return {(item.pending.topic, item.pending.partition) for item in self._items}

    def add(self, pending: "PendingEdgeRecord", buckets: EndpointBuckets) -> GatedPendingRecord:
        """Retain one record in FIFO order or fail before exceeding capacity."""
        if len(self._items) >= self._max_records:
            raise LeaseStateError("slot admission buffer is full")
        item = GatedPendingRecord(pending, buckets, self._next_sequence)
        self._next_sequence += 1
        if item.provenance in self._provenance:
            raise LeaseStateError("slot admission buffer has duplicate Kafka provenance")
        self._items.append(item)
        self._provenance.add(item.provenance)
        return item

    def requeue(self, entries: Iterable[GatedPendingRecord]) -> None:
        """Merge returned entries by arrival sequence without duplicate provenance."""
        returning = tuple(entries)
        return_provenance = [entry.provenance for entry in returning]
        return_sequences = [entry.arrival_sequence for entry in returning]
        if len(set(return_provenance)) != len(return_provenance):
            raise LeaseStateError("slot admission requeue has duplicate Kafka provenance")
        if len(set(return_sequences)) != len(return_sequences):
            raise LeaseStateError("slot admission requeue has duplicate arrival sequence")
        if any(provenance in self._provenance for provenance in return_provenance):
            raise LeaseStateError("slot admission requeue duplicates retained Kafka provenance")
        retained_sequences = {entry.arrival_sequence for entry in self._items}
        if any(sequence in retained_sequences for sequence in return_sequences):
            raise LeaseStateError("slot admission requeue duplicates retained arrival sequence")
        if len(self._items) + len(returning) > self._max_records:
            raise LeaseStateError("slot admission requeue exceeds capacity")
        self._items = sorted((*self._items, *returning), key=lambda item: item.arrival_sequence)
        self._provenance.update(return_provenance)

    def release_owned(
        self, admission: "SlotAdmission", *, now_ms: int, blocked_buckets: frozenset[int] = frozenset()
    ) -> tuple[GatedPendingRecord, ...]:
        """Release exactly the currently owned records in original arrival order."""
        released: list[GatedPendingRecord] = []
        retained: list[GatedPendingRecord] = []
        for item in self._items:
            if (
                admission.owns(item.buckets, now_ms=now_ms)
                and item.buckets.source not in blocked_buckets
                and item.buckets.target not in blocked_buckets
            ):
                released.append(item)
                self._provenance.remove(item.provenance)
            else:
                retained.append(item)
        self._items = retained
        return tuple(released)


class SlotAdmission:
    """Reject unsafe clock input and decide one edge's current ownership."""

    def __init__(self, *, edge_type: str, run_id: str, bucket_count: int) -> None:
        if not isinstance(edge_type, str) or not edge_type.strip():
            raise LeaseStateError("lease admission requires a nonblank edge type")
        if not isinstance(run_id, str) or not run_id.strip():
            raise LeaseStateError(f"lease admission edge={edge_type} requires a nonblank run id")
        if (
            isinstance(bucket_count, bool)
            or not isinstance(bucket_count, int)
            or not 1 <= bucket_count <= 4096
        ):
            raise LeaseStateError(f"lease admission edge={edge_type} has invalid bucket count")
        self._edge_type = edge_type
        self._run_id = run_id
        self._bucket_count = bucket_count
        self._current: ActiveSlotLease | None = None

    @property
    def current(self) -> ActiveSlotLease | None:
        """Return the retained current lease, if one was accepted."""
        return self._current

    @property
    def bucket_count(self) -> int:
        """Return the immutable configured endpoint bucket cardinality."""
        return self._bucket_count

    def _context(self, reason: str, lease: ClockLease | None = None) -> LeaseStateError:
        suffix = ""
        if lease is not None:
            suffix = f" epoch={lease.epoch} slot={lease.slot_id}"
        return LeaseStateError(
            f"stage=lease edge={self._edge_type} run_id={self._run_id}{suffix} reason={reason}"
        )

    @staticmethod
    def _valid_now(now_ms: int) -> bool:
        return not isinstance(now_ms, bool) and isinstance(now_ms, int) and now_ms >= 0

    def accept_lease(self, payload: bytes, *, now_ms: int) -> bool:
        """Accept exactly one new current-run lease per increasing epoch.

        A valid foreign-run message is ignored.  Every duplicate, stale, or
        conflicting current-run delivery fails closed without replacing the
        last accepted lease.
        """
        if not self._valid_now(now_ms):
            raise self._context("invalid current time")
        try:
            lease = decode_clock_lease(payload, expected_run_id=self._run_id, now_ms=now_ms)
        except ClockProtocolError as exc:
            raise self._context(f"invalid clock lease: {exc}") from exc
        if lease is None:
            return False
        if set(lease.bucket_owners) != set(range(self._bucket_count)):
            raise self._context("lease bucket ownership does not match configured bucket count", lease)
        if self._edge_type not in lease.active_edge_types:
            raise self._context("lease omits loader edge type", lease)
        if self._current is not None:
            current = self._current.lease
            if lease.epoch < current.epoch:
                raise self._context("stale clock epoch", lease)
            if lease.epoch == current.epoch:
                if lease == current:
                    raise self._context("duplicate clock lease", lease)
                raise self._context("conflicting clock lease in current epoch", lease)
        self._current = ActiveSlotLease(lease)
        return True

    def require_current(self, *, now_ms: int) -> ActiveSlotLease:
        """Return a non-expired accepted lease or raise a contextual error."""
        if not self._valid_now(now_ms):
            raise self._context("invalid current time")
        if self._current is None:
            raise self._context("no current clock lease")
        if now_ms >= self._current.lease.expires_at_ms:
            raise self._context("clock lease expired", self._current.lease)
        return self._current

    def owns(self, buckets: EndpointBuckets, *, now_ms: int) -> bool:
        """Return whether this loader owns both endpoint buckets right now."""
        if not 0 <= buckets.source < self._bucket_count or not 0 <= buckets.target < self._bucket_count:
            raise self._context("endpoint bucket is outside configured bucket count")
        if self._current is None:
            if not self._valid_now(now_ms):
                raise self._context("invalid current time")
            return False
        lease = self.require_current(now_ms=now_ms).lease
        return (
            lease.bucket_owners[buckets.source] == self._edge_type
            and lease.bucket_owners[buckets.target] == self._edge_type
        )
