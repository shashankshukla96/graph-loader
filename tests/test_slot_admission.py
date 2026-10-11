"""Tests for current-run clock lease admission without Kafka I/O."""
from __future__ import annotations

import pytest
import json

from src.loader.edge_loader import EdgeRecord, PendingEdgeRecord
from src.loader.mix_and_batch import EndpointBuckets
from src.loader.slot_admission import GatedPendingRecord, LeaseStateError, SlotAdmission, SlotAwareAdmissionBuffer
from src.orchestrator.coordination import ClockLease, encode_clock_lease
from src.orchestrator.dependency_manager import build_conflict_families
from src.orchestrator.rotation import build_rotation_plan


def _payload(*, epoch: int = 1, slot_id: int = 0, owners=None, run_id: str = "run") -> bytes:
    return encode_clock_lease(ClockLease(
        run_id=run_id, epoch=epoch, slot_id=slot_id, issued_at_ms=10,
        expires_at_ms=100, active_edge_types=("BOUGHT", "WORKS_AT"),
        bucket_owners=owners or {0: "WORKS_AT", 1: "WORKS_AT", 2: "BOUGHT", 3: "BOUGHT"},
    ))


def _admission() -> SlotAdmission:
    return SlotAdmission(edge_type="WORKS_AT", run_id="run", bucket_count=4)


@pytest.mark.parametrize("bucket_count", [True, 0, 4097])
def test_constructor_rejects_unsafe_bucket_count(bucket_count: int) -> None:
    with pytest.raises(LeaseStateError, match="bucket count"):
        SlotAdmission(edge_type="WORKS_AT", run_id="run", bucket_count=bucket_count)


def test_accepts_only_current_run_and_requires_both_owned_buckets() -> None:
    admission = _admission()
    assert admission.accept_lease(_payload(run_id="other"), now_ms=11) is False
    assert admission.current is None
    assert admission.accept_lease(_payload(), now_ms=11) is True
    assert admission.owns(EndpointBuckets(0, 1), now_ms=11) is True
    assert admission.owns(EndpointBuckets(0, 2), now_ms=11) is False


def test_rejects_malformed_wrong_size_and_missing_loader_without_payload_echo() -> None:
    admission = _admission()
    sentinel = b'{bad: secret-payload}'
    with pytest.raises(LeaseStateError) as malformed:
        admission.accept_lease(sentinel, now_ms=11)
    assert "secret-payload" not in str(malformed.value)
    with pytest.raises(LeaseStateError, match="bucket ownership"):
        admission.accept_lease(_payload(owners={0: "WORKS_AT", 1: "WORKS_AT"}), now_ms=11)
    payload = encode_clock_lease(ClockLease(
        run_id="run", epoch=1, slot_id=0, issued_at_ms=10, expires_at_ms=100,
        active_edge_types=("BOUGHT",), bucket_owners={0: "BOUGHT", 1: "BOUGHT", 2: "BOUGHT", 3: "BOUGHT"},
    ))
    with pytest.raises(LeaseStateError, match="omits loader"):
        admission.accept_lease(payload, now_ms=11)


def test_rejects_duplicate_stale_and_conflicting_epochs_without_replacing_current() -> None:
    admission = _admission()
    initial = _payload(epoch=1, slot_id=3)
    assert admission.accept_lease(initial, now_ms=11)
    with pytest.raises(LeaseStateError, match="duplicate"):
        admission.accept_lease(initial, now_ms=11)
    equivalent = json.dumps(json.loads(initial.decode("utf-8")), indent=2).encode("utf-8")
    assert equivalent != initial
    with pytest.raises(LeaseStateError, match="duplicate"):
        admission.accept_lease(equivalent, now_ms=11)
    with pytest.raises(LeaseStateError, match="conflicting"):
        admission.accept_lease(_payload(epoch=1, slot_id=2), now_ms=11)
    with pytest.raises(LeaseStateError, match="stale"):
        admission.accept_lease(_payload(epoch=0, slot_id=9), now_ms=11)
    assert admission.current is not None and admission.current.epoch == 1


def test_newer_epoch_accepts_wrapped_slot_and_expiry_fails_closed() -> None:
    admission = _admission()
    assert admission.accept_lease(_payload(epoch=1, slot_id=3), now_ms=11)
    assert admission.accept_lease(_payload(epoch=2, slot_id=0), now_ms=11)
    with pytest.raises(LeaseStateError, match="expired"):
        admission.owns(EndpointBuckets(0, 1), now_ms=100)


def test_out_of_range_endpoint_bucket_fails_with_lease_context() -> None:
    admission = _admission()
    assert admission.accept_lease(_payload(), now_ms=11)
    with pytest.raises(LeaseStateError, match="stage=lease edge=WORKS_AT run_id=run.*outside"):
        admission.owns(EndpointBuckets(4, 0), now_ms=11)


@pytest.mark.parametrize("now_ms", [True, -1])
def test_invalid_now_fails_closed(now_ms: int) -> None:
    with pytest.raises(LeaseStateError, match="invalid current time"):
        _admission().accept_lease(_payload(), now_ms=now_ms)


def _pending(offset: int) -> PendingEdgeRecord:
    return PendingEdgeRecord(EdgeRecord("p", "c", {}), "works-at-events", 0, offset)


def test_slot_buffer_merges_revoked_batch_before_later_retained_record() -> None:
    admission = _admission()
    buffer = SlotAwareAdmissionBuffer(max_records=3)
    first = buffer.add(_pending(1), EndpointBuckets(0, 1))
    assert admission.accept_lease(_payload(), now_ms=11)
    released = buffer.release_owned(admission, now_ms=11)
    assert released == (first,)
    second = buffer.add(_pending(2), EndpointBuckets(2, 3))
    buffer.requeue(released)
    bought = SlotAdmission(edge_type="BOUGHT", run_id="run", bucket_count=4)
    assert bought.accept_lease(_payload(epoch=2, owners={0: "BOUGHT", 1: "BOUGHT", 2: "BOUGHT", 3: "BOUGHT"}), now_ms=11)
    assert [item.pending.offset for item in buffer.release_owned(bought, now_ms=11)] == [1, 2]
    with pytest.raises(LeaseStateError, match="duplicate"):
        buffer.requeue((second, second))


def test_slot_buffer_rejects_capacity_and_duplicate_arrival_sequence() -> None:
    buffer = SlotAwareAdmissionBuffer(max_records=1)
    first = buffer.add(_pending(1), EndpointBuckets(0, 1))
    with pytest.raises(LeaseStateError, match="full"):
        buffer.add(_pending(2), EndpointBuckets(0, 1))
    other = GatedPendingRecord(_pending(2), EndpointBuckets(0, 1), first.arrival_sequence)
    empty = SlotAwareAdmissionBuffer(max_records=2)
    with pytest.raises(LeaseStateError, match="arrival sequence"):
        empty.requeue((first, other))


def test_legacy_slot_admission_rejects_label_scoped_lease_without_inferring_global_owner() -> None:
    payload = encode_clock_lease(ClockLease(
        run_id="run", epoch=1, slot_id=0, issued_at_ms=10, expires_at_ms=100,
        active_edge_types=("BOUGHT", "WORKS_AT"),
        bucket_owners={0: "WORKS_AT", 1: "WORKS_AT", 2: "BOUGHT", 3: "BOUGHT"},
        shared_bucket_owners={"Person": {0: "WORKS_AT", 1: "WORKS_AT", 2: "BOUGHT", 3: "BOUGHT"}},
    ))
    admission = _admission()
    with pytest.raises(LeaseStateError, match="label-scoped lease requires rotation-aware admission"):
        admission.accept_lease(payload, now_ms=11)
    assert admission.current is None


def test_rotation_aware_admission_uses_shared_labels_not_global_projection() -> None:
    class Edge:
        def __init__(self, edge_type, source, target):
            self.type = edge_type
            self.nodes = type("Nodes", (), {"source": source, "target": target})()

    plan = build_rotation_plan(build_conflict_families([
        Edge("WORKS_AT", "Person", "Company"), Edge("BOUGHT", "Person", "Product"),
    ]), bucket_count=4)
    lease = ClockLease(
        "run", 0, 0, 10, 100, ("BOUGHT", "WORKS_AT"),
        {bucket: "BOUGHT" for bucket in range(4)}, plan.owners_for_epoch(0),
    )
    works = SlotAdmission(
        edge_type="WORKS_AT", run_id="run", bucket_count=4,
        rotation_plan=plan, endpoint_labels=("Person", "Company"),
    )
    bought = SlotAdmission(
        edge_type="BOUGHT", run_id="run", bucket_count=4,
        rotation_plan=plan, endpoint_labels=("Person", "Product"),
    )
    payload = encode_clock_lease(lease, rotation_plan=plan)
    assert works.accept_lease(payload, now_ms=11)
    assert bought.accept_lease(payload, now_ms=11)
    assert works.owns(EndpointBuckets(0, 3), now_ms=11) is False
    assert bought.owns(EndpointBuckets(0, 3), now_ms=11) is True
    for labels in (("Company", "Person"), ("Person", "Product")):
        with pytest.raises(LeaseStateError, match="endpoint labels do not match"):
            SlotAdmission(
                edge_type="WORKS_AT", run_id="run", bucket_count=4,
                rotation_plan=plan, endpoint_labels=labels,
            )
