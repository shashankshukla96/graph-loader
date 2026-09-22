# Implementation Plan — Phase 4 / Slice 3 / Story 2

## Scope

Upgrade the run-scoped clock wire protocol and publisher to carry the Story 1
label-scoped fair rotation map. This story does not launch a fleet, change
loader admission, commit Kafka offsets, or implement self-reference isolation.

## Files

- Modify `src/orchestrator/coordination.py`.
- Modify `src/loader/slot_admission.py`.
- Modify `tests/test_coordination.py`.
- Modify `tests/test_slot_admission.py`.
- Modify `tests/test_rotation.py` only for emitted-lease/plan equivalence.

## Protocol contract

Extend the frozen `ClockLease` with:

```python
shared_bucket_owners: Mapping[str, Mapping[int, str]]
```

The canonical JSON carries both `bucket_owners` and
`shared_bucket_owners`. `shared_bucket_owners` maps a nonblank label to a
complete contiguous `0..bucket_count-1` bucket map, and every owner must be in
sorted `active_edge_types`. It may be empty only when no conflict family has a
shared label. Unknown/missing fields, duplicate JSON keys, malformed labels,
noninteger/bool buckets, incomplete maps, inactive owners, noncanonical owner
maps, malformed foreign leases, and expiry fail closed without echoing payload
data. Decode fully validates first and returns `None` only for a valid foreign
run.

`bucket_owners` remains an explicit deprecated compatibility projection for
the Slice 2 global admission API. It must still be complete and canonical, but
it must not be used by new shared-label fleet admission. While Story 3 has not
yet wired a fleet, the clock creates this projection deterministically from the
lexically first active type; Story 3 will change `SlotAdmission` to use only
`shared_bucket_owners` for shared endpoint labels. This prevents a silent
partial migration from treating a global bucket map as safe label ownership.

Extend decode/validation with an optional immutable expected schedule:

```python
def decode_clock_lease(
    payload: bytes, *, expected_run_id: str, now_ms: int,
    rotation_plan: RotationPlan | None = None,
) -> ClockLease | None: ...
```

Generic validation happens before foreign-run filtering. For a current-run
lease and supplied plan, validation requires the exact active edge set and
`shared_bucket_owners == rotation_plan.owners_for_epoch(epoch)` before the
lease can be accepted. It therefore rejects a wrong active owner from another
family, an omitted/empty conflict-label map, extra label, or any map that
differs from the deterministic epoch allocation. A valid foreign run remains
filtered after generic validation and is not evaluated against this run's
plan.

Until Story 3 supplies that expected plan to the rotation-aware admission
path, `SlotAdmission` explicitly rejects any lease with a nonempty
`shared_bucket_owners` map using an attributed `LeaseStateError`. It continues
to handle only legacy empty-map leases and never infers shared-label ownership
from `bucket_owners`; this intentionally fail-closed compatibility boundary
prevents a Slice 2 loader from writing under a label-scoped clock prematurely.

## Clock integration

Extend `GlobalBatchClock` with an optional validated `RotationPlan`:

```python
class GlobalBatchClock:
    def __init__(..., rotation_plan: RotationPlan | None = None) -> None: ...
```

When a plan is supplied, require its exact edge type set to equal the clock's
eligible non-self-referencing edges and use
`rotation_plan.owners_for_epoch(epoch)` for `shared_bucket_owners` on every
initial/advanced lease. It must validate the map against the configured bucket
count before `produce`, and validates the lease through the same expected-plan
validator before `produce`. The existing all-first-type behavior remains only for
direct legacy clock construction without a plan, emitting an empty shared map.

`_publish()` calls the exact health probe, validates non-regressing wall time,
constructs the next immutable lease, produces to the configured coordination
topic keyed by the sole run ID, requires exactly one successful delivery
callback and `flush() == 0`, and only then stores the lease. Producer/flush,
health, time, allocation, lease validation, and expiry-before-renewal failures
leave the previous accepted lease unchanged. Error messages use
`stage=clock run_id=<id> epoch=<n> slot=<n>` plus family/edge when known; they
never include raw control payloads.

## Tests

- Canonical encode/decode round-trip includes an immutable label-scoped map;
  valid foreign leases filter only after complete validation.
- Reject missing/additional fields, duplicate keys, blank/duplicate labels,
  incomplete/noncanonical bucket maps, invalid owners, stale/expired/malformed
  leases, and raw-payload exposure.
- Use Story 1 `WORKS_AT`/`BOUGHT` `Person` plan to prove each emitted epoch's
  lease exactly matches `owners_for_epoch`, has one owner per Person bucket,
  and alternates fairly.
- Verify plan/edge mismatch, failed health/produce/callback/flush, invalid or
  regressing time, and invalid allocation do not replace the current lease.
- Verify the compatibility projection remains complete but is explicitly not
  asserted as label ownership by the new protocol tests.
- Pass the expected plan to current-run decode and reject a wrong active owner
  from another family, a missing/empty shared map, and a complete map that
  differs from the exact epoch allocation. Prove valid foreign filtering still
  validates generic protocol shape without applying this run's plan.
- `tests/test_slot_admission.py` proves a legacy Slice 2 admission instance
  fails closed with context for a nonempty label-scoped map and never makes an
  ownership decision from `bucket_owners` in that case.

Run:

```bash
.venv/bin/python -m pytest tests/test_coordination.py tests/test_rotation.py tests/test_slot_admission.py -q --tb=short
```

## Rollback / safety

No loader, Docker, monitor, Neo4j, or offset-resolution path changes. A failed
clock publication exposes no new epoch, and the old global map is retained
only to keep Slice 2 direct tests compatible until Story 3 switches fleet
admission to the label-scoped map.
