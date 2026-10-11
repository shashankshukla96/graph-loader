# Implementation Plan — Phase 4 / Slice 2 / Story 1

## Scope

Add pure, deterministic endpoint-resource and current-lease primitives only.
This story does not consume Kafka, buffer records, start a worker, change
`EdgeLoader`, launch Docker, or change Cypher.

## Files

- Modify `src/loader/mix_and_batch.py` with public `EndpointBuckets`,
  `endpoint_bucket()`, and `endpoint_buckets()`.
- Create `src/loader/slot_admission.py` with `LeaseStateError`,
  `ActiveSlotLease`, and `SlotAdmission`.
- Modify `src/models/schema.py` and `config/graph_schema.yaml` only to add
  validated bounded slot-buffer/worker-queue configuration if required by the
  following stories.
- Modify `tests/test_mix_and_batch.py`, `tests/test_schema_validation.py`, and
  create `tests/test_slot_admission.py`.

## APIs and logic

```python
@dataclass(frozen=True)
class EndpointBuckets:
    source: int
    target: int

def endpoint_bucket(value: object, bucket_count: int) -> int:
    """Map a typed canonical endpoint value to [0, bucket_count)."""

def endpoint_buckets(record: EdgeRecord, bucket_count: int) -> EndpointBuckets:
    """Return stable source and target bucket resources for one edge."""

class SlotAdmission:
    def __init__(self, *, edge_type: str, run_id: str, bucket_count: int) -> None: ...
    def accept_lease(self, payload: bytes, *, now_ms: int) -> bool: ...
    def owns(self, buckets: EndpointBuckets, *, now_ms: int) -> bool: ...
    def require_current(self, *, now_ms: int) -> ActiveSlotLease: ...
```

`endpoint_bucket()` reuses `endpoint_digest()` and rejects bool/non-positive or
out-of-range configuration.  `SlotAdmission.accept_lease()` first calls
`decode_clock_lease(payload, expected_run_id=self._run_id, now_ms=now_ms)`.
Valid foreign messages return `False`; all invalid active-run messages raise
`LeaseStateError` with edge/run context and no payload echo.  It also requires
the decoded owner keys to equal `range(bucket_count)`, retains only a strictly
increasing **epoch**, rejects every duplicate delivery (including a byte-
identical or semantically identical re-encoding), and rejects a same-epoch
conflicting payload or an older epoch while retaining the prior valid lease.  `owns()`
returns `False` with no lease and raises only if the retained lease has expired;
it returns true only when both source and target bucket owners equal the
loader's edge type.  This intentionally implements the existing Slice 1
global bucket protocol without designing Slice 3 rotation.

`LoadingConfig` receives field-level immutable integer settings using
`Field(..., frozen=True)`: `slot_buffer_max_records: int = 10_000`
(1..100_000) and `slot_worker_queue_max_batches: int = 32` (1..1_024), both
rejecting booleans.  Do not freeze all of `LoadingConfig`, because existing
code and tests may legitimately alter other settings.  They are documented in
canonical YAML but unused by runtime until later stories.

The lease ordering invariant is exact: there is at most one accepted delivery
per epoch.  A new lease is accepted only if its epoch is strictly greater than
the retained epoch; its `slot_id` may be lower because `GlobalBatchClock`
wraps it on a later epoch.  Every second delivery for a retained epoch,
including an exact byte-equivalent duplicate and a semantically identical
re-encoding, is rejected as duplicate; any different same-epoch map and any
older epoch is rejected as conflicting/stale.  Rejection never replaces the
prior valid lease.  `slot_id` is not a secondary monotonic ordering key.

## Tests

- Deterministic typed hashing; string `"1"` differs from integer `1`; invalid
  bucket counts reject.
- Valid owned and not-owned endpoint pairs; no lease, foreign lease, malformed
  lease, expiry, wrong complete bucket range, byte-identical duplicate,
  semantically-identical duplicate, stale, and conflicting same-epoch behavior.
- Lease errors never include a sentinel raw payload.
- New configuration defaults, custom values, bounds, bool rejection, and
  attempted assignment to each new field raising Pydantic `ValidationError`.
- A newer epoch with a lower/wrapped slot is accepted; every second delivery
  in an accepted epoch and every lower epoch is rejected.

Run:

```bash
.venv/bin/python -m pytest tests/test_mix_and_batch.py tests/test_slot_admission.py tests/test_schema_validation.py -q --tb=short
```

## Rollback / safety

This story is pure and has no Kafka, Docker, or Neo4j side effect.  If the
contract is later incompatible with label-scoped Slice 3 ownership, preserve
the existing globally scoped behavior and extend it in a separate reviewed
story; do not silently reinterpret current leases.
