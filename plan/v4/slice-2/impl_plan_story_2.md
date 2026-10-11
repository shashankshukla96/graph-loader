# Implementation Plan — Phase 4 / Slice 2 / Story 2

## Scope

Introduce bounded lease-gated buffering in `EdgeLoader` while retaining its
existing synchronous writer path.  The loader can receive coordination input
through an injected poller, but this story deliberately does not create the
worker thread; Story 3 moves writes off the polling owner.

## Files

- Modify `src/loader/slot_admission.py` to add a pure bounded
  `SlotAwareAdmissionBuffer` storing `(PendingEdgeRecord, EndpointBuckets)`.
- Modify `src/loader/edge_loader.py` to accept opt-in `slot_admission`,
  `coordination_poll`, and wall-clock dependencies and to gate valid records.
- Modify `tests/test_slot_admission.py` and `tests/test_edge_loader.py`.

## APIs and logic

```python
class SlotAwareAdmissionBuffer:
    def __init__(self, *, max_records: int) -> None: ...
    def add(self, pending: PendingEdgeRecord, buckets: EndpointBuckets) -> GatedPendingRecord: ...
    def requeue(self, entries: Iterable[GatedPendingRecord]) -> None: ...
    def release_owned(self, admission: SlotAdmission, *, now_ms: int) -> tuple[GatedPendingRecord, ...]: ...
    @property
    def unresolved_partitions(self) -> set[tuple[str, int]]: ...
    def __len__(self) -> int: ...
```

`GatedPendingRecord` is a frozen typed wrapper containing a
`PendingEdgeRecord`, `EndpointBuckets`, and the loader-assigned monotonic
`arrival_sequence`; its Kafka provenance `(topic, partition, offset)` must be
unique.  `add()` preserves arrival order and raises `LeaseStateError` before
retaining a record when capacity is full.  `release_owned()` iterates retained
wrappers in arrival order, removes exactly the currently owned wrappers,
retains all other valid unleased work, and lets `SlotAdmission` surface expiry
with its context.  `requeue()` merges returned and retained wrappers by
`arrival_sequence`, rejects duplicate provenance, and enforces capacity, so a
formerly admitted A is restored ahead of later buffered B.  It does not mutate
the Kafka ledger or perform I/O.

`EdgeLoader` gains optional `slot_admission`, `slot_buffer`,
`coordination_poll: Callable[[float], object | None]`, and
`wall_clock_ms: Callable[[], int]`.  These dependencies are an all-or-none
constructor invariant: the all-absent direct-loader case is ungated, the
all-present case is gated, and every partial combination raises a contextual
configuration/lease error before consumer subscription.  When active, `run()` calls
`_poll_coordination()` before and after each work poll and before final flush;
it accepts only raw message values from the injected coordination poller,
passes them to `SlotAdmission.accept_lease()`, and calls
`_release_slot_buffer()` after a new lease.  Malformed active-run leases,
coordination poll exceptions, expired retained work, or a malformed
coordination message fail closed through a contextual `stage=lease` log and a
nonzero return.

`_buffer_message()` continues to validate payloads and fsync malformed records.
For a valid record in gated mode it computes `endpoint_buckets(...,
loading_config.coordination.bucket_count)` and either appends directly to the
existing batch if currently owned or adds it to `SlotAwareAdmissionBuffer`.
Neither path resolves offsets before the existing durable `_flush_batch()`.
Every gated pending entry is a `GatedPendingRecord`, retaining endpoint buckets
and a loader-assigned monotonic arrival sequence in addition to unique Kafka
provenance.  Immediately before a batch
is handed to the writer, `_flush_batch()` drains coordination, revalidates the
current lease/time for each wrapper, and uses `requeue()` to merge
no-longer-owned wrappers with retained buffered work.  It writes only the remaining owned
entries, then preserves the established post-write ownership/revoke checks
before resolution and commit.  Lease expiry during either revalidation fails
closed; an entry is never written/resolved after a discovered ownership loss.
`_on_revoke()` includes slot-buffer partitions in its unsafe-work set.
`_finish_run()` refuses success with retained slot-buffer work, ensuring a
bulk termination never claims an unleased record drained.

No coordination Kafka `Consumer` is constructed here.  Story 4 owns concrete
client configuration and lifecycle.  No queue/thread, retry/DLQ policy,
Docker lifecycle, global rotation, or self-reference policy is changed.

## Tests

- Pure buffer FIFO preservation, selective release, full capacity, and expiry.
- Edge-loader valid unowned input is neither written nor committed; later
  owned lease releases and durably commits it.
- Future/non-owned records remain bounded, duplicate/stale/malformed active
  leases fail closed without commit, and foreign valid input is ignored.
- Every partial slot-gating dependency combination rejects construction; the
  all-absent direct-loader path remains ungated.
- A newly accepted lease that removes a batched record's buckets before flush
  returns it to the slot buffer; it is neither written nor committed.
- When an earlier released A is revoked after later B is buffered, requeue
  merges A before B on the next ownership release and rejects duplicate Kafka
  provenance.
- Buffered record revoke and bulk finalization both fail closed; malformed
  data retains current fsync-and-commit behavior.

Run:

```bash
.venv/bin/python -m pytest tests/test_slot_admission.py tests/test_edge_loader.py -q --tb=short
```

## Rollback / safety

Slot gating remains opt-in, so direct loader behavior is untouched.  On any
gated failure `EdgeLoader` returns nonzero and does not publish a false drain
acknowledgement; valid buffered work is deliberately left uncommitted for
Kafka replay.
