# Implementation Plan — Phase 4 / Slice 2 / Story 3

## Scope

Move blocking relationship writes to one bounded worker thread while retaining
Kafka consumer ownership, coordination intake, lease validation, contiguous
ledger resolution, and every commit in the polling thread.  Story 4 alone
wires real coordination-consumer construction and Docker arguments.

## Files

- Modify `src/loader/edge_loader.py`.
- Modify `tests/test_edge_loader.py`.

## API and ownership model

```python
@dataclass(frozen=True)
class WorkerBatch:
    batch_id: int
    records: tuple[PendingEdgeRecord, ...]
    gated_records: tuple[GatedPendingRecord, ...]
    lease_epoch: int | None
    lease_slot_id: int | None
    lease_expires_at_ms: int | None

@dataclass(frozen=True)
class WorkerResult:
    batch: WorkerBatch
    outcome: Literal["SUCCESS", "WRITE_FAILURE", "CANCELLED_PRESTART"]
    error: Exception | None

@dataclass(frozen=True)
class InFlightResourceHold:
    batch_id: int
    endpoint_buckets: frozenset[int]
    lease_epoch: int | None
    slot_id: int | None

class EdgeLoader:
    def _start_worker(self) -> None: ...
    def _worker_loop(self) -> None: ...
    def _enqueue_batch(self) -> bool: ...
    def _drain_worker_results(self) -> bool: ...
    def _stop_worker(self) -> bool: ...
```

`WorkerBatch.__post_init__` rejects nonpositive ids, duplicate Kafka
provenance, non-monotonic per-partition offsets, mismatched gated/ordinary
provenance, and partial lease context. Gated batches carry exact accepted lease
epoch/slot/expiry; ungated batches carry all three as `None`.
`WorkerResult.__post_init__` requires SUCCESS/CANCELLED_PRESTART to have no
error and WRITE_FAILURE to have a non-null exception.

Use `queue.Queue(maxsize=loading.slot_worker_queue_max_batches)` for work and
an unbounded results queue; a unique sentinel requests worker stop.  The poll
owner creates/joins the worker.  It alone calls every work-consumer method
(`poll`, `assign`, `unassign`, `pause`, `resume`, `commit`) and every
coordination poller.  The worker receives an immutable batch snapshot and
executes only existing writer/partitioner/lane-batcher/coordinator work.

Before enqueue, the poll owner drains coordination and revalidates leases, then
assigns an increasing `batch_id` and snapshots current lease context; the poll
owner's outstanding map (not the value object) rejects globally duplicate ids.
It may
maintain up to `slot_worker_queue_max_batches` queued/in-flight batches so
queue saturation is real; it pauses assignments only when the next batch
cannot enqueue, then fails closed. It keeps poll-owner-only
`_outstanding_batches: dict[int, WorkerBatch]`, `_worker_inflight_batch_id`,
and `_delivered_results: dict[int, WorkerResult]`. Each outstanding batch also
has a poll-owner-issued `ExecutionPermit(batch_id, lease_epoch, lease_slot_id,
generation)` held in a lock-protected registry whose atomic per-batch state is
`QUEUED -> STARTED -> RESULT` or `QUEUED -> CANCELLED_PRESTART -> RESULT`.
Worker acquisition atomically changes only a current `QUEUED` permit to
`STARTED`; a dequeued cancelled item emits `CANCELLED_PRESTART`, never success.
Because `queue.Queue` cannot remove arbitrary items, a normal generation change
marks a queued permit `CANCELLED_PRESTART` and leaves its tombstone occupying
queue capacity until the worker dequeues it. The poll owner returns immutable
records to the slot buffer but retains outstanding/tombstone identity until it
consumes the matching cancelled result exactly once; only then does it remove
the old batch and permit. Capacity accounting includes queued tombstones, so a
rapid rotation cannot overfill or leak the queue.
`_on_revoke()` includes
partitions of all three collections, the current not-yet-enqueued batch, and
`SlotAwareAdmissionBuffer.unresolved_partitions` as unresolved. The poll owner
continues nonblocking Kafka and coordination polling while awaiting results and
calls `SlotAdmission.require_current(now_ms=wall_clock_ms())` every poll cycle,
even without a control message. A normal newer lease generation atomically
cancels/requeues only `QUEUED` work by original arrival order; a Kafka rebalance
revocation is fatal for queued, started, buffered, or unresolved work. A worker
must acquire its current permit immediately before writer handoff and reports a
cancelled item without writing it. If rotation arrives after `STARTED`, the
already-started transaction may finish, but the poll owner revalidates it
against the current lease before resolution and locally holds any buffered or
re-released record whose endpoint buckets overlap a STARTED batch until that
result resolves or fails. This local in-flight-resource hold is exposed as
read-only `EdgeLoader.inflight_resource_holds -> tuple[InFlightResourceHold, ...]`
for Slice 3's fleet/clock handoff barrier. The property snapshots the
lock-protected unresolved QUEUED/STARTED registry after linearizing against permit transitions;
each hold contains the batch id, union of source/target configured bucket
resources, and lease epoch/slot. Resource overlap is nonempty set intersection
of those bucket unions, and `_release_slot_buffer()` consults the snapshot so
it does not admit an overlapping record while a hold exists. Slice 2
does not claim to fence a different loader because Slice 1 has no coordinated
lease acknowledgement barrier. Expiry marks
failure but polling continues until the worker joins. The poll
owner accepts each result exactly once only
when its `batch_id` and immutable snapshot equal the outstanding entry. A successful result
is subjected to the same post-write coordination drain and exact written-batch
lease revalidation before the poll owner marks ledger offsets resolved and
commits its contiguous prefix.  Worker failures, queue full, unexpected result
identity, poll/rebalance failure, revoked queued/in-flight/buffered work, or
join timeout all fail closed with `stage=worker`, edge, replica, run, and
epoch/slot context.  No `DRAIN_COMPLETE` is published after one of these
failures.

Shutdown uses one state machine: stop accepting/enqueuing new records; keep
polling work/coordination and drain matching successful results under current
lease checks; when no outstanding/queued/delivered result remains, enqueue the
sentinel and bounded injected-deadline join of a non-daemon worker. A worker
error, lease expiry, Kafka rebalance revoke,
sentinel enqueue full, or join timeout fails the run, but the poll owner still
joins before release and never commits an uncertain batch.
Use injected `thread_factory`, `queue_factory`, and event barriers for tests;
do not use timing sleeps to prove heartbeat behavior.

## Tests

- A blocking fake writer proves the main thread continues work-consumer and
  coordination polling while it waits; only the main thread calls `commit`.
- Success resolves/commits one exact result; writer exception, result mismatch,
  full queue, and post-write revocation cause no commit.
- Revoke during queued/in-flight work fails closed and worker is joined.
- Lease expiry without a new control message fails closed while a worker blocks,
  then joins it without commit.
- A blocked worker plus admitted batches fills the bounded queue and fails
  closed with no premature commit.
- With A blocking and B queued under epoch N, an epoch N+1 revocation of B
  atomically invalidates B's permit; B never reaches the writer.
- Cancelled queue tombstones occupy capacity until their matching cancelled
  result is consumed, and locally overlapping re-released work waits until a
  STARTED batch resolves.
- Shutdown covers matching-result drain, sentinel enqueue/full queue, and join
  timeout; no test leaves a thread alive.

Run:

```bash
.venv/bin/python -m pytest tests/test_edge_loader.py -q --tb=short
```

## Rollback / safety

The worker never owns a Kafka client.  Any uncertain queue/result/lease state
returns nonzero with all valid work uncommitted for replay; no DLQ/retry or
relationship Cypher change is introduced.
