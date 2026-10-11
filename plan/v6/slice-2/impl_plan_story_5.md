# Story 5 implementation plan: Edge Loader and Edge-Execution Instrumentation

(Written retroactively, reflecting what was actually implemented and
reviewed — see `plan/v6/completion_slice-2.md` for the process note on
why Steps A and B interleaved for Stories 5-7 rather than a doc landing
fully before any code.)

## Files

- Edit: `src/loader/edge_loader.py` (both write paths)
- Edit: `src/loader/edge_execution.py`
- Edit: `tests/test_edge_loader.py`, `tests/test_edge_execution_writer.py`,
  `tests/test_bucket_lease_execution.py`

## Design

`EdgeLoader.__init__` gains `metrics: GraphLoaderMetrics | None = None`,
defaulting to `process_metrics()`.

**Two distinct write paths, both instrumented identically** (discovered
during implementation — `stories.md`'s "the streaming worker loop" text
names only one of them):

1. `_worker_loop` (the slot-gated streaming path, used when
   `self._slot_gating` and a worker thread has started): one
   `write_duration`/`records_consumed` observe/inc per worker batch,
   emitted in an `if write_failure is None:` branch that sits entirely
   outside the write's own `try:` (fail-soft pattern below), wrapped in
   its own inner `try/except` so even a log-line/queue-put failure can't
   leave the batch unresolved.
2. `_flush_batch` (the classic/bulk, non-slot-gated synchronous path) —
   not originally named in `stories.md`, found by tracing an early test
   failure (`test_worker_batch_increments_records_consumed_and_write_duration_once`
   passed a plain, non-gated `EdgeLoader` and the metric never appeared;
   `_flush_batch`'s own separate `stage=edge-write` log line was the
   giveaway). Same shape, same labels.

**Fail-soft restructuring (P6-S2 review finding S4-1, applied here too):**
both paths capture the write outcome in a `write_failure: Exception | None`
local instead of doing log+metrics inside the `try:` that guards the
write. `metrics_module.safe_emit(...)` wraps every `.inc()`/`.observe()`
call so a metrics-library exception can never be mistaken for a write
failure.

`edge_execution.py`:

- `ApocLockedEdgeWriter` gains `metrics`/`kind` (default `kind="edge"`,
  overridden to `kind="planned-writer"` by Story 6's own construction
  site). `write_rows` tracks a local `retried` flag; on the attempt that
  succeeds after ≥1 retryable `TransientError`, increments
  `deadlocks_recovered_total` exactly once (never per attempt, never on
  the path that ultimately raises) — via `safe_emit`, since this sits
  inside a `try:` that only catches `TransientError`, so an unguarded
  metrics exception would otherwise escape a batch that already
  committed (caught by the review's own test).
- `NativeDisjointEdgeWriter` gets no wiring — it has no retry loop at
  all, so `deadlocks_recovered_total` structurally cannot fire for it;
  documented as a comment, not a gap.
- `BucketLeaseExecutionCoordinator` gains `metrics`; `execute_pending`
  observes `lease_wait_seconds` once per outer flush (`state.lease_wait_seconds`,
  an accumulator the class already maintains) — no new timer.
- `build_edge_execution` threads `metrics`/`kind` through to both writer
  construction sites (lease mode, classic/native mode).

## Deviations from `stories.md`

1. Both edge write paths instrumented, not only the named
   `_worker_loop` — a scope completion, not a deviation in spirit, since
   it's the same "every loader's write path" requirement.
2. `RETRY_REASONS`'s `"missing-endpoint"` spelling (hyphenated, per Story
   1) is independent of `dead_letter.FAILURE_CLASSES`' `missing_endpoint`
   (underscored) — two different label domains, not a typo.

## Reviewer findings and resolutions (summary; full detail in the
`fix(metrics):` conversation history)

- **T1** (blocking): the three "worker" tests actually drove
  `_flush_batch`, not `_worker_loop` — renamed to `test_classic_flush_*`
  and added three genuine `_worker_loop` equivalents using `_gated_loader`.
- **T2**: `lease_wait_seconds` had zero test coverage — added a
  contended-vs-disjoint test in `test_bucket_lease_execution.py`; loosened
  the wall-clock assertions after a flakiness concern (thread-timing
  dependent) and added a HELP-text clause about RPC round-trip time.
- **T3**: added the one edge-path DLQ counter test Story 5 promises,
  using a real `DeadLetterProducer` (not a mock) so `on_dlq_routed` fires
  exactly as it does in production.
- **N-a**: hardened `_worker_loop`'s post-write log+metrics block with its
  own `try/except` so an unexpected failure there can't leave a worker
  batch unresolved.
- **N-c**: added a two-retries-then-success test to distinguish "count
  once" from "count per retry" (a one-retry test can't tell the two
  apart).
- **N-d**: documented that `lease_wait_seconds` sums across a flush's
  concurrent workers and can exceed the flush's own wall time.
- Also self-caught (my own new test, before sending to review): the
  `deadlocks_recovered.inc()` call was initially unguarded by `safe_emit`
  inside a `TransientError`-only `try:` — fixed immediately.

## Definition of Done

All of Story 5's stated ACs verified by the tests above; reviewed and
merged as part of the combined Stories 1-7 commit (see the completion
doc's process-deviation note).
