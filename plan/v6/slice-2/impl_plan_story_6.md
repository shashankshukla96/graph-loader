# Story 6 implementation plan: Planned Writer and Planned Dispatcher Instrumentation

(Written retroactively, reflecting what was actually implemented and
reviewed.)

## Files

- Edit: `src/loader/planned_writer.py`
- Edit: `src/orchestrator/planned_dispatcher.py`
- Edit: `tests/test_planned_writer.py`, `tests/test_planned_dispatcher.py`

## Design

**Planned writer.** `planned_writer.py`'s real `ApocLockedEdgeWriter`
construction site (inside `main()`, one writer per `plan.types` entry) now
passes `kind="planned-writer", metrics=process_metrics` — this is the only
change needed to make Story 5's `deadlocks_recovered_total` health signal
concrete for planned mode (reused verbatim, per `stories.md`'s own
dependency note).

`PlannedEdgeWriterWorker` gains a `metrics` param. Its `run()` loop emits
`write_duration`/`records_consumed` once per chunk, right beside the
existing `stage=edge-write ... planned=1 ...` log line, labelled by *that
chunk's own* `chunk.edge_type` — the one series in this catalogue where
`name` varies per observation rather than being fixed at process start
(a planned writer serves every type in the plan across its lifetime).
Wrapped in `safe_emit` since this sits inside the same broad `try:` that
surrounds the whole loop body.

**Planned dispatcher.** `DispatchState.__init__` sets
`plan_chunks_planned` once, to `len(plan.chunks)`, right after
`self.stats = DispatchStats()` — never changes for the life of the run.
`grant()` sets `plan_active_writers` after `stats.active += 1`; `done()`
sets both `plan_active_writers` (after `stats.active -= 1`) and
`plan_chunks_committed` (after `stats.completed += 1`) — both gauges
updated at the exact same mutation points the plain-Python stats already
use, no new bookkeeping. `PlannedDispatcher._resolve()` observes
`plan_resource_wait_seconds` once per wait *resolution* (not accumulated,
mirroring `total_wait_ms`'s own per-event shape) — including
`close()`'s own "ERR dispatcher closed" resolutions of still-parked
waiters (kept deliberately consistent with `total_wait_ms`'s existing
accounting of the same event, documented as a known skew source for a
mid-run shutdown, per P5-Q5's at-least-once precedent).

## Deviations from `stories.md`

None beyond the slice-wide port/name corrections already recorded in
`impl_plan_story_1.md`/`impl_plan_story_3.md`.

## Reviewer findings and resolutions

- **S6-1** (blocking): the first version of
  `test_deadlocks_recovered_stays_at_zero_for_planned_writer_kind` was
  tautological — it constructed `ApocLockedEdgeWriter` itself with the
  same kwargs it then asserted on, so it would not fail if
  `kind="planned-writer", metrics=process_metrics` were ever deleted from
  the real construction site. Replaced with
  `test_main_constructs_apoc_writer_and_worker_with_planned_writer_kind_and_process_metrics`,
  which drives the real `planned_writer.main()` with every I/O dependency
  faked and recording-mock classes swapped in for
  `ApocLockedEdgeWriter`/`PlannedEdgeWriterWorker` — confirmed to fail
  when the real line is reverted.
- **S6-2**: the dispatcher's `close()` resolving parked waiters with an
  error reply adds `plan_resource_wait_seconds` observations that are
  shutdown latency, not real contention — kept (see Design above) and
  documented rather than special-cased, to avoid a new inconsistency
  between the log field and the metric.
- **S6-3**: `test_plan_active_writers_never_exceeds_launched_writer_count`
  is a weak unit-level smoke check (the third writer's denial makes the
  bound trivially true); a real bound check belongs in Slice 4's stress
  test, noted in the completion doc.

## Definition of Done

All of Story 6's stated ACs verified by the tests above; reviewed and
merged as part of the combined Stories 1-7 commit.
