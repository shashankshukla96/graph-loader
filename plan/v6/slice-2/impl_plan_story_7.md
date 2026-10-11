# Story 7 implementation plan: Lease Server and Clock Runner Instrumentation

(Written retroactively, reflecting what was actually implemented and
reviewed.)

## Files

- Edit: `src/orchestrator/lease_server.py`
- Edit: `src/orchestrator/clock_runner.py` (already wired for the
  process-info triad in Story 3; this story only adds the AC-specific
  test proving the scope is exactly that triad)
- Edit: `tests/test_lease_server.py`, `tests/test_clock_runner.py`

## Design

`LeaseTable` gains `metrics`. `grant()` increments `lease_grants_total`
and sets `lease_held_resources` to `len(self._holder)` — this one method
covers every grant path (`ACQ`'s `try_acquire`, an immediate `ANY`, and
`_wake`'s wait-then-grant, since all three call this same method), which
is exactly Story 7's AC ("increases by exactly 1... whether granted
immediately or after a wait"). `release()` updates the same gauge.
`extend()` (the `EXT` command, used by the bucket-lease coordinator's
merged-cell extension) also updates the gauge when it actually adds
resources — found only after the reviewer traced a real hot-path gap
(see S7-1 below).

`BucketLeaseServer` gains `metrics`, passed to its `LeaseTable`. Both
denial sites (`ACQ`'s failed `try_acquire`, and `ANY` with
`wait_ms == 0`) increment `lease_denials_total`. `_any()` sets
`lease_waiters` after adding a waiter to `self._waiters`;
`_remove_waiter()` — the single method every removal path already funnels
through (`_expire`, `_wake`'s grant path, `close()`, disconnect cleanup)
— sets the same gauge after removing one, so it cannot drift regardless
of which path removed it. `_expire()` increments
`lease_wait_timeouts_total`.

`clock_runner.py`: unchanged from Story 3's wiring (process-info triad
only, no hot-path histogram — the clock never writes to Neo4j). This
story's only new work here is the test proving `/metrics` shows
`graph_loader_process_up{kind="clock",...}` but no actual sample line for
`graph_loader_neo4j_write_duration_seconds` (the HELP/TYPE header for
every metric always exists in every process's registry; the AC's "no
series exists" means no sample, not an absent header).

## Deviations from `stories.md`

None beyond the slice-wide port/name corrections already recorded.

## Reviewer findings and resolutions

- **S7-1** (blocking): `LeaseTable.extend()` mutates `self._holder`
  directly (a real hot-path mutation via the bucket-lease coordinator's
  `EXT` command) but originally updated only `stats.extensions`, not the
  gauge — `lease_held_resources` under-reported between an extension and
  the next grant/release. Fixed: `extend()` now updates the gauge
  whenever `added` is non-empty.
- **S7-2**: the epic's literal "never exceeds `bucket_count`" bound is
  wrong — resources are `label:bucket` tokens, so the true bound is
  `len(keyed_labels) * bucket_count`. Renamed the test to
  `test_lease_held_resources_tracks_grant_extend_and_release` (what it
  actually proves) and added a dedicated EXT-specific test.
- **S7-3**: the grant-count test only exercised immediate `ACQ` grants,
  not an immediate `ANY` grant, even though the AC's literal wording is
  about `ANY` — added one `acquire_any(..., wait_ms=0)` case.
- **S7-4**: the clock test asserted inside the mocked `GlobalBatchClock.run`,
  so a real failure was swallowed by `main()`'s own exception handler into
  a bare `assert 1 == 0` with no real message — fixed by capturing the
  scraped body and asserting after `main()` returns.

## Definition of Done

All of Story 7's stated ACs verified by the tests above; reviewed and
merged as part of the combined Stories 1-7 commit.
