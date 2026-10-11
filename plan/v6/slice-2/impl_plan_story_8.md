# Story 8 implementation plan: CLI/Orchestrator Host-Side Metrics

## Structural correction vs. `stories.md` (binding, confirmed with the
reviewer before implementing)

`stories.md` pictures `main()`'s bulk branch and `main()`'s
`mode == "stream"` branch as two separate top-level blocks needing their
own `start_metrics_server` call/stop pair, plus a third pair for
`_run_isolated_self_reference_stream`. Neither name matches this
codebase: `main()` dispatches to `handle_start(args)` via `args.func(args)`,
which wraps `_handle_start(args, cleanups)` in a try/finally that runs
`cleanups` in reverse. `_handle_start` is the single call-tree root for
bulk mode, fleet-wide streaming (`cli.py`'s `mode == "stream"` branch,
driving `monitor.supervise_stream(...)`), and the isolated stream phase
(the real function is `_run_isolated_relationship_stream_phase`, not
`_run_isolated_self_reference_stream`) — all three run inside one
`_handle_start()` execution.

**Resolution**: one `start_metrics_server` call near the top of
`_handle_start`, right before schema load, `addr="127.0.0.1"`,
`allow_ephemeral_fallback=True`, `kind="cli"`, `name="-"`. The stop+
snapshot cleanup is registered via `cleanups.append(...)` in the same
statement pair as the start, *before* any of `_handle_start`'s many early
`return 1` paths — because `cleanups` runs in reverse and this is
appended first, it runs *last*, after `planner.close(...)` etc., so the
exit-time snapshot captures final planner/lag values.

This is a documented deviation from the AC's literal wording ("stops the
metrics server in the same finally that restores signal handlers") — the
server now stops later, after container teardown, which the reviewer
judged better (the endpoint stays live while the shutdown itself is
still observable) while still meeting the AC's verifiable property: the
port is unbound immediately after `main()`/`handle_start()` returns, on
every path (bulk, fleet-wide stream, the isolated stream phase, and an
early-return path) — proven by four tests in `tests/test_cli.py`.

**Known gap, not fixed in this slice (P6-S5 scope)**: outside
`supervise_stream`'s own window — i.e. the whole bulk run, and stream
setup/teardown before/after that call — `_handle_start` installs no
SIGTERM handler of its own (the fleet-wide/isolated stream phases install
one only around their own `supervise_stream` call). A SIGTERM during
those windows kills the process immediately with no cleanup and no
snapshot. Pre-existing, not introduced by this slice; listed here next to
the clock's own late-SIGTERM-handler gap (Story 3/7's notes).

## Files

- Edit: `src/cli.py`
- Edit: `src/orchestrator/relationship_bulk_monitor.py`
- Edit: `src/loader/edge_planner.py`
- Edit: `tests/test_cli.py`, `tests/test_relationship_bulk_monitor.py`,
  `tests/test_edge_planner.py`

## Design

**Consumer lag** — scoped to `RelationshipBulkMonitor.supervise_stream`
(the streaming path) only, *not* `BulkMonitor`'s bulk-mode tick and not
node loaders in stream mode. This is a documented scope limitation, not
an oversight: `BulkMonitor`'s own tick has no natural per-(label, topic)
aggregation to reuse cleanly, and P6-Q1's own rationale (a short bulk
run's Markdown report is the durable record; live Prometheus is framed
for "streaming fleets and repeated bulk runs over time") already covers
the bulk case without a live gauge. **Slice 3's dashboard must not show a
lag panel for node loaders or bulk runs — there is no series for them.**

`_refresh_consumer_lag()` (`relationship_bulk_monitor.py`) is called from
`supervise_stream`'s existing tick. It throttles itself to once per
`_LAG_REFRESH_INTERVAL_SECONDS` (10s) on `self._clock()`, independent of
`self._interval` (which defaults to 0.25s in production — far too fast
for a fleet-wide admin RPC every tick; see the review finding S8-1
below). Each RPC (`_watermarks_low_high`, a lag-path-only sibling of the
shared `_watermarks()` that keeps the *low* watermark instead of
discarding it, and `_commits(timeout=...)`) uses a short explicit
`_LAG_REFRESH_TIMEOUT_SECONDS` (2s) rather than `_request_timeout()`'s
up-to-10s. A partition with nothing committed falls back to its *low*
watermark (Kafka's own lag convention), not 0 — an absolute-zero fallback
would wildly overstate lag on any topic aged past retention. Lag is
summed per `(edge_type, topic)` across every partition — never one gauge
per partition. The whole computation is wrapped in one broad
`except Exception`, logging a WARNING and leaving the gauge at its stale
value on failure, so a transient admin-API failure can never break
stream supervision (a load-bearing loop that also detects crashed
loaders and a lost clock lease).

**Planner progress** — `ParallelEdgePlanner._progress()` (the existing
heartbeat read function, called by `_Heartbeat` at most once per
`interval=1.0s`, never per row) advances `planner_rows_read_total`/
`planner_chunks_published_total` by the delta since the last read
(totals are running absolutes across workers; a Counter only supports
`.inc()`). `kind="cli"` only. `close()` also calls `_progress()` once,
*after* the heartbeat thread has joined (never concurrently with a beat
— avoids double-counting) and *before* `self._workers` is cleared — see
review finding S8-3.

## Reviewer findings and resolutions

- **S8-1** (blocking): the lag refresh originally ran on every
  supervision tick (0.25s in production), not "several seconds" as first
  claimed — ~150 admin RPCs/second for a 3-edge-type × 12-partition
  fleet, and a synchronous stall risk for the same loop's crash/lease-
  health checks. Fixed with the independent 10s throttle and 2s RPC
  timeouts described above; added a fake-clock test proving N ticks
  produce `ceil(elapsed / 10s)` refreshes, not N.
- **S8-2** (should-fix, treated as blocking-equivalent and fixed): a
  missing commit was originally treated as committed=0, which would
  report lag in the millions on any topic aged past retention. Fixed:
  falls back to the low watermark instead (kept via the new
  `_watermarks_low_high` helper, since the shared `_watermarks()`'s
  return contract is used by other callers and left untouched).
- **S8-3** (should-fix, fixed): `ParallelEdgePlanner.close()` stopped the
  heartbeat without a final progress read, so up to ~1s of trailing
  rows/chunks was never counted — a plan finishing in under a heartbeat
  interval would read 0 forever. Fixed with one final `self._progress()`
  call, ordered after the heartbeat joins and before `self._workers` is
  cleared.
- **S8-4** (non-blocking, noted): the label-policy docstring's "`-` for
  cli" wording applies to the CLI's own process-info triad and kind-only
  series, not to the per-entity `consumer_lag_records` series the CLI
  reports on behalf of loaders (`name=edge_type` there, per the
  catalogue's own row).
- **S8-5** (non-blocking, noted): `planner_chunks_published_total`
  counts chunks *enqueued* for production, not broker-acknowledged —
  correct for a progress counter, noted for the HELP text.

## Definition of Done

- [x] One CLI metrics server for the process lifetime, `127.0.0.1` with
      ephemeral fallback, stopped and snapshotted after `handle_start()`
      returns on every tested path.
- [x] `graph_loader_consumer_lag_records` live for relationship stream
      fleets, throttled independently of the supervision tick, correct
      under both contention and no-commit/retention edge cases.
- [x] `graph_loader_planner_rows_read_total`/`planner_chunks_published_total`
      advance at the existing heartbeat site, capture the trailing
      progress on close, and read a real 0 for the producer-side plan
      path.
- Reviewed and merged in this story's own commit.
