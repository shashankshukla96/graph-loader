# P6-S3 Story 2: Example Alert Rules — Implementation Plan

## Files touched
- New: `monitoring/prometheus/alerts/dlq.yml`
- New: `monitoring/prometheus/alerts/progress.yml`
- New: `monitoring/prometheus/alerts/lag_and_errors.yml`
- New: `monitoring/prometheus/alerts/concurrency.yml`
- New: `monitoring/prometheus/alert_tests/concurrency_test.yml` (promtool
  unit test file for the one alert with a documented, verifiable firing
  condition — added beyond `stories.md`'s literal 5-file list because the
  story's own AC requires `promtool test rules` proof for
  `GraphLoaderPlannedModeDeadlockObserved`; this is additive, not a
  deviation. Deliberately **not** under `monitoring/prometheus/alerts/`:
  that directory's glob is both `prometheus.yml`'s `rule_files:` target and
  Story 5/6's `promtool check rules alerts/*.yml` validation target, and a
  promtool *test* file is not a valid rule-group file — colocating it
  would break both.)

## Reviewer-driven fixes (applied before commit)
The dedicated reviewer (Opus) found four should-fix issues via a scratch
`promtool test rules` proof against inputs shaped to match how
`feat/p6-s2-metrics` actually emits these series:
1. `GraphLoaderNoRecordsConsumed` (`progress.yml`): the original
   `and on(kind, name)` join collapsed distinct replicas/runs, never
   matched planned-writer (whose `process_up` carries `name="-"` while
   `records_consumed` carries `name=<edge_type>`), and could never fire
   for a loader that has never written a first record (no series yet,
   `and` silently drops an absent right side). Replaced with an
   `unless on(instance)` join against `sum by (instance) (increase(...))`.
2. `GraphLoaderDlqRoutingDetected`/`Spike` (`dlq.yml`): plain `increase()`
   misses a `failure_class` series' very first sample (Phase 5 creates
   `missing_endpoint`/`ambiguous_endpoint` series lazily, already nonzero,
   with no earlier sample to diff against). Added an `or (... unless ...
   offset 15m)` clause to also catch a newly-appeared nonzero series.
3. `GraphLoaderConsumerLagHigh`/`Critical`/`NotDraining`
   (`lag_and_errors.yml`): `graph_loader_consumer_lag_records` is only
   emitted by the host-side CLI process (`kind="cli"`), which binds
   `127.0.0.1` on the host, not a container on `graph-loader-net` —
   Docker SD never finds it, so these three rules are dead as shipped in
   this slice's default stack. Kept (per the "registered now" convention)
   with an added group-level comment and per-rule `description:`
   explaining the gap; flagged for `docs/MONITORING.md` Known Limitations
   and for the P6-S2/orchestrator team as a cross-slice gap.
4. `GraphLoaderPlanResourceWaitHigh` (`concurrency.yml`): `sum by (le)`
   merged every concurrent run's planned-dispatcher into one p95. Changed
   to `sum by (le, run_id)` so contention is attributed per run.

Plus three nits: `GraphLoaderPlannedRunStalled` switched from
`increase()` to `changes()` (correct semantics on a monotonic gauge) and
its wording corrected to "~20m" (10m window + `for: 10m`);
`GraphLoaderPlannedModeDeadlockObserved` wrapped in `max_over_time(...[1h])`
so it does not auto-resolve when a ~60s bulk planned-writer container's
series goes stale; `GraphLoaderRetryRateHigh` grouped by `run_id` too.

## Contract cross-check
Every metric name below was verified verbatim against
`feat/p6-s2-metrics:src/utils/metrics.py`'s `EXPECTED_METRIC_FAMILIES` and
each metric's actual label list (not guessed):
- `graph_loader_dlq_routed_total{kind,name,failure_class}` — exists.
- `graph_loader_parked_total{kind,name}` — exists.
- `graph_loader_plan_chunks_committed{kind}` / `graph_loader_plan_chunks_planned{kind}` —
  exist, **kind-only label set** (no `run_id`, no `name`) — corrected the
  `progress.yml` rule below vs. `stories.md`'s literal text, which implied
  a `run_id` label existed on `graph_loader_plan_chunks_committed`
  (`{{ $labels.run_id }}` in its annotation): that label does not exist on
  this metric (it is a Prometheus *target* label added by Story 1's
  relabeling, present on every series scraped from that target including
  this one — actually confirmed: relabeled target labels ARE attached to
  every metric scraped from that target, so `run_id` IS present as long as
  it came from the target's Docker label, not from the metric's own
  `.labels(...)` call. Keeping `{{ $labels.run_id }}` in the annotation is
  correct after all, since Prometheus attaches every target label,
  including relabeled ones, to every scraped sample.).
- `graph_loader_process_up{kind,name}` — exists.
- `graph_loader_records_consumed_total{kind,name,label_or_type}` — exists.
- `graph_loader_consumer_lag_records{kind,name,topic}` — exists.
- `graph_loader_retry_total{kind,name,reason}` — exists.
- `graph_loader_deadlocks_recovered_total{kind,name}` — exists; `kind`'s
  closed enum includes `planned-writer` (verified in `PROCESS_KINDS`).
- `graph_loader_lease_waiters{kind}` — exists, kind-only.
- `graph_loader_lease_wait_timeouts_total{kind}` — exists, kind-only.
- `graph_loader_plan_resource_wait_seconds{kind}` (histogram) — exists,
  kind-only, **not suffixed `_bucket` in the catalogue's family list**
  (Histogram objects auto-generate `_bucket`/`_sum`/`_count`; the alert's
  `expr` correctly references `..._bucket` explicitly).

## DLQ backlog TODO (binding orchestrator constraint)
`dlq.yml`'s `GraphLoaderDlqRoutingDetected`/`GraphLoaderParkedLotGrowing`
descriptions already state, verbatim from `stories.md`, that this is a
growth-rate proxy, not a depth gauge. Per the First Mate's binding brief
(orchestrator constraint 4), add an explicit `# TODO(P5-S4):` comment above
the DLQ backlog-proxy rules pointing at the true gauge P5-S4 will add.

## Implementation
Write the five YAML files essentially as drafted in `stories.md` Story 2,
each prefixed with the verbatim "STARTING POINT, NOT AN SLO" header
comment, with the `progress.yml` label-shape note above applied (no text
change needed — analysis showed the original draft is correct), and the
`# TODO(P5-S4)` comment added to `dlq.yml`.

Add `monitoring/prometheus/alerts/tests.yml`, a `promtool test rules` unit
test file exercising `GraphLoaderPlannedModeDeadlockObserved` per the
story's own AC: one test group asserting the alert fires 1 minute after a
synthetic `graph_loader_deadlocks_recovered_total{kind="planned-writer",
name="ACTED_IN"} 1` series appears, and does not fire against an all-zero
input series.

## Validation
- `docker run --rm --entrypoint promtool -v "$(pwd)/monitoring/prometheus:/etc/prometheus:ro" prom/prometheus:v2.55.1 check rules /etc/prometheus/alerts/*.yml`
- `docker run --rm --entrypoint promtool -v "$(pwd)/monitoring/prometheus:/etc/prometheus:ro" prom/prometheus:v2.55.1 test rules /etc/prometheus/alerts/tests.yml`
- Re-run `promtool check config` on `prometheus.yml` now that `rule_files`
  resolves to real files.

## Commit
One commit: `feat(monitoring): example Prometheus alert rules (Story 2)`.
