# P6-S3 Story 4: Grafana Provisioning and Starter Dashboard — Implementation Plan

## Files touched
- New: `monitoring/grafana/provisioning/datasources/prometheus.yml`
- New: `monitoring/grafana/provisioning/dashboards/graph-loader.yml`
- New: `monitoring/grafana/dashboards/graph-loader-overview.json`

## Contract cross-check
Every metric name/label combination in the dashboard JSON was checked
against `feat/p6-s2-metrics:src/utils/metrics.py`'s actual `Counter`/
`Gauge`/`Histogram`/`Info` constructor label lists (not just
`EXPECTED_METRIC_FAMILIES`'s family-name set), since a metric can exist
with a *different* label set than the one a query assumes:
- `graph_loader_records_consumed_total{kind,name,label_or_type}` — query
  aggregates by `kind,name` (drops `label_or_type`); correct, since a
  starter throughput panel doesn't need the third dimension.
- `graph_loader_neo4j_write_duration_seconds{kind,name,stage}` (histogram)
  — `histogram_quantile` aggregates `sum by (le,kind,name)`, dropping
  `stage`; correct, `le` is required and present since it's the bucket
  series.
- `graph_loader_consumer_lag_records{kind,name,topic}` — used directly,
  all three labels available for legend.
- `graph_loader_plan_chunks_committed{kind}` / `graph_loader_plan_chunks_planned{kind}`
  / `graph_loader_plan_active_writers{kind}` — **kind-only**, no `name`.
  Queries filter by `kind`/`run_id` only (no `name=~"$name"` filter on
  these three, since the label does not exist and a nonexistent-label
  matcher, while harmless in PromQL — it matches the implicit empty
  string — would be misleading to include).
- `graph_loader_lease_grants_total{kind}` / `graph_loader_lease_denials_total{kind}`
  / `graph_loader_lease_waiters{kind}` / `graph_loader_lease_held_resources{kind}`
  — kind-only, same treatment.
- `graph_loader_lease_wait_seconds{kind,name}` (histogram) — kind+name,
  `sum by (le,kind,name)`.
- `graph_loader_plan_resource_wait_seconds{kind}` (histogram) — **kind-only**,
  `sum by (le,kind)` (stories.md's own table showed this histogram_quantile
  with no `by()` clause at all — corrected here the same way the alert
  rule's own `expr` already does, per Story 2's `concurrency.yml`).
- `graph_loader_deadlocks_recovered_total{kind,name}` — `sum by (kind,name)`.
- `graph_loader_dlq_routed_total{kind,name,failure_class}` — `sum by
  (failure_class)` per the story's own draft (a deliberately coarse
  starter aggregation across kind/name; an operator can add them back in
  Grafana's query editor).
- `graph_loader_parked_total{kind,name}` — used directly.
- `graph_loader_retry_total{kind,name,reason}` — `sum by (reason)`, same
  coarse-aggregation choice as DLQ.
- `graph_loader_process_up{kind,name}` / `graph_loader_process_info{kind,name}`
  (Info metric; prometheus_client exposes it as `<name>_info` with value 1
  and every Info label attached — confirmed the exposed series name is
  `graph_loader_process_info`, matching `EXPECTED_METRIC_FAMILIES`'s
  `graph_loader_process` entry with the `_info` suffix stripped, per that
  constant's own docstring).

## Template variables
`kind`, `name`, `run_id` — each `label_values(graph_loader_process_up,
<label>)`, `includeAll: true`, `multi: true`, default `$__all`. Every
per-entity panel filters `kind=~"$kind"` (+ `name=~"$name"` where the
label exists) `and run_id=~"$run_id"`; kind-only metrics omit the `name`
filter since the label doesn't exist on that series (Prometheus would
still not error on a nonexistent-label matcher, but including it would
misleadingly imply the label is meaningful there).

## "Zero data" and "no query error" mechanism
- Every panel's `fieldConfig.defaults.noValue` is `"No data"`.
- No panel wraps a query in a function that errors on an empty vector;
  `histogram_quantile` over an absent `_bucket` family returns an empty
  vector, which Grafana already renders as "No data", not an error.
- Template variables default to `$__all` (interpreted by Grafana/Prometheus
  as `.*`), which resolves to a valid (if unmatched) regex even when
  `label_values(...)` itself returns zero results before any container
  has ever run — verified in Story 5's scratch-run proof, not asserted
  here without evidence.

## Validation
- `python3 -m json.tool` (or Story 5's own `scripts/validate_monitoring.py`
  once it exists) on `graph-loader-overview.json` — confirmed valid JSON,
  nonblank `title`, 15 non-row panels, 20 panel `targets[].expr` strings,
  all with a recognized `graph_loader_[a-z_]+` metric-name prefix.
- Full render-and-populate proof deferred to Story 5's scratch run (needs
  live Prometheus + Grafana + at least one loader container).

## Commit
One commit: `feat(monitoring): Grafana provisioning and starter dashboard (Story 4)`.
