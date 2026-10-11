# P6-S3 Story 5: `promtool`/Dashboard Validation + Scratch-Run Proof — Implementation Plan

## Files touched
- New: `scripts/validate_monitoring.py`
- New: `tests/test_validate_monitoring.py` (explicit permitted exception to
  this slice's "no `tests/` edits" scope confinement, per the First
  Mate's binding brief — tests only this slice's own dependency-free
  script, never imports `src/`)

## Corrections to `stories.md`'s literal text
1. **`promtool` invocation.** `stories.md`'s draft command
   (`docker run --rm -v ... prom/prometheus:v2.55.1 promtool check config ...`)
   fails: `prom/prometheus`'s image `ENTRYPOINT` is `/bin/prometheus`
   itself, not a shell, so `promtool` as a plain argument becomes an
   argument *to* `prometheus` ("Error parsing command line arguments:
   unexpected promtool"). The correct invocation is
   `docker run --rm --entrypoint promtool -v ... prom/prometheus:v2.55.1 check config ...`.
   Verified empirically; fixed in this script's own docstring usage
   comment and in Story 6's Makefile targets.
2. **Metric-name-token regex.** `stories.md`'s draft regex
   (`graph_loader_[a-z_]+`) does not match digits, so it fails on
   `graph_loader_neo4j_write_duration_seconds` (the "4" in "neo4j").
   Corrected to `graph_loader_[a-z0-9_]+`.
3. **`promtool test rules` unit-test file location.** Story 2's
   `alert_tests/concurrency_test.yml` deliberately lives outside
   `monitoring/prometheus/alerts/` (see Story 2's plan) — this story's
   Makefile/validate commands reference it at its actual path.

## Implementation
`scripts/validate_monitoring.py` (stdlib-only: `json`, `re`, `glob`,
`pathlib`, `sys`):
1. Hardcoded copy of `feat/p6-s2-metrics:src/utils/metrics.py`'s
   `EXPECTED_METRIC_FAMILIES` (22 entries, transcribed verbatim).
2. Parses every `*.json` under `monitoring/grafana/dashboards/`: valid
   JSON, nonblank `title`, ≥1 non-row panel, every panel has ≥1 target,
   every target's `expr` is a nonblank string.
3. Line-oriented `expr:` extraction from `monitoring/prometheus/alerts/*.yml`
   (deliberately not a full YAML rule parser — `promtool check rules` is
   the source of truth for full semantic validation; this script only
   has to catch a metric-name typo).
4. Extracts every `graph_loader_[a-z0-9_]+` token from every expr found in
   both sources, strips at most one of `_total`/`_bucket`/`_sum`/`_count`/
   `_info`, and asserts membership in the hardcoded catalogue.
5. Exits nonzero with a specific, actionable message on any failure; exits
   0 and prints `"N dashboards, M panels, K alert exprs, J unique metric
   names, all known"` otherwise.

`tests/test_validate_monitoring.py`: unit tests for `_family_name`
suffix-stripping and `_check_metric_names`, plus an end-to-end subprocess
run against this repo's real `monitoring/` tree, plus the "prove the
guard actually guards" proof (Story 5's own AC) made into a **permanent**
regression test — copies the real tree to a `tmp_path`, corrupts one
metric name, asserts exit code 1, then asserts the unmodified copy exits
0 — rather than a one-off manual demonstration that could bit-rot.

## Manual proof performed once (recorded, not just claimed)
- `python3 scripts/validate_monitoring.py` against the real tree:
  `1 dashboard(s), 15 panel(s), 13 alert expr(s), 19 unique metric name(s), all known`
  — exit 0.
- Injected `graph_loader_records_consumed_total` ->
  `graph_loader_recrods_consumed_total` in the dashboard JSON: script
  exited 1 with `unknown metric name 'graph_loader_recrods_consumed_total'`.
  Reverted; re-ran; exit 0 again. (Also encoded as a permanent test,
  above.)
- `docker run --rm --entrypoint promtool ... check config` /
  `check rules` (all 4 files) / `test rules alert_tests/concurrency_test.yml`
  — all exit 0.
- `python3 -m pytest tests -m "not integration and not smoke"` (full
  existing suite, in a `/tmp` venv, `requirements.txt` +
  `requirements-dev.txt` installed via `PIP_CACHE_DIR=/mnt/graph-loader/.pip-cache`):
  1044 passed, 17 deselected — confirms the new test file doesn't break
  anything else and collects cleanly.
- `ruff check src tests examples`: All checks passed.

## Scratch-run proof (8-step, per stories.md)
Deferred to this slice's finish step: requires the compose profile (Story
3) and Grafana dashboard (Story 4) to exist first, plus at least one
loader container. Executed against a **dedicated scratch stack**
(`scratch-p6s3-*` containers/network, not `graph-loader-kafka`/
`graph-loader-neo4j`), per the First Mate's binding brief. Findings
recorded in `plan/v6/completion_slice-3.md`.

## Commit
One commit: `feat(monitoring): validation script + tests (P6-S3 Story 5)`.
