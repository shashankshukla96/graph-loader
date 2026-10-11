# Phase 6 / Slice 3 Completion — Metrics Delivery: Discovery, Starter Dashboard, Example Alerts

## Delivered scope vs. Definition of Done

All 8 stories in `plan/v6/slice-3/stories.md` are implemented, reviewed by
a dedicated Opus subagent, fixed, and merged onto `feat/p6-s3-monitoring`
(branched from `feat/streaming-observability`, then merged forward once
more mid-slice when P6-S2 landed there for real). Story 8 (the one-line
`docs/USAGE.md` link) is deferred per the First Mate's binding brief —
see `plan/v6/slice-3/deferred_usage_link.md` for the exact intended edit.

- [x] Prometheus discovers a loader container on `graph-loader-net` by
      label, scrapes within a few seconds, and shows `up` with no
      manually maintained target list — the mechanism was proven live
      twice: once with one real node-loader container
      (`graph-loader-node:p6s3`, built from the merged P6-S2 code), and
      once with fake node/lease-server exporters carrying the same
      label contract. The other process kinds (`edge`, `planned-writer`,
      `planned-dispatcher`, `clock`) were not run live in this slice's
      own validation; they are covered by the same shared label contract
      (`docker_service.py`'s `_metrics_labels`), not exercised
      end-to-end. See "Not proven" below.
- [ ] Two concurrent runs of the same `node_label` produce two
      distinguishable targets via the `run_id` target label — **verified
      by inspection, not run live**: `run_id` and `instance` are
      per-container target labels applied by relabeling regardless of
      how many containers share a `node_label`, and the real node loader
      in Round 2 did carry a correct, distinct `run_id`, but no test in
      this slice actually launched two concurrent runs of the same label
      side by side.
- [x] Neo4j/Kafka/Kafka UI/Prometheus/Grafana never appear as
      `graph-loader` job targets.
- [x] A container produces exactly one scrape target (9464), never a
      second control-port target — proven live, though not with the real
      lease server: the real `run_bucket_lease_server` only ever
      publishes the metrics port (`docker_service.py` never exposes
      7700), so this was proven with a fake exporter shaped like the
      *planned dispatcher* instead (exposing both 7700 and 9464), which
      is the container kind that actually has this two-port shape today.
- [x] `promtool check config`/`check rules`/`test rules` all exit 0.
- [x] `docker compose up` (no profile flag) is unaffected — verified via
      `docker compose -f docker-compose.yaml config --services`.
- [x] Four example alert rule groups (`dlq.yml`, `progress.yml`,
      `lag_and_errors.yml`, `concurrency.yml` — `stories.md`'s own DoD
      text says "five rule files", which is a miscount in the planning
      doc, the same kind of slip as its "nine rows"; see Deviation 14),
      each headed "STARTING POINT, NOT AN SLO" verbatim, covering DLQ
      backlog (growth-rate proxy, `TODO(P5-S4)` pointing at the real
      gauge), stuck planned-mode progress, no-records-consumed, consumer
      lag, retry rate, and concurrency canaries (planned-mode deadlock,
      lease waiters/timeouts, plan resource wait).
- [x] Grafana auto-provisions the Prometheus datasource and one starter
      dashboard (10 rows / 15 panels / 20 queries) with no manual step —
      verified via `/api/datasources` and `/api/search`, and every panel's
      query confirmed to return `"status":"success"` (with data in Round
      2, with an empty result set pre-Round-2) through Grafana's own
      datasource-proxy API. **Not verified in a browser**: Story 4's AC
      ("no red panel-error indicator anywhere on the page") is a visual
      check this slice's validation did not perform; query-level success
      is strong evidence but not the same claim.
- [x] `scripts/validate_monitoring.py` (stdlib-only) + `tests/test_validate_monitoring.py`
      (permitted `tests/` exception) validate dashboard/alert metric names
      against a hardcoded, type-aware copy of P6-S2's catalogue; the
      "prove the guard actually guards" typo-injection proof is a
      permanent regression test, not a one-off.
- [x] `make monitoring-up`/`monitoring-down`/`monitoring-validate`,
      Makefile changes are a pure append (verified via `git diff`).
- [x] `docs/MONITORING.md` covers discovery, cardinality, resource
      placement, alerts, and every known limitation found during review.

### Story-by-story delivery

| # | Story | Commit(s) |
| - | --- | --- |
| 1 | Prometheus Docker SD config | `aead19d` |
| 2 | Example alert rules | `dd79c19` |
| 3 | `docker-compose.monitoring.yml` | `09b0159` |
| 4 | Grafana provisioning + starter dashboard | `5b461d3` |
| 5 | `validate_monitoring.py` + tests | `71f518d`, `1744b67`, `2fca80a`, `cab1d83` |
| 6 | Makefile targets | `3f71629`, `cab1d83` |
| 7 | `docs/MONITORING.md` | `f6608a0` |
| 8 | Deferred `docs/USAGE.md` link | `c7acad7` (records the intended edit; does not touch `docs/USAGE.md` itself) |

Branch point: `1feda68` (`feat/p6-s3-monitoring` branched from
`feat/streaming-observability`). Merge: `91a3ec1` merges
`feat/streaming-observability` at `8aeca9b` (which itself carries
P6-S2's merge at `eee6f36`) into `feat/p6-s3-monitoring` — clean, zero
conflicts, confirming the file-overlap-risk assessment in `stories.md`
held in practice.

## Files changed

New files only, plus one append-only edit to `Makefile` (per the binding
scope confinement). `git diff --stat 8aeca9b..HEAD` — diffed against
`8aeca9b` (the `feat/streaming-observability` tip merged in at `91a3ec1`,
not the original branch point `1feda68`) specifically to exclude
everything P6-S2 and other concurrent slices landed on
`feat/streaming-observability`, isolating this slice's own changes:

```
Makefile                                            |  43 +++ (append only)
docker-compose.monitoring.yml                       | 144 ++
docs/MONITORING.md                                  | 386 +++
monitoring/grafana/dashboards/graph-loader-overview.json | 222 +++
monitoring/grafana/provisioning/dashboards/graph-loader.yml |  11 +
monitoring/grafana/provisioning/datasources/prometheus.yml  |  18 +
monitoring/prometheus/alert_tests/concurrency_test.yml      |  43 +++
monitoring/prometheus/alert_tests/dlq_test.yml              |  48 +++
monitoring/prometheus/alert_tests/progress_test.yml         |  46 +++
monitoring/prometheus/alerts/concurrency.yml        |  50 ++
monitoring/prometheus/alerts/dlq.yml                |  71 +++
monitoring/prometheus/alerts/lag_and_errors.yml     |  60 ++
monitoring/prometheus/alerts/progress.yml           |  59 ++
monitoring/prometheus/prometheus.yml                | 131 ++
plan/v6/slice-3/deferred_usage_link.md              |  29 ++
plan/v6/slice-3/impl_plan_story_{1..6}.md           | (new, planning)
plan/v6/completion_slice-3.md                       | (this file, not yet committed at diff time)
scripts/validate_monitoring.py                      | 304 ++
tests/test_validate_monitoring.py                   | 312 ++

23 files changed, 2445 insertions(+)
```

`src/`, `docker-compose.yaml`, `requirements.txt`, and `docs/USAGE.md`
are untouched, as required.

## Deviations from the plan (all flagged to the reviewer, all applied)

1. **`promtool` invocation.** `stories.md`'s literal
   `docker run ... prom/prometheus:v2.55.1 promtool ...` fails
   ("Error parsing command line arguments: unexpected promtool") — the
   image's `ENTRYPOINT` is the binary itself. Every invocation in this
   slice uses `--entrypoint promtool`.
2. **`promtool` does not glob.** `check rules alerts/*.yml` as a single
   argument fails ("path ... does not exist"); the Makefile expands the
   glob in its own host shell instead, one `docker run` per file.
3. **Metric-name-token regex.** `stories.md`'s literal
   `graph_loader_[a-z_]+` never matches `graph_loader_neo4j_write_duration_seconds`
   (the digit in "neo4j"); corrected to `graph_loader_[a-z0-9_]+`.
4. **`docker compose ... config` does not fail on a missing external
   network** (verified empirically with a disposable throwaway compose
   file); only `up` does. `stories.md`'s Story 3 AC is retargeted to `up`.
5. **`honor_labels: true`** added to the `graph-loader` scrape job
   (not in `stories.md`'s draft): every `graph_loader_*` metric already
   carries its own `kind` label, and without `honor_labels`, Prometheus's
   default collision rule renames the *scraped* label to `exported_kind`
   and keeps the *target* label instead — harmless today (the two values
   agree) but would silently paper over a future divergence.
6. **Two real blockers found and fixed via a scratch reproduction**,
   not caught by inspection alone:
   - Prometheus (`USER nobody`) cannot read `/var/run/docker.sock`
     without `group_add` set to the socket's actual gid; `make
     monitoring-up`/`monitoring-down` compute and export
     `DOCKER_GID=$(stat -c %g /var/run/docker.sock)`, and the compose
     file fails loudly (`${DOCKER_GID:?...}`) if it's unset.
   - Prometheus/Grafana's bind-mounted data directories are not writable
     by their containers' default uids (65534/472); `make monitoring-up`
     `chown`s them after `mkdir -p`, sourcing `.env` first so an operator
     who only sets `GRAPH_LOADER_DATA_DIR` in `.env` (a
     `docs/USAGE.md`-documented option) gets the *same* directory chowned
     that Compose itself mounts.
7. **`--web.enable-lifecycle` dropped.** Combined with a `0.0.0.0`-bound
   Prometheus, this would be an unauthenticated `POST /-/quit`/`/-/reload`
   kill switch; nothing in this slice's own tooling needs it.
8. **`monitoring-down` is scoped `stop`+`rm`, never bare `down`.** With
   both `-f` files loaded, Compose treats `neo4j`/`kafka`/`kafka-ui` as
   part of the same project — an unscoped `down` would have stopped/
   removed those too and attempted to tear down `graph-loader-net`, which
   this profile declares external and does not own. (`stories.md`'s own
   Story 5 scratch-run step 8 has this exact hazard; not reproduced here
   or in `docs/MONITORING.md`.)
9. **Grafana dashboards mount moved** from `/var/lib/grafana/dashboards`
   (nested inside the data bind mount, which Compose then half-creates as
   an empty directory) to `/etc/grafana/dashboards`.
10. **Dashboard fixes beyond `stories.md`'s literal table**: `allValue:
    ".*"` on every template variable (without it, "All" expands to a
    regex of only-discovered values, silently excluding any node/edge
    loader launched without `--run-id`); the "Process health" panel
    queries Prometheus's own `up{job="graph-loader",...}` instead of
    `graph_loader_process_up` (the latter is only ever set to 1 and can
    never show a crashed process as down); the Deadlocks and
    DLQ-by-`failure_class` panels also plot the raw cumulative counter
    alongside `rate()`/`increase()`, since a `failure_class` series' very
    first sample can already be nonzero with no earlier sample to diff
    against; the plan-resource-wait panel groups by `run_id`, not `kind`,
    so concurrent runs don't merge into one p95.
11. **Alert-rule fixes beyond `stories.md`'s literal exprs** (all proven
    with scratch `promtool test rules` runs before and after):
    `GraphLoaderNoRecordsConsumed` rewritten from an `and on(kind, name)`
    join (silently never fired for `planned-writer`, and never fired for
    a loader that had not written a first record at all) to an
    `unless on(instance)` join; `GraphLoaderDlqRoutingDetected`/`Spike`
    gained an `or (... unless ... offset 15m)` clause so a lazily-created
    DLQ `failure_class` series is caught on its first nonzero sample;
    `GraphLoaderPlanResourceWaitHigh` groups by `run_id`; the three
    consumer-lag rules are annotated as dead-by-default (see Known
    Limitations); `GraphLoaderPlannedRunStalled` uses `changes()` instead
    of `increase()`; `GraphLoaderPlannedModeDeadlockObserved` wrapped in
    `max_over_time(...[1h])` so a ~60s bulk container's deadlock doesn't
    auto-resolve when its series goes stale; `GraphLoaderRetryRateHigh`
    grouped by `run_id` too.
12. **`validate_monitoring.py`'s metric-name check rewritten to be
    type-aware**, not suffix-blind: the original "strip any known
    suffix" approach wrongly accepted dead queries a Counter/Gauge/
    Histogram/Info would never actually expose (e.g.
    `graph_loader_process_up_total` — a Gauge has no `_total`;
    `graph_loader_records_consumed` with no suffix at all — a Counter is
    never exposed bare). Replaced with `QUERYABLE_METRIC_NAMES`, derived
    from a hardcoded `{family: metric_type}` table, that encodes exactly
    which suffixed name(s) each type actually exposes. Also extended to
    check template-variable `label_values(...)` queries and to descend
    into a collapsed row panel's nested `panels`.
13. **`tests/test_validate_monitoring.py`'s catalogue-drift test**
    (`pytest.importorskip("src.utils.metrics")`) now runs for real
    (P6-S2 merged mid-slice) and compares both family names and metric
    types against the live registry — 27/27 tests pass.
14. **Row/file counts in `stories.md`'s own text are off by one, twice.**
    Story 4's AC text says "nine rows"; the table above it lists ten
    (Throughput, Write latency, Consumer lag, Plan progress, Lease
    server, Lease wait/lock waits, Deadlocks, DLQ, Errors, Process
    health) — implemented all ten. Story 2's DoD text says "five rule
    files"; there are four (`dlq.yml`, `progress.yml`,
    `lag_and_errors.yml`, `concurrency.yml`) — implemented all four,
    matching the story's own five-rule-group table content, just not
    its file-count sentence.
15. **Two more `promtool` unit tests added at completion-review time**
    (`monitoring/prometheus/alert_tests/progress_test.yml`,
    `dlq_test.yml`), beyond `stories.md`'s own AC (which only required a
    unit-test proof for `GraphLoaderPlannedModeDeadlockObserved`):
    `GraphLoaderNoRecordsConsumed`'s `unless on(instance)` rewrite and
    `GraphLoaderDlqRoutingDetected`'s `or (... unless ... offset 15m)`
    addition were the two riskiest rewritten exprs in this slice and had
    previously been proven only by the reviewer's own uncommitted
    scratch `promtool test rules` runs — now permanent regression tests.
    `progress_test.yml` gained two more test groups in a follow-up
    commit (optional nit from the reviewer's final pass), covering the
    other two original `and on(kind,name)` bugs the rewrite fixed:
    replica cross-attribution (a busy replica no longer flags an idle
    sibling sharing the same `kind`+`name`) and the planned-writer
    `name="-"` vs. `name=<edge_type>` mismatch (a busy planned-writer no
    longer fails to match its own `records_consumed` series). All four
    `GraphLoaderNoRecordsConsumed` test groups pass.

## Known limitations

`docs/MONITORING.md` §7 is the operator-facing, maintained copy of this
list — consult it directly rather than this section for anything not
delivery-specific, so the two can't drift apart. It additionally covers
three operator-relevant items not repeated here: containers launched on
a network other than `graph-loader-net` are silently dropped with no
error anywhere; this stack assumes a trusted local/private network
(Prometheus has no auth; Grafana ships `admin`/`admin` plus anonymous
Viewer, both bound `0.0.0.0` by default); and `make monitoring-down`
must run before a base `docker compose down`, or Prometheus/Grafana
become orphan containers blocking network teardown.

- **No Pushgateway** (orchestrator P6-Q1 decision) — bulk-run durability
  for a container shorter than one scrape interval is P6-S2 Story 9's
  exit-time `.prom` snapshot, not a Prometheus series at all.
- **No true Kafka topic-depth metric** for DLQ/parked topics — the DLQ
  alert rules and dashboard panel are explicit growth-rate proxies over
  routed/parked *counts*, each carrying a `TODO(P5-S4)` pointing at the
  true backlog gauge that slice will add.
- **`graph_loader_consumer_lag_records` and the planner series
  (`graph_loader_planner_rows_read_total`,
  `graph_loader_planner_chunks_published_total`) are CLI-only and never
  scraped by this stack by default.** Confirmed against the merged
  `relationship_bulk_monitor.py`/`cli.py`: these are emitted only by the
  host-side CLI process, bound to `127.0.0.1` on the host — unreachable
  from a bridge-networked Prometheus container. The three consumer-lag
  alert rules and the dashboard's consumer-lag panel are annotated
  dead-by-default for this reason.
- **A planned dispatcher launched with a non-default `--port`** (not
  done anywhere in this codebase today) leaves its control port
  undropped by Story 1's literal-value relabel rule, producing an extra,
  always-`down` target.
- **These Makefile targets are Linux-host-only** (`stat -c` is GNU
  coreutils syntax; macOS needs `stat -f %g`, and Docker Desktop's host
  socket gid is meaningless there regardless).
- **`.env` is sourced as a shell script** by `make monitoring-up`, not
  parsed with Compose's own `.env` syntax — a value containing
  shell-special characters could misbehave. Documented in
  `docs/MONITORING.md` §5; not fixed, since `GRAPH_LOADER_DATA_DIR`
  itself is not expected to need such characters.
- **Sub-~5-second containers may never be scraped live** — Story 1's own
  stated boundary (`refresh_interval + scrape_interval`); their only
  durable record is P6-S2's exit-time snapshot.
- **`MONITORING_CPUSET`'s default (28-31) deliberately overlaps** the top
  of `docs/USAGE.md`'s own documented `LOADER_CPUSET=12-31` example — a
  named tradeoff for this stack's stated use case (a human watching a
  bounded run), not an oversight.
- **`/var/run/docker.sock` is root-equivalent host access**, not merely
  a read-only capability, even mounted `:ro`. Scoped to "trusted local
  host only"; a socket-proxy is the documented hardening option, not
  built here.

## Test evidence

Environment: a `/tmp` venv (never the repo's own `.venv`), Python
3.12.14, pytest 9.1.1, `requirements.txt` + `requirements-dev.txt`
installed via `PIP_CACHE_DIR=/mnt/graph-loader/.pip-cache`.

### Full unit suite

```
/tmp/p6s3-venv/bin/python3 -m pytest tests -m "not integration and not smoke" -q
...
1168 passed, 18 deselected in 23.09s
```

(1044 before the P6-S2 merge; +124 from that merge; no regressions.)

### This slice's own test file

```
/tmp/p6s3-venv/bin/python3 -m pytest tests/test_validate_monitoring.py -v
...
27 passed in 0.16s
```

Includes the catalogue-drift test against the real, merged
`src/utils/metrics.py` (no longer skipped) and the permanent "guard
actually guards" typo-injection regression tests (dashboard JSON, a
folded `expr: >` alert rule, and a single-line `expr:` alert rule).

### Lint

```
/tmp/p6s3-venv/bin/ruff check src tests examples
All checks passed!
```

### `make monitoring-validate`

Literal output (post the two new alert unit tests, see Deviation 15):

```
Checking /etc/prometheus/prometheus.yml
  SUCCESS: 4 rule files found
 SUCCESS: /etc/prometheus/prometheus.yml is valid prometheus config file syntax

Checking /etc/prometheus/alerts/concurrency.yml
  SUCCESS: 4 rules found

Checking /etc/prometheus/alerts/dlq.yml
  SUCCESS: 3 rules found

Checking /etc/prometheus/alerts/lag_and_errors.yml
  SUCCESS: 4 rules found

Checking /etc/prometheus/alerts/progress.yml
  SUCCESS: 2 rules found

[... check rules repeated per file, same counts ...]

docker run --rm --user "$(id -u):$(id -g)" --entrypoint promtool -v "$(pwd)/monitoring/prometheus:/etc/prometheus:ro" prom/prometheus:v2.55.1 test rules /etc/prometheus/alert_tests/concurrency_test.yml /etc/prometheus/alert_tests/progress_test.yml /etc/prometheus/alert_tests/dlq_test.yml
  SUCCESS

  SUCCESS

  SUCCESS

python3 scripts/validate_monitoring.py
1 dashboard(s), 15 panel(s), 13 alert expr(s), 19 unique metric name(s), all known
```

### "Prove the guard actually guards" (Story 5's own AC)

Performed manually once, then encoded as a permanent regression test:
injecting `graph_loader_records_consumed_total` ->
`graph_loader_recrods_consumed_total` into the dashboard JSON makes
`validate_monitoring.py` exit 1 naming the exact bad token; reverting
makes it exit 0 again. The same proof was repeated for a typo inside a
folded (`expr: >`) alert rule and a single-line `expr:` alert rule, to
exercise both code paths of the hand-rolled block-scalar parser.

### Scratch Docker end-to-end validation

Two rounds, both against dedicated `scratch-p6s3-*` resources on a
throwaway `scratch-p6s3-net` network — `graph-loader-net` and the shared
`graph-loader-kafka`/`graph-loader-neo4j` containers were never touched,
started, stopped, or connected to at any point in this slice's work.

**Important caveat on both rounds:** neither used the committed
`monitoring/prometheus/prometheus.yml` unmodified. Both used a copy with
exactly one line changed — the network-scoping `keep` rule's `regex:`
from `graph-loader-net` to `scratch-p6s3-net` — so Prometheus's Docker SD
would discover the scratch network instead of the real one. Every other
line, including the two-control-port drop, the `metrics_enabled` drop,
`honor_labels`, and every label-promotion rule, is the exact committed
file. What was proven live is that the keep-rule *mechanism* correctly
discriminates one network from another (Round 1's `droppedTargets`
evidence below shows a different team's `scratch-p6s2-net` containers
excluded once the regex was set to `scratch-p6s3-net`) — not that the
committed file's actual `graph-loader-net` value, specifically, was ever
the active rule in a live scrape.

**Round 1 — fake exporters** (before the P6-S2 merge, to validate
discovery/relabeling mechanics independent of real loader code):
- `scratch-p6s3-prometheus` (group_add fix applied) started with zero
  Docker-socket permission errors across multiple discovery refresh
  cycles; TSDB wrote successfully to a freshly-`chown`ed data directory
  (the B2 fix).
- A fake "node" exporter (labels matching `docker_service.py`'s real
  contract) and a fake "lease-server" exporter (exposing both 7700 and
  9464) were both discovered `health: "up"`; the lease-server showed
  exactly one active target (9464) with 7700 in `droppedTargets` — Story
  1's AC, proven live.
- Prometheus's own unscoped Docker API call surfaced a **different
  team's concurrent scratch stack** (`scratch-p6s2-net`, 13 containers)
  in `droppedTargets`, correctly excluded from `activeTargets` by the
  network-scoping `keep` rule — an unplanned but valuable proof that
  relabel rule #3 actually prevents cross-talk between concurrent
  scratch environments on one host.
- `honor_labels` confirmed: `kind` came through as `"node"`/`"lease-server"`
  with no `exported_kind` twin; `run_id` promoted correctly;
  `instance` = `container_name`.
- Grafana auto-provisioned both the datasource and dashboard (checked via
  `/api/datasources` and `/api/search`, zero manual steps).
- Querying an unscraped metric through Grafana's datasource proxy
  returned `{"status":"success","data":{"resultType":"vector","result":[]}}`
  — the "No data" mechanism, no error.
- Stopped `scratch-p6s3-prometheus` mid-run of a separate long-lived
  container; it kept running unaffected (`ExitCode=0`, still running).

**Round 2 — real images from the merged code** (after the orchestrator's
merge notice; `feat/streaming-observability` at `8aeca9b`, carrying
P6-S2's merge at `eee6f36`):
- Built `graph-loader-node:p6s3` / `graph-loader-edge:p6s3` from this
  branch's own `Dockerfile.node_loader`/`Dockerfile.edge_loader`.
- Stood up a full scratch stack: `neo4j:5-enterprise` +
  `confluentinc/cp-kafka:7.7.0` + `scratch-p6s3-prometheus` +
  `scratch-p6s3-grafana`, all on `scratch-p6s3-net`.
- Published 3 real `Person` records to a real Kafka topic, ran a real
  `graph-loader-node:p6s3` container against them.
- Confirmed via `MATCH (p:Person) RETURN ...` in `cypher-shell`: exactly
  3 nodes (Ada, Grace, Alan) landed in Neo4j.
- Confirmed via Prometheus's own query API:
  `graph_loader_records_consumed_total{kind="node",name="Person",run_id="scratch-run-real"}`
  = `3` — matching Neo4j's row count exactly, discovered with the correct
  labels (`node_label`, `replica_id`, `run_id`, `instance`,
  `container_name`) with no manual target configuration.
- Confirmed the same value through Grafana's datasource-proxy API, using
  the dashboard's own query shape.
- Stopped `scratch-p6s3-prometheus` mid-run again, this time with a real,
  still-live loader container: unaffected.
- All scratch containers, the network, both `:p6s3` images, and all
  scratch data directories were removed afterward
  (`docker rm -f`/`docker network rm`/`docker rmi`/`rm -rf`), verified
  with `docker ps -a --filter name=scratch-p6s3` (empty) and
  `docker network ls | grep scratch-p6s3` (empty).

**Not proven**, listed exhaustively rather than glossed over:

- **A full multi-container run** exercising every process kind
  (`edge`, `planned-writer`, `planned-dispatcher`, `lease-server`,
  `clock`) simultaneously with real traffic on every dashboard row at
  once. Round 1 covered the *dispatcher's* two-port-drop shape with a
  fake exporter (not the real lease server, which never publishes a
  second port — see the DoD note above); Round 2 covered one real node
  loader end-to-end. No real `graph-loader-edge:p6s3` container was
  ever run, even though the image was built.
- **Two concurrent runs of the same `node_label`** producing two
  distinguishable targets (Story 1's own AC) — verified by inspection
  of the relabeling rules, not run live in either round.
- **The dashboard's visual "no red panel-error indicator" criterion**
  (Story 4's own AC) — verified at the query level (every query returns
  `"status":"success"`, with or without data) through Grafana's
  datasource-proxy API, never by opening the dashboard in a browser.
- **The committed `prometheus.yml`'s own `graph-loader-net` keep-rule
  value**, as opposed to the keep-rule *mechanism* — see the caveat
  above both rounds.
- **`GraphLoaderPlanResourceWaitHigh`'s `run_id` grouping and
  `GraphLoaderConsumerLagHigh`/`Critical`/`NotDraining`'s dead-by-default
  status** have no committed `promtool` unit test (unlike
  `GraphLoaderPlannedModeDeadlockObserved`, `GraphLoaderNoRecordsConsumed`,
  and `GraphLoaderDlqRoutingDetected`, all three of which now do — see
  Deviation 15). Both are lower-risk than the three that got tests
  (one is a grouping-only change with no semantic branch; the other is
  documented as intentionally unreachable in this stack), but they are
  not proven by a committed regression test either.

Recommended follow-up once this slice merges alongside P5's own slices:
re-run the 8-step scratch-run proof from `stories.md` Story 5 against a
full real fleet (every process kind, two concurrent `run_id`s, a browser
check of the dashboard) rather than this slice's own fake-exporter/
single-real-loader validation.

## Reviewer findings and resolutions

A single dedicated Opus subagent reviewed every story's plan and diff for
the full lifespan of this slice (per Workflow 3's binding process),
re-reviewing after each fix. Every finding below was applied before the
corresponding story was marked done; none were deferred except where
explicitly noted as an accepted, documented limitation above.

| Story | Severity | Finding | Resolution |
| --- | --- | --- | --- |
| 1 | should-fix | `kind` label collision (target vs. metric) would rename the scraped label to `exported_kind` | `honor_labels: true` |
| 1 | should-fix | Garbled port-fallback comment described a mechanism that doesn't exist | Rewrote to describe real docker_sd one-target-per-port behavior |
| 1 | nit | Container IP:port as `instance` is not human-readable/reused across runs | Relabeled `instance` = `container_name` |
| 1 | nit | A `planned-dispatcher` with a non-default `--port` breaks the literal 7700/7701 drop | Documented as an accepted limitation (Known Limitations), not fixed |
| 1 | nit | `run_id` wording should say "absent", not "empty-string", for a bulk loader with no `--run-id` | Wording corrected in `docs/MONITORING.md` |
| 2 | should-fix x4 | `GraphLoaderNoRecordsConsumed` join semantics; DLQ rules' first-sample gap; consumer-lag rules dead by default; plan-resource-wait not grouped by run | All four rewritten/annotated, proven with scratch `promtool test rules`, two now with committed unit tests (Deviation 15) |
| 2 | nit x6 | `changes()` vs `increase()` on a gauge; deadlock alert auto-resolving on stale series; retry-rate not grouped by run; `LeaseWaitersStuck` wording (healthy contention vs. starvation); `TODO(P5-S4)` comment naming all three proxy rules with "e.g." on the illustrative gauge name; stale `alerts/tests.yml` path reference in `impl_plan_story_2.md` | All six applied |
| 3 | blocker x2 | Prometheus can't read `docker.sock` (uid mismatch); data dirs not writable | `group_add` + `chown`, both reproduced and fixed live |
| 3 | should-fix x4 | Stale AC comment; `:ro` overstated as a mitigation; `external: true` breaks combined-project `down`; unauthenticated lifecycle flag | All four fixed |
| 3 | nit x2 | Dashboards mount nested in data volume; CPUSET assumes >=32 cores | Both addressed |
| 4 | blocker | Missing `allValue` hides the default (no-`--run-id`) bulk run | Added to all three variables |
| 4 | should-fix x3 | `process_up` panel can't show "down"; deadlock/DLQ panels miss first-sample spikes; consumer-lag panel misleadingly shows "No data" | All three fixed |
| 4 | nit x3 | Fragile hidden datasource variable; missing `timeInterval`; unit formatting | All three fixed |
| 5 | should-fix x4 | Type-blind suffix stripping; template-variable queries unchecked; no catalogue-drift detection; guard tests didn't verify message content | All four fixed |
| 5 | should-fix | Record which scratch proof (fake vs. real) covers which AC, and don't reproduce `stories.md` Story 5 scratch-run step 8's bare `docker compose down` | Both addressed — see "Not proven" and the Round 1/2 write-up above, and Deviation 8 |
| 5 | nit x2 | Stale skip-reason docstring after merge; `.env`-as-shell caveat | Both addressed |
| 6 | (from Story 3 review) should-fix x2, nit x2 | `.env` not sourced by `make`; silent `chown` failure; echoed comment lines; portability | All four fixed |
| 6 | bug (reviewer-raised, then independently rediscovered while fixing) | `DOCKER_GID:?` broke `monitoring-down` too (Compose interpolates the whole file for every subcommand) — the reviewer's own Story 5/6 Q3 answer first flagged this, correcting the reviewer's own earlier `:?` suggestion | Exported in both `monitoring-up` and `monitoring-down` |
| 6 | nit x2 | `--user "$(id -u):$(id -g)"` needed on the promtool containers (image's default user can't read a restrictive-umask checkout); don't add `SHELL := /bin/bash` (it's global to every existing target, not just this slice's) | Applied `--user`; did not add `SHELL :=` |
| 7 | should-fix x5 | False port-read mechanism claim; wrong "target down" troubleshooting cause; scratch-check overpromised every row populating; missing network-name discovery gotcha; missing trusted-network posture/teardown-order/external-network-missing entries | All five fixed |
| 7 | nit x6 | Per-alert firing-behavior notes; exit-time snapshot's `GRAPH_LOADER_TIMING_DIR` conditionality and filename pattern; `cli` label-table wording; `run_id`-deletion impact (two lines, breaks the dashboard variable and three alert files); missing `DOCKER_GID` env-table row; port-9090/3000-collision troubleshooting row | All six added |
| 3/4/5/6 | — | Post-merge spot-check against the real `src/utils/metrics.py`/registry | Zero drift found; type-comparison test added per reviewer follow-up |

No finding was contested or left unresolved.

## Sign-off

All 8 stories implemented and reviewed; 7 committed to
`feat/p6-s3-monitoring` (Story 8's intended edit recorded but deferred
per the binding brief). Full unit suite green (1168 passed), lint green,
`make monitoring-validate` green (including two new committed `promtool`
unit tests closing a coverage gap found at completion-review time). Two
scratch-Docker rounds (fake exporters, then real merged-code images)
confirm discovery, relabeling, alerting config validity, dashboard
provisioning and query-level rendering, and "Prometheus down does not
affect a load" — with the specific scope of each proof, and what remains
unproven (two concurrent runs, a real edge-loader container, the
dashboard's visual no-panel-error criterion, and the committed
`prometheus.yml`'s own network-regex value), recorded plainly above
rather than folded into a blanket claim. No shared infrastructure
(`graph-loader-net`, `graph-loader-kafka`, `graph-loader-neo4j`) was
touched at any point.
