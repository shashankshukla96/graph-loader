# graph-loader monitoring guide

Related docs: [CLI reference](CLI.md), [Architecture](ARCHITECTURE.md), [Changelog](../CHANGELOG.md), [USAGE.md](USAGE.md), [BULK_LOAD.md](BULK_LOAD.md), [INCREMENTAL_LOAD.md](INCREMENTAL_LOAD.md).

An optional, local Prometheus + Grafana stack that discovers every
loader/writer/dispatcher/lease-server/clock container on `graph-loader-net`
by Docker label, scrapes the metrics endpoints P6-S2 (`docs/USAGE.md` §8)
already exposes, and provides a starter dashboard and a set of example
alert rules.

**What this is not:** a production-grade, long-lived, multi-tenant
observability platform. It is a disposable, short-retention, local
operator tool — a human watching a bounded run live, not twelve-month
capacity planning. See §4 for the cardinality reasoning behind that
distinction.

This guide covers:

1. [Overview](#1-overview)
2. [Quick start](#2-quick-start)
3. [How discovery works](#3-how-discovery-works)
4. [Cardinality budget](#4-cardinality-budget)
5. [Resource and data placement](#5-resource-and-data-placement)
6. [Alert rules](#6-alert-rules)
7. [Known limitations](#7-known-limitations)
8. [Troubleshooting](#8-troubleshooting)

## 1. Overview

- **Prometheus** (`prom/prometheus:v2.55.1`) discovers targets via
  `docker_sd_configs` against the host's Docker socket — no manually
  maintained target list, and no code in this library polls the Docker
  API itself.
- **Grafana** (`grafana/grafana-oss:11.3.1`) is pre-provisioned with the
  Prometheus datasource and one starter dashboard; no manual click-through
  setup.
- **No Pushgateway.** The original epic text proposed one for bulk runs
  shorter than a scrape interval; the orchestrator's P6-Q1 decision drops
  it entirely (`plan/v5-v6-orchestration.md`). Bulk-run durability for a
  container shorter than one scrape interval is already solved by P6-S2
  Story 9's periodic + exit-time snapshot — **if `GRAPH_LOADER_TIMING_DIR` is set** —
  into that directory (filename pattern
  `metrics-<kind>[-<name>][-<replica>].prom`, Prometheus text format,
  per `docs/USAGE.md`'s own wording) — this stack does live pull
  scraping only.
- **Off by default.** `docker compose up` alone never starts Prometheus or
  Grafana; this file's containers are defined in a separate override file,
  `docker-compose.monitoring.yml`, never referenced by the base
  `docker-compose.yaml`.
- **Prometheus down never affects a load.** Every loader/writer/dispatcher
  process's metrics HTTP server (P6-S2) never blocks or crashes if nobody
  scrapes it. Prometheus is a pull-based, passive observer: it calls out to
  loader containers; no loader container calls in to Prometheus, waits on
  a shared volume, or blocks on Prometheus's liveness in any way.

## 2. Quick start

```bash
docker compose up -d neo4j kafka          # base stack, if not already up
make monitoring-up                        # starts prometheus + grafana
```

- Prometheus UI: `http://localhost:9090` (no auth).
- Grafana: `http://localhost:3000` — anonymous Viewer access needs no
  login; editor login is `admin`/`admin` by default (both overridable, see
  §5).
- **Trusted network assumption, stated plainly:** Prometheus ships with no
  authentication at all, and Grafana's defaults are an unauthenticated
  Viewer plus a well-known `admin`/`admin` editor login — both bound
  `0.0.0.0` by default. This mirrors `docker-compose.yaml`'s own existing
  posture for `kafka-ui` (also unauthenticated, also `0.0.0.0`), not a new
  precedent, but it means this stack assumes a trusted local/private
  network (`stories.md` Story 3's own framing). On any host reachable by
  others, set `GRAFANA_ADMIN_PASSWORD` to something real and bind
  `PROMETHEUS_HOST_BIND`/`GRAFANA_HOST_BIND` to `127.0.0.1`.
- `docker compose up` (no `-f docker-compose.monitoring.yml`) behavior is
  unaffected — verified: `docker compose -f docker-compose.yaml config
  --services` lists only `neo4j`, `kafka`, `kafka-ui`.
- `make monitoring-down` stops and removes only the `prometheus`/`grafana`
  services — **never** run a bare `docker compose -f docker-compose.yaml -f
  docker-compose.monitoring.yml down` yourself: with both files loaded,
  Compose treats `neo4j`/`kafka`/`kafka-ui` as part of the same project, so
  an unscoped `down` would stop/remove those too and attempt to tear down
  `graph-loader-net`, which this profile declares external and does not
  own.
- **Teardown order:** run `make monitoring-down` **before** a base
  `docker compose down` (or `make dev-down`). Prometheus/Grafana stay
  attached to `graph-loader-net` until you stop them; tearing down the
  base stack first leaves them as orphan containers and the network
  itself can't be removed while they're still attached ("has active
  endpoints").
- `make monitoring-validate` runs `promtool check config`, `promtool check
  rules` (all five alert files), the `promtool test rules` unit tests (four test files), and
  `scripts/validate_monitoring.py` as one command.

### Scratch-run check

1. Run any small example (`examples/social/`) in bulk mode.
2. `curl -s localhost:9090/api/v1/targets | jq` — every launched container
   kind shows `"health": "up"`.
3. `curl -s 'localhost:9090/api/v1/query?query=graph_loader_records_consumed_total'`
   — at least one nonzero sample.
4. Open `localhost:3000` — the starter dashboard shows live data in the
   Throughput, Write latency, Errors and Process-health rows on any bulk
   run. The other rows only populate for the mode that emits them: Plan
   progress needs planned mode (the bulk default, or `--edge-plan`); Lease
   server/wait need lease mode (`--edge-leases`, or a schema that enables
   relationship `dead_letter`, which selects it automatically); Consumer lag
   is stream-only and is CLI-only, so it does not populate in this stack unless
   you opt in (§7). Deadlocks, DLQ (routed, backlog)
   and Parked correctly read 0 or "No data" on a clean run — that is the
   expected healthy state, not a broken panel.
5. `docker stop graph-loader-prometheus` mid-run — the load finishes with
   its normal exit code, unaffected. Bring it back with `make
   monitoring-up` afterwards.

## 3. How discovery works

Every container `docker_service.py` launches carries these Docker labels
(confirmed by reading `src/orchestrator/docker_service.py` directly):

| Label | Set by | Meaning |
| --- | --- | --- |
| `app` | every launcher | always `graph-loader` |
| `component` | every launcher | e.g. `node-loader`, `bucket-lease-server` |
| `graph_loader.kind` | every launcher | the `kind` metric label's closed enum (`node`, `edge`, `planned-writer`, `planned-dispatcher`, `lease-server`, `clock`, `retry-consumer` — `cli` is also in this enum, but it is never a *container* label value: the CLI/orchestrator process runs on the host, not as a container `docker_service.py` launches) |
| `graph_loader.metrics_port` | every launcher | the container's own resolved metrics port (default 9464) |
| `graph_loader.metrics_enabled` | every launcher | `"1"`/`"0"`, mirrors `GRAPH_LOADER_METRICS_ENABLED` |
| `run_id` | planned-writer/dispatcher/lease-server/clock/retry-consumer always; node/edge loaders only when `--run-id` was passed | correlates one run's containers; **absent**, not empty, on a bulk node/edge loader with no explicit run id |
| `node_label` | node loaders | the node label this replica loads |
| `edge_type` | edge loaders | the relationship type this replica loads |
| `writer_id` | planned writers | `w<index>` |
| `replica_id` | node/edge loaders and retry consumers | the replica index |

Prometheus's `docker_sd_configs` job (`monitoring/prometheus/prometheus.yml`)
narrows the Docker API call itself to containers carrying
`graph_loader.kind` (`filters:`) — Neo4j, Kafka, Kafka UI, Prometheus and
Grafana's own containers are never even fetched. `relabel_configs` then:

1. Drops the two known **control** ports (`7700` = the bucket-lease
   server's own RPC port, `7701` = the planned dispatcher's own RPC port —
   never the metrics port) by literal value, since RE2 has no
   backreferences and cannot compare "this port equals whatever this
   container's `graph_loader.metrics_port` label says" dynamically.
   **Reserved-value note:** if you ever set `GRAPH_LOADER_METRICS_PORT` to
   `7700` or `7701`, that container gets no scrape target. **Known gap:**
   if a `planned-dispatcher` is ever launched with a non-default `--port`
   (not done anywhere in this codebase today), its control port is not
   `7701` and this literal drop does not catch it — that container then
   shows an extra, always-`down` target.
2. Drops any container with `graph_loader.metrics_enabled == "0"`.
3. Keeps only targets on `graph-loader-net` (a container joined to a
   second network would otherwise be discovered twice). **Gotcha:** if you
   start containers with `--network` set to anything other than
   `graph-loader-net` (`src/cli.py`'s `--network` flag, default
   `graph-loader-net`), this rule silently drops them — Prometheus never
   even shows them as a target, and nothing anywhere logs an error. The
   dashboard just shows no data for that run. Fix: either use the default
   network, or edit this rule's `regex:` and connect the `prometheus`
   container itself to your network too.
4. Promotes the Docker labels above to series (target) labels an operator
   queries by: `kind`, `run_id`, `node_label`, `edge_type`, `writer_id`,
   `replica_id`, plus `container_name`/`instance` from the container's own
   name.

`honor_labels: true` is set on this scrape job: every `graph_loader_*`
metric already carries its own `kind` label (`src/utils/metrics.py`).
Without `honor_labels`, Prometheus's default label-collision rule would
rename the *scraped* `kind` to `exported_kind` and keep this job's
target-level `kind` instead — today the two values always agree, but
`honor_labels: true` keeps the metric's own value authoritative rather
than silently papering over a future divergence.

**Address construction.** `docker_sd_configs` builds one target per
`(container, network, exposed port)` tuple using that network's private
IP — Prometheus, itself a container on `graph-loader-net`, reaches every
target directly over that network. It never uses the host-published
`127.0.0.1:<ephemeral>` port mapping (`ports={"9464/tcp": ("127.0.0.1",
None)}`) that P6-S2 also adds — that mapping is for a human on the host to
`curl` one container directly, a different consumer of the same port.

**Port note.** The metrics port is **9464**
(`src/utils/metrics.py:DEFAULT_METRICS_PORT`), not Prometheus's own
default port (9090) — this stack runs a real Prometheus server on the
same `graph-loader-net` network, so the two must differ. Prometheus scrapes each
container's own **exposed** metrics port, which `docker_service.py`
always publishes as the same value it records in the
`graph_loader.metrics_port` label — so overriding
`GRAPH_LOADER_METRICS_PORT` needs no change to any file here.
`prometheus.yml`'s own `port: 9464` is a fallback only, used solely if a
`graph_loader`-labelled container ever exposes zero TCP ports, which
never happens today; it is not itself read from the Docker label.

**Scrape-vs-container-lifetime boundary, stated precisely.** A container
whose total lifetime is shorter than `refresh_interval + scrape_interval`
(2s + 3s = under ~5s here) can legitimately exit and be reaped before
Prometheus ever discovers it. Its only durable record in that case is
P6-S2 Story 9's periodic + exit-time `.prom` snapshot file, not a Prometheus series at
all. This stack's tuning is chosen so the documented IMDb 20k-seed bulk run
(~60s end-to-end, `docs/USAGE.md` §9) gets many scrapes per container — not
to guarantee every possible run shape is seen live.

**The CLI's own metrics server is discovered differently, not by this
job.** It runs on the host, not as a `graph-loader-net` container, so
`docker_sd_configs` never finds it regardless of any label. See §7 for
the separate `graph-loader-cli` host-gateway job that targets it.

## 4. Cardinality budget

P6-S2's own metric-label budget is bounded by schema size (label/type
count), never by record volume, and deliberately excludes `run_id` from
every *metric* label — a long-lived Prometheus server scraping many runs
over weeks would otherwise accumulate one time series per historical
`run_id` forever.

This stack adds `run_id` back as a **target** label (via Docker SD
relabeling, §3 point 4), not a metric label: it is a disposable, local,
short-retention stack (48h default, `MONITORING_RETENTION`), not the
long-lived production backend P6-S2's decision was written against. The
growth this adds is bounded by *concurrently running or recently-retained*
runs within the retention window, not by history.

**If you run this stack long-lived in a shared environment:** delete the
`run_id` relabel rule (the `source_labels`/`target_label` pair for
`run_id`, two lines) in `monitoring/prometheus/prometheus.yml` to
reproduce P6-S2's own metric-label discipline at the target-label level
too. Doing so also empties the dashboard's `run_id` template variable and
breaks the `{{ $labels.run_id }}` annotations and `by (le, run_id)`
grouping in `alerts/lag_and_errors.yml`/`alerts/concurrency.yml`/
`alerts/progress.yml` — expected, not a bug, since those exist
specifically to distinguish concurrent runs.

### Multi-topic relationship types

A relationship type can be fed by several topics (`edges[].topics`). Metrics keep `{kind, name=<type>}`:

* **The only per-topic series is `graph_loader_consumer_lag_records{kind,name=<type>,topic}`**, one series per
  topic of a type. The bound is therefore **one series per topic in the schema**: a type with K topics adds K
  series, and the total never exceeds the number of schema topics (3 types with 4 topics each = exactly 12 series;
  `tests/test_multi_topic_metrics.py` pins this). Stream supervision pre-touches one series per `(type, topic)`
  when it starts, so every topic reads 0 rather than being absent.
* **No other label changes.** There is no per-record, per-pair, per-partition or per-sub-stage label.
  `precedence` sub-stages (one per topic, classic and lease bulk) add no series; their per-topic view is the
  `substage-<type>.t<rank>.json` timing rows and the `stage=bulk-substage` log lines (`docs/USAGE.md` section 8),
  and their loader containers write `metrics-edge-<type>.t<rank>-<replica>.prom` snapshot files. The DLQ gauge
  stays `graph_loader_dlq_backlog{kind,name,queue}` (one DLQ per type).
* **Zero lag is over all topics.** One stuck topic keeps the whole type above zero lag and keeps a bulk stage
  open; the lag alerts fire per series and name the stuck `topic`. No per-topic stall alert is shipped on purpose.
* **The dashboard** has a "Consumer lag: multi-topic types" row: the slowest topic of each type
  (`max by (name)`) and the number of topics (lag series) per type (`count by (name)`), then a row of
  lag-by-topic panels, one per type (repeated over a hidden `edge_type` variable). All three filter on
  `kind="cli"`: the retry consumer also exports this gauge (`kind="retry-consumer"`, for its DLQ topic), which is
  not a data topic of the type.
* **Planner rejection count is not a metric.** Planned mode skips malformed relationship records into
  `var/rejections/<run id>-planner-<worker>.jsonl` and reports the count in the logs only
  (`stage=edge-plan-rejections ... rejected=<n>`, `stage=outcome rejected=<n>`; `rejected` in the plan statistics).
  Alert on that log line or the rejection files.

### Relationship types with several label pairs

A type whose topics bind different label pairs (for example `LIKES` Person->Post from `likes-posts` and
Person->Comment from `likes-comments`, `docs/USAGE.md` section 3) exports **exactly the series of a multi-topic
type with the same topics**: `{kind, name=<type>}` everywhere, lag `{kind, name, topic}` (one series per topic),
the DLQ backlog `{kind, name, queue}` (still one DLQ per type) and the parked-pair index wait `{kind, name}`. No
label names a pair, so dashboards and alerts need no change and the cardinality bound above is unchanged
(`tests/test_multi_pair_metrics.py`). `graph_loader_lease_wait_seconds` gets one observation per outer flush, the
sum over the pairs, which are written concurrently. The pair appears only in logs (`stage=edge-multi-pair ...
pairs=Person>Post,Person>Comment` at start, `pair=Person>Comment cells=...` in the debug `stage=edge-lease-flush`
line, write-failure lines) and in the plan file (`types.<type>.pairs`, and the `pair` field of each chunk).

## 5. Resource and data placement

| Variable | Default | Meaning |
| --- | --- | --- |
| `MONITORING_CPUSET` | `28-31` | CPUs for both `prometheus` and `grafana` |
| `MONITORING_RETENTION` | `48h` | Prometheus TSDB retention (see §4) |
| `GRAPH_LOADER_DATA_DIR` | `/mnt/graph-loader/data` | Same root `docker-compose.yaml` already uses for Neo4j/Kafka; this stack adds `prometheus/` and `grafana/` subdirectories under it — never the repo checkout |
| `PROMETHEUS_HOST_BIND` | `0.0.0.0` | Host bind address for Prometheus's UI |
| `GRAFANA_HOST_BIND` | `0.0.0.0` | Host bind address for Grafana |
| `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` | `admin` / `admin` | Grafana editor login (anonymous Viewer needs none) |
| `DOCKER_GID` | computed by `make monitoring-up`/`monitoring-down` (`stat -c %g /var/run/docker.sock`) | Required by Prometheus's `group_add` to read the Docker socket; export it yourself first for any *direct* `docker compose` command against `docker-compose.monitoring.yml` (see §7) |

**CPU overlap, named deliberately.** `MONITORING_CPUSET`'s default
(`28-31`) is the top of `docs/USAGE.md` §2's own documented
`LOADER_CPUSET=12-31` example — a deliberate overlap for this stack's
stated use case (a human watching a bounded validation run), not an
oversight. If you are benchmarking peak throughput, either narrow
`LOADER_CPUSET` to `12-27` first, or move `MONITORING_CPUSET` outside your
own `LOADER_CPUSET`. Both are pinned distinctly from Neo4j's (`0-7`) and
Kafka's (`8-11`) `cpuset:` in `docker-compose.yaml`.

**`/var/run/docker.sock`.** Mounted read-only into the Prometheus
container because `docker_sd_configs` talks to the Docker Engine API
directly. **`:ro` does not make the Docker API itself read-only** — any
process able to connect to this socket has root-equivalent access to the
whole host (create, exec into, and delete any container; bind-mount the
host filesystem into a new one), not merely this project's containers.
Scoped to "trusted local host only." A socket-proxy (e.g.
`tecnativa/docker-socket-proxy`) is the real hardening option for anyone
who needs this off a single trusted workstation — not built by this slice.

**`DOCKER_GID` (Linux hosts only).** The Prometheus image runs as `USER
nobody` (uid 65534), which cannot read the Docker socket without also
joining its group inside the container. `make monitoring-up`/
`monitoring-down` compute this automatically
(`DOCKER_GID=$(stat -c %g /var/run/docker.sock)`); a direct `docker
compose ... up`/`down`/`stop`/`rm` on `docker-compose.monitoring.yml`
without exporting `DOCKER_GID` yourself first fails loudly (`required
variable DOCKER_GID is missing a value`) rather than silently defaulting
to a guessed, possibly-wrong gid. `stat -c` is GNU coreutils syntax
(Linux); these Makefile targets are Linux-host-only (macOS's BSD `stat`
needs `-f %g`, and Docker Desktop's host socket gid is meaningless there
regardless, since Docker Desktop runs a Linux VM).

**Data ownership.** `make monitoring-up` also `chown`s the two data
subdirectories (`65534:65534` for Prometheus, `472:472` for Grafana, both
images' own runtime uids) after `mkdir -p`, and sources `.env` itself
first if present (exported variables win: the recipe re-applies the
exported environment after sourcing, so `.env` only fills in variables
that are not set) (docs/USAGE.md §2 documents `GRAPH_LOADER_DATA_DIR` as
settable "with an environment variable (or in `.env`)" — `docker compose`
auto-loads `.env`, but `make`'s recipe shell does not, so this step exists
to keep the two in sync). A `chown` failure (e.g. running as a non-root
user) fails the target loudly with the exact manual fix, rather than
deferring to a much more confusing `queries.active: permission denied`
inside the Prometheus container later. **Caveat:** this recipe sources
`.env` as a shell script (`. ./.env`), not with Compose's own `.env`
parser — a value containing shell-special characters (spaces, `$`, `;`,
quotes) can misbehave or fail when sourced this way, even though the same
value is perfectly valid to Compose's parser. Keep `GRAPH_LOADER_DATA_DIR`
itself free of shell-special characters, or export it directly in your
shell instead of only setting it in `.env`.

**`--web.enable-lifecycle` is deliberately not enabled.** That flag
(`POST /-/reload`, `/-/quit`) combined with a `0.0.0.0`-bound Prometheus
would be an unauthenticated remote kill switch; nothing in this stack's
own tooling needs it. Add it back explicitly (and narrow
`PROMETHEUS_HOST_BIND` to `127.0.0.1`, or add your own auth in front) if
you want live config reload.

## 6. Alert rules

**STARTING POINT, NOT AN SLO** (P6-Q4). Every rule file begins with this
header verbatim. Thresholds are conservative defaults chosen without
production traffic data — tune every one to your own environment before
treating a firing alert as an incident.

| File | Covers |
| --- | --- |
| `alerts/dlq.yml` | DLQ backlog high, DLQ backlog gauge absent while the fleet runs, retry writes stalled, DLQ routing detected/spiking, parking-lot growing |
| `alerts/progress.yml` | A planned run stalled; a process consuming zero records |
| `alerts/lag_and_errors.yml` | Consumer lag high/critical/not-draining (per `topic`; a stuck topic of a multi-topic type keeps the whole type above zero lag), retry rate high |
| `alerts/concurrency.yml` | A deadlock recovered in planned mode (must stay 0 by construction), lease waiters/timeouts, plan resource wait |
| `alerts/write_outage.yml` | A live stream loader riding out a Neo4j outage for longer than 2 minutes |

**DLQ backlog.** `graph_loader_dlq_backlog{kind,name,queue}` is the true
backlog gauge, split by a bounded `queue` label. `queue="dlq"` is the DLQ high
watermark minus the retry consumer's committed offset: retry work that should
drain. The value counts DLQ records (offsets), not distinct rows: a row re-queued several times, or duplicated by at-least-once publishing, is counted once per copy until the consumer's committed offset passes it. Treat it as a depth and drain signal, not a distinct-row count. `queue="parked"` is the parking-lot topic size: terminal and
cumulative (the parked topic has infinite retention and `replay-dlq --parked`
does not delete from it), so it only grows and needs a human. Only the
retry-consumer process emits it (edge types, `kind="edge"`), refreshed every
`retry_consumer.backlog_check_interval_ms` (default 10s), so it can lag by
one interval and it is absent when no retry consumer runs. With several
retry-consumer replicas each reports the same group-wide value, so aggregate
with `max by (kind, name, queue)`.
`GraphLoaderDlqBacklogHigh` alerts on `queue="dlq"` only (example threshold:
above 100 for 15m, a starting point to tune) and clears when the retries
drain; a large parked queue does not fire it. Parked records are covered by
`GraphLoaderParkedLotGrowing`. The dashboard's "DLQ backlog by queue" panel
plots both queues.
`GraphLoaderDlqBacklogAbsent` (owner decision D2) fires when the backlog
gauge of an edge type was reported within the last 6 hours, is absent now,
and that type's edge loaders are still up (`graph_loader_process_up`) for 5
minutes: the retry consumer is gone or no longer scraped while the fleet
runs. It stays silent for bulk runs (no retry consumer, so no gauge ever) and
after a clean stop (the edge loaders go too). In `--mode stream` the CLI also
stops the fleet with exit code 1 when a retry-consumer container exits; the
alert covers the gauge disappearing for any other reason.

**Retry stall.** `graph_loader_retry_stalled_seconds{kind,name}` (retry
consumer only, edge types, refreshed every tick, a real 0 from start-up) is
how long that type's retry writes have been failing with nothing written:
a read-only or unavailable database, an unrecognised server-side error, or a
lone row whose error is not known to be row-specific. Owner decision D1: the
retry consumer never counts or parks a row for a failure it has no evidence is
the row's own, so such rows wait and this gauge is the signal (with a
`reason=retry_stalled` ERROR in the retry consumer's log). It returns to 0 at
the first written row. For a type of several label pairs the evidence is per
pair, so a pair whose rows fail with none written keeps the type's gauge
rising even while its other pairs write (the ERROR then names `pair=S>T`;
there is still no pair label on the metric). `GraphLoaderRetryStalled` fires above 600 s for 1m
(example threshold); the dashboard's "Retry writes stalled" panel plots it.

**Stream write outage.** `graph_loader_write_outage_seconds{kind,name}` (lease-fenced
stream node and edge loaders only, `kind="node"|"edge"`, a real 0 from start-up,
computed at scrape time so it keeps rising while an attempt is blocked) is how
long the loader's Neo4j writes have been failing with connectivity-class errors:
Neo4j unavailable or restarting, an expired session, a connection pool or
acquisition timeout, a read-only database, a transient error, or the lease server
unavailable. While it is above 0 the loader does not exit. It holds its current
batch uncommitted, holds no bucket lease, pauses its partitions (it keeps polling,
so the group does not rebalance) and retries the same batch, in order, with a
backoff that doubles from 0.5 s to 30 s. It logs one WARNING when the outage
begins and one per failed retry, a `reason=neo4j_outage` ERROR once the outage
reaches 30 s (repeated every 5 minutes), and `stage=write-outage-recovered` when
a write succeeds; the gauge then returns to 0. A rebalance during the outage does
not reset it. An authentication or
missing-database error that persists for 10 minutes ends the ride-out: the loader
logs `reason=neo4j_fatal` and exits 1, and stream supervision stops the fleet (the
retry consumer's D2 rule). `GraphLoaderWriteOutage` fires above 120 s for 1m
(example threshold, `severity: critical`). The dashboard's "Stream write outage"
panel, in the Errors row, plots it. Bulk loaders never emit this series: they fail
fast on a Neo4j error, as before.

**DLQ growth and sizing.** DLQ topics have unlimited retention, so size their
disk: a relationship type's DLQ holds at most `records x (max_retry_attempts + 1)`
envelopes from its own failures (the live writer's first copy plus one re-DLQ per
failed attempt of a pair's earliest outstanding row), plus the blocked copies the
retry consumer publishes only when a DLQ partition's missing-endpoint backlog exceeds
`max_records_per_partition` plus `max_overflow_held_per_partition` (see docs/USAGE.md
section 12, "DLQ sizing", for the sizing guidance). `rate(graph_loader_dlq_routed_total[5m])` against
`rate(graph_loader_records_consumed_total[5m])` for the same `name` shows the
envelope rate relative to the input; a ratio that stays high while
`graph_loader_dlq_backlog{queue="dlq"}` grows means endpoints are not arriving. The
dashboard's "DLQ envelopes per consumed record" panel plots it per type, and "Parked-pair
index admission wait" plots the gauge below. No new alert: the existing backlog and routing
alerts already fire on sustained DLQ growth (`dlq.yml` is unchanged).

**Parked-pair index admission.** `graph_loader_parked_pair_index_wait_seconds{kind,name}`
(stream edge loaders with a parked-pair index, v7-S4) is how long the loader has
held its newly assigned partitions because its copy of the index has not yet
caught up with the index as of the assignment; 0 once admitted. It normally
reads 0 or a fraction of a second after a rebalance. A value that keeps growing
means the index topic is unreadable or the loader cannot keep up with it: the
partitions stay paused rather than risk writing a row of a parked pair (the
log line is `reason=parked_pair_index_lagging`). No example alert ships for it.

`GraphLoaderDlqRoutingDetected`/`Spike` are kept on purpose as activity
signals (records just started going to the DLQ): they fire without a retry
consumer and before the depth alert's `for:` elapses. They are not depth
proxies.

**Non-obvious firing behavior, worth knowing before you see it fire:**
- `GraphLoaderNoRecordsConsumed` fires for a legitimately idle long-lived
  streaming loader with no traffic, and for any node/edge/planned-writer
  container that has not written its first record within roughly 20
  minutes (a 10m window plus `for: 10m`) — tune or scope it to bulk runs
  only if that's noisy for you.
- `GraphLoaderPlannedModeDeadlockObserved` stays firing for up to 1 hour
  after the writer that recovered the deadlock has already exited
  (`max_over_time(...[1h])`) — intentional, so a ~60s bulk container's
  deadlock doesn't silently auto-resolve the moment its series goes
  stale.
- `GraphLoaderLeaseWaitersStuck` can fire under sustained but healthy
  lease-mode contention, not only real starvation; real starvation is
  more reliably signaled by `GraphLoaderLeaseWaitTimeoutsDetected`.
- `GraphLoaderPlannedRunStalled` also takes roughly 20 minutes to fire
  (same window-plus-`for:` shape).

## 7. Known limitations

- **No Pushgateway.** See §1; P6-S2's periodic + exit-time `.prom` snapshot is the
  durable record for a run shorter than a scrape interval.
- **DLQ backlog is edge-only and retry-consumer-scoped** (§6): there is
  no depth metric for a node DLQ, and no per-topic depth for anything else;
  use `kafka-ui` (already in `docker-compose.yaml`) or a Kafka lag exporter.
- **`graph_loader_consumer_lag_records` and the planner series
  (`graph_loader_planner_rows_read_total`,
  `graph_loader_planner_chunks_published_total`) are CLI-only, and
  scraping them needs one opt-in step every other series doesn't (found
  during P6-S3, closed during P6-S5).** These are emitted only by the
  host-side CLI process, whose metrics server binds `127.0.0.1` on the
  *host* by default — Docker service discovery (the `graph-loader` job)
  never finds it, and a bridge-networked Prometheus container cannot
  reach host loopback.
  * `monitoring/prometheus/prometheus.yml` has a second,
    host-gateway-based job, `graph-loader-cli`, targeting
    `host.docker.internal:9464`, and `docker-compose.monitoring.yml`
    gives the `prometheus` service the matching
    `host.docker.internal:host-gateway` `extra_hosts` entry (Linux;
    macOS/Windows Docker Desktop already resolve this hostname without
    it).
  * **Reaching it needs an explicit operator opt-in.** Set
    `GRAPH_LOADER_METRICS_BIND_ADDR=0.0.0.0` (unset, or `127.0.0.1`,
    keeps today's loopback-only default — no change in behavior) in the
    CLI's own environment before running `start`. This is logged at
    `WARNING` on every run that sets it: it exposes an unauthenticated
    Prometheus endpoint beyond localhost, on whatever network interfaces
    the CLI's host has, not only the Docker bridge — treat it the same
    trust boundary as this whole monitoring stack (§7's own "no auth at
    all" note) and never set it on a host with an untrusted network.
    Also keep `GRAPH_LOADER_METRICS_PORT` fixed (the CLI's
    ephemeral-port fallback on a bind conflict,
    `allow_ephemeral_fallback=True`, cannot be followed by this job's
    static target) — the default `9464` is fine as long as nothing else
    on the CLI's host is already using it.
  * Without the opt-in, the `graph-loader-cli` job shows its one target
    as **down** in Prometheus's own target list — harmless (a down
    target never fails a scrape for anything else, and never blocks
    `docker compose up`), and the consumer-lag/planner series stay
    unscraped by this stack exactly as before this option existed, with
    the three consumer-lag alert rules and the dashboard's consumer-lag
    panel still annotated as dead-by-default for the same reason.
    (P6-S5 Story 6 added a test proving a SIGTERM-driven stream run
    leaves a parseable `metrics-cli.prom` snapshot.)
- **The planned dispatcher's control port (7701) is only ever the
  default** in this codebase today — see §3's known gap if that ever
  changes.
- **`docker compose ... config` does not check that `graph-loader-net`
  exists** — only `docker compose ... up` does (verified empirically).
  Don't rely on `config` to catch a missing base stack.
- **This slice never claims "every short-lived container is always
  scraped"** — see §3's scrape-vs-container-lifetime boundary.
- **These Makefile targets are Linux-host-only** (§5's `DOCKER_GID` note).
- **Containers launched on a network other than `graph-loader-net` are
  silently dropped**, not shown as an error anywhere (§3's discovery
  gotcha).
- **This stack has no built-in security posture beyond "trusted local
  network"** — Prometheus has no auth at all; Grafana's defaults are an
  unauthenticated Viewer plus `admin`/`admin` (§2). Change the password
  and narrow the host binds before exposing either port beyond your own
  workstation.
- **Fixed host ports (9090, 3000) can collide** with any Prometheus or
  Grafana instance already running locally for something else — there is
  no env override for the port *number* itself, only the bind address
  (`PROMETHEUS_HOST_BIND`/`GRAFANA_HOST_BIND`). Stop the conflicting
  instance, or edit the port mapping in `docker-compose.monitoring.yml`
  directly.

## 8. Troubleshooting

| Symptom | Check |
| --- | --- |
| A target shows `down` in Prometheus | Not a `metrics_enabled=0` container — those are dropped entirely, never shown `down` (§3, drop rule 2). Actual causes: the container's metrics server failed to bind (it logs a `stage=metrics` warning and keeps loading — check its own logs); a `planned-dispatcher` launched with a non-default `--port` (§3's known gap); or the container is exiting mid-scrape. |
| A container is missing from the target list entirely (not even `down`) | `graph_loader.metrics_enabled=0`; its total lifetime was under ~5s (§3's scrape-vs-lifetime boundary); it's on a network other than `graph-loader-net` (§3's gotcha, §7); or it has already exited — targets vanish on exit, they don't turn `down` first. |
| Dashboard panel says "No data" for a running load | The `kind`/`name`/`run_id` template variable selection — confirm it isn't scoped to a different run than the one you're watching |
| Consumer-lag panel/alerts always empty | Expected — see §7; that series is CLI-only and not scraped by this stack |
| `docker compose ... up`/`down` on `docker-compose.monitoring.yml` fails with "required variable DOCKER_GID is missing a value" | Use `make monitoring-up`/`monitoring-down`, or export `DOCKER_GID=$(stat -c %g /var/run/docker.sock)` yourself first |
| `docker compose ... up` fails with "network graph-loader-net declared as external, but could not be found" | The base stack isn't up yet — run `docker compose up -d neo4j kafka` first |
| Prometheus container fails with `queries.active: permission denied` | The data directory under `GRAPH_LOADER_DATA_DIR` isn't owned by uid 65534 — `make monitoring-up` does this for you; if running Compose directly, `chown 65534:65534` that directory (`472:472` for Grafana's) |
| `graph_loader_write_outage_seconds` above 0, `reason=neo4j_outage` in a loader log | A stream loader cannot write to Neo4j and is retrying its held batch. Check Neo4j's health (`SHOW DATABASES`, its logs). Nothing is lost or committed past it; ingestion resumes by itself once writes succeed. A `reason=neo4j_fatal` exit means credentials or the database name are wrong (§6, "Stream write outage") |
| `make monitoring-validate` fails on `promtool` | Confirm Docker can pull `prom/prometheus:v2.55.1`; the target needs one-time image pull plus Docker itself |
| Port 9090 or 3000 already in use | Something else on the host is already bound to it (§7) — stop it, or edit the port mapping in `docker-compose.monitoring.yml` |
