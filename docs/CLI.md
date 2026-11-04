# Command-line reference

`graph-loader <command>` is the installed entry point.
`python -m neo4j_graph_loader <command>` and the existing CLI module form are
equivalent. Top-level `--version` reports the installed package version. This page is derived
from `build_parser()` in `src/neo4j_graph_loader/cli.py` and the code behind it; run
`graph-loader <command> --help` for the live text (no Kafka, Neo4j or
Docker needed to print help).

Related: [README](../README.md) (quick start) | [USAGE](USAGE.md) (detailed
reference) | [ARCHITECTURE](ARCHITECTURE.md) | [BULK_LOAD](BULK_LOAD.md) |
[INCREMENTAL_LOAD](INCREMENTAL_LOAD.md) | [MONITORING](MONITORING.md) |
[PROCESS_RUNTIME](PROCESS_RUNTIME.md) (`--runtime process`, host and systemd) |
[CHANGELOG](../CHANGELOG.md)

Contents: [Commands](#commands) | [Logging](#logging) | [start](#start) | [stop](#stop) |
[status](#status) | [status-store](#status-store) | [monitoring export](#monitoring-export) |
[replay-dlq](#replay-dlq) | [Edge mode selection](#edge-mode-selection-bulk) |
[Exit codes](#exit-codes) | [stage= log lines](#key-stage-log-lines) |
[Environment variables](#environment-variables)

## Commands

| Command | Purpose |
| --- | --- |
| `start` | Apply the schema DDL, launch the node-loader fleet (Docker containers, or child processes with `--runtime process`) and the relationship machinery, in `--mode bulk` (finite) or `--mode stream` (open-ended). |
| `stop` | Stop node-loader containers (all, or one label). With `--runtime process`: stop the run's command, or one label's node loaders; `--recover` cleans up after an abruptly killed command. |
| `status` | Without options: list node-loader containers (or, with `--runtime process`, node-loader processes) and their state. With `--run`, `--json`, `--watch`, `--list` or `--execution-id`: the **run status** of an execution (state, roles, progress), from the JSON files of a run directory or from the SQL status database. |
| `status-store` | `migrate`, `prune` and `flush` the SQL run status database ([status stores](STATUS_SQL.md)). |
| `monitoring export` | Write the Prometheus scrape configuration, alert rules, promtool tests, Grafana dashboards and provisioning (and Kubernetes manifests) for your own Prometheus and Grafana ([MONITORING](MONITORING.md#9-bring-your-own-prometheus-and-grafana)). Needs no services. |
| `images build` | Build the four loader images from the installed package for a registry you control, from your own mirrors or offline ([images build](#images-build)). Needs Docker with buildx. |
| `schema-example` | Print the bundled default or social schema with `--name default\|social`, without services. |
| `validate-schema` | Validate `--config PATH` or `--sample default\|social` without services. |
| `replay-dlq` | Republish dead-lettered (or parked) records of one node label or relationship type to their source topic. |

See the [installed schema/runtime guide](CLI_RUNTIME.md) for resource lifetimes,
private run directories and read-only installations. Related helpers are separate modules: `python -m neo4j_graph_loader.loader.node_loader`, `python -m neo4j_graph_loader.loader.edge_loader`
(single-topic debugging runs), `python -m neo4j_graph_loader.loader.retry_consumer`
(standalone DLQ drain), `scripts/validate_monitoring.py`, and the
`make monitoring-up|monitoring-down|monitoring-validate` targets.

Every command that talks to roles accepts `--runtime {docker,process}`
(environment `GRAPH_LOADER_RUNTIME`; default `docker`). `docker` runs one container
per role; `process` runs the roles as child processes of the `start` command on a
Linux host, with no Docker daemon, network or images. See
[Process runtime](PROCESS_RUNTIME.md).

## Logging

`--log-format {text,json}` (environment `GRAPH_LOADER_LOG_FORMAT`; default `text`) is accepted
before or after the command. `text` is the historical `LEVEL: message` line. `json` writes **one JSON object per line on
stderr** (the log stream; command output such as YAML or `status --json` stays on stdout and is never affected): `ts`,
`level`, `logger`, `execution_id`, `run_id`, `kind`, `name`, `replica`, `incarnation`, `stage`, `message`, and the
structured fields of the documented `stage=` lines. Every role of a run carries the same `execution_id` (and its own
`incarnation`), so lines of one run can be selected across the CLI and its roles; credentials are redacted from the
message, the fields and tracebacks. With `--runtime process` the roles' lines are relayed on the command's stdout as
`[name] line` (`--child-logs`), unchanged; with the Docker runtime they stay in the containers' logs.

The CLI does **not** read `.env`. Export the connection variables yourself
(see [Environment variables](#environment-variables)); `docker compose` reads
`.env` for the local stack only.

## start

```text
graph-loader start --mode {bulk,stream} [--config PATH] [options]
```

Validation errors are logged as `stage=launch reason=...` and exit 1 before any
container (or process) is started, except where noted.

### General

| Flag | Default | Valid values / range | Applies to | Notes |
| --- | --- | --- | --- | --- |
| `--mode {bulk,stream}` | required | `bulk`, `stream` | both | `bulk` is finite and verifies a zero-lag drain; `stream` runs until SIGINT/SIGTERM. |
| `--config PATH` | bundled canonical schema | a caller schema YAML | both | Explicit caller paths take priority; no cwd config fallback. |
| `--run-dir BASE` | OS temporary directory | a writable parent directory | both | Create a unique private persistent attempt under BASE; see [runtime paths](CLI_RUNTIME.md). |
| `--runtime {docker,process}` | `docker`, or `$GRAPH_LOADER_RUNTIME` | `docker`, `process` | both | How roles run: one container each, or child processes of this command (Linux >= 5.3 only; refused on other systems with `stage=runtime reason=...`). See [Process runtime](PROCESS_RUNTIME.md). |
| `--network NAME` | `graph-loader-net` | an existing Docker network | both | Docker runtime only: the network the loader containers join; it must reach Kafka and Neo4j under the `LOADER_*` addresses. Refused with `--runtime process`, even with the default value. |
| `--skip-image-build` | off | flag | both | Docker runtime only: require matching preloaded images; never pull. Starts never build images. See [image policy](DOCKER_EXECUTION.md). Refused with `--runtime process`. |
| `--metrics-addr HOST[:PORT]` | `127.0.0.1`, OS-assigned port per role | host, or `HOST:PORT` | both | Process runtime only: where every role binds its metrics endpoint. The ports are listed in `<attempt>/metrics/targets.json`. Use `0.0.0.0` only when something outside the host must scrape. Refused with `--runtime docker`. |
| `--health-addr HOST:PORT` | off | `HOST:PORT`, `:PORT` or `PORT`; port `0` allowed | both | Process runtime only: serve `GET /healthz` and `GET /readyz`; the bound port is logged as `stage=health`. Refused with `--runtime docker`. |
| `--health-stale-after SECONDS` | `30` | float | both | Process runtime only: `/healthz` turns unhealthy when the supervision loop has not ticked for this long. Refused with `--runtime docker`. |
| `--child-logs {relay,files}` | `relay` | `relay`, `files` | both | Process runtime only: `relay` also prints every role's output to this command's stdout as `[name] line`; `files` keeps it only in `<attempt>/logs/`. Refused with `--runtime docker`. |
| `--status-store {json,sql}` | `json`, or `$GRAPH_LOADER_STATUS_STORE` | `json`, `sql` | both | Where the run's status document and event history are kept: `json` (files in the attempt directory) or `sql` (a database named by `GRAPH_LOADER_STATUS_DB_URL`; needs `neo4j-graph-loader[postgres]` or `[sql]`). See [status stores](STATUS_SQL.md). |
| `--status-store-auto-migrate` | off (`GRAPH_LOADER_STATUS_AUTO_MIGRATE=1`) | flag | both | With `--status-store sql`: create or upgrade the status schema at start instead of refusing an older one. |
| `--metrics-port PORT` | off | 1-65535 | both | Process runtime only: serve **one aggregated** `/metrics` on `--metrics-addr`'s host at this port, merging this command's metrics and every role's. Scrape it **or** the per-role `targets.json`, never both for the same run. Refused with `--runtime docker`. |
| `--run-id ID` | random UUID hex (bulk, and stream with relationship types); none for a node-only stream | 1-128 chars of `[A-Za-z0-9._-]`, starting with a letter or digit | both | Run identity (the `run_id` label of every sample and field of the status; reusable, so it never replaces the per-start `execution_id`). Scopes container names and labels, the planned chunk topic, the lease server and the **bulk** relationship consumer groups. Stream groups do **not** contain it; in stream mode it only lets the CLI migrate legacy run-scoped offsets (see the stream notes below). |
| `--bulk-timeout-seconds S` | `300` | float > 0, validated before any side effect | bulk | Ignored in stream (any value is accepted there). Maximum seconds for each bulk lifecycle stage. Progress-based for node draining, waiting for the edge plan (planner heartbeat) and the planned dispatch: those fail only after `S` seconds **without progress**. |
| `--accept-former-dlq TOPIC` | none | repeatable; a topic name | both (classic, lease, stream; not planned) | Start although `TOPIC`, a relationship type's former default DLQ name (`<topic>.dlq` or `<type>.dlq`), still holds records. Deleting the old topic is the expected step. |
| `--accept-dlq-leftovers TYPE` | none | repeatable; a relationship type | both (classic, lease, stream; not planned) | Start although `TYPE`'s parked-pair index holds records of a topic it no longer reads or of a topic now bound to another label pair. Draining the DLQ with the previous schema first is the expected step. |

The former-DLQ and DLQ-leftover preflights are skipped when the run is planned
(planned mode ignores `dead_letter` entirely).

### Relationship mode (bulk)

`--edge-plan`, `--edge-leases` and `--edge-classic` are **mutually exclusive**
(exit 1). With none of them, bulk picks the mode itself; see
[Edge mode selection](#edge-mode-selection-bulk).

| Flag | Default | Valid values / range | Applies to | Notes |
| --- | --- | --- | --- | --- |
| `--edge-plan [PLAN_FILE]` | off (but planned is the bulk default) | optional path | bulk only | Globally planned, node-disjoint chunks; zero lock waits. Without a file the loader plans from the relationship topics up to the bulk boundary while nodes load; with a file it loads a producer-written plan (waited for up to `--bulk-timeout-seconds`). Requires `python_apoc` edges. With `--mode stream` it exits 1. |
| `--edge-leases` | off | flag | bulk only | A run-scoped bucket-lease coordinator; concurrent relationship transactions are node-disjoint across all types and containers. Implies `--bulk-edge-overlap shared`. Requires `python_apoc` edges and `loading.bucket_lease.tx_timeout_ms >= 6000`. With `--mode stream` it exits 1. |
| `--edge-classic` | off | flag | bulk only | The per-type APOC-locked loaders. The one mode that can wait on Neo4j locks. (`native_disjoint` edges can only be attempted here, but they need the Cypher 25 dialect and fail closed on the supported Neo4j 5.26; keep `python_apoc`.) With `--mode stream` it is an error (exit 1). |
| `--bulk-edge-overlap {labels,shared}` | `labels` | `labels`, `shared` | bulk, **classic only** | `labels` serializes types that share an endpoint label; `shared` runs them concurrently under globally ordered APOC locks and co-schedules `python_apoc` self-references. Outside classic: warning `--bulk-edge-overlap/--max-concurrent-bulk-edges tune classic only; ignored`. |
| `--max-concurrent-bulk-edges N` | `0` (unlimited) | integer >= 0 | bulk, **classic only** | Cap on concurrently running shared edge types. Ignored with a warning outside classic. |

### Planned mode tuning (`--edge-plan`, also the bulk default)

Every value is derived automatically (`src/neo4j_graph_loader/orchestrator/auto_settings.py`); a
flag overrides it. Invalid ranges exit1 before state creation for explicit
planning and default bulk starts. Planner settings have no effect in explicit
classic/lease modes; stream `--edge-plan` is rejected.

| Flag | Default | Range | Applies when | Notes |
| --- | --- | --- | --- | --- |
| `--plan-writers N` | Neo4j available processors + 2 (10 if the server cannot be probed); never more than the plan's chunks | 1-64 | planned (with or without a file) | Writer containers across all types. |
| `--plan-workers N` | `min(4, CPUs)` | 1-64 | planned, no file | Planner processes reading the edge topics. |
| `--plan-buckets N` | `clamp(writers, 8, 64)`, at most `isqrt(records / (types * 500))`, at least 2 | 1-4096 | planned, no file | Hash buckets per node label. |
| `--plan-hub-degree N` | the chunk size | 0-2^40 (0 disables hubs) | planned, no file | A node with at least this total degree over all types gets its own resource. |
| `--plan-chunk-rows N` | `clamp(records / (20 * writers), 500, 5000)` | 1-100000 | planned, no file | Maximum rows per chunk transaction. |
| `--plan-partitions N` | `4` | 1-1024 | planned, no file | Partitions of the run's chunk topic. |
| `--plan-topic NAME` | `graph-loader-edge-chunks-<run id>` | a new topic name | planned, no file | The chunk topic to create. |
| `--plan-dir DIR` | private attempt `plans/` | a directory | planned, no file | Holds `edge-plan.json` and `edge-plan.json.ledger.json`. |
| `--plan-memory-mb N` | a quarter of available RAM over the planner processes, 256-4096 MiB | 16 - 1048576 | planned, no file | Per-planner memory budget; beyond it the planner spills to `--plan-spill-dir` and produces the identical plan. |
| `--plan-spill-dir DIR` | private attempt `spill/` | a directory | planned, no file | Planner-owned spill children are removed after workers reap; attempt state persists. Needs about the relationship topics' uncompressed size free. |
| `--replace-stale-plan-topic` | off | flag | planned, no file, **explicit `--run-id`** | Delete the chunk topic left behind by a killed run with the same run id, if no container of that run is alive. Default: refuse to start. Ignored if `--plan-topic` is given. |
| `--keep-plan-topic` | off | flag | planned, no file | Keep the run's chunk topic afterwards (default: deleted). |

### Bucket-lease tuning (`--edge-leases`, the lease default for `dead_letter`, and `--mode stream`)

| Flag | Default | Range | Applies when | Notes |
| --- | --- | --- | --- | --- |
| `--bucket-lease-buckets N` | `16` | 1-4096 | lease runs (bulk lease, stream) | Stable lease buckets per node label. |
| `--bucket-lease-outer-batch-size N` | `32000` | 1-1000000 | lease runs | Edge outer flush size, split into bucket cells. Overrides `edge_unwind_batch_size`. |
| `--bucket-lease-keyed-max-nodes N` | `1000` | integer >= 0 (0 = never) | lease runs | Endpoint labels whose node topic holds at most this many records are leased per node key instead of per hash bucket. |
| `--lease-buckets`, `--lease-outer-batch-size`, `--lease-keyed-max-nodes` | none | as above | hidden, **deprecated** | Aliases of the three flags above (not shown in `--help`). Log `stage=deprecation flag=... superseded_by=...` once; giving an alias and its replacement with different values is an error. |

Lease runs also require every relationship type to use `execution.mode:
python_apoc`, and `loading.bucket_lease.tx_timeout_ms >= 6000` (the fenced
BEGIN needs `T` minus 4 s above 1 s plus 1 s margin).

### Node loaders

| Flag | Default | Range | Applies to | Notes |
| --- | --- | --- | --- | --- |
| `--node-replicas-min-records N` | `10000` | integer >= 0 (0 = always the YAML value) | bulk | A node label whose YAML omits `replicas` gets one loader when its topic retains fewer than `N` records, else one per topic partition up to `--max-node-replicas`. Labels with an explicit YAML `replicas` keep it. Ignored in stream. |
| `--max-node-replicas N` | `8` | integer | bulk | Cap for derived node-loader replicas. Not range-checked at parse time. |

### Stream mode

| Flag | Default | Range | Applies to | Notes |
| --- | --- | --- | --- | --- |
| `--retry-consumer-replicas N` | `1` | 1-64, validated before any side effect | stream | Replicas of the fleet-wide DLQ retry consumer; launched only when at least one relationship type has DLQ routing enabled, and never under `--stream-clock-rotation`. |
| `--stream-group-member-wait-seconds S` | `loading.session_timeout_ms` + 5 s | float > 0 | stream | How long to wait for a stable consumer group's members to expire before refusing to start (one live fleet per `consumer_group_id`). A member that died during a rebalance can stay up to `max_poll_interval_ms + session_timeout_ms`. With the default session timeout (45000 ms) a second `start` against a live fleet takes about 50 s to refuse, because the live members never expire; pass a smaller value to fail faster. |
| `--stream-clock-rotation` | off | flag | stream | **Deprecated** (logs `stage=deprecation`): the legacy global batch clock with rotating/isolated-phase scheduling instead of the default bucket-lease fleet. Refused for multi-pair types. No effect in bulk. |

### Behaviour notes

* **Bulk** creates a run-scoped fleet, waits until every replica proves its
  Kafka assignment, captures the end-offset boundary of each partition
  (records arriving later fail the run), waits for committed offsets to reach
  it, stops exactly the containers it created, then verifies zero lag. The
  containers are removed at the end, also on failure, after their diagnostics
  were logged.
* **Stream** never stops on zero lag. The CLI stays in the foreground,
  supervising the node loaders, relationship loaders, retry consumer and lease
  server. SIGINT/SIGTERM stops them (exit 0). With a schema that declares no
  relationship types, `start --mode stream` launches the node loaders and
  returns 0 immediately, leaving them running (stop them with `stop`).
* **Stream consumer groups** are stable: `<consumer_group_id>-<type>` for each
  relationship type, `<consumer_group_id>-retry-consumer` for the retry
  consumer; node loaders use `<consumer_group_id>-<label>` (shared with bulk). A
  restart resumes from committed offsets, with or without `--run-id`. Before
  launching, the CLI refuses to start while a `<prefix>-<type>` or
  `<prefix>-retry-consumer` group still has members (after waiting for them to
  expire: about 50 s with the default session timeout), and migrates offsets of
  earlier run-scoped groups once, only into a stable group that has no commits,
  when `--run-id` names the earlier run
  (`stage=consumer-groups reason=migrated_legacy_offsets`). Node groups are only
  checked for a name clash with a relationship group. The member check and the
  migration are skipped when the schema has no relationship types, and there is
  no retry group under `--stream-clock-rotation`.
* **Topic preflight (stream):** missing node or relationship topics are
  reported before launch and refuse the start (exit 1). The control topic is created by the CLI.
* `GRAPH_LOADER_METRICS_BIND_ADDR` and the other metrics variables apply to the
  CLI itself too (see below).

## Edge mode selection (bulk)

`stage=launch edge_mode=<mode> reason=<reason>` is logged once.

| Condition (checked in this order) | `edge_mode` | `reason` |
| --- | --- | --- |
| `--edge-plan` given | `planned` | `flag` |
| `--edge-leases` given | `lease` | `flag` |
| `--edge-classic` given | `classic` | `flag` |
| none given, any relationship type uses `execution.mode: native_disjoint` | refused (exit 1): `native_disjoint` needs the Cypher 25 dialect and is not supported on Neo4j 5.26; use `python_apoc` | |
| none given, any relationship type has an enabled `dead_letter` | `lease` | `dead_letter` |
| none given | `planned` | `default` |

Planned and lease modes have zero Neo4j lock waits by construction; classic can
wait by design. A schema without relationship types skips the mode selection.
Stream mode always uses the bucket-lease fleet (unless the deprecated
`--stream-clock-rotation`).

## stop

```text
graph-loader stop [--runtime {docker,process}] [--loader LABEL] [--run-dir DIR] [--recover]
```

| Flag | Default | Notes |
| --- | --- | --- |
| `--runtime {docker,process}` | `docker`, or `$GRAPH_LOADER_RUNTIME` | Which runtime's run to stop. |
| `--loader LABEL` | all | Stop only the node-loader containers whose `node_label` label is `LABEL` (process runtime: the registered node-loader processes of that label). |
| `--run-dir DIR` | OS temporary directory | Process runtime only: the run's attempt directory, or the base given to `start` (exactly one live run under it is chosen; otherwise name the attempt directory). |
| `--recover` | off | Process runtime only: after the `start` command died abruptly (`kill -9`), stop what it left behind and report what is retained; deletes nothing. Refused while that command is alive. No effect with `--runtime docker`. |

Stops containers labelled `component=node-loader` (running or not) once their
stop-signal handler is ready. It does not stop relationship loaders, the retry
consumer or the lease server: stop a stream run by sending SIGINT/SIGTERM to
the `start` process, which stops everything it launched. Exits 0; exits 1 with a clear message when Docker is unreachable.

`stop` is not scoped to a run. On a shared Docker host it stops every
graph-loader node-loader container, including other users' runs. Use
`--loader LABEL`, or stop your own run through its `start` process.

**Process runtime.** `stop` works through the run's `registry.jsonl`. Without
`--loader` it opens a pidfd to the registered `start` command, verifies its start
time and boot id, and sends it `SIGTERM`, so the command performs its normal
ordered stop of everything it started (it is scoped to the one run). With
`--loader LABEL` it signals only that label's node loaders; in a stream run with
relationships the supervisor then fails the run (exit 1). With `--recover` it
signals the leftover registered processes of a dead command (`SIGTERM`, `SIGKILL`
after 15 s), appends a `recovered` record and logs
`stage=recover attempt=... signalled=[...] killed=[...] remaining=[...]` and
`stage=recover retained (never deleted) files=<n>: ...`; it exits 1 only if a
process is still alive. A stale or ambiguous registry exits 1 with
`stage=runtime reason=...`. See [Process runtime](PROCESS_RUNTIME.md#7-stopping-and-recovery).

## status

```text
graph-loader status [--runtime {docker,process}] [--config PATH] [--run-dir DIR]
                    [--run] [--execution-id ID] [--json] [--watch] [--interval SECONDS]
                    [--refresh-lag] [--refresh-timeout SECONDS] [--list] [--active]
                    [--containers] [--stale-after SECONDS] [--status-store {json,sql}]
```

**Without** `--run`, `--json`, `--watch`, `--list` and `--execution-id` the command is the node-loader listing it
always was: with `--runtime process` (and `--run-dir`, as for `stop`) the node loaders are read from the run's registry
and listed in the same format with status `running`; otherwise one line per node-loader container (`Loader <name>
(<id>): <status> [Node: <label>]`) or `No loaders running.`; `--config` is only logged; exit 0, or 1 with a clear
message when Docker is unreachable.

**Run status.** Any of the options below shows the status of an execution instead
([status stores](STATUS_SQL.md), [field sources](STATUS_FIELDS.md)):

| Flag | Default | Notes |
| --- | --- | --- |
| `--run` | off | The newest run found under `--run-dir` (or the OS temporary directory), or the one named by `--execution-id`: a summary of its state, roles and per-type progress. Implied by `--json`, `--watch`, `--refresh-lag`, `--list` and `--execution-id`; `--run-dir` alone is still the container listing. |
| `--execution-id ID` | newest | Show this execution (32 hex digits). With `--status-store sql` it is looked up in the database. |
| `--run-dir DIR` | OS temporary directory | An attempt directory (either runtime's) or the base given to `start`. |
| `--json` | off | Print the status document as JSON on stdout (one line per refresh), plus a `reader` member (`stale`, `dead`, `heartbeat_age_seconds`, `stale_after_seconds`, `exit_code`, `label`). With `--list`: the list as a JSON array. |
| `--watch`, `--interval SECONDS` | off, `2` | Refresh until the run ends. |
| `--refresh-lag`, `--refresh-timeout SECONDS` | off, `5` | Ask Kafka once, within the deadline, for the current consumer lag; never blocks otherwise. |
| `--list`, `--active` | off | List the runs of the store (one line each: execution id, state, run id, mode, runtime, start, heartbeat age) instead of showing one; `--active`: only runs that have not finished. |
| `--containers` | off | Also print the container listing above. |
| `--stale-after SECONDS` | `30` (`GRAPH_LOADER_STATUS_STALE_AFTER_S`) | A running execution whose heartbeat is older than this is reported stale, and dead when its process is gone. |
| `--status-store {json,sql}` | the run directory's locator | Which store `--list` and `--execution-id` read when no run directory is given (the SQL store needs `GRAPH_LOADER_STATUS_DB_URL` in the environment). |

**Exit code of the run status view:** `0` the execution is running or succeeded, `1` it failed, `2` it is stale or dead
(heartbeat older than `--stale-after`, process gone), or no status was found or the store cannot be read, `3` it was
stopped. `--list` exits 0 (printing `no runs found` when there are none). Run status needs no Docker and no Kafka
(except `--refresh-lag`). A `?` in the summary is "not known", never `0`.

## status-store

```text
graph-loader status-store migrate [--schema NAME]
graph-loader status-store prune --older-than DURATION [--schema NAME]
graph-loader status-store flush --run-dir DIR [--wait SECONDS] [--schema NAME]
```

Maintain the SQL run status database named by `GRAPH_LOADER_STATUS_DB_URL` (never a command-line argument; the password
is read from the environment, `.pgpass` or a service file). `migrate` creates or upgrades the schema (idempotent);
`prune --older-than 30d` (or `12h`, `90m`) deletes **finished** executions older than that and never a running one;
`flush` delivers the events a killed run, or a database that was down, left in the run's local outbox (it waits up to
`--wait` seconds, default 30, for a live process that owns it). `--schema` (`GRAPH_LOADER_STATUS_SCHEMA`) is the
PostgreSQL schema, default `graph_loader`. Needs `neo4j-graph-loader[postgres]` (or `[sql]` plus your driver). Exit 0 on
success, 1 on any error (messages never contain credentials).

## monitoring export

```text
graph-loader monitoring export --out DIR --discovery {docker,file,static,kubernetes} [options]
```

Writes the monitoring bundle for your own Prometheus and Grafana; no Docker, Kafka or Neo4j is needed. It refuses to
overwrite existing files unless `--force`. Full guide: [MONITORING §9-§10](MONITORING.md#9-bring-your-own-prometheus-and-grafana).

| Flag | Default | Notes |
| --- | --- | --- |
| `--out DIR` | required | Directory to write; created. |
| `--discovery` | required | `docker` (Docker service discovery), `file` (the process runtime's `metrics/targets.json`), `static` or `kubernetes` (the aggregated `--metrics-port` endpoint). |
| `--scrape-interval DURATION` | `15s` | `1s` to `1h`. |
| `--job-name NAME` | `graph-loader` | The scrape job; the health alert follows it. |
| `--extra-labels K=V[,K=V...]` | none | Added to every target and alert. `run_id`, `execution_id`, `exporter` and `runtime` are reserved and refused. |
| `--metrics-port PORT` | none | The aggregated endpoint port; required for `static` and `kubernetes`. |
| `--targets HOST:PORT[,...]` | `localhost:--metrics-port` | `static`: scrape these instead. |
| `--targets-file PATH` | `/etc/prometheus/graph-loader/targets.json` | `file`: the `targets.json` Prometheus reads (a wildcard only in the last path element). |
| `--docker-network NAME` | `graph-loader-net` | `docker`: the network the roles run on. |
| `--namespace`, `--service-name` | `default`, `graph-loader` | `kubernetes`: namespace and Service name. |
| `--prometheus-url URL` | `http://prometheus:9090` | The datasource URL written to the Grafana provisioning file. |
| `--datasource-uid UID` | unset (provisioning file uses `prometheus`) | The Grafana Prometheus datasource uid, written to the provisioning file and preset in the provisioned dashboard's datasource variable; give it when Grafana has several Prometheus datasources. |
| `--values FILE` | none | YAML: `job_name`, `labels`, per-alert `threshold`/`for`/`severity`, `disable`. |
| `--force` | off | Overwrite existing files. |

Exit 0 on success; 1 for an invalid option or values file, or a file that cannot be written.

## images build

The whole picture for a network without internet access (the three routes, mirrors, wheelhouse, registry login) is in [Restricted networks](RESTRICTED_NETWORKS.md).

Builds the four loader images (`graph-loader-cli`, `-node`, `-edge`, `-clock`) for the **installed package version**, under a registry prefix you
control. It is for an organization that cannot pull the released images: the build needs only your own base-image mirror, your own PyPI mirror
(or a wheelhouse, for no index at all) and, for the CLI image's `tini`, your Debian mirror. Docker with the `buildx` plugin (BuildKit) is required
and the command stops with a clear message without it. The images are built for the architecture of the Docker daemon (linux/amd64 or
linux/arm64); the hashed dependency lock of that platform ships with the package.

```sh
graph-loader images build --registry registry.example.com:5000/team \
    --base-image registry.example.com:5000/mirror/python:3.12-slim \
    --pip-index-secret ./pip-index-url.txt --pip-ca-bundle ./ca.pem --package-wheel ./neo4j_graph_loader-<version>-py3-none-any.whl --push
```

| Option | Meaning |
| --- | --- |
| `--registry PREFIX` | where the images are named: `<prefix>/graph-loader-<role>:<version>`; the prefix grammar of `GRAPH_LOADER_IMAGE_REGISTRY` (default: that variable). Point `GRAPH_LOADER_IMAGE_REGISTRY` at the same prefix and `start` finds the images. |
| `--package-version V` | must equal the installed version; anything else is refused. |
| `--package-wheel FILE` | the wheel of this installed package. Its file hashes must equal the installed package's; with `--wheelhouse` the wheel is taken from the wheelhouse. |
| `--base-image REF` | default `python:3.12-slim`; give your mirror's reference. It is resolved to a digest before the build and the Dockerfiles build `FROM` that digest. |
| `--pip-index-url URL` | your PyPI mirror, **without credentials** (a URL with credentials is refused). |
| `--pip-index-secret FILE` | a file whose only content is your PyPI mirror's URL, credentials included. It is passed to BuildKit as a secret and read by the installer inside the build; it is never on a command line, in the build context, in a layer, in a log or in the manifest. |
| `--wheelhouse DIR` | offline build: exactly the wheels of the lock for the build platform plus the package wheel (prepared inside the base image, see [Distribution development](DISTRIBUTION_DEVELOPMENT.md#image-dependency-locks)). The directory is checked against the lock before anything starts: a missing wheel, a wheel for another architecture or a changed wheel is refused. |
| `--pip-ca-bundle FILE` | CA bundle (PEM) for an https index with a private certificate. |
| `--debian-mirror URL` | your Debian mirror (archive layout) for the `tini` install in the CLI image; a base image that already has `tini` 0.19.0 skips apt. |
| `--proxy URL` | an HTTP(S) proxy for the build steps. None is passed unless you give this, whatever your environment holds. |
| `--docker-config DIR` | the Docker config whose registry logins are used (default: yours). Only the logins for the base image's registry and the target registry are copied into the throwaway configuration the build runs with. |
| `--push` | push the four images with `docker push` (same logins) and record each digest. |
| `--tag-suffix S` | tag `<version>-<S>` for a local development build; it cannot be pushed. |
| `--dry-run` | validate everything and print the plan as JSON; build and push nothing. |
| `--output DIR` | write the build manifest `images.json` (and `image-overrides.sh`) into this new directory. |
| `--replace` | overwrite an image tag that already exists locally; refused otherwise. |

`docker` runs with a sanitized environment: a fresh `DOCKER_CONFIG` (the scoped logins), the daemon of your current Docker context (a local socket
only), no proxy or `PIP_*` variable of yours. The Docker SDK is not used to build or push. The manifest records the package wheel and its sha256,
the lock and every dependency artifact sha256, the Dockerfile and install-helper hashes, the resolved base image digest, the build arguments
without secrets, the Debian package versions of the CLI image, and the image ids and references. Exit code 1 for any refusal or failed step.

## replay-dlq

```text
graph-loader replay-dlq (--node-label LABEL | --edge-type TYPE) [options]
```

`--runtime` is accepted but changes nothing: `replay-dlq` talks only to Kafka.

Reads the entity's DLQ topic (or parking lot) up to a captured boundary and
republishes the matching envelopes to their source topic. Every envelope is
replayed from its first DLQ copy in first-arrival order; filters see the latest
copy.

| Flag | Default | Notes |
| --- | --- | --- |
| `--config PATH` | bundled canonical schema | Caller schema YAML when explicitly supplied. |
| `--node-label LABEL` / `--edge-type TYPE` | one is required | **Mutually exclusive.** Unknown name exits 1. |
| `--reason-code CODE` | none | Only envelopes with exactly this `reason_code`. |
| `--since TS` / `--until TS` | none | ISO-8601 timestamps; a timestamp without offset is UTC. `--until` earlier than `--since` exits 1. |
| `--limit N` | none | Replay at most `N` matching envelopes (not messages scanned). Must be > 0. |
| `--target-topic TOPIC` | the entity's own topic; for a type with several topics, each envelope's `original_topic` | Republish destination. A topic outside the entity's topics logs a warning (`reason=target_outside_entity`). |
| `--source-topic TOPIC` | none | Repeatable. Only envelopes whose `original_topic` is `TOPIC`, which must be one of the entity's topics. |
| `--dlq-topic TOPIC` | resolved from the entity's `dead_letter` block (even if `enabled: false`) | Override the topic to read. Required if the entity has no `dead_letter` block. |
| `--parked` | off | Read the parking-lot topic instead. Ignored when `--dlq-topic` is given. Replayed parked records re-enter with `attempt_count=0`. Exits 1 if `dead_letter.enabled` is false, even when `parked_topic` is set; in that case pass `--dlq-topic <parked topic>`. |
| `--dry-run` | off | Count and log matches without publishing. |

For multi-topic types with `topic_conflict: precedence` or `arrival` a replay
warns that it preserves neither arrival nor precedence order (only `order_by`
protects newer values). For a multi-pair type, `--target-topic` needs
`--source-topic` values that all bind one pair. Output ends with
`stage=replay-dlq matched=<n> replayed=<n> dry_run=<bool> dlq_topic=... target_topic=...`.

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | `start`: bulk run verified and complete; stream run stopped cleanly by SIGINT/SIGTERM (or a stream schema without relationships launched its node loaders). `stop`, `status`: when Docker is reachable. `replay-dlq`: finished (including `--dry-run`). |
| `1` | Any failure: invalid flags or combinations (mutual exclusions, ranges, `--edge-classic` with stream, native_disjoint without `--edge-classic`, `tx_timeout_ms` below 6000 in lease modes), schema load or validation failure, Neo4j schema initialization failure, stale DLQ topics or leftovers, stream group members still alive, image resolution or launch failure, a bulk stage failing or timing out, an operator stop during a bulk run or before launch completed, a node loader dying in a stream run, a foreign member joining a stream group (`stage=consumer-groups reason=foreign_members`), a container that would not stop (bulk, and stream edge/retry containers), `--edge-plan` or `--edge-leases` with `--mode stream`, a stream schema with a multi-topic `precedence` type, a stream relationship type named like a node label, `--stream-clock-rotation` with a multi-pair type, an edge-loader or retry-consumer crash in a stream run. A stream node loader that fails to stop is only logged; the run still returns 0. `replay-dlq`: unknown entity, invalid filter, replay error. |
| `2` | Command-line usage error from `argparse` (unknown flag, missing `--mode`, invalid number, `--node-label` together with `--edge-type`). |

A nonzero bulk result makes no claim of a successful load: inspect the run's
logs and the rejection files before retrying.

With `--runtime process`, exit `1` also covers: a system other than Linux >= 5.3,
a Docker-only or process-only option given with the wrong runtime, a role that
exits before it reports ready or is silent for 60 seconds, a planner budget that
does not fit the memory limit (`stage=sizing`), and `stop`/`status` finding no
unambiguous live run or a registry that does not match the running command.
A role killed by a signal *n* is reported as exit `128 + n` (`137` after
`SIGKILL`).

## Key `stage=` log lines

All logs go to stderr/stdout of the process in `LEVEL: message` form; the
`stage=<name>` token marks the lifecycle step, followed by `key=value` fields.
Selected lines (source in parentheses):

| Line | Logged by | Meaning |
| --- | --- | --- |
| `stage=launch edge_mode=<mode> reason=<reason>` | CLI | The chosen bulk relationship mode. |
| `stage=launch reason=<error>` | CLI | A validation failure before any side effect (exit 1). |
| `stage=launch node replicas <label>=<n> ...` | CLI | Derived node-loader replicas (bulk). |
| `stage=launch planned writers=<n> (Neo4j cores=<n>)` | CLI | Derived `--plan-writers`. |
| `stage=node-label-ready label=<L> elapsed_s=<s>` | CLI | A node label fully loaded; edge chunks needing it may start. |
| `stage=edge-plan-settings records=... writers=... buckets=... chunk_rows=... buckets_reason=...` | CLI planned | Settings the plan uses; `buckets_reason` says why `B` has its value (`writers`, `self_ref_share=S`, `volume_cap=V,...` or `explicit`). |
| `stage=edge-plan-start / edge-plan-scan / edge-plan-ready plan_id=...` | planner | Planning progress; `edge-plan-ready` carries chunks, rows, resources, hubs. |
| `stage=edge-plan-rejections plan_id=... rejected=<n> dir=...` | planner | Malformed relationship records skipped to `<attempt>/rejections/<run id>-planner-<worker>.jsonl`. |
| `stage=planned-dispatcher-ready ... chunks=<n> rows=<n>` / `stage=planned-ready labels=...` / `stage=planned-done chunk=... writer=...` | dispatcher | Dispatcher up; labels ready; a chunk committed. |
| `stage=planned-dispatch-failed reason=<r>` | dispatcher | The planned dispatch failed (the run fails). |
| `stage=planned-writer-complete writer=<n> chunks=<n> rows=<n>` | writer | A planned writer finished. |
| `stage=planned-edges-complete plan_id=... chunks=... rows=... elapsed_s=...` | CLI planned | The ledger holds every chunk exactly once. |
| `stage=outcome rejected=<n> ...` | CLI | Planned run succeeded, with `n` skipped malformed records. |
| `stage=bulk-edge ...` / `stage=bulk-edge-complete edge=<T> elapsed_s=... outcome=...` | CLI | Classic/lease per-type stage progress. |
| `stage=bulk-substage ...` | CLI | Topic-precedence sub-stages (classic, lease). |
| `stage=lease-server-ready host=... port=... buckets=... generation=...` / `lease-server-stats` | lease server | Coordinator up; periodic stats. |
| `stage=lease-fence ... reason=late_transaction_begin / deadline_requeue` | lease writers | A leased cell was re-queued instead of risking the fence. |
| `stage=consumer-groups ...` | CLI | Stable-group checks and legacy offset migration (`resume=committed`, `resume=stable-group`, `reason=migrated_legacy_offsets`, `reason=groups_have_members`, `reason=foreign_members`). |
| `stage=retry-consumer-ready run_id=... dlq_topics=...` | retry consumer | DLQ retry consumer running. |
| `stage=replay-dlq matched=... replayed=...` | CLI | `replay-dlq` summary. |
| `stage=metrics-ready kind=... name=... addr=... port=...` | every process | Its Prometheus endpoint is up. |
| `stage=process-preflight platform=... pidfd=ok run_dir=...` | CLI (process runtime) | The local checks passed (Linux >= 5.3, pidfd, `/proc`, writable run directories, an isolated child can import the package). |
| `stage=sizing planner_workers=... planner_memory_mb=... headroom_mb=... reserved_mb=... planner_mb=... roles=...` | CLI (process runtime, planned bulk) | The planner budget computed inside the memory limit. `stage=sizing reason=...` at ERROR refuses the run before any role starts. |
| `stage=health addr=... port=...` | CLI (process runtime) | The `/healthz` and `/readyz` server is listening. |
| `stage=recover ...` | CLI (`stop --recover`) | What was signalled and killed, and which files are retained. |
| `stage=shutdown ...` | all | Operator stop, container stop and cleanup progress. |
| `stage=deprecation flag=<old> superseded_by=<new>` | CLI | A deprecated flag was used. |
| `stage=write-outage*` | stream writers | A Neo4j outage was ridden out (stream only). |

## Environment variables

The CLI runs on the host; the containers it launches get their own values,
some derived from the host variables. "Host" below means the `start`/
`replay-dlq` process.

### Connections

| Variable | Default | Read by | Notes |
| --- | --- | --- | --- |
| `NEO4J_URI` | `bolt://localhost:7687` | CLI (host); loaders and retry consumer (container, set by the CLI from `LOADER_NEO4J_URI`; with `--runtime process` the roles get this value unless `LOADER_NEO4J_URI` is set) | Bolt endpoint. |
| `NEO4J_USERNAME` | `neo4j` | CLI; passed through unchanged to containers | The CLI falls back to the legacy `NEO4J_USER` when it is unset, and passes the resolved value to containers as `NEO4J_USERNAME`. |
| `NEO4J_PASSWORD` | `changeme` | CLI; passed through unchanged to containers | Also used by `docker compose` (from `.env`) to set the local Neo4j password. |
| `NEO4J_DATABASE` | server default | every process opening sessions (CLI schema init, loaders, retry consumer, planned writers) | Target a named database; forwarded to containers when set on the host. |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | CLI (host); every container (set by the CLI from `LOADER_KAFKA_BOOTSTRAP_SERVERS`; with `--runtime process` the roles get this value unless `LOADER_KAFKA_BOOTSTRAP_SERVERS` is set) | Kafka bootstrap. |
| `LOADER_NEO4J_URI` | `bolt://neo4j:7687` | CLI, when building container environments | Bolt URI as seen from the Docker network. |
| `LOADER_KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | CLI, when building container environments | Kafka as seen from the Docker network. |
| `LOADER_CPUSET` | unset | CLI | Docker `cpuset_cpus` pin for every loader container (keeps them off the cores of Kafka and Neo4j). With `--runtime process` each role pins itself to these CPUs (`12-15,18` form); the command and the planner workers are not pinned. |
| `GRAPH_LOADER_RUNTIME` | `docker` | CLI | Default for `--runtime` (`docker` or `process`); the flag wins. Any other value exits 1. The process runtime also sets `GRAPH_LOADER_RUNTIME=process` for the roles it starts, which is how they label their samples and log lines `runtime=process` (absent means `docker`); it is not an operator setting for roles. |
| `GRAPH_LOADER_LOG_FORMAT` | `text` | CLI and every role | `text` or `json` (see [Logging](#logging)); `--log-format` wins. |

### Run status

| Variable | Default | Read by | Notes |
| --- | --- | --- | --- |
| `GRAPH_LOADER_STATUS_STORE` | `json` | CLI | Default for `--status-store`. |
| `GRAPH_LOADER_STATUS_DB_URL` | none | CLI (never a role) | SQLAlchemy URL of the status database, for example `postgresql+psycopg://USER:PASSWORD@HOST:5432/DB`. Required for `--status-store sql`, `status --list` on SQL and `status-store`. Never on the command line. |
| `GRAPH_LOADER_STATUS_SCHEMA` | `graph_loader` | CLI | PostgreSQL schema of the status tables. |
| `GRAPH_LOADER_STATUS_AUTO_MIGRATE` | off | CLI | `1` is `--status-store-auto-migrate`. |
| `GRAPH_LOADER_STATUS_STALE_AFTER_S` | `30` | `status` | Heartbeat age after which a running execution is stale. |
| `GRAPH_LOADER_STATUS_INTERVAL_S` | `5` | CLI and every role | How often roles write their metrics snapshot for the run status (the telemetry freshness window is three intervals). |
| `GRAPH_LOADER_STATUS_SNAPSHOT_INTERVAL_S` | `1` | CLI | Minimum seconds between rewrites of `status.json` while it changes. |
| `GRAPH_LOADER_STATUS_EVENT_FILE_MB`, `GRAPH_LOADER_STATUS_EVENT_FILES` | `8`, `8` | CLI | Size of one `events.<first seq>.jsonl` file and how many are kept (JSON store). |
| `GRAPH_LOADER_STATUS_OUTBOX_MB`, `GRAPH_LOADER_STATUS_OUTBOX_SEGMENT_MB` | `64`, `4` | CLI | Bound of the local outbox in front of the SQL store, and its segment size. |
| `GRAPH_LOADER_STATUS_SHUTDOWN_DEADLINE_S` | `10` | CLI | Seconds the CLI waits at exit for the database to receive the last events (the rest stays in the outbox for `status-store flush`). |
| `GRAPH_LOADER_EXECUTION_ID`, `GRAPH_LOADER_INCARNATION` | set by the CLI | roles | The identity a role stamps on its log lines and samples (not operator settings). |

### Container images

Docker runtime only; `--runtime process` does not read them.

| Variable | Default | Read by |
| --- | --- | --- |
| `GRAPH_LOADER_IMAGE_REGISTRY` | unset: `ghcr.io/neburyx` | CLI (every default image reference: `<prefix>/graph-loader-<role>:<package-version>`, for the four roles including `cli`) |
| `GRAPH_LOADER_NODE_IMAGE` | `ghcr.io/neburyx/graph-loader-node:<package-version>` | CLI (node loaders) |
| `GRAPH_LOADER_EDGE_IMAGE` | `ghcr.io/neburyx/graph-loader-edge:<package-version>` | CLI (edge loaders, planned dispatcher/writers, retry consumer, lease server) |
| `GRAPH_LOADER_CLOCK_IMAGE` | `ghcr.io/neburyx/graph-loader-clock:<package-version>` | CLI (global clock, only with `--stream-clock-rotation`) |

`GRAPH_LOADER_IMAGE_REGISTRY` is a registry prefix for an organization that keeps the images in its own
registry: `host[:port]` followed by lowercase path components, for example `registry.example.com:5000/team`. It takes no
scheme (`https://`), no credentials (`user:password@`), no tag or digest, no spaces and no trailing slash; a value that
breaks the grammar stops `start` before anything is launched, with a message that does not repeat the value. The three
`GRAPH_LOADER_<ROLE>_IMAGE` variables win for their role, and a full reference with credentials in it is refused too (log in
with `docker login` instead). Empty means unset. `--runtime process` does not read it.

`make build-images CANDIDATE_SUFFIX=unique-name` builds all four local development
candidates; export the matching `GRAPH_LOADER_*_IMAGE` tags from its manifest.
Every default/override requires exact package-version, component and contract
labels; see [Docker execution](DOCKER_EXECUTION.md).

### Run behaviour and output paths

| Variable | Default | Read by | Notes |
| --- | --- | --- | --- |
| `GRAPH_LOADER_TIMING_DIR` | private attempt `timings/` | CLI | Explicit host timing directory overrides this run default; mounted into containers for timings and metrics snapshots. Plan output has its own private default. |
| `GRAPH_LOADER_TIMING_LOG_PATH` | set by the CLI | loader containers | Per-replica write-timing log path inside the container. |
| `GRAPH_LOADER_EDGE_EXPLICIT_LOCKS` | `1` | classic edge loaders (forwarded by the CLI) | `0` skips the `apoc.lock.nodes` step (benchmark experiment; deadlocks become possible). |
| `GRAPH_LOADER_PLAN_NODE_PHASE_WRITERS` | `0` (unlimited) | planned dispatcher (forwarded by the CLI) | Cap on active chunks while node labels still load. |
| `GRAPH_LOADER_RUN_ID` | set by the CLI | edge loaders, retry consumer | The run id inside a container. |
| `KAFKA_GROUP_ID` | node loaders: `<consumer_group_id>-<label>` (not set by the CLI); edge loaders and retry consumer: set by the CLI | node, edge loaders, retry consumer | Consumer group of a container. The CLI sets it for edge loaders (bulk `<prefix>-<type>-<phase run id>`, stream `<prefix>-<type>`) and the retry consumer (`<prefix>-retry-consumer`). A standalone retry consumer defaults to the legacy `graph-loader-retry-consumer-<run id>`. |
| `KAFKA_COORDINATION_GROUP_ID` | set by the CLI | edge loaders with slot gating (only the deprecated `--stream-clock-rotation` path) | Group used to read the control (coordination) topic. |
| `REJECTION_LOG_PATH` | `loading.rejection_log_path` (`var/rejections/node-loader.jsonl`) | node, edge and retry loaders | Per-replica JSONL sink for records rejected when no DLQ is configured; the CLI mounts its private rejection directory and gives each replica its own file (including retry fallback). |

### Metrics

See [MONITORING](MONITORING.md) for the metric catalogue.

| Variable | Default | Read by | Notes |
| --- | --- | --- | --- |
| `GRAPH_LOADER_METRICS_ENABLED` | enabled | every process (forwarded by the CLI) | `0` or `false` disables the endpoint and snapshots. |
| `GRAPH_LOADER_METRICS_PORT` | `9464` | every process (forwarded by the CLI) | Prometheus endpoint port. The CLI falls back to an ephemeral port when taken. |
| `GRAPH_LOADER_METRICS_LISTEN` | set by the runtime | roles, `--runtime process` only | `host:port` from `--metrics-addr` (port `0` = OS-assigned); replaces `GRAPH_LOADER_METRICS_PORT` for the roles. Not an operator setting. |
| `GRAPH_LOADER_METRICS_BIND_ADDR` | `127.0.0.1` | CLI only | Set to expose the CLI's endpoint to a Prometheus container; logs a warning because the endpoint is unauthenticated. |
| `GRAPH_LOADER_METRICS_SNAPSHOT_PATH` | set by the CLI per replica | containers; CLI uses its resolved timing directory/metrics-cli.prom | Atomic Prometheus-text snapshot written periodically and at exit. |
| `GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S` | `15` | every process (forwarded by the CLI) | Seconds between periodic snapshot rewrites. `0` disables the periodic timer (the exit-time write still happens); non-finite values fall back to the default, values below 1 are raised to 1 and values above 86400 are clamped. |
| `GRAPH_LOADER_VERSION` | package version (`0.1.0a2` in this release) | every process (nonempty overrides forwarded by the CLI to containers) | Startup override for the process-info metric only; does not change package/image identity. A directly supplied empty value remains empty for compatibility. |
| `GRAPH_LOADER_LEASE_MERGE_ROWS` | `200` | stream edge loaders and the retry consumer (forwarded by the CLI) | Small lease cells are merged under one lease up to this many rows. |
| `GRAPH_LOADER_BUCKET_LEASE_ADDR` | set by the CLI | edge loaders, node loaders, retry consumer | Lease server address. `GRAPH_LOADER_LEASE_ADDR` is a deprecated alias (logs `stage=deprecation`). |
