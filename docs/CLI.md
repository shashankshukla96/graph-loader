# Command-line reference

`python -m src.cli <command>` is the single entry point. This page is derived
from `build_parser()` in `src/cli.py` and the code behind it; run
`python -m src.cli <command> --help` for the live text (no Kafka, Neo4j or
Docker needed to print help).

Related: [README](../README.md) (quick start) | [USAGE](USAGE.md) (detailed
reference) | [ARCHITECTURE](ARCHITECTURE.md) | [BULK_LOAD](BULK_LOAD.md) |
[INCREMENTAL_LOAD](INCREMENTAL_LOAD.md) | [MONITORING](MONITORING.md) |
[CHANGELOG](../CHANGELOG.md)

Contents: [Commands](#commands) | [start](#start) | [stop](#stop) |
[status](#status) | [replay-dlq](#replay-dlq) | [Edge mode selection](#edge-mode-selection-bulk) |
[Exit codes](#exit-codes) | [stage= log lines](#key-stage-log-lines) |
[Environment variables](#environment-variables)

## Commands

| Command | Purpose |
| --- | --- |
| `start` | Apply the schema DDL, launch the node-loader fleet (Docker containers) and the relationship machinery, in `--mode bulk` (finite) or `--mode stream` (open-ended). |
| `stop` | Stop node-loader containers (all, or one label). |
| `status` | List node-loader containers and their state. |
| `replay-dlq` | Republish dead-lettered (or parked) records of one node label or relationship type to their source topic. |

There are no other subcommands. Related helpers are separate modules, not CLI
commands: `python -m src.loader.node_loader`, `python -m src.loader.edge_loader`
(single-topic debugging runs), `python -m src.loader.retry_consumer`
(standalone DLQ drain), `scripts/validate_monitoring.py`, and the
`make monitoring-up|monitoring-down|monitoring-validate` targets.

The CLI does **not** read `.env`. Export the connection variables yourself
(see [Environment variables](#environment-variables)); `docker compose` reads
`.env` for the local stack only.

## start

```text
python -m src.cli start --mode {bulk,stream} [--config PATH] [options]
```

Validation errors are logged as `stage=launch reason=...` and exit 1 before any
container is started, except where noted.

### General

| Flag | Default | Valid values / range | Applies to | Notes |
| --- | --- | --- | --- | --- |
| `--mode {bulk,stream}` | required | `bulk`, `stream` | both | `bulk` is finite and verifies a zero-lag drain; `stream` runs until SIGINT/SIGTERM. |
| `--config PATH` | `config/graph_schema.yaml` | a schema YAML | both | Validated before anything starts. |
| `--network NAME` | `graph-loader-net` | an existing Docker network | both | The network the loader containers join; it must reach Kafka and Neo4j under the `LOADER_*` addresses. |
| `--skip-image-build` | off | flag | both | Use already-built images (`make build-images`, or tags from `GRAPH_LOADER_*_IMAGE`) instead of building them at start. |
| `--run-id ID` | random UUID hex | 1-128 chars of `[A-Za-z0-9._-]`, starting with a letter or digit | both | Run identity. Scopes container names and labels, the default `--plan-dir`, the planned chunk topic, the lease server and the **bulk** relationship consumer groups. Stream groups do **not** contain it; in stream mode it only lets the CLI migrate legacy run-scoped offsets (see the stream notes below). |
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
| `--edge-classic` | off | flag | bulk only | The per-type APOC-locked loaders. The one mode that can wait on Neo4j locks; required for `native_disjoint` edges. With `--mode stream` it is an error (exit 1). |
| `--bulk-edge-overlap {labels,shared}` | `labels` | `labels`, `shared` | bulk, **classic only** | `labels` serializes types that share an endpoint label; `shared` runs them concurrently under globally ordered APOC locks and co-schedules `python_apoc` self-references. Outside classic: warning `--bulk-edge-overlap/--max-concurrent-bulk-edges tune classic only; ignored`. |
| `--max-concurrent-bulk-edges N` | `0` (unlimited) | integer >= 0 | bulk, **classic only** | Cap on concurrently running shared edge types. Ignored with a warning outside classic. |

### Planned mode tuning (`--edge-plan`, also the bulk default)

Every value is derived automatically (`src/orchestrator/auto_settings.py`); a
flag overrides it. Range errors exit 1. These flags are validated only when the
run is planned and are silently unused in lease, classic and stream runs.

| Flag | Default | Range | Applies when | Notes |
| --- | --- | --- | --- | --- |
| `--plan-writers N` | Neo4j available processors + 2 (10 if the server cannot be probed); never more than the plan's chunks | 1-64 | planned (with or without a file) | Writer containers across all types. |
| `--plan-workers N` | `min(4, CPUs)` | 1-64 | planned, no file | Planner processes reading the edge topics. |
| `--plan-buckets N` | `clamp(writers, 8, 64)`, at most `isqrt(records / (types * 500))`, at least 2 | 1-4096 | planned, no file | Hash buckets per node label. |
| `--plan-hub-degree N` | the chunk size | 0-2^40 (0 disables hubs) | planned, no file | A node with at least this total degree over all types gets its own resource. |
| `--plan-chunk-rows N` | `clamp(records / (20 * writers), 500, 5000)` | 1-100000 | planned, no file | Maximum rows per chunk transaction. |
| `--plan-partitions N` | `4` | 1-1024 | planned, no file | Partitions of the run's chunk topic. |
| `--plan-topic NAME` | `graph-loader-edge-chunks-<run id>` | a new topic name | planned, no file | The chunk topic to create. |
| `--plan-dir DIR` | `$GRAPH_LOADER_TIMING_DIR`, else `var/edge-plans/<run id>` | a directory | planned, no file | Holds `edge-plan.json` and `edge-plan.json.ledger.json`. |
| `--plan-memory-mb N` | a quarter of available RAM over the planner processes, 256-4096 MiB | 16 - 1048576 | planned, no file | Per-planner memory budget; beyond it the planner spills to `--plan-spill-dir` and produces the identical plan. |
| `--plan-spill-dir DIR` | the plan directory | a directory | planned, no file | Temporary spill files, removed afterwards; needs about the relationship topics' uncompressed size free. |
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
| `--stream-clock-rotation` | off | flag | stream | **Deprecated** (logs `stage=deprecation`): the Phase 4 global batch clock with rotating/isolated-phase scheduling instead of the default bucket-lease fleet. Refused for multi-pair types. No effect in bulk. |

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
| none given, any relationship type uses `execution.mode: native_disjoint` | refused (exit 1): pass `--edge-classic` or use `python_apoc` | |
| none given, any relationship type has an enabled `dead_letter` | `lease` | `dead_letter` |
| none given | `planned` | `default` |

Planned and lease modes have zero Neo4j lock waits by construction; classic can
wait by design. A schema without relationship types skips the mode selection.
Stream mode always uses the bucket-lease fleet (unless the deprecated
`--stream-clock-rotation`).

## stop

```text
python -m src.cli stop [--loader LABEL]
```

| Flag | Default | Notes |
| --- | --- | --- |
| `--loader LABEL` | all | Stop only the node-loader containers whose `node_label` label is `LABEL`. |

Stops containers labelled `component=node-loader` (running or not) once their
stop-signal handler is ready. It does not stop relationship loaders, the retry
consumer or the lease server: stop a stream run by sending SIGINT/SIGTERM to
the `start` process, which stops everything it launched. Exits 0; exits 1 with a clear message when Docker is unreachable.

`stop` is not scoped to a run. On a shared Docker host it stops every
graph-loader node-loader container, including other users' runs. Use
`--loader LABEL`, or stop your own run through its `start` process.

## status

```text
python -m src.cli status [--config PATH]
```

Logs one line per node-loader container (`Loader <name> (<id>): <status> [Node:
<label>]`) or `No loaders running.` `--config` (default
`config/graph_schema.yaml`) is only logged. Exits 0; exits 1 with a clear message when Docker is unreachable. For lag, use the
metrics ([MONITORING](MONITORING.md)) or Kafka's consumer-group tooling.

## replay-dlq

```text
python -m src.cli replay-dlq (--node-label LABEL | --edge-type TYPE) [options]
```

Reads the entity's DLQ topic (or parking lot) up to a captured boundary and
republishes the matching envelopes to their source topic. Every envelope is
replayed from its first DLQ copy in first-arrival order; filters see the latest
copy.

| Flag | Default | Notes |
| --- | --- | --- |
| `--config PATH` | `config/graph_schema.yaml` | Schema YAML. |
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
| `1` | Any failure: invalid flags or combinations (mutual exclusions, ranges, `--edge-classic` with stream, native_disjoint without `--edge-classic`, `tx_timeout_ms` below 6000 in lease modes), schema load or validation failure, Neo4j schema initialization failure, stale DLQ topics or leftovers, stream group members still alive, image build or launch failure, a bulk stage failing or timing out, an operator stop during a bulk run or before launch completed, a node loader dying in a stream run, a foreign member joining a stream group (`stage=consumer-groups reason=foreign_members`), a container that would not stop (bulk, and stream edge/retry containers), `--edge-plan` or `--edge-leases` with `--mode stream`, a stream schema with a multi-topic `precedence` type, a stream relationship type named like a node label, `--stream-clock-rotation` with a multi-pair type, an edge-loader or retry-consumer crash in a stream run. A stream node loader that fails to stop is only logged; the run still returns 0. `replay-dlq`: unknown entity, invalid filter, replay error. |
| `2` | Command-line usage error from `argparse` (unknown flag, missing `--mode`, invalid number, `--node-label` together with `--edge-type`). |

A nonzero bulk result makes no claim of a successful load: inspect the run's
logs and the rejection files before retrying.

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
| `stage=edge-plan-rejections plan_id=... rejected=<n> dir=...` | planner | Malformed relationship records skipped to `var/rejections/<run id>-planner-<worker>.jsonl`. |
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
| `NEO4J_URI` | `bolt://localhost:7687` | CLI (host); loaders and retry consumer (container, set by the CLI from `LOADER_NEO4J_URI`) | Bolt endpoint. |
| `NEO4J_USERNAME` | `neo4j` | CLI; passed through unchanged to containers | The CLI falls back to the legacy `NEO4J_USER` when it is unset, and passes the resolved value to containers as `NEO4J_USERNAME`. |
| `NEO4J_PASSWORD` | `changeme` | CLI; passed through unchanged to containers | Also used by `docker compose` (from `.env`) to set the local Neo4j password. |
| `NEO4J_DATABASE` | server default | every process opening sessions (CLI schema init, loaders, retry consumer, planned writers) | Target a named database; forwarded to containers when set on the host. |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | CLI (host); every container (set by the CLI from `LOADER_KAFKA_BOOTSTRAP_SERVERS`) | Kafka bootstrap. |
| `LOADER_NEO4J_URI` | `bolt://neo4j:7687` | CLI, when building container environments | Bolt URI as seen from the Docker network. |
| `LOADER_KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | CLI, when building container environments | Kafka as seen from the Docker network. |
| `LOADER_CPUSET` | unset | CLI | Docker `cpuset_cpus` pin for every loader container (keeps them off the cores of Kafka and Neo4j). |

### Container images

| Variable | Default | Read by |
| --- | --- | --- |
| `GRAPH_LOADER_NODE_IMAGE` | `graph-loader-node:latest` | CLI (node loaders) |
| `GRAPH_LOADER_EDGE_IMAGE` | `graph-loader-edge:latest` | CLI (edge loaders, planned dispatcher/writers, retry consumer, lease server) |
| `GRAPH_LOADER_CLOCK_IMAGE` | `graph-loader-clock:latest` | CLI (global clock, only with `--stream-clock-rotation`) |

`make build-images IMAGE_TAG=x` builds all three; export the matching
`GRAPH_LOADER_*_IMAGE` tags to use them.

### Run behaviour and output paths

| Variable | Default | Read by | Notes |
| --- | --- | --- | --- |
| `GRAPH_LOADER_TIMING_DIR` | unset | CLI | Host directory mounted into containers for write-timing logs, metrics snapshots (`metrics-cli.prom`) and, if set, the default `--plan-dir`. |
| `GRAPH_LOADER_TIMING_LOG_PATH` | set by the CLI | loader containers | Per-replica write-timing log path inside the container. |
| `GRAPH_LOADER_EDGE_EXPLICIT_LOCKS` | `1` | classic edge loaders (forwarded by the CLI) | `0` skips the `apoc.lock.nodes` step (benchmark experiment; deadlocks become possible). |
| `GRAPH_LOADER_PLAN_NODE_PHASE_WRITERS` | `0` (unlimited) | planned dispatcher (forwarded by the CLI) | Cap on active chunks while node labels still load. |
| `GRAPH_LOADER_RUN_ID` | set by the CLI | edge loaders, retry consumer | The run id inside a container. |
| `KAFKA_GROUP_ID` | node loaders: `<consumer_group_id>-<label>` (not set by the CLI); edge loaders and retry consumer: set by the CLI | node, edge loaders, retry consumer | Consumer group of a container. The CLI sets it for edge loaders (bulk `<prefix>-<type>-<phase run id>`, stream `<prefix>-<type>`) and the retry consumer (`<prefix>-retry-consumer`). A standalone retry consumer defaults to the legacy `graph-loader-retry-consumer-<run id>`. |
| `KAFKA_COORDINATION_GROUP_ID` | set by the CLI | edge loaders with slot gating (only the deprecated `--stream-clock-rotation` path) | Group used to read the control (coordination) topic. |
| `REJECTION_LOG_PATH` | `loading.rejection_log_path` (`var/rejections/node-loader.jsonl`) | node and edge loaders | Per-replica JSONL sink for records rejected when no DLQ is configured; the CLI mounts `var/rejections` and gives each replica its own file. |

### Metrics

See [MONITORING](MONITORING.md) for the metric catalogue.

| Variable | Default | Read by | Notes |
| --- | --- | --- | --- |
| `GRAPH_LOADER_METRICS_ENABLED` | enabled | every process (forwarded by the CLI) | `0` or `false` disables the endpoint and snapshots. |
| `GRAPH_LOADER_METRICS_PORT` | `9464` | every process (forwarded by the CLI) | Prometheus endpoint port. The CLI falls back to an ephemeral port when taken. |
| `GRAPH_LOADER_METRICS_BIND_ADDR` | `127.0.0.1` | CLI only | Set to expose the CLI's endpoint to a Prometheus container; logs a warning because the endpoint is unauthenticated. |
| `GRAPH_LOADER_METRICS_SNAPSHOT_PATH` | set by the CLI per replica | containers; CLI uses `$GRAPH_LOADER_TIMING_DIR/metrics-cli.prom` | Atomic Prometheus-text snapshot written periodically and at exit. |
| `GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S` | `15` | every process (forwarded by the CLI) | Seconds between periodic snapshot rewrites. `0` disables the periodic timer (the exit-time write still happens); non-finite values fall back to the default, values below 1 are raised to 1 and values above 86400 are clamped. |
| `GRAPH_LOADER_VERSION` | `dev` | every process (forwarded by the CLI to containers) | Reported in the process-info metric. |
| `GRAPH_LOADER_LEASE_MERGE_ROWS` | `200` | stream edge loaders and the retry consumer (forwarded by the CLI) | Small lease cells are merged under one lease up to this many rows. |
| `GRAPH_LOADER_BUCKET_LEASE_ADDR` | set by the CLI | edge loaders, node loaders, retry consumer | Lease server address. `GRAPH_LOADER_LEASE_ADDR` is a deprecated alias (logs `stage=deprecation`). |
