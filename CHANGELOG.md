# Changelog

## Unreleased

No versioned release has been published. These notes describe the current
implementation and changes from the original loader.

See [docs/CLI.md](docs/CLI.md) for every current flag,
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for how the pieces fit together and
[docs/USAGE.md](docs/USAGE.md) for the reference.

### Upgrade notes / breaking changes

Read these before upgrading from the original loader or an earlier development
build. Each item says what changed and what to do.

**Read first: `dead_letter` blocks that the original loader ignored are now live.** The original loader’s
own `config/graph_schema.yaml` has `dead_letter` blocks with `enabled: true`
that did nothing there. Now: a plain bulk start selects the lease mode (which
needs `tx_timeout_ms >= 6000`); the DLQ, parking-lot and parked-pair-index
topics are created; a stream run launches a retry consumer; and malformed
records go to the DLQ instead of the rejection file. *What to do:* review every
`dead_letter` block; set `enabled: false` to keep the original loader’s behaviour.

1. **Planned is the bulk default.** `start --mode bulk` now loads relationships
   in planned mode (`--edge-plan` with every setting derived automatically): a
   plan of node-disjoint chunks, zero lock waits. If a relationship type enables
   `dead_letter`, a plain bulk start selects the lease mode instead (also zero
   lock waits). The log says which: `stage=launch edge_mode=<mode> reason=<reason>`.
   *What to do:* nothing for `python_apoc` schemas. To keep the old behaviour
   (per-type APOC-locked loaders, the one mode that can wait on Neo4j locks) pass
   **`--edge-classic`**. A schema with a relationship type using
   `execution.mode: native_disjoint` is **refused** on a plain bulk start (neither
   zero-wait mode supports it): pass `--edge-classic` or switch the type to
   `python_apoc`. Planned and lease modes also require `python_apoc` edges.
   Planned mode ignores `dead_letter` and fails closed on a missing endpoint.
2. **`extra="forbid"` on configuration blocks.** Unknown keys are now errors
   (the message names the field) in `loading` and its nested blocks
   (`coordination`, `bucket_lease`, `retry_consumer`), `mix_and_batch`, `retry`,
   `execution`, `dead_letter` and `topics` entries. Previously they were silently
   ignored, so a misspelt setting did nothing. *What to do:* run the schema
   through `load_schema` (any `start` validates it before side effects) and
   remove or correct the keys it reports. Node, edge and property definitions
   themselves do not forbid unknown keys.
3. **Multi-topic relationship types: per-type DLQ naming.** A relationship type
   may now be fed by several topics. A single-topic type keeps `<topic>.dlq`; a
   type with two or more topics defaults to `<type>.dlq` (one DLQ, parking lot
   `<dlq>.parked` and parked-pair index `<dlq>.parked-pairs` per type);
   `dead_letter.topic` overrides both. Changing a type's topic count therefore
   changes its default DLQ name. `start` now refuses (exit 1) when a type's
   former default DLQ still holds records, or when its parked-pair index holds
   records of a topic it no longer reads or of a topic now bound to another label
   pair. *What to do:* drain the old DLQ with the previous schema
   (`replay-dlq`) and delete the old topics, or accept the leftovers on purpose
   with `--accept-former-dlq TOPIC` (repeatable) and
   `--accept-dlq-leftovers TYPE` (repeatable). The simplest remedy is to pin the
   old name with `dead_letter.topic: <old name>`: the former-DLQ check applies only
   to types without an explicit `dead_letter.topic`. Planned runs skip these checks.
4. **Stable stream consumer groups; one live fleet per prefix.** Stream groups
   are now `<consumer_group_id>-<type>` and `<consumer_group_id>-retry-consumer`,
   independent of `--run-id` (earlier builds used `<prefix>-<type>-<run id>` and
   `graph-loader-retry-consumer-<run id>`). A restart resumes from committed
   offsets with or without `--run-id`. Before launch the CLI refuses to start
   while a stable group still has members (waiting `session_timeout_ms` + 5 s, or
   `--stream-group-member-wait-seconds S`), and a running fleet stops with exit 1
   (`stage=consumer-groups reason=foreign_members`) if another member joins.
   *What to do:* stop the old fleet first, then find its run id `R` (the old
   containers' `run_id` label, or `kafka-consumer-groups --list`; the original loader used a
   random UUID per start, so it is not predictable). Fleets from the original loader (the
   clock-rotation path, groups `<R>.shared.0.<h>` / `<R>.isolated.<i>.<h>`): start
   once with `--stream-clock-rotation --run-id <R>`, then drop both flags.
   Earlier lease-fleet builds: `--run-id <R>` alone is enough. The
   CLI copies the old groups' offsets once (only offsets inside the partition's
   current range, never over an existing commit) and logs
   `reason=migrated_legacy_offsets`. Without `--run-id` nothing is migrated and a
   stable group without commits starts at the earliest offset. The legacy groups
   are no longer read and can be deleted. Never run two stream fleets with the
   same `consumer_group_id` on one cluster; give separate deployments their own
   id. Bulk lease/classic groups stay run-scoped; planned mode commits no edge
   groups.
5. **`tx_timeout_ms` must be at least 6000 in lease modes.** Leased writes are
   fenced: the Neo4j transaction timeout is `loading.bucket_lease.tx_timeout_ms`
   minus 4 s (3 s transaction-monitor slack plus 1 s BEGIN allowance) and must
   stay above 1 s. Bulk `--edge-leases` (including the dead-letter default) and
   every stream run refuse smaller values at `start`. The default is 30000.
   The checks apply to the default stream lease fleet with relationship types only:
   they are skipped under `--stream-clock-rotation` and for schemas without edges.
   Planned and classic runs keep using any value the schema allows.
6. **The `neo4j` Python driver is pinned `>=5.20,<7`.** The new
   dependencies `numpy>=1.26` and `prometheus_client>=0.20` were added.
   *What to do:* reinstall from `requirements.txt` and
   rebuild the images (`make build-images`).
7. **`--bulk-edge-overlap` and `--max-concurrent-bulk-edges` only tune classic.**
   Outside `--edge-classic` they are ignored with a warning
   (`--bulk-edge-overlap/--max-concurrent-bulk-edges tune classic only; ignored`).
   Scripts that passed them and relied on the old default now run planned: add
   `--edge-classic` if you want them honoured. `--edge-leases` still implies
   `--bulk-edge-overlap shared`.
8. **Planner rejection files.** In planned mode an invalid relationship record
   found while planning from the edge topics (bad JSON, missing required field,
   wrong type, undeclared field) no longer needs a DLQ and does not fail the run:
   it is written to `var/rejections/<run id>-planner-<worker>.jsonl` (topic,
   partition, offset, reason, edge type), counted, and excluded from the plan;
   the run still exits 0 and, when `n > 0`, logs `stage=outcome rejected=<n> ...`. Records of a
   producer-written plan (`--edge-plan PLAN_FILE`) and a missing or ambiguous
   endpoint at write time still fail the run. *What to do:* check
   `var/rejections/` (and the `rejected` count) after planned runs. Stale planner
   rejection files for the same run id are removed at start.
9. **`make monitoring-up` environment precedence.** The target sources `.env`
   itself, but variables you exported win over `.env` (`.env` only fills unset
   ones), the same rule as `docker compose`. If your shell exports connection
   variables for another stack, `monitoring-up` now honours them rather than
   `.env`. Export every variable of the stack you mean before running it.
10. **Opt-in e2e tests need a scratch database and credentials.** The
    developer-stack end-to-end tests (`test_rotation_e2e`,
    `test_streaming_lease_e2e`, `test_relationship_bulk_e2e`,
    `test_self_reference_isolation_e2e`, `test_social_planned_integration`,
    `test_schema_initializer_int`) skip unless `GRAPH_LOADER_IT_DATABASE` names a
    scratch database (refused if it is `neo4j`, `latesttesttemp` or `system`) and
    `NEO4J_PASSWORD` (or `GRAPH_LOADER_IT_NEO4J_PASSWORD`) is set; they never fall
    back to the compose default password. See USAGE section 13.
11. **Streaming no longer uses the global batch clock by default.** All
    relationship types, including self-references, run together in one fleet
    under a run-scoped bucket-lease server, which the CLI starts before the node
    loaders (stream node writes are leased too). The mixed shared/self-reference
    and multiple-self-reference stream rejections of the clock path no longer
    apply to the default fleet. The clock path is
    reachable only through the deprecated `--stream-clock-rotation`, which will be
    removed. The `--lease-buckets`, `--lease-outer-batch-size` and
    `--lease-keyed-max-nodes` flags are now hidden deprecated aliases of
    `--bucket-lease-buckets`, `--bucket-lease-outer-batch-size` and
    `--bucket-lease-keyed-max-nodes`; update scripts (an alias and its new name
    with different values is an error).
12. **Local Docker stack changes.** Without environment overrides, Compose pins Neo4j to CPUs
    `0-7` (heap 8G, page cache 12G) and Kafka to CPUs `8-11` (4g), and stores data
    under `${GRAPH_LOADER_DATA_DIR:-/mnt/graph-loader/data}` as bind mounts (Neo4j logs
    move to `${GRAPH_LOADER_DATA_DIR}/neo4j/logs`; `NEO4J_AUTH` is hard-coded to
    `neo4j/${NEO4J_PASSWORD}`) instead
    of named volumes (so `docker compose down -v` no longer removes it). Existing
    named-volume data is not migrated. *What to do:* set the `NEO4J_*`/`KAFKA_*`
    sizing and `GRAPH_LOADER_DATA_DIR` in `.env` for your machine. The current
    `.env.example` uses local `./var/data`, smaller memory values and blank CPU
    sets to disable pinning. `make
    build-images` (new) tags `:latest`, the CLI default (`IMAGE_TAG=x` plus the
    `GRAPH_LOADER_*_IMAGE` variables to override).
13. **Other visible behaviour.** All containers run with `init`, so a SIGTERM
    during startup is not lost. A node-loader, edge-loader or retry-consumer crash in a
    stream run stops the run with exit 1. A planned or bulk operator stop logs at WARNING. Stream runs
    ride out a Neo4j outage instead of exiting (bulk is unchanged).
14. **New schema validation in every mode.**
    `reconnect_timeout_ms + tx_timeout_ms + (tx_timeout_ms + restart_quiet_margin_ms)`
    must stay below `max_poll_interval_ms - 5000` (with the defaults,
    `max_poll_interval_ms` must exceed about 130000; or set
    `reconnect_timeout_ms: 0`); `tx_timeout_ms >= 10 * expected_chunk_commit_ms`;
    `heartbeat_interval_ms < session_timeout_ms`. *What to do:* adjust the values
    the error names.
15. **Stream refuses missing input topics** (exit 1 before launch). Create the
    node and relationship topics first.
16. **Stream refuses a relationship type named like a node label** (their consumer
    groups would clash). Rename the type.
17. **A uniqueness constraint is created for every node key** automatically
    (`<label>_<key>_key_unique`, lowercased). Duplicate keys in existing data make
    the DDL fail and `start` exit 1. *What to do:* deduplicate first.
18. **Bulk derives node-loader replicas** for labels without `replicas` (1 under
    10000 topic records, else one per partition up to 8). *What to do:* set
    `replicas` in the YAML, or pass `--node-replicas-min-records 0`.
19. **Stream edge batch size** comes from `--bucket-lease-outer-batch-size`
    (default 32000), which overrides `edge_unwind_batch_size`.
20. **Metrics are on by default** on port 9464: containers bind 0.0.0.0 inside
    their network namespace, the CLI binds 127.0.0.1. *What to do:* opt out with
    `GRAPH_LOADER_METRICS_ENABLED=0`.
21. **`GRAPH_LOADER_LEASE_ADDR` is a deprecated alias** of
    `GRAPH_LOADER_BUCKET_LEASE_ADDR` (logs `stage=deprecation`); update external
    scripts.
22. **Small CLI fixes.** Containers receive `NEO4J_USERNAME`, falling back to
    `NEO4J_USER`; `--bulk-timeout-seconds` and `--retry-consumer-replicas` (1-64)
    are validated before any side effect; `stop` and `status` exit 1 with a clear
    message when Docker is unreachable; `GRAPH_LOADER_VERSION` is forwarded to
    containers; the `--run-id` help text now describes its real scope.

### Streaming resilience

* **Bucket-lease streaming.** The bucket-lease coordinator becomes
  streaming's default zero-wait mechanism: settings renamed `bucket_lease_*`;
  server-side generation token; client reconnect with bounded backoff; abort,
  never commit, on a broken lease connection; supervised, restartable, run-scoped
  lease server (`bucket_lease` settings for health checks, restart count and
  backoff); stream relationship loaders and, later, stream node loaders write
  through it. The clock path is deprecated (`--stream-clock-rotation`).
* **Dead-letter queue.** Opt-in `dead_letter` block per node label and
  relationship type: envelope codec, DLQ producer and topic auto-creation, node
  and edge loaders route malformed records to the DLQ, CLI provisions DLQ topics
  before launch, and a new **`replay-dlq`** command republishes envelopes
  (`--reason-code`, `--since`, `--until`, `--limit`, `--target-topic`, `--dlq-topic`,
  `--dry-run`). Without `dead_letter` the fsync'd rejection file remains.
* **Row-level DLQ for missing endpoints.** Edge-before-node rows go to
  the DLQ row by row with a two-attempt write and `retry_after_ms` (envelope v2,
  v1 still decoded). With `dead_letter.enabled: true` only: always on in stream; in bulk classic/lease only with
  `dead_letter.route_missing_endpoints_in_bulk`; never in planned mode.
* **Retry consumer and parking lot.** A fleet-wide retry consumer
  (`--retry-consumer-replicas`, default 1) retries DLQ rows with backoff under
  leases and parks them after `dead_letter.max_retry_attempts` in a parking-lot
  topic (`replay-dlq --parked`). The parked-pair index keeps a newer record of a
  pair from being overwritten by an older parked one; `order_by` conditional-SET
  writes protect newer values; DLQ backlog gauge.
* **Stream Neo4j outage ride-out.** Leased live loaders pause, keep polling and
  retry the same batch with capped backoff; fatal-class errors still fail after
  600 s.
* **Stable stream consumer groups** and the fenced BEGIN (see upgrade notes 4
  and 5), plus `LEASE_MERGE_ROWS` passthrough and a lease restart-count reset.

### Observability and hardening

* **Metrics.** Prometheus endpoint per process (`GRAPH_LOADER_METRICS_*`,
  default port 9464), CLI/orchestrator host-side metrics, periodic atomic
  snapshots (`GRAPH_LOADER_METRICS_SNAPSHOT_*`), the `graph_loader_dlq_backlog`
  gauge (split by `queue`: `dlq` or `parked`), opt-in
  `GRAPH_LOADER_METRICS_BIND_ADDR` for the CLI endpoint.
* **Monitoring stack.** Prometheus Docker service discovery, example
  alert rules, a Prometheus + Grafana Compose profile and starter dashboard,
  `scripts/validate_monitoring.py`, `make monitoring-up|down|validate`, and
  [docs/MONITORING.md](docs/MONITORING.md).
* **Stress and regression tests.** Star-topology stress tests proving zero
  lock waits in planned and lease modes (100k and opt-in 1M tiers),
  correctness regressions across the three bulk modes.
* **Graceful shutdown.** One CLI-wide SIGINT/SIGTERM handler for bulk and
  stream, no orphaned node containers, monitors that abort promptly, normalised
  exit codes for loaders and writers, an operator guide ("Stopping a run safely"
  in USAGE).

### Planned mode, lease mode and external planner

* New bulk relationship modes: **`--edge-plan`** (globally planned node-disjoint
  chunks, dispatcher, writer pool, completion ledger, library planner with
  bounded-memory spilling, `--plan-*` tuning flags, producer-side planning API)
  and **`--edge-leases`** (bucket-lease coordinator). Auto-derived settings
  (`src/orchestrator/auto_settings.py`), derived node-loader replicas
  (`--node-replicas-min-records`, `--max-node-replicas`), progress-based
  `--bulk-timeout-seconds`.
* Classic-mode throughput work: overlap of edge types sharing labels
  (`--bulk-edge-overlap`, `--max-concurrent-bulk-edges`), single-statement
  validate-lock-merge batches, `GRAPH_LOADER_EDGE_EXPLICIT_LOCKS=0` experiment toggle.
* Node-loader lifecycle: concurrent launches with exact rollback, per-label
  readiness and non-blocking stops, commit on revoke.
* Examples: `examples/social` (exact verification), IMDb benchmark runner,
  library usage guide.
* Infrastructure: configurable `NEO4J_DATABASE`, `LOADER_CPUSET`, pinned CPUs for
  Neo4j and Kafka, Neo4j query-log rotation config, CSV metrics.

### Multi-topic and multi-pair relationship types

* **Several topics per relationship type** (`topics:`), with a conflict rule per
  type: `order_by` (newest value wins, every mode), `precedence` (later-listed
  topic wins; bulk only, refused in stream) and `arrival`. Supported in stream,
  bulk classic/lease (precedence as sequential per-topic sub-stages) and planned
  (per-task ordering).
* **One DLQ, parking lot and parked-pair index per type** with causal parked-pair
  order, `replay-dlq --source-topic`, `--accept-former-dlq` and the DLQ sizing
  rule; a warning when an existing DLQ has fewer partitions than the type's widest
  topic.
* **Several label pairs per type** (`topics` entries that bind their own
  `nodes`/key properties, e.g. `LIKES` from Customer to Post and to Comment):
  pair-split writes, pair-split lease coordinator, per-pair planner tasks and plan
  chunks, per-pair retry writers, label pair in DLQ and parked-pair identity.
  Refused with `--stream-clock-rotation`.
* **Bulk default is planned**, with `--edge-classic` (see upgrade note 1).
* Per-type lag panels and alerts, DLQ envelope and parked-pair-index admission
  panels, `examples/multi_topic` (single-pair and multi-pair schemas), docs YAML
  snippet test (`tests/test_docs_yaml_snippets.py`).

### Reliability fixes

* **Shutdown robustness:** `init=True` containers (a SIGTERM during startup is
  not lost); a `409 removal already in progress` is treated as success during
  cleanup; node loaders drain buffered work only on a graceful stop;
  `--replace-stale-plan-topic` recovers the chunk topic of a killed run; planner
  processes are terminated and spill/progress files cleaned on stop; planned
  writers exit 0 on a stop between chunks.
* **Stream:** node writes are lease-fenced against relationship writers; stream
  node loaders are supervised (a death stops the run, exit 1); stream topic
  preflight refuses to start (exit 1) when input topics are missing.
* **Retry consumer:** fixes for the writer adapter, batching and per-partition
  commits, outage handling (outages never park rows), evidence-gated write
  failure counting, pair-order duplicates and tombstones, liveness and
  resurrected-copy handling.
* **Lease:** the BEGIN is fenced so no leased transaction outlives its lease
  deadline; the fenced-budget check applies to lease modes only; 3 s monitor slack.
* **Stable stream groups** with legacy offset migration and a live-member guard.
* **Docs:** new [bulk](docs/BULK_LOAD.md) and [incremental](docs/INCREMENTAL_LOAD.md)
  load guides, then this documentation pass: README rewritten as an entry point
  with a quick start, [CLI reference](docs/CLI.md),
  [architecture](docs/ARCHITECTURE.md), [history](docs/HISTORY.md) and this
  changelog.

### Repository maintenance

* Runtime dependencies are separate from development/test tools; install
  `requirements-dev.txt` for tests and lint, or the IMDb example requirements
  alongside `requirements.txt` for its publisher and dataset builder.
* Python 3.12 is used by all loader images and the lint configuration.
* `.env.example` distinguishes host and container connections, provides local
  data and memory defaults, and removes unused settings. Blank CPU sets now
  disable Compose CPU pinning.
* Planning and implementation records and authoring workflows are retained on
  `codex/implementation-archive`. Generated `var/` output stays local.
* GitHub Actions workflows are not included in the current tree; validation
  commands are documented in [USAGE](docs/USAGE.md#13-development-and-validation).

### Known limitation

Retry-consumer resolution memory does not survive a process restart. Under
`arrival` ordering, redelivered parked records can overwrite newer values.
Declare `order_by` to prevent stale property writes; durable resolution markers
are not implemented. See [USAGE](docs/USAGE.md) for the full retry limitations.
