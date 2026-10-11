# Phases 5 and 6: functional test plan (manual and semi-manual, real local stack)

Branch under test: `feat/streaming-observability` (local only, never pushed).
Audience: the owner and the team, executing together against a real local
Kafka + Neo4j Enterprise stack with rebuilt loader images.

This plan contains test case texts only, no code. Every flag, environment
variable, metric, topic and log-stage name below was checked against the
implementation (`src/cli.py`, `src/models/schema.py`, `src/utils/metrics.py`,
`src/orchestrator/docker_service.py`, `Makefile`, `docker-compose*.yml`,
`monitoring/`). Anything not confirmed in code is marked **(verify)**.

Runner notes applied (runner1 to 3, retestA to C): Neo4j database names need at least 3 characters (use e.g. `p0runner1`, not `p0`); legacy rejection-file runs (P5-DLQ-10) append to the tracked `var/rejections/Person-0.jsonl`, so restore it with `git checkout` afterwards; the query log rotates about every 20 MB, so long bulk runs need `db.logs.query.rotation.*` settings or a timestamp window across rotated files.

Sources: `plan/v5-v6-final-report.md`, `plan/v5-v6-orchestration.md` (binding
decisions), `plan/v5/completion_slice-{1..4}.md`, `plan/v6/completion_slice-{1..5}.md`
(including their "Pending owner e2e" sections), `docs/USAGE.md`,
`docs/MONITORING.md`, `examples/social/`, `examples/imdb/`.

Contents

1. Scope, environment, data sets, universal checks
2. Test cases
   * ENV: environment and build
   * P5-LEASE: P5-S1 streaming via bucket leases
   * P5-DLQ: P5-S2 malformed-record DLQ
   * P5-ROW: P5-S3 row-level DLQ for missing endpoints
   * P5-RETRY: P5-S4 retry consumer and parking lot
   * P6-CI: P6-S1 CI
   * P6-MET: P6-S2 metrics
   * P6-MON: P6-S3 monitoring stack
   * P6-STRESS: P6-S4 stress and regression suite
   * P6-SHUT: P6-S5 graceful shutdown
   * REG: cross-cutting regressions
3. Traceability table, execution order, time estimates
4. Known gaps and observations to raise with the owner

---

## 1. Scope, environment, data sets, universal checks

### 1.1 Scope

In scope: everything Phases 5 and 6 added on `feat/streaming-observability`:

* P5-S1 zero-wait streaming relationship writes via bucket leases (default in
  `--mode stream`, self-referencing types included, supervised lease server,
  time-based fencing, deprecated `--stream-clock-rotation`).
* P5-S2 malformed-record DLQ (`dead_letter` block, `<topic>.dlq`, envelope,
  `replay-dlq`).
* P5-S3 row-level DLQ for missing or ambiguous relationship endpoints.
* P5-S4 retry consumer, parking lot (`<topic>.dlq.parked`), parked-pair index
  (`<topic>.dlq.parked-pairs`), `order_by`, `graph_loader_dlq_backlog`.
* P6-S1 CI, P6-S2 metrics, P6-S3 monitoring stack, P6-S4 stress and regression
  tests, P6-S5 graceful shutdown.

Out of scope: pushing to GitHub (CI has never run remotely), production
hardening of the monitoring stack, Docker Desktop (macOS/Windows) behaviour
(the lease readiness probe assumes Linux bridge networking; known limitation),
throughput benchmarking beyond the indicative comparisons named below.

Priorities: **P0** must pass before the phases are called done; **P1** should
pass, a failure needs a triage note; **P2** nice to have or documented edge.

### 1.2 Test environment

| Item | Requirement |
| --- | --- |
| Host | Linux with Docker; the compose file pins CPUs by default: `NEO4J_CPUSET=0-7`, `KAFKA_CPUSET=8-11`, loaders `LOADER_CPUSET` (for example `12-31`), monitoring `MONITORING_CPUSET=28-31`. Check `nproc` (the P6-S4 measurements were taken on a 32 vCPU host). On a smaller host, shrink the cpusets so they do not overlap and record the values in the run log. |
| Compose services | `docker compose up -d neo4j kafka` (also `kafka-ui`, useful for inspecting DLQ topics). Container names: `graph-loader-neo4j`, `graph-loader-kafka`, `graph-loader-kafka-ui`; network `graph-loader-net`. |
| Neo4j | Enterprise, `neo4j:5-enterprise` floating tag in `docker-compose.yaml` (5.26 expected). **Minimum supported 5.23** (scoped `CALL (vars) { ... }` subqueries); 5.21 is known to fail in the classic and lease writers. APOC plugin needed for the classic and lease writers. |
| Query log | The compose file does not enable per-query logging. For lock-wait checks enable the Neo4j query log with a zero threshold and verbose mode (settings `db.logs.query.enabled=VERBOSE` and `db.logs.query.threshold=0`; the slice reports wrote them as `dbms.logs.query.*`, so **(verify)** the exact name for the running 5.26 image; via env this is `NEO4J_db_logs_query_enabled` and `NEO4J_db_logs_query_threshold`). The log is `query.log` in the mounted logs directory `${GRAPH_LOADER_DATA_DIR:-/mnt/graph-loader/data}/neo4j/logs`. Recreate the `neo4j` container after adding the settings. |
| Images | Run `make build-images` (builds `graph-loader-{node,edge,clock}:latest`, the CLI default, after the gap fix; `IMAGE_TAG=x make build-images` plus the `GRAPH_LOADER_*_IMAGE` overrides for another tag). Pass `--skip-image-build`, or omit `--skip-image-build` so the CLI rebuilds the `:latest` tags itself. Several slices changed loader code, so stale images invalidate every result. Record `docker image ls` ids in the run log. |
| Python | 3.12 on the host with `pip install -r requirements.txt` (the checked-in `venv` is Python 3.9 and cannot import the CLI; use a 3.12 venv). |
| Env vars | `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`, `NEO4J_URI=bolt://localhost:7687`, `NEO4J_USERNAME`, `NEO4J_PASSWORD` (set from your own shell; do not paste into the run log), `LOADER_KAFKA_BOOTSTRAP_SERVERS=kafka:29092`, `LOADER_NEO4J_URI=bolt://neo4j:7687`. Optional per test: `NEO4J_DATABASE` (target database for host and containers), `LOADER_CPUSET`, `GRAPH_LOADER_TIMING_DIR` (one directory per run), `GRAPH_LOADER_METRICS_ENABLED`, `GRAPH_LOADER_METRICS_PORT` (default 9464), `GRAPH_LOADER_METRICS_BIND_ADDR` (CLI only, default `127.0.0.1`), image overrides above. |
| Isolation per test | Use a fresh Neo4j database per test (`CREATE DATABASE <name> WAIT` in `system`; Enterprise supports several databases; drop afterwards), a fresh `loading.consumer_group_id` in the schema copy (topic delete does not clear group offsets), and unique `--run-id` values. Reset procedure **R**: stop the run and sweep containers (UC-3), `python examples/social/social_graph.py reset`, delete any `*.dlq`, `*.dlq.parked`, `*.dlq.parked-pairs` topics the test created, and change `consumer_group_id`. |

Scratch schemas live outside the repository (for example a `$T` directory under
the session scratch area) and are never committed. Variants used below:

| Name | Definition |
| --- | --- |
| **S-PLAIN** | `examples/social/graph_schema.yaml` unmodified (six node labels; six edge types including self-referencing `FRIEND_OF`; no `dead_letter` blocks). Only `consumer_group_id` may be changed. |
| **S-DLQ-EDGES** | S-PLAIN plus `dead_letter: {enabled: true, max_retry_attempts: 3}` on every edge type, and a short `retry` block with a small `base_delay_seconds` (the retry consumer's backoff source; **(verify)** which of `edges[].retry.base_delay_seconds` or another setting drives the DLQ `retry_after_ms`, read `docs/USAGE.md` section 3 and the envelope). |
| **S-DLQ-NODES** | S-PLAIN plus `dead_letter: {enabled: true}` on `Person` and `Company` nodes and on `WORKS_AT` and `BOUGHT`. |
| **S-ORDER** | S-DLQ-EDGES where `FRIEND_OF` declares `updated_at: {type: datetime}` and `order_by: updated_at`, and `WORKS_AT` keeps pair-parking (no `order_by`). |
| **S-BULKDLQ** | S-PLAIN plus `dead_letter: {enabled: true, route_missing_endpoints_in_bulk: true}` on `WORKS_AT` only. |
| **S-TXT** | S-PLAIN with `loading.bucket_lease.tx_timeout_ms` and related fields set per the validation cases. |

### 1.3 Data sets

| Set | How | Use |
| --- | --- | --- |
| **Social small** | `social_graph.py publish --persons 2000` (about 32k records, 30,456 relationships after last-write-wins per the P5-S1 evidence) | Most streaming and DLQ tests |
| **Social medium** | `--persons 20000` (323,252 relationship records, 13,940 repeated pairs) | Lease throughput, lease kill under load, bulk comparisons |
| **Social large** (optional) | `--persons 150000` (2.43M records) | Long-running shutdown phases, nightly-like soak |
| **IMDb 20k / 40k** | `examples/imdb/data/subsets/titles-20000-depth-4` and `titles-40000-depth-4` already present locally; load with `publish_and_load_nodes.py --edge-plan` (Quick start in `examples/imdb/README.md`); first run `./scripts/cleanup_imdb_local_load.sh` | Bulk regression, shutdown phases, metrics totals in the report |
| **Hand-crafted records** | Published with a small producer (or the example's `node_record` and `edge_record` helpers so the wire shape stays exact): node records are flat JSON objects of the declared properties; edge records are `{"source": {<key prop>: v}, "target": {<key prop>: v}, "properties": {...}}`; the Kafka key is the node key (nodes) or source key (edges). Catalogue: HC-1 valid Person; HC-2 malformed JSON bytes; HC-3 Person missing required `name`; HC-4 wrong type (`age` as a string); HC-5 undeclared field; HC-6 empty payload; HC-7 edge whose source Person does not exist yet; HC-8 edge whose target Company does not exist; HC-9 edge missing both; HC-10 two Person nodes sharing one key value (ambiguous endpoint; requires a schema without the uniqueness constraint on the key, or a label without a constraint, **(verify)** how to create a duplicate-key situation since `Person.person_id` has a uniqueness constraint; use `Company`, which has none, as the ambiguous target). | DLQ and retry cases |

### 1.4 Universal checks (referenced by ID from the test cases)

**UC-1 Exact graph.** Social: `python examples/social/social_graph.py verify --persons N [--seed S] --database <db>` must print `"exact_match": true` with an empty `problems` list and exit 0 (use the same `--persons` and `--seed` as `publish`). For two loads compare with `--against-database <other>` (`graphs_identical: true`). `expect --persons N` prints the expected node and relationship counts without touching Neo4j. For hand-crafted data: explicit Cypher counts and property reads named in the case. IMDb: per-label and per-type counts plus a property fingerprint identical to a classic-mode load into a second database (**verify** which fingerprint routine was used for the documented 40k comparison; if none is scripted, use counts plus an aggregate hash over key properties, for example with an APOC hashing function).

**UC-2 Zero lock waits.** Record the `query.log` line count at test start. At the end: `grep -o 'waiting: [0-9]*' query.log` over lines after the mark, `sort | uniq -c`; only `waiting: 0` may appear, and the number of matched lines must be above a sanity floor (a log that is empty or unreadable proves nothing: at least as many lines as write transactions expected). While the run is live, poll `SHOW TRANSACTIONS YIELD transactionId, currentQuery, status, waitTime` about every 0.5 s (plain `SHOW TRANSACTIONS` returns no `waitTime` column, which is why the `YIELD` is mandatory) and count rows whose `status` starts with `Blocked` or whose `waitTime` is non-zero; the expected count is 0. Applies to planned, bucket-lease (bulk and stream) and retry writes. It does not apply to classic and shared-overlap modes, where waits are expected (used there only as a positive control).

**UC-3 Orphan-container check.** `docker ps -a --filter label=app=graph-loader` must print no rows after a run has ended, and only the expected components while it runs. Narrow with `--filter label=component=<kind>` (`node-loader`, `edge-loader`, `bucket-lease-server`, `retry-consumer`, `planned-dispatcher`, `planned-edge-writer`, `global-batch-clock`) or `--filter label=run_id=<id>` (node and edge loaders carry `run_id` only if `--run-id` was passed). Cleanup after a failed test: `docker rm -f $(docker ps -aq --filter label=app=graph-loader)`.

**UC-4 Offsets and lag.** In the Kafka container run `kafka-consumer-groups --bootstrap-server kafka:29092 --list` and `--describe --group <id>` for `<consumer_group_id>-<Label>` node groups and the edge groups (**verify** edge group naming from `--list`). At quiescence every partition shows LAG 0 and `CURRENT-OFFSET == LOG-END-OFFSET`. For DLQ topics use `kafka-get-offsets` (or `kafka-run-class kafka.tools.GetOffsetShell`, **verify**) to compare high watermarks before and after. "Offset committed only after the durable outcome" is checked by interrupting the durable step (DLQ unavailable, lease loss) and seeing `CURRENT-OFFSET` stay put.

**UC-5 Log hygiene.** `docker logs <container>` and the CLI output contain no `Traceback`, and every `ERROR` line is explained by the test's injected fault. Note the first `stage=...` line per component for the record.

**UC-6 Metrics sanity.** For a live component: `docker port <container> 9464` gives the host mapping (`127.0.0.1:<ephemeral>`); `curl -s 127.0.0.1:<port>/metrics` returns the catalogue. For the host CLI: `127.0.0.1:9464` (or the port in its `stage=metrics-ready kind=cli` log line if 9464 was taken).

### 1.5 Conventions

* "Stream run to quiescence" means: start with `--mode stream`, wait until UC-4 shows lag 0 on every group and the Neo4j counts stop changing for at least 30 s, then stop with a single SIGTERM to the `start` process. A stream run supervising its fleet exits `0` on a graceful stop.
* All `start` commands take `--config <schema> --run-id <id>` and, when using prebuilt images, `--skip-image-build`. Do not pass `--edge-plan`, `--edge-leases` or bulk-only flags with `--mode stream` (the CLI rejects them).
* Record for every case: pass or fail, run id, schema variant, image ids, notes. A failed case keeps the container logs (`docker logs` before removal) and the `GRAPH_LOADER_TIMING_DIR` contents.

---

## 2. Test cases

Each case: **ID, title (priority)**; *Pre* (preconditions beyond the group
setup); *Steps*; *Expect*; *Pass*.

### 2.0 ENV: environment and build

**Group setup:** none. Do these first; they gate everything else.

**ENV-01 Local stack starts (P0).**
*Steps:* `docker compose up -d neo4j kafka kafka-ui`; wait until Neo4j accepts Bolt and Kafka lists topics. In Neo4j run `CALL dbms.components()` and `SHOW PROCEDURES YIELD name WHERE name STARTS WITH 'apoc.lock'`.
*Expect:* Neo4j Enterprise version 5.23 or later (5.26 expected); `apoc.lock.nodes` listed; `graph-loader-net` exists.
*Pass:* versions and procedures as stated; versions recorded in the run log.

**ENV-02 Query log enabled with zero threshold (P0).**
*Steps:* apply the query-log settings (section 1.2), recreate `neo4j`, run a trivial write query, look for it in `query.log`.
*Expect:* the query appears with a `waiting:` field.
*Pass:* UC-2 is measurable; record the exact setting names that worked (resolves the **(verify)** in section 1.2).

**ENV-03 Images rebuilt and tags wired (P0).**
*Steps:* `make build-images`; no image overrides are needed (default tags are `:latest`). Confirm with `docker inspect` the entrypoints: node `src.loader.node_loader`, edge `src.loader.edge_loader`, clock `src.orchestrator.clock_runner`.
*Expect:* three images built; entrypoints as listed (also the entrypoint of the retry consumer is `python -m src.loader.retry_consumer` run from the edge image).
*Pass:* no build errors; a later `start --skip-image-build` launches containers from these ids (`docker ps` image column).

**ENV-04 Python environment (P1).**
*Steps:* in the 3.12 venv run `python -m src.cli start --help` and `python -m src.cli replay-dlq --help`.
*Expect:* help lists `--edge-leases`, `--bucket-lease-buckets`, `--bucket-lease-outer-batch-size`, `--bucket-lease-keyed-max-nodes`, `--retry-consumer-replicas`, `--stream-clock-rotation`, `--edge-plan`, `--run-id`, `--skip-image-build`, `--bulk-timeout-seconds`; and for `replay-dlq`: `--node-label|--edge-type`, `--reason-code`, `--since`, `--until`, `--limit`, `--target-topic`, `--dlq-topic`, `--parked`, `--dry-run`. The old `--lease-*` names are hidden from help.
*Pass:* all names present.

**ENV-05 Minimum Neo4j version behaviour (P2).**
*Pre:* a throwaway `neo4j:5.21-enterprise` container on its own network (never the shared one).
*Steps:* run the smallest classic-lease bulk write (or the P6-S4 correctness regression) against it.
*Expect:* a Cypher syntax error on the `CALL (vars) { ... }` scoped subquery, matching `docs/USAGE.md` section 2; planned mode has no such lower bound.
*Pass:* behaviour matches the documented minimum (5.23).

**ENV-06 Stream topic preflight (P0).**
*Pre:* S-DLQ-EDGES schema; delete or never create one relationship topic (and, in a second variant, one node topic); Neo4j and Kafka up.
*Steps:* `start --mode stream --skip-image-build` with the topic missing; then create only the missing topics (producer-side, with its chosen partition count) and rerun.
*Expect:* the first run exits 1 within seconds with `stage=launch reason=missing input topics: <names>. Create or publish them first ...` naming every missing node and relationship topic; no container is created (UC-3 empty, no lease server), no DDL applied, no DLQ/control/retry topic created, and the missing topics are not auto-created. The rerun starts normally. `--mode bulk` has no preflight (unchanged behaviour). A Kafka outage gives `topic preflight failed: could not list topics` and rc 1.
*Pass:* rc 1, no launch, message actionable; no `relationship topic metadata unavailable` loader failure.

---

### 2.1 P5-LEASE: P5-S1 streaming via bucket leases

**Group setup:** fresh database and group id per case; Social small unless
stated; S-PLAIN unless stated. Enable the query log (ENV-02).

**P5-LEASE-01 Default stream mode uses the lease fleet (P0).**
*Steps:* publish Social small; `start --mode stream --config S-PLAIN --run-id lease-1`. Watch CLI output and `docker ps`.
*Expect:* a lease server container `graph-loader-lease-<token>` (component `bucket-lease-server`) is created before the node loaders (`docker ps`/`docker events`); on a successful stream run the CLI logs no `stage=lease-server` or `stage=lease-relationship-fleet` failure line (both are emitted only on failure in stream mode; `stage=lease-server address=...` is the bulk `--edge-leases` line). The informational `stage=lease-server-stats ...` line printed at shutdown is expected and is not a failure line; no `graph-loader-clock-*` container; edge loader containers for all six edge types run concurrently; no `stage=isolation ... mixed-stream` failure.
*Pass:* lease server present, clock absent, no isolation rejection.

**P5-LEASE-02 Unmodified social schema (shared plus self-referencing FRIEND_OF) in one run, exact graph (P0).**
*Pre:* S-PLAIN. Nodes and edges are published together, so the documented node/edge race may make an edge loader exit with a missing-endpoint error before its nodes have landed (fleet-fatal, see P5-LEASE-19); if so, rerun the identical command once and record it.
*Steps:* stream run to quiescence; SIGTERM the CLI; run UC-1 (`verify --persons 2000`), UC-2, UC-3, UC-4.
*Expect:* `exact_match: true`; only `waiting: 0`; no `Blocked` transactions sampled; no containers left except (per the documented gap) none in a stream run with edges; all lags 0; CLI exit code 0.
*Pass:* all universal checks pass.

**P5-LEASE-03 Same as 02 with medium scale (P1).**
*Steps:* Social medium; otherwise as 02. Note wall time from start to lag 0.
*Expect:* same as 02; lease server stats line (`stage=lease-server-stats`) at shutdown shows grants greater than 0 and `disconnect_releases=0`.
*Pass:* exact graph, zero waits, stats present.

**P5-LEASE-04 Leases really engaged, not bypassed (P1).**
*Steps:* during 02/03 scrape the lease server container's `/metrics` (UC-6) and read `graph_loader_lease_grants_total{kind="lease-server"}`, `graph_loader_lease_held_resources`, `graph_loader_lease_waiters`; also check the shutdown stats line.
*Expect:* grants greater than 0; `waited_grants` (lease-level queueing) may be nonzero and is not a Neo4j lock wait.
*Pass:* grants greater than 0 while UC-2 stays clean.

**P5-LEASE-05 No `apoc.lock.nodes` on the streaming path (P1).**
*Steps:* after 02, search `query.log` for `apoc.lock.nodes` for the stream window.
*Expect:* no occurrence from stream edge loaders.
*Pass:* zero occurrences.

**P5-LEASE-06 Last-write-wins for repeated pairs in lease mode (P0).**
*Pre:* Social medium contains 13,940 repeated pairs whose final properties differ from the first record (`social_graph.py expect` prints `last_write_wins` evidence).
*Steps:* stream run to quiescence; `verify --persons 20000`.
*Expect:* `exact_match: true`, meaning every repeated pair carries the properties of the last published record (property-level comparison, not just counts). Also repeat with the bulk lease mode (`--mode bulk --edge-leases`, fresh database and group) and compare.
*Pass:* both runs exact; this covers the fixed `sort_rows=False` behaviour (P6-S4 finding 1).

**P5-LEASE-07 Lease-server kill mid-stream, supervised restart (P0).**
*Pre:* Social medium published; stream started; edges in flight (watch relationship count rising).
*Steps:* `docker kill` the lease server container while edge writes are running. Note time. Leave the CLI running.
*Expect:* CLI logs `stage=lease-server-restart run_id=... attempt=1/5`; a new lease container with the same DNS-safe name appears; edge loaders reconnect with backoff; no grant is issued until the quiet period `Q = tx_timeout_ms + restart_quiet_margin_ms` (default 30 s + 5 s = 35 s) has passed. Depending on timing, an in-flight write logs `lease connection broke before commit; aborting instead of committing`; that edge container may then exit, which is fleet-fatal by design (P5-LEASE-19), in which case go to P5-LEASE-09.
*Pass:* restart line observed; if the fleet survived, the run reaches lag 0 and UC-1 passes; UC-2 shows 0 waits throughout, including the restart window.

**P5-LEASE-08 Quiet period observed after restart (P0).**
*Steps:* as 07, timestamp the restart line and the first lease grant of the new generation (new container's log or metrics: `graph_loader_lease_grants_total` stays 0 until the quiet period elapses; the counter resets to 0 on restart).
*Expect:* first grant no earlier than `tx_timeout_ms + restart_quiet_margin_ms` after the new server starts.
*Pass:* measured gap at least Q (35 s by default); no lock wait in `query.log` around the restart.

**P5-LEASE-09 Whole-process recovery after fleet exit: no lost or duplicated writes (P0).**
*Steps:* if 07 took the fleet down (edge container exit, CLI exits nonzero), rerun the same `start` command with the same schema and group id and the same topics.
*Expect:* the run resumes from committed offsets and completes; final `verify` exact; relationship counts equal expected (no duplicates since writes are idempotent `MERGE`).
*Pass:* UC-1, UC-2 (for the resumed run), UC-4 pass.

**P5-LEASE-10 Kill between PING and commit (P1).**
*Pre:* there is no fault-injection hook, so the window is hit statistically. Use Social medium with a high write rate (larger `--bucket-lease-outer-batch-size`) so writes are long.
*Steps:* repeat the kill of P5-LEASE-07 at least 10 times across separate runs, at random moments during sustained writes, each followed by recovery to quiescence. Grep edge container logs for the abort message.
*Expect:* across the runs at least one `aborting instead of committing` occurrence is desirable (not guaranteed); in every run the graph ends exact and query log waits are 0.
*Pass:* every run passes UC-1 and UC-2; report how many runs hit the abort path.

**P5-LEASE-11 Writer rollback on lease loss (P1).**
*Steps:* while an edge write is open, break the lease connection by `docker kill` of the lease server during a large-batch write; then inspect Neo4j for partial data of that batch before recovery (count relationships of the affected type just after the abort, before the rerun).
*Expect:* the aborted transaction left nothing partial (the type's count only grows in whole committed chunks); the Kafka offset for that batch did not advance (UC-4 `CURRENT-OFFSET` unchanged for the edge group at abort time).
*Pass:* no partial batch visible; offsets not advanced.

**P5-LEASE-12 Client reconnect and bounded backoff (P1).**
*Pre:* schema copy with `loading.bucket_lease.reconnect_timeout_ms: 60000` (default in code is 60000; the repository's `config/graph_schema.yaml` now also sets 60000).
*Steps:* `docker pause` the lease server for 20 s, then `docker unpause`; watch edge loader logs.
*Expect:* clients retry with growing delay (capped) and resume without process exit; no writes without a lease during the pause; no lock waits.
*Pass:* fleet survives the pause; graph exact after quiescence.

**P5-LEASE-13 Restart limit fails closed (P2).**
*Pre:* `max_consecutive_restarts: 2` and `restart_backoff_base_ms: 500`.
*Steps:* kill the lease server three times quickly (each time it comes back).
*Expect:* after more than the limit, the run fails closed with an error naming the restart limit; containers are cleaned up (UC-3).
*Pass:* clean failure, nonzero exit, no orphans.

**P5-LEASE-14 SIGTERM while the lease server is restarting or connecting (P2).**
*Steps:* kill the lease server, then send SIGTERM to the CLI during its backoff or quiet period.
*Expect:* the supervision sleep is interrupted; clean shutdown; no orphans.
*Pass:* exit within the documented bound; UC-3 clean.

**P5-LEASE-15 Deprecated `--stream-clock-rotation` still works and warns (P1).**
*Pre:* a schema copy without the self-referencing `FRIEND_OF` edge type (the deprecated path still refuses mixed shared plus self-referencing schemas with `stage=isolation ... mixed-stream`).
*Steps:* `start --mode stream --stream-clock-rotation --config <copy>`; run to quiescence. Separately run the same command with S-PLAIN.
*Expect:* for the copy: a `graph-loader-clock-*` container, a `stage=clock`/rotation flow, exact graph for the remaining types, and a deprecation warning. For S-PLAIN: the documented isolation rejection, fleet not started, node loaders cleaned up (P6-SHUT-12 covers the cleanup).
*Pass:* the deprecated path works on the supported schema and rejects the mixed one; a deprecation message is printed (**verify** the exact wording; `--stream-clock-rotation` help says DEPRECATED, the log line `stage=deprecation` exists for the renamed `--lease-*` flags, so check whether the clock flag also logs it).

**P5-LEASE-16 Renamed `bucket_lease_*` flags and deprecated aliases (P1).**
*Steps:* three stream runs (or bulk `--edge-leases` runs on a tiny set): (a) `--bucket-lease-buckets 8 --bucket-lease-outer-batch-size 16000 --bucket-lease-keyed-max-nodes 500`; (b) the old `--lease-buckets 8 --lease-outer-batch-size 16000 --lease-keyed-max-nodes 500`; (c) old and new together with different values.
*Expect:* (a) no deprecation lines; (b) works, one `stage=deprecation flag=--lease-buckets superseded_by=--bucket-lease-buckets` line per used old flag (logged once per process per flag); (c) the new flag's value wins (**verify** the precedence and whether a conflict is reported).
*Pass:* aliases functional and warned; `--help` hides the old names.

**P5-LEASE-17 Old lease address environment variable warns (P2).**
*Steps:* start an edge loader or the lease client path with `GRAPH_LOADER_LEASE_ADDR` set (instead of `GRAPH_LOADER_BUCKET_LEASE_ADDR`), for example through the bulk `--edge-leases` flow with an externally started lease server (**verify** how to exercise this manually; otherwise mark unit-covered).
*Expect:* `stage=deprecation flag=GRAPH_LOADER_LEASE_ADDR superseded_by=GRAPH_LOADER_BUCKET_LEASE_ADDR`.
*Pass:* warning observed, or case marked unit-covered.

**P5-LEASE-18 Transaction timeout configuration validation (P1).**
*Steps:* for each schema copy run `start ... --mode stream` and note whether the schema is rejected at load time before any container starts: (a) `tx_timeout_ms: 5000` with `expected_chunk_commit_ms: 1000` (violates the 10x rule); (b) `restart_quiet_margin_ms: 1000` (below the 2000 floor); (c) `tx_timeout_ms: 500` (below 1000); (d) `restart_backoff_max_ms` below `restart_backoff_base_ms`; (e) `reconnect_timeout_ms: 300000` with default `max_poll_interval_ms: 300000` (budget must stay under `max_poll_interval_ms - 5000`); (f) `reconnect_timeout_ms: 0` (allowed: explicit unbounded); (g) a boolean where an integer is expected.
*Expect:* (a) to (e) and (g) rejected with a clear message and no containers started; (f) accepted.
*Pass:* every invalid case fails fast at schema load.

**P5-LEASE-19 Known limitation: any edge container exit is fleet-fatal (P2).**
*Steps:* `docker kill` one edge loader container of a healthy stream run.
*Expect:* the CLI tears the whole fleet down (all edge loaders, lease server, retry consumer if present, node loaders per P6-SHUT-12) and exits nonzero; a rerun resumes cleanly.
*Pass:* behaviour as documented (not a defect for this release); record for the owner.

**P5-LEASE-20 Lease path and Kafka poll liveness (P2).**
*Steps:* with the lease server frozen (`docker pause`) for longer than `reconnect_timeout_ms`, watch a stream edge loader.
*Expect:* the client gives up after its bound and the loader exits rather than being evicted silently from its consumer group after `max_poll_interval_ms`.
*Pass:* bounded failure; no silent stall.

**P5-LEASE-21 Throughput comparison lease vs clock (indicative, P2).**
*Pre:* a schema copy without `FRIEND_OF` (the clock path cannot run the mixed schema).
*Steps:* run the same Social small or medium data through default leases and through `--stream-clock-rotation`; record time from start to lag 0.
*Expect:* the lease path is much faster (P5-S1 evidence: 8.92 s for 30,456 relationships including FRIEND_OF vs more than 66 s unfinished on the clock path). Figures are indicative only.
*Pass:* lease path not slower than the clock path; numbers recorded.

**P5-LEASE-22 Metrics name continuity across a supervised restart (P2).**
*Steps:* scrape the lease server `/metrics` before and after the kill in 07.
*Expect:* `graph_loader_lease_*` counters reset to 0 on the new generation (documented); with `GRAPH_LOADER_TIMING_DIR` set, per-restart snapshots `metrics-lease-server-<run>-r<attempt>.prom` exist (none after a hard `docker kill` for the killed generation).
*Pass:* behaviour as documented.

---

### 2.2 P5-DLQ: P5-S2 malformed-record DLQ

**Group setup:** S-DLQ-NODES for node and edge cases; a fresh stream run (or bulk, classic) per case; hand-crafted records HC-1 to HC-6. Topic under test for Person: `social-person` (DLQ: `social-person.dlq`); for WORKS_AT: `social-works-at` (DLQ: `social-works-at.dlq`). Use `kafka-console-consumer --from-beginning` on the DLQ topic. Decode envelopes (canonical JSON with base64 payload fields) or use `src.loader.dead_letter.decode_envelope` in a short Python snippet.

**P5-DLQ-01 Topic auto-provisioned before launch, retention unlimited (P0).**
*Pre:* DLQ topics do not exist.
*Steps:* `start`; before containers launch check topics. `kafka-configs --describe --topic social-person.dlq` and the edge equivalent.
*Expect:* topics created by the CLI immediately after schema load (before DDL, planner and image builds); `retention.ms=-1`, `retention.bytes=-1`, `max.message.bytes=8454144`. (Planned mode, that is an explicit `--edge-plan`, does not create edge DLQ topics. A plain `start --mode bulk` with any relationship `dead_letter.enabled: true` switches to bucket-lease mode, `stage=launch edge_mode=lease reason=dead_letter`, and does provision the edge DLQ topics; see P5-DLQ-16.)
*Pass:* topics and configs as stated.

**P5-DLQ-02 Malformed JSON node record routed with correct envelope (P0).**
*Steps:* publish HC-1 (valid) then HC-2 (malformed JSON) then another valid Person to `social-person`; run stream to quiescence.
*Expect:* the valid Persons are in Neo4j; exactly one envelope in `social-person.dlq` with `reason_code=invalid_json`, `entity_kind`/`entity_name` for `Person`, `original_topic=social-person`, the true `original_partition`/`original_offset`, `value_bytes` byte-identical to what was sent (base64 decode), `attempt_count=0`, populated `loader_identity`, timestamps; envelope schema version 2.
*Pass:* fields verified byte for byte.

**P5-DLQ-03 Missing required field (P0).**
*Steps:* publish HC-3. *Expect:* one envelope, `reason_code=missing_required` (**verify** exact code string); other records unaffected. *Pass:* as stated.

**P5-DLQ-04 Wrong type (P0).**
*Steps:* HC-4 (`age` as a string). *Expect:* `reason_code=invalid_value`. *Pass:* as stated.

**P5-DLQ-05 Undeclared field (P0).**
*Steps:* HC-5. *Expect:* `reason_code=undeclared_property`. *Pass:* as stated.

**P5-DLQ-06 Empty payload (P2).**
*Steps:* HC-6 (null value). *Expect:* envelope with `reason_code=empty_payload` (**verify**); `replay-dlq` skips it with a warning. *Pass:* as stated.

**P5-DLQ-07 Malformed relationship record (P0).**
*Steps:* on `social-works-at` publish an invalid-JSON record and a record missing the `source` section, plus valid edges.
*Expect:* two envelopes in `social-works-at.dlq` (`entity_kind` edge, `entity_name=WORKS_AT`): the invalid-JSON record with `reason_code=invalid_json`, the record missing `source` with `reason_code=invalid_value` (a missing section is a malformed record, not a `missing_*` endpoint code); valid edges written; no `MissingRelationshipEndpoint`-class treatment (failure_class `malformed`, `retry_after_ms` unset).
*Pass:* as stated.

**P5-DLQ-08 Offsets commit only after DLQ ack; DLQ unavailable fails closed (P0).**
*Pre:* S-DLQ-NODES with the DLQ topic **deleted after provisioning** (or a topic whose produce fails, for example delete `social-person.dlq` with auto-create disabled in the loader producer; the loader never auto-creates DLQ topics). Start the node loader directly (or the CLI with provisioning bypassed: create the DLQ topic, start, then delete it).
*Steps:* publish HC-2 after the DLQ topic is gone; watch the node loader and `kafka-consumer-groups --describe`.
*Expect:* the loader logs a DLQ publish failure, exits or stalls, and `CURRENT-OFFSET` stays at the malformed record; the topic is not auto-created. Recreate the topic with correct retention and rerun: the record is DLQ'd and the offset then advances.
*Pass:* no offset advance without a DLQ ack; no auto-created topic.

**P5-DLQ-09 Existing DLQ topic violating policy fails `start` closed (P1).**
*Pre:* create `social-person.dlq` by hand with default 7-day retention.
*Steps:* `start`.
*Expect:* `start` fails before any container launches with `DLQ topic ... already exists but violates the DLQ policy`; UC-3 empty.
*Pass:* clean fail-closed. Then fix the topic (`retention.ms=-1`, `retention.bytes=-1`) and confirm `start` proceeds.

**P5-DLQ-10 Legacy behaviour without dead_letter block unchanged (P0).**
*Pre:* S-PLAIN and, separately, the repository `config/graph_schema.yaml` shape (blocks present but `enabled: false`).
*Steps:* publish HC-2 to a node topic and run.
*Expect:* no `*.dlq` topic is created; the rejection is written to `rejection_log_path` (JSONL with topic, partition, offset and reason, no payload); loaders continue.
*Pass:* byte-for-byte legacy behaviour; `kafka-topics --list` shows no DLQ topics.

**P5-DLQ-11 Typo in `dead_letter` block rejected (P2).**
*Steps:* `enable: true` instead of `enabled: true`; `route_missing_endpoints_in_bulk: true` with `enabled: false`; `max_retry_attempts: 0` and `1001`; `parked_topic` without `enabled`; DLQ `topic` equal to another entity's topic.
*Expect:* schema load errors, no containers.
*Pass:* every invalid block rejected.

**P5-DLQ-12 `replay-dlq --dry-run` does not produce (P0).**
*Pre:* at least two envelopes in `social-person.dlq` (P5-DLQ-02/03).
*Steps:* record high watermarks of `social-person` and `social-person.dlq`; `python -m src.cli replay-dlq --config <schema> --node-label Person --dry-run`; recheck watermarks and `kafka-consumer-groups --list`.
*Expect:* log shows matched count, replayed 0, topic/partition/offset, `reason_code`, truncated `error_text` (no raw payload); watermarks unchanged; no lingering consumer group.
*Pass:* zero produced records.

**P5-DLQ-13 Real replay after fix, filters and limit (P0).**
*Steps:* (a) create `staging-person` first (`replay-dlq --target-topic` never auto-creates: a missing target fails with `UNKNOWN_TOPIC_OR_PART`, rc 1), replay into it with `--target-topic staging-person` and check bytes; (b) fix the situation (for example publish nothing, replay HC-3 after adding the missing field is not possible, so instead replay an `invalid_value`-class record to a fresh topic); (c) `--reason-code invalid_json`; (d) `--since`/`--until` around one envelope's `first_failure_at_ms`; (e) `--limit 1`; (f) `--until` earlier than `--since`.
*Expect:* (a) staging topic contents byte-identical to the original payloads, DLQ topic unchanged; (c) to (e) only matching envelopes counted and replayed; (f) rejected. A record that is still malformed after replay into the real source topic returns to the DLQ as a new envelope with fresh `first_failure_at_ms` and `attempt_count=0`.
*Pass:* filters exact; no source-topic change when a staging topic is used.

**P5-DLQ-14 Replay idempotency (P1).**
*Steps:* replay the same (now fixed) envelope twice into the real source topic, with the node key already loaded once.
*Expect:* Neo4j node count for that key stays 1; second replay does no harm (MERGE). Note the documented caveat: replaying an old envelope over a newer valid update overwrites with old values (last-write-wins by application order); confirm this by test on a key updated since, and record it.
*Pass:* count stable; caveat reproduced and understood.

**P5-DLQ-15 Replay of an entity switched back to file sink (P2).**
*Steps:* set `enabled: false`, keep the block; `replay-dlq --node-label Person --dry-run`; then with the block removed use `--dlq-topic social-person.dlq`.
*Expect:* the topic resolves from the block even when disabled; without a block `--dlq-topic` is required.
*Pass:* as stated.

**P5-DLQ-16 Planned mode skips malformed relationship records to the planner rejection file (P0).**
*Note:* a plain `start --mode bulk` with any relationship `dead_letter.enabled: true` (S-DLQ-NODES) logs `stage=launch edge_mode=lease reason=dead_letter` and runs bucket-lease mode, where the malformed edge goes to `social-works-at.dlq` (docs/USAGE.md, bulk modes; planned mode ignores relationship DLQ). Pass `--edge-plan` explicitly to test planned mode.
*Steps:* bulk run with `--edge-plan` explicitly, S-DLQ-NODES, and an invalid relationship record (edge missing `source`) published to an edge topic.
*Expect:* rc 0; the planner logs `WARNING Malformed relationship record rejected by the planner`, `stage=edge-plan-rejections` and `stage=outcome rejected=1`, and writes the record (topic, partition, offset, reason) to `var/rejections/<run-id>-planner-<worker>.jsonl` (the rejection directory of the checkout the CLI runs from; delete it afterwards); no relationship DLQ envelope; no edge DLQ topic provisioned. Node-side DLQ (enabled for Person) still routes malformed Person records as in P5-DLQ-02 during the bulk node phase. Planned mode still fails closed (rc 1, `MissingRelationshipEndpointError`) for missing or ambiguous endpoints: P5-ROW-08.
*Pass:* the malformed relationship is skipped and recorded, nothing written for it, every valid relationship loaded.

**P5-DLQ-17 Duplicate envelopes are tolerated (P2).**
*Steps:* kill a node loader with `docker kill` right after a malformed record was published, restart the run.
*Expect:* possibly two envelopes for the same `(topic, partition, offset)`; no data loss; documented at-least-once.
*Pass:* duplicates, if any, identical and harmless.

**P5-DLQ-18 Oversized envelope stalls its partition fail-closed (P2).**
*Steps:* publish a malformed record larger than about 6 MiB so the envelope exceeds the 8 MiB cap (requires topic `max.message.bytes` on the source topic to allow it).
*Expect:* the loader exits with `... over the 8388608-byte DLQ envelope limit`; offset does not advance (documented).
*Pass:* fail-closed as documented.

---

### 2.3 P5-ROW: P5-S3 row-level DLQ for missing and ambiguous endpoints

**Group setup:** S-DLQ-EDGES in stream mode with `--retry-consumer-replicas 1` (retry consumer running) unless the case says otherwise; to observe pure S3 behaviour (before retries) use a very long retry delay or read the DLQ quickly. Nodes for the case are published first unless stated. Company `c-1` exists (HC).

**P5-ROW-01 Edge before node in streaming: only unresolved rows are DLQ'd, resolved rows commit (P0).**
*Steps:* publish a batch of edges to `social-works-at` where 3 of 10 reference Persons not yet published, then run.
*Expect:* 7 relationships in Neo4j immediately; exactly 3 envelopes in `social-works-at.dlq`: `failure_class=missing_endpoint`, `reason_code=missing_source`, `retry_after_ms` set and later than `first_failure_at_ms`, schema version 2; `value_bytes` in the loader's wire input shape (`source`, `target`, `properties`); Kafka key `[source_key, target_key]`. Offsets of `social-works-at` advance past the whole batch. Then run UC-2.
*Pass:* mixed batch committed in one pass, zero lock waits.

**P5-ROW-02 Reason codes: missing source, missing target, missing both (P0).**
*Steps:* publish HC-7, HC-8, HC-9.
*Expect:* `missing_source`, `missing_target`, `missing_both` respectively.
*Pass:* codes match.

**P5-ROW-03 Ambiguous endpoint (P1).**
*Pre:* two `Company` nodes with the same `company_id` (Company has no uniqueness constraint), created directly in Neo4j.
*Steps:* publish a `WORKS_AT` edge to that company.
*Expect:* envelope with `failure_class=ambiguous_endpoint`, `reason_code=ambiguous_target`; nothing written.
*Pass:* as stated.

**P5-ROW-04 All-resolved fast path unchanged (P1).**
*Steps:* full Social small stream run with S-DLQ-EDGES where nodes are loaded first (start node loaders, wait lag 0, then publish edges) so no edge is unresolved.
*Expect:* no DLQ envelopes for any edge topic; `graph_loader_dlq_routed_total` unchanged; exact graph (UC-1) and UC-2 clean; throughput comparable to S-PLAIN.
*Pass:* zero DLQ traffic on the happy path.

**P5-ROW-05 Same-pair ordering within a flush (P0).**
*Steps:* in one publish burst, publish for the same (Person P missing, Company c-1) pair record R1 with `role: old` then R2 with `role: new`, plus R3 for a different pair that resolves.
*Expect:* R1 and R2 are both DLQ'd with `failure_class=missing_endpoint` and their own reason, `reason_code=missing_source`, in offset order (same pair, same endpoints, so one lookup classifies both alike); R3 written. Neither is written. After P arrives (retry consumer) the final relationship has `role=new`. `blocked_by_earlier_unresolved_row` is not expected here: `_classify_rows` (`src/loader/edge_execution.py`) assigns it only to a row that would otherwise match, and only when an earlier row of the same pair is outstanding (`pre_blocked_pairs` or the parked-pair index) or in the retry consumer. To see it, publish P between R1 and R2 (R2's endpoints now resolve while R1 is still outstanding) or use P5-RETRY-06/07; R2 is then DLQ'd `blocked_by_earlier_unresolved_row` with `failure_class=missing_endpoint` and is not written before R1.
*Pass:* order preserved; later row not written first.

**P5-ROW-06 Bulk classic and lease opt-in `route_missing_endpoints_in_bulk` (P0).**
*Pre:* S-BULKDLQ; nodes for a few Persons deliberately withheld from `social-person`.
*Steps:* (a) `start --mode bulk --edge-classic --bulk-edge-overlap shared` ; (b) `start --mode bulk --edge-leases`; each in a fresh database.
*Expect:* unresolved `WORKS_AT` rows go to `social-works-at.dlq`, others commit, the run completes. Other edge types (without the opt-in) still fail closed if they have unresolved rows.
*Pass:* opt-in per type respected; run completes.

**P5-ROW-07 Bulk classic and lease stay fail-closed without the opt-in (P0).**
*Pre:* same data, S-PLAIN (or `dead_letter.enabled: true` without `route_missing_endpoints_in_bulk`).
*Steps:* `start --mode bulk --edge-classic` and `start --mode bulk --edge-leases`, passed explicitly (the bulk default is planned mode, P5-ROW-08; with any relationship `dead_letter.enabled: true` a plain bulk start switches to lease mode, see P5-DLQ-16).
*Expect:* whole batch fails with `MissingRelationshipEndpointError`-style message ("did not resolve exactly one source and target endpoint"), nothing from that batch committed, no relationship DLQ envelopes; run exits nonzero.
*Pass:* fail-closed default unchanged.

**P5-ROW-08 Planned mode fail-closed regardless of DLQ config (P0).**
*Steps:* bulk `--edge-plan` with an edge to a missing node, S-BULKDLQ.
*Expect:* planned run fails with the missing-endpoint error (`MissingRelationshipEndpointError`, rc 1), nothing written for that relationship type; no DLQ envelope and no edge DLQ topic. This, not a malformed record (P5-DLQ-16), is what keeps planned mode fail-closed.
*Pass:* as stated.

**P5-ROW-09 Lease-loss abort is not counted as DLQ (P1).**
*Steps:* during a stream run with unresolved rows flowing, `docker kill` the lease server mid-write; afterwards read `graph_loader_dlq_routed_total` for `failure_class` values and the DLQ topic offsets.
*Expect:* rows in the aborted batch are not published to the DLQ merely because the lease was lost (the batch fails closed and replays); after recovery each unresolved row appears in the DLQ at most as a duplicate of a genuine missing-endpoint row.
*Pass:* no DLQ envelopes with a lease-loss cause; graph exact after recovery.

**P5-ROW-10 DLQ produce failure fails the batch (P0).**
*Steps:* with unresolved rows in the next batch, make the DLQ topic unavailable (delete `social-works-at.dlq` or stop the broker briefly) and publish HC-7 plus resolvable rows.
*Expect:* the batch fails closed: no offsets from that batch advance, including the resolvable rows that were already written moments earlier (they replay harmlessly); after the DLQ is restored the batch completes; the resolvable rows are not duplicated in Neo4j.
*Pass:* UC-4 shows offset unchanged during the outage; final counts exact.

**P5-ROW-11 Zero lock waits during edge-before-node races (P0).**
*Steps:* stream a Social medium run with S-DLQ-EDGES where edges are published a few seconds before nodes so many rows are DLQ'd and retried; UC-2 throughout.
*Expect:* zero `waiting:` values above 0 and no `Blocked` transactions, including during the row-classification fallback and the retry writes.
*Pass:* UC-2 clean; UC-1 exact after the retry consumer drains.

**P5-ROW-12 Edge-before-node integration test scaffold (P1).**
*Steps:* run `pytest tests/integration/test_edge_before_node_dlq.py -m integration` (needs scratch Docker); this is the P6-S4 scaffold now driving the real opt-in interface; its retry-and-verify half was skipped in P5-S3 pending P5-S4.
*Expect:* the test runs its full flow (no longer skipped) and passes.
*Pass:* passed (record if still skipped and why).

**P5-ROW-13 `native_disjoint` behaviour (P2).**
*Steps:* an edge type with `execution.mode: native_disjoint`: (a) stream (bucket leases): start; (b) bulk classic with `route_missing_endpoints_in_bulk: true`, unresolved row.
*Expect:* (a) `ExecutionModeError` at startup before Kafka subscription; (b) opt-in has no effect and the batch fails closed (silent, not a schema error).
*Pass:* as documented.

**P5-ROW-14 Deprecated clock path never uses row-level DLQ (P2).**
*Steps:* `--stream-clock-rotation` with an unresolved edge and `dead_letter.enabled: true`.
*Expect:* fail-closed missing-endpoint behaviour, no envelope.
*Pass:* as documented.

**P5-ROW-15 Lease hold does not cause lock waits (P2).**
*Steps:* burst of 500 unresolved rows in one cell (all missing sources) with a normal flow of resolvable edges on other cells.
*Expect:* DLQ publishes are synchronous after commit; lease hold time grows but UC-2 stays clean and other types keep progressing.
*Pass:* UC-2 clean; no starvation alert (`graph_loader_lease_waiters` returns to 0).

---

### 2.4 P5-RETRY: P5-S4 retry consumer and parking lot

**Group setup:** S-DLQ-EDGES (`max_retry_attempts: 3`, short base delay), S-ORDER for ordering cases; `start --mode stream --retry-consumer-replicas 1 --run-id retry-N`; publish nodes and edges by hand (HC set) after the fleet is up. On start the CLI provisions `social-works-at.dlq`, `social-works-at.dlq.parked` and `social-works-at.dlq.parked-pairs` (the last is compacted; not created for `order_by` types). The retry consumer runs as container `graph-loader-retry-<replica>-<run>` (component `retry-consumer`) from the edge image. Query log on. Metrics of the retry consumer: publish port via `docker port`.

**P5-RETRY-01 Fleet launches the retry consumer only when needed (P0).**
*Steps:* (a) S-DLQ-EDGES stream start; (b) S-PLAIN stream start; (c) S-DLQ-EDGES with `--retry-consumer-replicas 2`.
*Expect:* (a) one `retry-consumer` container launched after the lease server and before the edge loaders, log `stage=retry-consumer...`; (b) zero retry-consumer containers; (c) two replicas in one consumer group across the DLQ topics.
*Pass:* counts as stated.

**P5-RETRY-02 Late-arriving node: edge retried with backoff and written (P0).**
*Steps:* publish company c-1; publish HC-7 (Person 900001 missing); confirm the envelope in the DLQ; publish Person 900001 within the first backoff window; watch.
*Expect:* the relationship `(:Person {person_id:900001})-[:WORKS_AT]->(:Company {company_id:'c-1'})` appears exactly once (count 1); `graph_loader_retry_total{reason="missing-endpoint",kind="edge"}` increases by the number of attempts taken; `graph_loader_parked_total` unchanged; the DLQ envelope's `attempt_count` grows for re-DLQ'd cycles. Record the delay between node arrival and relationship visible (indicative latency).
*Pass:* count 1; UC-2 clean.

**P5-RETRY-03 Node never arrives: parked after `max_retry_attempts`, never dropped (P0).**
*Steps:* publish HC-7 for Person 900002 and never publish the node. Consume `.dlq` and `.dlq.parked`; sample `kafka-get-offsets` of `.dlq` over time.
*Expect:* exactly `max_retry_attempts` (3) DLQ cycles with growing `retry_after_ms`, then one record in `social-works-at.dlq.parked`; `graph_loader_parked_total` +1 exactly; the `.dlq` high watermark stops advancing for that row; no relationship written; the record is never dropped (still readable in `.parked`, retention `-1`).
*Pass:* parked once; `.parked` topic has `retention.ms=-1`.

**P5-RETRY-04 `replay-dlq --parked` restores a parked row (P0).**
*Steps:* after 03 publish Person 900002; `replay-dlq --config <schema> --edge-type WORKS_AT --parked --dry-run`; then without `--dry-run`; then again a second time.
*Expect:* dry-run matches 1 and produces nothing; live replay republishes onto `social-works-at`; the row re-enters with `attempt_count=0` and is written (count 1); the second replay leaves count 1 (no duplicate). If the node is still missing it is DLQ'd again with fresh attempt count 0.
*Pass:* row restored once.

**P5-RETRY-05 Backoff never blocks poll; other partitions progress (P1).**
*Steps:* park many rows with a long `retry_after_ms` (large `base_delay_seconds`), then publish resolvable-late rows on other partitions/edge types whose delay is short.
*Expect:* short-delay rows are retried and written on time while long-delay rows are still waiting; the consumer group shows no rebalance or eviction (`kafka-consumer-groups --describe --group <retry group>` stable; retry container logs show no poll-timeout warnings).
*Pass:* no head-of-line blocking; no eviction.

**P5-RETRY-06 Cross-flush pair ordering, pair-parking default (P0).**
*Pre:* a long retry delay for `WORKS_AT` (retry `base_delay_seconds`, for example 90 s) so R1 is still outstanding in `.dlq` when R2 arrives. A parked R1 is removed from the parked-pair index (retry_consumer.py module docstring: rows "written or parked (their index entry is gone)"), so an R2 arriving after R1 parked is written directly.
*Steps:* on `WORKS_AT` publish R1 for pair (P, c-1) with `role: "old"` while P is missing; wait for the flush (R1 DLQ'd `missing_source`); publish node P; then, in a separate later publish, send R2 for the same pair with `role: "new"` while R1 is still outstanding. Also publish a resolvable row for a different pair.
*Expect:* R2's endpoints now resolve, but it is diverted to the DLQ behind R1 with `reason_code=blocked_by_earlier_unresolved_row` (the stories called this `blocked_by_parked_pair`; the implementation uses the former) and is not written before R1; R1 resolves first, R2 next; final `role = "new"`; the different pair is written at once. If P is still missing when R2 arrives, R2 is simply `missing_source` like R1 (`blocked_by_earlier_unresolved_row` is assigned only to a row whose endpoints resolve while an earlier row of its pair is outstanding, see P5-ROW-05); the final `role = "new"` holds in that variant too.
*Pass:* final property is R2's; R2 never written before R1.

**P5-RETRY-07 Parked-pair index rebuild after restart (P0).**
*Pre:* as P5-RETRY-06 (long retry delay, so R1 stays outstanding across the restart).
*Steps:* with R1 outstanding (P missing), restart the fleet: SIGTERM the CLI (rc 0), then start again with the same run parameters (or restart edge loader and retry consumer containers). Then publish P and R2 for the same pair.
*Expect:* the rebuilt index still blocks the pair: R2 is DLQ'd `blocked_by_earlier_unresolved_row` behind R1, not written; then R1 and R2 resolve in order and final `role = "new"`. The rebuild from `social-works-at.dlq.parked-pairs` is EOF-based and must not stall startup on compaction. There is no dedicated startup log line for a successful rebuild (only warnings for unreadable entries or for entries the type no longer binds, `reason=index_holds_entries_the_type_no_longer_binds`), so verify the rebuild by the behaviour above and by the edge loader and retry consumer reaching steady state without delay.
*Pass:* ordering survives restart.

**P5-RETRY-08 `order_by` conditional SET: newer wins (P0).**
*Pre:* S-ORDER; `FRIEND_OF` with `updated_at`.
*Steps:* (a) publish R1 for pair (P, Q) with `updated_at` older while Q is missing; then in a later flush publish R2 (newer) for the same pair; (b) publish Q. Inspect `FRIEND_OF` properties and the topics.
*Expect:* R2 is written immediately (no diversion; the pair is not parked); no `.parked-pairs` topic exists for FRIEND_OF while `.parked` does; after Q arrives R1 resolves but does not overwrite R2: final properties are R2's.
*Pass:* final `updated_at` equals R2's.

**P5-RETRY-09 `order_by` ties and missing values (P1).**
*Steps:* (a) write two records for one pair with identical `updated_at` and different other properties; (b) a record without `updated_at` after one with it; (c) a first-time record without `updated_at`.
*Expect:* (a) tie is a no-op (first keeps its values); (b)/(c) applied unconditionally with a batched WARNING once per batch; a strictly older value never regresses the stored one. Type mismatch (string vs number for `updated_at`) is a silent no-op.
*Pass:* as documented in `docs/USAGE.md` section 12.

**P5-RETRY-10 Malformed records are not retried (P0).**
*Steps:* publish HC-2 style malformed JSON to `social-works-at` with the fleet running (S-DLQ-EDGES).
*Expect:* one `failure_class=malformed` envelope in `.dlq`; the retry consumer does not retry or park it (`graph_loader_retry_total` and `parked_total` unchanged); it stays in the DLQ for `replay-dlq`.
*Pass:* terminal.

**P5-RETRY-11 Duplicate DLQ copies deduplicated (P1).**
*Steps:* produce two identical copies of one envelope into `.dlq` (or force one via P5-ROW-10 partial-batch republish); with the node arriving later, and a newer record of the same pair already written after the first copy resolved.
*Expect:* the first copy writes; the stale second copy is committed past as a duplicate (`duplicate_skipped`, not counted in `retry_total`) and never overwrites the newer properties.
*Pass:* final properties are the newest; one relationship.

**P5-RETRY-12 Retry writes are lease-fenced, zero lock waits (P0).**
*Steps:* run scenario 02 and 06 while a bulk stream of ordinary `social_graph.py` edges (Social medium) hits overlapping Persons; UC-2 with `SHOW TRANSACTIONS` polled at 0.5 s.
*Expect:* only `waiting: 0`; no `Blocked` statuses; lease server grants include retry-consumer acquisitions (grants counter rises during retries).
*Pass:* UC-2 clean.

**P5-RETRY-13 Retry consumer crash and restart (P1).**
*Steps:* `docker kill` the retry consumer while rows are waiting in the DLQ; watch the CLI.
*Expect:* the kill is treated like any fleet-container exit (fleet-fatal, P5-LEASE-19) or supervised (**verify** actual behaviour and record); after recovery (rerun `start`) waiting rows are still retried, none lost or duplicated; committed offsets per record mean already-resolved rows are not re-attempted.
*Pass:* every DLQ'd row eventually written or parked; counts exact.

**P5-RETRY-14 Retry consumer SIGTERM behaviour (P1).**
*Steps:* `docker stop` the retry consumer container (or SIGTERM the CLI) while records are held.
*Expect:* exit code 0 on SIGTERM; held records not committed (auto-commit off) so they are retried after restart; final snapshot written when `GRAPH_LOADER_TIMING_DIR` is set.
*Pass:* exit 0; no lost rows.

**P5-RETRY-15 Backlog gauge rises and drains (P0).**
*Steps:* watch `graph_loader_dlq_backlog` on the retry consumer's `/metrics` (UC-6, `watch -n1`). Publish a burst of 200 edges whose Persons are missing; then publish the Persons. Repeat withholding the Persons until rows park.
*Expect:* `graph_loader_dlq_backlog{kind="edge",name="WORKS_AT",queue="dlq"}` rises within one `backlog_check_interval_ms` (default 10 s) to about 200 and drains to 0 after the nodes arrive. In the parking case the rows move to `queue="parked"`, which stays counted permanently (high minus low watermark of an infinite-retention topic; replay does not trim it) while `queue="dlq"` drains to 0; record what is observed. If the host CLI port does not show the gauge, scrape the retry-consumer container.
*Pass:* gauge rises and drains in the retry-then-write case; the parked case behaviour is recorded.

**P5-RETRY-16 Retry and park counters (P1).**
*Steps:* after 02, 03, 06 read `graph_loader_retry_total{reason="missing-endpoint"}` and `graph_loader_parked_total`.
*Expect:* retry counts only real outcomes (written, re-DLQ'd, parked); blocked and duplicate-skipped outcomes are not counted; counters are at-least-once (do not treat as exact).
*Pass:* values consistent with the observed scenarios.

**P5-RETRY-17 Poison pills do not crash-loop the consumer (P2).**
*Steps:* publish into `social-works-at.dlq` an envelope for an edge type the running schema does not know, and one with unreadable `key_bytes`.
*Expect:* logged and committed past; the consumer keeps running; the records remain in the DLQ for `replay-dlq`.
*Pass:* no crash loop.

**P5-RETRY-18 Bulk DLQ backlog is not auto-retried; standalone retry consumer (P2).**
*Steps:* after P5-ROW-06 there are bulk-routed envelopes; run stream with the same DLQ topic (bulk rows have no index entry), then separately run `python -m src.loader.retry_consumer --config ... --dlq-topics social-works-at.dlq` standalone (no `--lease-address`, after the bulk run finished).
*Expect:* in-fleet: bulk rows are committed past, not retried (documented); standalone: rows are retried through the classic path without an ordering guarantee; use `replay-dlq` otherwise.
*Pass:* behaviours as documented in `docs/USAGE.md` sections 3 and 12.

**P5-RETRY-19 Parking lot survives; topics never auto-deleted (P2).**
*Steps:* inspect `kafka-configs --describe` for `.dlq`, `.dlq.parked`; `.dlq.parked-pairs` shows `cleanup.policy=compact` and `segment.ms=3600000`. Pre-create `.parked-pairs` as non-compacted and start.
*Expect:* first: configs as listed; second: `start` fails closed (index topic must be compacted).
*Pass:* as stated.

**P5-RETRY-20 Metrics presence for the retry-consumer process (P1).**
*Steps:* scrape the retry consumer.
*Expect:* `graph_loader_process_up{kind="retry-consumer"} 1`, `process_start_time_seconds`, `process_info`, `graph_loader_consumer_lag_records` for the DLQ topics; edge write metrics stay under `kind="edge"`.
*Pass:* series present with `kind="retry-consumer"`.

**P5-RETRY-21 Ordering + retry with multiple replicas (P2).**
*Steps:* `--retry-consumer-replicas 2`, several pairs across partitions with outstanding records, then supply nodes.
*Expect:* per-pair order preserved (best-effort across source partitions per documentation), no duplicates, exact final graph.
*Pass:* exact final properties; note any ordering anomaly (documented race across live replicas).

**P5-RETRY-22 Retry drain batching settings (P1).**
*Pre:* S-DLQ-EDGES, a DLQ backlog of several thousand missing-endpoint rows across partitions (publish edges first, nodes later).
*Steps:* run with defaults (`loading.retry_consumer.max_records_per_partition: 1000`, `max_batch_records: 2000`, `write_workers: 4`), then with `max_records_per_partition: 10`, `max_batch_records: 20`, `write_workers: 1`; publish the nodes and time the drain (rows/s) and watch `graph_loader_dlq_backlog`.
*Expect:* defaults drain at roughly 13k rows/s on the reference host (about 22 rows/s before batching); the small settings drain visibly slower but correctly; a partition pauses only at `max_records_per_partition` outstanding records and a not-yet-due record does not stall due records behind it; each partition's offset is committed once, below its lowest unresolved record; out-of-range values (for example `write_workers: 0`) are rejected at schema load; zero lock waits (UC-2); final graph exact.
*Pass:* drain rate scales with the settings, exact graph, no lost or duplicated row.

**P5-RETRY-23 `write_failed` escalation (P1).**
*Pre:* a DLQ'd row whose retry write persistently fails in Neo4j while other rows in the batch succeed (for example a property value the edge write rejects), **(verify)** how to inject on the local stack.
*Steps:* let the retry consumer attempt the batch; watch its log.
*Expect:* the failing slice is bisected so good rows are written; the bad row is left outstanding for the first two isolated failures and, on the third, logged `stage=retry-consumer ... reason=write_failed_escalated` and routed as unresolved with `reason_code=write_failed`, then follows the normal attempts-then-park path (`max_retry_attempts`, parking lot, `replay-dlq --parked`). A whole-Neo4j outage (no write succeeds recently) must not escalate rows.
*Pass:* good rows written, bad row parked with `write_failed`, consumer keeps running.

---

### 2.5 P6-CI: P6-S1 CI

**Group setup:** a Python 3.12 venv with `requirements-dev.txt`; `actionlint` on PATH for CI-04. Run in a clean checkout of the branch. Do not modify the working tree for negative tests; use a throwaway copy.

**P6-CI-01 `make lint` is clean (P0).**
*Steps:* `make lint` (runs `ruff check src tests examples`).
*Expect:* "All checks passed", exit 0, no mass reformat pending (no `ruff format` step by design).
*Pass:* exit 0.

**P6-CI-02 `make test-unit` (P0).**
*Steps:* `make test-unit` (runs pytest on `tests` and `examples/imdb/tests` with `-m "not integration and not smoke"`).
*Expect:* no failures; record the actual counts (they grow with every slice: 2661 passed, 1 skipped, 49 deselected at `3e9c75a` in the P0 final run; the old "about 1609 passed, 29 deselected" is long outdated).
*Pass:* zero failures, zero errors; run it twice to catch flakiness (`tests/test_clock_runner.py` was fixed for flakiness).

**P6-CI-03 `make build-images` (P0).**
*Steps:* `make build-images`; `docker inspect` each image entrypoint.
*Expect:* `graph-loader-node:latest`, `graph-loader-edge:latest`, `graph-loader-clock:latest` built; entrypoints match ENV-03.
*Pass:* three images, correct entrypoints.

**P6-CI-04 `make validate-workflows` (P1).**
*Steps:* `make validate-workflows` (YAML parse of `.github/workflows/*.yml`, then `actionlint`).
*Expect:* silent success. Missing `actionlint` fails loudly (exit 127), not silently.
*Pass:* exit 0 with actionlint installed.

**P6-CI-05 Negative: a lint error and a broken entrypoint are caught (P2).**
*Steps:* in a throwaway copy, add an unused import and run `make lint`; change a Dockerfile `ENTRYPOINT` and compare with the `Verify entrypoint` expectation in `ci.yml` (three matrix legs: node, edge, clock).
*Expect:* lint exits nonzero naming the finding; the entrypoint mismatch is what the CI leg would report.
*Pass:* both detected; the real tree untouched.

**P6-CI-06 Workflow semantics review (P1).**
*Steps:* read `.github/workflows/ci.yml` and `ci-integration.yml`.
*Expect:* `ci.yml` triggers on `push` and `pull_request`; job `lint` first; `unit-tests` and `build-images (node|edge|clock)` both `needs: lint` and do not need each other; PR concurrency cancels superseded runs. `ci-integration.yml` (Integration (advisory)) triggers only on nightly schedule (03:00 UTC) and `workflow_dispatch`, has a "Fail if Docker-backed tests were skipped" guard, and a separate `star-topology-stress-1m` job.
*Pass:* as described; no push/pull_request trigger on the integration workflow.

**P6-CI-07 After the first push (future, P1).**
*Steps:* once pushed: confirm a PR run shows checks `lint`, `unit-tests`, `build-images (node)`, `build-images (edge)`, `build-images (clock)`; make them required status checks in branch protection; never add `integration`; trigger `ci-integration.yml` manually once (only possible from the default branch); confirm the 1M job timing (25 min budget is an extrapolation).
*Expect:* all green.
*Pass:* checks green and required; the integration workflow not required.

---

### 2.6 P6-MET: P6-S2 metrics

**Group setup:** `GRAPH_LOADER_TIMING_DIR=<fresh dir per run>` exported; query metrics with UC-6. Component metrics ports are the container port 9464 published on `127.0.0.1:<ephemeral>`; find with `docker port <name> 9464`. The host CLI binds `127.0.0.1:9464` (fallback ephemeral). Metric catalogue (23 families): `graph_loader_records_consumed_total{kind,name,label_or_type}`, `graph_loader_neo4j_write_duration_seconds{kind,name,stage}` (histogram), `graph_loader_deadlocks_recovered_total`, `graph_loader_retry_total{kind,name,reason}` (`neo4j-transient`, `missing-endpoint`), `graph_loader_dlq_routed_total{kind,name,failure_class}` (`malformed`, `missing_endpoint`, `ambiguous_endpoint`), `graph_loader_parked_total`, `graph_loader_dlq_backlog{kind,name,queue}`, `graph_loader_consumer_lag_records{kind,name,topic}`, `graph_loader_lease_wait_seconds`, `graph_loader_lease_grants_total`, `_denials_total`, `_wait_timeouts_total`, `_held_resources`, `_waiters`, `graph_loader_plan_chunks_planned`, `_committed`, `graph_loader_plan_active_writers`, `graph_loader_plan_resource_wait_seconds`, `graph_loader_planner_rows_read_total`, `graph_loader_planner_chunks_published_total`, `graph_loader_process_start_time_seconds`, `graph_loader_process_up`, `graph_loader_process_info`.

**P6-MET-01 Endpoint present in every process kind (P0).**
*Steps:* run (a) a bulk planned social run (node loaders, planned dispatcher, planned writers, CLI) and (b) a stream run with DLQ edges (node loaders, edge loaders, lease server, retry consumer), and (c) `--stream-clock-rotation` on a supported schema (clock). While each is live, curl `/metrics` of one instance per kind and read `graph_loader_process_up` and `graph_loader_process_info`.
*Expect:* `kind` values `node`, `edge`, `planned-writer`, `planned-dispatcher`, `lease-server`, `clock`, `retry-consumer`, `cli` each reachable with `graph_loader_process_up 1`. Bulk-lease `--edge-leases` also exposes the lease server.
*Pass:* all eight kinds observed (short-lived bulk containers may need a large dataset or the snapshot files; for those use MET-07).

**P6-MET-02 Catalogue series present with sane values (P0).**
*Steps:* after a full stream run with DLQ scenarios (P5-ROW-01, P5-RETRY-03) scrape and list the `graph_loader_*` families per process.
*Expect:* every family in the catalogue appears on the process that owns it (for example planner series and consumer lag only on the CLI; lease series on the lease server and `lease_wait_seconds` on edge loaders; plan series on the dispatcher); `_total` counters are non-negative and monotonic between two scrapes; histogram buckets cumulative; `dlq_routed_total{failure_class="malformed"}` is pre-created at 0.
*Pass:* no family missing; no negative or decreasing counters within one process life.

**P6-MET-03 Totals match the loaded graph on a fresh load (P0).**
*Steps:* fresh database, bulk planned social run (`--persons 20000`); after exit sum the final snapshot files `metrics-*.prom` (or use live scrapes) per family.
*Expect:* node `records_consumed_total` per label equals published node counts; edge `records_consumed_total` per type (summed over writer replicas) equals the published relationship record count per type (records written, not the deduplicated final relationship count, which is lower for repeated pairs); planner `planner_rows_read_total` equals the total edge records; dispatcher `plan_chunks_planned` equals `plan_chunks_committed`.
*Pass:* all equalities hold (record the numbers).

**P6-MET-04 Label cardinality bounded (P1).**
*Steps:* compare the number of distinct series (count lines of `/metrics`) for a small run and a run 10x larger; inspect label names.
*Expect:* series count grows with schema size only, not with data volume; no label carries a record key, `run_id`, or unbounded value; `kind`, `failure_class`, `reason`, `stage` are closed enums.
*Pass:* series count unchanged across data volumes.

**P6-MET-05 Metrics server failure does not crash a loader (P0).**
*Steps:* (a) occupy port 9464 on the host (for example `python -m http.server 9464`) and start the CLI; (b) set `GRAPH_LOADER_METRICS_PORT=notanumber`; (c) set `GRAPH_LOADER_METRICS_ENABLED=0`.
*Expect:* (a) CLI logs `stage=metrics ... reason=port_in_use` and falls back to an ephemeral port (`stage=metrics-ready kind=cli`), the run proceeds; containers are unaffected (each has its own network namespace); (b) a `reason=invalid_port_env` warning and the loader keeps running; (c) no endpoint, no snapshot files, the load still succeeds and the graph is exact.
*Pass:* every variant completes the load with the exact graph.

**P6-MET-06 CLI bind address opt-in (P1).**
*Steps:* run the CLI with `GRAPH_LOADER_METRICS_BIND_ADDR=0.0.0.0`; then with the variable set to an empty string; then unset.
*Expect:* `0.0.0.0`: WARNING `stage=metrics reason=cli_metrics_bind_addr_overridden` and the endpoint reachable on the host's other interfaces (and via the Docker gateway); empty or unset: loopback only, no warning.
*Pass:* as stated.

**P6-MET-07 Snapshots written periodically and on SIGTERM (P0).**
*Steps:* with `GRAPH_LOADER_TIMING_DIR` set run a stream fleet; list `metrics-<kind>[-<name>][-<replica>].prom` files and note modification times over a minute; then SIGTERM the CLI; then repeat the run and `docker kill` one edge loader instead.
*Expect:* files exist for every kind (`metrics-cli.prom`, node/edge/lease-server/retry-consumer, etc.); mtimes advance while the process runs; after SIGTERM each has a final snapshot that parses with `prometheus_client`'s OpenMetrics parser and matches the last live scrape; a killed container leaves only its last periodic snapshot; lease-server restart attempts write `metrics-lease-server-<run>-r<attempt>.prom`.
*Pass:* files present and parseable; final values equal last scrape after graceful exit.

**P6-MET-08 IMDb report "Metrics totals" section (P1).**
*Steps:* run `publish_and_load_nodes.py --edge-plan --report-dir examples/imdb/reports` on the 20k subset with `GRAPH_LOADER_TIMING_DIR` set (the driver sets or uses one; **verify**). Open the generated report.
*Expect:* a `## Metrics totals` table with Records consumed, DLQ routed, Deadlocks recovered, Retries (zero for DLQ, deadlocks and retries on a clean planned load); the disclaimer about at-least-once counters is present; Records consumed is consistent with published records.
*Pass:* section present with sane values.

**P6-MET-09 Instrumentation overhead negligible (P1).**
*Steps:* IMDb 20k `--edge-plan` three times each with `GRAPH_LOADER_METRICS_ENABLED=0`, default (endpoint on, unscraped), and default while a script scrapes every loader's `/metrics` once per second.
*Expect:* end-to-end times within run-to-run noise (about 60 s each; treat more than 5 percent median difference as a finding, **verify** the acceptance threshold with the owner).
*Pass:* no measurable slowdown.

**P6-MET-10 Consumer lag metric on the CLI (P1).**
*Steps:* during a bulk run, scrape the CLI `/metrics` repeatedly.
*Expect:* `graph_loader_consumer_lag_records{kind="cli",topic=...}` per node topic falls to 0 as loading completes; matches `kafka-consumer-groups` lag within a poll.
*Pass:* values agree with UC-4.

**P6-MET-11 Lease and plan series behave (P1).**
*Steps:* planned run: sample dispatcher metrics; lease-bulk run (`--edge-leases`): sample the lease server.
*Expect:* `plan_active_writers` rises to `--plan-writers` and returns to 0; `plan_resource_wait_seconds` observed; `lease_held_resources` and `lease_waiters` non-negative and back to 0 at the end; grants equal releases in the shutdown stats line.
*Pass:* gauges return to baseline.

**P6-MET-12 Process identity and version label (P2).**
*Steps:* start with `GRAPH_LOADER_VERSION=test-1` exported for the CLI (containers get their own environment, so this only labels the CLI unless passed through, **verify**); scrape `graph_loader_process_info`.
*Expect:* `version` label reflects the variable where it is set, otherwise `dev`.
*Pass:* as stated.

**P6-MET-13 Stale snapshot warning: reused directory (P2).**
*Steps:* reuse a `GRAPH_LOADER_TIMING_DIR` from an earlier run, start a new run, read files before the new process rewrites them.
*Expect:* old files persist until overwritten (documented; one directory per run advised).
*Pass:* behaviour understood; no crash.

---

### 2.7 P6-MON: P6-S3 monitoring stack

**Group setup:** base stack up (`docker compose up -d neo4j kafka`). `make monitoring-up` starts `graph-loader-prometheus` (`prom/prometheus:v2.55.1`, host port 9090) and Grafana (`grafana/grafana-oss:11.3.1`, host port 3000, dashboard uid `graph-loader-overview`, titled "graph-loader overview"). It requires root (chowns `$GRAPH_LOADER_DATA_DIR/prometheus` to 65534 and `.../grafana` to 472) and Linux. Prometheus scrapes every 3 s with 2 s Docker-SD refresh. A browser is needed for MON-11 and MON-12.

**P6-MON-01 `make monitoring-validate` (P0).**
*Steps:* `make monitoring-validate`.
*Expect:* `promtool check config`, `check rules` for the four alert files, `promtool test rules` for the three committed tests (concurrency, progress, dlq), and `scripts/validate_monitoring.py` all pass.
*Pass:* exit 0.

**P6-MON-02 `make monitoring-up` and `-down` lifecycle (P0).**
*Steps:* `make monitoring-up`; open `http://localhost:9090` and `http://localhost:3000`; `make monitoring-down`; check `docker ps`.
*Expect:* Prometheus and Grafana up; on `-down` only those two are stopped and removed; `graph-loader-neo4j`, `graph-loader-kafka`, `graph-loader-kafka-ui` and `graph-loader-net` remain untouched.
*Pass:* neo4j and kafka still running and healthy after `-down`.

**P6-MON-03 Discovery of a real fleet, including short-lived bulk containers (P0).**
*Steps:* `make monitoring-up`; run a stream fleet (`S-DLQ-EDGES`, retry consumer on); then a bulk planned run. Query `http://localhost:9090/api/v1/targets`.
*Expect:* job `graph-loader` targets UP for node loaders, edge loaders, lease server, retry consumer (stream), and node loaders, planned dispatcher, planned writers (bulk); control ports 7700 (lease RPC) and 7701 (dispatcher RPC) are dropped, not shown; containers with `graph_loader.metrics_enabled=0` are dropped entirely; targets vanish when containers exit (not "down"). Bulk containers that live under about 5 s may be missed live; their record is the snapshot files.
*Pass:* every long-lived kind is UP with correct labels.

**P6-MON-04 Target labels (P1).**
*Steps:* inspect the labels of one target per kind.
*Expect:* `kind`, `run_id` (when `--run-id` given; absent, not empty, for a bulk node/edge loader without it), `node_label`, `edge_type`, `writer_id`, `replica_id`, `container_name`, `instance`; metric `kind` matches target `kind` (no `exported_kind` twin, `honor_labels: true`).
*Pass:* labels as documented.

**P6-MON-05 CLI target with `GRAPH_LOADER_METRICS_BIND_ADDR=0.0.0.0` (P0).**
*Steps:* start a stream run with `GRAPH_LOADER_METRICS_BIND_ADDR=0.0.0.0` and the default port 9464; check job `graph-loader-cli` (target `host.docker.internal:9464`); query `graph_loader_consumer_lag_records` and planner series in Prometheus (planner series appear in bulk `--edge-plan` runs).
*Expect:* the `graph-loader-cli` target is UP and consumer lag and planner series are queryable. WARNING about the bind address is in the CLI log.
*Pass:* target up with data.

**P6-MON-06 CLI target down without the opt-in (P1).**
*Steps:* repeat without the variable.
*Expect:* `graph-loader-cli` target shows down (harmless); consumer lag panel stays empty; other targets unaffected.
*Pass:* as documented.

**P6-MON-07 Two concurrent runs distinguishable (P0).**
*Steps:* start two stream runs at the same time with different `--run-id` (`mon-a`, `mon-b`), each against its own topics or at least its own consumer group and database, with the same node label. The second CLI needs a different `GRAPH_LOADER_METRICS_PORT` (for example 9465) and will not be scraped by the static host-gateway job (only 9464 is a target).
*Expect:* every container-level series carries a distinct `run_id`; queries grouped by `run_id` separate the runs; the dashboard `run_id` variable lists both; stopping one leaves the other's targets up; no cross-talk in records-consumed totals.
*Pass:* both runs separable in Prometheus and Grafana.

**P6-MON-08 Alert rules load and stay silent when idle (P0).**
*Steps:* open `/api/v1/rules` (or the Alerts page) with the fleet idle or nothing running.
*Expect:* the rule files under `monitoring/prometheus/alerts/` load: 17 rules in five groups (`dlq` 6, `progress` 2, `lag_and_errors` 4, `concurrency` 4, `write_outage` 1; the P0 final run at `3e9c75a`, before the write-outage group, counted 16 in four groups; the count `scripts/validate_monitoring.py` prints is authoritative); alerts present: `GraphLoaderDlqRoutingDetected`, `GraphLoaderDlqRoutingSpike`, `GraphLoaderParkedLotGrowing`, `GraphLoaderDlqBacklogHigh`, `GraphLoaderDlqBacklogAbsent`, `GraphLoaderRetryStalled`, `GraphLoaderPlannedRunStalled`, `GraphLoaderNoRecordsConsumed`, `GraphLoaderConsumerLagHigh`, `GraphLoaderConsumerLagCritical`, `GraphLoaderConsumerLagNotDraining`, `GraphLoaderRetryRateHigh`, `GraphLoaderPlannedModeDeadlockObserved`, `GraphLoaderLeaseWaitersStuck`, `GraphLoaderLeaseWaitTimeoutsDetected`, `GraphLoaderPlanResourceWaitHigh`, `GraphLoaderWriteOutage`; none firing with no traffic and no loaders (note `GraphLoaderNoRecordsConsumed` can fire for legitimately idle long-lived loaders after about 20 min).
*Pass:* all rules loaded, zero firing when idle.

**P6-MON-09 DLQ alerts fire in a controlled scenario (P0).**
*Steps:* run P5-ROW-01 (a few unresolved rows) and then P5-RETRY-03 (a row parks) with monitoring up; watch Alerts.
*Expect:* `GraphLoaderDlqRoutingDetected` (info, `for: 0m`) fires within one evaluation after the first routed record, including for the first-ever `failure_class="missing_endpoint"` series; `GraphLoaderParkedLotGrowing` (critical) fires when `graph_loader_parked_total` increases; `GraphLoaderDlqRoutingSpike` fires after more than 50 routed in 15 minutes (publish 60 unresolved rows) and after its 5 min `for:`. They resolve after the window passes.
*Pass:* the three DLQ alerts fire as described and clear.

**P6-MON-10 Progress and concurrency alerts (P2).**
*Steps:* (a) planned run on a large set; `docker pause` all writers for over 21 minutes to trigger `GraphLoaderPlannedRunStalled` (10 min window plus 10 min `for:`); unpause; (b) leave a stream loader idle for over 20 minutes for `GraphLoaderNoRecordsConsumed`; (c) confirm `GraphLoaderPlannedModeDeadlockObserved` does not fire in any planned run (it must stay 0 by construction).
*Expect:* (a) and (b) fire after roughly 20 minutes and clear after progress resumes; (c) never fires.
*Pass:* as stated. Note there is no Prometheus alert on Neo4j lock waits (no such metric in the catalogue); lock waits are verified only through UC-2.

**P6-MON-11 Grafana dashboard renders with zero data (P0).**
*Steps:* with monitoring up and no loaders, open the dashboard in a browser.
*Expect:* every panel (23 on this branch, 22 in the P0 final run at `3e9c75a`, not 15; `scripts/validate_monitoring.py` prints the current count) across rows Throughput, Write latency, Consumer lag, Consumer lag: multi-topic types, Lag by topic, Plan progress, Lease server, Lease wait/lock waits, Deadlocks, DLQ, Errors, Process health renders without red error indicators; empty panels show "No data" (expected healthy state); template variables (`kind`, `name`, `run_id`) populate later.
*Pass:* no panel error state.

**P6-MON-12 Grafana dashboard during a run (P0).**
*Steps:* during a stream fleet with DLQ scenarios open the dashboard and select the run; then during a bulk planned run.
*Expect:* (panel count as P6-MON-11) Throughput, Write latency, Lease server, Lease wait, Deadlocks (planned-writer reads 0), DLQ routed by `failure_class`, Parked total, Retry rate by reason and Scrape health panels show data; Plan progress panels populate in the planned run; Consumer lag populates only with the CLI opt-in (MON-05). The dashboard's "DLQ backlog" panel ("DLQ backlog by queue") plots `graph_loader_dlq_backlog` for both `dlq` and `parked`.
*Pass:* the applicable panels have data; no error state.

**P6-MON-13 Stopping Prometheus never affects a load (P0).**
*Steps:* during a long bulk run `docker stop graph-loader-prometheus`; after the run `make monitoring-up`.
*Expect:* the load finishes with exit 0 and an exact graph; Prometheus returns afterwards (older data intact within retention).
*Pass:* load unaffected.

**P6-MON-14 Containers on a different network are silently dropped (P2).**
*Steps:* run a small load with `--network <other>` (the network must be able to reach Kafka and Neo4j).
*Expect:* no targets appear (documented gotcha); dashboard shows no data.
*Pass:* behaviour matches the documentation.

**P6-MON-15 Teardown order and direct compose commands (P2).**
*Steps:* run `docker compose -f docker-compose.yaml -f docker-compose.monitoring.yml stop prometheus grafana` without exporting `DOCKER_GID`; then with it; then a base `docker compose down` while monitoring is still up (do this last, on a disposable stack).
*Expect:* the first fails with `required variable DOCKER_GID is missing a value`; the second works; the base `down` with monitoring attached leaves orphans and cannot remove `graph-loader-net` (documented; use `make monitoring-down` first).
*Pass:* as documented.

---

### 2.8 P6-STRESS: P6-S4 stress and regression suite

**Group setup:** these tests use testcontainers (their own Neo4j 5.26 and Kafka), Docker socket access, and the `integration` and `stress` pytest markers. Confirm the shared `graph-loader-neo4j` and `graph-loader-kafka` are not touched (compare `docker ps` before and after; they may keep running). Free disk space is needed (images and 1M edges).

**P6-STRESS-01 `make stress-100k` (P0).**
*Steps:* `make stress-100k`.
*Expect:* about five tests pass in roughly 110 s on the reference host: three planned-mode hubs (target hub, source hub, multi-type hub; 100,000 relationships into one hub) with zero lock waits measured from the query log (with a liveness floor), zero attempts over one, and a full exact-graph compare; plus the two contended modes (shared-overlap: measured lock waits above 0 as a precondition and zero deadlock retry lines; bulk lease: counterfactual contention measured, real run zero).
*Pass:* all pass; no skipped test (a skip means Docker or Kafka did not start).

**P6-STRESS-02 `make stress-1m` (P1, nightly tier).**
*Steps:* `make stress-1m`.
*Expect:* one test passes in about 80 s on the reference 32 vCPU host (1M edges into one hub, planned mode, zero lock waits).
*Pass:* pass; record wall time and host size (the CI budget of 25 minutes is unmeasured on a hosted runner).

**P6-STRESS-03 Correctness regressions across the three modes (P0).**
*Steps:* `python3 -m pytest tests/integration/test_relationship_correctness_regressions.py -m integration`.
*Expect:* social example at 200 persons: planned and classic each equal the independently computed expected graph and each other; bulk lease matches the expected graph (the earlier structural-only limitation should now be exact after the last-write-wins fix, **verify** whether the test was tightened); a missing-endpoint `VISITED` record fails closed with zero relationships committed in all three modes (planned pinned to `MissingRelationshipEndpointError`).
*Pass:* all pass, none skipped.

**P6-STRESS-04 Lock-wait helper proven against a real injected wait (P1).**
*Steps:* `python3 -m pytest tests/test_lock_wait_integration.py -m integration`.
*Expect:* two threads produce a genuine measured wait; the assertion helper does not pass vacuously (an empty or unreadable log fails loudly).
*Pass:* 2 passed (about 26 s).

**P6-STRESS-05 Edge-before-node DLQ integration test is active (P0).**
*Steps:* `python3 -m pytest tests/integration/test_edge_before_node_dlq.py -m integration -v`.
*Expect:* not skipped: the row is DLQ'd with a v2 envelope, then retried and verified (the retry half was pending P5-S4). If it still skips, record the reason.
*Pass:* passed.

**P6-STRESS-06 Streaming lease E2E (opt-in) (P1).**
*Steps:* `RUN_STREAMING_LEASE_E2E=1 python3 -m pytest tests/test_streaming_lease_e2e.py -q -s` against a Docker network, Kafka, Neo4j and images as the test's docstring requires (**verify** the exact prerequisites and whether it uses `graph-loader-*` names; do not point it at data you need).
*Expect:* `WORKS_AT` and `KNOWS` writes overlap in time (no isolated phase); zero nonzero wait samples; a real `docker kill` of the lease server produces `stage=lease-server-restart` and the relationship count settles at exactly 72; a further post-restart record lands; clean SIGTERM exit 0.
*Pass:* 1 passed, not skipped.

**P6-STRESS-07 Social planned integration (P2).**
*Steps:* set the `GRAPH_LOADER_IT_*` variables (Kafka, loader Kafka, Neo4j URI, loader Neo4j URI, password, network) to a scratch stack and run `tests/test_social_planned_integration.py -m integration`.
*Expect:* planned equals expected equals classic, and an invalid record fails the planned run with its topic, partition and offset.
*Pass:* pass.

**P6-STRESS-08 Repeatability and hygiene (P2).**
*Steps:* run STRESS-01 twice; check for leftover containers (`docker ps -a`) and dangling testcontainers after each.
*Expect:* same results both times; no leftovers (Ryuk reaper cleans).
*Pass:* stable and clean.

---

### 2.9 P6-SHUT: P6-S5 graceful shutdown

**Group setup:** signals go to the `python -m src.cli start` process (`kill -TERM`, `kill -INT`, or Ctrl-C). Always pass `--run-id shut-N`, `GRAPH_LOADER_TIMING_DIR=<dir>`, and use `--skip-image-build` with prebuilt images (a signal during an image build is only honoured when the build returns). After each stop check: exit code, time from signal to exit (expected seconds, worst case about 30 s), UC-3 (must print no rows), UC-4 (offsets consistent), and that `metrics-cli.prom` exists in the timing directory. Documented exit codes: bulk stopped or stream stopped before fully up: nonzero; stream run supervising with edges: `0`. Phase markers to wait for in the CLI log: `stage=launch`, `stage=node-label-ready`, `stage=edge-plan`, `stage=lease-relationship-fleet`, `stage=lease-server`, `stage=retry-consumer...` (**verify** exact strings). Sweep with `docker rm -f $(docker ps -aq --filter label=app=graph-loader)` and reset (R) between cases. Use IMDb 20k or Social large so every phase lasts long enough to hit.

**P6-SHUT-01 Bulk, SIGTERM during container launch (P0).**
*Steps:* `start --mode bulk --edge-plan`; send SIGTERM as soon as the first node-loader containers appear in `docker ps`.
*Expect:* the CLI stops everything Docker had started; exits nonzero promptly.
*Pass:* UC-3 empty; exit within bound.

**P6-SHUT-02 Bulk, SIGTERM during node drain (P0).**
*Steps:* SIGTERM while node loaders run and before `stage=node-label-ready` for the big labels.
*Expect:* all node loader containers stopped within the bound (30 s Docker stop timeout); no waiting for `--bulk-timeout-seconds`; offsets consistent (UC-4: committed offsets never ahead of what was written).
*Pass:* UC-3 empty; prompt.

**P6-SHUT-03 Bulk, SIGTERM during the plan wait (P0).**
*Note:* the planner subprocess starts together with the node launch (`stage=edge-plan-start` precedes `Started node loader`), so at 150k persons the scan (about 13 s) ends before node readiness (21 to 30 s): there is no "nodes drained, planner still finishing" window, even with `--plan-workers 1 --plan-memory-mb 100` (P0 final run). The reachable variant is SIGTERM during the planner scan, while the node loaders are still loading.
*Steps:* `--edge-plan` (auto plan), about 150k persons; SIGTERM after `stage=edge-plan-start` while the planner is still scanning (planner heartbeat). If a much larger relationship input makes the planner outlast node readiness, also run the plan-wait variant (SIGTERM after nodes have drained).
*Expect:* the planner subprocesses are terminated, node containers stop (every one exits 0: no 137 and no 143), the CLI exits 1 within a few seconds (P0: 5.3 s), chunk topic cleanup happens (the run's chunk topic deleted unless `--keep-plan-topic`); the aborted plan logs `stage=edge-plan plan_id=... stopped by operator: ...` as a WARNING, not an ERROR. In the plan-wait variant the stop is honoured only when the planner subprocess ends (bounded by the planner runtime; runner3 D6).
*Pass:* UC-3 empty; prompt exit.

**P6-SHUT-04 Bulk, SIGTERM during the planned dispatcher phase (P0).**
*Steps:* SIGTERM while `graph-loader-planned-dispatcher-*` and writer containers are running; repeat with SIGTERM right after node readiness finishes (the case fixed in review, `dispatcher-after-readiness`).
*Expect:* the dispatcher and writers stop; a writer that is between chunks when the orchestrator aborts may exit 1 (`ERR failed aborted by orchestrator`, ERROR-level lines, observed by runner3 D5) but sends no FAIL and leaves no half-applied chunk (a plain `docker stop` of a writer is SHUT-16); an in-flight chunk transaction finishes by commit or rollback, never half-applied.
*Pass:* UC-3 empty; exit promptly in both variants; rerun (SHUT-09) completes exactly.

**P6-SHUT-05 Bulk shared scheduler and classic phases (P1).**
*Steps:* SIGTERM during `--bulk-edge-overlap shared` and during `--edge-leases` edge loading (lease server present).
*Expect:* edge loaders stop before the lease server (order matters); prompt; nothing left.
*Pass:* UC-3 empty; lease server's final stats line printed.

**P6-SHUT-06 Stream, SIGTERM during node/fleet launch (P0).**
*Note:* in stream mode the lease server container is created BEFORE the node loaders (they receive its address), so "SIGTERM before the lease server exists" cannot be hit; the case is a stop during the node launch.
*Steps:* `start --mode stream` (S-DLQ-EDGES); SIGTERM right after the first node loader container appears (watch `docker ps`).
*Expect:* `stage=shutdown reason=stop requested during node launch`; every started node loader and the lease server are stopped, every container exits 0 (no 137 and no 143: a launch-phase stop is held until a just-started container can take its SIGTERM, and a node loader stopped while still starting exits 0 with `stage=node-startup-stopped`); exit nonzero (stopped before fully up).
*Pass:* UC-3 empty.

**P6-SHUT-07 Stream, SIGTERM during supervision (P0).**
*Steps:* wait until the lease server, retry consumer and edge loaders are all up and records are flowing; SIGTERM the CLI.
*Expect:* edge loaders and the retry consumer stop before the lease server; node loaders also stopped; exit code 0; each container's final snapshot written; consumer offsets consistent.
*Pass:* exit 0; UC-3 empty; rerun completes the graph exactly (SHUT-10).

**P6-SHUT-08 SIGINT (Ctrl-C) equivalents (P0).**
*Steps:* repeat SHUT-02, SHUT-04 and SHUT-07 with SIGINT.
*Expect:* identical to SIGTERM.
*Pass:* same as the SIGTERM cases.

**P6-SHUT-09 Double and triple signals (P0).**
*Steps:* during SHUT-02 and SHUT-07 send two, then three, SIGINT/SIGTERM in quick succession (a second before containers finish stopping).
*Expect:* the second and third are ignored (no escalation): no interrupted container stop, no traceback, no orphan; exit code as in the single-signal case.
*Pass:* UC-3 empty; no Python traceback in the CLI output.

**P6-SHUT-10 Rerun after a stop completes the graph exactly (P0).**
*Steps:* after each of SHUT-02, 03, 04 (bulk) rerun the same `start` (bulk: consumer groups of node loaders resume; use the same `consumer_group_id`) to completion; after SHUT-07 rerun the stream to quiescence.
*Expect:* the final graph passes UC-1 (exact match; no duplicates; idempotent MERGE); no lost node or relationship.
*Pass:* exact match after the rerun for each interrupted phase.

**P6-SHUT-11 Offsets consistent after an interrupted run (P1).**
*Steps:* after SHUT-02/07 and before the rerun run UC-4; compare Neo4j counts with committed offsets.
*Expect:* committed offset never ahead of the durable outcome: rows past the committed offset re-appear on rerun; DLQ'd rows (with envelopes acknowledged) have offsets committed past them, and no DLQ envelope is lost.
*Pass:* no gap: after rerun the graph plus DLQ contents account for every record.

**P6-SHUT-12 Stream with edge failure path stops node loaders (P0).**
*Steps:* during a healthy stream run `docker kill` one edge loader; separately run `--stream-clock-rotation` with S-PLAIN (mixed shared plus self-referencing, so the isolation check rejects it after node loaders started).
*Expect:* both failure paths stop and remove every container the run started, including node loaders (found during P6-S2: they were left running before P6-S5); nonzero exit.
*Pass:* UC-3 empty in both.

**P6-SHUT-13 Stream without relationships (documented behaviour) (P1).**
*Steps:* a schema with only node types (remove `edges`), `start --mode stream`.
*Expect:* the CLI launches the node loaders and exits 0 immediately, leaving them running by design; `python -m src.cli status --config <schema>` lists them; `python -m src.cli stop [--loader NAME]` stops them; the label recipe also works.
*Pass:* behaviour as documented; cleaned up afterwards.

**P6-SHUT-14 SIGKILL of the CLI and label-based cleanup (P0).**
*Steps:* `kill -9` the CLI in a bulk run and in a stream run; `docker ps -a --filter label=app=graph-loader`; `docker rm -f $(docker ps -aq --filter label=app=graph-loader)`; narrow with `--filter label=run_id=shut-N` and `--filter label=component=node-loader`.
*Expect:* containers keep running after the kill (documented); the recipe removes all of them (scope it with `--filter network=<net>` on a shared host); a bulk planned run also leaves `graph-loader-edge-chunks-<run-id>`, so a rerun needs a new `--run-id` or `--replace-stale-plan-topic` (after the containers are removed); the narrowed filter misses node/edge loaders started without `--run-id`, but finds them under `app=graph-loader`. Rerun afterwards completes the graph exactly.
*Pass:* recipe leaves nothing; rerun exact.

**P6-SHUT-15 Loader SIGTERM: node loader (P1).**
*Steps:* `docker stop` a running node loader mid-batch (bulk and stream).
*Expect:* the current batch is written and its offset committed (or the batch is left uncommitted), exit code 0, final `metrics-node-<Label>-<replica>.prom` snapshot; a rerun neither loses nor duplicates nodes.
*Pass:* exit 0; graph exact after rerun.

**P6-SHUT-16 Loader SIGTERM: edge loader, planned writer, dispatcher (P1).**
*Steps:* `docker stop` (a) an edge loader in stream, (b) a planned writer, (c) the planned dispatcher.
*Expect:* (a) exits cleanly; the CLI treats it as a fleet failure (fleet-fatal policy) unless it was part of a CLI-initiated stop; (b) exit code 0 and no FAIL sent to the dispatcher for a requested stop; (c) a dispatcher exit means the run stops (no mid-run recovery, documented). A granted chunk is never half committed.
*Pass:* as described; UC-3 after the CLI's cleanup.

**P6-SHUT-17 Lease server shutdown behaviour (P1).**
*Steps:* during a lease-contended lease-bulk or stream run send SIGTERM to the lease server container.
*Expect:* a client parked on an `ANY` wait receives `ERR shutting down` (clean reply) rather than hanging; writers abort and roll back; the supervisor restarts the server if the CLI is still running (stream), else the run stops; the quiet period applies to the new generation.
*Pass:* no hung client; graph exact after completion.

**P6-SHUT-18 Retry consumer SIGTERM (P1).** See P5-RETRY-14; expected exit code 0, held records uncommitted.

**P6-SHUT-19 Signal during setup and image build (P2).**
*Steps:* run without `--skip-image-build`; SIGTERM during the image build, during schema setup or topic provisioning.
*Expect:* the stop is honoured only after the in-flight Docker call or setup step returns (documented); then everything started is stopped; nonzero exit.
*Pass:* eventual clean exit; no orphans.

**P6-SHUT-20 Parked-pair index rebuild is not promptly interruptible (P2).**
*Steps:* with a large `.parked-pairs` topic (many outstanding records) start a stream fleet and SIGTERM during the index rebuild.
*Expect:* the stop is honoured when the rebuild ends or Docker's grace period (SIGKILL) expires; no corruption (rebuild is read-only); rerun works.
*Pass:* documented limitation reproduced or not reproducible with the available size; record time to exit.

**P6-SHUT-21 CLI metrics snapshot after SIGTERM (P1).**
*Steps:* stream run with a pinned `GRAPH_LOADER_METRICS_PORT`; SIGTERM; parse `metrics-cli.prom`.
*Expect:* the file exists and parses; the port is closed afterwards.
*Pass:* parseable; port closed.

**P6-SHUT-22 Concurrent runs are stopped independently (P1).**
*Steps:* start two runs with different `--run-id`; stop one with SIGTERM.
*Expect:* only that run's containers stop (a run stops exactly what it started); the other continues; note node/edge loaders without a `run_id` label from either run cannot be told apart by label, so use explicit `--run-id` here.
*Pass:* the second run unaffected.

**P6-SHUT-23 Bounded stop drain: `edge-drain-abandoned` and `node-drain-abandoned` (P0).**
*Steps:* `start --mode stream --run-id shut-23`; with a load in flight, `docker kill` one edge loader (or node loader) so its leases stay fenced for `T + margin`, then SIGTERM the CLI (or `docker stop` a surviving loader that needs the fenced bucket).
*Expect:* the stopping loader waits at most 5 s in total (`DEFAULT_DRAIN_BUDGET_SECONDS`) for leases, then logs WARNING `stage=edge-drain-abandoned` (or `stage=node-drain-abandoned`) with `records=N`, writes nothing more, commits no offset for the unflushed batch and exits 0 (not 137). Total stop time stays near the budget plus container teardown, far below the fence time. Rerun: the abandoned records are redelivered and the graph ends exact (replayed cells are idempotent).
*Pass:* exit 0 within about 5 s of the stop, warning line present, rerun exact, UC-3 empty.

**P6-SHUT-24 Single stop per container (P0).**
*Steps:* SIGTERM a stream run and a bulk run (`--edge-plan` and classic); record `docker events --filter event=kill --filter event=stop --filter event=die` for the run's containers.
*Expect:* each container receives exactly one stop (one `kill sig=15`, one `stop`) even when the failure sweep, the success sweep and the finally sweep all name it; exit codes 0 (not 143 from a second SIGTERM after the handler was restored, not 137); no `409 ... removal already in progress` ERROR; CLI stage `stage=diagnostics` dumps appear only for real failures.
*Pass:* one stop event per container, no 143/137, UC-3 empty.

---

### 2.10 REG: cross-cutting regressions

**REG-01 Bulk planned IMDb 20k: timing, zero lock waits, exact graph (P0).**
*Pre:* `./scripts/cleanup_imdb_local_load.sh`; fresh database; query log on; prebuilt images or rebuild.
*Steps:* `python examples/imdb/publish_and_load_nodes.py --subset-dir examples/imdb/data/subsets/titles-20000-depth-4 --partitions 4 --consumer-workers 4 --publish-workers 4 --edge-plan --report-dir examples/imdb/reports` (add `--skip-image-build` with current images).
*Expect:* about 60 s end-to-end (documented 11.0M input records, 2.15M nodes, 8.4M relationships); UC-2 only `waiting: 0` (0 ms lock waiting); report written; graph counts equal the documented figures.
*Pass:* time within roughly 10 percent of 60 s (record host differences), zero lock waits, counts match.

**REG-02 Bulk planned IMDb 40k (P0).**
*Steps:* as REG-01 with `titles-40000-depth-4`.
*Expect:* about 108 s (107.8 s documented; 23.3M input, 4.06M nodes, 17.8M relationships), 0 lock waits.
*Pass:* within about 10 percent; zero waits.

**REG-03 Planned equals classic (fingerprint) on IMDb (P1).**
*Steps:* load the 20k subset into two databases: planned (`--edge-plan`) and shared overlap (`--bulk-edge-overlap shared --edge-consumers 4`, expected to show lock waits, which doubles as a positive control for UC-2); compare per-label and per-type counts and the property fingerprint.
*Expect:* identical graphs; the classic run shows non-zero waits (about 82 percent of edge-query time in the documented run) and the planned run zero.
*Pass:* fingerprints equal; detection method demonstrated.

**REG-04 Incremental batches into an existing graph (P1).**
*Steps:* load `titles-40000-depth-4`, then clear Kafka input topics and node consumer groups, then load `titles-40000-offset-40000-depth-4` into the same database (README's incremental flow), then re-run the second batch again.
*Expect:* union counts equal the sum minus overlap (overlapping nodes and relationships upserted, no duplicates); the repeat run changes nothing; lock waits 0 for both planned runs.
*Pass:* no duplicates (uniqueness constraints hold; relationship counts unchanged by the repeat).

**REG-05 Generic library: examples/social bulk planned vs classic vs lease identical (P0).**
*Steps:* `publish --persons 20000`; load into `NEO4J_DATABASE=social_planned` with `--edge-plan`; change `consumer_group_id` (or delete the `social-example-*` groups); load into `social_classic` with `--bulk-edge-overlap shared`; then `social_lease` with `--edge-leases`; `verify --database social_planned --against-database social_classic`, and again for the lease database; each database also `verify` against expected.
*Expect:* all three `exact_match: true`; `graphs_identical: true`; the shared-overlap run shows lock waits while planned and lease show none.
*Pass:* identical graphs; lease and planned zero waits.

**REG-06 Rerun idempotency (P1).**
*Steps:* rerun REG-05's planned load on the same database after resetting groups.
*Expect:* counts unchanged; `exact_match: true`.
*Pass:* no duplicates.

**REG-07 CLI surface unchanged for existing commands (P1).**
*Steps:* `python -m src.cli status --config config/graph_schema.yaml`; `python -m src.cli stop --loader <name>` on a running node loader; a bulk classic run with the repository default `config/graph_schema.yaml` shape (its `dead_letter` blocks on `WORKS_AT` and `KNOWS` are present but `enabled: false`).
*Expect:* commands work; the default schema loads; no DLQ topic is created for it; rejections still use the file sink.
*Pass:* legacy paths intact.

**REG-08 Write-timing logs still produced (P2).**
*Steps:* run any bulk load with `GRAPH_LOADER_TIMING_DIR` set.
*Expect:* `stage=node-write` and `stage=edge-write` lines, plus `edge-plan.json` and its ledger in the plan directory for planned runs.
*Pass:* files present.

**REG-09 Minimum Neo4j version for planned mode (P2).**
*Steps:* see ENV-05: on 5.21 planned mode works (no scoped subqueries) while classic and lease fail.
*Expect:* as documented.
*Pass:* as documented.

---

## 3. Traceability, execution order and time estimates

### 3.1 Traceability (test ID to slice, decision, acceptance criterion)

| Test IDs | Slice / decision | Criterion covered |
| --- | --- | --- |
| ENV-01 to ENV-05 | P6-S5 (Neo4j pin 5.26, min 5.23), P6-S1 | Supported environment; `make build-images`; image tags |
| P5-LEASE-01, 02, 03 | P5-S1 stories 6, 10; orchestration P5-Q1, "Found during P6-S2 item 3" | Streaming defaults to leases; unmodified social schema incl. `FRIEND_OF` runs in `--mode stream`; exact graph; 0 waits |
| P5-LEASE-04, 05 | P5-S1 DoD 1 | Every stream relationship write leased; no `apoc.lock.nodes` |
| P5-LEASE-06 | Found during P6-S4 item 1 (`sort_rows=False`) | Last-write-wins in lease mode |
| P5-LEASE-07, 08, 09, 10 | P5-Q1 amendment (time-based fencing, quiet period), P5-S1 story 5, 10 | Supervised restart; quiet period `Q`; kill between PING and commit; no lost or duplicated writes |
| P5-LEASE-11, 12, 20 | P5-S1 stories 3, 4, 7 | Rollback on lease loss; reconnect with bounded backoff; poll liveness |
| P5-LEASE-13, 14, 19 | P5-S1 story 5; known limitations | Restart limit; shutdown while restarting; fleet-fatal policy |
| P5-LEASE-15, 21 | P5-Q1, P5-S1 story 6, 10 | Deprecated `--stream-clock-rotation`; throughput comparison |
| P5-LEASE-16, 17, 18 | P5-S1 story 1; P5-Q1 amendment 1 (`tx_timeout` at least 10x commit time) | Renamed `bucket_lease_*` settings and deprecated aliases; config validation |
| P5-LEASE-22 | P5-S1 story 8; P6-S2 story 9 | Metrics across a lease-server restart |
| P5-DLQ-01, 09, 11 | P5-S2 stories 2, 3, 6; P5-Q4 | Topic provisioning, retention -1, policy check, schema validation |
| P5-DLQ-02 to 07 | P5-S2 stories 1, 4, 5; owner decision 1 | Envelope contents and reason codes for node and edge malformed records |
| P5-DLQ-08, 17, 18 | P5-S2 DoD 1; fail-closed offsets | Offset commit only after DLQ ack; DLQ unavailable fails closed |
| P5-DLQ-10 | P5-S2 DoD 2, 4 | Legacy rejection-file behaviour unchanged without opt-in |
| P5-DLQ-12 to 15 | P5-S2 story 7 | `replay-dlq` dry-run, filters, staging, idempotency, disabled-block replay |
| P5-DLQ-16 | P5-Q3 | Planned mode stays fail-closed |
| P5-ROW-01, 02, 03, 04 | P5-S3 stories 3, 4; DoD 1, 3 | Row-level DLQ; missing vs ambiguous; fast path unchanged |
| P5-ROW-05 | P5-S3 decision 4, deviation 4 | Same-pair ordering within a flush |
| P5-ROW-06, 07, 08, 13, 14 | P5-S3 story 1, 5; P5-Q3 | Bulk opt-in `route_missing_endpoints_in_bulk`; default fail-closed; planned fail-closed; scope boundaries |
| P5-ROW-09, 10 | P5-S3 story 6; DoD 2 | Lease-loss aborts not DLQ; DLQ produce failure fails the batch |
| P5-ROW-11, 15 | P5-S3 story 7; P5-Q1 | Zero lock waits with row-level DLQ |
| P5-ROW-12, P6-STRESS-05 | Found during P6-S4 item 3; P5-S3 story 7 | Edge-before-node scaffold active |
| P5-RETRY-01 | P5-S4 story 6; P5-Q2 | Retry consumer launch, gating, replicas |
| P5-RETRY-02, 03, 04 | Owner decision 2; P5-S4 story 5, 8 | Late node retried; never-arriving node parked; `replay-dlq --parked` |
| P5-RETRY-05, 13, 14, 17, 21 | P5-S4 stories 5, 6 (M8 to M11) | Backoff never blocks poll; crash, SIGTERM, poison pills, replicas |
| P5-RETRY-06, 07, 11 | P5-S3 to P5-S4 handoff (ordering across flushes); M2, M12 | Cross-flush pair ordering, index rebuild, duplicate copies |
| P5-RETRY-08, 09 | P5-S4 story 4; handoff optional `order_by` | Conditional SET semantics |
| P5-RETRY-10, 18 | P5-Q2 | Malformed not retried; bulk backlog not auto-retried |
| P5-RETRY-12 | P5-Q1 fencing; P5-S4 story 6 | Retry writes lease-fenced, 0 lock waits |
| P5-RETRY-15, 16, 20 | P5-Q4 follow-up; P5-Q5; P5-S4 story 7 | `graph_loader_dlq_backlog`, retry and parked counters |
| P5-RETRY-19 | P5-Q4; P5-S4 story 2 | Retention, compaction policy of parked and index topics |
| P6-CI-01 to 07 | P6-S1 stories 1 to 5; P6-Q2, P6-Q6 | Lint, unit tests, three image builds, workflow validation, blocking versus advisory |
| P6-MET-01 to 04 | P6-S2 stories 1 to 4; P6-Q5 | Endpoint in every kind; catalogue; cardinality; totals |
| P6-MET-05, 06 | P6-S2; P6-S5 (`GRAPH_LOADER_METRICS_BIND_ADDR`) | Failure isolation; bind-address opt-in |
| P6-MET-07, 08 | P6-Q1; P6-S2 story 9 | Snapshots and IMDb report totals |
| P6-MET-09 | P6-S2 performance overhead | Negligible overhead |
| P6-MET-10 to 13 | P6-S2 | Lag, lease and plan gauges, identity |
| P6-MON-01, 02, 15 | P6-S3 stories 1 to 5 | Validate, up, down, teardown order |
| P6-MON-03, 04, 07, 14 | P6-S3 story 1; P6-S5 "Found during P6-S3 item 4" | Discovery, labels, concurrent runs |
| P6-MON-05, 06 | P6-S5 "Found during P6-S3 item 2" | CLI target via host gateway |
| P6-MON-08, 09, 10 | P6-Q4; P6-S3 alerts | Rules load, idle silence, firing |
| P6-MON-11, 12 | P6-S3 dashboard story | Dashboard with zero data and during a run |
| P6-MON-13 | P6-S3 principle | Prometheus down never affects a load |
| P6-STRESS-01 to 04, 06 to 08 | P6-S4 stories 2 to 8; P6-Q3 | 100k and 1M stress, correctness regressions, helper proof |
| P6-SHUT-01 to 05, 08, 09 | P6-S5 stories 1, 2, 7 | Signals at every bulk phase; SIGINT; double signal |
| P6-SHUT-06, 07, 12 | P6-S5 story 1; orchestration "Found during P6-S2 items 1, 2" | Stream phases; node loaders stopped on all paths |
| P6-SHUT-10, 11 | P6-S5 "graceful" contract | Rerun completes exactly; offsets consistent |
| P6-SHUT-13, 14 | P6-S5 story 8 | Stream without relationships; SIGKILL recipe |
| P6-SHUT-15 to 20, 22 | P6-S5 stories 3 to 5, 9 | Per-process SIGTERM behaviour; accepted limitations |
| P6-SHUT-21 | P6-S5 story 6 | CLI snapshot after SIGTERM |
| P6-SHUT-23, 24 | Test-phase fixes (bounded stop drain; single stop per container) | `edge-drain-abandoned`, `node-drain-abandoned`; one stop per container |
| P5-RETRY-22, 23 | Test-phase fixes (retry drain batching; write-failure isolation) | `max_records_per_partition`, `max_batch_records`, `write_workers`; `write_failed` escalation |
| ENV-06 | Test-phase fix (stream topic preflight) | Missing input topics fail fast, nothing launched |
| REG-01 to 04 | Final report item 6 | Bulk planned timings, fingerprint, incremental |
| REG-05 to 09 | Generic library; final report item 6; P6-S5 pin | Social identical across modes, idempotency, legacy CLI |

### 3.2 Suggested execution order for the morning session

Run in this order; stop and triage at the first P0 failure of a group.

| Step | Group / cases | Est. time | Notes |
| --- | --- | --- | --- |
| 0 | ENV-01 to 04, `make build-images` | 30 min | Gates everything; wrong image tags invalidate results |
| 1 | Fast smoke: P6-CI-01 to 04 | 15 min | Run in parallel with step 2 setup |
| 2 | Fast smoke on the live stack: P5-LEASE-01 and 02 (Social small), P5-DLQ-01 and 02, P5-ROW-01, P6-MET-01 (bulk part), P6-SHUT-01 and 07 | 60 min | If any fails, fix or stop before deep runs |
| 3 | P5-LEASE remaining P0 (03, 06, 07, 08, 09) then P1 | 2 h | Includes the lease-server kill loops (10 needs about 10 runs, schedule last) |
| 4 | P5-DLQ (all) | 1.5 h | Mostly hand-crafted records |
| 5 | P5-ROW (all) | 1.5 h | Reuses stream fleet setups |
| 6 | P5-RETRY (all) | 2.5 h | Longest manual group; pair ordering and restarts |
| 7 | P6-SHUT (all) | 2.5 h | Needs long phases; do bulk phases on IMDb 20k |
| 8 | P6-MET (all) | 1.5 h | Much can be observed during steps 3 to 7; overlap the scrapes |
| 9 | P6-MON (all except the 20-minute alert cases) | 1.5 h | Start `make monitoring-up` early and leave it up during steps 3 to 7 |
| 10 | REG-01 to 06 | 1.5 h | IMDb 20k and 40k; REG-03 is the slowest |
| 11 | P6-STRESS-01 to 05 (unattended, background) | 30 min | Start during step 8 or 10; needs spare cores, so do not overlap with REG timings |
| 12 | P2 cases and the slow alert cases (P6-MON-10) | 1.5 h | Only if time remains; MON-10 waits 20 minutes per alert, run in the background |

Total: about 18 hours of execution; split over two sessions. Minimum viable
morning (P0 only): steps 0, 2, and the P0 cases of steps 3 to 7, 9, 10 (about
6 to 7 hours).

Record results next to each case ID in a copy of this file; attach failing logs
to the matching completion doc (`plan/v5/completion_slice-N.md`,
`plan/v6/completion_slice-N.md`).

---

## 4. Known gaps and observations to raise with the owner

These were found while grounding the plan in the code; none is a test failure
by itself, but each changes what a passing run means.

1. **(Fixed on `fix/test-plan-gaps`)** Backlog gauge not yet consumed. `graph_loader_dlq_backlog` exists
   (`src/utils/metrics.py`, `scripts/validate_monitoring.py`), but
   `monitoring/prometheus/alerts/dlq.yml` still carries a `TODO(P5-S4)` and
   uses the routed and parked growth-rate proxies, `docs/MONITORING.md` section
   6 still calls the gauge name illustrative, and the Grafana dashboard has no
   backlog panel. The orchestration record (P5-Q4 follow-up) said the P6-S3
   backlog alert should switch to the gauge. **(verify)** and decide whether to
   add a follow-up.
2. **No alert on Neo4j lock waits.** The catalogue has no lock-wait metric, so a
   "lock waits above 0" alert scenario cannot be tested; lock waits are only
   observable through the query log and `SHOW TRANSACTIONS` (UC-2).
3. **(Fixed)** Stale limitation text in `docs/USAGE.md` section 12** says a clean SIGTERM
   can leave stream node-loader containers running, while section 10 and
   P6-S5 say this is fixed for runs with relationships. P6-SHUT-06, 07 and 12
   settle which is true.
4. **(Fixed)** Image tag mismatch. `make build-images` produces `:local` tags but the
   CLI defaults to `:latest`; without the `GRAPH_LOADER_*_IMAGE` overrides a
   `--skip-image-build` run uses stale or missing images.
5. **(Fixed: now 60000)** `config/graph_schema.yaml` set `bucket_lease.reconnect_timeout_ms: 0`**
   (unbounded), whereas the code default is 60000. A run with the repository
   default schema therefore uses the documented-risk unbounded reconnect.
6. **Reason code naming.** Cross-flush blocked rows use
   `blocked_by_earlier_unresolved_row`; the stories called it
   `blocked_by_parked_pair`.
7. **Query-log setting names** in the P5 completion docs (`dbms.logs.query.*`)
   may differ from Neo4j 5.26's (`db.logs.query.*`); resolved in ENV-02.
8. **Linux-only assumptions:** the lease readiness probe (routable container
   IPs from the host) and the `make monitoring-*` targets (`stat -c`, `chown`).
9. **Unmeasured or unproven items** to record while running: the 1M stress tier
   on a hosted runner, `GraphLoaderPlanResourceWaitHigh` and the lag alerts have
   no promtool unit test, the Grafana dashboard was never opened in a browser
   during slice work, the two-concurrent-run monitoring proof, and P5-S4's
   reviewer N-items, which are now all closed (fixed, resolved, documented or moot; see the table in
   `plan/v5/completion_slice-4.md`; N1 and N2 live on branch `feat/v7-s1-schema`).
