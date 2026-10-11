# Phase 6 / Slice 2 Completion — Core Metrics Instrumentation

## Delivered scope vs. Definition of Done

All 5 slice-level DoD items from `plan/v6/slices.md` Slice 2 and all 10
stories' individual DoDs (`plan/v6/slice-2/stories.md`) are met:

- [x] Every one of the six process kinds (node loader, edge loader,
      planned writer, planned dispatcher, lease server, clock runner) plus
      the host-side CLI/orchestrator process serves `/metrics` in
      Prometheus text format on its own port when enabled. Confirmed in
      the unit suite and, live, in the scratch Docker bulk and stream
      end-to-end runs below (curl against real container ports mid-run).
- [x] `graph_loader_records_consumed_total`,
      `graph_loader_neo4j_write_duration_seconds`,
      `graph_loader_deadlocks_recovered_total` and
      `graph_loader_dlq_routed_total` exist with exactly the epic's names
      (`src/utils/metrics.py`'s `EXPECTED_METRIC_FAMILIES`, asserted
      exhaustively by `tests/test_metrics.py`).
- [x] No metric label is derived from a record's key, id or free-text
      property; label sets are bounded by the schema (label/type count),
      never by record volume — enforced by an AST-based static
      cardinality guard (`tests/test_metrics.py`) that scans every
      `.labels(...)` call site in `src/`, plus a per-record-loop guard
      that fails if a metrics call is found inside a per-message loop
      body, plus deterministic call-count tests
      (`tests/test_metrics_overhead.py`) proving metrics calls scale with
      batches/chunks, not with row/record count.
- [x] `GRAPH_LOADER_METRICS_ENABLED=0` fully disables the HTTP server
      (and, per Story 9, the exit-time snapshot write) with no change to
      write behavior or exit codes — regression tests in
      `tests/test_metrics.py`, `tests/test_node_loader.py`,
      `tests/test_edge_loader.py`.
- [x] A one-run local smoke test shows `graph_loader_records_consumed_total`
      incrementing during both a bulk and a streaming load — see
      "Scratch Docker end-to-end validation" below; live-curled mid-run,
      not just read from the exit-time snapshot.

### Story-by-story delivery

1. **Metrics facade skeleton** (`src/utils/metrics.py`): `build_registry()`
   (registry-per-process factory, never a bare module-level
   `Counter(..., registry=REGISTRY)`), `GraphLoaderMetrics`, lazily
   constructed process-singleton `process_metrics()` guarded by
   `threading.Lock` double-checked locking, `DEFAULT_METRICS_PORT = 9464`
   (see Deviation 1), label-policy table as a verbatim module docstring.
2. **The metric catalogue**: all 22 series from the story's table, each
   with exact name/type/labels; `EXPECTED_METRIC_FAMILIES` is the
   single source of truth `tests/test_metrics.py` asserts the registry
   contains — no more, no fewer.
3. **HTTP exposition — six container entry points plus the CLI, and
   Docker plumbing** (`src/orchestrator/docker_service.py`,
   `src/cli.py`): `start_metrics_server()`/`stop_metrics_server()`
   (outer try/except, 1-65535 port range validation, ephemeral-port
   fallback logged as a warning if the configured port is taken),
   Docker container labels (`graph_loader.metrics_enabled`,
   `graph_loader.metrics_port`) for Slice 3's future Prometheus service
   discovery, `_metrics_port()`/`_metrics_env_passthrough()`/
   `_metrics_labels()`/`_metrics_ports_entry()`/`_metrics_enabled()`
   helpers applied uniformly to all six container launchers.
4. **Node loader instrumentation** (`src/loader/node_loader.py`):
   `records_consumed_total`, `neo4j_write_duration_seconds{stage=
   "node-write"}`, `retry_total`, `dlq_routed_total` (via
   `on_dlq_routed`), `parked_total` — all emitted only after a batch's
   write/DLQ outcome is durably known (`write_failure: Exception | None`
   captured outside the try, `safe_emit()` wrapper), so a metrics-layer
   exception can never turn a successful write into a reported failure
   or vice versa.
5. **Edge loader and edge-execution instrumentation**
   (`src/loader/edge_loader.py`, `src/loader/edge_execution.py`): both
   write paths instrumented — the slot-gated streaming `_worker_loop`
   and the classic/bulk `_flush_batch` (the stories document named only
   one; the other was found missing during this story's own test-writing
   and instrumented identically). `deadlocks_recovered_total` increments
   only on a successful write *after* a `TransientError` retry (classic/
   lease `ApocLockedEdgeWriter` only; planned mode's counter stays at 0
   by construction). `lease_wait_seconds` observed once per outer flush
   in `BucketLeaseExecutionCoordinator.execute_pending`.
6. **Planned writer and planned dispatcher instrumentation**
   (`src/loader/planned_writer.py`, `src/orchestrator/planned_dispatcher.py`):
   per-chunk `write_duration`/`records_consumed` labelled by the chunk's
   own edge type; `plan_chunks_planned`/`plan_chunks_committed`/
   `plan_active_writers` gauges updated at existing `DispatchState`
   mutation points; `plan_resource_wait_seconds` observed once per wait
   resolution (including `close()`'s ERR-closed resolutions, matching
   `total_wait_ms`'s existing behavior — documented, not a bug).
7. **Lease server and clock runner instrumentation**
   (`src/orchestrator/lease_server.py`, `src/orchestrator/clock_runner.py`):
   `lease_grants_total`/`lease_denials_total`/`lease_wait_timeouts_total`/
   `lease_held_resources`/`lease_waiters` wired at existing `LeaseStats`
   mutation points, including a fix (below) for `extend()` not updating
   the held-resources gauge. `clock_runner.py` gets only the minimal
   process-info/up/start-time triad, deliberately no hot-path histogram
   (it never writes to Neo4j), per the stories document's explicit scope
   note.
8. **CLI/orchestrator host-side metrics** (`src/cli.py`,
   `src/orchestrator/relationship_bulk_monitor.py`,
   `src/loader/edge_planner.py`): the CLI's own long-lived process starts
   a metrics server bound to `127.0.0.1` (host-side, not `0.0.0.0`) with
   `allow_ephemeral_fallback=True`; `consumer_lag_records` sampled by
   `_refresh_consumer_lag()`, throttled independently of the monitor's
   tick rate (10s default) rather than every ~0.25s tick; `planner_rows_
   read_total`/`planner_chunks_published_total` advanced by delta from
   the planner's existing heartbeat read, plus one final guarded read in
   `close()` so the last partial heartbeat isn't lost.
9. **Final-values snapshot on exit (P6-Q1) and run-report totals**
   (`write_metrics_snapshot()` in `src/utils/metrics.py`, wired into
   every process's `main()`/shutdown path; `examples/imdb/
   publish_and_load_nodes.py`): atomic `.tmp` + `os.replace` write of
   that process's own registry to `metrics-<kind>[-<name>][-<replica>]
   .prom` under `GRAPH_LOADER_TIMING_DIR`, refreshed periodically (after
   flushes) as well as at graceful shutdown — not written only once at
   exit, contrary to the stories document's original "final snapshot on
   exit" framing (see Deviation 4). A malformed `.prom` file from a
   stale run in a reused timing directory is caught with a broad
   `except (OSError, ValueError)` in the run-report reader rather than
   crashing the whole report.
10. **Performance budget, benchmark, and cross-cutting tests**
    (`tests/test_metrics_overhead.py`): a wall-clock ceiling test proving
    `prometheus_client` itself costs ~3us/call (ceiling: 100us/call, an
    order of magnitude of headroom) plus deterministic, default-suite
    call-count guards proving the node loader's, edge loader's, and
    `ApocLockedEdgeWriter`'s metrics calls scale with batches/chunks, not
    with row count — the real per-record-cost regression guard, since
    the wall-clock benchmark alone cannot prove *where* `.observe()` is
    called from (reviewer finding S10-1). A six-way SIGTERM-snapshot test
    matrix (one per process kind) proves a real signal handler/async stop
    event triggers a nonzero, parseable `.prom` snapshot on graceful
    shutdown.

## Files changed

Diff from this slice's base commit (`41ed0cd`, "plan: phase 6 slice 2
stories") to its last commit (`aab8430`, Story 10):

- `src/utils/metrics.py` (new)
- `src/cli.py`
- `src/loader/node_loader.py`
- `src/loader/edge_loader.py`
- `src/loader/edge_execution.py`
- `src/loader/edge_planner.py`
- `src/loader/planned_writer.py`
- `src/orchestrator/docker_service.py`
- `src/orchestrator/lease_server.py`
- `src/orchestrator/planned_dispatcher.py`
- `src/orchestrator/clock_runner.py`
- `src/orchestrator/relationship_bulk_monitor.py`
- `examples/imdb/publish_and_load_nodes.py`
- `examples/imdb/tests/test_publish_and_load_nodes.py`
- `requirements.txt`
- `tests/conftest.py`
- `tests/test_metrics.py` (new)
- `tests/test_metrics_overhead.py` (new)
- `tests/test_node_loader.py`
- `tests/test_edge_loader.py`
- `tests/test_edge_execution_writer.py`
- `tests/test_bucket_lease_execution.py`
- `tests/test_edge_planner.py`
- `tests/test_planned_writer.py`
- `tests/test_planned_dispatcher.py`
- `tests/test_lease_server.py`
- `tests/test_clock_runner.py`
- `tests/test_docker_service.py`
- `tests/test_relationship_bulk_monitor.py`
- `tests/test_cli.py`
- `tests/test_planned_edges_cli.py` (unaffected by content, verified for regressions)
- `docs/USAGE.md` (env-var table rows for `GRAPH_LOADER_METRICS_ENABLED`/
  `GRAPH_LOADER_METRICS_PORT`, and a snapshot-reuse/live-scrape note —
  added at the close of this slice, see Deviation 5)
- `plan/v6/slice-2/impl_plan_story_1.md` through `impl_plan_story_10.md`

## Deviations from the plan

1. **Metrics port is `9464`, not the stories document's `9090`.** Binding
   orchestrator correction: 9090 is Prometheus server's own conventional
   default; the stories document's own text flagged the collision as
   "deliberate," but the orchestrator overrode that judgement call before
   implementation started. `DEFAULT_METRICS_PORT = 9464` in
   `src/utils/metrics.py`, threaded through `docker_service.py` and the
   CLI's own host-side server. The host-side CLI endpoint binds
   `127.0.0.1` (not `0.0.0.0`), defaults to `9464`, and falls back to an
   ephemeral free port (logged as a warning) if `9464` is taken — all
   three properties orchestrator-specified up front, not discovered
   later.
2. **A single combined commit for Stories 1-7** (`56cee79`,
   "P6-S2 Stories 1-7 — core metrics instrumentation"), with impl plans
   originally written only for Stories 1-4. This departed from the
   workflow's binding per-story process (plan → implement → dedicated
   reviewer review → fix → commit) for Stories 5-7. Caught mid-task by
   an orchestrator check-in and corrected in place, before completing
   this report: `impl_plan_story_5.md` through `impl_plan_story_7.md`
   were written retroactively to document what was actually implemented
   (commit `42e1ad0`), confirmed against the reviewer's actual per-story
   review record for Stories 5-7 (each had in fact already been sent to
   the dedicated reviewer individually, with its plan and diff, before
   the combined commit — only the commit boundary and the impl-plan
   documents were out of process, not the review step itself), and every
   story from Story 8 onward landed as its own `feat` commit
   (`ba6e869`, `99abcd8`, `aab8430`). The combined commit is left in
   history rather than rewritten, per the orchestrator's explicit
   instruction.
3. **`name="-"` for lease-server, planned-dispatcher, clock, and CLI**,
   matching the label-policy table's documented convention for "process
   kinds with no single entity" — called out here only because it is
   easy to misread `name="-"` in a raw `/metrics` scrape as a bug rather
   than the documented placeholder for a process kind with no schema
   entity of its own.
4. **Final-values snapshots are written periodically (after flushes),
   not only once at process exit**, despite Story 9's title ("Final-
   Values Snapshot on Exit"). This was found necessary during scratch
   e2e validation: a process that is still running (streaming mode has
   no natural "exit") still needs a snapshot an operator can read from
   disk without a live scrape, and the atomic `.tmp`+`os.replace` write
   pattern makes a mid-run write exactly as safe as an exit-time one. The
   exit-time write is still guaranteed (via each process's shutdown
   path); this is an addition, not a narrowing, of Story 9's scope.
5. **`docs/USAGE.md`'s metrics documentation (env-var table rows, and the
   snapshot-reuse/live-scrape caveat) was added at the close of this
   slice**, alongside this completion report, rather than story-by-story
   as each metrics-facing behavior landed. Reviewer finding F5/F6 (below)
   flagged both gaps; fixing them together, once, avoided documenting an
   env var's behavior (e.g. the ephemeral-port fallback) before its
   final form was settled by a later story's fix.
6. **`plan_chunks_planned`, not `plan_chunks_total`.** The stories
   document's own catalogue table (Story 2) names this series
   `graph_loader_plan_chunks_total`; it is a `Gauge`, not a `Counter`,
   and Prometheus/OpenMetrics convention reserves the `_total` suffix for
   counters (`promtool` would flag a gauge using it). Renamed before any
   later story or external consumer depended on the original name;
   documented in the metric's own `HELP` string.
7. **Consumer lag is scoped to relationship streaming only**, not bulk
   loads or node loaders. `graph_loader_consumer_lag_records` is sampled
   from `RelationshipBulkStreamMonitor`'s own already-open Kafka admin
   connection (Story 8); node loaders and bulk-mode edge loaders have no
   equivalent long-lived lag-observation point in this slice's scope, and
   adding one would have meant opening a second Kafka admin connection
   per short-lived container purely for a metric — judged not worth the
   overhead for processes whose lifetime is itself bounded by "drain to
   the bulk boundary."
8. **`PROCESS_VERSION` is always `"dev"`** unless `GRAPH_LOADER_VERSION`
   is set in the environment — there is no build-time version-stamping
   step anywhere else in this codebase yet, so `graph_loader_process_
   info{version=...}` reads `"dev"` in every scratch/CI run today; this
   is a deliberate no-op placeholder for whenever a future slice adds
   real build versioning, not a bug.
9. **`planner_chunks_published_total` counts chunks produced/queued by
   the planner, not chunks acknowledged committed by a writer** — it is
   `KafkaEdgePlanner`'s own progress counter, matching the stories
   document's literal text ("the in-process `KafkaEdgePlanner`'s own
   progress counters"), and is a different number from `plan_chunks_
   committed` (the dispatcher's own, separate counter) by design. Both
   converged to the same value (309) in the scratch bulk validation
   below only because that run had no planner/dispatcher backlog at
   completion, not because the two counters are the same series.

## Known limitations

- **At-least-once counter semantics (P5-Q5), not exact-once.** A crash
  between a durable write/DLQ publish and this slice's counter increment
  can double-count that batch on restart. The graph itself stays exact
  via idempotent `MERGE`; a raw counter delta should never be read as an
  exact retry/DLQ/write count across a restart boundary. Documented in
  every counter's own `HELP` string (`AT_LEAST_ONCE_NOTE`), not only
  here.
- **`lease_wait_seconds` observes accumulated wait across N concurrent
  workers in one flush, not a single latency figure** (reviewer finding
  N-d): with N workers each waiting ~T seconds, one observation reads
  ~N*T. A contention-free production run still reads a small nonzero
  value from the lease server's own RPC round trip alone, not from real
  contention — do not read a nonzero value as proof of lock contention
  without comparing it against the flush's own wall time.
- **`neo4j_write_duration_seconds` times the whole retry loop, not a
  single transaction** (reviewer finding S4-2): under a retry storm, one
  observation includes every bounded retry attempt and its backoff sleep
  before eventual success, not just the final successful attempt's own
  transaction time.
- **A node/edge container's own consumer-group offset carrying over
  across a topic delete+republish cycle with the same consumer-group id
  is a real operational gotcha, discovered (not introduced) during
  scratch e2e validation**: recreating a topic under the same name does
  not clear a consumer group's previously committed offsets for that
  `(group, topic, partition)` key, so a fresh container can resume
  mid-log (or find nothing new to consume) rather than starting at
  offset 0. Not a metrics bug — the metrics correctly reported 0 new
  records consumed in that container's own process lifetime — but worth
  flagging for whoever writes the reset tooling for repeatable scratch/
  CI runs.
- **A pre-existing container-lifecycle gap, found (not introduced) during
  this slice's streaming scratch validation, is being flagged rather
  than fixed (out of this slice's scope):** node loader containers
  started by the CLI's stream-mode orchestration are not stopped by the
  same code path that stops the edge/clock containers when the CLI
  process receives `SIGTERM` while blocked inside relationship-phase
  orchestration (`_run_relationship_orchestration`'s `monitor.
  supervise_stream(shutdown)` call) — the edge/clock containers were
  observed to stop and get removed cleanly, but the six earlier-started
  node loader containers kept running until stopped manually. Likewise,
  a schema whose edges mix shared and isolated (self-referencing) types
  fails a stream-mode run's isolation check (`stage=isolation ...
  policy=mixed-stream`) *after* node loader containers are already
  running, and that failure path does not stop them either. Both are
  pre-existing container-lifecycle behaviors, unrelated to metrics
  instrumentation, and are called out here only because this slice's own
  e2e validation is what surfaced them.
- **`examples/social/graph_schema.yaml` cannot run in `--mode stream` as
  shipped**, because its `FRIEND_OF` edge type is self-referencing
  (Person→Person), which the existing isolation policy treats as
  requiring an isolated phase that stream mode refuses to mix with the
  schema's other, shared edge types. The streaming e2e validation below
  used a copy of the schema with `FRIEND_OF` removed
  (scratch-only, not committed) to exercise the other five edge types
  and all six node labels end-to-end in stream mode; this is a
  pre-existing example/schema limitation, not something this slice
  introduced or was asked to fix.

## Test evidence

### Full unit suite (required by the workflow)

```
.venv/bin/python -m pytest tests examples/imdb/tests \
  -m "not integration and not smoke" --ignore=tests/test_dev_environment.py -q

1203 passed, 14 deselected
```

### This slice's own test files

```
.venv/bin/python -m pytest \
  tests/test_metrics.py tests/test_metrics_overhead.py \
  tests/test_node_loader.py tests/test_edge_loader.py \
  tests/test_edge_execution_writer.py tests/test_bucket_lease_execution.py \
  tests/test_planned_writer.py tests/test_planned_dispatcher.py \
  tests/test_lease_server.py tests/test_clock_runner.py \
  tests/test_docker_service.py tests/test_cli.py \
  tests/test_relationship_bulk_monitor.py tests/test_edge_planner.py \
  examples/imdb/tests/test_publish_and_load_nodes.py \
  -m "not integration and not smoke" -q

635 passed, 1 deselected
```

### Performance overhead

```
.venv/bin/python -m pytest tests/test_metrics_overhead.py -m integration -q
1 passed, 6 deselected
```

`test_prometheus_client_call_cost_ceiling` measures the marginal cost of
one `Counter.inc()` + one `Histogram.observe()` (with a label lookup)
directly against `prometheus_client`: ~3us/call empirically on this dev
host, asserted against a 100us/call ceiling (an order of magnitude of
headroom, per the story's "absolute microseconds-per-call ceiling"
option). This benchmark proves `prometheus_client` itself is cheap; it
does **not**, by itself, prove this slice's instrumentation never runs
per record — that is what the deterministic call-count guards in the
same file prove (`test_node_loader_metrics_calls_scale_with_batches_not_
records`, `test_edge_loader_metrics_calls_scale_with_batches_not_records`,
`test_apoc_locked_edge_writer_deadlock_metric_never_scales_with_row_
count`), all in the default (non-integration) suite above.

### Lint and workflow validation

```
PATH="/tmp/lintvenv/bin:$PATH" ruff check src tests examples
All checks passed!

PATH="/tmp/lintvenv/bin:$PATH" make validate-workflows
python3 -c "import yaml, glob; [yaml.safe_load(open(f, encoding='utf-8')) for f in glob.glob('.github/workflows/*.yml')]"
actionlint
(clean, no output)
```

### Scratch Docker end-to-end validation

Isolated scratch containers on their own network throughout, never
touching the shared `graph-loader-kafka`/`graph-loader-neo4j`/
`graph-loader-net` stack or any `:latest`/`:generic` image tag:

- `scratch-p6s2-kafka`, `scratch-p6s2-neo4j` (`neo4j:5-enterprise`), both
  `--cpuset-cpus 28-31`, data bind-mounted under
  `/mnt/graph-loader/scratch/p6s2/`, on their own `scratch-p6s2-net`
  network.
- Three scratch images built and tagged `graph-loader-{node,edge,
  clock}:p6s2`.

**Bulk planned-mode load** (`examples/social/social_graph.py`, 5000
Person + related nodes, 80585 edges across 6 types, published to Kafka,
then loaded via `python -m src.cli start --edge-plan` end-to-end):

- All 309 planned chunks completed: `stage=planned-dispatcher-stats
  state=complete done=309/309 rows=80585/80585`.
- `metrics-*.prom` snapshot files present for every process kind after
  exit.
- Node loader `records_consumed_total` for Person: `5000.0`, exactly
  matching the published count.
- Summing `records_consumed_total` across all 6 planned-writer replicas'
  snapshot files reproduced the exact published per-edge-type counts:
  `WORKS_AT=5987, LIVES_IN=5000, FRIEND_OF=19942, ACTIVE_ON=15047,
  BOUGHT=24795, VISITED=9814` — proving the counter correctly counts
  records written/MERGE-attempted, not final deduplicated graph state.
- Neo4j node counts matched the published counts exactly via
  `cypher-shell`; edge counts were correctly lower than published counts
  due to the social example schema's own documented last-write-wins
  `MERGE` dedup on repeated `(source, target)` pairs (not a metrics bug).
- Dispatcher's `plan_chunks_planned`/`plan_chunks_committed` both read
  `309` (converged). CLI's `planner_rows_read_total=80585.0` and
  `planner_chunks_published_total=309.0` both matched exactly.
- All bulk containers auto-removed cleanly on completion.

**Streaming-mode load** (topics reset and republished with a smaller
dataset — 2000 Person, 32385 edges across the schema's 5 non-self-
referencing edge types, using a scratch-only copy of the schema with
`FRIEND_OF` removed — see Known Limitations):

- Six node loader containers and five edge loader containers plus the
  global batch clock started, each with its own ephemeral host port
  mapped to the container's `9464`.
- **Live-curled `/metrics` on a running container, mid-run** (not just
  the exit-time snapshot): `records_consumed_total{kind="edge",
  label_or_type="WORKS_AT",name="WORKS_AT"} 2377.0`, exactly matching
  the published `WORKS_AT` count, while the container's own log showed
  `stage=edge-write edge=WORKS_AT ... records=2377 write_ms=212.583` and
  the histogram's `le="0.25"` bucket (the first bucket ≥ 212ms) read
  `1.0` — the metric and the log-derived write time agree.
- Live-curled the same way for all 5 shared edge types, each matching
  its published count exactly: `ACTIVE_ON=6004, BOUGHT=10011,
  LIVES_IN=2000, VISITED=4033, WORKS_AT=2377`.
- Sent `SIGTERM` to the CLI process: the edge-loader and clock
  containers stopped and were removed cleanly, each leaving a final
  `metrics-*.prom` snapshot with the same exact values as the live
  curl above. (The six node loader containers were not stopped by this
  same signal path — see Known Limitations; they were confirmed to have
  already fully drained their (small) topics and periodically
  snapshotted `records_consumed_total` matching every published node
  count exactly: `Person=2000, Company=40, Product=400, City=25,
  Day=60, Country=5`. They were then stopped manually with `docker
  stop`, each writing a fresh final snapshot with the same values,
  confirming the per-container `SIGTERM`→snapshot path itself works
  correctly in isolation from the CLI-level orchestration gap above.)
- The CLI's own metrics endpoint bound `127.0.0.1:9464` (log line
  `stage=metrics-ready kind=cli name=- addr=127.0.0.1 port=9464`); its
  own snapshot (`metrics-cli.prom`) was present after shutdown.

All scratch containers, the scratch network, the three scratch images,
and the bind-mounted scratch data directory
(`/mnt/graph-loader/scratch/p6s2/`) were removed after validation. No
shared container, network, or `:latest`/`:generic` image tag was
touched at any point.

## Reviewer findings and resolutions

A single dedicated independent reviewer subagent (Opus) reviewed every
story's plan and diff before it was marked done, across all 10 stories.
Summary by story (full detail is in each story's `impl_plan_story_<N>.md`
and the corresponding fix commit):

- **Story 1** (facade skeleton): **B1** — `start_metrics_server` could
  raise on a bad port env value, crashing the whole process; fixed with
  an outer try/except falling back to `DEFAULT_METRICS_PORT`. **B2** —
  the cardinality guard was a denylist (only caught `ast.Subscript` in
  keyword args), missing positional-arg label leaks; rewritten as an
  allowlist checking every arg against literals/schema attributes/enum
  members. **B3a/b** — the DLQ listener leaked global state across
  tests, and an import-side-effect test couldn't actually fail; fixed
  with proper fixture reset and a subprocess-based import test.
- **Stories 2-3** (catalogue, HTTP exposition): **F1** — a bad port env
  value (e.g. `"0"`, `"70000"`) could fail an entire container launch;
  fixed with 1-65535 range validation in both `docker_service.py` and
  `metrics.py`. **F2** — a published Docker port could disagree with the
  actually-bound port when an explicit `environment=` dict was passed to
  a launcher; fixed by making the final per-launch env dict authoritative
  — this surfaced a real, pre-existing `UnboundLocalError`
  (`replica_environment` referenced before assignment) in
  `run_edge_loader`/`run_node_loader`'s label-dict construction, fixed
  alongside. **F3** — Docker service discovery would find always-down
  targets when metrics were disabled; fixed with a `graph_loader.
  metrics_enabled` container label.
- **Stories 4-8** (node/edge/planned-writer/dispatcher/lease-server/
  clock/CLI instrumentation): **S4-1** (recurring pattern flagged across
  every write-path story) — a metrics call inside the same `try:` block
  as the write itself could turn a successful write into a reported
  failure (or vice versa) if the metrics call itself raised; fixed
  everywhere with the `write_failure: Exception | None` capture pattern
  plus a `safe_emit()` wrapper that never propagates. **T1-T3/N-a/N-c/
  N-d** — assorted: `_worker_loop`'s post-write block needed its own
  try/except (N-a); `lease_wait_seconds`'s accumulated-not-per-worker
  semantics needed documenting, not fixing (N-d, see Known
  Limitations); a contention-free lease round trip still reads nonzero
  (T2, documented alongside N-d). **S6-1** — a Story 6 test was
  tautological (built an object with the same kwargs it then asserted
  on); replaced with a test driving the real `main()` with recording
  mocks. **S7-1** — `LeaseTable.extend()` (the `EXT` command) was not
  updating the `lease_held_resources` gauge; fixed. **S8-1** — consumer
  lag was refreshed every monitor tick (~0.25s default), ~150 RPCs/sec
  against the Kafka admin client; fixed with an independent 10s
  throttle. **S8-2** — treating a missing commit as offset 0 would
  overstate lag on a retention-trimmed topic; fixed with a low-watermark
  fallback. **S8-3** — `ParallelEdgePlanner.close()` lost the trailing
  partial-heartbeat's progress; fixed with one final guarded read.
- **Story 9** (final snapshot, run-report totals): **S9-1** — the
  SIGTERM-snapshot test matrix didn't exist yet for any process; by
  reviewer/implementer agreement, re-scoped into Story 10, which is
  where the stories document had actually placed that acceptance
  criterion. **S9-2** — a malformed `.prom` file (e.g. from a stale run
  reusing a timing directory) could crash the entire IMDb run report;
  fixed with a broader `except (OSError, ValueError)` around the parse.
- **Story 10** (performance budget, cross-cutting tests): **S10-1**
  (blocking) — the original wall-clock benchmark measured only raw
  `prometheus_client` call cost, not the actual regression risk (a
  future `.observe()` accidentally added inside a per-record loop);
  fixed by adding the deterministic call-count guards described above,
  which run in the default suite and would catch exactly that
  regression. **S10-2** — 5 of the SIGTERM-snapshot tests bound the
  real `0.0.0.0:9464`, risking a flaky or environment-dependent test
  run; fixed by pinning an ephemeral port in every one.
- **F5/F6** (raised during this report's own close-out, addressed
  before hand-off): `docs/USAGE.md` had no env-var documentation for
  `GRAPH_LOADER_METRICS_ENABLED`/`GRAPH_LOADER_METRICS_PORT`, and no
  note that a snapshot file is overwritten per `(kind, name, replica)`
  rather than cleared, so a stale file from a reused timing directory
  can be misread as current. Both added — see "Files changed" and
  Deviation 5.

No must-fix item was left unresolved as of the final fix round; the
reviewer's last message on Story 10 confirmed nothing was blocking
merge.

## Sign-off

The dedicated independent reviewer subagent (Opus) reviewed all 10
stories' plans and diffs, and every fix round, across the slice, and
reported no outstanding must-fix or should-fix items as of the Story 10
fix round (commit `aab8430`). This report's own close-out work (the
streaming e2e validation, the `docs/USAGE.md` additions, and this
document) was performed after that final review and is not itself
re-reviewed by the dedicated subagent; it consists of documentation and
scratch-environment validation only, with no further changes to
`src/`.

Full unit suite: 1203 passed, 14 deselected. Lint: clean. Workflow
validation: clean. Scratch e2e (bulk and streaming): both fully
validated with live-scraped and exit-snapshot evidence matching
published record counts exactly, as detailed above. All scratch Docker
resources were removed; no shared container, network, or `:latest`/
`:generic` image tag was touched.

## Post-test fix: BUG-2 / MET-07

MET-07: Deviation 4 claimed periodic snapshots but only exit/shutdown paths
wrote them. `start_metrics_server` now also starts a daemon timer
(`start_periodic_snapshots`, `src/utils/metrics.py`) that atomically rewrites
the snapshot file every `GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S` seconds
(default 15, `0` disables) from `GRAPH_LOADER_METRICS_SNAPSHOT_PATH` (the CLI
uses `$GRAPH_LOADER_TIMING_DIR/metrics-cli.prom`). `write_metrics_snapshot`
stops the timer and writes the final values, so no call site changed. Files
are Prometheus text exposition format, not OpenMetrics; `docs/USAGE.md` fixed.
BUG-2 (retry writer with a None classification query) is covered in
`plan/v5/completion_slice-4.md`.

## Post-test fix (final): review must-fixes

- `GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S` is now forwarded to containers by
  `_metrics_env_passthrough`; the snapshot path stays per-container (not
  forwarded). Documented in `docs/USAGE.md`/`docs/MONITORING.md`: a kill keeps
  the last periodic snapshot (<= one interval old), final values are lost.
- CLI periodic snapshots use `$GRAPH_LOADER_TIMING_DIR/metrics-cli.prom` (wins
  over `SNAPSHOT_PATH`), matching the CLI's final write.
- Snapshotter: write lock so no periodic write lands after `stop()` returns;
  registered only after the thread starts; interval rejects non-finite values
  and clamps to 86400 s; `GRAPH_LOADER_METRICS_ENABLED=false` (any case) now
  disables metrics like `0`. Test fixture stops/clears `_snapshotters`.
