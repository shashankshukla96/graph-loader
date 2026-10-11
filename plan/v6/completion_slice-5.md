# Phase 6 / Slice 5 Completion — Graceful Shutdown Audit & Gap Closure

Covers all three stretches of the slice: Phase A, Phase B part 1, and this
part (Phase B part 2). Backlog: `plan/v6/slice-5/stories.md`; the audit
inventory table there is the living audit record and was updated in place.

Owner directive for this part: code and local unit tests only. No Docker,
integration or e2e test was run; the real-Docker items are listed under
"Pending owner e2e" below.

## Delivered

### Phase A (commits `4146b51` .. `253d7bc` predecessors, before `c890249..253d7bc`)

- Story 3: `clock_runner.py` installs its SIGTERM handler right after
  `argparse`, before schema load and the Kafka producer.
- Story 4 (node/planned-writer): `planned_writer.py` exits 0 on a requested
  stop (dedicated `_WriterShutdownRequested`, no false `FAIL`); `node_loader.py`
  now tracks `finalization_error` like `edge_loader.py`.
- Story 5: regression guards for concurrent per-label stops and a clean
  `ERR shutting down` for a parked `ANY` lease waiter.
- Flaky clock test fixed; shared Neo4j fixture pinned to 5.26 (minimum
  supported 5.23, documented); monitoring scrape prepared for the CLI.

### Phase B part 1

- Story 4 (edge_loader): handler-restoration test.
- `GRAPH_LOADER_METRICS_BIND_ADDR` opt-in for the CLI metrics endpoint.
- Story 7 part 1 (xfail orphan regression test) and Story 8 part 1
  (USAGE.md "Stopping a run safely").

### This part (Phase B part 2)

- **Story 1 — CLI shutdown Event.** `_handle_start` installs one
  SIGINT/SIGTERM handler pair (bulk and stream; bulk had none, so SIGTERM
  killed the CLI and orphaned every container). The handler only sets an
  Event, so double signals are idempotent and cannot interrupt a
  container stop. The Event is published as `cli._cli_shutdown` so the nested
  stream phases (which still install their own local handlers for direct
  callers) share it instead of missing an early signal. Handlers are restored
  through the existing `cleanups` list, registered first so they are restored
  last. Stream runs with edges register a cleanup that stops and removes the
  node-loader containers they started, on every exit path: graceful stop
  (result 0), failure, isolation rejection (`mixed-stream`), or an escaping
  exception. A stop requested during node launch stops everything Docker had
  already started. Stream without relationships still exits 0 and leaves its
  detached node loaders running (unchanged; documented). The xfail orphan test
  is now a passing test with a nonzero-result companion (parametrized).
- **Story 2 — prompt bulk waits.** `BulkMonitor` and `RelationshipBulkMonitor`
  take `abort_requested`, checked every poll ahead of the deadline, raising
  `BulkShutdownRequested` (a `BulkTimeoutError` subclass). The bulk node-drain
  wait, the shared/planned scheduler (via `scheduler_cancelled`) and the
  planned dispatcher phase all observe a stop within one poll (0.1-0.25s), not
  `--bulk-timeout-seconds`. The stream rotating fleet deliberately does not use
  it (it has its own `supervise_stream(shutdown)`).
- **Story 6.** `test_sigterm_driven_stream_run_leaves_a_parseable_cli_metrics_snapshot`
  (real SIGTERM, pinned ephemeral metrics port, parsed with
  `prometheus_client`, port closed afterwards).
- **Story 7 (unit only).** `DockerService.list_run_containers` (default label
  `app=graph-loader`, the only label every launcher sets) and
  `tests/test_cli_shutdown.py`: seven real-signal scenarios (launch bulk,
  launch stream, node drain, node drain with the shared scheduler, plan wait,
  dispatcher phase, stream supervision) each reconciled symbolically: every
  started fake container stopped exactly once and not running. Waits are
  condition-based with bounded guards and use a `BaseException` so the code
  under test cannot swallow a not-prompt failure.
- **Story 8.** `docs/USAGE.md` "Stopping a run safely" rewritten for the
  fixed behaviour: bound, repeated signals, stream-without-edges caveat,
  recovery recipe (now lists `retry-consumer`).
- **Story 9.** Re-audit as a code/test review; the table in
  `plan/v6/slice-5/stories.md` has no DEFERRED rows left.
  * Retry consumer: PASS. There is no `_drain_on_shutdown`; `run_forever`
    exits between atomic ticks, held records are never committed
    (auto-commit off), and `main()` never raises (N13), closes everything in
    `finally`, exits 0 on SIGTERM and 1 on a loop failure. Three regression
    tests added.
  * Supervised lease server: PASS (backoff honours shutdown, over-limit
    restarts fail closed, half-created relaunch swept by label, writers abort
    without committing on lease loss).
  * Parked-pair index: ACCEPTED. The rebuild is read-only (throwaway group, no
    commits, producer created after) so SIGKILL mid-rebuild is safe, but
    `start()` does not poll the shutdown flag, so a SIGTERM during a rebuild is
    honoured only when Docker's grace period ends.
  * No must-fix gap was found in Phase 5 code.
- Docs only: stale `planned_dispatcher.py` FAIL wording corrected (a stopped
  writer never sends `FAIL`); stale 5.21-pin comments in `tests/stress` and
  `tests/integration`.

## Known limitations (not gaps in the write-safety contract)

- A Docker call already in flight (image build, container launch) is not
  cancelled; the stop is observed when it returns.
- After the first signal, further signals are ignored (there is no
  escalation); `kill -9` plus the label-based recipe is the operator's exit.
- Parked-pair index rebuild is not promptly interruptible (above).
- Stream mode with no edges leaves detached node loaders by design.

## Pending owner e2e

Nothing below was run in this part (owner directive: no Docker tonight).

1. **Story 7 real-Docker orphan-container test** (still to be written as
   `tests/test_shutdown_orphan_containers.py`, `integration`-marked, and wired
   into the advisory job in `.github/workflows/ci-integration.yml`). Spawn the
   CLI with `subprocess.Popen`, wait for a log line per phase (no fixed
   sleeps), send SIGTERM, then assert
   `DockerService().list_run_containers()` is empty within a few Docker stop
   timeouts. Phases: node launch; node drain (`stage=node-label-ready`
   absent); plan wait and dispatcher (`--edge-plan`, bulk only); stream
   supervision (`stage=retry-consumer-ready` / lease fleet up). Manual
   equivalent for each of bulk and stream, with `--run-id shut-1`:

   ```bash
   set -a; source .env; set +a   # owner's environment, not read here
   python -m src.cli start --mode bulk   --config <cfg> --run-id shut-1 &  PID=$!
   # wait for the phase's log line, then:
   kill -TERM $PID; wait $PID; echo "exit=$?"
   docker ps -a --filter label=app=graph-loader     # must print no rows
   python -m src.cli start --mode stream --config <cfg> --run-id shut-2 &  PID=$!
   kill -TERM $PID; wait $PID; docker ps -a --filter label=app=graph-loader
   # repeat with kill -INT, and with two signals in a row (kill -INT $PID twice)
   ```

   Expected: exit within ~30s worst case (usually seconds), zero
   `app=graph-loader` rows, `metrics-cli.prom` present when
   `GRAPH_LOADER_TIMING_DIR` is set.
2. **P6-S3 monitoring proof against a real fleet**:

   ```bash
   make monitoring-up
   GRAPH_LOADER_METRICS_BIND_ADDR=0.0.0.0 python -m src.cli start --mode stream --config <cfg with dead_letter enabled edges> --run-id mon-a &
   GRAPH_LOADER_METRICS_BIND_ADDR=0.0.0.0 GRAPH_LOADER_METRICS_PORT=9465 python -m src.cli start --mode stream --config <cfg> --run-id mon-b &   # second concurrent run
   make monitoring-validate
   # Prometheus http://localhost:9090/targets: edge loaders, lease server,
   # retry consumer, graph-loader-cli (host gateway) all UP for both runs.
   # Grafana http://localhost:3000 (dashboard "graph-loader"): open in a
   # browser and confirm every panel has data for both run_id values.
   ```

   Then stop both runs with SIGTERM and confirm the targets drop and no
   containers remain (item 1's `docker ps` check).

## Verification

```text
python -m pytest tests examples/imdb/tests -m "not integration and not smoke" \
  --ignore=tests/test_dev_environment.py -q
1609 passed, 29 deselected in 34s
ruff check src tests examples          -> All checks passed
workflow YAML parse + actionlint       -> ok
```

Reviewer (one dedicated Opus subagent, one pass): two must-fix items, both
fixed in `22e421b`. M1: a SIGTERM during the planned dispatch phase after node
readiness finished was ignored (the main thread blocked in
`edge_future.result()` with nothing bridging the shutdown flag); fixed by
polling the future, with a new `dispatcher-after-readiness` matrix case. M2:
the audit row and USAGE claim were wrong until M1 landed. Non-blocking note
taken: USAGE now says a signal during schema setup, topic provisioning and the
image build is honoured only when they finish. Also fixed after the full run:
`tests/test_bulk_e2e.py` now expects the new `abort_requested` kwarg.

## Post-test fixes (branch `fix/shutdown-robustness`)

Functional testing (`bench/p0/runner3`, `runner2`) found these; each is one commit.

* **D1** every loader/writer/dispatcher/lease/retry/clock container is launched with `init=True` (tini), so a SIGTERM during Python startup is no longer lost (no more 10 s/30 s stop timeouts and exit 137). Tests assert `init` for every launch kind.
* **D3** `_remove_exact_containers` treats `409 ... removal already in progress` as success (no ERROR line on stream stop).
* **D4** `--replace-stale-plan-topic` (explicit `--run-id` only) deletes a chunk topic left by a killed run when no container of that run is alive; without it the start fails with an actionable message. Chosen over silent auto-delete because a topic could still be read by a surviving writer.
* **D5** operator stop: `WARNING stopped: operator/signal stop requested`; the orchestrator aborts the dispatcher with reason `stop requested` and planned writers exit 0 when they hear it between chunks. A mid-chunk writer still fails closed.
* **D6** `KafkaEdgePlanner.close()` terminates still-scanning planner processes and removes spill, progress and failure-marker files (chunk topic deleted unless `--keep-plan-topic`).
* **Stream supervision** `_NodeLoaderWatcher`: a dead node loader in a stream run with relationships logs `stage=node-loader-supervision`, stops the fleet, rc 1.
* Edge loader legacy file sink logs `phase5_action=reject_to_file` (node loader still says `route_to_dlq`, `src/loader/node_loader.py` ~439, left to its owner).
* `var/rejections/*` untracked (`.gitkeep` kept) and `var/rejections/`, `var/edge-plans/` ignored.
* Compose: Neo4j query log rotates at 512 MB x 20 via the mounted log4j file `config/neo4j/server-logs.xml` (mounted at `/graph-loader-conf/server-logs.xml` with `server.logs.config` pointing to it, because mounting into `conf/` breaks the Neo4j entrypoint's chown; Neo4j 5 has no `db.logs.query.rotation.*` settings; `NEO4J_QUERY_LOG_ROTATION_SIZE`, `NEO4J_QUERY_LOG_KEEP_FILES`; `NEO4J_QUERY_LOG_PARAMETERS=false` disables parameter logging for big runs). Requires a Neo4j container restart.
