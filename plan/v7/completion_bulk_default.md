# Completion: bulk default is planned mode (owner decision 5)

Branch `feat/bulk-default-planned`, from `feat/multi-topic` (f102301). Binding text: `plan/v7/slices.md`, owner decisions, item 5.

## What changed

* `src/cli.py`
  * New `--edge-classic` flag. `--edge-plan`, `--edge-leases` and `--edge-classic` are mutually exclusive
    (`_validate_edge_mode_flags`, checked first in `_handle_start`, before the schema is loaded).
    `--edge-classic` is rejected in stream mode: the only classic path there is the deprecated
    `--stream-clock-rotation`, so it is not genuinely supported; stream keeps the lease fleet default.
  * `_resolve_bulk_edge_mode` picks the mode when no flag is given and logs
    `stage=launch edge_mode=<planned|lease|classic> reason=<default|dead_letter|execution_mode|flag>`:
    lease when any relationship type enables `dead_letter`, otherwise planned (sets `edge_plan` to the
    no-file sentinel, so every planned option derives exactly as for a bare `--edge-plan`). It runs before the
    precedence and multi-topic checks, so those see the resolved mode.
  * Which `dead_letter` settings planned mode cannot honour: any relationship type whose DLQ topic resolves,
    i.e. `enabled: true` (with or without a custom `topic`, `route_missing_endpoints_in_bulk`,
    `max_retry_attempts`, `parked_topic`; all of those require `enabled`). A block with `enabled: false` is the
    file-sink fallback and does not count. Node `dead_letter` does not matter for the edge mode.
  * `--bulk-edge-overlap` and `--max-concurrent-bulk-edges` now default to `None` (resolved to `labels` / `0`
    afterwards) so an explicit use can be detected. Given without `--edge-classic` they are ignored with a logged warning in bulk
    (silently in stream; with `--edge-leases` the overlap is forced to `shared`); the code does not refuse them.
  * Precedence: works under the planned default. A DLQ schema (lease default) with `topic_conflict: precedence`
    stays refused, with a message saying the default chose the lease fleet because of `dead_letter` and that v7-S5
    is needed; it points at `--edge-plan`. The multi-topic DLQ refusal (v7-S4) also hints at `--edge-plan`.
* Helpers: `examples/imdb/publish_and_load_nodes.py` passes `--edge-classic` when none of its `--edge-leases`,
  `--edge-plan` or kafka-plan options is given (it always passes the classic tuning flags, so it keeps classic
  semantics; `/mnt/graph-loader/bench/run_bench.sh` goes through this runner with `"$@"`, so it is unchanged).
  `tests/test_social_planned_integration.py` and `tests/test_relationship_bulk_e2e.py` (integration) now pass
  `--edge-classic` where they need classic. The Makefile stress targets, `scripts/` and `tests/integration` do not
  launch the bulk CLI. `examples/social/social_graph.py` docstring still shows `--edge-plan`, which keeps working.
* Docs: `docs/USAGE.md` (mode table, default selection rules, option table, limitations, migration/release note),
  `README.md`, `examples/README.md`, `examples/social/README.md`, `examples/imdb/README.md`.

## Release note

Classic was the default before; pass `--edge-classic` to keep it. A plain `start --mode bulk` now runs planned
mode (zero lock waits). Differences from classic: an invalid relationship record fails the run instead of going to
the rejection log; `dead_letter` is ignored by planned mode (so schemas that enable it get the lease mode instead);
relationship records pass through a run-scoped chunk topic. `--bulk-edge-overlap` / `--max-concurrent-bulk-edges`
need `--edge-classic`.

## Where planned cannot replace classic

* Schemas with a non-`python_apoc` relationship (`native_disjoint`): neither zero-wait mode supports them, so the
  default falls back to classic (`reason=execution_mode`, logged).
* Schemas without `dead_letter` that rely on the rejection-log behaviour for invalid relationship records:
  planned fails closed. Classic and lease both use the rejection file; only `--edge-classic`/`--edge-leases` keep
  that. (Documented in the migration note; the failure is loud, not silent.)
* Precedence together with relationship `dead_letter` (until v7-S5): refused, as before.
* Multi-topic type with `dead_letter` (v7-S4): refused under the lease default; `--edge-plan` ignores `dead_letter`.

## Tests

`tests/test_bulk_default_mode.py` (23 tests): default to planned, exact planned-option parity with `--edge-plan`,
DLQ variants to lease, disabled block stays planned, explicit flags win (with log), mutual exclusion (helper and
through `handle_start`), stream unchanged and `--edge-classic` refused in stream, classic tuning flags need
`--edge-classic`, native_disjoint fallback, no-edge schema, precedence (planned ok, DLQ lease refused with message),
log lines. Existing tests that assumed the classic default now pass `edge_classic=True`
(`tests/test_cli.py`, `tests/test_cli_shutdown.py`, `tests/test_multi_topic_stream_bulk.py`), the parser-default
assertion changed to `None`, and an IMDb runner test asserts `--edge-classic` is passed. Full suite: 1954 passed,
30 deselected; `ruff check src tests examples` clean.

## Docker proof (shared stack, database `bulkdef`, 10,000 persons = 161,755 relationship records)

Images `*:bulkdef`, `LOADER_CPUSET=12-17`, topics and groups prefixed `bdflt1`. Query log filtered to database
`bulkdef` and each run's UTC window.

| Run | Flags | Log line | Verify | Query-log entries | `waiting` > 0 | Wall time |
| --- | --- | --- | --- | --- | --- | --- |
| a | none | `edge_mode=planned reason=default` | exact_match true | 1290 | 0 | 17.5 s |
| b | none, BOUGHT has `dead_letter: {enabled: true}` | `edge_mode=lease reason=dead_letter` | exact_match true | 1840 | 0 | 22.1 s |
| c | `--edge-classic` | `edge_mode=classic reason=flag` | exact_match true | 446 | 0 | 57.6 s |

Timings are indicative (single runs, shared host). Classic recorded no lock waits at this small size either
(446 entries, all `waiting: 0`); waits appear with larger or hub-heavy inputs (docs/USAGE.md section 9).
Cleanup: `bulkdef` dropped, all `bdflt1*` topics (including the BOUGHT DLQ topic) and consumer groups deleted, no
containers of these runs left. `latesttesttemp`, `neo4j` and the other databases and containers were not touched.

## Review round 2 (supersedes the statements above where they differ)

* **native_disjoint no longer falls back to classic.** A plain start with a `native_disjoint` relationship (disjoint by
  pair, not by node, so it can wait on endpoint locks) is refused with "relationship type(s) X use execution.mode
  native_disjoint, which neither zero-wait mode supports; pass --edge-classic (can wait on Neo4j locks) or use
  python_apoc". The check runs before the dead_letter choice, so native_disjoint plus dead_letter gets the same message
  instead of a lease error naming a flag the user never passed. `reason=execution_mode` no longer exists.
* **Classic tuning flags** (`--bulk-edge-overlap`, `--max-concurrent-bulk-edges`) are warned about and ignored in planned
  and lease mode (flagless, `--edge-plan`, `--edge-leases`): "tune classic only; ignored (add --edge-classic to use
  classic)". The `None` parser defaults stay so explicit use is detected.
* **IMDb runner** (`examples/imdb/publish_and_load_nodes.py`) has `--edge-classic`; with no mode flag it goes through the
  kafka-plan path (`--edge-plan` with no file, `plan_source=kafka`), so the command, plan report and banner agree. The
  classic tuning flags are passed, and the overlap/concurrency banner and report lines shown, only with `--edge-classic`.
  `--edge-plan`, `--edge-leases` and `--edge-classic` are mutually exclusive there too. `run_bench.sh` therefore now
  benchmarks planned unless it is given `--edge-classic`.
* **Planner rejection log (planned mode, edge topics).** `scan_kafka_tasks` takes an optional `on_reject`; the
  `PlannerWorkerState` routes `ValueError`-family failures (undecodable, JSON, record validation: the classic set) to
  `RejectionSink` files `var/rejections/<run id>-planner-<worker>.jsonl` (classic entry format), skips
  `add`, still finishes a partition on a rejected last record, counts `rejected` per lane and in `plan.stats` (only when
  nonzero, so plans without rejections are byte-identical), and logs a WARNING summary. Other exceptions and the
  producer-plan `SourceTask` path stay fail-closed. The plan is exactly the plan of the valid records for any worker count
  (tested). The migration note no longer lists "invalid record fails the run".
* **Node monitor:** `BulkMonitor.verify_zero_lag` uses `_partition_ok(..., exact=True)` with the same "reused run id?"
  detail; `_watermarks` returns `(highs, lows)` (no `_last_lows` side channel). Tests extended in
  `tests/test_monitor_empty_partitions.py`.
* **Docs and launchers:** USAGE rule 3 and migration note (same-`--run-id` rerun re-plans and re-reads, needs
  `--replace-stale-plan-topic` after a kill; needs topic-creation rights, `max.message.bytes`, planner memory and spill disk;
  managed Kafka may need `--edge-leases`), IMDb README, `plan/v7/slice-2/docker_proof/run.sh` classic arm and
  `plan/v5-v6-functional-test-plan.md` P5-ROW-06 now pass `--edge-classic`.
* **Tests:** a `handle_start` test shows a flagless bulk start runs `_validate_edge_plan_settings`, calls
  `start_kafka_edge_planner` and `_ensure_dead_letter_topics(include_edges=False)`; native_disjoint (with and without
  dead_letter); warn-and-ignore; planner rejection (determinism for 1 and 2 workers, rejected last record, counts, exact plan,
  fail-closed without a directory); IMDb runner tests are parametrized helpers. Full suite: 1968 passed, 30 deselected;
  ruff clean.

### Docker proof, round 2 (shared stack, db `bulkdef`, `*:bulkdef` images, cpuset 12-17, prefixes `bdflt1`/`bdflt2`)

| Run | Result |
| --- | --- |
| IMDb `titles-20000-depth-4` via `publish_and_load_nodes.py`, no mode flag (bench conventions: 4 partitions, 4 consumer and publish workers, 1 edge consumer, 4 edge writers) | rc 0, wall 68.9 s including publishing; CLI logged `edge_mode=planned reason=flag` (the runner passes `--edge-plan`); node and relationship counts match `bench/expected_nodes.txt` / `expected_rels.txt`; `fingerprint.py` output produced (24 lines); 5,202 query-log entries for `bulkdef`, all `waiting: 0` |
| social 10,000 persons, flagless, no `dead_letter`, two malformed BOUGHT records injected (one per partition, each the last record of its partition) | rc 0, `edge_mode=planned reason=default`, `exact_match: true`, both records in the planner rejection file with topic, partition, offset and reason, `rejected=2` logged, 1,290 query-log entries all `waiting: 0`, 17.1 s |

The IMDb topics were prefixed through a scratch wrapper (patching the runner's topic tables and a prefixed schema copy) so
the existing `imdb-*` topics on the shared stack were not touched; `cleanup_imdb_local_load.sh` was not used. Cleanup: `bulkdef`
dropped, all `bdflt1*` and `bdflt2*` topics and groups deleted, no containers left.
