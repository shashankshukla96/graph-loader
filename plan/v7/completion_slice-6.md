# Completion: v7-S6 metrics, monitoring, examples, docs, zero-lock-wait test, compatibility

Branch `feat/v7-s6-polish`, from `feat/multi-topic` ffdeb92. v7-S4 (per-type DLQ) is not merged yet, so everything that
depends on it is listed in the TODO block at the end. Not touched, to avoid S4 conflicts: `retry_consumer.py`,
`parked_pair_index.py`, `dead_letter.py`, `dead_letter_replay.py`, `edge_loader.py`, `edge_execution.py`, and
`docs/USAGE.md` sections 3, 10 and 12 (the section 3 multi-topic subsection written by S1/S5 is therefore unchanged).

## Stories

* 6.1 Metrics cardinality: done. No label changed. `tests/test_multi_topic_metrics.py` (2 tests): 3 types x 4 topics
  through a real `RelationshipBulkMonitor` give exactly 12 lag series (`{kind,name,topic}`), a refresh adds none, and
  only the lag gauge has a `topic` label while no catalogue label names a record, pair, partition, sub-stage or run.
  `tests/test_metrics.py` label-set assertions untouched. Bound documented in `docs/MONITORING.md` section 4 (new
  "Multi-topic relationship types"), `docs/USAGE.md` section 8, and a comment at the gauge in `src/utils/metrics.py`.
* 6.2 Dashboards and alerts: done. New dashboard row "Consumer lag: multi-topic types": "Slowest topic of a type"
  (`max by (name)`), "Topics per type" (`count by (name)`) and a lag-by-topic timeseries repeated over `$name`;
  later rows shifted down by 9, panel ids 22-25 are new. Lag alert rules unchanged in expression (a comment states a
  stuck topic keeps the type above zero lag); `alert_tests` untouched. `make monitoring-validate` passes (promtool
  config, rules, rule tests, `validate_monitoring.py`: 20 panels).
* 6.3 Examples: done, with one deviation (below). New `examples/multi_topic/` (schema, generator/publisher/verifier,
  README): `RATED` (`order_by: rated_at`, newer value in the earlier topic for half of the shared pairs, in the later
  for the other half) and `WISHLISTED` (`topic_conflict: precedence`, the corrections topic omits `note` sometimes);
  the second topic is published first. README has the mode x rule table and the operating notes. IMDb: README
  section "Variant: a backfill topic for one relationship type" with a `HAS_GENRE` `topics`/`precedence` snippet; the
  default IMDb schema is unchanged. `examples/README.md` and the root `README.md` link the new example.
  Tests: `tests/test_multi_topic_example.py` (schema rules, stream refusal of the precedence type, generator
  properties, library normalization, expected graph, and a real-planner replay of the precedence type equal to the
  expected graph; 7), `examples/imdb/tests/test_backfill_variant.py` (README snippet loads into the IMDb schema; 2).
* 6.4 Docs: done outside the forbidden sections. `docs/USAGE.md` section 5 new "Multiple topics per relationship type:
  running and operating" (mode x rule table, planned default and `--edge-classic`, `--topics` on the edge-loader
  entrypoint, empty-partition note, late records fail closed, N consumer groups per precedence type, fresh `--run-id`,
  local stack versus `.env`, the 4096-topic planned limit); section 8 "Multi-topic runs" (lag series bound, sub-stage log
  lines, `substage-<type>.t<rank>.json` rows and `edge-<type>.t<rank>-*` files, planner rejection count, which is a log
  line and plan statistic and not a metric); section 11 four troubleshooting rows; section 13 CI notes. Every
  ```yaml snippet in README.md, USAGE.md, MONITORING.md and the example READMEs loads through `load_schema`
  (`tests/test_docs_yaml_snippets.py`, 8 cases, classifier for whole schemas, edge lists, `loading` and `dead_letter`
  fragments; an unclassified shape fails on purpose).
* 6.5 Zero-lock-wait integration test: done, `tests/integration/test_multi_topic_relationship.py` (8 tests, marked
  `integration`, skips cleanly without Docker). One module-scoped Neo4j 5.26 (query log verbose, APOC) plus the shared
  `kafka_bootstrap` fixture; `LIKES` Person -> Item with topics `likes-a`/`likes-b`, 20,000 distinct pairs
  (`GRAPH_LOADER_MULTI_TOPIC_PAIRS`), 30% in both topics, Item 0 is a hub with 4,000 incoming pairs, newer value in the
  earlier topic for half of the overlaps and the later topic for the other half. Cases and what they assert:

  | Case | Final state | Lock waits |
  | --- | --- | --- |
  | order_by, stream lease fleet (3 replicas, in-process lease server) | `updated_at` = max per pair, 20,000 rels | 0 (query log, plus no blocked `SHOW TRANSACTIONS` sample) |
  | order_by, bulk lease (3 replicas, static assignment) | same | 0 |
  | order_by, bulk classic (1 replica) | same | reported, not asserted (65 ms measured) |
  | order_by, planned (real planner, dispatcher, 4 writers) | same | 0, no write retries |
  | precedence, bulk lease (sequential per-topic stages) | later topic wins, earlier-only property survives | 0 |
  | precedence, bulk classic (1 replica, sequential stages) | same | reported, not asserted |
  | precedence, planned | same | 0 |
  | arrival, stream lease | relationship count only | 0 |

  All cases also assert zero loader exit codes, an empty rejection file and no DLQ topic. Ran once: 8 passed in 58 s.
* 6.6 Compatibility suite: done. `tests/test_single_topic_compat.py` (7) with `tests/golden/single_topic_compat.json`.
  For `config/`, `examples/imdb` and `examples/social` it fingerprints node and edge DLQ / parked / index topic names,
  stream and bulk consumer group ids, the edge-loader container command and rejection file, lag series, and a SHA-256
  over every planned-mode chunk message (byte-identical, unlike the plan JSON whose digest S3 changed on purpose via
  `stats.type_topics`). The golden was generated with the same module on the pre-v7 tree (`git archive 4382960`) and
  the dump from the current tree is byte-identical; the test also checks the new naming API agrees with the old
  names. A tracked-schema guard fails if a new single-topic `graph_schema.yaml` is shipped without being added. The full
  unit suite and all examples' tests pass untouched.
* S1-S5 carry-overs: lag series bound (done), `--topics` and the empty-partition rule (done), sub-stage log lines,
  timing rows and the N-groups/fresh-run-id runbook (done), planner rejection count (done), 4096-topic planner limit
  (done), S2 and S5 docker proofs in the nightly wiring (below).

## Nightly wiring

`.github/workflows/ci-integration.yml`: the new integration test runs in the existing nightly `integration` job (no
change needed). The container proofs live in their own workflow, `.github/workflows/ci-multi-topic-proofs.yml`
(`workflow_dispatch` only, so dispatching it does not run the integration and 1M stress jobs): slice-2 (`stream`,
`lease`, `classic`) and slice-5 (`lease` with topics `a,b` and `c,b,a`, `classic`) `docker_proof` harnesses against
the compose Neo4j (pinned to `5.26-enterprise`) and Kafka stack, proof outputs uploaded as an artifact. It has NOT
been run on a GitHub-hosted runner; `actionlint` 1.7.12 (installed into a scratch venv) is clean. Needs internet and a
runner of about 16 GB. To make it nightly add a `schedule:` once it has been seen green. Local recipe: USAGE section 13.

## Evidence

* Example, local stack (database `v7s6`, images `:v7s6`, cpuset 12-17, 3,000 customers, 20,106 records): planned
  default (`edge_mode=planned reason=default`) `exact_match: true`; `--edge-leases` `exact_match: true` with 337
  query-log entries in the run window, all `waiting: 0`. Cleanup: database dropped, `mt-*` topics and groups deleted,
  `:v7s6` images removed, no containers left; `:latest` and the other databases untouched.
* Integration test: 8 passed (58 s). Classic's 65 ms wait is why classic is correctness-only there.
* Full suite `python -m pytest tests examples/imdb/tests -m "not integration and not smoke" -q
  --ignore=tests/test_dev_environment.py`: 2132 passed, 38 deselected. `ruff check src tests examples`: clean.
  `make monitoring-validate`: passes. New unit tests: 26 (2 + 7 + 7 + 2 + 8).

## Deviations

* Story 6.3 text puts the second topic into `examples/social`. A new `examples/multi_topic` was built instead (the
  task allowed it): changing social would change its default run, its generator and the compat golden. The IMDb variant
  is a documented, tested README snippet rather than a second 100-line schema file.
* Story 6.5 drives in-process loaders (EdgeLoader, `build_edge_execution`, a real `BucketLeaseServer`, the real planner,
  dispatcher and writers) like `tests/stress`, not containers; the container runs are the `docker_proof` harnesses.
  Bulk precedence in lease and classic is the same per-topic sequence `cli._run_precedence_substages` runs, executed in
  process. Classic runs one replica and is correctness-only.
* Story 6.2 skipped a per-topic stall alert, as the story said.
* Story 6.6 golden covers `config/`, `examples/imdb`, `examples/social`; `var/` YAML files are generated or old
  artifacts and were not included.

## TODO after S4 merges (not done here, by design)

When S4 (`feat/v7-s4-dlq`) merges, rebase this branch (only `docs/USAGE.md` sections 5, 8, 11, 13 and new files are
touched here) and run `tests/test_single_topic_compat.py` first: the single-topic golden must stay identical.

1. Multi-topic DLQ examples: a `dead_letter` variant of `examples/multi_topic` (and the social / IMDb variants) showing
   the per-type DLQ (`<type>.dlq`, `.dlq.parked`, `.parked-pairs`), `replay-dlq --source-topic`, and the
   `ParkedPairIndex ... not caught up` warning; consider a counter for barrier give-ups (S4 note).
2. USAGE sections 3, 10, 12: update the DLQ/replay/retry text and remove the remaining "precedence together with
   `dead_letter` is refused until v7-S4" sentence in section 3; add the migration warning (set `dead_letter.topic`
   to the old name before adding a second topic) to the section 5 runbook as well.
3. DLQ growth alerting and sizing (S4 observation: DLQ growth under missing endpoints); the DLQ backlog gauge stays
   `{kind,name,queue}`. Extend `monitoring/prometheus/alerts/dlq.yml` and its `alert_tests` only if an expression changes.
4. S4 nightly proof: add `plan/v7/slice-4/docker_proof/run.sh orderby|arrival|bulk` (needs its own stack via
   `stack.sh up`, ports 29892 and 28287) to `ci-multi-topic-proofs.yml`.
5. A DLQ-enabled zero-lock-wait case in `tests/integration/test_multi_topic_relationship.py`, reusing the S4 `arrival`
   dataset (the one that exercises the sequenced parked-pair index end to end), plus DLQ-empty assertions that then
   check the real `<type>.dlq` topic.
6. Remove the S2/S5 start-time refusals documented in `plan/v7/completion_slice-2.md` and `completion_slice-5.md`
   if S4 has not already; check `docs/USAGE.md` section 5's mode table text for anything that still says "refused".

## Review round (Opus review of a208d44)

* M1: new fixture `tests/golden/fixtures/single_topic_dlq.yaml` (node and edge `dead_letter` with default names,
  explicit `topic`/`parked_topic`, an `order_by` edge, a disabled block); the golden was re-recorded on the archived
  pre-v7 tree (4382960) and the current tree's dump is byte-identical. A test asserts the golden contains non-null DLQ
  names and the order_by edge, so None == None can no longer pass.
* M2: `lag_series` now comes from a real `RelationshipBulkMonitor` refresh on a fresh registry (labels and value),
  in both trees.
* M3: the zero-wait guard filters completed `LIKES` entries (multi-line log records) of the `neo4j` database, excludes
  `Query started` and SHOW TRANSACTIONS, requires >= the expected number of write transactions
  (`ceil(records / 2000)` per stage, `len(plan.chunks)` for planned), polling up to 10 s, then sums `waiting`. Observed
  122 to 853 entries per case against bounds of 13 to 120.
* S1: precedence in lease and classic runs the real `cli._run_precedence_substages` (real `RelationshipBulkMonitor`,
  control topic, snapshots, zero-lag proof) with a fake `DockerService` whose containers are in-process fleets
  (`cli._stop_exact_containers` patched to stop them). The test also asserts one launch per topic in order.
* S2/S3: `lock_wait._query_log_bytes` reads rotated files oldest-first then the live file; `poll_show_transactions`
  takes an optional `samples` list and the test asserts at least one successful sample.
* S4-S6: pair count must be a multiple of 10; order_by checks `a_only == pair index` when a wins and rejects keys
  outside `{updated_at, src, a_only}`; arrival checks every stored value was published for its pair; the hub is now
  really a hub (item 0 gets half of the pairs); docstrings corrected.
* S7: the golden pins the plan JSON minus `stats` (timings differ run to run, so all of `stats` is excluded, not only
  `type_topics`), node container launch commands, and edge launches through `topics=edge.topics`. The chunk hash
  depends on `test_edge_planner.FakeCluster` (documented). Node loader group ids are computed inline in
  `node_loader.main` and are not pinned (documented).
* S8: test with sequential single-topic sub-stage monitors on one registry: series equal the type's topics.
* S9: panels 23-25 filter `kind="cli"`, NOT `kind="edge"` as the review suggested: the relationship monitor exports
  the gauge with `kind="cli"` and the retry consumer with `kind="retry-consumer"`; `kind="edge"` never has this
  series. Hidden variable `edge_type` (`label_values(graph_loader_consumer_lag_records{kind="cli"}, name)`), the
  repeated panel is in its own row. `make monitoring-validate` passes.
* S10-S13: `.env` wording corrected (the CLI never reads `.env`; compose lets exported values win; the IMDb scripts use
  `override=False`; `make monitoring-up` sources `.env` over exported values), IMDb backfill topic must exist,
  USAGE section 13 claims match the golden, generator comment and README reworded.
* S14: separate manual workflow, artifacts uploaded, Neo4j pinned, memory and internet documented, actionlint run.
