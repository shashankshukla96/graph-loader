# Completion: v7-S5 bulk topic precedence (classic and lease sub-stages) and stream refusal

Branch `feat/v7-s5-precedence`, from `feat/multi-topic` 56af85f.
Files: `src/cli.py` (bulk paths only), `docs/USAGE.md`, tests, `plan/v7/slice-5/docker_proof/`.
Not touched: retry_consumer, parked_pair_index, dead_letter, schema.py, `supervise_stream`, retry-consumer launch.

## Stories

* 5.1 Sub-stage planning: done. `_precedence_substage_plan(edge, run_id)` returns `(rank, topic, sub_run_id, sub_edge)` ordered by
  `precedence_rank`, lowest first. `_run_precedence_substages` runs each as a full single-topic `_run_bulk_relationship_edge`
  (launch, assignment coverage, own boundary capture, completion, container stop, DRAIN_COMPLETE, zero-lag). The dispatch is one
  guard at the top of `_run_bulk_relationship_edge` (`conflict_rule == "precedence"` and 2+ topics), so BOTH bulk paths that run
  edge types through it (classic and `--edge-leases`; the isolated self-reference phase too) get it, and the scheduler
  (`_run_completion_driven_bulk_edges`) is unchanged: other types keep their parallelism, only the precedence type is serial inside
  its own scheduler slot (it holds its endpoint labels for the sum of its sub-stages, as before for one stage). A failed or
  cancelled sub-stage stops the type; later topics never launch. `on_launch` re-registers the current sub-stage's containers so the
  scheduler's cancel path stops exactly the running ones; each sub-stage's `finally` stops and removes its own containers.
  Sub-edge = `edge.model_copy(topics=[topic], topic_conflict=None)`; replicas stay `edge.replicas`.
* 5.2 Correctness proof: Docker, below (lease and classic).
* 5.3 Stream refusal and matrix: `require_no_precedence_in_stream` untouched. `_require_supported_topic_conflicts` no longer refuses
  precedence in classic/lease bulk (its message now says precedence runs in bulk modes only). Precedence + `dead_letter` on the same
  multi-topic type stays refused by the existing S2/S4 DLQ guard, whose message now names precedence and, when the lease default was
  chosen because of dead_letter, says so and points at `--edge-plan`. Precedence next to a DLQ on a different (single-topic) type now
  runs under the lease default.
* 5.4 Reporting: each sub-stage logs `stage=bulk-substage ...` / `stage=bulk-substage-complete ... elapsed_s outcome` and writes
  `substage-<type>.t<rank>.json` (with `rank`, `topic`) to `GRAPH_LOADER_TIMING_DIR`; the type keeps its own `stage-<type>.json` total.
  (A first version named the rows `stage-<type>.t<rank>.json`, which the IMDb report reader's `stage-*.json` glob folded into the type,
  reporting the last sub-stage as the total; fixed by the rename.) `read_edge_substage_timings` and the report show per-topic rows under
  the type. Metrics stay `{kind,name=<type>}`.

## Review fixes (second commit)

* Per-sub-stage timing and metric files: `run_edge_loader(timing_tag="t<rank>")` names them `edge-<type>.t<rank>-<replica>.log` and
  `metrics-edge-<type>.t<rank>-<replica>.prom`, so sequential sub-stages no longer overwrite each other (existing globs still match).
* Fail-closed across sub-stages: every topic's watermarks are snapshotted before sub-stage 0; a sub-stage whose captured boundary
  (`boundary_out`) differs from the snapshot fails; after each sub-stage, and so once after the last, every finished topic is re-read and
  the type fails if any end offset changed (records appended to a finished topic would otherwise be silently skipped).
* Reruns with the same `--run-id` are refused when a lower topic still has work while a higher-ranked sub-stage group already has commits.
* Operator stop between sub-stages is checked (`_shutdown_requested`) and logged at WARNING.
* Under `--max-concurrent-bulk-edges`, a precedence type is weighted by its topic count so it starts first.

## Deviations

* Sub-stage group name is `{prefix}-{type}-{sub_run_id}` where `sub_run_id = <run_id>.topic.<i>.<hash>`
  (via `_derive_relationship_phase_run_id`), not `{prefix}-{type}-t{i}-{run_id}`. Same property (distinct group, control events and
  DRAIN proof per topic), no new naming code, bounded length.
* Boundaries are captured per sub-stage, but each must equal the snapshot taken before the first sub-stage, so (stricter than story 5.1's
  text) nothing may be appended to any topic of the type during the run.
* `_run_relationship_bulk_stages` (an unused legacy helper) is not changed.

## Tests

* New `tests/test_precedence_substages.py` (21 after the review fixes): plan order and distinct valid run ids, declared (not alphabetical) order, strictly
  sequential launch/coverage/boundary/completion/zero-lag per topic, failure stops later topics, cancelled run launches nothing,
  non-precedence multi-topic types keep one stage, per-topic timing rows, start matrix for classic/lease/planned, stream still refused,
  precedence + DLQ refused; late append to a finished topic (mid-run and after the last sub-stage), boundary differing from the snapshot,
  reused-run-id refusal, operator stop (WARNING, no later launch), exact containers stopped on failure, a precedence type running next to
  another type under `_run_completion_driven_bulk_edges`, cap weighting. Also `tests/test_docker_service.py` (timing tag file names) and
  `examples/imdb/tests/test_precedence_report.py` (per-topic rows under the type, with the correct type total).
* Updated `tests/test_multi_topic_stream_bulk.py` (3 tests) and `tests/test_bulk_default_mode.py` (1 split into 2).
* Full suite: 1996 passed, 30 deselected (`-m "not integration and not smoke"`, excluding `test_dev_environment.py`). `ruff check src tests examples`: clean.

## Docker proof (shared stack, database `v7s5`, images `:v7s5`, LOADER_CPUSET=12-17)

`[PROOF_TOPICS=a,b|c,b,a] plan/v7/slice-5/docker_proof/run.sh lease|classic` + `proof.py`. The first table is the original run (277/24 query-log entries); the review rerun adds the checks listed after it. WORKS_AT with `topics: [v7s5-works-at-a, v7s5-works-at-b]`,
`topic_conflict: precedence`, `replicas: 3`. 6,000 records, 3,000 unique pairs per topic, 2,286 pairs in both, 3,714 distinct pairs.
Topic b is published first (arrival order opposite to precedence). Topic a carries `since` and `role`; every other b record carries
`since` only, so the merge is checked (b's since wins, a's role survives). Expected graph computed exactly.

| Run | rc | Graph | Sub-stage order | Lag | Query log (db v7s5, window) |
| --- | --- | --- | --- | --- | --- |
| `--edge-leases` | 0 | exact (3714/3714, 0 missing, 0 extra, 0 wrong) | a start, a complete, b start, b complete | 0 on 7/7 partitions | 277 entries, all `waiting: 0` |
| `--edge-classic` | 0 | exact (3714/3714) | same | 0 on 7/7 | 24 entries, 2 with waiting > 0 (allowed in classic) |

Each sub-stage took about 8.6 s (a and b serial).

Review rerun (harness now also asserts: every query-log entry carries a `waiting:` field, one consumer group per topic, partitions
described equal the topic partitions, zero containers left; the dead `stream` branch was removed):

| Run | rc | Graph | Sub-stage order | Groups / lag | Query log |
| --- | --- | --- | --- | --- | --- |
| lease, topics `a,b` | 0 | exact 3714/3714 | a, b | 2 groups, 0 lag on 7/7 | 257 entries, 257 with a waiting field, all `waiting: 0` |
| lease, topics `c,b,a` (3 topics, reversed list) | 0 | exact 3877/3877 (a now wins) | c, b, a | 3 groups, 0 lag on 9/9 | 358 entries, 358 with a waiting field, all `waiting: 0` |

Zero containers left in both. Cleanup: database dropped, `v7s5-*` topics and groups deleted, `:v7s5` images removed.

## What S6 must pick up

* Document the sub-stage log lines, the `substage-<type>.t<rank>.json` rows and the `edge-<type>.t<rank>-*` files in the monitoring/runbook
  docs. Each precedence type leaves N consumer groups per run (one per topic) and needs a fresh `--run-id` per rerun; put both in the runbook.
* Add `plan/v7/slice-5/docker_proof/run.sh` (both `PROOF_TOPICS` cases) to the compat/nightly list.
* S4 must remove the precedence + `dead_letter` refusal (message in `_require_supported_topic_conflicts`, tests
  `test_precedence_type_with_its_own_dead_letter_stays_refused_until_s4` and
  `test_handle_start_refuses_bulk_precedence_with_dead_letter_until_s4`). The sub-edge made by `model_copy(topics=[topic])` resolves
  `dead_letter_entity` to the TOPIC name, so DLQ provisioning and resolution must use the parent edge (type-wide DLQ, provisioned once,
  not per sub-stage).
* The zero-lock-wait integration test in S6 can use `proof.py`'s dataset for the lease cell of the precedence type.
