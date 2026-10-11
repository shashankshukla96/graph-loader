# Completion: v7-S3 planned mode over all topics of a type

Branch `feat/v7-s3-planned`, rebased onto `feat/multi-topic` 2281060 (S1 plus its review fixes).
Files: `src/loader/edge_planner.py`, `src/orchestrator/planned_edges.py`, `docs/USAGE.md` (section 6),
`tests/test_planned_multi_topic.py` (new).

## Stories

* 3.1 Boundary: done. `capture_edge_boundary` covers `{t for e in edges for t in e.topics}`; the
  publish-before-plan check (`_verify_boundary_unchanged`) walks that boundary, so a changed end offset on
  any topic (tested on the second) aborts the plan. `planned_edges.py` uses only the chunk topic: unchanged.
* 3.2 Read tasks and order: done. `kafka_read_tasks` iterates `(edge, topic)` pairs; `KafkaReadTask` has
  `topic_index` (= `edge.precedence_rank(topic)`); `task.order = (topic_index << 20) | first partition`
  (range-checked: 4096 topics, 2^20 partitions, else `EdgePlanError`). The chunk key and the chain sort key
  already embed `order`, so the chain is `(topic index, partition group, offset)`. Order for index 0 equals
  the old first-partition order, so single-topic plans are unchanged. Task splitting stays per topic; the
  fair share is computed over all topics. Golden tests: pair in `a` (v1) and `b` (v2) stores v2 with
  merged properties (`b` omits `weight`, `a`'s survives); list reversed stores v1; identical for workers 1
  and 4, in memory and forced-spill (1 MiB, tiny shares). Chains are identical across worker counts; chunk
  messages are identical across memory budgets for a given worker count. Chunk boundaries follow the task
  split (worker count) as before, so "byte-identical plan across worker counts" holds for the chains and
  resources, not for chunk cut points (same as the pre-existing behaviour for one topic).
* 3.3 Memory with many topics: done, no planner change needed. Spill buffers are keyed per task/cell, and
  handlers per `(topic, partition)` of the worker's own tasks. Tests: 50 topics x 4 partitions under a 1 MiB
  row-buffer budget (row buffers only; the Kafka client floors of 16 MiB or more still apply) keep `peak_buffered` within budget and assign each non-empty partition exactly once by one
  consumer; with 4 workers the assignments are disjoint and the chains equal the 1-worker plan. A node of
  degree 6 in each of two topics (hub_degree 10) becomes a hub; it does not with one topic.
* 3.4 order_by: verified, nothing to implement. `planned_writer.main` builds `ApocLockedEdgeWriter(...,
  explicit_locks=False, sort_rows=False)`, the same writer class and `build_apoc_locked_upsert_query` as the
  other modes, which already has the `sort_rows=False` + `order_by` conditional-SET branch, binds
  `$order_by`, and collapses same-pair rows per batch. The S1 cartographer's "no order_by reference in
  planned_writer.py" is correct but the path is shared. Added a test that pins the query and the bound
  parameters for a planned-shaped writer, with and without `order_by`. Cross-chunk behaviour is covered by
  the Docker run (pair split over chunks, see below).
* 3.5 Plan file: done. The Kafka planner records `stats.type_topics = {type: [topics]}` (no format
  change); `validate_plan_against_schema` fails closed with a clear message when a recorded list differs
  from the schema's (different topics or order), or has no entry (or a non-list entry) for a type. A Kafka
  plan without the record predates multi-topic types: refused when a schema type lists more than one topic
  or a single topic absent from `stats.boundary`; only producer-source plans skip the check (review
  must-fix 1 and 2).
* Straggler sites: the S1 list's S3 entries (`edge_planner.py` 1487, 1509, 1518-1534, 1638) are moved.
  `planned_writer.py`, `planned_dispatcher.py`, `edge_plan.py`, `planned_edges.py` and the planned branches
  of `src/cli.py` contain no `edge.topic` use (their `.topic` is the chunk topic). `src/cli.py` is untouched.

## Documentation

`SET r += props` merges (a later row overwrites only the properties it carries) is documented in the
`edge_planner.py` module docstring ("Ordering") and in `docs/USAGE.md` section 6, together with the
precedence/arrival/order_by behaviour and the plan topic check.

## Tests and lint

* New `tests/test_planned_multi_topic.py`: 15 tests. Full unit suite on the rebased branch: 1870 passed,
  29 deselected; `ruff check src tests examples` clean.

## Docker proof (own stack p0v3-*, images `:v7s3`, removed afterwards)

Kafka on cpuset 25; Neo4j 5-enterprise + APOC 2G/2G on cpuset 20-23 with the repo query-log config;
`LOADER_CPUSET=24`. Social dataset, 2000 persons, VISITED fed by two topics (`social-visited`,
`social-visited-b`, 4 partitions, keyed by source) plus a `note` property: 4033 records in the first topic,
2258 overlapping and 362 new-pair records in the second; `--edge-plan --plan-writers 4 --plan-workers 2
--plan-buckets 2 --plan-chunk-rows 40`. Expected graph computed independently (chain-order replay).

| Run | Rule | Result |
|---|---|---|
| prec | `topic_conflict: precedence`; `b` omits `note` on half of the overlapping pairs | rc 0, `exact_match: True` (4124 VISITED rels; `b` wins `times`, `a`'s `note` kept where `b` has none), all other types exact |
| ord | `order_by: times`; `b` carries an older value on half the overlapping pairs, plus a second newer `b` record for some | rc 0, `exact_match: True`; the newest value wins whichever topic it came from |

Query log (database `v3db`, both runs): 2249 completed entries, every one `waiting: 0`. Orphan containers:
none. Cleanup done (containers, network, images); `:latest` untouched. Scripts and outputs are in
`/mnt/graph-loader/bench/p0/v7s3/`.

## Deviations

* No separate reviewer subagent was run inside this slice; the orchestrator runs the Opus review.
* The stack `SHOW DATABASES` readiness loop in the recipe's `waitup.sh` (`RETURN 1` against `system`)
  does not work; the scripts here use `SHOW DATABASES`.
* The order_by proof used unique `times` values per pair, so outcomes do not depend on tie handling
  (ties keep the stored value, unchanged from the other modes).

## What S4-S6 must pick up

* S4/S5: planned mode never writes or reads relationship DLQs, so nothing for S4 there. S5's
  `precedence` sub-stages are not relevant to planned mode, which orders by chain.
* S2 still owns the classic/lease `edge.topic` sites (`cli.py` bulk and stream launchers, metric labels,
  `_missing_schema_topics`, `relationship_bulk_monitor`). A multi-topic `--edge-plan` run does not reach them
  (verified by the Docker run), but `_missing_schema_topics` (stream preflight) still uses `edge.topic`.
* S6: the order/partition bit split of a task order (20 bits of partition, 12 of topic index) is a planner
  limit of 4096 topics per type.
* Plan-file note: `stats.type_topics` is the only new plan field.

## Review follow-up (Opus review of 248aabb)

* Must-fix 1 and 2: done (`validate_plan_against_schema`, tests for old Kafka plans, reordered lists,
  producer plans, and a `type_topics` record missing a type or holding a non-list).
* Group overhead: each `(task, cell)` `Group` (about 544 B) stays resident after its rows spill. It is now
  charged (`GroupStore.group_bytes`, `GROUP_OVERHEAD_BYTES`) and lowers the flush threshold
  (`GroupStore.limit`, floor a quarter of the row budget), so the budget is honest with many topics.
* Cost documented in `docs/USAGE.md` section 6: a unit gets at least one chunk per topic touching it (more,
  smaller transactions), because chunks never span tasks.
* Wording: the 1 MiB claim covers row buffers only (Kafka client floors of 16 MiB or more apply). A
  single-topic plan's JSON gains `stats.type_topics`, so the plan digest changes; chunk messages stay
  byte-identical.
* A >4096-topic type is not rejected in `schema.py` (out of scope); planning fails with
  `topic index N / partition P exceed the planner's order range`.
* Added: in-repo integration test `tests/integration/test_planned_order_by_chunks.py` (real Neo4j via
  the `neo4j_driver` fixture; one row per chunk, later-listed topic carries the older value; ran in 13 s),
  and unit tests for precedence through a hub unit, repeats inside one topic plus across topics, the
  2^32-1 order bound, and the group-overhead accounting.

### Deferred proposal: cross-topic chunk sharing

One planner task could span several topics of a type, reading them with one consumer and sorting its rows
by `(topic_index, partition, offset)` before chunking. Chunks would then cross topic boundaries within a
unit, so a unit touched by N small topics would need about `rows / chunk_rows` chunks instead of up to N.
Expected benefit: fewer, fuller transactions and fewer chunk messages for types with many small topics; no
change for one large topic. Cost: a task's rows must be held (or merge-read) in that order, so the
per-topic task split and worker balancing must be redone per type. Not implemented.

### Single-topic regression check (owner priority: bulk load time)

Synthetic social data through `ParallelEdgePlanner` (inline worker, fake producer), 1.58M rows, 5 edge types,
B=64, chunk rows 1238, 1 worker, comparing f5ac6e4's full overhead charge against 248aabb behaviour in one
process. Row flushes were identical (0 at 1024 MiB, 1 at 512 MiB, 2 at 256 MiB), spilled bytes differed by
under 4% (145 vs 150 MB), and wall time was within machine noise (6.4-6.5 s vs 6.5-8.5 s at 1024 MiB; 6.9-8.7 s
vs 7.2-7.0 s at 512 MiB; 9.4-10.1 s vs 8.7-9.0 s at 256 MiB; runs alternated, other jobs on the host). To
make the single-topic behaviour exactly unchanged, the overhead up to a quarter of the row budget is now free:
`GroupStore.limit = flush_bytes - max(0, group_bytes - flush_bytes // 4)` (floor a quarter). Ten types at
B=64 (about 22 MB) fit the allowance at the smallest auto budget (256 MiB, allowance about 22 MB), so they
flush exactly as before; only many-topic loads pay.
