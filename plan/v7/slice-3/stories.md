# Slice 3: planned mode (`--edge-plan`) over all topics of a type

Depends on S1. Parallel with S2 (disjoint files). Files:
`src/loader/edge_planner.py`, `src/loader/edge_plan.py`, `src/orchestrator/planned_edges.py`,
`src/loader/planned_writer.py` (only for order_by verification), `src/loader/edge_spill.py` (check only),
`tests/test_edge_planner.py`, `tests/test_external_planner.py`, `tests/test_planned_*`, `tests/reference_edge_planner.py`.
Planned units stay `(type, src resource, tgt resource)`; chunk topic is the run's own chunk topic (not a data topic): unchanged. Dispatcher unchanged (it serves chunks by id).

## Story 3.1 Boundary over all edge topics
`capture_edge_boundary` (edge_planner.py:1474) already returns `topic -> ranges` keyed by `{edge.topic ...}`: change to `{t for e in edges for t in e.topics}`. The publish-before-plan check (line ~1671: "end offsets changed") covers every topic. `planned_edges.py:123,219-235` use the CHUNK topic: unchanged.
Acceptance: boundary for a 2-topic type contains both topics; changed end offsets on the second topic abort the plan.

## Story 3.2 Read tasks and deterministic topic order
`kafka_read_tasks` (1500): iterate `(edge, topic)` pairs; tasks carry `topic_index` (position in `edge.topics`). `KafkaReadTask` gets `topic_index: int`; `task_order` = `(topic_index, first_partition)` instead of first partition (edge_planner.py docstring "Ordering", `task order is its first partition`). The plan's unit chain becomes ordered by `(topic_index, partition group, offset)`; chain stays identical for any worker count. The split of a large topic over `workers` tasks stays per topic; share computation uses the total over all topics.
Important: repeated pairs from two topics now apply `SET r += props` in topic-list order along the chain: later-listed topic wins = `precedence`. Under `arrival` the same deterministic order applies (document as arbitrary, not guaranteed).
Acceptance: golden test: pair P in topic a (v1) and b (v2) -> stored v2; reverse the list -> v1; identical for workers=1 and workers=4 and for forced-spill (tiny memory budget) and in-memory planning; plan is byte-identical across worker counts.

## Story 3.3 Bounded memory with many topics
Spill planner buffers are keyed per unit/cell, not per topic; confirm memory estimate and the one-consumer-per-worker design (edge_planner.py:~318-360, handlers keyed `(topic, partition)`) scale with number of topics. Add a stress-ish unit test: 50 topics x 4 partitions for one type under a small memory budget; peak RSS/ buffer counters stay within the budget and handler dict is per assigned partitions only.
Hub/bucket stats (degree counts, `hub_degree`, bucket assignment) are computed from merged rows per resource, so a hub formed by two topics is detected: test a node below `hub_degree` in each topic but above when combined.
Acceptance: the above tests; `PlanSettings` unchanged.

## Story 3.4 order_by in planned mode (verify, else implement)
Planned writer shares `build_*upsert_query` with the classic/lease writers (`edge_execution.py:95-157`), but `planned_writer.py` has no `order_by` reference. Verify the planned write path binds `$order_by` and uses the conditional SET; if it does not, add it (same query text for types without order_by, byte-for-byte). `_collapse_same_pair_rows_for_order_by` semantics across chunk boundaries: an older row in a later chunk must not overwrite (conditional SET handles it).
Acceptance: planned load of 2 topics with `order_by` where the LATER-listed topic carries the OLDER value stores the newer value; repeat with pair split across two chunks of one unit.

## Story 3.5 Plan file and validation
`planned_edges.py:188-197` compares plan edge types/labels to schema: add per-type topic-list comparison if plan metadata records topics (`edge_plan.py:506`). Plans built before this change (single `topic`) must still load.
Acceptance: old plan fixture loads; plan with different topic list than schema fails closed with a clear message.

Test plan: unit (no Docker) for 3.1-3.3, 3.5; Docker planned e2e for 3.4 (extend `tests/test_planned_harness.py` / `tests/test_social_planned_integration.py` pattern; skipped without Docker).
