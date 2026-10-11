# Slice 2: stream and bulk (classic, lease) over all topics of a type

Depends on S1. Parallel with S3. Files: `src/loader/edge_loader.py`,
`src/loader/edge_execution.py` (only if ledger keys need it),
`src/orchestrator/relationship_bulk_monitor.py`, `src/cli.py` (bulk/stream
orchestration + boundary/verification; NOT replay-dlq, that is S4),
`src/orchestrator/docker_service.py` (env for topics), tests.
No change expected in `lease_server.py`/`lease_client.py` (resources are label/key buckets).

## Story 2.1 Loader subscribes to all topics of its type
As an operator, I want one edge loader group per type that reads every declared topic.
Technical: `EdgeLoader(topic=...)` becomes `topics: Sequence[str]`; `consumer.subscribe(list)` at edge_loader.py ~273; container env/arg `--topic` -> repeated `--topics a,b` (keep `--topic` accepted). Group id stays `{consumer_group_id}-{type}` (edge_loader.py:1592) and `{prefix}-{type}-{run_id}` in bulk. Partition ledgers/commit already keyed by `(topic, partition)` (edge_loader.py:393, 615): verify contiguous per-partition commits remain correct when one batch mixes topics, and rebalance revoke uses `partition.topic` (866-938, already topic-aware).
Acceptance: a type with 2 topics drains both; committed offsets exist for every (topic, partition); a rebalance mid-batch loses/duplicates nothing (fake-consumer unit test); zero behaviour change for single-topic.

## Story 2.2 Batching and lanes are topic-agnostic
Mix-and-batch lanes partition by source/target key; a pair's rows from two topics land in the same lane (same key => same lane). Verify and test: same-pair rows from both topics are written in one lane in consumption order (arrival rule) and, with `order_by`, the conditional SET ignores order (`_collapse_same_pair_rows_for_order_by` already collapses same-pair rows in a batch by max order value: test cross-topic rows in ONE batch keep the newest, including rows missing the order value).
Acceptance: unit tests on `_collapse_same_pair_rows_for_order_by` with mixed-topic provenance; lane routing test.

## Story 2.3 Boundary snapshot and zero-lag over all topics
`relationship_bulk_monitor.py`: `_topic_by_edge_type` (line 124) and `_discover_topology` (202-205) assume one topic. Change to `topics_by_edge_type`, `raw[edge_type] = {(t, p) for t in topics for p in partitions(t)}`; `WorkPartition = (edge_type, topic, partition)` already carries the topic, so boundary capture, committed-offset checks (474-475) and the lag refresh work per (topic, partition). Missing metadata for ANY topic fails the stage (name the topic). Zero-lag and "boundary reached" require ALL topics drained. Bulk node-monitor untouched. Each topic's end offsets are captured in one snapshot before any loader starts (same as today); late records after the snapshot are out of the run (document).
`cli.py:404`, `cli.py:519/616/955/1167/1421`: replace `topic=edge.topic` with `topics=edge.topics`. `cli.py:370` lease-count consumer counts records across all topics.
Acceptance: unit tests with a fake admin: 2 topics x 3 partitions, one topic lagging keeps the stage open; an empty topic (low==high) does not block; a missing topic fails with its name; replica sizing (`auto_settings`, `replicas`) sums records/partitions over topics (auto_settings.py:156 uses node topics only: check `edge` path, add sum).

## Story 2.4 Lease/record-count based sizing and the lease fleet
Verify the lease fleet needs no topic knowledge (keyed labels from endpoints: `cli.py` lease_keyed_labels). Record the grep result in the completion file; if any topic coupling is found, fix it here.
Acceptance: `tests/test_lease_server.py` unchanged and green; no edits under `src/orchestrator/lease_server.py`.

## Story 2.5 Lag and metrics plumbing
Consumer lag gauge is `{kind,name,topic}` (keep). Ensure lag is emitted per (type, topic) for each topic in the list, with pre-touched series at startup so dashboards show zero rather than absent. No new label.
Acceptance: metrics unit test asserts series count == number of topics of the type.

Test plan: extend `tests/test_edge_loader.py`, `tests/test_relationship_bulk_monitor.py`, `tests/test_cli.py`, `tests/test_edge_rebalance.py`, `tests/test_streaming_lease_e2e.py` (Docker, nightly). Integration: see S6.
