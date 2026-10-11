# Completion: v7-S2 stream and bulk (classic, lease) over all topics of a type

Branch `feat/v7-s2-stream-bulk`, rebased on `feat/multi-topic` 2281060 (S1 merged, incl. `schema_data_topics`).
Files: `src/loader/edge_loader.py`, `src/loader/edge_execution.py` (one line), `src/orchestrator/relationship_bulk_monitor.py`,
`src/orchestrator/docker_service.py`, `src/cli.py`, tests, `plan/v7/slice-2/docker_proof/`.

## Stories

* 2.1 Loader subscribes to all topics: done. `EdgeLoader(topics=...)` (legacy `topic=` still accepted; default is
  `edge_config.topics`; passing both `topic` and `topics` raises); `consumer.subscribe(list(topics))`; group id unchanged (`{prefix}-{type}` stream,
  `{prefix}-{type}-{run_id}` bulk). Static bulk assignment now round-robins the sorted `(topic, partition)` list over replicas (replica i owns
  entries i, i+N, ...; equals `p % N` for one topic, no idle replicas when topics are small; per-topic metadata
  and committed-offset lookups, a missing topic fails closed naming it, the ASSIGNMENT event lists `(topic, partition)`).
  Ledgers, commits, revokes and pause/resume were already keyed by `(topic, partition)`; tests prove a mixed batch with
  the same partition number and overlapping offsets in two topics commits a contiguous prefix per `(topic, partition)`
  and that a revoke settles the whole buffered batch before unassign. CLI: `--topics a,b` (container gets `--topic` for one
  topic, `--topics` for several; `run_edge_loader(topics=...)`, `topic=` kept).
* 2.2 Lanes and order_by topic-agnostic: verified, no code change. `MixAndBatchPartitioner.route` keys on the record's
  endpoints only (same pair, two topics => same lane/direction, test). `_collapse_same_pair_rows_for_order_by` works on rows,
  so cross-topic rows of a batch keep the newest and rows lacking the order value are untouched (test).
* 2.3 Boundary and zero-lag: done. `_topics_by_edge_type`; `_discover_topology` builds `{(topic, partition)}` over all topics,
  a missing/errored topic raises `OffsetResolutionError` naming it. Boundary snapshot, quiescence check, commit check and lag
  refresh already iterate `(edge_type, topic, partition)`. One addition: an absent commit is accepted when the partition was
  empty at the snapshot (`low == high`, previously only `high == 0`), so a retention-trimmed empty topic beside a busy one
  does not hang the stage (single-topic types share this, it was a latent hang). cli.py call sites now pass
  `topics=edge.topics` (bulk classic, lease, isolated phase, rotating, stream fleet, lease fleet).
  Late records after the snapshot are NOT out of the run: `_assert_quiescent` fails the stage (fail closed); the empty-partition
  rule uses the same snapshot (`low == high`) in `wait_for_completion` and `verify_zero_lag`, and also accepts a stale commit
  at or below `low` (reused run id). Replica sizing: edge replicas are schema-declared (`edge.replicas`); `auto_settings` sizes node topics only and has no edge
  path, so there is nothing to sum (the story text assumed one). The keyed-label count at `cli.py:~370` counts NODE topics.
* 2.4 Lease fleet: verified. `grep -n topic src/orchestrator/lease_server.py src/loader/lease_client.py` returns nothing;
  resources are label + key bucket (`lease_keyed_labels` come from endpoint labels). No edits, `test_lease_server.py` untouched.
* 2.5 Lag metrics: done. The existing `{kind,name,topic}` gauge is emitted per `(type, topic)` already; stream
  supervision pre-touches one series per topic when it starts (`_pretouch_lag_series`; bulk stages export nothing; test: 2
  series, then 0 and 30 after a refresh with one topic drained). No new label; the cardinality guard test passes.
* Review additions: `_require_supported_topic_conflicts` in `_handle_start` (after schema load, before any launch) calls
  `require_no_precedence_in_stream(schema, args.mode)`; `edge_loader.main` calls it too with `--mode`. Preflight uses
  `schema_data_topics`, builds the list outside the broker `try` so schema errors are not reported as "could not list topics".

## Deviations and decisions

* Classic and `--edge-leases` bulk with `topic_conflict: precedence` fail closed at start (they cannot order topics before
  the S5 sub-stages; silently degrading to arrival order would be wrong). `--edge-plan` is allowed (S3 orders by task). S5
  must remove this refusal (`_require_supported_topic_conflicts` in `src/cli.py`) when it adds the sub-stages.
* Minimal S4-owned touches, because every multi-topic start otherwise raises `MultiTopicEdgeError` on `edge.topic`:
  `edge_loader.py` (`entity_topic=edge_config.dead_letter_entity`, `edge_parked_pair_index_topic_name(edge_config)`),
  `edge_execution.py:1929` (`edge_dead_letter_topic_name`), `cli.py` retry-topic filter and `_ensure_dead_letter_topics` /
  `_ensure_retry_topics` (`edge.dead_letter_entity`, same as `edge.topic` for single-topic types). Names are identical for
  single-topic types. NOT done for S4: partition count of a multi-topic DLQ (`ensure_dead_letter_topic(source_topic=<type>)`
  would create a one-partition DLQ), the parked-pair index across topics, replay, retry consumer. To avoid silent misbehaviour
  `start` now fails closed before any side effect (`_require_supported_topic_conflicts`): a multi-topic type with
  `dead_letter` enabled is refused (bulk classic/lease and stream), and stream also refuses any DLQ-enabled edge type next to a
  multi-topic type (the retry consumer would crash late on `edge.topic`). Planned mode ignores edge `dead_letter` and is exempt.
  S4 removes these refusals. The Docker proof runs without `dead_letter`.
* Existing test doubles (`SimpleNamespace` edges) gained `topics`, `dead_letter_entity`, `is_multi_topic`; two assertions on
  `run_edge_loader` kwargs changed `topic` to `topics`.
* The story's "`cli.py:370` lease-count consumer counts records across all topics" concerns node topics only (see 2.3).
* Not touched: retry_consumer, parked_pair_index, dead_letter, monitoring/, schema.py, edge_planner, planned_*. `edge_for_topic`
  was not needed (no topic-to-type lookup exists in these paths).

## Tests

* New `tests/test_multi_topic_stream_bulk.py` (48 tests): subscription, static assignment across topics, mixed-topic commit and
  revoke, CLI/`run_edge_loader` topics args, lane routing, order_by collapse, monitor topology/coverage/boundary (lagging topic
  keeps stage open, empty and retention-trimmed topic do not block, uncommitted non-empty partition blocks), lag series count,
  preflight (all topics, schema errors not blamed on the broker), start-time precedence check (`_handle_start` refuses in
  stream before any launch; `edge_loader.main` refuses), DLQ provisioning does not raise for multi-topic without DLQ.
* Full suite: 1903 passed, 29 deselected (`-m "not integration and not smoke"`, excluding `test_dev_environment.py`); baseline
  on S1 was 1840 + S1's own additions. `ruff check src tests examples`: clean.
* Not run here: `tests/test_streaming_lease_e2e.py` (Docker, nightly).

## Docker proof (shared stack, database `v7s2`, images tag `v7s2`, LOADER_CPUSET=12-17)

Self-contained scripts: `plan/v7/slice-2/docker_proof/run.sh stream|lease|classic` (+ `proof.py`). Social-style Person/Company +
WORKS_AT fed by two topics (`v7s2-works-at-a` 4 partitions, `v7s2-works-at-b` 3), `order_by: since`, **`replicas: 3`**, 12,000
records over 3,738 distinct pairs (heavy overlap, unique `since` per record, shuffled publish order). The script refuses to run
unless the database is the proof database, asserts rc, the exact graph (per pair the record with the greatest `since`), zero lag
on all 7 partitions, and that every query-log entry for the db and run window has `waiting: 0`. Run ids are unique per execution
(a reused bulk run id conflicts with leftover control-topic events, found in an earlier attempt).

| Run | rc | Graph | Lag | Query log (db v7s2, window) |
| --- | --- | --- | --- | --- |
| stream (3 replicas, SIGTERM after verify) | 0 | exact (3738/3738) | 0 on 7/7 partitions | 321 entries, all `waiting: 0` |
| bulk `--edge-leases` | 0 | exact | 0 on 7/7 | 119 entries, all `waiting: 0` |
| bulk classic | 0 | exact | 0 on 7/7 | 26 entries, 3 with `waiting > 0` (waiting 5 to 11 ms) |

Classic bulk serialises replicas with APOC node locks by design, so lock waits between its 3 replicas are expected there; the
zero-wait guarantee is the lease/stream path (both clean). Cleanup: database dropped, `v7s2-*` topics and groups deleted, no
containers left, `:v7s2` images removed.

## What S3-S6 must pick up

* S3: `edge_planner.py` (`edge.topic` at ~1487/1509/1518/1529/1534/1638) and `planned_*` still raise for multi-topic types.
  `relationship_bulk_monitor.py` is not used in planned mode.
* S4: finish DLQ: provisioning partition count over the type's topics (`cli.py` `_ensure_dead_letter_topics`,
  `ensure_dead_letter_topic`), retry consumer, parked-pair index across topics (`edge_loader.main` builds the index with the
  type-wide name; arrival-rule types in stream use it), replay `--source-topic`.
* S5: remove the classic/lease `precedence` refusal in `_require_supported_topic_conflicts`; sub-stages must run topics of a
  type in `precedence_rank` order with the existing bulk monitor (it now supports per-topic launches only if the loader
  takes `--topics <one>`, which `run_edge_loader(topics=[t])` already does).
* S6: document `--topics` on the edge-loader entrypoint, the lag series bound (one per topic), and that a never-consumed empty
  partition (`low == high`) no longer blocks bulk completion.
