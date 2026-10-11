# Completion: v7-S7 (item B) one relationship type over several label pairs

Branch `feat/v7-s7-multi-pair`, branched from `feat/multi-topic` d334cdd and rebased onto 8748274, then 540fe71
(the fix/dlq-overbudget-hold merge and the integration env-isolation commit). Plan: `plan/v7/slice-7/stories.md`
(the refreshed stories, with section 10, the orchestrator decision on Q1, and section 11, the line references
re-checked at d334cdd: `src/` there equals s4:72ca167). Binding: `plan/v7/slices.md` owner decisions 1-5 (decision 3:
each topic binds its own label pair, never inferred from a record) and `plan/v5-v6-orchestration.md`.

`LIKES` can read `likes-posts` and `likes-posts-backfill` (Person->Post) and `likes-comments` (Person->Comment).
Every record is normalised, written, dead-lettered, indexed, retried, planned and replayed with the pair of the
topic it came from.

## Stories

| Story | Result |
| --- | --- |
| 7.1 single-pair golden | `tests/test_single_pair_compat.py` + `tests/golden/single_pair_compat.json`, recorded on `git archive 4382960` (pre-v7). It pins the query texts (APOC-locked x3, preflight, classification, legacy upsert and preflight, native_disjoint), both normalisers over 5 records, the DLQ envelope key and value bytes and the encoded envelope hash, the parked-pair index wire key and add() value, and BucketCellRouter cells and resources (unkeyed and keyed). It covers all 26 edges of the 4 compat schemas. The dump is byte-identical on 4382960, d334cdd, 8748274, 540fe71 and HEAD. |
| 7.2 schema | `topics` entries may be `{name, nodes, source_key_property, target_key_property}`; a string entry inherits the top-level pair (R1-R8). New `TopicEndpoints`, `EndpointPair`, `MultiPairEdgeError`; `EdgeConfig.topic_endpoints`, `endpoint_pairs`, `pair_for_topic`, `pair_labels_for_topic` (an unknown topic maps to the pseudo pair `("?", topic)`), `endpoint_labels`, `has_self_reference`, `single_pair`, `pair_index`, `pair_view` and `topic_view` (cached outside the pydantic privates, behind a lock, DLQ names pinned to the type's), and `GraphSchema.pair_for_topic`. Types whose topics bind one pair normalise to today's shape: the `exclude_unset` dumps and key bindings of every shipped and fixture schema are identical to d334cdd. Q1 is implemented as decided: the conflict rule is evaluated per label pair. |
| 7.3 write path | The loader normalises with the topic view. `build_edge_execution` builds one single-pair execution per pair view and composes them. Classic uses `PairSplitLaneExecution` (pairs one after another, later pairs skipped after a failure). A guard writer refuses type-wide writes. Every single-pair builder raises `MultiPairEdgeError` on a parent. `--slot-gating` refuses multi-pair types. |
| 7.4 leases | `PairSplitLeaseCoordinator` runs one unchanged `BucketLeaseExecutionCoordinator` per pair (its own router and client pool) concurrently, observes `lease_wait` once per outer flush (the sum), puts a real error before a drain abandon on every failed result, and gives all pairs one shared stop-drain deadline. Keyed labels are the union over pairs. `lease_server.py`, `lease_client.py` and `test_lease_server.py` are unchanged. |
| 7.5 orchestration | Conflict plan, ready labels, readiness and the missing-loader check use the union of labels. Isolation and the co-scheduled self-reference check use "any pair" (this fixes the silent `getattr(...) is True`). `--stream-clock-rotation` is refused before any side effect. Conflict families, the shared clock, the fleet contract, the rotating fleet and the isolated stream phase refuse multi-pair types. Precedence sub-edges are topic views, and their self-reference is checked against the parent. `relationship_bulk_monitor.py` and `docker_service.py` are unchanged. |
| 7.6 planned | Read and producer tasks carry the pair view and the pair index; the precedence rank stays the parent's. `ChunkSpec.pair` and `PlannedChunk.pair`. A multi-pair plan type is `{pairs: [...], rows, chunks, units}` with 11-field chunks; single-pair plan JSON is unchanged. `validate()` checks each chunk's and predecessor's pair, per-pair sums and types, and refuses a topic listed under two pairs. The plan-vs-schema check reports "plan binds X to S->T; the schema binds it to ...". Decoder normalisers and writers are keyed `(type, pair)`. Producer sources of a multi-pair type must name `topic`, and the mapping form is refused. Hub degrees stay per label. |
| 7.7 DLQ and index | `route_unresolved_endpoint(endpoint_labels=)` gives the key `[S, T, sk, tk]`; `LabelledEndpointSink` adds it per pair. `ParkedPairIndex(pair_of_topic=)` keys its in-memory view by `(labels of original_topic, sk, tk)`; the wire bytes are unchanged, and entries written before S7 rebuild into their topic's pair. `is_blocked(endpoint_pair=)` (required on a multi-pair index), `scoped(labels)` and `pair_of(topic)`. Each pair's lease coordinator checks its own pair only. |
| 7.8 retry and replay | `_pair_from_envelope` accepts 2- or 4-element keys. A legacy 2-element key of a multi-pair type is rewritten at read to its `original_topic`'s labelled key, before any `key_bytes` grouping, so a re-DLQ or park keeps it. The retry index uses `pair_of_topic`. `PairSplitRetryWriter`: per-pair writers with their own view, router, coordinator and one drain deadline. Pairs run concurrently in lease mode and one after another in standalone mode. An unknown topic or mismatched labels gives `invalid_on_retry` (bounded park); the pair writer also refuses a key of another pair. `replay-dlq --target-topic` on a multi-pair type needs `--source-topic` values of one pair and a target of that pair (otherwise exit 1 before reading). Overflow-hold, ordering and tombstone logic are untouched. |
| 7.9 metrics | No new label. A 2-pair 3-topic type exports exactly the series (names, label sets, label values) of a 1-pair 3-topic type, with one `lease_wait` observation per flush. MONITORING.md section 4 has a paragraph on it. |
| 7.10 docs and example | USAGE sections 3 (fields, "Several label pairs per type", DLQ bullet, validation), 4, 5, 6, 7, 8, 10, 11 (5 rows), 12; the orchestrator's nit (the `max_overflow_held_per_partition` worst case is (cap + overflow) x envelope size per partition). MONITORING, README and examples/README. New `examples/multi_topic/graph_schema_multi_pair.yaml` and `multi_topic_graph.py --variant multi-pair`: a generator with colliding ids and a hub, a publisher, and a per-labelled-pair verifier that also counts cross-pair LIKES. The docs YAML test classifies mapping entries. |
| 7.11 integration | `tests/integration/test_multi_pair_relationship.py`, 6 cases; results below. |
| 7.12 Docker proof | `plan/v7/slice-7/docker_proof/{stack.sh, env.sh, run.sh, proof.py}` and a manual `slice-7-multi-pair-docker-proofs` job in `ci-multi-topic-proofs.yml`; results below. |

## Review (workflow 03: one dedicated reviewer, plan and code, 6 rounds)

* Round 1 (7.1, 7.2): M1, views of pair index 1 or above could not be split further (fixed: look up by index).
  S1: a topics subset is kept in the parent's order. S2: the view cache is thread-safe. S3: a second golden,
  `tests/golden/single_pair_multi_topic_compat.json`, recorded on 8748274, pins multi-topic single-pair types
  (sub-edges and their queries, DLQ bytes, sequenced index bytes, precedence sub-stages, plan). It is identical at HEAD.
* Round 2 (7.3, 7.4): M1, a stop-drain abandon of one pair could hide another pair's real error (fixed: the real
  error is stamped on every failed result). S1: one stop-drain budget per type. S2: pair identity leaked through the
  DLQ and index until 7.7 (fixed by 7.7, with regression tests for a parked Comment (7,7) next to a live Post (7,7),
  lease and classic, the same and the next flush).
* Round 3 (7.5, 7.7): no must or should fixes; 2 nits done.
* Round 4 (7.6): no must or should fixes; 4 nits done (orders per (type, pair), plan pair type checks, topic under
  two pairs, spawn pickling test).
* Round 5 (7.8): S1, standalone retries ran pairs concurrently without leases, so they could wait on Person locks
  (fixed: sequential without leases). Nits: replay info line; the fuzz now flags the read-time key rewriting being
  off (legacy keys reach the writer in 75/80 seeds; corrected wording, see "Final follow-ups" below).
* Round 6 (7.9-7.11): S1, the `order_by` integration data could not see a Post->Comment leak (half the Comment values
  are now older than every Post value; the Docker dataset got the same fix). S2, the DLQ case now proves that every
  Post like is written while every Comment row is parked, and that only Comment rows entered the DLQ. S3: docs give
  per-pair `worker_count`/`write_workers` in lease modes.

## Tests and counts

* Full unit suite (`tests examples/imdb/tests -m "not integration and not smoke"`, excluding
  `test_dev_environment.py`): **2526 passed, 1 skipped** (the multi-pair example schema in the shipped-schema
  check), 45 deselected. The base 540fe71 collects 2310, so this slice adds 217, 82 of them from the two-pair fuzz
  parametrisation and its control.
* `ruff check src tests examples`: clean. `make monitoring-validate`: SUCCESS (22 panels, all metric names known).
* Goldens: `single_topic_compat.json` unchanged; `single_pair_compat.json` (new) and
  `single_pair_multi_topic_compat.json` (new) identical between their base trees and HEAD.
* Sequenced fuzz: 0/80 bad orders for one pair and 0/80 for two pairs with colliding keys. The controls without the
  order key fail 41/80 (one pair) and 27/80 (two pairs). With the read-time key rewriting disabled, legacy
  `[sk, tk]` keys reach the writer in 75/80 seeds (and, because the model writer cannot attribute an unlabelled
  row, those seeds also stall), but no seed shows an order violation: this proves the rewrite is applied and keeps
  the consumer live, not that order depends on it (order rests on the parked-pair index; `p_nocanon.py`). The
  order-sensitive control is the one without the order key.
* Integration (local testcontainers, Neo4j 5.26 with query log, 20,000 labelled pairs, colliding keys, hubs Person
  0 / Post 0 / Comment 0): **6 passed in 62 s**. Zero-wait cases, query-log `waiting` sum 0 over LIKES write
  entries (bound in brackets):

  | Case | LIKES entries (bound) |
  | --- | --- |
  | order_by stream lease, 3 replicas | 1636 (12) |
  | order_by bulk lease, static | 1641 (12) |
  | order_by planned (2 pairs, Person/Post/Comment hubs) | 255 (252) |
  | arrival stream lease | 1279 (12) |
  | dead_letter stream lease + real retry consumer, Comment nodes late | 1908 (10); 13,163 envelopes, all labelled and all from the Comment topic, parking lot empty |
  | order_by classic, 1 replica | waits reported: 67 ms |

  S6's multi-topic module still passes with the shared planned harness change (both modules: 15 passed).

## Docker proof (private stack p0rh-*, database `v7s7`, images `:v7s7` built from 5bf2841's `src/`)

Stack: `p0rh-net`; `p0rh-kafka` on cpuset 30; `p0rh-neo4j` 5-enterprise + APOC, 2G heap and 2G page cache, cpuset
26-29, query log mounted on the host; `LOADER_CPUSET=31`. Every connection variable is exported by `env.sh` and
printed before each run (`KAFKA_BOOTSTRAP_SERVERS=localhost:29992 NEO4J_URI=bolt://localhost:28387
NEO4J_DATABASE=v7s7`); the project `.env` is never read. `LIKES` reads `v7s7-likes-posts-a` (4 partitions) and `-b`
(6), Person->Post, and `v7s7-likes-comments` (3), Person->Comment, with `replicas: 2`. 1,500 persons; Post and Comment
ids are both 0..39. In the order_by datasets 862 keys exist in both pairs, and half of the Comment values are older
than every Post value.

| Run | Result |
| --- | --- |
| `orderby` (stream, dead_letter) | `LIKES.dlq` provisioned with **6** partitions. With Person and Post nodes loaded, all 13 partitions consumed: **1391/1391** Post likes written, **0** Comment likes, and every Comment row (2,001) in the DLQ. After the Comment nodes arrived, the retry consumer drained it: **exact** (Post 1391/1391, Comment 1509/1509, **0 cross-pair edges**). 6,617 envelopes, **all** keyed `[S,T,sk,tk]` of their `original_topic`'s pair, **0** labelled pairs split over DLQ partitions. Parked: the orphan's row from each topic (2 labelled pairs). `replay-dlq --source-topic comments --dry-run`: matched=2001. `--target-topic posts-a --source-topic comments`: exit 1, "replaying v7s7-likes-comments records into v7s7-likes-posts-a would write them as Person->Post", 0 publishes (topic watermark 2001 -> 2001). Retry group 0 lag on 6/6, edge group 0 lag on 13/13, the edge loader logged `stage=edge-multi-pair edge=LIKES mode=lease pairs=Person>Post,Person>Comment`, cli rc 0 after SIGTERM, 0 containers left, query log **1,377 entries, all `waiting: 0`**. |
| `arrival` (stream, dead_letter) | Topic b and the comments DLQ'd first (8,144 DLQ records), then topic a (13,798), then the nodes: **exact** (Post 1291/1291, Comment 1205/1205), so each Post pair ended with its last-indexed record. All 14,175 envelopes labelled; nothing parked; retry 0 lag on 6/6, edge 0 lag on 13/13, rc 0, 0 containers, query log **3,407 entries, all `waiting: 0`**. |
| `bulk` (plain `start --mode bulk`, `route_missing_endpoints_in_bulk`) | `edge_mode=lease reason=dead_letter`, rc 0, **exact** (2900, 0 cross-pair). `LIKES.dlq` (6 partitions) holds exactly the 3 orphan rows, one per topic, each with its pair's labelled key. Edge groups 0 lag on 13/13, 0 containers, query log **178 entries, all `waiting: 0`**. |
| `planned` (no dead_letter, flagless) | `edge_mode=planned reason=default`, rc 0, **exact**. The plan has `types.LIKES.pairs` = Person->Post (2 topics, 4,000 rows, 36 chunks) and Person->Comment (2,000 rows, 18 chunks), no top-level source/target, and every chunk entry has **11** fields (pairs 0 and 1). 0 containers, query log **71 entries, all `waiting: 0`**. |
| `classic` (`--edge-classic`) | `edge_mode=classic reason=flag`, rc 0, **exact**, edge groups 0 lag on 13/13, 0 containers. Query log: 30 entries, 3 with waiting > 0 (1, 3 and 5 ms); reported, not asserted. |

Cleanup: `p0rh-kafka`, `p0rh-neo4j` and `p0rh-net` removed, the `:v7s7` images removed, and the password file
deleted. No `v7s7` container is left, and `:latest` was not touched. The run outputs, logs and query logs stay in
`/mnt/graph-loader/bench/p0/v7s7/`.

## Deviations and decisions

* **Q1** is implemented per the orchestrator decision of 2026-10-01, which is pending owner confirmation (stories,
  section 10). A `precedence`/`arrival` on a type where no pair has two topics resolves to `conflict_rule ==
  "single"` with a "has no effect" warning, so stream mode accepts it too.
* **Error message change.** A single-pair schema missing `nodes` or a key property now fails with "Edge 'X' topic
  't' binds no label pair: set [...]" instead of pydantic's per-field "Field required" (the fields became Optional).
  Endpoint validation messages of single-pair types are byte-identical.
* **Full dump.** `EdgeConfig.model_dump()` (without `exclude_unset`) gains `topic_endpoints: None`. No `src/` code
  dumps models, and the documented `exclude_unset=True` form is unchanged.
* **Views.** A view is an in-process object: `pair_index` and its bindings do not survive `model_dump`/`model_validate`
  (pickle keeps them). Whole-type decisions (`is_multi_topic` for the sequenced index, `conflict_rule`,
  `dead_letter_entity`, `precedence_rank`) are always taken on the parent.
* **Second golden.** `tests/golden/single_pair_multi_topic_compat.json` was added (review round 1) on top of the
  story's one golden.
* **Planned stress harness.** `tests/stress/planned_harness.py` now keys writers like `planned_writer.main`
  (single-pair keying unchanged), so the integration test can plan multi-pair types.
* **D1** was taken as written here: a WRITTEN outcome of any pair of the type counted as same-edge-type evidence.
  Superseded by the orchestrator decision under D1 in the final follow-ups (S4): evidence is per (type, label pair).
* **Precedence sub-stages** of a multi-pair type run every topic as its own sub-stage, including topics of pairs
  fed by one topic. This is correct but serial (documented).
* **Collapsing a type** back to one pair needs a drained DLQ first: a single-pair index ignores the topic. This was
  documented, not enforced; the final follow-ups (S2) add the start-time warning and refusal.
* **Not done** (review NIT N4 of round 5): one extra `json.loads` per retried envelope on single-pair types in
  `_record_from_envelope` (retry path only).
* **actionlint** is not installed here, so the new workflow job is YAML-validated only.
* **No change** to `lease_server.py`, `lease_client.py`, `relationship_bulk_monitor.py`, `docker_service.py`,
  `planned_dispatcher.py`, `mix_and_batch.py`, `node_loader.py` or `dead_letter_replay.py`.

## Commits (on 540fe71)

b44ddc8 plan, 0ed6591 7.1, 170a6c9 7.2, 5ad5930 7.3, 146ab6c review 1, 7d277bd 7.4, 0d367b6 7.5, d95ca25 review 2,
33c8d6a 7.7, 5a6e0c2 review 3, 54d33f4 7.6, a35f5d5 review 4, 35d9177 7.8, fd334e1 7.9, a070ca7 review 5, dd5c4df 7.10,
5bf2841 7.11, b005b07 review 6, then the 7.12 commit (harness, workflow job, this report).

## What follows

* Owner: confirm or reverse the Q1 decision. Reversing it changes only `_conflict_rule_is_declared` and
  `conflict_rule` in `schema.py` and their tests.
* Run the `slice-7-multi-pair-docker-proofs` job once on a GitHub runner (as for S4/S6, it has not run there yet).
* Optional: put uncontended topics of a precedence multi-pair type into one sub-stage (it is serial today); warn at
  startup when a single-pair type's index still holds entries of topics it no longer reads (the collapse case).

## Final follow-ups (branch `fix/v7-final-followups` from feat/multi-topic 239deb4)

| Item | Resolution |
| --- | --- |
| S1 | Per-pair retry lease coordinators are built with `observe_lease_wait=False`; `PairSplitRetryWriter` observes the sum once per call (`kind="retry-consumer"`). Test: 3 calls give one observation each for a 2-pair and a 1-pair type, plus `main()` wiring. |
| S2 | Multi-pair index values stamp `"labels": [S, T]` (single-pair bytes unchanged, goldens identical). Loaders and the retry consumer log `reason=index_holds_entries_the_type_no_longer_binds` for entries of removed or rebound topics (incl. a collapsed type). `start` refuses them, reading only the compacted index topic (`--accept-dlq-leftovers TYPE` overrides); not visible without a DLQ scan, so documented: `order_by` types, bulk opt-in rows, single-pair or pre-stamp rebinds. A single-pair type reads a key labelled with its own pair back as `[sk, tk]`. Collapse fuzz (40 seeds, from p_collapse_fuzz/p_collapse2) and a control (37/40 without the rewrite). |
| S3 | The 75/80 claim reworded (above): without the rewrite legacy keys reach the writer, order still holds; a test asserts exactly that. Docker `scoping` variant (Person and Post nodes present, Comment nodes late), added to `run.sh` and the CI job; the stack scripts take `V7S7_PREFIX`/ports/`V7S7_IMAGE_TAG`. |
| S4 | D1 evidence keyed by (type, label pair) for multi-pair types; a pair failing with nothing written is reported as a stall of the type (ERROR names the pair). Plan note in `plan/v5-v6-orchestration.md`. |
| Nits | Q1 warning once per process; a key labelled with another pair than its topic is parked by its first batch, never grouped; `replay --target-topic` outside the entity warns (`reason=target_outside_entity`); USAGE: removed-topic leftovers park; planned hub path (`plan_hub`) pinned in both goldens (pre-v7 4382960 and 8748274 recordings, all other keys identical). |
| Part B | A real retry-consumer bug (two causes): parallel re-DLQ lineages of stale/duplicate copies (fixed: one copy per record and batch, skip copies at or below a re-DLQ'd attempt), and lowest-offset-first capped batches starving roots behind re-woken followers (fixed: longest-due first). |

Docker `scoping` (private p0ri-* stack, images `:final1`, database `v7s7`): all checks PASS. Before the Comment nodes,
1291/1291 Post likes written live and exact (652 colliding keys), 0 Comment likes, and the DLQ held the 1,500 Comment
rows and no Post row; after the Comment nodes, exact (1291 + 1205, 0 cross-pair), 7,574 envelopes all labelled and all
from the Comment topic, nothing parked, retry 0 lag on 6/6, edge 0 lag on 13/13, rc 0, 0 containers, query log 1,266
entries all `waiting: 0`. Stack, images and password file removed; outputs in `/mnt/graph-loader/bench/p0/final1/`.
