# Completion: v7-S4 type-wide DLQ, retry consumer, parked-pair index, replay-dlq

Branch `feat/v7-s4-dlq`, rebased on feat/multi-topic ffdeb92 (dca2e2c plus the final retry-consumer rework, rounds
4-5). Files: `src/loader/dead_letter.py`, `src/loader/dead_letter_replay.py`,
`src/loader/parked_pair_index.py`, `src/loader/retry_consumer.py`, `src/loader/edge_loader.py`, `src/cli.py`,
`docs/USAGE.md`, tests, `plan/v7/slice-4/docker_proof/`. Not touched: `schema.py` (the S1 helpers were enough),
`edge_execution.py` (S2 had already moved its DLQ name to `edge_dead_letter_topic_name`), monitoring.

## Stories

* 4.1 Naming: done using the S1 helpers (`edge_dead_letter_topic_name`, `edge_parked_topic_name`,
  `edge_parked_pair_index_topic_name`, `dead_letter_entity`). Every edge DLQ site in cli (retry topic list,
  `_dead_letter_edge_types`, `_ensure_dead_letter_topics`, `_ensure_retry_topics`, replay) and retry_consumer `main()`
  now resolves per type; `main()` used to raise `MultiTopicEdgeError` for ANY schema containing a multi-topic edge.
  Names: `<type>.dlq`, `<type>.dlq.parked`, `<type>.dlq.parked-pairs`. The index follows the resolved DLQ (as in S1 and
  for single-topic types), so "`.parked-pairs`" in owner decision 2 is read as `<type>.dlq.parked-pairs`. An explicit
  `dead_letter.topic` keeps all three names when a second topic is added (tested).
* 4.2 Provisioning: `ensure_dead_letter_topic` / `ensure_parked_pair_index_topic` take `source_topics=[...]` (exactly
  one of `source_topic` / `source_topics`); partitions = max over the topics with metadata (a topic without metadata is
  skipped with a warning; none at all falls back to 1, warned). `_ensure_dead_letter_topics` passes the parent edge's
  `topics`; parked and index topics are sized from the DLQ. Provisioning stays once in `_handle_start` from the
  schema's parent edges; precedence sub-stages never provision (tested: no `ensure_*` call during sub-stages, loader
  containers get `edge_type=WORKS_AT` and load the parent edge, whose `dead_letter_entity` is the type). An existing
  DLQ is never resized (documented).
* 4.3 Index and retry consumer across topics: the index topic is per type and the pair key never had a topic, so a
  pair outstanding from topic a already blocks its records from topic b (live `is_blocked`). The ordering check found
  a real bug: `is_earlier_than` / `earlier_outstanding` (and the retry consumer's own `_identity_of` comparisons) order
  a pair's records by `(original_topic, original_partition, original_offset)`. Across topics that is topic NAME
  order: a pair DLQ'd from `b`, then a later record blocked behind it from `a`, was retried a-first and the older b
  record overwrote the newer a record (reproduced in `test_without_the_sequence_the_topic_name_order_would_invert_the_pair`;
  the Docker `arrival` run is built so that this order would fail it). Fix:
  * `ParkedPairIndex(sequenced=True)` (live writer and retry consumer pass `edge.is_multi_topic`): each `add` stamps a
    per-pair causal `seq = 1 + max(seq)` over the pair's outstanding records in the local view (a re-add keeps its
    seq); the order key is the strict lexicographic `(seq, topic, partition, offset)` (`order_key()`), used by
    `is_blocked_by_earlier` and `earlier_outstanding`. A record blocked behind another one has seen it, so it sorts
    after it; a total order, so no cycles.
  * Single-topic types: unsequenced, every seq 0, wire value byte-identical (`{"first_failure_at_ms":N}`); an
    unsequenced add that finds leftover sequenced entries of its pair (the type went back to one topic) ties with the
    highest seq so identity order decides instead of sorting the new record first.
  * Rebalance barrier: `EdgeLoader(on_partitions_assigned=...)` tells the index the assignment; the first sequenced
    `add` after it waits (bounded 10 s, one snapshot per rebalance, stops on SIGTERM) until the index has caught up, so
    the new owner of a topic-partition sequences after the previous owner's records (offset order kept).
  * Retry consumer: every pair-order comparison goes through `_order_of` / a per-batch `order()` cache (held blockers,
    blocked delay, follower sort and `min`, and after the rebase be0fa09's tombstone chains, `pair_rows` sort and
    `written_hi` guard). `_write_candidates` takes `order_keys` from the batch. Equality/set uses of identities stay.
  * `order_by`: no index (unchanged); retried rows use the conditional SET (tested on the retry writer's query, and in
    Docker). `precedence`: bulk only, no index; documented that a lower-ranked topic's DLQ'd row replayed after a
    higher-ranked sub-stage overrides precedence.
  * The DLQ key is the normalized `[source_key, target_key]`, not the Kafka key, so a pair's records from all topics
    share one DLQ partition (Docker: `pairs_split_over_dlq_partitions: 0`). Correctness rests on the index order, not on
    co-location.
* 4.4 replay-dlq: `--source-topic T` (repeatable) filters on `original_topic`, each T validated with
  `schema.edge_for_topic(T)` (a node replay requires T == the node topic). A multi-topic type without `--target-topic`
  republishes each envelope to its own `original_topic`; before any live publish a validate-only pass (after the full
  filter) checks every `original_topic` is one of the type's topics and every target topic exists, so an unknown topic
  aborts with zero publishes (a dry run reports the same error; the live pass re-checks and names the replayed count;
  an empty envelope is skipped, not fatal). `--parked` resolves `<type>.dlq.parked`; the default DLQ for
  `enabled: false` is `dead_letter.topic or <dead_letter_entity>.dlq`. Single-topic: target = the topic, unchanged.
  Replaying into an `arrival` / `precedence` type logs the ordering warning.
* 4.5 Bulk opt-in: both topics' rows route to `<type>.dlq` in bulk (unit test with the real sink and
  `build_edge_execution`; Docker lease run). Planned stays fail-closed (untouched).
* Refusals: `_require_supported_topic_conflicts` keeps only the stream precedence refusal; the S2 multi-topic+DLQ, the
  stream "DLQ type next to a multi-topic type" and the S5 precedence+DLQ refusals are gone. Their tests now assert
  acceptance (`test_multi_topic_stream_bulk.py`, `test_precedence_substages.py`, `test_bulk_default_mode.py`).
* Docs (USAGE §3 "Dead-letter queues across topics", §3 validation, §5 bulk default, §10 replay, §12 limitations):
  per-type DLQ and partitions, ordering by conflict rule (including that the cross-replica race is the normal case
  for multi-topic `arrival` and that the older value then wins), topic-membership changes need a stopped fleet and a
  drained DLQ, the migration warning (now also naming the parked and index names), replay per original topic.

## Review (workflow 03, one Opus reviewer, plan + code)

Plan review: must-fix replay partial abort (validate pass added) and topic-membership change inverting order (tie rule
+ docs); should-fix rebalance inversion (barrier), precedence wording, standalone ordering (documented). Code review:
no must-fix; should-fix one-snapshot-per-rebalance, barrier honours SIGTERM, plainer arrival-race wording (all done);
nits done: per-batch order cache at the top of the batch, empty envelopes skipped in validation, cli single source of
truth for the DLQ name. Not done (nits): a metric for barrier give-ups; `_blocked_delay_seconds` counting a no-longer-
indexed rerouted entry as earlier (only lengthens a delay, capped).

## Tests

* New `tests/test_multi_topic_dlq.py` (11): the retry consumer over a REAL `ParkedPairIndex` (in-memory Kafka): pair
  parked from b then blocked on a retries b-then-a for both DLQ orders; the unsequenced control inverts; interleaved
  per-topic offsets; names; retry consumer `main()` per type with a sequenced index; a single-topic DLQ type next to a
  multi-topic one; the EdgeLoader assignment hook (and a failing hook); order_by conditional SET on the retry writer;
  bulk classic 2-topic opt-in.
* `test_parked_pair_index.py` +10 (wire bytes, seq, rebuild, re-add, second replica, legacy entries, tie rule,
  barrier, bounded barrier, SIGTERM), `test_dead_letter_replay.py` +14, `test_dead_letter.py` +6 (3 and 8 partitions ->
  8, idempotent, missing metadata, index, argument check), CLI and precedence tests updated.
* Full suite on ffdeb92: 2151 passed, 30 deselected (`-m "not integration and not smoke"`, excluding
  `test_dev_environment.py`); this slice adds 45 test items (baseline 2094 at 146d5c4, 2139 with this slice there). `ruff check src tests examples` clean; `make monitoring-validate` SUCCESS.

## Docker proof (private stack p0rg-*, database `v7s4`, images `:v7s4`, LOADER_CPUSET=31)

`plan/v7/slice-4/docker_proof/stack.sh up|down` (p0rg-net; p0rg-kafka cpuset 30; p0rg-neo4j 5-enterprise + APOC, 2G heap /
2G page cache, cpuset 26-29, query log mounted on the host) and `run.sh orderby|arrival|bulk` (+ `proof.py`, `env.sh`:
every connection variable exported, endpoints printed before each run, the project `.env` never read). WORKS_AT reads
`v7s4-works-at-a` (4 partitions) and `v7s4-works-at-b` (6), `replicas: 2`, `dead_letter.enabled`. Final runs on the code
as committed (rebased on ffdeb92); the same three runs had also passed on the be0fa09 rebase.

| Run | What | Result |
| --- | --- | --- |
| `orderby` (stream, `order_by: since`) | 4,002 records (2,001 per topic, shuffled), edges published before nodes, so every row of both topics went to the DLQ (DLQ held 4,002 before nodes were published). One orphan pair (person 999999, never published) with one row per topic. | One `WORKS_AT.dlq` with **6** partitions (max of 4 and 6) at start. The retry consumer drained it once the nodes arrived: **exact graph 1,380/1,380** (0 missing, extra or wrong). DLQ: 7,679 envelopes from both topics (a 3,963, b 3,716), 1,381 pairs, 0 pairs split over DLQ partitions. Parked: exactly the orphan's 2 rows (one per topic). `replay-dlq --source-topic a --dry-run` matched exactly a's 3,963 envelopes; a foreign `--source-topic` exits 1; `--parked --dry-run` matched 2. After publishing the orphan's Person, `--parked --source-topic b` republished 1 envelope to topic b (the edge appeared with b's values), then `--parked --source-topic a` republished the older a row to topic a, and the graph stayed exact **1,381/1,381** with b's newer value kept (order_by). Retry group 0 lag on 6/6, edge group 0 lag on 10/10, cli rc 0 after SIGTERM, 0 containers left, query log **1,146 entries, all `waiting: 0`**. |
| `arrival` (stream, `topic_conflict: arrival`, no `order_by`) | 1,500 records on topic **b** published and fully DLQ'd first, then 1,500 on topic **a** (names sort the other way), then nodes. 783 pairs, 582 of them with records in both topics. | **Exact 783/783**: every pair ends with its last-indexed record (a's last if the pair has one, else b's), i.e. the parked-pair index sequence replayed each pair b-then-a across topics and replicas. An unsequenced index would order a before b (the unit control test shows the inversion). Retry group 0 lag on 6/6, edge group 0 lag on 10/10, rc 0, 0 containers left, query log **4,389 entries, all `waiting: 0`**. |
| `bulk` (plain `start --mode bulk`, `route_missing_endpoints_in_bulk`) | Nodes and the 4,002 records published first. | `edge_mode=lease reason=dead_letter` chosen by the default, rc 0, exact 1,380/1,380 (orphan absent). `WORKS_AT.dlq` with 6 partitions holds exactly the 2 orphan rows, one per original topic. Edge groups 0 lag on 10/10, 0 containers left, query log **79 entries, all `waiting: 0`**. |

Harness note: the first `orderby` run (pre-rebase images) passed every check except my edge-group lag query, which
used the wrong group name (the stream fleet group is `<prefix>-<type>-<run_id>`); checked by hand then (10/10 at 0
lag), fixed in `run.sh`, and green in every later run. The first `arrival` attempt parked rows because the harness's
`max_retry_attempts: 30` with a 3 s max delay ran out while topic b waited for its nodes; parking unblocks a pair's
later rows by design, so that run's expectation was void; the harness now uses 200 attempts / 5 s for `arrival`.

Observation (pre-existing retry-consumer behaviour, not changed here): while endpoints are missing the DLQ grows fast.
In `arrival` 3,000 records became about 96k DLQ envelopes over about two minutes (59k `blocked_by_earlier_unresolved_row`
re-publishes of rows behind a re-DLQ'd root, the rest re-DLQs at the 5 s max delay). The DLQ is unlimited-retention.
Worth a look by the owner of the retry consumer: holding a pair's followers locally while their root is re-DLQ'd,
instead of re-publishing them.

Cleanup: p0rg-kafka, p0rg-neo4j and p0rg-net removed, `:v7s4` images removed, password file deleted. The run outputs
and the query logs stay in `/mnt/graph-loader/bench/p0/v7s4/` (scratch data).

## Deviations

* The index default is `<type>.dlq.parked-pairs` (derived from the resolved DLQ, as S1 implemented and the docs said),
  reading owner decision 2's "`.parked-pairs`" as a suffix of the DLQ name.
* Story 4.3 called the index "likely no change"; it needed the causal sequence and the rebalance barrier (above), plus
  `order_key()` and `note_assigned()` as new API. The retry consumer changes are limited to ordering comparisons, the
  `order_keys` argument of `_write_candidates` and `main()` naming; its failure handling is untouched.
* After rebasing on be0fa09, its three new pair-order sites (tombstone chains, `pair_rows` sort, `written_hi`) compared
  raw identities; they were moved to the order key in a separate commit (`fix(retry): be0fa09's pair-order sites ...`).
  `test_multi_topic_dlq.py` fails without it. ffdeb92's `flush_tombstones` works on `pair_rows` positions and needed no
  change.
* Replay preflight: the validate-only pass reads the selected DLQ range twice for a multi-topic live replay (a
  single-topic replay reads it once, unchanged).
* `EdgeLoader` gained `on_partitions_assigned` (S2's file) for the barrier.

## What S6 and S7 must pick up

S6:
* Docs/runbook: the per-type DLQ in examples (social/IMDb multi-topic variants with `dead_letter`), the migration
  warning in the runbook, the `ParkedPairIndex ... not caught up` warning (consider a counter for barrier give-ups),
  and the replay recipes above. Add `plan/v7/slice-4/docker_proof/run.sh orderby|arrival|bulk` to the compat/nightly list
  (it needs its own stack: `stack.sh up`; ports 29892/28287).
* The zero-lock-wait integration test can reuse the `arrival` dataset: it is the one that exercises the sequenced
  index end to end.
* DLQ growth under missing endpoints (observation above) for alerting/sizing; the DLQ backlog gauge stays `{kind,name,queue}`.

S7 (several label pairs per type):
* `ParkedPairIndex._pair_key` is `[source_key, target_key]`; with several label pairs it must include the labels (or the
  topic's label pair), and so must the DLQ key (`route_unresolved_endpoint` key_bytes) and the retry consumer's pair
  grouping (`envelope.key_bytes`). The order key and sequence are per pair key, so they follow automatically.
* The retry writer resolves a DLQ'd row through the type's writer; with several label pairs it must pick the pair from
  the envelope's `original_topic` (each topic binds its own pair, owner decision 3), e.g. via `GraphSchema.edge_for_topic`.
* The DLQ stays one per type; `replay-dlq` already routes each envelope to its own `original_topic`.

## Review round 2 (Opus review of 72bde81: not ready)

Commits on top of 72bde81: da9ef6b (M1), b3604b2 (M2, S1, S2), 891d02f (S3), deb6e1f (S4), cce2cc1 (S6),
832fcce (S7 warning), 8de12bf (fuzz tests), 68dfaff (S5 harness), 3851d8a (M1 budget, harness attribution).

| Item | Resolution |
| --- | --- |
| M1 DLQ amplification | Fixed in `retry_consumer.py`. A follower blocked behind an earlier row of its pair whose next attempt this consumer knows (re-DLQ'd in this batch, a pending re-DLQ wake, held or due here) is held locally until just after that wake instead of re-published. A pair's held rows wake as soon as a row of the pair is written or parked: without that, followers with lower DLQ offsets held for a short time re-entered every small batch ahead of their roots and starved them (found by the sequenced fuzz, seed 76, with `max_batch_size=2`). A blocked copy that still has to be published waits the blocker's remaining wait, else the type's max delay, never 0.5 s. Hold budget: 90% of `max_records_per_partition` outstanding (deviation from the prototype's `// 2`: holding a fetched row costs no memory, the budget only keeps room to fetch the roots' re-DLQ'd copies; the first Docker rerun at about 500 outstanding per DLQ partition overflowed `// 2`). Optional root-local hold not done. Tests: `tests/test_retry_amplification.py` (O(roots x cycles), scaling with roots, never-arriving endpoint = (k+1) x (attempts-1) copies, no starvation at batch caps 1-3, the over-budget path); 5 round-4 tests updated from "published blocked copy" to "held locally". |
| M2 stale-view re-stamp | Fixed by the preferred design: an admission barrier in `EdgeLoader`. On every assignment (any loader with a parked-pair index) the assigned partitions are paused and prefetched records deferred until `ParkedPairIndex.caught_up(snapshot taken after the assignment)`; non-blocking, no timeout into a stale view, a warning every 30 s after 10 s, and the new gauge `graph_loader_parked_pair_index_wait_seconds{kind,name}` (catalogue now 25 families; MONITORING.md). The add-time barrier (`note_assigned`) is removed. Tests: never admits on a lagging view (an hour later still held), retry of a failed snapshot, re-arm per assignment, prefetched records deferred, a re-read keeps its seq. |
| S1 | Covered by the admission barrier: `is_blocked` is gated too, and nothing waits inside the lease-held write path. USAGE corrected. |
| S2 tie | `seq = highest + 1 if highest else 0`; test from pT_tie; USAGE corrected. |
| S3 replay | Bounds captured once; a selection pass filters, keeps one envelope per `(original_topic, partition, offset)` (its latest DLQ copy), applies `--limit`, validates targets; a live run re-reads the same bounds cut at the last selected positions and publishes exactly the selection. `matched` counts distinct records (documented). Tests: dedupe, distinct dry-run count, DLQ growing between the passes (incl. a bad-topic record), limit with dedupe. Replay test factory now gives distinct offsets by default. |
| S4 migration preflight | `start` refuses (before creating any topic) when a DLQ-enabled type without an explicit `dead_letter.topic` has a former default DLQ (`<topic>.dlq` or `<type>.dlq`, not another entity's topic) holding records, naming the `dead_letter.topic` to set. Both directions documented. Tests with fake admin/consumer. |
| S5 harness | Arrival asserts the parking lot is empty; growth assertion (every envelope is a first copy, an own-failure cycle or a blocked copy; retry-published blocked copies at most a tenth of the cycles); copies attributed by `dlq_routed_at_ms != first_failure_at_ms` (a re-DLQ keeps `loader_identity`); gap-safe `dlq()` reader with a stall timeout; replay check compares with distinct records. |
| S6 | `_single_topic_sub_edge` pins `dead_letter.topic` (and `parked_topic`) to the parent's resolved names and raises if the copy's names differ; test. |
| S7 | Warning when an existing DLQ has fewer partitions than the widest topic (never resized); tests. The probes are tests: pX (amplification, park), pS (admission barrier, re-read keeps seq), pT (tie), pB_seq (cross-topic tombstone failure), pK_seq_fuzz (80 seeds) and its control (`tests/test_retry_pair_order_fuzz.py`). |

### Probes (re-run on the final code)

* s4probes: pX_amplify 0 blocked copies (was 78-7,800 per case; 1 pair x 30 followers: 27 envelopes, was 807);
  pX_park 0 blocked copies, (k+1) x 9 cycles (k=30: 279, was 4,434); pK_seq_fuzz 0/300 and 0/1000 bad;
  pK_seq_control 124/200 bad (sensitive); pK_count 0 bad; pB_seq, pA, pB, pL, pM unchanged; pT_tie fixed (a/2 blocked
  behind b/40); pS_barrier_timeout cannot run (the add-time barrier API is gone) -- `pS_admission.py` (same lagging
  view, in s4probes/) shows no admission for an hour while lagging, then admission, re-read seq 1, write order A1, B1.
* r5probes (copied and pointed at this worktree): every output identical to the same probes run against ffdeb92
  (`git archive ffdeb92`), including pH_lone/pH3 with all 9 code x traffic arguments.

### Counts

Full suite 2255 passed, 30 deselected; `ruff check src tests examples` clean; `make monitoring-validate` SUCCESS.

### Docker rerun (fresh p0rg-* stack, images `:v7s4` from 3851d8a, cleaned up afterwards)

| Run | DLQ envelopes before (72bde81) | after | Breakdown after | Other checks |
| --- | --- | --- | --- | --- |
| arrival | 95,827 (59k blocked copies) | 37,143 (12.4x input) | 3,000 first copies, 34,143 root re-DLQs (783 roots x about 43 cycles at the harness's 5 s max delay over a ~3.5 min wait), **0** retry-published blocked copies | exact 783/783, parking lot empty, lag 0, rc 0, 0 containers, 4,884 query-log entries all `waiting: 0` |
| orderby | 7,679 | 6,495 (1.6x) | 4,002 first copies, 2,493 own-failure cycles, **0** blocked copies | exact 1,380 then 1,381 incl. the orphan (order_by kept b's value), replay `--source-topic a` = 2,001 distinct records, foreign topic exit 1, `--parked` 2, lag 0, rc 0, 0 containers, 1,153 entries all `waiting: 0` |

The 3x-of-input guide is not met for `arrival`: what remains is O(roots x cycles), one re-DLQ per root attempt,
inflated by the harness's 5 s max delay and the long node-less phase (with the default 60 s max delay the same wait
is about 6 cycles, about 2.6x). Reaching a constant factor needs the optional root-local hold (re-DLQ a root only every
N attempts), not done.

An intermediate rerun (before the budget change and the harness attribution fix) failed two harness checks: it
compared the replay count with envelopes instead of distinct records, and it overflowed the `// 2` hold budget
(70,673 envelopes, 31k blocked copies). Both are fixed above.

## Review round 3 (Opus review of c4b4151: nearly ready) and the S6 post-S4 TODO

Rebased on feat/multi-topic b2fa3d3 (v7-S6 merged); one conflict (`src/utils/metrics.py`, S6's cardinality comment
next to the new gauge), resolved by keeping both. `tests/test_single_topic_compat.py` passes unchanged (10/10, golden
not re-recorded). It pins names, commands, lag series and planned chunks, not runtime behaviour, so it does not show
the one deliberate single-topic change of S4: a stream loader of a single-topic DLQ type (non-`order_by`) now also
pauses its partitions after each assignment until its parked-pair index view has caught up (the admission barrier;
the same race existed for single-topic types).

| Item | Resolution |
| --- | --- |
| M-a stop bypass | `_finish_run` drops the deferred records while the admission barrier is up (uncommitted, the next owner re-reads them); test runs the REAL `_finish_run` (and the admitted control); pU_stop_bypass: 0 buffered |
| S-a over-budget growth | A published blocked copy waits `clamp(now - first_failure_at_ms, max_delay, 1 h)`; a fetched copy whose earlier rows already left the index is due at once. Measuring pX_budget_park showed the plain change stalls: copies held for an hour fill the partition, the root's next copy can not be fetched, the chain parks one row per hour (and other pairs of the partition would stall). So over the hold budget neither a long hold nor the held-blocker hold applies (rows take the publish path, which frees their slot). Result (31-row chain, endpoint never arrives): blocked copies 2,348 -> 902 at cap 20, 3,389 -> 2,289 at cap 10, 3,921 -> 3,402 at cap 5; every row parks (cap 5 within 16,000 s instead of 4,000 s). Not O(log t) at small caps: a follower over the budget must be fetched again to reach its root's next copy. Endpoint-arrives runs drain with 3-6x fewer copies. Tests in `tests/test_retry_amplification.py` |
| S-b replay order | Each record is filtered by its latest copy (current `reason_code`) but replayed from its FIRST copy (same key and value), in first-arrival order: a root re-DLQ'd after its followers is replayed first; tests |
| S-c preflight | `start --accept-former-dlq TOPIC` (repeatable) for a drained DLQ kept for audit or a `<type>.dlq` owned by another deployment; deleting the old topics documented as the expected step; tests |
| DOC sizing | USAGE §12 "DLQ sizing" and MONITORING "DLQ growth and sizing": at most `records x (max_retry_attempts + 1)` plus blocked copies; the 12.4x of the arrival harness explained |
| TODO 1 example | `examples/multi_topic/graph_schema_dead_letter.yaml` (stream, `RATED` with one `RATED.dlq`), `publish --only nodes|edges`, `verify --types`, README section (replay `--source-topic`, the `reason=parked_pair_index_lagging` warning and the gauge); tests |
| TODO 2 docs | §5 runbook "Dead-letter queues (one per type)" with the migration warning both ways; no "refused until v7-S4" text left |
| TODO 3 monitoring | Dashboard panels "DLQ envelopes per consumed record" and "Parked-pair index admission wait"; no alert expression changed (`dlq.yml` untouched: backlog and routing alerts already cover growth) |
| TODO 4 CI | Second job `slice-4-dlq-docker-proofs` in `ci-multi-topic-proofs.yml` (own stack via `stack.sh`, `V7S4_*` overrides for state dir, cpusets and Neo4j image, artifact upload); actionlint 1.7.12 clean |
| TODO 5 integration | `test_dead_letter_arrival_stream_type_wide_dlq_drained_by_the_retry_consumer`: in-process DLQ-wired lease fleet + the real retry consumer subprocess; one 4-partition `LIKES.dlq` for both topics, exact arrival graph, empty parking lot, no per-topic DLQ, zero lock waits through the S6 filtered guard. The whole file ran once: 9 passed in 71 s (DLQ case: 3,957 envelopes for 3,000 records, 1,036 LIKES write entries all `waiting: 0`) |
| TODO 6 | No S2/S5 refusal text left in src, tests, docs, examples or workflows |

Probes (final code): pU 0 buffered; pX_amplify / pX_park 0 blocked copies; pX_budget drains at caps 5/10/20 (638,
808, 483 blocked copies, was 2,721, 3,884, 900); pX_budget_park as above; pK_seq_fuzz 0/1000 bad, pK_seq_control
124/200 bad, pK_count 0; pB_seq, pA, pB, pL, pM unchanged; pS_admission and pT fixed; r5probes and pH_lone/pH3 (9
argument sets) identical to ffdeb92.

Counts: full suite 2300 passed, 39 deselected; ruff clean; `make monitoring-validate` SUCCESS (22 panels, all metric
names known); actionlint clean; integration file 9 passed.

Docker (fresh p0rg stack, images from the final code, cleaned up): `arrival` exact 783/783, parking lot empty,
36,928 DLQ envelopes for 3,000 records (3,000 first copies + 33,928 root re-DLQs, **0** blocked copies), retry and
edge groups at 0 lag, rc 0, 0 containers left, 5,209 query-log entries all `waiting: 0`.
