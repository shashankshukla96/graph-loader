# Phase 7: multiple topics per relationship type (and multi-label-pair types)

Status: Navigator/Cartographer output, written against `feat/streaming-observability`
at 03de1a6. Binding rules: `plan/v5-v6-orchestration.md` (zero Neo4j lock waits,
generic schema-driven library, leases/planned mode, DLQ/retry semantics).

## 1. The feature

Today `edges[].topic` is one string and `GraphSchema.validate_edge_endpoints`
raises `Duplicate edge types detected in schema.` when a type repeats
(`src/models/schema.py:783`). Item (A): `edges[].topics: [a, b]` with `topic:`
as single-topic shorthand. Item (B): one type over several label pairs
(LIKES Person->Post and Person->Comment). B is separable from A (it changes
endpoint labels, not topics) but reuses A's per-topic structure, so it is the
last slice.

## 2. Conflict semantics (the key design question)

Kafka orders records only inside one partition. Two topics feeding one type can
both carry the same (source key, target key) pair. Rule set:

| Rule | schema | stream | bulk classic | bulk lease | planned (`--edge-plan`) |
| --- | --- | --- | --- | --- | --- |
| 1 `order_by` | `order_by: updated_at` (exists, P5-S4) | Deterministic. Conditional `SET r += props` only if strictly newer; order of arrival irrelevant. Ties keep the earlier-arriving value (document: ties across topics are arrival-order). | Deterministic (same query, `edge_execution.py:121-157`). | Deterministic. | Deterministic only if the planned writer applies the conditional SET. The planned writer shares the query builder (`edge_execution.py:95`) but this must be VERIFIED and tested (S3-story 4); if not, S3 adds it. |
| 2 `topic precedence` (later-listed wins) | `topic_conflict: precedence` | NOT achievable. No cross-topic order and no timestamp on the record. Best effort is worse than nothing. Validation rejects `precedence` in stream; `start --mode stream` fails closed. Option (see Q3): Kafka CreateTime as an implicit order key. | Achievable by running a type's topics as sequential sub-stages, each bounded by the captured boundary, in list order. Loses intra-type cross-topic parallelism (other types still run in parallel). | Same as classic (sub-stages). | Exact and free: the plan chain already orders rows by `(task order, row order)` (`edge_planner.py:~56-66`). Make task order `(topic index, first partition)`; last write in the unit chain wins. Property-level merge (`SET r += props`), not whole-relationship replace. |
| 3 arrival order | `topic_conflict: arrival` | Nondeterministic for pairs in several topics. | Nondeterministic. | Nondeterministic. | Deterministic but arbitrary (topic-index order). Do not advertise as a guarantee. |

### Recommendation

1. `order_by` is the recommended default and the only rule that is correct in
   every mode, including stream. Because a type with `topics` of length >= 2
   has a real ambiguity the owner must own, validation REQUIRES an explicit
   choice for multi-topic types: either `order_by` is set, or
   `topic_conflict: precedence | arrival` is declared. No silent default.
   A type with one topic (including `topics: [a]`) is unchanged and needs
   nothing. `order_by` set => rule 1 (no `topic_conflict` needed; setting both
   is an error unless `topic_conflict: order_by`).
2. `precedence` allowed only for bulk modes (classic, lease, planned); rejected
   at schema-validate time when `loading.mode: stream`, and at `start --mode
   stream` (the schema may be shared between runs, so both checks).
3. `arrival` is allowed everywhere; it emits a startup WARNING naming the type
   and topics ("pairs present in several topics resolve in arrival order").
4. Parked-pair index (non-`order_by` DLQ types) becomes type-wide so a parked
   pair blocks later records of that pair from ANY topic of the type.

Why: a required explicit rule costs one line in the YAML, costs nothing for
existing schemas, and removes a silent correctness trap. Precedence is offered
because bulk backfills with a clean "base topic then corrections topic" shape
are common and planned mode makes it free, but we refuse to offer a
guarantee we cannot keep in stream.

## 3. Design decisions shared by all slices

* Canonical model: `EdgeConfig.topics: list[str]` (non-empty, unique, no blanks).
  `topic:` accepted on input (`before` validator) and normalized to
  `topics=[topic]`. Exactly one of `topic`/`topics` may be present. To catch
  every straggler, `EdgeConfig.topic` remains as a READ property that returns
  the single topic and RAISES `MultiTopicEdgeError` when `len(topics) > 1`.
  Around 40 call sites use `edge.topic` (cli, edge_loader, edge_planner,
  relationship_bulk_monitor, retry_consumer, dead_letter naming); S1 makes the
  property raise, the 2-topic fixture then fails loudly in each consumer slice.
* Topic ownership: a data topic may belong to only one entity (existing check).
  A topic may not repeat across types and may not equal a DLQ/parked/index
  topic.
* Consumer group: still one group per type (`<prefix>-<type>[-<run_id>]`); one
  consumer subscribes to the type's whole topic list. No group-per-topic
  (cardinality and rebalance cost scale with topics). Exception: precedence
  sub-stages (S5) use `<prefix>-<type>-t<i>-<run_id>`.
* DLQ: ONE DLQ per TYPE, not per topic. Default name for `len(topics) == 1`
  stays `<topic>.dlq` (compat, parked `.dlq.parked`, index `.dlq.parked-pairs`
  follow). For `len(topics) > 1` the default is `<type>.dlq` (type names are
  Cypher identifiers, so safe), overridable by `dead_letter.topic`. Reason: the
  parked-pair index is keyed by pair and must be type-wide to keep per-pair
  order across topics. Envelope already carries `original_topic`,
  `original_partition`, `original_offset` (identity is already topic-qualified,
  `dead_letter.py:98`). DLQ partition count = max partitions over the type's
  topics. Changing topic order or membership of an existing single-topic type
  does NOT rename its DLQ (single-topic keeps `<topic>.dlq` only while it has
  one topic; document the rename hazard when adding a second topic: set
  `dead_letter.topic` to the old name first).
* Metrics: keep `{kind, name=<type>}`; never add a per-record topic label.
  `graph_loader_consumer_lag_records` already has `topic` (bounded by schema
  topics; `name=<type>, topic=<t>` for a K-topic type adds K series). Document
  the bound: series <= number of schema topics. DLQ backlog gauge stays
  `{kind,name,queue}`.
* Leases: `lease_server.py` knows only label+key-bucket resources, not types or
  topics. Expect NO change. Verification story is included in S2 (grep + test
  that a 2-topic stream fleet against the unchanged server gets zero lock
  waits).
* Zero lock waits: no slice adds a Neo4j lock path. Sub-stages (S5) are
  sequential, never concurrent writers on the same type+pair.

## 4. Slices

| Slice | Title | Depends on | Parallel with |
| --- | --- | --- | --- |
| S1 | Schema model, `topics`, conflict-rule validation, compat | none | none (foundation) |
| S2 | Stream and bulk classic/lease: subscription, boundary, verification, lag | S1 | S3 (disjoint files) |
| S3 | Planned mode: boundary, read tasks, task order, order_by in planner | S1 | S2 |
| S4 | Type-wide DLQ, retry consumer, parked-pair index, replay-dlq | S1, S2 (cli.py shared) | none with S2; may overlap S3 |
| S5 | Bulk topic precedence (classic/lease sub-stages) + stream refusal | S2 (S3 for planned part) | S4 (mostly disjoint) |
| S6 | Metrics/monitoring, examples (social+IMDb), docs, zero-lock-wait integration test, compat suite | S2, S3, S4, S5 | none |
| S7 | (B) One type, several label pairs | S1, S3, S4 | none |

Slice B is cleanly separable: it changes `EdgeConfig.nodes` (single
`SourceTargetConfig`), planner resources/units, the plan file's endpoint-label
check (`planned_edges.py:188-197`), the `ParkedPairIndex` pair key, the lease
key-label derivation, and isolation/dependency graphs, none of which A needs.
Ship A first; S7 is optional for the owner's timeline.

## 5. Shared file-touch map (for parallelism judgements)

Hot files: `src/cli.py` (S2, S4, S5, S6), `src/models/schema.py` (S1, S4, S7),
`src/loader/edge_loader.py` (S2, S4), `src/loader/edge_planner.py` (S3, S7),
`src/orchestrator/relationship_bulk_monitor.py` (S2, S5). S2 and S3 are the
safe parallel pair. S4 and S5 both touch `cli.py`; run S4 then S5, or merge
carefully.

## 6. Out of scope

Node labels with several topics; cross-type conflict; changing planned-mode
DLQ fail-closed behaviour (P5-Q3); a topic regex/pattern subscription;
reading Kafka timestamps as a default ordering (Question Q3).

## 7. Open questions for the owner

See the final report of this cartography run and `plan/v7/slice-1/stories.md`
(Decisions D1-D5). Recommended answers are recorded there.

## Owner decisions (binding, 2026-09-30)

These answer the questions above. Where earlier text in this file disagrees, this section wins.

1. **Conflict rule: an explicit rule is required.** A relationship type with two or more topics must declare one of the following. There is no silent default, and single-topic types and existing schemas are unchanged.
   - `order_by`: works in every mode.
   - `topic_conflict: precedence`: later-listed topic wins. Allowed in bulk classic, lease and planned modes only. It is rejected in stream mode at schema validation and again at `start`.
   - `topic_conflict: arrival`: allowed everywhere, with a startup warning, and documented as nondeterministic.
   - Ties on `order_by` keep the existing value.
2. **One DLQ per type** for multi-topic types.
   - Default names: `<type>.dlq`, `.dlq.parked` and `.parked-pairs`, overridable with `dead_letter.topic`.
   - Single-topic types keep `<topic>.dlq`.
   - `replay-dlq` republishes each record to its `original_topic` and gains `--source-topic`.
   - The docs warn to set `dead_letter.topic` to the old name before adding a second topic to a type that already has DLQ data.
3. **Item B (S7) comes after multi-topic (S1–S6).** Each topic binds its own label pair; the pair is never inferred from the record.
4. **Kafka record timestamps as an implicit stream order key: deferred.** Stream users set `order_by`.
5. **Bulk default is planned mode.** A plain `start --mode bulk` (no `--edge-plan`, `--edge-leases`, or classic flag) uses planned mode (`--edge-plan` behaviour with auto-derived settings), which gives zero lock waits by construction. The classic APOC-locked writer is available only with an explicit `--edge-classic` flag, documented as the one mode that can wait on locks. If the schema opts any edge type into bulk DLQ routing (planned mode ignores `dead_letter` and fails closed), the default is `--edge-leases` instead (also zero lock waits); the CLI logs which mode it chose and why.
6. **The conflict rule applies per label pair** (confirmed 2026-10-02). A rule is required only when some label pair of a type is fed by two or more topics. A declared `precedence` or `arrival` on a type where no pair has two or more topics is accepted, with a "has no effect" warning. See slice-7/stories.md section 10.
7. **D1 write evidence is per label pair for multi-pair types** (confirmed 2026-10-02). A written row of one pair is not evidence against a failing row of another pair. See plan/v5-v6-orchestration.md, D1.
8. **`server`-class errors (MemoryPool) count under the D1 evidence rule**, like capacity errors, and never without evidence (confirmed 2026-10-02).
9. **Option A, durable resolved markers, is approved** (2026-10-02). It closes the replay-resurrection gap that option B leaves open across a retry-consumer restart.

