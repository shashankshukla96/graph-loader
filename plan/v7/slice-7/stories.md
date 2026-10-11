# v7-S7 (item B): one relationship type, several label pairs -- refreshed stories

Status: Slice Cartographer output (workflow 02), refresh of `plan/v7/slice-7/stories.md`.
Written against `feat/multi-topic` b2fa3d3 (S1-S3, S5, S6 merged) and `feat/v7-s4-dlq` 72ca167 (S4, not merged,
still receiving commits: d5d410d, b761637, 308b06c, 6dd00b7, 322ce40, 72ca167 landed after its completion
report 489d0ec). Read-only; nothing was run against Docker.

Binding inputs: `plan/v7/slices.md` owner decisions 1-5 (decision 3: item B comes after multi-topic, each topic
binds its own label pair, the pair is never inferred from the record); `plan/v5-v6-orchestration.md` (zero Neo4j
lock waits, generic schema-driven library, per-pair order, D1/D2); `plan/v7/completion_slice-{1..6}.md`,
`completion_bulk_default.md` and S4's `completion_slice-4.md` (incl. review round 2).

S7 branches from `feat/multi-topic` **after S4 is merged** (and after S6's post-S4 TODO block). S7 changes
`retry_consumer.py`, `parked_pair_index.py`, `dead_letter.py`, `edge_loader.py` and `edge_execution.py`, which
are all S4 hot files. Line numbers below are given as `b2fa3d3` / `s4:72ca167`. They will drift. Re-grep with the
commands in section 9 before starting.

---

## 1. What changed against the original S7 plan

| Original S7 text | Refresh | Why |
| --- | --- | --- |
| 7.2: one `UNWIND` per pair inside one transaction | One writer per label pair, built from a single-pair **view** of the type. Execution composites split a flush by pair. A Neo4j transaction never mixes label pairs. | The query builders, `ApocLockedEdgeWriter`, `BucketLeaseExecutionCoordinator` and the classic lane path stay unchanged. EXT merges can't cross pairs. Flush-scoped `ParkedPairs` state is per pair, so `(source_key, target_key)` tuples of different pairs can't collide. |
| 7.5: "add a version/prefix compat rule" for index records | Not needed. The index wire key already holds `original_topic`, so the pair is derived from the topic **in memory**. Wire bytes are unchanged. | `parked_pair_index._record_wire_key` = `[sk, tk, original_topic, partition, offset]`. |
| 7.5: `_pair_key` includes the pair id | Yes, in memory: `(pair labels of original_topic, sk, tk)`. | S4 note. |
| (not in the original) DLQ key | Multi-pair types use `[S, T, sk, tk]`. Single-pair types keep `[sk, tk]` byte for byte. The retry consumer canonicalises legacy 2-element keys when it reads them. | About 24 retry-consumer sites group by `envelope.key_bytes`. A labelled key keeps all of them correct with no edits. |
| 7.4: plan file records labels per (type, pair) | `types[T].pairs[...]` plus an explicit per-chunk `pair` field. Both apply only to multi-pair types, so single-pair plan JSON is byte-identical. | A type may bind A->B and B->A, which have the same resource set. The chunk resources alone can't tell the direction. |
| 7.6: social example (`LIKES` Person->Product/City) | A new variant schema in `examples/multi_topic/`. `examples/social` is unchanged. | S6 precedent: changing social would change its default run and the compat golden. |
| (not in the original) Phase 4 paths | `--stream-clock-rotation`, slot gating, rotation families and the shared-clock fleet refuse multi-pair types before any side effect. | Deprecated, and each of them hard-codes exactly 2 labels per type. |

## 2. Design in one page

* **Schema.** Each `topics` entry is either a string (it inherits the top-level `nodes`/key properties) or a
  mapping `{name, nodes, source_key_property, target_key_property}`. After normalisation the model is one of:
  * single-pair type: the top-level `nodes` and keys are set, `topic_endpoints` is `None`. This is exactly
    today's shape and dump.
  * multi-pair type: the top-level `nodes` and keys are `None`, and `topic_endpoints: {topic: TopicEndpoints}`
    covers every topic.

  `EdgeConfig.topics` stays `list[str]`, so every S1-S6 API (`topics`, `precedence_rank`, `dead_letter_entity`,
  DLQ helpers, `edge_for_topic`) is unchanged.
* **Views.** `edge.pair_view(pair)` / `edge.topic_view(topic)` return a single-pair `EdgeConfig` copy: `nodes`
  and keys set, that pair's key configs bound, DLQ, parking-lot and index names pinned to the parent's (S4's
  `_single_topic_sub_edge` pattern, moved into `schema.py`). Every Cypher builder, normaliser, writer and router
  receives a view, so their code is unchanged. A guard (`edge.single_pair`) makes them raise
  `MultiPairEdgeError` if they receive a multi-pair parent.
* **Topic decides the pair (decision 3).** These all pick the pair from the topic, never from the record:
  * the loader normaliser, from the message topic;
  * the planner task, from the read topic;
  * the plan chunk, from the `pair` it records;
  * the retry writer, from `envelope.original_topic`;
  * replay, which republishes to `original_topic`.
* **Composites, not mixed transactions.**
  * Lease: `PairSplitLeaseCoordinator` runs one `BucketLeaseExecutionCoordinator` per pair, concurrently. Each
    one has its own `BucketCellRouter(S, T, B, keyed)`, and all of them use the same unchanged lease server.
  * Classic: `PairSplitLaneExecution` runs pairs sequentially.
  * Retry: `PairSplitRetryWriter`.
  * Planned: writers keyed `(type, pair)`.
* **Leases.** Resources are `Label:bucket` / `Label#digest` strings. `Post:5` and `Comment:5` are different
  resources and `Person:3` is shared, so a Person->Post and a Person->Comment cell that share a Person bucket are
  serialised by the server. **The lease server is unchanged.** Keyed labels are the union over every pair.
* **Pair identity.**
  * DLQ key: `[S, T, sk, tk]` for multi-pair types.
  * Parked-pair index: the in-memory pair key is `(labels(original_topic), sk, tk)`. Wire bytes are unchanged.
  * Retry grouping: the canonical labelled `key_bytes`.
  * One DLQ per type (decision 2). Its name and partition rule are unchanged.
* **Planned.** One read task per `(type, topic)` binds that topic's pair view. Hub degrees are counted per label
  window, so a Person's degree sums over both pairs and over every other type, while Post 7 and Comment 7 are
  different keys. A unit is `(type, source resource, target resource)`, which is already distinct per pair.
* **Metrics.** Still `{kind, name=<type>}`. Lag is per topic (bounded by the schema topics). No pair label
  anywhere. Pairs appear only in logs and plan stats.
* **Single-pair compatibility.** No wrapper objects. All of these stay byte-identical, pinned by a new golden
  recorded on the pre-v7 tree (story 7.1): query text, DLQ key/value bytes, index wire bytes, lease tokens, plan
  JSON, chunk messages, groups and container commands.

## 3. Endpoint-label use sites (re-grepped)

Grep (section 9) gives 103 matching lines in `src/`; the matched content is identical on b2fa3d3 and
s4:72ca167. They collapse to the 60 logical sites below. Five sites (E10, E27, E32, E41, E58) are not matched by the
endpoint grep but read the type's single pair implicitly. The original plan's "42" undercounted the planner,
plan-file and cli scheduling sites.

Treatment codes:

* **V**: receives a single-pair view. Code unchanged; the guard `edge.single_pair` raises on a multi-pair parent.
* **P**: becomes per pair or per topic.
* **U**: uses the union of pair labels (`edge.endpoint_labels`).
* **S**: true if any pair is self-referencing (`edge.has_self_reference`).
* **R**: refused for multi-pair types (deprecated Phase 4 path).
* **N**: no change.

| ID | File:line b2fa3d3 (s4:72ca167 if different) | What | Code | Story |
| --- | --- | --- | --- | --- |
| E1 | models/schema.py:758-759 | `source_key_property`/`target_key_property` fields | P (Optional) | 7.2 |
| E2 | models/schema.py:774-775, 876-898 | `_source/_target_key_config`, `bind_endpoint_properties`, `source/target_key_config` | P (per-pair binding; single-pair unchanged) | 7.2 |
| E3 | models/schema.py:863 | identifier validator on key properties | P (skip None) | 7.2 |
| E4 | models/schema.py:1121-1150 | GraphSchema endpoint validation and binding | P (loop over pairs) | 7.2 |
| E5 | models/schema.py:383-391 | `SourceTargetConfig.is_self_referencing` | N (used per pair) | - |
| E6 | loader/edge_records.py:64-65 | `normalize_edge_record` key configs | V | 7.3 |
| E7 | loader/edge_records.py:87, 94 | key property names of the record sections | V | 7.3 |
| E8 | loader/edge_loader.py:168-178 | `build_edge_upsert_query` (legacy `EdgeWriter`, tests only) | V + guard | 7.3 |
| E9 | loader/edge_loader.py:181-192 | `_build_endpoint_preflight_query` | V + guard | 7.3 |
| E10 | loader/edge_loader.py:1206 (s4:1271) | `normalize_edge_record(decoded, self._edge_config)` | P (per-topic view) | 7.3 |
| E11 | loader/edge_loader.py:1691 (s4:1773) | `SlotAdmission(endpoint_labels=(src, tgt))` | R | 7.3, 7.5 |
| E12 | loader/edge_execution.py:127-128 | planned/lease upsert, no `order_by` | V + guard | 7.3 |
| E13 | loader/edge_execution.py:137-138 | planned/lease upsert, `order_by` | V | 7.3 |
| E14 | loader/edge_execution.py:162-163 | collect/APOC-locked upsert (classic, lease-bulk) | V | 7.3 |
| E15 | loader/edge_execution.py:195-197 | `_preflight_query` | V | 7.3 |
| E16 | loader/edge_execution.py:218-220 | `_endpoint_classification_query` | V | 7.3 |
| E17 | loader/edge_execution.py:234-238 | `MissingEndpointSink.route_unresolved_endpoint` protocol | P (+`endpoint_labels`) | 7.7 |
| E18 | loader/edge_execution.py:321-337 | `IndexedMissingEndpointSink` passthrough and `index.add` | P | 7.7 |
| E19 | loader/edge_execution.py:432-433, 939 | writer key properties handed to the DLQ sink | V | 7.3 |
| E20 | loader/edge_execution.py:1146-1160 | `_record_from_envelope` (retry decode) | V + labelled-key check | 7.8 |
| E21 | loader/edge_execution.py:1175-1176 | `NativeDisjointEdgeWriter` query | V | 7.3 |
| E22 | loader/edge_execution.py:2006-2007 | `BucketCellRouter(edge.nodes.source, ...)` in `build_edge_execution` | P | 7.4 |
| E23 | loader/dead_letter.py:1052-1121 (s4:1097-1166) | `route_unresolved_endpoint` value (key property names) and key | value V, key P | 7.7 |
| E24 | loader/planned_writer.py:293-296 | `compile_record_normalizer` | V + guard | 7.6 |
| E25 | loader/planned_writer.py:343 | `ChunkDecoder` label check per type | P | 7.6 |
| E26 | loader/planned_writer.py:355 | `labels_of(chunk)` in decode | P | 7.6 |
| E27 | loader/planned_writer.py:662, 677-686 | decoder and writers keyed by type | P (`(type, pair)`) | 7.6 |
| E28 | loader/edge_planner.py:479-480 | `SourceTask` key types | P (`source.topic`) | 7.6 |
| E29 | loader/edge_planner.py:728 | `_ScannedTask` labels | V (+`pair`) | 7.6 |
| E30 | loader/edge_planner.py:742 | `scan` adder labels | V | 7.6 |
| E31 | loader/edge_planner.py:754 | `scan_lane` adder labels | V | 7.6 |
| E32 | loader/edge_planner.py:1602, 1614, 1619 | `KafkaReadTask(edge, topic, ...)` | P (pair view + pair) | 7.6 |
| E33 | loader/edge_planner.py:1742 | `KafkaEdgePlanner` `types={type: (src, tgt)}` | P | 7.6 |
| E34 | loader/edge_planner.py:1840 | `producer_edge_planner` `types` | P | 7.6 |
| E35 | loader/edge_plan.py:404-432, 446, 469, 512, 531, 537 | `EdgeTypePlan`, `labels_of`, validate, to/from JSON | P | 7.6 |
| E36 | orchestrator/planned_dispatcher.py:131, 139 | `labels_of` for READY gating | N (follows `labels_of`) | 7.6 (verify) |
| E37 | orchestrator/planned_edges.py:234 | plan vs schema endpoint labels | P | 7.6 |
| E38 | orchestrator/planned_edges.py:490 | plan labels for the loader check | N | - |
| E39 | loader/retry_consumer.py:453-485 (s4:460-485) | `_pair_from_envelope` (2-element key) | P (2- or 4-element) | 7.8 |
| E40 | loader/retry_consumer.py:2473 (s4:2637) | `BucketCellRouter(edge.nodes.source, ...)` | P | 7.8 |
| E41 | loader/retry_consumer.py:657 (s4:680) | `build_retry_base_writer(driver, edge, ...)` | P (per pair view) | 7.8 |
| E42 | orchestrator/dependency_manager.py:94 | `_endpoint_labels` (conflict plan) | U | 7.5 |
| E43 | orchestrator/dependency_manager.py:159 | conflict-family self-reference check | R | 7.5 |
| E44 | orchestrator/dependency_manager.py:177, 181 | `ConflictFamily` endpoints/order (exactly 2 labels) | R | 7.5 |
| E45 | orchestrator/isolation.py:85-96 | self-reference classification, isolated phase label | S | 7.5 |
| E46 | orchestrator/coordination.py:187 | clock refuses self-references | R | 7.5 |
| E47 | orchestrator/fleet_contract.py:39 | shared fleet refuses self-references | R | 7.5 |
| E48 | cli.py:406 | `_select_keyed_lease_labels` endpoint labels | U | 7.4 |
| E49 | cli.py:611 | bulk stage self-reference vs phase check | S | 7.5 |
| E50 | cli.py:942 (s4:963) | `_co_scheduled_self_references`: `getattr(...nodes...) is True`, **silently False** for a multi-pair parent | S | 7.5 |
| E51 | cli.py:1008 (s4:1029) | shared schedule rejects self-references | S | 7.5 |
| E52 | cli.py:1029 (s4:1050) | default `ready_labels` | U | 7.5 |
| E53 | cli.py:1059-1060 (s4:1080-1081) | `dependencies_ready` per type | U | 7.5 |
| E54 | cli.py:1165 (s4:1186) | isolated stream phase (clock path only) | S / R | 7.5 |
| E55 | cli.py:1324 (s4:1345) | rotating fleet self-reference filter | R | 7.5 |
| E56 | cli.py:1338 (s4:1359) | rotating fleet self-reference check | R | 7.5 |
| E57 | cli.py:2555 (s4:2640) | planned/shared missing endpoint loaders | U | 7.5 |
| E58 | cli.py:705-721 `_precedence_substage_plan` (s4:705-743 incl. `_single_topic_sub_edge`) | precedence sub-edge `model_copy(topics=[t])` keeps `nodes=None` for a multi-pair parent | P (`topic_view`) | 7.5 |
| E59 | loader/node_loader.py:276 | node cell router `(label, label)` | N | - |
| E60 | loader/lease_client.py:143-195 | `BucketCellRouter` itself | N | - |

### Pair-identity sites (not endpoint labels, but they define "same pair")

| File:line (s4:72ca167) | What | S7 |
| --- | --- | --- |
| dead_letter.py:1161-1164 (b2fa3d3:1116) | DLQ key `[sk, tk]` | `[S, T, sk, tk]` for multi-pair types (7.7) |
| parked_pair_index.py:189 `_pair_key` and its callers 461, 495, 527, 613, 642, 662, 703, 754, 782 | in-memory pair key | `(pair_of_topic(original_topic), sk, tk)`; wire unchanged (7.7) |
| parked_pair_index.py:493 `is_blocked(sk, tk)` | live check, no topic | `endpoint_pair=` keyword, required for multi-pair (7.7) |
| edge_execution.py:1339-1350 `_LeaseFlush.is_parked` -> `durable_index.is_blocked(*pair)` | live fallback | per-pair coordinator gets `index.scoped(labels)` (7.4/7.7) |
| edge_execution.py:1221 `_ParkedPairSet`, 840 `_pre_blocked_pairs`, 1292 `_LeaseFlush.parked_pairs` | flush-scoped `(sk, tk)` | per-pair instance via composites (7.3/7.4) |
| retry_consumer.py: 24 `envelope.key_bytes` sites (1056-1166 holds, 1462-1559 blockers, 1630-1692 chains, 1773) | retry pair grouping | unchanged code; correct because the key is canonical-labelled (7.8) |
| retry_consumer.py:546, 592, 1024, 1076, 1198, 1438, 1567, 1668, 1889 `_pair_from_envelope` callers | sk/tk for index calls | unchanged (returns `(sk, tk)` from either form) |
| edge_loader.py:1840, retry_consumer.py:2626 `ParkedPairIndex(..., sequenced=is_multi_topic)` | index construction | `+ pair_of_topic=` for multi-pair types (7.7) |

---

## 4. Stories

Story format is workflow 02's. Each story lists the files touched, binary acceptance criteria (AC) and a lean
test plan. Unless a story says otherwise, its tests are unit tests in the default suite
(`-m "not integration and not smoke"`).

### Story 7.1: Compatibility net first (single-pair golden on the pre-v7 tree)

*As a maintainer, I want every single-pair byte-level contract pinned on the pre-v7 tree before S7 changes code,
so that "single-pair types do not change" is proven by a before/after golden, not asserted.*

Technical:
* New `tests/test_single_pair_compat.py` with a `fingerprint()` and `--dump`, built like S6's
  `tests/test_single_topic_compat.py`. It covers the same four schemas: `config/`, `examples/imdb`,
  `examples/social`, and `tests/golden/fixtures/single_topic_dlq.yaml`. For every edge it records:
  * SHA-256 of `build_apoc_locked_upsert_query(edge, explicit_locks, sort_rows)` for (True, True),
    (False, True) and (False, False); of `_preflight_query`, `_endpoint_classification_query`,
    `edge_loader.build_edge_upsert_query` and `_build_endpoint_preflight_query`; and of the
    `NativeDisjointEdgeWriter` query (built with a stub capabilities object).
  * `normalize_edge_record` and `compile_record_normalizer` outputs for 5 sample records. Reuse S6's `_sample`,
    which covers string, integer, float, date and datetime keys.
  * `DeadLetterRecordSink.route_unresolved_endpoint` envelope `key_bytes` and `value_bytes` for one row, with a
    fixed clock and a capturing fake producer.
  * `parked_pair_index._record_wire_key`, and the `add()` value bytes for an unsequenced add.
  * `BucketCellRouter(S, T, 64, keyed).cell(...)` and `.resources(...)` for 5 keys, with keyed `{}` and
    `{target label}`.
* Record `tests/golden/single_pair_compat.json` from a `git archive 4382960` (pre-v7) tree, the S6 procedure.
  Every fingerprinted function exists there (checked: `build_apoc_locked_upsert_query`,
  `_endpoint_classification_query`, `route_unresolved_endpoint`, `_record_wire_key`, `BucketCellRouter`).
* `tests/golden/single_topic_compat.json` stays byte-identical. It is not re-recorded.
* If any entry differs between 4382960 and the S7 base (S4 merged), stop and explain it in the completion report
  before writing S7 code. That would be a pre-existing v7 change.

Files: `tests/test_single_pair_compat.py` (new), `tests/golden/single_pair_compat.json` (new).

AC:
1. The golden was recorded on the 4382960 archive (procedure in the module docstring).
2. The dump from the S7 base, before any S7 change, is byte-identical to it.
3. The dump at the end of S7 is byte-identical to it, and S6's golden test still passes untouched.
4. A non-vacuity test asserts the golden contains at least one `order_by` edge, one DLQ-enabled edge, one
   self-reference (social `FRIEND_OF`) and one keyed-label case.

Tests: the new module only (runs in under 5 s).

### Story 7.2: Schema shape: per-topic label-pair binding, compatibility, validation

*As a schema author, I want each topic of a relationship type to bind its own (source label, target label), so
that `LIKES` can connect Person->Post and Person->Comment without the pair ever being inferred from a record.*

YAML (both forms are valid):
```yaml
- type: LIKES
  topics:
    - name: likes-posts
      nodes: {source: Person, target: Post}
      source_key_property: person_id
      target_key_property: post_id
    - name: likes-posts-backfill          # same pair, so the S1 conflict rule applies to this pair
      nodes: {source: Person, target: Post}
      source_key_property: person_id
      target_key_property: post_id
    - name: likes-comments
      nodes: {source: Person, target: Comment}
      source_key_property: person_id
      target_key_property: comment_id
  order_by: liked_at
  properties:
    liked_at: {type: datetime, required: true}

- type: LIKES                              # top-level pair as the default for string entries
  nodes: {source: Person, target: Post}
  source_key_property: person_id
  target_key_property: post_id
  order_by: liked_at
  topics:
    - likes-posts
    - {name: likes-comments, nodes: {source: Person, target: Comment},
       source_key_property: person_id, target_key_property: comment_id}
  properties:
    liked_at: {type: datetime, required: true}
```

Data model (`src/models/schema.py`):
```python
class TopicEndpoints(BaseModel):            # extra="forbid", frozen; identifiers validated
    nodes: SourceTargetConfig
    source_key_property: str
    target_key_property: str

@dataclass(frozen=True)
class EndpointPair:
    index: int                              # order of first appearance in `topics`
    source_label: str; target_label: str
    source_key_property: str; target_key_property: str
    topics: tuple[str, ...]                 # this pair's topics, in `topics` order
    labels -> tuple[str, str]; is_self_referencing -> bool

class MultiPairEdgeError(ValueError): ...

class EdgeConfig:
    nodes: Optional[SourceTargetConfig] = None          # None <=> multi-pair
    source_key_property: Optional[str] = None
    target_key_property: Optional[str] = None
    topic_endpoints: Optional[dict[str, TopicEndpoints]] = None   # multi-pair only, keys == topics
    endpoint_pairs -> tuple[EndpointPair, ...]; is_multi_pair -> bool
    pair_for_topic(topic) -> EndpointPair                # ValueError for a topic the type does not read
    endpoint_labels -> frozenset[str]; has_self_reference -> bool
    single_pair -> EndpointPair                          # raises MultiPairEdgeError (names the type)
    pair_index -> int                                    # 0, or the view's pair
    pair_view(pair: EndpointPair | int, *, topics: Sequence[str] | None = None) -> EdgeConfig
    topic_view(topic) -> EdgeConfig                      # pair_view(pair_for_topic(t), topics=[t])
GraphSchema.pair_for_topic(topic) -> tuple[EdgeConfig, EndpointPair] | None
```

Normalisation extends `_normalize_topic_shorthand` (schema.py:777):
* R1. S1's rules are kept: exactly one of `topic`/`topics`; names stripped, unique, non-blank. `topic:` accepts
  only a string.
* R2. A mapping entry needs `name`, `nodes`, `source_key_property` and `target_key_property`. Other keys are
  rejected.
* R3. A string entry inherits the top-level `nodes` and keys, so they are required if any string entry exists.
* R4. Top-level `nodes` that no entry uses (every entry is a mapping) is an error: "top-level nodes is unused".
* R5. If every topic resolves to one `(S, T, skp, tkp)`, the type is single-pair. The top-level fields are set
  and `topic_endpoints` is dropped, so `model_dump(exclude_unset=True)` equals today's shape.
* R6. Otherwise the type is multi-pair: the top-level fields are `None`, `topic_endpoints` holds every topic, and
  there are at least 2 distinct `(S, T)`.
* R7. The same `(S, T)` with different key properties is an error naming both topics.
* R8. The canonical dump form (`topics` as strings plus `topic_endpoints`) is accepted as input, so it round-trips.

`GraphSchema.validate_edge_endpoints` (schema.py:1121-1150):
* Runs today's three checks per pair: the label exists, the key property equals the node's `key_property`, and
  that key property is required.
* Single-pair error messages stay byte-identical.
* Multi-pair messages add `topic '<t>'`.
* Single-pair types are bound with `bind_endpoint_properties` exactly as today. Multi-pair types fill a private
  `_pair_key_configs[(S, T)]`.

`pair_view`:
* Built with `model_copy(update=...)`: `nodes` and keys from the pair; `topics` = the given subset or the pair's
  topics; `topic_endpoints=None`; `topic_conflict` = the parent's if the view has 2 or more topics, else `None`.
* `dead_letter` is pinned to the parent's resolved DLQ (and parking lot). It raises if the view's three names
  differ from the parent's (S4 review S6 check).
* It binds the pair's key configs, sets `_pair_index`, and is cached on the parent.
* For a single-pair type, `topic_view(t)` must equal today's `cli._single_topic_sub_edge(edge, t)`.

Conflict rule (owner question Q1, section 7):
* Recommended: a rule is required only when some label pair is fed by 2 or more topics.
* `conflict_rule` returns `"single"` when `order_by` is unset and every pair has one topic.
* A declared `precedence`/`arrival` on such a type is accepted with a WARNING that it has no effect.
* `arrival_conflict_warnings` names the pair groups.
* If the owner keeps the literal decision 1, `_conflict_rule_is_declared` (schema.py:928) stays as it is.

Unchanged: topic ownership (each topic belongs to one type), DLQ/parked/index naming (`dead_letter_entity`),
`edge_for_topic`, `precedence_rank`, `require_no_precedence_in_stream`.

Files: `src/models/schema.py`, `tests/test_schema_multi_pair.py` (new), `tests/test_schema_multi_topic.py` (only the
cases Q1 touches).

AC:
1. Both YAML forms load. `endpoint_pairs` equals `((Person, Post, person_id, post_id, (likes-posts,
   likes-posts-backfill)), (Person, Comment, person_id, comment_id, (likes-comments,)))`, in that order.
2. Every shipped and fixture schema (`config/`, `examples/*`, `var/*.yaml`, `tests/golden/fixtures/*`) loads with
   `nodes`, key properties, `source_key_config`/`target_key_config` and `model_dump(exclude_unset=True)` equal to
   before, and `is_multi_pair is False`. S1's round-trip and pickle tests pass unchanged.
3. Mapping entries that all bind one pair normalise to the single-pair form (the dump has no `topic_endpoints`).
4. Each of these fails at load, with a message naming the type and topic:
   * an unknown label;
   * a key property that differs from the node's key, or is not required;
   * a mapping entry without `nodes`, or with an unknown key;
   * a string entry with no top-level pair;
   * an unused top-level pair;
   * the same pair with different key properties;
   * `topic:` given a mapping;
   * `topic_endpoints` keys that differ from `topics`.
5. A reversed pair (A->B and B->A) and a self-reference next to a normal pair both load. `has_self_reference` is
   True for the latter.
6. On a multi-pair type: `nodes is None`, `single_pair` raises `MultiPairEdgeError`, and `topic` still raises
   `MultiTopicEdgeError`.
7. `pair_view` and `topic_view` return single-pair configs:
   * key configs are bound;
   * DLQ, parked and index names equal the parent's;
   * `pair_index` is correct;
   * for single-pair types, `topic_view(t)` equals `_single_topic_sub_edge(edge, t)` field by field.
8. Pickle, `model_copy`, and dump-then-validate preserve `endpoint_pairs` and the key-config bindings.
9. (Q1 as recommended) A multi-pair type whose pairs each have one topic needs no rule. A pair with 2 topics and
   no rule fails with S1's message plus the pair. Precedence in stream is still refused.

Tests: table-driven form x error cases; round trip; pickle; view equality; golden 7.1.

### Story 7.3: Endpoint resolution and normalisation in the live write path

*As an operator, I want each record normalised and written with the label pair bound to the topic it was read
from, so that a Comment id never resolves a Post, even when the key values collide.*

Technical:
* `EdgeLoader`: add `self._config_for_topic = {t: edge.pair_view(edge.pair_for_topic(t)) for t in topics}` for
  multi-pair types. Single-pair types get `{}` and keep using `self._edge_config`, so their path is
  byte-identical.
* The normalisation at edge_loader.py:1206 (s4:1271) uses `self._config_for_topic.get(topic, self._edge_config)`.
  A topic that is not subscribed raises (it can't happen: S2 validates topics).
* `build_edge_execution` (edge_execution.py:1884), when `edge.is_multi_pair`:
  * Build each pair by calling the same function on `edge.pair_view(pair)`. Story 7.7 adds the sink adapter
    (`LabelledEndpointSink`) and `index.scoped(labels)`.
  * Lease branch: compose the per-pair coordinators in `PairSplitLeaseCoordinator` (7.4).
  * Classic branch: compose them in a new `PairSplitLaneExecution` (`routes_records = True`):
    * `execute_pending(pending)` groups items by `pair_index_of_topic[item.topic]`, keeping order;
    * each pair is routed through its own `MixAndBatchPartitioner` + `LaneBatcher` +
      `LaneExecutionCoordinator.execute(...)`;
    * pairs run **sequentially**, since classic is the correctness-only mode that may wait on locks;
    * results are concatenated.
  * `native_disjoint` works through the same composite, with one `NativeDisjointEdgeWriter` per view.
  * The returned `writer` is a `_MultiPairWriterGuard` whose `write`/`write_batch` raise `ExecutionModeError`. The
    loader always has a coordinator.
  * Single-pair types return exactly the objects returned today.
* These functions start with `edge_config.single_pair`, so a parent raises `MultiPairEdgeError` naming the type:
  * `build_apoc_locked_upsert_query`, `_preflight_query`, `_endpoint_classification_query`;
  * `ApocLockedEdgeWriter.__init__`, `NativeDisjointEdgeWriter.__init__`;
  * `edge_loader.build_edge_upsert_query`, `_build_endpoint_preflight_query`;
  * `planned_writer.compile_record_normalizer`.
* Unchanged, but now per pair: missing/ambiguous classification per row, the `order_by` collapse inside a pair's
  batch, and row-level DLQ.
* `edge_loader.main` raises before subscribing if `args.slot_gating` and the type is multi-pair (E11).

Files: `src/loader/edge_loader.py`, `src/loader/edge_execution.py`, `src/loader/planned_writer.py` (guard only),
`tests/test_multi_pair_write_path.py` (new).

AC:
1. Normalisation follows the topic:
   * `likes-comments` `{target: {comment_id: 7}}` normalises with Comment's key type;
   * `likes-posts` `{target: {post_id: 7}}` normalises with Post's;
   * a `likes-comments` record carrying `post_id` is malformed (rejection or DLQ, reason names the field).
2. A batch mixing both pairs (same partition numbers, overlapping offsets, Post 7 and Comment 7) writes exactly
   Person->Post 7 and Person->Comment 7. A capturing fake driver shows one label pair per transaction.
3. Classic composite: no transaction mixes pairs. With the bulk DLQ opt-in, each pair's unresolved rows carry
   that pair's key property names in `value_bytes`.
4. For single-pair types, `build_edge_execution` returns the same classes as before and the query texts equal
   golden 7.1.
5. Every guarded builder raises `MultiPairEdgeError` for a parent.
6. If one pair fails, the flush fails, no offset of either pair commits, and the replay rewrites both (the
   existing at-least-once contract).

Tests: fake driver and session recording `(query, rows)`; normalisation table; composite routing; single-pair
identity.

### Story 7.4: Lease resources, buckets and keyed labels

*As an operator, I want each label pair's endpoints leased as their own `Label:bucket` resources through the
unchanged lease server, so that multi-pair types keep zero Neo4j lock waits.*

Technical:
* Router per pair (E22): `BucketCellRouter(pair.source_label, pair.target_label, B, keyed)`.
  * Person 1->Post 5 gives the cell `(Person:b1, Post:b5)`; Person 1->Comment 5 gives `(Person:b1, Comment:b5)`.
  * Bucket count `B` is per label and global (unchanged); `Label#digest` keyed tokens are unchanged.
  * Comment and Post are separate label bucket spaces, so equal key values never share a resource.
* New `PairSplitLeaseCoordinator(coordinators: dict[int, BucketLeaseExecutionCoordinator], pair_of:
  Callable[[object], int], *, edge_type, metrics, kind)` in `edge_execution.py`:
  * `routes_records = True`; `prime()` and `close()` delegate.
  * `execute_pending(pending, *, budget_seconds=None)` groups by `pair_of(item)`, keeping order. A single group
    is a direct call. Otherwise each group's `execute_pending(group, budget_seconds=...)` runs concurrently on a
    pool of `len(groups)` threads, and the results are concatenated in pair order.
  * `describe_last_flush()` returns `pair=Person>Post <sub> | pair=Person>Comment <sub>`.
  * `pair_of` is a callable so the retry path can pass `_RetryItem` (story 7.8).
* `BucketLeaseExecutionCoordinator`: two additive changes.
  * New `observe_lease_wait: bool = True` (sub-coordinators pass False).
  * New `last_lease_wait_seconds` attribute. The composite observes `lease_wait{kind,name}` once per outer flush
    with the sum (keeps P6-S2 Story 5's "one observation per outer flush").
  * Nothing else in the class changes. Because each sub-coordinator only sees its own pair's cells, EXT merges
    (`_merge_plan`, 1678) and `_LeaseFlush.parked_pairs` stay within one pair.
* `worker_count` (decision, documented in 7.10): each pair coordinator gets `edge.execution.worker_count`. A
  replica therefore opens up to `pairs x worker_count` write sessions and lease connections.
* Keyed labels (E48, `_select_keyed_lease_labels`, cli.py:395-417): use `edge.endpoint_labels`. Node topic
  counts cover every pair label. The HELLO scheme mechanism is unchanged.
* **Lease server: no change.** `src/orchestrator/lease_server.py` has no type, topic or pair awareness (grep:
  only resource tokens, `--buckets`, `--scheme`). Fencing (P5-Q1: per-grant deadline, quiet period, rollback on
  loss) is per transaction and so per pair unchanged.

Files: `src/loader/edge_execution.py`, `src/cli.py` (E48 only), `tests/test_multi_pair_leases.py` (new).

AC:
1. Router cells for Person 1->Post 5 and Person 1->Comment 5 differ and share only `Person:b1`. This holds with
   `B=1` too.
2. Against an in-process `BucketLeaseServer`, the grant log shows:
   * no two concurrent holders of the same `Person:b`;
   * overlapping grants of `Post:x` and `Comment:x`;
   * no transaction with rows of 2 pairs;
   * no EXT merge of a Post cell with a Comment cell.
3. A 2-pair flush records exactly one `lease_wait` observation.
4. A small Comment node topic makes `Comment` keyed (union over pairs).
5. A stop during a 2-pair flush abandons both groups with `LeaseDrainAbandoned`, and no offset commits.
6. The slice diff contains no change to `lease_server.py` or `lease_client.py`. `tests/test_lease_server.py` is
   untouched.

Tests: in-process lease server helpers from the existing lease tests; fake writers recording rows per transaction.

### Story 7.5: Orchestration: bulk monitor, stream, scheduling, isolation

*As an operator, I want bulk and stream orchestration to treat a multi-pair type as touching all its endpoint
labels, so that it starts only once every one of them is loaded and never runs beside a conflicting type in
classic mode.*

Technical (site IDs from section 3):
* Union of labels: E42 (`dependency_manager._endpoint_labels`), E52, E53, E57.
  * A 3-label type conflicts with any type touching Person, Post or Comment.
* Any self-reference: E45 (isolation; the isolated phase label is the first self-referencing pair's label), E49,
  E50 (fixes the silent `getattr(...) is True`), E51.
  * In classic bulk, a type with a self-referencing pair runs as an isolated phase.
  * In lease and planned mode it runs in the shared phase (unchanged rule).
* Deprecated Phase 4 paths:
  * `_require_supported_topic_conflicts` (cli.py:1910 / s4:1932) refuses `--stream-clock-rotation` when any type
    is multi-pair, before any side effect: "edge type LIKES binds several label pairs; --stream-clock-rotation
    (deprecated) supports one pair per type; use the default lease fleet".
  * As a second line of defence, E43, E44, E46, E47, E54-E56 call `single_pair`/`has_self_reference` and raise.
* Precedence sub-stages (E58): the sub-edge is `edge.topic_view(topic)`. S4's DLQ-name pinning check moves into
  `pair_view`, and `_single_topic_sub_edge` becomes a thin wrapper.
* Bulk monitor (`relationship_bulk_monitor.py`), static assignment, DRAIN proof, stream supervision, lag series
  and `docker_service.run_edge_loader`: **no change**. They key on (type, topic, partition), and a replica
  receiving records of several pairs is handled by the 7.3/7.4 composites.
* Test doubles: existing `SimpleNamespace` edges gain `endpoint_labels`, `has_self_reference` and `is_multi_pair`
  (S2 precedent).

Files: `src/cli.py`, `src/orchestrator/dependency_manager.py`, `src/orchestrator/isolation.py`,
`src/orchestrator/coordination.py`, `src/orchestrator/fleet_contract.py`, `tests/test_multi_pair_orchestration.py`
(new), doubles in existing CLI/scheduler tests.

AC:
1. `build_relationship_conflict_plan`: LIKES {Person, Post, Comment} conflicts with WROTE {Person, Post} and with
   TAGGED {Comment, Tag}. Stages are deterministic.
2. `_run_completion_driven_bulk_edges` does not launch LIKES while Comment is not ready, and launches it once it
   is.
3. A multi-pair type with a self-referencing pair runs as an isolated phase in classic, and in the shared phase
   in lease and planned mode.
4. `start --mode stream --stream-clock-rotation` with a multi-pair type exits 1 before creating any topic or
   container. The message names the type.
5. Precedence sub-stages of a multi-pair type launch one loader per topic. Each sub-edge is single-pair with the
   parent's DLQ names. S5's tests are green.
6. The planned missing-loader check names a missing Comment loader.
7. The slice diff contains no change to `relationship_bulk_monitor.py` or `docker_service.py`.

Tests: unit and existing scheduler/precedence suites with updated doubles.

### Story 7.6: Planned mode: hub stats per label, units, cells, plan-file format

*As an operator, I want planned bulk loads of multi-pair types to plan each label pair with hub degrees counted
per label, so that planned mode stays zero-wait by construction and the plan file states which pair every chunk
writes.*

Technical:
* Read tasks (E32, `kafka_read_tasks`, edge_planner.py:1582-1621):
  * `topic_index = edge.precedence_rank(topic)` is still computed on the **parent**, so precedence and task order
    are unchanged.
  * The task carries `edge = edge.pair_view(edge.pair_for_topic(topic))` (one cached view per pair) and a new
    `pair: int`. `SourceTask` gets `pair` too.
  * The `_adder(task.edge.nodes...)` call sites E29-E31 then work unchanged. `_ScannedTask` gains `pair`.
* Hub stats: `KeyWindows` are per label (unchanged).
  * A Person's degree sums over both pairs and every other type (the documented "total degree over all types").
  * Post 7 and Comment 7 live in different windows.
  * The `ResourceTable` labels are the union over pairs (edge_planner.py:1462 via `types`).
* Units and cells: a unit is `(type, source resource, target resource)` (unchanged) and is distinct per pair,
  because resources are per label. Cells are per task, so per topic and per pair. The chain is per unit.
  `ChunkSpec` gains `pair: int = 0`, set from `scanned.pair`. The chunk key text is unchanged.
* Plan file (`PLAN_FORMAT` stays `graph-loader-edge-plan/1`; single-pair JSON is byte-identical):
  * A multi-pair `types[T]` entry is:
    ```json
    {"pairs": [{"source": "Person", "target": "Post", "topics": ["likes-posts", "likes-posts-backfill"],
                "rows": 812, "chunks": 9, "units": 7},
               {"source": "Person", "target": "Comment", "topics": ["likes-comments"],
                "rows": 400, "chunks": 4, "units": 4}],
     "rows": 1212, "chunks": 13, "units": 11}
    ```
    It has no top-level `source`/`target`, so a pre-S7 reader fails closed with "malformed plan".
  * Chunk entries of multi-pair types have 11 fields (`..., bytes, pair`). All others keep 10. `from_json`
    accepts 11 fields only for multi-pair types, with `pair` in range.
  * `EdgeTypePlan.pairs: tuple[PlanPair, ...]`. `source_label`/`target_label` become properties that are valid
    only with one pair (`EdgePlanError` otherwise). `PlannedChunk.pair: int = 0`.
  * `labels_of(chunk)` returns the chunk's pair labels.
  * `validate()` checks: `pair` in range; chunk labels equal `{S, T}` of its pair; a predecessor has the same pair
    and resources; the per-pair rows and chunks add up to the type's.
  * `assemble_plan(types=...)` accepts the old `(S, T, rows)` tuple or `(pairs, rows_by_pair)`.
* Plan vs schema (E37, `validate_plan_against_schema`, planned_edges.py:234):
  * multi-pair types compare `[(S, T, topics)]` with `edge.endpoint_pairs`;
  * single-pair types compare labels as today;
  * the `stats.type_topics` check is unchanged;
  * message: "plan binds likes-comments to Person->Comment; the schema binds it to ...; re-plan the load".
* Planned writer (E25-E27):
  * `ChunkDecoder` normalisers are keyed `(type, pair)` and built from pair views; decode uses `labels_of(chunk)`;
  * `writers[(type, pair)]` hold pair views, and `PlannedEdgeWriterWorker` looks up
    `self._writers[(chunk.edge_type, chunk.pair)]`;
  * metrics keep `name=type`.
* Dispatcher (E36): no change. READY gating is per label of each chunk.
* Producer planning (E28, E34, `plan_and_publish_edges`): `EdgeRecordSource` gains an optional `topic: str`.
  * A source for a multi-pair type must name one of the type's topics: `EdgePlanError` "source for LIKES must
    name its topic (each topic binds a label pair)".
  * The mapping form `{type: rows}` is refused for multi-pair types.
  * Single-pair producers are unchanged.

Files: `src/loader/edge_planner.py`, `src/loader/edge_plan.py`, `src/loader/planned_writer.py`,
`src/orchestrator/planned_edges.py`, `tests/test_planned_multi_pair.py` (new).

AC:
1. A two-pair plan (FakeCluster/FakeProducer) has chunks of both pairs, every chunk's resources match its pair's
   labels, and the Post 7 and Comment 7 rows land in different units.
2. With H=10: Person 0 (6 Post likes + 6 Comment likes) is a hub; Post 0 (6) and Comment 0 (6) are not.
3. Reversed pairs (A->B and B->A, same resource set) give two chains, and each is written in its own direction.
4. Plan JSON round-trips. Single-pair plan JSON minus `stats`, and the chunk messages, are byte-identical (S6
   golden).
5. `validate_plan_against_schema` refuses a plan in each of these cases:
   * a topic rebound to another pair;
   * a removed pair;
   * an 11-field chunk under a single-pair type.
6. A planned load of the two-pair fixture equals the classic result: with the in-memory writer here, and on real
   Neo4j in 7.11.
7. A producer source without `topic` for a multi-pair type fails with the message above.

Tests: unit; a small two-pair plan JSON golden recorded on the S7 tree and labelled as new.

### Story 7.7: DLQ and parked-pair index with the label pair in pair identity

*As an operator, I want DLQ and parked-pair identity to include the label pair, so that Person 1->Post 1 and
Person 1->Comment 1 never block or reorder each other while each pair keeps its order across its topics.*

Technical:
* DLQ key (E23, `route_unresolved_endpoint`, dead_letter.py:1052 / s4:1097):
  * new keyword `endpoint_labels: tuple[str, str] | None = None`;
  * the key is `[S, T, sk, tk]` when it is given, else `[sk, tk]` (byte-identical);
  * `value_bytes` is unchanged; its key property names are already the pair's, from the view;
  * still one DLQ per type (decision 2): names, partitions = max over topics, and S4's migration preflight and
    `--accept-former-dlq` are unchanged.
* `LabelledEndpointSink(sink, labels)` (next to `IndexedMissingEndpointSink`) adds `endpoint_labels`. The
  `MissingEndpointSink` protocol (E17) gets the optional keyword. The wrapping order per pair is
  `IndexedMissingEndpointSink(LabelledEndpointSink(dlq_sink, labels), index.scoped(labels))`. Single-pair types
  are not wrapped.
* `ParkedPairIndex`:
  * new constructor argument `pair_of_topic: Callable[[str], Hashable] | None = None`;
  * the in-memory key is `_pair_key(pair_id, sk, tk)` with `pair_id = pair_of_topic(original_topic)`;
    `None` means a constant pair id, which is today's behaviour for single-pair types, including removed topics
    and S4's tie rule;
  * `_apply` takes the pair from the wire key's `original_topic`, and so do `add`, `remove`, `remove_many`,
    `contains`, `is_blocked_by_earlier`, `order_key` and `earlier_outstanding`;
  * **wire key and value bytes are unchanged, and there is no versioning**;
  * `is_blocked(sk, tk, *, endpoint_pair=None)`: `endpoint_pair` is required (`ParkedPairIndexError`) when
    `pair_of_topic` is set;
  * an unknown topic in a multi-pair index maps to the pseudo pair `("?", topic)`: it blocks no live pair, and the
    retry consumer parks the record (7.8).
* `ParkedPairIndex.scoped(labels)` returns an object that satisfies `DurableParkedPairIndex`: `is_blocked(sk, tk)`
  forwards `endpoint_pair=labels`, and `add(...)` passes through, asserting
  `pair_of_topic(original_topic) == labels`. The per-pair lease coordinator's `_LeaseFlush.durable_index` uses it.
* Construction: edge_loader.py:1756 / s4:1840 and retry_consumer.py:2465 / s4:2626 pass
  `pair_of_topic=edge.pair_labels_for_topic` (unknown -> pseudo pair) for multi-pair types.
  `sequenced=edge.is_multi_topic` is unchanged.
* Check of S4's sequenced-index assumptions (all still hold):

| S4 assumption | Holds with S7 because |
| --- | --- |
| A pair's adds are serialised by its caller (one lane or lease cell per pair, rows routed in order) | A labelled pair maps to one cell of its own pair coordinator (deterministic per-pair router). Colliding keys of different pairs are different pair keys. |
| Admission barrier: a new owner admits nothing until its view has caught up with the index as of the assignment | Unchanged. It is per index, not per pair. |
| `seq = 1 + max(seq)` over the pair's outstanding records | Per labelled pair. Two pairs with colliding keys each start at 1. |
| Order key `(seq, topic, partition, offset)` is a strict total order per pair | Unchanged within a pair. Pairs are never compared. |
| One type-wide index blocks a pair's records from any topic of the type | It still does, for topics bound to the **same** label pair (A semantics). Topics of different pairs never block each other. |
| The DLQ key co-locates a pair's records on one DLQ partition | `[S, T, sk, tk]` co-locates all records of a labelled pair. Correctness still rests on the index order. |
| The tie rule when a type goes back to one topic | Single-pair types map every topic, removed ones included, to the constant pair, as today. |
| Retry grouping by `envelope.key_bytes` | The key is labelled. Legacy 2-element keys are canonicalised at read (7.8). |
| The cross-replica race residual | Unchanged and documented. |

Files: `src/loader/dead_letter.py`, `src/loader/parked_pair_index.py`, `src/loader/edge_execution.py`,
`src/loader/edge_loader.py`, `tests/test_multi_pair_dlq.py` (new), `tests/test_parked_pair_index.py` (+cases).

AC:
1. Single-pair: DLQ envelope bytes and index wire bytes equal golden 7.1.
2. Multi-pair: the key is `["Person","Comment",1,1]`. The Post (1,1) and Comment (1,1) envelopes have different
   keys and, with the same partition count, may hash to different DLQ partitions without affecting the result.
3. An outstanding Person1->Post1 from `likes-posts`:
   * blocks a later Person1->Post1 from `likes-posts-backfill`, with a higher seq;
   * does not block Person1->Comment1 on the live path (`is_blocked`) or the retry path
     (`is_blocked_by_earlier`);
   * a rebuild from the compacted topic yields the same view.
4. Index entries written before S7 (same wire bytes) are assigned to their topic's pair on rebuild.
5. `is_blocked` without `endpoint_pair` raises on a multi-pair index.
6. A parked Post (1,1) in one outer lease flush does not block Comment (1,1) in the same flush.

Tests: in-memory Kafka fakes from `test_parked_pair_index.py` / `test_multi_topic_dlq.py`.

### Story 7.8: Retry consumer and replay

*As an operator, I want the retry consumer to retry each DLQ'd row through the label pair bound to its
`original_topic`, and replay-dlq to refuse replays that cross pairs, so that retried and replayed rows can never
land on the wrong label pair.*

Technical:
* `_pair_from_envelope` (E39) accepts `[sk, tk]` or `[S, T, sk, tk]` (string labels) and returns `(sk, tk)`. New
  `_pair_labels_from_envelope` returns the labels or `None`.
* Canonical key at read (`_on_message`, retry_consumer.py:938 / s4:961, after `_unretryable_reason`):
  * For a multi-pair type whose `original_topic` is one of its topics, a 2-element key becomes the topic pair's
    `[S, T, sk, tk]` (`dataclasses.replace`).
  * 4-element keys and single-pair types are untouched.
  * All 24 `key_bytes` grouping sites then see one identity per labelled pair. A re-DLQ republishes the
    canonical copy.
* Writers per pair (E40, E41, `main()`):
  * Per pair: `build_retry_base_writer(driver, edge.pair_view(p), ...)` and a router per pair.
  * Lease mode: `build_retry_lease_coordinator` per pair, then `build_retry_writer`.
  * Standalone mode: `ApocRetryWriterAdapter` per pair.
  * All composed in a new `PairSplitRetryWriter(writers_by_pair, pair_of_envelope)`:
    * it splits envelopes by pair (stable) and calls the sub-writers concurrently with the same `budget_seconds`,
      remapping indices;
    * an envelope whose `original_topic` is not a topic of the type, or whose key labels differ from that topic's
      pair, gets `("unresolved", "invalid_on_retry")` without touching Neo4j. That is the existing
      bounded-attempts-then-park path, and the condition is the row's own (D1);
    * if no outcome exists and a sub-writer raised, it re-raises the first error in pair order. If some outcomes
      exist, it returns `_PartialOutcomes(outcomes, first error)`;
    * `close()` delegates.
* `_record_from_envelope` (E20) on a pair writer also raises when a labelled key's labels differ from its pair,
  which becomes `invalid_on_retry`.
* Unchanged: hold, follower, tombstone and order logic. Every index call passes `original_topic`, so it is
  pair-scoped through 7.7.
* D1 is applied as written: write evidence is "same edge type", so a WRITTEN outcome of any pair of the type
  counts. Documented in 7.10.
* D2 (supervision) is unchanged.
* replay-dlq (`handle_replay_dlq`, cli.py:2740 / s4:2825; `dead_letter_replay.py` unchanged):
  * The default (republish to each envelope's `original_topic`) is pair-consistent: unchanged.
  * `--target-topic T` on a multi-pair type requires `--source-topic` values that all bind one pair. If T is a
    topic of the type, it must bind that same pair. Otherwise exit 1 before reading: "replaying likes-comments
    records into likes-posts would write them as Person->Post".
  * Single-pair types are unchanged.
  * Replayed messages carry the envelope key (labelled for multi-pair types). Loaders never parse relationship
    keys.

Files: `src/loader/retry_consumer.py`, `src/loader/edge_execution.py` (E20), `src/cli.py` (replay only),
`tests/test_multi_pair_retry.py` (new), `tests/test_dead_letter_replay.py` (+3), `tests/test_retry_pair_order_fuzz.py`
(parametrised).

AC:
1. Over a real `ParkedPairIndex` (in-memory Kafka): Person1->Post1 parked from `likes-posts` and Person1->Comment1
   parked from `likes-comments` retry independently. Publishing the Post nodes releases only Post rows. The final
   graph is exact.
2. Within the Post pair across its two topics, retries follow the sequenced order (S4's b-then-a case),
   interleaved with Comment rows that have colliding keys.
3. A legacy 2-element envelope of a multi-pair type forms one chain with the 4-element envelopes of the same pair.
4. An unknown-topic or label-mismatched envelope becomes `invalid_on_retry`, is parked after
   `max_retry_attempts`, has its index entry tombstoned, and is never written.
5. Lease mode: no retry transaction mixes pairs.
6. replay-dlq refusals behave as specified with 0 publishes. The default replay works for both pairs. Dry-run
   counts per `--source-topic` are correct.
7. The single-pair retry suite, including the 80-seed sequenced fuzz and its control, is unchanged and green. The
   fuzz parametrised with 2 pairs and colliding keys has 0 bad orders, and its control is still sensitive.

Tests: as listed.

### Story 7.9: Metrics labels stay `{kind,name}` and bounded

*As an SRE, I want multi-pair types to export exactly the series a multi-topic type exports, so that dashboards and
alerts need no change and cardinality stays bounded.*

Technical:
* No new label.
  * All families keep `{kind, name=<type>}`.
  * Lag keeps `{kind, name, topic}`, at most one series per schema topic.
  * DLQ backlog keeps `{kind, name, queue}`.
  * The parked-pair index wait keeps `{kind, name}`.
  * Planner and planned-writer metrics keep `name=<type>`.
  * `lease_wait` gets one observation per outer flush (7.4).
* The pair appears only in logs (`pair=Person>Comment` in `stage=edge-lease-flush`, write-failure lines and
  planner lines) and in plan `types[T].pairs`.

Files: `tests/test_multi_pair_metrics.py` (new), `docs/MONITORING.md` (one paragraph). No `src/` change beyond 7.4.

AC:
1. A 2-pair, 3-topic type produces the same set of series (names and label sets) as a single-pair, 3-topic type.
2. No label value in the registry contains a node label of a pair (checks for "Comment" and "Post").
3. `tests/test_metrics.py` label-set assertions are untouched, and `make monitoring-validate` passes.

### Story 7.10: Docs, the example, and docs YAML

*As a new user, I want the schema reference, runbook and a runnable example to show one relationship type over
several label pairs, so that I can model `LIKES` Person->Post/Comment correctly the first time.*

Technical:
* `docs/USAGE.md`:
  * §3 `edges[]` fields: both `topics` entry forms, the R1-R8 rules and the conflict rule (per Q1).
  * §3 Validation: the new errors.
  * §3 DLQs across topics: the labelled DLQ key; adding a pair means adding a topic (S4's stop-the-fleet,
    `dead_letter.topic` and drained-DLQ rules apply); rebinding a topic to another pair needs a drained DLQ.
  * §4 Input contract: each topic's records use that topic's key property names; the pair is never inferred.
  * §5 Modes table: stream lease, bulk lease, planned and classic support multi-pair types;
    `--stream-clock-rotation` refuses them; `execution.worker_count` is per pair.
  * §6: the plan format (`pairs`, `pair` per chunk); hubs per label.
  * §7: `EdgeRecordSource.topic`.
  * §8: no per-pair series.
  * §10: the replay `--target-topic` rule.
  * §11: troubleshooting rows.
  * §12: limitations.
* `docs/MONITORING.md`: the 7.9 paragraph. `README.md`: one line.
* Example: `examples/multi_topic/graph_schema_multi_pair.yaml` (new). Nodes Customer, Post (`post_id` integer)
  and Comment (`comment_id` integer) with **overlapping id ranges**. `LIKES` reads `mt-likes-posts`,
  `mt-likes-posts-import` (same pair) and `mt-likes-comments`, with `order_by: liked_at`.
  * `multi_topic_graph.py` gains generator/publisher/verifier support. The verifier checks the exact graph per
    labelled pair and finds 0 cross-pair edges.
  * `examples/multi_topic/README.md` gets a section.
* `tests/test_docs_yaml_snippets.py`: its classifier accepts mapping `topics` entries.

Files: `docs/USAGE.md`, `docs/MONITORING.md`, `README.md`, `examples/multi_topic/{graph_schema_multi_pair.yaml,
multi_topic_graph.py, README.md}`, `tests/test_multi_topic_example.py` (+cases), `tests/test_docs_yaml_snippets.py`.

AC:
1. The example schema loads. The generator produces overlapping ids, both pairs and a hub. The expected graph is
   computed independently. A real-planner replay equals it.
2. Every ```yaml snippet loads (the docs test).
3. Both compat goldens are unchanged, and the tracked-schema guard is still green: the new schema is not
   single-topic.

### Story 7.11: Integration test, including zero lock waits

*As a maintainer, I want a real Neo4j + Kafka test of a two-pair type in every zero-wait mode, so that exactness
and zero lock waits are proven end to end in CI's nightly job.*

Technical: new `tests/integration/test_multi_pair_relationship.py` (marker `integration`, skips without Docker). It
reuses S6's module patterns: the `neo4j_logged` fixture (5.26, query log, APOC), `Fleet`, in-process
`LeaseServer`, `FakeDocker`, `assert_zero_waits` (filtered LIKES entries, at least the expected number of write
transactions, polling up to 10 s).

Dataset:
* Person 0..P, Post 0..M and Comment 0..M with the **same ids**.
* `LIKES`:
  * `likes-posts-a` and `likes-posts-b` (Person->Post, 30% overlap, newer `liked_at` in each topic for half of
    the overlaps);
  * `likes-comments` (Person->Comment, chosen to reuse many `(person, id)` values of the Post likes).
* Hubs: Person 0 across both pairs, Post 0 and Comment 0.
* `GRAPH_LOADER_MULTI_PAIR_PAIRS` defaults to 20,000.

Cases:

| Case | Mode | Asserted |
| --- | --- | --- |
| order_by | stream lease fleet, 3 replicas | exact per labelled pair, 0 cross-talk, 0 waits |
| order_by | bulk lease, 3 replicas, static assignment | same |
| order_by | planned (real planner, dispatcher, 4 writers) | same, plan has 2 pairs, no write retries |
| order_by | bulk classic, 1 replica | exact; waits reported, not asserted |
| arrival | stream lease | counts per pair, every value was published for its pair, 0 waits |
| DLQ (if S6 post-S4 TODO 5 exists by then) | stream lease + in-process retry consumer, Comment nodes seeded late | exact after the late seeding, labelled DLQ keys, parking lot empty, 0 waits |

All cases also assert: loader exit codes 0, an empty rejection file, and no DLQ topic where none is configured.

Files: `tests/integration/test_multi_pair_relationship.py` (new).

AC: every case passes once locally; the zero-wait cases show a `waiting` sum of 0 over at least the bound; about
90 s at the default size; runs in the existing nightly `integration` job without workflow changes.

### Story 7.12: Docker proof

*As the owner, I want a container-level proof on a private stack, so that the real CLI, loaders, retry consumer,
planner and lease server load a two-pair type exactly with zero lock waits.*

Technical: `plan/v7/slice-7/docker_proof/{stack.sh, env.sh, run.sh, proof.py}`, copied from S4's harness with new
names and ports (for example `p0s7-*`), images `:v7s7`, its own `LOADER_CPUSET`, every connection variable
exported and printed, and the project `.env` never read.

Schema:
* Person, Post and Comment with colliding ids.
* `LIKES` reads `v7s7-likes-posts-a` (4 partitions), `v7s7-likes-posts-b` (6) and `v7s7-likes-comments` (3).
* `replicas: 2`, `dead_letter.enabled`.

| Run | What | Checks |
| --- | --- | --- |
| `orderby` stream | Post rows written; Comment rows published before Comment nodes (all DLQ'd), then Comment nodes | exact graph per labelled pair, 0 cross-pair edges; one `LIKES.dlq` with **6** partitions; every envelope key has 4 elements with labels equal to its `original_topic`'s pair; 0 labelled pairs split over DLQ partitions; `replay-dlq --source-topic v7s7-likes-comments --dry-run` count; `--target-topic v7s7-likes-posts-a --source-topic v7s7-likes-comments` exits 1 with 0 publishes |
| `arrival` stream | Post pair: topic b fully DLQ'd first, then a, then nodes; Comment ids collide | exact (sequenced per labelled pair), parking lot empty |
| `bulk` | plain `start --mode bulk` with `route_missing_endpoints_in_bulk` (lease default) | `edge_mode=lease reason=dead_letter`; exact; orphan rows per pair in the DLQ |
| `planned` | no `dead_letter`, flagless (planned default) | `edge_mode=planned reason=default`; exact; plan has `types.LIKES.pairs` (2) and 11-field chunks |
| `classic` | `--edge-classic` | exact; waits reported only |

Every run asserts: rc 0, edge and retry groups at 0 lag on all 13 / 6 partitions, query-log entries of the
proof database in the run window all `waiting: 0` (classic excepted), 0 containers left, and cleanup. Add a
slice-7 job to `.github/workflows/ci-multi-topic-proofs.yml` (manual dispatch).

AC: as in the table, recorded in `plan/v7/completion_slice-7.md`.

---

## 5. Execution order and parallelism

| Step | Story | Depends on | Parallel with |
| --- | --- | --- | --- |
| 1 | 7.1 golden | S4 merged | none (do it first, before any S7 code) |
| 2 | 7.2 schema | 7.1 | none (foundation) |
| 3a | 7.3 write path, then 7.4 leases, then 7.7 DLQ/index (one track, shared files) | 7.2 | 3b, 3c |
| 3b | 7.6 planned | 7.2 | 3a, 3c |
| 3c | 7.5 orchestration | 7.2 | 3a, 3b (7.4's one cli line at E48 rebases trivially) |
| 4 | 7.8 retry/replay | 7.7, 7.5 (cli) | 7.9 |
| 5 | 7.9 metrics | 7.4 | 7.8 |
| 6 | 7.10 docs/example | 7.2-7.9 | 7.11 |
| 7 | 7.11 integration | 7.6, 7.8 | 7.10 |
| 8 | 7.12 Docker proof | all | none |

Definition of done for the slice: full unit suite green (`-m "not integration and not smoke"`, excluding
`test_dev_environment.py`); `ruff check src tests examples` clean; `make monitoring-validate`; both goldens
unchanged; 7.11 and 7.12 recorded in `plan/v7/completion_slice-7.md` together with "what follows".

## 6. File-touch map

| File | 7.1 | 7.2 | 7.3 | 7.4 | 7.5 | 7.6 | 7.7 | 7.8 | 7.9 | 7.10 | 7.11 | 7.12 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| src/models/schema.py | | X | | | | | | | | | | |
| src/loader/edge_records.py | | | (guard only if needed) | | | | | | | | | |
| src/loader/edge_loader.py | | | X | | | | X | | | | | |
| src/loader/edge_execution.py | | | X | X | | | X | X (E20) | | | | |
| src/loader/dead_letter.py | | | | | | | X | | | | | |
| src/loader/parked_pair_index.py | | | | | | | X | | | | | |
| src/loader/retry_consumer.py | | | | | | | | X | | | | |
| src/loader/dead_letter_replay.py | | | | | | | | none (cli check) | | | | |
| src/loader/edge_planner.py | | | | | | X | | | | | | |
| src/loader/edge_plan.py | | | | | | X | | | | | | |
| src/loader/planned_writer.py | | | guard | | | X | | | | | | |
| src/orchestrator/planned_edges.py | | | | | | X | | | | | | |
| src/orchestrator/dependency_manager.py, isolation.py, coordination.py, fleet_contract.py | | | | | X | | | | | | | |
| src/cli.py | | | | E48 | X | | | replay | | | | |
| src/orchestrator/lease_server.py, src/loader/lease_client.py, relationship_bulk_monitor.py, docker_service.py, planned_dispatcher.py, mix_and_batch.py, node_loader.py | **unchanged** (verified in AC) | | | | | | | | | | | |
| docs/USAGE.md, docs/MONITORING.md, README.md | | | | | | | | | X | X | | |
| examples/multi_topic/* | | | | | | | | | | X | | |
| tests (new) | test_single_pair_compat.py, golden | test_schema_multi_pair.py | test_multi_pair_write_path.py | test_multi_pair_leases.py | test_multi_pair_orchestration.py | test_planned_multi_pair.py | test_multi_pair_dlq.py | test_multi_pair_retry.py | test_multi_pair_metrics.py | example and docs tests | integration/test_multi_pair_relationship.py | plan/v7/slice-7/docker_proof/* |

Hot files (merge risk): `edge_execution.py` (7.3, 7.4, 7.7, 7.8): keep these on one track in this order.
`cli.py` (7.4 one line, 7.5, 7.8 replay).

## 7. Owner question

**Q1. Does decision 1's "a type with two or more topics must declare a conflict rule" apply to a multi-pair type
whose label pairs each have only one topic?**

Example: LIKES reads `likes-posts` (Person->Post) and `likes-comments` (Person->Comment). Records of different
label pairs are different relationships, so they can never conflict.

* Literal reading: such a type must declare `order_by` or `topic_conflict`. A user would pick `arrival` and get a
  "nondeterministic" warning that is false here, or would have to add an `order_by` property they don't need.
* Recommended answer: evaluate the rule **per label pair**. A rule is required only when some pair is fed by two
  or more topics. A declared `precedence`/`arrival` on a type where no pair has two topics is accepted with a
  "has no effect" warning. Everything else in decision 1 (no silent default where an ambiguity exists, precedence
  bulk-only, the arrival warning, ties keep the existing value) applies unchanged per pair.
* Cost either way: one validator branch in `schema.py` (`_conflict_rule_is_declared`) plus `conflict_rule`.
  Story 7.2 AC 9 follows the answer.

Decided here (not questions; each follows from the binding decisions or is cheaply reversible):

| Topic | Decision |
| --- | --- |
| Reversed pairs (A->B and B->A in one type) | Supported. The plan records the pair per chunk. |
| A self-referencing pair beside a normal pair | Supported. In classic the whole type runs as an isolated phase. |
| Key properties in mapping entries | Required, matching the top-level form. The node-key equality check is unchanged. |
| Top-level `nodes` that no entry uses | An error. |
| `execution.worker_count` | Applies per label pair. |
| `--stream-clock-rotation`, slot gating, rotation, shared clock | Refuse multi-pair types (deprecated Phase 4 paths). |
| Producer planning | Multi-pair sources must name their topic. The mapping form is refused for them. |
| D1 write evidence | Per edge type, as D1 states ("same-edge-type"). |
| An envelope whose topic is not in the type, or whose labels mismatch | `invalid_on_retry`, then the bounded park (row-own condition, D1). |

## 8. Out of scope

* Inferring the pair from a record (decision 3), or one topic carrying several pairs.
* Per-pair metric labels.
* Multi-pair types on deprecated Phase 4 paths.
* Changing planned-mode DLQ fail-closed (P5-Q3).
* Node labels with several topics.
* Kafka timestamps as an order key (decision 4).
* S3's deferred cross-topic chunk sharing.
* S4's optional root-local hold.

## 9. Re-grep commands (run on the S7 base before starting)

```bash
# endpoint-label sites (section 3); compare with this file's table
P='nodes\.source|nodes\.target|is_self_referencing|\.nodes\b|source_key_property|target_key_property|source_key_config|target_key_config|bind_endpoint_properties|BucketCellRouter\(|labels_of\(|endpoint_labels='
git grep -nE "$P" HEAD -- 'src/*.py' | grep -vE 'schema\.nodes|self\.nodes\b'
# implicit single-pair sites not matched above
git grep -nE 'normalize_edge_record\(decoded|KafkaReadTask\(edge|writers = \{|build_retry_base_writer\(|_single_topic_sub_edge|model_copy\(update=\{"topics"' HEAD -- src
# pair-identity sites
git grep -nE '_pair_key\(|def is_blocked\b|_pair_from_envelope\(|envelope\.key_bytes|key = json\.dumps|ParkedPairIndex\(' HEAD -- src
# lease server stays unaware of types/topics/pairs
git grep -nE 'topic|pair|edge_type' HEAD -- src/orchestrator/lease_server.py src/loader/lease_client.py
```

## 10. Owner decision (proposed 2026-10-01, confirmed by the owner 2026-10-02)

**Q1 (section 7).** Owner decision 1's conflict rule applies **per label pair**:

* A rule (`order_by` or `topic_conflict: precedence | arrival`) is required only when some label pair of the type is
  fed by two or more topics. The S1 error message then also names that pair and its topics.
* A multi-pair type whose pairs each have one topic needs no rule (`conflict_rule == "single"` when `order_by` is
  unset).
* A declared `precedence` or `arrival` on a type where no pair has two or more topics is accepted with a WARNING that
  it has no effect.
* Everything else in decision 1 is unchanged and applies per pair: no silent default where an ambiguity exists,
  precedence is bulk-only (refused in stream at schema validation and at `start`), the arrival warning names the pair
  groups, ties on `order_by` keep the existing value.

Story 7.2 AC 9 follows this decision. If the owner keeps the literal reading instead, only
`_conflict_rule_is_declared` and `conflict_rule` change back.

## 11. Line references re-checked at the S7 base (`feat/multi-topic` d334cdd)

`git diff 72ca167 d334cdd -- src` is empty, so every `s4:72ca167` reference above holds at d334cdd, and every
`b2fa3d3` reference holds for the files S4 did not touch (`schema.py`, `edge_execution.py`, `edge_planner.py`,
`edge_plan.py`, `edge_records.py`, `planned_*.py`, `orchestrator/*`). The section 9 commands give the same results as
section 3: 103 endpoint-label lines in `src/`; the implicit sites at edge_loader.py:1271, edge_planner.py:1602/1614/1619,
planned_writer.py:677, retry_consumer.py:680/2606 and cli.py:719/725; the pair-identity sites at
parked_pair_index.py:189 (callers 461-782), dead_letter.py:1161, retry_consumer.py:460 and the `key_bytes` sites
1056-1779; the index constructions at edge_loader.py:1840 and retry_consumer.py:2626; and only a docstring match
(lease_client.py:155) for the lease-server grep. Other anchors: `_select_keyed_lease_labels` cli.py:394,
`_precedence_substage_plan` cli.py:705, `_co_scheduled_self_references` cli.py:945,
`_require_supported_topic_conflicts` cli.py:1932, `handle_replay_dlq` cli.py:2825, `build_edge_execution`
edge_execution.py:1884, `route_unresolved_endpoint` dead_letter.py:1097, `_on_message` retry_consumer.py:961,
`kafka_read_tasks` edge_planner.py:1582, `ChunkSpec` edge_plan.py:334, `validate_plan_against_schema`
planned_edges.py:193.
