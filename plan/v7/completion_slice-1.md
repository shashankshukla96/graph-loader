# Completion: v7-S1 schema model

Branch `feat/v7-s1-schema` (from `feat/multi-topic` 4382960). Files: `src/models/schema.py`,
`tests/test_schema_multi_topic.py` (new), `docs/USAGE.md`, `plan/v7/slice-1/straggler_list.md`.

## Stories

* 1.1 `topics` / `topic` shorthand: done. A `before` validator maps `topic` to `topics`, strips entries,
  rejects both/neither/empty/duplicate/blank/non-str (every error names the type). `topic` is a read
  property raising `MultiTopicEdgeError(ValueError)` when `len(topics) != 1`. `model_dump` emits only
  `topics`; pickle, `model_copy` and dump/validate round trip are tested.
* 1.2 Ownership: done. The owner loop iterates every edge topic; DLQ/parked/index collision checks
  resolve names through `EdgeConfig.dead_letter_entity`. Duplicate-type message unchanged.
* 1.3 Conflict rule: done, per the owner decisions (explicit rule mandatory for 2+ topics, `precedence`
  rejected in stream, `arrival` warns). A table-driven test covers topics (1,2,3) x order_by x
  topic_conflict x loading.mode (absent/bulk/stream).
* 1.4 Backward compatibility: done. `config/`, `examples/imdb`, `examples/social` and the `var/` YAML
  load with `edge.topic` unchanged; full unit suite unchanged-green. Straggler list written.
* N1: `NodeConfig` validator rejects `max_retry_attempts`, `parked_topic` and
  `route_missing_endpoints_in_bulk` (by `model_fields_set`, so an explicit default also fails). Comments
  in `DeadLetterConfig` and the `GraphSchema` DLQ block corrected.
* N2: `extra="forbid"` added to `RetryConsumerConfig`, `BucketLeaseStreamConfig`, `CoordinationConfig`
  (frozen kept) and, because every shipped schema, fixture and the full suite still passed, also to
  `LoadingConfig`, `MixAndBatchConfig`, `RetryConfig`, `EdgeExecutionConfig`. Not changed:
  `PropertyConfig`, `ConstraintConfig`, `IndexConfig`, `NodeConfig`, `EdgeConfig`,
  `SourceTargetConfig`, `GraphSchema` (not loading configs, left alone to limit the blast radius;
  `EdgeConfig` also relies on `topic` being consumed by its before validator, which works either way).

## Deviations

* Tests live in a new `tests/test_schema_multi_topic.py` instead of extending `test_schema_validation.py`
  (that file is 840 lines; existing tests were left untouched).
* `precedence` is rejected at load only when `loading.mode` is explicitly present in the YAML
  (`"mode" in loading.model_fields_set`). The field defaults to `stream`, and the CLI `--mode bulk`
  run of a schema without `loading.mode` must keep working, so the default case is deferred to the
  start-time helper.
* The arrival warning is logged from `GraphSchema` validation (once per type per validation, so at
  `load_schema()`), not separately at `start`. `arrival_conflict_warnings(schema)` returns the same
  messages for callers/tests; S2 needs no extra call.
* `topic_conflict` on a single-topic type is accepted and ignored (as planned). `conflict_rule` returns
  `"single"` (one topic, no `order_by`), `"order_by"` (whenever `order_by` is set, including one topic),
  or the declared `precedence` / `arrival`.
* Duplicate topics within one list are rejected in `EdgeConfig` (named by type) rather than in the
  ownership loop; cross-entity duplicates stay in the ownership loop.
* `order_by` property type warning fires for any type with `order_by` and a `string` property (also
  existing single-topic schemas); it is a log line only.
* Pre-existing, not fixed: `GraphSchema.model_validate(schema.model_dump())` fails for a
  `dead_letter: {enabled: false}` block because the dump carries the default `max_retry_attempts`,
  which the DLQ validator rejects without `enabled`. Unchanged by this slice.

## What S2-S6 must pick up

API (all in `src.models.schema`):

* `EdgeConfig.topics: list[str]`, `EdgeConfig.topic` (raises `MultiTopicEdgeError` if not exactly one),
  `EdgeConfig.is_multi_topic`, `EdgeConfig.conflict_rule` (`"single" | "order_by" | "precedence" |
  "arrival"`), `EdgeConfig.topic_conflict`, `EdgeConfig.dead_letter_entity` (topic if single, else type).
* `edge_dead_letter_topic_name(edge) -> str | None`, `edge_parked_topic_name(edge) -> str | None`,
  `edge_parked_pair_index_topic_name(edge) -> str | None`. The old
  `dead_letter_topic_name(entity_topic, dl)` family is unchanged; do not call it with `edge.topic`.
* `require_no_precedence_in_stream(schema: GraphSchema, mode: str) -> None` (raises `ValueError`; S2/S5
  call it in `start` with the resolved mode, after `--mode` is applied).
* `precedence_edge_types(schema) -> list[str]` (S5 sub-stage selection), `arrival_conflict_warnings(schema)
  -> list[str]`.

Open items for later slices:

* Every site in `plan/v7/slice-1/straggler_list.md` (about 30 `edge.topic` uses in cli, edge_loader,
  edge_planner, relationship_bulk_monitor, edge_execution, retry_consumer) raises for a 2-topic type
  until its slice moves it to `topics`. S1 deliberately leaves `src/` outside `schema.py` untouched.
* S2: the start-time stream check above; subscribe to `edge.topics`; per-topic boundary and lag; the
  topic preflight at `cli.py:~1617`.
* S3: planner task order `(topic index, first partition)` for `precedence`; conditional `SET` for `order_by`.
* S4: type-wide DLQ naming via the new helpers; `replay-dlq --source-topic`; DLQ partition count is the
  max over the type's topics; parked-pair index type-wide. `retry_consumer.py` / `parked_pair_index.py` /
  `dead_letter.py` were deliberately not touched here (another fix pass owns them).
* S5: `precedence` sub-stages and the `start --mode stream` refusal (use the helper).
* S6: document the lag series bound and update examples; the docs already cover the schema reference,
  DLQ naming, migration warning and N1/N2.

## Follow-up commit (review M1 and should-fix items)

* M1: the N1 node check and the edge `max_retry_attempts`-requires-enabled check now reject only a field
  that is set AND differs from its default, so `model_dump()` re-validates; the three shipped schemas
  round-trip (tested). `model_dump(exclude_unset=True)` is the documented serialization contract (a full
  dump makes `loading.mode: stream` explicit, which a `precedence` schema refuses). Mode default unchanged.
* N2 `extra="forbid"` kept; release note added to USAGE.md (older images refuse newer keys).
* The two rule-combination checks (`order_by` + `precedence|arrival`, `topic_conflict: order_by` without
  `order_by`) now apply for any topic count; only "rule required" needs 2+ topics. `topic_conflict` is
  therefore no longer fully ignored on a single-topic type.
* New API (additive): `EdgeConfig.precedence_rank(topic) -> int` (index in `topics`, higher wins,
  `ValueError` on unknown topic); `GraphSchema.edge_for_topic(topic) -> EdgeConfig | None`;
  `schema_data_topics(schema) -> list[str]` (nodes first, deduped, stable order).
* Load-time precedence error now says to set `loading.mode: bulk` (or remove it) and run `start --mode bulk`.
* S2 must: call `require_no_precedence_in_stream(schema, resolved_mode)` in `start` and surface a clear
  error in the stream topic preflight. S4: `cli.py:2385` added to the straggler list.
