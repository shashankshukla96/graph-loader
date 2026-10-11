# Slice 1: schema model, `topics`, conflict-rule validation

Foundation. Nothing else starts before this merges. Files: `src/models/schema.py`,
`src/utils/schema_loader.py` (check only), `tests/test_schema_validation.py`,
`docs/USAGE.md` section 3 (field table only; full docs in S6).

## Decisions (recommended answers, owner may override)
* D1 conflict rule: explicit choice required for multi-topic types (see slices.md section 2).
* D2 `topic` property raises for multi-topic types (straggler detection).
* D3 DLQ default name for multi-topic types is `<type>.dlq`.
* D4 `precedence` rejected in stream.
* D5 YAML keys: `topics: [..]`, `topic: x` shorthand, `topic_conflict: order_by|precedence|arrival` (optional).

## Story 1.1 Accept `topics` / `topic`
As a schema author, I want `topics: [a, b]` (or `topic: a`), so that one relationship type can read several topics.

Technical: `EdgeConfig`: add `topics: list[str]`; `@model_validator(mode="before")` maps
`topic` -> `topics=[topic]`, rejects both or neither present, strips, rejects
blanks/non-str/duplicates within the list. Keep `topic` as a `@property`
(single topic) raising `MultiTopicEdgeError(ValueError)` for len>1. `model_dump()`
must not emit both keys (serialized plans and planner processes pickle
`EdgeConfig`; verify pickling and `model_copy`). `topic` must remain an init
kwarg for code/tests that construct `EdgeConfig(topic=...)`.

Acceptance:
* `topic: a` and `topics: [a]` load to identical config; `edge.topic == "a"`.
* `topics: [a, b]`: `edge.topic` raises `MultiTopicEdgeError`; `edge.topics == ["a","b"]`.
* Both keys, neither key, `topics: []`, `topics: [a, a]`, blank entry: each a clear ValueError naming the type.
* List order is preserved (it is semantically meaningful for `precedence`).
* `EdgeConfig` pickles/unpickles and round-trips through `model_dump`/`model_validate`.

## Story 1.2 Topic ownership validation
Rewrite the `owners` loop in `validate_edge_endpoints` to iterate every edge topic; a topic claimed by any other node/edge (or twice in one list) fails, naming both owners. Coordination topic and DLQ/parked/parked-pairs topics (resolved for each entity, via the new naming helpers in S4) must not collide with any data topic. Duplicate edge TYPES stay an error (`Duplicate edge types detected in schema.`), message unchanged; it must not be relaxed, because types remain the key for metrics/groups/plan units.

Acceptance: two types sharing a topic fails; a type listing a node's topic fails; existing messages unchanged for single-topic schemas (existing tests pass untouched).

## Story 1.3 Conflict-rule field and validation
Add `topic_conflict: Literal["order_by","precedence","arrival"] | None`.
Validator (`mode="after"`, EdgeConfig):
* len(topics)==1: `topic_conflict` must be None or `arrival`-equivalent no-op (accept any value; ignore; log nothing). `order_by` behaves as today.
* len>1: if `order_by` set and `topic_conflict` None -> effective `order_by`; if `order_by` set and `topic_conflict` in (precedence, arrival) -> error ("pick one"); if `order_by` None and `topic_conflict` None -> ValueError listing the three options and showing the YAML fix; `topic_conflict: order_by` without `order_by` -> error.
* Expose computed `EdgeConfig.conflict_rule` ("single" | "order_by" | "precedence" | "arrival").
* `GraphSchema` validator: any `precedence` type with `loading.mode == "stream"` -> error. (The CLI `--mode` flag can override; S5 adds the runtime check.)
* Warning (logger.warning, once per type at load) for `arrival`; no warning for order_by.
* Warn (not error) when `order_by` property type is not date/datetime/integer/float (strings compare lexicographically; ISO-8601 is fine, say so).

Acceptance: table-driven test over all (n topics x order_by x topic_conflict x loading.mode) combinations; warning text asserted with caplog.

## Story 1.4 Backward compatibility
Acceptance: every existing YAML under `examples/`, `tests/` fixtures and `docs/USAGE.md` snippets validates and dumps the same; `load_schema()` on them returns `edge.topic` unchanged. Full unit suite green. Run `grep -rn "\.topic\b"` and list remaining single-topic assumptions into `plan/v7/slice-1/straggler_list.md` for S2-S5 (new file is allowed here).

Test plan: `tests/test_schema_validation.py` (extend). No Docker.
