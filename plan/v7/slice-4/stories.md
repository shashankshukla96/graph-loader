# Slice 4: type-wide DLQ, retry consumer, parked-pair index, replay-dlq

Depends on S1, S2 (shares `cli.py`). Files: `src/models/schema.py` (naming helpers),
`src/loader/dead_letter.py`, `src/loader/dead_letter_replay.py`, `src/loader/retry_consumer.py`,
`src/loader/parked_pair_index.py` (check; likely no change), `src/loader/edge_loader.py` (DLQ/index wiring at ~1667-1720),
`src/loader/edge_execution.py` (line ~1929 `dead_letter_topic_name`), `src/cli.py` (lines ~1404, 1652-1704, 2310-2375 replay),
tests `test_dead_letter*.py`, `test_retry_consumer.py`, `test_parked_pair_index.py`, `test_cli_dead_letter.py`.

## Decision: per-type DLQ (not per-topic)
Per-topic DLQs would need a parked-pair index per topic, so a pair parked from topic a would not block a later record of the same pair arriving via topic b, and the retry consumer could replay a's old record after b's newer one (the exact P5-S4 bug). Per-type DLQ keeps one index, one retry stream, one parking lot per type and bounded topic count (3 per type, not 3 per topic). Envelope identity already includes `original_topic`, so dedupe (`ParkedPairIndex.contains` by topic/partition/offset) stays correct.

## Story 4.1 Naming helpers take the edge
`dead_letter_topic_name(entity_topic, dl)` -> add `dead_letter_topic_name_for_edge(edge)`: single topic => `<topic>.dlq` (unchanged), multi-topic => `dl.topic or f"{edge.type}.dlq"`. Parked and index names derive from the resolved DLQ (already so). Keep old functions for nodes. Update all edge call sites (`cli.py:1404,1694-1704`, `retry_consumer.py:1447,1505-1536`, `edge_loader.py:1694`, `edge_execution.py:1929`).
Validation: resolved DLQ/parked/index names collision-checked against all data and DLQ topics (extends S1.2).
Acceptance: golden-name tests for 1 and 2 topics; adding a second topic to a type whose explicit `dead_letter.topic` is set keeps the name; schema load fails on a collision.

## Story 4.2 DLQ topic provisioning
`ensure_dead_letter_topic(source_topic=...)` sizes partitions from ONE source topic (dead_letter.py:679-720). Change to `source_topics` and use `max(partitions)`. DLQ key stays `envelope.key_bytes` so one pair's records from all topics land in one DLQ partition (pair order there is DLQ arrival order; this is the same best-effort as arrival rule). Existing topic: verify retention only (unchanged).
Acceptance: fake admin: topics with 3 and 8 partitions -> DLQ 8; idempotent on rerun.

## Story 4.3 Retry consumer and parked-pair index across topics
Retry consumer is keyed by edge type and reads the DLQ; it re-writes the row via the type's writer (topic-agnostic). Verify `_pending` identity and `retry_after` heap use `(topic, partition, offset)` of the DLQ record, and index identity uses `original_topic` etc. Index topic name becomes per type. Pair key `_pair_key(source_key, target_key)` is unchanged in A (S7 must extend it). Loader-side `ParkedPairs` check (live rows consult the index) needs no topic knowledge.
With `order_by`: no index (unchanged); retried rows use the conditional SET so older retries cannot win.
Without order_by and `precedence`/`arrival` rule in stream: pair-parking is the only protection; test that topic b's later record of a parked pair is also routed behind it.
Acceptance: unit: pair P missing-endpoint from topic a, later P from topic b is parked behind it; after the endpoint appears, retry order preserves a-then-b DLQ order; parked record from topic a replays and never overwrites b's newer committed value when order_by is set.

## Story 4.4 replay-dlq
`handle_replay_dlq` (cli.py ~2310): `source_topic = entity.topic` -> for multi-topic types, republish each envelope to ITS `original_topic` (validated as one of the type's topics); `--target-topic` still overrides; add `--source-topic T` filter (repeatable) to replay only one topic's envelopes. `--parked` uses the resolved parked topic. `ReplayFilter` gets `original_topic`. The replay pre-flight that checks the target topic exists/partitions (dead_letter_replay.py) loops over the distinct targets. Warn when replaying into a `precedence` or `arrival` type without `order_by` that stream ordering is not preserved.
Acceptance: DLQ with envelopes from a and b replays to a and b respectively; `--source-topic a` replays only a; unknown original_topic aborts with a clear error; single-topic behaviour byte-identical.

## Story 4.5 Bulk DLQ opt-in
`route_missing_endpoints_in_bulk` types with several topics: in bulk all topics' rows route to the type DLQ. Planned stays fail-closed (P5-Q3), untouched. Document that a DLQ'd row of the EARLIER topic replayed later can overwrite the later topic's value unless `order_by` is set.
Acceptance: test message/doc; bulk classic 2-topic DLQ opt-in unit test.

Test plan: unit with fakes; one Docker test via the S6 integration test (stream, missing endpoint on topic a, retry).
