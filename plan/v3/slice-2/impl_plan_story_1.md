# Implementation Plan — Phase 3 / Slice 2 / Story 1

## Scope

Add explicit lane-count configuration and the immutable transport record that
retains a normalized relationship event's Kafka provenance. This story does not
route values, batch records, use threads, call Neo4j, or commit Kafka offsets.

## Files

- Modify `src/models/schema.py`: add `lane_count: int = Field(default=1, ge=1,
  le=4096)` to `MixAndBatchConfig` and document it.
- Modify `config/graph_schema.yaml`: declare readable lane counts for each
  canonical edge (default `1`, preserving existing single-lane behavior).
- Create `src/loader/mix_and_batch.py`: define `Direction` literal and frozen
  `RoutedEdgeRecord(record, topic, partition, offset, lane_id, direction,
  source_bucket, target_bucket)` with no consumer/writer imports.
- Modify `tests/test_schema_validation.py`; create `tests/test_mix_and_batch.py`.

## Logic

`lane_count` is bounded to avoid unbounded queue allocation and maintains the
current behavior by defaulting to 1. `RoutedEdgeRecord` is an immutable data
carrier so later lane batching cannot lose or rewrite source topic, partition,
or offset. Its `__post_init__` performs runtime invariants: a nonblank string
topic; non-boolean, nonnegative integer partition/offset/lane/source-bucket/
target-bucket values; and exactly `forward` or `reverse` direction. The work
record intentionally does not validate bucket/lane range against `lane_count`;
Story 2 owns routing assignment.

## Tests

- defaults and explicit values parse from the canonical schema;
- zero, negative, and over-limit lane counts fail validation;
- a routed work item preserves `EdgeRecord` identity and all Kafka metadata;
- invalid routing metadata, blank topic, and invalid direction fail before it
  reaches a future executor.

Run: `.venv/bin/python -m pytest tests/test_schema_validation.py tests/test_mix_and_batch.py -v`

## Rollback

No external resources are touched. Remove the isolated config/type additions if
validation breaks a pre-existing schema; no offsets or database writes exist.
