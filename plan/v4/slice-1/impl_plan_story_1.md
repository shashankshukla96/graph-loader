# Implementation Plan — Phase 4 / Slice 1 / Story 1

## Scope

Add only backward-compatible schema/YAML configuration for the global batch
clock. No Kafka topics, producer, Docker health check, loader gating, or
runtime clock behavior is introduced here.

## Files

- Modify `src/models/schema.py`: create `CoordinationConfig` and add
  `LoadingConfig.coordination` via a default factory.
- Modify `config/graph_schema.yaml`: document default coordination settings.
- Modify `tests/test_schema_validation.py`: defaults and invalid values.

## Contract

`CoordinationConfig` fields:

```python
topic: str = "graph.loader.coordination"
bucket_count: int = Field(default=64, ge=1, le=4096)
slot_duration_ms: int = Field(default=60_000, ge=1, le=3_600_000)
lease_timeout_ms: int = Field(default=120_000, ge=1, le=3_600_000)
```

Reject blank/whitespace topic and bool values for all integer fields. An
after-model validator requires `lease_timeout_ms >= slot_duration_ms`. The
field is nested under existing `loading`, so configurations omitting it retain
defaults and no existing call site changes.

## Tests

- Loaded canonical/default schema exposes all expected values.
- Custom values survive YAML load.
- Parametrized zero, negative, over-limit, bool, blank topic, and lease-shorter
  cases fail as `SchemaLoadError`/Pydantic validation.
- Existing minimal schema fixtures omit coordination and still validate.

Run `.venv/bin/python -m pytest tests/test_schema_validation.py -v`.

## Rollback

This story has no runtime side effects. Revert the isolated config model and
YAML section if a later protocol needs different settings.
