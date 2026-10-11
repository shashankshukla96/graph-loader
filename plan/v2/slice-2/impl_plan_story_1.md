# Story 1 Plan — Schema-backed Batch Settings

Modify `src/models/schema.py`, `config/graph_schema.yaml`, and
`tests/test_schema_validation.py`.

- Extend `LoadingConfig` with positive bounded `unwind_batch_size`, `flush_interval_ms`,
  `retry_max_attempts`, `retry_base_delay_ms`, `retry_max_delay_ms`, and configurable
  `rejection_log_path` defaults.
- Validate `retry_max_delay_ms >= retry_base_delay_ms`; retain existing edge retry settings
  unchanged because these are node-loader runtime settings.
- Document units/defaults in model docstrings and YAML comments, including that rejection
  records are an fsync'd Phase 2 interim sink to be replaced by Phase 5 DLQ routing.
- Tests cover defaults, zero/negative bounds, invalid retry range, and custom values.

No loader behavior changes in this story. Rollback is limited to the new schema fields and
their tests.
