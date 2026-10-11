# Story 4 implementation plan: Node Loader Instrumentation

## Files

- Edit: `src/loader/node_loader.py`
- Edit: `tests/test_node_loader.py`

## Design

`NodeLoader.__init__` gains `metrics: GraphLoaderMetrics | None = None`,
defaulting to `process_metrics()` — mirrors how `writer`/`producer`/
`consumer` are already injected. `main()` passes the already-constructed
`process_metrics` instance (Story 3 already calls `process_metrics()`
before constructing `NodeLoader`).

`_write_batch_with_retry` (line ~507): compute `elapsed_seconds =
time.perf_counter() - started_at` once on the success path (replacing the
inline `(time.perf_counter() - started_at) * 1000` in the log call), then:

```python
self._metrics.write_duration.labels(
    kind="node", name=self._node_config.label, stage="node-write"
).observe(elapsed_seconds)
self._metrics.records_consumed.labels(
    kind="node", name=self._node_config.label, label_or_type=self._node_config.label
).inc(len(records))
return True
```

One observe/inc per batch, immediately beside the existing log call, using
the same timer already computed — no second timer, no per-record loop.

Retry branch: `self._metrics.retry.labels(kind="node",
name=self._node_config.label, reason="neo4j-transient").inc()` added right
before the existing `self._pause_assignments()` call, i.e. only on an
attempt that *will* retry (retryable and not yet exhausted) — never on the
final, non-retried failure, and never once per record.

DLQ routing (`build_malformed_record_sink`/`DeadLetterProducer.publish`)
already fires `dead_letter.on_dlq_routed`, and Story 3 already calls
`metrics_module.install_dlq_routed_listener()` in `main()` — this story
adds no new DLQ wiring, only a test proving a malformed node record
increments `dlq_routed_total{kind="node",...}` and *not*
`records_consumed_total` (Story 2's resolved deviation: a DLQ'd record is
not "consumed").

## Acceptance Criteria

Per `stories.md` Story 4 (unchanged by any correction): batch-level
`records_consumed_total`/`neo4j_write_duration_seconds`, retry-count
`retry_total`, DLQ'd-record-does-not-count-as-consumed, and
`GRAPH_LOADER_METRICS_ENABLED=0` still runs end to end with no
`AttributeError` (the metric *objects* still exist and accept
`.inc()`/`.observe()` even with no HTTP server exposing them).

## Tests

`tests/test_node_loader.py`: construct `NodeLoader` with an injected
`GraphLoaderMetrics(registry=CollectorRegistry())`; assert exact
`records_consumed_total`/`neo4j_write_duration_seconds` counts after one
successful batch; assert `retry_total` increments by attempt-count-minus-one
on a retried-then-successful batch, `records_consumed_total` still by N
exactly once; assert a DLQ'd malformed record increments `dlq_routed_total`
and leaves `records_consumed_total` at 0.
