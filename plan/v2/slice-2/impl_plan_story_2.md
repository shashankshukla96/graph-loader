# Story 2 Plan — Atomic Batched Writes and Offset Ledger

Modify `src/loader/node_loader.py` and `tests/test_node_loader.py`.

- Add `NodeWriter.write_batch(records: Sequence[NodeRecord])` using one session,
  transaction, and the existing `UNWIND $batch` query. Consume the query result within that
  transaction before resolution; retain `write()` as a one-record wrapper.
- Pass `LoadingConfig` from `main()` into `NodeLoader`; its batch size and idle interval must
  come from the loaded schema and be asserted by CLI-construction tests.
- Add `PendingRecord` with its complete Kafka metadata and a per-partition ordered ledger
  initialized from that partition's first observed offset, including nonzero starts.
- Resolve entries only after the whole cross-partition batch succeeds. Commit explicit
  `TopicPartition(topic, partition, last_contiguous_offset + 1)` values per partition.
  Record each successful partition commit independently; if a later partition commit fails,
  stop fail-closed without regressing or retrying already confirmed commits.
- Flush at the configured full-batch size and monotonic idle interval. On `max_messages`,
  shutdown event, or normal close, final-flush and commit before publishing `DRAIN_COMPLETE`
  or returning success. A final write/commit failure emits no completion and returns nonzero.
- Tests cover one materialized query/transaction per batch, configured size cap, partial idle
  and max-message flush, two nonzero-start partitions, partial commit failure, write barriers,
  and failure-before-`DRAIN_COMPLETE`.

No retry, durable malformed sink, or rebalance handling is added until Story 3.
