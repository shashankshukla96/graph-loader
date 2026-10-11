# Story 3 Plan — Retry and Malformed-Record Resolution

Modify `src/loader/node_loader.py`, `tests/test_node_loader.py`, and deployment
configuration used by the node-loader containers.

- Add `RejectionSink(path: str | Path)` with
  `append(*, topic, partition, offset, node_label, reason) -> None`. Each append
  creates the parent directory, writes one compact JSON object without the raw
  payload, flushes the file, and calls `os.fsync()` before returning. A failed
  append is fatal and does not resolve or commit the Kafka offset. The sink is
  explicitly an interim Phase 2 log-and-skip record; Phase 5 will replace it
  with DLQ publication.
- Extend the ordered partition ledger so every first-observed Kafka position is
  registered before parsing. Valid records remain unresolved until their whole
  Neo4j batch succeeds. Malformed JSON, UTF-8, top-level shape, and schema records
  become resolved only after a durable rejection append. Commit only the longest
  contiguous resolved prefix; never jump a valid pending record or a failed sink
  append. Add one shared `_commit_contiguous_resolutions()` helper invoked after
  successful batch resolution and immediately after a successful rejection
  append, so malformed-only partitions advance promptly while malformed records
  behind a pending valid record remain blocked. Broker/message errors remain
  fatal rather than being classified as malformed input.
- Add `NodeLoader(..., rejection_sink=None, sleeper=time.sleep)` and private
  helpers for `_is_retryable_neo4j_error`, `_write_batch_with_retry`,
  `_heartbeat_backoff`, `_pause_assignments`, and `_resume_assignments`.
  Retry only `Neo4jError` instances whose `is_retryable()` returns true. Attempts
  include the initial call. Delay is
  `min(retry_base_delay_ms * 2 ** retry_index, retry_max_delay_ms)`.
- Before a batch write, pause the consumer's current assignments to cap input.
  During every retry delay, repeatedly call `consumer.poll(0)` and sleep in
  bounded slices no longer than one third of `max_poll_interval_ms`; use the
  injected monotonic clock and sleeper for deterministic tests. Resume only
  after the complete write and all eligible synchronous offset commits succeed.
  Retry exhaustion, non-retryable errors, commit errors, and shutdown during
  backoff fail closed with topic/partition/offset attribution and leave the
  consumer paused.
- Add a FIFO deferred-message queue. Every callback/heartbeat `poll(0)` return
  value is retained in that queue rather than ignored, because Kafka may return
  an already-prefetched data message even after `pause()`. The main loop drains
  deferred messages before its next blocking poll, and only after the current
  batch is fully resolved/committed and assignments resume; no polled message is
  dropped or committed past.
- Subscribe with `on_assign` and `on_revoke`. Track the latest assigned
  topic-partitions. A revoke that intersects a pending batch or an unresolved
  ledger sets a fatal rebalance barrier; revoked partitions are removed before
  any later commit decision, so no write result or rejection can advance them.
  Before every contiguous commit decision—including immediately after a durable
  rejection append—and after every synchronous Neo4j write, call the same
  callback poll helper to dispatch a pending revoke callback while preserving
  any returned message in the deferred FIFO, then re-check the barrier and
  ownership. The revoke callback calls
  `consumer.unassign()` for the existing manual `assign` model. Clean revokes
  permit the next assignment epoch.
- Construct the default `RejectionSink` from `REJECTION_LOG_PATH` when set,
  otherwise `schema.loading.rejection_log_path`, in `main()`. Extend
  `DockerService.run_node_loader(..., rejection_dir="var/rejections")` to create
  and mount the absolute host directory at `/app/rejections`, and set a
  per-replica `REJECTION_LOG_PATH=/app/rejections/<node-label>-<replica-id>.jsonl`.
  This exact bind mount preserves records when `remove=True` deletes a stopped
  container and avoids concurrent writers sharing a file. Add DockerService unit
  coverage for the mount and environment override; no Kafka DLQ topic or producer
  path is added.
- Unit tests cover retryable success with exact capped delays, retry exhaustion,
  non-retryable failure, heartbeat polls and assignment pause/resume, shutdown
  during backoff, durable JSONL fields plus `flush`/`fsync`, rejection-write
  failure, malformed/valid interleaving across partitions, a malformed-only
  immediate commit, a valid-record barrier preceding a malformed record, commit
  failure, and revoke during unresolved work. Tests also prove no DLQ publish,
  no commit of a revoked partition, and that the post-write poll catches a revoke
  before the commit decision. A heartbeat poll returning prefetched data is also
  verified to defer and later process that message exactly once.

Rollback is limited to these loader/runtime changes; the Story 1 schema fields
remain valid and Story 2's batch/ledger behavior remains the fallback baseline.
