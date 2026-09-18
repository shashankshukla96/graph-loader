# Story 4 Plan — Integration and Operational Verification

Modify `tests/test_node_loader_integration.py`, `README.md`, and
`plan/v2/slice-2/task.md`. No production loader behavior changes are planned.

- Add marked `@pytest.mark.integration` Kafka-and-Neo4j end-to-end tests using
  the existing Testcontainers fixtures. They create disposable Person (two
  partitions), Company (one partition), and retry topics through `AdminClient`,
  deleting each in `finally` blocks. The main schema copy has a unique
  consumer-group prefix, `unwind_batch_size=2`, and a `tmp_path` JSONL rejection
  sink.
- Produce Person records explicitly to partition 0 (valid at offset 0 and valid
  at offset 1) and a malformed Person record to partition 1 (offset 0), plus two
  Company records (an initial value followed by an update). Execute the actual
  `node_loader_main` command once per label, with deterministic caps
  `--max-messages 3` for Person and `--max-messages 2` for Company. Assert the graph contains two
  Person nodes and one Company node with updated values; assert the rejection
  JSONL contains topic, partition, offset, node label, and reason but no raw
  payload.
- Query Kafka through an actual `Consumer(...).committed([TopicPartition(...)])`
  for `unique-group-Person` partition 0 and partition 1, and
  `unique-group-Company` partition 0. Assert next offsets are respectively
  `2`, `1`, and `2`; this proves the malformed partition neither blocks nor
  advances the valid partition.
- Re-run the Person topic under a second unique group with
  `unwind_batch_size=3`. Its two valid records must use the final partial-batch
  flush while the malformed record remains independently resolved; invoke it
  with `--max-messages 3`. Assert both
  partition next offsets again and unchanged graph counts/values, proving both
  replay idempotency and the undersized final flush.
- Add a separate container-backed successful-retry integration test. Feed a real
  Kafka Consumer and actual Neo4j `NodeWriter` to `NodeLoader`, wrapping the
  writer in a local fail-once adapter that raises `TransientError` once then
  delegates, and run it with `max_messages=1`. Assert two write attempts, a durable graph node, and the actual
  consumer-group next offset. Keep the existing deterministic unit tests as
  named coverage for capped delay, retry exhaustion, non-retryable failures,
  rejection-sink failure, heartbeat budget/prefetched data, partition gaps, and
  rebalance barriers.
- Do not add a simulated DLQ; assert the integration sink is a local file.
- Replace the obsolete README claim that batching/retries are future work with
  Phase 2 operating guidance: `loading` settings, explicit next-offset commit
  semantics, retry conditions, the persistent `var/rejections` bind mount for
  Docker loaders, and the explicit statement that the fsync JSONL rejection log
  is an interim Phase 2 sink scheduled for Phase 5 DLQ routing.
- Mark Story 4 complete in the Slice 2 task status only after the integration
  test is written, unit tests pass, and the persistent reviewer approves.

Container precondition: Docker must be available to run the marked integration
test. The test uses disposable Testcontainers Kafka and Neo4j instances and
cleans up every disposable Person, Company, and retry topic in `finally` blocks. Rollback is limited to
removing this test and documentation; Stories 1–3 remain unchanged.
