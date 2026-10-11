# Plan — Slice 3 Story 3: Native DISJOINT BY Writer

Add `NativeDisjointEdgeWriter(driver, edge_config, capabilities)` to
`edge_execution.py`. Constructor calls `require_execution_mode` before any
driver write. `write_batch` first uses the Slice 1 per-row endpoint preflight
through implicit `session.run` and consumes/validates all rows; a miss
means no native query or durable-success signal. It then uses a separate implicit
`session.run` (never an explicit transaction) with parameterized Cypher 25:
`CYPHER 25 UNWIND $rows AS row CALL (row) { MATCH endpoints; MERGE relationship;
SET properties } IN $concurrency CONCURRENT TRANSACTIONS OF $batch_size ROWS
DISJOINT BY (row.source_key,row.target_key) ON ERROR CONTINUE REPORT STATUS AS
status RETURN status`.
Parameters use `execution.worker_count` and `mix_and_batch.batch_size`.
Consume every returned status and raise when `status.committed is not True` or
`status.errorMessage` is nonblank.
Concurrent server batches can partially commit before another fails; raise and
leave offsets uncommitted, relying on idempotent MERGE for safe replay. Tests
cover preflight miss/no native query, exact query/parameters, successful statuses,
failed/noncommitted-null-error status, runtime error, exact ON ERROR CONTINUE
placement, two types, session closing, and refusal before
`session()`. No Docker image upgrade: 5.21 remains rejected.
