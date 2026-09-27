# Plan — Slice 3 Story 4: Lane Execution Coordinator

Add `build_edge_execution(edge_config, driver, *, executor_factory=...)` in
`edge_execution.py`; it calls `probe_server_capabilities(driver)` and
`require_execution_mode` before `EdgeLoader` construction/subscription and returns
writer/partitioner/batcher/coordinator. Extend `EdgeLoader.__init__` with those
injected components. Add immutable `LaneExecutionResult(lane_id: int | None,
routed_records, success, exception: Exception | None)` and
`LaneExecutionCoordinator.execute(lane_batches) -> tuple[LaneExecutionResult,...]`.
Python/APOC uses one task per lane (ascending lane
submission, `ThreadPoolExecutor(max_workers=worker_count)`); batches are FIFO
sequential within each task, never concurrent in one lane; it waits/shuts down
every future and converts exceptions to result diagnostics. Native flattens all
records into one call, no executor, and returns one `lane_id=None` result; any
failure marks every input unresolved. `_flush_batch` routes every PendingEdgeRecord,
uses `LaneBatcher.drain_all()`, maps only successful result records into existing
PartitionLedger resolution, then calls existing contiguous commit; failure returns
nonzero. Tests in edge_loader/execution/mix files cover probe-before-subscribe,
prefix-safe commits, native failure no offsets, FIFO, bounded injected executor,
and rebalance safety. The initial integration deliberately uses conservative
all-or-nothing resolution: if any lane fails, `_flush_batch` resolves no lane
offsets and returns failure; idempotent writes make replay safe.
