# Correction Plan — Phase 4 / Slice 3 / Story 5 E2E Blocker

## Problem

The opt-in rotating relationship Docker E2E reaches Neo4j 5.26.30, where
`CYPHER 25 RETURN 1` returns
`Neo.ClientError.Statement.ArgumentError`.  `probe_server_capabilities()`
only treats `CypherSyntaxError` as an unsupported capability, so the
default `python_apoc` loader aborts during construction before it can poll
Kafka or accept a rotation lease.

## Scope

- Modify `src/loader/edge_execution.py`.
- Modify `src/loader/edge_loader.py`.
- Modify `src/orchestrator/relationship_bulk_monitor.py`.
- Modify `tests/test_edge_execution.py`.
- Modify `tests/test_edge_execution_writer.py` to prove the default APOC build
  does not issue a Cypher-25 capability query and its query binds collected
  endpoint nodes before `MERGE`.
- Modify `tests/test_edge_loader.py` for lane-cause logging and no commit on a
  failed writer batch.
- Modify `tests/test_relationship_bulk_monitor.py` for bounded control and
  coordination backlog draining.

No Kafka commit, lease admission, Docker cleanup, rotation schedule, retry,
or self-reference behavior changes.

## Implementation

1. In `build_edge_execution(edge_config, driver, ...)`, construct
   `ApocLockedEdgeWriter`, partitioner, batcher, and Python lane coordinator
   directly when `edge_config.execution.mode == "python_apoc"`.  Do not call
   `probe_server_capabilities()` for that default, Neo4j-5-compatible mode.
2. Retain the probe and `require_execution_mode()` only for
   `native_disjoint`, because Cypher 25 is an explicit prerequisite of that
   strategy.  The native path remains fail-closed before Kafka subscription.
3. Make `probe_server_capabilities()` recognize only Cypher-version-rejection
   `ClientError`s (including Neo4j 5.26's
   `Neo.ClientError.Statement.ArgumentError`) as `cypher_25=False`; propagate
   authentication, authorization, connectivity, and unrelated client errors.
   Classification will use the Neo4j error code and the Cypher-25 context,
   never a blanket `ClientError` catch.

## Tests

- A default `python_apoc` execution build makes no server-info or Cypher-25
  probe call.
- A native build maps the exact Neo4j 5.26 `Statement.ArgumentError` response
  to unsupported and then raises the existing `ExecutionModeError` before
  constructing the native writer.
- An unrelated `ClientError` still propagates.
- Run focused edge-execution tests, then the opt-in isolated rotation E2E.

## E2E timing correction

The E2E's clock must rotate quickly enough to observe both owners while also
leaving a lease live through Docker process/Kafka group startup and one durable
Neo4j transaction. Use a 10,000 ms slot and a 30,000 ms lease timeout in only
the isolated fixture. This preserves two observable rotations within the
existing bounded 90-second E2E deadline and preserves strict post-write lease
revalidation; it does not weaken production protocol or monitor behavior.

## APOC writer correction

The real Neo4j transaction exposed a Phase 3 query defect independent of the
rotation protocol: Cypher rejects `MERGE (rel.s)-[...]` because a dotted map
access cannot appear as a node variable in a graph pattern.  After locking,
the query must use `WITH rel.s AS s, rel.t AS t, rel.props AS props`, then
execute `MERGE (s)-[r:<type>]->(t) SET r += props`.  Preserve the existing
preflight, sorted lock acquisition, atomic transaction, and record-free error
handling.  Add an assertion for the binding and valid `MERGE` form.

## Attributed lane-failure diagnostic

`LaneExecutionCoordinator` currently converts a writer exception into an
unattributed `RuntimeError("lane execution failed")`, and the loader logs only
that wrapper.  Preserve the public worker failure message and durable
fail-closed behavior, but chain the first failed lane's original exception as
the wrapper cause and log the attributed worker failure with exception info.
No record payloads are logged.  Add a focused assertion that a failed lane
retains its original cause for attributed operator diagnostics.  In
`EdgeLoader._worker_loop`, select the first failed `LaneExecutionResult` in
the coordinator's deterministic return order and raise the generic worker
wrapper from its exception.  The outer `RuntimeError` handler uses
`exc_info=True`, inside that handler, to emit the chain without interpolating
the `WorkerBatch` or any record.  The focused test asserts the failure keeps
all offsets uncommitted.

## Run-scoped control backlog correction

The Docker E2E exposed that each unique monitor group starts the shared Phase
3 control topic at `earliest`, while `_poll()` decodes only one record per
250 ms iteration. Historical foreign-run acknowledgements can therefore keep
the current run's valid `ASSIGNMENT` events behind the 90-second deadline.

- Modify `src/orchestrator/relationship_bulk_monitor.py` so each monitor poll
  performs its existing bounded wait for the first control record, then drains
  a bounded number of immediately available control records with `poll(0)`.
  Decode every record through the existing strict run/replica/epoch logic.
- Bound each drain (1,000 records) so health checks, deadlines, coordination
  polling, and shutdown cannot be starved by an unbounded topic.
- Apply the same helper shape to coordination without weakening malformed,
  stale, duplicate, expired, foreign, or conflicting lease rejection.
- Extend `tests/test_relationship_bulk_monitor.py` with a backlog containing
  many foreign control records followed by the current run's two assignment
  acknowledgements; one `_poll()` cycle must establish coverage. Add a bound
  test proving the helper performs one timed initial poll plus at most 1,000
  additional `poll(0)` calls per consumer (at most 1,001 decoded records) in
  one cycle, then returns so the next health/deadline check can run.

No producer, Kafka commit, work offset, lease ownership, Docker lifecycle, or
topic-retention semantics change.

## Failure handling and cleanup

The change occurs before consumer subscription or writer execution; an
unsupported native server still has no Kafka side effects.  The E2E continues
to use only exact run-scoped containers, topics, nodes, and config cleanup.
