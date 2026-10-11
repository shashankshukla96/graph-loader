# Story 6 implementation plan: deliberately contended modes

Two more test functions in `tests/stress/test_star_topology_stress.py`,
driving `src.loader.edge_execution` classes directly -- planned mode has no
lock waits to contend with by construction (Stories 4-5's point).

## Corrections vs. the story doc's sketch

- **Neo4j image version.** `neo4j:5.21-enterprise` (every other test in
  this file, and `tests/conftest.py`'s own fixture) cannot run
  `ApocLockedEdgeWriter`'s `explicit_locks=True` or `sort_rows=True` write
  path at all: `build_apoc_locked_upsert_query`'s `CALL (vars) { ... }`
  scoped-subquery syntax is a Cypher feature that landed after 5.21
  (empirically confirmed: a syntax error on 5.21.2, works on 5.26.31).
  `docker-compose.yaml`'s own production/dev stack already pins the newer
  floating `neo4j:5-enterprise` tag, so this is a pre-existing version-pin
  gap in the test fixtures, not something this slice introduces -- no
  pre-existing integration test happens to exercise this exact query path
  against a live instance. Every container in this file (including
  Stories 4-5's, unaffected either way) now uses `neo4j:5.26-enterprise`
  (a specific, reproducible minor pin) instead. Recorded as a residual
  risk in `plan/v6/completion_slice-4.md`; fixing `tests/conftest.py` or
  `src/` is out of scope for this tests-only slice.
- **`count_retry_log_lines(...) > 0` is the wrong assertion for shared-
  overlap.** The story doc's sketch expected a recovered-deadlock count
  greater than zero here, but `docs/USAGE.md`'s own "Why lock waits used
  to dominate" section states plainly: "Sorted explicit locks
  (`apoc.lock.nodes`) prevent deadlocks, but not waiting." A real run with
  genuine, measured contention (`total_lock_wait_ms > 0`) produced exactly
  0 retries, confirming the documented claim empirically rather than
  refuting it. The corrected assertion is `count_retry_log_lines(...) ==
  0` -- which *is* a meaningful regression check (it would fail if a
  future change broke the ordered-lock guarantee and reintroduced real
  deadlocks), not a doomed attempt to force a deadlock that the design
  provably prevents.
- **Shared-overlap needs APOC.** `ApocLockedEdgeWriter(explicit_locks=True)`
  (the default) calls `apoc.lock.nodes`; traced in Story 2. A **second**
  container fixture, `stress_neo4j_apoc` (query-logged + transaction-timeout
  + `lock_wait.with_apoc`), is added for this one test; the bulk-lease test
  reuses the plain `stress_neo4j` fixture (lease mode never calls APOC).
- **Bulk-lease mode's real call shape** (confirmed in Story 3's own
  tracing of `build_edge_execution`): it returns `(writer, None, None,
  coordinator)` where `writer` is only used *internally* by `coordinator`.
  Rows go in through `coordinator.execute_pending(pending)`, `pending` a
  sequence of `PendingEdgeRecord(EdgeRecord(source_key, target_key,
  properties), topic, partition, offset)` (from `src.loader.edge_loader`) --
  never `writer.write_rows(...)` directly for this mode.
- **Lease address is `host:port`**, no `tcp://` scheme (confirmed against
  `parse_lease_address` and `tests/test_bucket_lease_execution.py::
  test_executor_against_real_coordinator`'s exact pattern, reused here for
  starting `BucketLeaseServer` on its own event-loop thread).

## Test 1: classic shared-overlap, zero *unhandled* deadlocks (contention expected)

```python
@pytest.mark.integration
@pytest.mark.stress
def test_100k_edges_into_one_hub_shared_overlap_zero_unhandled_deadlocks(
    stress_neo4j_apoc, tmp_path, caplog,
):
    container, driver = stress_neo4j_apoc
    with lock_wait.hard_timeout(600):
        dataset = std.generate(HUB_EDGES_100K, "target_hub")
        schema = _load_schema("target_hub", uuid.uuid4().hex[:8], tmp_path)
        std.seed_nodes(driver, dataset, schema)
        mark = lock_wait.query_log_mark(container)
        edge_config = next(e for e in schema.edges if e.type == "POINTS_AT")
        writer = ApocLockedEdgeWriter(driver, edge_config,
                                       missing_endpoint_error_factory=MissingRelationshipEndpointError)
        rows = _classic_rows(dataset, "POINTS_AT")
        workers, batch_size = 16, 25   # tuned -- see Definition of Done note
        batches = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]
        with caplog.at_level(logging.WARNING, logger="src.loader.edge_execution"):
            with ThreadPoolExecutor(max_workers=workers) as pool:
                attempts = list(pool.map(writer.write_rows, batches))
        assert all(isinstance(a, int) for a in attempts)          # no unhandled deadlock reached the caller
        assert lock_wait.total_lock_wait_ms(container, since=mark) > 0   # contention was real
        assert lock_wait.count_retry_log_lines(caplog, edge_type="POINTS_AT") > 0  # a deadlock was recovered
        with driver.session() as session:
            count = session.run("MATCH ()-[r:POINTS_AT]->(:Hub) RETURN count(r) AS c").single()["c"]
        assert count == len(std.distinct_pairs(dataset, "POINTS_AT"))
```

No Kafka needed here at all -- rows are built in-memory from the dataset,
never published (this test drives the writer class directly, matching
Decision 1). `_classic_rows(dataset, edge_type) -> list[dict]` returns
`{"source_key": s, "target_key": t, "properties": props}` dicts, the exact
shape `ApocLockedEdgeWriter.write_rows` expects.

## Test 2: bulk lease mode, zero lock wait AND zero unhandled deadlocks

```python
@pytest.mark.integration
@pytest.mark.stress
def test_100k_edges_into_one_hub_bulk_lease_mode_zero_lock_waits(stress_neo4j, tmp_path):
    container, driver = stress_neo4j
    with lock_wait.hard_timeout(600):
        ... seed, mark = query_log_mark(container) ...
        loop, loop_thread = _event_loop_thread()          # same pattern as planned_harness
        server = BucketLeaseServer()
        try:
            host, port = asyncio.run_coroutine_threadsafe(server.start("127.0.0.1", 0), loop).result(10)
            writer, _, _, coordinator = build_edge_execution(
                edge_config, driver, environ={LEASE_ADDRESS_ENV: f"{host}:{port}", LEASE_BUCKETS_ENV: "16"},
            )
            pending = _pending_rows(dataset, "POINTS_AT", edge_config.topic)
            results = coordinator.execute_pending(pending)
            assert all(result.success for result in results)
            ... exact count assertion, same as test 1 ...
            lock_wait.assert_zero_lock_wait(container, since=mark, min_queries=1)
        finally:
            coordinator.close()
            asyncio.run_coroutine_threadsafe(server.close(), loop).result(10)
            loop.call_soon_threadsafe(loop.stop); loop_thread.join(5); loop.close()
```

`_pending_rows(dataset, edge_type, topic) -> list[PendingEdgeRecord]`
constructs `PendingEdgeRecord(EdgeRecord(source, target, properties), topic,
partition=0, offset=index)`.

"Neither test hangs if the lease server fails to start" (the story's own
AC) is inherited from `BucketLeaseClient`'s existing fail-closed
connect-deadline behaviour, already proven by
`tests/test_lease_server.py::test_unreachable_coordinator_fails_closed_after_connect_deadline`
-- not re-proven here; this test's own `hard_timeout` is the backstop if it
somehow still did.

## Definition of Done

Tuned `workers`/`batch_size` values recorded here once measured locally
(this First Mate's own run, per the story's DoD): see
`plan/v6/completion_slice-4.md`.
