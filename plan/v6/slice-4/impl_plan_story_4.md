# Story 4 implementation plan: 100k target-hub planned-mode stress test

New file: `tests/stress/test_star_topology_stress.py` (this story starts it;
Stories 5-7 extend the same file with more test functions).

## Corrections vs. the story doc's sketch

- **No `pytest-timeout` dependency.** Checked `requirements.txt` and
  `requirements-dev.txt`: neither lists it, and this slice must never edit
  either file (orchestrator file-ownership rule). Using
  `@pytest.mark.timeout(600)` would silently no-op (unknown marker) without
  the plugin installed -- worse than no ceiling, because it *looks* like a
  ceiling exists. Implemented instead with the stdlib `faulthandler` module:
  `lock_wait.hard_timeout(seconds)` (new helper, added to `lock_wait.py`
  since every stress test in this file needs the same ceiling) calls
  `faulthandler.dump_traceback_later(seconds, exit=True)` on entry and
  `cancel_dump_traceback_later()` on exit -- a genuine hang dumps every
  thread's traceback and hard-exits the test process; a slow-but-completing
  run is unaffected. Flagged in `plan/v6/completion_slice-4.md` as a
  candidate dependency for a future slice, per this task's own instruction
  to list a needed test-only dependency there rather than edit
  `requirements-dev.txt` directly.
- **Own container, not the shared `neo4j_driver` fixture** (Story 2's own
  note, reused here): every test in this file builds its own
  function-scoped `Neo4jContainer` via a local `stress_neo4j` fixture
  (`lock_wait.with_verbose_query_log` applied before `.start()`; no APOC --
  planned mode's writer (`explicit_locks=False, sort_rows=False`) never
  calls an APOC procedure, confirmed in Story 2/3's own tracing). The
  session-scoped `kafka_bootstrap` fixture from `tests/conftest.py` is
  reused as-is (Kafka needs no special per-test config here).
- **Measurement window excludes node seeding.** `lock_wait.query_log_mark
  (container)` is captured right after `std.seed_nodes(...)` (Story 2's
  Required Fix 3: a shared container's earlier phase must not be able to
  make a later "zero lock wait" claim vacuous by accident); every
  `assert_zero_lock_wait`/`total_lock_wait_ms` call in this file passes
  `since=mark`.
- **Hub resource check, precisely.** `EdgePlan.resources` has no
  "is this key a hub" boolean query beyond `ResourceTable.hub_resource(label,
  key)` returning `int | None` -- used directly:
  `plan.resources.hub_resource(HUB_LABEL, HUB_KEY) is not None`, in addition
  to `plan.resources.hub_count >= 1` (the AC's own two-part check: a hub
  resource exists at all, AND this specific key is the one that got it, not
  some other accidental threshold crossing).

## Test

```python
GRAPH_LOADER_STRESS_SCALE = int(os.environ.get("GRAPH_LOADER_STRESS_SCALE", "100000"))

@pytest.mark.integration
@pytest.mark.stress
def test_100k_edges_into_one_hub_planned_mode_zero_lock_waits(stress_neo4j, kafka_bootstrap, tmp_path):
    container, driver = stress_neo4j
    with lock_wait.hard_timeout(600):
        run_id = uuid.uuid4().hex[:8]
        schema = _load_schema("target_hub", run_id, tmp_path)
        dataset = std.generate(GRAPH_LOADER_STRESS_SCALE, "target_hub")
        std.seed_nodes(driver, dataset, schema)
        mark = lock_wait.query_log_mark(container)
        try:
            std.publish(dataset, kafka_bootstrap, schema)
            result = planned_harness.run_planned_mode(
                schema, driver, kafka_bootstrap, plan_dir=tmp_path, writers=4, plan_hub_degree=1000,
            )
            assert result.plan.resources.hub_count >= 1
            assert result.plan.resources.hub_resource(std.HUB_LABEL, std.HUB_KEY) is not None
            with driver.session() as session:
                count = session.run("MATCH ()-[r:POINTS_AT]->(:Hub) RETURN count(r) AS c").single()["c"]
            assert count == len(std.distinct_pairs(dataset, "POINTS_AT"))
            lock_wait.assert_zero_lock_wait(container, since=mark)
            assert result.total_attempts_over_one == 0
        finally:
            std.delete_topics(kafka_bootstrap, std.edge_topic_names(schema))
```

`plan_hub_degree=1000` forces hub detection deterministically (well under
100,000, matching the story's own AC rationale), rather than trusting
`auto_settings.py`'s derived default at this exact record count.

## Determinism / repeatability

`std.generate(..., seed=13)` (the module default) is fixed, so re-running
this test twice produces the identical dataset and (given planned mode's
structural zero-lock-wait guarantee) the identical pass/fail outcome --
satisfying the AC without a literal "run twice in one pytest session"
loop (each run needs its own fresh containers/topics anyway, which is what
a real second CI run gives it).

## Wall-clock measurement (First Mate's own local run, recorded per Story 4's DoD)

Measured locally before fixing Story 8's CI timeout; see
`plan/v6/completion_slice-4.md` for the actual number and host.
