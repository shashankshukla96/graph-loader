# Story 7 implementation plan: 1M-edge nightly/manual tier

One more test function in `tests/stress/test_star_topology_stress.py`,
reusing every Story 4-6 helper with `topology="target_hub"` at 1,000,000
rows, gated by a runtime `pytest.skip` (Decision 2 in `stories.md`): a
dedicated marker would need every CI invocation to remember to *exclude*
it; a runtime env-var check is opt-in by construction.

```python
@pytest.mark.integration
@pytest.mark.stress
def test_1m_edges_into_one_hub_planned_mode_zero_lock_waits(stress_neo4j, kafka_bootstrap, tmp_path):
    if os.environ.get("GRAPH_LOADER_STRESS_SCALE") != "1000000":
        pytest.skip(
            "1M-edge tier only runs when GRAPH_LOADER_STRESS_SCALE=1000000 is set "
            "(the nightly/manual star-topology-stress-1m CI job, or an explicit local "
            "override) -- never as part of an ordinary `pytest -m integration` run, per "
            "P6-Q3's tiering decision."
        )
    ... # identical body to the 100k target_hub test, generate(1_000_000, "target_hub")
```

`GRAPH_LOADER_STRESS_SCALE` is read **only** inside this one test function,
never at module scope (Story 4's own fix: reading it at module scope would
make every 100k test in this file silently balloon to 1M rows the moment
Story 8's dedicated 1M job sets it and anyone also selects `-m
integration`).

## Resource/timing notes at this scale

- Kafka: 1M `POINTS_AT` records (~60-90 bytes JSON each) is trivial disk;
  `KafkaContainer` defaults are left alone.
- Neo4j: 1M relationships + ~1,000,001 nodes is small by this library's own
  numbers (`docs/USAGE.md` Section 9 measures 17.8M relationships
  comfortably); the constraint is CI **time**, not memory/disk.
- Every scratch path (schema YAML, plan file, ledger) already goes under
  pytest's `tmp_path` fixture -- no literal `/tmp/...` anywhere in this
  file.
- No `cpuset` pinning attempted inside this test (Story 7's own note:
  `testcontainers`' `Neo4jContainer` has no `cpuset` passthrough, and a
  CI runner's 2-4 shared vCPUs have no stable core-affinity guarantee
  worth chasing).
- `lock_wait.hard_timeout(...)` and `run_planned_mode(..., timeout_s=...)`
  are both sized generously above the **measured** wall-clock below, with
  headroom, not guessed.

## Measured wall-clock (First Mate's own local run, before sizing Story 8's job timeout)

Recorded here once measured: see `plan/v6/completion_slice-4.md` for the
actual number, host, and the resulting `timeout-minutes` derivation for
Story 8's `star-topology-stress-1m` job.
