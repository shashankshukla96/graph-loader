# Story 9 implementation plan: relationship correctness regression suite

New files: `tests/integration/__init__.py`,
`tests/integration/test_relationship_correctness_regressions.py`.

## Corrections vs. the story doc's sketch

- **Neo4j image.** Same finding as Story 6: `build_edge_execution`'s
  default (classic) path constructs `ApocLockedEdgeWriter` with
  `explicit_locks` defaulting to `True` (env `GRAPH_LOADER_EDGE_EXPLICIT_
  LOCKS` unset), and its bulk-lease path builds one with `sort_rows=True`
  -- both hit `build_apoc_locked_upsert_query`'s `CALL (vars) { ... }`
  branch, which is a syntax error on `neo4j:5.21-enterprise`. Only planned
  mode (`explicit_locks=False, sort_rows=False`) avoids it. This file uses
  one dedicated `neo4j:5.26-enterprise` + APOC container for all three
  modes (matching Story 6's fixture), not the shared `neo4j_driver`
  fixture.
- **Dataset scale.** `examples.social.social_graph.generate(persons=200)`
  (not the default 20,000) -- small enough that planned, classic and
  lease modes each run in a small multiple of seconds.
- **Topics are static, not run-scoped.** `social_graph.EDGE_TOPICS`/
  `NODE_TOPICS` are fixed strings (unlike `star_topology_data.schema_dict`'s
  `run_id`-parameterized ones) -- this file publishes to Kafka at most
  once (for planned mode's `KafkaEdgePlanner` read) and calls
  `social_graph.delete_topics(kafka_bootstrap)` in a `finally`, matching
  the module's own existing `reset` CLI subcommand's cleanup. Classic and
  lease modes never touch Kafka at all (Decision 1: driven from the
  in-memory dataset directly).
- **Node seeding bypasses `NodeLoader`/Kafka**, same as the star-topology
  tests (node-loading correctness is `test_node_loader_integration.py`'s
  job): one `UNWIND ... MERGE` per label, straight from
  `dataset.nodes[label]`, run before each mode (each mode's own `DETACH
  DELETE` + reseed, since all three write into the SAME graph sequentially
  and must not see each other's relationships when computing their own
  `read_graph`).

## Test file structure

```python
@pytest.fixture(scope="module")
def social_container():
    # neo4j:5.26-enterprise + APOC + verbose query log, shared by every
    # test in this module (module-scoped: three sequential loads, not
    # three containers) -- reset via DETACH DELETE between modes, not a
    # fresh container each time (this file is a correctness suite, not a
    # lock-wait stress suite; module-scoped keeps its own runtime low).

@pytest.fixture(scope="module")
def social_dataset():
    return social_graph.generate(persons=200)

def _reset_and_seed(driver, dataset):
    with driver.session() as s: s.run("MATCH (n) DETACH DELETE n").consume()
    # apply_schema(driver, load_schema(examples/social/graph_schema.yaml))
    # then one UNWIND MERGE per label

def test_last_write_wins_evidence_is_real(social_dataset):
    evidence = social_graph.last_write_evidence(social_dataset)
    assert evidence["WORKS_AT"]["pairs_overwritten"] > 0
    assert evidence["FRIEND_OF"]["pairs_overwritten"] > 0

@pytest.mark.integration
def test_planned_classic_and_lease_modes_all_match_the_expected_graph(
    social_container, social_dataset, kafka_bootstrap, tmp_path,
):
    ...
```

- **Planned:** `_reset_and_seed`; `social_graph.publish(dataset,
  kafka_bootstrap)`; `planned_harness.run_planned_mode(schema, driver,
  kafka_bootstrap, plan_dir=tmp_path, writers=2)`; `read_graph` +
  `compare` against `expected_graph(dataset)` -> `planned_graph`.
- **Classic (label-serialized-equivalent):** `_reset_and_seed` again; for
  each edge type in the schema, `build_edge_execution(edge_config,
  driver)` (default env: classic mode, `explicit_locks=True`) then one
  `writer.write_batch([EdgeRecord(...) for source, target, props in
  dataset.edges[edge_type]])` call -- sequential across types, matching
  label-serialized mode's defining property for free; `read_graph` +
  `compare` -> `classic_graph`.
- **Bulk lease:** `_reset_and_seed` again; one in-process
  `BucketLeaseServer` (Story 6's pattern); for each edge type,
  `build_edge_execution(edge_config, driver, environ={LEASE_ADDRESS_ENV:
  address})` and `coordinator.execute_pending([PendingEdgeRecord(...)
  ...])`; `read_graph` + `compare` -> `lease_graph`.
- **Cross-mode check:** `compare((planned_graph[0], planned_graph[1]),
  (classic_graph[0], classic_graph[1], {}))  == []` and the same for
  lease, mirroring `test_social_planned_integration.py`'s own
  `compare(planned_graph[:2], classic_graph)` idea.

## Missing-endpoint fail-closed test

`tests/integration/test_relationship_correctness_regressions.py`'s own
`test_missing_endpoint_fails_closed_in_every_mode`: constructs one `VISITED`
row whose `person_id` does not exist, for each mode:

- **Classic:** `writer.write_batch([bad_record])` (alongside a few good
  ones in the same batch) raises `MissingRelationshipEndpointError`; a
  before/after count query proves nothing from that batch committed.
- **Bulk lease:** same via `coordinator.execute_pending([...])` --
  `LaneExecutionResult.success is False` and `.exception` is the same
  error type; count unchanged.
- **Planned:** publish a tiny separate schema/dataset (its own `run_id`,
  reusing `tests.stress.star_topology_data`-style scoping is overkill
  here; a small ad hoc schema copy suffices) containing one bad row;
  `planned_harness.run_planned_mode(...)` raises `PlannedHarnessError`
  (the worker's `write_rows` raises, `worker.run()` catches and returns
  1); count unchanged.

All three assert the *default* config (no `dead_letter` opt-in on the
`VISITED` edge) -- this is Phase 5 Slice 3's opt-in, not merged, and
today's uniform fail-closed default, matching `plan/v5/slices.md`'s owner
decision extended to classic/lease bulk modes too.
