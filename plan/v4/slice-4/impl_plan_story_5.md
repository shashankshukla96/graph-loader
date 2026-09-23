# Implementation Plan — Phase 4 / Slice 4 / Story 5

## Scope

Add a real, opt-in Docker/Kafka/Neo4j acceptance test for a finite mixed
relationship run containing `WORKS_AT(Person,Company)` and
`KNOWS(Person,Person)`.  The test will exercise the public phase orchestration
against UUID-scoped inputs and prove graph state, consumer offsets, phase
ordering, and only its precomputed container cleanup.  It will not add a DLQ,
retry policy, or new Cypher.

## Files

- Create `tests/test_self_reference_isolation_e2e.py`.
- No production-code change is planned.  README policy documentation was
  completed in Story 4.

## Test lifecycle

The test is marked `integration` and skipped solely when
`RUN_SELF_REFERENCE_ISOLATION_E2E != "1"`.  Once opted in it must actively
require Docker connectivity, `graph-loader-net`, Kafka admin connectivity,
Neo4j connectivity, and exact edge/clock images; a missing prerequisite calls
`pytest.fail`, never a skip.

It will:

1. allocate UUID-scoped node/work/knows/clock topics and write one schema under
   `var/self-reference-e2e/`; use a UUID parent run ID and derive the exact
   shared and isolated child IDs with the production helper, then precompute
   the exact node, shared-edge, shared-clock, and isolated-KNOWS container
   names from those IDs;
2. produce finite node, `WORKS_AT`, and directional `KNOWS` records, then call
   the public `handle_start`/CLI bulk path with `skip_image_build=True`, exact
   network, timeout, and explicit parent `--run-id`.  This covers the actual
   node-drain-to-relationship handoff rather than calling an underscored helper;
3. use Docker's event stream filtered only by the precomputed exact names (or
   exact `get(name)` polling with a recorded ordered lifecycle) to prove the
   shared edge/clock removal occurs before isolated `KNOWS` creation. Assert no
   clock name exists for the isolated phase; no broad Docker list/stop is used;
4. query Neo4j for the expected `WORKS_AT` and `KNOWS` counts/no duplicate
   relationships, compare each edge consumer-group committed offset to its
   finite topic watermark, and assert every exact child container name is gone;
5. in `finally`, stop/remove only the precomputed exact node/shared/isolated
   names, delete only created topics/tagged nodes/config, and close every
   external client.

The finite edge loader has no node-loading dependency here: endpoint nodes are
precreated directly and fixture topics remain isolated.  Existing Phase 3
directional lanes/APOC locks are used untouched by the regular edge image.

## Verification

- Focused non-Docker collection/run proves the test is skipped unless its
  explicit environment gate is set.
- If the environment gate and required services are available, run the actual
  E2E and record its result.  A skipped unavailable environment is reported as
  unavailable, not as E2E success.
- Then run the complete required non-Docker suite and `git diff --check` for
  final reviewer integration review.
