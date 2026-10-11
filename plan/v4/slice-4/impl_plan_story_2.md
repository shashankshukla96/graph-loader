# Implementation Plan — Phase 4 / Slice 4 / Story 2

## Scope

Harden the existing shared-clock fleet selector so it validates against Story
1's canonical isolation plan. This story preserves the public Slice 3 selector
as a compatibility wrapper and does not implement isolated execution phases.

## Files

- Modify `src/orchestrator/fleet_contract.py`.
- Modify `src/orchestrator/clock_runner.py`.
- Modify `src/orchestrator/docker_service.py`.
- Modify `src/loader/edge_loader.py`.
- Extend `tests/test_fleet_contract.py`, `tests/test_clock_runner.py`, and
  `tests/test_edge_loader.py`, `tests/test_docker_service.py`.

## API and logic

Add `select_shared_fleet_edges(edges: Sequence[EdgeConfig], fleet_types:
tuple[str, ...]) -> tuple[EdgeConfig, ...]`. It first calls
`build_relationship_isolation_plan(edges)`, then requires `fleet_types` to
equal that plan's full, lexical `shared_edge_types`, and returns exactly those
edge objects in contract order. It independently requires its tuple argument
to be lexical, unique, and nonblank, so direct unsorted/duplicate callers fail
at the API boundary. A missing/extra/self-reference type fails before resource
construction.

Retain `select_fleet_edges()` as a delegating compatibility alias. Migrate
`clock_runner` and slot-gated `edge_loader` validation to the explicit shared
selector. Their existing parser/clock error wrappers remain responsible for
stage/run/type attribution; no new broad catches are added. Direct
`GlobalBatchClock` self-reference validation is intentionally retained as a
defense-in-depth boundary.

In `DockerService.run_edge_loader(..., slot_gating=True)`, parse the canonical
fleet contract and load the exact mounted schema before `os.makedirs` or
`client.containers.run`. Pass its edges to `select_shared_fleet_edges`, then
require the requested `edge_type` to be selected. A forged
`edge_type=KNOWS, fleet_edge_types=KNOWS,WORKS_AT` therefore fails before
rejection-directory creation or container launch. Docker rejection messages
must include `stage=launch`, run ID, and edge type.

## Tests

- Mixed shared plus `KNOWS(Person, Person)` accepts only canonical shared types.
- A forged shared contract including `KNOWS`, omitting `WORKS_AT`, or adding an
  unknown type fails before clock producer or Docker work is touched.
- `clock_runner.main` and `edge_loader.main --slot-gating` reject a forged
  self-reference fleet before Kafka Consumer/Producer construction.
- `DockerService.run_edge_loader` rejects the forged edge-type/contract pair
  and a self-reference-inclusive contract before `os.makedirs` and
  `containers.run`, with run/type attribution.
- Direct selector calls with unsorted or duplicate tuples fail explicitly.
- Existing all-shared Slice 3 contracts retain their selected objects/order.

## Failure/rollback

All new failures are pre-launch/pre-poll `ValueError` paths. No offsets,
leases, containers, or schema state are modified; there is no cleanup action.
