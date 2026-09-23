# Implementation Plan — Phase 4 / Slice 4 / Story 1

## Scope

Implement the pure, deterministic self-reference isolation planner. This story
does not launch loaders, publish Kafka messages, create Neo4j sessions, or
alter Docker lifecycle behavior.

## Files

- Create `src/orchestrator/isolation.py`.
- Create `tests/test_isolation.py`.

## API and invariants

Create frozen `IsolatedRelationshipPhase(index: int, edge_type: str, label:
str)` and `RelationshipIsolationPlan(shared_edge_types: tuple[str, ...],
isolated_phases: tuple[IsolatedRelationshipPhase, ...])`, each validating
non-boolean nonnegative indices, nonblank names, lexical canonical ordering,
and no duplicate/overlapping types. Each isolated phase has a nonblank label.

`build_relationship_isolation_plan(edges: Sequence[EdgeConfig]) ->
RelationshipIsolationPlan` will reject duplicate type names, sort by type, and
classify only through `edge.nodes.is_self_referencing`. A self-reference adds
one phase with its equal source/target label; all other types become the
canonical shared tuple. The planner verifies every input type appears once and
does not mutate inputs.

## Tests

- Empty, shared-only, isolated-only, and mixed schemas.
- `WORKS_AT(Person, Company)`, `BOUGHT(Person, Product)`, and
  `KNOWS(Person, Person)` classify deterministically.
- Multiple self-reference labels and shuffled input retain lexical phase order.
- Duplicate edge types and forged noncanonical/overlapping immutable plans
  fail clearly.
- Planner tests use only simple schema fixtures and prove no runtime I/O.

## Failure/rollback

All errors are `ValueError` before runtime resources exist; there is no Docker,
Kafka, Neo4j, offset, or cleanup action in this story.
