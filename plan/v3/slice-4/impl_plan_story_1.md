# Implementation Plan — Phase 3 / Slice 4 / Story 1

## Story

**Build a Deterministic Relationship Conflict Plan**

## Scope and non-goals

This story adds only a pure planner that maps schema edge declarations to a
conflict graph and deterministic bulk stages.  It does not launch containers,
consume Kafka, write Neo4j, alter CLI behavior, or implement lifecycle control
messages.  Those operations remain in Stories 2–4.

## Files

| Action | Path | Purpose |
| --- | --- | --- |
| Create | `src/orchestrator/dependency_manager.py` | Immutable plan/value types and pure conflict/stage builder. |
| Create | `tests/test_dependency_manager.py` | Unit coverage for graph and stage safety/determinism. |

## Public API

```python
@dataclass(frozen=True)
class RelationshipStage:
    """One deterministic, pairwise endpoint-label-disjoint bulk stage."""
    index: int
    edge_types: tuple[str, ...]

@dataclass(frozen=True)
class RelationshipConflictPlan:
    """Conflict adjacency and ordered safe stages for declared edge types."""
    conflicts: MappingProxyType
    stages: tuple[RelationshipStage, ...]

def build_relationship_conflict_plan(
    edges: Sequence[EdgeConfig],
) -> RelationshipConflictPlan:
    """Return a deterministic graph and greedy label-disjoint stages."""
```

## Algorithm

1. Materialize `edges` once and collect type names.  Raise `ValueError` for a
   duplicate so callers that bypass `GraphSchema` remain safe.
2. For each edge, derive immutable endpoint labels
   `frozenset((edge.nodes.source, edge.nodes.target))`.  This naturally makes
   a self-referencing edge a single-label resource.
3. Build a complete adjacency mapping over all types.  For every sorted pair
   of distinct types, add the pair symmetrically when their label sets
   intersect.
4. Assign types in sorted lexical order.  Place each type in the first stage
   whose accumulated label set is disjoint; otherwise create a new stage.
   This stable first-fit rule makes the plan reproducible while maximizing
   safe co-scheduling under the documented deterministic policy.
5. Freeze adjacency values with `frozenset` and wrap a copied outer mapping
   in `types.MappingProxyType`; stage type lists are tuples.  `RelationshipStage`
   indexes are contiguous and zero-based.  Do not expose mutable internal
   state.

## Tests

- Empty input creates no vertices/stages.
- Two label-disjoint types occupy stage 0 together.
- `WORKS_AT(Person, Company)` and `BOUGHT(Person, Product)` conflict and
  appear in distinct stages.
- A chain (`A-B`, `B-C`, `C-D`) yields only pairwise-safe stages.
- A self-reference shares no stage with a type using its label but can share
  with one using unrelated labels.
- Shuffling the same edge declarations produces identical conflicts and
  stages.
- Every co-staged pair has disjoint endpoint labels, and duplicate types are
  rejected clearly.
- A caller cannot mutate the outer conflict adjacency mapping, its neighbour
  sets, or a stage's tuple membership.

Run:

```bash
python -m pytest tests/test_dependency_manager.py -v
```

## Rollback / cleanup

The implementation is side-effect-free and creates no external data.  If the
stage policy proves unsuitable, callers have not yet been wired to it; revert
only this module and its test file.
