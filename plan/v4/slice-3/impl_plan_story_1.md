# Implementation Plan — Phase 4 / Slice 3 / Story 1

## Scope

Create pure, deterministic conflict-family and rotating-ownership planning.
This story changes neither `ClockLease`, loader admission, Kafka/Docker
lifecycle, offset resolution, nor the Phase 4 Slice 4 self-reference policy.

## Files

- Modify `src/orchestrator/dependency_manager.py`.
- Create `src/orchestrator/rotation.py`.
- Modify `tests/test_dependency_manager.py`.
- Create `tests/test_rotation.py`.

## Public values and APIs

```python
@dataclass(frozen=True)
class SharedLabelResource:
    label: str
    bucket: int

@dataclass(frozen=True)
class ConflictFamily:
    edge_types: tuple[str, ...]
    shared_labels: tuple[str, ...]
    edge_endpoint_labels: Mapping[str, frozenset[str]]
    edge_shared_labels: Mapping[str, frozenset[str]]

def build_conflict_families(
    edges: Sequence[EdgeConfig],
) -> tuple[ConflictFamily, ...]: ...

@dataclass(frozen=True)
class RotationPlan:
    families: tuple[ConflictFamily, ...]
    bucket_count: int

    def owners_for_epoch(self, epoch: int) -> Mapping[str, Mapping[int, str]]: ...
    def owner_for(self, resource: SharedLabelResource, epoch: int) -> str: ...
    def max_wait_epochs(self, edge_type: str, label: str) -> int: ...

def build_rotation_plan(
    families: Sequence[ConflictFamily], *, bucket_count: int,
) -> RotationPlan: ...
```

All public mappings are copied into `MappingProxyType` values and all tuples
are canonical lexical order. They contain type/label/bucket metadata only;
they never retain Kafka values or record payloads.

## Family algorithm

1. Call `build_relationship_conflict_plan(edges)` to preserve the Phase 3
   duplicate-type guard and exact label-conflict definition.
2. Find connected components over its immutable conflict adjacency in lexical
   order. Every eligible non-self-referencing edge appears once; an isolated
   eligible edge is a singleton family.
3. Reject any self-referencing edge at this shared-rotation boundary. This is
   classification and rejection only—Slice 4 remains responsible for its
   isolated execution policy.
4. In each component, preserve every edge's complete endpoint label resource
   set as `edge_endpoint_labels[edge_type]`, then calculate labels used by at
   least two component members. Those are `shared_labels`. For each edge,
   retain only its endpoint labels in that shared set as
   `edge_shared_labels[edge_type]`. A singleton therefore has no shared labels
   and does not need a rotating resource lease.
5. Validate coverage, lexicographic ordering, nonblank identifiers, and that
   every shared-label map is a subset of the immutable endpoint-label map.
   Reject an endpoint set with one label at this boundary, which unambiguously
   identifies a source/target-equal self-reference without changing Slice 4
   execution policy.

## Rotation algorithm and fairness invariant

`build_rotation_plan()` validates a non-bool `bucket_count` in `1..4096`,
unique edge types across families, unique nonblank labels, sorted canonical
families, no self-references (using `edge_endpoint_labels`), and no partially
described shared resources. A manually constructed family with a singleton
endpoint set is rejected before schedule emission; it cannot masquerade as a
normal shared-label edge.

For a non-singleton family of sorted types `T`, epoch `e` selects
`T[e % len(T)]` as the owner for **every bucket of every label shared by that
family**. Thus a member that needs more than one shared endpoint label always
owns all of them in its selected epoch: partial label ownership is impossible.
`owners_for_epoch(e)` emits a label-scoped map containing every `(label,
bucket)` resource exactly once. It rejects duplicate labels across families;
such input is unsafe because one label resource cannot be assigned by two
families. Singleton/no-shared-label families contribute no resource map.

For any member and any label it actually uses, selection recurs every
`len(T)` epochs, so `max_wait_epochs` is that family size. Ownership is
deterministic under shuffled edge/family input, and no two edge types can own a
shared `(label, bucket)` in one epoch.

## Failure handling

Raise `ValueError` with the family/edge/label/bucket/epoch context available
for duplicate types, self-reference inclusion, invalid bucket/epoch, duplicate
label ownership, incomplete family metadata, or a request for an edge/label
outside the plan. This story performs no I/O and has no Docker/Kafka cleanup.

## Tests

- `tests/test_dependency_manager.py`: `WORKS_AT(Person, Company)` and
  `BOUGHT(Person, Product)` form one family with only `Person` shared;
  isolated types form a deterministic singleton; transitive and multi-label
  components remain deterministic under shuffled input; self-references fail.
- `tests/test_rotation.py`: simulate multiple epochs and all buckets for the
  shared-Person pair; prove a single owner per `(Person, bucket)`, exact
  alternating ownership, bounded wait of two epochs, and deterministic replay.
- Cover multi-label families, ensuring one selected owner receives all needed
  labels together; assert no partial admission schedule is emitted. Construct a
  malformed family containing an endpoint-set singleton and prove rotation
  rejects it before emitting any owner map.
- Cover invalid buckets/epochs, duplicate family members/labels, malformed
  mappings, cross-family resource collision, unknown resource lookup, and
  immutable values.

Run:

```bash
.venv/bin/python -m pytest tests/test_dependency_manager.py tests/test_rotation.py -q --tb=short
```

## Rollback / safety

The patch is pure planning only. It preserves Phase 3 staging code for later
replacement by Slice 3 and makes no changes to Kafka commits, Neo4j writes,
Docker containers, control messages, or self-reference execution.
