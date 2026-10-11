# Story 5 implementation plan: reverse fan-out and multi-type hub

Extends `tests/stress/test_star_topology_stress.py` (same fixtures, same
`lock_wait`/`planned_harness`/`star_topology_data` helpers as Story 4) with
two more test functions.

## Correction vs. the story doc's sketch

`PlannedChunk` (`src/loader/edge_plan.py`) has no `source_resource`/
`target_resource` field -- only `resources: tuple[int, ...]` (1 or 2 sorted,
deduplicated resource ids; confirmed reading the dataclass directly). To
prove the hub's resource is used as the chunk's *source* (not just "is a
member of `chunk.resources`"), match each resource id's label via
`plan.resources.label_of(resource)` against `plan.labels_of(chunk)`'s
`(source_label, target_label)` pair -- since `source_hub`'s two endpoint
labels (`Hub`, `Leaf`) always differ, the resource whose label equals the
edge type's source label is unambiguously the source resource:

```python
def _source_resource(plan, chunk) -> int:
    source_label, _target_label = plan.labels_of(chunk)
    (resource,) = (r for r in chunk.resources if plan.resources.label_of(r) == source_label) \
        if len({plan.resources.label_of(r) for r in chunk.resources}) == len(chunk.resources) \
        else _self_referencing_source(plan, chunk)  # not applicable here: source != target label
    return resource
```

(In practice, for this slice's two-distinct-label topologies, this
simplifies to: `next(r for r in chunk.resources if plan.resources.label_of(r) == source_label)`.)

## Tests

```python
@pytest.mark.integration
@pytest.mark.stress
def test_100k_edges_from_one_hub_planned_mode_zero_lock_waits(stress_neo4j, kafka_bootstrap, tmp_path):
    """topology="source_hub": one Hub node is every record's SOURCE."""
    ...
    assert result.plan.resources.hub_count >= 1
    hub_resource = result.plan.resources.hub_resource(std.HUB_LABEL, std.HUB_KEY)
    assert hub_resource is not None
    points_from_chunks = [c for c in result.plan.chunks if c.edge_type == "POINTS_FROM"]
    assert points_from_chunks
    for chunk in points_from_chunks:
        source_label, _ = result.plan.labels_of(chunk)
        assert source_label == std.HUB_LABEL
        source_resource = next(r for r in chunk.resources if result.plan.resources.label_of(r) == source_label)
        assert source_resource == hub_resource
    ... # exact count + zero lock wait + zero extra attempts, identical to Story 4

@pytest.mark.integration
@pytest.mark.stress
def test_100k_edges_five_types_into_one_hub_planned_mode_zero_lock_waits(stress_neo4j, kafka_bootstrap, tmp_path):
    """topology="multi_type_hub": LIKES/FOLLOWS/RATED/VIEWED/SHARED, all -> Hub."""
    ...
    hub_resource = result.plan.resources.hub_resource(std.HUB_LABEL, std.HUB_KEY)
    assert hub_resource is not None
    types_sharing_hub = {
        chunk.edge_type for chunk in result.plan.chunks
        if hub_resource in chunk.resources
    }
    assert len(types_sharing_hub) > 1  # the hub's resource is not a separate one per type
    for edge_type in std.MULTI_TYPES:
        with driver.session() as session:
            count = session.run(f"MATCH ()-[r:{edge_type}]->(:Hub) RETURN count(r) AS c").single()["c"]
        assert count == len(std.distinct_pairs(dataset, edge_type))
    ... # zero lock wait + zero extra attempts
```

Both reuse `_load_schema`, `std.generate`, `std.seed_nodes`, `std.publish`,
`std.delete_topics`, `lock_wait.hard_timeout`/`query_log_mark`/
`assert_zero_lock_wait`, and `planned_harness.run_planned_mode` verbatim
from Story 4 -- the only new logic is dataset topology selection and the
two structural plan-resource checks above.

## Dependencies / sequencing

Lands after Story 4 (same file). Estimated wall-clock: comparable to
Story 4's ~25s each (same 100k scale), recorded in the completion report.
