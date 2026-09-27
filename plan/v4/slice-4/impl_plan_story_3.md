# Implementation Plan — Phase 4 / Slice 4 / Story 3

## Scope

Add one reusable, clock-free bulk lifecycle for exactly one self-referencing
relationship type.  It will use the existing edge loader and Phase 3
`RelationshipBulkMonitor` fixed-boundary proof, but will neither construct nor
touch rotation/clock state.  Shared-plus-isolated ordering and stream policy
remain Story 4.

## Files

- Modify `src/cli.py`.
- Extend `tests/test_cli.py`.
- Extend `tests/test_relationship_bulk_monitor.py` only if an isolated-monitor
  invariant needs a regression test.  No Docker-service code change is
  expected because its existing `slot_gating=False` launch path already omits
  coordination arguments.

## API and lifecycle

Add:

```python
def _run_isolated_relationship_bulk_phase(
    edge: EdgeConfig,
    docker_service: DockerService,
    args: argparse.Namespace,
    *,
    run_id: str,
    phase_index: int,
    consumer_group_prefix: str,
) -> int:
    """Prove one exact, clock-free self-reference bulk phase drained."""
```

Before launching, validate `run_id` with `_validated_run_id`, a non-boolean
nonnegative integer phase, a nonblank `consumer_group_prefix`, and
`edge.nodes.is_self_referencing is True`; an accidental shared edge fails
closed with `stage=isolation`, run ID, phase, and edge type.  Import
`EdgeConfig` in `src/cli.py` because this module does not postpone annotations.
Story 4 will pass the already-loaded `schema.loading.consumer_group_id`; this
helper must not reload the schema.  Launch exactly `edge.replicas` via
`DockerService.run_edge_loader` with `mode="bulk"`, the caller-supplied run
ID, consumer-group prefix, and `slot_gating=False`. It must not pass a
coordination topic or fleet types and must not call `run_global_batch_clock`.

Immediately retain every returned container.  Validate count, per-launch
object uniqueness, and exact `(edge.type, replica_id)` accounting before
constructing `RelationshipBulkMonitor([edge], run_id, expected, tracked, ...)`
without `rotation_plan`, `coordination_topic`, `tracked_clock`, or a
coordination consumer.  Execute the Phase 3 finite proof in order:

1. assignment coverage;
2. capture finite watermarks;
3. wait for durable completion (the existing Neo4j-before-offset commit
   condition);
4. stop only the retained returned loaders with replica identities;
5. wait for drain acknowledgements; and
6. verify zero lag.

On launch, monitor, shutdown, or drain failure, log/return failure with
`stage=isolation run_id=<...> phase=<...> edge=<...>` and replica attribution
where a specific target fails.  Cleanup is exclusively `_stop_exact_containers`
over the retained objects, first in failure handling and idempotently in
`finally`; no Docker discovery/list/label sweep is permitted.  Always close a
created monitor.  The helper does not introduce Cypher, retry, DLQ, clock, or
lease behavior.

## Tests

- Successful `KNOWS(Person, Person)` launches only its replicas with
  `slot_gating=False`, no coordination/fleet kwargs, creates a non-rotation
  monitor, performs the complete finite proof, and never calls clock launch.
- A returned duplicate object or wrong replica count is rejected before monitor
  construction and all retained objects are stopped.
- A monitor durable-write/completion failure stops only returned loaders,
  closes the monitor, and emits phase/run/edge attribution; it does not imply a
  Kafka commit before Neo4j success.
- Shutdown/drain failure identifies the exact `edge=KNOWS replica=0` and does
  not discover or stop unrelated containers.
- A non-self-reference edge is rejected before Docker work.
- Invalid/blank run IDs and boolean/negative phase indexes are rejected before
  Docker work, and schema is not reloaded to obtain the consumer-group prefix.
- Preserve a unit assertion that a monitor instantiated without rotation
  dependencies has no coordination consumer/clock requirement, if current
  coverage does not make this explicit.

## Failure and compatibility boundaries

No offset or lease is created by the helper.  Existing loader durable semantics
are unchanged; monitor completion remains the only pre-stop success condition.
All cleanup uses exact objects that this invocation received.  Story 4 alone
will derive phase run IDs and sequence shared/isolated work; this story accepts
its exact phase run ID as input.
