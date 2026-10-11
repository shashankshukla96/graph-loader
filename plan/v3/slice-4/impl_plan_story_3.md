# Implementation Plan — Phase 3 / Slice 4 / Story 3

## Story

**Launch and Monitor One Relationship Bulk Stage**

## Scope and non-goals

This story supplies stage-local Docker execution and monitoring. It does not
choose stage order or wire any CLI command; Story 4 owns that orchestration.
The node bulk monitor remains unchanged.

## Files

| Action | Path | Purpose |
| --- | --- | --- |
| Create | `Dockerfile.edge_loader` | Edge-loader image entrypoint. |
| Modify | `src/orchestrator/docker_service.py` | Edge image build and exact run-scoped edge-container launch. |
| Create | `src/orchestrator/relationship_bulk_monitor.py` | Fixed-boundary lifecycle monitor for one edge stage. |
| Modify | `tests/test_docker_service.py` | Edge image/container command, labels, env, and rollback tests. |
| Create | `tests/test_relationship_bulk_monitor.py` | Stage coverage, boundary, health, drain, and stale-control tests. |

## Docker contract

Add `DockerService.build_edge_image(tag="graph-loader-edge:latest",
dockerfile="Dockerfile.edge_loader")` and `run_edge_loader(...)`. The latter
uses image `graph-loader-edge:latest`, runtime-schema/rejection bind mounts,
and Docker-network defaults `bolt://neo4j:7687` / `kafka:29092`. Its command
contains `--config`, `--edge-type`, `--mode`, `--topic`, `--replica-id`, and,
for scoped calls, `--run-id`.

Each scoped container receives labels `app=graph-loader`,
`component=edge-loader`, `edge_type`, `replica_id`, and `run_id`, plus the
existing hex-encoded run suffix. A partial launch failure stops only objects
created by that exact method invocation. Scoped calls also set
`KAFKA_GROUP_ID={consumer_group_prefix}-{edge_type}-{run_id}` and a rejection
filename incorporating the encoded run id; two overlapping runs therefore
cannot share consumer offsets or rejection audit records.

`Dockerfile.edge_loader` mirrors `Dockerfile.node_loader` but uses
`ENTRYPOINT ["python", "-m", "src.loader.edge_loader"]`.

## RelationshipBulkMonitor contract

Create `RelationshipBulkMonitor`:

```python
Replica = tuple[str, str]  # (edge_type, replica_id)

class RelationshipBulkMonitor:
    def __init__(self, edges: Sequence[EdgeConfig], run_id: str,
                 expected_replicas: set[Replica],
                 tracked_containers: Mapping[Replica, object], *,
                 bootstrap_servers: str, consumer_group_prefix: str, ...): ...
```

It shares exception classes from `bulk_monitor` but has independent state keyed
by `(edge_type, topic, partition)`, since distinct edge consumer groups may
legitimately read one topic. It must:

1. Discover every configured stage-edge topic partition and group as
   `{consumer_group_prefix}-{edge_type}-{run_id}`.
2. Subscribe to `CONTROL_TOPIC`, accepting only current `run_id`, expected
   edge/replica, valid event/epoch, and that edge's own topic. Old epochs are
   ignored; same-epoch assignment/idle data must be identical or fail rather
   than overwrite; and `DRAIN_COMPLETE` qualifies only at exactly the latest
   assignment epoch. Exact coverage requires all expected replicas and every
   `(edge_type, topic, partition)` exactly once. Idle proves no ownership only.
3. Capture watermarks after coverage. Completion and final verification require
   unchanged watermarks and the relevant edge group commits at/exactly boundary.
4. Inspect only exact tracked containers. Before drain any missing/exited
   container raises attributed `LoaderCrashError`; during drain it is accepted
   only after current-epoch `DRAIN_COMPLETE`.
5. Bound all waits, include stage types in errors, and close owned consumers.

No node-monitor refactor occurs: its node-label public contract remains stable,
whereas this monitor requires edge-type namespace isolation.

## Tests

- Docker build, command/environment/mount/label invocation, encoded names, and
  run-scoped consumer/rejection names, and exact partial-launch rollback.
- Monitor current run/type/epoch filtering, exact coverage, idle surplus,
  boundaries and per-edge groups, late arrivals, missing offsets, crash
  attribution, drain acknowledgement before removal, final zero lag,
  conflicting same-epoch controls, and future drain controls.

Run:

```bash
.venv/bin/python -m pytest tests/test_docker_service.py \
  tests/test_relationship_bulk_monitor.py tests/test_bulk_monitor.py -v
```

## Rollback / cleanup

Tests are mocked. Docker rollback stops only exact new containers and never
alters shared Kafka, Neo4j, network, or another run's containers.
