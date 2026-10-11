# Implementation Plan — Phase 3 / Slice 4 / Story 4

## Story

**Orchestrate Sequential Conflict Stages from the CLI**

## Files

| Action | Path | Purpose |
| --- | --- | --- |
| Modify | `src/cli.py` | Run relationship stages after successful node bulk. |
| Modify | `src/models/schema.py` | Declare bounded relationship fleet replica count. |
| Modify | `README.md` | Document staged relationship bulk operations. |
| Modify | `tests/test_cli.py` | Parallel-stage launch, sequencing, and exact cleanup unit tests. |
| Modify | `tests/test_schema_validation.py` | Validate edge replica defaults and bounds. |
| Create | `tests/test_relationship_bulk_e2e.py` | Docker-marked staged-fleet proof. |

## Implementation

Add a private `_run_relationship_bulk_stages(schema, docker_service, args)`.
It builds `RelationshipConflictPlan`, generates one run id for the whole edge
fleet, and for each ordered stage: launches every edge before constructing its
`RelationshipBulkMonitor`; validates assignment; captures boundary; waits for
completion; stops only stage-returned container objects; waits for drain;
verifies zero lag; and closes the monitor in `finally`.

It passes `schema.loading.consumer_group_id`, `args.network`, config path,
bulk mode, and the edge topic to `run_edge_loader`. On launch/monitor/stop
failure it logs the stage/type context, stops exact current-stage objects, and
returns 1. It never advances to later stages. `handle_start` runs this only
after the existing successful node bulk lifecycle, while stream remains
node-only. Build the edge image only when `schema.edges` is nonempty and image
building is not skipped.

Add `EdgeConfig.replicas: int = Field(default=1, ge=1)` so each type declares
its fleet size. For every edge, preserve all returned container objects before
validating the exact count against its expected `(edge_type, replica_id)` set;
a short/extra/malformed result fails the current stage. Every stage owns a
`finally` cleanup that retries `_stop_exact_containers` even after drain has
begun, and it closes its monitor on all paths. It never stops a prior stage or
another run.

Tests mock two disjoint edges to prove both launch before monitoring, plus
`WORKS_AT`/`BOUGHT` sharing Person to prove stage 2 starts only after stage 1
drains. They also prove stage failure cleans only stage containers and blocks
later launch. README documents the order and quiescent-input condition.
They also cover image-build failure, skip-build, zero-edge schema, exact launch
arguments (replicas/prefix/run id), monitor close, and stage/type attribution.
Existing CLI schema doubles explicitly use `edges=[]`. Add a Docker-marked E2E
test for disjoint and conflicting topics, relationship counts, zero lag, and
no edge container remaining after success.
Schema tests cover default one replica and reject zero, negative, noninteger,
and boolean replica counts. The canonical schema keeps the default unless it
needs to demonstrate a multi-replica fleet explicitly.

Run:

```bash
.venv/bin/python -m pytest tests/test_cli.py -v
```
