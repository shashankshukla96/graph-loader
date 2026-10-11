# Implementation Plan — Phase 4 / Slice 3 / Story 3

## Scope

Replace Phase 3's sequential stages for eligible non-self-referencing edges
with one exact, run-scoped rotating fleet plus one clock. This story wires
live lease admission and launch/monitor attribution; Story 4 owns rotating
bulk completion and Story 5 owns stream behavior and Docker E2E.

## Files

- Modify `src/loader/slot_admission.py` and `src/loader/edge_loader.py`.
- Modify `src/orchestrator/docker_service.py` and `src/cli.py`.
- Modify `src/orchestrator/relationship_bulk_monitor.py`.
- Create `Dockerfile.clock` and `src/orchestrator/clock_runner.py`.
- Modify `tests/test_slot_admission.py`, `tests/test_edge_loader.py`,
  `tests/test_docker_service.py`, `tests/test_cli.py`, and
  `tests/test_relationship_bulk_monitor.py`.

## Label-aware admission

Extend `SlotAdmission` with immutable `RotationPlan` and the loader edge's
source/target labels. Its current-run `accept_lease()` calls
`decode_clock_lease(..., rotation_plan=plan)`. Its ownership method receives
both endpoint labels and bucket numbers: unshared labels require no lease;
each shared label must have exactly the current edge owner at its endpoint
bucket. It rejects a missing plan, mismatched edge/label metadata, a nonempty
map missing an actually shared endpoint resource, stale/foreign/duplicate/
conflicting/expired lease, or a partial two-endpoint decision. `bucket_owners`
is never used in rotation-aware admission.

`EdgeLoader` obtains the exact label-aware admission instance from CLI wiring
and keeps the Slice 2 poll-owner/worker/commit model unchanged. Every rejected
lease/worker/rebalance failure identifies `stage`, edge, replica, run ID, and
last epoch/slot where known; no valid record offset commits before the worker's
durable result and current post-write lease validation.

## Exact fleet and clock lifecycle

The CLI derives one lexically sorted, comma-delimited `fleet_edge_types`
string from all eligible non-self-referencing schema edges. It passes the
identical `--fleet-edge-types <canonical-list>` argument, plus `--run-id` and
`--coordination-topic`, to every edge and clock container. Each entry point
loads the runtime schema, selects **exactly** this declared set (not all schema
edges), rejects a missing/duplicate/self-referencing/unknown type, and rebuilds
the same Phase 3 conflict plan, Story 1 families, and RotationPlan. Thus an
edge container never attempts to reconstruct a fleet schedule from its own
type alone, and the decoded lease plan is byte-for-byte derivable from the
shared contract.

Extend `DockerService.run_edge_loader()` with required keyword-only
`fleet_edge_types` whenever `slot_gating=True`. It rejects absent, blank,
duplicate, non-lexically-sorted, or otherwise noncanonical strings before any
directory/container operation and appends `--fleet-edge-types <canonical-list>`
to the loader command. The edge-loader parser applies the same canonical
validation before Kafka/Neo4j resources exist. Both edge loader and
`clock_runner` reject a supplied `--coordination-topic` that differs from
`schema.loading.coordination.topic`; a shared run ID can never hide a split
topic configuration.

Add a keyword-only `DockerService.run_global_batch_clock()` contract accepting
the exact run ID, schema path, canonical fleet type string, coordination topic,
and network. It starts `python -m src.orchestrator.clock_runner --config
/app/runtime_schema.yaml --run-id <id> --fleet-edge-types <canonical-list>
--coordination-topic <topic>`, labels only the returned clock container with
`app=graph-loader`, `component=global-batch-clock`, and `run_id`, and returns
that exact object. `clock_runner` owns its Kafka Producer and calls
`GlobalBatchClock(run_id, selected_edges, rotation_plan, config, producer,
health_probe, wall_clock_ms, sleeper)`; its local health probe is process-local
and never discovers Docker containers. The parent monitor performs Docker
health checks by reloading only this returned clock object.
`DockerService.build_clock_image()` builds `Dockerfile.clock`, and the CLI
builds that image before any rotating fleet launch. A failed image build or
argument/schema mismatch is a `stage=clock`/`stage=launch` nonzero result
before a clock or edge container is created.
It creates exactly one named/labelled clock object for that launch attempt;
its health probe reloads only that returned clock object. No `containers.list`
or broad stop is permitted. It uses the existing hex run-ID naming convention;
launch rollback stops only objects accumulated by this call.

`_run_rotating_relationship_fleet()` in `src/cli.py`:

1. partitions schema edges into self-referencing and eligible edges, rejecting
   self-references from this shared path without implementing their Slice 4
   policy;
2. builds Phase 3 conflict plan, Story 1 families/rotation, and one run ID;
3. launches every eligible edge with `slot_gating=True`, the same canonical
   fleet types, coordination topic, run ID, and group prefix before monitor
   work;
4. creates/starts one clock with the same canonical types and exact target;
5. creates `RelationshipBulkMonitor(edges, run_id, expected_replicas,
   tracked_edges, rotation_plan=plan, coordination_topic=topic,
   tracked_clock=clock_container, coordination_consumer=...)`. Its owned
   consumer group is `graph-loader-edge-clock-monitor-<run_id>`, distinct from
   work and loader coordination groups; `close()` closes only its owned
   coordination/watermark/control consumers; and
6. on launch, clock, lease, monitor, writer, or shutdown failure, stops the
   exact clock container first (preventing new leases), then only retained edge
   containers. This fail-closed failure ordering may abandon uncertain worker
   work uncommitted for replay; it never claims a graceful bulk drain (Story 4
   owns normal boundary/drain ordering). It closes monitor-owned consumers and
   returns nonzero with stage/run/edge/replica/last epoch/slot attribution.

`RelationshipBulkMonitor` consumes the coordination topic using a separate
manual-commit consumer/group, filters by run ID, validates each lease with the
same RotationPlan, tracks monotonically increasing `(epoch, slot)`, and
exposes current lease context for failure attribution. It does not yet declare
bulk completion from rotating offsets (Story 4).

## Tests

- Label-aware `WORKS_AT`/`BOUGHT` Person admission: only the plan-selected
  edge owns Person buckets per epoch; unshared Company/Product endpoints do
  not require ownership; a partial/missing map never admits or commits.
- Edge-loader tests prove stale/foreign/malformed/conflicting labels and
  worker/rebalance failure preserve unresolved offsets and include context.
- Docker tests prove one fleet-wide run/topic/config/canonical-type propagation, a distinct
  coordination group per replica, exact clock labels/name/rollback, and no
  stop beyond returned objects.
- Parser/Docker/clock-runner tests reject absent or noncanonical fleet types
  and edge/clock coordination-topic mismatches before Kafka, directories,
  image launch, or containers; successful propagation uses one canonical
  string.
- CLI tests prove all eligible types launch before monitoring and no Phase 3
  serial conflict stage is used; self-reference edges are explicitly excluded;
  exact clock/edge cleanup and stage attribution cover launch/failure paths.
- Monitor tests cover current lease filtering against the exact plan, epoch
  monotonicity, malformed lease failure, tracked-clock health, distinct
  consumer lifecycle, and edge/replica/slot/epoch
  diagnostics while retaining assignment/boundary checks.

Run focused changed-module tests before review. No Docker E2E is claimed in
this story.

## Safety / rollback

No retry/DLQ logic or self-reference execution policy is added. The old staged
bulk helper remains available for Slice 4 isolation until this shared path is
fully selected. All Kafka work commits stay in EdgeLoader's polling owner and
only follow durable Neo4j success.
