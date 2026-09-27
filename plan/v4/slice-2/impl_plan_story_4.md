# Implementation Plan — Phase 4 / Slice 2 / Story 4

## Scope

Wire the already-tested slot-gated relationship loader into its opt-in CLI and Docker launch contract. This story does not launch a clock, rotate ownership, change Phase 3 stage ordering, add retry/DLQ behavior, or alter the self-reference policy.

## Files

- Modify `src/loader/edge_loader.py`.
- Modify `src/orchestrator/docker_service.py`.
- Modify `tests/test_edge_loader.py`.
- Modify `tests/test_docker_service.py`.
- Modify `tests/test_relationship_bulk_monitor.py` only if a loader lifecycle attribution assertion needs an existing monitor fixture; no monitor behavior change is planned.
- Create `plan/v4/completion_slice-2.md` only after final full-suite reviewer approval.

## CLI and Kafka contract

Add opt-in `--slot-gating` and `--coordination-topic` arguments. Set parser `--run-id` to `default=None`, preserving whether the user supplied the option. The parser rejects a coordination topic without `--slot-gating`, and slot gating unless `--coordination-topic` and `--run-id` were both explicitly supplied and nonblank. Only after those checks does ungated construction substitute `default_run` for a missing run ID. Validation occurs before any Kafka consumer, Neo4j driver, writer, or worker is constructed. Direct invocations retain the current work-only consumer behavior when `--slot-gating` is absent.

For a gated invocation, `main()` creates a second `Consumer` subscribed only to the supplied coordination topic. Its configuration is:

```python
{
    "bootstrap.servers": bootstrap_servers,
    "group.id": os.environ.get(
        "KAFKA_COORDINATION_GROUP_ID",
        f"{work_group}-clock-{edge_type}-{run_id}-{replica_id}",
    ),
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest",
    "max.poll.interval.ms": schema.loading.max_poll_interval_ms,
    "session.timeout.ms": schema.loading.session_timeout_ms,
}
```

Resolve the work and coordination group IDs before resource creation. Reject a blank group ID or equality between the resolved IDs (including environment overrides) before either consumer or any Neo4j resource exists. The coordination group is therefore distinct from the work group and replica-specific so every loader replica observes every compacted clock lease independently. `SlotAdmission(edge_type, run_id, schema.loading.coordination.bucket_count)`, `SlotAwareAdmissionBuffer(max_records=schema.loading.slot_buffer_max_records)`, the coordination consumer's `poll`, and an integer UTC-millisecond wall clock are passed together to `EdgeLoader`. `enable.auto.commit` remains false for both consumers; only the work consumer can commit work offsets after durable Neo4j success.

`main()` uses an explicit `exit_code` plus `finalization_error` control flow rather than returning from inside `try`: it attempts every owned cleanup exactly once in the stable order coordination consumer, work consumer, producer flush, driver close; then returns nonzero if normal execution failed or any cleanup failed. A coordination close failure is logged as `stage=coordination edge=<type> replica=<id> run_id=<id> reason=close failed`; work-consumer close, producer flush, and driver close failures are each logged as `stage=shutdown edge=<type> replica=<id> run_id=<id> component=<component> reason=<operation> failed`. Cleanup continues after every such failure.

## Docker contract

Extend `DockerService.run_edge_loader()` with keyword-only `slot_gating: bool = False` and `coordination_topic: str | None = None`. Reject an incomplete gated contract and an absent/blank `run_id` before it creates a rejection directory, builds volumes, or launches a container. For a gated launch it appends, in deterministic order:

```text
--slot-gating --coordination-topic <topic>
```

and sets `KAFKA_COORDINATION_GROUP_ID` to a run/edge/replica scoped name that is different from the existing run-scoped work group. Existing container names, hex run-id suffixes, labels, rejection paths, and exact local rollback remain unchanged. Slice 3 is the sole caller that will pass the gated arguments for a shared clock fleet; existing Phase 3 bulk launches remain ungated here.

## Failure and lifecycle behavior

- A malformed, stale, foreign, expired, duplicate, or conflicting lease still fails in `EdgeLoader` with its existing `stage=lease`, edge, replica, run, and known epoch/slot context; this story only supplies the real consumer.
- Coordination consumer construction, subscription, poll, or close failures return nonzero through the explicit startup/finalization path and do not commit work offsets. A close failure carries `stage=coordination`, edge, replica, and run attribution while remaining owned resources are still closed.
- The work consumer, coordination consumer, producer, driver, and worker are all closed/joined only by their owning process. Docker rollback stops only containers collected by this exact `run_edge_loader()` invocation.

## Tests

- CLI tests prove ungated behavior creates one manual-commit work consumer; gated behavior creates a distinct second manual-commit earliest-reset consumer, subscribes it to the requested topic, injects one complete gating dependency set, and closes it on success and failure.
- CLI validation tests reject a coordination topic without slot gating, the no-`--run-id`/no-`--coordination-topic` gated invocation, blank explicit gated run IDs or coordination topics, blank resolved group IDs, and equal work/coordination group IDs before Kafka/Neo4j resources are constructed. They also prove a distinct explicit coordination-group override succeeds and that ungated no-`--run-id` construction still receives `default_run`.
- CLI construction/subscription and close-failure tests prove nonzero result, no work commit, contextual coordination failure attribution, and exact cleanup of the work consumer and driver after a coordination failure. A parameterized or representative work-consumer/producer/driver cleanup failure test proves `stage=shutdown` edge/replica/run/component attribution while later cleanup still executes.
- Docker tests prove gated command order, run/edge/replica propagation, and distinct work/coordination group identities. Before any rejection-directory or container operation, they reject `slot_gating=True` with a missing/blank topic, `slot_gating=False` with a supplied topic, and missing/blank gated run IDs; existing ungated commands and exact rollback remain unchanged.
- Focused tests run first, then the user-required full non-Docker suite and `git diff --check`. Docker E2E is not run or claimed in Slice 2.

## Verification commands

```bash
.venv/bin/python -m pytest tests/test_edge_loader.py tests/test_docker_service.py -q --tb=short
.venv/bin/python -m pytest tests/ --ignore=tests/test_dev_environment.py --ignore=tests/test_docker_build.py --ignore=tests/test_node_loader_integration.py -q --tb=short
git diff --check
```

## Rollback / safety

The patch is additive and opt-in at the CLI/Docker boundary. It adds no Cypher, no offset-resolution path, no broad Docker cleanup, and no retry/DLQ policy. Removing `--slot-gating` preserves the existing direct relationship loader invocation.
