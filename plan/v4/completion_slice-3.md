# Phase 4 / Slice 3 Completion — Rotate Shared-Label Loaders Without Starvation

## Status

**Complete and reviewer-approved.** The persistent Slice 3 reviewer
`/root/slice3_reviewer` approved Stories 1–5, the post-E2E corrections, and
the final integration on 2026-09-22.

## Approved stories

1. **Deterministic conflict families and fair rotation plan** — approved.
2. **Label-scoped rotating clock leases** — approved.
3. **Exact relationship fleet and clock lifecycle** — approved.
4. **Rotating bulk boundary completion** — approved.
5. **Non-terminal stream supervision and opt-in Docker E2E** — approved.

## Delivered changes

- Added immutable Phase 3 conflict-family derivation and deterministic,
  bounded-fair label/bucket ownership rotation.
- Extended the run-scoped clock protocol with canonical
  `shared_bucket_owners`; stale, foreign, malformed, expired, duplicate and
  conflicting lease input fails closed.
- Added one canonical fleet contract shared by every edge loader and the clock,
  plus `Dockerfile.clock` and the clock entry point.
- Replaced sequential eligible relationship stages with one exact shared fleet,
  run-scoped clock, label-aware admission, and exact-object cleanup.
- Added rotation-aware finite bulk completion and drain attribution.
- Added a durable stream supervisor that remains live until explicit shutdown;
  zero lag is never terminal in stream mode.
- Added optional validated `start --run-id` propagation for deterministic
  lifecycle/E2E identities.
- Added an executable opt-in Docker E2E for `WORKS_AT(Person, Company)` and
  `BOUGHT(Person, Product)`, including graph, Kafka offset/watermark,
  coordination-lease epoch ownership, and exact cleanup assertions.
- Corrected Neo4j 5.26 capability handling so the default APOC path does not
  probe Cypher 25 and native mode remains capability-gated and fail-closed.
- Corrected the APOC writer's endpoint binding before `MERGE`, retained the
  first failed lane's causal diagnostic without logging record payloads, and
  preserved no-commit-on-write-failure behavior.
- Added bounded shared-control-topic backlog draining so historical foreign
  runs cannot starve current assignment acknowledgements while health and
  deadline checks remain bounded.

## Files changed

- `src/cli.py`
- `src/loader/edge_loader.py`
- `src/loader/edge_execution.py`
- `src/loader/slot_admission.py`
- `src/orchestrator/coordination.py`
- `src/orchestrator/dependency_manager.py`
- `src/orchestrator/docker_service.py`
- `src/orchestrator/relationship_bulk_monitor.py`
- `src/orchestrator/fleet_contract.py`
- `src/orchestrator/rotation.py`
- `src/orchestrator/clock_runner.py`
- `Dockerfile.clock`
- `tests/test_cli.py`
- `tests/test_clock_runner.py`
- `tests/test_coordination.py`
- `tests/test_dependency_manager.py`
- `tests/test_docker_service.py`
- `tests/test_edge_loader.py`
- `tests/test_edge_execution.py`
- `tests/test_edge_execution_writer.py`
- `tests/test_fleet_contract.py`
- `tests/test_relationship_bulk_monitor.py`
- `tests/test_rotation.py`
- `tests/test_rotation_e2e.py`
- `tests/test_slot_admission.py`
- `plan/v4/slice-3/stories.md`
- `plan/v4/slice-3/impl_plan_story_1.md`
- `plan/v4/slice-3/impl_plan_story_2.md`
- `plan/v4/slice-3/impl_plan_story_3.md`
- `plan/v4/slice-3/impl_plan_story_4.md`
- `plan/v4/slice-3/impl_plan_story_5.md`
- `plan/v4/slice-3/impl_plan_story_5_capability_probe_correction.md`

## Verification

- Focused correction suites:
  - capability, writer, and loader: `115 passed`;
  - monitor and CLI: `54 passed`.
- Required full non-Docker suite:

  ```text
  .venv/bin/python -m pytest tests/ \
    --ignore=tests/test_dev_environment.py \
    --ignore=tests/test_docker_build.py \
    --ignore=tests/test_node_loader_integration.py \
    -q --tb=short

  476 passed, 3 skipped in 2.85s
  ```

- `git diff --check` passed.

## Docker E2E status

Docker, `graph-loader-net`, Kafka, and Neo4j were confirmed available. The
real opt-in test was executed with `RUN_ROTATING_RELATIONSHIP_E2E=1` and
passed:

```text
1 passed in 26.97s
```

The test provisioned finite isolated input, executed the rotating relationship
fleet, verified both graph relationship sets, proved each Kafka group commit
equalled its topic watermark, observed `WORKS_AT` and `BOUGHT` owning the
shared `Person` buckets in distinct accepted epochs, and verified all exact
run-scoped edge and clock containers were cleaned up.

## Reviewer sign-off

`/root/slice3_reviewer` gave final integration **APPROVED** on 2026-09-22 and
independently reran the required non-Docker suite (`476 passed, 3 skipped in
2.89s`) plus `git diff --check`.
