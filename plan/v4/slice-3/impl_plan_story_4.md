# Implementation Plan — Phase 4 / Slice 3 / Story 4

## Scope

Prove a rotating relationship bulk fleet reaches the finite input boundaries
captured for every edge type.  A zero-lag observation while a shared-label
loader is temporarily unowned is never completion.  This plan changes only
bulk completion and shutdown proof; it does not add stream behavior, retries,
DLQ, or a self-reference policy.

## Files

- Modify `src/orchestrator/relationship_bulk_monitor.py`.
- Modify `src/cli.py`.
- Modify `tests/test_relationship_bulk_monitor.py` and `tests/test_cli.py`.

## Completion protocol

`RelationshipBulkMonitor.wait_for_rotating_completion(timeout_seconds)` will
be a distinct method, not an alias or reuse of the legacy one-shot
`wait_for_completion()`. It will
require a current, non-expired, plan-validated lease before every boundary
decision.  It retains the full multi-edge watermark boundary captured only
after assignment coverage and lease evidence.  On every poll it checks exact
clock/edge health, control acknowledgements, quiescent watermarks, and all
edge-group commits against that fixed boundary.  A slot with no admitted
shared resource remains progress-neutral: only durable commits at every
captured partition boundary complete the method.  A missing, stale, expired,
malformed, same-epoch-conflicting, or plan-incompatible lease fails closed with
`stage=monitor`, run ID, last epoch/slot, and affected edge/partition where
known.  Moving watermarks remain a quiescent violation.

The CLI will replace the legacy `wait_for_completion()` call in the rotating
fleet with `wait_for_rotating_completion()`.  On normal completion it stops
only the exact retained edge containers while its exact clock remains live,
waits for each edge `DRAIN_COMPLETE`, verifies commit equality and immutable
watermarks, then stops the exact retained clock.  On a clock, lease, launch,
writer, monitor, or shutdown error it preserves the Story 3 fail-closed order:
stop the exact clock first, then exact edge containers, and reports stage/run/
edge/replica/epoch/slot context.  It never creates a graceful-drain success
claim after failure.

## Tests

- Multiple lease epochs with fixed all-edge boundaries prove that an early
  zero-lag or an unowned `WORKS_AT`/`BOUGHT` slot cannot complete bulk; later
  commits at all boundaries can. The test demonstrates two accepted epochs
  before boundary success.
- Expired/no lease and moved watermark fail before a completion decision.
- Partial or late `DRAIN_COMPLETE` acknowledgements fail with the affected
  edge/replica and last epoch/slot attribution; they cannot become a graceful
  success claim.
- CLI asserts rotating completion is used, legacy completion is not, and edge
  shutdown occurs before clock shutdown only on the successful drain path.
- A completion/monitor failure proves no later work is launched or monitored
  and that cleanup targets only the clock and edge container objects retained
  by this fleet attempt.
- Existing durable-commit and Stage 3 exact-container tests remain green.

## Definition of Done

- Focused monitor/CLI tests pass.
- Story 3's run-scoped lease and exact-container contracts remain unchanged.
- No Docker E2E is claimed here; Story 5 owns it.
