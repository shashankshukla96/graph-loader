# Implementation Plan — Phase 4 / Slice 3 / Story 5

## Scope

Finish the slice by making rotating eligible relationship fleets usable in
stream mode without interpreting zero lag as terminal, and add a real,
opt-in Docker acceptance test for `WORKS_AT(Person, Company)` plus
`BOUGHT(Person, Product)`.  It does not implement DLQ/retry or a
self-reference execution policy.

## Files

- Modify `src/cli.py` and `src/orchestrator/relationship_bulk_monitor.py`.
- Create `tests/test_rotation_e2e.py`.
- Update focused CLI/monitor tests and Docker test configuration only as
  required by the executable E2E.

## Stream lifecycle

Refactor `_run_rotating_relationship_fleet()` so the canonical type contract,
families, rotation plan, run ID, exact edge objects, and one exact clock are
shared by bulk and stream launch.  In stream mode it launches every eligible
non-self-referencing edge with the same label-scoped lease contract and starts
the exact clock, but performs **none** of the bulk-only operations:

- no assignment coverage/boundary capture;
- no completion/zero-lag decision;
- no stop/drain sequence;
- no return of success merely because input is currently idle.

`RelationshipBulkMonitor.supervise_stream(shutdown_requested)` will be the
durable parent supervisor.  The stream CLI remains running after exact launch,
subscribes its separately owned run-scoped coordination consumer, waits for an
initial plan-validated lease within the configured lease budget, and then
polls/reloads only retained edge and clock objects until an explicit signal.
The method receives an injectable `threading.Event`-compatible shutdown flag,
monotonic clock, sleep and poll interval through its existing constructor/
method seams, so tests can drive deterministic initial-lease timeout, expiry,
health and explicit-shutdown transitions without real time or Kafka.
It never uses lag, boundary, or completion to terminate.  An expired/malformed/
duplicate/stale/conflicting lease, clock exit, edge exit, monitor failure, or
startup failure is surfaced with `stage=monitor`, run ID, edge/replica and last
epoch/slot where known; the CLI stops the exact clock first, then exact edges,
and returns nonzero.  On an explicit shutdown signal it stops only these exact
objects and exits without making any finite-bulk completion claim.  Bulk
retains Story 4's synchronous boundary proof.

Add optional validated `start --run-id <id>`.  Both bulk and stream rotating
fleets use it verbatim when supplied; otherwise they generate one UUID.  It is
rejected before any container launch if blank/invalid.  The identical run ID is
passed to every fleet edge, clock, monitor group, and test assertion, allowing
exact name/group derivation without broad Docker/Kafka discovery.

## Docker E2E

`tests/test_rotation_e2e.py` is marked `integration` and skips only when
`RUN_ROTATING_RELATIONSHIP_E2E != "1"`.  When enabled, it must:

1. skip **only** when `RUN_ROTATING_RELATIONSHIP_E2E != "1"`; once set to
   `1`, assert Docker, Kafka and Neo4j access up front and fail diagnostically
   if any service/network/image prerequisite is absent;
2. create unique run-scoped Kafka topics and a temporary isolated schema with
   Person, Company, Product, `WORKS_AT`, and `BOUGHT`, short clock slots, and
   no self-reference edge; the work topics **and coordination topic** are all
   precomputed unique names;
3. provision finite isolated Neo4j nodes and produce records spanning multiple
   Person buckets for both relationship types;
4. precompute one unique valid explicit test run ID, build/use the exact
   loader and clock images, launch the bulk CLI/fleet with `--run-id`, and wait
   for its actual finite completion;
5. query Neo4j for every expected relationship, query Kafka consumer-group
   offsets/watermarks for each run-scoped edge group, and prove commits equal
   the finite boundaries; consume/decode only the coordination topic records
   keyed to that run ID and prove `WORKS_AT` and `BOUGHT` each own Person
   buckets in distinct accepted epochs; and
6. assert every returned run-scoped edge/clock container is removed/stopped,
   using only precomputed names derived from the supplied run ID:
   `graph-loader-edge-<type>-<replica>-<hex-run-id>` and
   `graph-loader-clock-<hex-run-id>`, without inspecting, stopping, or
   deleting containers from another run.

The test never calls a skip-path a passing E2E result.  Its `finally` cleanup
uses only exact precomputed unique topic names, returned container IDs/names,
and Neo4j node IDs created by the test.

## Tests / Definition of Done

- Unit tests prove stream starts the identical canonical fleet and clock but
  never calls any bulk monitor/boundary/drain/stop method at zero lag.
- Supervisor tests drive an injected shutdown event, initial-lease timeout and
  expiry, and exact edge/clock exits.  Each failure path asserts an attributed
  nonzero CLI outcome and exact clock-then-edge cleanup; explicit shutdown
  proves the same exact cleanup without a bulk-success claim.
- Parser/CLI tests prove an invalid `--run-id` fails before Docker launch and a
  supplied valid value is propagated identically to every edge, clock and
  monitor group.
- Existing bulk tests continue to prove exact cleanup and failure attribution.
- The E2E is executable under its opt-in environment; if infrastructure is
  unavailable in this session, its status is recorded as not run, not passed.
- Focused tests pass before review; Story 5 and final full-suite outputs go to
  the persistent reviewer.
