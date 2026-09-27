# Implementation Plan — Phase 4 / Slice 4 / Story 4

## Scope

Turn the approved isolation plan into the relationship orchestration policy.
Bulk runs will drain the shared rotating fleet first, then execute one lexical,
clock-free self-reference phase at a time.  Streams will reject mixed schemas,
permit exactly one isolated-only edge without a clock, and reject more than one
isolated edge because a non-terminal stream cannot cross an absolute boundary.

## Files

- Modify `src/cli.py`.
- Modify `src/orchestrator/relationship_bulk_monitor.py` to support exact
  non-rotation stream health supervision without a lease check.
- Extend `tests/test_cli.py` and `tests/test_relationship_bulk_monitor.py`.
- Modify `README.md` to state the bulk order and approved stream restriction.

## APIs and orchestration

1. Add an internal deterministic phase-ID helper in `src/cli.py` that accepts
   an already `_validated_run_id` parent, phase kind/index/type, creates only
   the accepted run-ID alphabet, and remains at most 128 characters.  It will
   retain a readable bounded parent prefix plus a SHA-256-derived suffix over
   all parent/phase inputs, so truncation cannot collapse otherwise-distinct
   long parent IDs.  Tests will prove deterministic output, valid length, and
   distinct IDs for differing phase inputs.
2. Refactor `_run_rotating_relationship_fleet()` to accept an explicit
   selected shared-edge sequence and a caller-provided phase run ID while
   preserving current direct-call behavior.  With an explicit subset it will
   reject any self-reference, validate the subset against the schema's
   isolation plan/shared contract, return success with no clock when the
   subset is empty, and build conflict/rotation state only from that subset.
   It will retain its exact-container cleanup and existing run/epoch/slot
   attribution.  No isolated type may reach a shared launch.
3. Add a relationship orchestration helper that receives the already-loaded
   schema, Docker service, args, and parent run ID.  It calls
   `build_relationship_isolation_plan(schema.edges)` once, maps type names to
   the original edge objects, then in **bulk** mode:
   - derives the shared phase ID and runs the selected shared fleet if nonempty;
   - returns immediately on shared failure, after that helper's exact cleanup;
   - invokes Story 3's `_run_isolated_relationship_bulk_phase` for each
     `isolated_phases` entry in lexical/index order using an individual
     collision-safe phase ID and the already-loaded consumer-group prefix;
   - returns immediately on an isolated failure, so no later phase launches.
   It does not build clocks or leases for isolated phases, and it never uses
   Docker list/discovery cleanup.
4. In **stream** mode, derive the same plan before any relationship launch:
   - shared plus isolated types: log an attributed policy rejection and launch
     neither relationship fleet;
   - shared-only: launch the selected shared fleet using the existing clock
     path;
   - exactly one isolated-only type: launch that one edge exactly once with
     `slot_gating=False`, no fleet/coordination arguments and a deterministic
     phase run ID; it remains alive until SIGINT/SIGTERM, supervises only its
     exact containers, then stops those exact objects;
   - more than one isolated-only type: reject before relationship launch.
5. Generalize `RelationshipBulkMonitor.supervise_stream()` narrowly: preserve
   the current rotation lease wait/expiry/clock-health behavior when a rotation
   plan exists, and for a monitor without rotation dependencies continuously
   poll/check only its exact edge replicas until the supplied shutdown event.
   Neither branch captures a watermark, treats zero lag as terminal, or
   commits offsets.  The non-rotation branch must not allocate/subscribe a
   coordination consumer.
6. `handle_start()` will generate and validate one parent run ID for both bulk
   and stream mode (even where the user omitted `--run-id`) before relationship
   orchestration.  It will call this orchestration only after the node bulk
   monitor has finished/zero-lag proof (or after node stream launch).  No phase
   starts after an earlier relationship phase returns failure.  Relationship
   orchestration failures/logs must carry the full `parent_run_id`, derived
   `phase_run_id`, phase, and edge type, including when a long parent was
   truncated in the Docker/Kafka-safe phase ID.

## Tests

- Mixed bulk `WORKS_AT`, `BOUGHT`, and `KNOWS(Person,Person)` launches the
  shared fleet first, proves its returned loaders and clock are stopped before
  the first `KNOWS` call, then runs the isolated phase with a different
  deterministic phase run ID and no clock.
- All-shared and self-only bulk paths retain their expected behavior; empty
  shared selection launches no clock.
- A shared or first-isolated failure prevents each later isolated launch and
  records full parent/derived phase/phase-index/type attribution; cleanup
  remains confined to returned objects.
- Explicit selected fleet arguments cannot smuggle a self-reference into
  `_run_rotating_relationship_fleet`.
- Stream mixed and multi-self schemas reject before relationship Docker calls;
  one isolated-only stream launches one no-slot-gated loader and uses no clock.
  Its monitor remains live at zero lag until injected shutdown, then cleans up
  exact objects.  A no-`--run-id` stream test proves a generated valid parent
  ID reaches the derived isolated phase and run-scoped loader group.
- `RelationshipBulkMonitor` non-rotation stream supervision polls/checks its
  exact tracked edge and does not use coordination/clock/lag completion;
  existing rotation stream tests remain unchanged.
- README examples/documentation describe shared-then-isolated bulk execution
  and the stream restriction.

## Failure and compatibility boundaries

No new retry, DLQ, Cypher, or self-reference lane/lock policy is introduced.
The existing durable Neo4j-before-Kafka-commit loader implementation and the
Phase 3 monitor boundary proof remain unchanged.  Kafka control leases stay
run/epoch scoped for the shared clock path only; isolated work creates none.
Every error includes a relevant stage and parent/phase run ID, edge type, and
replica where an exact container operation fails.  All Docker cleanup receives
only retained returned containers; it never discovers broad labels or stops an
overlapping run.
