# Retry/ordering case matrix

All restricted dispositions were **approved under D02/D03 on 2026-10-16**,
through the user's full approval of the brief and its conditional `order_by`
assumptions. See [direct approval evidence](evidence/e01-user-decisions-20261016.md).
This is an approved contract target; implementation/proof remain downstream.
The matrix neither authorizes a new durability design nor labels an unsafe
counterexample a safety pass. Every row still needs exact test IDs, environment
and evidence before E09 closes.

| Case | Boundary | Proposed restricted-alpha disposition | Deterministic assertion / evidence | Owner |
| --- | --- | --- | --- | --- |
| O1 | Retry-consumer restart | In-memory arrival resurrection protection is not restart-safe | Reconstruct consumer after resolution, replay stale row; retain known counterexample as limitation. For supported `order_by` conditions, assert approved newest-value result and committed offsets on installed/live path. | E09.F2.S1 |
| O2 | DLQ partition revoke/rebalance | Revocation forgets resolution memory; no universal arrival-order promise after ownership moves | Revoke the owning partition, verify memory loss and resulting replay outcome; validate any approved `order_by` invariant. | E09.F2.S1 |
| O3 | LRU eviction | Protection does not persist beyond 100,000 cached pair/source-partition entries | Force a small bound; reproduce stale-value outcome from `test_lru_eviction_falls_back_to_the_behaviour_without_the_memory`; do not call this safety. | E09.F2.S1 |
| O4 | Same pair across source topics | Offsets from different topics are not comparable | Interleave/replay old and new records from distinct topics; assert actual approved `arrival` or `order_by` behavior using final graph properties and source identities. | E09.F2.S1 |
| O5 | Same pair across source partitions | Offsets across partitions are not comparable | Use two partitions with conflicting offset magnitudes; ensure no false global-offset ordering claim, and prove any advertised property-version rule. | E09.F2.S1 |
| O6 | Multiple replicas with asynchronously refreshed pair index | No universal non-`order_by` order safety across replicas | Force one replica's stale index view and delayed older retry; demonstrate restriction or assert the supported property-version behavior. Distinguish lock isolation from update ordering. | E09.F2.S1 |
| O7 | Manual parking/DLQ replay | Replay uses new source offsets and can bypass original index history; no blanket ordering promise | Replay a selected older parked value after a newer value; assert documented command/identity/index behavior and the exact supported `order_by` outcome, including tie/missing-value policy. | E09.F2.S1 |
| O8 | Delete/recreate a source topic and reuse offsets | Active-fleet topic recreation is unsupported; stop the fleet/retry consumer first | In owned scratch topics only, exercise/reject/document identifier reuse. Demonstrate the stop/recreate/restart precondition or known lower-offset skip hazard; never recreate IMDb topics as a probe. | E09.F2.S1 |

`test-inventory.json` names the initial reference tests and proposed installed
probe lanes. Some references are unit/in-process tests: they preserve known
counterexamples but do not alone prove a stronger live-service guarantee.
Supported service guarantees require executed assertions via E09.F1.S7's real
isolated harness, with source offsets and final properties checked. Approved
exclusions may use the targeted deterministic counterexample, precondition or
clear rejection plus documentation; record why a live probe is or is not needed.
No required supported guarantee may be silently moved to an excluded lane.

For every supported `order_by` claim, first record exact field validation,
comparison, equal-version, missing/null-value, external-writer and replay
assumptions in D02/D03. Use both an older-after-newer example and the relevant
identity/replica boundary. If the chosen contract requires new durable state or
coordination, add design/implementation/proof epics and re-review them; do not
absorb that redesign into these validation stories.
