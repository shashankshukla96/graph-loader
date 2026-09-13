# Phase 4 (Epic): Multi-Loader Concurrency (Global Batch Clock)

## Business Objective
Maximize system throughput by allowing conflicting edge loaders (e.g., WORKS_AT and BOUGHT, which both touch Person) to run simultaneously safely.

## Value Proposition
- **Extreme Scale:** Unlocks the final tier of parallel execution. Instead of waiting for one edge type to finish loading, all edge types are ingested simultaneously.
- **Hardware Utilization:** fully saturates Neo4j cluster CPUs and I/O.
- **Fairness:** Ensures no single relationship type falls behind during continuous ingestion.

## Features (High-Level)
1. **Global Batch Clock:** A synchronization mechanism that acts like a traffic light, giving specific loaders exclusive rights to specific node buckets per time slot.
2. **Rotating Ownership:** Buckets rotate so all loaders eventually process all data.
3. **Self-Referencing Isolation:** Dedicated handling for relationships that connect nodes of the same type (they will run entirely isolated).

## Acceptance Criteria
- [ ] A central "Clock" orchestrator broadcasts slot assignments (e.g., 0 to 11) continuously.
- [ ] Edge loaders subscribe to the clock and only process records whose node hashes match their assigned buckets for the current slot.
- [ ] Edge loaders pause consumption or buffer records that do not belong to the current slot.
- [ ] Self-referencing edges (e.g., `Person KNOWS Person`) automatically bypass the clock and are scheduled in absolute isolation to avoid dual-side lock conflicts.
