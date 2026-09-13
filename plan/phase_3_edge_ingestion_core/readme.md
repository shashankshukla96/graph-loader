# Phase 3 (Epic): High-Throughput Edge Loading (Core)

## Business Objective
Implement the core Mix-and-Batch engine to load relationships (Edges) at extreme scale without triggering database deadlocks.

## Value Proposition
- **High Performance:** Achieves 2-3x (or higher) throughput compared to sequential edge loading.
- **Deadlock Immunity:** Structurally prevents the Neo4j engine from locking up on "super nodes" or star schemas.
- **Bulk Loading Efficiency:** Allows massive historical datasets to be loaded quickly.

## Features (High-Level)
1. **Mix-and-Batch Partitioner:** The core algorithm that hashes and grids incoming edges.
2. **Concurrent Transaction Engine:** Utilizing Neo4j 5.21+ native parallelism (CALL IN CONCURRENT TRANSACTIONS).
3. **Sequential Conflict Grouping:** As a starting point for cross-loader coordination, edge loaders that conflict (share node types) run sequentially to guarantee safety.

## Acceptance Criteria
- [ ] The Mix-and-Batch partitioner correctly routes edges into non-overlapping groups based on source and target hash buckets.
- [ ] The lock-ordering algorithm successfully sorts intra-batch edges by `min(src_id, tgt_id)` to prevent intra-transaction deadlocks.
- [ ] Neo4j 5.21+ `CALL { ... } IN CONCURRENT TRANSACTIONS` is utilized with `DISJOINT BY` parameterization.
- [ ] Conflicting edge loaders (e.g., those sharing the `Person` node type) are executed one after the other in bulk mode to ensure zero lock contention between loaders.
