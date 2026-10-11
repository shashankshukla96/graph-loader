# Phase 3 / Slice 3: Concurrent Edge Batches Without Deadlocks

## Strategy Decision

Both requested strategies are delivered behind an explicit execution-mode
contract. `python_apoc` is compatible with the project Neo4j 5.21 baseline.
`native_disjoint` is enabled only when the server reports Cypher 25 / Neo4j
2026.06+ support; otherwise it fails before consuming or committing work.

## Story 1: Declare and Validate Edge Execution Modes

**As a** Pipeline Operator, **I want** a declared execution strategy and
concurrency limit, **so that** unsupported native scheduling fails safely.

Modify `src/models/schema.py` to add edge execution config (`python_apoc` or
`native_disjoint`, worker count). Create `src/loader/edge_execution.py` strategy
protocol and server-capability guard. Test defaults, invalid settings, and native
mode rejection on a 5.21 capability response. **Estimate: 5.**

## Story 2: Implement Globally Ordered APOC Relationship Batches

**As a** Data Engineer, **I want** every Python worker batch to lock distinct
endpoints in Neo4j internal-id order, **so that** concurrent MERGEs cannot
deadlock.

Add `ApocLockedEdgeWriter` in `src/loader/edge_execution.py`, using the
technical-plan Cypher: resolve endpoint pairs; collect distinct nodes; `ORDER BY
id(dist_n)`; `CALL apoc.lock.nodes(sorted_nodes)`; then `MERGE`. Preserve
preflight/no-commit semantics and test queries, lock ordering, missing endpoints,
and database failures. **Estimate: 8.**

## Story 3: Implement Native DISJOINT-BY Batches

**As a** Data Engineer, **I want** a native concurrent Cypher strategy,
**so that** supported Neo4j versions schedule overlapping endpoint resources
without parallel lock contention.

Add `NativeDisjointEdgeWriter` using implicit driver execution of a parameterized
`UNWIND $rows AS row CALL (row) { ... MERGE ... } IN $concurrency CONCURRENT
TRANSACTIONS OF $batch_size ROWS DISJOINT BY (row.source_key, row.target_key)`.
Return/report every batch status; any failed status means no offset resolution.
Test exact generated Cypher and capability gate. **Estimate: 8.**

## Story 4: Concurrently Execute Safe Lane Batches

**As a** Pipeline Operator, **I want** bounded concurrent lane execution with
durable completion results, **so that** Slice 1 offsets resolve only after its
selected strategy succeeds.

Add a bounded `LaneExecutionCoordinator` using `ThreadPoolExecutor` for
`python_apoc`; native mode submits one server-side execution and does not nest
Python concurrency. It returns per-lane success/failure without committing
offsets; the following loader integration resolves only successful batches.
Stress/mock test super-node, reversed edges, worker bound, failure isolation,
and no premature offsets. **Estimate: 8.**

## Total

**29 points**, approximately one sprint. Slice 4 follows after this is complete.
