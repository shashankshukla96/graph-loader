# Slice 5: bulk topic precedence (later-listed wins) and stream refusal

Depends on S2; planned part uses S3. Files: `src/cli.py`, `src/orchestrator/relationship_bulk_monitor.py`,
`src/orchestrator/dependency_manager.py`, `src/orchestrator/docker_service.py` (group names), `src/models/schema.py` (check only), tests.

## Feasibility summary (honest)
* planned: exact, free (S3.2).
* bulk classic and bulk lease: exact, by running a precedence type's topics as sequential sub-stages in list order; each sub-stage subscribes one topic, drains to that topic's captured boundary, reaches zero lag, stops, then the next topic starts. Other types in the same stage keep running concurrently. Cost: no cross-topic parallelism inside a type (mitigated by `replicas` within a topic).
* stream: not achievable. Refuse.

## Story 5.1 Sub-stage planning
As an operator, I want `topic_conflict: precedence` to give a deterministic winner in bulk.
Technical: the relationship stage builder (cli.py ~519, 616, 1167; bulk dependency phases) expands a precedence type into N ordered sub-stages `(type, topic_i)`. Group `{prefix}-{type}-t{i}-{run_id}`. Boundary for all topics is still captured once, up front. The bulk monitor's expected partitions and zero-lag gate are per sub-stage; commit offsets are per group (committed offsets are not shared between sub-stage groups, so a rerun with a new run_id re-reads; same as today).
Lease mode: one lease fleet per run is unchanged; sub-stages only change which loader containers exist. Transactions stay node-disjoint under leases, so zero lock waits.
Acceptance: unit: stage plan for `topics [a,b,c]` yields a then b then c; other types unaffected; replicas of sub-stage i = `edge.replicas`; SIGTERM/failure path stops exactly the containers started (P6-S5 rule).

## Story 5.2 Correctness proof
Acceptance (Docker, nightly): pair P in a (v1), b (v2), c (v3) -> r.v == v3 in classic bulk and lease bulk; list reversed -> v1. Pair only in a survives. Property merge: a has {x,y}, b has {y}: y from b, x from a.

## Story 5.3 Stream refusal and modes matrix
`start --mode stream` with any `precedence` type fails before any container starts with an actionable message ("use order_by, or topic_conflict: arrival, or run in bulk"). `arrival` warns. Planned mode with `precedence` needs no extra step.
Acceptance: CLI unit tests for each (mode x rule) cell in slices.md section 2; no containers launched in the refusal case (mock docker service).

## Story 5.4 Reporting
Stage timing report lists each sub-stage; metrics stay `{kind,name=<type>}` (no sub-stage label).
Acceptance: report fixture shows per-topic rows under the type.

Test plan: unit for 5.1/5.3/5.4; Docker bulk e2e for 5.2 (`tests/test_relationship_bulk_e2e.py` pattern).
