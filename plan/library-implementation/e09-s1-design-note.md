# E09.F1.S1 design note — checkpoint A (2026-10-31)

Substrate label for every result: **native shared Docker Engine substrate** (same engine, owned-resource rules and weakenings as [E09.F1.S7](evidence/E09.F1.S7.md); AC4 of S7 stays an accepted gap). Nothing below has been built or run. S7 was accepted by independent review (round 4) but its backlog status has not been changed by the orchestrator; S1 builds on S7's frozen harness and does not modify its lane.

Scope (from [epics/e09.md](epics/e09.md) and [execution-e09.md](execution-e09.md)): installed CLI, candidate images, planned bulk (the default bulk mode). Assert source-derived graph, plan digest, every chunk exactly once in the ledger and expected offsets; inject one worker failure and prove fail-closed / replay / cleanup **without claiming distributed exactly-once writes**; capture lock-wait evidence with the S7 detector and distinguish coordinator wait counters from database lock waits.

## What the product does (read from `src/`, not yet observed live)

1. `run_planned_edge_phase` ([planned_edges.py](../../src/neo4j_graph_loader/orchestrator/planned_edges.py)) waits for the plan file (written by the in-process Kafka edge planner), validates it against the schema and the run-scoped chunk topic, starts one **dispatcher** container and `min(--plan-writers, chunks)` **writer** containers (default 10), forwards node-label readiness, and returns 0 only if the dispatcher is `complete`, the ledger holds every planned chunk exactly once with its planned row count (rows sum to the plan totals), and every writer exited 0 after `END`.
2. Failure is closed: writer `FAIL`, orchestrator `ABORT`, a protocol violation, **or a writer disconnecting while it holds a chunk** moves the dispatcher to `failed`. A failed dispatcher never grants again and never releases held resources. The CLI then aborts, stops and removes the exact containers of the phase and exits 1. Chunks already committed stay committed (no rollback); the documented recovery is a full replay, safe because relationship writes are MERGE.
3. Chunks granted concurrently are node-disjoint by construction, so a clean planned run should show **zero database lock waits** for the edge writers. The dispatcher's own waiting (`WAIT` replies, grant eligibility) is a coordinator counter and says nothing about Neo4j locks.
4. Tiny S7 smoke (3 nodes of one type, 2 of another, 3 relationships) yields 1 chunk and 1 writer, so it can neither exercise two writers nor a writer loss while another chunk is pending. **S1 needs a slightly larger deterministic fixture** (below).

## Proposed design

### Fixture
Deterministic, generated from a fixed integer seed, produced to owned token-named topics: `Person` (60), `Company` (6), `WORKS_AT` (120 relationships, each Person twice, spread over all companies, with `role` and a numeric `since` property). Run with `--plan-chunk-rows 20 --plan-writers 2` (to be confirmed against the planner's actual chunking in exploration; the aim is ≥ 4 chunks and 2 writers). Expected graph, per-relationship properties, plan digest/id, chunk list and row counts are computed in the test **from the fixture and the plan file's schema-derived rules**, not copied from run output. Whether the plan's chunk boundaries can be predicted from the fixture alone is an open question for exploration; if not, the independent check is: chunk row counts sum to 120, every chunk exactly once, ledger rows == planned rows, edge multiset equals the fixture's.

### Scenarios (one installed `graph-loader start --mode bulk` each, owned stack, explicit candidate image IDs, run directory outside git)

| ID | Scenario | Expected |
|---|---|---|
| B1 | Clean planned run | exit 0; graph == fixture exactly (60 Person, 6 Company, 120 WORKS_AT with properties); persisted ledger holds every chunk once with planned rows; `plan_id`/digest equal the plan file and the CLI log; node-topic groups committed at the log end; observer: created == observed, all ended and removed, components = preflight 1, node-loader n, planned-dispatcher 1, planned-edge-writer 2; no corrupt registry line |
| B2 | Lock-wait evidence on the clean run | S7 query window over B1: every identified edge-writer query (identified by the actual product statement text, captured in exploration) has `waiting == 0`; **positive control in the same stack**: the S7 synchronized conflict still reports `waiting ≥ 500 ms` on the same detector and Neo4j version; coordinator counters (dispatcher ledger/status wait fields) are read separately and reported as a different quantity |
| B3 | Writer loss while holding a chunk | a separate test holds an `apoc.lock.nodes` transaction on one endpoint node the first granted chunk needs; the writer's transaction blocks (observed as the writer's identified query with `waiting > 0` / `Blocked by` in `SHOW TRANSACTIONS`); that writer container is killed by exact ID (SIGKILL, ownership policy applied); the lock holder is then released. Expected: CLI exits non-zero within its bound; the dispatcher reports failure for the lost holder (CLI log); **no further grant** (no chunk beyond the in-flight set committed after the kill); partial graph is a strict subset of the fixture with no duplicate relationships and no foreign data; ledger is not persisted as complete; all application containers stopped and removed; offsets as observed; nothing of the failed attempt looks complete |
| B4 | Replay after B3 | a second installed `start` with the same fixture and topics (and the product's own flags for a stale chunk topic, as the CLI requires) completes: graph == fixture exactly, no duplicate relationships, complete ledger for the **replay's** plan, clean cleanup. Claim is "MERGE replay converges to the fixture", **not** exactly-once writes |
| B5 | Detector coverage for B3 | the blocked writer's identified query is parsed from the structured log with `waiting` ≥ 500 ms in the same window, proving the detector sees a product lock wait; and the B1 window shows zero for the same query shape |

B3/B4 depend on three things I cannot know without running: (a) the exact writer statement text, (b) how a second `start` treats node-topic consumer groups already committed by the first attempt, (c) whether the run-scoped chunk topic from a killed run blocks replay (`--replace-stale-plan-topic`, `--keep-plan-topic`, `--run-id`). So the first deliverable is an **exploration run** (like S7's `exp*` runs, red logs kept) that answers them and records the answers in the evidence before test assertions are frozen. If (b) turns out to mean a replay does not reread nodes (offsets already at the end), B4 is still valid because MERGE nodes exist; I will record exactly what the product does rather than assume.

### New code (additive; S7 files unchanged except where noted)
- `tests/installed_service/test_service_bulk.py`: B1–B5, using the S7 `stack`, `AppObserver`, `qw` (query window), `graph_observations`, `committed_offsets`, `plan_ledger`, `lock_conflict`. A fixture module `bulk_fixture.py` (deterministic generator + expected-graph computation).
- `AppObserver`/`LiveStack`: a `kill_observed(container_id)` helper that applies the ownership policy (candidate image, owned network, not pre-existing, fresh inspect), records the control action with its deadline, and reads back the exit state. S7's `kill_owned` handles only owner-labelled helper containers; the CLI's own containers are unlabelled, which review round 2 listed as outside the supported scope. This closes that for S1.
- `scripts/freeze_installed_inventory.py`: additive `service-bulk` lane entry (new file, `SERVICE_REASON` for E09.F1.S1); the lane stays `proposed` today with 0 IDs and the runner already maps `service-bulk` to `E09.F1.S1`.
- Evidence: `evidence/E09.F1.S1.md` plus manifest; run directories `~/e09-s1-runs/*` on the host; red logs retained.

### Execution plan
Exploration run(s) first (~2 × 5 min), then freeze and dry run, then **five** frozen runs at final bytes (the failure injection and lock-wait controls are timing-sensitive), then read-only postflight, a live reaper/canary control only if the reaper or ownership code changes, local ruff and the inventory check, then checkpoint B and an independent review through the same Codex session (`01a19269-…`). Expected wall time per run ~6–8 min. Hard deadlines: lane 1800 s, each CLI 240 s, replay 240 s, kill/steady-state waits 60 s each.

## What this will and will not show

Will show: installed-CLI planned bulk against an owned stack produces exactly the fixture-derived graph and an exact one-per-chunk ledger; the node-disjoint design yields zero DB lock waits for edge writers on the proven detector (with positive controls); loss of a chunk-holding writer fails the run closed, stops further grants and cleans up; a full replay converges.

Will not show: distributed exactly-once writes; behaviour at scale or with hub nodes (plan hubs, spill, multi-partition planner input); node-loader failure/restart (S5 owns recovery for each mode); Neo4j outage mid-run (S5); DLQ behaviour for bad records (S4); arm64; the shipped rolling query-log appender (S7 weakening 4 still applies: the detector is proven on the injected non-rolling appender); the lock-wait detector's source-to-sink completeness.

## Weakenings inherited from S7 that apply here
Shared Docker Engine (no fenced daemon or durable broker); unlabelled CLI containers are adopted by candidate-image + owned-network + non-pre-existence, not by creation provenance; the observer barrier proves stream currency, not Docker's cross-type event ordering; the CLI holds engine-wide socket authority; dependencies and the `neo4j:5-enterprise` tag float (image ID recorded).

## Questions for the orchestrator / user

1. **Failure injection (B3):** approve killing a *chunk-holding planned-edge-writer* while an external lock holder pins its transaction, as the single injected worker failure? Alternative: kill without a holder (timing race, not repeatable ≥ 5×). I recommend the holder approach.
2. **Fixture size:** approve 60 / 6 / 120 with `--plan-chunk-rows 20 --plan-writers 2` (adjusted if exploration shows different chunking)? It remains a "small fixture", not a load test.
3. **Replay (B4):** accept "converges by MERGE replay" as the replay claim, recording exactly how the second `start` treats node offsets and the stale chunk topic, with `--replace-stale-plan-topic` allowed only if the CLI itself requires it?
4. **Lane membership:** a new `service-bulk` frozen lane with a new test file (additive freezer change), leaving the S7 `service-harness` lane untouched?
5. **Dependency:** S1 depends on S7 whose backlog status is still "in progress"; proceed on the strength of the round-4 acceptance, leaving the status change to the orchestrator?

## Decisions recorded (2026-10-31, user answers to the five questions)

1. **Failure injection:** NOT the lock-holder variant. One chunk-holding planned-edge-writer is SIGKILLed by exact ID shortly after the dispatcher logs its first `planned-done`, with no external lock holder. Consequence: B5 (blocked-writer query wait) is dropped; the kill timing is a race, so it is repeated 5 of 5 runs and the assertions are limited to outcomes that hold whether or not the writer held a chunk at that instant (the CLI aborts when any writer exits non-zero before the plan completes); whether it held a chunk is recorded from the dispatcher log, not assumed.
2. **Fixture:** approved 60 / 6 / 120 with `--plan-chunk-rows 20 --plan-writers 2` (labels `Worker`/`Employer`, type `EMPLOYED_BY`, so earlier harness tests' `Person`/`Company` data is untouched).
3. **Replay claim:** "converges by MERGE replay", not exactly-once.
4. **Lane:** NOT a new `service-bulk` lane. The new tests go into the existing `service-harness` lane (freezer file tuple extended). Consequence: the `service-harness` selection grows from 65 IDs and its frozen bytes change, so S7's `final4-1..5` no longer describe the current lane; S7 evidence stays valid for its own frozen bytes, and S1's five runs execute the whole enlarged lane (S7 IDs included) at the new frozen bytes, which re-proves S7's IDs there as a side effect.
5. **Dependency:** proceed; S7 status is left to the orchestrator.
