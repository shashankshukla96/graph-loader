# E09 exit-gate independent review — 2026-10-31

Reviewer: Codex CLI session 01a19269 (read-only), resumed per the E09 review convention. Prompt: E09 exit review after the D08 plan reset. Output reproduced verbatim below.

**Verdict: ACCEPT WITH CHANGES. Keep E09 open until the coverage and assertion gaps below are resolved.** The final run binds to the reviewed bytes; it does not establish every claimed acceptance criterion.

1. **Major — S5 excludes supported modes required by its acceptance criteria.**  
   [e09.md:123](/Users/shashank/Desktop/Code/Graph/graph-loader/plan/library-implementation/epics/e09.md:123) requires graph, commits and recovery behavior for **each D02-supported mode**. [test_service_kill_and_outage.py:13](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/test_service_kill_and_outage.py:13) explicitly excludes classic and lease bulk; [S5 evidence:42](/Users/shashank/Desktop/Code/Graph/graph-loader/plan/library-implementation/evidence/E09.F1.S5.md:42) confirms this. These modes have distinct orchestration, so planned-bulk replay does not prove their behavior.  
   **Required:** execute bounded failure/replay cases with graph, offset and cleanup assertions for those modes, or obtain an explicit scope amendment before marking S5 complete.

2. **Major — the deferred classic ordering coverage never ran.**  
   [S3 evidence:25](/Users/shashank/Desktop/Code/Graph/graph-loader/plan/library-implementation/evidence/E09.F1.S3.md:25) defers selected classic `order_by` and precedence cases to F2. [F2 evidence:47](/Users/shashank/Desktop/Code/Graph/graph-loader/plan/library-implementation/evidence/E09.F2.S1.md:47) then excludes bulk/planned/classic ordering; its fixture hardcodes stream mode at [order_fixture.py:28](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/order_fixture.py:28). Meanwhile, [BULK_LOAD.md:146](/Users/shashank/Desktop/Code/Graph/graph-loader/docs/BULK_LOAD.md:146) advertises `order_by` for planned, lease and classic. Recording “not covered” does not exclude a supported guarantee under D02.  
   **Required:** close the deferred ordering cases with installed executions, including controlled older-after-newer application and offsets, or explicitly approve a narrower release contract. Timestamp ordering also remains unexecuted and must remain identified as such.

3. **Major — graph assertions can hide conflicting duplicate relationships.**  
   [order_fixture.py:74](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/order_fixture.py:74) and [test_service_ingest_dlq.py:45](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/test_service_ingest_dlq.py:45) collapse physical relationships into dictionaries keyed by endpoint pair. Later rows overwrite earlier rows. Fake-driver reproductions supplying two conflicting relationships returned the expected single correct relationship in both inspectors:
   ```text
   ORDER_CONFLICTING_TWO_EDGES_PASSES_ONE_EXPECTED True
   DLQ_CONFLICTING_TWO_EDGES_PASSES_ONE_EXPECTED True
   ```
   Consequently, the ordering and DLQ graph checks can pass with extra, incorrect relationships.  
   **Required:** assert physical relationship counts and pair uniqueness before projecting properties; rerun affected live cases.

4. **Major — inspection failures can become evidence of absence.**  
   [test_service_ingest_dlq.py:50](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/test_service_ingest_dlq.py:50) converts every offset-inspection exception to zero, supporting “nothing parked” at line 133. [dlq_fixture.py:116](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/dlq_fixture.py:116) returns partial topic contents after its deadline without requiring the captured watermarks to be reached. Fake reproductions produced:
   ```text
   INSPECTION_ERROR_IS_ZERO 0
   READER_WATERMARK_2_RETURNED 1
   ```
   Similarly, [test_service_last_value_order.py:249](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/test_service_last_value_order.py:249) accepts `None` from a helper that catches arbitrary exceptions as evidence that deleted-topic commits disappeared.  
   **Required:** propagate inspection failures, distinguish confirmed missing resources from errors, and require complete watermark coverage before drawing absence or envelope-completeness conclusions.

5. **Minor — “no grant after loss” is not established by the assertion.**  
   [test_service_load_bulk.py:300](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/test_service_load_bulk.py:300) checks `grants == completed + active`. Granting another chunk increments both grants and active, preserving that equality. This cannot establish temporal ordering, although [S1 evidence:23](/Users/shashank/Desktop/Code/Graph/graph-loader/plan/library-implementation/evidence/E09.F1.S1.md:23) claims it does.  
   **Required:** add ordered failure/grant evidence or narrow the evidence to the live outcomes actually asserted, identifying any separate unit proof.

6. **Minor — the short Neo4j-outage case lacks the required three repetitions.**  
   [execution-e09.md:33](/Users/shashank/Desktop/Code/Graph/graph-loader/plan/library-implementation/execution-e09.md:33) requires three executions for timing-sensitive cases. The unparameterized outage test at [test_service_kill_and_outage.py:134](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/test_service_kill_and_outage.py:134) has two retained green executions with matching current test/helper hashes: `s5/final-1` and `f2/final-1`. The earlier green exploration used different hashes. The twice-parameterized kill/stop cases have four executions across those two passing lanes.  
   **Required:** complete a third outage execution at the applicable final bytes and record the repetition mapping.

I independently verified these retained-artifact results:

- `f2/final-1`: **134 selected == 134 collected**, all 402 setup/call/teardown outcomes passed; no skips, xfails, exclusions or errors. Duration: **1,237.43 seconds**.
- Inventory SHA matches the working tree: `98cb724eec6ce11fe2aa74f5bfb1ab6657a19f6dc45517fc19360d247f8a9c9d`.
- **44/44 staged files, 6/6 recorded runner tools and 128/128 frozen inputs** match current files.
- Postflight is clean: no registry corruption/truncation, reaper errors or foreign differences. Foreign snapshots are byte-identical, SHA `201030875d26ca3dc31f970faa604616f8a5adc9efe2066cd7349c806e1e80e3`.
- Installed safety evidence reports **1,335/1,335** passes, with current staged-file hashes. My local read-only checks passed **626 tests** and ruff.

Inventory preservation holds against **HEAD**: earlier IDs and dispositions remain unchanged and ordered; only `service-harness` changed. Against **e949225**, five bulk IDs were renamed from `test_service_bulk.py` to `test_service_load_bulk.py`; their mapped dispositions match and remaining IDs retain order. The rename is disclosed at [S1 evidence:34](/Users/shashank/Desktop/Code/Graph/graph-loader/plan/library-implementation/evidence/E09.F1.S1.md:34), so strict “all IDs unchanged” is false against that baseline, but this is not an undisclosed coverage deletion.

O1–O8 have recorded outcomes or restricted dispositions consistent with D02. Arrival nondeterminism is explicitly documented for U3. Their coverage remains subject to findings 2–4. Fixed polling intervals and the shared 32 MiB query-log bound remain fragile, but the bound fails closed; the retained final run does not show overflow.

The `native_disjoint` Cypher 25 incompatibility with Neo4j 5.26 remains visibly excluded and assigned to E10.F1.S2. The foreign-baseline change contains exactly the two reported image additions, `postgres:16.15` and `testcontainers/ryuk`; I verified the snapshot delta, **not who pulled them**. D08’s unfenced shared-engine limitations and the accepted E11 arm64 deferral are not new E09 defects. No live-host state was inspected.

After the required changes, freeze the resulting inventory and retain a new complete final-byte lane run before closing E09.