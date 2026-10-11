# Phases 5 and 6: final report (code complete; e2e with the owner pending)

Branch: `feat/streaming-observability`, local only and never pushed. It is 184 commits ahead of `main`.
Checks on the integration branch after the last merge (`21f6f9f`), each run once:
**1609 unit tests passed** (29 integration or smoke tests deselected), `make lint`
clean, and `make validate-workflows` (YAML plus actionlint) clean.
Following the owner's instruction, no Docker, integration or end-to-end runs were done tonight.

## What was delivered

| Slice | What it adds | Completion doc |
| --- | --- | --- |
| P6-S1 CI | Blocking `ci.yml` (lint, unit tests, builds of the node, edge and clock images) plus a nightly or manual `ci-integration.yml`; zero-reformat ruff baseline; `make lint`, `test-unit`, `build-images`, `validate-workflows` | `plan/v6/completion_slice-1.md` |
| P5-S2 Malformed-record DLQ | Per-type `<topic>.dlq` (opt in through the YAML `dead_letter` block), versioned envelope, fail-closed offsets, `replay-dlq` | `plan/v5/completion_slice-2.md` |
| P6-S2 Core metrics | Prometheus endpoint on :9464 in every process, a catalogue of about 23 series, per-flush snapshots in `GRAPH_LOADER_TIMING_DIR`, metrics totals in the IMDb report | `plan/v6/completion_slice-2.md` |
| P6-S3 Metrics delivery | `docker-compose.monitoring.yml` (Prometheus Docker service discovery plus Grafana), a 15-panel dashboard, example alerts with promtool tests, `make monitoring-up/down/validate`, `docs/MONITORING.md` | `plan/v6/completion_slice-3.md` |
| P6-S4 Stress and regression tests | Star topology at 100k (CI) and 1M (nightly); planned and lease modes show 0 lock waits; correctness regressions; lock-wait helpers proven against a real injected wait | `plan/v6/completion_slice-4.md` |
| P5-S1 Streaming leases | Streaming defaults to the zero-wait lease fleet, including self-referencing types; supervised lease server with time-based fencing (transaction timeout plus quiet period); the clock only through the deprecated `--stream-clock-rotation`; fixed lease-mode last-write-wins | `plan/v5/completion_slice-1.md` |
| P5-S3 Row-level DLQ | Rows with missing or ambiguous endpoints are dead-lettered and the rest of the batch commits; the fast path is unchanged; same-pair ordering within a flush; default in streaming, bulk opt-in (`route_missing_endpoints_in_bulk`); planned mode stays fail-closed | `plan/v5/completion_slice-3.md` |
| P5-S4 Retry consumer and parking lot | Retry consumer with backoff (pause and resume, never blocking `poll()`); lease-fenced retry writes; durable parked-pair index for ordering across flushes; optional `order_by` conditional SET; `<topic>.dlq.parked` (never dropped) with `replay-dlq --parked`; `graph_loader_dlq_backlog` gauge | `plan/v5/completion_slice-4.md` |
| P6-S5 Graceful shutdown | SIGINT/SIGTERM handled in bulk and stream mode; every exit path stops exactly the containers it started (no orphaned node loaders); prompt shutdown during every bulk wait; fixed exit codes, clock handler timing and flaky tests; Neo4j test pin 5.26 (minimum 5.23 documented); operator guide "Stopping a run safely" | `plan/v6/completion_slice-5.md` |

Binding decisions, their amendments, and who owns each follow-up: `plan/v5-v6-orchestration.md`.

## E2E checklist for the morning (to run together)

Exact commands are in each slice's "Pending owner e2e" section:

1. **Streaming leases (P5-S1).** Run `examples/social` in `--mode stream` and check `social_graph.py verify` reports exact_match. Kill the lease server mid-stream; expect 0 lock waits and an exact graph afterwards. The P5-S1 First Mate already ran this once on scratch, before later slices landed. Re-run it on the integrated branch.
2. **Row-level DLQ (P5-S3).** Publish edges before their nodes in a stream. The unresolved rows should land in `<topic>.dlq` with v2 envelopes, the resolved rows should commit, and there should be 0 lock waits.
3. **Retry consumer (P5-S4, `plan/v5/completion_slice-4.md` §"Pending owner e2e (Story 9)").**
   * A node that arrives late: its edge is retried and written.
   * A node that never arrives: its edge is parked, and `replay-dlq --parked` restores it.
   * Ordering is preserved across flushes, and with `order_by`.
   * 0 lock waits, and the backlog gauge rises and drains.
4. **Shutdown (P6-S5, `plan/v6/completion_slice-5.md` §"Pending owner e2e").** Send SIGTERM and SIGINT (including a double signal) to real bulk and stream runs at each phase: launch, node drain, plan wait, dispatcher, and stream supervision. `docker ps --filter label=app=graph-loader` must be empty afterwards.
5. **Monitoring (P6-S3/S5).** Start a real fleet (edge loaders, lease server, retry consumer), then run `make monitoring-up`. Check that the Prometheus targets are up, including the CLI with `GRAPH_LOADER_METRICS_BIND_ADDR=0.0.0.0`, and that the Grafana dashboard renders in a browser.
6. **Regression of the bulk path.** Load the IMDb 20k or 40k subset with `--edge-plan`. Expect the same timings (about 60 s and 108 s) and 0 lock waits.
7. **Image rebuild first.** Several slices changed loader code, so run `make build-images` (or leave out `--skip-image-build`) before any Docker run.

## Known limitations, all documented in the completion docs

* In streaming, if any edge-loader container crashes, the whole fleet stops. Recovery is a process restart, which is safe but not highly available. The host-side lease readiness probe assumes Linux bridge networking and is likely broken on Docker Desktop.
* Replaying a record from the parking lot can overwrite a newer write unless `order_by` is configured. DLQ counters are at least once.
* A Docker call already in flight (an image build or a container launch) is not cancelled on shutdown. The parked-pair index rebuild cannot be interrupted promptly.
* Some P5-S4 reviewer notes (N-items) are marked "to verify" in its completion doc.
* The DLQ reason code for records held behind a parked pair is `blocked_by_earlier_unresolved_row`, not the `blocked_by_parked_pair` named in the stories.
* Nothing was pushed, so CI has never run on GitHub. Branch protection needs the required checks `lint`, `unit-tests` and `build-images (node|edge|clock)`.

## Gap fixes

Branch `fix/test-plan-gaps` (from `feat/streaming-observability`) closes four gaps found while writing `plan/v5-v6-functional-test-plan.md`. Commit shas are those of the branch before the final review-fix commit; see `git log`.

1. `95bf22a` `make build-images` now tags `graph-loader-*:$(IMAGE_TAG)`, default `latest`, matching the CLI default. USAGE updated.
2. `11517a0` New alert `GraphLoaderDlqBacklogHigh` on `graph_loader_dlq_backlog` (above 100 for 15m, example threshold), promtool tests, a Grafana "DLQ backlog" panel, MONITORING.md rewritten. The routing/parked rules were kept as activity signals. Known limitation found in review: the gauge's parking-lot term never decreases (infinite retention, replay does not trim), so the alert latches once enough records have been parked; documented.
3. `9f01f6d` USAGE section 12 no longer claims a clean SIGTERM leaves node loaders running (verified in `src/cli.py`); the stream-without-relationships case remains documented. Stale MONITORING Story 6 note fixed.
4. `df8c947` Example schema `reconnect_timeout_ms` 0 to 60000 (the 0 predated the Story 7 bound, no documented reason); one test adjusted.
5. Review-fix commit: latching documented, test plan updated for the above.
6. Latching fix (orchestrator decision): `graph_loader_dlq_backlog` gained a bounded `queue` label (`dlq` = retry work that should drain, `parked` = terminal parking-lot size). `GraphLoaderDlqBacklogHigh` now uses `max by (kind, name) (...{queue="dlq"}) > 100 for 15m` and clears when retries drain; parked records stay covered by `GraphLoaderParkedLotGrowing`. Dashboard panel shows both queues. The item-2 latching caveat above is superseded.

## Test phase fixes

Fixes merged on `feat/streaming-observability` while the functional test plan was being run (runner1 to 3, retestA to C). Shas are from `git log --oneline 218f9ab~20..03de1a6`.

1. **BUG-1** `8c58e18`: the retry consumer had no real writer; a `RetryWriter` adapter over `ApocLockedEdgeWriter` is now wired in (`6f94587` asserts the `main()` wiring).
2. **BUG-2** `636bba3`: the retry writer always builds the endpoint classification query, and `main()` uses `build_retry_base_writer`.
3. **MET-07 periodic snapshots** `d0ac4eb`: metrics snapshots are refreshed atomically while a process runs, not only at exit. Follow-ups `1cfb077` and `483657c` forward the interval to containers, add the CLI snapshot path helper, a 1 s interval floor and a bounded stop/final-write lock.
4. **Stream node leases** `16d7a97`, `ebbd429`, `3b1c4cb`: stream node writes are lease-fenced against relationship writers, the lease server starts before the node loaders, and USAGE is updated. `6871650` makes the CLI supervise stream node loaders (rc 1 when one dies); `edeec73` and `253cb6d` prime the whole lease client pool so a stop-time drain never reconnects.
5. **Shutdown fixes D1 to D6:** D1 `08f50ba` (containers run with `init=True`) and `c9489d4` (SIGTERM handler installed first); D2 `c9489d4` (a stopping node loader drains buffered work only); D3 `32d2b5b` (409 removal-in-progress treated as success); D4 `2083bfb` (`--replace-stale-plan-topic`); D5 `8e882b4` (operator stop logs WARNING, planned writers exit 0 between chunks); D6 `d90cfb8` (unfinished planner processes are terminated on stop and spill/progress files cleaned).
6. **Shutdown fixes F1/F2** `856030d`: F1 is a bounded 5 s stop drain (`stage=edge-drain-abandoned`, WARNING, exit 0, no offset commit), and the lease server holds a disconnected holder's leases for `T + margin` measured from the disconnect (the **disconnect-time lease fence**); F2 is one stop per container, with loaders keeping a flag-only SIGTERM handler until exit. Review fixes `aff9c1d`: SIGTERM set to `SIG_IGN` as the last step, node loaders get the drain budget (`stage=node-drain-abandoned`), a `LeaseDrainAbandoned` exception, docs.
7. **Retry drain batching** `a74dae5`: batched, lease-coordinated drain with contiguous per-partition commits; the retry drain rose from about 22 to about 13k rows/s. New settings `loading.retry_consumer.max_records_per_partition`, `max_batch_records`, `write_workers`. Review fixes `03de1a6`: bounded batches, held (not-yet-due) records, write-failure isolation (`write_failed` escalation after 3 isolated failures) and shutdown.
8. **Normalized-key lease cell fix** (review finding M15, in `a74dae5`/`03de1a6`): the retry batch's lease cell is computed from the normalized record keys, not the wire keys (test `test_lease_cell_is_routed_by_the_normalized_record_keys_not_the_wire_keys`).

Also merged: `4363778` untracked `var/rejections` runtime files, `33f84c2` legacy file sink no longer logs `route_to_dlq`, `a3face6` and `bab78fe` query-log compose fixes, `9458b34` ephemeral metrics port in the clock test.

Follow-up on `fix/docs-and-topic-preflight`: `start --mode stream` now checks that every declared node and relationship topic exists before any side effect and exits 1 naming the missing ones (retestA root cause 3); the functional test plan gained ENV-06, P5-RETRY-22/23 and P6-SHUT-23/24, and its ROW-05 and stage-name text was corrected.
