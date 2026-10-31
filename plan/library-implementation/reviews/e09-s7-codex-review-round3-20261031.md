# E09.F1.S7 — independent review, round 3

**Verdict: accept with repairs.** The **60 IDs × five passing runs and their frozen identities are verified**. R2 remains partial: the barrier introduces an unchecked volume-removal path, and network attachment still authorizes removal of an unlabelled foreign container. A short registry write followed by teardown can also defeat recovery.

Read-only review; no file changes, SSH, network or Docker execution.

## Findings

| ID | Severity | File:line | Evidence | Required fix |
|---|---|---|---|---|
| N1 | major | [live_cli.py:208](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/live_cli.py:208) | `barrier()` ignores the returned volume identity/labels and unconditionally removes its name in `finally`. With a fake returning an existing differently labelled volume, the barrier timed out **and deleted that foreign volume**, recording `removed`. This bypasses the new ownership policy. | Validate the returned identity and inspect the current owner label before removal. Refuse mismatches and preserve the failure. Cover existing-name and replacement-volume cases. |
| N2 | major | [live_stack.py:48](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/live_stack.py:48), [live_cli.py:215](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/live_cli.py:215), [service_lane.py:60](/Users/shashank/Desktop/Code/Graph/graph-loader/scripts/service_lane.py:60) | Using the actual `Registry.write()` with mocked file operations, I injected a short `created` write after successful barrier-volume creation, then appended the normal teardown `close` row. The partial record became a malformed interior line. `read_registry()` raised `JSONDecodeError`, preventing recovery of the preceding valid intent; the volume remained. Barrier cleanup is outside the scope protecting that registry write. | Poison the writer after a partial append, and let recovery retain valid preceding records while marking corruption unclean. Cover short write followed by fixture teardown and verify exact owned-resource recovery. |
| N3 | major | [ownership.py:25](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/ownership.py:25), [live_cli.py:161](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/live_cli.py:161) | After `start()` verified the network label, I attached an **unlabelled foreign-monitor container** through a fake network event. It was registered as observed and removed by `reap()`. Network attachment proves attachment, not creation ownership. The initial foreign snapshot is not used as a protected-ID set. A later snapshot can detect this destruction but cannot prevent it. | At minimum, prohibit adoption/removal of pre-run foreign IDs. Require positive CLI/candidate provenance before adopting unlabelled containers. Keep same-user label impersonation explicitly within the accepted unfenced-substrate limitations. |

The direct H3 helper removal at [test_service_harness.py:56](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/test_service_harness.py:56) also still lacks the claimed fresh owner check. It removes the create-returned container ID, so this is narrower than N1, but the statement that every cleanup path applies the new policy is inaccurate.

## R1–R8: repaired?

| Prior finding | Status | Verification |
|---|---|---|
| **R1 — delayed observer stream** | **Repaired for the reproduced FIFO-delivery race** | [live_cli.py:208](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/live_cli.py:208), [test_service_harness.py:169](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/test_service_harness.py:169). In five independent delayed-stream reproductions, the barrier consumed the late candidate creation before shutdown and reported it unobserved. Reader termination is now asserted. Completeness remains conditional on the event-source assumptions discussed below. |
| **R2 — conflicting ownership and unchecked cleanup** | **Partial** | [live_cli.py:186](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/live_cli.py:186) now refuses the original differently labelled container on the owned network; my reproduction performed no removal. `kill_owned`, `remove_owned` and `close` check labels. N1 and N3 remain. |
| **R3 — inspect error aborts outer reaper** | **Repaired** | [service_lane.py:99](/Users/shashank/Desktop/Code/Graph/graph-loader/scripts/service_lane.py:99), [verify_installed.py:566](/Users/shashank/Desktop/Code/Graph/graph-loader/scripts/verify_installed.py:566). The original first-container inspect `APIError` now produces an error tuple while the second container is removed. Partial results survive. Docker, requests/OSError and urllib3 failures are covered; postflight errors keep the result unclean. N2 is a separate registry-recovery defect. |
| **R4 — reusable termination** | **Repaired at the disclosed helper-container scope** | [live_stack.py:311](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/live_stack.py:311). `kill_owned()` checks ownership, records the action/deadline and reads back terminal state. H5 uses it five times. Unlabelled CLI application termination remains outside this helper’s supported scope. |
| **R5 — conflicting DDL completions** | **Repaired** | [structured_query_log.py:83](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/structured_query_log.py:83). Both original reproductions—different waiting time, and pipelined followed by schema runtime—now raise `RuntimeError`. The repeat must agree with the first completion’s wait and schema runtime. |
| **R6 — missing-inventory `UnboundLocalError`** | **Repaired** | [verify_installed.py:619](/Users/shashank/Desktop/Code/Graph/graph-loader/scripts/verify_installed.py:619). The same missing-inventory invocation returns 1 and reports `FileNotFoundError`; it no longer masks the error. |
| **R7 — baseline omissions** | **Repaired** | [capture_baseline.py:36](/Users/shashank/Desktop/Code/Graph/graph-loader/plan/library-implementation/tools/capture_baseline.py:36). Actual Git-selection evaluation includes all three previously omitted files and the new `ownership.py`. |
| **R8 — Kafka persistence wording** | **Repaired** | [test_service_harness.py:277](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/test_service_harness.py:277). Code and evidence now state end-offset persistence only, explicitly excluding payload and committed-offset readback. |

## Barrier and ownership stress results

The barrier fixes consumer lag on an ordered stream. It does **not** detect omitted events or establish that every earlier operation has published its event. In five reordered/late-publication fake runs, candidate events placed after the marker were discarded at shutdown while equality, error and stopped-thread checks passed. This demonstrates the dependency on causal publication order; it does **not** demonstrate that Docker 29 actually reorders those events.

The subscription includes `volume`, and the code requires `Actor.ID == volume_name` for both `create` and `destroy`. The local SDK forwards those filters without excluding volume events. Five passing live receipts are consistent with that event shape. Raw barrier events and Docker 29 server implementation evidence are not retained locally, so I could not independently establish the server’s ordering/delivery guarantees.

Barrier failure recovery behaved as follows:

- Successful creation followed by registry failure leaves a volume locally; the outer reaper recovered it from a valid intent.
- A lost create response likewise recovered through the intent.
- Removal failure leaves the `created` row pending; the outer reaper successfully retried.
- A short registry append followed by teardown defeats that recovery, as N2 shows.
- A foreign volume at the barrier name is deleted without an ownership check, as N1 shows.

The ownership policy is consistently implemented as **matching label, or unlabelled attachment to a trusted network**. It is not creation-provenance verification. Matching-owner labelled recorded containers are accepted even off the owned network; that remains a label-trust assumption.

The outer reaper caches `network_labels` before container and volume cleanup. A fake changing network authority afterward caused both the container and network to be removed using cached authority. I did not establish that such a label change is possible for the same native bridge-network ID through supported Docker operations. Both implementations still have an inspect/remove interval; neither provides atomic protection against concurrent replacement or daemon changes. Those limits must remain distinct from verified non-interference.

## Claims verified

| Claim | Checked result |
|---|---|
| Final runs | `final3-1..5`: exact **60 frozen IDs**, in order; all setup/call/teardown outcomes passed |
| Skips/exclusions | None; no xfails, deselections, receipt errors or approved exclusions |
| Durations | **258.35, 259.53, 262.68, 259.47, 263.22 seconds** |
| Resources per run | **16 containers**—11 created plus five observed—**one network, four volumes** |
| Cleanup receipts | All report clean postflight, no reaped/refused/error entries, and empty fixture cleanup failures |
| Foreign snapshots | All before/after files match SHA-256 `be83bde7595f92f5503151378bc90ad362ceb56cf683115c86324bd389513dd9` |
| Frozen inputs | Five identical **115-file** manifests, SHA-256 `182ef13d0cc470cd1411f5438080e75d9e1c4fe4f2b6283b2d586d764e869683`; every hash matches current bytes |
| Build/overlay closure | The 78 build inputs and 39-entry overlay union exactly equals the frozen map |
| Inventory | SHA-256 `8f47c97a5420a64d59ea4d5ba98a012bae7767bd1811340b7af1e3df0e61ee94`, matching all five results |
| Wheel | Unchanged SHA-256 `09b8b7fc836e2c93d74ef66ed132c5bff3b94fb4c66a20823933c92fe6a78226` |
| Runner tools | Recorded hashes match current files in the live and regression results |
| Retained-file manifest | **411/411 files** match size/hash; no missing or unlisted retained files |
| Fresh collection | Independently reproduced **4,364 IDs / 139 files**; freezer `check()` matched |

The pre-repair inventory copy hashes to the prior round’s `66f08114…` inventory. All **4,312 pre-round IDs remain in order**, with unchanged existing dispositions and baseline entries. There are 52 additions: 24 service-lane IDs and 28 checkout reaper-control IDs. **Only `service-harness` changed among lane definitions.** HEAD IDs are also preserved.

The retained regression results contain the exact current lane selections, successful command receipts and matching inventory/runner identities:

| Installed lane | IDs |
|---|---:|
| service-helper-unit | 41 |
| core | 454 |
| safety | 1,335 |
| cli-runtime | 636 |
| public-contracts | 269 |
| writer-runtime | 540 |
| orchestration-runtime | 282 |
| example-contracts | 116 |

The envelope result contains **134 unique passing cases**, in the exact order generated by current `process_cases()`, with `complete=true` and guard configuration restored. Its SHA-256 is `bae925d108d0b97e001ccfd27dedd1c1bfb0701f44959597ea717a014c3d4d3b`, matching the runner receipt.

`reaper-control-2.json` records exactly five removals, truncated-registry/unclean status, an untouched canary, detected canary mutation and no leftovers. `postflight-final3.json` records the stated clean checks.

Independent local checks: **108 passed, seven filesystem-dependent cases deselected**; ruff passed. The retained broader log honestly reports **521 passed and seven live-configuration setup errors**, rather than a clean full-suite pass. New artifact credential-pattern matches were synthetic test IDs, not apparent runtime credentials.

## Claims I could not verify

- Docker 29 cross-type causal event ordering, loss behavior and raw volume-event identity; no server source or raw event trace was retained locally.
- The host-only `dry-9` execution and its claimed digest.
- Actual current host resources, image contents or daemon continuity.
- Raw graph, offset, ledger, query-log and container-inspection observations underlying the passing live assertions.
- Full per-case regression/envelope subprocess evidence beyond the retained aggregate results; I did not re-execute those installed paths.
- Universal foreign-resource safety, given N1/N3 and the acknowledged unfenced substrate.

AC4 remains explicitly recorded as unmet and user-accepted. This review does not reopen that gap.

## Verdict

**Accept with repairs.**

The new execution counts, identities and regression summaries are substantiated. Do not claim R1–R8 are all fully resolved or close S7 until the unchecked barrier removal, registry-corruption recovery and foreign-container adoption paths are repaired and revalidated.