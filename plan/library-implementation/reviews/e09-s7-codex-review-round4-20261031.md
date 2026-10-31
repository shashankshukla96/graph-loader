# E09.F1.S7 — independent review, round 4

**Verdict: accept for the recorded shared-engine S7 scope.** N1–N3 and the H3-helper/cached-network-removal repairs are verified. No blocker or major remains in the executed path. Two non-blocking limitations remain for broader helper reuse and registry-corruption recovery.

Read-only review; no files modified, and no SSH, network or Docker execution.

## Findings

| Severity | File:line | Evidence | Fix |
|---|---|---|---|
| minor | [live_cli.py:225](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/live_cli.py:225), [live_cli.py:213](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/live_cli.py:213) | Different observers sharing an owner reuse `…-barrier-1`. An already subscribed second observer can cache the first observer’s marker events. In five fake repetitions, those cached events allowed its barrier to pass without fresh marker delivery. **The recorded lane uses one observer per owner and is unaffected.** | Before supporting multiple observers per owner, use an instance/barrier nonce or enforce the single-observer restriction. This is outside the executed concurrency scope. |
| minor | [service_lane.py:67](/Users/shashank/Desktop/Code/Graph/graph-loader/scripts/service_lane.py:67) | Malformed JSON and non-dict JSON are counted as corrupt, allowing valid surrounding rows to survive. A valid JSON object missing `owner`, such as `{}`, instead makes the entire registry fail owner validation. My reproduction returned `ValueError`; recovery did not proceed. **It fails closed, without a false clean result.** | For broader corruption recovery, validate row structure before accepting it into owner accounting, mark schema-invalid rows corrupt, and continue rejecting conflicting valid owners. This exceeds the repaired short-write case. |

Neither finding invalidates the retained five-run proof.

## Per-finding status

| Prior finding | Status | Code and reproduction |
|---|---|---|
| **N1 — unchecked barrier removal** | **Repaired** | [live_cli.py:227](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/live_cli.py:227), [live_cli.py:239](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/live_cli.py:239). An existing foreign volume and a replacement with another owner both survived; neither was removed. A failed `created` write now triggers ownership-checked cleanup, leaving no barrier volume. |
| **N2 — short write followed by teardown defeats recovery** | **Repaired for the reported fault** | [live_stack.py:49](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/live_stack.py:49), [service_lane.py:60](/Users/shashank/Desktop/Code/Graph/graph-loader/scripts/service_lane.py:60). Using the actual writer with mocked file operations, a short append was truncated before later rows. When truncation failed, `broken=True` prevented subsequent writes and recovery retained the valid intent plus a truncated marker. Malformed interior-line recovery also worked. |
| **N3 — foreign monitor adopted through network attachment** | **Repaired within the disclosed provenance model** | [live_cli.py:133](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/live_cli.py:133), [live_cli.py:194](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/live_cli.py:194), [service_lane.py:148](/Users/shashank/Desktop/Code/Graph/graph-loader/scripts/service_lane.py:148). A late non-candidate monitor and a pre-existing candidate-image container were refused and preserved. The outer reaper also protects pre-run IDs. |
| **H3 helper lacks fresh owner check** | **Repaired** | [test_service_harness.py:57](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/test_service_harness.py:57). A fake whose label changed during reload was refused; `remove()` was never called. |
| **Cached labels authorize network removal** | **Repaired for network removal** | [service_lane.py:166](/Users/shashank/Desktop/Code/Graph/graph-loader/scripts/service_lane.py:166). Repeating the changed-label fake now refused the network instead of removing it. Earlier network authority still governs container decisions; atomicity limits remain below. |

The newly retained raw observer/barrier receipts also repair the previous retention note.

## Attempts to break the new rules

**Protected IDs and stack events.** Ordinary Neo4j/Kafka container events were ignored by the observer when those containers had not been adopted. An owned-network connection event for a protected stack ID was refused and never removed. H4’s `refused == {}` remains appropriate for its stable, normal execution: unexpected network activity makes the proof fail conservatively. All five retained observers have an empty refused set.

**Foreign candidate created after observer start.** A new unlabelled container using a candidate image on the owned network was still adopted and removed in my fake. Image, network and non-pre-existence cannot distinguish it from a CLI-created container. This is **an inherent limitation of the accepted unfenced provenance model**, now explicitly disclosed—not an outstanding N3 repair or proof of creation authorization.

**Missing image metadata.** `images=None` permits the older network-only rule; an empty set refuses unlabelled containers. Consequently, `images=… or None` disables filtering when metadata is absent or empty. I reproduced that difference. The actual runner path supplies four validated candidate identities, and the observer passes its candidate set explicitly. I classify this as an **unsupported-caller/hardening limitation**, not a reachable failure in the recorded S7 path.

**Registry locking.** Five concurrent short-write/close repetitions preserved readable rows after successful rollback. Five failed-rollback repetitions set `broken` and rejected the concurrent close. The size check, append, truncation and broken-flag handling are under the same lock. I found no race between writers using this Registry instance. Independent processes bypassing that lock remain outside its protection.

**Registry structure.** `null` and `[]` became corrupt markers while surrounding records survived. Objects missing `owner` rejected the registry, as reported in the minor finding.

**Inspect/remove interval.** Fresh label checks reduce mistakes but do not make inspection and deletion atomic. Label impersonation, replacement by a socket-authorized actor, and daemon changes remain **accepted unfenced-substrate limitations**. This review does not reopen AC4.

## Claims verified

| Claim | Independently checked result |
|---|---|
| Final runs | `final4-1..5`: exact **65 frozen IDs**, in order, all passing setup/call/teardown |
| Skips/exclusions | None; no xfails, deselections, receipt errors or approved exclusions |
| Durations | **263.63, 266.52, 261.52, 263.60, 263.25 seconds** |
| Resources per run | **16 containers, one network, four volumes** |
| Cleanup receipts | Clean; no reaped/refused/error entries, no corrupt registry lines, empty fixture cleanup failures |
| Foreign snapshots | All match SHA-256 `be83bde7595f92f5503151378bc90ad362ceb56cf683115c86324bd389513dd9` |
| Frozen inputs | Five identical **115-file** manifests, SHA-256 `a6a0449ac5c553ffa98c954d9bad9dce762c845e9ca6cf589b12a41dcaeeae9f`; every listed hash matches current bytes |
| Build/overlay closure | The 78 build inputs and 39-entry overlay union exactly equals the frozen map |
| Inventory | SHA-256 `be93061a16026839435a7a2d4abb41eba9270745d676d17fdedc92e191b1c964`, matching all five results |
| Wheel | Unchanged SHA-256 `09b8b7fc836e2c93d74ef66ed132c5bff3b94fb4c66a20823933c92fe6a78226` |
| Manifest | **602/602 retained files** match sizes and hashes; no missing or unlisted files |
| Fresh collection | **4,378 IDs / 139 files**; freezer `check()` matched |

The pre-round inventory copy matches the previous `8f47c97a…` inventory. All **4,364 previous IDs remain in order**, with unchanged existing dispositions and baseline entries. There are 14 additions. **Only `service-harness` changed among lane definitions.** HEAD IDs remain preserved.

Each `observer.json` contains:

- Five inspected containers, exactly matching the candidate-created set.
- Expected component counts: preflight 1, node-loader 2, dispatcher 1, edge writer 1.
- Correct candidate image IDs and owned-network identity.
- No privileged/PID-namespace configuration, errors or refusals; reader stopped.
- Barrier `create` then `destroy`, with matching volume name and nanosecond timestamps.
- Recorded application events preceding the barrier, with terminal events for every observed container.

The eight retained regression selections and successful runner receipts match current inventory and runner hashes:

| Lane | IDs |
|---|---:|
| service-helper-unit | 41 |
| core | 454 |
| safety | 1,335 |
| cli-runtime | 636 |
| public-contracts | 269 |
| writer-runtime | 540 |
| orchestration-runtime | 282 |
| example-contracts | 116 |

The envelope contains the exact **134 unique expected cases**, all passed, with `complete=true` and guard configuration restored. Its SHA-256 is `3f552cca702e37f4fcfb9e32b2d23e976b3fc35243ebfaac94f92b28daf56b7d`.

`reaper-control-5` records four exact removals, two refusals, one live network-removal `APIError`, one corrupt line, a truncated tail and an unclean result. The stray and protected canary survived. The retained red controls and dry-10 failure are consistent with the described control/import mistakes. Dry-11 passed 65 IDs at the final frozen digest.

Independent local checks: **119 passed, ten filesystem-dependent cases deselected**; ruff passed. The broader retained log reports **535 passed and seven live-configuration setup errors**. Credential scans found only the known synthetic credential-URL test IDs.

## Claims not verified

- General Docker cross-type causal ordering or losslessness. The new receipts prove the observed marker sequence, not a universal delivery guarantee; the evidence now states that limitation honestly.
- Actual current host state, daemon continuity or image contents.
- Raw graph, committed-offset, ledger and query-log observations beyond the passing assertions.
- Linux execution of the eight other installed lanes and envelope; their new executions are macOS receipts.
- Multiple observers sharing one owner, arbitrary schema-invalid registry corruption, or concurrent unrelated candidate-image workloads as supported scenarios.

## Verdict

**Accept** the recorded S7 harness proof under the user-accepted AC4 gap.

The reported N1–N3 failures and both cleanup notes are repaired. The two minor limitations above do not affect the exercised one-observer lane and should be addressed before expanding that usage or its recovery guarantees.