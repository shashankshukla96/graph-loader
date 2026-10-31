# E09.F1.S7 independent repair review — 2026-10-31

**The five passing runs are verified. The claim that all four prior majors are repaired is refuted.** Observer completeness and cleanup safety still have reproducible defects; reusable termination controls remain partial.

Read-only review: no files modified; no SSH, network or Docker commands executed. Local reproductions used fake clients only.

## Findings

| ID | Severity | File:line | Evidence | Required fix |
|---|---|---|---|---|
| R1 | major | [live_cli.py:175](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/live_cli.py:175), [live_cli.py:120](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/live_cli.py:120) | `drain()` checks only containers already observed. It returns immediately when those containers have ended, without establishing that the event stream has caught up. `stop()` then sets `_stop`; `_loop()` discards the next event before handling it. A deterministic delayed-stream reproduction omitted another candidate container’s create/destroy events while reporting `errors=[]`, `closed_early=False`, created-set equality and no unobserved candidates. An already removed container also escapes the outer snapshot. | Establish a bounded event-stream boundary after CLI completion and consume through it before accepting completeness. Require observer-thread termination. Add delayed-event and early-stream-loss controls, repeated at least five times. |
| R2 | major | [live_cli.py:139](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/live_cli.py:139), [live_cli.py:205](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/live_cli.py:205) | Network attachment adopts any container. The inner `AppObserver.reap()` checks attachment but never checks the owner label. I supplied a container explicitly labelled with another owner, delivered an owned-network connection event, and the fake removal API was called; the result reported successful removal. The outer reaper’s repaired conflicting-owner rejection does not protect this earlier path. `project()` also omits the owner label. | Reject conflicting ownership before adoption/removal and apply one ownership policy across cleanup paths. Verify the network’s owner label before using attachment as authority. Add a foreign-owner-on-owned-network control. Audit `LiveStack.remove_owned()` and direct container/network cleanup, which likewise lack fresh owner checks. |
| R3 | major | [service_lane.py:96](/Users/shashank/Desktop/Code/Graph/graph-loader/scripts/service_lane.py:96), [service_lane.py:113](/Users/shashank/Desktop/Code/Graph/graph-loader/scripts/service_lane.py:113), [verify_installed.py:566](/Users/shashank/Desktop/Code/Graph/graph-loader/scripts/verify_installed.py:566) | “Docker errors reported not raised” covers removal `APIError` only. Intent lookup and container/volume/network inspection catch only `NotFound`; transport errors also escape. An inspect `APIError` for the first of two recorded owned containers aborted `reap()` and left the second unprocessed. Earlier removal results would also be lost when a later inspection raises. `service_postflight()` does not catch these Docker exceptions. | Record per-resource inspection/removal failures, preserve partial results and continue independent safe cleanup. Keep the result unclean. Add controls for intent lookup, each inspection type, transport failure and failure after a successful removal. |
| R4 | minor | [test_service_harness.py:278](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/test_service_harness.py:278), [live_stack.py:294](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/live_stack.py:294) | `run_owned()` and `remove_owned()` extract creation and cleanup. SIGKILL, bounded wait, exit-code and OOM inspection remain inline in H5. There is still no reusable termination control for an observed application container. | Extract an ownership-checked termination helper with recorded control action, deadline and expected outcome, or explicitly retain AC3’s termination portion as partial. |
| R5 | minor | [structured_query_log.py:69](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/structured_query_log.py:69) | The “identical” DDL completion exception compares transaction/database/user/query, excluding waiting time and the preceding runtime. I supplied completion waits of `0` and `2500`; the second was discarded and `waits(..., expected=1)` returned `[0]`. A first `pipelined` completion followed by a `schema` completion also passed. Current MATCH-based H6 controls are unaffected. | Reject conflicting duplicate measurements/runtime, or conservatively combine waits under an explicitly documented rule. Cover these cases before reusing the detector for DDL zero-wait claims. |
| R6 | minor | [verify_installed.py:611](/Users/shashank/Desktop/Code/Graph/graph-loader/scripts/verify_installed.py:611), [verify_installed.py:789](/Users/shashank/Desktop/Code/Graph/graph-loader/scripts/verify_installed.py:789) | `service` is initialized after constraints and inventory loading, but referenced by the exception handler. A unit-lane invocation with a missing inventory raises `UnboundLocalError`, masking the original error. Invalid early dependency inputs have the same problem. | Initialize `service = None` before entering `try`; cover early validation failures and interruptions. |
| R7 | minor | [capture_baseline.py:29](/Users/shashank/Desktop/Code/Graph/graph-loader/plan/library-implementation/tools/capture_baseline.py:29) | The untracked-input allowlist omits `scripts/service_lane.py`, `scripts/installed_guard_load.py` and `tests/test_service_lane.py`. I evaluated the actual Git selection: all three are excluded from baseline capture in this uncommitted checkout. The separate S7 frozen-input manifest does cover the runtime files. | Include these files so baseline identities cover the new runner, loader and reaper controls. |
| R8 | minor | [test_service_harness.py:269](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/test_service_harness.py:269), [E09.F1.S7.md:39](/Users/shashank/Desktop/Code/Graph/graph-loader/plan/library-implementation/evidence/E09.F1.S7.md:39) | The Kafka restart check verifies the topic’s end offset. It does not consume earlier payloads or verify committed consumer offsets. “Keeps every earlier message” exceeds that assertion. | Describe the result as end-offset persistence, or add payload and committed-offset readback if claiming those properties. |

## Prior majors: repaired? (yes/partial/no)

1. **Observer completeness — partial.** Candidate creations are tracked, names are populated, the unused attempt argument is gone, and stream closure is detected. The shutdown race in R1 still permits incomplete observation to pass. The six synthetic controls do not exercise that race.

2. **Reaper/snapshot controls — partial.** The retained live control reports exactly five removals, a truncated registry, an unclean outcome, an untouched canary and detection of the subsequent canary change. Truncated-tail parsing and exact-name intent recovery exist. R2 and R3 leave ownership and Docker-error handling incomplete.

3. **Reusable outage/termination controls — partial.** Kafka now has an owned data volume and reusable stop/start methods. Lock contention moved into `lockwait.py`. Termination remains inline, as described in R4.

4. **AC4, weakenings and evidence wording — yes, subject to this review’s remaining corrections.** The evidence explicitly records AC4 as unmet and user-accepted. It now discloses engine-wide CLI authority, the non-rolling injected log, floating dependencies and Neo4j tag, capture limits, and supervision limits. I have not reopened the accepted AC4 gap. S7 remains in progress in the backlog; its proposed closing text is explicitly unapplied.

## Claims verified

### Retained runs and identities

| Claim | Independently checked result |
|---|---|
| Final repaired lane | `final2-1..5`: all report exit 0 and exactly the frozen **36 IDs**, in order |
| Outcomes | Every ID has passing setup/call/teardown; no skips, xfails, deselections, approved exclusions or receipt errors |
| Pytest durations | **259.30, 255.94, 259.27, 260.16, 255.34 seconds**; published rounded range is correct |
| Resource counts, each run | **16 distinct containers**: 11 created plus 5 observed; **1 network; 3 volumes** |
| Cleanup receipts | All five report no reaped leftovers, refusals or errors; registry close rows have `failed=[]` |
| Foreign snapshots | All ten before/after files have SHA-256 `be83bde7595f92f5503151378bc90ad362ceb56cf683115c86324bd389513dd9` |
| Frozen inputs | All five manifests have **114 files**, SHA-256 `84d512a70d793da58f19b1fa57819d362b353d4ad4f3d97b977c29c62391449a`; every listed hash matches current checkout bytes |
| Overlay/build closure | Each overlay has 38 entries; its union with the 78 current build inputs equals the 114-file frozen map |
| Inventory SHA-256 | `66f08114e6b3bea305b298ac1a7c55713ed2f8e80ce7bfd35166f8d185611a12`, matching all five receipts |
| Wheel SHA-256 | Local wheel matches `09b8b7fc836e2c93d74ef66ed132c5bff3b94fb4c66a20823933c92fe6a78226`; 65 members, including the exact 59-file application payload |
| Candidate identities | The four full image IDs, tags and `linux/amd64` platform agree across all five result files |
| Runner hashes | Every recorded runner-tool hash matches the current corresponding file |
| Evidence manifest | **262/262 files** match recorded size and SHA-256; no unlisted files under the retained S7 directory |

### Inventory and assertions

- Fresh local collection reproduced **4,312 IDs / 139 files**; the freezer’s `check()` matched.
- All **3,873 HEAD IDs** remain in order. Existing test/file dispositions and every existing installed unit lane are unchanged.
- Every HEAD-inventoried test file is byte-unchanged versus HEAD. No existing unit selector or exclusion was loosened.
- The disclosed S6 test change at `tests/test_installed_service.py:96` replaces the obsolete “controller integration incomplete” expectation with failed-preflight rejection. Its frozen ID remains.
- The repaired H4 count assertion, narrow Neo4j outage exception, H7 vanished-process handling, and live prefix/inode continuity controls are present.

### Controls, provenance and safety evidence

- Each final registry records five Neo4j stop/start pairs and five Kafka stop/start pairs. H5 and H6 also contain five helper-kill and contention/negative-control iterations respectively, across five passing runs.
- Each final provenance log has six process rows for six distinct PIDs; the installed CLI has 41 module records. No duplicate guard startup is apparent.
- Retained command receipts report successful guard probing and `pip check`, with no timeout or owned-process cleanup error.
- Grep and structured scans found no apparent runtime credential in retained logs, receipts or process argv. Credential-URL matches were synthetic test IDs.
- Published ports are checked in code for loopback binding and the static forbidden-port set. Kafka’s bind-probe race is honestly disclosed.
- No retained artifact shows removal of a foreign resource. This does **not** establish the universal safety claim, given R2.

### Local checks

**58 tests passed**, covering observer controls, query-window controls, non-filesystem reaper controls and the existing structured-log parser tests. Ruff passed on the requested code and changed plan tools.

The broader unit invocation could not start because the execution sandbox denied temporary-file creation. The retained `270 passed` log matches its manifest hash, but I did not independently reproduce that full run.

## Claims I could not verify

- Actual live-host state, daemon continuity, image contents or absence of current leftovers: live-host access was expressly out of scope.
- Direct H4 graph/property/offset/ledger/query-log observations and inspected mount projections. Locally retained `service-run` files contain only the registry and CLI stdout/stderr; the passing source assertions support these claims, but the underlying observations are unavailable for independent recalculation.
- The clean candidate image manifest on the host. Its resulting image identity maps are retained locally; the manifest itself is not.
- Universal foreign-resource non-interference. Snapshot equality covers the recorded projection, not database contents or transient changes.
- Preservation against the lost pre-S7 inventory copy. Preservation against Git HEAD is verified.
- Other installed unit lanes and the E08 `--process-envelope` path with the new loader. Only the earlier Mac `service-helper-unit` receipt and the repaired service-lane receipts are retained here.
- True concurrent kill/create recovery. The reaper control constructs a simulated interrupted registry; it does not kill a controller during daemon creation.

Against the backlog, **AC1’s cleanup safety remains incomplete**, **AC2 has passing installed-run evidence with the observation gap in R1**, and **AC3 remains partial**. **AC4 is honestly recorded as an accepted gap.**

## Verdict

**Accept with repairs.**

Accept the retained **36-ID, 5/5 result and frozen-byte identities** as verified. Do not mark the four prior majors fully repaired or close S7 until R1–R3 are fixed and revalidated, and AC3’s termination scope is completed or explicitly recorded as partial.