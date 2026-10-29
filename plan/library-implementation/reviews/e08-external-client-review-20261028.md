# Focused external-client review — 2026-10-28

**Initial snapshot disposition: one actionable P2 coverage finding, subsequently resolved by the accepted repair follow-up below. The additive spawn/inline client and supervisor changes otherwise have no actionable finding within this bounded review. This is not S2, native-envelope, E08 or final-delivery acceptance.**

Reviewed `tests/fixtures/e08_process_clients.py`, the 14 additive cases in `tests/test_public_cancellation.py`, and the external-client delegation/context/worker/deadline changes in `plan/library-implementation/tools/run_e04_fault.py`. Read the approved execution plan, S2 evidence and accepted global S1 review. No applicable ancestor AGENTS.md was found. Only this report and ignored review outputs under `var/e08-process-20261028/external-client-review-v1/` were written. No application/test source modification, private dotenv read, SSH, Docker operation, service access, staging or commit occurred.

## Finding

**[P2] Require evidence that the intended abrupt source failure was reached — `tests/fixtures/e08_process_clients.py:115–117`.** The worker-exit branch accepts any exception containing `exited unexpectedly` with a cause. It does not require that source 0 executed, that it ran in a spawned child, or that its process actually exited with code 37. The later source-identity assertions apply only when `expected_error` is false. A boot/import failure before scanning can therefore pass the worker-exit control while never exercising the intended source failure. The outer test also only requires nonempty error/message fields in this case.

Two independently executed negative controls establish the gap without editing the maintained files:

- `exit38_control.py` changes only the ignored audit process's fault-source behavior to `os._exit(38)`. The unmodified client validation returns `status: observed` and accepts a message explicitly reporting exit code 38.
- `early_exit38_control.py` substitutes an immediate exit-38 worker entry in the ignored audit process, before any source records run. The unmodified client validation again returns `status: observed`, with `source_identities: []` and an exit-code-38 message.

Each control used the existing owned process-group supervisor with a 180-second external deadline and retained zero-exit command/cleanup receipts. Raw scripts, logs, receipts and client observations remain in the review output directory. These are deliberate failed-proof controls, not production behavior or passing S2 cells.

Require the intended source-0 activation identity (spawn context and nonparent PID) and actual exit-37 evidence. Assert the exit code after cleanup if its earlier observation can race process termination. Keep the generic unexpected-exit/cause/marker/cleanup assertions as well. The normal source already calls `os._exit(37)`; the finding concerns proving that this particular path was reached. Existing prototype logs show 37, but those observations do not close the maintained assertion gap. The implementer acknowledged this finding and plans a bounded repair; that repair is not part of this reviewed snapshot.

## Checks that support the bounded changes

The client invokes planning/schema through public modules, supplies 64 external source objects containing 192 distinct records, and injects a file-backed producer rather than connecting to services. Its nonpicklable control contains a real lock attribute; inline appropriately succeeds because it does not serialize that attribute. Source-error branches require the sentinel in both the error and its cause. The success branches verify exact row contents, unique published delivery identities, chunk byte sizes/counts/keys/type/run, absence of unreferenced messages, a serialized plan round trip and the actual plan-byte SHA-256.

The caller explicitly chooses forkserver where available while the planner receives an explicit spawn context; successful identities verify every source ran in spawn children, including the measured worker count. Inline identities verify parent execution. Both contexts assert the caller's global method remains unchanged. No library method-setting mutation or application-payload change is introduced by these files.

Cleanup assertions cover the actual operation's aggregate upper bound using its cleanup start, the explicit repeat-close duration, active children, lock, spill directory, heartbeat thread and caller-owned sentinel. The internal cleanup-start field is read for observation; no planner internals are patched by the maintained client. The conservative aggregate upper bound includes the small post-finish data-validation interval. The separate client session/group and hard deadline are preserved. New context/worker arguments are required for external scenarios and rejected for legacy scenarios; the legacy command/default mode/default timeout and cleanup implementation remain unchanged. Finite positive deadline validation is additive.

An independent run of the current `tests/test_public_cancellation.py` completed **33 passed in 8.39 seconds**, with deprecation warnings treated as errors, plugin autoload disabled and shared conftest excluded. An audit hook installed before pytest blocked the private root dotenv and external socket connections; it recorded no attempted access. The external supervisor's receipt reports no timeout, owned leftovers or cleanup errors. `git diff --check` passes for the two modified tracked files.

Independently inspected the retained noneditable installed public-contract observation at `/private/tmp/graph-loader-e08-s2-installed-20261028/public-contracts-v1/`: all **261** selected IDs match in exact order, each has exactly one passed setup/call/teardown phase, and the receipt has no errors, deselections or nonzero pytest exit. All 38 staged hashes verify; the three reviewed helper/test hashes also match checkout. The provenance log contains 2,174 module records, 192 startup records and 190 Python-child records; all expected child PIDs/startup creation identities are represented, and no violation is recorded. The result's 191-process set predates the final successful pip-check startup, explaining the extra log startup. No exec records occur in this lane, so its post-exec check is vacuously satisfied, not an executed exec-specific control. Retained numerical/hash checks are in `review-checks.json`.

## Reviewed input identity and limits

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| `tests/fixtures/e08_process_clients.py` | 9359 | `6f9aac5d12ff3f7f146a429fefe12002cf979f8d86ea8e0cae5b7f5884b0f2cd` |
| `tests/test_public_cancellation.py` | 6301 | `99690254f5fad36be56905d154dec11b74f83a61402300a424de53a4400a5c26` |
| `plan/library-implementation/tools/run_e04_fault.py` | 3223 | `e3a64fda343f9d4cb278165ea15013b364722f9ed13f98b7abfa9bf48efd1ca5` |

The added tests currently cover workers 1 and 4. The complete normal 1–8/16/32/64 envelope, failure 1/2/4/8/64 envelope, explicit private/public transport fault parameterization, required native cells and fresh full S2 acceptance remain open as already recorded in S2 evidence. This report does not reaccept S1, the separate warning repair, the separately executing installed-example lane, full inventory preservation or final archive/wheel/image/service/R1/B3/adversarial/delivery gates. It does not infer native support from this macOS observation.

## Exit-proof repair follow-up — 2026-10-28

**Disposition: accept the bounded repair; the P2 finding is resolved. No actionable finding remains in this focused external-client review. Full S2/native/envelope/final acceptance remains open.**

The source activation marker now includes its scenario. The worker-exit branch retains the generic error/cause checks and additionally requires an exit-code-37 message plus the actual source-0 activation marker: order 0, worker-exit scenario, spawn context and a PID distinct from the caller. It records that marker and exit code in the observation, and the outer pytest case explicitly checks the exit-code/order observations. Success, serialization, source-error, global-context and cleanup assertions remain. The supervisor is unchanged by this repair.

Verified that `var/e08-process-20261028/external-client-before-exit-repair.py` has exactly the initial fixture hash recorded above. Both original negative controls were rerun against the repaired maintained fixture with new output names, preserving the old passing observations. Added an immediate exit-37 worker entry control to isolate activation proof from exit-code proof:

| Retained negative control | Repaired observation |
| --- | --- |
| `repaired-exit38` | Exit 1; rejected by the exact exit-code assertion |
| `repaired-early-exit38` | Exit 1; rejected by the exact exit-code assertion |
| `repaired-early-exit37` | Exit 1; rejected because source-0 activation marker is absent |

None writes a successful client observation. Each bounded command receipt explicitly allows only the expected exit 1 and reports no timeout, owned leftovers or cleanup errors. The early-exit-37 control reaches the marker read after accepting the actual exit-37 message, demonstrating that an unrelated boot-time exit 37 cannot pass. New raw logs, receipts and `repair-negative-results.json` are retained separately in the same ignored review directory. No maintained source was modified by the reviewer.

An independent repaired-fixture focused run reports **33 passed in 8.02 seconds**, with deprecation warnings treated as errors and the same pre-pytest private-dotenv/external-connection guard; no blocked attempt, timeout or cleanup error occurred. The separately retained combined `external-and-warning-v2` run reports **60 passed in 14.53 seconds**. Inspected its executed command, verified its log SHA-256 `9279884b9769b39babfc98c65d938190a097d0e4eab33c5240fc87b209e9a978`, zero exit, no timeout/cleanup errors/leftovers, audit installed before pytest, no blocked attempts and terminated benign fixture thread. This does not constitute a new warning-fix review.

The repaired reviewed identities are:

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| `tests/fixtures/e08_process_clients.py` | 9749 | `4c6afc3bf126511c8ade54684d496d0784cbf8a17402aa51b3f957a55c46dbea` |
| `tests/test_public_cancellation.py` | 6443 | `beda9c6ec06524b2d66ed942183a98fcbacaed80763ebf104f3ad89bbcc59861` |
| `plan/library-implementation/tools/run_e04_fault.py` | 3223 | `e3a64fda343f9d4cb278165ea15013b364722f9ed13f98b7abfa9bf48efd1ca5` |

The earlier installed 261-case observation belongs to the initial hashes, not these repaired fixture/test bytes. A refreshed frozen inventory and installed rerun are still required; this follow-up supplies no installed-equality or native claim. All broader open gates listed above remain open.

## Proposed process-envelope harness design review — 2026-10-28

**Design-only disposition: the opt-in additive approach can preserve existing contracts, subject to the correction and qualifications below. This is not implementation or runtime acceptance.** Reviewed the proposed brief against current `e04_faults.py`, planner start/anchor/hub code, private transport and installed runner/guard.

**Required correction: the proposed 16K-element uint32 counter does not guarantee later-command backpressure.** `_hubs()` converts candidate flags with `np.packbits()` before sending `degrees`. A 16,384-element counter occupies 65,536 bytes, but its later `degrees` payload is only 2,048 bytes plus codec framing. That need not fill the requested 4,096-byte socket send buffer, which the OS can enlarge. The old 4M-element counter produces a 512-KiB degrees payload. An additive 262,144-element counter would reduce counter traffic to 1 MiB per peer while yielding a 32-KiB degrees payload. Whatever bounded size is chosen, observe the actual socket buffer and require the existing `EVENT_WRITE` activation marker specifically on `degrees`; a large raw counter is insufficient evidence. Keep the original default counter unchanged.

The remaining design is appropriate with these explicit limits:

- Waiting inside the fixture's `_begin_operation` occurs after initial scan submission. It deliberately excludes submit-to-worker-boot time that ordinary deadline execution would include. Label these as prepared transport/cleanup cells, record preparation separately and retain original uninstrumented/default operation-clock controls. Do not claim high-count cold-start timeout or stronger whole-operation coverage from those prepared cells. A preparation timeout/missing marker must fail the case; the original .8-second deadline, .3-second lateness and 10-second cleanup assertions still apply after activation. The external-source success/error envelope remains ordinary execution unless separately disclosed.
- New cancellation readiness may start its existing four-second window after the operation anchor to isolate post-activation cancellation. Waiting for that anchor must also observe the stop signal so a startup failure cannot leave the setter thread alive. Assert fixture threads die. Keep the legacy timer path unchanged.
- Preserve `public-gap`'s actual submission-anchor and later-gap check. Its current fake clock increments by five for every scan send; copying that logic to N lanes yields an anchor of 5N rather than five. Either advance once in the additive branch or assert the actual cumulative anchor and set the later clock relative to it. Never silently omit the gap assertion.
- Report requested count, planned lanes, started/activated identities and observed cleanup count separately. Mixed-source controls require at least two actual workers; partial-startup failure intentionally starts two, not N. Neither can be reported as an actual N-worker observation when its construction differs. Other blocked-peer cells must prove every advertised peer exists and enters the intended blocked phase.
- Per-case provenance logs in the reused owned target are reasonable only sequentially: finish the prior case's owned cleanup and validate it before switching the config; restore the exact config in a `finally` path, including failures/interruption. Preserve all existing startup PID/creation/launch, origin/hash, post-exec and violation checks per case, rather than accepting log presence alone. Preserve original lane receipts and separately manifest all case commands, logs, helper hashes, actual counts and outcomes. Missing/failed envelope cases must fail an explicitly requested envelope run.
- Assert production public cleanup before any fixture-owned fallback reaping, as the legacy observations do. For private N-peer fixtures, use one aggregate cleanup measurement/budget and verify every owned peer and endpoint, rather than summing fresh per-peer allowances. Preserve the original private .15-second control/.75-second observed bound.

`--process-envelope` restricted to the public-contracts lane, explicitly passed by the matrix controller with all defaults unchanged, is a sensible boundary. The proposed guard-log switching creates no need to change application bytes or add runtime dependencies. Native observations, a fresh implementation review and complete S2 acceptance remain required.

The implementer's design response resolves the counter-size issue before code: the additive worker will use `1 << 18` counter entries, record the actual parent `SO_SNDBUF` and encoded degrees-frame size, require that frame to exceed the actual buffer, and retain the `EVENT_WRITE`/`degrees` activation assertion. The original 4M-entry worker stays unchanged. Prepared cells will explicitly disclose the additional submit-to-boot exclusion; separate cold, unmodified blocked-source timeout cells at measured counts will check the ordinary deadline/cleanup path without requiring the blocked source to activate first. Those cold observations must report whether activation occurred rather than imply it. This combination can separately establish ordinary clock/cleanup behavior and activated prepared transport behavior without conflating them.

The design response also commits to separate requested/actual scopes for mixed-one-worker exclusions and the two-actual-child partial-startup fault, per-case provenance validation before success, exact owned guard-config restoration in `finally`, and overall failure on any guard/config-restoration problem. Record these rules in the plan addendum before implementation, together with the fake-gap-clock and cancellation-thread conditions above.

**Bounded design conclusion: proceed with that amended opt-in harness design. No unresolved design finding remains in the proposed counter/preparation/provenance approach. Implementation, negative controls, preservation, complete worker/fault observations, native execution and fresh S2 acceptance are still open.**
