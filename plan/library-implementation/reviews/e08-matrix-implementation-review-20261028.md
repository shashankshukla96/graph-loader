# Focused E08 process-matrix implementation review — 2026-10-28

**Disposition: accept the bounded opt-in harness implementation after the repairs below. No actionable finding remains in the reviewed snapshot. This is not S2, native-envelope, E08, or final-delivery acceptance.**

Read the approved execution plan, amended process-matrix design and focused external-client review. Reviewed the fault-fixture additions, new preparation/private ownership helper, supervisor forwarding, installed runner/provenance extension and support-matrix forwarding. No applicable ancestor AGENTS.md was found. The current human authorization to resume implementation and Linux validation supersedes the older review's read-only/Linux restriction; this reviewer nevertheless used only local inspection and isolated local controls. No SSH, Docker, services, private environment-file access, application-source changes, staging, or commit occurred. This report and ignored audit outputs are the reviewer's only writes.

## Findings and repair verification

### P2 — Prove every advertised blocked or partial-reply peer activated — resolved

The initial prepared public path waited for boot markers emitted before worker-body delegation, but used one shared source/read readiness marker. Four actual children therefore did not prove four peers entered the intended phase. The initial `blocked_neighbor_control.py` boots four peers, delegates the real worker only for peer zero, and leaves peers one through three sleeping before records. The unmodified initial fixture returned a successful four-worker cancellation observation with all three `neighbor-never-records` markers present. The initial observation is retained at `/private/tmp/e08-review-blocked-neighbors-20261028-v1/client.json`; its raw output is in the ignored audit directory. This initial local control completed directly; it is not presented as an independently supervised matrix cell.

The repair adds opt-in per-source blocked-phase markers and per-worker partial-reply markers emitted after the header and partial body have been sent. The prepared barrier waits for every required marker under one existing 60-second preparation deadline. Production cleanup and all marker assertions occur before fallback reaping. Observations separately report expected/actual blocked and partial peers and explicitly describe the excluded interval as boot plus phase preparation. Cold cells retain the ordinary anchor; the two-child startup failure is now honestly labeled unprepared.

The repaired `public-many` and `public-partial` negative controls both boot four children while leaving three peers inactive. Both reject with the respective preparation-deadline assertion, produce no successful client observation, and complete cleanly under a separate 90-second process-group supervisor. The unmodified 60-second fixture deadline was exercised. The earlier 30-second supervised repeat, interrupted while the repair was being applied, is retained as an incomplete negative attempt and supplies no fixture-deadline acceptance. The send case already gathers every counter before reaching degrees-write backpressure; it does not have the same later-peer activation gap.

### P2 — Bind fault observations to actual child provenance — resolved

The initial installed fault branch checked the reported count/context and cleanup booleans without matching `child_pids` to provenance. An isolated mocked-command control supplied four nonexistent reported children, a log containing only one importing parent, and no child-launch records. The runner accepted `status: passed` and `complete: true`. The original result is retained as `fault-pid-provenance-negative-before-repair.json`.

The repaired branch requires an actual client startup, exactly the expected number of unique nonclient child PIDs, membership in both actual startup and expected-child sets, a launch record belonging to the observed client, and installed planning-module evidence for every child. The shared strict validator also binds launch time, startup parent, creation identity, target prefix/site, module origin/hash, post-exec startup and violation status. The original missing-child control now rejects with `fault child identities lack bound target launch/startup/module provenance`, restores the exact guard configuration and never records completion.

Additional isolated contract controls accept a complete synthetic receipt set and reject missing startup, wrong startup parent, wrong module hash, wrong environment prefix, stale post-exec startup, and a changed frozen fixture. These are checker controls, not actual installed execution or native proof.

### P2 — Include the public client's identity in its observation — resolved

The first provenance repair read `observed['client_pid']` for all cases, but `public_fault` did not return that field. Every public envelope case would consequently fail with `KeyError`. The repair adds the actual caller PID to the public observation. All six subsequent focused smoke cases contain a valid client PID; the private additive helper also supplies it.

## Other checked contracts

- The option is explicit, restricted to executed public-contracts lanes, and explicitly forwarded by support-matrix tooling. All 134 cases are distinct: normal spawn/inline counts 1–8/16/32/64; failure counts 1/2/4/8/64; mixed/startup only where at least two workers are requested. Startup reports two actual children; private N-peer controls report one exercised channel and N−1 blocked neighbors.
- Legacy three-positional/default calls, default worker behavior, 4M-entry send counter, 20-second supervisor, original IDs and original assertions remain. New paths preserve .8-second operation timeout, .3-second lateness, .15-second private control, .75-second adapter bound, and the 10-second aggregate production/private cleanup budgets. The cumulative fake-gap anchor remains 5N, with later expiry checked relative to that anchor.
- The additive send counter has `1 << 18` uint32 entries. Its packed degrees payload is about 32 KiB; the fixture measures its encoding and actual SO_SNDBUF, requires the encoding to exceed that buffer, and requires the existing EVENT_WRITE/degrees activation marker. Preparation does not reset the real operation budget between phases.
- Public resource/liveness assertions precede fixture fallback cleanup. Cancellation setters observe the startup-failure stop signal, are joined and checked dead. Private peers are signaled together and joined under one aggregate budget. Cold timeout reports activation honestly without requiring it.
- Sequential cases use separate logs and owned outputs, recheck frozen fixture hashes before execution, and retain failed case receipts. Exact target guard configuration is restored in `finally`. Independent controls prove ordinary command failure and KeyboardInterrupt preserve their original exception, retain incomplete/failed results and restore exact bytes; a restoration-write failure propagates and cannot produce a complete result.
- No application constructor cap, application global-start-method mutation, or `src/` change is introduced. The already reviewed external client keeps caller-method and serialization/source-error/exit-37 checks. The envelope additionally checks successful source/data/worker observations and actual installed provenance.

## Checks and evidence limits

Independent repaired-fixture smoke controls passed public-many/cancel/4, public-partial/cancel/4, public-send/timeout/4, public-startup/timeout/requested-4-actual-2, public-cold/timeout/4 and private-recv/cancel/8. Their command receipts have zero exits, no timeout, owned leftovers or cleanup errors. The two repaired boot-only-neighbor controls also exit zero only after asserting the expected rejection, with no supervisor timeout or cleanup error. Raw scripts, command receipts, observations and negative-control results are retained under `var/e08-matrix-implementation-review-20261028/`.

Inspected the separate original-controls log: **145 passed in 25.42 seconds**. That run finished before the final peer-marker/client-ID repairs; it is preservation evidence for the preceding snapshot, not a fresh full regression of the final hashes below. `git diff --check` passes for the modified tracked harness files. No installed inventory refreeze, complete 134-case envelope, Linux architecture cell or final gate is supplied by this focused review. Those remain required and cannot be inferred from local smoke or checker controls.

## Reviewed stable input identity

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| `tests/fixtures/e04_faults.py` | 19576 | `6418a50caa72d1d6d480ddec50e6d17b251e7e25fa58bc674df37371b4539cb2` |
| `tests/fixtures/e08_fault_matrix.py` | 4478 | `c485f2ad6b2e08da882669700b914db37964aaaa31f590e7518005e15bb1f664` |
| `tests/fixtures/e08_process_clients.py` | 9749 | `4c6afc3bf126511c8ade54684d496d0784cbf8a17402aa51b3f957a55c46dbea` |
| `plan/library-implementation/tools/run_e04_fault.py` | 3731 | `8c7a359584029ae66c0610746ad5780ca9cd787c9b354e813f32d8dc3acd72f1` |
| `scripts/verify_installed.py` | 34756 | `cd76fb3e0c000bd1160bc1716457f3288630f68cd0069bbe37cb697cc2adc344` |
| `scripts/support_matrix.py` | 16692 | `848d02ea2371a7050f8b6129440d1e8a13f4f038ed534c099f6c229679813bf3` |

## Early-cleanup import provenance follow-up — 2026-10-28

**Disposition: accept the scoped startup/cold-timeout receipt repair. No actionable finding remains. This does not accept either failed full installed attempt or a complete process/native/S2 matrix.** Only runner validation and its authoring controls changed in this follow-up; the maintained source/fault fixtures are unchanged. The separate atomic-identity repair and its source hash remain covered by `e08-source-identity-review-20261028.md`.

The actual second installed attempt at `/private/tmp/graph-loader-e08-matrix-macos-20261028-v2/` completed **269 installed tests in 40.14 seconds**, followed by **62 passed process cases and one failed case**, with envelope `complete: false` and overall `status: failed`. Case 63, `public-startup-spawn-2-timeout`, passed its actual fixture and supervisor but failed the runner's requirement for a planning-module import from every worker. Its command-log hash verifies; no timeout, owned leftovers or cleanup errors occurred. These results remain red and are not a 134-case pass.

Independently replayed that case's original provenance against the strict environment/origin validator. Client 17387 launched children 17389 and 17390. Both children have fresh parent/launch/creation-bound target startups. Child 17389 has 11 application-module records; child 17390 has none. The latter was killed during the intentional failure immediately after its audited second startup, before application import. Its missing module receipt is therefore an honest observation of the specified fault. The unchanged installed startup hook validates every one of the 59 payload members before recording process startup. The retained installed hook equals the maintained hook, and all 59 current installed payload hashes also match its configuration. Neither fact is converted into a fabricated module-import record.

The new `validate_fault_children` preserves the exact count/uniqueness, expected-child membership, real startup and launch-parent checks. The caller still runs the existing strict startup creation/parent/launch/prefix/site checks and every actual module origin/hash check first. Planning-module receipts remain mandatory for ordinary fault cases. Absence is permitted only for the designated partial-start fault, with a RuntimeError observation, the specific second-process-started-then-failed marker and no operation anchor, or the designated unprepared cold-timeout path, with a real operation anchor, EdgePlanTimeoutError and the original .8-second configured timeout. The outer caller independently enforces two actual startup children, or the requested cold-cell count, and normal production resource cleanup. Actual per-child module lists and the missing-receipt PID list are returned explicitly.

The startup exception is appropriate for either early child: the injected failure and subsequent cleanup can happen before either has imported planning. The cold exception is similarly appropriate because that existing cell deliberately allows ordinary timeout/cleanup before source activation. Prepared transport cases still require their ordinary per-child planning imports; no startup/launch/origin validation is relaxed for them.

Independent receipt controls accept the actual failed startup receipt and reject missing startup, wrong launch parent, foreign prefix, duplicate children, missing imports in an ordinary fault, an unrelated startup marker, a non-null startup anchor, the wrong startup error type and a changed actual module hash. Separate synthetic cold-contract controls accept the intended unprepared timeout shape and reject prepared, anchorless, wrong-timeout and wrong-error variants. The positive cold shape is a checker control derived from startup receipts, explicitly **not an executed cold timeout or native observation**. All 15 controls and raw replayed rows are retained under `var/e08-startup-provenance-review-20261028/`.

An independent isolated authoring run completed **46 passed in 0.13 seconds** for `tests/test_support_matrix.py`, without warnings. Shared conftest, plugin autoload and pytest cache were disabled. The pre-pytest access guard recorded no private-dotenv or external-socket attempt. Its bounded command receipt and log hash verify, with no timeout or owned cleanup issue. `git diff --check` passes. No source edits, SSH, Docker, service access or private environment-file reads were performed by this reviewer.

Reviewed follow-up identities, superseding the corresponding earlier runner/test identities only:

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| `scripts/verify_installed.py` | 36782 | `21f3c0ce10a6d830f07841571c5520e9879717443f504c396128728542367222` |
| `tests/test_support_matrix.py` | 14471 | `e613b9ad7b377bfde396f465e5fb858043ce1f57437420c128e3f6280bd4244b` |
| Retained pre-repair runner | 34756 | `cd76fb3e0c000bd1160bc1716457f3288630f68cd0069bbe37cb697cc2adc344` |

Both failed attempts must remain retained. A fresh requested full matrix with these runner bytes and matching frozen inputs remains necessary; no complete envelope, Linux architecture cell, S2 or final acceptance follows from this repair review.
