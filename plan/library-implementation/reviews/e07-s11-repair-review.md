# Independent E07.F1.S11 repair re-review — 2026-10-25

**Disposition: accepted for S11. No remaining actionable finding in the bounded repair.** The original startup-interruption finding is independently reproduced against the repaired guard and now fails closed. S6 still requires its own subsequent independent review. This does not accept S6, any later story, final artifact parity, or E07 completion.

This is a fresh zero-history review on `codex/pypi-library-checklist`, HEAD `5c3f72b6cd16d0e14ac931f220a3f73eb7e5a02d`. I read canonical E07, the execution/startup repair plans, approved startup plan review, original S11 rejection, repaired machine evidence, retained interruption red, guard/runner/receipt implementation, and maintained startup controls. The original [S11 rejection](e07-s11-review.md) remains unchanged. No source was edited. Only this report, new owned files under `var/e07-validation-20261024/s11-repair-review/`, and new outside temporary outputs/environments were written.

## Original finding and repair

The previous review showed that a caught `KeyboardInterrupt` could interrupt the receipt wait before obtaining a receipt, yet leave no durable violation. A genuine receipt arriving more than 3.5 seconds after launch then let the final runner incorrectly pass.

The repaired `child_record` places the complete post-registration deadline/polling loop inside a `BaseException` handler. It records a durable violation with the already registered child identity. Ordinary exceptions retain the previous startup `RuntimeError` behavior; interruptions outside `Exception` are re-raised unchanged. The repair does not extend the three-second wait, change the audited receipt predicate, import application modules in the guard, or alter launch/cleanup behavior. The diff against the retained failing guard is confined to this handling change.

I reran the **byte-identical original reviewer KeyboardInterrupt fixture** through copies of the final three tools in a new isolated wheel environment. The complete runner now exits **1**, with `status: failed` and `missing or violated Python child provenance`. Pytest still exits **0** with **one passing test**. The actual child, PID **2690**, supplies its matching audited process receipt **3.544347458 seconds after its pre-creation launch anchor**. One durable `child startup receipt failed: KeyboardInterrupt` violation remains. Thus failure is specifically sticky despite the eventual genuine receipt; absence of a final receipt is not the reason for rejection.

The maintained interruption fixture independently exercises **KeyboardInterrupt, SystemExit and a custom BaseException**. All **three pytest IDs pass**, all three actual children eventually provide matching genuine receipts more than 3.5 seconds after launch, and the runner exits **1** with exactly the three corresponding durable violation records. The test also confirms that the original interruption type returns promptly to the caller.

The exact reproduction command, final tool hashes, fixture, complete results, provenance, pytest receipts and logs are retained in `reproduce_keyboard.py`, `keyboard-reproduction.json`, `keyboard-*`, and `matrix-receipts/interrupted-startup/` under the reviewer-owned directory. The author’s original red and previous rejected C evidence remain retained separately.

## Fresh independent validation

Using frozen byte-identical copies of the final runner/guard/receipt tools, the independent matrix executes **14 positive probe IDs and all 24 required negative invocations** with expected outcomes. The original KeyboardInterrupt reproduction above is an additional negative invocation.

- Parent, actual multiprocessing spawn/fork, minimal environment, env wrapper, POSIX spawn, fork/exec and console positives pass. All six immediate-termination APIs pass with matching startup identities before their immediate termination.
- Wrong origin; missing child hook; env/POSIX/multiprocessing bypass; missing post-exec hook; detached child; absent final pytest receipt; ordinary late startup timeout; all three caught interruptions; frozen shared clocks; stale/wrong-prefix receipt; malformed receipt; and detached hard timeout reject the runner as required. The late ordinary timeout and interruption controls have successful pytest receipts and eventual child receipts, while durable violations preserve failure.
- Actual missing module, resource and mandatory dependency metadata wheels fail preflight. Ordinary Git and actual linked-worktree ancestry, default temporary output under Git, existing output, symlink output and checkout output reject. Owned sentinel bytes and modes remain unchanged.
- The malformed detached receipt reports corruption while retaining and reclaiming its known child identity. Independently queried identities confirm the detached-success, detached-timeout and malformed-record owned children are gone; no emergency cleanup was needed.

The matrix, preflight and receipt-verification scripts all exit **0**. Commands, outcomes and copied raw receipts are retained in `matrix.py`, `matrix-results.json`, `matrix-receipts/`, `preflight.py`, `preflight-results.json`, `verify_receipts.py`, `receipt-verification.json`, and `owned-child-verification.json`.

Additional real mechanism controls pass against the repaired guard. A new child's fully audited process receipt is split across two appends 150 ms apart; the parent defers the incomplete trailing line and returns only after the complete receipt, with no violation. A real short-lived child is automatically reaped before the delayed parent OS lookup; that lookup raises actual `psutil.NoSuchProcess`, and the guard retains the OS creation identity from the full matching audited receipt. A separate forced-query control also passes. Details remain in `extra-mechanisms.json/log` and `exited-actual.json/log`.

The fresh full public selection passes in **36.78 seconds overall**: **250 full IDs, 247 selected passes, exactly three approved checkout exclusions**, no skipped/xfail/failed phase or receipt error. Pytest reports `247 passed in 24.73s`; its supervised command takes 27.26 seconds. All **137 actual children** match their OS creation/startup identities, parent and pre-creation anchor. There are **138 regression process identities**, **1,697 module-origin receipts**, and one subsequent pip-check startup row. Every positive module origin matches its actual target-environment bytes. Pip checks pass, and positive command receipts contain no timeout, leftover owned process or cleanup error. Original public timing/assertion bytes remain unchanged. This validates S11 instrumentation, not uninstrumented startup latency or separate S6 acceptance.

Safe mechanism execution installs private-dotenv/service-connect audit blocking before pytest or source imports: **35 passed, four smoke IDs deselected, exit 0, zero blocked attempts**. Targeted Ruff over the three tools and new interruption fixture passes; `git diff --check` passes. No developer smoke or live service test ran.

## Preservation and exact identities

Independent safe before/after captures match each other and the author proof exactly:

- **307 source inputs:** `f94b1e9e68d6bcb750752782d15c253a4b4fa28325d52ae09129c56e1e5adcf6`.
- Environment: `ae2f84a14eebdcb4c6fef629ef822b72fcb62658806808a5e2bc8c43794a5cc3`.
- Inventory: `770da5b51b6ec05d59d219516b5fac1f2fddef60d74f2394db5c34c3e9f1ee08`.
- Wheel: `cdd73ac361c99a8c73a15c929d150ce7d5e75d9446e1229dba5a1614cd494feb`.
- Runner: `bb79a159ee3958b66b218227f738cdf35f8a9d4ad91246e6b2a081412a5afdcb`.
- Repaired guard: `02deb3760f24718cf19165de005b0dbabc7efbbcce6034b0d0889a51cdd91ca5`.
- Pytest receipt tool: `38f1feb53fae9b0f552c24837c42c471140469f403795a4efbe4c000158cf62d`.

All **129 raw files** declared by `e07-s11-repaired.json` match their retained hashes and sizes. All **14 public test files** match actual HEAD bytes and the frozen baseline, and application source has no diff. Original E04 bounds, cancellation/cleanup/startup-failure assertions and original rejected S6 evidence remain preserved. Checks are retained in `before.json`, `after.json`, `static-verification.json` and `preservation-and-repro.json`. Persistently copied author receipts were used where older temporary outputs had expired.

No private environment file was read, remote IMDb endpoint contacted, live developer smoke/database test executed, foreign resource altered, Docker cleanup performed, or push/upload/tag/publication/workflow/E08+ work undertaken. S11 is accepted at the identities above; S6 and all remaining E07 gates stay open.
