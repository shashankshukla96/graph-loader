# Independent E07.F1.S11 implementation review — 2026-10-24

**Disposition: changes required. S11 is not accepted.** One reproduced durable-failure hole remains. The bounded startup synchronization otherwise passes the independent controls below. S6 requires its own subsequent independent review; no S6 or final E07 gate is certified here.

This is a fresh zero-history review on `codex/pypi-library-checklist`, HEAD `5c3f72b6cd16d0e14ac931f220a3f73eb7e5a02d`. I read the supplied handoff, canonical E07/backlog order, approved startup plan/review, E07 execution plan, accepted earlier story reports, original S6 rejection, S11 narrative/machine proof, the guard/runner/pytest implementation and maintained controls. Author inputs and prior evidence were preserved. Only this report, new owned files under `var/e07-validation-20261024/s11-review/` and new outside temporary directories were written.

## Finding

### 1. [P2] A caught startup interruption leaves no durable violation

`scripts/installed_guard.py:142` catches only `Exception` around the receipt polling loop. `KeyboardInterrupt`, `SystemExit` and other `BaseException` subclasses escape without recording the required sticky violation. If test/application code catches the interruption and the child subsequently writes its receipt, final reconciliation accepts the lane even though its startup gate was interrupted before obtaining any receipt. A receipt arriving beyond the three-second startup bound can therefore erase the only remaining evidence of failure.

This is independently reproduced through the complete runner, using byte-identical copies of all three author tools and the unchanged candidate wheel in new isolated environments. The owned fixture delays the child's site hook by 3.5 seconds, starts an actual target-Python subprocess and uses an ordinary signal handler to raise `KeyboardInterrupt` after 0.2 seconds. It catches that interruption, restores the signal handler/site hook and waits four seconds for the child to finish. The child eventually supplies its valid audited receipt, but the full runner exits **0**, writes **`status: passed`**, and contains **zero violation records**. The actual owned child is PID **8421**. Its receipt tick is more than 3.5 seconds after its launch anchor. This does not rely on an absent final process receipt or counterfeit application origin.

A separate first reproduction uses a custom `BaseException` subclass and an 0.8-second child delay; that also returns a successful lane after the caught interruption. Both complete results, provenance logs, pytest receipts, commands, tool identities and fixtures remain retained:

- `var/e07-validation-20261024/s11-review/reproduce_keyboard.py`, `keyboard-reproduction.json`, `keyboard-result.json`, `keyboard-provenance.jsonl`, `keyboard-pytest.json` and `keyboard-pytest.log`;
- `var/e07-validation-20261024/s11-review/reproduce_interrupt.py` and corresponding `interrupt-*` evidence.

The concrete command is recorded in each reproduction JSON. The reviewer invocation is `var/e07-validation-20261023/dev/bin/python -I var/e07-validation-20261024/s11-review/reproduce_keyboard.py` (harness exits 0 after preserving the erroneous successful runner result). Outside output is `/private/var/folders/5p/84cp5b7s72q_ymcsd4qlvq8r0000gn/T/graph-loader-e07-s11-independent-keyboard-p0nps6s0/run`.

Record a durable violation for interrupted receipt waits as well as ordinary exceptions, retaining the already registered child identity. Preserve interruption semantics when re-raising as appropriate. Add an actual caught `KeyboardInterrupt`/late-receipt negative control, with pytest itself succeeding while the runner still fails specifically for the durable violation. Repair, rerun the required matrix and obtain independent re-review before S11 acceptance.

## Independent controls and preserved strengths

The real clock/sleep captures, parent/launch-anchor/PID/OS-creation/prefix/site matching, receipt-after-full-payload-audit ordering, ownership registration before waiting, incomplete trailing append deferral and separate post-exec freshness checks otherwise implement the approved strategy. No new explicit polling lock is introduced. The existing multiprocessing insertion point waits before sending user payload, but the startup audit imports no application code and requires no payload, avoiding that dependency cycle. The non-UTF8 cleanup repair retains good identities through bad records and reports corruption.

Using frozen byte-identical copies of the reviewed tools, the independent matrix executes **14 positive probe IDs, all 23 original/new negative invocations, and the full public selection**. The matrix runs on copies to avoid any concurrent author repair changing a reproduction halfway through. All expected existing outcomes occur:

- Parent, multiprocessing spawn/fork, minimal environment, env, POSIX spawn, fork/exec and console positives pass. All six actual immediate-termination APIs pass with matching child identities.
- Wrong origin; missing hook; env/POSIX/multiprocessing bypass; missing post-exec hook; detached child; absent final pytest receipt; late ordinary startup timeout; frozen shared clocks; stale/wrong-prefix receipt; malformed detached receipt; and detached hard timeout all fail the runner.
- Missing module, resource and mandatory metadata wheels fail preflight. Ordinary and actual linked Git worktree ancestry, default temporary output under Git, existing output, symlink output and checkout output all reject. Existing owned sentinel bytes and modes are preserved.
- The ordinary late-timeout control has a successful pytest receipt and subsequently complete child receipts, but a durable violation correctly retains runner failure. The malformed detached child's known identity is reclaimed and corruption reported. The detached hard-timeout case cannot leave its owned child behind.

Commands/results, all copied logs/receipts, input inventories and scratch paths are retained in `matrix.py`, `matrix-results.json`, `matrix-receipts/`, `preflight.py`, `preflight-results.json`, `verify_receipts.py` and `receipt-verification.json` under the reviewer-owned directory. `matrix.py`, `preflight.py` and the receipt verifier all exit 0.

Additional actual mechanism controls pass. One child writes its fully audited process receipt in two appends 150 ms apart; the parent defers the incomplete trailing record and returns only after the complete record, without a violation. A real very short-lived child is automatically reaped with `SIGCHLD` ignored before the delayed parent OS query. That query raises actual `psutil.NoSuchProcess`; the guard records `identity_from_fresh_receipt` using the full matching audited receipt. Actual owned PID **8437** is gone. A separate forced-query control also exercises the fallback. Details are in `extra-mechanisms.json/log` and `exited-actual.json/log`; both reviewer invocations exit 0.

The public selection independently passes in **32.11 seconds overall**: **250 full IDs, 247 selected passes, exactly three approved exclusions**, no skips, xfails, warnings or failed phase. All **137 actual children** match their OS creation/startup identities, parent and pre-creation launch anchor. There are **138 regression process identities** and **1,698 module-origin receipts**; the subsequent pip-check interpreter adds one later startup row. All positive application origins resolve into their actual target environments with matching bytes, pip checks pass and no positive command reports timeout, leftover owned process or cleanup error. This confirms the instrumentation repair's intended effect but does not supply S6 acceptance or uninstrumented R1 timing proof.

Independently compared all **14 public test files with actual HEAD bytes**: every file is identical, and application source has no diff. Original E04 bounds/assertions and fixtures remain intact. The original rejected S6 proof remains rejected and retained. Every raw receipt hash declared by the author S11 machine proof matches its retained file.

Safe mechanism execution installs the private-dotenv/service-connect audit block before pytest or source imports. It exits **0**, **35 passed, four smoke IDs deselected**, with **zero blocked attempts**. Targeted Ruff and `git diff --check` pass. Static preservation and proof-file verification are retained in `static-verification.json`, `mechanism-status.json`, `mechanism.log` and `evidence-file-check.json`.

## Exact reviewed identities and limits

Safe before/after captures match each other and the author proof:

- **306 source inputs:** `d3426b558606b569705572835e8319e6682c13450793ac884c69277b44c6f00f`.
- Environment: `ae2f84a14eebdcb4c6fef629ef822b72fcb62658806808a5e2bc8c43794a5cc3`.
- Inventory: `770da5b51b6ec05d59d219516b5fac1f2fddef60d74f2394db5c34c3e9f1ee08`.
- Wheel: `cdd73ac361c99a8c73a15c929d150ce7d5e75d9446e1229dba5a1614cd494feb`.
- Runner: `bb79a159ee3958b66b218227f738cdf35f8a9d4ad91246e6b2a081412a5afdcb`.
- Guard: `11615ade4532a13a039a7bd9c7b4dff759340a2464e3f669566ccf19c5d4ab9f`.
- Pytest receipt tool: `38f1feb53fae9b0f552c24837c42c471140469f403795a4efbe4c000158cf62d`.

No private environment file was read, remote IMDb endpoint contacted, live developer smoke/database test executed, foreign resource altered, Docker cleanup performed, or push/upload/tag/publication/workflow/E08+ work undertaken. No S6, final artifact parity, final bare/image/R1/service gate or E07 completion claim is made. The one finding requires repair and independent re-review.
