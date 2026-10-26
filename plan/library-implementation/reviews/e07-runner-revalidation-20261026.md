# E07 runner independent revalidation — 2026-10-26

Scope: delivered runner, installed guard, pytest receipts and E07 runner/startup
plans. Review entry is `91d765d18d5711402764bb5be51e3cd9c3bbed89`;
implementation is `9b1ddd2ff24f3d86b1a19aa35ea188c66e6155d7`, compared with
E06 baseline `5c3f72b6cd16d0e14ac931f220a3f73eb7e5a02d`. No applicable
AGENTS.md was found in the repository or its ancestor paths. This review did
not edit product/tests, install into retained environments, access private
dotenv, contact the remote IMDb host, or change services/containers.

## Original delivered findings

### P2 — Runner interruption bypasses owned cleanup and durable receipts

`scripts/verify_installed.py:168` waits for the owned subprocess, but only
handles `subprocess.TimeoutExpired`. A `KeyboardInterrupt` leaves the function
before the process-group termination at lines 180–184, `cleanup_owned` at line
185, or receipt append at line 189. The outer `main` exception boundary at line
336 catches only `Exception`, so the same interruption also bypasses its failed
owned `result.json` manifest. This contradicts the bounded owned supervision
and retained failure proof required by the E07 runner plan.

Independent actual-process reproduction:

```text
var/e07-validation-20261023/dev/bin/python \
  var/e07-runner-revalidation-20261026/repro_interruption.py
```

The owned supervisor invokes the actual delivered `run` with a sleeping Python
child, receives SIGINT after the child's recorded OS identity appears, and
preserves the resulting KeyboardInterrupt. Supervisor exit is 0 in this
controlled harness; the actual child remains alive and the receipt list is
empty. `interrupt-result.json` retains the PID/creation identity and result.
The harness then terminates only that exact owned identity and waits for it.
This is an E07-added function and finding, not an application regression.

### P2 — Unsupported `os.forkpty` silently avoids actual-child accounting

`scripts/installed_guard.py:192` handles the `os.exec` and `os.system` audit
events but allows `os.forkpty`. That API does not enter the wrapped `os.fork`
at lines 173–181. The inherited after-fork hook eventually audits a surviving
child, but the parent API returns without synchronization or a `python_child`
ownership record. An immediately terminated child can therefore have no
startup record at all. The final checks in
`scripts/verify_installed.py:305`–`320` accept this absence because their
expected-child set comes exclusively from `python_child` records.

Independent actual-guard reproduction:

```text
var/e07-validation-20261023/dev/bin/python \
  var/e07-runner-revalidation-20261026/forkpty_final.py
```

The owned probe environment contains the actual retained E07 wheel payload,
the actual delivered guard and a copied psutil dependency; it is a manually
assembled mechanism probe, not an asserted full runner lane. An isolated
target Python imports the application, creates actual forkpty child PID 8032
with creation time 1793057640.402504, immediately SIGTERMs/reaps it, and exits
0. The fresh provenance log contains only parent PID 8031 and application
module receipts. Expected children are empty; all exact final provenance
rejection predicates are false. Proof is retained in `forkpty-final.jsonl`
and `forkpty-final-result.json`. No child remains. This does not demonstrate
an application source-origin escape; it demonstrates silent missing actual
child/startup/ownership evidence, contrary to the fail-closed launcher rule.

Bounded correction: reject the unsupported `os.forkpty` audit event before
creation. Adding a new synchronized supported child API is unnecessary.

## Independent checks and repair-plan assessment

The development interpreter was identified from current E07 F2 evidence:
`var/e07-validation-20261023/dev/bin/python` (CPython 3.12).

```text
var/e07-validation-20261023/dev/bin/python -m pytest -q -o addopts= \
  tests/test_installed_runner.py \
  --basetemp=var/e07-runner-revalidation-20261026/pytest-temp
```

Result before repair: **24 passed in 0.69 seconds**. Existing controls do not
exercise the two reproduced gaps. Actual guard probes independently confirm
ordinary application import and target Popen child import succeed, while a
target `-S` child launch exits 86 with a durable violation. Broader lane and
artifact/full-suite revalidation belongs to the parent review.

The new `execution-e07-revalidation.md` has appropriate scope, ordering,
independent gates and proof-refresh requirements. The narrow proposed repair
was independently approved before product edits: preserve original interruption
types; perform owned group/receipt-bound cleanup on every exit after successful
Popen ownership; retain a durable command receipt, including interruption and
cleanup errors; persist the failed owned main manifest before re-raising
non-Exception failures; and reject unsupported forkpty via its audit event.
Cleanup failure must not mask the original KeyboardInterrupt/SystemExit.
Keep current success/timeout bounds, ordinary fail-closed cleanup behavior,
actual process ownership and all application/test deadlines. Add meaningful
real-process interruption and real forkpty negative regressions; refresh the
frozen inventory and affected final proof.

## Independent post-repair acceptance

Both findings are closed by the bounded proposed repair, independently read
and exercised after the author supplied stable code. `run` retains a
BaseException, performs bounded owned process-group termination/reaping and
receipt-bound cleanup in `finally`, records interruption/cleanup errors, and
then propagates the original exception. The main boundary retains its failed
owned manifest before propagating non-Exception interruptions. The guard
rejects the actual `os.forkpty` audit event before child creation. No supported
child API or application behavior was expanded.

Independent verification:

- The original real-process interruption reproducer, copied to
  `repro_interruption_green.py` with fresh output names and its expected result
  changed to child-gone/receipt-present, passes. The owned child is gone;
  command exit is -15, `KeyboardInterrupt` is preserved, and the command
  receipt has no cleanup error. `interrupt-green-result.json` retains proof.
- `forkpty_green.py` verifies the newly built wheel payload hashes equal the
  owned target payload, copies the repaired actual guard into that owned
  target, and runs isolated actual processes. Unsupported forkpty exits 86
  with one parent identity, a durable forkpty violation, and no child record;
  ordinary target Popen/import still succeeds; missing-guard `-S` still exits
  86. Exact commands/records and fresh wheel identity are in
  `guard-green-results.json` and the three fresh JSONL logs.
- `var/e07-validation-20261023/dev/bin/python -m pytest -q -o addopts=
  tests/test_installed_runner.py` reports **29 passed in 1.47 seconds**. This
  includes real SIGINT with both successful and deliberately failing cleanup,
  KeyboardInterrupt/SystemExit main-manifest cases, and real forkpty rejection.
- The initial repair test invocation reused this review's inside-Git
  `--basetemp`; 27 passed and two main-manifest cases correctly rejected that
  location before reaching the mocked interrupt. That harness placement was
  corrected by using pytest's ordinary outside-Git temporary directory. It is
  not counted as a product failure or as a successful interruption exercise.
- Exact previously owned red/green child identities were rechecked; none is
  alive. No foreign resource was signalled.

Reviewed source SHA-256 values, also retained in
`var/e07-runner-revalidation-20261026/post-repair-reviewed-inputs.json`:

| File | SHA-256 |
| --- | --- |
| `scripts/verify_installed.py` | `ff526f065bf609c4a78f8a3288fe967950609314829889d5a7ffaf27b4180edc` |
| `scripts/installed_guard.py` | `f914e2712b1532f7d476d3689084a7649d549ef9f0553bc9dbec9f663f00fe28` |
| `tests/test_installed_runner.py` | `c198836a22ebd83c27fa5626b9852d51eb8039c7eca412da385e3010743d9571` |

There is no remaining actionable runner finding from this review. This accepts
the narrow runner repair at those exact bytes; the parent still owns refreshed
inventory, full suite, all installed lanes, artifacts and final delivery gates.
