# E07.F1.S1 — current independent disposition

**Accepted after repair and re-review on 2026-10-23. No actionable S1 mechanism
finding remains.** This accepts the installed runner mechanism and maintained
probe only. S3 inventory freezing, lane ports and the final E07 artifact/service
gates remain outstanding. The original rejected review and first rejected
re-review below are preserved; the final acceptance and exact input identities
are recorded at the end.

---

# Independent E07.F1.S1 implementation review — 2026-10-23

**Verdict: changes required. S1 is not accepted.** Four actionable mechanism
findings remain below. This is an S1 review; it does not demand completed S3
inventory classification, legacy ports or final artifact/service evidence.

The reviewer started without the author's conversation history and read the
supplied user handoff, canonical E07 story/dependencies, execution plan and
accepted plan review, S1 evidence/JSON, actual runner/guard/receipt implementation,
17 safety tests, three maintained fixtures, baseline-capture changes and raw
proof receipts. HEAD was `5c3f72b6cd16d0e14ac931f220a3f73eb7e5a02d`.
Original implementation hashes were:

- `scripts/verify_installed.py`: `68bcc1c15e212c722a3f402bd7cbf65a16fd475528a67894503ab7555dbf7b05`
- `scripts/installed_guard.py`: `cd6ba1ec34f53ce07b07b45640f52448e043f63c1fe56cf5b03ac2709b1ad44b`
- `scripts/installed_pytest.py`: `6dd5ec6cbb9a814282e8355e58f37eabb8466890a8dac921d591f90e8dd15d70`

## Findings

### 1. [P1] Child provenance is optional for ordinary launch paths

`installed_guard.py:73–79` only records `subprocess.Popen` children whose
executable basename starts with `python`. It misses `/usr/bin/env` and shell
launchers, `os.posix_spawn`, and multiprocessing's direct spawn implementation.
`verify_installed.py:204–210` only requires receipts for the children that happen
to have been recorded, so absence from the expected set becomes a successful
lane. The positive author's receipt already illustrates the gap: its real
multiprocessing spawn/fork processes have startup receipts, but only the direct
`subprocess` PID is in `expected_python_children`. Positive startup records do
not prove missing startup records would fail.

An independently maintained scratch fixture was run through a byte-identical
copy of all three scripts, with the same wheel/payload and a fresh environment.
Three actual cases each imported counterfeit application code without the guard:

1. `subprocess.run(['/usr/bin/env', sys.executable, '-S', ...])`;
2. `os.posix_spawn(sys.executable, [sys.executable, '-S', ...], os.environ)`;
3. real multiprocessing `spawn` with `set_executable` pointing to an owned
   launcher that invokes the target interpreter with `-S`.

All three assertions passed. The full runner exited **0**, wrote `status: passed`,
and recorded **no expected children**. Only the actual pytest parent supplied
startup/application receipts. This is a concrete violation of the S1 requirement
that missing actual-child provenance cannot yield a successful lane.

Require every supported child creation path to declare its actual PID and a
fresh post-exec handshake. Support ordinary target-Python/multiprocessing paths;
explicitly reject unsupported launcher/process APIs before they can create an
unchecked child. A process record emitted before exec or inherited through fork
must not satisfy provenance for a new post-exec interpreter. Keep independent
negative controls for wrappers, direct spawn and multiprocessing spawn, alongside
the existing direct `python -S` control.

Evidence: `var/e07-validation-20261023/s1-review/reproduction.json`, `result.json`,
`pytest.json`, `provenance.jsonl`, `reproduce.py`, `test_review.py.txt`,
`inventory.json` and retained outside scratch path recorded by the reproduction.
The scratch runner copies have the exact original hashes above. No repository
test or product file was edited to construct this reproduction.

### 2. [P1] A detached owned Python child survives a successful runner

`verify_installed.py:80–100` supervises and kills only one process group. The
installed child wrapper permits `start_new_session=True`, and there is no owned
PID/group cleanup after provenance checking. A child that starts another session
therefore escapes the group the runner kills.

The full fresh runner reproduction added a direct target-Python child sleeping
for 60 seconds with `start_new_session=True`. Its guard ran and its PID had
matching expected/provenance records, so this does not rely on finding 1. After
four passing tests, the runner exited **0** and wrote `status: passed`; the exact
owned detached PID **11061** was still alive. The reviewer then sent SIGKILL only
to that PID. An additional direct invocation of `run()` reproduced the same leak
with owned PID **10971**, which was also cleaned by exact PID only.

Either reject session/group escape in supported test launchers or supervise the
complete set of explicitly owned descendants/groups and finish bounded cleanup
on success, failure and timeout. Do not scan/kill foreign resources. Retain a
negative control proving a detached child cannot survive the lane result and a
failure control proving cleanup errors prevent a pass.

Evidence: `s1-review/extended-reproduction.json`, `extended-result.json`,
`extended-provenance.jsonl`, `extended-pytest.json`, `extended_reproduce.py`,
`cleanup-reproduction.json` and `cleanup_reproduce.py` under the raw evidence
directory above. The successful result identifies the detached child as expected
and instrumented; the reproduction records its survival and exact owned cleanup.

### 3. [P2] Outside-checkout staging can still be inside a Git worktree

`verify_installed.py:144–154` checks only ancestry under its own `ROOT` and does
not reject a different Git ancestor. The accepted plan requires staging and
its environment genuinely outside checkout/Git. Merely omitting `.git` from the
copied file allowlist does not satisfy this: Git discovery walks parents.

The reviewer created a new owned Git repository under the external scratch
parent and selected its fresh `output` directory. The full runner accepted that
output, ran the tests and returned success. `git rev-parse --show-toplevel` from
its **actual stage directory** returned **0** and the owned Git root. No foreign
Git tree was changed.

Before creating staging/environment, reject Git worktree ancestry, including
`.git` directories and worktree pointer files, after resolving the requested
path. Apply this to both explicit output and default temporary output. Retain
actual rejection controls for ordinary and linked-worktree ancestry, and keep
the successful proof that Git discovery fails in accepted staging.

Evidence: the same `extended-reproduction.json` records the zero Git exit and
resolved Git root; `extended-result.json` records the accepted runner output.

### 4. [P2] Wheel metadata drift can be masked by test dependencies

`verify_installed.py:139–141` validates only Name, Version and absence of extras.
It never compares mandatory `Requires-Dist`, entry points, Requires-Python or
other accepted metadata against a frozen contract. `pip check` validates the
wheel's own remaining declarations; it cannot reveal declarations that have been
removed. Separately installed test dependencies may mask that packaging defect.

The reviewer created an actual wheel with all 59 application members unchanged,
but removed the two mandatory `Requires-Dist` lines for `neo4j` and
`neo4j-rust-ext`. A single ordinary installed-parent probe passed, `pip check`
passed, and the full fresh runner returned **0** / `status: passed`. The mutated
wheel hash is `5bede509821dfa516534b9a2d861abd019f6213af8a482d0368d28ea1b0335f0`.
The existing `testcontainers[neo4j,kafka]` tool declaration also installs Neo4j,
illustrating why the eight mandatory runtime dependencies need independent
metadata validation.

Freeze and validate the accepted package metadata separately from test-tool
requirements, including the exact eight mandatory requirement specifications,
Python requirement, namespace/root identity, console script and no extras.
Record that metadata contract in the result. Reuse the established artifact
verifier's semantics where appropriate. Keep a real mutated-metadata wheel
negative control as well as the existing omitted module/resource controls.

Evidence: `s1-review/metadata-reproduction.json`, `metadata-result.json`,
`metadata-pip-check.log`, `metadata-install.log`, `metadata-inventory.json` and
`metadata_reproduce.py`. The original candidate wheel and original failures are
unchanged and retained.

## Checks and preserved strengths

The reviewer reran
`var/e07-validation-20261023/dev/bin/python -m pytest tests/test_installed_runner.py -q`:
**17 passed**. `git diff --check` passed. The actual archive allowlist and hashes
correctly reject the author's omitted module/resource wheels before installation.
The retained direct counterfeit-origin child exits86 and its successful pytest
assertion still produces a nonzero runner result; the direct `python -S` case
also correctly fails for a missing child receipt. These are useful controls,
but they do not close the alternate launch paths demonstrated above.

Finite staged file hashes, symlink rejection for source fixtures, sanitized
runtime environment, explicit pytest configuration, disabled plugin autoload,
exact ordered collection, deselection and unexpected skip/xfail rejection are
sound parts of the S1 mechanism. There is no finding requiring a product/runtime
change, dependency expansion, weakened assertion or S3 coverage shortcut.

All independent reproductions use new owned external scratch, fresh environments
and copied runner bytes; raw/copy receipts remain retained in
`var/e07-validation-20261023/s1-review/`. Only this review and its owned scratch
artifacts were written. No private `.env` access, remote IMDb contact, Docker or
service launch, foreign cleanup, global prune, commit, push, upload, publication
or E08+ execution occurred. Acceptance requires repair, actual positive and
negative retesting, and an independent re-review of the repaired exact inputs.

## First repair re-review — changes still required

The author reproduced the original rejected launch, detached/Git and metadata
cases with preserved original runner copies. The first repaired positive attempt
had an incorrect expected console-version string (7 passed, 1 failed); that
failure remains retained. The corrected `s1-repairb-results.json` records eight
positive exact IDs and actual failures for wrong origin, missing child hook,
env/POSIX/multiprocessing bypass, missing post-exec hook, detached child,
early pytest exit, missing module/resource/mandatory metadata and Git directory/
linked-worktree ancestry. Re-review reran the expanded safety suite: **21 passed**.

The fixes address the mechanisms in findings 1–4: supported launch policy plus
Popen/POSIX/multiprocessing/fork PID accounting; creation identities and a fresh
post-exec record; exact receipt-bound cleanup that fails on leftover children;
Git ancestor rejection; and six frozen distribution-member hashes independently
of test dependencies. Strict inspection of the original wheel/sdist checks the
canonical dependency/Python/console/namespace contract before freezing metadata.
`psutil` remains a declared test/development tool, outside runtime dependencies.
An additional author-discovered zero-exit pytest termination is also correctly
rejected when it omits the final exact-ID/outcome receipt. Original red evidence
is retained in `s1-author-red/early-exit-reproduction.json`.

### 5. [P2] Rejected existing output overwrites the previous result receipt

The output branch assigns `work` before checking whether the requested directory
exists. The general exception handler then writes `work/result.json` whenever
`work.is_dir()`, even though this run never created or took ownership of that
directory. Reusing an output path therefore reports refusal while overwriting
an existing failure or success receipt. A checkout path or directory symlink can
likewise become a write target after rejection.

The reviewer created a new external directory containing a sentinel previous
`result.json`, then invoked the repaired actual runner with that existing output.
The runner exited **1** with “output must be a new directory outside checkout”,
but replaced the sentinel with its own failed-result JSON. No environment or test
was created. Evidence and the exact before/after bytes are retained in
`var/e07-validation-20261023/s1-review-repair/existing-output-reproduction.json`
and `existing_output_reproduce.py`.

Assign `work` or an explicit ownership flag only after successful exclusive
creation. Failure evidence must only be written to directories this invocation
created. Rejecting existing/symlink/checkout output must leave every existing
byte unchanged. Add a retained reuse control so accidental reruns cannot destroy
the required original receipts. **S1 remains unaccepted pending this repair and
its re-review.**

Independent repaired-process retesting retained in
`s1-review-repair/recheck.json` ran byte-identical external copies of repaired
runner `bb5b3affbd93e2eaa5e93279f131c5fc6d854a0672d3fbc524d32abceaf8e6c9`,
guard `bcca55cf4c4e34e2f9921cd0d8682e596ee7d6102494fd20ebd665a1126d5b0f`
and unchanged pytest plugin. Eight positive IDs passed in a fresh environment.
A second fresh actual detached-child run failed and its exact owned child was
absent after the runner returned, without reviewer emergency cleanup. A further
actual hanging-test/detached-child run with `--timeout 3` failed with a recorded
3.55-second command duration, SIGTERM exit, timeout flag and successful owned
cleanup; its detached child was also absent after return. These independent
controls are in `timeout-recheck.json` and accompanying copied logs. They close
the normal success/timeout paths of finding 2 for those repaired script bytes;
finding 5 still prevents acceptance of that revision.


## Final independent repair re-review — accepted

**Disposition: S1 accepted. Findings 1–5 are closed.** The reviewer inspected the
final ownership change and actual final candidate proof
[`e07-s1-final.json`](../evidence/e07-s1-final.json), its strict original-wheel/sdist
inspection authority, actual command/provenance/pytest receipts,
metadata/Git/output controls and corrected author evidence. No actionable S1
mechanism finding remains. This does not accept the unported lanes or complete
E07.

The final successful fresh run binds these exact tools:

- Runner: `188b2ff4aaa77e00e755de6faa8b3f9e0516560918428e5bdbafbd3f64b75572`
- Guard: `bcca55cf4c4e34e2f9921cd0d8682e596ee7d6102494fd20ebd665a1126d5b0f`
- Receipt plugin: `6dd5ec6cbb9a814282e8355e58f37eabb8466890a8dac921d591f90e8dd15d70`
- Safety tests: `66ae5085b070736a29f90297693c7dc56a3b28d7f118061ac179a164e3bd44c2`
- Development requirements: `e53eb647a7d5ab3093039e220467e9e79ed2d46253d4ac7884d565d50fc15287`

The final capture `var/e07-validation-20261023/s1-final-source.json` contains
294 safe source inputs. The reviewer independently compared every captured
byte hash, size and executable mode with the current files: **zero mismatches**.
Source identity is
`746baab55ecc764aac327d2b6dc71db1881e89f72098010c69ce86d402bc5066`;
environment identity is
`ae2f84a14eebdcb4c6fef629ef822b72fcb62658806808a5e2bc8c43794a5cc3`.
An additional independent manifest of 18 directly reviewed runner/test/fixture/
configuration inputs, including exact modes, is retained in
`s1-review-repair/final-reviewed-inputs.json`; it also has zero current mismatches.
The current tools match the successful final run's recorded hashes.

The final candidate executes all **eight exact positive IDs**, with all **eight
expected actual child PIDs** accounted for. This includes parent, real spawn and
fork, minimal environment, env wrapper, POSIX spawn, fork/exec and console-child
paths. All **17 actual negative invocations** reject: wrong origin, missing hook,
three alternate bypasses, missing fresh post-exec handshake, leaked detached
child, missing final pytest receipt, omitted module/resource/mandatory metadata,
ordinary/linked/default-temporary Git ancestry and three existing/symlink/
checkout output-preservation controls. Negative cases retain real pytest success
where relevant; a green assertion cannot override missing provenance or owned
cleanup. The declared launcher policy fails unsupported launches closed before
creation. Exact ordered collection/outcome and six distribution-member hashes
remain independent of the test-tool dependency set.

The reviewer independently reran **23 safety tests** and whitespace checks after
the final ownership fix; they pass. The reviewer also directly reran the actual
CLI with existing-directory and directory-symlink outputs. Both reject without
changing any sentinel result byte; `r5-fixed-recheck.json` binds the final runner
hash. Earlier independent fresh eight-ID positive, detached-success cleanup and
bounded detached-timeout cleanup remain valid for the unchanged launch/cleanup
implementation; their original receipts and copied source identities are
preserved above. The author's complete final suite rerun additionally binds the
final ownership-adjusted runner bytes. Targeted author Ruff checks pass.

Explicit ownership is now false until this invocation exclusively creates its
output. Thus rejection cannot overwrite original success/failure evidence or
follow a rejected output symlink. Cleanup signals only receipt-bound OS creation
identities, fails an otherwise successful run for an owned survivor or cleanup
error, and proved removal after normal success and timeout. Runtime application
sources, version, public namespace and eight mandatory runtime dependencies are
unchanged; `psutil>=6,<8` is a declared development/runner input only.

All original red evidence, the first incorrect console-version assertion and its
failed run, both rejected review rounds, author reruns and independent repair
receipts remain preserved. No private environment file, remote IMDb endpoint,
foreign resource, Docker/service launch, commit, push, publication or E08+ work
was used by this review. S3 may proceed in the already accepted canonical order;
its exhaustive file/ID/helper closure, bounded splits and disappearance controls
require their own independent gate before porting.
