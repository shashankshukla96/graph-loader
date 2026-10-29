# Independent E08 reaped-exit fixture review — 2026-10-28

**Disposition: accept the bounded fixture-only exit-status repair, its focused
controls, the actual arm64 minimum race observation, and the exact latest helper
bytes below. No actionable finding remains. Full native S2 results, S2 acceptance
and final E08 acceptance remain open.**

This review used a task-only repair brief and local source/retained evidence. It
reviewed the design amendment, fixture delta from the previously frozen input,
actual earlier failures, deterministic positive/negative controls, the guarded
173-pass receipt, current collection preservation, the fresh minimum process
envelope, and the latest export verifier/summarizer. No new tests, native runs,
SSH, Docker operation, private environment read, IMDb/service access, source
mutation, commit or publication occurred. Only this report was written. A prior
reviewer's usage-limit failure was not treated as acceptance or bypassed.

## Repair and original failures

The retained original arm64 minimum/current public-contract runs fail at
`external-worker-exit-spawn-4-timeout` and
`external-worker-exit-spawn-2-timeout`, respectively. Their raw client logs show
the original unexpected-exit error with `exit code None`, followed by the old
fixture's assertion failure; both retain source0's actual worker-exit activation.
The 97,701,059-byte failed archive recomputes to
`7f4d8d56392a793a2282a2323b5ffedf4099c06c84d6e354b10978895d38261b`.
Those runs remain failed. Their diagnostic is consistent with the unchanged
production receive path reading EOF before `multiprocessing` has observed/reaped
the child's exit status; an unobserved diagnostic value is not proof of a
different worker exit.

`ObservedSpawnContext` subclasses the real spawn context and delegates every
`Process` call, arguments and return object to `mp.get_context('spawn').Process`.
It only retains the parent handles. The worker body still atomically records its
source activation and calls `os._exit(37)`. The production application payload,
transport/receive, first-error cause, operation clocks and cleanup are unchanged.
The original unexpected-exit message may contain 37 or transient None, but the
fixture additionally requires the source0 activation's spawn PID to match one of
those real handles, be dead, and have actual reaped exitcode 37 after `finish`'s
existing cleanup. It preserves the original message/cause/failure marker and
records all real process exits. No diagnostic wait, join, clock reset or fixture
fallback reaping is inserted. The original less-than-ten-second cleanup,
active-child/heartbeat/lock/spill/sentinel/global-method assertions remain.

This is narrower than accepting arbitrary None text: an unrelated boot-time 37
lacks source0 activation, while an activated source exiting 38 has the wrong
actual exit status. Both must fail. All 59 application payload hashes and six
distribution hashes match the earlier frozen inventory and current source. All
3,873 per-ID dispositions remain identical to the earlier inventory; the retained
current source collection contains exactly that same ID set.

## Independently checked controls and native minimum evidence

The deterministic control calls the unchanged receive implementation, then forces
only its 37/38 diagnostic text to None. Under that deliberately ambiguous text,
normal activated exit37 succeeds and records the real dead/37 handle. The actual38
control exits nonzero specifically with `AssertionError: 38`; the early37 control
exits nonzero because `source-0.json` was never committed. Neither negative case
produces a successful client receipt. All three raw command-log hashes match
their receipts; no outer timeout, owned cleanup fallback/error, blocked private
file or network attempt is recorded. The controls install their audit hook before
application import and are focused checkout controls, not installed native proof.

The guarded original-control run retains **173 passed in 33.97 seconds** with
DeprecationWarning promoted to an error. Its command/log hash, zero exit,
no-timeout/no-cleanup-error receipt and pre-pytest audit agree; the benign parent
thread is dead afterward and no forbidden access is recorded. No warnings are
reported. This supports the original selected controls without substituting for
the native process envelope.

Independently replayed all **134 cases** under
`/Users/shashank/graph-loader-e08-s2-arm64-reaped-7f7vvtva/minimum/cells/public-contracts/observation/installed/process-envelope/`.
Every case matches the exact maintained enumeration, aggregate and individual
result; raw client observation; successful non-emergency child supervisor; outer
command zero exit, log hash, timeout/owned-cleanup gates; and actual provenance.
Checked **2,054 parent/child launch records** against actual startup parent,
launch anchor and creation identity, plus **31,291 application-module records**
against target prefix/site, process creation and frozen payload origins/hashes.
No violation record is present. All 39 public-lane staged files match their
recorded hashes, and its staged external client matches the exact reviewed fixture.
Actual external worker PID lists agree with audited multiprocessing launches and
the new dead/reaped process-exit records. The caller method and original cleanup
bound remain satisfied.

At 1/2/4/8/64 workers, the activated source0 PID is dead with actual exit37. The
four-worker case naturally reproduces `exit code None` in the public message and
cause while the matching PID 8209 records exit37; all four process handles are
dead, with the other three terminated during ordinary cleanup. Its original
failure marker retains the exact message, and the recorded operation cleanup is
0.009821333 seconds. This is actual current installed Linux arm64 evidence for
the repaired race. It does not accept the ongoing full native runs, the other
dependency/architecture results, read-only-volume export equality or S2 as a whole.

## Latest evidence helper review

The export verifier adds an actual hash of target-owned
`site-packages/_e07_guard_config.json`, read through the already identity-verified
read-only native volume. For a process envelope, that hash must equal the
restoration receipt and `restored` must be true; it is then retained in export
equality. The existing complete snapshot, 59-member payload, six wheel metadata,
five unchanged installed metadata and installed RECORD gates remain intact.
The outer source and embedded client compile without execution. Actual mounted
restoration equality remains an execution/result gate.

The summarizer requires both successful watchdogs, all 18 exact dependency/cell
rows, installed exact IDs/full collection/stage/distribution checks, per-phase
outcomes and approved skips, image/architecture/read-only/offline/user/mount
metadata, and restored guard equality. For each of the 134 process cases it
checks exact enumeration, individual/aggregate/client equality, successful raw
command/log hashes, no timeout/owned cleanup error, successful non-emergency
child supervision and nonempty violation-free provenance. It explicitly leaves
release/story acceptance false/pending; an independent complete evidence review
is still necessary. A local import-path issue identified during review is fixed:
the exact reviewed version adds the repository to `sys.path` before importing its
maintained helper. It compiles; no summary was generated by this review.

| Exact reviewed input | SHA-256 |
| --- | --- |
| `tests/fixtures/e08_process_clients.py` | `93618044882f15d4736845f46356b5338749f5b66bd09472375de6fb802f30b9` |
| `scripts/verify_installed.py` (unchanged worker-count verifier) | `c915abcd7abc1740597dbf2988ecc58134bb0a48b6748442670609f2f16a2267` |
| `plan/library-implementation/e08-process-matrix-design.md` | `5c96a97d2f30c6a076683435d8c6dbb93ea26e3bf1193e2684297d1c44a2b894` |
| `plan/library-implementation/test-inventory.json` | `c774c0f0c6009a36843afc9c2da0720af50feca64266971fd1bf7927acb3cfbc` |
| `var/e08-completion-20261028/verify_native_exports.py` | `b495a775a13c754d54acc1f991e7f407af411a62246bb92c97bd8ef1c3a286b9` |
| `var/e08-completion-20261028/summarize_native.py` | `28ad7da4f5a9ea5fdb95aec3287e7298c28117c848e4dac09d1bf90e0e7f8667` |
| `var/e08-completion-20261028/reaped-exit-controls/control.py` | `ed67e5fb4e44a4ab038da14ec15795ef9b575fd5a92d883358ec224d4ee1a31e` |
| `var/e08-completion-20261028/reaped-exit-controls/commands.json` | `b8d5680acf4d08ed3f0d75924d13a64508100cd7f3914bcdce9ff887258c0db4` |
| `var/e08-process-20261028/reaped-exit-original-controls-v1.log` | `2b46c0fd831b178bc84e6fac5b33c880c571bfde68c6945ad6f9480252ef40fc` |
| `var/e08-completion-20261028/arm64-minimum-reaped-exit-observations.json` | `742964cee3739740b12e2c975eda55708af219a452a8cec6f83acb37c7bd1bdd` |
| Actual minimum `process-envelope/result.json` | `c18c33358c3aac5bb62bceea04fb57ae049651c1a7de10815f524ba8330f1c73` |
| Actual minimum four-worker exit `result.json` | `e60668ba25e01c1ac85547819bacfafa3487660187b761ba3e24fb55af035090` |
