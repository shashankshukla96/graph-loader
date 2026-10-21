# Independent E04-R1 plan review — 2026-10-20

**Disposition: accepted for implementation. No actionable plan finding remains.**
This accepts the repair plan, not repaired code, executed regression evidence or
E04 completion. S3/S4 remain reopened and E05 remains blocked until those later
gates pass.

Reviewed current HEAD `6508bd31779c49c0d1dd78f0ee242c91b9f9a9fc` and the pending
`execution-e04-r1.md` and tracker changes. Plan SHA-256:
`3acb490c61048068c99b2c39e26b600175df14622336acd45563b6be016815eb`.
This independently initialized reviewer read the actual current source and tests,
canonical `epics/e04.md`, `cancellation-contract.md`, the original execution plan,
and both completion/lifecycle revalidation reports. No applicable AGENTS.md was
found. Only this report was written; no source/test edits, test executions,
service operations, commits or pushes were performed. No private environment
file or remote IMDb was accessed.

## Source and test-gap assessment

The current `_gather()` confirms the reported defect. It receives a reply,
performs a fresh `_check()`, saves a non-ok diagnostic/cause, and then receives
every remaining worker before raising. A later blocked lane therefore prevents
the known failure from reaching `finish()`'s exception/finally cleanup. A signal
or deadline checked after receiving the error can also replace that diagnostic.
The existing shared cleanup budget starts only in `close()`, so it cannot bound
this earlier wait.

The planned immediate raise before the post-receive check is an appropriately
narrow correction. It preserves the current ordinary RuntimeError form and its
inline original exception or remote diagnostic cause. Successful replies retain
their post-receive checks and ordered results. `finish()` already writes the
failure marker and invokes `close(terminate=True)` on an ordinary failure; no
cleanup or transport redesign is justified by this finding alone.

The actual current fixture's `public-source-error` supplies only one failing
source. Its separate multiple-worker control blocks every source. Existing
public cleanup assertions check a general total duration and error class in the
marker, but do not establish prompt transition from a received source error to
cleanup alongside a blocked peer. The new mixed-source and deterministic
post-error signal/deadline controls directly cover these omissions.

## Required implementation and evidence boundaries retained by the plan

- Real spawn cases cover both the supported default without a timeout and a
  finite timeout. Readiness coordination makes the peer block before failure;
  receive instrumentation only observes the actual returned response. The
  external owner bounds regressions and cleans only its own client group.
- The planned assertions distinguish prompt cleanup entry after error observation
  from aggregate cleanup duration, and require the ordinary error/cause,
  source-bearing marker, all child/endpoint exits, no helper/heartbeat leak,
  lock/spill reclamation and unchanged caller-owned sentinel/signal. Deterministic
  controls cover signal/deadline activation immediately after the error reply.
- The guarantee begins when this ordered gather receives the error. An error
  behind an earlier blocked lane remains outside this repair's observation
  guarantee. The plan explicitly states this limit; no multiplexing or broader
  scheduling promise is implied. Broader lane-placement coverage is useful but
  is not a missing gate for the concrete known-first-error correction.
- Existing successful-result, cancellation, timeout, ownership, partial-frame,
  backpressure, partial-startup and unreapable-worker controls remain required.
  The inherited warning/skip/deselection ownership is recorded without turning
  those outcomes into passes or weakening the approved contract.
- Fresh focused/full checks, isolated original and no-Git rebuilt artifacts,
  two clean installs, all four newly built immutable component images and exact
  no-skip B3 are required after the source change. Historical green evidence
  cannot certify the repaired bytes.
- The separate zero-history repair review and final adversarial evidence review,
  repair/retest/re-review loop, safe manifests, link/render checks, staged-diff
  inspection and actual-SHA documentation follow-up preserve the completion and
  local commit protocol. E05, publication, remote IMDb, foreign cleanup and push
  remain outside scope.

No additional actionable discrepancy was found between this plan and the
canonical E04/cancellation requirements. The implementation review must verify
these promised controls in actual code and fresh results before restoring E04
acceptance.
