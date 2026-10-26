# Independent E07 startup-provenance plan review — 2026-10-24

**Disposition: approved for bounded S11 implementation. No blocking plan finding.**
This approves the scope and strategy, not repair code, passing evidence, S6 or
E07 completion. A fresh independent review of the implementation and actual
controls is required before accepting S11; S6 retains its separate review.

## Reviewed scope

This is a fresh zero-history review against HEAD
`5c3f72b6cd16d0e14ac931f220a3f73eb7e5a02d` on
`codex/pypi-library-checklist`. I read the supplied handoff, the proposed
[startup repair](../execution-e07-startup-provenance.md), the
[execution plan](../execution-e07.md), canonical S11/backlog ordering, accepted
S1/S3/S2/S4/S5 review material, the
[rejected S6 proof](../evidence/e07-s6-rejected.json), both runner scripts and
the original public cancellation/cleanup fixture and assertions. No repair code
was written or executed by this review. Only this report was added.

The first S6 proof is correctly rejected despite 247 pytest passes: three
actual multiprocessing children have creation records without startup receipts.
The original `public-gap` and `public-startup` fixtures can terminate a new
process before its site hook completes. That is a test-instrumentation ordering
problem; it does not justify accepting absent provenance or changing the tests.

## Strategy assessment

The bounded parent-side gate is a suitable test-only repair. Registering an
actual child and then waiting for its audited startup receipt before returning
the launcher gives immediate-termination tests a complete origin record without
changing application code or their assertions. Keep the process receipt after
the existing complete installed-payload audit. A pre-creation monotonic anchor,
PID, OS creation identity and exact target prefix/site are sufficient matching
inputs for this owned-log mechanism. A valid fresh receipt can remain valid
after the child exits; current liveness must not be a success requirement.

The proposed multiprocessing insertion point is viable because the site hook's
audit does not depend on receiving the parent's pickled process payload. It
must stay application-free. Waiting for application imports, spawn-main
initialization or fixture readiness at `spawnv_passfds` would instead risk a
cycle with the parent's subsequent pipe write. No general launcher redesign,
new IPC protocol or application handshake is needed for the approved repair.

The original process planner begins its operation clock after initial process
construction/submission. The existing public tests separately retain real wall
time and supervisor limits. Synchronization overhead must be reported and must
fit those unchanged limits; it cannot become a new scheduling allowance or an
adjustment to the .8-second operation deadline, .3-second observation bound,
10-second aggregate cleanup budget, 13.8-second elapsed assertions or existing
hard supervisors. This instrumented run does not establish uninstrumented
startup latency. The final bare/image and R1 controls remain required.

## Implementation acceptance details

These are concrete consequences of the proposed bounds and fail-closed design,
to be checked in the implementation review:

1. Use captured real monotonic timing and sleep functions for the at-most-three
   second wait. Application/test clock monkeypatches must not turn the guard's
   wait into an infinite loop or cause it to expire immediately. Reuse one
   deadline per launcher gate; nested supported wrappers must not reset it
   repeatedly. The polling path must not acquire a lock that a forked child
   could inherit held.
2. Register the child PID and OS creation identity before waiting or failing.
   Timeout, malformed/missing receipt, parent termination and early child exit
   must retain receipt-bound cleanup. Never signal a reused/foreign PID. If
   the OS lookup races a very short-lived child, use an explicitly validated
   fresh receipt containing its OS identity or fail closed; do not silently
   downgrade to PID-only matching or treat `child_created=None` as proof.
3. Treat a missing or invalid startup receipt as a durable guard violation.
   A catchable ordinary startup exception alone is insufficient: application
   error handling or a test expecting startup failure must never turn it into
   lane success. Retain registration and failure evidence even if a late
   receipt appears after the deadline.
4. Read concurrent append-only records without accepting a truncated row.
   During polling, an incomplete trailing line can be retried; complete invalid
   JSON or invalid required fields must fail closed. Receipt matching must use
   the pre-creation anchor, not the later parent registration timestamp,
   because a child may write first. Accept only `process` receipts with the
   expected identity and target environment after the complete payload audit.
5. Keep the separate final post-exec freshness check. A fresh fork receipt
   establishes that fork's initial state, but cannot establish the interpreter
   subsequently entered by exec. Preserve the missing-post-exec negative and
   every existing unsupported-launch rejection.

The bounded half-day S11 split is reasonable for these existing wrappers and
tests. If solving an implementation issue requires a new launcher family,
application changes, waiting on user code, altered E04 timing/assertions or a
new synchronization protocol, stop and review a revised split before that work.

## Required proof before acceptance

Run the full accepted S1 positive/negative and ownership matrix against the
final changed runner bytes, including actual env/POSIX/fork/exec/console/spawn,
missing hook, detached success/timeout, unsupported launch, omitted
module/resource/metadata, final pytest receipt and output-preservation controls.
Add actual early-termination positives for each supported creation route and
show receipts exist before the caller can immediately terminate its child.
Exercise a missing-hook timeout with a still-live child, a short-lived child
that already wrote its receipt, stale/wrong-identity receipt rejection and
concurrent log reading. Prove that timeout remains bounded and cleanup retains
only owned identities. Existing post-exec failure remains independently gated.

Run the original public selection with exact 250 full/247 selected IDs,
unchanged fixture/assertion bytes, fault supervisors and receipt reconciliation,
then mechanism checks/lint/whitespace. Preserve the original rejected S6 proof
and bind all new evidence to exact runner/guard/test/inventory identities.
Independently review S11 before the separate S6 acceptance review. Accepted
earlier lane evidence remains historical until the required final artifact
parity reruns bind the final tooling.

No private environment file was read, remote IMDb contact made, live service or
developer smoke launched, foreign resource modified, Docker cleanup performed,
or push/publication/E08+ work undertaken by this review.
