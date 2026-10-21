# E04 repair revalidation — 2026-10-21

**Disposition: accepted. E04-R1 is closed; no actionable finding remains.**
E04 S3/S4 completion and the existing E05 handoff gate are valid.

Reviewed clean entry `1871898ea350d0e17c0e926b3b87595fbde0c2ef`, including
repair `aa03f2c0d71409a7ed46534ec57780e5bef966bf`. This is a new review of
the delivered repair, preserving the [original failed validation](e04-completion-verification-20261020.md).
No runtime code was changed during this review. E05 remains unimplemented.

## Repair and fresh reproduction

`ParallelEdgePlanner._gather` raises a received non-ok reply before receiving
another lane or checking interruption again. Inline original causes and remote
diagnostic causes are retained. Successful replies keep their post-receive
checks and lane order. The error therefore reaches the existing failure-marker
and shared terminating cleanup paths immediately.

Reran the original independent lifecycle reproduction against current committed
code in a new owned directory. Both default and finite-timeout clients exited
normally under their independent supervisor. Public operations took 0.125 and
0.129 seconds; both observed the blocked peer, retained the ordinary source
error with a remote LookupError diagnostic cause, wrote a source-bearing failure
marker, closed the planner and reclaimed its lock. Neither reported incomplete
cleanup or required supervisor termination. The new committed regression cases
add coordinated peer readiness and final child/endpoint/spill/heartbeat and
caller-ownership assertions. Deterministic controls cover timeout/cancellation
becoming active immediately after receiving the failure.

## Independent verification

A new zero-history reviewer [approved the scoped repair](e04-r1-independent-revalidation-20261021.md)
with no actionable findings: **221 focused tests passed** in 58.43 seconds,
plus seven separately authored, externally supervised public real-spawn cases.
Those cases include a successful first lane followed by a failed second lane
and blocked third lane, process-error interruption races, and the documented
unobserved-error boundary. Extra deterministic controls preserve false-valued
inline causes, success checks/order and an extracted-old-code negative control.
No source/test changes were needed after review.

## Fresh complete-suite and static checks

The delivered repair's development interpreter ran:

```text
var/e04-r1-20261020/dev/bin/python -m pytest tests examples/imdb/tests -m 'not integration and not smoke' -q -rs
```

**3,516 passed, four skipped, 49 deselected, 19 warnings in 187.80 seconds;
exit 0.** The four inherited skips remain the bash-only shell control, two
historical missing-Git-object slice guards, and the intentionally multi-pair
schema exclusion. The 19 existing fork warnings remain owned by A5/E08:
15 IMDb subset and four planned-edge warnings. No new skip or warning appeared.
Ruff, dependency parity, pip check, whitespace and canonical plan rendering pass.
The source inputs and environment remain unchanged after all checks.

[Machine-readable results](../evidence/e04-revalidation-20261021.json) record
the reviewed commit, exact fresh commands/results, retained-proof audit and
raw-log hashes. The [existing E05 handoff](../handoff-e05.md) remains the next
implementation scope; this review performs no E05 work.

## Proof continuity and limits

Recomputed the allowlisted manifest with the repair's owned CPython 3.12
environment. All **259** tested input contents, sizes and executable flags equal
current files; committed HEAD blobs also match every input's content and size.
The environment digest is unchanged. HEAD-based source digests differ because
documentation delivery advanced HEAD; per-file comparisons establish continuity.

Independently checked all **25** retained result-log hashes, **9** artifact-command
logs and **57** image-command logs. The latter preserve one initial registry EOF
failure followed by successful builds of all four images and **52** successful
immutable-image checks. Reinspected the original and rebuilt archives against
current source: all **52** runtime modules, exact inventories and metadata match;
all original/rebuilt wheel member bytes agree. All **12** retained checkout-free
mixed-source cases have matching raw logs, correct ordinary diagnostic causes,
successful child/endpoint cleanup and reclaimed ownership.

The repair's complete suite, exact no-skip B3 service proof, clean installations
and image checks are recorded in [repair evidence](../evidence/e04-r1-repair.md).
This review audits those retained service/build/image results rather than
repeating unchanged passing gates. Its full source-suite rerun and original
reproduction are fresh executions. No private environment file, remote IMDb
service, publication or push was used.

Sequential lane receives remain intentional: a later-lane error can remain
unobserved behind an earlier blocked lane until interruption. This documented
boundary is separate from the repaired handling of an already received error.
The synchronous caller/inline and OS-uninterruptible exclusions remain; E07
installed full-suite and E08/E09 support matrices remain future work.

Owned raw review outputs: `var/e04-check-20261021/`.
