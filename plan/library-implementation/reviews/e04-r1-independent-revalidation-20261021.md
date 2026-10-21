# E04-R1 independent revalidation — 2026-10-21

**Verdict: APPROVED for the scoped R1 repair. No actionable finding.**

Reviewed HEAD `1871898` and repair `aa03f2c0d71409a7ed46534ec57780e5bef966bf`
from a new reviewer context. Read the E04 epic, R1 execution plan, original
completion-verification blocker, current repair evidence, production diff,
worker/transport/cleanup paths and the new tests/supervisor. No applicable
AGENTS.md was found. This review edits no product/test/build input and makes
no commit, push, remote-service mutation or publication.

## Assessment

`src/neo4j_graph_loader/loader/edge_planner.py:1580–1595` fixes the reported
failure: a received non-ok reply raises immediately with the inline original
exception or a local cause carrying the remote diagnostic. It neither receives
the next lane nor calls the post-receive interruption check first. Explicit
`is None` preserves even a false-valued original exception. The unchanged
`finish()` failure path writes the diagnostic marker and invokes terminating
cleanup under the existing aggregate budget. Successful replies retain their
post-receive check and ordered accumulation; broadcast, transport and cleanup
logic have no production changes in this repair.

`tests/test_public_cleanup.py:166–216` covers both default and finite-timeout
mixed-worker failures and deterministic post-reply interruption races. These
are substantive public planner regressions: the mixed cases use actual spawned
children and real source iteration, with a readiness handshake proving the
unrelated source entered its blocking loop. Observation wrappers delegate to
production receive/close. Liveness and resource values are captured before
fixture emergency cleanup. Fresh output directories prevent stale readiness
files, and the external supervisor bounds its own client process group.

The guide correctly limits immediate priority to **received** errors. A failure
behind an earlier blocked lane can remain unobserved; this is unchanged and
explicitly outside the narrow receive-order repair.

## Fresh executions

Interpreter: `var/e04-r1-20261020/dev/bin/python` (the environment recorded in
current repair evidence).

```text
-m pytest tests/test_public_cleanup.py tests/test_public_cancellation.py tests/test_planner_transport.py tests/test_public_lifecycle.py tests/test_public_api.py tests/test_external_planner.py tests/test_library_preflight.py tests/test_planner_abort.py tests/test_edge_planner.py tests/test_planner_imports.py tests/test_cli_shutdown.py -q
```

**221 passed in 58.43 seconds**, exit 0; no skip, failure or warning reported.
This includes all four new cases and existing cancellation, partial-frame,
backpressure, shared-budget, retained-owner, startup and CLI shutdown controls.

Separately authored public probes are retained under
`var/e04-r1-independent-revalidation-20261021/` (`probe.py`, `supervise.py`,
`results.json`, per-case logs). Each runs in its own subprocess/session with a
15-second external cutoff and cleanup confined to that owned group:

| Additional public real-spawn case | Observed result |
| --- | --- |
| Failing lane 0, blocked lane 1, default / finite timeout | Both preserve source RuntimeError / LookupError diagnostic cause. |
| Successful lane 0, failing lane 1, blocked lane 2, default / finite timeout | Both propagate lane 1 failure without receiving lane 2. |
| Timeout / cancellation activated immediately after a process error reply | Both preserve the received source failure. |
| Blocked lane 0, failing lane 1, finite timeout | Timeout with zero observed error replies, matching the documented boundary. |

All seven clients exit 0 before the independent deadline. The six received-error
operations finish in **0.131946–0.141009 seconds**, with **0.001372–0.001537
seconds** from observed failure through cleanup/raise. Each asserts the source
marker, dead production children, closed endpoints, no active child/heartbeat,
and reclaimed lock/spill **before fixture recovery**. The blocked-first boundary
case times out and cleans up in 0.517295 seconds.

Additional deterministic controls in `deterministic.py` pass: two public inline
false-valued-cause/cleanup cases under post-reply timeout/cancellation; successful
result order and all five expected checks for two lanes; successful-reply timeout
and cancellation checks; and an old-versus-repaired gather comparison. The old
gather is extracted from `aa03f2c^` into memory without replacing source files:
it hits an unrelated-peer receive guard, while the repaired gather raises the
source error before that guard. Ruff passes for the repaired source/tests and
new review scripts; `git diff --check` passes.

## Limits

This independently approves R1 source, regression realism and affected behavior.
The complete suite, B3, archives, clean installs and image matrix were not rerun
by this reviewer; their separate current verification remains the coordinator's
responsibility. Local executions do not establish broader E08/E09 platform
support. Existing synchronous inline/initial-submission/OS-uninterruptible
exclusions and sequential receive ordering remain in force.
