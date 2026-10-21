# Independent E04 lifecycle/API revalidation — 2026-10-20

Disposition: **changes requested**. One reproduced actionable finding remains.

Reviewed current HEAD `786260bec3cceb8ed14990896380640cb52115d2`, E04 implementation
`55dd4938188768090c10d851f8f7330706975ea5`, and baseline `98cd267`. Read canonical
`epics/e04.md`, `cancellation-contract.md`, `execution-e04.md` and
`docs/LIBRARY_API.md`; no applicable AGENTS.md was found. Inspected the actual
public schema/planning exports, settings/source/result contracts, start/finish/
close and convenience ownership paths, metrics default ownership, public import
fixture, and public cancellation/cleanup/lifecycle tests. Prior review approvals
were not treated as proof. No product changes, services, remote IMDb access,
commits or pushes were performed.

## R1 [P1] Abort on an observed source failure before waiting on another worker

Location: `src/neo4j_graph_loader/loader/edge_planner.py:1587–1592`
(`ParallelEdgePlanner._gather`); relevant negative-control gap:
`tests/fixtures/e04_faults.py:153–156`.

After receiving a worker's `error` response, `_gather()` saves its diagnostic
but continues receiving from all remaining workers before raising. A later
worker can be blocked inside `records()`. With the supported default
`timeout_seconds=None`, a source failure already received by the coordinator
therefore never reaches `finish()`'s exception/finally cleanup: the normal
blocked worker remains alive and the output lock/spill remain held. With a
timeout, `_check()` eventually raises `EdgePlanTimeoutError` instead, discarding
the already available ordinary source failure and its cause. The marker then
contains only the timeout. The separate 10-second cleanup budget cannot help
until the coordinator actually leaves `_gather()`.

The gather-all behavior exists in baseline `98cd267`; this is an inherited
defect, not a newly introduced gather scheduling regression. Nevertheless, E04
newly exposes this public lifecycle and claims source-error diagnostics plus
cleanup after failure. The current ordinary-source-error integration control
uses only `[FailingSource()]`, while the four-worker control makes every source
block. Neither tests one failed worker alongside one blocked worker, so these
passing controls do not establish that combined acceptance case.

Reproduction uses the public `producer_edge_planner`, `PlanSettings` and
`load_schema`, real default-spawn workers and the existing importable
`FailingSource()` (order 0) plus `BlockedSource(..., order=1)`, with two workers,
`memory_mb=1`, a temporary output path and no Kafka access. The source failure is
during scan, before producer creation. An observation-only wrapper around
`_ProcessWorker.recv` logs the received status/payload and returns it unchanged.

Retained independently bounded script:

```text
var/e04-validation-20261018/dev/bin/python \
  var/e04-revalidation-20261020/lifecycle-source-failure/probe.py
```

An optional first argument selects a separate output directory. The supervisor
starts each client in its own process group, bounds the default case at 2 seconds
and the timed case at 8 seconds, kills only that owned group, waits for the
client, and reclaims its own temporary output. No unbounded negative control or
service is required.

Actual saved results:

- `var/e04-revalidation-20261020/lifecycle-source-failure/default.log`: coordinator
  received `error` with `LookupError: ordinary child source failure` and the
  child's traceback. `default.json`: still waiting at the independent 2.007 s
  cutoff, output lock present, no failure marker; supervisor ended the owned
  process group.
- `var/e04-revalidation-20261020/lifecycle-source-failure/timeout.log`: coordinator
  received the same ordinary source error, then raised `EdgePlanTimeoutError`
  after 0.818 s for `timeout_seconds=.8`; `__cause__ is None`. Failure marker:
  `EdgePlanTimeoutError: edge planning operation deadline expired`. Normal
  timeout cleanup reclaimed the lock and spill (`cleanup_incomplete=False`).

Required repair: once an ordinary worker error has been received, propagate it
with its existing diagnostic cause and enter the shared terminate/cleanup path
without awaiting an unrelated blocked worker. Add an externally supervised
public regression combining failure and blocked workers with no timeout and
with a finite timeout, asserting the original diagnostic/cause, prompt error,
total cleanup bound, worker/endpoint liveness, failure marker and owned-resource
reclamation. The known-first-error case must pass; testing error placement across
lanes would additionally expose head-of-line error observation limits.

## Other independent verification and limits

The following focused command passed against the current editable source:

```text
var/e04-validation-20261018/dev/bin/python -m pytest \
  tests/test_public_api.py tests/test_public_lifecycle.py \
  tests/test_planner_imports.py tests/test_public_cancellation.py -q \
  -k 'not externally_supervisor and not external_supervisor and not supervised and not process_waits'
```

Result: **37 passed, 7 deselected in 0.79 s**. The deselections intentionally
leave the existing process fault matrix to the parent's fresh comprehensive
lane; the real spawn controls described above were additional independent
probes. Public planner and implementation module `__file__` both resolved to
this checkout's `src/neo4j_graph_loader`, and public/canonical
`ParallelEdgePlanner` identity was confirmed.

No additional concrete defect was found in the inspected one-shot lifecycle,
configured-output rejection, lock/stale-marker protection, convenience versus
caller-owned topic handling, operation clock anchors, cooperative inline scan,
shared cleanup scheduling/retained-resource branch, private default metrics
ownership, schema error wrapping or import-effect controls. This statement is
limited to the inspected branches and focused executions; it does not certify
E08's platform/start-method/max-worker matrix or live Kafka/Neo4j behavior.

## Addendum — independent review of the final disposition and evidence

Read the primary report `e04-completion-verification-20261020.md`, its
`../evidence/e04-revalidation-20261020.json`, the five current status-document
diffs, and the primary reproduction script/logs/JSON under
`var/e04-revalidation-20261020-w62ij3nv/source-error-repro/`.

The primary reproduction independently corroborates R1: the received payload
contains the ordinary child's `LookupError` traceback in both modes; default
mode reaches the external 2.007329 s cutoff with the blocked source entered,
lock held and no marker. Timed mode exits with `EdgePlanTimeoutError` after
0.818383 s, no cause, and a timeout-only marker. The script owns an independent
client process group, bounds both modes and performs emergency cleanup of only
its own processes/output. This addendum does not rerun those probes or tests.

Independently read and verified every one of the evidence manifest's **17 raw
record sizes and SHA-256 values**. The before/after records are byte-identical;
all **259** captured source inputs still match the current files' bytes, sizes
and executable modes. Read the full-suite and exact B3 outcome records, artifact
command/probe records and image results. The retained records support the
reported 3,512 passes, four skips, 49 deselections and 19 warnings, the exact
no-skip B3 result, two 52-module clean-install artifact probes, and four image
matrices comprising **52 zero-exit commands** with network disabled and no
source mounts. This is verification of the parent's retained execution records,
not a claim that this reviewer reran those passing gates. Their stated scope
limits and the continued changes-requested verdict are appropriate.

The status changes accurately preserve approved plan design while reopening
only E04 implementation acceptance and S3/S4. The README, tracker and E05
handoff explicitly block E05 pending R1 correction, regression tests and
independent re-review; historical green results remain historical. No product
source change appears in the working-tree diff. **No material overclaim or
additional actionable discrepancy was found in these report/evidence/status
edits. R1 remains open; E04 completion is not accepted.**
