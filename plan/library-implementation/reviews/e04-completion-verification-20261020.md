# E04 completion revalidation — 2026-10-20

**Verdict: CHANGES REQUESTED. One P1 lifecycle finding remains open.**
Do not treat E04 as accepted or begin E05 until E04-R1 is fixed, regression-tested
and independently re-reviewed. The existing suite passes but omits this combined
failure case. No product repair or E05 implementation was performed in this review.

Reviewed clean HEAD `786260bec3cceb8ed14990896380640cb52115d2`, including
implementation `55dd4938188768090c10d851f8f7330706975ea5`. All **259** delivered
inputs match current source bytes, sizes and modes. Prior approvals/results are
preserved as historical evidence; this review supersedes their completion
disposition for the newly reproduced case.

## E04-R1 — P1: an observed source failure waits behind another blocked worker

Location: `src/neo4j_graph_loader/loader/edge_planner.py:1587–1592`,
`ParallelEdgePlanner._gather()`.

After receiving a worker's `error` response, the coordinator saves the error and
continues receiving all remaining workers' replies. If a later worker is blocked
inside its source, the known source failure does not reach `finish()` and its
shared cleanup path. With default `timeout_seconds=None`, it continues waiting
with its worker, output lock and spill still owned. With a finite timeout,
`EdgePlanTimeoutError` replaces the already received ordinary error and has no
source diagnostic cause. The failure marker also loses the source error.

Both the [independent lifecycle reviewer](e04-lifecycle-revalidation-20261020.md)
and the primary agent reproduced this using public imports, real spawn workers,
the existing `FailingSource()` in lane 0 plus `BlockedSource(..., order=1)` in
lane 1, two workers and no Kafka/server access. The observation wrapper logs
`recv()` results without changing their behavior. Primary-agent observations:

| Public configuration | Reproduced outcome |
| --- | --- |
| No timeout | The parent already received `LookupError: ordinary child source failure`, and the second source entered its blocking loop. The independent supervisor stopped the still-waiting client at **2.007 s**; output lock existed and no failure marker had been written. This is a bounded observation of the unbounded receive branch, not a test allowed to hang indefinitely. |
| `timeout_seconds=.8` | The parent received the same ordinary source error, then raised **`EdgePlanTimeoutError` after 0.818 s**, with `__cause__ is None`; the failure marker contained only the timeout. Timeout cleanup reaped the children and reclaimed the lock/spill. |

The ordinary-source-error control currently uses only one failing source; the
multiworker control blocks every source. Neither combines a known failure with
an unrelated blocked peer. The new cleanup budget cannot help until `_gather()`
actually leaves its receive loop.

The gather-all strategy predates E04, so this is not attributed as a newly
introduced scheduling regression. It remains a blocker for E04's newly exposed
public error-diagnostic and cleanup-after-source-failure contract. Specifically,
the current S3 cause-preservation and S4 end-to-end failure-cleanup gates are
reopened; their approved design/estimates/dependencies remain unchanged.

**Required correction:** propagate an already received ordinary worker error
with its diagnostic cause immediately, without awaiting the unrelated blocked
worker, and let the owned shared terminate/cleanup path run. Add externally
supervised public regressions for this mixed-source case with no timeout and
with a finite timeout. Assert the ordinary error/cause, prompt failure, aggregate
cleanup time, every child/endpoint's liveness, source-bearing failure marker and
owned lock/spill reclamation. Preserve caller-owned resources and all existing
timeout/cancellation/partial-frame/backpressure controls. Error placement across
lanes is an additional useful check; the known-first-error case is the minimum
required repair here. Re-run affected/full checks and independent review before
restoring E04 completion or releasing the E05 handoff gate.

## Reproduction and retained evidence

Primary bounded script:
`var/e04-revalidation-20261020-w62ij3nv/source-error-repro/probe.py`.
Its fresh installed interpreter is recorded in the sibling review directory's
`paths.json`; run that interpreter with the script from the repository root.
The script supervises separate client process groups, captures the received
error, bounds both cases, kills/reaps only its own group and removes only its own
temporary output. It does not call Kafka, Neo4j or remote IMDb. Separate primary
and independent logs are retained; one execution did not overwrite the other's.

[Machine-readable verification](../evidence/e04-revalidation-20261020.json)
records exact commands, identities, outcomes and raw-record SHA-256 values.
Raw fresh output is retained under ignored
`var/e04-revalidation-20261020-w62ij3nv/`.

## Checks that passed

| Gate | Fresh result |
| --- | --- |
| Declared dependencies + editable install in a new CPython 3.12 environment | Passed; repository environment preserved. |
| Full unit/example suite, excluding integration/smoke | **3,512 passed, 4 inherited skips, 49 deselected, 19 known warnings**, pytest **189.14 s**; exit 0. The two additional mixed-source probes above still expose the blocker. |
| Exact isolated maintained B3 regression | **1 passed, no skip**, supervised exit 0, no timeout, no remaining containers. |
| Fresh isolated wheel/sdist and extracted-sdist rebuild without Git | Passed, including strict Twine and original/rebuilt wheel-member byte equality. |
| Two clean noneditable installs away from checkout | Passed: **52 runtime modules**, installed origins, inert imports, public success/error/cancel/deadline examples, old-import rejection, eight component helps and dependency checks each. These examples do not test the mixed-source failure. |
| Four retained immutable E04 image matrices | Freshly executed **52 commands** passed with network disabled and no source mounts. Source-module hashes match current 52 modules. No new image build is claimed by this review. |
| Lint, requirements parity, dependency consistency and whitespace | Passed. |
| Independent private transport checks | **55 tests passed**; **50,002 malformed-payload probes** found no unexpected decoder exceptions. [Scoped transport review](e04-transport-revalidation-20261020.md). |
| Independent focused API/lifecycle/import checks | **37 passed, 7 intentionally deselected**, plus the additional real-process failure reproduction. [Scoped lifecycle review](e04-lifecycle-revalidation-20261020.md). |

Before/after all **259** source inputs and the fresh environment match. Current
source identity: `7b7a0ed8ab192b813bf50a05e4e7425ae9e1f4b452d9919b27e663c9caa86946`;
environment: `9e5780f8fd795e55c49853009e4dd5e738ed8f30acd9c3c5beceb4ccb0bb17ee`.
Code was not changed to obtain the failure or passing results.

The four inherited skips remain the bash-only control, two guards against absent
historical Git objects and the intentionally multi-pair example. Fifteen IMDb
subset and four planned-edge multithreaded-fork warnings remain A5/E08 work;
the inherited mocked-unit localhost Kafka diagnostic remains visible. Skips and
deselections are not passing evidence. Native macOS arm64 and retained Linux
arm64 image probes do not establish E08/E09's broader matrices.

## Current disposition

E04 implementation acceptance is **changes requested**, with S3/S4 reopened.
The change tracker and E05 handoff gate record E04-R1; original reports and
historical green runs are preserved. Only review/evidence/status documentation
changed. No product fix, new E05 work, publication, tag, GitHub push or remote
graph operation was performed.
