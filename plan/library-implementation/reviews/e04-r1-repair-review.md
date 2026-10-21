# Independent E04-R1 source/test repair review — 2026-10-20

**Disposition: accepted within the source/test repair scope. No actionable finding remains.**
This closes the reviewed E04-R1 code/test defect; it does not accept the final
whole-proof evidence or restore E04 S3/S4 completion/E05 handoff by itself.
Fresh complete-suite, artifact, image and exact B3 acceptance remains the
separate final adversarial evidence gate required by the execution plan.

Reviewed HEAD `6508bd31779c49c0d1dd78f0ee242c91b9f9a9fc` plus the actual pending
source, fixture, test and API-documentation diff. This independently initialized
reviewer read `execution-e04-r1.md`, the accepted `e04-r1-plan-review.md`, the
superseding `e04-completion-verification-20261020.md` and the approved
`cancellation-contract.md`. No applicable AGENTS.md was found. Only this report
and ignored owned review outputs were written; source/tests were not edited.
No private environment file, remote IMDb, foreign resource, global cleanup,
service operation, commit or push was used.

## Repair assessment

At `src/neo4j_graph_loader/loader/edge_planner.py:1580–1595`, `_gather()` now
raises immediately when `worker.recv()` returns a non-ok response, before the
following `_check()` and before receiving any later lane. Its ordinary
RuntimeError retains the inline exception object as its cause, or a local
RuntimeError containing the existing remote type/message/traceback diagnostic.
The explicit `is None` test also avoids accidentally discarding an original
false-valued exception. Successful replies retain both surrounding checks,
ordered result accumulation and existing broadcast/phase behavior.

The exception still reaches the existing public `finish()` marker and shared
`close(terminate=True)` path (`:1619–1635`); the repair introduces no new cleanup
budget, receiver helper, scheduler or protocol. The old gather-all collection
that delayed cleanup behind a blocked peer is removed. First observed failure
is intentionally decisive, rather than an aggregate of later failures.

`docs/LIBRARY_API.md:99–107` explicitly limits the promise to a received failure.
Ordered receives can still leave a later-lane error unobserved behind an earlier
blocked lane. This matches the accepted narrow repair and avoids implying
multiplexed receive or arbitrary inline-code interruption.

## Test and ownership assessment

`tests/fixtures/e04_faults.py:89–97` makes the failing source wait for the peer's
readiness file before raising. The peer itself touches this file inside
`BlockedSource.records()` immediately before its endless sleep loop. The new
output directory is created exclusively by the supervisor, so a reused readiness
file cannot satisfy the handshake. The fixture asserts the failing source is
actually in lane 0 and uses two real spawned production worker processes.

The receive wrapper calls the production `recv()` first and only records its
returned diagnostic/time. The close wrapper records first cleanup entry and
then delegates unchanged. Both are observation controls; neither manufactures
a reply nor unblocks the peer. State recorded for the test is captured before
the fixture's emergency `finally` reap, so emergency cleanup cannot disguise a
production child/endpoint leak or unreclaimed lock/spill. The outer supervisor
uses a separately created owned process group and a hard timeout; it does not
kill unrelated processes or remove foreign resources.

`tests/test_public_cleanup.py:166–189` exercises the public mixed failure with
both `timeout_seconds=None` and `.8`, separately bounds each client, and checks
source diagnostic/cause, prompt cleanup entry, aggregate cleanup allowance,
children/endpoints/heartbeat, source-bearing marker, owned lock/spill and the
unchanged caller sentinel/signal. Both controls would reject the old behavior:
the default case is externally bounded and the finite case rejects a timeout
replacing the source error.

The deterministic public inline controls at `:192–216` make cancellation or
timeout become true immediately after the actual error reply is returned.
They assert original exception identity and successful shared cleanup. The
retained `red-check.log` shows both fail against the old code with the specific
post-receive timeout/cancellation replacement; the retained old real-spawn
reproductions separately show the default stall and finite diagnostic loss.
These controls cover a race the real finite-timeout case alone would not prove.
Existing partial-frame, later-send/stop backpressure, ownership, partial-startup,
unreapable-worker, cooperative inline and success controls remain in place.

## Independent execution

Interpreter: owned `var/e04-r1-20261020/dev/bin/python`, CPython **3.12.14**.
The imported planner resolves to this checkout's repaired `src/` module.
From the repository root, the independent focused command was:

```text
var/e04-r1-20261020/dev/bin/python -m pytest -q tests/test_public_api.py tests/test_public_cancellation.py tests/test_public_cleanup.py tests/test_public_lifecycle.py tests/test_planner_transport.py tests/test_edge_planner.py --basetemp=var/e04-r1-20261020/repair-review/pytest-tmp
```

Result: **157 passed in 15.75 s**, with no skip/warning/failure in the retained
`var/e04-r1-20261020/repair-review/focused.log`. This independently executes the
four new controls alongside the existing relevant public/transport/planner
regressions; it does not substitute for the complete-suite gate.

The retained independent mixed client/supervisor records under
`repair-review/pytest-tmp/test_observed_source_error_doe{0,1}/mixed/` show:

| Configuration | Error-to-cleanup entry | Observation through cleanup/raise | Total public operation | Supervisor |
| --- | --- | --- | --- | --- |
| No timeout | 0.000146 s | 0.001522 s | 0.132852 s | exit 0, no timeout, no emergency group cleanup |
| `.8` seconds | 0.000135 s | 0.001477 s | 0.123864 s | exit 0, no timeout, no emergency group cleanup |

Both retain `LookupError: ordinary child source failure` as the local cause and
marker diagnostic; both report two dead production children, closed production
endpoints, no active child/heartbeat, no lock/spill, and preserved caller state.

Independent Ruff over the repaired module, changed fixture/test and external
supervisor returned **exit 0, All checks passed**. `git diff --check` also returned
**exit 0**. No actionable concern required a source/test change or retest loop.

Reviewed content SHA-256 values:

| Input | SHA-256 |
| --- | --- |
| `src/neo4j_graph_loader/loader/edge_planner.py` | `0d8b1066b9a0352a489cf270073a8ed67cc1c389ed61f13b02907f747bf559a8` |
| `tests/fixtures/e04_faults.py` | `d73e790ca630d7f14f4d97d6a7aa08ec60223db7aa372cd33939b5dde2768556` |
| `tests/test_public_cleanup.py` | `f44c66c8d9370427b6718aaaf4a84e7d53e164acfeaa6ec055c6607e40ef7f63` |
| `docs/LIBRARY_API.md` | `0286d1cac6a7338d6e53fd62b51ce8c247e10076f9be8a6391789921104b205e` |
| `execution-e04-r1.md` | `3acb490c61048068c99b2c39e26b600175df14622336acd45563b6be016815eb` |
| `reviews/e04-r1-plan-review.md` | `9fbd747fe62eb8e2aab138b0422f2fb85dcc5244f059a23ba3fb5b97e8b011cc` |
