# E04.F2.S1 independent ownership checkpoint — 2026-10-18

Status: **accepted after repair**; R1 is closed (see final recheck below). No
lifecycle algorithm defect was found in the exercised S1 behavior.

Reviewed the actual dirty/untracked working tree against entry HEAD `98cd267`.
Read execution-e04.md, epics/e04.md, decision-brief.md, cancellation-contract.md
and the accepted e04-s1-review.md. Inspected the public schema/planning reexports,
LIBRARY_API.md ownership section, public lifecycle/API tests, inherited B1
preflight tests, planner abort/external tests, and actual start/finish/close,
output claim, spill and convenience topic paths. Reviewer wrote this report only.

## Independent validation

Using `var/e04-validation-20261018/dev/bin/python`, reran:

```
python -m pytest tests/test_public_lifecycle.py tests/test_public_api.py tests/test_library_preflight.py tests/test_planner_abort.py tests/test_external_planner.py -q
```

Result: **61 passed in 2.03s** (exit 0). Read the retained
`var/e04-validation-20261018/s2-tests.log`: 61 passed in 2.04s. Targeted Ruff on
public modules and test_public_lifecycle.py passed; `git diff --check` passed.
No `.env`, remote IMDb, commit or push operations performed.

## R1 [P2] Retain a public regression for failure after a worker is owned

Location: `tests/test_public_lifecycle.py:19` (lifecycle coverage);
related inherited coverage: `tests/test_library_preflight.py:85`.

The ordered execution plan explicitly requires public tests for partial startup.
The new lifecycle suite does not exercise startup failure. The inherited B1 test
replaces `_InlineWorker` with a constructor that always raises, so it fails
before the first handle is appended to `_workers`. It checks lock/spill removal
but never checks release of an already-created worker, the startup failure
marker, or one-shot state after this failure. The passing 61-test lane therefore
does not protect this stated ownership path when later transport changes begin.

Add a public-factory regression with two lanes: let the first worker be created,
then fail creation or initial submission of the second. Test-only injection into
the existing internal worker seam is appropriate; application calls should use
the approved public imports. Assert the original startup failure, closure of the
already-owned first worker, removal of the owned spill directory and output lock,
retention of the startup `.failed` marker, and rejection of restart/finish after
the failure. This requires a regression test, not a lifecycle algorithm rewrite
or the future aggregate cleanup/liveness guarantee.

Independently reproduced the desired current behavior using public `load_schema`,
`PlanSettings` and `producer_edge_planner`, an explicit inline context, two schema
edge types with `workers=2`, and a test patch that raises
`RuntimeError('second worker startup')` on the second `_InlineWorker` constructor.
Actual results:

```
RuntimeError second worker startup
created_workers 1 worker_state_closed True
closed True lock_exists False spill_exists False
failure_marker RuntimeError: second worker startup
```

## Otherwise verified

One-shot completion, active start/closed close idempotence, finish-path rejection,
exclusive live/stale output locks, protection of owner progress/evidence, clearing
stale result/failure state after claim, unique spill directories for matching plan
IDs, caller/convenience topic ownership, topic retention on success and logged
best-effort deletion failure all agree with the documented ordinary paths and
focused tests. Inline original causes and process diagnostic causes are preserved
by the accepted S1 change. Documentation explicitly prohibits overlapping
lifecycle calls and includes try/finally usage.

S5 private transport, S3 public cancellation/deadline integration, S4 aggregate
cleanup and S2 process effects remain separate future gates. Their absent
behavior is not a finding here; this checkpoint certifies no new cancellation or
bounded cleanup claim. Re-review R1 before moving to private transport.


## Repair recheck — accepted

Inspected the new `test_public_partial_startup_reclaims_the_first_owned_worker`.
It calls the public factory with two public `IterableEdgeSource` objects and two
workers, allows the first real inline worker to be owned, then injects failure
of the second constructor. Assertions cover the startup error, closure of the
first worker state, the closed planner with no retained handles, released lock
and spill directory, and retained startup failure marker. Closed-call rejection
is also covered by the existing public one-shot test. No production code changed
for this repair. **R1 is closed; no unresolved findings for E04.F2.S1.**

Independently repeated the exact five-file focused command above against the
repaired source: **62 passed in 2.02s** (exit 0). The updated owned s2-tests.log
records 62 passed in 2.03s. Targeted Ruff on public modules and lifecycle tests,
and `git diff --check`, pass. Earlier 61-test results are historical pre-repair
runs. This acceptance is limited to existing ownership/lifecycle behavior;
transport, public cancellation/deadlines, aggregate cleanup and process effects
remain their separately ordered gates.
