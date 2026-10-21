# Independent E04.F2.S3 cancellation review — 2026-10-18

**Accepted for S3: no actionable findings.** This zero-history reviewer read the
actual handoff, execution plan, S3 acceptance criteria and approved cancellation
contract, then inspected the current implementation and ran independent checks.
This is acceptance of public interruption/clock integration, not S4's future
aggregate cleanup/resource guarantee or final E04 artifact/platform acceptance.

Reviewed checkout: HEAD `98cd267db5c70d3517a89ec68c1a4cedf2c254fd`, including
the dirty/untracked E04 implementation. No implementation files were edited.

## Verified behavior and negative controls

- `edge_planner.py:1298-1312` validates positive finite timeouts and an
  `is_set()`-capable signal before initialization side effects. The public class,
  producer factory and convenience function propagate the controls;
  `planning.py` explicitly exports both approved errors under `EdgePlanError`.
  An additional nine independent invalid-control probes covered all three
  interfaces, with no output state created.
- `edge_planner.py:1349-1363,1475-1487` stores one clock/deadline. Process mode
  anchors only after every initial submission completes; inline mode anchors
  before synchronous scan submission. `SourceTask.scan:494-498` checks before
  obtaining the source iterator and after each returning record. Neither later
  phases nor a second `start()` resets the stored deadline.
- `edge_planner.py:1500-1521,1533-1535,1554-1660` checks gather, broadcast,
  callbacks, phase transitions and parent results. The process connection uses
  the same public check after initial submission. `FrameStream` invokes that
  check during incremental header/body reads, later-command writes, selector
  waits and codec traversal. No coordinator blocking recv/send/sendall was
  substituted after the deadline begins.
- The public tests include the ten-record inline scan expiry, cooperative
  signal, cumulative phases/callback time and actual process start/finish gap.
  The gap fixture advances the excluded submission clock by five seconds before
  asserting the anchor is five, then expires the stored deadline before finish.
- All six blocked-source/partial-reply/later-command cases run through the
  independent process-group supervisor for timeout and cancellation. The
  partial fixture sends a valid large frame but stalls after body bytes; its
  parent-side probe requires actual body consumption before arming cancellation
  or accepting timeout activation. The send fixture requires actual write
  readiness waiting on the later `degrees` command, after a real counter reply;
  scan submission or initial traffic cannot satisfy that marker.
- The supervisor owns an independent hard deadline and emergency cleanup of
  only its newly created process group. S3 observations are recorded when the
  public check raises, before legacy cleanup, so the tests do not accidentally
  require S4 behavior. Original inline source exceptions remain the actual
  cause; remote errors retain type/message/traceback in a local diagnostic cause.
  A separately executed supervised `public-source-error` probe passed with the
  expected `LookupError: ordinary child source failure` diagnostic and cause.
- `docs/LIBRARY_API.md:101-137` describes controls, one clock, supported waits,
  inline cooperation and construction/pickling/callback/admin/OS exclusions.
  It explicitly leaves aggregate cleanup and the full platform matrix open.

## Executed checks

Using `var/e04-validation-20261018/dev/bin/python`:

```text
-m pytest tests/test_public_cancellation.py tests/test_external_planner.py
  tests/test_public_api.py tests/test_public_lifecycle.py
  tests/test_library_preflight.py -q
78 passed in 6.93s
```

Focused Ruff over the public/implementation modules, cancellation tests, fault
fixture and supervisor: **all checks passed**. The independently supervised
ordinary-source-error probe returned exit 0, no hard timeout or emergency group
cleanup, and retained the remote traceback. Nine invalid-control probes passed.
An initial pytest invocation named nonexistent `tests/test_preflight.py`; it
collected no tests and was corrected to the exact command recorded above.

## Reviewed source identities (SHA-256)

| File | SHA-256 |
| --- | --- |
| `src/neo4j_graph_loader/loader/edge_planner.py` | `ac69a48f4f6f7d70fd9a60b215cc45653df673d8524895a9f416500202195274` |
| `src/neo4j_graph_loader/loader/edge_plan.py` | `247be9b892a345dfdb9d942c649e97ea6086005232519d6dadf2e084fefe027f` |
| `src/neo4j_graph_loader/planning.py` | `3064cfbfd28721a8250a2629ca1380474d1acb4a1517e195730670af187a0d8a` |
| `docs/LIBRARY_API.md` | `9bef148dc277090eac9052c5808203e3e4005e7703cc405c48f8e2775147004c` |
| `tests/test_public_cancellation.py` | `646e69e27329bd96047ba4b900c724bc0552e106cc64055d5edb39420784222f` |
| `tests/fixtures/e04_faults.py` | `4f551f1582f6f0479c3a28ead3621a7d81fb2716a2b5f31d45cbc9d36224ccbf` |
| `plan/library-implementation/tools/run_e04_fault.py` | `b854f756f41d3ae5fdcfe0b4c558d9ca1891189fb33d80dd3b639ac67b746fc0` |

There is no missing required S3 proof identified in this review. S4's shared
cleanup budget, stop backpressure and complete final liveness/ownership checks
remain explicitly future gates.
