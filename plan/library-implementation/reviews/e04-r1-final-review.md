# E04-R1 final adversarial review — 2026-10-21

**Verdict: accepted. No actionable finding remains in the R1 repair or its complete proof.**

This independently initialized review closes the final E04-R1 acceptance gate.
The previously interrupted final review is not treated as acceptance. Restoring
canonical S3/S4 completion and the E05 handoff gate, rendering/link validation,
and the planned local repair/documentation commits remain subsequent delivery
work; this report does not claim they already happened or authorize publication.

Reviewed entry HEAD `6508bd31779c49c0d1dd78f0ee242c91b9f9a9fc` plus the actual
pending repair, tests, fixture and public-guide changes. Read the
[execution plan](../execution-e04-r1.md),
[superseding finding](e04-completion-verification-20261020.md),
[lifecycle reproduction](e04-lifecycle-revalidation-20261020.md),
[canonical E04](../epics/e04.md), and
[cancellation contract](../cancellation-contract.md). Prior plan/source-review
labels were not substituted for inspection. No applicable AGENTS.md was found.
Only this report and owned ignored review outputs were written; source/tests
were not edited. No private environment file, remote IMDb host, foreign
resource cleanup, global Docker pruning, commit or push was used.

## Repair and regression assessment

At `src/neo4j_graph_loader/loader/edge_planner.py:1580–1595`, a returned non-ok
reply now raises immediately, before the post-receive check and before any
later lane receive. The inline original exception remains the cause, including
a false-valued exception; process diagnostics remain a local RuntimeError cause
containing remote type/message/traceback. The exception reaches the existing
public marker and shared terminating cleanup path. Successful replies retain
their checks, ordered accumulation and phase/broadcast behavior. No scheduler,
protocol, cleanup-budget or receiver-helper change is hidden in the repair.

The guide at `docs/LIBRARY_API.md:99–107` correctly limits the promise to an
observed reply. Sequential receives may leave a later-lane failure unobserved
behind an earlier blocked lane. Existing initial-submission, synchronous inline
source/callback and OS-uninterruptible exclusions remain explicit. The repair
does not claim multiplexed failure observation or broader E08 support.

The fixture at `tests/fixtures/e04_faults.py:89–96` requires the unrelated real
spawned peer to enter its blocking source before lane 0 reports its source
failure. The exclusively created output directory prevents a stale readiness
file from satisfying this handshake. Receive/close wrappers observe production
behavior and then delegate; they do not manufacture replies or unblock workers.
Production liveness, endpoint and resource values are captured before fixture
emergency recovery. Supervisors own only their newly created process group or
explicit container ID, with independent hard limits.

The real-spawn controls at `tests/test_public_cleanup.py:166–189` separately
exercise no timeout and `.8` seconds, asserting source cause, prompt cleanup
entry, aggregate cleanup allowance, dead children, closed endpoints, absent
heartbeat, reclaimed lock/spill, source-bearing failure marker and preserved
caller sentinel/signal. The deterministic public controls at `:192–216` make
timeout/cancellation true immediately after receiving the error and assert
original cause identity. Existing partial-frame, command/stop-backpressure,
startup, unreapable-worker and ownership controls remain intact.

The retained old-code default reproduction was externally stopped at 2.006 s
after the source error had arrived, with the blocked peer entered, lock held and
no marker. Its finite-timeout counterpart instead raised EdgePlanTimeoutError
at 0.827 s with no cause. The two retained deterministic tests fail against the
entry code. These are relevant negative controls, rather than successful cleanup
produced by timing out the same blocked operation.

## Source continuity and complete-proof audit

Independently recalculated the allowlisted manifest with the owned CPython
3.12.14 interpreter. All **259** current inputs, their bytes/sizes/modes and
the complete recorded environment match the before/after freeze:

- Source: `3f00faeef6a887516b3501590226537e71ea1bd3a9ea1f1f76315fdf2c110a7f`.
- Environment: `01e164fd8501e329de5a67ac77646f90a6ca3eb4c5825057afcf0c98162e3356`.

All **25** raw files enumerated in
[the results](../evidence/e04-r1-results.json) match their recorded sizes and
hashes. Inspected the full-suite and exact B3 raw outputs, not only summaries:
**3,516 passed, four inherited skips, 49 deselections, 19 owned fork warnings,
192.57 s, exit 0**; exact B3 **one passed, zero skipped, 24.88 s**, supervisor
exit 0 without timeout and empty before/after container inventories. The four
skip reasons and A5/E08 warning ownership are preserved. The full suite and B3
were audited here rather than rerun; unchanged source/environment identity
ties those completed runs to the reviewed repair.

Recomputed both archive inspections against current source: exact allowed
wheel/sdist inventories, all **52** runtime module bytes, metadata/dependencies,
license and package README match. Independently compared every member of the
original and extracted-sdist rebuilt wheels: names and bytes are equal. The
isolated build/rebuild logs, no-Git extraction, strict Twine pass, and clean
installation commands are retained. All nine artifact-command logs verify.

All **57** image-command records and their log hashes verify: four successful
component builds, **52** successful checks using immutable image IDs, and one
explicitly retained initial edge-build failure caused by a Docker Hub manifest
HEAD EOF. The unchanged-source retry successfully built edge/clock. Checks use
network-none containers without source mounts and verify origins/module bytes,
inert imports, public examples, component/default helps and pip check. The
retained **12** checkout-free mixed cases match their raw logs and image/build
identities. Reviewed their public client and supervisor: cleanup assertions run
before owned emergency recovery, native clients have a 15 s process-group bound,
and containers have an inner 15 s and outer 25 s bound with explicit owned IDs.

## Fresh independent executions

Owned outputs/scripts: `var/e04-r1-final-review-20261021/`.
Interpreter: `var/e04-r1-20261020/dev/bin/python`. The exact focused command is
retained in `runs.json` and reproduced here:

```text
var/e04-r1-20261020/dev/bin/python -m pytest tests/test_public_cleanup.py tests/test_public_cancellation.py tests/test_planner_transport.py tests/test_public_lifecycle.py tests/test_public_api.py tests/test_external_planner.py tests/test_library_preflight.py tests/test_planner_abort.py tests/test_edge_planner.py tests/test_planner_imports.py -q
```

Result: **200 passed in 18.06 s**, no skip/warning/failure, outer independent
60 s process-group deadline, exit 0 without timeout. The additional
`adversarial.py` ran under a separate 20 s deadline and passed:

- Failure precedence over post-reply timeout and cancellation without receiving
  the later peer; successful replies still apply those post-reply checks.
- Successful result lane order.
- Public inline cleanup and original false-valued exception identity in both
  post-reply interruption races.
- The extracted entry-commit gather fails the same adversarial control, while
  the repaired gather passes; no source-file replacement was used.

The historical temporary installation directory had been removed, so two new
owned environments were created outside the checkout and installed from the
verified original/rebuilt wheels. Both installs pass pip check. Their four
real-spawn mixed controls pass with isolated Python execution and site-packages
origins. Docker was initially unavailable; that pre-container failure is retained
in `initial-daemon-unavailable-cli-default.log`. After the parent started local
Docker, only the remaining eight image cases were resumed against the exact
recorded immutable IDs, without rebuilds, mounts or networking. All eight pass.

Across all **12 fresh installed/image cases**, total public operation time was
**0.118–0.171 s** and observation through cleanup/raise was at most **0.008823 s**.
Every case preserves the source diagnostic/cause and source-bearing marker,
reaps both production children, closes endpoints, leaves no active child or
heartbeat, and reclaims owned lock/spill before emergency recovery. None reaches
its external deadline. `installed-runs.json`, clean-install logs, the reviewed
client and supervisor scripts retain these fresh results. A final manifest
recalculation still matches the frozen source/environment, and `git diff --check`
passes.

These checks close E04-R1 and the reviewed E04 S3/S4 failure-propagation/cleanup
proof. They do not replace E07's installed full-suite lane, E08/E09's broader
support matrices, or the remaining documentation/local-commit delivery work.
