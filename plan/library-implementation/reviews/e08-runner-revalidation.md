# Independent E08 runner repair revalidation — 2026-10-27

**Disposition: accept the reviewed interim tooling repairs. No actionable
finding remains in this review's scope. This is not E08.F1.S1 acceptance,
support-matrix acceptance or E08 completion.** Fresh matrix execution and the
subsequent story review remain required. Native Linux amd64 is explicitly
unavailable and stays open as the user requested; arm64 or emulated observations
cannot close that gate.

The reviewer began with a task-only brief and no preceding implementation
conversation. Reviewed the [execution plan](../execution-e08.md),
[plan review](e08-plan-review.md), [dependency-floor review](e08-dependency-floor-review.md)
and [handoff](../handoff-e08.md), current runner/controller/dependency helpers,
standalone base probe, testcontainers import adapter, public artifact probe,
associated tests and current local matrix caller. Only this report was written;
there were no implementation edits, Docker/service actions, environment-file
reads, remote IMDb access, commits, pushes, uploads or new chats.

## Repaired findings

- The current `var/e08-validation-20261026/run_cells.py` invokes the maintained
  `matrix_docker.run_container`, retains each cell failure in `results.json`,
  propagates interruptions and exits nonzero if any recorded cell fails. Its
  earlier all-failed zero exit must remain retained as rejected evidence.
- The maintained Docker controller recovers a complete CID from its new cidfile
  after interrupted creation. Initial/final inspection failures enter the same
  cleanup path. Every removal is preceded by inspection matching the complete
  CID, immutable image ID, owner label and, once observed, creation timestamp.
  Missing/foreign identity fails closed. Cleanup retries retain errors, and an
  original exception remains the exception raised after `controller.json` is
  written. Successful removal is followed by a bounded absence check. Mocked
  tests exercise interrupted create, both inspection gaps and foreign ownership;
  they establish controller behavior, not a real Docker execution result.
- Constraints, wheelhouse manifests and wheel bytes now pass regular-file and
  size checks before reading, with capped reads. Existing FIFO inputs cannot
  block these readers. Constraint grammar still rejects directives, paths,
  ranges, URLs and normalized duplicate names; manifested wheel bytes are copied
  from the verified snapshot for installation.

## Preservation and compatibility

The supervisor's old default remains one combined log and exit code zero.
Separate stderr and finite allowed exit codes are explicit options. The public
probe adapter uses them to retain intentional parser exits while the existing
helper still asserts each exact expected exit, stdout/stderr parity and all
public examples. The helper's ordinary callers keep `subprocess.run` by default.
All 29 existing installed-runner controls pass, including real timeout,
interruption/cleanup receipt and foreign-process ownership controls.

The testcontainers adapter falls back to the older import layout only when the
missing module is exactly `testcontainers.community`; other missing dependencies
still propagate. The affected tests change imports only and retain their
assertions. Actual minimum/current installed collection and execution remain
necessary to establish compatibility. The Rust probe explicitly selects the
driver 5/6 native-module layouts, checks matched driver/extension versions,
requires the packstream acceleration functions to be the native codec functions
and performs a real pack operation. Its target-cell execution is still pending.

## Validation and exact review inputs

Using the fresh E08 development interpreter, with `--noconftest` to avoid shared
collection/service side effects:

```text
var/e08-validation-20261026/dev/bin/python -m pytest --noconftest -q tests/test_support_matrix.py
37 passed in 0.09s
var/e08-validation-20261026/dev/bin/python -m pytest --noconftest -q tests/test_installed_runner.py
29 passed in 1.43s
```

Additional direct synthetic checks under a resolved temporary directory rejected
a FIFO wheelhouse manifest and FIFO wheel member before opening them. An initial
temporary-path alias was rejected by the symlink guard; the checks were repeated
with the resolved path so the actual regular-file checks were exercised.

SHA-256 of the reviewed inputs:

| Input | SHA-256 |
| --- | --- |
| `scripts/support_matrix.py` | `60c4644f78f43f792c45ffd66dad7a3a1d3f6168ec8d4c07c79ed5f79d71c2fb` |
| `scripts/installed_dependencies.py` | `11434237aec2be294787d7aca60cc66f5446487a775fb0c4d13386b52b52543e` |
| `scripts/matrix_docker.py` | `d565b009379c7db285d3b4cad3416f0e5077879ca21539c62697ac51bacb2953` |
| `scripts/verify_installed.py` | `a53730fc59a222051c124a90927fc6b6202cd934b0bab40418467bec0e0dd138` |
| `tests/test_support_matrix.py` | `038fe4f5abfc1fe434052da3a3a1d003223207a0eaa8c2d69440f4cf86d42f45` |
| `tests/container_support.py` | `8afecb0b15db67c992669b28f37c2e7ded571b60fc6daf6c14696f7284d0886c` |
| `tests/fixtures/e08_base_probe.py` | `f7d55e32b7a928f92e65c34fd44423b100824c5639e6142f6f5570a2ccc9df4f` |
| `plan/library-implementation/tools/verify_e03_artifacts.py` | `39a1644d1a523eb0bc1f5703b3e20c9204161bede579829a443dd85942a8b17e` |
| `var/e08-validation-20261026/run_cells.py` | `47b6480bb27c510e5a33cbbed50153d38932a9da8bb212f83adc36c64ba948e3` |

Refresh the frozen inventory and portable bundles after these changes before
rerunning target cells. This review does not promote stale bundle bytes,
historical test totals, failed observations or unavailable native cells into
successful matrix evidence. All ordered S1/S2 and final delivery gates remain
as specified in the accepted execution plan.
