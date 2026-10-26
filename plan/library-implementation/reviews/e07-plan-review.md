# Independent E07 execution-plan review — 2026-10-23

**Disposition: accepted for implementation. No actionable execution-plan finding remains.**
This accepts the ordered plan only. It does not accept the runner, inventory,
ported assertions, candidate artifacts, process provenance or E07 completion.

This reviewer started without prior conversation history. Actual HEAD is
`5c3f72b6cd16d0e14ac931f220a3f73eb7e5a02d` on
`codex/pypi-library-checklist`. The reviewed [execution plan](../execution-e07.md)
has SHA-256
`916b15f14aa5b4c6dfc4cb643ee7ecf4502a3c432f42bc2bda510e7836c7f59b`.
Reviewed the supplied user handoff, canonical [E07](../epics/e07.md),
[inventory](../test-inventory.json), backlog/tracker, D01–D07 and decision brief,
ordering/cancellation contracts, current packaging/configuration and
`verify_e03_artifacts.py`, E06 completion/revalidation records and service helper,
conftest, selected source-scanning, planner/process, shutdown and provenance
tests. Only this report was written by this reviewer. No product/test edit,
test/service execution, private environment-file access, remote IMDb access,
commit, push or upload occurred.

## Ordered execution and coverage

The order S1 runner → S3 inventory freeze → S2 core → S4 safety → S5 CLI/runtime
→ F2.S1 rebuilt-artifact parity matches the canonical dependencies. S1's small
maintained probe proves the mechanism; it cannot stand for unported lanes. S3
must finish exact parametrized collection, recursive import plus dynamic/data/
conftest/config closure, per-file classification and reviewed effort/splits
before any lane port starts. Current AST function counts and the initial helper
list are discovery inputs, not the frozen executable inventory.

The initial selection omits other runtime/public regressions, including
`test_public_cancellation.py` and the E06 image/topology/selection tests. S3's
requirement to classify **every** remaining test under tests and examples,
preserve runtime coverage and create bounded reviewed stories is therefore
material. Merely retaining these in the checkout suite would not establish
installed coverage where the same runtime behavior requires it. The existing
`run_e04_fault.py` launcher and its fixture-module chain also demonstrate why
recursive Python imports alone are insufficient: subprocess script paths and
dynamic `-m` targets require explicit closure.

Approval relies on the plan's explicit preservation of every selected test ID
and assertion, source-to-installed mapping, justified existing skips and negative
controls for disappearance/deselection. A green total cannot replace comparing
individual collected/executed IDs and outcomes with that frozen source baseline.
Porting changes, fake replacements and helper/config adaptations need review at
the relevant story gate; difficult regressions cannot disappear through a new
marker, skip, xfail, helper-only classification or reduced parameter matrix.

## Isolation and provenance

S1 requires fresh noneditable target environments and test staging genuinely
outside Git, a finite maintained allowlist and explicit pytest configuration.
The runner must neutralize checkout cwd, editable paths, environment imports,
pytest path/plugin configuration and test-owned shims while leaving the required
fixtures available. Installation and test-tool inputs must be recorded separately
from the eight mandatory runtime dependencies. `tests/conftest.py` imports
testcontainers at collection time, so that dependency is part of the test closure
even when selected tests use fake services.

The plan explicitly requires checking every installed application payload member,
metadata and module origin and enforcing/recording origins in the test process
and **actual** Python children, including spawn and fork. The existing archive
verifier is useful for expected payload/metadata and public smoke checks; its
preliminary `-I` import subprocess cannot certify later regression children.
S1 review must establish that child evidence comes from the processes doing the
work, that already-imported modules inherited through fork are covered, and that
missing or failed provenance evidence cannot yield a successful lane. A separate
preflight receipt, a `site-packages` substring or a package version alone cannot
prove those requirements. Mechanism details remain S1 implementation work.

The prescribed actual missing-module/resource wheel controls and contaminated
path controls are appropriate. They must demonstrate failure against the
expected maintained payload, including an omitted member that a small smoke
selection does not happen to import. Checking only the damaged wheel's own
remaining member list would be insufficient. Preserve sanitized runtime inputs,
command exits/logs and bounded owned process-group supervision on success and
failure; parent timeout alone must not leave real planner/CLI children running.

## Concrete source-dependent cases

The plan already resolves two risks found in the initial selection:

- `tests/test_docker_build.py::test_docker_build_node_loader` really invokes
  Docker with `path="."` and a fixed `graph-loader-node:test` tag. It cannot run
  unchanged in a source-free fake-service unit lane. The promised separately
  reviewed exact-ID service disposition must precede the port, record the
  assertion/owner mapping and retain the checkout test without claiming this
  unit lane proved a real build. No foreign tag/resource may be overwritten.
- Three metrics guards scan a checkout-relative `src` tree and would pass
  vacuously when that tree is absent. The plan explicitly requires scans of the
  installed payload with the same assertions. Verify a nonempty, complete
  installed module set at S5 review. Similarly,
  `test_every_tracked_schema_with_only_topic_keys_is_covered` currently calls
  `git ls-files`: S2 needs the frozen source schema inventory in maintained
  closure rather than a Git tree, a deleted assertion or an empty schema set.

The same principle applies to the early-stop subprocess's deliberately minimal
environment and fake dependency path, metrics subprocess cwd and spawn pickling
fixtures. Adapt their locations/launch configuration while preserving real
startup, signal, failure, cleanup and child-origin assertions. The plan allows
these bounded test adaptations without changing application behavior.

## Artifacts, preservation and completion

The final parity story covers isolated strict wheel/sdist builds, true outside-
Git sdist extraction/rebuild, every-wheel-member equality and independent runs of
all accepted installed lanes against both artifacts in clean environments. The
current finite `MANIFEST.in` and archive verifier exclude tests/tooling; any
deliberate sdist test inclusion must remain explicit and reviewed. Keeping runner,
plan and review tooling external is consistent with the existing distribution
contract. Original smoke probes can remain dependency-clean separately from
regression environments that legitimately contain pytest/testcontainers.

The plan retains the current version, sole public namespace/root-export policy,
two resources and eight mandatory dependencies. It preserves all accepted E04
transport/deadline/cleanup/R1 behavior, E05 path/resource protections and E06
immutable-image/topology/database/index revalidation controls, including rejected
repair history and independent compaction reproduction. Known O1–O8 limitations
remain limitations; E07 does not certify E09 service guarantees or E08 platform/
maximum-worker support.

Full checkout/example validation, lint/parity/pip/whitespace, fresh strict
artifacts/two installs/four immutable images with actual read-only/no-source
probes, all 12 individually supervised R1 cases and exact no-skip B3 remain final
gates on the final tested inputs. Required infrastructure skips stay open;
historical totals do not freeze E07 counts. Fresh independent acceptance of each
ordered story/split, repair/retest/re-review, final adversarial acceptance, plan
render/link checks, local delivery and byte/size/mode equality documentation are
explicit. Private `.env`, remote IMDb, foreign cleanup, global Docker pruning,
publication/push/workflows and E08–E12 execution remain outside scope.

No further change to the execution plan is required before starting S1. Each
implementation obligation above is already required by the reviewed plan or
canonical acceptance criteria and remains subject to its independent gate.
