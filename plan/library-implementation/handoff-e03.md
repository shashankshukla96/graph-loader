# E03 next-agent handoff — atomic namespace migration

Updated 2026-10-17. Give this entire document to the next implementation agent.
**Implement and verify E03 only**, following the workflow below. E00–E02 are
complete and freshly revalidated in the [completion review](reviews/e00-e02-validation-20261017.md).
Do not start E04–E12. No GitHub push, package/image upload or release is authorized.

## Preserve the actual checkout

Work in `/Users/shashank/Desktop/Code/Graph/graph-loader`, branch
`codex/pypi-library-checklist`, HEAD
`d53a92cfef652b7aae5aca6472736c10e851db42` at the tested E02 baseline, plus
required dirty/untracked work and any subsequent local documentation commits.
Read current HEAD rather than assuming it still equals that baseline.
Recheck status/diffs and AGENTS.md instructions; none were found at E02 entry.
Never replace this with HEAD alone or clean/stash away the uncommitted fixes.
Preserve all original E00/E01 source/test/fixture/plan work and the E02 additions.
Prior E02 implementation was uncommitted; local handoff/documentation commits
may follow it. Preserve the distinction when inspecting the current diff.

## E02 result and limitations

The distribution is an expressly incomplete **development-only scaffold**:

- Explicit setuptools selection ships only `neo4j_graph_loader/__init__.py`
  and `_version.py`, plus dist-info/license. No loader, API, CLI or resources.
- `PACKAGE_README.md` states those limits; a strict sdist allowlist excludes
  legacy `src`, tests, plans, datasets and private/runtime files.
- `_version.py` is the single literal file authority: **0.1.0a1.dev0**. Backend
  metadata, installed scaffold and default checkout metrics agree. D03's E11
  release target remains **0.1.0a1**, not an authorized E02 upload.
- `[project].dependencies` is authoritative. All eight original runtime bounds
  remain unchanged and mandatory (including matched Rust/Neo4j <7); no extras.
  `scripts/sync_requirements.py` generates/checks Docker's requirements file;
  `make build-images` first checks parity. Dev/build/IMDb requirements stay out
  of base. Dockerfiles still use checkout source; installed delivery is later.
- `GRAPH_LOADER_VERSION` explicitly overrides only the startup diagnostic label;
  an empty direct value remains empty. Existing Docker forwarding is nonempty
  only; existing image overrides/defaults are preserved. It is not image proof.

Evidence: [S1](evidence/E02.F1.S1.md), [S2](evidence/E02.F1.S2.md),
[S3](evidence/E02.F2.S1.md), [per-story reviews](reviews/e02-checkpoints.md),
[final adversarial report](reviews/e02-final-adversarial.md), and JSON manifests/results.
Final tested source SHA-256:
`4e293357a858209d250d91818b732030a5951784f2dc468495d2c6ad21858f2b`;
environment SHA-256:
`048a4d2d18ada968b28398105c6086473679ae25eb88c7a6e5d175dad965bf89`.
All 232 allowlisted inputs/environment and binary diffs matched before/after
the final new full suite. Later documentation-only mirrors may update diff hashes.
Re-capture the actual current source/environment before reusing any evidence.

Final E02 full baseline: **3,380 passed, 4 inherited skips, 49 deselected,
19 inherited fork warnings**, 172.12 seconds. All skips and deferred warning
ownership are individually explained; focused version/metrics/CLI/Docker/lifecycle
lane **147 passed**. Parity negative controls pass. Fresh isolated sdist/wheel,
strict Twine, exact archive inventory, no-service/logging/thread-side-effect
scaffold imports and extracted-sdist rebuild without `.git` pass. Every member
byte of original/rebuilt wheels agrees. Both exact wheels install/import away
from checkout and pass `pip check` in the clean S2 base environment.

Clean current base resolution uses Neo4j **6.4.1** and Rust **6.4.1.0**, with
the extension's exact `neo4j==6.4.1` constraint verified; no dev/IMDb/build tools
leak into base. The repository venv remains at its original dependency versions.
These are native macOS/CPython 3.12 checks, not E08 platform/minimum proofs or
installed-loader service validation. Earlier B3 proof is historical E00 evidence,
not a new release-image test for E02 artifacts.

Fresh validation on 2026-10-17 independently reproduced **3,380 passes** in
173.41 seconds and the exact B3 integration case (**1 passed, no skip**, 23.40
seconds), rebuilt wheel/sdist artifacts and verified clean imports/base resolution.
The 232-input source/environment identity above remained unchanged. Earlier
`/tmp` logs/artifacts had disappeared; the new safe [review evidence](evidence/e00-e02-review-20261017.json)
records fresh executions, not a claim to have reverified missing historical bytes.

## E03 seams that must move together

1. Read canonical E03 stories; migrate legacy modules/imports, dynamic patch
   strings, module/subprocess commands, executable examples and all Dockerfiles
   as one runnable change. No generic `src` compatibility shim was approved.
2. The namespace directory already contains `__init__.py` and `_version.py`.
   Merge the legacy loader into it without creating a nested duplicate package
   or overwriting the single version authority.
3. Current package discovery is deliberately **only** `packages =
   ["neo4j_graph_loader"]`; expand reviewed discovery to all migrated subpackages.
   Adjust the sdist allowlist/inventory for actual migrated source, keeping
   private/planning/runtime data excluded. Required resources are E05 work.
4. `src/utils/metrics.py` currently imports
   `from ..neo4j_graph_loader._version import __version__` to support uninstalled
   legacy checkout commands. Once moved to `neo4j_graph_loader/utils/metrics.py`,
   change that bridge to `from .._version import __version__`.
5. Each Dockerfile must immediately install/resolve the migrated package while
   preserving reviewed dependency inputs/version. A successful build alone is
   not an import proof. E06 later implements selected checkout-free GHCR delivery.
6. `tools/verify_e02_scaffold.py` intentionally rejects more than two shipped
   modules; preserve it as E02 proof but replace/extend artifact validation for
   E03's larger namespace instead of weakening an installed regression gate.
   E07's isolated installed-code runner is still future work.
7. Expand the safe source manifest's explicit untracked inventory for newly
   added/moved modules/tests/build inputs. E00 source hashes no longer identify
   E02, and E02 hashes will not identify E03. Rebuild affected artifacts and
   rerun affected/full checks against the new snapshot.

E01 decisions remain in force: selected API/ordering/cancellation contracts,
Linux/architecture support targets conditional on E08/E09, no extras, file-based
versioning/manual Twine/no workflows, GHCR namespace under Shashank ownership.
Preserve E04 S5 private transport -> S3 public integration -> S4 cleanup test
allocation and separate E11/E12 clean-source/exact-byte/publication gates.
The user's Agent-review-after-every-step and final adversarial-review requirement
continues to apply; retain findings/corrections/retest evidence.

## Required SDLC workflow

1. **Inspect and plan before coding.** Read applicable repository instructions,
   the [canonical E03 epic](epics/e03.md), `backlog.json`, `decisions.md`, the
   preceding review and the existing source/tests. Capture current branch/HEAD,
   staged/unstaged diffs, relevant untracked inputs and environment. Create
   `execution-e03.md` with E03.F1.S1, E03.F2.S1 and E03.F2.S2 acceptance checks,
   change surfaces, test/build commands, ownership/cleanup and rollback notes.
   Inspect executable imports, patch strings, subprocess/pickle identities,
   examples, Docker launch paths, resources and package discovery. Do not
   treat a global string replacement as proof of a complete migration.

2. **Keep scope bounded and reviewable.** Preserve algorithms, CLI behavior/exit
   codes, schema validation, plan/chunk/DLQ formats, Kafka keys/groups, lease and
   ordering behavior. Names remain `neo4j-graph-loader`, `neo4j_graph_loader`,
   `graph-loader`; D03 permits **no generic `src` compatibility shim**. Do not
   implement E04 public wrappers/cancellation, E05 resource/CLI redesign or E06
   image delivery early. If discovery requires a larger design, update the
   canonical plan and obtain independent re-review before expanding scope.
   Do not silently relax an acceptance criterion or hide a failing test.

3. **Prepare meaningful tests and make the cohesive change.** Inventory existing
   regressions and add focused migration tests for actual failure risks: missing
   runtime subpackages, stale dynamic/module targets, wrong version import,
   accidental generic `src` shipping and spawn/pickle imports. Reproduce a defect
   before its repair where applicable; avoid tests that only repeat literal
   implementation text. Move all E03.F1.S1 executable surfaces together so the
   loader remains runnable, then complete the approved compatibility-policy and
   artifact-integrity stories. Update development/migration documentation and
   `PACKAGE_README.md` to match what this intermediate artifact actually contains.
   E05's installed console executable is not an E03 completion claim.

4. **Bootstrap and test a fresh development environment.** Use CPython 3.12,
   record resolved dependencies and document the new editable-install workflow.
   Remove root-only pytest/path shims that mask the source-layout change while
   retaining legitimate test-helper imports. A concrete starting sequence is:

   ```sh
   E03_DEV_DIR="$(mktemp -d "${TMPDIR:-/tmp}/graph-loader-e03-dev.XXXXXX")"
   .venv/bin/python -m venv "$E03_DEV_DIR"
   E03_DEV_PY="$E03_DEV_DIR/bin/python"
   "$E03_DEV_PY" -m pip install -r requirements-dev.txt
   "$E03_DEV_PY" -m pip install -e .
   "$E03_DEV_PY" -m pip check
   "$E03_DEV_PY" scripts/sync_requirements.py --check
   "$E03_DEV_PY" -m ruff check src tests examples scripts/sync_requirements.py
   "$E03_DEV_PY" -m pytest tests examples/imdb/tests -m "not integration and not smoke" -q -rs
   git diff --check
   ```

   Also run the focused changed-code tests and multiprocessing/spawn planner
   regressions. Record counts, exact test IDs, skips, warnings and import origins.
   Every new failure must be diagnosed; the 3,380 prior passes are a comparison
   baseline, not a substitute for testing renamed code. Explain each inherited
   skip; keep the known fork warnings assigned to A5/E08. Do not reduce test
   collection or alter assertions merely to recreate an old pass count.

5. **Search for residual executable references.** Use `rg` over all first-party
   source, tests, examples, scripts, Makefile, Dockerfiles and executable docs.
   Check `from src`, `import src`, `-m src.`, string monkeypatch/sys.modules
   targets, source-path insertions and the metrics import bridge. Include new
   untracked migration files in searches and manifests. Record a classified
   residual inventory: executable references must migrate; intentional historical
   prose and explicit old-import rejection tests may retain the old name.
   Do not mechanically rewrite archived evidence to pretend it ran new commands.

6. **Build and verify real artifacts independently of the editable install.**
   Build wheel/sdist with isolated backend tooling, run strict Twine checks,
   inspect their inventories and compare runtime-module coverage with the actual
   migrated source tree. Preserve metadata/license/version/dependency authority;
   exclude planning records, tests, private/runtime files and the old generic
   `src` package. Explicitly rebuild a wheel from extracted sdist without `.git`.
   Install exact built wheels into clean noneditable environments away from the
   checkout. Remove inherited PYTHONPATH/PYTHONHOME, use isolated interpreter
   probes and assert every application import resolves inside site-packages.
   No `sys.path` insertion, working directory or editable `.pth` may supply code.
   Verify new namespace imports and the approved absence of old imports in fresh
   processes. Test metadata/runtime version equality, `pip check`, schema/planner/
   CLI module imports and supported component help. E02's two-file scaffold
   verifier is not the correct validator for E03's complete migrated code.

7. **Build all images and execute their modules.** Build `Dockerfile`,
   `Dockerfile.node_loader`, `Dockerfile.edge_loader` and `Dockerfile.clock` with
   unique local task tags and record immutable image IDs/build inputs. Preserve
   the package/dependency installation policy chosen in E03. For each image,
   run namespace import/origin probes and its applicable component help commands
   without host source bind mounts or checkout PYTHONPATH rescue. Cover the
   CLI, node loader, edge loader, retry consumer, planned writer, planned
   dispatcher, lease server and legacy clock in the images that launch them;
   keep an explicit module-to-image matrix with exact commands/results.
   A successful build alone is not a pass. Help probes should exit without
   contacting services. Keep legacy clock behavior as scoped by current policy.
   Use clean wheel environments as a separate proof from image commands.

8. **Rerun the affected live integration path safely.** Namespace migration
   changes the retry child module path and fixture/test imports. Rerun the exact
   maintained B3 test with isolated testcontainers and an independently bounded
   supervisor, migrating the supervisor's interpreter/module targets as needed:

   ```sh
   "$E03_DEV_PY" -m pytest tests/integration/test_multi_topic_relationship.py::test_dead_letter_arrival_stream_type_wide_dlq_drained_by_the_retry_consumer -m integration -v
   ```

   The case must execute and pass; an infrastructure skip remains an open gate.
   Keep real final-property/offset/DLQ/lock-wait assertions. Capture diagnostics
   before removing only owned resources. Never use the remote IMDb database as
   a test target. This is an E03 migration regression, not E09 release-image
   service-matrix acceptance or a reason to rerun the large IMDb benchmark.

9. **Use an independent subagent after every implementation step.** This user
   explicitly requests internal independent review. Spawn a reviewer with
   `fork_turns: "none"` (zero inherited conversation) when available. Supply the
   repository, E03 story/acceptance criteria, exact working-tree diff/untracked
   files, source/artifact identities, test evidence and constraints. Its task is
   to inspect code, tests, package contents and claimed results itself; request
   concrete findings with file/line, impact, reproduction and missing coverage.
   It must not approve by trusting your summary or prior approval labels. The
   reviewer should remain separate from the implementer and should not edit
   implementation files. Do not create user-visible chats for internal review.
   If internal agents are unavailable, report that gate explicitly; do not label
   self-review independent approval.

10. **Repeat change → test/build → independent review until resolved.** Save each
    reviewer report and reproduce every actionable finding with a focused check.
    Fix its cause, rerun affected tests, rebuild invalidated wheels/images and
    refresh evidence. Send the exact revised state and results back for
    independent re-review. Repeat without a fixed round limit until **all
    actionable issues are fixed and verified**, not merely the highest-priority
    findings. False positives must have reproducible evidence and reviewer
    agreement; do not silently waive issues, weaken gates or mark them fixed
    from prose alone. Scope changes require replanning/re-review. Missing
    infrastructure stays an explicit blocker instead of becoming a pass.

    Because E03.F1.S1 is an atomic integration change, do not ask the reviewer
    to accept an intentionally broken half-move as a completed story. Draft
    feedback is useful, but each story's actual acceptance requires its tests.
    Once all three stories pass, launch a **fresh zero-history final reviewer**
    to inspect the whole E03 delta and final evidence. Apply the same fix,
    retest/rebuild and re-review loop to its findings before closing E03.

11. **Freeze evidence and complete the acceptance gate.** After the last repair,
    run the applicable final full suite and affected artifact/image/live checks
    against the final source/environment. Record safe tracked/untracked file
    manifests, exact commands, exit codes, artifact/image hashes, import origins,
    skips/warnings and limitations. Do not reuse stale artifacts after code,
    version, metadata or dependency changes. Store per-story evidence at
    `evidence/E03.F1.S1.md`, `E03.F2.S1.md` and `E03.F2.S2.md`, plus preserved
    independent review reports. Keep secrets and large logs out of tracked files.
    Mark statuses complete only after all applicable gates pass. Regenerate
    epics/traceability through `backlog.json`, validate local links and run:

    ```sh
    python3 plan/library-implementation/tools/render_plan.py
    git diff --check
    ```

12. **Deliver a reviewable local result and next handoff.** Report what changed,
    validation results, all review resolutions, remaining limits and precise
    source/artifact identities. Prepare the E04 handoff without implementing it,
    preserving the approved private-transport → public-integration → cleanup
    sequence. Follow the user's local-commit instruction after tests/review pass;
    stage only the completed task's intended files/evidence and inspect the
    staged diff before committing. Preserve unrelated work. A commit changes a
    HEAD-based manifest: retain tested HEAD+diff/per-file identities and compare
    final contents rather than claiming an old combined hash is unchanged.
    **Never push to GitHub unless the user specifically asks.** No publication,
    image push, release tag, merge or workflow addition is authorized here.

Persistent operating constraints: preserve the existing `.env` without printing
it, do not source remote settings into isolated tests, do not clear/reload IMDb
at `192.168.1.75`, do not globally prune Docker or remove another run's resources,
and do not remove a live owner's planner lock. Ask only for information required
to resolve a real blocker and continue independent authorized work meanwhile.
