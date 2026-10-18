# E03 independent execution-plan checkpoint — 2026-10-17

Reviewed actual branch `codex/pypi-library-checklist`, HEAD
`6de3562083cc7079091d056b89d66f18b69931e8`, including the required dirty/untracked
E00–E02 implementation. No implementation files were changed. This report is a
plan checkpoint, not approval of an implementation or unexecuted test results.

Read `execution-e03.md`, `handoff-e03.md`, canonical `epics/e03.md`, the E03
backlog entry, decisions D01–D07, entry evidence, actual package/discovery/build
inputs, runtime entry paths, source-sensitive tests, and the proof tooling.
No applicable ancestor `AGENTS.md` was found. Conclusions below come from those
files rather than their existing approval labels.

## Findings to resolve in the execution plan

1. **[P2] Make B3's fresh child interpreter an explicit implementation/check.**
   `execution-e03.md:84` names `tools/run_dlq_proof.py`, which does not exist
   relative to the repository root. The actual file is
   `plan/library-implementation/tools/run_dlq_proof.py`; its lines 17–20 build
   `COMMAND` with the repository `.venv/bin/python`. Merely running that script
   with the fresh development interpreter leaves its pytest child on the old
   environment. This can make the mandatory B3 result use a different
   dependency resolution/install state than the full migration suite.
   Reproduction: executing `runpy.run_path` on the actual supervisor under
   `/tmp/graph-loader-e03-work/dev/bin/python` reports `sys.executable` in the
   fresh environment but `COMMAND[0]` in the repository `.venv`.
   **Required change:** name the correct repository-relative tool path and
   explicitly include its interpreter selection in the E03 change surface
   (for example, `sys.executable` or a validated explicit interpreter option).
   Before accepting B3, verify its recorded `command[0]` equals the fresh
   development interpreter. Keep its exact test ID, independent process-group
   timeout, ownership/cleanup policy, and no-skip gate.

2. **[P2] Include the installed artifact's intermediate capability description.**
   `execution-e03.md:21–47` describes executable docs, compatibility docs and
   packaging checks but omits the explicit `PACKAGE_README.md` update required
   by `handoff-e03.md:140–141`. Actual `PACKAGE_README.md:3–5` says the artifact
   contains only the namespace/version and contains no loader implementations;
   `pyproject.toml:18` repeats the scaffold-only description. Those statements
   become false when F2.S2 ships the complete migrated runtime. Twine and
   metadata/source equality checks can still pass with that stale description.
   **Required change:** list `PACKAGE_README.md` and the project description in
   the cohesive E03 change surface, and review the built metadata for truthful
   intermediate capabilities. Explain migrated internal modules/module commands
   and remaining E04/E05 limitations without claiming approved public wrappers,
   packaged resources, an installed console command, supported release platforms,
   or publication readiness.

## Required gates otherwise present

- Fresh owned CPython 3.12 environment, declared development dependencies,
  editable install, dependency parity/health, focused regressions, and the full
  `tests examples/imdb/tests` non-integration/non-smoke suite with baseline
  comparison, skip/warning accounting, and no reduced assertions/collection.
- Cohesive runtime/import/patch/subprocess/spawn/pickle migration; preservation
  of the metrics version authority; removal of application-only path rescue;
  classified residual references including untracked inputs and historical
  exceptions.
- All four Dockerfiles built with installed package/dependency inputs and
  isolated in-image origins plus all eight component help commands; explicit
  image/module matrix and immutable image identities.
- Exact bounded B3, task-owned throwaway resources, no remote IMDb target, and
  infrastructure skips keeping the gate open (subject to finding 1).
- Restricted namespace discovery, strict sdist allowlist, complete runtime
  source-byte/inventory checks, isolated wheel/sdist builds, strict Twine,
  explicit no-`.git` sdist rebuild, and every-wheel-member comparison.
- Two clean noneditable environments away from checkout, stripped interpreter
  path variables, isolated probes with site-packages origins, dependency/version
  checks, schema/planner/CLI imports, component helps, and fresh-process rejection
  of old import targets. No generic `src` compatibility shim.
- Separate independent plan/story checkpoints and final zero-history adversarial
  review, repair/retest/rebuild loops, final source identities, and E04 handoff
  without E04 implementation. No push, upload, workflow or release action.

## Re-review disposition

Independently reread the revised plan after the implementer accepted both
findings. `execution-e03.md:27–28` now explicitly includes `PACKAGE_README.md`
and the project description, bounded by the intermediate E03 capabilities.
Its lines 86–89 now name the actual supervisor path, require a `--python`
selection for the fresh development interpreter, and require asserting the
recorded child command uses that interpreter. Both **plan findings are
resolved**; their implementation and executed evidence remain later gates.
`git diff --check` also passed at this checkpoint.

The revised plan is sufficiently bounded and concrete for the cohesive E03
implementation. There are no remaining actionable plan findings from this
review. Runtime/build/service completion still requires the stated later
executions and independent implementation reviews.
