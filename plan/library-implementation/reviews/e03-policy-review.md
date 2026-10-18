# E03.F2.S1 independent policy review

Reviewed 2026-10-17 against branch `codex/pypi-library-checklist`, HEAD
`6de3562083cc7079091d056b89d66f18b69931e8`, preserving the required E00–E02
working-tree changes. Reviewed the actual untracked migrated package, tests,
build inputs, current diff, source backup and named evidence; implementation
files were not changed by this reviewer.

**Verdict: E03.F2.S1 approved after the documentation correction below. No
unresolved actionable findings.** No runtime, class-identity, alias, or accidental
generic-package defect was found.

## Finding P3 — describe the migrated artifact accurately in the README

- Location: `README.md:38`.
- The distribution-development link still describes the E02 version-only
  scaffold and says installed loader APIs and CLI are unavailable. E03 now
  ships the migrated modules, and the installed module CLI works. Public API
  wrappers and the `graph-loader` console command remain future work, which
  should be distinguished from the available module commands.
- Impact: readers following the primary documentation receive contradictory
  migration/install capabilities at the link to the correct development guide.
- Reproduction: compare that row with `PACKAGE_README.md:3–14` and
  `docs/DISTRIBUTION_DEVELOPMENT.md:3–5`; execute either clean environment's
  `python -I -m neo4j_graph_loader.cli --help` from `/tmp` (both passed).
- Correction: describe the E03 migrated development artifact, and identify
  public wrappers/console/resources as the remaining work. No code change or
  broad regression rerun is needed for this wording repair.
- Coverage: manual cross-document consistency check; runtime probes already
  establish the capability being documented.
- Resolution independently verified: `README.md:38` now describes installation,
  build and checks of E03 migrated runtime modules, with public wrappers and the
  console executable explicitly deferred to E04–E05. Finding closed.

## Follow-up verification

The namespace tests now remove a caller's `GRAPH_LOADER_VERSION` before checking
the default metrics/package version. Read the actual updated test and reran it
with `GRAPH_LOADER_VERSION=caller-label`: **7 passed in 0.73 s**. This isolates the
default-version probe without changing runtime override behavior. The README
and test-only updates do not invalidate the archived runtime bytes checked
below. The initial candidate hash below identifies the first reviewed snapshot;
the implementer is refreshing the final manifest and full-suite evidence.

## Independently executed checks

1. Read D03 and E03 acceptance requirements. D03 explicitly rejects a legacy
   `src` shim. Restricted regular-package discovery, removal of the old
   initializer, and absence of aliases implement that policy. The migration
   guide provides old/new imports and module commands and explicitly rejects
   old pickle identities.
2. Reran `tests/test_namespace_migration.py` using the fresh development Python:
   **7 passed in 0.73 s**, including the actual spawned `PlanSettings` child.
3. In **both** `/tmp/graph-loader-e03-work/wheel-env/bin/python` and
   `/tmp/graph-loader-e03-work/rebuilt-env/bin/python`, from `/tmp`, with
   `-I` and inherited `PYTHONPATH`/`PYTHONHOME` removed, separately launched
   a fresh process for each of `src`, `src.cli`, `src.loader`, `src.models`,
   `src.utils`, `src.orchestrator`, `src.loader.edge_planner`,
   `src.models.schema`, `src.utils.metrics`, and
   `src.orchestrator.docker_service`. All twenty imports failed specifically
   because `src` was absent.
4. Separate positive processes in both clean environments imported **all 49
   runtime modules**, verified every origin under that environment's
   site-packages, checked no `src` modules or editable `.pth` supplied code,
   and verified runtime/metadata/metrics version equality at `0.1.0a1.dev0`.
   Both `pip check` executions passed.
5. Independently sent `PlanSettings` and `GraphSchema` instances through a
   real `multiprocessing.get_context('spawn').Pool(1)` in each clean
   environment. The child returned each instance's class and the parent
   asserted canonical class identity. Pickle bytes contained no `src.` name.
   Both classes passed in both environments.
6. Checked actual archive hashes against the evidence. Each original/rebuilt
   wheel has exactly the current 49 Python source modules byte for byte;
   neither ships a top-level `src` package. Every corresponding wheel member
   byte matches across the explicit sdist rebuild. The actual sdist has 60
   files and no legacy application package. Original wheel SHA-256:
   `37430125c84d81f0e8c7b79387378ce5e4c2ec5d87d4ad3ea1f27656ae174a0f`;
   rebuilt wheel:
   `d9c0c170d53cb24c972d637a0801e5939deb3d08a195470a586c5103fec230f3`.
7. Verified the candidate's 284 manifest entries against actual bytes and
   intentional missing paths at review time, source identity
   `e806b37332d56b5080cb691e46f2eb677308bd4d63d268af6433372aeccc3ed2`.
   Compared migrated modules with the safe entry-source archive, then
   inspected namespace/version/path-reference differences. No alias mechanism
   or duplicated runtime tree appeared.
8. Searched first-party runtime/tests/examples/scripts/docs/build targets for
   old imports, module commands and dynamic targets. Remaining old names are
   negative import tests and migration prose; the observed checklist row was
   being refreshed during review. Example repository-path insertions resolve
   example helpers and do not restore the removed application layout.

The recorded full suite, image matrix and B3 execution were inspected as
supporting evidence; they are not represented here as independently rerun.
This is the F2.S1 policy review, not final E03, installed service/platform, or
release acceptance. A checkout directory called `src` may form a Python
namespace directory from that checkout; it supplies no old application modules
and is not a shipped compatibility shim.
