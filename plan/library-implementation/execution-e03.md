# E03 execution plan — 2026-10-17

Scope: E03.F1.S1, E03.F2.S1 and E03.F2.S2 only. D01/D03/D05/D07 apply:
`neo4j-graph-loader`, `neo4j_graph_loader`, future `graph-loader`; no generic
`src` shim, no extras, literal version `0.1.0a1.dev0`. No publication or push.

## Entry and preservation

Actual branch `codex/pypi-library-checklist`, HEAD `6de3562` (read at entry).
Required E00–E02 dirty/untracked implementation and records are retained.
[Entry manifest](evidence/e03-entry.json) captures 232 safe inputs, full HEAD,
diff identities and repository CPython 3.12.14 dependency resolution. No
AGENTS.md exists in this checkout or ancestors. Safe source backup and binary
diffs are in `/tmp/graph-loader-e03-work/entry-source.tar.gz` and
`/tmp/e03-entry*.patch`. `.env` is preserved and never printed or sourced.

## Steps and acceptance

1. Plan checkpoint: independently review this plan against canonical
   [E03](epics/e03.md), backlog, decisions and actual checkout before coding.
2. Atomic implementation (F1.S1): merge legacy cli/models/loader/orchestrator/
   utils into the existing `src/neo4j_graph_loader`, preserving `_version.py`.
   Migrate static/dynamic imports, monkeypatch/sys.modules strings, `-m` child
   commands, pickle identities, executable examples/docs/scripts, all four
   Dockerfiles and source-sensitive regression checks. Change the metrics
   bridge to `.._version`. Keep algorithms/protocols/config defaults unchanged.
   Update PACKAGE_README.md and the pyproject description to state the actual
   intermediate migrated-module capabilities, with no E04/E05/release claims.
   Remove pytest's root path setting and application import path insertions;
   retain path access used for example/test helper modules. Use editable install
   for development, never for artifact proofs. Test all changed behavior,
   multiprocessing/spawn, full regression suite, all image module commands,
   exact supervised B3 test, then obtain independent story review.
3. Compatibility (F2.S1): document old/new commands and explicit absence of
   aliases. Fresh isolated processes must import new modules and reject old
   `src.cli`, `src.loader`, `src.models`, `src.utils`, `src.orchestrator`; an
   empty filesystem source directory is not a compatibility package. Verify
   spawn/pickle identity. Independent review after tests.
4. Artifact integrity (F2.S2): replace explicit two-file package discovery with
   restricted `neo4j_graph_loader*` discovery, regular packages only. Keep the
   sdist allowlist, shipping only Python runtime source plus reviewed build/
   metadata/license inputs. Preserve E02 scaffold verifier unchanged as history;
   add a separate E03 validator comparing every shipped runtime module with
   source bytes, inventory and metadata. Build isolated sdist/wheel, strict Twine,
   explicitly rebuild extracted sdist without `.git`, compare every wheel member.
   Two clean noneditable environments, unrelated cwd, stripped PYTHONPATH/PYTHONHOME,
   `python -I` probes must establish site-packages origins, version/dependency
   equality, schema/planner/CLI imports, eight supported component helps and old
   import rejection. Independent review after artifact tests.
5. Freeze: record final source/environment manifests, commands/exits, safe logs,
   skips/warnings, wheel/image SHA identities, module/image matrix, reviews and
   retests. Fresh zero-history final adversarial reviewer; repair/retest/rebuild
   until no actionable findings. Run applicable final full suite and checks on
   final code. Complete backlog only if every gate passes. Render canonical plan,
   validate links/whitespace, prepare E04 handoff (no E04 implementation).
   Inspect staging before a local commit; preserve E00–E02 content and explain
   their preexisting status. No pushes/tags/uploads/workflows.

Discovery/manifest changes and tests are part of the cohesive implementation;
story reviews cannot accept a deliberately broken half-move. Each checkpoint
uses a zero-history internal reviewer who reads the actual files and executes
checks, writes findings with file/line/reproduction, and does not edit product.

## Validation commands and environments

Fresh owned CPython 3.12 venv: install `requirements-dev.txt`, then `pip install
-e .`, `pip check`, `scripts/sync_requirements.py --check`; record resolution.
Run `python -m ruff check src tests examples scripts/sync_requirements.py`, focused
namespace + planner/spawn regressions, then `python -m pytest tests
examples/imdb/tests -m "not integration and not smoke" -q -rs`.
Baseline comparison: 3,380 passed / 4 inherited skips / 49 deselected / 19 fork
warnings (A5/E08). Diagnose every new failure, preserve assertions/collection.

Use `rg` across runtime/tests/examples/scripts/build/docs (including untracked
inputs) for old import/module targets, runtime source paths and path insertions.
Archive historical plan/evidence prose unchanged; classify intentional negative
controls and historical Git slice references separately from executable imports.

Build `Dockerfile{,.node_loader,.edge_loader,.clock}` under unique local tags.
Install generated dependency requirements first, then package with `--no-deps`
using isolated setuptools. No PYTHONPATH. Run origin probes from `/tmp` using
`python -I` and component helps for CLI, node, edge, retry, planned writer,
dispatcher, lease and clock in all images (same package contents). Keep config
outside wheel; E05 resource/console redesign and E06 GHCR delivery stay future.

Run exact B3 ID via `plan/library-implementation/tools/run_dlq_proof.py` with
explicit `--python /tmp/graph-loader-e03-work/dev/bin/python` selection; change
the supervisor to honor that argument and assert the recorded child command
uses that exact development interpreter. Keep bounded
owned process group, throwaway testcontainers and safe before/after inventory.
No remote IMDb access, global prune, foreign container removal or live lock
removal. Infrastructure skips leave the gate open.

## Evidence, ownership and rollback

Per-story evidence: `evidence/E03.F1.S1.md`, `E03.F2.S1.md`, `E03.F2.S2.md`;
review reports under `reviews/e03-*`; safe manifests/results under `evidence/`.
New validators/manifests explicitly cover untracked migrated modules and tests.
Large/raw build/test logs and venvs stay in an owned temporary work directory;
retain safe summaries and artifact hashes in repository records.
Rollback uses the safe entry backup and reviewed reverse E03 delta, never
`reset --hard`, clean, stash or HEAD-only restore. Only task-owned temporary
resources may be removed. Final commit changes HEAD/diff hashes: compare
per-file contents and retain precommit tested identity.

## Completion

All three E03 stories passed with independent acceptance, followed by a fresh
zero-history [final adversarial review](reviews/e03-final-adversarial.md) with
no unresolved findings. [Frozen final evidence](evidence/e03-completion.md)
records 3,387 full passes, 55 focused passes, all artifact/image/import proofs
and exact no-skip B3 with unchanged source/environment. E04–E12 are unstarted.
The [E04 handoff](handoff-e04.md) is documentation only. Required inherited
E00–E02 implementation is intentionally retained in local delivery.
