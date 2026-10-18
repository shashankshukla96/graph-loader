# E03 final validation and local delivery

Prepared 2026-10-17; independent final disposition is retained in
[final adversarial review](../reviews/e03-final-adversarial.md). All three story
reviews accepted the revised implementation. [Finding/resolution index](../reviews/e03-resolutions.md).

## Final tested identities (before staging/commit)

Actual branch `codex/pypi-library-checklist`, HEAD
`6de3562083cc7079091d056b89d66f18b69931e8`, plus preserved required E00–E02
implementation and E03 changes. [Entry](e03-entry.json),
[pre-final](e03-pre-final.json), [after final checks](e03-final.json).

- Source **`cd427a17593bc208581490d2959cca9dadc285bb8829cfca1a803002a6c3e559`**,
  293 safe tracked/untracked/missing-path rows, including moved runtime modules,
  seven new namespace tests, executable validation tools and documentation.
- Fresh CPython 3.12.14 development environment
  **`40e3fd48f013bb7b15f4694c39f2d6a53c38f3adda9a2a2b687d195c6a960b32`**.
- Source/environment and staged/unstaged binary diff hashes agree before/after
  the final suite and artifact/image/live checks. `_version.py` is unchanged.
  Artifact source-member closure and runtime AST comparison are independently
  verified; historical E02 source hashes are not reused as E03 identities.

Final dependency resolution, import origins, module inventories, exact commands
and SHA-256 values are linked from each result below. E00–E02 input changes at
entry were preserved intentionally; the entry archive/manifest distinguishes
those required prerequisites from E03's mechanical move.

## Final results

| Gate | Result and exact records |
| --- | --- |
| Fresh full unit/example suite | **3,387 passed, 4 inherited skips, 49 deselected, 19 inherited warnings**, 174.51 s; [command, skips/warnings and log identity](e03-final-tests.json) |
| Focused migration/planner/foundation/preflight/DLQ lane | **55 passed**, 3.08 s; all 55 exact IDs in [same record](e03-final-tests.json), including real spawned planner and canonical-class spawn |
| Required real B3 regression | **1 passed, no skip**, 25.53 s; [supervisor and actual property/offset/lock proof](e03-final-b3.json); fresh dev child, bounded owned process group, empty Docker before/after |
| Complete wheel/sdist and extracted no-Git rebuild | All 49 runtime modules match source bytes, all 54 wheel member bytes equal, strict Twine and five mutated-archive rejection controls pass; [artifact records](e03-final-artifacts.json), [exact commands](e03-final-artifact-commands.json) |
| Two clean noneditable environments | Original and rebuilt wheels separately pass 49 site-packages imports, no editable/path rescue, eight helps each, fresh old-import rejections, import-side-effect checks, version equality and pip check; [origins/resolutions](e03-final-artifacts.json) |
| Four local Docker images | All build; each passes 49 imported module source hashes/site-packages origins, eight component helps, default help, old-import rejection and pip check under isolated Python, unrelated cwd, no source mounts and no network; [IDs/builds](e03-final-images-build.json), [full command matrix](e03-final-image-probes.json) |
| Development and plan checks | Ruff (including new tools), pip check, runtime parity, whitespace, 477 relative links (zero broken); 13 epics / 26 features / 54 stories / 48 checklist IDs, acyclic |

The four inherited skip reasons remain distinct: bash-only readonly shell
control, unavailable historical Git objects in two slice guards, intentional
multi-pair example excluded from a single-pair invariant. Fifteen IMDb subset
and four planned-edge fork deprecations remain A5/E08 work. No collection or
assertions were removed to preserve counts. The seven new namespace tests
explain the increase from the 3,380-pass E02 baseline; the new positive import/
spawn controls were reproduced red before migration.

Final live proof: 3,000 input rows, **783 exact final pairs**, **582 overlapping
pairs**, four type-wide DLQ partitions, **4,154 envelopes = 4,154 committed**, no
per-topic DLQs or parked rows, **594 completed LIKES writes with zero lock
waiting**. Scheduling-dependent retry/write counts are this run's evidence,
not a claim to reproduce E02's counts. Only throwaway local services were used.

## Exact local artifact identities

- `neo4j_graph_loader-0.1.0a1.dev0-py3-none-any.whl`:
  `21a588d49dd37cd0671ed752643815b556a9dd169367902f9fafe660858a3aec`.
- `neo4j_graph_loader-0.1.0a1.dev0.tar.gz`:
  `96b6e49bcdac6b6c936d937dcf952fc23c1f3dfbe3b63da9f416095d251dce55`.
- Rebuilt wheel: `b88cd65ffad210366c33c861a50b3ed7b987719fc69635ebe05b35bf8d0a2b1d`.
  Wheel ZIP container hashes differ; **all member bytes agree**.

Final logs and exact archive copies are retained locally under ignored
`var/e03-validation-20261017-6de3562/{artifacts,rebuilt}`. Fresh venvs/raw work are
owned under `/tmp/graph-loader-e03-work`; neither belongs in version control.
Immutable local image IDs are recorded in the linked JSON, never registry
push digests. No test services remain; local proof images/candidates are retained.

## Scope and next work

Version remains `0.1.0a1.dev0`; all eight original mandatory dependency bounds,
paired Neo4j/Rust policy and no-extras decision remain. These are development
candidates, not an alpha release or E08 platform/E09 service acceptance. The
wheel includes migrated internal modules; E04 public wrappers/cancellation,
E05 resources/console command, E06 GHCR delivery and E07 installed full runner
remain unimplemented. [E04 handoff](../handoff-e04.md) preserves S5 private
transport → S3 public integration → S4 cleanup and all approved boundaries.

No GitHub push, upload, publication, release tag, merge or workflow was added.
The local commit retains the required inherited prerequisite implementation;
its bookkeeping changes HEAD/tracked/diff identities. Keep this tested snapshot
and compare actual per-file contents after committing; never claim its combined
HEAD-based hash is unchanged by a commit. Postcommit comparison is recorded
separately after the final review and inspected staging.

## Local implementation commit and postcommit comparison

Implementation commit **`bf5471e9243dbda90cdf066a5d811486119dba7c`** on the
existing branch includes E03 plus its intentionally preserved required E00–E02
foundation. [Postcommit manifest/comparison](e03-postcommit.json) confirms all
**245 existing safe source inputs** retain the exact tested content, sizes and
executable modes; the environment is unchanged. The 48 missing legacy-path
rows naturally leave the Git index after the move. Combined HEAD-based identity
changes are expected. The accompanying documentation-only evidence commit
contains this postcommit record; it changes no tested source/build input.

Final local link validation checks **516 relative links**, zero broken. The
pre-final count of 477 is retained in its historical check record; added review
and completion links explain the increase. No extra test rerun is claimed
from committing identical tested source contents.
