# Library implementation plan

Prepared 2026-10-15; latest repair acceptance 2026-10-21. **All 13 epic decompositions remain approved by the [fresh independent plan audit](reviews/fresh-audit-02.md). E00–E04 are accepted. [E04-R1](reviews/e04-completion-verification-20261020.md) reopened S3/S4 after the original delivery; the [repair evidence](evidence/e04-r1-repair.md) and [fresh final review](reviews/e04-r1-final-review.md) now close that finding. [E05 delivered-code revalidation](reviews/e05-completion-verification-20261021.md) repairs two low-priority findings and is independently accepted after 3,571 test passes; E06–E12 have not started. Historical failures, reviews and results are preserved.**

This decomposes the [library/PyPI checklist](../../docs/PYPI_LIBRARY_CHECKLIST.md)
into **13 phase epics, 26 features and 54 stories**. It is a planning deliverable;
implementation and release approval remain separate. No push or publication is
authorized by this plan. Existing source changes and the loaded IMDb graph must
be preserved.

## Baseline and scope

The historical baseline was HEAD `d53a92c` plus required prerequisite code/doc/test
changes; E03 began at `6de3562` and its manifests record actual contents. The 2026-10-15 evidence is 3,366 tests passed, 4 skipped, 49 integration/
smoke tests deselected; 406 focused tests passed; lint/dependency checks passed.
The resumed IMDb 10,000-seed, depth-4 planned load produced 1,247,026 nodes and
4,510,991 relationships, with all 1,780 chunks accounted for. Logs and benchmark
artifacts are local/ignored. These results prove the exercised planned-load
path, not the separately verified DLQ regression or future release artifacts.

B1/B2 are implemented and E00 now binds fresh evidence to the repaired snapshot:
**3,374 unit/example tests passed, four inherited skips individually explained,
49 integration/smoke cases deselected and 19 fork warnings; 417 focused cases
passed**. **B3 passed without a skip** against owned real Kafka/Neo4j services,
with the exact graph, DLQ commits, parking and lock-wait assertions checked.
See [baseline evidence](evidence/E00.F1.S1.md), [DLQ evidence](evidence/E00.F2.S1.md)
and the [conditional E02 handoff](handoff-e02.md). The earlier results above
remain historical. Nineteen existing fork deprecation warnings remain A5 work.
These E00 results predate package builds; no release exists, and the approved
public API contract is a future implementation target.

The [decision register](decisions.md) records approved names, public scope,
compatibility/support/dependency targets and publishing policy. Under the user's
instruction to finish the remaining recommendations, prebuilt release images
use `ghcr.io/shashankshukla96`; Shashank Shukla is accountable for images and
releases. [Independent completion checkpoints](reviews/e01-completion-checkpoints.md)
review the investigation, applied records and final validation. Later account,
artifact, support and publication evidence/authorization remain separate. Normal
local implementation choices can be made within an approved story. Changes to
public scope, guarantees, external ownership or support promises require the
relevant user decision, not repeated permission for routine reversible work.

## E02 distribution foundation

[E02](epics/e02.md) is complete under [per-story independent review](reviews/e02-checkpoints.md)
and the [fresh final adversarial review](reviews/e02-final-adversarial.md), which
found no actionable issue. The local
artifacts ship only the `neo4j_graph_loader` namespace/version scaffold, with
explicit development-only metadata. Legacy loader commands still use the checkout;
no installed loader/API/CLI is supplied. Runtime dependencies are authoritative
in pyproject and generated/checkable for Docker, with unchanged bounds. The file
version `0.1.0a1.dev0` agrees with metadata and default metrics; deployment overrides
remain explicit. Sdist rebuild and clean artifact-import/base-dependency checks pass.

Final E02 evidence: **3,380 passed, four explained inherited skips, 49 deselected,
19 inherited fork warnings**; **147 focused passes**. See [S1](evidence/E02.F1.S1.md),
[S2](evidence/E02.F1.S2.md), [S3](evidence/E02.F2.S1.md), final source/artifact JSON
and the [E03 handoff](handoff-e03.md). These are a new source identity, not a
relabeling of E00's earlier baseline. Those are historical E02-only results. E04–E12 remain unstarted; no publication or
remote graph operation was performed.

## E03 completion and current handoff

E03 implementation is committed at `bf5471e`, followed by the evidence-only
`ffb8625`. [Fresh verification on 2026-10-18](reviews/e03-completion-verification-20261018.md)
and the [independent source review](reviews/e03-revalidation-independent-20261018.md)
accept the actual migration: 3,387 tests passed, the exact no-skip DLQ regression
passed, and current artifacts/clean installs/four-image matrices passed.
The wheel now includes all 49 migrated runtime modules; explicit public wrappers
and cancellation are E04, console/resources are E05, and release delivery remains
later work. Historical E02-only scaffold statements above describe that earlier
phase, not the current E03 artifact.

Use the [E04 handoff](handoff-e04.md) for the next assignment and maintain the
[commit change tracker](CHANGE_TRACKER.md) for every implementation/fix/delivery.
E04–E12 remain unstarted. Nothing authorizes a GitHub push or publication.

## Phases / epics

Every row links to features and stories with change surfaces, dependencies,
acceptance criteria, checks, evidence locations and estimates. Each story is
sized at 0.5–2 focused engineering days; unknown external waits are excluded.
The estimate is a planning aid, not a delivery commitment. Split a story if
execution discovery exceeds that bound and re-review the changed epic.

| Phase epic | Deliverable | Depends on |
| --- | --- | --- |
| [E00 — Prerequisite baseline](epics/e00.md) | Tracked/untracked content identity and actual B3 DLQ pass | Current checkout |
| [E01 — Alpha release contract](epics/e01.md) | Resolved D01–D07 choices | May run alongside E00 |
| [E02 — Distribution foundation](epics/e02.md) | Development-only build/version scaffold and dependency authority | E00, E01 |
| [E03 — Namespace migration](epics/e03.md) | Complete renamed loader, imports and executable references | E02 |
| [E04 — Public API](epics/e04.md) | Explicit schema/planning contracts, ownership and process isolation | E03 |
| [E05 — Installed CLI/resources](epics/e05.md) | Executable, packaged samples and writable state paths | E04 |
| [E06 — Docker delivery](epics/e06.md) | Checkout-free image selection/build context and networking | E05 |
| [E07 — Installed regressions](epics/e07.md) | Isolated wheel and sdist-built-wheel regression runner | E06 |
| [E08 — Support matrix](epics/e08.md) | Dependency, platform and multiprocessing proofs | E07 |
| [E09 — Service guarantees](epics/e09.md) | Supported load modes, failure/offset/ordering evidence | E08 |
| [E10 — User documentation](epics/e10.md) | Executable quick starts, compatibility/support and final release text | E09 |
| [E11 — Candidate rehearsal](epics/e11.md) | Frozen artifacts and separately authorized TestPyPI/image rehearsal | E10 |
| [E12 — Production/maintenance](epics/e12.md) | Separately authorized PyPI release and recovery runbook | E11 |

The chain describes exit gates, not a ban on independent drafting: E01 may run
with E00, documentation drafts may start early, and test harness work may be
prepared ahead of integration. A dependent epic cannot be accepted until its
prerequisites pass. Within an epic, story dependency IDs add ordering constraints;
all epic prerequisites apply to every story. Do not start live release operations
under a documentation-drafting exception.

The E03 mechanical migration is one cohesive integration change: module moves,
Python imports, monkeypatch string targets, subprocess/module commands,
Dockerfiles and executable examples must change together to keep the loader
runnable. It is deliberately not split into partially usable namespace releases.

## Executing a story

1. Resolve prerequisites and any referenced D01–D07 decisions. Read applicable
   repository instructions, preserve existing user work and choose only named
   throwaway service/test resources.
2. Make the story's concrete change, including its focused regression where
   behavior can fail. Keep unrelated algorithm rewrites out of migration work.
3. Execute its validation and inherited epic gate; record exact environment,
   source/artifact identity, commands, results, skips, warnings and limitations.
4. Create `evidence/<story-id>.md` with safe summaries/links. Large logs/data,
   credentials and private files remain outside tracked plan evidence. A skip
   is not a pass; N/A requires an explicit scope decision and rationale.
5. Mark implementation complete only with evidence. An epic closes when all
   its applicable stories and exit gate pass. Reviewer approval of the plan
   never marks implementation complete.

If an integration prerequisite fails, repair the bounded cause and rerun it;
if diagnosis expands scope, split new stories and obtain independent plan
approval. Do not restore removed planning scripts as runtime/test dependencies.
No test may clear the remote IMDb database; use throwaway containers or an
explicitly named scratch database with existing safety guards.

## Source, package and image freshness

Every runtime/dependency/resource/version change invalidates the affected wheel,
sdist and component-image evidence. Rebuild those artifacts from the same
identified working-tree contents before any downstream installed/service gate;
record staged/unstaged differences plus tracked/untracked source hashes. E03's
checkout bootstrap is a documented editable install, and each legacy Dockerfile
must immediately install/resolve the renamed package. E07 artifact environments
must not inherit that editable install or development path setup.

E07 freezes an explicit [test inventory](test-inventory.json) and ports it in
bounded core, safety and CLI/runtime slices. E09 first provisions one real
isolated-service adapter, then separately proves planned bulk, streaming,
classic (if included), DLQ, failure/supervision and the
[ordering case matrix](ordering-matrix.md). Unit/in-process reference tests do
not substitute for required real-service/image executions.

E04 separates existing ownership behavior from the proposed
[cancellation contract](cancellation-contract.md). D02 must approve its surface
and limits before implementation. Inline arbitrary blocked caller code is not
promised universal interruption. Complete-message nonblocking framing, parent
wait/cancellation integration and bounded cleanup each have their own story and
negative-control tests; readiness followed by blocking recv is insufficient.

## Publication and candidate boundaries

E02–E10 local artifacts are development builds. E11 freezes code and package
release notes at a clean reviewed snapshot; SCM versioning creates the local
candidate tag before building. File-based versioning records the tested commit
and creates the release tag there in E12. Record SHA-256 hashes and reuse the
same wheel/sdist bytes through TestPyPI and production; no rebuild by assumption.

E06/E09 development image IDs are not release identities. After the clean E11
source/version freeze (and local SCM tag where applicable), E11.F1.S3 builds all
final components from that source/version or the exact frozen wheel, checks
package/CLI/metrics/image versions, and records build-input and image identities.
E11.F1.S4 reruns final installed/matrix/service gates with those exact images.
For bundled-context delivery, validate the final packaged context and images
built from it; no registry publication is required merely by choosing that path.
A
registry digest is recorded only after the separately authorized E11 image push;
re-pull and verify content compatibility before accepting the rehearsal. The
TestPyPI upload and image push need their own explicit instruction naming the
candidate; production PyPI and any Git pushes need the E12 production instruction.
E10.F2.S2 prepares and dry-runs the recovery runbook before E12 can request
production approval. E12's post-release maintenance story only fills actual
release references into that already exercised procedure.
The current **do-not-push** instruction remains in force until explicitly changed.
All existing no-workflows policy changes are user decisions.

## Canonical files and validation

- [backlog.json](backlog.json) is the canonical hierarchy/dependencies/coverage.
- [Checklist traceability](traceability.md) maps all 48 checklist IDs to stories.
- [Decision register](decisions.md) holds unresolved choices, not invented answers.
- [Cancellation contract](cancellation-contract.md) and [ordering matrix](ordering-matrix.md) make API and guarantee boundaries explicit.
- [Test inventory](test-inventory.json) names bounded lanes, helper/config closure and proposed real-service targets.
- [Independent review record](reviews/README.md) tracks rounds and per-epic verdicts.

After changing backlog data, run:

```bash
python3 plan/library-implementation/tools/render_plan.py
git diff --check
```

The renderer validates IDs, 0.5–2-day story sizing, references, checklist coverage
and dependency cycles, and regenerates the epic/traceability Markdown. Generated
files should be changed through `backlog.json`, not edited in parallel. A plan
change after review invalidates approval for affected epics until reviewed again.
Plan/development records must be excluded by future wheel/sdist inventory gates.

## E03 namespace migration

All 49 runtime Python modules use `neo4j_graph_loader`; no generic `src` aliases
ship. [Execution](execution-e03.md), [runtime story](evidence/E03.F1.S1.md),
[compatibility story](evidence/E03.F2.S1.md), [artifact story](evidence/E03.F2.S2.md)
and the independent [runtime](reviews/e03-runtime-review.md),
[policy](reviews/e03-policy-review.md), [artifact](reviews/e03-artifact-review.md)
reports record checks and repairs. E03 leaves public API wrappers, resources,
console command, GHCR delivery and installed/platform/service matrices to their
approved E04–E12 gates. The [E04 handoff](handoff-e04.md) preserves the private
transport → public integration → cleanup sequence without implementing it.

E03 final acceptance: [completion evidence](evidence/e03-completion.md),
[fresh zero-history adversarial review](reviews/e03-final-adversarial.md). All
actionable findings were repaired and independently rechecked. No upload or
GitHub push was performed; E04–E12 remain unstarted.


## E04 public API and bounded lifecycle

[E04](epics/e04.md) is complete after all six ordered stories and a fresh
[final adversarial review](reviews/e04-final-adversarial-review.md). The
[public API guide](../../docs/LIBRARY_API.md) specifies explicit schema/planning
exports, Kafka/source requirements, exclusive ownership, cooperative operation
deadlines and one aggregate cleanup budget with retained-worker diagnostics.
Imports are inert and public planners own independent default metrics registries.

[Final evidence](evidence/e04-final-validation.md) binds the fresh3,512-pass suite,
four inherited skips,49 deselections and19 known fork warnings to source and
environment; exact B3 passed without skip. Isolated artifacts/no-Git rebuild,
two clean installs and all four new immutable image matrices passed.
[Review resolutions](reviews/e04-resolutions.md) preserve findings/repairs and
independent rechecks. [E05 handoff](handoff-e05.md) is prepared; E05–E12 are not
implemented. [Change tracker](CHANGE_TRACKER.md) records actual local delivery
and per-file tested-content equality; no publication is authorized.


## E04-R1 repair acceptance — 2026-10-21

The coordinator now propagates a received worker failure with its diagnostic
cause before waiting on another lane or checking interruption again. This lets
shared cleanup reap a blocked peer. Sequential receive order is unchanged;
errors behind an earlier blocked lane may remain unobserved until interruption.
The new public regressions cover mixed failing/blocked real sources with default
and finite timeout, plus interruption immediately after receiving an inline error.

[Repair evidence](evidence/e04-r1-repair.md) and
[fresh final acceptance](reviews/e04-r1-final-review.md) bind the repaired state
to3,516 passes, four inherited skips,49 deselections and19 known warnings;
exact B3, isolated original/rebuilt artifacts, two clean installs, four newly
built image matrices and12 checkout-free installed mixed-source controls pass.
E04 S3/S4 are restored complete. [E05 handoff](handoff-e05.md) is ready after
local delivery recording; no E05 implementation, push or publication is included.


## E05 accepted — 2026-10-21

Installed graph-loader/package-module commands, finite canonical/social schema
resources and private persistent run state are complete. All three stories and
the [fresh final review](reviews/e05-final-adversarial-review.md) accepted after
retained fixes. Exact [final evidence](evidence/e05-final-validation.md) records
3566passes, four inherited skips,49 deselections,19 known warnings, real no-Git
rebuild/member equality/two clean installs, four immutable read-only image checks,
actual data binds,12public R1 controls and no-skip B3. All270 tested inputs and
environment are unchanged across finald proof.

Local delivery is `2265e4fd1d9f76eb2813856157dea89a80b9f216`;
[postcommit proof](evidence/e05-postcommit.json) verifies all270 tested, committed
and current inputs with unchanged environment. The [tracker](CHANGE_TRACKER.md)
records the actual hash; its documentation follow-up follows rule5. The
[E06 handoff](handoff-e06.md) is prepared; E06–E12 and publication are not executed.
