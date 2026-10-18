# Independent implementation-plan review — round 01

Date: 2026-10-15. Reviewer task identity: `/root/review_library_epics`, independently delegated by `/root`. This report is the reviewer's original assessment of the plan as received; it does not implement product stories.

**Overall verdict: CHANGES REQUESTED.** Six epics are approved and seven require changes, as enumerated below. Approval means that the decomposition is executable; it never means implementation completion, release readiness, or authorization to publish, push, change accounts, or operate on user data.

## Material reviewed

Read in full:

- `plan/library-implementation/README.md`
- `plan/library-implementation/decisions.md`
- `plan/library-implementation/backlog.json` (parsed the entire canonical structure and checked every story's change, acceptance and validation text against the rendered epic)
- `plan/library-implementation/traceability.md`
- `plan/library-implementation/reviews/README.md`
- `plan/library-implementation/tools/render_plan.py`
- Every epic: `plan/library-implementation/epics/e00.md`, `e01.md`, `e02.md`, `e03.md`, `e04.md`, `e05.md`, `e06.md`, `e07.md`, `e08.md`, `e09.md`, `e10.md`, `e11.md`, `e12.md`
- `docs/PYPI_LIBRARY_CHECKLIST.md`
- `docs/PYPI_CHECKLIST_REVIEW.md`
- `pyproject.toml`, `requirements.txt`, `requirements-dev.txt`
- `Dockerfile`, `Dockerfile.node_loader`, `Dockerfile.edge_loader`, `Dockerfile.clock`, `Makefile`
- `tests/conftest.py`, `tests/it_scratch_db.py`, `tests/fixtures/multi_topic_arrival.py`, `src/utils/schema_loader.py`, `src/utils/consumer_groups.py`

Inspected relevant implementation/test sections and search results in:

- `src/loader/edge_planner.py`: settings/source/result interfaces, worker IPC, inline execution, startup/output ownership, finish/close, convenience/public producer calls and multiprocessing context selection
- `src/loader/edge_plan.py`: formats, types, encoding and public errors
- `src/loader/retry_consumer.py`: retry/deduplication/ordering contracts
- `src/utils/metrics.py`: registry model, environment handling and version identity
- `src/orchestrator/docker_service.py`: image defaults/builders, metrics version propagation, runtime paths and launch surfaces
- `src/cli.py`: eager dependencies, process-control setup, runtime-path references and entry point
- `tests/test_external_planner.py`, `tests/test_library_preflight.py`: reference/fixture dependencies, ownership and source-isolation regressions
- `tests/integration/test_multi_topic_relationship.py`: service fixtures, supported modes, arrival DLQ case and subprocess/resource assumptions
- `tests/test_streaming_lease_e2e.py`: opt-in developer services, scratch database, checkout paths and container supervision
- `examples/imdb/publish_and_load_nodes.py`: source-path bootstrap, child-process working directories, runtime schemas and start methods
- Repository-wide searches of `src/`, `tests/`, `examples/`, and `scripts/` for namespace imports and checkout/path assumptions.

No applicable `AGENTS.md` was found in the repository or checked ancestor directories. Existing prerequisite modifications and untracked test files were inspected but preserved. No services, product tests, package/image builds, remote connections, uploads, account changes, or pushes were performed.

## Read-only checks and strengths

A read-only Python validation parsed all backlog data, checked unique IDs, confirmed 13 epics / 26 features / 40 stories, checked the actual 0.5–2-day numeric range, and confirmed all **48/48** checklist IDs have references. The combined graph, including inherited epic prerequisites for every story, is acyclic. Every canonical story change/acceptance/validation string appears in its rendered epic. The renderer was read rather than executed because it writes generated files.

This validates structural traceability, not semantic completion of the checklist or realistic effort. The plan correctly separates the actual `src` package from a future source-layout directory; does not mistake checkout tests or the IMDb load for wheel/DLQ evidence; requires wheel and independently rebuilt sdist provenance in parent and child processes; preserves Neo4j/Rust pairing; and makes D01–D07 user decisions. The prebuilt-image/bundled-context, stronger-ordering, expanded-platform and automated-publisher branches are explicit choices rather than silently accepted defaults. The no-push, exact-artifact and scratch-data protections are generally sound.

## Per-epic verdicts

| Epic | Verdict | Reason / required finding resolution |
| --- | --- | --- |
| E00 | **CHANGES REQUESTED** | R01: the exact baseline identity must include the currently untracked prerequisite tests/fixtures. |
| E01 | **APPROVED** | Identity, scope, environments, dependency/image/publisher choices and escalation for broader scope are explicit user gates. |
| E02 | **APPROVED** | The deliberately incomplete development scaffold avoids pretending the loader is already installed; metadata, dependency authority and version work are bounded. |
| E03 | **CHANGES REQUESTED** | R02: explicitly implement the true-source-layout bootstrap for checkout tests and existing component images as part of the atomic move. |
| E04 | **CHANGES REQUESTED** | R03: public cancellation/timeout behavior needs a concrete bounded implementation contract and separate work from inherited ownership documentation. |
| E05 | **APPROVED** | Installed command/resources and writable-state work have concrete acceptance checks and correctly defer image selection. |
| E06 | **APPROVED** | Local development image delivery, selection and host/container tests are coherent; final release-image rebuilding belongs to the E11 fix in R06. |
| E07 | **CHANGES REQUESTED** | R04: the regression-port story is an unenumerated cross-cutting bucket; inventory and bounded porting slices are required. |
| E08 | **APPROVED** | Minimum/current paired dependency lanes and supported start methods are gated by D04/D05; untested cells block support claims. |
| E09 | **CHANGES REQUESTED** | R05: split the broad mode/failure story and make ordering-boundary probes/exclusions explicit, including replicas, replay and topic recreation. |
| E10 | **APPROVED** | Installed examples, index description, compatibility/support text and pre-freeze release notes are actionable and tied to executed behavior. Any added pre-release recovery draft must be reviewed as an affected change. |
| E11 | **CHANGES REQUESTED** | R06: freeze/rebuild and test final component images from the final versioned source, rather than merely recording earlier image IDs. |
| E12 | **CHANGES REQUESTED** | R07: establish and dry-run the recovery procedure before the production approval that is required to present it. |

Each APPROVED verdict applies only to this reviewed revision and to decomposition quality. Changed downstream interfaces/gates must be reviewed again. Approval of a prerequisite epic does not waive changes requested in a dependent epic.

## Required changes

### R01 — Include untracked prerequisite contents in baseline identity

Priority: P2. Affected epic/story: E00 / E00.F1.S1.

The story requests HEAD, a working-tree patch hash and a changed-file inventory. In this actual checkout, `tests/test_library_preflight.py` and `tests/fixtures/` are untracked and contain the new B1/B2 regressions and the maintained B3 dataset. A normal Git diff hash does not identify those contents. An inventory of filenames cannot establish that the linked 3,366/406 results tested the exact same files. This matters before these files are moved or changed during migration.

**Required fix:** define a safe content manifest for all relevant tracked modifications and untracked prerequisite files, alongside HEAD and staged/unstaged patch identity. Record the tested interpreter/dependency environment and identify any difference from the earlier evidence. Do not capture ignored datasets, credentials or private operational files. Clarify that the no-skip B3 requirement applies to its required integration case, while inherited unit-suite skips must be individually explained rather than accidentally making the recorded baseline impossible to reuse.

**Approval criterion:** E00's concrete snapshot procedure identifies changes to an untracked fixture/test as a different baseline and explicitly determines when existing evidence can be reused or rerun.

### R02 — Make checkout and image bootstrapping part of the source-layout migration

Priority: P1. Affected epic/story: E03 / E03.F1.S1, with verification in E03.F2.S2.

The existing pytest configuration has `pythonpath = ["."]`; the component Dockerfiles copy the current package to `/app/src` and use `PYTHONPATH=/app`. Moving modules under `src/<import_name>/` and replacing imports/`-m` names alone makes the new top-level import unavailable through those existing paths. Updating Docker entry-point strings and successfully building an image does not prove the entry point starts. This is part of preserving a runnable atomic migration, before E06's delivery redesign. The checkout contains 48 runtime Python modules and 149 Python/shell files with namespace references, so the bootstrap must be an explicit part of the migration checklist.

**Required fix:** explicitly name `pyproject.toml`/test bootstrap, development installation instructions and all existing Dockerfiles as migration change surfaces. Choose a coherent checkout path (for example a documented editable install or source-layout pytest path) and an immediately runnable component image path (install the package or use the correct source root). Keep installed-artifact tests independent of that development bootstrap. Define exact clean-environment checkout test and built-container module/help smoke commands.

**Approval criterion:** the atomic story has verifiable commands that resolve the renamed package in checkout tests and start each required built component image; it cannot pass by only renaming imports and building an unstartable image. The mechanical migration may remain cohesive; this finding does not require independently released partial renames.

### R03 — Bound and specify cancellation/timeout work before stabilizing the API

Priority: P1. Affected epic/stories: E04 / E04.F2.S1 and E04.F2.S2 as needed.

The one-day ownership story combines already-implemented B1 ownership guarantees, public cancellation/timeouts/thread-safety, lifecycle usage, partial-start cleanup and a promise to assert bounded shutdown. These are not all documentation of existing behavior. `_ProcessWorker.recv()` currently blocks on a pipe without a deadline, while `_InlineWorker.send()` executes arbitrary caller sources synchronously. A source that never returns cannot universally be made bounded by documenting `close()` or a context manager. No story specifies which cancellation input, deadline, exception or concurrency behavior is to be implemented, or which inline-source blocking limits the public contract accepts.

**Required fix:** separate the inherited ownership/lifecycle contract from a bounded cancellation/timeout story. Resolve the supported cancellation surface and inline-versus-process behavior within the approved API scope; enumerate concrete changes to wait/poll/worker-stop behavior if required. If arbitrary inline source interruption is unsupported, say so explicitly and scope bounded-shutdown claims accordingly. Require deterministic blocked-worker, source failure, caller cancellation and cleanup tests through the public surface. A larger cancellation redesign must trigger a revised small-story decomposition rather than being absorbed into this one-day story.

**Approval criterion:** a contributor can implement the selected semantics and tests without inventing a new public guarantee; each resulting story remains within the claimed size, and bounded shutdown has an explicit testable boundary.

### R04 — Enumerate and divide the installed regression port

Priority: P2. Affected epic/story: E07 / E07.F1.S2.

“Schema/planner/chunk/multi-topic/multi-pair/fencing/DLQ/retry/offset/shutdown tests” is a coverage category list, not an executable porting slice. The current suites have cross-test imports, reference helpers, root-relative config reads, subprocess checkout working directories and global conftest behavior. For example, `test_external_planner.py` imports both `reference_edge_planner` and helpers from `test_edge_planner`; service tests also use `tests.stress` helpers. Recording the selection only after porting leaves the promised 1.5-day task unbounded and makes silent coverage reduction difficult to detect during implementation.

**Required fix:** add a small inventory/classification story, with an explicit maintained list of test files or test IDs, helper/config fixture closure, provenance cases and justified exclusions. Split actual porting into coherent small slices, such as schema/planner/encoding and coordination/retry/offset/shutdown, while retaining one common isolated runner and one final parity gate. State which service probes are exercised later in E09 rather than silently excluding them from installed-code evidence.

**Approval criterion:** the plan names initial concrete suite/fixture surfaces and each porting story has a bounded selection, explicit preservation checks and an executable runner command. The later evidence map can refine this inventory without substituting a smaller undisclosed regression set.

### R05 — Make mode/failure and ordering validation executable small slices

Priority: P2. Affected epic/stories: E09 / E09.F1.S2 and E09.F2.S1.

E09.F1.S2 places supported streaming and classic modes, final graphs, offsets, endpoint delays, DLQ parking/replay, outages, supervision, graceful shutdown and the installed arrival-DLQ rerun in one 1.5-day story without naming test selections. These currently span in-process testcontainer coverage and an opt-in Docker/developer-stack E2E path. They do not share the same bootstrap or failure harness automatically. E09.F2.S1 explicitly lists restart/revocation/eviction/cross-topic cases, but leaves replicas, manual replay and topic recreation under the generic phrase “advertised order_by boundaries.” Those are explicit original V7 boundaries; identifier/offset reuse after recreation particularly needs an explicit supported/excluded disposition.

**Required fix:** split mode correctness and retry/outage/supervision into independently executable small stories with concrete existing test targets and any new harness/probe changes identified. Preserve the required installed arrival-DLQ case. Add an ordering case matrix covering restart, revocation/rebalance, eviction, cross-topic/partition records, multiple replicas, manual replay and topic recreation. For each, identify a supported guarantee and deterministic assertion, or an approved exclusion demonstrated/documented appropriately. Do not require a new durability promise or fix a deliberately excluded guarantee.

**Approval criterion:** every supported mode/failure case has an identifiable runner and assertion within bounded stories; all named V7 boundaries have explicit owners/dispositions, and excluded counterexamples cannot be reported as passing safety guarantees.

### R06 — Freeze final images together with the final package candidate

Priority: P1. Affected epic/story: E11 / E11.F1.S2; publication/retrieval stories consume the resulting manifest.

E06 builds development images before E08 may change multiprocessing/runtime source and before E10 finalizes the candidate version. E11.F1.S2 requires building and hashing wheel/sdist files, repeating E07–E09 and recording local image identities, but never explicitly rebuilds final component images from the frozen/tagged candidate. The existing Dockerfiles copy source directly rather than installing the tested wheel. Merely carrying forward E06/E09 image IDs can therefore release older source or `dev` version labels alongside a newer tested wheel. Hashing or repulling those images does not prove source/version agreement.

**Required fix:** add an explicit final-image freeze/build step or story after the clean candidate commit and, for SCM mode, after its local version tag. Build every shipped component from that exact source/version and reviewed dependency inputs (or explicitly from the frozen wheel), record source/version/build inputs and local image IDs/OCI identities, and assert package/CLI/metrics/image version compatibility. Run final integration gates against these final images before upload approval. Any earlier development-image evidence must be marked superseded; E11 publication and E12 must consume only the frozen manifest. Account for the bundled-context branch without inventing a registry publication requirement for it.

**Approval criterion:** a source/version change after E06 necessarily invalidates earlier image evidence; the final manifest proves which immutable images/context accompany the exact validated wheel/sdist, and those final images are the ones tested and authorized for publication.

### R07 — Produce recovery instructions before requesting production approval

Priority: P2. Affected epic/stories: E12 / E12.F1.S1 and E12.F2.S2; an earlier documentation/rehearsal story may own the draft.

E12.F1.S1 requires presenting a rollback plan to the user before production authorization. The only concrete runbook/recovery dry-run story is E12.F2.S2, which depends on verified production publication. The README's general permission to draft documentation early is not a dependency or an acceptance gate. As written, the production approval can be requested before the procedure it is supposed to review has an owner or completed dry run.

**Required fix:** move the actionable recovery/runbook draft and hypothetical broken-release exercise into an explicit pre-production story (for example in E10/E11 or before E12.F1.S1), and make production authorization depend on it. Keep actual release references and post-release maintenance updates in E12.F2.S2 if useful. Retain separate explicit authorization for any real yank/deletion/data mutation; the dry run must not modify user services or uploaded artifacts.

**Approval criterion:** the pre-production gate links to an already reviewed/exercised procedure with explicit project/version targeting, yanking/new-version repair, immutable artifacts and data-preservation rules. No future post-release task is required to supply the rollback material used to obtain production permission.

## Re-review conditions

Revise canonical backlog data, regenerate the epic/traceability pages, and validate the combined dependency graph and all 48 checklist mappings. Preserve this report unchanged and record each finding's resolution in the next round. Re-review every changed epic and any changed downstream interface/gate; retain explicit per-epic verdicts. No finding asks for product implementation, a service run, a package/image build, a new workflow, publication, or a push during this planning task.
