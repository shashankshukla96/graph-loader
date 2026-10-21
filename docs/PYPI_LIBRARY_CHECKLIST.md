# Library and PyPI publishing checklist

Status: proposed work for review, not a release approval. Initially audited on 2026-10-11 against `main` at `50187c8`;
prerequisite review updated on 2026-10-12. This document lives on
`codex/pypi-library-checklist`; `main` remains unchanged.

The loader works from a checkout, but it is not yet a supported, installable
Python distribution. Adding a wheel alone will not make its Docker orchestration,
paths, lifecycle, or imports usable by another application.

## Historical E03 evidence

This retained checkpoint predates the public API and E05 installed command,
resources and private state. Current implementation proof is in
[E05 validation](../plan/library-implementation/evidence/e05-final-validation.md)
and the [change tracker](../plan/library-implementation/CHANGE_TRACKER.md).
The release, platform and installed service matrix remain open.

| Area | What exists | What is missing |
| --- | --- | --- |
| Packaging | E03 development wheel/sdist contain all 49 migrated Python runtime modules; isolated rebuild/install/import checks pass | Public APIs, resources and console entry point remain E04–E05 |
| Imports | Runtime and first-party executable targets use `neo4j_graph_loader`; old imports are rejected without aliases | E04 public exports and E07 installed regression runner |
| Reusable code | `load_schema`, schema models, `PlanSettings`, `EdgeRecordSource`, `producer_edge_planner`, `plan_and_publish_edges` | Explicit public API, compatibility promises, and examples run from an installed wheel |
| CLI | `python -m neo4j_graph_loader.cli` and migrated component module commands | E05 installed executable/resources |
| Deployment | Dockerfiles and `DockerService` image builders using `path="."`; runtime mounts and rejection paths | A deployment strategy that works without the checkout and outside its working directory |
| Dependencies | pyproject-authoritative eight unchanged runtime bounds, generated Docker requirements/parity gate; clean base resolution and Rust/driver pairing pass | E08 minimum/current platform matrix; selected base remains mandatory with no extras |
| License/docs | Apache-2.0 `LICENSE`, operational guides, unversioned changelog | Packaged license metadata and an installation/API guide for PyPI users |
| Validation | Fresh E03 suite: 3,387 passed, 4 inherited skips, 49 deselected, 19 inherited warnings; 55 focused passes; isolated artifacts and four image import/help probes pass | Installed-artifact service integration, platform matrix and A5 warning resolution |
| Integration fixtures | Maintained arrival dataset and exact B3 real-service test passed without skips on 2026-10-16 | Installed release-image service/mode/failure matrix under V6 |
| Publishing | GitHub repository exists | No release publisher or verified PyPI/TestPyPI project ownership |

Every unchecked item below requires its own evidence before marking it complete.
Use the current implementation tracker for delivered APIs and commands; approved
release/support targets still require their remaining downstream proof. The proposed normalized
name returned HTTP 404 from both indexes on 2026-10-16; this does not reserve it
or establish ownership. Account access remains unverified. See the
[D01–D07 decisions and remaining ownership gate](../plan/library-implementation/decision-brief.md).

## 0. Establish the prerequisite baseline

Complete this phase before the namespace migration. Scope/identity decisions
can proceed alongside it; no packaging or publication gate is implied.

- [x] **B1 — Prove planner isolation and recovery.** Regression-test concurrent
  planners with identical IDs, exclusive output-path ownership, failure followed
  by success at the same path, partial startup cleanup, validation before Kafka
  side effects, and cleanup of only topics created by the failing call. A killed
  owner can leave a `.lock` file: remove it only after confirming that run is
  stopped. Unique spill directories must never be deleted by another run.
- [x] **B2 — Establish a service-independent unit baseline.** Mock Neo4j and
  Docker in the CLI snapshot failure test; run unit tests without a running
  stack. Test public schema-read exceptions and logging-neutral imports.
Evidence for B1/B2 (2026-10-15): 3,366 unit/example tests passed, 4 skipped,
49 integration/smoke tests deselected; Ruff and `pip check` passed. The focused
lifecycle/auto-settings/planner suite passed 406 tests. The existing 19
multithreaded-fork deprecation warnings remain tracked by A5; these runs do not
prove cross-platform support. No release gate is satisfied by this baseline.

- [x] **B3 — Restore maintained integration fixtures.** Keep executable arrival
  data under `tests/fixtures/`, test its ordering and overlap, and execute the
  affected real-service DLQ test without any removed planning directory.

Fresh B1–B3 evidence (2026-10-16): **3,374 unit/example tests passed**, four
inherited skips explained, 49 integration/smoke cases deselected, 19 existing
fork warnings; **417 focused regressions passed**. The exact B3 real-service
case passed without a skip: 783 exact relationship values, all 4,059 DLQ
envelopes committed, nothing parked and 561 completed LIKES writes with zero
lock waiting. This includes bounded ownership/fixture-cleanup repairs found
by independent review. See [identified baseline](../plan/library-implementation/evidence/E00.F1.S1.md),
[B3 proof](../plan/library-implementation/evidence/E00.F2.S1.md) and
[independent execution acceptance](../plan/library-implementation/reviews/execution-e00-e01-01.md).
V6's installed release-image/mode matrix remains open; this source-checkout
proof does not satisfy that later gate.

## 1. Agree the first release contract

- [x] **S1 — Choose package identity.** Select and check a PyPI distribution
  name (candidate: `neo4j-graph-loader`), its normalized-name collisions, and an
  import package (candidate: `neo4j_graph_loader`). Record ownership and a
  public maintainer contact; do not copy the current machine-local Git email.
- [x] **S2 — Define what the library offers.** Recommended initial scope:
  schema loading/validation and existing relationship planning APIs, alongside
  the installed loader CLI. Decide whether embedded node/edge ingestion or
  fleet orchestration is public in v1. Resolve V7 here: choose supported retry
  ordering guarantees before exposing ingestion APIs. State that the producer planner publishes
  chunks to Kafka; it is not currently an offline, broker-free planner.
- [x] **S3 — Choose release maturity and version policy.** Select an initial
  PEP 440 version, preferably an alpha while the contract is settling. Record
  API/deprecation policy and compatibility expectations for schema YAML,
  plan JSON/chunk messages, DLQ envelopes, parked-pair identities, and consumer
  groups. Prerelease users need an explicit version or `pip install --pre`.
- [x] **S4 — Define supported environments.** Start with Python 3.12 and Linux
  loading/orchestration unless additional platforms are tested. Declare
  the tested Python range in documentation/classifiers and set `requires-python`
  for actual compatibility requirements. Do not add an upper bound merely because
  newer interpreters are untested; justify any upper bound with incompatibility. Test macOS/Windows separately if advertised,
  including Docker Desktop networking and multiprocessing. Document required
  Kafka/Neo4j versions, Enterprise needs, and APOC's classic-mode scope.

D01–D05 and D07 policy were explicitly approved on 2026-10-16; names, public
maintainer/index owner/contact, API/ordering/cancellation contracts, alpha/
format rules, support targets and mandatory dependencies are recorded in the
[decision register](../plan/library-implementation/decisions.md). S4 records
chosen proof targets, not executed support-matrix results. Prebuilt images are
selected; delegated completion records `ghcr.io/shashankshukla96` with Shashank
Shukla responsible for images/releases. All E01 decision stories are complete;
[completion checkpoint review](../plan/library-implementation/reviews/e01-completion-checkpoints.md)
checks the final records. Account access, public visibility/anonymous pull,
actual artifacts/digests and publication remain later gates. Technical approvals
need no repeat request.

## 2. Build an installable distribution

These tasks implement the standard metadata described in the
[PyPA pyproject guide](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/).

- [ ] **P1 — Select a build backend and package layout.** Use a maintained
  backend such as setuptools or Hatchling. Prefer
  `src/neo4j_graph_loader/` with explicit package discovery. Move current `src`
  code under that namespace and update imports, tests, subprocess module
  commands, Docker entry points, and examples together. Decide whether any
  temporary `src` compatibility shim is justified.
- [ ] **P2 — Add distribution metadata.** Add `[build-system]` and `[project]`
  with name, description, README/content type, supported Python range,
  dependencies, useful classifiers/keywords, and repository/docs/issues URLs.
  Set the SPDX expression `Apache-2.0` and include `LICENSE` via backend-supported
  `license-files`; verify emitted metadata rather than assuming inclusion.
- [ ] **P3 — Use one version source.** Choose a maintained version file or
  SCM-based versioning with an sdist-safe fallback. Ensure wheel metadata,
  package version, CLI `--version`, metrics and image versions agree.
  With SCM versioning, create the local candidate version tag at the clean
  release commit before building; verify no unintended development/local
  suffix. Reuse that tag and those artifacts through rehearsal and production,
  publishing the tag after release approval. See the
  [setuptools-scm versioning guide](https://setuptools-scm.readthedocs.io/en/latest/usage/).
- [ ] **P4 — Declare the installed CLI.** Add `[project.scripts]` for an agreed
  executable (candidate: `graph-loader`) targeting a no-argument entry function.
  Support module invocation if documented. Check name collisions and preserve
  command behaviour and exit codes.
- [ ] **P5 — Control artifact contents.** Include all required package modules,
  license and runtime resources. Explicitly exclude planning/review documents,
  authoring workflows, `.env`, `var/`, datasets, benchmark output and private
  operational files from wheel and sdist. Decide which fixtures/tests belong
  in the sdist; keep installation independent of example datasets.
- [ ] **P6 — Package required resources.** Use `importlib.resources` for any
  bundled sample schemas or templates. Provide explicit file paths for user
  schemas. Verify behaviour when resources are read-only and when launched
  outside the checkout; installing a wheel must not require a root `config/`.

## 3. Make the API safe to embed

- [ ] **A1 — Publish a small explicit API.** Export the approved schema loader,
  schema/settings models, source protocol and planner functions from documented
  modules. Define supported exceptions, returned results and input types.
  Keep scheduling workers, test hooks and CLI internals private.
- [ ] **A2 — Define lifecycle and cleanup.** Document planner `start`/`finish`,
  resource ownership, timeouts, cancellation and cleanup after failure. Include
  B1 regression cases and distinguish caller-owned topics from topics created
  by a convenience call. Offer
  context-managed lifecycle where useful. For any public loader/fleet API,
  specify who owns drivers, clients, workers, containers and shutdown.
- [ ] **A3 — Separate CLI process control from library calls.** Audit signal
  handler installation, `sys.exit`, logging configuration, environment mutation
  and metrics globals. Public calls should accept explicit configuration and
  cancellation; imports must not connect, launch services, bind ports or change
  caller signal/logging configuration. Preserve CLI supervision intentionally.
- [ ] **A4 — Make filesystem use explicit.** Replace checkout-relative runtime
  assumptions (`var/rejections`, runtime schemas, ledgers, spill/progress files)
  with documented writable directories or caller paths. Avoid writing beside
  installed package files. Specify cleanup and permissions for shared runs.
- [ ] **A5 — Validate multiprocessing as a library.** Test caller-supplied
  sources under the supported start methods, require picklable sources where
  needed, document the `if __name__ == "__main__"` rule, and avoid globally
  changing the application's start method. Address the prior multithreaded
  `fork` warnings rather than claiming full cross-platform support.
- [ ] **A6 — Add installed-package examples.** Provide a small schema-validation
  example and a producer-planning example using public imports and an explicit
  Kafka address. Remove `sys.path` shims and `PROJECT_ROOT` insertion from the
  public path; keep source-development instructions separate.

## 4. Resolve dependencies and Docker distribution

- [x] **D1 — Make package metadata authoritative.** Declare runtime dependencies
  in `pyproject.toml`; generate/check existing requirements files against that
  source so container and library environments cannot drift. Development tools
  and IMDb dependencies must not become mandatory runtime dependencies.
- [ ] **D2 — Choose optional extras using actual import paths.** Consider
  `docker`, `rust` and example tooling extras. Keep NumPy, Pydantic, YAML,
  Neo4j/Kafka and Prometheus dependencies in base unless their APIs are truly
  separated. If Docker or metrics becomes optional, add lazy imports and test
  the base package without that extra; labels alone do not remove dependencies.
- [ ] **D3 — Validate Rust acceleration and dependency bounds.** Decide whether
  `neo4j-rust-ext` remains mandatory or is opt-in. Preserve its matching Neo4j
  driver constraint and the current `<7` fence assumption. Test supported
  architectures/interpreters, unavailable-wheel behaviour, pure-driver fallback
  if offered, minimum supported versions and a current dependency resolution.
- [ ] **D4 — Choose a checkout-free deployment path.** Recommended: publish
  versioned loader images and let the installed CLI select matching images.
  Alternatively, bundle a tested image-build context. Resolve
  `DockerService.build_*` currently building from `.`; no implicit dependence
  on local Dockerfiles. Define registry ownership, release tags/digests and the
  package/image compatibility contract.
- [ ] **D5 — Test Docker filesystem and networking contracts.** Cover schema
  mounts, rejection output, plan/ledger/spill paths, host/container connection
  addresses and lease/dispatcher reachability. Document remote-daemon path
  requirements and Docker Desktop limits. `--skip-image-build` must work from
  an installed package with available images.
- [ ] **D6 — Keep infrastructure separate from pip installation.** Installing
  the library does not install/start Neo4j, Kafka, Docker or Compose. Document
  how to supply these services and validate credentials/database selection;
  preserve the existing explicit CLI environment and `.env` behaviour.

## 5. Prove the built artifacts work

Use the [PyPA build guide](https://packaging.python.org/en/latest/tutorials/packaging-projects/)
for wheel/sdist production. Checkout tests do not prove either artifact works.

- [ ] **V1 — Build and inspect a wheel and sdist.** From a clean release
  checkout, run isolated `python -m build`, then `python -m twine check --strict`
  on the exact outputs. Inspect archive inventories, dependency metadata,
  entry points, license, version and README rendering. Scan shipped contents
  for credentials/private files.
- [ ] **V2 — Test wheel installation away from the checkout.** In a fresh
  environment with `PYTHONPATH` unset and an unrelated working directory,
  install the wheel, run `pip check`, public imports, CLI help/version, schema
  validation and an approved API example. Verify imported paths are inside
  that environment and no source checkout supplies imports or data.
- [ ] **V3 — Rebuild and test the sdist.** Extract into a clean directory,
  build its wheel in isolation, install and repeat V2. No `.git`, original
  workspace, unlisted file or absolute developer path may be required.
- [ ] **V4 — Test the support/dependency matrix.** Repeat base and extras
  installations across every advertised Python/OS/architecture combination;
  test minimum and current dependencies. Do not label the package OS-independent
  if the advertised APIs need unsupported platform facilities.
- [ ] **V5 — Run installed-code regression tests.** Retain meaningful schema,
  planner determinism/spilling, chunk encoding, multi-type/topic/pair ordering,
  lease/fencing, DLQ/retry, offset safety, shutdown and compatibility coverage.
  Run tests against the installed artifact without `pythonpath=["."]` masking
  packaging omissions. Also neutralize test/example `sys.path` insertions and
  checkout-based subprocess working directories. Run from isolated test/fixture
  directories without application source; assert imports resolve to the
  installed environment in both test and child processes. Editable installs
  are a separate development check.
- [ ] **V6 — Run real service integration tests.** Use throwaway Kafka/Neo4j
  services and the release images. Verify bulk planned, streaming leases and
  classic if included: final graph, offsets, failure/restart behaviour, DLQ
  recovery and lock-wait checks. Record passes and skips; a skipped Docker run
  is not proof. Keep developer-stack tests confined to named scratch databases.
  B3 moves executable datasets out of removed planning directories before
  refactoring. Prove the affected multi-topic DLQ integration test runs using
  `tests/fixtures/multi_topic_arrival.py` without restoring development records.
- [ ] **V7 — Decide how retry ordering affects release scope.** Resolution
  memory is lost on restart, DLQ partition revocation and LRU eviction (100,000
  entries); offsets from different source partitions/topics are not comparable.
  `arrival` and other non-`order_by` retries can apply older values after newer
  ones. Decide the release contract during S2/S3. Fix and test each supported
  guarantee, or explicitly restrict support and document `order_by` protection
  and its boundaries, including replicas, replay and topic recreation. Durable
  memory alone does not resolve all ordering races. Cover restart, rebalance,
  eviction and cross-topic cases; no unqualified exactly-once/restart-safe claim.

## 6. Prepare public documentation

- [ ] **U1 — Write an installation/API quick start.** Show `pip install`,
  relevant extras, public imports and the installed executable. Specify service
  prerequisites, a runnable small fixture, expected output and cleanup.
- [ ] **U2 — Make the PyPI description self-contained.** Replace relative
  documentation/image links with published absolute URLs in the PyPI README,
  or use a dedicated package README. Follow the
  [PyPA README guide](https://packaging.python.org/en/latest/guides/making-a-pypi-friendly-readme/).
- [ ] **U3 — Describe guarantees precisely.** Explain coordinated concurrency
  across relationship types, modes and ordering rules, external writers,
  supported Neo4j isolation assumptions, missing endpoints, DLQ retry limits,
  resource costs and measurable performance scope.
- [ ] **U4 — Write release and migration notes.** Replace `Unreleased` with
  the approved version/date and document namespace/CLI migration, config
  changes, legacy clock deprecation and image upgrades. Keep development plans
  on this branch rather than in public artifacts or `main`.
- [ ] **U5 — Publish support information.** Provide issue reporting,
  contribution and security contact/process, license/attribution information,
  compatibility policy and a minimal bug-reproduction template.

## 7. Rehearse on TestPyPI

TestPyPI is a separate service with separate accounts and dependencies; follow
the [official TestPyPI guide](https://packaging.python.org/en/latest/guides/using-testpypi/).

- [ ] **T1 — Establish release ownership.** Verify PyPI and TestPyPI account
  access, email verification, required account security, chosen project name
  and authorized maintainers. Record who is responsible for each upload.
- [x] **T2 — Choose the publication mechanism.** Default for the existing
  no-workflows policy: manual Twine uploads from a controlled release
  environment using appropriate API tokens kept outside the repository.
  Automated Trusted Publishing is an optional future choice; it requires a
  deliberate change to that policy, not a workflow silently added to `main`.
- **T3 — If automation is selected, configure it fully.** **N/A: manual publication approved on 2026-10-16.** Register the
  correct repository owner/name, workflow filename and protected environment
  with each index, including a pending publisher for a new project. Use a
  separate publish job with `id-token: write`, reviewed release triggers and
  immutable tested artifacts. See
  [PyPI Trusted Publishing](https://docs.pypi.org/trusted-publishers/) and
  [publisher configuration](https://docs.pypi.org/trusted-publishers/adding-a-publisher/).
  If manual publication is selected, record this item as not applicable.
- [ ] **T4 — Upload the exact candidate artifacts.** Record version, Git
  commit/tag, image versions, wheel/sdist filenames, SHA-256 hashes and all
  gate results. Upload those files to TestPyPI; do not glob a stale `dist/`.
- [ ] **T5 — Verify retrieval from TestPyPI.** In a clean environment,
  install the candidate with an explicit version and TestPyPI index using
  `--no-deps`. Install the already-reviewed dependency set from normal PyPI
  separately, then run `pip check` and V2/V6 checks. Do not combine indexes
  using `--extra-index-url`; a no-deps install by itself is not full validation.
- [ ] **T6 — Inspect the project page.** Check rendered README, license,
  links, Python support, maintainers, downloaded files and hashes. Record any
  fixes, issue a new version if needed, and repeat the failed checks.

## 8. First production release and maintenance

- [ ] **R1 — Complete the release gate.** All B/S/P/A/D/V/U items and applicable
  T items have evidence; remaining exclusions/limits are stated explicitly.
  The user reviews the completed implementation and authorizes publication.
  This checklist's review alone does not authorize an upload.
- [ ] **R2 — Publish the tested version to PyPI.** Create the release tag at
  the tested commit if using file-based versioning; with SCM versioning, verify
  and publish the existing candidate tag created before building (P3).
  Ensure compatible images are available, and upload the
  same verified wheel/sdist bytes used in rehearsal. If anything is rebuilt
  or changed, repeat artifact validation before uploading.
- [ ] **R3 — Verify the public installation.** Download from PyPI into a fresh
  environment with the exact version, run `pip check`, imports/API/CLI smoke
  checks and a small real load. Confirm page metadata and artifact hashes.
- [ ] **R4 — Record the release.** Publish release notes, compatibility/image
  references, test results and maintainer contact. Update installation docs
  to reference the actual released version.
- [ ] **R5 — Define recovery and repeatability.** Document yanking a broken
  release and publishing a new version rather than replacing uploaded files;
  preserve user data and avoid destructive automatic migrations. Keep an
  actionable release procedure, dependency update process and regression gate.

## Suggested implementation order and review decisions

1. Agree S1–S4 (including the V7 ordering decision) and D2/D4: identity, public scope, support range, extras and
   image distribution determine the rest of the work.
2. Complete B1–B3 and record the pre-migration test baseline.
3. Implement packaging, namespace migration and CLI/resource fixes (P1–P6).
4. Stabilize the API and dependency/deployment contracts (A1–A6, D1–D6).
5. Add installed-artifact checks, integration evidence and public docs
   (V1–V7, U1–U5).
6. Rehearse on TestPyPI and review the concrete candidate (T1–T6).
7. Publish only after the release gate and publication instruction (R1–R5).

Reviewer decisions: package/import names; planner plus CLI versus a broader
embedded API; alpha maturity; supported Python/platform range; Docker/Rust
extras; prebuilt images versus bundled build context; manual versus automated
publication; treatment of the retry-restart limitation. None blocks writing
this checklist, but all must be resolved before claiming release readiness.
