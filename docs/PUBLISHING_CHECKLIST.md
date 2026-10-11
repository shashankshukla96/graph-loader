# GitHub and Python library publishing checklist

Reviewed against the local working tree on **2026-10-11**.

**Verdict:** the source can be published on GitHub after publication hygiene and review of the intended commit. It is not yet ready to advertise as an installable Python library. The implementation has substantial documentation and tests, but packaging, an explicit public API, installation outside the checkout, and release automation need work. Start with an alpha release after the gates below pass; a stable release needs additional correctness and support evidence.

GitHub source hosting, a GitHub Release, and a Python package release are separate deliverables. PyPI is optional: a properly packaged project can first be distributed through a Git tag and GitHub Release artifacts.

Checked boxes mean the artifact exists and was inspected, **not** that its tests passed today. Unchecked boxes are unfinished or unverified.

## Existing foundation

- [x] Schema-driven implementation, record validation, and Neo4j schema initialization.
- [x] Bulk planning, streaming leases, DLQ/retry handling, and monitoring implementations.
- [x] README, architecture, bulk/stream guides, CLI reference, and a changelog.
- [x] IMDb, social, and multi-topic examples.
- [x] Unit, integration, stress, and compatibility tests are present.
- [x] Apache-2.0 license text is present in `LICENSE`.
- [x] `.env` is ignored; `.env.example` is tracked.
- [x] A GitHub `origin` remote is configured. Remote visibility and current hosted contents were not checked.

## Gate A — before publishing the source

- [ ] Review the exact public commit. There are **458 staged file changes** in this checkout; a push of the current HEAD would not include these changes. Commit the intended release tree deliberately.
- [ ] Scan both the proposed tree and Git history for credentials and private information. Review operational notes and organization references in `plan/`; ignoring `.env` does not establish that history or other files are clean. A secret scan has not been run.
- [ ] Remove generated runtime artifacts from tracking. `var/runtime_schemas/` and `var/rotation-e2e/` currently contain tracked generated YAML and an `.smbdelete…` artifact. Keep intentional fixtures under tests and ignore runtime output.
- [ ] Confirm the code and bundled assets are authorized for public distribution. Include attribution or a NOTICE where applicable. A license file alone does not verify provenance.
- [ ] Make the README state the current support scope and alpha status. Keep default examples local and identify the Neo4j Enterprise/APOC requirements by mode.
- [ ] Add contribution and issue-reporting instructions, plus a private route for reporting security problems.

**Source publication gate:** complete these items before making the intended revision public. These tasks do not require packaging the entire application first.

## Gate B — before calling it an installable library

- [ ] Define a named import package, proposed as `graph_loader`, instead of exposing the generic `src` namespace. Update source, examples, tests, and container entry points together. A conventional layout would be `src/graph_loader/`.
- [ ] Add package metadata and build configuration: build backend, name, version, description, supported Python versions, runtime dependencies, license metadata, and project links. **Current `pyproject.toml` has only `[tool.*]` configuration; no `[project]` or `[build-system]`, and no legacy setup files exist.** Follow the [PyPA pyproject guide](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/).
- [ ] Declare package discovery and included resources explicitly. Inspect wheel/source archive contents so config assets are included intentionally and operational data or planning notes are excluded.
- [ ] Separate runtime dependencies from development dependencies. `pytest` and `testcontainers` currently live in `requirements.txt` and are installed into loader images. Declare runtime dependencies in package metadata; use development dependencies or extras for testing. Evaluate optional acceleration such as `neo4j-rust-ext` on supported platforms.
- [ ] Expose a small documented public API. Candidates already implemented include schema loading, schema initialization, record normalization, and `PlanSettings` / `plan_and_publish_edges`. Specify lifecycle, resource ownership, errors, and which imports are internal. A large new facade is not required for the first release.
- [ ] Remove library imports of CLI helpers and import-time logging configuration. Node and edge loaders import `get_neo4j_credentials` from `src.cli`; that module calls `logging.basicConfig()` on import. Move shared configuration into a library module and let callers configure logging.
- [ ] Define explicit connection/configuration inputs for the public API. Document environment-variable fallbacks, database selection, and required credentials; importing the package should not launch services or alter the caller's logging.
- [ ] Add the console entry point, proposed as `graph-loader = graph_loader.cli:main`. Verify it works after installation. Document which commands need Docker and which APIs work without Docker.
- [ ] Make the CLI usable outside the repository directory. Image builders use `path="."` with repository-relative Dockerfiles, and the default config is `config/graph_schema.yaml`. Choose versioned prebuilt images or deliberately distributed build resources; support explicit config/output paths without requiring a checkout.
- [ ] Build a wheel and source distribution, validate metadata, and install each into a fresh environment. Verify imports and CLI help from an unrelated directory without `PYTHONPATH` or pytest's `pythonpath = ["."]` workaround. See the [PyPA packaging tutorial](https://packaging.python.org/en/latest/tutorials/packaging-projects/).
- [ ] Add a consumer-facing example using only the public package and documented API. Run it against isolated Kafka/Neo4j and compare the resulting graph exactly.

**Library publication gate:** all Gate B items pass on the chosen supported environment. Python 3.12 on Linux is a reasonable initial scope based on the repo's documented setup; it still needs installation and end-to-end validation.

## Gate C — first tagged alpha release

- [ ] Restore or add `.github/workflows/` workflows. This directory is absent in the checkout, despite README/usage documentation describing CI workflows. The `workflows/` directory contains development-process documents, not GitHub Actions.
- [ ] Run lint, the unit suite, package builds, and installed-package checks against the exact release commit. Run integration tests in isolated infrastructure; make unexpectedly skipped integration checks visible.
- [ ] Test the advertised Docker images and package together. If prebuilt images are the installation strategy, publish versioned image tags tied to the release commit and avoid relying on mutable `latest` for reproducibility.
- [ ] Declare supported Python, OS, Kafka, Neo4j, and driver versions. Resolve the mixed Python signals: `.python-version` and node/edge images use 3.12, while lint and the clock image target 3.11. Either test both or narrow support.
- [ ] Fix Docker Desktop reachability or explicitly limit the affected orchestration path to Linux. `docs/USAGE.md` documents a streaming lease readiness probe that relies on host access to container IPs and can fail on macOS/Windows.
- [ ] Add a versioned changelog entry, compatibility/deprecation policy, and an alpha tag such as `v0.1.0a1`. No local Git tags were present when inspected.
- [ ] Create a GitHub Release with wheel/source archives, install instructions, supported environments, and known limits. Optionally add PyPI publication later; publishing source to GitHub does not require PyPI.

## Gate D — before a stable release

- [ ] Decide and test the supported relationship ordering contract. The docs acknowledge cross-replica races in the parked-pair index and stale replay after retry-consumer restart, rebalance, or memory eviction. Use a documented restricted contract or durable resolution tracking; do not advertise unconditional order safety.
- [ ] Document and test at-least-once delivery, `MERGE` replay behavior, PATCH property updates, and the limits of bulk verification. Rejected malformed records can coexist with a successful load; there is no atomic Kafka/Neo4j commit.
- [ ] Verify restart, outage, rebalance, shutdown, DLQ replay, repeated bulk batches, and loads into an existing graph on the declared support matrix.
- [ ] State contention-avoidance assumptions: coordinated fleet writes, endpoint/index requirements, transaction fencing, and external-writer limitations. Describe behavior under competing writes.
- [ ] Check schema/DLQ migration and deprecation behavior. Turn “removed after one release” into a named version policy.
- [ ] Reconcile historical reports with current code and publish reproducible benchmark methodology. Do not treat old reported test counts or performance numbers as validation of the release commit.

## Suggested order

1. Publication hygiene and intended commit review (Gate A).
2. Package namespace, metadata, dependency split, and smallest public API.
3. Installation outside the checkout, including the container/image strategy.
4. CI, clean-environment validation, and supported-platform declaration.
5. Tagged alpha release; resolve stable ordering/support commitments before promoting it.

## Validation performed for this assessment

Inspected packaging/configuration files, imports, image-build paths, tracked files, tests, documentation, and local Git tags. The current Python 3.12 interpreter has several runtime/test dependencies, but is missing `prometheus_client`, `ruff`, `build`, and `twine`. No dependency installation, full test run, package build, live service run, history secret scan, remote publication, or remote CI verification was performed. Historical success reports are supporting context only.
