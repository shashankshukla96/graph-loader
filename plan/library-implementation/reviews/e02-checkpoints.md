# E02 independent checkpoint reviews

Date: 2026-10-16. Reviewer started without inherited conversation history and
received the bounded E02 assignment and accepted E00/E01 identities from the
parent. This report is the reviewer's only repository edit. The dirty checkout
is preserved. No push, publication, installed CLI, namespace migration or Docker
delivery implementation is authorized by these reviews.

## Preflight — inspection before E02 implementation

**Inspection only; no implementation approval.** No applicable AGENTS.md was
found in repository/ancestor paths. I read canonical E02 stories from both
`backlog.json` and `epics/e02.md`, the E02 handoff, E00 execution evidence,
E01 decisions/approval hashes/completion review, source metrics, current build
configuration, requirements, all four Dockerfiles and relevant test seams.

Concrete checks required at the story checkpoints:

- Explicit package discovery must ship only `neo4j_graph_loader` scaffold;
  legacy `src` is already a package and must stay excluded from the wheel.
- Clean import must run away from the checkout with no editable installation
  or legacy module/logging/service imports. Extracted-sdist rebuilding must
  retain the same file-based version without Git.
- Existing checkout imports use the repository root; a new package under
  `src/neo4j_graph_loader` is not automatically top-level importable there.
  Metrics must remain usable without installing the scaffold first.
- Existing metrics reads `GRAPH_LOADER_VERSION` once at import with default
  `dev`; `docker_service` forwards nonempty overrides. Default/override label
  evidence and CLI-reference documentation must agree with the new authority.
  All current Dockerfiles copy `src`, but install only `requirements.txt`.
- Exactly eight current runtime dependency bounds must be retained, including
  mandatory `neo4j-rust-ext>=5.20,<7` paired with `neo4j>=5.20,<7`. Resolver
  output must demonstrate the extension's exact driver-version dependency.
  Development tools and IMDb requirements remain outside base metadata.
- Root README describes the full checkout loader. Artifact-facing text must
  clearly describe the development-only scaffold and absent installed API/CLI.
- E00's capture tool has a deliberately narrow untracked allowlist. New E02
  scaffold/build/helper/test inputs need explicit identity coverage; changed
  packaging/source inputs cannot inherit unchanged E00 source hashes.

The parent received these risks before implementation review. Awaiting
E02.F1.S1 evidence and concrete artifacts.

## Checkpoint 1 — E02.F1.S1 backend metadata and namespace scaffold

**APPROVED. No actionable correction remains for this story.** This accepts
only the development scaffold; dependencies are the next story and no installed
loader/API/CLI, platform support or publication gate is satisfied here.

I inspected the actual pyproject additions, manifest, package README, two
scaffold modules, build/development requirements, capture-tool changes, evidence
JSON and archive bytes. Explicit setuptools selection includes only the new
package. The literal file version is `0.1.0a1.dev0`; the separately recorded
release target remains `0.1.0a1`. No legacy source or application-resource
migration occurred. Metadata carries the approved distribution/maintainer,
Python >=3.12, Apache-2.0 SPDX/license file and project links, without OS-support
classifiers or entry points. The artifact README is explicit about its absent
installed capabilities and forbidden upload at this stage.

Independent verification:

- Recomputed and compared all 229 source entries and environment contents to
  `e02-s1-source.json`; digests match. Against E01's 224 entries, only
  `pyproject.toml` and `requirements-dev.txt` changed, with exactly the five
  expected new scaffold/build inputs. All inherited prerequisite inputs remain
  byte-identical and the repository environment remains unchanged.
- Verified both submitted archive sizes/SHA-256 values against their bytes.
- Read and reran `verify_e02_scaffold.py` against the submitted artifacts into
  an owned temporary report. Exact wheel contents, safe sdist inventory,
  metadata/README/license/version and no-entry-point assertions passed. Clean
  noneditable installation imported from an unrelated temporary directory with
  Python isolated mode and socket/subprocess/thread side-effect traps.
- Independently ran default isolated `python -m build` into
  `/tmp/graph-loader-e02-s1-review-20261016`; this built an sdist and its wheel
  successfully. Repeated the inventory/metadata/clean-import probe on those
  independently built artifacts: pass. The rebuilt wheel's version agrees with
  source despite its extracted-sdist build having no Git dependency.
- Independently reran strict Twine checks on both submitted archives: pass.
  Read the actual focused checkout log: 417 passed in 3.38 seconds.

The four absent `requires.txt` manifest warnings are the disclosed intermediate
state before S2 defines runtime metadata. They do not hide metadata/license
errors; S2 must remove them. A fresh full checkout baseline is still required
after S3 changes the runtime version consumer. No service rerun is justified by
this metadata/scaffold-only checkpoint.

## Checkpoint 2 — E02.F1.S2 dependency authority and parity

**APPROVED. No actionable correction remains for this story.** This accepts
the current native resolver/base-install development evidence, not E08's
minimum-pair, Linux or multi-platform support matrix.

I inspected the actual authoritative dependency array, generated requirements,
stdlib-only generator, negative-control tests, Makefile parity prerequisite,
developer docs, modified scaffold verifier, archive metadata and source/
resolution evidence. All eight strings exactly match the pre-E02 requirements
read directly from Git HEAD, as well as the recorded original vector. No bound
or mandatory dependency changed; no extras were introduced. Existing Dockerfiles
still consume the same generated requirements input. The new Makefile step
checks it without implementing future package/image delivery. IMDb, testcontainers
and developer/build tools remain outside base metadata.

Independent verification:

- All 232 source entries and environment contents/digests match
  `e02-s2-source.json`; the repository environment retains the accepted E00/E01
  SHA-256. Both archive sizes/SHA-256 values match their recorded bytes.
- Reran the three parity tests: **3 passed in 0.23 seconds**, including changed
  metadata detection, non-mutating failure, regeneration and missing-file
  failure. Independently ran `make check-requirements`: pass.
- Inspected and reran the scaffold metadata/inventory/isolated-import probe on
  S2 archives: pass. Its PEP 508 comparison correctly tolerates setuptools'
  reordered version constraints. Strict Twine checks pass for wheel and sdist.
  The actual isolated-build log reports success and contains no warning/error;
  S1's intermediate missing `requires.txt` warning is resolved.
- Read the actual pip dry-run JSON, checked every selected direct dependency
  against its unchanged bounds, evaluated extension markers with no extras,
  and verified the active `neo4j==6.4.1` dependency. Marker-conditional optional
  extension extras do not enter base. Neo4j 6.4.1 and Rust extension 6.4.1.0
  satisfy the required exact driver relationship and both remain below 7.
- Independently queried the clean base venv's installed distributions: they
  exactly match the raw resolver report plus pip, and match the safe summary.
  No pytest/testcontainers/IMDb/build/Twine tools leaked into base. Reran that
  environment's `pip check`: no broken requirements.

The parent has not started S3 at this checkpoint. Runtime metrics remains
unchanged, and its forthcoming change still requires a fresh checkout baseline
and version-label proof. No service test or external operation was performed.

## Checkpoint 3 — E02.F2.S1 version consumer, provisional inspection

**No actionable finding in the inspected implementation; acceptance pending
the final fresh full-suite result and unchanged after-manifest.** The parent
explicitly froze final source while that suite runs; this is not yet approval.

The actual legacy metrics change imports the inert version through its checkout
package, so no editable install is required. Default labels use the same literal
version consumed by setuptools. Explicit empty/custom startup overrides retain
existing behavior. Existing nonempty-only Docker forwarding and image-selection
overrides are unchanged. CLI/developer docs distinguish diagnostic labels from
package/image identity, record the E03 relative-import relocation obligation and
retain E11 candidate freeze/E12 tagging. No installed metrics/API/CLI is claimed.

I independently ran the new foundation, metrics and CLI environment tests:
**73 passed in 6.26 seconds**. These include fresh subprocess default/empty/custom
labels through the real public metrics-start path with only HTTP binding mocked,
lazy registry/import isolation and existing container override forwarding.
The actual before-change log contains the expected default-label failure with
five passing tests; the parent's focused final log records 147 passed.

I verified recorded final/rebuilt archive sizes and hashes, plus every named
version-validation log/tool hash. Every member byte of the original and explicit
sdist-rebuilt wheel is identical. I reran archive/metadata/license/dependency/
clean-isolated-import probes on both artifact directories: pass. Independently
captured source/environment still exactly match `e02-final-before.json`:
`4e293357a858209d250d91818b732030a5951784f2dc468495d2c6ad21858f2b` and
`048a4d2d18ada968b28398105c6086473679ae25eb88c7a6e5d175dad965bf89`.

The earlier full run superseded before the final docstring change is explicitly
excluded. Final story acceptance follows only after the parent's fresh full run
and after-capture evidence arrive and are independently inspected.

## Checkpoint 3 final — E02.F2.S1 and bounded E02 implementation

**APPROVED. All three E02 implementation stories satisfy their bounded
acceptance criteria; no actionable implementation finding remains.** This
accepts the explicitly incomplete development distribution and checkout version
integration. It does not approve installed loader/API/CLI support, the Linux
platform matrix, future Docker delivery, candidate publication or Git pushes.
The parent's status/handoff bookkeeping and requested fresh independent final
adversarial review still follow this implementation checkpoint.

I read the final S3 evidence and safe results, and inspected the actual final
full-suite log bytes. It reports **3,380 passed, 4 skipped, 49 deselected,
19 warnings in 172.12 seconds**. The exact six added foundation cases account
for the increase from E00's accepted 3,374 tests. The parent's focused final log
reports **147 passed in 6.77 seconds**; my independent 73-case rerun and final
artifact/import checks are recorded above. All named final result log sizes
and SHA-256 values independently match their bytes. The superseded earlier
full run is excluded from acceptance.

All four skips match their source conditions. I reread those test sites,
confirmed sh/bash resolve differently, and independently confirmed absent Git
objects 8748274/239deb4. The multi-pair example is deliberately outside its
single-pair invariant. The two historical guards' "not a git checkout" message
is misleading but remains explained accurately, not counted as a runtime pass.
The actual warning summary records 15 IMDb subset plus four planned-edge
multithreaded-fork deprecations, retained as A5/E08 work. The unit producer's
localhost refusal remains visible. No smoke/integration result is claimed.

I compared final before/after manifests and captured the checkout independently
again. All 232 input entries, environment contents, source/environment digests
and both staged/unstaged diff hashes agree with before/after evidence. Source:
`4e293357a858209d250d91818b732030a5951784f2dc468495d2c6ad21858f2b`;
environment:
`048a4d2d18ada968b28398105c6086473679ae25eb88c7a6e5d175dad965bf89`.
The repository environment remains unchanged from accepted prerequisites.

Independently reran Ruff on source/tests/examples/generator, repository and
clean-base `pip check`, requirements parity and whitespace validation: all pass.
Earlier checkpoint verification already established safe inventories, unchanged
eight dependency bounds, current matched Neo4j/Rust resolution, no developer/
IMDb leakage, clean installed version imports and identical original versus
explicit-sdist-rebuilt wheel member bytes. Those artifact/source inputs have
not changed. No full-suite or live-service rerun was needed merely to restate
the now byte-verified final results.

Only this review report was edited by the reviewer. Dirty prerequisite work
and all later-epic/publication boundaries remain preserved.
