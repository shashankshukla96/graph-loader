# E06 execution plan — 2026-10-22

Scope: execute only [E06](epics/e06.md) under D06's approved versioned prebuilt
strategy. User supplied the E06 execution handoff on 2026-10-22. Clean entry is
`a2bc7bb` on `codex/pypi-library-checklist`; E05 repair delivery is
`c35a6045ef39a822dcbcfcfc2c20d6b9c2227a28`. No applicable AGENTS.md was found.
No registry publication, push, upload, workflow or E07–E12 implementation.

## Ordered implementation and independent gates

1. **E06.F1.S1 — candidate identity.** Add a development candidate builder for
   all four existing Dockerfiles. Read the one package version authority; pass
   it as a build argument and verify against the installed version during build.
   Label component, package version and a narrowly defined image contract.
   Construct local candidates with proposed approved GHCR version tags; record
   actual immutable local IDs, labels, entry points, platform and input hashes.
   No invented registry digest, bundled build context or remote publication.
   Independently review the story and repair/retest/re-review findings.
2. **E06.F1.S2 — deterministic selection.** Defaults become
   `ghcr.io/shashankshukla96/graph-loader-{role}:<package-version>`.
   Retain explicit GRAPH_LOADER_{NODE,EDGE,CLOCK}_IMAGE overrides, requiring the
   same package version, expected component and image-contract labels.
   Runtime resolves local images first, pulls only missing images by default,
   and never builds from cwd. The retained --skip-image-build flag means strict
   local-only resolution, including missing-image diagnostics. Resolve once per
   service and pin container launches to immutable IDs, avoiding mutable-tag
   races. Keep former build_* call names as compatibility entry points for
   resolution; reject obsolete explicit Dockerfile builds clearly. Unit controls
   cover overrides, mismatch/missing/daemon/pull errors and no hidden builds.
   Independently review before the next story.
3. **E06.F2.S1 — topology and installed load.** Inspect existing host/container
   address, coordinator and mount behavior before narrowing fixes. Supported
   topology is a local Docker daemon with a user-defined bridge, CLI host access
   to published services/coordinator and distinct container DNS/advertised Kafka
   addresses. Require daemon-visible absolute host paths. Prove real schema,
   rejection, plan/ledger/spill and timing ownership; coordinator/dispatcher
   host and writer addresses; database selection and credential failures with
   secret-free diagnostics. A tiny installed-wheel load runs outside the checkout
   against new owned services with selected immutable images, with graph,
   source offsets and cleanup assertions. Negative controls exercise missing
   network, inaccessible binds/services, Kafka metadata and credentials/database.
   Explicitly document unsupported remote-daemon/loopback topology rather than
   translating or silently assuming shared paths. Independently review.

## Preservation and verification

Never read/source/disclose private .env; preserve all E05 source, evidence and
five public symlink-loop regressions. Preserve public schema/planning ownership,
transport/cancellation bounds, 10-second aggregate cleanup and original R1
failure causes. No root exports beyond version, src alias, extras or version
change. Never contact IMDb 192.168.1.75, delete foreign resources or prune Docker.
Use new E06 tags, environments, logs and explicitly owned services only.

A fresh CPython3.12 development environment installs requirements-dev then the
editable project. Capture safe tracked/relevant-untracked source and environment
identities before and after proof. Extend exact allowlists for E06 files only.
Focused checks accompany each story; preserve failures and reviewer dispositions.
After final product freeze run the full checkout/example suite, lint, requirement
parity, pip check, whitespace, strict isolated wheel/sdist builds, truly outside-
Git sdist rebuild and every-member byte equality; use two clean noneditable
outside-checkout environments. Rebuild all four immutable component images and
prove installed origins/resources/help without source-mount rescue, including
actual read-only runs. Repeat all 12 separately supervised installed/image R1
mixed-source default/finite controls and exact no-skip supervised B3 with new
owned Kafka/Neo4j. Required infrastructure failures/skips remain open.

A fresh zero-history final adversarial reviewer must accept exact final source,
artifacts and evidence after every actionable issue is repaired and rechecked.
Statuses remain incomplete until actual acceptance. Render canonical plan,
validate local links, inspect staged scope, commit locally, then record actual
full delivery SHA and every tested/committed/current byte/size/mode equality in a
documentation-only follow-up. Obtain that follow-up's own SHA from Git history,
without recursive evidence edits. Create a future E07 handoff without executing it.

## Execution dispositions

All three ordered stories received independent acceptance after their recorded
repairs; [resolutions](reviews/e06-resolutions.md) preserve original failures and
rechecks. S3 includes a narrow signal teardown repair required by the inherited
full gate: ignore repeated SIGTERM before asyncio loop closure. A deterministic
real-service red/green regression proves the boundary; attribution of the original
discarded-stderr storm failure remains explicitly limited. The exact repaired
source passed every finalb gate under `var/e06-validation-20261022/finalb/`.
[Fresh final adversarial acceptance](reviews/e06-final-adversarial-review.md) on
2026-10-23 accepts all E06 stories. Local delivery is
`a034420f98a57d17c7591b23eb1011d7bc346d25`; [postcommit equality](evidence/e06-postcommit.json)
confirms all 279 tested, committed and current inputs and unchanged environment. No E07–E12 implementation or publication scope has been added.
