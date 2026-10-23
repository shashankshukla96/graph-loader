# Independent E06.F1.S1 review — 2026-10-22

**Final disposition: accepted for E06.F1.S1 after repair and independent re-review.**

Initial disposition was changes required. Original R1 is retained below; its
repair and verification are recorded at the end. No actionable S1 finding remains.

This review covers only candidate component construction and identity (E06.F1.S1).
It does not accept S2/S3, supported topology, installed orchestration or E06
completion. Reviewed execution-e06.md, e06-plan-review.md, canonical E06 and D06,
all four Dockerfile changes, scripts/build_candidate_images.py,
tests/test_candidate_images.py, capture_baseline.py's allowlist, the story evidence
and image manifest, and raw s1-* build/check/test records.

## Original R1 — copied build inputs missing from image identity (P2, repaired)

`scripts/build_candidate_images.py:37–43` hashes only package `*.py`, the direct
package data YAMLs and direct `config/*.yaml`, but the Dockerfiles copy whole
`src/` and `config/` directories. The current images demonstrably contain
`/app/config/neo4j/server-logs.xml`, whose bytes are absent from `input_sha256`.
The manifest also omits copied `config/.gitkeep` and the six present
`src/neo4j_graph_loader.egg-info/` metadata files. This is an actual closure gap,
not a hypothetical future extension: changing the XML changes the CLI/node/edge
image filesystem while leaving every recorded input hash unchanged.

Independent read-only runs of the CLI/node/edge immutable IDs obtained XML hash
`d2375b2e25222138064c463d33293c6a05f01051c7af1dad42d16e21594735d6`, matching the
host's tracked file. That tracked file is absent from both the evidence manifest
and builder enumeration. All currently recorded input hashes otherwise match.

Repair by making the deliberate COPY boundary and hashed input boundary coincide:
either safely enumerate every included copied file, with explicit exclusions and
symlink/secret policy, or narrow the copied trees so omitted generated/auxiliary
files cannot enter images. Add a focused closure regression for a nested config
file and generated/auxiliary source input. Rebuild/refresh the candidate evidence
with the repaired boundary, preserving the original logs, then re-review. The
builder itself should appear in fresh input evidence as its current code intends.

## Checks that passed

- Independently reran the focused suite in the new E06 development environment:
  `5 passed in 0.08s`.
- Inspected all four actual immutable image IDs from e06-s1-images.json. IDs,
  local tags, linux/arm64 platform, labels and module entrypoints match the
  manifest. Names use the approved GHCR namespace and version authority
  `0.1.0a1.dev0`; proposed unsuffixed release tags remain proposals.
- Independently ran Python by each immutable ID, read-only, with network disabled,
  working directory `/`, no bind mounts and only `/tmp` writable. Every image
  reports installed metadata version `0.1.0a1.dev0` and the package origin under
  `/usr/local/lib/python3.12/site-packages/`.
- Raw four-image build logs and help checks show successful builds/component help.
  The wrong-version raw Docker build reaches the installed-version assertion and
  fails there with `image/package version mismatch` (exit 1), rather than merely
  failing a tag or label check.
- Independently reran the builder with the existing S1 suffix and a new owned
  output path. It exits 1 before building, refusing the existing CLI candidate
  tag. The new `var/e06-validation-20261022/s1-review-overwrite-attempt/`
  directory is empty; original image IDs and proofs were preserved.
- The build commands perform local builds/inspection only. There is no push,
  release-tag overwrite or invented registry publication digest in the evidence.
  Docker reports RepoDigests locally, but this alone is not remote publication
  proof; the evidence correctly retains `registry_digest: null`.
- Baseline capture's additions are exact E06 input names and retain the prior
  private `.env` exclusion. No runtime source/API change is introduced by S1.

Only this report and the empty owned overwrite-test output directory were added
by the reviewer. No product files or original proofs were modified. No private
`.env` access, remote IMDb access, registry publication, foreign resource cleanup,
commit or push occurred.

## Repair re-review and final acceptance

The author repaired R1 by narrowing every Dockerfile to
`COPY src/neo4j_graph_loader/ src/neo4j_graph_loader/` and introducing
`build_inputs()`, which recursively hashes the complete copied package/config
file trees, including nested XML and `.gitkeep`. Generated host egg-info is no
longer copied; pip can still create its own build metadata in the image. The
builder rejects symlinks/private environment names before input hashing and
excludes Docker-ignored bytecode. A focused closure/symlink regression was added.

Independent re-review reproduced `6 passed in 0.07s`. The new evidence
`e06-s1b-images.json` is byte-identical to raw `s1b-images/images.json`; its input
map exactly equals a fresh invocation of the current builder's `build_inputs()`.
This includes nested XML, `.gitkeep` and the builder's own bytes.

Inspected all four new immutable IDs and checked actual IDs, local tags, platform,
labels and module entrypoints against the new manifest. Independently reran
installed metadata/origin probes by all four new IDs with read-only roots, no
network, no source mounts and working directory `/`: all exit 0, version is
`0.1.0a1.dev0`, and origin is installed site-packages. The raw new
`s1b-checks.json` records all four read-only component-help runs exiting 0.
`s1b-wrong-version.log` shows the fresh negative build installing the package and
then failing the actual installed-version comparison with exit 1. Original S1
images/logs/evidence remain preserved; the new candidates use a new owned suffix.
No publication is recorded or performed.

This satisfies the S1 acceptance boundary. S2/S3, final frozen image builds,
installed service/topology proof and final E06 adversarial acceptance remain
outside this verdict. No additional actionable finding was found.

Reviewed repair SHA-256 identities:

- `scripts/build_candidate_images.py`: `c8917c041d0788b748581b7d354932694c39f02bbdfc100557d9b980f3186286`
- `tests/test_candidate_images.py`: `3197d48c2d2d8d42416c60fe608c5a211967868c38118b88cdb90c3cf842701a`
- `plan/library-implementation/evidence/e06-s1b-images.json`: `2c5f9abf719774c1e2a4e60815a6773b5beb02ec7e4ff288579edaebebeaf10e`
