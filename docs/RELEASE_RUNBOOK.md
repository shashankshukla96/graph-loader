# Release and recovery runbook (maintainers)

For the person publishing `neo4j-graph-loader`. It is written to be followed
step by step. Users who need to recover from a
problem start with [Migration](MIGRATION.md#7-if-something-goes-wrong).

Every command below names its package, version and image explicitly. The first
public release is **`neo4j-graph-loader` `0.1.0a2`** (`0.1.0a2` was frozen but never published); for any later release replace
`0.1.0a2` consistently, including in the image tags. Values that exist only after
a step has run (file hashes, image digests, commit) are shown as `<...>` and are
taken from the frozen manifest, never typed from memory. Nothing here is a
workflow or automation: every upload is a manual action that needs a specific,
fresh instruction from the release owner.

## 1. Targets, roles and contacts

| Item | Value |
| --- | --- |
| Project, import, command | `neo4j-graph-loader`, `neo4j_graph_loader`, `graph-loader` |
| Version | `0.1.0a2` (file authority: `src/neo4j_graph_loader/_version.py`) |
| Files | `neo4j_graph_loader-0.1.0a2-py3-none-any.whl`, `neo4j_graph_loader-0.1.0a2.tar.gz` |
| Indexes | TestPyPI (`https://test.pypi.org/`) for the rehearsal, PyPI (`https://pypi.org/`) for the release; separate accounts and tokens |
| Images | `ghcr.io/neburyx/graph-loader-cli:0.1.0a2`, `...-node:0.1.0a2`, `...-edge:0.1.0a2`, `...-clock:0.1.0a2` (public, anonymous pull) |
| Release owner, index owner and owner of the `Neburyx` organization (images and repository) | Shashank Shukla, `shashankshukla96@gmail.com` |
| Maintainers | a single maintainer, Shashank Shukla; there is no delegate, so only the owner can publish, yank or answer a security report |
| Source repository | `https://github.com/Neburyx/graph-loader`; release tag `v0.1.0a2` on the validated public snapshot commit |

Credentials: use a project-scoped API token per index, kept outside the
repository, shell history and logs. Give `twine` the token through the
`TWINE_USERNAME=__token__` and `TWINE_PASSWORD` environment variables set from a
password manager for the single command, or through a mode-0600 `.pypirc` outside the
repository; never as an argument. Registry pushes use a personal access token
through `docker login ghcr.io --password-stdin` from the password manager. Enable
two-factor authentication on both indexes and the registry account. Never paste
a token into an issue, a log or an evidence file.

## 2. Preconditions

1. The candidate is a clean, reviewed commit; the version file says `0.1.0a2`, the
   changelog heading carries the release date (set on the day the candidate is built
   and frozen; those same files go unchanged to TestPyPI and then PyPI), and
   [Support and limits](../README.md#support-and-limits) matches the evidence.
2. A manifest `MANIFEST.sha256` lists the wheel and sdist SHA-256 hashes, the
   commit, and the eight image IDs (four per architecture), their archive hashes and, after push, the per-architecture and index digests. Artifacts and the
   manifest are kept read-only in one directory that nothing else writes to.
3. All gates have passed on **these exact bytes**: lint and unit tests, installed
   lanes, the platform lanes, the real-service lanes (see section 5), the quick-start
   guide ([QUICKSTART](QUICKSTART.md)), link check and `twine check`.
4. The user has given a specific instruction for each external action (TestPyPI
   upload, image push, PyPI upload, Git tag push). One instruction never covers the next.

## 3. Procedure

Work in a clean directory outside the checkout; use `<dir>` for it.

**3.1 Build and check the candidate (no network side effects).**

```bash
python -m build --outdir <dir>/dist
python -m twine check --strict <dir>/dist/neo4j_graph_loader-0.1.0a2-py3-none-any.whl <dir>/dist/neo4j_graph_loader-0.1.0a2.tar.gz
shasum -a 256 <dir>/dist/neo4j_graph_loader-0.1.0a2-py3-none-any.whl <dir>/dist/neo4j_graph_loader-0.1.0a2.tar.gz
```

Record the hashes in the manifest. The frozen inventory records the hash of the
wheel's `WHEEL` file, which names the build backend (`Generator: setuptools (84.2.1)`
for `0.1.0a2`), so build from a clean `git archive` of the frozen commit with that
backend (for example `PIP_CONSTRAINT=<file containing setuptools==84.2.1>`); a different
`setuptools` changes that hash and the lanes refuse the wheel. Build the four images natively on each architecture (amd64 and arm64) from the frozen
source, save each with `docker save` and record the archive hashes, assert that each carries `org.opencontainers.image.version=0.1.0a2`,
`io.graph-loader.component` and `io.graph-loader.image-contract=1`, and run the
service lanes against those image IDs.

**3.2 Stop on any hash mismatch.** Immediately before every upload and every push,
compare the files with the manifest:

```bash
cd <dir>/dist && shasum -a 256 -c ../MANIFEST.sha256 && ls -1
```

`ls` must list exactly the wheel and the sdist: `shasum -c` does not notice an extra file.

If a hash differs, a file is missing or extra, or a test result belongs to other
bytes: **halt publication**. Do not upload "the same version again". Find out why
the bytes changed, discard the candidate, build a new one, and repeat the affected
gates; if the version number was already used on any index, the next version
number is mandatory (section 4).

**3.3 Rehearse on TestPyPI (after instruction).**

```bash
python -m twine upload --repository testpypi <dir>/dist/neo4j_graph_loader-0.1.0a2-py3-none-any.whl <dir>/dist/neo4j_graph_loader-0.1.0a2.tar.gz
python -m pip download --no-deps --no-cache-dir --index-url https://test.pypi.org/simple/ "neo4j-graph-loader==0.1.0a2" --dest <dir>/retrieved
shasum -a 256 <dir>/retrieved/*
```

The retrieved hashes must equal the manifest. Install the retrieved wheel into a
fresh virtual environment with `--no-deps`, install the reviewed dependency set from
PyPI as a separate step (never `--extra-index-url`), then run `pip check`, the
[quick start](QUICKSTART.md) and a small real load with the published images. Open
the project page and check the README rendering, links, license, Python support,
maintainers and file hashes. A defect means a new version and repeated gates.

**3.4 Push the images (after instruction): one image per architecture, then a multi-arch tag.**
`ghcr.io/neburyx/graph-loader-<component>:0.1.0a2` must be one multi-platform
index that lists the `linux/amd64` and `linux/arm64` images; the two images are built
natively (3.1) and are never merged into one local image. For each component
`<c>` in `cli`, `node`, `edge`, `clock`:

1. Check that each local `rc1` image still has the ID recorded in the manifest (if an
   image is missing, `docker load -i` its archive from the manifest directory and compare
   the loaded ID and the archive's SHA-256 with the manifest), then retag it locally. Nothing is
   rebuilt for the push.

```bash
docker tag ghcr.io/neburyx/graph-loader-<c>:0.1.0a2-rc1-amd64 ghcr.io/neburyx/graph-loader-<c>:0.1.0a2-amd64
docker tag ghcr.io/neburyx/graph-loader-<c>:0.1.0a2-rc1-arm64 ghcr.io/neburyx/graph-loader-<c>:0.1.0a2-arm64
```

   Retag each architecture on the machine that holds its image (or after `docker load`).

2. Push each architecture tag, and record the digest the registry reports for each:

```bash
docker push ghcr.io/neburyx/graph-loader-<c>:0.1.0a2-amd64
docker push ghcr.io/neburyx/graph-loader-<c>:0.1.0a2-arm64
```

3. Create the multi-arch tag from the two recorded digests. It is created once, only after
   both digests are recorded, and never moved afterwards:

```bash
docker buildx imagetools create -t ghcr.io/neburyx/graph-loader-<c>:0.1.0a2 \
  ghcr.io/neburyx/graph-loader-<c>@sha256:<amd64 digest> \
  ghcr.io/neburyx/graph-loader-<c>@sha256:<arm64 digest>
```

   Without `buildx`, `docker manifest create ghcr.io/neburyx/graph-loader-<c>:0.1.0a2
   --amend ...@sha256:<amd64 digest> --amend ...@sha256:<arm64 digest>` followed by `docker manifest push` does
   the same.

4. Verify. The index must list exactly two platforms, and each platform's digest must equal
   the digest recorded in step 2:

```bash
docker buildx imagetools inspect ghcr.io/neburyx/graph-loader-<c>:0.1.0a2
docker buildx imagetools inspect --raw ghcr.io/neburyx/graph-loader-<c>:0.1.0a2
```

Record the index digest and both platform digests for each component in the manifest, then verify an
anonymous pull by digest with an empty Docker configuration so no stored login is used (index
digest, then each platform digest):

```bash
DOCKER_CONFIG="$(mktemp -d)" docker pull ghcr.io/neburyx/graph-loader-<c>@sha256:<index digest>
DOCKER_CONFIG="$(mktemp -d)" docker pull --platform linux/amd64 ghcr.io/neburyx/graph-loader-<c>@sha256:<amd64 digest>
DOCKER_CONFIG="$(mktemp -d)" docker pull --platform linux/arm64 ghcr.io/neburyx/graph-loader-<c>@sha256:<arm64 digest>
```

Then compare each pulled image ID and labels with the local candidate of the same architecture
(IDs from the manifest). Package visibility must be public. Never retag or overwrite `0.1.0a2`,
`0.1.0a2-amd64` or `0.1.0a2-arm64` afterwards.

**3.5 Publish to PyPI (after a separate instruction).** Only after the rehearsal,
the images and the user's review pass, upload the same files, re-checking the
manifest first (3.2):

```bash
python -m twine upload --repository pypi <dir>/dist/neo4j_graph_loader-0.1.0a2-py3-none-any.whl <dir>/dist/neo4j_graph_loader-0.1.0a2.tar.gz
git tag -a v0.1.0a2 <public snapshot commit> -m "neo4j-graph-loader 0.1.0a2"
git rev-parse v0.1.0a2^{commit}
```

Run the tag command in the public repository, on the commit of the validated public
snapshot whose product files equal the frozen wheel and sdist; a tag is never copied from
a development repository. The tag must resolve to that commit. Push it only when the
user authorizes that reference: `git push origin v0.1.0a2`.

**3.6 Verify the public release.** In a fresh environment and unrelated directory:

```bash
python -m pip install --no-cache-dir "neo4j-graph-loader==0.1.0a2"
python -m pip check
graph-loader --version
```

Compare the downloaded file hashes with the manifest, run the quick start and a
small real load, and check the PyPI project page. Record the released URLs and the
digests in the table at the end of this runbook and in the changelog; this must not
change any uploaded file.

## 4. Recovery

**Uploaded files are immutable.** PyPI never accepts a different file under an
existing filename and a deleted version's number cannot be reused. Therefore:

* Never delete a release to "fix" it, and never try to re-upload a file. Publish a
  **new version** (for the first release, `0.1.0a2`), with a changelog entry that
  says what was wrong.
* Keep the retired candidate's artifacts and manifest unchanged for diagnosis.
* A broken **image** is handled the same way: do not retag or replace
  `...:0.1.0a2`; push `...:0.1.0a3` for all four components together. Deleting a
  registry version needs its own instruction.

**Yanking a broken release (only when the owner instructs it).** On the PyPI project
page open Manage, the release `0.1.0a2`, Options, Yank, and give a reason that says what is
wrong and which version fixes it. A yanked release stays downloadable for
requirements that pin it exactly (`neo4j-graph-loader==0.1.0a2`) but is skipped by
ordinary resolution. Then: publish the fixed version (section 3 again, with the
new version), add the changelog entry, and open a pinned issue at
`https://github.com/Neburyx/graph-loader/issues` naming the affected
versions and the fix. A security problem is announced after the fix is available
(see [SECURITY.md](../SECURITY.md)).

**Protecting users' data.** A release must never migrate, rewrite or delete user
data on its own. If a defect could have written wrong data:

1. Tell users first (issue, changelog, yank reason): stop the fleet
   (`graph-loader stop`), do not delete topics, consumer groups, the DLQ or the
   database, and keep the loader's retained run directory.
2. The recovery stays user-driven and non-destructive: planned bulk loads are rerun
   on the same topics (`MERGE` upserts converge), a stopped stream fleet resumes
   from its committed offsets, parked records are replayed with `replay-dlq`.
3. A fix may change a stored format only with the compatibility rules of
   [Migration](MIGRATION.md#4-stored-and-wire-formats) (new format version, old
   versions still decoded, explicit documented steps for the user to run).

## 5. Dependency updates and the regression gate

Review dependency bounds on a schedule and whenever a dependency announces a
security fix. Cadence and owner: the release owner, **at least every three months**.
Process:

1. Edit the minimum and current inputs under `config/support-matrix/` and the
   bounds in `pyproject.toml`; regenerate the pins with
   `python3 scripts/sync_requirements.py` and confirm
   `python3 scripts/sync_requirements.py --check`.
2. Re-resolve the minimum and current sets and run the installed lanes, as
   described in [SUPPORT_MATRIX_VALIDATION](SUPPORT_MATRIX_VALIDATION.md).
3. Run the regression gate below. A changed bound is a new version with a changelog entry.

**Regression gate (every release, on the exact candidate bytes).** The release is
blocked unless all of these pass:

| Gate | Command (run from a checkout of the candidate commit, outputs outside it) |
| --- | --- |
| Lint and unit tests | `make lint` and `make test-unit` |
| Requirements parity | `python3 scripts/sync_requirements.py --check` |
| Installed regression lanes on the wheel | `python3 scripts/verify_installed.py --lane core --wheel <dir>/dist/neo4j_graph_loader-0.1.0a2-py3-none-any.whl --inventory tests/inventory/test-inventory.json --output <new dir>` and the same for lanes `safety`, `cli-runtime`, `public-contracts`, `writer-runtime`, `orchestration-runtime`, `example-contracts`, `service-helper-unit` |
| Real-service lane on the final amd64 images | `python3 scripts/verify_installed.py --lane service-harness --wheel <wheel> --inventory tests/inventory/test-inventory.json --images <images.json for 0.1.0a2> --docker-host unix:///var/run/docker.sock --output <new dir> --timeout 3000` on a Linux host whose Docker you own |
| Arm64 smoke | installed `graph-loader` from the wheel plus the `0.1.0a2` arm64 images, one small real load |
| Documentation | the guides' bash blocks run unchanged in a fresh wheel environment (`docs/QUICKSTART.md`), link check, `twine check --strict` |

Run the lane commands from a controller environment (CPython 3.12) that has
`psutil==7.2.2`, `packaging==26.3` and `docker>=7.1,<8` installed; the target
environment the runner creates is separate. The runner compares the wheel's
metadata and payload with hashes frozen in `test-inventory.json`
(`distribution_files`, `package_payload`). Any change to the wheel's `METADATA`
(README text, description, classifiers, version) or to the packaged source makes it
refuse the wheel with `wheel metadata differs from frozen distribution contract`.
When that is the intended candidate, refreeze deliberately with
`python3 scripts/freeze_installed_inventory.py --inventory tests/inventory/test-inventory.json --collection-output <new dir> --freeze --wheel <wheel> --sdist <sdist>`,
review the diff of the inventory, and commit it with the candidate, before the lanes
run. Never edit the hashes by hand.

Every lane must report all frozen IDs run, with no unexplained skip or exclusion.
A failure blocks the release; do not weaken a test to pass it.

## 6. Records to fill in after the release

| Field | Value |
| --- | --- |
| Release date | `<the date in the changelog heading, set when the candidate was frozen>` |
| PyPI project URL | `https://pypi.org/project/neo4j-graph-loader/0.1.0a2/` |
| Wheel and sdist SHA-256 | `<from MANIFEST.sha256>` |
| Public commit and tag | `<public snapshot commit>`, `v0.1.0a2` |
| Image digests (`cli`, `node`, `edge`, `clock`) | `<sha256 from step 3.4>` |
| Next dependency review | `<date and owner>` |
