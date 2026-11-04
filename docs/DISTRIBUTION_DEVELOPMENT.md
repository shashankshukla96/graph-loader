# Working from a source checkout (contributors)

This page is for developing the library itself. To use the package, install it
from PyPI and follow the [quick start](QUICKSTART.md). The code lives in the
`neo4j_graph_loader` package; the supported modules are `neo4j_graph_loader.schema`
and `neo4j_graph_loader.planning` ([public API guide](LIBRARY_API.md)). The installed
`graph-loader` command and two bundled schema resources work without a checkout.
Private writable run directories are documented in the [CLI guide](CLI_RUNTIME.md);
image selection and local candidate construction are in
[Docker execution](DOCKER_EXECUTION.md). A local build from a working tree
is not a release candidate; only the frozen candidate is uploaded. Publishing is described in the
[release runbook](RELEASE_RUNBOOK.md).

## Fresh checkout development

Use CPython 3.12 and install the application before running tests or examples:

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/python -m pip install -e .
.venv/bin/python -m pip check
.venv/bin/python -m pytest tests examples/imdb/tests -m "not integration and not smoke" -q
.venv/bin/python -m neo4j_graph_loader.cli --help
```

The pytest root-path setting was removed. Example/test helper imports may still
use the repository root, but application imports require the installed package.
Editable installs are development conveniences; artifact validation uses clean
noneditable environments away from the checkout.

## Migration from the old namespace

(For users, see [Migration](MIGRATION.md).) There is no `src` compatibility shim. Replace `from src.models.schema import
GraphSchema` with `from neo4j_graph_loader.schema import GraphSchema`,
and `python -m src.cli` with `python -m neo4j_graph_loader.cli`. The same prefix
change applies to `loader`, `orchestrator` and `utils`, including subprocess and
patch targets. Old pickles that encode `src.*` class identities are not supported;
new process/spawn traffic uses the canonical migrated identities. Serialized
schema, plan/chunk and DLQ formats and Kafka groups/keys retain their behavior.
Use `neo4j_graph_loader.schema` and `neo4j_graph_loader.planning` for the
supported surface; other module interfaces remain internal.

## Dependency authority

`pyproject.toml` `[project].dependencies` is the sole runtime dependency list.
All eight existing dependencies remain mandatory, including the matching
`neo4j-rust-ext`/Neo4j driver below 7; no extras are selected. The extension's
dependency metadata enforces its exact driver version. The declared bounds are
unchanged; minimum and current dependency sets are tested per release (see
[Reproducing the support-matrix observations](SUPPORT_MATRIX_VALIDATION.md)). IMDb tools, testcontainers and build/review tools remain outside base.

```sh
python3 scripts/sync_requirements.py          # regenerate requirements.txt
python3 scripts/sync_requirements.py --check  # non-mutating drift check
```

`make requirements` and `make check-requirements` provide the same operations.
`make build-images` checks parity before invoking Docker. The four Dockerfiles no longer copy the checkout: each one is built from a
**generated build context** (see "Image build files" below) that holds the package wheel and a hashed dependency lock, so a plain
`docker build .` in a checkout does not work. The literal file version needs no SCM fallback. Images resolve application code from
site-packages without PYTHONPATH. See the candidate builder (`scripts/build_candidate_images.py`) for exact compatible labels and fresh local tags: it builds the candidate wheel
and drives the same code as `graph-loader images build`, so a candidate image and a user-built image come from one path. No new workflow exists.

## Build and inspect locally

Use an owned throwaway environment, keeping build tools outside runtime images:

```sh
python3 -m venv /tmp/graph-loader-build-env
/tmp/graph-loader-build-env/bin/python -m pip install -r requirements-build.txt
/tmp/graph-loader-build-env/bin/python -m build --outdir /tmp/graph-loader-artifacts
/tmp/graph-loader-build-env/bin/python -m twine check --strict /tmp/graph-loader-artifacts/<exact-wheel> /tmp/graph-loader-artifacts/<exact-sdist>
```

Use the exact produced filenames; the placeholders above are not literal
arguments. Default `build` constructs the wheel from its isolated sdist. An
explicit second build from extracted sdist checks that no Git/checkout files
are needed. `MANIFEST.in` and restricted regular-package discovery include every migrated
Python module, excluding plans, tests, datasets, secrets and operational logs.

The version authority is the literal `_version.py` value. The current version is
`0.1.0a1`; the release process freezes a clean reviewed source snapshot before
building exact retained artifacts. File-based
versioning needs no SCM fallback or Git at runtime. The release tag is created at
the tested commit during publication ([release runbook](RELEASE_RUNBOOK.md)).

## Metrics and image identity

Checkout process-info metrics default to this same file-based package version.
`GRAPH_LOADER_VERSION`, read at process import/startup, explicitly overrides only
the diagnostic version label; it does not rewrite package metadata or establish
which image bytes ran. A directly set empty string remains an empty label for
compatibility. The existing CLI forwards nonempty overrides to containers;
an unset/empty launcher value is not forwarded, so that child uses its own
package-version default. No signal/logging/metrics server is started by import.

The migrated `neo4j_graph_loader.utils.metrics` imports `.._version` from the
single literal authority. No signal/logging/metrics server is started by import.

Keep `GRAPH_LOADER_NODE_IMAGE`, `GRAPH_LOADER_EDGE_IMAGE` and
`GRAPH_LOADER_CLOCK_IMAGE` deployment overrides under the exact version/component/
image-contract policy in [Docker execution](DOCKER_EXECUTION.md). Runtime selects
matching prebuilt versioned GHCR references, inspecting locally and pulling only
missing images; --skip-image-build is local-only. It never builds from cwd.
Candidate labels derive from the same literal file version; each service pins
immutable local IDs. An override is not provenance authentication. The release process binds
actual metadata/CLI/metrics/image versions and immutable bytes. Registry digests
exist only after separately authorized push/re-pull/public-pull checks.

## Image dependency locks

The loader images install a **locked, hashed** dependency set instead of the ranges in `requirements.txt`. There is one lock per
supported target, CPython 3.12 on glibc Linux:
`src/neo4j_graph_loader/image_build/locks/py312-linux-amd64.txt` and `py312-linux-arm64.txt` (package data). Each is a pip requirements file
(`name==version` plus one `--hash=sha256:` for the wheel pip picks on that platform) with a comment header that records the platform,
the base image it was resolved in, the sha256 of `requirements.txt` and a digest of its own body, so a hand edit is detected offline.
`requirements.txt` stays the generated list of direct ranges for developers; the package's own metadata ranges are unchanged.

```sh
make lock          # regenerate both locks; needs Docker (the other architecture runs under emulation)
make check-lock    # offline: parse, digest, requirements.txt hash, every direct dependency pinned inside its range
```

`make lock` runs `scripts/lock_requirements.py generate` once per platform. Inside a throwaway container of the pinned base image
(`python:3.12-slim` by digest, on `--platform linux/<arch>`) it installs `pip-tools`, resolves `requirements.txt` with
`--only-binary=:all:` and `--generate-hashes`, downloads the resolved wheels, and keeps for each pin only the hash of the wheel actually chosen
on that platform (after checking it is one of the hashes the index publishes for that release). **`pip-tools` exists only inside that
container**; it is not in `pyproject.toml`, in any requirements file or in an image, and the package gains no dependency. Containers are created,
started and removed by their recorded id; nothing is forced or pruned. A new release of a dependency changes a lock only when you run
`make lock`; commit the two files together, review the diff, and never lower a pin to make an install work.

**Preparing a wheelhouse** (for building images without any index, see the [restricted-network guide](RESTRICTED_NETWORKS.md#route-3-build-the-images-with-graph-loader-images-build)) runs inside the target base image on the
matching platform, so pip chooses the same wheels the build will install:

```sh
docker run --rm --platform linux/arm64 -v "$PWD/src/neo4j_graph_loader/image_build/locks/py312-linux-arm64.txt:/lock.txt:ro" \
    -v "$PWD/wheelhouse:/wheelhouse" --user "$(id -u):$(id -g)" -e HOME=/tmp python:3.12-slim \
    pip download --no-deps --only-binary=:all: --require-hashes -r /lock.txt -d /wheelhouse
```

then copy the package wheel next to them. Do not use `pip download --platform manylinux2014_<arch>` from another machine: current
`confluent-kafka` and NumPy wheels are `manylinux_2_27`/`_2_28` and are rejected under the `manylinux2014` tag. `python3
scripts/lock_requirements.py wheelhouse --platform linux/arm64 --package-wheel <wheel> --output <new dir>` runs the same command and then verifies
the directory against the lock: every pin has exactly one wheel with its hash, each wheel fits the platform, and nothing else (no sdist, no other
wheel, no symlink) is present. A wheel for the other architecture, a missing wheel and a changed wheel are all refused.

## Image build files

Each image is built from a context directory that holds: the Dockerfile of the role (as `Dockerfile`), `install.py`
(`neo4j_graph_loader/image_build/install.py`), `lock.txt` (the lock for the build platform), the package wheel under its own file name, `pip-ca.pem`
(a private CA bundle, or an empty file) and, for an offline build, `wheelhouse/`. The Dockerfiles use BuildKit (`docker buildx build`, which can pass
secrets) and need no registry other than the one holding the base image:

```sh
docker buildx build --load -t my-registry.example.com/team/graph-loader-node:0.1.0a1 \
    --build-arg BASE_IMAGE=my-registry.example.com/mirror/python@sha256:<digest> \
    --build-arg GRAPH_LOADER_VERSION=0.1.0a1 \
    --build-arg PACKAGE_WHEEL=neo4j_graph_loader-0.1.0a1-py3-none-any.whl --build-arg PACKAGE_WHEEL_SHA256=<sha256 of the wheel> \
    --build-arg DEPENDENCY_SOURCE=index-secret --secret id=pip_index,src=pip-index-url.txt <context>
```

`DEPENDENCY_SOURCE` is `index-secret` (the file `pip-index-url.txt` holds the index URL, credentials included, and nothing else), `index-url`
(`--build-arg INDEX_URL=...`, only for an index without credentials, since a build argument is visible in the image history) or `wheelhouse`
(offline). **No credential is ever a build argument.** `DEBIAN_MIRROR` (CLI image only) replaces the Debian sources for the `tini` install, which
a base image that already carries `tini` 0.19.0 skips.

`install.py` starts pip in a fresh environment (only `PATH`, `PIP_CONFIG_FILE=/dev/null`, the index URL and the CA bundle, so no pip
configuration file and no `PIP_*` or proxy variable of the build can matter), checks with `pip config list` that pip sees exactly those keys,
installs the lock with `--require-hashes --only-binary=:all: --no-deps`, then installs the package wheel by its digest, never from an index.
The URL is read from the secret file by `install.py` itself, so it is in no argument list, layer or log (pip's own output is redacted). The base
image is whatever `BASE_IMAGE` names: give a digest of your own mirror.
