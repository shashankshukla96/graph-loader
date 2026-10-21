# Local distribution development (E04)

The development artifact now contains the migrated runtime modules under
`neo4j_graph_loader`. The supported schema/planning modules are documented in the
[public API guide](LIBRARY_API.md). The installed `graph-loader` command and two bundled schema resources work
without a checkout. Private writable run directories are documented in the [CLI guide](CLI_RUNTIME.md);
checkout-free image delivery remains E06 work.
Do not upload these development artifacts.

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

There is no `src` compatibility shim. Replace `from src.models.schema import
GraphSchema` with `from neo4j_graph_loader.schema import GraphSchema`,
and `python -m src.cli` with `python -m neo4j_graph_loader.cli`. The same prefix
change applies to `loader`, `orchestrator` and `utils`, including subprocess and
patch targets. Old pickles that encode `src.*` class identities are not supported;
new process/spawn traffic uses the canonical migrated identities. Serialized
schema, plan/chunk and DLQ formats and Kafka groups/keys retain their behavior.
Use `neo4j_graph_loader.schema` and `neo4j_graph_loader.planning` for the
supported E04 surface; other module interfaces remain internal.

## Dependency authority

`pyproject.toml` `[project].dependencies` is the sole runtime dependency list.
All eight existing dependencies remain mandatory, including the matching
`neo4j-rust-ext`/Neo4j driver below 7; no extras are selected. The extension's
dependency metadata enforces its exact driver version. The declared bounds are
unchanged; support for minimum/current pairs across platforms is future E08
work. IMDb tools, testcontainers and build/review tools remain outside base.

```sh
python3 scripts/sync_requirements.py          # regenerate requirements.txt
python3 scripts/sync_requirements.py --check  # non-mutating drift check
```

`make requirements` and `make check-requirements` provide the same operations.
`make build-images` checks parity before invoking Docker. All four Dockerfiles
consume generated `requirements.txt`, copy reviewed build inputs and runtime
source, then install the package with isolated setuptools and `--no-deps`.
The literal file version needs no SCM fallback. Images resolve application code
from site-packages without PYTHONPATH. E06 implements checkout-free delivery. Before any
direct `docker build`, run the parity check explicitly. No new workflow exists.

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
The E02 two-module verifier is retained as historical scaffold proof; E03 uses
`plan/library-implementation/tools/verify_e03_artifacts.py` for the larger tree.

The version authority is the literal `_version.py` value. Current local builds
are `0.1.0a1.dev0`; E11 freezes the approved candidate target `0.1.0a1` at a clean
reviewed source snapshot before building exact retained artifacts. File-based
versioning needs no SCM fallback or Git at runtime. Release tags are created
at the tested commit in E12; no tag or publication is part of E04.

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

Keep existing `GRAPH_LOADER_NODE_IMAGE`, `GRAPH_LOADER_EDGE_IMAGE` and
`GRAPH_LOADER_CLOCK_IMAGE` deployment overrides. Their current checkout defaults
and Docker build behavior are unchanged. E06 will select matching prebuilt GHCR
images; E11 binds actual metadata/CLI/metrics/image versions and immutable bytes.
Future image metadata reads the same file version; an override is never proof
of compatibility. Record local image IDs before publication, and registry digests
only after the separately authorized E11 push/re-pull/public-pull checks.
