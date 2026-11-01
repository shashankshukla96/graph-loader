# Migrating from the source checkout to the installed package

For anyone who ran the loader from a checkout of this repository
(`python -m src.cli ...`, locally built `graph-loader-*:latest` images) and now
installs `neo4j-graph-loader`. It lists what changed, what to do, and what is
carried over unchanged. Release-level notes are in the [changelog](../CHANGELOG.md).

Statements are marked: **proved** means the release's installed-package service
tests exercised it against real Neo4j 5.26 and Kafka 7.7.0 (see
[Support and limits](../README.md#support-and-limits)); **carried over** means the
behavior comes from the original loader, is documented and unit-tested, and was
not exercised live for this release.

## 1. Commands and imports

There is **no `src` compatibility shim**, and none is planned: old imports and
module commands fail immediately (`No module named 'src'`) instead of working
for a while. Nothing needs to wait for a sunset date; change the names once.

| Original checkout | Installed package |
| --- | --- |
| `python -m src.cli start --mode bulk --config my_graph.yaml` | `graph-loader start --mode bulk --config my_graph.yaml` (also `python -m neo4j_graph_loader` and `python -m neo4j_graph_loader.cli`: the same parser and exit codes) |
| `python -m src.cli stop` / `status` / `replay-dlq ...` | `graph-loader stop` / `status` / `replay-dlq ...` |
| `python -m src.loader.node_loader`, `src.loader.edge_loader` | `python -m neo4j_graph_loader.loader.node_loader`, `...edge_loader` (internal modules, for debugging a single topic) |
| `from src.models.schema import GraphSchema` | `from neo4j_graph_loader.schema import GraphSchema` (supported module) |
| `from src.loader.edge_planner import plan_and_publish_edges` | `from neo4j_graph_loader.planning import plan_and_publish_edges` (supported module) |
| any other `src.<package>.<module>` | `neo4j_graph_loader.<package>.<module>`, **internal**: not part of the supported API and free to change between alphas |

Subprocess commands and patch targets follow the same prefix change. Pickles that
encode `src.*` class identities cannot be loaded; new process traffic uses the
`neo4j_graph_loader` identities. The distribution is `neo4j-graph-loader`, the
import name `neo4j_graph_loader`, the command `graph-loader`; the package root
exports only the version.

## 2. Configuration, files and defaults that moved

| What | Original checkout | Installed package |
| --- | --- | --- |
| Schema file | `--config` defaulted to `config/graph_schema.yaml` in the **current directory** | `--config PATH` is resolved from the current directory; **omitting it uses the bundled canonical schema**, whatever `config/` holds. Always pass `--config`. |
| Schema content | any key | The schema format is unchanged (see section 4), but unknown keys in `loading`, its nested blocks, `mix_and_batch`, `retry`, `execution`, `dead_letter` and `topics` entries are now errors that name the field. |
| Environment | variables from your shell | Same. The installed command never reads `.env` files; export `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`, `NEO4J_DATABASE`, `KAFKA_BOOTSTRAP_SERVERS` and the `LOADER_*` container-side addresses yourself ([CLI reference](CLI.md)). |
| Generated files (plans, spill, rejections, timings) | `var/...` under the working directory, for example `var/edge-plans/<run id>` | A private `graph-loader-*` directory under the OS temporary directory (or under `--run-dir BASE`), logged as `stage=runtime directory=...`. It is kept after success and failure; delete it yourself once no worker uses it. `--plan-dir`, `--plan-spill-dir` and `GRAPH_LOADER_TIMING_DIR` still override. |
| Compose stack, `scripts/start_dev.sh`, `make` targets | in the checkout | still only in the checkout; the pip package ships none of them. Provide Neo4j, Kafka and Docker yourself. |

## 3. Images: upgrade the package and the images together

| | Original checkout | Installed package |
| --- | --- | --- |
| Images | built locally by `make build-images`: `graph-loader-node`, `graph-loader-edge`, `graph-loader-clock`, tag `latest` | four prebuilt images, one tag per release: `ghcr.io/shashankshukla96/graph-loader-{cli,node,edge,clock}:<package version>`; there is no `latest` release identity |
| Selection | `GRAPH_LOADER_*_IMAGE` or the local default | defaults to the tag equal to `graph-loader --version`; `GRAPH_LOADER_NODE_IMAGE`, `GRAPH_LOADER_EDGE_IMAGE` and `GRAPH_LOADER_CLOCK_IMAGE` select explicit overrides |
| Build | `start` could use whatever was built locally | `start` **never builds**: it uses a local image if present, pulls a missing one, and with `--skip-image-build` only uses local images and never pulls |
| Version check | none | every selected image, overrides included, must carry `org.opencontainers.image.version` equal to the installed package version, `io.graph-loader.component` for its role and `io.graph-loader.image-contract=1`; a mismatch fails at `start` before any container runs |

To upgrade: stop any running fleet (`graph-loader stop`, or SIGTERM the stream
`start`), install the new package, then run the first `start` with network access
to the registry (or preload the four images and use `--skip-image-build`). Never
mix a package of one version with images of another, and never replace images under
a running fleet. A stopped stream fleet resumes from its committed offsets with the
same command (**proved**).

## 4. Stored and wire formats

D03 keeps these compatible across the move to the package. "Unit-tested" means
the format is covered by the repository's tests; none was migrated live.

| Surface | Policy and what to do |
| --- | --- |
| Schema YAML | Fields, defaults and validation are preserved; additions are documented in the changelog. The original loader's own schemas (its `config/graph_schema.yaml` and the IMDb, social and three multi-topic example schemas) all pass `graph-loader validate-schema` with the installed package. A schema from an early development snapshot that declared edges without `source_key_property` and `target_key_property` is refused with a message naming them: add the two keys. |
| Plan JSON | Format `graph-loader-edge-plan/1`, unchanged. A plan belongs to the run that wrote it; after an upgrade **replan** (rerun `start`, or the planning API) rather than reusing an old plan file. |
| Kafka chunk topic | Chunk format 1, unchanged; the chunk topic is per run. Delete a leftover chunk topic from an interrupted old run (or `--replace-stale-plan-topic` when no container of that run is alive) before rerunning. |
| DLQ envelopes | Versions 1 and 2 are both decoded and version 2 is written (unit-tested; the live DLQ tests wrote, retried, parked and replayed envelopes with the current writer). Do not point a downgraded checkout at topics that new code has written. |
| Parked-pair index | Keys, label/source identity, sequencing and tombstones are preserved; the index topic is not reset or renamed. Changing a type's topic count changes its default DLQ name: see changelog upgrade note 3 (`--accept-former-dlq`, or pin `dead_letter.topic`). |
| Consumer groups | Stable stream groups `<consumer_group_id>-<type>` and `<consumer_group_id>-retry-consumer`; bulk groups stay run-scoped `<prefix>-<type>-<run id>`; planned mode commits no relationship group. A restart resumes from committed offsets with or without `--run-id` (**proved** for lease streaming). Groups created by the original clock-rotation fleet are migrated once with `--stream-clock-rotation --run-id <R>`: **carried over**, not run live for this release. |

Nothing in the upgrade rewrites, deletes or resets a topic, a consumer group, a
database or a plan. The loader refuses (exit 1) rather than migrating
destructively, for example on leftover DLQ records of a renamed default DLQ.

## 5. The legacy clock path

`--stream-clock-rotation` selects the original rotating global batch clock for
streaming. It is deprecated: `start` logs `stage=deprecation
flag=--stream-clock-rotation`, it is refused for multi-pair relationship types,
and it remains in `0.1.0a1` and at least through the next alpha before any
removal, which the changelog will announce first (D03). The default streaming
mechanism is the bucket-lease fleet, the one the release tests exercised; the
clock path was **not** exercised live. The hidden `--lease-*` flags and
`GRAPH_LOADER_LEASE_ADDR` follow the same rule: they still work, log a
deprecation line, and are removed no sooner than after the next alpha. Update
scripts to `--bucket-lease-*` and `GRAPH_LOADER_BUCKET_LEASE_ADDR`.

## 6. Walk-through: an old command, then the installed one

Old, from the checkout, with `config/graph_schema.yaml` in the working directory:

```bash
python -m src.cli start --mode bulk
```

New, from any directory, with the package installed in an environment:

```bash
graph-loader validate-schema --config my_graph.yaml       # no services needed
export NEO4J_URI=bolt://localhost:7687 NEO4J_USERNAME=neo4j NEO4J_PASSWORD='<your password>'
export NEO4J_DATABASE=mygraph KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export LOADER_NEO4J_URI=bolt://neo4j:7687 LOADER_KAFKA_BOOTSTRAP_SERVERS=kafka:29092
graph-loader start --mode bulk --config my_graph.yaml
```

The explicit `--config`, the exported variables (the command reads no `.env`) and
the four versioned images are the three things the old command did not need to
say. A first `start` pulls missing images; the log then shows
`stage=launch edge_mode=planned reason=default`.

## 7. If something goes wrong

* **Planned bulk run failed or was stopped:** it leaves a duplicate-free partial
  graph and no ledger; rerun the same command on the same topics. Writes are
  `MERGE` upserts, so the rerun converges (**proved**, including after a lost
  writer and a Neo4j stop). Stale chunk topics: see section 4.
* **Stream fleet stopped or an edge loader was lost:** run the same `start`
  command again; it resumes from committed offsets and reaches the exact graph
  (**proved**).
* **`start` refuses:** read the message; refusals (leftover DLQ topics, live
  group members, image version mismatch) are checks made before any container
  is launched.
  Do not delete topics, groups or images you cannot identify as yours.
* **Going back to the checkout:** stop the fleet first; stable consumer groups and
  topics stay as they are. Messages written in DLQ envelope version 2 may not be
  readable by an older checkout.
