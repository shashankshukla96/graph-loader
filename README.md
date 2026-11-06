# Neo4j Distributed Graph Loader

**Status: alpha (`0.1.0a2`), not published.** The package is not on PyPI and the images
are not on `ghcr.io`, so `pip install neo4j-graph-loader` and the default image pulls do
not work yet. [Build the package and the images yourself](#build-and-run-locally) from
this repository (CPython 3.12; Neo4j and Kafka are yours to run, and the local stack below
starts both). What is supported and what is not is in [Support and limits](#support-and-limits).
The walk-through below runs a complete load from a source checkout; see the
[development guide](docs/DISTRIBUTION_DEVELOPMENT.md) first. Other ways to run and operate it: [process runtime](docs/PROCESS_RUNTIME.md) (no Docker), [single container](docs/SINGLE_CONTAINER.md), [run status stores](docs/STATUS_SQL.md), [monitoring](docs/MONITORING.md) and [restricted networks](docs/RESTRICTED_NETWORKS.md).

graph-loader loads data from Kafka topics into a Neo4j graph. You describe the
graph model in a YAML schema (node labels, relationship types, property types,
keys, one topic per label and per type) and publish JSON records to those
topics; the loader does the rest. No model-specific code and no tuning flags
are required.

* **Bulk mode** loads a finite, quiescent set of records and proves the result
  (zero lag at a fixed boundary, a complete write ledger). The default
  relationship mode is **planned**: the loader plans the whole relationship
  load into node-disjoint chunks, so concurrent transactions never wait on each
  other's Neo4j locks.
* **Stream mode** keeps loading as records arrive, with leased node-disjoint
  writes, a dead-letter queue and retry consumer for edge-before-node arrivals,
  stable consumer groups and graceful stop/restart.
* Relationship types may be fed by several topics and may span several label
  pairs; self-references, several types between the same labels and labels
  without relationships are supported.

## Documentation

| Document | Read it to |
| --- | --- |
| [Quick start (installed package)](docs/QUICKSTART.md) | Validate a schema and plan relationships into Kafka with an installed package. Until one is published, install the wheel you [built locally](#build-and-run-locally). |
| [Bulk load guide](docs/BULK_LOAD.md) | Run an initial load: prerequisites, producer contract, every `start --mode bulk` flag, the edge modes, tuning, verification, recovery, a worked example. |
| [Incremental load guide](docs/INCREMENTAL_LOAD.md) | Load repeatedly: repeated bulk batches versus continuous streaming, ordering, DLQ, restart. |
| [Loading from a relational database](docs/RELATIONAL_SOURCES.md) | Put graph-loader behind Oracle, SQL Server or PostgreSQL: modeling tables, shaping rows into records, producers, bulk then CDC, deletes. |
| [CLI reference](docs/CLI.md) | Look up a command, flag, default, range, exit code, `stage=` log line or environment variable. |
| [Architecture](docs/ARCHITECTURE.md) | Understand how the components fit together and how zero lock waits is achieved in each mode. |
| [Usage guide](docs/USAGE.md) | The detailed reference: schema fields, input contract, planned-mode internals, dead-lettering, performance, operations, troubleshooting, limitations. |
| [Monitoring](docs/MONITORING.md) | Metrics, Prometheus and Grafana stack, alerts. |
| [Migration from the checkout](docs/MIGRATION.md) | Move from `python -m src.cli` and local images to the installed package: names, defaults, images, stored formats, recovery. |
| [Release and recovery runbook](docs/RELEASE_RUNBOOK.md) | For maintainers: publish, verify, yank, recover, dependency updates and the regression gate. |
| [Changelog](CHANGELOG.md) | See what changed and what to do when upgrading (read the upgrade notes). |
| [Examples](examples/README.md) | [IMDb](examples/imdb/README.md) (a real 23M-record benchmark), [social](examples/social/README.md) (synthetic, exact verification), [multi_topic](examples/multi_topic/README.md) (several topics or label pairs per type). |
| [Support and reporting problems](docs/SUPPORT.md) | Get help, minimal reproduction template, contributing; [security policy](SECURITY.md). |
| [History](docs/HISTORY.md) | Project origins and design evolution. |
| [Public library API](docs/LIBRARY_API.md) | Schema and producer planning, Kafka requirements, ownership, cancellation and cleanup. |
| [Distribution development](docs/DISTRIBUTION_DEVELOPMENT.md) | Install/build/check the runtime, installed command, bundled schemas and private run state. |

## Full load walk-through from a checkout (about 5 minutes)

This walk-through runs a complete load against the local development stack in
this repository, using a source checkout (it needs `docker-compose.yaml`,
`scripts/start_dev.sh` and `make build-images`, none of which ship in the pip
package). Installed-package users start with the [quick start](docs/QUICKSTART.md).

Prerequisites: Docker with Compose v2 and Buildx, and Python 3.12. `.env.example` starts
Neo4j with a 1 GB heap and a 1 GB page cache, pins no CPUs and keeps the data in `./var/data`.
Change `NEO4J_HEAP_SIZE`, `NEO4J_PAGECACHE_SIZE`, `NEO4J_CPUSET`, `KAFKA_CPUSET` and
`GRAPH_LOADER_DATA_DIR` in `.env` to suit your machine
([USAGE section 2](docs/USAGE.md#2-requirements-and-environment) lists them). **On macOS**,
also set `KAFKA_EXTERNAL_HOST=127.0.0.1` in `.env` and use `127.0.0.1:9092` wherever these
steps say `localhost:9092`: Docker Desktop does not publish the port on IPv6.

**1. Install and start the local stack** (Neo4j Enterprise 5 and Kafka):

```bash
python3.12 -m venv .venv && .venv/bin/pip install -r requirements.txt -r requirements-build.txt && .venv/bin/pip install -e .
export PATH="$PWD/.venv/bin:$PATH"
cp .env.example .env        # set NEO4J_PASSWORD; set GRAPH_LOADER_DATA_DIR to a writable directory
bash scripts/start_dev.sh   # waits until Neo4j and Kafka are healthy
```

**2. Set the connection environment explicitly.** The CLI does not read
`.env`; export what it needs. Host-side values are for the CLI and your
scripts, `LOADER_*` values are for the loader containers on the Docker network:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092 NEO4J_URI=bolt://localhost:7687
export NEO4J_USERNAME=neo4j NEO4J_PASSWORD='<the password from .env>'
export LOADER_KAFKA_BOOTSTRAP_SERVERS=kafka:29092 LOADER_NEO4J_URI=bolt://neo4j:7687
export NEO4J_DATABASE=mygraph    # the database the load writes to (created below)
docker exec graph-loader-neo4j cypher-shell -u neo4j -p "$NEO4J_PASSWORD" \
  "CREATE DATABASE mygraph IF NOT EXISTS WAIT"
```

Database names allow only ASCII letters, digits, dots and dashes (no
underscores). Without `NEO4J_DATABASE` the load targets the server default,
`neo4j`.

**3. Describe the graph.** Save as `my_graph.yaml`:

```yaml
loading:
  mode: bulk
  consumer_group_id: my-graph        # node loaders resume this group on later runs
nodes:
  - label: Person                    # Cypher identifier
    topic: person-events             # one topic per label
    key_property: person_id          # MERGE key; must be declared and required
    properties:
      person_id: {type: integer, required: true, constraint: {type: uniqueness}}
      name:      {type: string, required: true}
      joined:    {type: date}
    # replicas: 4                    # optional; derived from the topic size when omitted
  - label: City
    topic: city-events
    key_property: name
    properties:
      name: {type: string, required: true}
edges:
  - type: LIVES_IN
    topic: lives-in-events           # one topic per relationship type
    nodes: {source: Person, target: City}
    source_key_property: person_id   # must equal the source label's key_property
    target_key_property: name        # must equal the target label's key_property
    properties:
      since: {type: date}
```

**4. Publish records** (the [input contract](#the-input-contract) below). This
creates the topics and publishes a few records, keying relationship records by
their source key:

```bash
python - <<'EOF'
import json
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

bootstrap = "localhost:9092"
admin = AdminClient({"bootstrap.servers": bootstrap})
for name, future in admin.create_topics(
    [NewTopic(t, num_partitions=2, replication_factor=1) for t in ("person-events", "city-events", "lives-in-events")]
).items():
    try:
        future.result()
    except Exception as exc:  # already exists is fine
        print(name, exc)

producer = Producer({"bootstrap.servers": bootstrap})
people = [(1, "Ada", "2024-01-31"), (2, "Grace", "2023-05-02"), (3, "Linus", "2022-11-30")]
for pid, name, joined in people:
    producer.produce("person-events", key=str(pid), value=json.dumps({"person_id": pid, "name": name, "joined": joined}))
for city in ("Paris", "London"):
    producer.produce("city-events", key=city, value=json.dumps({"name": city}))
for pid, city in ((1, "Paris"), (2, "London"), (3, "Paris")):
    producer.produce("lives-in-events", key=str(pid), value=json.dumps(
        {"source": {"person_id": pid}, "target": {"name": city}, "properties": {"since": "2020-05-01"}}))
producer.flush()
EOF
```

For this unpublished development build, prepare local candidates once from the
checkout and select their references explicitly (use a fresh suffix each build):

```bash
make build-images CANDIDATE_SUFFIX=quickstart
. var/candidate-images-quickstart/image-overrides.sh
```

**5. Run the bulk load.** The CLI resolves compatible versioned images, applies the schema DDL,
launches the node loaders, plans and writes the relationships, and
exits `0` only when the result is proven:

```bash
graph-loader start --mode bulk --config my_graph.yaml
```

Watch for `stage=launch edge_mode=planned reason=default`, one
`stage=node-label-ready` per label and `stage=planned-edges-complete`.

**6. Verify.**

```bash
docker exec graph-loader-neo4j cypher-shell -u "$NEO4J_USERNAME" -p "$NEO4J_PASSWORD" -d "$NEO4J_DATABASE" \
  "MATCH (p:Person)-[r:LIVES_IN]->(c:City) RETURN p.name, c.name, r.since ORDER BY p.person_id"
```

Expect three rows. Re-running the same load is safe (`MERGE`), but node loaders
resume the `consumer_group_id` and would read nothing new: change the id (or
publish new records) to reload the same topics. To scale up, use
[examples/social](examples/social/README.md), which publishes and exactly
verifies a synthetic graph of any size.

Stop the local stack with `docker compose down`. Its data lives in
`GRAPH_LOADER_DATA_DIR` (bind mounts), so `down -v` does not delete it; remove
that directory to start from an empty database and empty topics.

## Build and run locally

No release is needed: you can build everything from this repository and run it on your
own machine. You need Python 3.12 and, for the images, Docker with Buildx. These commands
were run from a clean copy of the repository on macOS arm64, up to a finished bulk load.

**1. Build and install the package.**

```bash
python3.12 -m venv .venv
.venv/bin/pip install -r requirements-build.txt
.venv/bin/python -m build                       # writes the wheel and sdist to dist/
.venv/bin/pip install dist/neo4j_graph_loader-*.whl
export PATH="$PWD/.venv/bin:$PATH"
graph-loader --version
```

Wherever a guide says `pip install neo4j-graph-loader`, install this wheel instead.

**2. Build the images.** This builds `graph-loader-cli`, `-node`, `-edge` and `-clock` for
your machine's architecture from the wheel you just built, with the dependency versions
locked in the package. It pushes nothing. The name after `--registry` is only a prefix for
the local tags:

```bash
graph-loader images build --registry local/graph-loader \
  --pip-index-url https://pypi.org/simple \
  --package-wheel dist/neo4j_graph_loader-*-py3-none-any.whl \
  --output var/local-images
export GRAPH_LOADER_IMAGE_REGISTRY=local/graph-loader
```

Keep that `export` in the shell that runs `graph-loader start`. Without it the command looks
for `ghcr.io/neburyx` images, which are not published. To use your own PyPI mirror, base
image, Debian mirror or no network at all, see
[restricted networks](docs/RESTRICTED_NETWORKS.md).

If you work from an editable install, as the walk-through above does,
`make build-images CANDIDATE_SUFFIX=<new name>` does the same job (it builds its own wheel).
Then run `. var/candidate-images-<new name>/image-overrides.sh` instead of the `export`.

**3. Run a load.** Start Neo4j and Kafka with steps 1 and 2 of the
[walk-through](#full-load-walk-through-from-a-checkout-about-5-minutes), skipping its
`pip install` line, then follow steps 3 to 6. `graph-loader start` launches one container per
role from your images and removes them when the load ends. Stop the stack with
`docker compose down`.

**Without images (Linux only).** `--runtime process` runs every role as a child process of
the command, so step 2 and Docker are not needed. It needs Linux 5.3 or newer and your own
Neo4j and Kafka; see [process runtime](docs/PROCESS_RUNTIME.md). On macOS it stops with an
error, and macOS stays a place to try small loads rather than a
[supported platform](#support-and-limits). To run everything in one container instead, see
[single container](docs/SINGLE_CONTAINER.md).

## The input contract

* **Node** topic: one JSON object per record, the node's properties by name,
  e.g. `{"person_id": 7, "name": "Ada", "joined": "2024-01-31"}`. Later records
  for the same key update it (`SET n += props`). Key node records by their node
  key.
* **Relationship** topic: the endpoint envelope
  `{"source": {"person_id": 7}, "target": {"name": "Paris"}, "properties": {"since": "2020-05-01"}}`.
  All three sections are mandatory; use `"properties": {}` when there are none.
  Repeated records for one `(source, target)` pair MERGE one relationship and
  apply `SET r += props` in topic order (a later record overwrites the
  properties it carries and keeps the others).
* Property types: `string`, `integer`, `float`, `date`, `datetime`; any of them
  may be a node key. `required: true` properties must be present and non-null in
  every record. Wire formats: dates `YYYY-MM-DD`, datetimes ISO-8601 with `T` and
  an offset (`2024-01-31T12:00:00+01:00` or `...Z`), finite JSON numbers only.
* **Key relationship records by their source key**: Kafka orders records only
  within a partition, and that order is what "last write wins" follows.
* In bulk mode, publish everything before starting the load: records produced
  after the run captured its boundary fail the run instead of being half-loaded.
* The schema is validated before anything starts, with errors naming the field
  (unknown endpoint labels, endpoint key properties that differ from the
  label's `key_property`, undeclared property types, non-Cypher identifiers,
  duplicate labels or types, a topic used twice). Every node key is indexed: a
  key declared without a uniqueness constraint or range index gets the
  constraint `<label>_<key>_key_unique`, with the label and key lowercased
  (e.g. `person_person_id_key_unique`).

The full field reference is [USAGE section 3](docs/USAGE.md#3-the-yaml-schema)
and the contract is [USAGE section 4](docs/USAGE.md#4-the-input-contract-publishing-records).

## Using the loader for your own graph model

Copy [`examples/social/graph_schema.yaml`](examples/social/graph_schema.yaml),
rename the labels, types, keys and topics, and point your producer at those
topics. Nothing else changes: `examples/imdb`, `examples/social` and
`examples/multi_topic` are three users of the same contract.

## Choosing a mode

| You want | Run | Details |
| --- | --- | --- |
| An initial load of a finite data set | `graph-loader start --mode bulk --config my_graph.yaml` | [BULK_LOAD](docs/BULK_LOAD.md). Planned mode is the default; a schema that enables relationship `dead_letter` gets the lease mode (also lock-free); `--edge-classic` selects the per-type APOC-locked loaders, the one mode that can wait on locks. |
| Continuous loading as records arrive | `graph-loader start --mode stream --config my_graph.yaml` | [INCREMENTAL_LOAD](docs/INCREMENTAL_LOAD.md). Runs until SIGINT/SIGTERM; restart resumes the stable consumer groups. |
| Repeated finite batches | `start --mode bulk` per batch | [INCREMENTAL_LOAD](docs/INCREMENTAL_LOAD.md). |

Everything in planned mode (writers, buckets, chunk size, planner processes and
memory) is derived automatically from the schema, the data volume and the Neo4j
server; flags override each value (see [CLI](docs/CLI.md#planned-mode-tuning---edge-plan-also-the-bulk-default)
and [USAGE section 6](docs/USAGE.md#6-planned-mode-in-depth)). Loader images
default to matching package-version tags under `ghcr.io/neburyx` (or under your own registry with `GRAPH_LOADER_IMAGE_REGISTRY=<prefix>`; behind a network that cannot reach the public registries, see [Restricted networks](docs/RESTRICTED_NETWORKS.md)).
`GRAPH_LOADER_NODE_IMAGE`, `GRAPH_LOADER_EDGE_IMAGE` and `GRAPH_LOADER_CLOCK_IMAGE`
select explicitly compatible overrides. `start` uses local images first and pulls
only missing images; `--skip-image-build` requires local images and never pulls.
Runtime never builds from the caller's directory. Local development candidates
are not registry publications; see [Docker execution](docs/DOCKER_EXECUTION.md).

## Worked examples

```bash
python examples/social/social_graph.py publish --persons 20000       # per the contract above
graph-loader start --mode bulk --config examples/social/graph_schema.yaml
python examples/social/social_graph.py verify --persons 20000        # exact graph comparison
```

`verify` recomputes the expected graph from the generator (without library
code) and compares every node, relationship and property. See
[examples/README.md](examples/README.md) for the shared setup, the IMDb
benchmark and the multi-topic example (relationship types fed by several
topics, with `order_by` or `precedence` conflict rules).

## Support and limits

<!-- support-and-limits:begin -->
This is an alpha, maintained by one person on a best-effort basis. What is
stated here was exercised on the environment listed; everything else is outside
what the project has checked.

**Supported environment**

* **Python:** CPython 3.12 (`requires-python >=3.12`; no newer version is tested).
* **Platform:** Linux amd64 with live Neo4j and Kafka. Linux arm64 for the
  installed package, schema, planning API and command line, without a live-service
  run. macOS and Windows are not supported; in particular the streaming lease
  server's readiness probe needs container addresses that are routable from the
  host, which Docker Desktop does not provide.
* **Services:** Neo4j 5.26 Enterprise and Kafka 3.7 (`confluentinc/cp-kafka:7.7.0`),
  one broker, plaintext listener. Other versions and security settings were not
  tested. The loaders run as Docker containers (`--runtime docker`, the default,
  which needs a Docker Engine on Linux) or as child processes of the one
  `graph-loader start` command (`--runtime process`: Linux kernel 5.3 or newer, no
  Docker, also as a single container or a systemd service); both runtimes were
  exercised against real Neo4j and Kafka on Linux amd64.
* **Dependencies:** the declared ranges were tested at their minimum and at a
  recent set of versions.

**What was checked against real services.** Through the installed command and
the versioned images, against real Neo4j 5.26 and Kafka, with the resulting
graph compared to the expected graph: planned bulk loads, streaming with leases,
classic bulk loads (`--edge-classic`), dead-letter routing with retry, parking
and replay, and recovery from killed workers, a lost lease server and a Neo4j
outage. In those runs no planned-mode or lease write waited on a Neo4j lock;
classic mode with concurrent sessions did, as documented.

**Limits**

* **Concurrency and isolation.** The zero-lock-wait design covers the loader's
  own transactions. Other writers to the same database are not coordinated and
  can wait on or deadlock with loader transactions. Run one loader fleet per
  consumer-group id and database. A load is a series of `MERGE` upserts, not one
  transaction: readers see a partial graph while it runs, and a failed planned
  bulk run leaves a duplicate-free partial graph that a full rerun on the same
  topics completes. Writes are at-least-once and idempotent; there is no
  exactly-once promise.
* **Missing endpoints.** A planned bulk run fails closed, rolling back the chunk,
  when a relationship's source or target does not match exactly one node, so
  publish and load nodes first. Streaming and the lease or classic bulk loaders
  route such rows to a dead-letter topic only when the relationship type enables
  `dead_letter` (bulk also needs the per-type opt-in); otherwise the batch or the
  stream fleet stops and a rerun resumes from the committed offsets. Node records
  are not dead-lettered.
* **Dead-letter queue.** Rows are retried by a retry consumer and parked after
  `max_retry_attempts`; `replay-dlq` republishes parked rows at new offsets. Planned mode
  ignores `dead_letter`.
* **Ordering.** Only a restricted contract is promised. Records of one
  relationship pair published to one partition, keyed by source key, are applied
  in topic order and the last one applied wins. With `order_by` (tested on an
  integer property) the newest version wins across topics, partitions and
  replicas whatever the arrival order; an equal version keeps the stored value, an
  older one is ignored, and a record without the property is applied unconditionally.
  Without `order_by` the record applied last wins even if it is older, and
  when several topics feed one pair the result depends on arrival order. The
  retry consumer's memory of resolved rows lives in process: it is lost on a restart,
  on a partition revoke, and beyond 100,000 cached entries, so arrival order is
  not restart-safe. A manual replay can overwrite a newer value with an older
  one unless the type has `order_by`. Stop the fleet before deleting and
  recreating a source topic. Timestamp-typed `order_by` and external writers
  were not tested.
* **Unsupported modes.** `execution.mode: native_disjoint` needs a Neo4j server with
  the Cypher 25 dialect and is not supported on Neo4j 5.26: with `--edge-classic`
  the edge loader exits 1 and writes no relationship. It is outside these
  guarantees. The deprecated `--stream-clock-rotation` flag and every module other
  than `neo4j_graph_loader.schema` and `neo4j_graph_loader.planning` are internal.
* **Resources and performance.** Planning is bounded in memory but needs spill
  disk of about the uncompressed size of the relationship records, and the chunk
  topic holds them once more. The one large run measured loaded 14,483,774 nodes
  and 70,210,506 relationships (from 78,005,287 relationship records) in 989 s
  end to end on a single Linux host with Neo4j on 8 CPUs (3 GB heap, 5 GB page
  cache) and Kafka on a 3 GiB container, with every write in Neo4j's query
  log showing zero lock waiting. That is one measured environment, not a performance promise; throughput
  depends on Neo4j CPUs, memory, data shape and Kafka. No streaming throughput
  figure is claimed.
<!-- support-and-limits:end -->

See also [Support and reporting problems](docs/SUPPORT.md) and the [security policy](SECURITY.md). Licensed under the Apache License 2.0 ([LICENSE](LICENSE)); the IMDb example uses IMDb's non-commercial datasets, which are not distributed with the package and are subject to [IMDb's terms](https://developer.imdb.com/non-commercial-datasets/). Neo4j and Kafka are trademarks of their owners; Neo4j Enterprise is licensed separately by Neo4j.

## Development

* Development dependencies: `python -m pip install -r requirements-dev.txt`
  (includes runtime dependencies and the IMDb example tools).
* Unit tests: `make test-unit` (runs `tests` and `examples/imdb/tests`, excluding integration and smoke).
  Docker-backed smoke tests for the local stack: `python -m pytest tests/test_dev_environment.py -v -m smoke`.
* Opt-in end-to-end tests run against your developer stack and need
  `GRAPH_LOADER_IT_DATABASE` (a scratch database name, never `neo4j`) plus the
  `GRAPH_LOADER_IT_*` connection variables and credentials; see
  [USAGE section 13](docs/USAGE.md#13-development-and-validation).
* Lint: `make lint`. Images: `make build-images`. Monitoring stack:
  `make monitoring-up` (it needs `GRAFANA_ADMIN_PASSWORD` and binds to 127.0.0.1; see [MONITORING](docs/MONITORING.md)).
* Debug a single topic with `python -m neo4j_graph_loader.loader.node_loader` /
  `python -m neo4j_graph_loader.loader.edge_loader` ([HISTORY](docs/HISTORY.md#direct-single-topic-runs)).
