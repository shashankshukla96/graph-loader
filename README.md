# Neo4j Distributed Graph Loader

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
| [Bulk load guide](docs/BULK_LOAD.md) | Run an initial load: prerequisites, producer contract, every `start --mode bulk` flag, the edge modes, tuning, verification, recovery, a worked example. |
| [Incremental load guide](docs/INCREMENTAL_LOAD.md) | Load repeatedly: repeated bulk batches versus continuous streaming, ordering, DLQ, restart. |
| [CLI reference](docs/CLI.md) | Look up a command, flag, default, range, exit code, `stage=` log line or environment variable. |
| [Architecture](docs/ARCHITECTURE.md) | Understand how the components fit together and how zero lock waits is achieved in each mode. |
| [Usage guide](docs/USAGE.md) | The detailed reference: schema fields, input contract, planned-mode internals, dead-lettering, performance, operations, troubleshooting, limitations. |
| [Monitoring](docs/MONITORING.md) | Metrics, Prometheus and Grafana stack, alerts. |
| [Changelog](CHANGELOG.md) | See what changed and what to do when upgrading (read the upgrade notes). |
| [Examples](examples/README.md) | [IMDb](examples/imdb/README.md) (a real 23M-record benchmark), [social](examples/social/README.md) (synthetic, exact verification), [multi_topic](examples/multi_topic/README.md) (several topics or label pairs per type). |
| [History](docs/HISTORY.md) | Project origins and design evolution. |

## Quick start (about 5 minutes)

Prerequisites: Docker with Compose v2 and Python 3.12. The bundled Compose
file pins Neo4j to CPUs `0-7` (8 GB heap, 12 GB page cache) and Kafka to CPUs
`8-11`, and keeps the data under `GRAPH_LOADER_DATA_DIR` (default
`/mnt/graph-loader/data`). On a smaller machine set `NEO4J_CPUSET`,
`KAFKA_CPUSET`, `NEO4J_HEAP_SIZE`, `NEO4J_PAGECACHE_SIZE` and
`GRAPH_LOADER_DATA_DIR` in `.env` first
([USAGE section 2](docs/USAGE.md#2-requirements-and-environment) lists them).

**1. Install and start the local stack** (Neo4j Enterprise 5 and Kafka):

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
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

**5. Run the bulk load.** The CLI applies the schema DDL, builds the images
(on every `start`, retagging `:latest`, unless you pass `--skip-image-build`),
launches the node loaders, plans and writes the relationships, and
exits `0` only when the result is proven:

```bash
python -m src.cli start --mode bulk --config my_graph.yaml
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
| An initial load of a finite data set | `python -m src.cli start --mode bulk --config my_graph.yaml` | [BULK_LOAD](docs/BULK_LOAD.md). Planned mode is the default; a schema that enables relationship `dead_letter` gets the lease mode (also lock-free); `--edge-classic` selects the per-type APOC-locked loaders, the one mode that can wait on locks. |
| Continuous loading as records arrive | `python -m src.cli start --mode stream --config my_graph.yaml` | [INCREMENTAL_LOAD](docs/INCREMENTAL_LOAD.md). Runs until SIGINT/SIGTERM; restart resumes the stable consumer groups. |
| Repeated finite batches | `start --mode bulk` per batch | [INCREMENTAL_LOAD](docs/INCREMENTAL_LOAD.md). |

Everything in planned mode (writers, buckets, chunk size, planner processes and
memory) is derived automatically from the schema, the data volume and the Neo4j
server; flags override each value (see [CLI](docs/CLI.md#planned-mode-tuning---edge-plan-also-the-bulk-default)
and [USAGE section 6](docs/USAGE.md#6-planned-mode-in-depth)). Loader images
default to `graph-loader-node:latest` / `graph-loader-edge:latest`; set
`GRAPH_LOADER_NODE_IMAGE` / `GRAPH_LOADER_EDGE_IMAGE` to use other tags. `start`
rebuilds the images and retags `:latest` every time unless `--skip-image-build`
is given (use it with images from `make build-images`).

## Worked examples

```bash
python examples/social/social_graph.py publish --persons 20000       # per the contract above
python -m src.cli start --mode bulk --config examples/social/graph_schema.yaml
python examples/social/social_graph.py verify --persons 20000        # exact graph comparison
```

`verify` recomputes the expected graph from the generator (without library
code) and compares every node, relationship and property. See
[examples/README.md](examples/README.md) for the shared setup, the IMDb
benchmark and the multi-topic example (relationship types fed by several
topics, with `order_by` or `precedence` conflict rules).

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
  `make monitoring-up` (see [MONITORING](docs/MONITORING.md)).
* Debug a single topic with `python -m src.loader.node_loader` /
  `python -m src.loader.edge_loader` ([HISTORY](docs/HISTORY.md#direct-single-topic-runs)).
