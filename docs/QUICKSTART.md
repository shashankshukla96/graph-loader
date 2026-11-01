# Quick start for the installed package

Two short walk-throughs that need only `pip` and, for the second one, a Kafka
broker: describe a graph and validate the schema, then plan relationship rows
into Kafka with the public Python API. They use the installed `graph-loader`
command and the public `neo4j_graph_loader.schema` and `neo4j_graph_loader.planning`
modules, nothing from a source checkout. Developing the library itself is a
separate path: see [Distribution development](DISTRIBUTION_DEVELOPMENT.md).

This is an alpha (`0.1.0a1`). The guarantees and limits of this release are in the
[README](../README.md#support-and-limits); the full API contract is
[Public library API](LIBRARY_API.md).

## What you need

| You need | For | Notes |
| --- | --- | --- |
| CPython 3.12 and `pip` | both | `requires-python` is `>=3.12`. Use a virtual environment. |
| Nothing else | [Quick start 1](#quick-start-1-describe-and-validate-a-graph-schema) | It runs without Kafka, Neo4j or Docker. |
| A Kafka broker you can reach and create topics on | [Quick start 2](#quick-start-2-plan-relationships-into-kafka) | Producer planning publishes chunks to Kafka, even for in-memory rows. A single broker is enough. The release is tested against Kafka 3.7 (`confluentinc/cp-kafka:7.7.0`). |
| Neo4j 5.26 Enterprise, Docker, and the loader images | [loading the graph](#after-the-quick-starts-loading) | Not needed for either quick start. |

`pip install` installs the Python package and its eight runtime dependencies
(`neo4j`, `neo4j-rust-ext`, `confluent-kafka`, `pydantic`, `PyYAML`, `docker`,
`numpy`, `prometheus-client`). All eight are mandatory; the package has **no
extras**. **pip does not install or start Neo4j, Kafka or Docker.** You provide
them, and the loader never creates or removes them. If you have no broker, start
a throwaway one you will delete afterwards (a single `confluentinc/cp-kafka:7.7.0`
container is enough) or use a broker your team already runs for experiments.

## Install

Create a working directory with its own virtual environment:

```bash
mkdir graph-loader-quickstart && cd graph-loader-quickstart
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install "neo4j-graph-loader==0.1.0a1"
graph-loader --version
```

The version is pinned because pip skips pre-releases of a package unless you ask
for one. `graph-loader --version` prints `graph-loader 0.1.0a1`.

## Quick start 1: describe and validate a graph schema

A schema is a YAML file: node labels with their key property and topic,
relationship types with their endpoints and topic. Save this as `my-graph.yaml`
in the working directory (the commands below use that relative path
explicitly; nothing is read from the environment or from a `.env` file):

```bash
cat > my-graph.yaml <<'EOF'
loading:
  mode: bulk
  consumer_group_id: quickstart
nodes:
  - label: Person
    topic: quickstart-person
    key_property: person_id
    properties:
      person_id: {type: integer, required: true, constraint: {type: uniqueness}}
      name: {type: string, required: true}
  - label: City
    topic: quickstart-city
    key_property: name
    properties:
      name: {type: string, required: true, constraint: {type: uniqueness}}
edges:
  - type: LIVES_IN
    topic: quickstart-lives-in
    nodes: {source: Person, target: City}
    source_key_property: person_id
    target_key_property: name
    properties:
      since: {type: date}
EOF
```

Validate it with the installed command, then with the public Python API:

```bash
graph-loader validate-schema --config my-graph.yaml
python - <<'EOF'
from neo4j_graph_loader.schema import load_schema

schema = load_schema("my-graph.yaml")
print([node.label for node in schema.nodes], [edge.type for edge in schema.edges])
EOF
```

Expected output:

```text
Schema valid: 2 node types, 1 relationship types
['Person', 'City'] ['LIVES_IN']
```

A broken file exits `1` and names the problem, for example a missing required
section: `Schema validation failed for <path>: ... edges Field required`. The
package also carries two sample schemas you can print and use as a starting
point: `graph-loader schema-example --name social` (and `--name default`).
`graph-loader validate-schema` with no `--config` validates the bundled default.
Field meanings are in [USAGE section 3](USAGE.md#3-the-yaml-schema).

## Quick start 2: plan relationships into Kafka

Producer planning takes relationship rows you already have (here three, in
memory), groups them into node-disjoint chunks, publishes the chunks to a Kafka
topic it creates, and writes a plan file. It does not read or write Neo4j. The
loader later consumes the plan with `graph-loader start --mode bulk --edge-plan`.

Name your broker explicitly. Replace `localhost:9092` with your broker's
address; the script reads it from the environment and fails if it is not set:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

Save the planning script as `plan_edges.py` and run it:

```bash
cat > plan_edges.py <<'EOF'
import os

from neo4j_graph_loader.planning import PlanSettings, plan_and_publish_edges
from neo4j_graph_loader.schema import load_schema

schema = load_schema("my-graph.yaml")
rows = [
    {"source": {"person_id": 1}, "target": {"name": "Paris"}, "properties": {"since": "2020-05-01"}},
    {"source": {"person_id": 2}, "target": {"name": "London"}, "properties": {"since": "2021-03-15"}},
    {"source": {"person_id": 3}, "target": {"name": "Paris"}, "properties": {}},
]
result = plan_and_publish_edges(
    schema,
    {"LIVES_IN": rows},
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    plan_path="quickstart-plan.json",
    settings=PlanSettings(topic="quickstart-lives-in-chunks"),
)
print("plan file:", result.path)
print("digest:   ", result.digest)
print("rows:     ", result.plan.total_rows)
EOF
python plan_edges.py
```

Expected output (the digest differs on every run; on a freshly started broker
librdkafka may first print a harmless `GETPID ... Coordinator load in progress:
retrying` warning on stderr):

```text
plan file: /path/to/graph-loader-quickstart/quickstart-plan.json
digest:    <64 hexadecimal characters>
rows:      3
```

The planner needs a new, empty chunk topic. Running the script a second time
fails with `chunk topic quickstart-lives-in-chunks already exists` until you
delete that topic (see [Clean up](#clean-up-what-you-created)) or choose another
topic name in `PlanSettings(topic=...)`.

Each row is the relationship envelope the loader expects: `source` and `target`
carry the endpoint key properties named in the schema, `properties` is
mandatory (use `{}` when there are none). The mapping key `LIVES_IN` must be a
relationship type in the schema; a type that feeds several topics or label pairs
needs the source classes described in [Public library API](LIBRARY_API.md).

Check that Kafka holds what the plan says it published:

```bash
python - <<'EOF'
import json
import os

from confluent_kafka import Consumer, TopicPartition

plan = json.load(open("quickstart-plan.json"))
topic = plan["topic"]
consumer = Consumer({
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "group.id": "quickstart-check",
    "enable.auto.commit": False,
})
partitions = consumer.list_topics(topic, timeout=30).topics[topic].partitions
messages = sum(
    high - low
    for low, high in (consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=30) for p in partitions)
)
consumer.close()
assert messages == plan["stats"]["chunks"], (messages, plan["stats"]["chunks"])
print(f"OK: {messages} chunk messages in {topic}, as the plan says")
EOF
```

Expected output: `OK: 3 chunk messages in quickstart-lives-in-chunks, as the plan says`
(the number of chunks is chosen by the planner; the check compares Kafka with
the plan file rather than with a fixed number).

## Clean up what you created

Everything above is yours, and only these things are created: the working
directory with its virtual environment and files, and the one Kafka topic
`quickstart-lives-in-chunks`. `plan_and_publish_edges` keeps the topic after a
successful run so a loader can read it; you delete it:

```bash
python - <<'EOF'
import os

from confluent_kafka.admin import AdminClient

admin = AdminClient({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})
for future in admin.delete_topics(["quickstart-lives-in-chunks"], operation_timeout=30).values():
    future.result(30)
print("deleted topic quickstart-lives-in-chunks")
EOF
rm -f quickstart-plan.json plan_edges.py my-graph.yaml
deactivate
cd .. && rm -rf graph-loader-quickstart
```

If a run fails or is interrupted, a lock or failure marker may remain beside the
plan file (`quickstart-plan.json.lock`, `.failed`); see
[Public library API](LIBRARY_API.md#ownership-and-lifecycle) before removing a
lock. Do not delete topics or consumer groups you did not create. A throwaway
broker you started for this exercise is yours to stop and remove by the
container id you recorded when you started it.

## After the quick starts: loading

Loading the graph (`graph-loader start --mode bulk ...`) is a separate step that
needs Neo4j 5.26 Enterprise (the tested version; the APOC plugin is needed only for the
classic `--edge-classic` mode), Kafka and Docker, because the
command runs the loaders as containers from the versioned images
`ghcr.io/shashankshukla96/graph-loader-{cli,node,edge,clock}:<package version>`
(see [Docker execution](DOCKER_EXECUTION.md)). Two environment settings choose
where the load goes; the command does not read `.env` files:

| Setting | Meaning |
| --- | --- |
| `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD` | the Neo4j server (credentials are read from the environment, never from arguments) |
| `NEO4J_DATABASE` | the database to write to. Unset means the server's default database (`neo4j`). A named database must already exist (`CREATE DATABASE <name> IF NOT EXISTS WAIT`); the command never creates it. Names use ASCII letters, digits, dots and dashes. |

Producer planning itself ignores both: it never opens a Neo4j connection. The
end-to-end load walk-through is the [bulk load guide](BULK_LOAD.md).
