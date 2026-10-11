# How to run a bulk (initial) load

Related docs: [CLI reference](CLI.md), [Architecture](ARCHITECTURE.md), [Changelog](../CHANGELOG.md), [USAGE.md](USAGE.md), [INCREMENTAL_LOAD.md](INCREMENTAL_LOAD.md), [MONITORING.md](MONITORING.md).

This guide shows how to load a graph from Kafka into Neo4j with `python -m src.cli start --mode bulk`. It is a task-oriented companion to the full reference, [`docs/USAGE.md`](USAGE.md). Where the two overlap, USAGE.md has the fine print and this guide has the order of work. To add data to a graph that is already loaded, or to keep a graph in sync continuously, read [`docs/INCREMENTAL_LOAD.md`](INCREMENTAL_LOAD.md) after this one.

Every flag, default, log line, metric and path below was checked against the code on branch `feat/streaming-observability` (re-checked at commit `3d435be`). Things that could not be checked, or that the code and the older docs disagree on, are listed in [Notes / open questions](#18-notes--open-questions). The worked example in section 17 was assembled from commands that exist in the repository; it was not executed while this guide was written.

1. [What a bulk load is](#1-what-a-bulk-load-is)
2. [Prerequisites](#2-prerequisites)
3. [The schema](#3-the-schema)
4. [Publishing the data (the producer contract)](#4-publishing-the-data-the-producer-contract)
5. [The command](#5-the-command)
6. [Edge modes and how the default is chosen](#6-edge-modes-and-how-the-default-is-chosen)
7. [Planned mode tuning](#7-planned-mode-tuning)
8. [Lease mode tuning (and a word on classic)](#8-lease-mode-tuning-and-a-word-on-classic)
9. [What happens during a run](#9-what-happens-during-a-run)
10. [Invalid records and the DLQ in bulk](#10-invalid-records-and-the-dlq-in-bulk)
11. [Verifying the result](#11-verifying-the-result)
12. [Monitoring a bulk run](#12-monitoring-a-bulk-run)
13. [Stopping safely, and exit codes](#13-stopping-safely-and-exit-codes)
14. [Re-running and recovering](#14-re-running-and-recovering)
15. [Performance numbers](#15-performance-numbers)
16. [Troubleshooting](#16-troubleshooting)
17. [Worked example (examples/social)](#17-worked-example-examplessocial)
18. [Notes / open questions](#18-notes--open-questions)

---

## 1. What a bulk load is

A bulk load is a **bounded snapshot load**. You publish every record to the Kafka topics your schema declares, then run one command. The run:

1. captures the end offset (high watermark) of every topic partition it will read. That is the **boundary**;
2. loads every record up to the boundary, nodes first, then relationships once both of their endpoint labels are complete;
3. proves the load is complete (committed offsets equal the boundary, or the planned-mode completion ledger holds every chunk exactly once);
4. stops and removes the containers it started, and exits `0`.

If anything is appended to a topic after its boundary was captured, the run fails instead of loading half of it. A bulk run never "tails" a topic and never waits for more data: when the proof succeeds the process ends.

Every write is an idempotent `MERGE`, so a failed or repeated load is safe to run again (see [section 14](#14-re-running-and-recovering)). Bulk mode is for a finite, quiescent input set. For data that keeps arriving use `--mode stream` ([INCREMENTAL_LOAD.md](INCREMENTAL_LOAD.md), part B).

The CLI runs on the host. It applies the schema DDL to Neo4j, runs the planner processes itself (planned mode), and launches node loaders, a dispatcher and writer containers on a Docker network. A run is identified by a **run id** (`--run-id`, otherwise a random UUID hex) that names its containers, its chunk topic and its rejection files.

## 2. Prerequisites

| Component | What you need |
| --- | --- |
| Neo4j | **5.23 or later, Enterprise.** The compose file uses `neo4j:5-enterprise` and installs APOC, GDS and GenAI plugins on first start (internet needed once). **APOC is needed only for `--edge-classic`** (`apoc.lock.nodes`). The lease and classic writers use the `CALL (vars) { ... }` scoped-subquery syntax introduced in 5.23; planned mode does not use it and has no such lower bound ([USAGE.md section 2](USAGE.md#2-requirements-and-environment)). The target database must already exist; the CLI never creates it. |
| Kafka | Any broker reachable from the host **and** from the loader containers. The CLI needs permission to create topics for the control topic (`__graph_loader_control`, best effort), DLQ topics (when you enable `dead_letter`) and, in planned mode, the per-run chunk topic with a raised `max.message.bytes`. Where that is not allowed, use `--edge-leases` (also zero lock waits). |
| Topics | **Every node and relationship topic in the schema must exist and be fully published before `start`.** Bulk mode does not run the stream preflight; a missing topic surfaces as an error during the run ([troubleshooting](#16-troubleshooting)). The loader never creates data topics: the partition count is your producer's choice. The bundled compose broker has `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"`, which is a property of that broker, not something the loader relies on. |
| Docker | A local daemon. The CLI builds (unless `--skip-image-build`) and runs `graph-loader-node` and `graph-loader-edge`. The build context is the current directory, so **run `start` from the repository root**. `make build-images` builds `graph-loader-{node,edge,clock}:latest` (override the tag with `IMAGE_TAG=x`; point the CLI at other tags with `GRAPH_LOADER_NODE_IMAGE`, `GRAPH_LOADER_EDGE_IMAGE`, `GRAPH_LOADER_CLOCK_IMAGE`). The clock image is built only for `--mode stream`; bulk does not use it. Images embed `src/`: rebuild after changing code. |
| Python | 3.12 with `pip install -r requirements.txt` (pins the Neo4j driver to `neo4j>=5.20,<7`). |

### The local stack

`docker compose up -d neo4j kafka` starts both services from [`docker-compose.yaml`](../docker-compose.yaml) on the `graph-loader-net` network. Compose reads `NEO4J_PASSWORD` (it builds `NEO4J_AUTH` from it), so the variable must be set in the environment or in `.env`. `make dev` (`scripts/start_dev.sh`) does the same and additionally refuses to run when `.env` is missing; it waits for both services to be healthy and also starts `kafka-ui` (port 8080). CPU pinning (`NEO4J_CPUSET` default `0-7`, `KAFKA_CPUSET` default `8-11`) and `LOADER_CPUSET` for loader containers are described in [USAGE.md section 2](USAGE.md#2-requirements-and-environment); pin them if you care about throughput, because a noisy neighbour on Neo4j's CPUs slows everything.

### The `.env` caveat

`python -m src.cli` and the loader containers **never read `.env`**; they use the process environment only. The project `.env` may point at remote systems. `docker compose` and `make monitoring-up` read it, and so do `examples/imdb/publish_and_load_nodes.py` and `scripts/cleanup_imdb_local_load.sh` (exported variables win over `.env` in all of them). `examples/social/social_graph.py` and `examples/multi_topic/multi_topic_graph.py` do not read it. So export every connection variable yourself and print what the run will use before you start. For a local stack:

```bash
export NEO4J_URI=bolt://localhost:7687 NEO4J_USERNAME=neo4j NEO4J_PASSWORD=...   # NEO4J_DATABASE=<db> optional
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092                                     # as seen from the host
export LOADER_KAFKA_BOOTSTRAP_SERVERS=kafka:29092 LOADER_NEO4J_URI=bolt://neo4j:7687  # as seen from the containers
echo "neo4j=$NEO4J_URI db=${NEO4J_DATABASE:-<server default>} kafka=$KAFKA_BOOTSTRAP_SERVERS"   # never print the password
```

On the reference host there is a helper outside the repository, `/mnt/graph-loader/bench/local_env.sh [database]`, that exports the host-side variables (`NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD` read from the Neo4j container, `NEO4J_DATABASE`, `KAFKA_BOOTSTRAP_SERVERS`) for the local stack. It does not set the `LOADER_*` variables. If you do not have it, the block above is equivalent.

Two environment defaults to know about: `NEO4J_URI` falls back to `bolt://localhost:7687` and `NEO4J_PASSWORD` falls back to `changeme` when unset (`get_neo4j_credentials` in `src/cli.py`). A wrong password repeated against a shared Neo4j can trip `Neo.ClientError.Security.AuthenticationRateLimit` and lock other clients out for a while, so set the password explicitly.

### Memory and disk for the planner (planned mode)

Planned mode (the default) reads every relationship record once, in planner processes **on the host running the CLI**:

* **Planner memory.** Each of `--plan-workers` processes keeps at most `--plan-memory-mb` of data (default: a quarter of the available RAM divided over the processes, clamped to 256-4096 MiB) plus about 150 MB for the interpreter and Kafka client. Beyond the budget it spills to disk, so the input can be much larger than RAM and the plan is identical.
* **Spill disk.** About the uncompressed size of the relationship records plus roughly 10%, on `--plan-spill-dir` (default: the plan directory). Use a large local disk, not tmpfs. A full disk fails the plan closed with `planner spill directory ... is full`. Spill files are deleted when planning ends, also on failure.
* **Kafka.** The run's chunk topic holds every record once more (lz4 compressed, typically a third to a quarter of the JSON size), created with `retention.ms=-1`, `retention.bytes=-1` and 1 GiB segments, and deleted after the run.
* **Plan directory.** `edge-plan.json` is about 70 bytes per chunk. It must be on the CLI host: it is bind-mounted into the dispatcher container.

Sizing example from USAGE.md: about 450M relationship records (about 70 GB of JSON, about 90k chunks) plan in roughly 12-15 minutes on four cores.

## 3. The schema

A schema has `loading`, `nodes` and `edges`. The complete field reference is [USAGE.md section 3](USAGE.md#3-the-yaml-schema); this is the part a bulk user needs.

```yaml
loading:
  mode: bulk
  consumer_group_id: my-graph          # node loaders commit offsets under <id>-<Label>
nodes:
  - label: Person
    topic: person-events               # one topic per label
    key_property: person_id            # the MERGE key; must be declared and required
    properties:
      person_id: {type: integer, required: true, constraint: {type: uniqueness}}
      name: {type: string, required: true}
      joined: {type: date}
  - label: City
    topic: city-events
    key_property: name
    properties:
      name: {type: string, required: true}
edges:
  - type: LIVES_IN
    topic: lives-in-events             # one topic per relationship type
    nodes: {source: Person, target: City}
    source_key_property: person_id     # must equal the source label's key_property
    target_key_property: name          # must equal the target label's key_property
    properties:
      since: {type: date}
```

### Nodes

* `label` and `key_property` must be Cypher identifiers (`^[A-Za-z_][A-Za-z0-9_]*$`); so must every property name.
* Every node needs at least one property. The key property must be declared under `properties` with `required: true`.
* Property types: `string`, `integer`, `float`, `date`, `datetime`. Any can be a key. `constraint: {type: uniqueness | not_null}` and `index: {type: range | text}` generate DDL before loading. A key with neither a uniqueness constraint nor a range index automatically gets the constraint `<label lowercased>_<key lowercased>_key_unique`; planned mode depends on every key being indexed.
* `replicas` (default `1`) is the number of node-loader containers for the label. When you omit it in bulk mode the CLI derives it from the topic: 1 below 10,000 retained records, otherwise one per partition up to 8 (`--node-replicas-min-records`, `--max-node-replicas`). The result is logged as `stage=launch node replicas Person=4 City=1`.

### Relationships

* **Every relationship type needs at least one property** (`Edge '<type>' must have at least one property.`). If you have nothing to store, declare a dummy optional one.
* `nodes.source` / `nodes.target` name declared labels (they may be the same label); `source_key_property` / `target_key_property` must equal the respective label's `key_property`.
* Exactly one of `topic` or `topics` per type. Each topic belongs to one node label or one relationship type, never two, and never the coordination topic.
* `replicas`, `mix_and_batch`, `retry` and `execution` are classic and lease-mode tuning. Planned mode ignores them. Keep `execution.mode` at its default `python_apoc` unless you need `native_disjoint` ([section 6](#6-edge-modes-and-how-the-default-is-chosen)).
* `loading.mode` in the YAML is informational for validation: the CLI's `--mode` decides the run. One catch: if you write `loading.mode: stream` explicitly, a `topic_conflict: precedence` type is rejected at load even for a bulk run. Write `loading.mode: bulk` (or omit it) for schemas that use `precedence`.

### Several topics per type: the conflict rule

A type with two or more topics must say how a `(source, target)` pair that appears in more than one topic is resolved. There is no default.

| Rule | Meaning | In bulk |
| --- | --- | --- |
| `order_by: <property>` | The newest value wins; a tie keeps the stored value. A record that carries no value for the property is applied unconditionally. Use a `date`, `datetime`, `integer` or `float` property. | planned, lease, classic |
| `topic_conflict: precedence` | The later-listed topic wins, property by property (`SET r += props`). | planned (chunks ordered by topic index), lease and classic (topics run as sequential sub-stages) |
| `topic_conflict: arrival` | Whichever record arrives last wins. Nondeterministic; a warning is logged at load. | all |

`order_by` together with `precedence` or `arrival` is an error, as is `topic_conflict: order_by` without `order_by`.

```yaml
edges:
  - type: RATED
    topics: [ratings-app, ratings-import]
    order_by: rated_at
    nodes: {source: Person, target: Movie}
    source_key_property: person_id
    target_key_property: movie_id
    properties:
      rated_at: {type: datetime, required: true}
      stars: {type: integer}
```

One type can also connect **several label pairs** (for example `LIKES` Person->Post and Person->Comment). Each topic binds exactly one label pair, either through a `topics` mapping entry `{name, nodes, source_key_property, target_key_property}` or through the type's top-level pair. The pair is taken from the topic and never from the record. A conflict rule is required only when some pair is fed by two or more topics. See [USAGE.md "Several label pairs per type"](USAGE.md#several-label-pairs-per-type) and [`examples/multi_topic`](../examples/multi_topic/README.md).

### `dead_letter`

```yaml
dead_letter:
  enabled: true                          # the block alone does not opt in when enabled is false
  route_missing_endpoints_in_bulk: true  # relationships only; see section 10
```

`dead_letter` is accepted on nodes and relationship types. `enabled` defaults to `true` once the block is present; `max_retry_attempts` defaults to 5 (1-1000); `topic` defaults to `<entity topic>.dlq` (`<type>.dlq` for a type with two or more topics); `parked_topic` defaults to `<DLQ topic>.parked`. On a node, `max_retry_attempts`, `parked_topic` and `route_missing_endpoints_in_bulk` are rejected when set to anything but their default. **Declaring an enabled relationship `dead_letter` changes which bulk edge mode a plain `start` uses** (it switches to leases, [section 6](#6-edge-modes-and-how-the-default-is-chosen)). What it does in bulk is in [section 10](#10-invalid-records-and-the-dlq-in-bulk).

## 4. Publishing the data (the producer contract)

Publish JSON values to the declared topics, then start the load. The loader does not care what produces the records (any language, Kafka Connect, a script). [`examples/social/social_graph.py`](../examples/social/social_graph.py) is a small reference producer.

| Record | Value | Kafka key |
| --- | --- | --- |
| Node | the node's properties by name, e.g. `{"person_id": 7, "name": "Ada", "joined": "2024-01-31"}` | the node key as text |
| Relationship | the endpoint envelope `{"source": {"person_id": 7}, "target": {"name": "Paris"}, "properties": {"since": "2020-05-01"}}`. All three sections are required (use `"properties": {}` when there is nothing to store); an extra top-level section is invalid | the **source key** as text |

Rules:

* **Key relationship records by source key.** With the default partitioner every record of one `(source, target)` pair then lands in one partition. Kafka orders records only within a partition, and that order is what "last write wins" follows. Planned mode applies a pair's records in `(partition, offset)` order (for several topics, `(topic index, partition, offset)`), so records of one pair spread over several partitions are applied in a deterministic but not time-ordered way. The loader never parses a relationship message's key; the key only drives partitioning.
* **One label pair per topic.** A topic of a multi-pair type carries only records of the pair it binds, with that pair's key property names.
* **Only declared properties.** An undeclared field, a missing required field or a wrong type makes a record invalid ([section 10](#10-invalid-records-and-the-dlq-in-bulk)). A `null` optional property is dropped from the write, so it never clears a stored value.
* **Wire formats.** Dates `YYYY-MM-DD`; datetimes ISO-8601 with `T` and an offset (`2024-01-31T12:00:00+01:00` or `...Z`); numbers must be finite JSON numbers; `integer` properties must be JSON integers; `float` properties also accept integers.
* **Publish everything before you start.** Nodes and relationships can be published in any order relative to each other, but all of it must be there and flushed before `start`. Records produced after a topic's boundary fail the run.
* **Partitions.** Node loaders parallelize per partition, so give large node topics at least as many partitions as you want replicas (4 is a good default). Small label topics can have one partition. Relationship topics can have any partition count.

A minimal producer for the schema in section 3 (a sketch following `examples/social/social_graph.py`; adjust the replication factor for a real cluster):

```python
import json
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = "localhost:9092"
admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
existing = admin.list_topics(timeout=20).topics
wanted = {"person-events": 4, "city-events": 1, "lives-in-events": 4}
new = [NewTopic(t, num_partitions=n, replication_factor=1) for t, n in wanted.items() if t not in existing]
for future in (admin.create_topics(new).values() if new else ()):
    future.result(30)

producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True, "compression.type": "lz4"})

def send(topic, key, value):
    producer.produce(topic, key=str(key).encode("utf-8"), value=json.dumps(value, separators=(",", ":")).encode("utf-8"))

send("person-events", 7, {"person_id": 7, "name": "Ada", "joined": "2024-01-31"})
send("city-events", "Paris", {"name": "Paris"})
send("lives-in-events", 7, {"source": {"person_id": 7}, "target": {"name": "Paris"}, "properties": {"since": "2020-05-01"}})
if producer.flush(120):
    raise RuntimeError("Kafka did not deliver every record")
```

## 5. The command

```bash
python -m src.cli start --mode bulk --config my_graph.yaml
```

The command blocks until the load has been verified, then exits `0`. Any failure (invalid input, writer crash, timeout, operator stop) exits `1` after stopping every container the run started. The flags below are all of `start`'s options that matter for bulk, grouped. The numeric range checks ("`--plan-writers must be 1-64`" and so on) run before any side effect; `--bulk-timeout-seconds` being positive is the exception, checked only after the DDL, the planner start and the image build.

### Core

| Flag | Default | Meaning |
| --- | --- | --- |
| `--config PATH` | `config/graph_schema.yaml` | Schema YAML. |
| `--mode {bulk,stream}` | required | `bulk` for this guide. |
| `--network NAME` | `graph-loader-net` | Docker network for every container the run starts. Must reach Kafka and Neo4j under the `LOADER_*` addresses. |
| `--skip-image-build` | off | Use the existing `graph-loader-node` / `graph-loader-edge` images instead of building them. |
| `--run-id ID` | random UUID hex | Explicit run identity: 1-128 characters, `[A-Za-z0-9][A-Za-z0-9._-]*`. Names containers, the chunk topic and rejection files. See [section 14](#14-re-running-and-recovering). |
| `--bulk-timeout-seconds N` | `300` | Stall limit of each monitored bulk stage (must be positive). Long stages are progress-based: node draining, waiting for the edge plan (the planner's heartbeat) and the planned dispatch fail only after `N` seconds **without progress**. |
| `--node-replicas-min-records N` | `10000` | A label whose YAML omits `replicas` gets 1 loader when its topic retains fewer records than this (`0` = always the YAML value, i.e. 1). |
| `--max-node-replicas N` | `8` | Cap on derived node-loader replicas. |
| `--accept-former-dlq TOPIC` | none | Repeatable. Start although a relationship type's former default DLQ still holds records ([INCREMENTAL_LOAD.md migration notes](INCREMENTAL_LOAD.md#5-migration-notes-single-topic-to-multi-topic)). |
| `--accept-dlq-leftovers TYPE` | none | Repeatable. Start although a type's parked-pair index holds records of a topic it no longer reads. |

### Edge mode (give at most one)

| Flag | Meaning |
| --- | --- |
| `--edge-plan [PLAN_FILE]` | Planned mode. Without a file the loader plans from the schema's relationship topics while the nodes load. With a file it loads the plan a producer wrote (waited for up to `--bulk-timeout-seconds` of no progress). |
| `--edge-leases` | Bucket-lease mode (zero lock waits, per-type topics, supports relationship `dead_letter`). |
| `--edge-classic` | Classic per-type APOC-locked loaders. The one mode that can wait on Neo4j locks. |

### Planned-mode tuning (only with planned mode; ignored with `--edge-plan FILE`)

| Flag | Default | Range | Meaning |
| --- | --- | --- | --- |
| `--plan-writers K` | Neo4j available processors + 2 (10 if the probe fails); never more than the plan's chunks | 1-64 | Writer containers shared by all relationship types. |
| `--plan-workers W` | `min(4, CPUs)` | 1-64 | Planner processes reading the relationship topics. |
| `--plan-buckets B` | derived: `clamp(K, 8, 64)`, or `clamp(round(K x (1 + s)), 8, 64)` when self-referencing pairs carry a share `s > 0.10` of `R`; at most `isqrt(R / (T x 500))`, at least 2 | 1-4096 | Hash buckets per node label. An explicit value always wins. |
| `--plan-hub-degree H` | the chunk size | 0-2^40 | Minimum total degree for a node to get its own resource. `0` disables hubs. |
| `--plan-chunk-rows C` | `clamp(ceil(R / (20 x K)), 500, 5000)` | 1-100000 | Maximum rows per chunk transaction. |
| `--plan-partitions N` | `4` | 1-1024 | Partitions of the run's chunk topic. |
| `--plan-topic NAME` | `graph-loader-edge-chunks-<run id>` | | Chunk topic to create. It must not exist yet. |
| `--plan-dir DIR` | `$GRAPH_LOADER_TIMING_DIR`, else `var/edge-plans/<run id>` | | Where `edge-plan.json` and its ledger go. |
| `--plan-memory-mb M` | a quarter of available RAM over the planner processes, 256-4096 | 16-1048576 | Memory budget of each planner process. |
| `--plan-spill-dir DIR` | the plan directory | | Planner spill files, removed afterwards. |
| `--keep-plan-topic` | off | | Keep the chunk topic after the run. |
| `--replace-stale-plan-topic` | off | | With an explicit `--run-id` and the default chunk topic name: delete a chunk topic left by a killed run with that id, but only if no container of that run is alive. |

(`R` = relationship records, `T` = relationship types with records, `K` = writers, `s` = share of `R` in self-referencing label pairs such as Person->Person; for a type of several label pairs only its same-label topics count.)

### Lease-mode tuning

| Flag | Default | Range | Meaning |
| --- | --- | --- | --- |
| `--bucket-lease-buckets` | `16` | 1-4096 | Stable hash buckets per node label. |
| `--bucket-lease-outer-batch-size` | `32000` | 1-1,000,000 | Relationship outer flush size, split into bucket cells. Overrides `loading.edge_unwind_batch_size`. |
| `--bucket-lease-keyed-max-nodes` | `1000` | >= 0 | Endpoint labels whose node topic retains at most this many records are leased per node key instead of per bucket (`0` = never). |

`--lease-buckets`, `--lease-outer-batch-size` and `--lease-keyed-max-nodes` are deprecated aliases (one `stage=deprecation` line is logged; giving both spellings with different values is an error).

### Classic-mode tuning (only with `--edge-classic`)

| Flag | Default | Meaning |
| --- | --- | --- |
| `--bulk-edge-overlap {labels,shared}` | `labels` | `labels` serializes relationship types that share an endpoint label; `shared` runs them concurrently under globally ordered APOC node locks and also schedules self-referencing types alongside them. |
| `--max-concurrent-bulk-edges N` | `0` (unlimited) | Cap on concurrently running relationship types. |

Without `--edge-classic` these two are **ignored with a warning in bulk** (in `--mode stream` they are ignored silently; with `--edge-leases` the overlap is forced to `shared`): `--bulk-edge-overlap/--max-concurrent-bulk-edges tune classic only; ignored (add --edge-classic to use classic)`.

### Stream-only flags (do nothing in bulk)

`--retry-consumer-replicas` (default `1`), `--stream-group-member-wait-seconds S` (stable consumer-group member wait; see [INCREMENTAL_LOAD.md](INCREMENTAL_LOAD.md#b10-graceful-stop-restart-consumer-groups-and-resume)) and the deprecated `--stream-clock-rotation`. `--edge-classic` is rejected with `--mode stream`. Other commands: `status`, `stop`, `replay-dlq` ([sections 10 and 13](#10-invalid-records-and-the-dlq-in-bulk)).

## 6. Edge modes and how the default is chosen

All modes produce the same graph. They differ in how relationship writes are scheduled and what they can do about invalid records.

| Mode | Flag | Lock waits | Speed (IMDb 20k, [section 15](#15-performance-numbers)) | Invalid relationship records | Needs |
| --- | --- | --- | --- | --- | --- |
| **Planned** (default) | none, or `--edge-plan` | none, by construction | fastest | planner rejection file; `dead_letter` ignored | topic creation, planner memory and spill disk |
| Planned, producer plan | `--edge-plan PLAN_FILE` | none | fastest; skips the per-type edge topics | the producer API fails on an invalid record | a producer that writes the plan |
| **Bucket leases** | `--edge-leases` (chosen automatically for DLQ schemas) | none | about 1.7-1.8x the planned wall time | rejection file, or the DLQ when `dead_letter` is enabled | a lease-server container |
| Classic | `--edge-classic` | frequent (explicit ordered APOC locks prevent deadlocks, not waiting) | about 4.8-5.2x the planned wall time | rejection file, or the DLQ when `dead_letter` is enabled | APOC |

**How planned mode gets zero waits.** Every node belongs to exactly one scheduling resource (a hash bucket of its label, or a resource of its own if it is a hub). Relationship transactions that run at the same time never share a resource, so they never share a node, so Neo4j never makes one wait for another's locks and deadlocks cannot occur. Lease mode gets the same guarantee at run time from a lease server instead of a precomputed plan.

### How a plain `start --mode bulk` picks its mode

`_resolve_bulk_edge_mode` in `src/cli.py` decides, after the schema is loaded:

1. If you gave `--edge-plan`, `--edge-leases` or `--edge-classic`, that is the mode (`reason=flag`). Giving two or more is refused: `--edge-plan and --edge-leases are mutually exclusive; choose one relationship mode` (the message names the flags you gave).
2. Otherwise, if any relationship type has an enabled `dead_letter` (`enabled: true`, which also covers a custom `topic`, `route_missing_endpoints_in_bulk`, `max_retry_attempts` and `parked_topic`; a block with `enabled: false` does not count), the mode is **leases** (`reason=dead_letter`), because planned mode ignores `dead_letter`. Node `dead_letter` has no influence.
3. Otherwise the mode is **planned**, with every planned option derived exactly as for a bare `--edge-plan` (`reason=default`).
4. A plain start **never reaches classic.** If any relationship type has `execution.mode: native_disjoint` (disjoint by pair, not by node, so it can wait on endpoint locks and neither zero-wait mode supports it) and you gave no mode flag, the start is refused:
   `relationship type(s) X use execution.mode native_disjoint, which neither zero-wait mode supports; pass --edge-classic (can wait on Neo4j locks) or use python_apoc`.

The choice is logged once: `stage=launch edge_mode=<planned|lease|classic> reason=<default|dead_letter|flag>`. A schema with no relationship types skips all of this.

| Your schema / flags | Mode that runs | Logged |
| --- | --- | --- |
| no flags, no enabled relationship `dead_letter`, all `python_apoc` | planned | `edge_mode=planned reason=default` |
| no flags, some relationship `dead_letter.enabled: true` | leases | `edge_mode=lease reason=dead_letter` |
| no flags, some `native_disjoint` type | refused | (error: pass `--edge-classic` or use `python_apoc`) |
| `--edge-plan` (with or without a DLQ-enabled type) | planned. Relationship `dead_letter` is ignored and its DLQ topics are not even created; invalid records go to the planner rejection files | `edge_mode=planned reason=flag` |
| `--edge-plan PLAN_FILE` | planned from the producer's plan | `edge_mode=planned reason=flag` |
| `--edge-leases` | leases | `edge_mode=lease reason=flag` |
| `--edge-classic` | classic (the only way to run `native_disjoint`) | `edge_mode=classic reason=flag` |
| two of the three flags | refused | (mutually exclusive) |
| `--edge-plan` or `--edge-leases` with a `native_disjoint` type | refused | `--edge-plan requires python_apoc edges; unsupported: X` / `--edge-leases requires python_apoc edges; unsupported: X` |

Which one should you pick? Leave it at the default unless: you need relationship DLQ routing in bulk (leases, automatic), your Kafka does not let the CLI create the chunk topic or you cannot afford planner memory and spill disk (`--edge-leases`), you need `native_disjoint` or a comparison against the old behaviour (`--edge-classic`), or your producer already holds all edges and can plan while it publishes (`--edge-plan PLAN_FILE`, [USAGE.md section 7](USAGE.md#7-planning-in-the-producer-optional)).

> **Migration note.** Classic used to be the default. A command that relied on that must now add `--edge-classic`. Compared with classic, planned mode writes invalid relationship records to a per-planner-worker rejection file, does not apply `dead_letter`, re-plans and re-reads everything on a rerun (it does not resume), and needs topic-creation rights and spill disk.

## 7. Planned mode tuning

For almost every graph the derived settings are right and no `--plan-*` flag is needed. They were tuned on IMDb and a synthetic social graph; try the flags below when degree distributions are unusual or the machine is unusual. Derivation lives in `src/orchestrator/auto_settings.py` and the chosen values are logged:

```
stage=edge-plan-settings records=... edge_types=... writers=... buckets=... hub_degree=... chunk_rows=... planner_workers=... memory_mb=... spill_dir=... topic=... plan=... self_ref_records=... buckets_reason=...
```

| Knob | Why and when |
| --- | --- |
| **Writers** (`--plan-writers`, `K`) | Neo4j is the bottleneck, so `K` is set slightly above its core count to cover commit and network gaps. More writers than Neo4j has cores does not help once it is saturated. Lower it only to leave headroom for other work on the database. |
| **Buckets** (`--plan-buckets`, `B`) | `B` must be large enough that about `K` chunks can run at once without sharing a bucket, and small enough that units stay big. Small inputs automatically get fewer buckets (a unit should average at least 500 rows). A self-referencing pair (for example `FRIEND_OF` Person->Person) holds two buckets of one label per chunk, so with `B = K` only about `K / 2` of its chunks fit at once. When such pairs carry a share `s > 0.10` of the records (counted from the boundary offsets; for a type of several label pairs only the same-label topics count), `B = clamp(round(K x (1 + s)), 8, 64)`, still under the volume cap. Below the threshold (IMDb: 0.3%) `B` is exactly `clamp(K, 8, 64)`. Measured on the social example with 80% / 93% `FRIEND_OF` rows (B 10 -> 18 / 19): mean concurrent chunks 6.0 -> 9.2 / 5.2 -> 9.4, relationship dispatch -25% / -40%, wall -10% / -22% (medians on a noisy shared host), zero lock waits. `buckets_reason` in the settings line shows which rule applied. |
| **Chunk rows** (`--plan-chunk-rows`, `C`) | Each transaction costs a few ms of fixed overhead plus roughly 15-25 microseconds per row, so chunks are kept large (500-5000 by default) while leaving about 20 chunks per writer for load balancing. |
| **Hub degree** (`--plan-hub-degree`, `H`) | A node whose total degree over all types is at least `H` gets its own resource so its necessarily serial rows do not block the rest of its bucket (for example `Genre:Drama`). `0` disables hubs. |
| **Planner workers** (`--plan-workers`) | Scale with cores. Planning measured 590-660k records/s on four processes (four cores). |
| **Memory budget** (`--plan-memory-mb`) | Budget per planner process. Share of it: 35% read-record buffers, 15% endpoint-key window, 40% one cell while it is cut, 10% + 20% Kafka prefetch and producer queue. Plans are identical whether or not the planner spilled. A 1 MiB budget is a test setting, not a supported one: Kafka client queues have floors of 16 MiB or more per process. |
| **Spill** (`--plan-spill-dir`) | Point at a large local disk. Each planner needs about the uncompressed size of its relationship records plus roughly 10%. |
| **Chunk topic** (`--plan-topic`, `--plan-partitions`, `--keep-plan-topic`) | The topic is created per run with `max.message.bytes` raised (a chunk is capped at 8 MiB, a message at 16 MiB) and unlimited retention, and deleted after the run unless you keep it. Keep it only to inspect a failed plan; a kept topic of an aborted plan is partial. |

Environment knob: `GRAPH_LOADER_PLAN_NODE_PHASE_WRITERS=N` caps the active chunks while node labels are still loading so large node labels get more Neo4j CPU. The default `0` (unlimited) measured as fast or faster on IMDb.

### Plan files

In `--plan-dir` (default `$GRAPH_LOADER_TIMING_DIR`, else `var/edge-plans/<run id>`; the CLI resolves it to an absolute path):

| File | Meaning |
| --- | --- |
| `edge-plan.json` | The plan: every chunk with type, resources, rows, priority, predecessor and Kafka offset, plus the topic list of each type. Written when planning completes; the dispatcher starts from it. |
| `edge-plan.json.progress` | Heartbeat the planner rewrites while it reads, counts and publishes. The CLI's wait for the plan is progress-based on it. |
| `edge-plan.json.failed` | Written instead of the plan when the planner fails; its text is the reason. |
| `edge-plan.json.ledger.json` | The verified completion ledger (`plan_id`, `plan_digest`, `state`, `chunks`, `rows`, one entry per chunk), written and fsync'd at the end of a successful run. |

At the start of a run the CLI deletes a previous `edge-plan.json`, its `.failed`, `.progress` and ledger files in the same directory, and a previous `var/rejections/<run id>-planner-*.jsonl` of the same run id. With `GRAPH_LOADER_TIMING_DIR` set you also get `planned-summary.json`, `planned-ledger.json`, `planned-dispatcher.log` and per-type `stage-<type>.json`.

### `--edge-plan PLAN_FILE`: planning in the producer

A producer that already holds every relationship can plan while it publishes nodes and skip publishing and re-reading the per-type edge topics:

```python
from src.loader.edge_planner import PlanSettings, plan_and_publish_edges
from src.utils.schema_loader import load_schema

schema = load_schema("my_graph.yaml")
rows = {"LIVES_IN": [{"source": {"person_id": 7}, "target": {"name": "Paris"}, "properties": {}}]}
plan_and_publish_edges(schema, rows, bootstrap_servers="localhost:9092",
                       plan_path="plans/edge-plan.json", settings=PlanSettings(topic="my-graph-edge-chunks"))
```

```bash
python -m src.cli start --mode bulk --config my_graph.yaml --edge-plan plans/edge-plan.json
```

The CLI waits for the file (up to `--bulk-timeout-seconds` without progress), validates it against the schema (edge types, endpoint labels, topic lists, label pairs; a mismatch says `re-plan the load`) and checks that the plan's chunk topic retains exactly the plan's messages. Records in a producer plan are validated by the producer API, which fails on an invalid record. On the IMDb 40k subset this path took 107.8 s end to end against 122.4 s through the per-type topics, almost entirely the cost of publishing 19.2M relationship records. For large inputs pass picklable `EdgeRecordSource` objects; see [USAGE.md section 7](USAGE.md#7-planning-in-the-producer-optional).

### `--replace-stale-plan-topic`

A `kill -9`ed planned run leaves its chunk topic. Starting again with the same explicit `--run-id` then fails with `chunk topic graph-loader-edge-chunks-<id> already exists; planned runs need a new, empty topic ...`. Either use a new run id, or add `--replace-stale-plan-topic`: the CLI deletes the topic (waiting up to 30 s for Kafka to finish) after checking that no container carrying that `run_id` label is still alive; if one is, the start is refused and names them. The flag only applies to the default chunk topic name (not with `--plan-topic`) and to planned runs that plan from the edge topics.

## 8. Lease mode tuning (and a word on classic)

`--edge-leases` starts one run-scoped lease-server container (the edge image running `src.orchestrator.lease_server`). Every relationship transaction leases the exclusive node-disjoint set of resources it touches before writing, so concurrently running transactions never wait on Neo4j locks. All relationship types, including self-referencing ones, run concurrently (it forces `--bulk-edge-overlap shared`), and the `apoc.lock.nodes` step is skipped. Each type still runs as its own stage with its own boundary, completion and zero-lag proof. If the lease server is unreachable or dies, the edge loaders fail and the run fails (in bulk the coordinator is not supervised or restarted; only stream mode does that).

Resources and flushes:

* Each label has `--bucket-lease-buckets` (default 16) hash buckets. Labels whose node topic retains at most `--bucket-lease-keyed-max-nodes` records (default 1000, and more than 0) get **one resource per node** instead, so hub nodes such as IMDb's `Drama` and `Comedy` never block each other. The choice is logged: `stage=lease-server address=... buckets=16 keyed_labels=Genre,... outer_batch_size=32000` and written to `lease-settings.json` in `GRAPH_LOADER_TIMING_DIR` when set. Retained record counts over-count nodes (updates, duplicates), which only makes keying more conservative.
* Each outer flush (`--bucket-lease-outer-batch-size`, default 32000) is split into `(source resource, target resource)` cells. Writers take the largest leasable cells and cap each transaction at `mix_and_batch.batch_size` (default 1000). With too small a flush the cells become tiny; with 16 buckets, 32000 rows give about 125 rows per cell for two large labels.
* Schema knobs that matter in lease mode (and in classic): `replicas` (edge-loader containers per type, 1-64; replica `i` of `N` statically owns entries `i, i+N, ...` of the sorted `(topic, partition)` list, so more replicas than partitions only idle), `execution.worker_count` (concurrent write sessions per replica, per label pair, 1-64), `mix_and_batch.batch_size`, and the fencing settings under `loading.bucket_lease` (`tx_timeout_ms` default 30000, `restart_quiet_margin_ms` default 5000: the lease server holds a disconnected owner's resources for `tx_timeout_ms + margin` so a transaction that is still open on the server can never overlap another writer). `--edge-leases` requires `tx_timeout_ms` of at least 6000. Each leased BEGIN gets a timeout of the time left on its lease minus 4 s, and a BEGIN that is too slow is rolled back and re-granted (up to 16 times per flush) rather than risking a lock wait. Planned and classic bulk use `tx_timeout_ms` as a flat timeout and have no such minimum.
* A `topic_conflict: precedence` type runs its topics as **sequential sub-stages** in `topics:` order, each with its own consumer group, boundary and zero-lag proof; the next topic starts only after the previous one is proved drained. Other types keep their parallelism. Log lines: `stage=bulk-substage edge=<type> topic=<t> rank=<i> run_id=<sub run id> start` and `stage=bulk-substage-complete edge=<type> topic=<t> rank=<i> elapsed_s=<s> outcome=<...>`.

**Classic** (`--edge-classic`) uses the same per-type stage structure without leases: types that share an endpoint label are serialized (`--bulk-edge-overlap labels`) or overlapped under globally ordered APOC locks (`shared`). It is the only mode where Neo4j can make a transaction wait; in the IMDb 20k run about 12k transactions waited for roughly 106-113 seconds in total ([section 15](#15-performance-numbers)). Use it for `native_disjoint`, debugging, or comparison with earlier runs. Setting `GRAPH_LOADER_EDGE_EXPLICIT_LOCKS=0` in the environment removes the explicit locks and is expected to deadlock; the locks are required in classic mode.

## 9. What happens during a run

### Phases (planned mode)

```
host CLI                                               containers
--------                                               ----------
validate flags and schema, resolve the edge mode
create DLQ topics (nodes always; relationships only when not planned)
apply schema DDL (constraints, indexes); probe Neo4j cores
start planner processes (they capture the relationship boundary)
build images (unless --skip-image-build)
create control topic; resolve node replicas
                                                       launch node loaders for every label
   +-- per label: coverage -> boundary -> committed to boundary -> stop -> drain -> exact check -> "ready"
   +-- planner: read low..boundary of each relationship topic -> write edge-plan.json -> publish chunks
wait for the plan; validate it against the schema
                                                       launch dispatcher + K writers
                                                       chunks granted only when both endpoint labels are ready
verify ledger: every chunk exactly once, rows match; writers exit 0
exit 0
```

Planning overlaps node loading, so on inputs where the node phase takes longer than planning it adds no wall time. Chunks are granted highest priority first (busiest resources and longest hub chains first), so no serial tail is left at the end.

### Node-label readiness

Labels are independent: each advances `joining -> running -> draining -> ready` on its own, so a tiny label is not held behind a huge one. For each label the CLI's `BulkMonitor`:

1. **Proves exact assignment coverage.** Every replica acknowledges the partitions it owns on the control topic `__graph_loader_control`; the union must be exactly the label's topic partitions, each owned once. Surplus replicas report themselves idle.
2. **Captures the label's boundary**: the high watermark of each of its partitions.
3. **Waits for durable progress**: committed offsets of the label's consumer group (`<consumer_group_id>-<Label>`) must reach the boundary. A partition that never held a record (low = high at the capture) needs no commit.
4. **Stops the label's containers** (SIGTERM). Each loader flushes, commits and publishes a drain acknowledgement.
5. **Proves zero lag**: after every replica has acknowledged the drain, the committed offsets must **equal** the boundary exactly, and no topic's high watermark may have moved since the capture.
6. Logs `stage=node-label-ready label=<Label> elapsed_s=<seconds since readiness started>` and tells the planned dispatcher (or the relationship scheduler) that the label is ready.

A relationship chunk (planned) or stage (lease/classic) starts only when both of its endpoint labels are ready, so `MATCH` always finds its endpoints.

### Fail-closed rules

The run fails, stops its containers and exits `1` when any of these happens; nothing is ever reported as a success while skipping data:

* a topic's end offsets move after the boundary was captured (`bulk input changed topic=... partition=... boundary=... current=...` for nodes; `relationship topic <t> changed after the bulk boundary was captured (end offsets {...} -> {...}); publish every relationship before starting the load` for planned relationship topics);
* committed offsets do not equal the boundary after drain (`nonzero lag ...`);
* a loader or writer container exits before it should (`loader exited label=... replica=... status=... exit_code=...`; the last log lines are printed as `stage=diagnostics`);
* a stage makes no progress for `--bulk-timeout-seconds` (`timed out waiting for ...`);
* a chunk's rows do not each match exactly one source and one target node (the transaction rolls back and the run fails);
* the completion ledger does not hold every planned chunk exactly once with its planned row count, or per-type or total rows differ from the plan;
* a writer disconnects while it holds a chunk (its resources are never handed to another writer while its transaction might still be committing).

Bulk mode never rides out a Neo4j error (only leased stream writers do): it fails fast, and rerunning completes the load.

### Log lines worth watching

All are emitted as `stage=...` key/value lines by the CLI (stderr, format `LEVEL: message`) unless noted:

| Line | When |
| --- | --- |
| `Starting pipeline in bulk mode with config <path>` | start |
| `stage=launch edge_mode=<...> reason=<...>` | mode chosen |
| `stage=launch ensured dead-letter topic <t> (source=...)` | a DLQ topic was created or verified |
| `Schema initialization complete. Ready for ingestion.` | DDL applied |
| `stage=launch planned writers=<K> (Neo4j cores=<n>)` | writer count derived |
| `stage=edge-plan-start`, `stage=edge-plan-settings`, `stage=edge-plan-memory`, `stage=edge-plan-scan`, `stage=edge-plan-ready` | planner lifecycle (`-ready` carries chunks, rows, resources, hubs and timings) |
| `stage=launch node replicas A=1 B=4` and `Started node loader for <Label>` | node fleet |
| `stage=node-label-ready label=<Label> elapsed_s=<s>` | a label is complete |
| `stage=planned-edges plan_id=... chunks=... rows=... resources=... hubs=... writers=... wait_s=...` | plan accepted, dispatcher starting |
| `stage=planned-edges ledger=<path>` then `stage=planned-edges-complete plan_id=... chunks=... rows=... elapsed_s=... <Type>=<rows> ...` | **success of the relationship phase** |
| `stage=edge-plan-rejections plan_id=<id> rejected=<n> dir=<dir>` and `stage=outcome rejected=<n> malformed relationship record(s) were skipped; see var/rejections/<run id>-planner-*.jsonl` | only when records were rejected |
| `stage=bulk-edge-complete edge=<type> elapsed_s=<s> outcome=<...>` | lease/classic: a type finished |
| `stage=lease-server address=... buckets=... keyed_labels=... outer_batch_size=...` | lease mode start |
| `Bulk run failed: <reason>` / `stage=planned-edges run_id=<id> failed: <reason>` | failure |
| `stage=shutdown Bulk run stopped by operator/signal request: ...` | operator stop (WARNING, not ERROR) |

There is no separate "bulk run succeeded" line: the exit code `0`, preceded by `stage=planned-edges-complete` (planned) or the last `stage=bulk-edge-complete ... outcome=...` (lease/classic), is the success signal. The dispatcher and writer containers have their own logs (`docker logs graph-loader-planned-dispatcher-*`; per-chunk `stage=planned-done chunk=... edge=... rows=... writer=... write_ms=... done=<n>/<total>` lines and `stage=planned-dispatcher-stats ...` are in the dispatcher's log, which is saved as `planned-dispatcher.log` in `GRAPH_LOADER_TIMING_DIR`).

## 10. Invalid records and the DLQ in bulk

An invalid record is malformed JSON, an undecodable payload, an undeclared field, a missing required field or a wrong type. It is never loaded and never silently lost; where it goes depends on the loader.

| Where | Default destination | With `dead_letter.enabled: true` |
| --- | --- | --- |
| Node loaders (every mode) | `var/rejections/<Label>-<replica>.jsonl` on the host, one fsync'd JSON line per record with `topic`, `partition`, `offset`, `reason` and `node_label`. Metadata only, no payload. The offset commits only after the line is fsync'd. | The node's DLQ topic (`<node topic>.dlq` unless `dead_letter.topic`): a full envelope with the payload, replayable. |
| Relationship, **planned** (plans from topics) | `var/rejections/<run id>-planner-<worker>.jsonl`: same entry format with `edge_type`. The record is excluded from the plan and the load continues. Counted as `rejected` in the plan stats. | Ignored. Planned mode never reads `dead_letter`. |
| Relationship, planned with a **producer plan** | Fails the run (`invalid <type> row ...`). | Ignored. |
| Relationship, **lease / classic** | `var/rejections/<Type>-<replica>-<hex of the derived stage run id>.jsonl` with `edge_type`. | The type's DLQ topic. |

Notes on the paths: for loaders started by the CLI the container environment sets `REJECTION_LOG_PATH` to `/app/rejections/<name>.jsonl`, which the CLI bind-mounts from the host `var/rejections/` (relative to the directory you ran `start` in). That takes precedence over `loading.rejection_log_path`, which therefore only matters for loaders you run directly. The rejection directory must be writable; an unwritable one fails planning with the path and the record's position.

**Missing or ambiguous endpoints.** In planned mode, a chunk row whose source or target matches no node or more than one node rolls the chunk back and fails the run (`... did not resolve exactly one source and target endpoint`). In lease and classic bulk the whole batch fails the same way unless the type sets `dead_letter.route_missing_endpoints_in_bulk: true`, in which case the unresolved row goes to the DLQ and the rest of the batch commits. That is a per-type choice: a type whose source label is loaded by a slow separate phase may want fail-closed while another opts in. It has no effect on a `native_disjoint` type under classic.

**There is no retry consumer in bulk.** The automatic DLQ retry loop belongs to stream mode ([INCREMENTAL_LOAD.md](INCREMENTAL_LOAD.md), part B). In bulk, nodes load first, so a missing endpoint is usually a genuinely absent node. Fix the data, then re-inject the backlog yourself:

```bash
python -m src.cli replay-dlq --config my_graph.yaml --edge-type LIVES_IN --dry-run        # count and list matches
python -m src.cli replay-dlq --config my_graph.yaml --edge-type LIVES_IN --reason-code missing_target
python -m src.cli replay-dlq --config my_graph.yaml --node-label Person --limit 100       # a bounded first attempt
```

`replay-dlq` takes exactly one of `--node-label` / `--edge-type` and optionally `--reason-code`, `--since`, `--until`, `--limit`, `--target-topic`, `--source-topic` (repeatable), `--dlq-topic`, `--parked` and `--dry-run`. It republishes matching envelopes onto the entity's source topic (each to its own `original_topic` for a multi-topic type), where the next load picks them up. A replay can overwrite a newer value with an older one (the order in which records are applied decides); `--dry-run` first, and prefer `order_by` types. Alternatively `python -m src.loader.retry_consumer --config ... --dlq-topics <comma-separated>` retries a finished bulk run's backlog standalone (without `--lease-address` it writes through the classic, non-leased session, which is safe once no other writer is left). Details: [USAGE.md "Replaying a DLQ topic"](USAGE.md#replaying-a-dlq-topic).

Existing JSONL rejection entries are metadata-only and cannot be replayed; to recover such a record, re-source it from its topic at the recorded offset.

## 11. Verifying the result

**Counts.** The number of relationships is lower than the number of input records whenever the same pair appears more than once (repeats `MERGE` into one relationship).

```bash
docker exec graph-loader-neo4j cypher-shell -u neo4j -p "$NEO4J_PASSWORD" -d "${NEO4J_DATABASE:-neo4j}" \
  "MATCH (n) UNWIND labels(n) AS l RETURN l, count(*) ORDER BY l"
docker exec graph-loader-neo4j cypher-shell -u neo4j -p "$NEO4J_PASSWORD" -d "${NEO4J_DATABASE:-neo4j}" \
  "MATCH ()-[r]->() RETURN type(r), count(*) ORDER BY type(r)"
```

Compare with the expected distinct node keys and distinct `(source, target)` pairs. For planned runs also read the ledger (`edge-plan.json.ledger.json`): `state` must be `complete`, and `chunks`/`rows` equal the plan. The run's own success already implies it.

**The example verifiers.**

* `python examples/social/social_graph.py verify --persons N` recomputes the expected graph from the generator, without library code, and compares every node, relationship and property (`exact_match: true`). `--against-database OTHER` also compares with a second load (for example a classic run). `expect` prints the expected counts without touching Neo4j.
* `python examples/multi_topic/multi_topic_graph.py verify --customers N` does the same for the multi-topic rules (and `--variant multi-pair`, `--types RATED`).
* IMDb: compare per-label and per-type counts, and a property fingerprint, with a second load. The IMDb run reports and `examples/imdb/README.md` describe the expected figures; the planned and classic loads of the 40k subset matched on every count and property checksum.

**Lock waits.** To prove a planned or lease run waited on no locks, look at Neo4j's query log. Each completed query line has `(planning: N, waiting: M)`:

```bash
docker exec graph-loader-neo4j sh -c 'cat $(ls -r /logs/query.log.* 2>/dev/null) /logs/query.log' \
  | grep -o 'waiting: [0-9]*' | sort | uniq -c
```

Expect only `waiting: 0` for planned and lease runs (on the IMDb 40k load, 0 ms across all 13,918 write transactions). The log holds every run since it was last rotated or cleared, so narrow it to your run's time window and database (the lines start with a timestamp and contain the database name). The compose file rotates `query.log` at 512 MB x 20 files instead of Neo4j's default so the evidence survives a big run (`NEO4J_QUERY_LOG_ROTATION_SIZE`, `NEO4J_QUERY_LOG_KEEP_FILES`; set `NEO4J_QUERY_LOG_PARAMETERS=false` to shrink the log on very large runs). A second check while the run is in progress: `SHOW TRANSACTIONS` should never show a status starting with `Blocked`. Classic runs are the positive control: they show many non-zero waits.

## 12. Monitoring a bulk run

* **Logs** ([section 9](#log-lines-worth-watching)) are the primary live view; `docker logs -f <container>` for a loader, dispatcher or writer.
* **Metrics endpoints.** Every process exposes Prometheus metrics (`GRAPH_LOADER_METRICS_ENABLED`, default on; `GRAPH_LOADER_METRICS_PORT`, default `9464`). The CLI's own server binds `127.0.0.1` (logged `stage=metrics-ready kind=cli name=- addr=127.0.0.1 port=9464`, falling back to an ephemeral port with a warning if taken); set `GRAPH_LOADER_METRICS_BIND_ADDR=0.0.0.0` only if a Prometheus container must reach the planner series, and know that this exposes an unauthenticated endpoint. Useful bulk series: `graph_loader_records_consumed_total` and `graph_loader_neo4j_write_duration_seconds` (loaders and writers); `graph_loader_planner_rows_read_total` and `graph_loader_planner_chunks_published_total` (CLI/planner); `graph_loader_plan_chunks_planned`, `graph_loader_plan_chunks_committed`, `graph_loader_plan_active_writers`, `graph_loader_plan_resource_wait_seconds` (dispatcher); `graph_loader_lease_*` (lease mode); `graph_loader_dlq_routed_total`; `graph_loader_deadlocks_recovered_total`. `graph_loader_consumer_lag_records` is stream-only: bulk stages never refresh it.
* **Metric snapshots.** With `GRAPH_LOADER_TIMING_DIR` set, every process also writes `metrics-<kind>[-<name>][-<replica>].prom` there (Prometheus text format), rewritten every `GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S` seconds (default 15; `0` disables the timer) and once at exit. This is the durable record for a container that lives shorter than a scrape interval (the dashboards do not use a Pushgateway). The CLI's is `metrics-cli.prom`. Use a fresh timing directory per run: files are overwritten in place by the next process with the same kind, name and replica.
* **Timing directory.** Besides snapshots it holds write-timing logs (`node-<Label>-<replica>.log`, `edge-<type>-<replica>.log`, `edge-planned-<writer>.log` with `stage=node-write` / `stage=edge-write` lines), the planned files above, `lease-settings.json`, and per-type `stage-<type>.json`, plus `substage-<type>.t<rank>.json` for precedence sub-stages.
* **Reports.** The example drivers write a Markdown report per run (`--report-dir` for IMDb) with per-type publish times, write statistics and plan statistics.
* **Prometheus and Grafana.** `make monitoring-up` starts an optional local stack that discovers the containers by Docker label; the starter dashboard rows for plan progress and lease server populate only for the mode that emits them. The alert `GraphLoaderPlannedRunStalled` covers a stalled planned run, and `GraphLoaderPlannedModeDeadlockObserved` must stay silent. See [`docs/MONITORING.md`](MONITORING.md). Containers started with `--network` other than `graph-loader-net` are silently ignored by that stack.
* The planner's rejection count is not a metric. Alert on the `stage=edge-plan-rejections` line or on the files in `var/rejections/`.

## 13. Stopping safely, and exit codes

* **Stop with `Ctrl-C` (SIGINT) or SIGTERM on the `start` process.** The first signal sets a shutdown flag that every wait notices within about 0.1-0.25 s (not after `--bulk-timeout-seconds`). The run then stops exactly the containers it started, in a safe order, and exits **`1`**, logging `stage=shutdown Bulk run stopped by operator/signal request` at WARNING. A chunk transaction in progress is never abandoned half-applied; planned writers between chunks exit `0`. A second signal changes nothing. Container stops run concurrently with Docker's own limits: up to 30 s for node loaders, 10 s for the lease server.
* A stop during the plan wait terminates the planner processes at once and removes their spill and progress files and the chunk topic (unless `--keep-plan-topic`).
* **`kill -9` is the last resort.** It cannot be caught, so every container the run started keeps running. Recover with:

  ```bash
  docker ps -a --filter label=app=graph-loader
  docker rm -f $(docker ps -aq --filter label=app=graph-loader)
  ```

  Narrow with `--filter label=run_id=<id>` when you used an explicit `--run-id` (in bulk every container carries a `run_id` label: node loaders the parent id, lease/classic edge loaders the derived stage id described in section 14, so `label=run_id=<id>` misses the edge loaders; only stream node loaders started without `--run-id` have no such label), or `--filter label=component=<kind>` (`node-loader`, `edge-loader`, `bucket-lease-server`, `retry-consumer`, `planned-dispatcher`, `planned-edge-writer`) to leave other runs alone. Then see [section 14](#14-re-running-and-recovering) for the chunk topic.
* `python -m src.cli stop [--loader NAME]` stops node-loader containers only. `python -m src.cli status --config ...` lists node-loader containers and their status; despite its help text it does not report lag.
* Run the recovery recipe after any stop you did not watch; after a normal stop it finds nothing.

| Exit code | Meaning |
| --- | --- |
| `0` | The completion proof succeeded. |
| `1` | Any failure: schema or flag error, setup failure, container crash, timeout, nonzero lag, boundary violation, or an operator/signal stop of a bulk run. Containers were stopped. |
| `2` | `argparse` rejected the command line. |

`replay-dlq` exits `0` on success and `1` on any refusal or failed republish.

## 14. Re-running and recovering

* **A failed run is safe to re-run.** Every write is an idempotent `MERGE` (`SET n += props` for nodes, `SET r += props` for relationships), so a partial load is completed, not duplicated. Do not run two bulk loads against the same topics or database at once.
* **Run ids.** `--run-id` defaults to a fresh random id, which is what you want for a rerun. The id names the containers (`graph-loader-edge-<type>-<replica>-<hex id>`), the planned chunk topic, the rejection files and, for lease/classic, the relationship consumer groups. Those are derived from the run id: shared and lease stages use `<consumer_group_id>-<type>-<R>.bulk.0.<h>` (`<h>` is the first 24 hex characters of `sha256("R|bulk|0|<type>")`), an isolated classic self-reference stage uses `.isolated.<i>.<h>`, and each precedence sub-stage adds `.topic.<i>.<h>` to the stage id. The same derived id, hex-encoded, appears in the edge container and rejection-file names, so a stage's launched containers do not carry the plain run id. The derivation is deterministic: the same `--run-id` gives the same group names. Reusing an id is safe in planned mode only if its leftovers are gone (below). **A precedence type must be rerun with a fresh run id**; reusing one is refused when a higher-ranked topic already has commits while a lower one still has work (`rerun a precedence type with a fresh --run-id`).
* **Node consumer groups persist across runs.** Node loaders commit under `<consumer_group_id>-<Label>`. A rerun resumes from those offsets (until Kafka expires them: committed offsets of an idle group are dropped after `offsets.retention.minutes`, 7 days by default), so a rerun after the nodes finished finds nothing left to read and the labels become ready almost at once. To reload the same node topics from the beginning, delete the groups or change `consumer_group_id`:

  ```bash
  docker exec graph-loader-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --delete --group my-graph-Person
  ```
* **Planned mode never resumes.** The planner always reads every relationship topic from its low watermark to its high watermark with run-scoped groups and topics. A rerun re-plans and re-reads everything.
* **Stale chunk topic after `kill -9`.** A random run id leaves a harmless but unbounded-retention topic `graph-loader-edge-chunks-<run id>`; delete it:

  ```bash
  docker exec graph-loader-kafka kafka-topics --bootstrap-server localhost:9092 --list | grep graph-loader-edge-chunks
  docker exec graph-loader-kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic graph-loader-edge-chunks-<run id>
  ```

  To reuse the same `--run-id` instead, add `--replace-stale-plan-topic` after removing the run's containers (the start is refused while any is alive). The planner also removes a leftover plan directory's files and its own spill directory at start.
* **Clearing the inputs.** Delete and recreate your topics, and delete the node groups too (see above): a committed offset that survives on a recreated topic would be used as the loaders' start position, or fails the zero-lag proof (`committed offset above the high watermark ... topic recreated under a reused run id?`). Wait until Kafka has finished deleting a topic before recreating it.
* **Fresh database.** `CREATE OR REPLACE DATABASE <name> WAIT` against the `system` database. Constraints and indexes are recreated on the next run.
* **Concurrent runs** must use different topics, groups and databases.

## 15. Performance numbers

All numbers here are from one host: Neo4j pinned to 8 cores (cpus 0-7), Kafka to 4 (cpus 8-11), loaders on `LOADER_CPUSET=12-17`, the IMDb example runner. The subsets are `titles-20000-depth-4` (11.0M input records, 2.15M nodes, 8.4M relationships created) and `titles-40000-depth-4` (23.3M input records, 4.06M nodes, 17.8M relationships created). They are indicative, not a guarantee for other hardware or data shapes.

### A/B bulk benchmark

Source: `/mnt/graph-loader/bench/p0/bulk_perf/RESULTS.md` (a host-local directory, not part of the repository), with the script `bench.sh` beside it. Two image/checkout pairs were run interleaved: p1 = checkout `3a94537`, p2 = a worktree at `9ba6f0e` (`feat/stream-outage-resilience`, docs-only over `a253b1f`). The table gives the medians of valid runs (range in brackets). It is not a measurement of `d8de173`.

| Benchmark | Mode | Wall, p1 | Wall, p2 | Consume + load, p1 / p2 | Lock waiting |
| --- | --- | --- | --- | --- | --- |
| IMDb 20k | planned (no mode flag) | 75.4 s (68.0-82.8), n=2 | 69.2 s (64.7-73.7), n=2 | 65.3 s / 60.9 s | 0 ms |
| IMDb 40k | planned | 126.5 s (120.5-132.5), n=2 | 126.7 s (125.1-128.4), n=2 | 110.7 s / 111.2 s | 0 ms |
| IMDb 20k | `--edge-leases` | 125.2 s (118.0-130.0), n=5 | 125.6 s (120.5-145.8), n=4 | 117.8 s / 118.3 s | 0 ms |
| IMDb 20k | `--edge-classic` | 359.2 s (355.5-362.9), n=2 | 360.9 s (357.9-363.8), n=2 | 350.0 s / 353.8 s | about 106,000 ms (p1), 109,000-113,000 ms (p2), about 12k waiting transactions |

"Wall" is the whole runner: topic setup, publishing the TSVs to Kafka (6-15 s), then the CLI run. "Consume + load" is the Kafka consumption, Neo4j loading and drain verification step only. Derived from the medians: leases take roughly 1.7-1.8x the planned wall time and classic roughly 4.8-5.2x.

**How it was measured** (`bench.sh`): the runner `examples/imdb/publish_and_load_nodes.py --partitions 4 --consumer-workers 4 --publish-workers 4 --edge-consumers 1 --edge-writers 4 --network graph-loader-net --skip-image-build`, plus no mode flag for planned (with a `--plan-topic` under the benchmark prefix), `--edge-leases` or `--edge-classic`. For every run: a fresh database (`CREATE OR REPLACE DATABASE ... WAIT`), the benchmark's own topics and groups deleted first, images prebuilt, node and relationship counts diffed against expected files (every valid run matched), and lock waits summed from `query.log` entries for that database inside the run's time window. Interleaved order, 2 repetitions plus extra lease replicates. One lease run on p2 was excluded because Neo4j rate-limited authentication after another client failed to log in. The host was busy during the second half of the benchmark (load average up to 54), so the spreads are wide (up to 20% for leases); the p1/p2 differences are within noise.

### Earlier documented figures (USAGE.md section 9)

On a quieter host, the same IMDb 20k load took about 60 s planned, about 92 s with leases, about 87 s with classic shared overlap (about 82% of edge-query time spent waiting on locks) and 525 s with the original label-serialized classic. The 40k load took 107.8 s with a producer plan, 122.4 s with library planning from Kafka and 166.5 s with classic shared overlap. A single final smoke run at `9ddf9ab` (`bench/p0/final_smoke/RESULTS.md`) loaded IMDb 20k planned in 71 s with every query-log entry `waiting: 0`. The absolute differences between these sets are host load and commit, not mode.

### Where the time goes

In planned mode Neo4j runs at about 7.5 of its 8 cores for the whole relationship phase, so the database CPU is the limit: each relationship costs about 18-25 microseconds of server CPU. More Neo4j cores give close to linear gains. Keep the page cache big enough to hold the graph (with 12 GB the hit ratio stayed at or above 99.7% on the 20k load; its lowest 5-second sample on the 40k load was 93%) and the heap at about 8 GB (GC under 2% of wall time). Loaders cost about 2-3 cores in total. Larger node batches (2,000) measured slower than 500.

## 16. Troubleshooting

| Symptom (exact text where one exists) | Cause and fix |
| --- | --- |
| `Failed to get metadata: Local: Broker transport failure` | The host cannot reach Kafka. Check `KAFKA_BOOTSTRAP_SERVERS` and the broker's advertised listener (`KAFKA_EXTERNAL_HOST`); after `docker compose up`, wait for the broker to be healthy. |
| `Schema initialization failed: Authentication failed: ...` | Wrong `NEO4J_USERNAME`/`NEO4J_PASSWORD`. The password defaults to `changeme` when unset. |
| `Neo.ClientError.Security.AuthenticationRateLimit` in a container | Repeated failed logins from some client locked the user out for a while. Fix the credentials and wait, then rerun. |
| `Schema initialization failed: Failed to connect to Neo4j after N retries.` | `NEO4J_URI` unreachable from the host. |
| `Schema initialization failed: Failed to execute DDL: ...` (for example `Neo.ClientError.Database.DatabaseNotFound`), `Index creation failed for: [...]`, or `Timeout waiting for indexes to become ONLINE` (120 s) | The DDL step failed. A missing database: `NEO4J_DATABASE` is applied to the DDL and passed to every container, and the database must exist (`CREATE DATABASE <name> IF NOT EXISTS WAIT` on `system`). Existing data that violates a declared uniqueness or not-null constraint also fails here. |
| A load "succeeds" but the graph is empty | It went into another database: check `NEO4J_DATABASE`. |
| `Failed to load schema: ...` | Schema validation names the field. Common bulk-time causes: a relationship type with no properties (`Edge '<type>' must have at least one property.`), a key property that is not `required`, an endpoint key property that differs from the label's `key_property`, a topic used twice, a multi-topic type without a conflict rule, or `precedence` with an explicit `loading.mode: stream`. |
| `stage=launch reason=--edge-plan and --edge-classic are mutually exclusive; choose one relationship mode` | More than one of `--edge-plan`, `--edge-leases`, `--edge-classic`. Give one. |
| `stage=launch reason=relationship type(s) X use execution.mode native_disjoint, which neither zero-wait mode supports; pass --edge-classic ...` | A `native_disjoint` type with no mode flag. Use `--edge-classic`, or switch the type to `python_apoc`. |
| `--bulk-edge-overlap/--max-concurrent-bulk-edges tune classic only; ignored (add --edge-classic to use classic)` | Warning: those flags do nothing without `--edge-classic`. |
| `--plan-writers must be 1-64` (and the other range messages) | A tuning flag is out of range ([section 5](#5-the-command)). |
| `stage=launch reason=invalid run id: run id must be 1-128 letters, digits, dot, underscore, or hyphen` | Bad `--run-id`. |
| `stage=edge-plan reason=planner could not start: relationship topic <t> has no partition metadata` | A relationship topic does not exist (or has no partitions). Create and publish it first. |
| `Bulk run failed: Kafka topic metadata unavailable for '<topic>'` | A node topic does not exist. |
| `relationship topic <t> changed after the bulk boundary was captured (end offsets {...} -> {...}); publish every relationship before starting the load` / `bulk input changed topic=<t> partition=<p> boundary=<b> current=<c>` | Something was published to a topic after the run captured its boundary. Stop publishing to the schema's topics for the duration of a bulk run, then rerun. |
| `chunk topic graph-loader-edge-chunks-<id> already exists; planned runs need a new, empty topic ...` | Leftover chunk topic from a killed run with the same id (or an existing `--plan-topic`). Use a new `--run-id`, or `--replace-stale-plan-topic`. |
| `chunk topic <t> exists and N container(s) of run <id> are still alive (...); stop them first (docker rm -f $(docker ps -aq --filter label=run_id=<id>))` | `--replace-stale-plan-topic` refused because the old run's containers are still up. Remove them. |
| `planner spill directory <dir> is full (...)` | The planner's spill files did not fit. Free space or set `--plan-spill-dir` on a larger local disk (about the records' uncompressed size). |
| `no relationship records within Ns; unfinished partitions ...` | The planner got no records for `N` seconds while partitions were unfinished (broker unreachable, or a boundary the topic cannot deliver). Check Kafka and the topic. |
| `edge plan <path> did not appear and the planner made no progress within Ns` | The plan wait is progress-based; the planner stalled or never started. Check the planner logs and `edge-plan.json.failed`. |
| `invalid <type> record at <topic> partition <p> offset <o>: ...` | The planner hit a record failure it does not skip (it skips validation failures into the rejection file). Fix the record at that offset or the schema, then rerun. |
| `Edge '<type>' did not resolve exactly one source and target endpoint` | A relationship names a node key that is not on the node topic, or one that matches more than one node. Publish the missing node or make the key unique (planned: the chunk rolled back and the run failed). |
| `plan binds <topic> to S->T; the schema binds it to ...; re-plan the load` (also: `plan edge type X was planned from topics [...]; the schema lists [...]`) | A plan file no longer matches the schema. Re-plan. |
| `timed out waiting for node label completion` / `... drain completion` / `... assignment coverage` / `no planned chunk completed within Ns (done=a/b)` | A stage made no progress for `--bulk-timeout-seconds`. Look at the named containers and Kafka. Raise the flag only if a single step (one node batch, one chunk transaction) can legitimately take that long. |
| `nonzero lag label=<L> topic=<t> partition=<p> committed=<c> boundary=<b>` (possibly with `topic recreated under a reused run id?`) | Committed offsets do not equal the boundary. Usually a node topic was recreated or trimmed while its consumer group was kept: delete the group `<consumer_group_id>-<Label>` and rerun. |
| `loader exited label=<L> replica=<n> status=exited exit_code=<c>` followed by `stage=diagnostics ...` | A loader crashed; the last log lines are printed. Check Neo4j connectivity and the record that was being written. |
| `DLQ topic <t> already exists but violates the DLQ policy: ...` | A pre-existing DLQ topic has the wrong retention or too small `max.message.bytes`. Fix its configuration or delete it and rerun. |
| `relationship type X now resolves its DLQ to Y, but its former default DLQ Z still holds N record(s) ...` | See the migration notes in [INCREMENTAL_LOAD.md](INCREMENTAL_LOAD.md#5-migration-notes-single-topic-to-multi-topic): set `dead_letter.topic: Z`, drain and delete `Z`, or pass `--accept-former-dlq Z`. |
| `relationship type X: its parked-pair index <t> holds ...: drain the DLQ with the previous schema first, or pass --accept-dlq-leftovers X to start anyway` | Same family. Applies when the type's topic list or label pairs changed. |
| Containers still running after the run | Run the recipe in [section 13](#13-stopping-safely-and-exit-codes). |
| Large-run planner fails mid-way | Check `var/edge-plans/<run id>` (or `--plan-dir`) for `edge-plan.json.failed` and the CLI log; planning needs memory and spill disk ([section 2](#memory-and-disk-for-the-planner-planned-mode)). |

## 17. Worked example (examples/social)

`examples/social` is a synthetic social/commerce graph (integer, string and date keys, a self-referencing type, two types between the same labels, hub-heavy small labels, repeated pairs). Its schema is [`examples/social/graph_schema.yaml`](../examples/social/graph_schema.yaml), consumer group `social-example`, topics `social-*`. For a larger real dataset see [`examples/imdb/README.md`](../examples/imdb/README.md) (below).

```bash
# 0. Services and images (from the repository root). NEO4J_PASSWORD must be set for compose.
export NEO4J_PASSWORD=...
docker compose up -d neo4j kafka
make build-images

# 1. Environment: host side, then container side. Do not rely on .env.
export NEO4J_URI=bolt://localhost:7687 NEO4J_USERNAME=neo4j NEO4J_DATABASE=social
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export LOADER_KAFKA_BOOTSTRAP_SERVERS=kafka:29092 LOADER_NEO4J_URI=bolt://neo4j:7687

# 2. A database to load into (Enterprise).
docker exec graph-loader-neo4j cypher-shell -u neo4j -p "$NEO4J_PASSWORD" -d system \
  "CREATE DATABASE social IF NOT EXISTS WAIT"

# 3. Publish: creates the social-* topics and publishes nodes and relationships.
python examples/social/social_graph.py expect  --persons 20000        # expected counts, no Neo4j needed
python examples/social/social_graph.py publish --persons 20000

# 4. Load. Planned mode is the default; this logs "stage=launch edge_mode=planned reason=default".
python -m src.cli start --mode bulk --config examples/social/graph_schema.yaml --skip-image-build

# 5. Verify: exact comparison with the independently computed graph.
python examples/social/social_graph.py verify --persons 20000         # expect exact_match: true

# 6. Prove zero lock waits (all lines must be waiting: 0).
docker exec graph-loader-neo4j sh -c 'cat $(ls -r /logs/query.log.* 2>/dev/null) /logs/query.log' \
  | grep -o 'waiting: [0-9]*' | sort | uniq -c
```

What you should see: the 20,000-person run has 323,252 relationship records, of which 13,940 repeated pairs end with properties that differ from their first record; the documented result is an exact match for both planned and classic loads. In the CLI log: `stage=launch edge_mode=planned reason=default`, `Schema initialization complete. Ready for ingestion.`, `stage=launch planned writers=...`, the planner's `stage=edge-plan-*` lines, one `stage=node-label-ready` per label, `stage=planned-edges plan_id=... chunks=...`, `stage=planned-edges-complete ...`, then exit code `0`.

Re-running without any change is a good idempotence test: the node groups (`social-example-<Label>`) are already at the boundary, so the labels become ready at once, the planner re-reads and re-plans every relationship, and `verify` still reports `exact_match: true`. To reload the nodes from scratch, delete the `social-example-*` groups or change `loading.consumer_group_id`.

To compare modes, load the same topics into a second database with `--edge-classic --bulk-edge-overlap shared` (delete the node groups first) and run `social_graph.py verify --database social --against-database social_classic` (create both databases first; the planned load goes into `social` as in the example above); that is what `tests/test_social_planned_integration.py` automates.

Clean up with `python examples/social/social_graph.py reset` (deletes the example's topics) and `DROP DATABASE social IF EXISTS WAIT`.

**IMDb variant.** After building a subset (`examples/imdb/README.md`) and with Kafka and Neo4j running:

```bash
./scripts/cleanup_imdb_local_load.sh           # clears only this example's input topics and node groups
python examples/imdb/publish_and_load_nodes.py \
  --subset-dir examples/imdb/data/subsets/titles-40000-depth-4 \
  --partitions 4 --consumer-workers 4 --publish-workers 4 \
  --edge-plan --report-dir examples/imdb/reports --skip-image-build \
  --network graph-loader-net
```

The runner publishes the nodes, plans every relationship (in the publisher with `--plan-source files`, the default, or from Kafka with `--plan-source kafka`) while the nodes load, starts the CLI in bulk mode, and writes a Markdown timing report. With no edge-mode flag it follows the CLI default (planned, from the edge topics). **If you export `LOADER_KAFKA_BOOTSTRAP_SERVERS` and `LOADER_NEO4J_URI`, the runner defaults to the Docker `bridge` network, where `kafka` does not resolve; pass `--network graph-loader-net`** (or leave the `LOADER_*` variables unset on the local stack). The runner reads the project `.env` too, so `LOADER_*` values set only in `.env` trigger the same default. The runner forwards SIGINT/SIGTERM to the CLI.

## 18. Notes / open questions

* **`--bulk-edge-overlap` / `--max-concurrent-bulk-edges`.** Without `--edge-classic` they warn and are ignored in bulk (`cli.py` `_resolve_bulk_edge_mode`), are ignored silently in stream, and with `--edge-leases` the overlap is forced to `shared`. `plan/v7/completion_bulk_default.md` has been corrected to say so.
* **`loading.rejection_log_path`.** `docs/USAGE.md` lists it as the place node loaders record malformed records. For loaders the CLI launches, the container variable `REJECTION_LOG_PATH` (set by `docker_service.py`) wins, so the files are `var/rejections/<Label>-<replica>.jsonl`. The schema field matters only for loaders run directly.
* **Node `dead_letter` fields.** The code rejects the relationship-only fields only when they are set **and** non-default (`schema.py`, `NodeConfig`); an explicit default loads. `docs/USAGE.md` has been corrected.
* **`GRAPH_LOADER_LEASE_MERGE_ROWS`** (rows merged per lease transaction, default 200) is forwarded to stream edge loaders and the retry consumer only. Bulk containers never receive it, so it cannot be set through `start --mode bulk`.
* **Performance figures** come from a host-local directory outside the repository, from commits `3a94537` and `9ba6f0e` (not `d8de173`), measured on a loaded host. They were not re-run.
* **The worked example was not executed**; the shared local stack was in use. Its commands come from the example READMEs and the CLI, and every flag was checked against `src/cli.py`.
* **Neo4j minimum version for planned mode.** USAGE.md states there is no lower bound for planned mode; only 5.26 was exercised.
* **`python -m src.cli status`** says it checks "status and lags" but only lists node-loader containers.
* `.env.example` could not be read in this environment, so its variables are not described.
