# graph-loader usage guide

graph-loader loads a property graph from Kafka into Neo4j. You describe the
graph once in a YAML schema, publish node and relationship records to the
topics that schema declares, and run one command. The loader works out the
load order, the parallelism and a lock-free write schedule by itself, so a new
graph model needs no model-specific code.

Two task-oriented guides sit on top of this reference:
[How to run a bulk (initial) load](BULK_LOAD.md) and
[How to load incrementally](INCREMENTAL_LOAD.md) (repeated bulk batches or
continuous streaming, and how to choose). Start there for a procedure; come
back here for the complete field, flag and behaviour reference.

This guide covers everything a library user needs:

1. [How it works](#1-how-it-works)
2. [Requirements and environment](#2-requirements-and-environment)
3. [The YAML schema](#3-the-yaml-schema)
4. [The input contract (publishing records)](#4-the-input-contract-publishing-records)
5. [Running a load](#5-running-a-load)
6. [Planned mode in depth (the fastest, lock-free mode)](#6-planned-mode-in-depth)
7. [Planning in the producer (optional)](#7-planning-in-the-producer-optional)
8. [Outputs, monitoring and verification](#8-outputs-monitoring-and-verification)
9. [Performance guide](#9-performance-guide)
10. [Operations: reruns, cleanup, failures](#10-operations-reruns-cleanup-failures)
11. [Troubleshooting](#11-troubleshooting)
12. [Limitations](#12-limitations)
13. [Continuous integration](#13-continuous-integration)

For a five-minute version see the top-level [README](../README.md#quick-start-about-5-minutes);
for complete runnable examples see [`examples/`](../examples/README.md).

Related docs: [CLI.md](CLI.md) (command and flag reference),
[ARCHITECTURE.md](ARCHITECTURE.md), [CHANGELOG](../CHANGELOG.md),
[BULK_LOAD.md](BULK_LOAD.md), [INCREMENTAL_LOAD.md](INCREMENTAL_LOAD.md),
[MONITORING.md](MONITORING.md) and the [examples index](../examples/README.md).

---

## 1. How it works

```
 your producer ──► Kafka (one topic per node label / relationship type)
                         │
            python -m src.cli start --mode bulk --config graph.yaml
                         │
   ┌─────────────────────┼──────────────────────────────────────────────┐
   │ 1. apply schema DDL (constraints, indexes) to Neo4j                │
   │ 2. node loader containers  ── MERGE nodes, one partition per loader│
   │ 3. planner processes (while nodes load) ── read relationship       │
   │    topics, build a node-disjoint plan, write chunks to a run topic │
   │ 4. dispatcher + writer pool ── write each chunk as one transaction │
   │    once both endpoint labels are loaded                            │
   │ 5. verify: every chunk committed exactly once, row counts match    │
   └────────────────────────────────────────────────────────────────────┘
```

* **Nodes before relationships.** A relationship is written only after every
  node of both its endpoint labels is committed, so `MATCH` always finds its
  endpoints. Independent labels load in parallel, and each label becomes
  "ready" on its own as soon as its topic is fully consumed and committed.
* **Idempotent writes.** Nodes use `MERGE` on the schema key plus `SET n += props`.
  Relationships use `MERGE (s)-[r:TYPE]->(t)` plus `SET r += props`. Replaying
  a load, or part of one, is always safe.
* **No lock waits (planned mode).** Every node belongs to exactly one
  scheduling *resource*. Relationship transactions that run at the same time
  never share a resource, so they never share a node. Neo4j therefore never
  makes one transaction wait for another's locks, and deadlocks cannot occur.
  See [section 6](#6-planned-mode-in-depth).
* **Everything runs in Docker.** The CLI runs on the host and launches node
  loaders, a dispatcher and writer containers on a Docker network, and runs
  the planner processes itself. Each of those containers talks to Kafka and Neo4j.

## 2. Requirements and environment

| Component | Requirement |
| --- | --- |
| Neo4j | **5.23 or later, Enterprise** (tested with 5.26, block store format). The classic and lease relationship writers (`build_apoc_locked_upsert_query` in `src/loader/edge_execution.py`) use Cypher's `CALL (vars) { ... }` scoped-subquery syntax, first available in Neo4j 5.23 — an earlier server (confirmed empirically on 5.21.x) rejects the query with a Cypher syntax error. Planned mode does not use this syntax and has no such lower bound. APOC is needed only for the classic relationship modes (`apoc.lock.nodes`); planned mode does not use it. |
| Kafka | Any broker reachable from the host **and** from the loader containers (tested with Confluent 7.7 in KRaft mode). |
| Docker | A local Docker daemon; the CLI builds and runs the `graph-loader-node` and `graph-loader-edge` images. |
| Python | 3.12, with `pip install -r requirements.txt` (the Neo4j Python driver is pinned `>=5.20,<7`). |

### Local services

`docker compose up -d neo4j kafka` starts both services from
[`docker-compose.yaml`](../docker-compose.yaml). Their resources are pinned so
that loader containers cannot compete with the database. Each setting can be
overridden with an environment variable (or in `.env`):

| Variable | Default | Meaning |
| --- | --- | --- |
| `NEO4J_CPUSET` | `0-7` | CPUs reserved for Neo4j |
| `NEO4J_HEAP_SIZE` | `8G` | Neo4j JVM heap (initial = max) |
| `NEO4J_PAGECACHE_SIZE` | `12G` | Neo4j page cache |
| `NEO4J_METRICS_INTERVAL` | `5s` | CSV metrics interval (GC, heap, page cache, transactions) |
| `NEO4J_QUERY_LOG_ROTATION_SIZE` / `NEO4J_QUERY_LOG_KEEP_FILES` | `512 MB` / `20` | Query-log rotation size (e.g. `512 MB`) / files kept. Neo4j 5 has no rotation settings, so `config/neo4j/server-logs.xml` (mounted read-only at `/graph-loader-conf/server-logs.xml` and selected via `server.logs.config`; mounting into `conf/` breaks the entrypoint's chown) applies them to `query.log`; its defaults of 20 MB x 7 rotate lock-wait evidence away within minutes of a bulk run. Needs a Neo4j container restart to change |
| `NEO4J_QUERY_LOG_PARAMETERS` | `true` | `db.logs.query.parameter_logging_enabled`; set `false` for very large runs to shrink the query log |
| `KAFKA_CPUSET` | `8-11` | CPUs reserved for Kafka |
| `KAFKA_MEM_LIMIT` / `KAFKA_HEAP_OPTS` | `4g` / `-Xms2g -Xmx2g` | Kafka memory |
| `GRAPH_LOADER_DATA_DIR` | `/mnt/graph-loader/data` | Host directory for Neo4j and Kafka data |
| `KAFKA_EXTERNAL_HOST` | `localhost` | Address Kafka advertises to host clients |

Set `LOADER_CPUSET` (for example `12-31`) to pin all loader containers to the
remaining CPUs.

### Connection variables

The CLI reads these from the environment. Example scripts also load a
project `.env` without overriding exported values.

| Variable | Used by | Meaning |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | host CLI, planners | Kafka address as seen from the host (e.g. `localhost:9092`) |
| `NEO4J_URI` | host CLI | Neo4j Bolt URI from the host (e.g. `bolt://localhost:7687`) |
| `NEO4J_USERNAME`, `NEO4J_PASSWORD` | host and containers | Neo4j credentials |
| `NEO4J_DATABASE` | host and containers | Target database; omit to use the server default |
| `LOADER_KAFKA_BOOTSTRAP_SERVERS` | containers | Kafka address inside the Docker network (default `kafka:29092`) |
| `LOADER_NEO4J_URI` | containers | Neo4j URI inside the Docker network (default `bolt://neo4j:7687`) |
| `LOADER_CPUSET` | containers | CPU set for every loader, dispatcher and writer container |
| `GRAPH_LOADER_NODE_IMAGE`, `GRAPH_LOADER_EDGE_IMAGE`, `GRAPH_LOADER_CLOCK_IMAGE` | CLI | Image tags (default `graph-loader-node:latest`, `graph-loader-edge:latest`, `graph-loader-clock:latest`; `make build-images` builds these defaults) |
| `GRAPH_LOADER_TIMING_DIR` | CLI | Directory for write-timing logs, the plan file and its completion ledger |
| `GRAPH_LOADER_METRICS_ENABLED` | host and containers | `1`/`true` (default) to expose a Prometheus `/metrics` endpoint and write the `metrics-<kind>[-<name>][-<replica>].prom` snapshot in `GRAPH_LOADER_TIMING_DIR`; `0`/`false` (case-insensitive) disables both |
| `GRAPH_LOADER_METRICS_PORT` | host and containers | Port for the metrics HTTP server (default `9464`; the host CLI binds `127.0.0.1` and falls back to an ephemeral free port, logged as a warning, if the configured port is taken) |
| `GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S` | host and containers | Seconds between periodic rewrites of the `.prom` snapshot (default `15`; `0` disables the periodic timer, the exit-time write still happens; non-finite values fall back to the default and positive values below `1` are raised to `1`, values above `86400` are clamped). Forwarded from the host into launched containers; the snapshot *path* is not forwarded (each container gets its own under `GRAPH_LOADER_TIMING_DIR`). |
| `GRAPH_LOADER_METRICS_BIND_ADDR` | host CLI only | Opt-in bind address for the CLI's own metrics server (default `127.0.0.1`, loopback-only). Set to `0.0.0.0` to make consumer-lag/planner metrics reachable from a monitoring-stack Prometheus container over the Docker host gateway (`docs/MONITORING.md` §7); a non-default value is logged at `WARNING` since it exposes an unauthenticated endpoint beyond localhost. Containers are unaffected — they always bind `0.0.0.0` inside their own isolated Docker network namespace, independent of this variable. |

Kafka must advertise an address that is reachable from both the host and the
containers. With the bundled Compose file, host clients use `localhost:9092`
and containers use `kafka:29092` on the `graph-loader-net` network. For a
remote broker, set `KAFKA_EXTERNAL_HOST` on the broker machine.

## 3. The YAML schema

A schema has three sections: `loading` (global behaviour), `nodes` and `edges`.

```yaml
loading:
  mode: bulk
  consumer_group_id: my-graph          # node loaders commit offsets under <id>-<Label>

nodes:
  - label: Person                      # Neo4j label (a Cypher identifier)
    topic: person-events               # one Kafka topic per label
    key_property: person_id            # the MERGE key
    # replicas: 4                      # optional; derived from topic size when omitted
    properties:
      person_id: {type: integer, required: true, constraint: {type: uniqueness}}
      name:      {type: string,  required: true}
      email:     {type: string,  index: {type: text}}
      joined:    {type: date}
  - label: City
    topic: city-events
    key_property: name
    properties:
      name: {type: string, required: true}

edges:
  - type: LIVES_IN                     # relationship type (a Cypher identifier)
    topic: lives-in-events             # one Kafka topic per type
    nodes: {source: Person, target: City}
    source_key_property: person_id     # must equal the source label's key_property
    target_key_property: name          # must equal the target label's key_property
    properties:
      since: {type: date}
      score: {type: float}
```

### `loading` fields

| Field | Default | Meaning |
| --- | --- | --- |
| `mode` | `stream` | `bulk` (one-shot load of everything on the topics) or `stream` (continuous); the CLI's `--mode` decides the run |
| `consumer_group_id` | `graph-loader` | Prefix for consumer groups. Node loaders resume `<id>-<Label>`, so use a new id (or clean up, see section 10) to reload the same topics. Stream relationship loaders resume `<id>-<type>` and the retry consumer `<id>-retry-consumer`, whatever the run id; bulk relationship groups are run-scoped (section 10, "Consumer groups and restarts") |
| `unwind_batch_size` | `500` | Records per node write transaction (1–10,000) |
| `edge_unwind_batch_size` | unset (= `unwind_batch_size`) | Outer batch (1–1,000,000) for the classic per-type relationship loaders only. Lease runs (stream, `--edge-leases`) always use `--bucket-lease-outer-batch-size` (default 32000); planned mode does not use it |
| `flush_interval_ms` | `1000` | Flush an incomplete node batch after this idle time |
| `retry_max_attempts`, `retry_base_delay_ms`, `retry_max_delay_ms` | `3`, `100`, `5000` | Retry of retryable Neo4j errors for node writes |
| `max_poll_interval_ms`, `session_timeout_ms`, `heartbeat_interval_ms` | `300000`, `45000`, `500` | Kafka consumer settings |
| `rejection_log_path` | `var/rejections/node-loader.jsonl` | Where node loaders record malformed records |
| `coordination` | see code | **Deprecated.** Settings of the Phase 4 global batch clock (topic, buckets, slot and lease timings); read only by the deprecated `--stream-clock-rotation` path, ignored by the default bucket-lease fleet |
| `bucket_lease`, `retry_consumer`, `coordination` | see code | These three blocks reject unknown keys: a typo such as `retry_consumer.poll_timeout` (for `poll_timeout_ms`) fails the schema load with "Extra inputs are not permitted" instead of being silently ignored. `loading` itself, `dead_letter`, the edge-level `mix_and_batch`, `retry` and `execution` blocks, and `topics` entries (`TopicEndpoints`) do the same. (The `nodes[]` and `edges[]` entries themselves, and `properties`, do not reject unknown keys) |
| `bucket_lease.tx_timeout_ms`, `.expected_chunk_commit_ms`, `.restart_quiet_margin_ms`, `.reconnect_timeout_ms` | `30000`, `1000`, `5000`, `60000` | Fencing and lease-server restart timing (section 12). `tx_timeout_ms` (`T`, 1000–600000, at least 10 x `expected_chunk_commit_ms`) is the Neo4j transaction timeout: leased writes get at most `T` minus 4 s (fenced BEGIN), planned and other unleased writes get the flat `T`. It **must be at least 6000 in the lease modes** (stream, `--edge-leases`, and the bulk lease mode chosen because of `dead_letter`); the check is not applied under `--stream-clock-rotation`; `restart_quiet_margin_ms` is at least 2000; `reconnect_timeout_ms` `0` means unbounded. `reconnect_timeout_ms + T + (T + margin)` must stay under `max_poll_interval_ms - 5000` unless `reconnect_timeout_ms` is `0` |
| `bucket_lease.max_consecutive_restarts`, `.restart_count_reset_ms` | `5`, `600000` | Streaming-only: the run fails after this many consecutive lease-server restarts. A restarted server that stays running for `restart_count_reset_ms` after its quiet period `Q` resets the count (and the restart backoff), so deaths far apart never add up to the cap |
| `retry_consumer.poll_timeout_ms`, `.backlog_check_interval_ms`, `.due_batch_coalesce_ms` | `500`, `10000`, `200` | Streaming-only (P5-S4): the retry consumer's Kafka `poll()` timeout, how often it recomputes `graph_loader_dlq_backlog`, and how long it waits after a tick's first due record before writing, so near-simultaneous due records from other partitions can join the same batch |
| `retry_consumer.max_records_per_partition`, `.max_batch_records`, `.write_workers` | `1000`, `2000`, `4` | Streaming-only: retry drain throughput. A DLQ partition may hold up to `max_records_per_partition` due, unresolved records (held records do not pause it; it is paused only at that bound); `max_batch_records` due records flush a batch at once instead of waiting out `due_batch_coalesce_ms` (and cap the records read per poll tick); `write_workers` is the number of concurrent lease-holding write threads per edge type, per label pair for a type of several label pairs (live-fleet mode: up to pairs x `write_workers`; standalone mode writes `mix_and_batch.batch_size`-row transactions sequentially). Each edge type's due records are written as one batch, then re-DLQ'd/parked/blocked with one producer flush and one parked-pair-index flush, and each partition's offset is committed once, below its lowest unresolved record |
| `retry_consumer.max_overflow_held_per_partition` | `null` (= 10 x `max_records_per_partition`) | Streaming-only (v7-S4): how many rows blocked behind an earlier row of their pair one DLQ partition may hold locally *beyond* `max_records_per_partition`. These overflow holds stay outstanding (the commit never passes them) but do not count towards the pause, so the partition keeps fetching its roots' next copies; beyond this bound a blocked row is re-published (a DLQ copy). Each held row keeps its envelope in memory, so the worst case per DLQ partition is (`max_records_per_partition` + this value) x the envelope size (up to the 8 MiB envelope cap; typically a few KiB); budget the retry consumer's memory for that times its assigned partitions. `0` disables overflow holds |

```yaml
loading:
  retry_consumer:
    poll_timeout_ms: 500
    backlog_check_interval_ms: 10000
    due_batch_coalesce_ms: 200
    max_records_per_partition: 1000
    max_batch_records: 2000
    write_workers: 4
    max_overflow_held_per_partition: null   # 10 x max_records_per_partition
```

### `nodes[]` fields

| Field | Meaning |
| --- | --- |
| `label` | Node label; unique across the schema |
| `topic` | Kafka topic with this label's records; not shared with any other label, type or the coordination topic |
| `key_property` | Property used to `MERGE` nodes; must be declared under `properties` with `required: true` |
| `replicas` | Node-loader containers for this label. When omitted in bulk mode: 1 if the topic holds fewer than 10,000 records, otherwise one per partition up to 8 |
| `properties` | Map of property name to `{type, required, constraint, index}` |
| `dead_letter` | Optional `{topic, enabled}`. Opts this label into DLQ routing for malformed records (Phase 5 Slice 2); see the dead-letter section below. Omitted (the default): malformed records keep going to `rejection_log_path`, unchanged. `max_retry_attempts`, `parked_topic` and `route_missing_endpoints_in_bulk` are relationship-only and are **rejected** on a node when set to a non-default value (an explicit default loads) |

### `edges[]` fields

| Field | Meaning |
| --- | --- |
| `type` | Relationship type; unique across the schema |
| `topic` or `topics` | Kafka topic(s) with this type's records. `topic: a` is shorthand for `topics: [a]`; give exactly one of the two. `topics: [a, b]` makes one type read several topics (non-empty, unique, no blanks; list order matters for `precedence`). Each topic belongs to one entity only. An entry may also be a mapping `{name, nodes, source_key_property, target_key_property}` that binds its own label pair (see "Several label pairs per type" below) |
| `topic_conflict` | `order_by`, `precedence` or `arrival`. **Required for a type with two or more topics unless `order_by` is set** (there is no silent default); for one topic only the combination errors below apply. For a type of several label pairs it is required only when some pair has two or more topics. See "Several topics per type" below |
| `nodes.source`, `nodes.target` | Endpoint labels (may be the same label: self-referencing types are supported). The pair of every topic given as a plain name; leave it out when every `topics` entry binds its own pair |
| `source_key_property`, `target_key_property` | Must be the key property of the respective label (likewise per `topics` mapping entry) |
| `properties` | Relationship properties, same format as node properties |
| `replicas`, `mix_and_batch`, `retry`, `execution` | Tuning of the per-type loaders. Classic uses all of them (including `mix_and_batch.lane_count` and the slot timings, which exist only for classic and `--stream-clock-rotation`). Lease modes use `replicas`, `execution.worker_count` and `mix_and_batch.batch_size`, but not `lane_count`. Planned mode ignores them. Planned, lease and stream modes require `execution.mode: python_apoc` (`native_disjoint` needs `--edge-classic`) |
| `dead_letter` | Optional `{topic, enabled, max_retry_attempts, parked_topic, route_missing_endpoints_in_bulk}`. Opts this type into DLQ routing for malformed records (Phase 5 Slice 2), for both the classic and bucket-lease relationship loaders, in bulk and stream mode; planned mode ignores it and keeps failing closed (see section 12). `max_retry_attempts`/`parked_topic` additionally opt into the retry consumer's bounded-attempts/parking-lot mechanism (Phase 5 Slice 4) — but that consumer is **streaming-only**: the bucket-lease fleet launches it automatically (section 5.3); bulk's own opt-in DLQ routing (`route_missing_endpoints_in_bulk`) has no automatic retry consumer at all (see section 10's non-goal note) |
| `order_by` | Optional; names one of this type's own declared `properties`. The *alternative* to the parked-pair index (section 3): a conditional `SET` writes a row for a pair only when its `order_by` value is strictly newer than what is already stored (a tie is a no-op, not an overwrite), instead of blocking later rows of the same pair. Requires that property to be declared under `properties` — a validator rejects any other name |

```yaml
edges:
  - type: WORKS_AT
    topic: works-at-events
    nodes: {source: Person, target: Company}
    source_key_property: personId
    target_key_property: companyId
    properties:
      since: {type: date}
      role: {type: string}
    dead_letter:
      enabled: true
      max_retry_attempts: 8
  - type: FOLLOWED_BY
    topic: followed-by-events
    nodes: {source: Person, target: Person}
    source_key_property: personId
    target_key_property: personId
    properties:
      updated_at: {type: datetime}
    dead_letter:
      enabled: true
    order_by: updated_at  # newest updated_at always wins; never blocks on the pair
```

#### Several topics per type

A relationship type can read several topics. Kafka orders records only within
one partition, so the same (source, target) pair may arrive on two topics in
either order; the type must say how that is resolved:

```yaml
  - type: RATED
    topics: [ratings-2024, ratings-corrections]
    order_by: rated_at              # or: topic_conflict: precedence | arrival
    nodes: {source: Person, target: Movie}
    source_key_property: personId
    target_key_property: movieId
    properties:
      rated_at: {type: datetime}
```

| Rule | Meaning | Modes |
| --- | --- | --- |
| `order_by: <property>` | The newest value wins; a tie keeps the stored value. Deterministic. Use a `date`, `datetime`, `integer` or `float` property (a `string` property compares lexicographically, which is only right for ISO-8601 timestamps; the loader warns) | every mode, including stream |
| `topic_conflict: precedence` | The later-listed topic wins | bulk classic (`--edge-classic`), lease (`--edge-leases`) and planned only; classic and lease run the topics as sequential sub-stages (see below). Rejected when `loading.mode: stream` is written in the YAML, and again by `start --mode stream` (the schema may be shared between runs) |
| `topic_conflict: arrival` | Whichever record arrives last wins. Nondeterministic, not a guarantee; a warning is logged when the schema loads | every mode |

`order_by` together with `topic_conflict: precedence` or `arrival` is an error
(pick one); `topic_conflict: order_by` without `order_by` is an error.
Existing single-topic schemas need no change.

**Precedence in classic and lease bulk (sub-stages).** With `--edge-leases` or
`--edge-classic`, a `precedence` type reads its topics one after another in
`topics:` order, first-listed first, so later topics overwrite earlier ones
(`SET r += props`: a later topic replaces only the properties it carries). Each
topic is a full stage of its own: its own consumer group, boundary, completion
and zero-lag proof, and the next topic starts only after the previous one is
proved drained. Every topic's end offsets are snapshotted before the first
topic starts. The run fails if a topic's boundary differs from that snapshot, or
if records are appended to any already finished topic (re-checked after every
topic and once after the last), because those records would not be loaded. A
failed topic stops the type (later topics never start). A rerun must use a
fresh `--run-id` (reusing one is refused when a higher-ranked topic already
committed while a lower one still has work); each run leaves one consumer group
per topic of the type.
**Cost:** the topics of one precedence type do not run in parallel (use
`replicas` to scale within a topic); other relationship types keep their usual
parallelism, and lease mode keeps zero lock waits. Planned mode has no such
cost. Precedence works with `dead_letter` (the type's one DLQ is created once,
before the first topic starts; see below and "Dead-letter queues across topics"). In `GRAPH_LOADER_TIMING_DIR` the type keeps its `stage-<type>.json`
total, each topic adds `substage-<type>.t<rank>.json`, and the loader write
logs and metric snapshots of topic `rank` are `edge-<type>.t<rank>-<replica>.log`
and `metrics-edge-<type>.t<rank>-<replica>.prom`.

#### Several label pairs per type

One relationship type can connect several label pairs, for example `LIKES`
Person->Post and Person->Comment. **Each topic binds exactly one label pair**;
the pair is never inferred from a record (a Comment id and a Post id can be
equal). A `topics` entry is either a topic name, which uses the top-level
`nodes` and key properties, or a mapping that binds its own pair:

```yaml
edges:
  - type: LIKES
    topics:
      - name: likes-posts
        nodes: {source: Person, target: Post}
        source_key_property: person_id
        target_key_property: post_id
      - name: likes-posts-backfill        # the same pair: the conflict rule applies to it
        nodes: {source: Person, target: Post}
        source_key_property: person_id
        target_key_property: post_id
      - name: likes-comments
        nodes: {source: Person, target: Comment}
        source_key_property: person_id
        target_key_property: comment_id
    order_by: liked_at
    properties:
      liked_at: {type: datetime, required: true}
```

The same type with the top-level pair as the default for string entries:

```yaml
edges:
  - type: LIKES
    nodes: {source: Person, target: Post}
    source_key_property: person_id
    target_key_property: post_id
    order_by: liked_at
    topics:
      - likes-posts
      - {name: likes-comments, nodes: {source: Person, target: Comment},
         source_key_property: person_id, target_key_property: comment_id}
    properties:
      liked_at: {type: datetime, required: true}
```

Rules (checked when the schema loads; every error names the type and topic):

* A mapping entry needs `name`, `nodes`, `source_key_property` and
  `target_key_property` and nothing else. `topic:` takes a name only.
* A string entry uses the top-level `nodes` and key properties, which are then
  required. Top-level ones that no entry uses (every entry is a mapping) are an
  error.
* Each pair's key properties must be its labels' `key_property` (required),
  exactly as for one pair. The same `(source, target)` under two different key
  properties is an error.
* If every topic binds the same pair, the type is an ordinary single-pair type
  (`nodes` set, the same dump and behaviour as before). Otherwise it is a
  *multi-pair* type: its pairs are listed in order of first appearance and its
  top-level `nodes` is empty.
* Reversed pairs (A->B and B->A) and a self-referencing pair beside other pairs
  are supported. A type with a self-referencing pair is scheduled exactly as a
  single-pair self-referencing type: an isolated phase in classic and lease
  classic bulk (unless `--bulk-edge-overlap shared` co-schedules it), together with
  the other types in planned mode, lease bulk (`--edge-leases` implies shared
  overlap) and the stream lease fleet.
* **Conflict rule per label pair.** Records of different pairs are different
  relationships, so they never conflict. A rule (`order_by` or
  `topic_conflict`) is required only when some label pair is fed by two or more
  topics, and it applies to each such pair. A `precedence` or `arrival`
  declared on a type where every pair has one topic has no effect (a warning
  says so; `precedence` is then also accepted in stream mode). Everything else
  in "Several topics per type" applies per pair.
* Each topic still belongs to exactly one type, and the type keeps one DLQ,
  parking lot and parked-pair index (below).

How the pair is used at run time:

* **Writes.** Every record is normalised with its topic's pair and written by
  that pair's writer; no transaction mixes two pairs. In lease mode (stream and
  `--edge-leases`) each pair has its own lease coordinator (pairs run
  concurrently against the same lease server; `Person:3` is shared by both
  pairs, `Post:5` and `Comment:5` are different resources), so zero lock waits
  hold. **`execution.worker_count` applies per pair**: a replica opens up to
  `pairs x worker_count` write sessions and lease connections. Classic writes the
  pairs of one flush one after another.
* **Scheduling.** The type touches every label of every pair: classic bulk
  starts it once Person, Post and Comment are all loaded and never runs it
  beside a type touching one of them; a small Comment topic makes Comment a
  keyed lease label like any other endpoint label.
* **Planned mode** plans each pair (section 6); producer sources must name
  their topic (section 7).
* `--stream-clock-rotation` (deprecated) refuses multi-pair types before any side
  effect; use the default lease fleet. Slot gating and the shared-clock fleet
  refuse them too.

#### Dead-letter queues across topics

A multi-topic type has **one** DLQ, parking-lot and parked-pair index for the
whole type, never one per topic. The defaults are `<type>.dlq`,
`<type>.dlq.parked` and `<type>.dlq.parked-pairs` (single-topic types keep
`<topic>.dlq` and its siblings), and `dead_letter.topic` overrides the base
name (the parking lot and index follow it). Unresolved and malformed rows of
every topic of the type go to that one DLQ, and each envelope keeps its
`original_topic`, `original_partition` and `original_offset`.

* **Partitions.** `start` creates the DLQ with as many partitions as the
  type's widest topic (for example topics with 3 and 8 partitions give an
  8-partition DLQ); the parking lot and index get the DLQ's count. An existing
  DLQ is never resized: adding a wider topic later keeps its partition count
  (only its retention is verified; `start` logs a warning when the DLQ is
  narrower than the widest topic, and `kafka-topics --alter` can add partitions). The DLQ key is the pair, so one pair's
  records from all topics land in one DLQ partition.
* **One retry consumer stream per type.** The retry consumer subscribes to the
  type's DLQ once and retries every topic's rows through the type's writer.
  Metrics keep `{kind="edge", name=<type>}`; no topic label is added.
* **Ordering across topics** (the parked-pair index). The index is keyed by
  pair only, so a pair waiting in the DLQ from topic `a` also parks that pair's
  later records arriving on topic `b`, and the retry consumer replays the
  pair's records in the order they entered the index, whatever their topics.
  For a multi-topic type every index entry carries a per-pair sequence number,
  because topic names have no order (without it a record parked from `b` and a
  later one parked behind it from `a` would be retried a-first, and the older
  record would overwrite the newer one). By conflict rule:
  * `order_by`: no index is used; a retried or replayed older record never
    overwrites a newer value (the conditional `SET`), whatever topic it came from.
  * `topic_conflict: arrival` (stream or bulk): the index keeps per-pair order
    across retries as above. The exception is two records of one pair that
    arrive on two topics within the index's propagation delay (normally
    milliseconds): the topics of a type are usually read by different
    replicas, so the cross-replica best-effort case in "The retry consumer and
    the parking lot" below is the normal case here, not the unusual one. If the
    later record reaches a replica that has not yet seen the earlier record's
    index entry, it is written at once, and the earlier record's retry then
    overwrites it: **the older value wins** for that pair. Use `order_by` when
    that matters. After a partition moves to another replica (every
    assignment, also for single-topic types), that replica reads nothing from
    its new partitions until its copy of the index has caught up with the index
    as of the assignment: the partitions stay paused, so no row is written,
    checked against the index or DLQ'd from a view that lacks the previous
    owner's entries, and records of one topic-partition keep their offset
    order. There is no timeout into a stale view: while it waits the loader
    logs `reason=parked_pair_index_lagging` every 30 s after 10 s and exports
    `graph_loader_parked_pair_index_wait_seconds{kind="edge",name=<type>}`
    (seconds held, 0 once admitted); a stop still exits at once.
  * `topic_conflict: precedence`: bulk only (stream refuses it), and bulk has
    no index and no automatic retry consumer. A row DLQ'd from a lower-ranked
    topic and replayed (or retried standalone) after a higher-ranked topic's
    sub-stage was written overrides precedence: the replayed row is applied
    last and its properties win. Precedence types cannot use `order_by`, so
    check for newer rows of the pair before replaying.
* **Topic membership changes.** Change a DLQ-enabled type's topic list only
  with the type's fleet stopped and its DLQ drained (no outstanding index
  entries), not in a rolling restart. (Going back from several topics to one
  keeps outstanding entries in order: a record indexed after the change sorts
  after every leftover sequenced entry of its pair. Mixed old and new replicas
  can not sequence each other's records.) `start` refuses to run when a DLQ
  name the type used before still holds records (see the migration warning).
* **Several label pairs.** The DLQ key of a multi-pair type is
  `[source label, target label, source key, target key]`, so Person 1->Post 1
  and Person 1->Comment 1 are different pairs everywhere: a parked Comment row
  never blocks a Post row with the same keys, live or on retry, and each pair
  keeps its own order across its topics. (Single-pair types keep
  `[source key, target key]`; the parked-pair index keys are unchanged and
  carry the topic, from which the pair is derived, and a multi-pair type's
  index values also record the pair (`"labels": [S, T]`). A DLQ record written
  before a type became multi-pair is relabelled from its `original_topic` when
  the retry consumer reads it; the other way round, a single-pair type reads a
  key labelled with its own pair back as `[source key, target key]`.) The
  retry consumer retries each row through the pair its `original_topic` binds.
  **Leftovers of a removed topic park:** a row whose topic a multi-pair type no
  longer reads is `invalid_on_retry` and parks after `max_retry_attempts`
  (a single-pair type retries it through its one pair); a row whose key names
  another pair than its topic's is parked by its first retry batch, without a
  write. Adding a pair means adding a topic, so the stop-the-fleet,
  `dead_letter.topic` and drained-DLQ rules above apply. Rebinding a topic to
  another pair, or turning a multi-pair type back into a single-pair one (whose
  index ignores the topic), needs a drained DLQ first.
* **Leftover check at start.** Loaders and the retry consumer log
  `reason=index_holds_entries_the_type_no_longer_binds` (a WARNING naming the
  topics, pairs and counts) when a type's rebuilt parked-pair index still holds
  records of a topic the type no longer reads, or (multi-pair indexes, which
  record each entry's pair) of a topic now bound to another pair, including a
  type collapsed back to one pair. Until such rows park they block records of
  the same keys of the pair their topic maps to now. `start` refuses to run,
  before creating any topic, when such rows cannot be written through the type
  as it is now (a removed topic of a multi-pair type, or an entry recorded under
  another pair than the one its rows would be retried as: a rebound topic or a
  collapsed type), and names the type; drain the DLQ with the previous schema
  first, or pass `start --accept-dlq-leftovers <type>` (repeatable) to start
  anyway. A single-pair type's leftovers of a removed topic are retried through
  its one pair as before, so `start` only logs the WARNING for them. The check reads only the compacted index topic
  (one entry per unresolved DLQ'd record), never the DLQ itself, so it does not
  see an `order_by` type's DLQ (no index), bulk opt-in rows (never indexed), or
  a rebound topic of a single-pair type or of entries written before the
  index recorded pairs; for those, drain the DLQ before the change.

**Migration warning.** A single-topic type's DLQ is named after its topic. Adding
a second topic changes the default to `<type>.dlq`, which orphans records
already in the old DLQ, parking lot and index. Before adding the second topic
to a type that has (or may have) DLQ data, set `dead_letter.topic` to the old
name (`<old-topic>.dlq`); the parking-lot and index names follow it, so
`<old-topic>.dlq.parked` and `<old-topic>.dlq.parked-pairs` keep being used.
The same holds in the other direction: going back from several topics to one
renames the default from `<type>.dlq` to `<topic>.dlq`; set `dead_letter.topic:
<type>.dlq` first. `start` checks this: for a DLQ-enabled type without an
explicit `dead_letter.topic`, if `<topic>.dlq` (for any of its topics) or
`<type>.dlq` exists, is not the name the type now resolves, is not another
entity's DLQ topic and still holds records, `start` refuses before creating
any topic and names the `dead_letter.topic` to set. To let the old DLQ go
instead, replay or drain it and **delete the old topics** (`<old>.dlq`,
`<old>.dlq.parked`, `<old>.dlq.parked-pairs`): that is the expected step,
because a drained DLQ still holds its records until it is deleted. If the old
topic must stay (kept for audit, or `<type>.dlq` belongs to another deployment
on a shared cluster), pass `start --accept-former-dlq <topic>` (repeatable).

**Release note.** `main` has no models that forbid unknown keys. Unknown keys are now refused (N2) in `loading` and its blocks, `dead_letter`, `mix_and_batch`, `retry`, `execution` and `topics` mapping entries (not in the `nodes[]`/`edges[]` entries or `properties` themselves). An older image reading a
schema that uses `topics` fails at load only because the required `topic` is missing, and
it silently ignores `topic_conflict`; upgrade every image before using them. No field has ever been removed, so existing
schemas load unchanged.

**Serialization contract.** `schema.model_dump(exclude_unset=True)` round-trips
(`GraphSchema.model_validate(dump)` equals the original). A full `model_dump()`
also round-trips for the shipped schemas, but it makes defaults explicit, for
example `loading.mode: stream`, which would turn a `precedence` schema without
`loading.mode` into a load-time error. Serialize with `exclude_unset=True`.

### Properties

* `type`: `string`, `integer`, `float`, `date` or `datetime`. Any of them can be a node key.
* `required: true` means the property must be present and non-null in every record.
* `constraint: {type: uniqueness | not_null}` and `index: {type: range | text}` generate the
  corresponding DDL before loading starts.
* Every node key is indexed. A key declared without a uniqueness constraint or
  range index gets the constraint `<label>_<key>_key_unique` automatically.
  Planned mode depends on this: it looks every endpoint up by its key.

### Validation

The schema is validated before anything starts, and every error names the
offending field. These are all rejected:

* unknown endpoint labels;
* endpoint key properties that differ from the label's `key_property` or are not required;
* undeclared or unsupported property types;
* names that are not Cypher identifiers;
* duplicate labels or relationship types;
* blank topics, both `topic` and `topics` (or neither), an empty or duplicated `topics` list;
* a multi-topic type with no conflict rule (`order_by` or `topic_conflict`), both an
  `order_by` and a `topic_conflict: precedence|arrival`, or `precedence` with
  `loading.mode: stream` (for a type of several label pairs: only when some pair
  has two or more topics, and the message names that pair);
* in `topics` mapping entries: a missing `name`, `nodes` or key property, an
  unknown key, a string entry without a top-level pair, a top-level pair no entry
  uses, one label pair with two different key properties, `topic:` given a
  mapping, and (in a dumped schema) `topic_endpoints` keys that differ from
  `topics`;
* a topic shared by two labels or types, or with the coordination topic;
* `max_retry_attempts`, `parked_topic` or `route_missing_endpoints_in_bulk` under a node's `dead_letter`;
* unknown keys in `loading` and its blocks (typos);
* a node or relationship type's dead-letter topic (explicit `dead_letter.topic`,
  or the `<topic>.dlq` default, `<type>.dlq` for a type with several topics),
  parking-lot or parked-pair index topic colliding with another entity's
  dead-letter topics, or with any node/edge/coordination topic — including its
  own source topics.

### Dead-lettering malformed records and missing endpoints

Both `nodes[]` and `edges[]` accept an optional `dead_letter` block:

```yaml
dead_letter:
  enabled: true     # default true once the block is present; false keeps the file fallback
  topic: my-dlq      # optional; defaults to "<this entity's topic>.dlq" ("<type>.dlq" for a type with 2+ topics)
  route_missing_endpoints_in_bulk: true  # edges only; see "Row-level missing-endpoint DLQ" below
  max_retry_attempts: 8   # edges only, P5-S4; permanently park after this many DLQ cycles
  parked_topic: my-dlq.parked  # edges only, P5-S4; optional, defaults to "<resolved DLQ topic>.parked"
```

`max_retry_attempts` and `parked_topic` require `dead_letter.enabled: true` (the
validator rejects them otherwise — there is nothing to retry or park if the
entity isn't DLQ-routed). Both are edge-only: node records have no
"permanently unresolvable, park it" concept, only the malformed-record DLQ
above. See "The retry consumer and the parking lot" below for what consumes
these fields.

Setting `dead_letter.enabled: true` on an entity is itself the opt-in — there
is no separate global flag. An entity with no `dead_letter` block, or with
`dead_letter.enabled: false`, keeps writing malformed records to
`rejection_log_path` exactly as before: zero behavior change. This
repository's own `config/graph_schema.yaml` ships both `WORKS_AT` and
`KNOWS` with a `dead_letter` block present but `enabled: false`, so a
default `start` still uses the file sink — declaring the block alone does
not opt in, only `enabled: true` does. Once opted in, the `start` command
creates the resolved DLQ topic automatically (unlimited retention:
`retention.ms=-1`, `retention.bytes=-1`) before any loader container
launches, and every malformed record for that entity is published there
instead of the file, with its Kafka offset committing only after the DLQ
publish is acknowledged. See section 10 for the migration procedure and the
`replay-dlq` command.

#### Row-level missing-endpoint DLQ (relationships only)

A relationship row whose source or target node hasn't arrived yet (or whose
key matches more than one node — ambiguous) no longer has to fail its whole
batch. This is a *different* failure class from the malformed-record DLQ
above (`failure_class` is `missing_endpoint` or `ambiguous_endpoint`, never
`malformed`), reusing the same envelope, producer, and per-entity DLQ topic:

* **Streaming activates it unconditionally** the moment `dead_letter.enabled:
  true` resolves a DLQ topic for that edge type — no extra flag. A batch's
  resolvable rows commit in one Neo4j transaction; unresolved rows publish to
  the DLQ instead of failing the batch.
* **Bulk (classic or lease `--edge-leases`) keeps its fail-closed default.**
  A missing endpoint in bulk still fails the whole batch
  (`MissingRelationshipEndpointError`) unless `route_missing_endpoints_in_bulk:
  true` is also set on that edge type. This is a deliberate, per-type choice:
  a type whose source label is populated by a slow, separate bulk phase might
  *want* fail-closed even while another, always-co-populated type opts in.
* **Planned mode (`--edge-plan`) never reads this field.** A missing endpoint stays
  fail-closed unconditionally — no line of `planned_writer.py`/
  `edge_planner.py` is touched by this mechanism.
* **`native_disjoint` execution mode is not supported by row-level DLQ, and
  is also incompatible with bucket leases and planned mode at all** (a
  pre-existing P5-S1 rule, not new here): bucket-lease mode — streaming's
  default, or bulk `--edge-leases` — and planned mode require
  `execution.mode: python_apoc`. The CLI refuses any other value, including
  `native_disjoint`, at `start` before any container launches (`... requires
  python_apoc edges; unsupported: <types>`; a plain `start --mode bulk` says to
  pass `--edge-classic`), and the loader raises `ExecutionModeError` as a
  backstop. So the only way an edge type
  configured `execution.mode: native_disjoint` actually *runs* at all is
  the classic (non-lease) branch, `--edge-classic` — and there, setting
  `route_missing_endpoints_in_bulk: true` on it has **no effect**: that
  writer already has its own `ON ERROR CONTINUE REPORT STATUS`
  per-transaction reporting (a structurally different mechanism), and
  row-level DLQ is not wired into it. A missing endpoint under classic
  `native_disjoint` still fails the batch closed regardless of the opt-in.
  The opt-in-has-no-effect case is silent (not a schema-load error);
  the lease and planned incompatibility is loud (a `start` error naming the
  offending types). Double-check `execution.mode` and whether leases are
  active before assuming either behavior for a given edge type.
* **Missing vs. ambiguous, and which endpoint.** The DLQ envelope's
  `reason_code` distinguishes `missing_source`/`missing_target`/
  `missing_both` from `ambiguous_source`/`ambiguous_target`/`ambiguous_both`.
  A row *blocked* only because an earlier row of the same pair didn't
  resolve (see "Same-pair ordering" below) gets `reason_code=
  "blocked_by_earlier_unresolved_row"` and always `failure_class=
  "missing_endpoint"`, even when the row that blocked it was itself
  ambiguous — the discriminator that matters operationally is *why nothing
  was written for this pair yet*, not the original row's own failure kind.
* **The DLQ envelope's value carries the row in the loader's own wire input
  shape** (`{"source": {key_property: ...}, "target": {key_property: ...},
  "properties": {...}}`), not the flat internal shape a much earlier design
  sketch considered — this is what makes a row directly re-injectable
  through `replay-dlq` (section 10) once its missing endpoint arrives,
  without any translation step. The DLQ message *key* is `[source_key,
  target_key]`, not the original Kafka key (never threaded through the
  hot-path batching structures to keep their memory footprint flat) — a
  replay's partition assignment may differ from the original message's,
  which is safe because the loader never parses a relationship message's
  key, only its value.
* **Same-pair ordering, now spans every flush (P5-S4).** If an earlier row
  (by Kafka offset) of a `(source_key, target_key)` pair doesn't resolve,
  every later row of that *same pair* is parked too — even in a
  **different, later** flush, and even if the later row would resolve on
  its own — so a future retry of the earlier row can never overwrite the
  later row's already-committed newer properties. This closes the
  cross-flush gap this section used to document as out of scope: every
  live writer replica and the retry consumer share a durable,
  cross-process `ParkedPairIndex` (see "The retry consumer and the
  parking lot" below) that every unresolved row is recorded in *before*
  its DLQ publish, and every flush consults as a blocking check alongside
  its own in-flush pass. A row diverted purely because of this check gets
  `reason_code="blocked_by_earlier_unresolved_row"`, identical to the
  in-flush case — the discriminator that matters operationally is *why
  nothing was written for this pair yet*, not which pass caught it.
* **Lease hold time.** For the lease (streaming default) path, a cell's DLQ
  publishes for its unresolved rows happen synchronously, one at a time,
  *after* the Neo4j commit and *before* the lease is released — a cell with
  many unresolved rows holds its lease a little longer while those publishes
  complete. This never blocks on a Neo4j lock (no Neo4j write happens after
  the commit), only on Kafka's own acknowledgement latency.
* **At-least-once, like every other counter/DLQ path in this system —
  duplicates come from two sources, and are safe but not ordering-safe.**
  A crash between a DLQ publish's ack and the batch's offset commit can
  redeliver the same envelope on replay. So can a DLQ publish that fails
  partway through a batch with more than one unresolved row: envelopes
  `1..k-1` are already durably published when the publish for row `k`
  fails and the whole batch (per the offset-discipline rule above) resolves
  no offsets and replays from Kafka — rows `1..k-1` publish *again* on that
  replay. Both are expected, not a bug, and both are safe for the Neo4j
  side of a batch, which is always idempotent `MERGE`. They are **not**
  ordering-safe on their own, which is why the retry consumer (P5-S4)
  never replays a DLQ'd row blindly in receipt order: it checks the same
  `ParkedPairIndex` the live path writes to, by the row's own identity
  (`original_topic`/`original_partition`/`original_offset`), and skips a
  record it has already durably resolved once — a duplicate copy of an
  already-written row is silently dropped (`"duplicate_skipped"`), never
  reapplied on top of a newer row's properties.

#### The retry consumer and the parking lot

Streaming's default fleet (section 5's lease-fleet launch) includes one
extra process alongside the lease server and every edge loader: the
**retry consumer**. It reads every DLQ-and-retry-enabled edge type's DLQ
topic, holds each row until its `retry_after_ms` elapses (without ever
blocking Kafka's `poll()`), and re-attempts the write through the same
lease-fenced path a live edge loader uses. This is what makes an
edge-before-node race self-heal automatically: the relationship's row sits
in the DLQ topic, retried with exponential backoff, until its node arrives.

* **`max_retry_attempts`** (`dead_letter.max_retry_attempts`, edge-only)
  bounds how many DLQ cycles a row gets before it is *parked* instead of
  re-queued — moved to a separate, durable **parking-lot topic**
  (`dead_letter.parked_topic`, defaults to `<resolved DLQ topic>.parked`)
  with unlimited retention, never re-attempted automatically again. A
  parked row is never dropped — see "Replaying a parking-lot topic" in
  section 10 for how an operator brings it back.
* **The parked-pair index** (an internal, compacted `<resolved DLQ
  topic>.parked-pairs` topic, never read by an operator or `replay-dlq`)
  is what makes the same-pair ordering guarantee above span every flush,
  not just one. Every live writer replica and the retry consumer publish
  to it (as an add/remove idempotent set) and rebuild their own view from
  it at startup; it is bounded in size by Kafka's own log compaction, since
  an `add`/`remove` pair for the same record collapses to nothing once
  compacted. **The index is eventually consistent, not linearizable —
  what is and isn't guaranteed:**
  * **Guaranteed:** the retry path never acts on a stale view of the
    index for a due batch — it holds a read barrier
    (`watermark_snapshot()`/`caught_up()`) taken when the batch freezes,
    and only writes once its own poller has caught up to that snapshot.
  * **Guaranteed, and safe:** a lagging `remove` (a tombstone the local
    poller hasn't applied yet) only makes a row look *more* blocked than
    it truly is — the safe direction, since it can delay a resolvable
    row but never let a stale duplicate through early.
  * **Not guaranteed (best-effort only) — a real, non-self-correcting
    risk across live writer replicas:** when one pair's records span
    more than one partition or writer replica, a replica can miss
    another replica's just-acknowledged `add()` for that same pair
    before its own poller catches up, and write a later row without
    ever seeing the block. If a stale, duplicate copy of the earlier row
    is retried afterward, it can then permanently overwrite that
    already-committed later row's properties — this is the identical
    last-write-wins risk section 3 already describes for a manual
    `replay-dlq`, reachable here too across live replicas, not merely
    self-correcting on the next poll.
  * **Closed, not a residual risk:** a duplicate, stale DLQ copy of a
    row *the retry consumer itself already resolved* is not reachable
    through this race — `ParkedPairIndex.contains()` (checked by
    identity: `original_topic`/`original_partition`/`original_offset`)
    silently skips it (`"duplicate_skipped"`) regardless of index
    staleness elsewhere.
* **`order_by`** (`edges[].order_by`, naming a declared property) is the
  *alternative* to the parked-pair index for an edge type where "the
  newest value always wins, and an out-of-order retry must never regress
  it" is a better fit than "block every later row of the pair until the
  earlier one resolves." A row for a pair already present writes through
  a conditional `SET` that only overwrites when the incoming value is
  strictly newer (ties keep the existing, earlier-arriving value); an
  `order_by`-configured edge type never diverts a resolvable row to the
  DLQ just because an earlier row of the same pair is still outstanding.
  This is Decisions §10's "alternative, not additive" framing: an edge
  type declares at most one of the two mechanisms, never both — declaring
  `order_by` skips parked-pair-index construction for that edge type
  entirely, by construction, not merely by convention.
* **Batched draining.** The retry consumer does not write one record at
  a time. Every due record of an edge type, across its DLQ partitions, is
  written as one batch through the same lease coordinator and cell routing
  the edge loader uses (node-disjoint transactions under bucket leases,
  so zero Neo4j lock waits; a pair's rows stay in original order; the
  `order_by` conditional `SET` and the parked-pair index checks are
  unchanged). A record whose `retry_after_ms` has not elapsed is held in
  memory and woken from a heap (its partition keeps flowing) without blocking
  `poll()`. A later row of the same pair that is blocked behind an earlier
  row held locally (and still indexed), or behind an earlier row of the same
  batch that just failed to write, is held locally at that row's backoff and
  not re-published. Behind an earlier row this consumer re-DLQ'd (in the
  same batch, or earlier with its wake still pending) or holds or has due, it
  is also held locally, until just after that row's next attempt, and it is
  woken as soon as a row of its pair is written or parked (v7-S4). So the DLQ
  grows by one copy per root cycle, not per follower: O(roots x cycles). When
  the partition is at 90% of `max_records_per_partition` outstanding, a blocked
  row is still held, as an *overflow hold* (up to
  `max_overflow_held_per_partition`, default 10 x the cap), for the delay its
  published copy would wait (at least the type's max delay, at least as long as
  it has been blocked, up to 1 h), woken as soon as its pair's earlier rows are
  gone; overflow holds do not count towards the pause, so the partition keeps
  fetching its roots' next copies. Only beyond that bound, or when the blocker
  is elsewhere (its copy not fetched yet), is a blocked copy published, with
  the blocker's remaining wait or else that age-scaled delay, never a short
  fixed delay. Offsets are committed once
  per partition and batch, at the highest offset below that partition's
  lowest unresolved record: a record counts as resolved only once its row
  is written, or its re-DLQ/park/blocked publish is acknowledged. On
  SIGTERM the batch in flight finishes or is abandoned by the lease
  coordinator's bounded stop drain; what it resolved is committed and the
  rest is re-read after a restart. On that re-read only rows that were
  written or parked are skipped (via `ParkedPairIndex.contains()`); a
  re-DLQ'd or blocked row is retried again as a duplicate copy, which is safe
  because every copy terminates. A not-yet-due record does not stall its
  partition (due records behind it flow; the commit stops below it); a
  partition pauses only at `max_records_per_partition` outstanding records.
  A failed write is classified, and a row is never parked because of a
  system-wide condition (owner decision D1). Connectivity errors (Neo4j or the
  lease server unavailable, pool or lease timeouts, transient errors,
  `Security.*`, `Database.*`, `Cluster.*`, a read-only database such as
  `General.WriteOnReadOnlyAccessDatabase`, a drain abandon) are never counted
  against a row and are not bisected. Other failures (row-specific Neo4j codes
  such as `Statement.TypeError` or `Schema.ConstraintValidationFailed`,
  transaction timeouts, the per-transaction memory limit, an exhausted global
  memory pool (`MemoryPoolOutOfMemoryError`, which one huge row can cause on
  its own), and any error not recognised) bisect the
  failing batch. A row that then fails alone is counted only with evidence
  that the failure is its own: a row-specific code, or a row of the same
  relationship type (and, for a type of several label pairs, of the same
  pair) that was *written* after the row's first uncounted
  failure (an unresolved outcome is no evidence: it may only have read).
  After 3 counted failures the row is routed as unresolved (`write_failed`)
  so the usual attempts-then-park path applies. Without evidence the row is
  not counted and waits, however long that takes: a lone row with an
  unrecognised error and no other traffic of its type, or every row during a
  read-only window, waits instead of parking.
  While a type's writes fail with nothing written, it is *stalled*: it makes
  one failing attempt per backoff period (0.5 s doubling to 5 s; once stalled,
  one row at a time, every row in turn), the gauge
  `graph_loader_retry_stalled_seconds{kind="edge",name=<type>}` counts the
  seconds, a `reason=retry_stalled` ERROR is logged after 2 minutes and every
  5 minutes after that, and the example alert `GraphLoaderRetryStalled` fires
  after 10 minutes. The stall ends at the first written row. After 10 minutes
  of nothing but authentication or missing-database errors the process exits
  non-zero; in `--mode stream` the CLI supervises retry-consumer containers
  like edge loaders, so a retry-consumer crash or fatal exit stops the fleet
  with exit code 1 (owner decision D2). A row that failed alone is afterwards
  only written alone, after the type's other rows, so it never holds up their
  writes. A later row of a pair that is blocked only behind earlier rows of
  the same batch is written right after them, in order; one blocked only
  behind a row that this batch wrote or parked is retried locally, not
  re-published.
  See `loading.retry_consumer` for the bounds. A crash after a re-DLQ publish is acknowledged but before the offset commit leaves two copies of the envelope in the DLQ. The Neo4j write stays idempotent (MERGE). Both copies may run their own attempt cycle, so the row can receive one extra attempt, and in the worst case it can be parked twice. A copy whose index entry was already removed by the other copy is skipped (duplicate_skipped). Same at-least-once contract as P5-Q5.
* **Standalone invocation.** `python -m src.loader.retry_consumer --config
  ... --dlq-topics <comma-separated>` runs the retry consumer manually
  against a subset of topics — the escape hatch for bulk's own opt-in DLQ
  backlog (bulk has no fleet-launched retry consumer companion at all;
  see section 10's non-goal note). Add `--lease-address host:port` to
  fence against a *live* fleet's lease server; omit it for a finished
  bulk run with no live fleet left to contend with, in which case writes
  fall back to the classic, non-leased Neo4j session path (ordinary
  Neo4j locking, safe in practice because there is no other writer left
  to collide with).

## 4. The input contract (publishing records)

Publish every record as a JSON value on the topic its label or type declares.

**Node record**: the node's properties by name. The Kafka key is the node key as text.

```json
{"person_id": 7, "name": "Ada", "joined": "2024-01-31"}
```

A later record for the same key updates the node (`SET n += props`): the
properties it carries are overwritten and the others are kept.

**Relationship record**: the endpoint envelope. The Kafka key is the **source key** as text.

```json
{"source": {"person_id": 7}, "target": {"name": "Paris"}, "properties": {"since": "2020-05-01"}}
```

Repeated records for one `(source, target)` pair `MERGE` a single
relationship and apply `SET r += props` in topic order, so the last record
wins for each property it carries.

Rules:

* **One label pair per topic.** A topic of a type with several label pairs
  (section 3) carries only records of the pair it binds, with that pair's key
  property names (`{"target": {"comment_id": 7}}` on `likes-comments`). The
  pair is taken from the topic, never from the record: a `post_id` record on
  `likes-comments` is invalid.
* **Key relationship records by source key.** With the default partitioner,
  every record of a pair then lands in one partition. Kafka only orders
  records within a partition, and that order is what "last write wins" follows.
* **Wire formats:** dates are `YYYY-MM-DD`; datetimes are ISO-8601 with `T`
  and an offset (`2024-01-31T12:00:00+01:00` or `...Z`); numbers must be
  finite JSON numbers. `integer` properties must be JSON integers, and `float` properties also accept integers.
* **Only declared properties.** A record with an undeclared field, a missing
  required field or a wrong type is invalid. The classic modes write it to the
  rejection log and continue; planned mode, when it plans from the edge topics
  (the bulk default), does the same into a per-planner-worker rejection file
  (section 6, "Guarantees"). Only a missing endpoint at write time fails a planned run.
* **Publish before you start (bulk).** A bulk run captures each topic's end
  offsets as its *boundary* when it starts. Records produced afterwards fail
  the run rather than being half-loaded.
* **Partitions.** Node loaders parallelize per partition, so use at least as
  many partitions as you want node-loader replicas (4 is a good default).
  Relationship topics can have any partition count.

The loader doesn't care what produces the records: any language, Kafka
Connect, a CDC pipeline or a script. [`examples/social/social_graph.py`](../examples/social/social_graph.py)
is a small, commented reference producer.

## 5. Running a load

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092 NEO4J_URI=bolt://localhost:7687
export NEO4J_USERNAME=neo4j NEO4J_PASSWORD=...          # NEO4J_DATABASE=... optional
export LOADER_KAFKA_BOOTSTRAP_SERVERS=kafka:29092 LOADER_NEO4J_URI=bolt://neo4j:7687

python -m src.cli start --mode bulk --config my_graph.yaml     # planned mode is the bulk default
```

The command blocks until the load has finished and been verified, then exits
`0`. Any failure (invalid record, writer crash, timeout) exits non-zero after
stopping every container the run started.

### Relationship modes

All modes produce the same graph. They differ in how relationship writes are
scheduled:

| Mode | Flags | Lock waits | Relative speed | Use when |
| --- | --- | --- | --- | --- |
| **Planned** (bulk default) | none, or `--edge-plan` | none, by construction | fastest | every bulk load |
| Planned, producer plan | `--edge-plan PLAN_FILE` | none | fastest; skips the per-type edge topics | the producer already holds all edges (section 7) |
| Bucket leases (bulk default when the schema enables relationship `dead_letter`) | `--edge-leases` | none | as fast as shared overlap, slower than planned | malformed-record DLQ routing in bulk; per-type edge topics without a planning pass |
| Classic, shared overlap | `--edge-classic --bulk-edge-overlap shared` | frequent (explicit ordered APOC locks prevent deadlocks) | fast | debugging, comparisons |
| Classic, label-serialized | `--edge-classic` | frequent | slowest | compatibility with earlier runs; the one mode that can wait on Neo4j locks |

Every mode above, and the stream lease fleet, supports relationship types
with several label pairs (section 3); the deprecated `--stream-clock-rotation`
refuses them. Lease modes run one lease coordinator per pair
(`execution.worker_count` applies per pair); classic writes the pairs of a flush
one after another.

**Which mode a plain `start --mode bulk` uses.** Exactly one of `--edge-plan`,
`--edge-leases` and `--edge-classic` may be given (combining them is an error).
Without any of them:

1. If any relationship type enables `dead_letter` (`enabled: true`, which also
   covers `route_missing_endpoints_in_bulk`, `max_retry_attempts`, `parked_topic`
   and a custom `topic`; a block with `enabled: false` does not count), the
   CLI uses the bucket-lease mode, because planned mode ignores `dead_letter`
   (it rejects invalid records to a file, not to the DLQ topic). Lease mode also
   has zero lock waits.
2. Otherwise it uses planned mode with every planned option derived exactly as
   for a bare `--edge-plan` (section 6).
3. A plain start never reaches classic. If some relationship has a
   non-`python_apoc` execution mode (`native_disjoint` is disjoint by pair, not
   by node, so it can wait on endpoint locks and neither zero-wait mode supports
   it), the start is refused unless `--edge-classic` is given.

The choice is logged: `stage=launch edge_mode=<planned|lease|classic>
reason=<default|dead_letter|flag>`. `topic_conflict: precedence`
therefore works in every bulk mode (planned orders topics by task; lease and
classic run them as sub-stages); a type that enables `dead_letter` together
with `precedence` runs under the lease default, with one DLQ for the type
(section 3, "Dead-letter queues across topics"). Node `dead_letter` does not influence the choice. Stream mode is
unchanged: the lease fleet is the default, and `--edge-classic` is rejected there.

> **Migration note (release note).** Classic was the default before; pass
> `--edge-classic` to keep it. A plain `start --mode bulk` now runs planned
> mode, which differs from classic in these ways:
>
> * Invalid relationship records go to a per-planner-worker rejection file
>   (same entry format as the classic rejection log), not to the classic
>   per-type file. `dead_letter` is not applied (schemas that enable it get the
>   lease mode instead, so their DLQ routing is unchanged).
> * A rerun with the same `--run-id` re-plans and re-reads everything (every
>   write is an idempotent `MERGE`); it does not resume. After a `kill -9` it
>   needs `--replace-stale-plan-topic` to remove the left-over chunk topic.
> * The run needs Kafka topic-creation rights and a chunk topic with a raised
>   `max.message.bytes` (created per run, deleted afterwards), plus planner
>   memory (`--plan-memory-mb`) and spill disk (`--plan-spill-dir`). On managed
>   or ACL-restricted Kafka where that is not allowed, pass `--edge-leases`
>   (also zero lock waits, per-type topics only).
> * `--bulk-edge-overlap` and `--max-concurrent-bulk-edges` tune the classic
>   loaders only; without `--edge-classic` they are ignored with a warning.

Measured on the IMDb 20k-seed subset (8.9M relationship records) on the same
host: planned about 60 s end-to-end, shared overlap about 87 s (with 82% of
edge-query time spent waiting on locks), label-serialized about 525 s before
other improvements. See [section 9](#9-performance-guide).

### `start` options

| Option | Default | Meaning |
| --- | --- | --- |
| `--config PATH` | `config/graph_schema.yaml` | Schema YAML |
| `--mode {bulk,stream}` | – | Bulk loads everything up to the boundary and exits; stream keeps consuming |
| `--network NAME` | `graph-loader-net` | Docker network for all containers |
| `--skip-image-build` | off | Use existing `graph-loader-node` / `graph-loader-edge` images instead of building them |
| `--bulk-timeout-seconds N` | `300` | Stall limit of each monitored bulk stage. Long stages are progress-based: node draining, waiting for the edge plan (the planner's heartbeat) and the planned dispatch fail only after `N` seconds without progress, however long they run |
| `--edge-plan [PLAN_FILE]` | the bulk default (no file) | Planned mode; with a file, load a producer-written plan |
| `--edge-classic` | off | Classic per-type APOC-locked relationship loaders (bulk only); the one mode that can wait on locks. Exclusive with `--edge-plan` and `--edge-leases` |
| `--plan-writers K` | Neo4j processors + 2 | Writer containers shared by all relationship types |
| `--plan-workers W` | `min(4, CPUs)` | Planner processes reading the relationship topics |
| `--plan-buckets B` | derived | Hash buckets per node label |
| `--plan-hub-degree H` | chunk rows | Minimum total degree for a node to get its own resource (`0` disables hubs) |
| `--plan-chunk-rows C` | derived | Maximum rows per chunk transaction |
| `--plan-partitions N` | `4` | Partitions of the run's chunk topic |
| `--plan-topic NAME` | `graph-loader-edge-chunks-<run id>` | Chunk topic (created per run) |
| `--plan-dir DIR` | `$GRAPH_LOADER_TIMING_DIR` or `var/edge-plans/<run id>` | Where the plan file and ledger are written |
| `--plan-memory-mb M` | a quarter of the available RAM over the planner processes, 256–4096 | Memory budget of each planner process; beyond it the planner spills to disk (see [large inputs](#large-inputs-bounded-memory-planning)) |
| `--plan-spill-dir DIR` | the plan directory | Temporary spill files of the planner, deleted when planning ends |
| `--keep-plan-topic` | off | Keep the chunk topic after the run |
| `--node-replicas-min-records N` | `10000` | Labels without YAML `replicas` get 1 loader below this many records |
| `--max-node-replicas N` | `8` | Cap on derived node-loader replicas |
| `--edge-leases`, `--bucket-lease-buckets`, `--bucket-lease-outer-batch-size`, `--bucket-lease-keyed-max-nodes` | off (on by default when the schema enables relationship `dead_letter`), 16, 32000, 1000 | Bucket-lease mode, with `--mode bulk`. The same `--bucket-lease-*` flags also tune the bucket-lease coordinator that `--mode stream` uses by default (section 5.1) — `--edge-leases` itself stays bulk-only |
| `--bulk-edge-overlap {labels,shared}` | `labels` | Classic scheduling of per-type relationship loaders; only takes effect with `--edge-classic` (in planned and lease modes it is ignored with a warning) |
| `--max-concurrent-bulk-edges N` | `0` (unlimited) | Classic mode: cap on concurrently running relationship types; only takes effect with `--edge-classic` (ignored with a warning in planned and lease modes) |
| `--run-id ID` | generated | Explicit run identity: scopes container names/labels, the default `--plan-dir`, the planned chunk topic (`--replace-stale-plan-topic` needs it), the lease server and the **bulk** relationship consumer groups (stream groups do not contain it); in stream mode it only lets `start` migrate a legacy run-scoped group's offsets (section 10) |
| `--accept-former-dlq TOPIC` | – | Repeatable. Start although a relationship type's former default DLQ topic (`<topic>.dlq` or `<type>.dlq`) still holds records (section 3, "Migration warning") |
| `--accept-dlq-leftovers TYPE` | – | Repeatable. Start although the type's parked-pair index still holds records of a topic it no longer reads or of a topic now bound to another label pair (section 3) |
| `--retry-consumer-replicas N` | `1` | Stream mode: replicas of the DLQ retry consumer (section 5.3) |
| `--replace-stale-plan-topic` | off | With `--edge-plan` (no file) and an explicit `--run-id`: delete the run's `graph-loader-edge-chunks-<run id>` topic left behind by a `kill -9`ed run, but only if no container of that run is alive; otherwise the start is refused. Without the flag a leftover topic makes the start fail with an actionable error (use a new `--run-id`, or this flag) |
| `--stream-group-member-wait-seconds S` | `session_timeout_ms` + 5 s | Stream mode: how long to wait for the members of a stable consumer group to expire before refusing to start (about 50 s with the default 45 s session timeout when another fleet is live; section 10, "Consumer groups and restarts") |
| `--stream-clock-rotation` | off | **Deprecated, removed one release after `plan/v5/slice-1`.** With `--mode stream`, use the Phase 4 global batch clock and rotating/isolated-phase scheduling instead of the default bucket-lease fleet (section 5.1) |

`--lease-buckets`, `--lease-outer-batch-size` and `--lease-keyed-max-nodes` are
accepted as deprecated aliases for the `--bucket-lease-*` flags above, for one
release; using one logs a `stage=deprecation` WARNING (once per flag), and
giving an alias and its new name with different values is an error.

Other commands:

```bash
python -m src.cli status --config my_graph.yaml   # list running node-loader containers
python -m src.cli stop [--loader NAME]            # stop loader containers
python -m src.cli replay-dlq --node-label Person --dry-run   # preview a DLQ replay (section 10)
```

### Streaming mode

`--mode stream` keeps node and relationship loaders running and consumes new
records continuously. Planned mode and the bulk-only options above do not
apply to streaming.

**Topics must exist first.** Before building images or launching any container,
`start --mode stream` checks that every node and relationship topic the schema
declares exists on the broker. If any is missing it logs
`missing input topics: <names>. Create or publish them first ...`, exits with
code 1 and launches nothing. Create the topics (or run your producer so it
creates them) and rerun. graph-loader never auto-creates these topics because
the partition count is the producer's choice. Bulk mode does not run this
check: its loaders are not known to fail the same way on a missing topic, so it
is left unchanged.

#### 5.1 Relationship write concurrency (default: bucket leases)

By default, every relationship type — conflicting-label, non-conflicting, and
self-referencing alike — writes through the same run-scoped bucket-lease
coordinator bulk's `--edge-leases` already uses (section 5, "Bucket leases"
row): a supervised, restartable TCP process that grants each relationship
transaction an exclusive, node-disjoint resource set before it writes, so
concurrent relationship transactions across the whole fleet never wait on a
Neo4j lock, including for self-referencing types (which no longer need — or
get — a separate isolated phase). `--bucket-lease-buckets` /
`--bucket-lease-outer-batch-size` / `--bucket-lease-keyed-max-nodes` tune it
for streaming exactly as they do for bulk `--edge-leases`; `loading.bucket_lease`
in the schema tunes the coordinator's supervision (restart backoff, the
post-restart quiet period) and the Neo4j transaction-timeout fencing that
makes a lease-server restart safe (see the limitations note below).

**Node writes are leased too.** In stream mode the zero-wait guarantee covers
node writes as well as relationship writes: the CLI starts the lease server
*before* the node loaders, and each node loader splits a flush into the same
per-resource cells edges use (a node and every relationship touching it map to
the same `Label:bucket` or keyed-label resource). It leases one cell at a
time, writes it in one fenced transaction (Neo4j transaction timeout,
rolled back if the lease is lost) and releases it; offsets are committed only
after every cell of the flush is durable. A node `MERGE` therefore never
waits on a concurrent relationship `MERGE` of the same node. Node cells are
smaller than a whole flush, so a larger `loading.unwind_batch_size` keeps node
transactions well filled. Bulk mode is unchanged (nodes load before edges).
On shutdown the CLI stops the edge loaders, then the node loaders (which drain
only what they already hold), then the lease server. Node metrics use the
existing catalogue (`graph_loader_lease_wait_seconds{kind="node"}`).

**Deprecated escape hatch.** The Phase 4 global batch clock
(`loading.coordination`) with rotating bucket leases and an isolated
self-reference phase — streaming's *only* mechanism before this slice — is
still available for one release via `--stream-clock-rotation`, reproducing
today's behavior exactly: no zero-wait guarantee inside a slot, and
self-referencing types run in a separate serial phase. It will be removed
after that release; there is no reason to opt into it except to reproduce an
existing deployment's exact behavior while migrating. `coordination.py`,
`rotation.py`, `clock_runner.py` and `slot_admission.py` are reachable only
through this flag now; `isolation.py`'s self-reference planning is still used
by bulk's own isolated-phase scheduling, independent of this flag.

#### 5.2 Missing or ambiguous relationship endpoints (row-level DLQ)

A relationship whose source or target node hasn't arrived yet — the
edge-before-node race, expected in a continuous stream, not exceptional — no
longer fails the whole batch. The affected row is routed to that edge type's
DLQ topic and every other row in the same batch still commits, in the same
Neo4j transaction. See section 3's "Dead-lettering malformed records and
missing endpoints" for the schema opt-in, envelope shape, and this
mechanism's documented limits (same-pair ordering scope, `native_disjoint`
exclusion, at-least-once duplicates).

#### 5.3 The retry consumer (automatic companion process)

The bucket-lease fleet described in 5.1 launches one more process alongside
the lease server and every edge loader whenever at least one edge type in
the schema has DLQ routing enabled: the **retry consumer** (section 3's
"The retry consumer and the parking lot" describes what it does and how
`max_retry_attempts`/`order_by` interact with it). It launches after the
lease server and before the edge loaders, is fenced by the same lease
server the live fleet uses, and is stopped and removed in the same
teardown pass as the edge loaders. `--retry-consumer-replicas N` (default
`1`) runs more than one replica in a single consumer group across every
resolved DLQ topic. A schema with no DLQ-enabled edge type launches zero
retry-consumer containers — there is nothing for it to do.

#### 5.4 Riding out a Neo4j outage

A live stream loader whose writes are lease-fenced (every node loader and every
relationship loader of the default lease fleet, which `start --mode stream` runs
with a bucket-lease server) no longer exits when Neo4j becomes unavailable, so a
Neo4j restart, failover or short outage no longer stops the fleet. Only fenced
writers ride out an outage: their transactions carry the Neo4j timeout `T`, and an
ambiguous failure quarantines their leases, so a retry can never wait on the locks
of a transaction that is still open on the server. When a write fails with a
*connectivity-class* error, the loader does the following:

* The failed transaction has already rolled back. The loader holds no bucket
  lease while it waits. It releases the leases whose outcome is known. For the
  others it closes the lease connection, and the lease server keeps those leases
  fenced for `T + margin`, as for any disconnect. A restarted or late
  transaction therefore never overlaps another writer: no Neo4j lock wait.
* It commits nothing. Offsets are never committed past the unwritten batch.
* It pauses its assigned partitions and keeps polling Kafka about five times a
  second, so `max.poll.interval.ms` is never exceeded and the consumer group
  does not rebalance. A record that was already prefetched is kept and written
  after the held batch.
* It waits a backoff that starts at 0.5 s and doubles up to 30 s, then retries
  **the same batch in the same order**. The retry is the redelivery a crash and
  restart would cause, minus the restart: rows of the batch that committed
  before the outage are rewritten (idempotent `MERGE`), and each pair keeps its
  order. A row that an earlier attempt of the held batch already dead-lettered
  (index entry added, DLQ copy acknowledged) stays blocked, but the retry neither
  re-adds it to the parked-pair index nor publishes another copy. Its first copy
  is either still outstanding (the retry consumer will handle it) or already
  resolved (written or parked). Both alternatives are wrong: a re-add without a
  copy leaves an entry nothing removes, so the pair stays blocked forever, and a
  re-add with a new copy can resurrect a row the retry consumer already wrote,
  so an older value is applied after a newer one. Rows whose earlier publish
  failed or never happened are routed in full. The memory covers one held batch
  and is dropped once it commits or is handed over.

*Connectivity-class* uses the retry consumer's error taxonomy
(`src/loader/write_errors.py`): Neo4j unavailable (`ServiceUnavailable`,
`SessionExpired`, a defunct connection), a connection pool or acquisition
timeout, a transient error once the writer's own bounded retries are spent,
database, cluster and read-only-database codes, authentication codes, and the
lease server being unreachable. Everything else fails as before:

* row-level and unknown errors keep today's behaviour (DLQ routing where the type
  has a DLQ, otherwise the loader exits 1 and supervision stops the fleet);
* capacity errors (a transaction timeout or the per-transaction memory limit),
  server-wide memory-pool pressure, and the transient
  `Neo.TransientError.General.OutOfMemoryError` and `StackOverFlowError` (one huge
  batch can cause them by itself, so retrying it could stall the loader forever)
  also fail as before. The retry consumer's own taxonomy is unchanged;
* an authentication or missing-database error (`Neo.ClientError.Security.*`,
  `Neo.ClientError.Database.DatabaseNotFound`) is ridden out only while it might
  heal. After 10 minutes of nothing but such errors (gaps under 60 s; only
  another Neo4j answer resets the clock), the loader logs `reason=neo4j_fatal`
  and exits 1, and supervision stops the fleet. This is the retry consumer's
  fatal rule (owner decision D2).

**Signals.** The loader logs one `WARNING` when the outage begins
(`stage=write-outage ... reason=neo4j_unavailable`) and one per failed retry
(`stage=write-outage-retry`). Once the outage has lasted 30 s it logs an `ERROR`
with `reason=neo4j_outage`, repeated every 5 minutes, and on recovery it logs
`stage=write-outage-recovered outage_s=... attempts=...`. The gauge
`graph_loader_write_outage_seconds{kind,name}` counts the outage's seconds (0
while writing), and the example alert `GraphLoaderWriteOutage` fires above
120 s (`docs/MONITORING.md`, section 6).

**Stopping during an outage.** A `SIGTERM` or `stop` while a loader is waiting is
honoured within about 0.2 s, or within the 5-second lease drain budget if the
retry is waiting for a lease (see "How long one attempt can block" below for the
cases that are not interrupted). The loader logs `stage=edge-drain-abandoned`
(or `stage=node-drain-abandoned`) with `reason=neo4j_outage`, commits nothing for
the held batch and exits `0`. The batch is redelivered on the next run.

**Rebalances during an outage.** If the partitions are revoked anyway (for
example, a sibling replica leaves), the loader never writes inside the rebalance
callback. It hands the unwritten batch to the next owner uncommitted, which
re-reads it from the committed offset, and carries on. Malformed records of
those partitions that were rejected but not yet committed are rejected again by
the next owner, as after a crash. A batch that is handed over (or redelivered
after a crash or a stop) loses the per-batch DLQ memory above. If an earlier
attempt had dead-lettered some of its rows and the retry consumer has resolved
them since, the next owner routes them again. That is the replay behaviour the
DLQ already had before this change; it is rare here because a handover during
an outage needs a rebalance. The retry consumer skips such a resurrected copy
instead of writing it (`reason=stale_resurrected_copy_skipped`, section 10,
"Redelivered flushes"), within the limits listed in section 12. A revoke does not end the outage: only the
backoff restarts, while the outage clock, the gauge and the ERROR cadence carry on
until a write succeeds, so frequent rebalances cannot keep the alert from firing.

**Not covered.** Bulk mode, in every variant (planned, `--edge-leases`,
`--edge-classic`, static replicas, bulk node loaders), keeps failing fast on a
Neo4j error: the run exits 1, and rerunning completes it. Unleased stream writers
also keep failing fast, because nothing fences them: the deprecated
`--stream-clock-rotation` path (its edge loaders, its isolated self-reference
phase and its node loaders), a stream run with no relationship types (its node
loaders run without a lease server) and any stream loader started without a
bucket-lease address. The retry consumer already waited out outages (section 3,
"The retry consumer and the parking lot") and is unchanged.

**How long one attempt can block.** The loader polls Kafka and checks for a stop
between attempts, not inside one. One attempt can block for the lease wait (up to
`T + margin`, 35 s by default, when the leases of a previous attempt are still
quarantined), for a lease-server reconnect (up to
`loading.bucket_lease.reconnect_timeout_ms`, 60 s by default, while the lease
server is down), or for a Neo4j driver call. Against a stopped Neo4j container the
driver fails at once. Against a blackholed host it blocks until its 30 s connection
timeout. All of these stay well under `max.poll.interval.ms` (300 s). A stop is
honoured within the 5-second drain budget while the attempt waits for a lease, but
not during a blocked driver connect or lease reconnect, which can outlast Docker's
10-second stop grace for edge loaders: the container is then killed (exit 137).
Nothing is committed either way, and the batch is redelivered.

### Multiple topics per relationship type: running and operating

The schema side (`topics:`, the conflict rule, DLQ naming) is in section 3, "Several topics per type". This
subsection is the operator's view. A complete, verified example is
[`examples/multi_topic`](../examples/multi_topic/README.md).

**Which mode for which rule**

| Rule | Planned (bulk default) | `--edge-leases` | `--edge-classic` | `--mode stream` |
| --- | --- | --- | --- | --- |
| `order_by` | yes | yes | yes | yes |
| `topic_conflict: precedence` | yes, the plan orders chunks by topic index | yes, sequential sub-stages | yes, sequential sub-stages | refused (schema load and `start`) |
| `topic_conflict: arrival` | yes | yes | yes | yes |

A plain `start --mode bulk` runs planned mode (zero lock waits by construction), so an `order_by` or `precedence`
type needs no extra flag. `--edge-classic` is the one mode that can wait on Neo4j locks; it is never chosen for
you, and it is rejected with `--mode stream`. When the schema enables relationship `dead_letter`, the bulk default
is `--edge-leases` instead (the CLI logs `stage=launch edge_mode=<planned|lease|classic> reason=...`).

**`--topics` on the edge-loader entrypoint.** The edge-loader container (`python -m src.loader.edge_loader`)
accepts `--topics a,b` (comma-separated, no blanks) in place of `--topic`; the two are mutually exclusive and,
with neither, the loader reads every topic of the type in the schema. The CLI passes `--topic` to a container for
a single-topic type and `--topics` for several. The container's `--run-id`, `--replica-id` and `--replica-count`
work as before. In bulk, replica `i` of `N` owns entries `i, i+N, ...` of the sorted `(topic, partition)` list, so
small topics do not leave replicas idle. In a precedence sub-stage each container is given exactly one topic.

**Empty partitions.** A partition that has never held a record (low watermark equals high watermark when the
boundary is captured) is not expected to have a committed offset and does not block bulk completion or the
zero-lag proof. A multi-topic type often has one busy topic next to a small or empty one; the empty one does not
hold the stage open. A partition with records and no commit still blocks, as before.

**Late records fail closed.** The boundary is the high watermark of every `(topic, partition)` of the type at
capture time. Records appended to any of them afterwards make the stage fail rather than being skipped silently.
For a precedence type the end offsets of all its topics are snapshotted before the first sub-stage, and every
finished topic is re-checked after each sub-stage.

**Consumer groups and run ids**

* A type without `precedence` uses one consumer group for all its topics, named as for a single-topic type. In
  bulk (`--edge-leases`, `--edge-classic`) the group is run-scoped, with the stage's derived run id: a shared or
  lease stage uses `<prefix>-<type>-<R>.bulk.0.<h>` and (classic only) an isolated self-reference phase
  `<prefix>-<type>-<R>.isolated.<i>.<h>`, where `<R>` is the run id (`--run-id` or generated), `<i>` the phase
  index and `<h>` a 24-character hash. In stream mode the group is the stable `<prefix>-<type>` (section 10,
  "Consumer groups and restarts").
* A `precedence` type in `--edge-leases` or `--edge-classic` runs one full stage per topic, in `topics:` order, so
  one run leaves **N consumer groups per precedence type** (one per topic), named
  `<prefix>-<type>-<R>.bulk.0.<h>.topic.<i>.<h2>` (`<i>` is the topic's position in run order, `<h2>` a second
  hash; an isolated self-reference uses `....isolated.<i>.<h>.topic.<i>.<h2>`). Each sub-stage has its own
  boundary, completion and zero-lag proof, and the next topic starts only after the previous one is proved drained.
* **Use a fresh `--run-id` for every rerun of a precedence type.** Reusing one is refused when a higher-ranked
  topic already has commits while a lower one still has work (continuing would let the lower topic overwrite the
  winner). Without `--run-id` the CLI generates a fresh one.
* Delete the groups of finished runs you do not need: `kafka-consumer-groups --bootstrap-server ... --delete
  --group <name>`. The node loaders' groups (`<prefix>-<Label>`) must also be deleted, or the nodes skipped, before
  loading the same topics again.
* Planned mode creates no per-topic sub-stage groups: its planner reads the topics with its own run-scoped
  groups, whatever the rule.

**Dead-letter queues (one per type).** A DLQ-enabled multi-topic type has one DLQ, parking lot and parked-pair
index for all its topics: `<type>.dlq`, `<type>.dlq.parked`, `<type>.dlq.parked-pairs` (section 3, "Dead-letter
queues across topics"). `start` creates the DLQ with the widest topic's partition count, once per type (also for
precedence sub-stages). `replay-dlq --edge-type T --source-topic <topic>` replays one topic's records, each back to
its own `original_topic` (section 10). Precedence with `dead_letter` runs in bulk under the lease default.

* **Before adding a second topic to a type that has DLQ data, set `dead_letter.topic` to the old name**
  (`<old-topic>.dlq`); going back from several topics to one, set it to `<type>.dlq`. Otherwise the default name
  changes and the old DLQ, parking lot and index are orphaned; `start` refuses while a former default DLQ still
  holds records and names the setting to add. To retire the old topics instead, replay or drain them and delete
  them (`start --accept-former-dlq <topic>` keeps one that must stay, for audit or because another deployment
  owns it).
* Change a DLQ-enabled type's topic list only with its fleet stopped and its DLQ drained, not in a rolling
  restart. `start` refuses while the type's parked-pair index still holds records of a removed topic or of a
  topic now bound to another label pair (`start --accept-dlq-leftovers <type>` starts anyway; those rows park).
* In stream, after each rebalance a loader holds its new partitions until its parked-pair index view has caught
  up (`graph_loader_parked_pair_index_wait_seconds`, `reason=parked_pair_index_lagging`).

**Planned-mode limits.** The plan orders chunks by `(topic index, first partition)`, packed into one number with 12
bits for the topic index and 20 bits for the partition: at most 4,096 topics per relationship type and 2^20
partitions per topic. A type beyond that fails planning with `topic index N / partition P exceed the planner's order
range` (the schema is not rejected earlier). Each topic that touches a plan unit adds at least one chunk to it, so a
type with many small topics produces more, smaller transactions than one large topic (section 6).

**The local stack versus `.env`.** The project `.env` may point at remote systems, so export every connection
variable yourself before starting anything:

```bash
export NEO4J_URI=bolt://localhost:7687 NEO4J_USERNAME=neo4j NEO4J_PASSWORD=... NEO4J_DATABASE=mydb
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export LOADER_KAFKA_BOOTSTRAP_SERVERS=kafka:29092 LOADER_NEO4J_URI=bolt://graph-loader-neo4j:7687
```

Who reads `.env` and who wins:

* `python -m src.cli` and the loader containers never read `.env`; they use the environment only.
* `docker compose` substitutes variables from `.env` but lets an exported variable win. Do not run it on a shared
  stack from a shell where you have not checked what `.env` contains.
* `examples/imdb/publish_and_load_nodes.py` and `scripts/cleanup_imdb_local_load.sh` call `load_dotenv(...,
  override=False)`: `.env` only fills in variables you did not export.
* `make monitoring-up` sources `.env` as a shell script but then re-applies everything you exported, so an
  exported variable wins and `.env` only fills in variables you did not set (an exported empty value also wins).
  Export the connection variables for the stack you mean before running it.

Print the resolved Neo4j URI and Kafka address (never the password) before the run and check them.

**Reading a multi-topic run:** see section 8 ("Multi-topic runs") for the sub-stage log lines, the
`substage-*.json` rows, the lag series bound and the planner rejection count.

## 6. Planned mode in depth

### The pipeline

1. **Setup.** The CLI applies the schema DDL, probes Neo4j's available
   processors, and captures the *bulk boundary*: the low and high watermark
   of every partition of every relationship topic.
2. **Nodes and planning in parallel.** Node loaders start, and at the same
   time `W` planner processes read every relationship record up to the
   boundary, once. Each process owns one group of topic partitions and has one
   consumer. The planners validate records with the same normalization the
   classic loaders use. Each process stays within its memory budget
   (`--plan-memory-mb`) and spills to disk beyond it, so the input can be much
   larger than RAM (see [large inputs](#large-inputs-bounded-memory-planning)).
3. **Resources.** Every node maps to exactly one resource:
   * a *hub* resource of its own if its total degree across all relationship
     types is at least `H`, for example `Genre:Drama` or `Language:en` in IMDb;
   * otherwise one of its label's `B` hash buckets, for example `Person:3`.
4. **Units and chunks.** Records are grouped into *units*, keyed by
   `(type, source resource, target resource)`. Each unit is cut into *chunks*
   of at most `C` rows. The chunks go to a run-scoped Kafka topic, one message
   per chunk, and are described in `edge-plan.json`. Each chunk entry records
   its type, resources, row count, priority, predecessor and Kafka offset.
5. **Dispatch.** A run-scoped dispatcher hands chunks to a pool of `K`
   writers. A chunk is granted only when all three of these hold:
   * both of its endpoint labels are loaded;
   * the previous chunk of its unit has committed;
   * **none of its resources is held by a running chunk**.

   Chunks are granted highest priority first: the busiest resources and the
   longest hub chains go first, so no serial tail is left at the end.

   **Types with several topics.** The planner reads every topic of a type (the
   bulk boundary covers all of them, and a record published to any of them
   after the boundary fails the plan). The chunks of one unit form a chain
   ordered by `(topic index, partition, offset)`, where the topic index is the
   topic's position in `topics:`. Along the chain each row applies
   `SET r += props`, which **merges** properties (a later row overwrites only
   the properties it carries; it never replaces the whole relationship). So
   with `topic_conflict: precedence`, the later-listed topic wins for each
   property it carries, for any planner worker count and any memory budget.
   `arrival` uses the same order, which is not guaranteed. With `order_by`,
   the planned writer uses the same conditional `SET` as the other writers
   (newest value wins, a tie keeps the stored value, also across chunks). The
   plan file records each type's topic list; a plan whose topic list differs
   from the schema's (different topics or a different order) is refused with
   a request to re-plan. A Kafka plan made before multi-topic types (no topic
   record) is refused if any schema type now lists more than one topic, or
   reads a topic the plan did not; only producer-source plans skip the check.
   A plan task's order packs the topic index into 12 bits and the partition
   into 20, so a type may list at most 4096 topics (of at most 2^20
   partitions each); beyond that planning fails with an "order range" error.

   **Types with several label pairs.** Each topic is planned with the label
   pair it binds. Resources are per label, so Post 7 and Comment 7 are
   different units, while hub degrees count per label over every pair and type
   (a Person who likes many posts and many comments can be a hub). A unit, and
   so a chunk and its chain, belongs to one pair; reversed pairs (A->B, B->A)
   share a resource set but form separate chains, each written in its own
   direction. The plan file states the pair: such a type's entry is
   `{"pairs": [{"source", "target", "topics", "rows", "chunks", "units"}, ...],
   "rows", "chunks", "units"}` (no top-level `source`/`target`, so an older
   reader refuses the plan), and each of its chunk entries has an 11th field, the
   pair's index. Single-pair types keep the 10-field format. A plan is refused
   with "plan binds `<topic>` to S->T; the schema binds it to ...; re-plan the
   load" when the schema now binds a topic to another pair, or when a pair was
   added or removed. `--plan-writers` count writers, not pairs: every writer
   writes chunks of every pair.

   **Cost of several topics.** Chunks never span planner tasks, and each
   topic of a type is at least one task. A unit therefore gets at least one
   chunk per topic that touches it, so types with many topics run more,
   smaller transactions than the same rows in one topic. The planner also
   charges each `(task, cell)` row group (about 544 bytes, kept in memory even
   after its rows spill) against the row-buffer budget; with many topics and
   many buckets the effective row buffer shrinks once that overhead exceeds a
   quarter of the row-buffer share (to at least a quarter of its share); a
   single-topic load stays below that and is unaffected. `--plan-memory-mb` bounds the row buffers and key
   windows only; Kafka client queues have floors of 16 MiB or more per
   process, so a 1 MiB budget is a test setting, not a supported one.
6. **Write.** Each chunk is one Neo4j transaction:
   `UNWIND` rows, `MATCH` both endpoints by key, `MERGE` the relationship,
   `SET r += props`, then check that every row matched exactly one source and
   one target. Otherwise the transaction rolls back and the run fails.
7. **Verify.** The run succeeds only if all of these hold:
   * the dispatcher's completion ledger holds every planned chunk exactly once, with its planned row count;
   * the per-type and total rows equal the planned rows;
   * every writer exited cleanly.

   The ledger is written, fsync'd, as `edge-plan.json.ledger.json`.

### Guarantees

* **No lock waits, no deadlocks.** A relationship `MERGE` locks exactly its two
  endpoint nodes. Chunks running at the same time hold disjoint resources, and
  every node belongs to one resource, so concurrent transactions are
  node-disjoint. Node writes are disjoint too: each node key lives in one
  partition, and each partition has one loader. On the IMDb 40k-seed load
  (23.3M records), Neo4j's query log recorded 0 ms of lock waiting across all
  13,918 write transactions.
* **Same graph as the classic modes.** Chunks of a unit form a chain in
  `(partition, offset)` order. The records of one pair are therefore applied
  in topic order, and the last one wins exactly as with the per-type loaders.
  The chain is identical for any number of planners. Planned and classic loads
  of the IMDb 40k-seed subset matched on every count and property checksum.
* **Invalid records are rejected, not lost.** When the planner reads the edge
  topics (no plan file), a malformed record (undecodable, bad JSON, or failing
  the schema's record validation) is appended, fsync'd, to
  `var/rejections/<run id>-planner-<worker>.jsonl`
  with the classic rejection-log entry format (`var/rejections/` is the directory the classic
  relationship loaders use too; `loading.rejection_log_path` only configures the node loaders), a rerun with
  the same `--run-id` deletes that run's earlier planner files first, an unwritable
  directory fails the plan with the path and the record's position, logged at WARNING, counted as
  `rejected` in the plan stats (`stats.rejected`, only present when nonzero) and
  left out of the plan. The plan is exactly the plan of the valid records
  for any number of planners, and the load completes. Records in a
  producer-written plan (`--edge-plan PLAN_FILE`) are validated by the
  producer API, which still fails on an invalid record. A missing or ambiguous
  endpoint at write time rolls the chunk back and fails the run. Nothing else
  is skipped silently.
* **Crash safety.** If a writer disconnects while it holds a chunk, the run
  fails. Its resources are never handed to another writer while its
  transaction might still be committing. A rerun replays safely, because
  `MERGE` is idempotent. For an operator-issued stop (`Ctrl-C`, `docker
  stop`, or `kill`) rather than a crash, see section 10's "Stopping a run
  safely" subsection.

### Large inputs: bounded-memory planning

Planning needs every relationship before the first chunk can be cut (a hub is
only known once all its rows were counted), but it never needs them all in
memory. Each planner process keeps at most `--plan-memory-mb` of data:

| Share of the budget | Holds | Beyond it |
| --- | --- | --- |
| 35% | read records, grouped by `(source bucket, target bucket)` cell | all buffers are appended to one spill file together |
| 15% | endpoint keys of the current *window* with their exact degree in it | the window is sealed: its keys go to a spill file and its degrees into a fixed-size counter |
| 40% | one cell while it is cut into chunks | a larger cell is split by unit on disk and streamed chunk by chunk |
| 10% + 20% | Kafka consumer prefetch, producer queue | – |

While everything fits, nothing is written and planning runs purely in memory;
there is only one planner, so there is no switch to get wrong. Plans do not
depend on the budget: chunks, keys, resources, chain order, ranks and the
chunk messages are identical whether the planner spilled or not (the test
suite proves this against the previous in-memory planner on randomized
inputs, and on a live 1.6M-relationship load).

**Hubs stay exact.** Degrees are first bounded by fixed-size
counters indexed by `crc32(key token)`: a counter slot is the sum of the
degrees of all keys that share it, so it never under-counts. Only keys in a
slot of at least `H` are candidates; their degrees are then summed exactly,
by key, over every window of every planner process. A slot collision costs a
little extra exact work, never a wrong hub, and the hub set is the same for
any worker count or budget.

**Sizing a large run** (for example ~450M relationship records, ~70 GB of
record JSON, ~90k chunks):

* **Memory:** each planner process uses about its budget plus ~150 MB
  (interpreter, Kafka client). The default gives four processes a quarter of
  the available RAM between them.
* **Spill disk:** about the uncompressed size of the relationship records
  plus ~10%, on `--plan-spill-dir` (default: the plan directory). Put it on a
  large local disk, not on a tmpfs. A full disk fails the plan closed with a
  clear error. The files are deleted when planning ends, also on failure.
* **Kafka:** the run's chunk topic holds every record once more (lz4
  compressed on the broker, typically a third to a quarter of the JSON size).
  It is created with unlimited retention (`retention.ms=-1`,
  `retention.bytes=-1`) whatever the broker default, and 1 GiB segments; it
  is deleted after the run. Writers fetch one chunk per request by offset.
* **Plan file and dispatcher:** ~70 bytes per chunk (7 MB for 100k chunks);
  loading and validating it takes under a second, and the dispatcher needs
  ~50 MB for 100k chunks and ~200 resources. A grant costs ~0.1 ms with 16
  writers and ~3 ms with 64 writers holding most resources.
* **Throughput:** planning from Kafka measured 590–660k records/s on four
  processes (4 cores), in memory or spilling; 450M records plan in roughly
  12–15 minutes on four cores. More `--plan-workers` scale with cores.
* **Timeouts:** planning and the dispatch phase can take much longer than
  `--bulk-timeout-seconds`; both are only failed after that long *without
  progress*. The planner rewrites `edge-plan.json.progress` while it reads,
  counts and publishes, and a loader waiting for a producer's plan file
  watches the same heartbeat.

### Automatically derived settings

`src/orchestrator/auto_settings.py` derives every setting, and a flag
overrides each one:

| Setting | Default |
| --- | --- |
| writers `K` | Neo4j available processors + 2 (10 if the probe fails), never more than the number of chunks |
| buckets per label `B` | `clamp(K, 8, 64)`, or `clamp(round(K × (1 + s)), 8, 64)` when self-referencing pairs carry a share `s > 0.10` of the records; in both cases at most `isqrt(R / (T × 500))` and at least 2, for `R` records across `T` types |
| chunk rows `C` | `clamp(ceil(R / (20 × K)), 500, 5000)` |
| hub degree `H` | `C` |
| planners `W` | `min(4, CPUs)` |
| planner memory | `MemAvailable / 4 / W` per process, clamped to 256–4096 MiB |
| node-loader replicas | YAML `replicas`; otherwise 1 below 10,000 topic records, else one per partition up to 8 |

Why these values:

* Neo4j is the bottleneck, so `K` slightly exceeds its cores to cover commit
  and network gaps.
* `B` has to be large enough that about `K` chunks can run at once without
  sharing a bucket, and small enough that units stay big.
* A self-referencing pair (source label = target label, for example
  `FRIEND_OF` Person→Person) puts two buckets of the *same* label in every
  chunk, so with `B = K` only about `K / 2` of its chunks fit at once. When
  such pairs carry a share `s` of the records, the average chunk holds
  `1 + s` buckets of the shared label, so `B = K × (1 + s)` keeps about `K`
  chunks running (`2K` for a load of only self-referencing rows). Only the
  same-label pairs count: for a type with several label pairs, only the
  rows of its `(L, L)` topics do. The share comes from the boundary offsets
  of the relationship topics, so it costs nothing. At or below 10% (IMDb's
  `EPISODE_OF` is 0.3%) `B` is exactly `clamp(K, 8, 64)`, so bipartite loads
  are unchanged. `C` and `H` don't depend on `B`. The volume cap still
  applies, and `--plan-buckets` always wins. The settings line logs the
  reason, for example
  `self_ref_records=1001462 buckets_reason=self_ref_share=0.80`, or
  `volume_cap=10,self_ref_share=0.25` when the cap is lower, or `explicit`.
  Measured gain: see section 9.
* `C` keeps transactions large, since each one costs about 3 ms of fixed
  overhead plus about 15–25 µs per row, while leaving enough chunks for
  load balancing.
* A node with a full chunk's worth of rows gets its own resource, so it
  serializes only its own rows, not its whole bucket.

### Tuning knobs you might touch

* `GRAPH_LOADER_PLAN_NODE_PHASE_WRITERS=N` caps the active chunks while node
  labels are still loading, so large node labels get more Neo4j CPU. The
  default `0` (unlimited) measured as fast or faster on IMDb.
* `--plan-writers` should stay near Neo4j's core count. More writers than
  cores doesn't help once Neo4j is saturated.

## 7. Planning in the producer (optional)

A producer that already holds every relationship (from files or a database
export) can plan while it publishes the nodes. It then skips publishing the
edges to their per-type topics and reading them back:

```python
from src.loader.edge_planner import PlanSettings, plan_and_publish_edges
from src.utils.schema_loader import load_schema

schema = load_schema("my_graph.yaml")
rows = {"LIVES_IN": [{"source": {"person_id": 7}, "target": {"name": "Paris"}, "properties": {}}]}
plan_and_publish_edges(schema, rows, bootstrap_servers="localhost:9092",
                       plan_path="plans/edge-plan.json",
                       settings=PlanSettings(topic="my-graph-edge-chunks"))
```

```bash
python -m src.cli start --mode bulk --config my_graph.yaml --edge-plan plans/edge-plan.json
```

For large inputs, pass picklable `EdgeRecordSource` objects (one per file or
shard) instead of lists; they are read on `settings.workers` processes, each
within `PlanSettings.memory_mb` (spilling to `PlanSettings.spill_dir` beyond
it). Use `producer_edge_planner(..., plan_path=path).start()` and
`.finish(path)` to overlap planning with publishing the nodes; passing
`plan_path` up front puts the spill files beside the plan and starts the
progress heartbeat that a waiting loader watches. Sources can yield either of:

* envelopes, which are validated while planning;
* prepared `(source_key, target_key, record_json)` rows. Only the keys are
  validated while planning, but writers validate every record before writing,
  so invalid records still fail the load.

For a type with several label pairs (section 3) every source must name the
topic whose pair its rows belong to, `EdgeRecordSource.topic`
(`IterableEdgeSource("LIKES", rows, topic="likes-comments")`); a source without
one fails with "source for LIKES must name its topic (each topic binds a label
pair)", and the `{type: rows}` mapping form is refused for such types. Sources
of different pairs may share an `order`; within one pair `order` orders the
sources as usual.

This path was faster for IMDb. On the 40k-seed subset it took 107.8 s
end-to-end, against 122.4 s through the per-type topics. The difference is
almost entirely publishing 19.2M relationship records to Kafka. The graph is
identical either way.

## 8. Outputs, monitoring and verification

* **Exit status:** `0` only after the completion proof described in
  [section 6](#6-planned-mode-in-depth).
* **Plan and ledger:** `edge-plan.json` and `edge-plan.json.ledger.json` in
  `--plan-dir`. These record every chunk, its resources and when it committed.
* **Write-timing logs:** set `GRAPH_LOADER_TIMING_DIR`. Each node batch and
  each relationship chunk is logged as `stage=node-write` or `stage=edge-write`,
  with records, write time, and fetch and lease wait.
* **Metrics snapshots:** every process (CLI, node/edge/planned-writer loaders,
  the lease server, the planned dispatcher, the global batch clock) also
  writes `metrics-<kind>[-<name>][-<replica>].prom` into
  `GRAPH_LOADER_TIMING_DIR` — a Prometheus text exposition format dump (not OpenMetrics; parse it with
  `prometheus_client.parser.text_string_to_metric_families`) of that process's
  own registry. It is rewritten every `GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S`
  seconds (default `15`; `0` disables the periodic refresh) by a daemon timer
  and once more on graceful shutdown, so a streaming run that is killed still
  leaves a recent record. Each write goes to a temp file and is atomically
  renamed into place, so readers never see a truncated file. Use one timing
  directory per run: a snapshot file is overwritten in place by the next
  process that reuses that same `kind`/`name`/`replica` combination, so an
  old run's file left over in a reused directory reads as if it were from the
  current run. Scrape `/metrics` on `GRAPH_LOADER_METRICS_PORT` (or the
  ephemeral fallback port logged at `stage=metrics-ready`) for live values
  instead of relying on the snapshot while a process is still up.
* **Lease-server metrics across a supervised restart (P5-S1):** every
  `graph_loader_lease_*` series (`kind="lease-server"`:
  `graph_loader_lease_grants_total`, `_denials_total`, `_wait_timeouts_total`,
  `_held_resources`, `_waiters`; `kind="edge"`: `graph_loader_lease_wait_seconds`)
  is already populated for streaming, with no new series or label value — the
  streaming default and bulk `--edge-leases` share the exact same
  instrumentation. A supervised restart (section 12) starts a fresh process
  with a fresh registry, so every `lease-server` counter **resets to 0** —
  Prometheus's own `rate()`/`increase()`/`resets()` already handle a counter
  that drops between scrapes correctly; a dashboard that instead subtracts
  two raw scrapes would not. The previous generation's final values are still
  recoverable from that container's own periodic and exit-time metrics snapshot when
  `GRAPH_LOADER_TIMING_DIR` is set, at
  `metrics-lease-server-<run>-r<attempt>.prom` (one file per restart attempt,
  never overwritten by the next one) — except after an ungraceful `docker
  kill`, which never runs the exit-time write: the last periodic snapshot
  (at most `GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S` old, default 15 s)
  survives a kill, but the generation's final values are lost (the last
  Prometheus scrape may be newer). This is the same "at-least-once,
  don't treat a raw counter delta as an exact count" caveat already noted for
  the DLQ counters (P5-Q5, section 10), extended here to say a restart can
  also *decrease* a counter's raw value.
* **DLQ/retry/parking-lot metrics are now live (P5-S4).** Three series
  registered by earlier slices at "registered now, held at zero" (per
  P5-Q4/P6-Q1's convention, since nothing populated them yet) now
  actually move once the retry consumer runs: `graph_loader_dlq_backlog`
  (`kind="edge"`, computed cheaply and periodically for every DLQ-and-
  retry-enabled edge type), `graph_loader_retry_total{reason="missing-endpoint"}`
  (`kind="edge"`, one increment per real retry attempt — "blocked" and
  "duplicate_skipped" outcomes are deliberately excluded, they are not
  attempts), and `graph_loader_parked_total` (`kind="edge"`, one
  increment per row permanently parked after `max_retry_attempts`). The
  `kind` label's closed enum (`PROCESS_KINDS`) also grows from 7 to 8
  values, adding `"retry-consumer"` for that process's own
  `process_up`/`process_start_time`/`process_info` triad and its DLQ-topic
  `graph_loader_consumer_lag_records` series — the retry consumer's own
  Neo4j write metrics (`records_consumed`, `neo4j_write_duration_seconds`,
  `deadlocks_recovered`, `retry_total`, `dlq_routed_total`) stay under
  `kind="edge"` instead, since they describe edge writes regardless of
  which process performed them. P5-S4 also added
  `graph_loader_retry_stalled_seconds{kind="edge",name=<type>}`, the seconds
  that type's retry writes have been failing with nothing written (0 when
  healthy; see section 3's batched-draining notes and `docs/MONITORING.md`).
  Lease-fenced stream node and edge loaders export
  `graph_loader_write_outage_seconds{kind,name}`, the seconds their Neo4j writes
  have been failing with connectivity-class errors while they hold their batch
  and retry it (0 while writing; section 5.4). Bulk loaders never export it.
* **Container logs:** `docker logs graph-loader-node-<Label>-<n>-...`, plus
  the `graph-loader-planned-dispatcher-*` and writer containers. The CLI logs
  each label becoming ready (`stage=node-label-ready`) and the plan statistics.
* **Reports:** the example drivers write a Markdown report per run, with
  per-type publish times, write statistics and plan statistics. See
  [`examples/imdb/README.md`](../examples/imdb/README.md).
* **Checking for lock waits yourself:**
  * Neo4j 5's query log (`logs/query.log`) prints `waiting: N ms` for every
    query. That is the time it spent blocked on locks.
  * `SHOW TRANSACTIONS YIELD status` shows `Blocked by ...` for waiting
    transactions.
  * Planned-mode loads should show 0 for both. A streaming run's default
    bucket-lease path should also show 0 for both, outside a brief
    coordinator-restart transition (section 12).
* **Multi-topic runs** (a relationship type with `topics:`):
  * *Lag series bound.* `graph_loader_consumer_lag_records{kind,name=<type>,topic}` has one series per topic of a
    type, so the total is at most the number of topics in the schema (3 types x 4 topics = 12 series). No metric
    gains a per-record, per-pair, per-partition or per-sub-stage label; `precedence` sub-stages add no series. The
    series is exported by the CLI process only (see `docs/MONITORING.md`). A type is at zero lag only when every
    one of its topics is, so one stuck topic keeps the whole type above zero.
  * *Precedence sub-stage log lines* (classic and lease bulk), one pair per topic in `topics:` order:
    `stage=bulk-substage edge=<type> topic=<t> rank=<i> run_id=<sub run id> start` and
    `stage=bulk-substage-complete edge=<type> topic=<t> rank=<i> elapsed_s=<s> outcome=<...>`. A failed or stopped
    sub-stage logs `stage=bulk-substage edge=<type> topic=<t> failed: ...` and later topics never start.
  * *Timing rows.* With `GRAPH_LOADER_TIMING_DIR` set, the type keeps `stage-<type>.json` (its total, all topics)
    and each sub-stage writes `substage-<type>.t<rank>.json` (with `rank` and `topic`); the IMDb report shows
    them as per-topic rows under the type. The loaders' write logs and metric snapshots of topic `rank` are
    `edge-<type>.t<rank>-<replica>.log` and `metrics-edge-<type>.t<rank>-<replica>.prom`.
  * *Planner rejection count (planned mode).* Malformed relationship records are skipped into
    `var/rejections/<run id>-planner-<worker>.jsonl`. The count is not a Prometheus metric: it is logged as
    `stage=edge-plan-rejections plan_id=<id> rejected=<n> dir=<dir>` when planning ends, as
    `stage=outcome rejected=<n> ...` at the end of the run, and recorded as `rejected` in the plan's per-lane
    statistics (present only when nonzero). Alert on the log line, or on the rejection files, not on a metric.
* **Types with several label pairs:** no metric gains a pair label; such a type exports exactly the series of a
  multi-topic type with the same topics (`docs/MONITORING.md` section 4). The pairs appear in the
  `stage=edge-multi-pair edge=<type> mode=<lease|lanes> pairs=Person>Post,Person>Comment` start line, as
  `pair=<S>><T> ...` in the debug `stage=edge-lease-flush` detail, and in the plan's `types.<type>.pairs`.
* **Verifying a graph:** `examples/social/social_graph.py verify` compares a
  loaded graph with an independently computed expected graph, and optionally
  with another database (`--against-database`).

## 9. Performance guide

### Measured results (this repository's host: Neo4j 8 cores, Kafka 4 cores)

| Load | Records | Mode | End-to-end | Neo4j lock waiting |
| --- | --- | --- | --- | --- |
| IMDb 20k-seed | 11.0M | label-serialized, original | 525 s | about 42% of edge-query time |
| IMDb 20k-seed | 11.0M | shared overlap, 4 consumers per type | about 87 s | about 82% |
| IMDb 20k-seed | 11.0M | bucket leases | about 92 s | 0 |
| IMDb 20k-seed | 11.0M | planned | **about 60 s** | **0** |
| IMDb 40k-seed | 23.3M (4.06M nodes, 17.8M relationships created) | shared overlap | 166.5 s | about 82% |
| IMDb 40k-seed | 23.3M | planned, producer plan | **107.8 s** | **0** |
| IMDb 40k-seed | 23.3M | planned, library plans from Kafka | 122.4 s | 0 |

"End-to-end" includes reading the source files and publishing to Kafka. It
excludes building the subset, cleaning up beforehand and building images.

**Self-referencing loads** (section 6, `B = K × (1 + s)`). Social example,
20k persons, with extra `FRIEND_OF` rows; `start --mode bulk` wall time from
already-published topics. Medians of 3 (3.32M: 4) interleaved A/B runs on a shared, busy
host (load1 2–26), so read the walls as ±10–20%; B was faster in every interleaved pair:

| Load | `B` before → after | Wall | Relationship dispatch | Mean concurrent chunks (of 10) | Lock waits |
| --- | --- | --- | --- | --- | --- |
| 1.24M rows, `FRIEND_OF` 80% | 10 → 18 | 24.1 s → 21.6 s (−10%) | 8.6 s → 6.4 s (−25%) | 6.0 → 9.2 | 0 |
| 3.32M rows, `FRIEND_OF` 93% | 10 → 19 | 43.0 s → 33.6 s (−22%) | 24.2 s → 14.6 s (−40%) | 5.2 → 9.4 | 0 |
| unmodified example, 323k rows, 25% | 10 → 10 (volume cap) | 18.0 s → 18.1 s | unchanged | 8.2 → 8.1 | 0 |

Bipartite loads such as IMDb get exactly the same `B` as before. Details:
`bench/p0/selfref_buckets/RESULTS.md` in the benchmark workspace.

**Streaming's lease-default vs. the deprecated clock/rotation path.** See
[`plan/v5/completion_slice-1.md`](../plan/v5/completion_slice-1.md) for the
scratch-cluster throughput comparison between `--mode stream`'s new
bucket-lease default and the deprecated `--stream-clock-rotation` path on
the same fixture data, plus the zero-lock-wait measurement methodology
below applied to streaming specifically.

### Where the time goes

In planned mode Neo4j runs at about 7.5 of its 8 cores for the whole
relationship phase, so the database's CPU is the limit. Each relationship
costs about 18–25 µs of server CPU, including property writes and index
lookups. So:

* **More Neo4j cores** give close to linear gains.
* **Heap and page cache:** the page cache should hold the graph. With 12 GB
  the hit ratio stayed ≥ 99.7% on the 20k-seed load, and its lowest 5 s sample
  on the 40k-seed load (4M nodes, 18M relationships) was 93%. With an 8 GB
  heap, GC took under 2% of wall time and the heap peaked at 5 GB. Beyond
  that, more memory doesn't help.
* **Keep other work off Neo4j's cores.** Loaders are cheap (about 2–3 cores
  in total), but a noisy neighbour on Neo4j's CPUs slows everything. Use the
  CPU sets in section 2.
* **Node phase:** node writes use 500-row batches. Larger node batches
  (2,000) measured slower.
* **Producer-side planning** saves the extra Kafka round trip of every
  relationship record (section 7).

### Why lock waits used to dominate

A relationship `MERGE` locks both endpoint nodes until commit. In the classic
per-type loaders:

* lanes were split by source only, so lanes of one type shared targets such
  as `Drama` or `US`;
* types that share a label (for example every Person→Title type) ran at the
  same time.

Sorted explicit locks (`apoc.lock.nodes`) prevent deadlocks, but not waiting.
Removing them makes Neo4j's own locking deadlock. The only real fix is to
make concurrent transactions node-disjoint, which is what planned and lease
modes do.

## 10. Operations: reruns, cleanup, failures

* **Reloading the same data:** node loaders resume the consumer group
  `<consumer_group_id>-<Label>`. Before reloading the same topics, delete
  those groups, or change `consumer_group_id`. Relationship planning always
  reads from the low watermark and uses run-scoped groups and topics. A
  stream fleet's relationship loaders and retry consumer resume their stable
  groups too (next item).
* **Consumer groups and restarts (stream mode).** Each relationship type has
  one stable group, `<consumer_group_id>-<type>`, for all its topics, label
  pairs and replicas, and the retry consumer reads every DLQ topic in
  `<consumer_group_id>-retry-consumer`. Neither name contains the run id, so
  stopping a stream fleet (`SIGTERM`) and running the same `start` command
  again resumes every relationship topic and the DLQ from the committed
  offsets, with or without `--run-id`. The restart re-reads nothing that was
  already committed: no transient value regressions from replayed old
  records, no second park of a parked row, no burst of lag, and no new group
  per restart. (A fresh group, or a partition without a commit, starts from
  the earliest offset.) Bulk groups are unchanged: run-scoped and
  phase-derived (section 5, "Consumer groups and run ids"). The deprecated
  `--stream-clock-rotation` path uses the same stable work groups; its clock
  coordination groups stay per run.
  * **Upgrading from run-scoped stream groups.** Earlier releases named the
    stream groups `<consumer_group_id>-<type>-<run id>` and
    `graph-loader-retry-consumer-<run id>`. Before a stream fleet launches,
    the CLI checks each stable group. If the group has no committed offset
    and `--run-id` names the earlier run, the CLI copies the offsets of that
    run's group into the stable one (the group is empty then, so Kafka allows
    it) and logs a WARNING, `stage=consumer-groups
    reason=migrated_legacy_offsets from_group=<old> to_group=<new>`. A stable
    group that already has a commit is never overwritten. For the deprecated
    `--stream-clock-rotation` path the old run id is the derived phase id
    (`<R>.shared.0.<h>`, or `<R>.isolated.<i>.<h>` for a self-reference).
    Without `--run-id` the earlier run id is not known and the CLI does not
    guess; it logs `stage=consumer-groups ... resume=stable-group` at INFO,
    and a stable group without commits starts from the earliest offset. To
    carry an earlier run's position over by hand, stop every loader, then
    for each type (and the retry consumer):

    ```bash
    kafka-consumer-groups --bootstrap-server <broker> --describe \
      --group <consumer_group_id>-<type>-<old run id>        # note CURRENT-OFFSET per partition
    kafka-consumer-groups --bootstrap-server <broker> --group <consumer_group_id>-<type> \
      --topic <topic>:<partition> --reset-offsets --to-offset <offset> --execute  # once per partition
    kafka-consumer-groups --bootstrap-server <broker> --delete \
      --group <consumer_group_id>-<type>-<old run id>        # optional, after checking the new group
    ```

    Or start once with `--run-id <old run id>` and let the CLI copy them.
    The old run-scoped groups are no longer read and can be deleted.
  * **One live fleet per schema and `consumer_group_id`.** Two stream fleets
    with the same prefix would share these groups, split the partitions
    between them, and write under two lease servers that do not know each
    other. Before anything launches, the CLI therefore refuses to start
    while any stable group still has members (`stage=consumer-groups
    reason=... still have active members`). It first waits up to
    `session_timeout_ms` plus 5 s, so the members of a crashed run's
    containers can expire. A member that died during a rebalance can stay
    up to `max_poll_interval_ms` plus `session_timeout_ms`; to wait that
    long, pass `--stream-group-member-wait-seconds S`. A stop during the wait
    is reported as a stop (`stage=shutdown`), and nothing is started. Stop the
    other fleet, or remove a crashed run's containers (this section,
    "Stopping a run safely"). The pre-launch check is best effort: two CLIs
    started at the same moment can both find the groups empty. A running
    fleet therefore also checks its groups every 10 s and stops with an
    ERROR (`stage=consumer-groups reason=foreign_members`, exit 1) when a
    group has more members than its own replicas on two consecutive checks.
    Two deployments that
    must run at the same time on one Kafka cluster need their own
    `consumer_group_id`. (The node loaders' groups were already shared in
    the same way, and two runs without `--run-id` on one Docker host already
    collide on the node container names.) For the same reason a relationship
    type may not share its name with a node label in stream mode: both would
    use `<consumer_group_id>-<name>`, and the start is refused.
  * **Offset retention.** Kafka deletes the committed offsets of a group
    that has had no members for `offsets.retention.minutes` (broker default
    10080, 7 days). A stream fleet that stays stopped longer than that
    restarts from the earliest offset of every relationship topic and DLQ,
    as if new: every write is an idempotent `MERGE`, but older values replay
    first, and DLQ rows are retried again. Raise the broker setting if a
    fleet may stay down longer, or record the offsets
    (`kafka-consumer-groups --describe`) before a long stop and reset them as
    above.
* **Fresh database:** `CREATE OR REPLACE DATABASE <name> WAIT` against the
  `system` database. Constraints and indexes are recreated on the next run.
* **Clearing the input topics:** delete and recreate your topics, as
  `scripts/cleanup_imdb_local_load.sh` does for the IMDb example. The chunk
  topic of a planned run is deleted automatically unless `--keep-plan-topic`
  is set.
* **After a failure:** fix the cause and rerun. Every write is an idempotent
  `MERGE`, so a partial load is completed, not duplicated.
* **Concurrent runs:** don't run two bulk loads against the same topics or
  database at once.
* **Redelivered flushes and the retry consumer (replay resurrection).** A stream
  edge loader that exits after part of a flush committed (a crash, a stop-drain
  abandon, a failed revoke settlement) leaves the flush uncommitted, and the
  next owner re-reads it. If some rows of that flush were dead-lettered and the
  retry consumer has written or parked them since, the redelivered flush finds
  their pair still blocked by a later row and dead-letters them again: the
  parked-pair index entry comes back and a fresh DLQ copy is published. That
  older row would then be applied after the newer row of its pair. The retry
  consumer prevents this: it remembers, per pair and source partition, the
  highest source offset it has written or parked. A copy at or below that
  offset that is indexed again is a resurrection. Its index entry is removed
  (it blocks nothing), its DLQ offset is committed past, and it is never
  written. "At or below" includes the resolved record itself: for a type with
  several topics the index gives a re-added record a new place after every
  outstanding row of its pair, so writing it again could land after a newer
  row. The same rule covers a written or parked copy whose index removal
  failed: next time it is only removed, not written or parked again. The log
  line is `stage=retry-consumer ... reason=stale_resurrected_copy_skipped
  topic=... partition=... offset=... resolved_offset=...`; it is not counted
  in `graph_loader_retry_total`. A replay (`replay-dlq`, with or without
  `--parked`) produces the record again at a new source offset, so it is never
  skipped this way. The memory holds up to 100,000 (pair, source partition)
  entries (least recently used first out); a forgotten entry only means the
  previous behaviour for that pair. For what it does not cover, see section 12,
  "Replay resurrection".

### Migrating a rejection-file entity to a DLQ topic

1. Add `dead_letter: {enabled: true}` (optionally with an explicit `topic:`)
   to the entity's schema block. Declaring the field is itself the opt-in;
   no separate global flag exists.
2. Rerun `start`. The CLI creates the resolved DLQ topic automatically
   (unlimited retention: `retention.ms=-1`, `retention.bytes=-1`) if it
   does not already exist, before any loader container launches. If the
   topic cannot be created or an existing one violates the retention/size
   policy, `start` fails closed before any container starts.
3. **Limitation:** existing on-disk JSONL entries from before the migration
   are metadata-only (topic, partition, offset, reason — no raw payload,
   by the original `RejectionSink` design) and cannot be replayed through
   the `replay-dlq` command (below), which needs an envelope's raw
   `value_bytes`. To recover a pre-migration rejection, re-source the
   original record from its source topic at the recorded offset while
   that offset is still retained there, or accept the old entries as
   informational-only. Every failure recorded *after* migration is fully
   replayable.
4. The switch is reversible: setting `enabled: false` (or removing the
   `dead_letter` block) later reverts that entity to the file sink with no
   other change.

**Durability note:** the DLQ producer uses `acks=all`, which is only as
durable as the DLQ topic's own `min.insync.replicas` — the broker default
(often `1`) means a single broker's write is still enough to acknowledge.
The topic is created with the broker's default replication factor and
`min.insync.replicas`; for a production deployment, set
`min.insync.replicas >= 2` on the DLQ topic (or raise the broker default
before this library's first run creates it) if you need the same
durability guarantee the "never auto-deleted" retention policy implies.

### Replaying a DLQ topic

Malformed records are terminal (they are never auto-retried): fix the
producer or the schema, then re-inject the backlog with `replay-dlq`.

```bash
python -m src.cli replay-dlq --config graph.yaml \
  (--node-label LABEL | --edge-type TYPE) \
  [--reason-code CODE] [--since ISO8601] [--until ISO8601] \
  [--limit N] [--target-topic TOPIC] [--source-topic TOPIC ...] \
  [--dlq-topic TOPIC] [--parked] [--dry-run]
```

| Option | Meaning |
| --- | --- |
| `--node-label` / `--edge-type` | Exactly one, required. Selects the entity whose `dead_letter` config gives the DLQ topic to read |
| `--reason-code CODE` | Only replay envelopes with this exact `reason_code` (e.g. `invalid_json`, `missing_required`) |
| `--since` / `--until` | ISO-8601 timestamps (e.g. `2024-01-31T00:00:00Z`); filter on the envelope's `first_failure_at_ms`. `--until` earlier than `--since` is rejected |
| `--limit N` | Replay at most `N` *matching* records (distinct records, not DLQ copies or messages scanned) — a bounded first attempt on a large backlog |
| `--target-topic TOPIC` | Republish destination; defaults to the entity's own source topic, and for a relationship type with several topics to each envelope's own `original_topic` (see below). Use this to replay into a staging topic first; a target that is not one of the entity's topics logs a `reason=target_outside_entity` WARNING (no loader of the entity reads it) |
| `--source-topic TOPIC` | Repeatable. Only replay envelopes whose `original_topic` is `TOPIC`; each `TOPIC` must be one of the entity's topics (checked against the schema, otherwise the command exits 1) |
| `--dlq-topic TOPIC` | Override the DLQ topic to read. Resolved from the entity's `dead_letter` block by default — *even when `enabled: false`*, so a backlog from before switching back to the file sink stays replayable (the migration is reversible in either direction; see section 10). Required if the entity has no `dead_letter` block at all |
| `--parked` | Replay from the entity's **parking-lot** topic (`parked_topic_name(...)`, section 3; `<type>.dlq.parked` for a type with several topics) instead of its ordinary DLQ topic — for a row already permanently parked after `max_retry_attempts`. Ignored when `--dlq-topic` is also given. `--source-topic` and the per-topic targets work the same way |
| `--dry-run` | Count and log matching envelopes (topic/partition/offset, `reason_code`, a truncated `error_text` — never the raw payload) without publishing anything |

**Several topics per type.** A type with several topics has one DLQ
(`<type>.dlq`) for all of them. Without `--target-topic`, `replay-dlq`
republishes each envelope to its own `original_topic`. Before a live replay
publishes anything the selection pass checks that every
`original_topic` is one of the type's topics and that each target topic
exists; an envelope from any other topic (for example a topic removed from
the type) stops the command with exit code 1 and names the envelope. Leave
such envelopes out with `--source-topic`, or send them somewhere explicit with
`--target-topic`. A dry run reports the same error. `--source-topic a` replays
only topic `a`'s envelopes. A replay into an `arrival` or `precedence` type logs
a warning: a replayed record is applied when it is read again, so it can
overwrite a newer value of its pair from any topic of the type (under
`precedence` also a higher-ranked topic's value); only `order_by` protects
newer values.

**Several label pairs per type.** The default replay is pair-safe: every
envelope returns to its own `original_topic`, whose pair the loader uses (the
replayed message keeps the envelope key, `[S, T, sk, tk]`; loaders never use a
relationship key for identity). `--target-topic` on such a type needs `--source-topic`
values that all bind one label pair, and if the target is a topic of the type it
must bind that same pair; otherwise the command exits 1 before reading anything,
for example "replaying likes-comments records into likes-posts would write them
as Person->Post". A target outside the type (a staging topic) is accepted for one
pair's sources.

```bash
python -m src.cli replay-dlq --config graph.yaml --edge-type WORKS_AT --source-topic works-at-a --dry-run
python -m src.cli replay-dlq --config graph.yaml --edge-type WORKS_AT --parked --source-topic works-at-b
```

The reader is a dedicated, non-group consumer: it never commits an offset
on the DLQ topic, so replaying (dry-run or live) never marks anything on
the DLQ itself — the envelope stays there afterward for audit, and the
same command can be run again safely (see the idempotency note below). It
reads each partition from its low watermark (or the offset resolved from
`--since`) up to the high watermark captured once when the command starts,
so a DLQ topic still receiving new failures during the run does not turn
into an unbounded tail. The range is read twice: a selection pass picks
the matching envelopes and checks them, then a live run re-reads the same
range (nothing published to the DLQ in between is read) and republishes
exactly the selected ones. The retry consumer re-publishes one record many
times, so the selection keeps **one envelope per record**
(`original_topic`, `original_partition`, `original_offset`). Filters see the
record's latest copy (its current `reason_code`); the record is replayed from
its first copy (all copies carry the same key and value), in the order the
records first reached the DLQ, so a pair's root is replayed before the rows
that were parked behind it. `matched` counts these distinct records, and
`--limit` applies to them. Progress is tracked by offset, not by a message
count, so an offset gap (a deleted record, a transactional control marker)
doesn't stop the scan from completing; a scan that goes over two minutes
without reading a single new message (broker unreachable, or a gap that
can never be filled) raises an error instead of hanging forever. Envelopes
are never buffered (the selection keeps a small entry per distinct record),
so a large backlog of up-to-8-MiB envelopes doesn't risk an out-of-memory
failure.

A live replay is fail-closed on the batch: the first envelope whose
republish is not acknowledged by Kafka stops the run immediately. Every
envelope replayed successfully before that point was already logged; the
command exits non-zero and never claims overall success while skipping a
record. An envelope with no payload (`reason_code=empty_payload`) is
skipped with a warning rather than replayed, since there is nothing
meaningful to republish.

**Idempotency, not order-safety:** every downstream write this library
performs is `MERGE`-based, so replaying a record whose underlying issue
was already fixed and independently re-published by the original producer
is safe, and running the same replay command twice is equally safe
(at-least-once, matching the existing counters' at-least-once precedent).
No deduplication bookkeeping is implemented or required. `MERGE` does
**not** make a replay order-safe: if a newer, valid record for the same
key has already been written since the failure, replaying the old DLQ'd
envelope overwrites that node's or relationship's properties back to the
old values — last-write-wins follows the order records are applied in,
and a replay is not necessarily in that order relative to the source
topic's own later traffic. Prefer `--dry-run` first, then a staging
`--target-topic`, or only replay when you know the affected keys have not
been legitimately updated since the failure. A record that is still
malformed after a replay is DLQ'd again as a new envelope with a fresh
`first_failure_at_ms` and `attempt_count=0`, which affects any later
`--since` window.

**Missing/ambiguous-endpoint envelopes (row-level DLQ, section 3) are
retried automatically** by the retry consumer, streaming's default fleet
companion (section 5.3) — manual `replay-dlq` against the ordinary DLQ
topic is no longer the primary path for this failure class, though it
remains available (e.g. to force an out-of-band retry sooner than the
next backoff, or against a bulk run's opt-in DLQ backlog, which has no
fleet-launched retry consumer at all — see the non-goal note below).
Filter with `--reason-code missing_source` (or `missing_target`/
`missing_both`/`ambiguous_source`/`ambiguous_target`/`ambiguous_both`/
`blocked_by_earlier_unresolved_row`) once the missing node has arrived.
This works because the envelope's value is already in the loader's own
wire input shape (section 3) — no translation step. The same-pair
ordering guard now spans every flush automatically (section 3), but a
*manual* replay bypasses the parked-pair index entirely: if you replay
one row of a pair while a different row of the same pair is still
outstanding, nothing stops that replay from writing older properties
over ones the still-outstanding row would eventually have set — check
for other envelopes of the same pair before manually replaying one in
isolation.

**Replaying a parking-lot topic.** A row that hit `max_retry_attempts` is
moved to its parking-lot topic and never auto-retried again — genuinely
replayable, not merely "not dropped" (owner decision 2). Add `--parked`
to the same `replay-dlq` command above; it resolves `parked_topic_name(...)`
for the named entity instead of the ordinary DLQ topic, so there is
nothing to compute or paste by hand:

```bash
python -m src.cli replay-dlq --config graph.yaml --edge-type WORKS_AT --parked --dry-run
python -m src.cli replay-dlq --config graph.yaml --edge-type WORKS_AT --parked
```

A replayed parked row republishes onto the original entity topic (or
`--target-topic`) and re-enters the pipeline exactly like a fresh record
— if its endpoint still doesn't resolve, it is DLQ'd again with a
**fresh** `attempt_count=0` (mirroring section 10's own malformed-record
replay note for the identical reason): a replay is a deliberate,
human-approved new attempt, not a continuation of the attempt count that
led to parking in the first place.

Order warning: a parked row stops blocking later rows of its pair once it is parked, so those rows are normally already written. Replaying the parked row writes its older properties over them (last write wins), unless the edge type declares order_by, in which case the conditional SET protects the newer value. Check for newer rows of the same (source, target) pair before replaying.

**Non-goal: bulk has no automatic retry consumer.** Only streaming's
default lease-fleet launch includes one (section 5.3); a bulk run's own
opt-in row-level DLQ (`route_missing_endpoints_in_bulk: true`) backlog is
never automatically retried. Use `replay-dlq` manually, or invoke
`python -m src.loader.retry_consumer` standalone against that backlog
(section 3's "Standalone invocation" note) — there is no live fleet left
to contend with once a bulk run has finished, so the classic, non-leased
Neo4j session path this standalone mode falls back to is safe in
practice.

### Stopping a run safely: SIGTERM, SIGINT, SIGKILL, and container cleanup

**Safe stop.** `Ctrl-C` (`SIGINT`) or `docker stop`/`kill -TERM` on the
`start` process behaves the same in bulk and stream mode: the first signal
sets a single shutdown flag, and every wait in the run notices it on its next
poll (a fraction of a second: 0.1-0.25s), not after `--bulk-timeout-seconds`.
The run then stops exactly the containers it started, in a safe order (edge loaders and the
retry consumer before the lease server they depend on), and exits nonzero (bulk, or a stream run stopped before it was fully up) or
`0` (a stream run that was supervising when you stopped it). A granted batch
or chunk transaction is never interrupted mid-write, only observed between
writes, so it always reaches a commit or a rollback first.

* **Bound.** Signal observed within about a quarter of a second, then the
  container stops run concurrently: at most 30s for node loaders (Docker's
  own `SIGKILL` fallback after that), 10s for the lease server. A Docker
  call already in flight (an image build, or a container launch) is never
  cancelled half-way, since that could leave a container with nothing
  tracking it; the stop takes effect as soon as that call returns, and
  anything Docker had already started is stopped.
* **Repeated signals.** A second or third `Ctrl-C` (or `SIGTERM`) during
  the shutdown changes nothing: it cannot interrupt a container stop that
  is still running. Use `kill -9` only as a last resort, and then follow
  the `SIGKILL` recipe below.
* **Bounded loader drain.** A stream edge loader or node loader that is
  stopping waits at most 5 seconds in total (all post-stop drain time, in
  `DEFAULT_DRAIN_BUDGET_SECONDS`) for bucket leases. This matters when
  another loader holding a lease was killed: its resources stay fenced for
  `T + margin`, far longer than Docker's stop grace. When the budget is spent
  the loader writes nothing more, commits no offsets for the unflushed batch,
  logs `stage=edge-drain-abandoned` (or `stage=node-drain-abandoned`) at
  `WARNING`, and exits `0`. The records are redelivered on the next run, and
  cells of that flush which were already written are replayed too (the
  writes are idempotent `MERGE`s), so a large final flush may be abandoned
  and replayed.
* **Stopping during a Neo4j outage.** A stream loader that is riding out a
  Neo4j outage (section 5.4) honours the stop within about 0.2 s, or within
  the same 5-second budget while it waits for a lease. It commits nothing for
  the held batch, logs the drain-abandoned line with `reason=neo4j_outage` and
  exits `0`, so the run still exits `0` and leaves no containers behind.
* **Containers ignore repeated `SIGTERM`.** Once a loader, planned writer,
  retry consumer, clock, dispatcher or lease server has handled its first
  `SIGTERM` it ignores further ones, so a container stopped twice exits `0`
  rather than `143`. The flip side: a process that is stuck during teardown
  is no longer ended by a second `SIGTERM`; it waits for Docker's `SIGKILL`.
* **Stopping during the launch.** `docker stop` sends one `SIGTERM`. A
  container started a moment earlier can lose it (its init has not yet set
  up signal forwarding, and the kernel discards the signal), and then runs
  until Docker's `SIGKILL` (exit `137` after the full grace period). The CLI
  therefore holds the stop of a container it launched until that container
  has been up for 1 second (`STOP_SIGNAL_READY_SECONDS` in
  `src/orchestrator/docker_service.py`) and `docker top` lists the init's
  child process (the init blocks signals before it forks, so it then
  forwards the `SIGTERM`), at most 5 seconds after the launch
  (`STOP_SIGNAL_READY_MAX_SECONDS`; if `docker top` fails, the 1-second hold
  alone applies). Older containers are stopped at once, as before. A node loader installs its `SIGTERM` handler before its
  imports: a stop while it is still importing ends it at once with exit `0`
  and `stage=node-startup-stopped` (it has opened, read and committed
  nothing). A stop after that is the usual graceful stop; a stream node
  loader that has not connected yet exits `0` with
  `stage=node-startup-stopped` instead of failing its lease connect. A retry consumer
  stopped while starting also exits `0`
  (`reason=stopped_during_startup`, WARNING).
* **Stream mode with no relationships.** The `start` command launches the
  node loaders and exits `0` at once; nothing supervises them, so nothing
  stops them for you. Stop those with `python -m src.cli stop` or the
  recipe below.
* Node loaders of a stream run *with* relationships are now stopped on
  every exit path (graceful stop, failure, an isolation-policy rejection
  such as `mixed-stream`, or an unexpected error), not only on failure.

* **Logging.** An operator/signal stop is logged at `WARNING`
  (`stopped: operator/signal stop requested`), not `ERROR`. Planned writers
  that are between chunks when the orchestrator aborts for a stop exit `0`
  (a chunk in progress is never abandoned half-applied; any other abort
  reason still makes a writer exit `1`). Removing an auto-remove container
  that Docker is already deleting (`409 removal ... already in progress`)
  counts as success.
* **Plan wait.** A stop during the plan wait terminates the planner
  processes at once and removes their spill and progress files and the run's
  chunk topic (unless `--keep-plan-topic`; a kept topic of an aborted plan is
  partial).
* **Startup.** Every loader, writer, dispatcher, lease-server, retry and
  clock container runs under Docker's init (`tini`), so a `SIGTERM` that
  arrives while Python is still importing is handled instead of waiting for
  Docker's `SIGKILL` fallback. Rebuild nothing: this is a launch option.
* **Stream supervision.** A node loader that dies during a stream run
  with relationships is detected within about a second: the CLI logs
  `stage=node-loader-supervision ... node loader exited`, stops the whole
  fleet like an edge-fleet failure and exits `1`. Stream node containers are
  auto-removed, so their logs are gone; follow `docker logs -f` beforehand
  if you need them.

Run the recovery recipe below as a safety net after any stop you weren't
watching closely; after a normal stop it finds nothing.


**`SIGKILL` (`kill -9`, or a `docker kill` on the CLI host process itself
if it runs inside a container).** Cannot be caught by any process, by
design: no cleanup runs at all, and every container the run had started is
left running. Recovery:

```bash
docker ps -a --filter label=app=graph-loader
docker rm -f $(docker ps -aq --filter label=app=graph-loader)
```

Optionally narrow either command with `--filter label=run_id=<id>` when
the run used an explicit `--run-id` — but note that node/edge-loader
containers launched **without** `--run-id` never carry that label at all
(it is only set when a run explicitly provides one; every other kind
always carries it), so the unnarrowed `app=graph-loader` filter is the one
guaranteed to catch everything a run started, with any command's own name
in `--filter label=component=<kind>` (`node-loader`, `edge-loader`,
`bucket-lease-server`, `retry-consumer`, `planned-dispatcher`, `planned-edge-writer`,
`global-batch-clock`) as a further narrowing option if you need to leave
other concurrent runs alone.

After a `kill -9` of a bulk planned run the run-scoped chunk topic
`graph-loader-edge-chunks-<run id>` is also left. Rerunning with a new
`--run-id` is always safe; to reuse the same `--run-id`, add
`--replace-stale-plan-topic` (refused while any container labelled with the
run id is still alive, so remove those first with the recipe above).

**What "graceful" means, restated in operator terms.** This is the
existing, unchanged contract from section 6's "Crash safety" note: an
offset is committed only after the corresponding Neo4j write is durable,
and a malformed or unresolved record's DLQ publish is acknowledged before
its own offset commits — either way, nothing is ever marked done ahead of
the durable outcome it represents. A rerun after any stop — clean, killed,
or anywhere in between — is always safe, because every write this library
performs is an idempotent `MERGE`. Nothing in this shutdown work changes
that guarantee; it only changes how promptly and how completely containers
are cleaned up around it.

**Accepted, not fixed.** Two behaviors this audit reviewed and left as-is,
both already safe under the guarantee above:

* The planned dispatcher keeps its chunk-grant state in memory only, never
  persisted. A dispatcher restart is not a supported mid-run recovery
  path today — a dispatcher exit already means the run is stopping, not
  recovering, matching this section's own "if a writer disconnects while
  it holds a chunk, the run fails" contract.
* A bucket-lease client parked waiting for a resource (`ANY`) when the
  lease server shuts down gets a best-effort `ERR shutting down` reply on
  its connection before it is closed — not silence, and not left hanging
  until its own wait timeout elapses. Either way, the client's own
  fencing rule already treats any lease-connection loss as "roll back,"
  so this is a clean-reply improvement, not a correctness fix.

## 11. Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| `Failed to get metadata: Local: Broker transport failure` | The host can't reach Kafka: check `KAFKA_BOOTSTRAP_SERVERS` and the advertised listener (`KAFKA_EXTERNAL_HOST`). Right after `docker compose up`, wait until the broker is ready. |
| A load "succeeds" but the graph is empty | It was loaded into another database: check `NEO4J_DATABASE`, which is passed to every container. |
| `start --mode stream` exits 1 with `missing input topics: ...` | A node or relationship topic declared in the schema does not exist on the broker (previously the loaders started and exited with `relationship topic metadata unavailable`). Create or publish the named topics, then rerun. Nothing was launched. |
| A stream loader logs `reason=neo4j_outage` and ingestion stops, but the fleet stays up | Neo4j is unavailable, read-only or refusing connections. The loaders hold their batches uncommitted and retry them in order (section 5.4). Fix Neo4j; ingestion resumes by itself. `reason=neo4j_fatal` followed by exit 1 means authentication or the database name failed for 10 minutes: fix the credentials or `NEO4J_DATABASE`, then restart. |
| Planned mode fails with an invalid-record error at `topic/partition/offset` | Fix the record at that offset (undeclared field, missing required field, wrong type) or the schema, then rerun. |
| `did not resolve exactly one source and target endpoint` | A relationship references a node key that isn't on the node topic, or that matches more than one node. Publish the missing node, or make the key unique. |
| Classic mode: `Kafka partitions revoked with unresolved relationship work` | Fixed: bulk relationship loaders now use static partition assignment. Rebuild the edge image if you see it. |
| Classic mode with `GRAPH_LOADER_EDGE_EXPLICIT_LOCKS=0`: `DeadlockDetected` | Expected. The explicit ordered locks are required in the classic modes; use planned mode instead. |
| Timeout in a bulk stage | The stage made no progress for `--bulk-timeout-seconds`: check the named containers or the planner's error. Raise it only if single steps (one node batch, one chunk transaction) can legitimately take that long. |
| `planner spill directory ... is full` | The planner's spill files did not fit: free space or point `--plan-spill-dir` at a larger disk (about the records' uncompressed size is needed). |
| A loader exits with `... over the 8388608-byte DLQ envelope limit` | A malformed record's DLQ envelope (base64-encoded payload plus metadata) exceeds the cap; that partition stalls fail-closed at that offset until an operator intervenes. Fix the producer to stop sending oversized records, or manually advance the consumer group's committed offset past it after capturing the record from the source topic for manual recovery. |
| `start` fails with `DLQ topic ... already exists but violates the DLQ policy` | A pre-existing DLQ topic (operator-created, or broker auto-created) has `retention.ms`/`retention.bytes` other than `-1`, or a `max.message.bytes` below the envelope cap. Fix the topic's configuration to match the policy, or delete it and rerun so the CLI recreates it correctly. |
| `chunk topic graph-loader-edge-chunks-<id> already exists` | A previous run with the same `--run-id` was `kill -9`ed. Use a new `--run-id`, or rerun with `--replace-stale-plan-topic` once the run's containers are gone. |
| `start --mode stream` is refused for a multi-topic type with `topic_conflict: precedence` | Precedence needs bulk (planned, `--edge-leases` or `--edge-classic`): there is no cross-topic order in a stream. Use `order_by`, or run in bulk |
| Rerun of a precedence type fails with `rerun a precedence type with a fresh --run-id` | A previous run under the same `--run-id` left commits on a higher-ranked topic while a lower one still has work. Use a new `--run-id` |
| Precedence stage fails with `records were appended to an already loaded topic during the run` or `boundary differs from the pre-run snapshot` | A topic of the type received records after the run captured its end offsets; they would not be loaded. Stop publishing to the type's topics for the duration of a bulk run, then rerun with a fresh `--run-id` |
| A multi-topic bulk stage seems stuck on a small topic | A partition with records but no commit blocks the stage; a partition that never held a record does not. Check the group's committed offsets for that topic |
| A record of a multi-pair type is rejected with a key-property error (for example `target.post_id is not declared` on `likes-comments`) | Each topic binds one label pair and its key property names (section 3, "Several label pairs per type"); the pair is never guessed from the record. Publish the record to the topic of its pair, with that pair's key property names |
| `start` refuses `--stream-clock-rotation`: `bind several label pairs` | The deprecated clock and rotation path knows one pair per type. Drop the flag: the default stream lease fleet supports multi-pair types |
| A planned run is refused with `plan binds <topic> to S->T; the schema binds it to ...` | The schema changed a topic's label pair (or added/removed a pair) after the plan was made. Re-plan the load |
| `replay-dlq` refuses with `replaying <topics> records into <topic> would write them as S->T` | `--target-topic` names a topic of another label pair. Replay without `--target-topic` (each record returns to its own `original_topic`), or name a target of the same pair and `--source-topic` values of that pair only |
| Retry consumer logs `reason=invalid_on_retry` for a multi-pair type | The row's `original_topic` is no longer a topic of the type, or its DLQ key names another pair. It parks after `max_retry_attempts`; fix the schema or replay the parked row to the right topic |
| Containers still running after I stopped the load | See section 10's "Stopping a run safely" subsection — run `docker ps -a --filter label=app=graph-loader` and the matching `docker rm -f` recipe there, regardless of how the run was stopped. |

**Duplicate DLQ envelopes:** DLQ routing is at-least-once, matching the
existing counters' precedent (P5-Q5) — a duplicate envelope for the same
`(topic, partition, offset)` is expected, not a bug, in these cases: a
crash between Kafka's DLQ-publish acknowledgement and the source topic's
offset commit (redelivery on restart); a slot-gated (bucket-lease)
streaming revoke, which always fails closed without settling (see section
5), so the malformed record is DLQ'd again on redelivery; and a
publish that is slow but eventually successful after the consumer has
already been evicted from its group for exceeding `max_poll_interval_ms`.

## 12. Limitations

* Planning is bounded in memory but not in disk: the spill directory needs
  about the relationship records' uncompressed size, and the chunk topic
  holds the records once more.
* Each planner worker handles at most 2^31 rows; use more `--plan-workers`
  beyond ~8 billion rows.
* The plan file directory must be on the host running the CLI; it is
  bind-mounted into the dispatcher container.
* Planned mode rejects malformed relationship records to the planner rejection
  files (section 6), never to a DLQ topic. Malformed-record DLQ routing
  (`dead_letter`) applies to the classic and bucket-lease relationship loaders
  in both bulk and stream mode; planned mode ignores it. Planned mode cannot honour any relationship
  `dead_letter` with `enabled: true` (including `route_missing_endpoints_in_bulk`,
  `max_retry_attempts`, `parked_topic` and a custom `topic`), so a plain bulk
  start picks the lease mode for such schemas (section 5).
* The derived settings are heuristics, tuned on IMDb and a synthetic social
  graph. For unusual degree distributions, try `--plan-buckets`,
  `--plan-hub-degree` and `--plan-chunk-rows`.
* Datetime keys, and `-0.0` versus `0.0`, are bucketed by UTC instant and by
  value equality. This is always safe, but it can put two distinct nodes in
  one resource. Hub keys are written to the plan in this canonical form
  (datetimes in UTC, `0.0`).
* The chunk byte cap counts UTF-8 bytes (the earlier in-memory planner
  counted characters of `str` rows, which could only differ for non-ASCII
  rows near the cap).
* Planned mode is bulk-only. Streaming uses the bucket-lease coordinator by
  default (section 5.1); the rotating global clock is available only via
  the deprecated `--stream-clock-rotation` flag, removed after one release.
* **Lease-server restarts and disconnects cause no lock waits.** No leased
  transaction outlives its lease. A lease granted at `G` has the deadline
  `D = G + T` (`loading.bucket_lease.tx_timeout_ms`). Each leased node,
  relationship or retry-consumer write begins its Neo4j transaction fenced
  (`src/loader/fenced_tx.py`). Just before BEGIN it sets the server-enforced
  timeout to `D - now - 4 s`: 3 s for Neo4j's transaction monitor, which
  terminates an expired transaction up to one check interval (2 s by
  default) late, plus 1 s of headroom, and 1 s for the BEGIN itself. Neo4j
  counts the timeout from BEGIN. If BEGIN (connection acquisition included)
  takes longer than 1 s, the transaction is rolled back before any statement
  runs and begun again. When less than 1 s of timeout would be left, the
  cell is put back and written under a fresh grant (after 16 re-grants in
  one flush, the flush fails and its records are redelivered). So no
  statement of a leased transaction starts after `D`. A COMMIT that is
  already in progress when the transaction expires still completes; the
  margin covers it. A new lease-server generation grants nothing for
  `Q = T + margin` after it starts, and a disconnected owner's resources
  stay held for `T + margin` after the disconnect, so no other writer gets
  the same nodes before the old transaction is over. Two leased
  transactions never overlap on a node, before, during or after a restart:
  zero lock waits and no deadlocks. The pre-commit `PING` is only a latency
  optimization. Stream mode and bulk `--edge-leases` need `T` of at least
  6 s and refuse to start otherwise; planned and classic bulk use `T` as a
  flat timeout and accept any value the schema allows. If you raise Neo4j's
  `db.transaction.monitor.check.interval` above 2 s, raise
  `loading.bucket_lease.restart_quiet_margin_ms` (minimum 2 s) by the same
  amount. With a pooled connection the fence costs nothing: one BEGIN, as
  before, with a timeout 4 s shorter.
* **Streaming's lease path blocks the poll thread, bounded, not on a
  separate worker thread.** This slice does not add a full dual-thread
  worker model for the lease-coordinated write path in streaming (only the
  deprecated `--stream-clock-rotation` path uses the worker thread for
  this): a relationship batch's lease acquisition and Neo4j write run
  synchronously on the same thread that calls Kafka `poll()`.
  `loading.bucket_lease.reconnect_timeout_ms` (default 60s, bounded; `0`
  is an explicit unbounded override) and a schema validator keep one
  reconnect attempt safely under `max_poll_interval_ms`, but the bound is
  *per reconnect attempt*, not per outer flush: a lease server that keeps
  crashing and restarting during one flush can still, across enough
  cycles, exceed `max_poll_interval_ms` and get the consumer evicted. A
  single flush-level wall-clock deadline in the lease coordinator would
  close this gap; it is not implemented in this slice.
* `config/graph_schema.yaml`, this repository's own default schema, ships
  both edge types' `dead_letter` blocks present but `enabled: false`, so a
  default `start` still uses the file rejection sink for them — declaring
  the block alone does not opt in, only `enabled: true` does (section 3).
* **Row-level missing-endpoint DLQ's scope boundaries** (section 3): it is
  never active on the classic (non-lease) branch for `execution.mode:
  native_disjoint` (that writer's own `ON ERROR CONTINUE REPORT STATUS`
  reporting is a structurally different mechanism) — `native_disjoint`
  cannot reach the *lease* branch at all, since bucket-lease mode requires
  `python_apoc` and the CLI refuses any other `execution.mode` at `start`
  (the loader's `ExecutionModeError` is the backstop; pre-existing, P5-S1). It is also never active for the
  deprecated `--stream-clock-rotation` streaming path (the only way
  streaming still reaches the classic, non-lease writer branch) — both
  keep today's exact fail-closed behavior regardless of
  `dead_letter`/`route_missing_endpoints_in_bulk`. The same-pair ordering
  guarantee now spans the whole run for a live, streaming, non-`order_by`
  edge type (P5-S4's `ParkedPairIndex`, section 3) — it is still scoped to
  one flush only for bulk's own opt-in row-level DLQ, which has no live
  retry consumer or parked-pair index protecting it at all (Decisions §1).
* `NodeLoader.process_message` (the non-buffered, one-message-at-a-time
  code path) is not used by `main()` — only `_buffer_message`'s batched
  path is — and does not route a malformed record to a DLQ or the file
  sink; it logs and returns failure. This is pre-existing and unchanged by
  Phase 5 Slice 2.
* **Streaming: any relationship-loader container's non-zero exit is fatal
  to the whole fleet.** Confirmed pre-existing (predates this slice; the
  deprecated `--stream-clock-rotation` path has the identical behavior):
  only the bucket-lease server itself gets supervised restart-and-continue
  (section 5.1); an edge loader exiting for any reason — including a
  correct Story 4 abort-not-commit after a lease-server kill, or a
  transient endpoint race described next — currently tears down every
  other edge type and the lease server too, rather than restarting just
  that one edge type. Because no relationship write commits its Kafka
  offset until Neo4j itself has confirmed it, simply re-running the same
  `start` command resumes cleanly with no lost or duplicated writes, with or
  without `--run-id`: the relationship and retry-consumer groups are stable
  (section 10, "Consumer groups and restarts"), so the restart continues
  from the committed offsets instead of re-reading the topics. This was
  proven by hand and by `tests/test_streaming_lease_e2e.py`'s kill/restart
  assertions.
* **Streaming: a node/edge topic race can trigger the fleet exit above --
  unless row-level DLQ (section 3) is active for that edge type.** If a
  relationship record is preflighted before its endpoint node's own
  `MERGE` has committed (node and edge consumers always start
  concurrently), the writer raises `MissingRelationshipEndpointError`,
  which by design commits nothing so the record replays safely on
  restart — but still causes the exit above. Also confirmed pre-existing
  and shared by the deprecated clock-rotation path; found during Phase 5
  Slice 1 Story 10's real scratch validation, not caused by it. **Phase 5
  Slice 3 changes this for any edge type with `dead_letter.enabled: true`
  in streaming (unconditional) or bulk (with the opt-in, section 3):** the
  race no longer raises at all — the unresolved row routes to the DLQ and
  the rest of the batch, and the fleet, keep running. This limitation now
  only applies to an edge type that has not opted in, and to the
  deprecated `--stream-clock-rotation` path, which never reads
  `dead_letter` for this purpose (section 3).
* **Streaming without relationships: node loaders are left running by
  design.** A stream `start` that defines no relationships launches the node
  loaders and exits `0` at once; nothing supervises them, so nothing stops
  them. Stop them with `python -m src.cli stop` or the recovery recipe in
  "Stopping a run safely". A stream run *with* relationships stops and
  removes the node loaders it started on every exit path, including a clean
  `SIGTERM`/`SIGINT` (P6-S5). A `SIGKILL` of the CLI itself still leaves
  every container running (same recipe).
* **Streaming: the lease server's readiness probe assumes a Linux Docker
  bridge network's container IPs are routable from the CLI's own host
  process.** On Docker Desktop (macOS/Windows), a user-defined bridge
  network's container IPs are typically not routable from the host at
  all, so this probe would time out and fail every default streaming
  lease run there. This repository's own validation is Linux Docker;
  not fixed in this slice on other platforms.
* **`order_by`'s tie and type-mismatch behavior is a silent no-op, not an
  error or a DLQ condition.** A tie, a strictly-older incoming value, or
  two values Cypher's `>` cannot compare (e.g. the declared property
  arrives as a string on one row and a number on another) all leave the
  stored value untouched — the relationship is still matched/merged, only
  the property update is skipped, counted separately as
  `skipped_as_not_newer_rows` (section 3, section 8's metrics catalogue
  note). There is no warning per skipped row, only a batched one the
  first time a matched row is missing the `order_by` property outright.
* **The parked-pair index's one real cross-replica race is best-effort,
  not self-correcting** (section 3 spells out what is and isn't
  guaranteed in full): the retry path's own read barrier and a lagging
  tombstone's safe-direction over-blocking are both guaranteed; what is
  *not* guaranteed is two live writer replicas racing on the same pair's
  records — one can miss another's just-acknowledged `add()` and write a
  later row without ever seeing the block, and a stale retried duplicate
  of the earlier row can then permanently overwrite it. This is a
  documented, accepted risk, not a bug being tracked — narrowing it
  further would mean synchronous cross-replica coordination on every
  write, defeating the zero-lock-wait design this slice builds on.
* **Several topics per type: the cross-replica race of the parked-pair index
  is the common case, not the unusual one.** The topics of a type are usually
  read by different replicas, so two records of one pair that arrive on two
  topics within the index's propagation delay can be ordered either way
  (`arrival` treats them as concurrent; use `order_by` when that matters).
  A replica that cannot catch up with the index after a partition assignment
  holds those partitions (no timeout); watch
  `graph_loader_parked_pair_index_wait_seconds`. In standalone retry mode (no index) a
  multi-topic type's records of one pair have no defined order (see the next
  item).
* **Several label pairs per type (v7-S7).** The pair always comes from the
  topic: one topic cannot carry several pairs, and a record's pair is never
  inferred. There are no per-pair metric labels. The deprecated Phase 4 paths
  (`--stream-clock-rotation`, slot gating, rotation, the shared clock) refuse
  such types. In the lease modes `execution.worker_count` applies per pair, so a
  replica opens up to `pairs x worker_count` Neo4j sessions and lease
  connections (and the retry consumer up to `pairs x retry_consumer.write_workers`);
  classic writes the pairs one after another (at most `worker_count` sessions). D1's
  "same-edge-type WRITTEN outcome" evidence is per label pair: a written
  Person->Post row is no evidence against a Person->Comment row that fails
  alone. A pair whose rows fail with none written is reported as a stall of
  the type (the gauge stays per type; the ERROR names `pair=S>T`) even while
  the type's other pairs write. Precedence sub-stages run every topic of the type in its own sub-stage,
  including topics of pairs fed by one topic (correct, only serial). Changing a
  topic's pair, or making the type single-pair again, needs a drained DLQ
  (section 3, "Dead-letter queues across topics").
* **Standalone `retry_consumer` mode (no `--lease-address`) gives no
  same-pair ordering guarantee at all** (section 3, section 10's non-goal
  note): it builds no `ParkedPairIndex`, by design — a finished bulk
  run's opt-in DLQ backlog never had one populated for it in the first
  place (Decisions §1), so building one there only made every candidate
  look like an unindexed duplicate and retried nothing. A DLQ'd row that
  is a bulk-routed duplicate of, or races with, another row of the same
  pair in this mode can resolve in either order; `replay-dlq` (which
  re-enters the live pipeline and its ordering protection) is the safer
  choice whenever that matters.
* **A bulk-routed DLQ'd row that lands in the *same* DLQ topic a live
  streaming fleet's own retry consumer is already reading gets committed
  past, not retried, either.** `route_missing_endpoints_in_bulk` shares
  the identical per-edge-type DLQ topic streaming uses — if that edge
  type also has a fleet-launched retry consumer running with a real,
  lease-mode `ParkedPairIndex` (the ordinary streaming case), a
  bulk-origin row still has no index entry (no live writer ever called
  `add()` for it), so the same `contains()` check (M12) treats it as an
  unindexed duplicate and skips it without a write attempt. `replay-dlq`
  is the correct manual path for this row too.

* **DLQ sizing (unlimited retention).** A relationship type's DLQ holds at most
  `records x (max_retry_attempts + 1)` envelopes from its own failures: the live
  writer's first copy plus one re-DLQ per failed attempt of a row that is the
  earliest outstanding one of its pair (a *root*; rows parked behind it are held
  by the retry consumer and do not cycle). Add the blocked copies the retry
  consumer still publishes: none while a DLQ partition's missing-endpoint
  backlog fits in `max_records_per_partition` plus
  `max_overflow_held_per_partition` (11 x the cap by default) and the blocker is
  fetched; beyond that, one per extra follower each time it is fetched again, at
  delays that grow with how long it has been blocked (at least the type's max
  delay, up to 1 h). **Sizing guidance:** what matters is the per-partition
  backlog of rows waiting on missing endpoints (roughly the edge-before-node
  backlog divided by the DLQ's partitions) against those two bounds, not the cap
  alone. Keep `max_records_per_partition` near the expected backlog per
  partition, or rely on the overflow budget (memory: about one envelope per
  held row), and watch the dashboard's "DLQ envelopes per consumed record"
  panel: a ratio that keeps climbing past `max_retry_attempts + 1` means blocked
  copies are being re-published (raise the bounds, or add DLQ partitions).
  Measured with 200 rows on one partition, endpoints missing for 600 s, 5 s max
  delay: 0 blocked copies at a cap of 1000, 72 at 100 and 9 at 20 (10,395 and
  2,728 before overflow holds). Measured
  examples: the S4 Docker `orderby` run, 4,002 records, 6,495 envelopes (1.6x); the
  `arrival` run, 3,000 records waiting about 3.5 minutes for their nodes at a 5 s
  max delay with 200 attempts allowed, 37,143 (12.4x, all root cycles). With the
  default 60 s max delay the same wait is a handful of cycles. Size the DLQ
  topic's disk for the worst case you accept (missing endpoints x attempts), and
  watch `graph_loader_dlq_backlog` and the routing rate (docs/MONITORING.md).
  The bound also holds across retry-consumer restarts and rebalances: a
  restart re-reads the DLQ from the committed offset, which a long-waiting
  row holds low, so every older copy of a cycling root is fetched again. The
  retry consumer keeps one copy per record: in a batch only the copy with the
  newest lineage and highest `attempt_count` (then the latest DLQ position) is
  retried, and a copy at or below the attempt of a copy of the same lineage
  (same `first_failure_at_ms`) it already re-DLQ'd is committed past
  (`reason=superseded_copy_skipped`; a fresh DLQ of the record by a loader is a
  new lineage and is always retried); the same holds for at-least-once duplicate
  copies. (Before this, each such copy started its own re-DLQ cycle: in a
  model with crashes, one row whose endpoint never arrived had 1,916 copies and
  had not parked after 1,600 s.) A capped batch (`max_batch_size`) takes the
  records due the longest first, so held rows woken again and again can never
  keep a root waiting behind them.
* **Replay resurrection: what the retry consumer's memory does not cover.**
  Section 10 ("Redelivered flushes") describes how the retry consumer skips a
  resurrected copy of a row it already wrote or parked. The memory is kept in
  the retry consumer process, so these cases are not covered:
  * **A retry-consumer restart between the two resolutions.** If the retry
    consumer restarts (crash, stop, redeploy) after resolving the row and
    before the resurrected copy arrives, the memory is gone and the copy is
    retried. The older value can again land after the newer one. A rebalance
    that revokes the DLQ partition is treated the same way: the memory of that
    partition is dropped, and the new owner starts without it.
  * **Order across topics.** Offsets are compared only within one source
    partition of one topic. For a type with several topics, a resurrected row
    of topic A is recognised when this process resolved that row itself, or a
    later row of the same topic and partition. A newer row of the same pair on
    topic B is not enough, because offsets of two topics say nothing about
    their order. This matters only together with a restart: once the memory is
    lost, a newer row of B written after the restart does not stop the older
    row of A.
  * **Eviction.** Above 100,000 remembered (pair, source partition) entries,
    the least recently used is forgotten, and that pair falls back to the
    previous behaviour.
  * **Do not recreate a source topic while a run is in progress.** Offsets of
    a recreated topic start again from 0. A retry consumer that is still
    running remembers the old high offsets, so it would skip new records of
    that topic at lower offsets as resurrected copies. That would lose them
    silently. Recreating a source topic mid-run is not supported. Stop the
    fleet first, including the retry consumer.

  **Mitigation:** declare `order_by` on the edge type (section 3). The
  conditional `SET` then never applies an older value over a newer one,
  whatever the retry consumer does. **Follow-up for the owner:** a durable
  fix needs the resolution to survive the retry-consumer process. One option
  is a "resolved" marker per record identity, kept in a compacted topic or in
  the parked-pair index and read by the live writer, so that a redelivered
  flush does not re-add a resolved record at all ("option A"). It is not
  built.

## 13. Continuous integration

Every push and pull request runs `.github/workflows/ci.yml`: a `lint` job
gates two independent jobs, `unit-tests` and `build-images` (a fail-fast:false
matrix building the node, edge and clock images — checks named
`build-images (node)`, `build-images (edge)`, `build-images (clock)` — and
verifying each one's `ENTRYPOINT`). A lint failure skips both dependent
jobs rather than failing them, so CI minutes aren't spent running a suite
whose lint gate has already failed. `make lint` needs
`pip install -r requirements-dev.txt` (not just `requirements.txt`).

Separately, `.github/workflows/ci-integration.yml` runs the
`testcontainers`-backed integration suite nightly (03:00 UTC) and on manual
`workflow_dispatch` only — it is advisory and never blocks a pull request.
It needs nothing beyond what a stock GitHub-hosted runner already provides:
no scratch Kafka/Neo4j stack, no secrets, no `services:` block (the suite's
own fixtures start and tear down their own containers). Locally, those
fixtures skip cleanly if Docker is unreachable; in the nightly job that
same skip is treated as a failure (a `docker info` step runs first, and a
guard step fails the job if any test's result contains "Docker is
required"), so a green nightly run means the containers actually ran, not
that they were silently skipped. Results are uploaded as the
`integration-results.xml` artifact (14-day retention) regardless of
pass/fail.

**Multi-topic relationship types in CI.** The nightly `integration` job includes
`tests/integration/test_multi_topic_relationship.py` (about one minute with the default 20,000 distinct pairs;
`GRAPH_LOADER_MULTI_TOPIC_PAIRS` scales it). Two topics feed `LIKES` with 30% overlapping pairs and a hub node;
stream lease, bulk lease, bulk classic (one replica, correctness only) and planned are checked for the final
`order_by` value per pair, the relationship count, no rejected record and no DLQ topic; stream lease, bulk lease and
planned also assert zero lock-wait time from the Neo4j query log and no blocked `SHOW TRANSACTIONS` sample.
`precedence` (bulk lease and classic through the real `cli._run_precedence_substages` with in-process loader
fleets as the containers, planned) and `arrival` (counts, published-value check and zero lock waits only) have their
own cases. A `dead_letter` case (stream lease, `arrival`, the S4 proof's dataset: topic b dead-lettered before
topic a, then the nodes) checks the one `LIKES.dlq` for both topics (as wide as the widest topic) drained by the
real retry consumer (a `python -m src.loader.retry_consumer` subprocess), the exact graph (each pair ends with its
last-indexed record), an empty parking lot, no per-topic DLQ and zero lock waits. The zero-wait assertion counts only completed `LIKES` write entries of the right database in the Neo4j
query log (never the `SHOW TRANSACTIONS` poller) and requires at least the number of write transactions the run
must have issued. `tests/test_single_topic_compat.py` compares the three shipped single-topic schemas plus a
`dead_letter`/`order_by` fixture (`tests/golden/fixtures/single_topic_dlq.yaml`) with a golden recorded on the tree
before the multi-topic work: DLQ, parked and parked-pair-index topic names (non-null for the fixture), the node and
edge container launch commands and group-id environment, the lag series the real monitor code creates, the plan
JSON (without its timing `stats`) and byte-identical planned chunk messages. It does not pin the node loaders'
inline default group id (`<prefix>-<Label>`) or the plan's `stats`.

The container-level proofs (`plan/v7/slice-2/docker_proof/run.sh stream|lease|classic` and
`plan/v7/slice-5/docker_proof/run.sh lease|classic`, with `PROOF_TOPICS=a,b` or `c,b,a`) need the compose
Neo4j/Kafka stack and locally built images, which the testcontainers suite does not use. They run in their own workflow,
`.github/workflows/ci-multi-topic-proofs.yml`, which is **manual only** (`workflow_dispatch`, so dispatching it
does not also run the integration and 1M stress jobs) until it has been seen green on a hosted runner; it needs
internet (image pulls, Neo4j plugin download) and a runner with about 16 GB of RAM, pins Neo4j to
`5.26-enterprise`, and uploads `/tmp/v7s2-proof` and `/tmp/v7s5-proof` as an artifact. `actionlint` is clean.
Locally, against your own stack (export every connection variable first, see
section 5):

```bash
IMAGE_TAG=v7s2 make build-images && IMAGE_TAG=v7s5 make build-images     # the harnesses use these tags
for mode in stream lease classic; do bash plan/v7/slice-2/docker_proof/run.sh "$mode"; done
for topics in a,b c,b,a; do PROOF_TOPICS=$topics bash plan/v7/slice-5/docker_proof/run.sh lease; done
PROOF_TOPICS=a,b bash plan/v7/slice-5/docker_proof/run.sh classic
```

The per-type DLQ proofs (`plan/v7/slice-4/docker_proof/run.sh orderby|arrival|bulk`) bring their own private stack
(`stack.sh up|down`: `p0rg-*` containers on ports 29892/28287; `V7S4_STATE`, `V7S4_*_CPUSET` and `V7S4_NEO4J_IMAGE`
override the state directory, cpusets and Neo4j image) and run as a second job of the same workflow, which uploads
the run outputs:

```bash
IMAGE_TAG=v7s4 make build-images && bash plan/v7/slice-4/docker_proof/stack.sh up
for kind in orderby arrival bulk; do bash plan/v7/slice-4/docker_proof/run.sh "$kind"; done
bash plan/v7/slice-4/docker_proof/stack.sh down
```

Each script refuses to run unless `NEO4J_DATABASE` is its own proof database, scopes its cleanup to its own topics,
groups and database, and prints `PASS`/`FAIL` lines (exit code 1 on any `FAIL`).

Every job has a matching local command, so a red check tells you exactly
what to run before pushing again:

| CI job | Local command |
| --- | --- |
| `lint` | `make lint` |
| `unit-tests` | `make test-unit` |
| `build-images` | `make build-images` (tags `graph-loader-{node,edge,clock}:latest`, the CLI default, so `--skip-image-build` uses them; override with `IMAGE_TAG=x` plus the `GRAPH_LOADER_*_IMAGE` variables) |
| `integration` (advisory) | `python3 -m pytest tests -m integration` (needs Docker) |
| `ci-multi-topic-proofs.yml` (manual) | the `run.sh` loop above (needs the compose stack and `IMAGE_TAG` images) |
| workflow YAML validity | `make validate-workflows` (also needs `requirements-dev.txt`) |

`tests/test_social_planned_integration.py` keeps skipping in the advisory
job exactly as it does locally without a named `GRAPH_LOADER_IT_*` scratch
stack — see `examples/social/README.md`'s "Run it" section and its "Comparing modes" section; the exact
variables are read in `tests/test_social_planned_integration.py`.

**Opt-in end-to-end tests need a scratch database.** `test_rotation_e2e`, `test_streaming_lease_e2e`,
`test_relationship_bulk_e2e`, `test_self_reference_isolation_e2e`, `test_social_planned_integration` and
`test_schema_initializer_int` target a developer-run Neo4j, whose server default database is `neo4j`. Tests must
never touch `neo4j` or `latesttesttemp`, so each one is skipped (with a message naming the variable) unless
`GRAPH_LOADER_IT_DATABASE` names a scratch database (recommended: `glit`). The name is refused (the test fails) if it
is `neo4j`, `latesttesttemp` or `system`. The test creates the database if it is missing, runs every query in it
and exports it as `NEO4J_DATABASE` so the CLI and its containers write there too; it does not drop it
(`DROP DATABASE glit` when done). `test_social_planned_integration` treats the value as a prefix and creates
(replacing) `<name>planned`, `<name>classic` and `<name>invalid`.

They also need explicit Neo4j credentials and never fall back to the compose default password `changeme`
(failed logins against a shared Neo4j trip `AuthenticationRateLimit` and lock out its other clients):
`NEO4J_PASSWORD`, plus `NEO4J_URI` (default `bolt://localhost:7687`) and `NEO4J_USERNAME` or `NEO4J_USER`
(default `neo4j`); `test_social_planned_integration` reads `GRAPH_LOADER_IT_NEO4J_PASSWORD` and
`GRAPH_LOADER_IT_NEO4J_URI` instead. Without a password a test is skipped before it connects, unless its opt-in
variable is set (`RUN_ROTATING_RELATIONSHIP_E2E`, `RUN_STREAMING_LEASE_E2E`, `RUN_RELATIONSHIP_BULK_E2E`,
`RUN_SELF_REFERENCE_ISOLATION_E2E`, or `GRAPH_LOADER_IT_KAFKA` for the social test): then it fails with a
message naming the missing variable. `tests/test_dev_environment.py`'s Neo4j smoke checks skip the same way when
`NEO4J_PASSWORD` is neither exported nor in `.env`. Example, against the local stack:

```bash
. /mnt/graph-loader/bench/local_env.sh glit            # or export NEO4J_URI/KAFKA_BOOTSTRAP_SERVERS/... yourself
GRAPH_LOADER_IT_DATABASE=glit RUN_RELATIONSHIP_BULK_E2E=1 python3 -m pytest tests/test_relationship_bulk_e2e.py
```

The testcontainers-backed tests are the opposite: the autouse fixture in `tests/conftest.py` drops ambient
`NEO4J_*`, `KAFKA_*`, `LOADER_NEO4J_*` and `LOADER_KAFKA_*` variables for every test that requests a fixture whose
name contains `neo4j` or `kafka` (`neo4j_driver`, `kafka_bootstrap`, ...), so a sourced bench environment cannot leak
into them.

For live metrics, Prometheus/Grafana discovery and example alert rules, see [`docs/MONITORING.md`](MONITORING.md).
