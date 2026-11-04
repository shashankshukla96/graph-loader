# Neo4j Graph Loader

Load Kafka topics into a Neo4j graph from a YAML description of the graph. You
declare node labels, relationship types, property types, keys and one topic per
label and per type, publish JSON records to those topics, and the loader writes
the graph. This package provides the `graph-loader` command, a schema loader and
a producer-side relationship planner you can call from Python.

**Status: alpha (`0.1.0a1`).** The public Python API (`neo4j_graph_loader.schema`
and `neo4j_graph_loader.planning`) and the command line may change between alpha
releases; deprecated names and flags are kept for at least one later alpha except
for urgent fixes. Read [Support and limits](#support-and-limits) before relying on
it.

* **Bulk mode** loads a finite, quiescent set of records and proves the result
  (zero consumer lag at a fixed boundary, a complete write ledger). Relationships
  are loaded in node-disjoint chunks so concurrent transactions do not wait on
  each other's Neo4j locks.
* **Stream mode** keeps loading as records arrive, with leased node-disjoint
  writes, a dead-letter queue and retry consumer for relationships that arrive
  before their nodes, stable consumer groups and graceful stop and restart.

## Install

You need CPython 3.12 and `pip`. Use a virtual environment:

```bash
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install "neo4j-graph-loader==0.1.0a1"
graph-loader --version
```

The version is pinned because pip skips pre-releases unless asked. The package
has eight mandatory runtime dependencies (`neo4j`, `neo4j-rust-ext`,
`confluent-kafka`, `pydantic`, `PyYAML`, `docker`, `numpy`, `prometheus-client`)
and two **optional extras, only for keeping run status in a database**:
`neo4j-graph-loader[sql]` (SQLAlchemy 2) and `neo4j-graph-loader[postgres]`
(SQLAlchemy 2 and psycopg 3). Nothing else needs them. **`pip` does not install or
start Neo4j, Kafka or Docker.** You provide them:

| To do this | You need |
| --- | --- |
| Validate a schema, call `neo4j_graph_loader.schema` | nothing beyond the install |
| Plan relationships with `neo4j_graph_loader.planning` | a Kafka broker you can create topics on |
| Run `graph-loader start` (a load) | Neo4j 5.26 Enterprise, Kafka, Docker Engine on Linux, and the loader images `ghcr.io/neburyx/graph-loader-{cli,node,edge,clock}:<package version>`, which `start` pulls when they are missing |
| Read the status of a run (`graph-loader status --run`), or log it as JSON lines (`--log-format json`) | nothing beyond the install; the JSON status files are written next to the run, no services needed |
| Keep run status and history in PostgreSQL (`--status-store sql`) | `pip install "neo4j-graph-loader[postgres]==0.1.0a1"` and a database ([status stores](https://github.com/Neburyx/graph-loader/blob/main/docs/STATUS_SQL.md)) |
| Write Prometheus rules, scrape configuration and Grafana dashboards for your own monitoring (`graph-loader monitoring export`) | nothing beyond the install and your own Prometheus and Grafana ([monitoring guide](https://github.com/Neburyx/graph-loader/blob/main/docs/MONITORING.md#9-bring-your-own-prometheus-and-grafana)) |
| Run `graph-loader start --runtime process` (a load without Docker, as host processes or a systemd service) | Neo4j 5.26 Enterprise, Kafka and Linux (kernel 5.3 or newer); see the [process runtime guide](https://github.com/Neburyx/graph-loader/blob/main/docs/PROCESS_RUNTIME.md) |

## Quick start

Print a sample schema and validate it; no services are involved:

```bash
graph-loader schema-example --name social > social.yaml
graph-loader validate-schema --config social.yaml
```

```text
Schema valid: 6 node types, 6 relationship types
```

The [quick start guide](https://github.com/Neburyx/graph-loader/blob/main/docs/QUICKSTART.md)
continues with your own schema and with planning relationships into Kafka from
Python, with exact commands, expected output and cleanup. Loading a graph is
described in the
[bulk load guide](https://github.com/Neburyx/graph-loader/blob/main/docs/BULK_LOAD.md)
and the
[incremental load guide](https://github.com/Neburyx/graph-loader/blob/main/docs/INCREMENTAL_LOAD.md).

## Documentation

| Document | Read it to |
| --- | --- |
| [Quick start](https://github.com/Neburyx/graph-loader/blob/main/docs/QUICKSTART.md) | Install the package, validate a schema, plan relationships into Kafka. |
| [Bulk load guide](https://github.com/Neburyx/graph-loader/blob/main/docs/BULK_LOAD.md) | Run an initial load: prerequisites, producer contract, flags, edge modes, verification, recovery. |
| [Incremental load guide](https://github.com/Neburyx/graph-loader/blob/main/docs/INCREMENTAL_LOAD.md) | Repeated batches or continuous streaming, ordering, dead-letter queue, restart. |
| [CLI reference](https://github.com/Neburyx/graph-loader/blob/main/docs/CLI.md) | Every command, flag, default, exit code and environment variable. |
| [Monitoring](https://github.com/Neburyx/graph-loader/blob/main/docs/MONITORING.md) | Metrics, alert rules and dashboards, the quick-start stack, and `monitoring export` for your own Prometheus and Grafana. |
| [Run status stores](https://github.com/Neburyx/graph-loader/blob/main/docs/STATUS_SQL.md) | Where a run's status and events are kept (JSON files or a database), the tables, the outbox, retention. |
| [Process runtime](https://github.com/Neburyx/graph-loader/blob/main/docs/PROCESS_RUNTIME.md) | Run the loader as host processes or a systemd service instead of containers: install, quick start, stopping and recovery, sizing. |
| [Single container](https://github.com/Neburyx/graph-loader/blob/main/docs/SINGLE_CONTAINER.md) | Run the whole loader in one CLI-image container with `--runtime process`: image, `docker run`, stopping, memory, limits. |
| [Public library API](https://github.com/Neburyx/graph-loader/blob/main/docs/LIBRARY_API.md) | The supported Python modules, Kafka requirements, ownership and cancellation. |
| [Usage reference](https://github.com/Neburyx/graph-loader/blob/main/docs/USAGE.md) | Schema fields, input contract, planned mode, dead-lettering, operations, troubleshooting. |
| [Support, security and reporting problems](https://github.com/Neburyx/graph-loader/blob/main/docs/SUPPORT.md) | Get help, report a vulnerability, write a minimal reproduction. |
| [Migration from the source checkout](https://github.com/Neburyx/graph-loader/blob/main/docs/MIGRATION.md) | Names, defaults, images and stored formats when moving from `python -m src.cli`. |
| [Changelog](https://github.com/Neburyx/graph-loader/blob/main/CHANGELOG.md) | What changed and how to upgrade. |

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
  tested. The loaders run as Docker containers; `graph-loader start` needs a
  Docker Engine on Linux.
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

## Getting help and reporting problems

* **Questions and bugs:** open an issue at
  <https://github.com/Neburyx/graph-loader/issues> with a minimal
  reproduction (template in the
  [support guide](https://github.com/Neburyx/graph-loader/blob/main/docs/SUPPORT.md#minimal-reproduction-template)).
* **Security problems:** do not open a public issue. Email
  **shashankshukla96@gmail.com** with the subject `graph-loader security`; see
  [SECURITY.md](https://github.com/Neburyx/graph-loader/blob/main/SECURITY.md).
* **Maintainer:** Shashank Shukla.

## License and attribution

Apache License 2.0; the full text is in
[LICENSE](https://github.com/Neburyx/graph-loader/blob/main/LICENSE).
Neo4j and Kafka are trademarks of their owners and this project is not affiliated
with or endorsed by them; Neo4j Enterprise Edition is licensed separately by Neo4j
and you are responsible for your own licence. The repository's IMDb example uses
IMDb's non-commercial datasets, which are not distributed with this package and
are subject to
[IMDb's terms](https://developer.imdb.com/non-commercial-datasets/).
