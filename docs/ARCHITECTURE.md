# Architecture

How the pieces of graph-loader fit together, and why concurrent relationship
writes do not wait on each other's Neo4j locks. This page explains the design;
[USAGE](USAGE.md) is the detailed reference for flags, schema fields and
operations, and [CLI](CLI.md) lists every command.

Related: [README](../README.md) | [CLI](CLI.md) | [BULK_LOAD](BULK_LOAD.md) |
[INCREMENTAL_LOAD](INCREMENTAL_LOAD.md) | [MONITORING](MONITORING.md) |
[CHANGELOG](../CHANGELOG.md) | [HISTORY](HISTORY.md) (how the design got here)

## The problem

Loading nodes in parallel is easy: each node `MERGE` touches one node. A
relationship `MERGE` locks **both** endpoint nodes. Concurrent transactions
that lock overlapping nodes in different orders deadlock, and ones that merely
overlap wait. In a star-shaped model (hub nodes with a huge degree) that is the
normal case. graph-loader loads Kafka topics into Neo4j from a YAML schema, and
every mode other than `--edge-classic` is built so that concurrent
transactions are **node-disjoint by construction**.

## Components

```text
                      +-----------------------------+
   YAML schema -----> |  CLI orchestrator (host)    |  python -m src.cli start|stop|status|replay-dlq
                      |  src/cli.py                 |  validates, applies DDL, launches Docker
                      +--+------+-------+-------+---+  containers, monitors Kafka + containers
                         |      |       |       |
         launches        |      |       |       +--> metrics endpoint + snapshot (host)
                         v      v       v
      +-------------+ +---------------+ +----------------------+ +---------------------+
      | node loaders| | planner       | | lease server         | | retry consumer      |
      | (per label, | | (processes in | | (bulk --edge-leases, | | (stream, when a type|
      |  N replicas)| |  the CLI host)| |  every stream run)   | |  enables dead_letter)|
      +------+------+ +-------+-------+ +----------+-----------+ +----------+----------+
             |                |                    ^ leases                  |
             |                v                    |                         |
             |        +---------------+    +-------+--------+                |
             |        | dispatcher    |    | edge loaders   |<---------------+
             |        | + N planned   |    | (per type,     |   DLQ / parking lot
             |        |   writers     |    |  classic/lease/|
             |        +-------+-------+    |  stream)       |
             |                |            +-------+--------+
             v                v                    v
          +------------------------------------------------+       +---------+
          |                    Neo4j                       |       |  Kafka  |
          +------------------------------------------------+       +---------+
   Every arrow into Neo4j is a Bolt transaction; every loader reads from and
   commits offsets to Kafka. Topics: node topics, relationship topics, the
   control topic, DLQ / parking-lot / parked-pair-index topics, the run's chunk
   topic (planned mode).
```

| Component | Where | Role |
| --- | --- | --- |
| **CLI orchestrator** | host process, `src/cli.py` | Parses and validates flags and schema, picks the bulk relationship mode, creates the control, DLQ and retry topics, applies the schema DDL (constraints and indexes) to Neo4j, launches and supervises Docker containers (`src/orchestrator/docker_service.py`), runs the bulk monitors, enforces stop/shutdown, serves its own metrics. |
| **Node loaders** | containers, `src/loader/node_loader.py` | One fleet per node label (`replicas`, or derived from topic size in bulk). `UNWIND ... MERGE` batches, offsets committed only after the Neo4j batch is durable, with a contiguous-resolution barrier per partition. Group `<consumer_group_id>-<label>`. In stream mode their writes are lease-fenced against relationship writers. |
| **Edge loaders** | containers, `src/loader/edge_loader.py`, `edge_execution.py` | One fleet per relationship type, for classic, lease and stream modes. Validate records, resolve endpoint pairs, write by APOC-locked batches (classic) or by leased bucket cells (lease/stream). Not used by planned mode. |
| **Planner** | processes started by the CLI, `src/loader/edge_planner.py`, `edge_plan.py` | Planned mode: reads every relationship topic up to the bulk boundary while the nodes load, validates records, assigns every node to one resource, groups rows into units and chunks, publishes chunks to the run's chunk topic and writes `edge-plan.json` (plus `edge-plan.json.progress` during the run and `.failed` on failure). Malformed records go to `var/rejections/<run id>-planner-<worker>.jsonl`. Memory is bounded: beyond its budget it spills to disk and produces the identical plan. |
| **Dispatcher** | container, `src/orchestrator/planned_dispatcher.py` | Planned mode: a small TCP service that grants chunks to writers under the node-disjointness rules and keeps the completion ledger in memory; the CLI verifies it and persists it as `edge-plan.json.ledger.json`. Fails closed. |
| **Planned writers** | containers, `src/loader/planned_writer.py` | Planned mode: ask the dispatcher for a chunk, read that message by `(partition, offset)`, validate it, run **one** `MERGE` transaction, report `DONE` after the commit. |
| **Lease server** | container, `src/orchestrator/lease_server.py` | Bulk `--edge-leases` (including the lease mode chosen automatically because of `dead_letter`) and every stream run, except under the deprecated `--stream-clock-rotation`: a TCP coordinator granting exclusive leases on resources (`Label:bucket`, or one resource per node for small "keyed" labels). Writers hold leases for exactly the resources their transaction touches. Restart-safe through time-based fencing. |
| **Retry consumer** | container, `src/loader/retry_consumer.py` | Stream only (not under `--stream-clock-rotation`), when a relationship type enables `dead_letter`: reads the DLQ, retries records whose endpoints were missing (edge-before-node) or whose write failed (`write_failed`), writes them under leases, and parks them after `max_retry_attempts`. Malformed records are terminal: they stay in the DLQ (replay them with `replay-dlq`). |
| **DLQ** | Kafka topic per entity | `<topic>.dlq` (single-topic type or node label) or `<type>.dlq` (multi-topic type), or `dead_letter.topic`. Holds an envelope (reason, original topic/partition/offset, attempt count) per undeliverable record. |
| **Parking lot** | Kafka topic `<dlq>.parked` (a default; `dead_letter.parked_topic` overrides it) | Records that exhausted their retries. Replayable with `replay-dlq --parked`. |
| **Parked-pair index** | compacted Kafka topic `<dlq>.parked-pairs`, in-memory view in each writer | Tracks which `(source, target)` pairs have a record in the DLQ, so a newer record of the same pair is not written ahead of an older one that is still waiting (and later overwritten by it). Stream only, and not used for `order_by` types. Internal; never read by operators. |
| **Control topic** | Kafka `__graph_loader_control` | Loaders publish assignment, ready and drain acknowledgements keyed by run id; the bulk monitors consume them to prove every replica has its partitions and has drained before the CLI stops it. |
| **Metrics** | every process | A Prometheus endpoint per process (default port 9464) and periodic atomic text snapshots under `$GRAPH_LOADER_TIMING_DIR`. See [MONITORING](MONITORING.md). |
| **Global batch clock** | container, `src/orchestrator/clock_runner.py` | Legacy rotating-slot scheduler. Only reachable through the deprecated `--stream-clock-rotation`; not used by any default path. |

## Data flow by mode

Node loading is the same in every bulk mode: node loaders consume their label's
topic; the CLI's bulk monitor proves assignment coverage, captures an end-offset
boundary, waits until committed offsets reach it, and when a label has fully
drained, stops that label's containers (`stage=node-label-ready`). Relationship
work that needs the label may then start.

### Bulk, planned (the default)

```text
 relationship topics --(planner procs, up to the boundary)--> chunk topic + edge-plan.json
 node topics ---------(node loaders)---> Neo4j                      |
                                                                     v
          dispatcher <--NEXT/DONE-- planned writers <--fetch chunk--+
              |  grants a chunk only when both endpoint labels are loaded,
              |  its unit's previous chunk committed and none of its resources is held
              v
        ledger: every planned chunk exactly once with its planned row count
```

The run succeeds only if the ledger is complete and every writer exited
cleanly. The plan is a pure function of the data and the settings, not of the
planner count. Selected when no mode flag is given (and no relationship type enables
`dead_letter` or uses `native_disjoint`), or explicitly with `--edge-plan`.

### Bulk, lease (`--edge-leases`, or the default when a type enables `dead_letter`)

```text
 node topics --> node loaders --> Neo4j
 relationship topics --> edge loaders (per type, static partition assignment)
        each flush split into bucket cells; for each cell:
        lease(resources of its endpoints) --> fenced BEGIN --> MERGE --> commit --> release
                                   ^
                                   +-- one run-scoped lease server for all types and containers
```

Types run concurrently under the shared coordinator, including self-references.
`dead_letter.route_missing_endpoints_in_bulk` can route missing endpoints to a
DLQ row by row; otherwise a missing endpoint fails the batch closed. A type
with several topics under `topic_conflict: precedence` runs its topics as
sequential sub-stages.

### Bulk, classic (`--edge-classic`)

Per-type APOC-locked loaders. The Cypher aggregates the batch's endpoint nodes,
sorts them by `elementId` and locks them with `apoc.lock.nodes()` before
merging. (Mix-and-batch forward/backward slots exist only on the deprecated
`--stream-clock-rotation` path.) Sorted
locking removes deadlocks **inside** a batch, but two loaders touching the same
node still queue on its lock. `--bulk-edge-overlap labels` (default) therefore
serializes types sharing an endpoint label; `shared` runs them concurrently and
accepts lock waits; self-references normally run in a final isolated phase.
Needed for `native_disjoint` edges.

### Stream (`--mode stream`)

```text
 node topics ------> node loaders (lease-fenced) ----------> Neo4j
 relationship topics -> edge loaders (per type, stable groups) -> leases -> Neo4j
                                   |  missing endpoint / invalid row
                                   v
                          DLQ topic --> retry consumer --> leases --> Neo4j
                                            |  after max_retry_attempts
                                            v
                                     parking-lot topic
```

All relationship types and the retry consumer run together under one
long-lived lease server per run; there is no shared/isolated split. The CLI
stays in the foreground supervising and stops the fleet on SIGINT/SIGTERM.
Live loaders ride out a Neo4j outage (they pause, keep polling Kafka, and retry
the same batch) instead of exiting; bulk runs do not.

## How zero lock waits is achieved

| Mode | Mechanism | Can a transaction wait on a Neo4j lock? |
| --- | --- | --- |
| **Planned** | Every node belongs to exactly one *resource*: its label's hash bucket, or its own resource if it is a hub (total degree at least the hub threshold). A chunk is one transaction over rows of one `(type, source resource, target resource)` unit. The dispatcher never has two running chunks that share a resource, and a relationship `MERGE` locks only its two endpoints, so running chunks are node-disjoint. | No. No deadlocks, no retries, no APOC lock step. |
| **Lease / stream** | Same resource idea (`Label:bucket`, or per node for small labels), but dynamic: a writer asks the lease server for all resources of a cell and receives them atomically or waits. A lease is bound to a deadline `D = grant + tx_timeout_ms`. The writer begins its Neo4j transaction with a timeout that ends before `D` (the **fenced BEGIN**: the timeout subtracts a transaction-monitor slack and a BEGIN allowance, which is why `tx_timeout_ms` must be at least 6000), re-queues the cell instead of beginning when too little budget is left, and never commits on a broken lease connection. A restarted server grants nothing for `T` plus a margin, and a disconnected owner's resources stay held for the same time. So even a writer that outlived its lease cannot overlap a newer holder. | No, under the fencing assumptions (Neo4j transaction timeouts and the documented clock behaviour). |
| **Classic** | `apoc.lock.nodes()` on `elementId`-sorted nodes, plus label-level serialization of types. | Yes, by design. It avoids deadlocks but not waits. |

Planned writes ignore `dead_letter` entirely and fail closed on a missing
endpoint; the lease modes can DLQ rows.

In planned mode hubs are detected by degree and serialize only their own rows,
not a whole hash bucket. In the lease modes "keyed" is decided per label
(`--bucket-lease-keyed-max-nodes`): every node of a small label has its own
resource, but a hub in a larger label still serializes its bucket.

## Ordering and idempotency

Relationship records for one `(source, target)` pair merge into one
relationship with `SET r += props`, applied in topic order within a partition
(publish keyed by the source key). Planned chunks of a unit form a chain in
`(partition, offset)` order. Multi-topic types add a conflict rule per type or
label pair (`order_by`, `precedence`, `arrival`). `MERGE` makes any replay of a
batch safe, which is what lets a failed bulk run be retried from the start and
a stream loader retry a batch after an outage.

## Consumer groups and offsets

| Component | Group | Lifetime |
| --- | --- | --- |
| Node loaders (bulk and stream) | `<consumer_group_id>-<label>` | Stable. A later run resumes it; change `consumer_group_id` to reload the same topics. |
| Edge loaders, stream | `<consumer_group_id>-<type>` | Stable, independent of `--run-id`. A restart resumes from committed offsets. |
| Retry consumer, stream | `<consumer_group_id>-retry-consumer` | Stable. |
| Edge loaders, bulk lease/classic | `<consumer_group_id>-<type>-<run id>` (derived stage id) | Run-scoped, so each stage proves its drain from an empty group. |
| Planned mode | none for relationships | The planner reads from the low watermark under a throwaway consumer; progress lives in the ledger, not in offsets. |

Before a stream fleet starts, the CLI refuses while a `<prefix>-<type>` or
`<prefix>-retry-consumer` group still has members (one live fleet per
`consumer_group_id`; a second fleet would split partitions between two lease
servers), and copies offsets from the earlier run-scoped groups once, only into
a stable group without commits, when `--run-id` names the earlier run. Node
groups are only checked for a name clash. These checks are skipped for schemas
without relationship types and under `--stream-clock-rotation`. A running fleet
also checks for foreign members and stops with exit 1 if it finds them.
Offsets are always committed after the Neo4j write is durable ("next offsets"),
and on partition revocation loaders commit buffered work first or fail closed.

## Failure and shutdown model

* **Fail closed.** A bulk run claims success only after proving it: node
  loaders drained to a zero-lag, boundary-checked end; the planned ledger
  complete; each lease/classic stage's groups at zero lag. Anything else exits
  1 and states no success. Records that arrive after the boundary fail the run.
* **Invalid records.** Without a DLQ they go to an fsync'd JSONL rejection file
  (`var/rejections/`) before their offsets may advance; with `dead_letter` they
  go to the DLQ with an envelope and stay there (terminal; replay with
  `replay-dlq`). Planner rejections are counted and the load
  continues.
* **Missing endpoints.** Stream routes them to the DLQ; the retry consumer
  retries them (and `write_failed` rows) with backoff and parks them after
  `max_retry_attempts`. Bulk planned
  and (by default) bulk classic/lease fail closed.
* **Container failure.** Bulk keeps the failed loaders for diagnosis, logs their
  tails, then removes the run's containers. In stream, the death of a node
  loader, edge loader or retry consumer stops the fleet with exit 1 (only the
  lease server is restarted); a Neo4j outage is ridden out by leased writers.
* **Operator stop.** One SIGINT/SIGTERM handler sets a CLI-wide shutdown event.
  Monitors abort promptly; the CLI stops exactly the containers it created
  (containers run with `init` so a signal during startup is not lost); node
  loaders drain buffered work only; planned writers stop between chunks. A
  stopped bulk run exits 1; a stopped stream run exits 0. A SIGKILL of the CLI
  cannot be handled and leaves its containers running; see
  [USAGE: Stopping a run safely](USAGE.md#stopping-a-run-safely-sigterm-sigint-sigkill-and-container-cleanup).
* **Lease server restart.** In stream the lease server is supervised and restartable; in bulk lease mode (including the `dead_letter` default) its death fails the run. Clients reconnect with
  bounded backoff; a lease whose connection broke is never committed under.

## Where to go next

* Run a load: [BULK_LOAD](BULK_LOAD.md), [INCREMENTAL_LOAD](INCREMENTAL_LOAD.md).
* Every flag and exit code: [CLI](CLI.md).
* Schema fields, planned-mode internals, performance, troubleshooting: [USAGE](USAGE.md).
* Metrics, dashboards, alerts: [MONITORING](MONITORING.md).
* What changed and how to upgrade: [CHANGELOG](../CHANGELOG.md).
