# How to load incrementally

Related docs: [CLI reference](CLI.md), [Architecture](ARCHITECTURE.md), [Changelog](../CHANGELOG.md), [USAGE.md](USAGE.md), [BULK_LOAD.md](BULK_LOAD.md), [MONITORING.md](MONITORING.md).

There are two ways to keep a graph up to date from Kafka with this library, and this guide covers both and helps you choose:

* **A. Incremental bulk batches.** Repeated, bounded `start --mode bulk` runs, each adding new data to a graph that already exists.
* **B. Continuous streaming.** One long-lived `start --mode stream` fleet that consumes new records as they arrive.

It assumes you have read [`docs/BULK_LOAD.md`](BULK_LOAD.md) (flags, edge modes, schema, producer contract) and uses [`docs/USAGE.md`](USAGE.md) as the full reference and [`docs/MONITORING.md`](MONITORING.md) for dashboards and alerts. Everything below was checked against the code on branch `feat/streaming-observability` (re-checked at commit `3d435be`). Statements that are inferred from the code rather than documented or exercised are marked **(derived)**, and unresolved points are collected in [Notes / open questions](#8-notes--open-questions). The two worked examples were assembled from commands that exist in the repository; they were not executed while this guide was written.

1. [Choosing between A and B](#1-choosing-between-a-and-b)
2. [Part A: incremental bulk batches](#2-part-a-incremental-bulk-batches)
3. [Part B: continuous streaming](#3-part-b-continuous-streaming)
4. [A versus B at a glance](#4-a-versus-b-at-a-glance)
5. [Migration notes: single-topic to multi-topic](#5-migration-notes-single-topic-to-multi-topic)
6. [Troubleshooting for stream](#6-troubleshooting-for-stream)
7. [Worked examples](#7-worked-examples)
8. [Notes / open questions](#8-notes--open-questions)

---

## 1. Choosing between A and B

Both approaches write with the same idempotent `MERGE`s, so they converge on the same graph for the same data. They differ in latency, failure handling and how much you operate.

| Question | Choose **A** (bounded bulk batches) | Choose **B** (stream) |
| --- | --- | --- |
| How soon must new data be visible? | Minutes to hours is fine; you load on a schedule or when a file/export lands | Seconds after the record is published |
| How does data arrive? | In discrete, finite sets you control (daily export, CDC dump, backfill) that you can hold still while the run reads them | Continuously, from producers you do not coordinate with |
| Can relationships arrive before their nodes? | No. Nodes load first and a missing endpoint fails the run (planned) or the batch (lease/classic) unless you opt in to a DLQ | Yes. Missing endpoints go to a row-level DLQ and are retried until the node arrives |
| Do you need `topic_conflict: precedence` across topics? | Yes, bulk supports it | No, stream refuses it; use `order_by` or `arrival` |
| What happens on a Neo4j outage? | The run fails fast with exit code 1; rerun | Leased stream writers hold their batch and retry; the fleet stays up |
| How much do you want to operate? | A job that starts, finishes and exits. The exit code is the health signal | A long-lived fleet to supervise, alert on and restart |
| Peak throughput? | Highest (planned mode, node-disjoint chunk plan) | Lower per record (leased cells); no at-scale stream benchmark is documented |

A common shape is a hybrid: do the initial load with A, then switch to B (see [Handing over from a bulk load to a stream](#handing-over-from-a-bulk-load-to-a-stream)).

## 2. Part A: incremental bulk batches

### A1. What stays the same, and what an update means

Each batch is an ordinary bulk run ([BULK_LOAD.md](BULK_LOAD.md)). Nothing special switches on "incremental"; the graph you load into just is not empty. Four properties make that work:

* **Nodes are `MERGE`d on the schema key and patched.** The write is `MERGE (n:Label {key: ...}) SET n += record.properties`. A later record for the same key updates the properties it carries and keeps the others. An optional property that is `null` or missing in the record is omitted from the write, so it never clears a stored value.
* **Relationships are `MERGE`d on `(source, target, type)` and patched.** The write is `MERGE (s)-[r:TYPE]->(t) SET r += props`. A relationship type has one relationship per ordered `(source, target)` pair; a repeated pair updates the existing relationship rather than creating a parallel one. A batch can reference nodes loaded by earlier batches: endpoints are matched by key in the graph, not by what is in this run's topics.
* **There is no delete.** Records only create and update. Nothing in a topic removes a node, a relationship or a property. Remove data with your own Cypher.
* **Declared properties only.** A record with an undeclared field is invalid. If you stop declaring a property, old values stay in the graph and new records that carry it are rejected.

**Last write wins, and `order_by`.** Without `order_by`, "last" means the order the loader applies the records in: within one topic that is partition order and offset order, which is why relationship records must be keyed by the source key ([BULK_LOAD.md section 4](BULK_LOAD.md#4-publishing-the-data-the-producer-contract)). Across batches, the batch applied later wins for every property it carries. If a later batch can carry data that is *older* than what is stored (a late correction, a replay of an old export), declare `order_by: <property>` on the relationship type: the conditional `SET` then applies a record only when its `order_by` value is strictly newer than the stored one (a tie keeps the stored value). A record that carries no value for the `order_by` property is applied unconditionally, and a stored relationship without a value is always overwritten. `order_by` must name one of the type's own declared properties; use `date`, `datetime`, `integer` or `float`. Nodes have no `order_by`: node records are applied in arrival order.

```yaml
edges:
  - type: WORKS_AT
    topic: works-at-events
    nodes: {source: Person, target: Company}
    source_key_property: person_id
    target_key_property: company_id
    order_by: updated_at          # newest updated_at always wins, in every batch and every mode
    properties:
      role: {type: string}
      updated_at: {type: datetime, required: true}
```

### A2. What a run reads

This is the part that decides how you publish a batch. Each component of a bulk run reads differently:

| Component | What it reads | Does it resume? |
| --- | --- | --- |
| **Node loaders** | Their label's topic through the consumer group `<loading.consumer_group_id>-<Label>`. They start at that group's committed offset, or at the earliest offset if the group has none, and stop at the boundary captured at the start of the run. | **Yes.** Records already committed by an earlier run are not read again. This group is not run-scoped. Kafka drops the committed offsets of a group that has been idle longer than `offsets.retention.minutes` (7 days by default); after that the group starts from the earliest offset again. |
| **Planner** (planned mode, the default) | Every retained record of every relationship topic from its **low watermark to its high watermark**, using explicit offset ranges and run-scoped groups. It reads nothing from committed offsets. | **No.** A rerun re-plans and re-reads everything still on the topic. |
| **Lease and classic edge loaders** | Their topics through a group derived from the run id: `<consumer_group_id>-<type>-<R>.bulk.0.<h>` for shared and lease stages (`<h>` = first 24 hex characters of `sha256("R|bulk|0|<type>")`), `.isolated.<i>.<h>` for an isolated classic self-reference stage, and `<prefix>-<type>-<R>.bulk.0.<h>.topic.<i>.<h>` for each `precedence` sub-stage. With static partition assignment they start at that group's committed offset, or the earliest offset if it has none. | Only with the **same `--run-id`**. A new run id is a new group and starts from the beginning of the topic. The derivation is deterministic, so the same id gives the same group. **(derived)** from `cli.py` and `edge_loader._assign_static`. `tests/test_edge_rebalance.py` covers the static assignment's resume from committed offsets; no document describes reusing a run id this way. |

The boundary is the high watermark captured when each part of the run starts; anything published afterwards fails the run (`relationship topic <t> changed after the bulk boundary was captured ...`, `bulk input changed topic=...`). So **hold your producers still while a batch runs**, or publish the next batch to somewhere the run is not reading.

Every declared topic must exist, even if empty. A label with nothing new in this batch is not a problem: its group is already at the boundary and the label becomes ready immediately. A planned chunk is released only once both of its endpoint labels are ready, so a batch with no new nodes for a label still works.

### A3. Publishing only the new batch, or re-reading the topics

Three patterns follow from the table above.

**Pattern 1: keep appending to the same topics.** Publish the new batch to the existing topics and run again with the default (planned) mode.
* Nodes: only the new records are read (the group resumes).
* Relationships: **everything still retained on the relationship topics is planned and written again**, old and new. That is correct and idempotent, but each run costs as much as a full load of what the topics still hold, and grows with history. Use it for small graphs, or when topic retention (`retention.ms`) keeps the topics short.
* It re-applies the full retained history in order on every run, so a `precedence` type behaves exactly as on a clean full load of that history.

**Pattern 2: one batch per set of topics.** Publish *only* the new batch, into emptied topics, so a run reads only that batch. This is the flow documented for IMDb in [`examples/imdb/README.md`](../examples/imdb/README.md) and implemented by `scripts/cleanup_imdb_local_load.sh`: **clear the input topics and the node consumer groups before each load, keep the Neo4j graph, point the loader at the next batch.** Two details matter:
* Delete the node groups (`<consumer_group_id>-<Label>`) whenever you delete or recreate a node topic. A committed offset that survives on the recreated topic would be used as the loaders' start position (records before it are not read), or can fail the run's zero-lag proof (`nonzero lag ... committed offset above the high watermark of an empty partition: topic recreated under a reused run id?`). Deleting the groups removes the question. (Alternatively give each batch its own topic names in a per-batch copy of the schema; new topics have no committed offsets.)
* Wait until Kafka has really finished deleting a topic before recreating it. The cleanup script polls for up to 60 seconds.
* The cost is that history is no longer replayed, so `precedence` and `arrival` only order records **within** a batch. Across batches whatever is applied later wins per property. Use `order_by` when that matters.

**Pattern 3: lease or classic with a reused `--run-id`. (derived)** With `--edge-leases` or `--edge-classic`, repeating the previous run's `--run-id` makes the relationship loaders resume at the group's committed offsets, so only appended records are read, without deleting anything. Caveats: it is not documented or covered by a test as an incremental strategy; a `precedence` type refuses a reused id when a higher-ranked topic already committed while a lower one still has work (`rerun a precedence type with a fresh --run-id`), so precedence types need a fresh id; and you give up planned mode's speed. Two more caveats: the bulk monitors read the control topic `__graph_loader_control` from the earliest offset under groups that never commit, so a reused id replays the previous run's `ASSIGNMENT` and `DRAIN_COMPLETE` acknowledgements; stale acknowledgements can mask a crashed replica, although the offset proofs still guard completeness. And committed offsets expire after Kafka's `offsets.retention.minutes` (7 days by default), after which the group starts from the beginning. Treat it as an option to test on your data before relying on it.

**Re-read everything on purpose** when you changed what a record means: you added a property, changed a conversion, fixed a producer bug, or need a clean replay. Use a fresh `--run-id`, delete the node groups, and keep the topics.

| You have | Pattern |
| --- | --- |
| A small or short-retention graph, or `precedence`/`arrival` rules you want honoured against history | 1: append and let planned mode re-read |
| A large graph that grows by a self-contained batch each time | 2: reset the topics and node groups, publish only the new batch |
| Lease or classic mode already, topics that must keep their history, no `precedence` | 3, after testing it |

### A4. Run ids, plan topics, precedence sub-stages, reruns

* **A fresh run id per batch is the default and the right habit.** The CLI generates a random UUID hex; you can pass a readable one that stays valid (`1-128` of letters, digits, `.`, `_`, `-`), for example `--run-id inc-20261001T0200Z`. The id names the containers, `var/edge-plans/<run id>`, the planned chunk topic `graph-loader-edge-chunks-<run id>`, the planner rejection files `var/rejections/<run id>-planner-<worker>.jsonl`, and (through a derived stage id) the lease/classic relationship groups and edge container names.
* **Planned chunk topics** are created per run and deleted after it. They are left behind only by a `kill -9`. A random id never collides with them, but each leftover keeps its data forever (unlimited retention), so delete leftovers. To reuse an id after a kill, add `--replace-stale-plan-topic` ([BULK_LOAD.md section 14](BULK_LOAD.md#14-re-running-and-recovering)).
* **Precedence sub-stages** (lease/classic): one full stage per topic in `topics:` order, each with its own group `<prefix>-<type>-<R>.bulk.0.<h>.topic.<i>.<h>`, boundary and zero-lag proof. One run leaves N groups per precedence type; delete the groups of finished runs you do not need (`kafka-consumer-groups --delete --group ...`). Reruns need a fresh id. Planned mode creates no sub-stage groups.
* **A rerun after a failed batch** is safe: every write is an idempotent `MERGE`. With Pattern 2 you must still hold the batch still; with Pattern 1 the failed batch is simply read again.
* **Never run two batches concurrently** against the same topics or database.

### A5. Performance on a non-empty graph

No incremental timings are documented in the repository: `plan/performance-review-20260927.md` records that larger and incremental loads "remain to be checked", and `plan/v5-v6-functional-test-plan.md` test REG-04 specifies the check (load `titles-40000-depth-4`, reset the topics and node groups, load `titles-40000-offset-40000-depth-4` into the same database, then repeat the second batch; expect union counts equal to the sum minus the overlap, no duplicates, an unchanged count after the repeat, and zero lock waits in both planned runs) but holds no results. Measurements of full loads on an empty graph are in [BULK_LOAD.md section 15](BULK_LOAD.md#15-performance-numbers). What follows is reasoning from the design, not measurement:

* The zero-lock-wait property comes from the plan or lease schedule of the run's own transactions, so it does not depend on the graph being empty. It does not coordinate with *other* clients writing the same nodes, or with a second concurrent run.
* Writes to existing nodes and relationships are updates (`SET +=`) rather than creates, and endpoint lookups hit indexes that now hold the whole graph. Expect the page cache to matter more: it should hold the graph (with 12 GB the hit ratio on the 40k IMDb load, a 4M-node 18M-relationship graph, had a lowest 5-second sample of 93%).
* The plan counts degrees from the rows of **this run only**, so a node that is already a hub in the graph but has few rows in this batch is not given a hub resource; its rows share a bucket resource. That stays lock-free, just less parallel for that node.
* Fixed per-run overhead (DDL check, container launches, planner start, image check) makes very small, frequent batches inefficient. The smallest run in the final smoke (20,106 records, planned) took 14 seconds.
* **Adding a constraint or index to a graph that already has data.** The CLI applies the schema DDL on every run (`IF NOT EXISTS`) and then waits for indexes to become ONLINE for at most 120 seconds (`Timeout waiting for indexes to become ONLINE`). A new uniqueness or not-null constraint fails if existing data violates it (`Failed to execute DDL`). On a large graph, create new indexes yourself ahead of the run.

### A6. The documented IMDb incremental flow

`examples/imdb` builds batches from different seed titles (`build_subset.py --titles N --offset M`; offset 0 keeps `titles-N-depth-D/`, others are `titles-N-offset-M-depth-D/`), all from the same extracted IMDb snapshot. Traversal around different seeds can overlap, which is fine: repeated nodes and relationships are upserted.

```bash
python examples/imdb/build_subset.py --titles 100 --offset 0   --depth 4
python examples/imdb/build_subset.py --titles 100 --offset 100 --depth 4
```

Then, for each batch: `./scripts/cleanup_imdb_local_load.sh` (it refuses to run while a graph-loader fleet is active, deletes this example's input topics and the node groups, and keeps Neo4j and the shared control topics), then `publish_and_load_nodes.py --subset-dir <that batch> ...`. The publisher also refuses to send into non-empty input topics, so a failed run cannot silently append a second copy. This repository records the flow and the REG-04 acceptance criteria, but no incremental run timings or reports (the reports under `examples/imdb/reports` are single full loads).

## 3. Part B: continuous streaming

```bash
python -m src.cli start --mode stream --config my_graph.yaml --run-id my-fleet
```

`--mode stream` keeps node and relationship loaders running and consumes new records continuously. It never exits because lag reached zero. Planned mode and every bulk-only option do not apply.

### B1. The fleet

The CLI stays in the foreground as the supervisor and launches, on the Docker network (`--network`, default `graph-loader-net`):

| Component | Count | Role |
| --- | --- | --- |
| **Lease server** | 1 (`graph-loader-lease-<run token>`) | Grants each write transaction an exclusive, node-disjoint set of resources before it writes. Started **before** the node loaders. Supervised: restarted with backoff if it dies. |
| **Node loaders** | `replicas` per label (YAML, default 1) | Consume the node topics; **node writes are leased too**, split into per-resource cells, so a node `MERGE` never waits on a relationship `MERGE` of the same node. |
| **Retry consumer** | `--retry-consumer-replicas` (default 1), one consumer group | Launched only when some relationship type has an enabled `dead_letter`. Reads every DLQ topic and retries parked-on-missing-endpoint rows with backoff. |
| **Edge loaders** | `replicas` per relationship type | Consume the relationship topics; write through the lease server. |

Because every writer (nodes, edges and the retry consumer) takes leases, concurrent transactions are node-disjoint and there are zero Neo4j lock waits, also across lease-server restarts and writer disconnects: every leased transaction carries a server-enforced timeout that ends it before its lease deadline, a disconnected writer's resources stay held for `T + margin`, and a restarted server grants nothing for a quiet period `Q = T + margin` ([USAGE.md section 12](USAGE.md#12-limitations)). The one condition: if you raise Neo4j's `db.transaction.monitor.check.interval` above its 2 s default, raise `loading.bucket_lease.restart_quiet_margin_ms` by the same amount. Stream mode (and bulk `--edge-leases`) requires `bucket_lease.tx_timeout_ms` of at least 6000. Each leased transaction's timeout is the time left until the lease deadline minus 4 s (3 s monitor slack plus 1 s BEGIN allowance). No statement of a leased transaction starts after its lease deadline. A COMMIT already in progress at that point still completes, and `restart_quiet_margin_ms` covers it. Streaming has no planned mode. The deprecated `--stream-clock-rotation` is a one-release escape hatch; do not use it for new deployments: it has no row-level DLQ, it is not zero-wait inside a slot, and it cannot run multi-pair types or ride out outages. A stream run with **no** relationship types launches the node loaders and exits `0` at once, leaving them running unsupervised (stop with `python -m src.cli stop` or the recovery recipe).

Stop order on shutdown: edge loaders and the retry consumer first, then the node loaders (they drain only what they already hold), then the lease server.

### B2. Preflight: what is checked before anything launches

`start --mode stream` does these in order and exits `1` without launching if one fails:

1. **Flag checks.** `--edge-classic requires --mode bulk (stream mode uses the bucket-lease fleet; its classic scheduling is only the deprecated --stream-clock-rotation)`; `--edge-plan requires --mode bulk`; `--edge-leases requires --mode bulk`; a `topic_conflict: precedence` type is refused (`topic_conflict='precedence' is not supported in stream mode (no cross-topic order exists): edge type(s) [...]. Use 'order_by: <property>' instead, or set `loading.mode: bulk` (or remove it) and run `start --mode bulk`. Precedence runs in bulk mode only (planned, --edge-leases or --edge-classic)`).
2. **Topic preflight.** Every node and relationship topic the schema declares must already exist. Otherwise: `stage=launch reason=missing input topics: <names>. Create or publish them first (the producer chooses the partition count; graph-loader does not create them), then rerun.` The loader never creates data topics. (Bulk mode does not run this check.)
3. **Former-DLQ and leftover checks** for DLQ-enabled types (the migration notes below): `--accept-former-dlq`, `--accept-dlq-leftovers`.
4. **DLQ provisioning.** Every enabled DLQ topic is created or verified before any container starts (unlimited retention, `retention.ms=-1`, `retention.bytes=-1`; `stage=launch ensured dead-letter topic <t> (source=...)`). So are the parking-lot topic `<DLQ>.parked` and, for types without `order_by`, the compacted parked-pair index `<DLQ>.parked-pairs`. A pre-existing topic with the wrong retention or too small `max.message.bytes` stops the start: `DLQ topic <t> already exists but violates the DLQ policy`.
5. Schema DDL and image build (node, edge and clock images unless `--skip-image-build`). A failure here also exits `1` without launching.
6. **Consumer-group checks** (after the DDL and the image build, before the control topic). The CLI copies legacy run-scoped offsets into the stable groups when `--run-id` names the earlier run (B10) and refuses to start while a stable group still has members, waiting up to `session_timeout_ms` plus 5 s (`--stream-group-member-wait-seconds S` to wait longer). A relationship type named like a node label is refused in stream.
7. Control topic, and then the launch.

The DLQ is created with as many partitions as the type's widest topic and is never resized afterwards.

### B3. A stream schema

```yaml
loading:
  mode: stream
  consumer_group_id: my-stream          # node loaders use <id>-<Label>; edge groups use <id>-<type>; retry consumer <id>-retry-consumer
  flush_interval_ms: 1000               # a partial batch is flushed after this idle time (latency bound)
  retry_consumer:
    max_records_per_partition: 1000
    write_workers: 4
nodes:
  - label: Person
    topic: person-events
    replicas: 2                         # stream does not derive replicas; at most the topic's partition count is useful
    key_property: person_id
    properties:
      person_id: {type: integer, required: true, constraint: {type: uniqueness}}
      name: {type: string, required: true}
  - label: Company
    topic: company-events
    key_property: company_id
    properties:
      company_id: {type: string, required: true, constraint: {type: uniqueness}}
      name: {type: string}
edges:
  - type: WORKS_AT
    topic: works-at-events
    replicas: 2
    nodes: {source: Person, target: Company}
    source_key_property: person_id
    target_key_property: company_id
    order_by: updated_at                # newest wins, whatever the retry order
    retry: {base_delay_seconds: 1.0, max_delay_seconds: 60.0}
    dead_letter:
      enabled: true                     # edges may arrive before nodes: route them to the DLQ and retry
      max_retry_attempts: 12            # about 6 minutes of waiting before a row is parked (see B4)
    properties:
      role: {type: string}
      updated_at: {type: datetime, required: true}
```

* In stream, relationship `dead_letter.enabled: true` is what makes the edge-before-node race self-healing. Without it a relationship whose endpoint has not committed yet raises `MissingRelationshipEndpointError` and the loader exits, which stops the whole fleet (B8).
* Keep node topics keyed by node key and relationship topics keyed by source key ([BULK_LOAD.md section 4](BULK_LOAD.md#4-publishing-the-data-the-producer-contract)).
* The schema is read at launch; change it by stopping and restarting the fleet.

### B4. Edges before nodes: the row-level DLQ and the retry consumer

**What happens to a relationship whose node has not arrived.** The relationship loader writes each batch's resolvable rows in one Neo4j transaction. A row whose source or target is missing, or ambiguous (the key matches more than one node), is published instead to the type's DLQ (`<topic>.dlq`, or `<type>.dlq` for a type with several topics, or `dead_letter.topic`) as an envelope, and the rest of the batch still commits. The Kafka offset commits only after the DLQ publish is acknowledged. The envelope carries the row in the loader's own input shape, a `reason_code` (`missing_source`, `missing_target`, `missing_both`, `ambiguous_source`, `ambiguous_target`, `ambiguous_both`, or `blocked_by_earlier_unresolved_row`), `original_topic`/`original_partition`/`original_offset`, `attempt_count` and `retry_after_ms`. Malformed records go to the same DLQ as `failure_class=malformed` and are terminal (never retried).

**The retry consumer** reads the DLQ topics (one consumer group `<consumer_group_id>-retry-consumer`, independent of the run id, across all of them), holds each row until its `retry_after_ms` passes without blocking Kafka's `poll()`, and re-attempts the write through the same lease-fenced path. If it still cannot resolve, it republishes the envelope with `attempt_count + 1` and a longer delay; after `max_retry_attempts` it **parks** the row: a copy goes to `<DLQ>.parked` (unlimited retention), and the row is never retried automatically again. A parked row is never dropped.

**The backoff settings.** They are per relationship type:

| Setting | Default | Meaning |
| --- | --- | --- |
| `retry.base_delay_seconds` | `1.0` | The first retry is due this long after the row was first dead-lettered; later delays double from it. |
| `retry.max_delay_seconds` | `60.0` | Cap on a delay (must be at least the base). |
| `dead_letter.max_retry_attempts` | `5` (1-1000) | The row is parked after this many failed retry attempts. |
| `retry.max_attempts` | `3` | Separate: in-process retries of a transient Neo4j error inside one write call (same base and cap). Not the DLQ cycle count. |

The k-th retry attempt happens `base` seconds after the first DLQ publish for k = 1 and `min(base x 2^(k-2), max)` seconds after the previous attempt for k >= 2. The row is parked right after attempt `max_retry_attempts` fails. So **the window in which a late node can still rescue an edge is** `base + sum over k=2..N of min(base x 2^(k-2), max)`:

| `base` / `max` / `max_retry_attempts` | Approximate window before parking |
| --- | --- |
| 1 s / 60 s / 5 (the defaults) | about 16 s (attempts at 1, 2, 4, 8, 16 s) |
| 1 s / 60 s / 12 | about 6 minutes (attempts at 1, 2, 4, 8, 16, 32, 64, 124, 184, 244, 304, 364 s) |
| 1 s / 10 s / 30 (the `multi_topic` dead-letter example) | about 4.4 minutes (266 s) |

These are minimum waits; scheduling adds slack. **Size `max_retry_attempts` and the delays for how late your nodes can be**, because the defaults park an edge whose node takes longer than about 16 seconds. A row that is only *blocked* behind an earlier unresolved row of its pair does not consume its own attempts. Failures that are system-wide (Neo4j unavailable, read-only database, authentication, pool timeouts) never count against a row either: the type is *stalled* instead (`reason=retry_stalled`, B9).

**Tuning the consumer itself** (`loading.retry_consumer`, streaming only, unknown keys are rejected): `poll_timeout_ms` (500), `backlog_check_interval_ms` (10000, refresh of the backlog gauge), `due_batch_coalesce_ms` (200), `max_records_per_partition` (1000; a DLQ partition pauses at this many outstanding rows), `max_batch_records` (2000), `write_workers` (4; concurrent lease-holding write threads per type, per label pair for multi-pair types), `max_overflow_held_per_partition` (`null` = 10 x `max_records_per_partition`; blocked rows one DLQ partition may hold in memory beyond the cap, each holding its envelope, up to 8 MiB). Memory per assigned partition is about (`max_records_per_partition` + overflow) x the envelope size (typically a few KiB).

**`replay-dlq`.** Missing-endpoint rows are retried automatically, so you rarely need it for them. You do need it for **parked** rows and for malformed records you have fixed upstream:

```bash
python -m src.cli replay-dlq --config my_graph.yaml --edge-type WORKS_AT --dry-run
python -m src.cli replay-dlq --config my_graph.yaml --edge-type WORKS_AT --reason-code missing_source --since 2026-10-01T00:00:00Z
python -m src.cli replay-dlq --config my_graph.yaml --edge-type WORKS_AT --parked --dry-run
python -m src.cli replay-dlq --config my_graph.yaml --edge-type WORKS_AT --parked --source-topic works-at-b
```

* `--parked` reads `<DLQ>.parked` instead of the DLQ. It requires an enabled DLQ.
* `--source-topic` (repeatable) replays only envelopes whose `original_topic` is that topic; each value must be one of the type's topics (otherwise exit `1`). Without `--target-topic`, each envelope goes back to its own `original_topic`. `--target-topic` is for replaying into a staging topic; for a multi-pair type it needs `--source-topic` values that all bind one pair.
* The command reads with a dedicated non-group consumer and never commits on the DLQ, so a replay (or a dry run) leaves the envelopes in place, up to the high watermark captured when it started. It replays each distinct record once, from its first copy, in the order the records first reached the DLQ. A live replay stops at the first republish that is not acknowledged and exits non-zero; an envelope with no payload is skipped with a warning.
* A replayed record re-enters the pipeline as a new record with a fresh `attempt_count=0`. **A replay can overwrite a newer value with an older one**, because it is applied when read again. `order_by` protects the newer value; otherwise check for newer rows of the same pair first. A replay into an `arrival` or `precedence` type logs a warning.
* `--since`/`--until` filter on the envelope's first-failure time, `--limit` bounds the number of distinct records, `--dlq-topic` overrides the topic (required if the entity has no `dead_letter` block).

**DLQ sizing.** DLQ topics have unlimited retention. A type's DLQ holds at most `records x (max_retry_attempts + 1)` envelopes from its own failures (the live writer's first copy plus one re-publish per failed attempt of a pair's earliest outstanding row), plus the blocked copies the retry consumer publishes only when a DLQ partition's backlog exceeds `max_records_per_partition` plus `max_overflow_held_per_partition`. Measured: 4,002 records with their nodes missing produced 6,495 envelopes in one run (1.6x), and 3,000 records waiting about 3.5 minutes at a 5 s maximum delay with 200 attempts allowed produced 37,143 (12.4x, all root cycles). Size the topic's disk for the worst case you accept (missing endpoints x attempts), watch `graph_loader_dlq_backlog`, and keep `max_records_per_partition` near the expected backlog per partition (roughly the edge-before-node backlog divided by the DLQ's partitions). A "DLQ envelopes per consumed record" ratio that keeps climbing past `max_retry_attempts + 1` means blocked copies are being re-published: raise the bounds or add DLQ partitions. Set `min.insync.replicas >= 2` on the DLQ topic for durability (the producer uses `acks=all`, which is only as durable as the topic).

### B5. Ordering guarantees, and their limits

* **Per-pair order inside a partition.** Kafka orders only within a partition. With records keyed by source key, every record of one `(source, target)` pair is in one partition of its topic, is processed in offset order, and a pair's rows stay in order inside the cells of a write.
* **A row that fails parks its pair's later rows (types without `order_by`).** When an earlier row of a pair cannot be written, every later row of the same pair is held (reason `blocked_by_earlier_unresolved_row`, even in a later flush) until the earlier row is resolved or parked. A durable, cross-process **parked-pair index** (compacted topic `<DLQ>.parked-pairs`) records this, so a retry of the earlier row can never overwrite the later row's newer properties. For a multi-topic type each index entry carries a per-pair sequence number so the pair's records are retried in the order they entered the index, whatever their topics.
* **`order_by` is the alternative.** An `order_by` type uses no parked-pair index: a conditional `SET` applies a record only if its value is strictly newer than the stored one, so any retry, replay or redelivery order converges to the newest value. Rows missing the `order_by` property are applied unconditionally. A tie, a strictly older value, or two values Cypher's `>` cannot compare (for example a string on one row and a number on another) leaves the stored value untouched, silently, counted as `skipped_as_not_newer_rows`.
* **What is only best-effort** (for non-`order_by` types):
  * **Several writer replicas.** When one pair's records span more than one partition or replica, a replica can miss another replica's just-acknowledged index entry and write a later row without seeing the block; a stale retry of the earlier row can then overwrite it. This is documented and accepted; narrowing it would need synchronous coordination on every write. After a partition moves to a new replica, that replica reads nothing from it until its copy of the index has caught up (log `reason=parked_pair_index_lagging` every 30 s after 10 s; gauge `graph_loader_parked_pair_index_wait_seconds`), with no timeout into a stale view.
  * **Manual replay** bypasses the index entirely.
  * **Standalone `retry_consumer`** (no `--lease-address`) has no ordering guarantee at all.
* **Replay resurrection.** If a stream edge loader exits after part of a flush committed (a crash, a drain abandon) and the next owner re-reads the flush, a row that the retry consumer already wrote or parked can be dead-lettered again, which would apply an older row after a newer one. The retry consumer prevents this by remembering, per pair and source partition, the highest source offset it has written or parked; a copy at or below that offset that is indexed again is removed from the index, committed past, and never written (`reason=stale_resurrected_copy_skipped`, not counted in `graph_loader_retry_total`). **Limits:** the memory lives in the retry consumer process, so it is lost on a retry-consumer restart or when a rebalance revokes the DLQ partition, so a resurrected copy arriving after that is retried and can land after the newer value. Offsets are compared only within one topic partition, so for a multi-topic type a newer row of the same pair on another topic does not count. The memory holds 100,000 (pair, source partition) entries, least recently used evicted. **Mitigation: declare `order_by`.**
* **The topic-recreation caveat.** Do not delete and recreate a source topic while a fleet is running. Offsets of a recreated topic start again from 0; a running retry consumer remembers the old high offsets and would skip the new records at lower offsets as resurrected copies, **silently losing them**. Stop the fleet first, including the retry consumer.
* **At-least-once.** DLQ publishing is at-least-once: a crash between the DLQ acknowledgement and the source offset commit, or a partial batch publish, yields duplicate envelopes. They are safe for the idempotent writes (a duplicate whose record was already resolved is skipped as `duplicate_skipped`), but not order-safe on their own, which is what the index and `order_by` are for.

### B6. Several topics or label pairs per type in stream

* **Conflict rule.** A type with two or more topics needs `order_by` or `topic_conflict: arrival` in stream. `precedence` is **refused** twice: at schema load when `loading.mode: stream` is written, and again by `start --mode stream` (the schema may be shared with a bulk run). It is accepted in stream only when every label pair of the type is fed by a single topic (the declared rule then has no effect and a warning says so).
* **`arrival`** is nondeterministic and warns at load. Two records of one pair on two topics within the index's propagation delay (normally milliseconds) can be written in either order: the cross-replica case above is the *normal* case here, because different replicas usually read different topics. Prefer `order_by`.
* **One DLQ per type**, never one per topic: `<type>.dlq`, `<type>.dlq.parked`, `<type>.dlq.parked-pairs` (or `dead_letter.topic` plus its siblings), sized to the widest topic. Each envelope keeps its `original_topic`, so `replay-dlq --source-topic` can pick one topic.
* **Several label pairs.** Each topic binds one pair; the DLQ key of a multi-pair type is `[source label, target label, source key, target key]`, so Person 1->Post 1 and Person 1->Comment 1 never block each other. `execution.worker_count` applies per pair (a replica opens up to `pairs x worker_count` sessions and lease connections; the retry consumer up to `pairs x write_workers`). The evidence rule for parking is per pair, and a stalled pair is reported as a stall of the type. `--stream-clock-rotation` refuses multi-pair types.
* **Metrics** keep `{kind, name=<type>}`; the lag gauge adds `topic` (one series per topic in the schema); nothing is labelled by pair.

### B7. Riding out a Neo4j outage

**Stream only, and only for lease-fenced writers** (every node and edge loader of the default fleet; the retry consumer already waited out outages). Bulk mode, `--stream-clock-rotation`, a stream run with no relationship types, and any loader without a lease address still fail fast.

On a connectivity-class write failure (Neo4j unavailable or restarting, an expired session, a connection pool or acquisition timeout, a transient error once the writer's own retries are spent, database, cluster or read-only-database error codes, authentication codes, the lease server unreachable) the loader: has already rolled back the failed transaction; holds **no lease** while it waits (it releases the leases whose outcome is known and closes its lease connection for the rest, which the server keeps fenced for `T + margin`); commits nothing; **pauses its partitions and keeps polling Kafka** (about five times a second, so the consumer group does not rebalance); waits a backoff that starts at 0.5 s and doubles to 30 s; and retries **the same batch in the same order**. Rows that committed before the outage are rewritten idempotently, and a row an earlier attempt already dead-lettered stays blocked without a second index entry or DLQ copy. It resumes by itself when writes succeed.

Signals: a WARNING `stage=write-outage kind=... name=... reason=neo4j_unavailable ...` when it begins; `stage=write-outage-retry` per failed retry; an ERROR with `reason=neo4j_outage` once it has lasted 30 s (repeated every 5 minutes); `stage=write-outage-recovered ... outage_s=... attempts=...` on recovery. The gauge `graph_loader_write_outage_seconds{kind,name}` counts the outage seconds (0 while writing) and the alert `GraphLoaderWriteOutage` fires above 120 s for 1 minute (critical).

**Fatal rules** (everything else keeps its old behaviour):

* Row-level and unknown errors: DLQ routing where the type has one, otherwise the loader exits `1` and supervision stops the fleet.
* Capacity errors (a transaction timeout, the per-transaction memory limit, server-wide memory-pool pressure, `OutOfMemoryError`, `StackOverFlowError`) fail as before, because one huge batch can cause them and retrying would stall forever.
* **Authentication and missing-database errors** (`Neo.ClientError.Security.*`, `Neo.ClientError.Database.DatabaseNotFound`) are ridden out only while they might heal. After 10 minutes of nothing but such errors (gaps under 60 s; only another Neo4j answer resets the clock) the loader logs `reason=neo4j_fatal` and exits `1`, and supervision stops the fleet. The retry consumer applies the same rule.

A `SIGTERM` or `stop` during an outage is honoured within about 0.2 s (or within the 5-second lease drain budget if the retry is waiting for a lease): the loader logs `stage=edge-drain-abandoned` (or `node-drain-abandoned`) with `reason=neo4j_outage`, commits nothing for the held batch and exits `0`; the batch is redelivered on the next run. A blocked driver connect or lease reconnect can outlast Docker's 10-second stop grace for edge loaders, in which case the container is killed (exit `137`); nothing is committed either way. If partitions are revoked during an outage (a sibling replica leaves), the loader hands the unwritten batch to the next owner uncommitted; the outage clock and alert cadence carry on.

### B8. Supervision: what stops the fleet

The CLI supervises the fleet until you stop it. It exits **`1`** after stopping everything when:

| Condition | Log |
| --- | --- |
| **Any edge loader container** is no longer running (any exit, including a clean exit `0` after a drain abandon, or a Docker disappearance) | `stage=lease-relationship-fleet run_id=<id> failed: relationship stage=<types> edge=<type> replica=<n> exited before drain acknowledgement status=... exit_code=... log_tail=...` |
| **A retry-consumer container** exits (crash, or the 10-minute authentication/missing-database fatal) | `stage=retry-consumer run_id=<id> replica=<n> retry consumer exited during stream supervision ... -- stopping the stream fleet` |
| **A node loader** exits while the run is active (detected within about a second; stream node containers are auto-removed, so their logs are gone: follow `docker logs -f` beforehand if you need them) | `stage=node-loader-supervision container=<name> reason=node loader exited while the stream run was active; stopping the run (rc 1)` |
| **The lease server** keeps dying: it is restarted with backoff (`restart_backoff_base_ms` 500 doubling to `restart_backoff_max_ms` 30000), and when the restart counter exceeds `max_consecutive_restarts` (5) the fleet stops (the count resets after `bucket_lease.restart_count_reset_ms`, default 600000, of healthy uptime plus the quiet period `Q`) | `stage=lease-server-restart run_id=<id> attempt=<n>/5` per restart, then `stage=monitor run_id=<id> lease server exceeded 5 restarts` |
| The lease server does not answer `PING` within 30 s at start | `stage=monitor run_id=<id> lease server did not become ready` |

An edge loader that exits tears down every other edge type and the lease server too; it is not restarted individually. Because no relationship write commits its Kafka offset before Neo4j confirmed it, restarting the same command resumes with no lost or duplicated writes ([B10](#b10-graceful-stop-restart-consumer-groups-and-resume)). The exits that remain after the outage ride-out are: unknown or row-level Neo4j errors on a type without a DLQ, an edge-before-node race on a type without `dead_letter`, an oversized DLQ envelope (`... is N bytes, over the 8388608-byte DLQ envelope limit`: that partition stalls fail-closed), the 10-minute authentication fatal, and crashes.

### B9. Monitoring and alerts

[`docs/MONITORING.md`](MONITORING.md) describes the optional Prometheus and Grafana stack (`make monitoring-up`; Prometheus on `:9090`, Grafana on `:3000`, `kafka-ui` on `:8080` from the base compose file). Alert thresholds are "starting points, not an SLO"; tune them. What to watch in a stream:

| Signal | Metric or alert | Meaning and action |
| --- | --- | --- |
| **Consumer lag** | `graph_loader_consumer_lag_records{kind="cli",name=<type>,topic}`; alerts `GraphLoaderConsumerLagHigh` (>100,000 for 10 m), `GraphLoaderConsumerLagCritical` (>1,000,000 for 5 m), `GraphLoaderConsumerLagNotDraining` | Edge-type lag per topic, refreshed by the CLI supervisor about every 10 s. **Only the CLI exports it, on `127.0.0.1`:** the monitoring stack scrapes it only if you set `GRAPH_LOADER_METRICS_BIND_ADDR=0.0.0.0` before `start` (see MONITORING.md section 7). There is no node-topic lag metric; use `docker exec graph-loader-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group <consumer_group_id>-<Label>`. |
| **DLQ backlog** | `graph_loader_dlq_backlog{kind="edge",name,queue="dlq"}`; alert `GraphLoaderDlqBacklogHigh` (>100 for 15 m, warning) | DLQ depth minus the retry consumer's committed offset. Sustained high = endpoints not arriving or the consumer not draining. Aggregate replicas with `max by (kind, name, queue)`. |
| **Parking lot** | `graph_loader_dlq_backlog{queue="parked"}`, `graph_loader_parked_total`; alert `GraphLoaderParkedLotGrowing` (critical on any growth) | Rows ran out of attempts. Cumulative; needs a human (`replay-dlq --parked`). |
| **DLQ activity** | `graph_loader_dlq_routed_total`; `GraphLoaderDlqRoutingDetected` (info), `GraphLoaderDlqRoutingSpike` (>50 in 15 m) | Routing just started, or is spiking. |
| **Retry consumer gone** | `GraphLoaderDlqBacklogAbsent` (critical, 5 m) | The backlog gauge of a type was reported in the last 6 h and is now absent while its edge loaders are up: the retry consumer is gone or unscraped. (In stream the CLI also stops the fleet when the container exits.) Silent for bulk and after a clean stop. |
| **Retry stalled** | `graph_loader_retry_stalled_seconds{kind="edge",name}`; `GraphLoaderRetryStalled` (>600 s for 1 m, warning) | Retry writes of a type keep failing with nothing written (read-only or unavailable database, an unrecognised error, a lone row with an unknown error). Rows wait instead of parking. Look for `reason=retry_stalled` ERROR lines (first after 2 minutes, then every 5). |
| **Write outage** | `graph_loader_write_outage_seconds{kind,name}`; `GraphLoaderWriteOutage` (>120 s for 1 m, critical) | A stream loader cannot write to Neo4j and is retrying its held batch (B7). Consumer lag grows meanwhile. |
| **Index admission** | `graph_loader_parked_pair_index_wait_seconds{kind,name}` | Seconds a loader has held new partitions waiting for its index view. 0 or a fraction of a second after a rebalance; a growing value means the index topic is unreadable. No alert ships for it. |
| **Lease server** | `graph_loader_lease_*` (`kind="lease-server"`), `GraphLoaderLeaseWaitersStuck`, `GraphLoaderLeaseWaitTimeoutsDetected` | Counters reset to 0 on a supervised restart; use `rate()`/`increase()`. |
| **Idle loaders** | `GraphLoaderNoRecordsConsumed` | Fires for a legitimately idle stream loader with no traffic about 20 minutes in; scope or tune it. |

The dashboard also has a "DLQ envelopes per consumed record" panel (the DLQ routing rate against the consumed-record rate, per type), a "Retry writes stalled" panel, a "Stream write outage" panel, and the multi-topic lag rows. Each container exposes metrics on its own `GRAPH_LOADER_METRICS_PORT` (default 9464, mapped to an ephemeral loopback host port); with `GRAPH_LOADER_TIMING_DIR` set, every process also writes a `metrics-*.prom` snapshot every 15 s, which survives a killed process. For a bulk-style run summary there is no "done" in a stream: judge health by lag, backlog and the alerts.

### B10. Graceful stop, restart, consumer groups and resume


* **Stop** with `Ctrl-C` or SIGTERM on the `start` process. A stream run that was supervising exits **`0`** (measured 4-9 s in the final smoke); one stopped before it was fully up exits non-zero. Loaders stop in the order edges and retry consumer, node loaders, lease server. A loader that is stopping waits at most 5 seconds in total for bucket leases; when that budget is spent it commits nothing for the unflushed batch, logs `stage=edge-drain-abandoned` (or `stage=node-drain-abandoned`) and exits `0`; the records are redelivered on the next run, and cells already written are replayed idempotently. A second `Ctrl-C` changes nothing. After a `kill -9` of the CLI every container keeps running; use the recovery recipe in [BULK_LOAD.md section 13](BULK_LOAD.md#13-stopping-safely-and-exit-codes).
* **Which consumer groups, and so what resumes:**

  | Group | Name | Survives a restart? |
  | --- | --- | --- |
  | Node loaders | `<loading.consumer_group_id>-<Label>` | Always. Not run-scoped. |
  | Edge loaders | `<loading.consumer_group_id>-<type>` (one group per type, for all its topics, label pairs and replicas) | Always, with or without `--run-id`. |
  | Retry consumer | `<loading.consumer_group_id>-retry-consumer` (all DLQ topics) | Always, with or without `--run-id`. |

  The stream groups are stable and do not contain the run id, so stopping the fleet and running the same `start` command again resumes every relationship topic and the DLQ from the committed offsets, even without `--run-id`. Nothing already committed is re-read: no replay of old values, no second park of a parked row, no lag burst. (A group without a commit starts from the earliest offset.) Bulk lease/classic groups are unchanged: run-scoped and derived from the run id and stage. Passing a fixed `--run-id` is still useful for predictable container and metric labels, and it is what lets the CLI migrate legacy offsets (below).
  * **Upgrading from run-scoped stream groups.** Earlier releases used `<prefix>-<type>-<R>` and `graph-loader-retry-consumer-<R>`. Before a fleet launches, for each stable group that has no commit, if `--run-id R` names the earlier run the CLI copies the offsets of the legacy group into it (only offsets inside the partition's current range) and logs a WARNING per copy, `stage=consumer-groups reason=migrated_legacy_offsets from_group=<old> to_group=<new>`. A stable group that already has a commit is never overwritten. Without `--run-id` the earlier id is unknown and nothing is migrated: a stable group without commits starts from the earliest offset (logged at INFO as `resume=stable-group`). The legacy groups are no longer read and can be deleted. A manual `kafka-consumer-groups --reset-offsets` recipe is in USAGE.md section 10, "Consumer groups and restarts".
  * **One live fleet per `consumer_group_id`.** Two stream fleets with the same prefix would share these groups and split the partitions between two lease servers that do not know each other. The CLI refuses to start while a stable group still has members (waiting first up to `session_timeout_ms` plus 5 s so a crashed run's members can expire; `--stream-group-member-wait-seconds S` waits longer). With the default `session_timeout_ms` of 45000 a second `start` against a live fleet therefore takes about 50 s to refuse (the members of the running fleet do not expire, so the full wait elapses); pass a smaller `--stream-group-member-wait-seconds S` to fail faster, and stop the first fleet instead of waiting. A running fleet also checks every 10 s and stops with `stage=consumer-groups reason=foreign_members` (exit 1) when a group has more members than its own replicas on two consecutive checks. Separate deployments on one Kafka cluster need their own `consumer_group_id`.
  * **Offset retention.** Kafka drops the committed offsets of a group with no members after `offsets.retention.minutes` (7 days by default). A fleet stopped for longer restarts from the earliest offset of every relationship topic and DLQ: idempotent, but older values replay first and DLQ rows are retried again. Raise the broker setting if a fleet may stay down longer.
* **Resume semantics.** An offset is committed only after the Neo4j write is durable (or the DLQ publish is acknowledged), so a restart loses and duplicates nothing that matters (writes are idempotent; duplicate DLQ envelopes are handled as above). The parked-pair index and the DLQ are Kafka topics and survive restarts; the retry consumer's resurrection memory does not (B5).
* **Container names.** With an explicit `--run-id`, node containers are named `graph-loader-node-<Label>-<replica>-<hex id>`; without it, `graph-loader-node-<Label>-<replica>`, so only one such fleet can exist per Docker host. After a `kill -9`, remove the old containers before restarting with the same id (a name clash otherwise).
* **Changing the topic list of a DLQ-enabled type**: stop the fleet and drain the DLQ first ([migration notes](#5-migration-notes-single-topic-to-multi-topic)).

### B11. Scaling replicas

| Lever | Where | Notes |
| --- | --- | --- |
| Node loaders per label | `nodes[].replicas` (default 1, YAML only; stream does not derive replicas) | Useful up to the topic's partition count. |
| Edge loaders per type | `edges[].replicas` (1-64) | Same bound over all the type's topics' partitions. Group membership rebalances; no static assignment in stream. |
| Concurrent write sessions | `edges[].execution.worker_count` (1-64) | Per replica, per label pair. Each session holds its own lease. |
| Batch sizes | `loading.unwind_batch_size` (nodes, 1-10,000), `--bucket-lease-outer-batch-size` (default 32000; `loading.edge_unwind_batch_size` is classic only), `mix_and_batch.batch_size` (rows per transaction, default 1000) | Larger node batches keep leased node cells well filled. `loading.flush_interval_ms` (default 1000) bounds how long a partial batch waits. |
| Retry consumer | `--retry-consumer-replicas N` (1-64), `loading.retry_consumer.write_workers` | One group across all DLQ topics; useful up to the DLQ partition count (widest topic at creation, never resized). Each replica reports the same group-wide gauge. |
| Lease server | one per run | Not scalable. `--bucket-lease-buckets` (default 16) and `--bucket-lease-keyed-max-nodes` (default 1000) tune its resources; `loading.bucket_lease.*` tunes supervision and fencing. |
| Neo4j | | The ceiling: each relationship costs about 18-25 microseconds of server CPU. |

Replica counts take effect only at the next `start`; stop and restart (the stable groups resume). The DLQ's partition count is fixed when it is created. USAGE.md notes `kafka-topics --alter` can add partitions to a narrower-than-needed DLQ. Adding partitions remaps the pair keys (default hash partitioner), so alter only with the fleet stopped and the DLQ drained (backlog 0). The `.parked` and `.parked-pairs` topics are not resized.

## 4. A versus B at a glance

| | **A. Incremental bulk batches** | **B. Continuous streaming** |
| --- | --- | --- |
| **Latency** | Batch interval plus run time. Fixed overhead per run; the smallest documented run (20,106 records) took 14 s; IMDb 20k from scratch about 70 s, 40k about 127 s ([BULK_LOAD.md section 15](BULK_LOAD.md#15-performance-numbers)) | Seconds: bounded by `flush_interval_ms` (default 1 s) plus write time, and for an edge that precedes its node by the retry delay. In the final smoke (small datasets, edges published 60 s before nodes) the graph was exact 10-15 s after the nodes |
| **Throughput** | Highest: planned mode, Neo4j-CPU bound (about 18-25 microseconds per relationship); zero lock waits | Lower per record: leased, smaller cells; zero lock waits. The only recorded stream figure is indicative: 30,456 relationships in 8.92 s on a small dataset (`plan/v5/completion_slice-1.md`). No at-scale stream benchmark is documented |
| **Failure handling** | Fail closed: any problem exits `1`, containers are stopped, rerun is idempotent. Planned: invalid relationship records to a rejection file, a missing endpoint fails the run. Lease/classic: opt-in DLQ, no retry loop | Row-level DLQ, automatic retry with backoff, parking lot, replay; rides out Neo4j outages; a loader crash or fatal error stops the fleet (restart the same command) |
| **Ordering** | `(partition, offset)` within a run; `precedence`, `arrival`, `order_by` | Per partition; `order_by` or `arrival` only; parked-pair index or `order_by` to survive retries; best-effort limits listed in B5 |
| **Edges before nodes** | Not supported (nodes load first) | Supported through the DLQ |
| **Ops burden** | A job. Hold producers still during a run; manage run ids, node groups and leftovers | A fleet. Alerts (lag, DLQ, parked, stalled, outage), DLQ disk, restarts, topic changes with drained DLQs |
| **Resources** | Spikes: planner memory and spill disk, chunk topic, writer pool; idle between runs | Steady: lease server, loaders and retry consumer always up |
| **Use when** | Discrete exports, backfills, nightly loads, very large volumes, `precedence` rules | Live feeds, low-latency freshness, producers you cannot coordinate with, out-of-order node and edge arrival |

### Handing over from a bulk load to a stream

A common path is an initial bulk load, then a stream for what arrives afterwards. Node loaders in both modes use the same group `<consumer_group_id>-<Label>`, so if you keep `loading.consumer_group_id`, the stream's node loaders continue from the offsets the bulk run committed. **There is no automatic relationship hand-over.** Bulk lease/classic edge groups are run-scoped (`<prefix>-<type>-<R>.bulk.0.<h>`) and a planned bulk run commits no edge group at all, while the stream uses the stable groups `<prefix>-<type>`. Either accept one idempotent re-read of the relationship topics by the stream, or pre-seed the stable groups to the end with `kafka-consumer-groups --reset-offsets --to-latest` (stop-state groups only) after a successful bulk run and before producers resume. That pre-seeding is untested. Rows that a bulk run routed to a DLQ with `route_missing_endpoints_in_bulk` are never indexed in the parked-pair index, so for non-`order_by` types the stream's retry consumer commits past them (`duplicate_or_unindexed_skipped`) without writing them: drain them first with a standalone `python -m src.loader.retry_consumer --config ... --dlq-topics <topics>`. `precedence` types cannot be handed to a stream (refused), and the stream schema must satisfy the stream rules (a conflict rule of `order_by` or `arrival`).

## 5. Migration notes: single-topic to multi-topic

A relationship type can read several topics (`topics: [a, b]`; see [BULK_LOAD.md section 3](BULK_LOAD.md#3-the-schema)). With DLQ-enabled types this changes names that hold state, so plan it.

* **The DLQ naming warning.** A single-topic type's DLQ is named after its topic: `<topic>.dlq`, with `<topic>.dlq.parked` and `<topic>.dlq.parked-pairs`. A type with two or more topics defaults to `<type>.dlq` (and `.parked`, `.parked-pairs`). Adding a second topic therefore **orphans** the records in the old DLQ, parking lot and index. **Before adding the second topic to a type that has (or may have) DLQ data, set `dead_letter.topic` to the old name** (`<old-topic>.dlq`); the parking-lot and index names follow it. Going back from several topics to one renames the default from `<type>.dlq` to `<topic>.dlq`; set `dead_letter.topic: <type>.dlq` first.

```yaml
edges:
  - type: WORKS_AT
    topics: [works-at-events, works-at-backfill]
    order_by: updated_at
    dead_letter:
      enabled: true
      topic: works-at-events.dlq          # keep the name the single-topic type used
    nodes: {source: Person, target: Company}
    source_key_property: person_id
    target_key_property: company_id
    properties:
      updated_at: {type: datetime, required: true}
```

* **`start` enforces it.** For a DLQ-enabled type **without** an explicit `dead_letter.topic`, if `<topic>.dlq` (for any of its topics) or `<type>.dlq` exists, is not the name the type now resolves, is not another entity's DLQ topic and still holds records, `start` refuses before creating any topic:
  `relationship type X now resolves its DLQ to Y, but its former default DLQ Z still holds N record(s): set `dead_letter.topic: Z` on X to keep using it (its parking lot and parked-pair index follow), or replay/drain and delete Z first (or pass --accept-former-dlq Z if it is kept for audit or owned elsewhere)`.
* **`--accept-former-dlq TOPIC`** (repeatable) starts anyway. Use it when the old topic must stay (kept for audit, or `<type>.dlq` belongs to another deployment on a shared cluster). The expected step is the opposite: replay or drain the old DLQ and **delete** the old topics (`<old>.dlq`, `<old>.dlq.parked`, `<old>.dlq.parked-pairs`), because a drained DLQ still holds its records until deleted.
* **`--accept-dlq-leftovers TYPE`** (repeatable) covers the parked-pair index: `start` refuses when a DLQ-enabled non-`order_by` type's index still holds records of a topic the type no longer reads, or of a topic now bound to another label pair, and those rows cannot be written through the type as it is now (`relationship type X: its parked-pair index <t> holds ...: drain the DLQ with the previous schema first, or pass --accept-dlq-leftovers X to start anyway`). Passing it starts anyway; those rows park after `max_retry_attempts`, blocking records of the same keys until then. A single-pair type's leftovers of a removed topic are retried through its one pair, so for it `start` only logs `reason=index_holds_entries_the_type_no_longer_binds`. The check reads only the compacted index topic, so it cannot see an `order_by` type's DLQ (no index), bulk opt-in rows, or a rebound topic of a single-pair type; drain the DLQ before any such change.
* **Change a DLQ-enabled type's topic list only with its fleet stopped and its DLQ drained**, not in a rolling restart. Leftovers of a removed topic of a multi-pair type are `invalid_on_retry` and park; adding a pair means adding a topic, so the same rules apply, and rebinding a topic to another pair or collapsing a multi-pair type back to one pair needs a drained DLQ first.
* **A bulk run needs the same care:** these preflights run in every non-planned start, and `--accept-*` apply there too. A planned bulk run skips them (it provisions no relationship DLQ topics).
* **The conflict rule:** the new multi-topic type needs `order_by` or a `topic_conflict`; for stream choose `order_by` or `arrival`.

## 6. Troubleshooting for stream

| Symptom | Cause and fix |
| --- | --- |
| `start --mode stream` exits 1 with `missing input topics: ...` | A declared node or relationship topic does not exist. Create (or publish to) the named topics and rerun. Nothing was launched. |
| `topic_conflict='precedence' is not supported in stream mode ...` | A multi-topic type uses `precedence`. Use `order_by`, or run it in bulk. |
| `--edge-classic requires --mode bulk ...`, `--edge-plan requires --mode bulk`, `--edge-leases requires --mode bulk` | Bulk-only flags on a stream start. Drop them. |
| `start` refuses: `relationship type X now resolves its DLQ to Y, but its former default DLQ Z still holds N record(s)` | A topic count change renamed the default DLQ. Set `dead_letter.topic: Z`, or drain and delete `Z`, or pass `--accept-former-dlq Z` (migration notes). |
| `start` refuses: `... its parked-pair index <t> holds ...` | The topic list or label pairs changed while the DLQ was not drained. Drain with the previous schema, or `--accept-dlq-leftovers X`. |
| `DLQ topic <t> already exists but violates the DLQ policy` | A pre-existing DLQ topic has retention other than -1 or a small `max.message.bytes`. Fix its config or delete it so the CLI recreates it. |
| Fleet exits 1 right after start with `stage=node-loader-supervision ...` or an edge `exited before drain acknowledgement ... exit_code=...` | A loader crashed. Read `log_tail` in the message or `docker logs <edge container>`; stream node containers are auto-removed, so follow `docker logs -f` in advance if you need their logs. |
| Fleet stops with `Edge '<type>' did not resolve exactly one source and target endpoint` | An edge-before-node race on a type **without** an enabled `dead_letter`. Enable `dead_letter` on it. |
| Fleet stops with `stage=retry-consumer ... retry consumer exited during stream supervision` | The retry consumer crashed or hit the 10-minute authentication/missing-database fatal. Check its log (`docker logs graph-loader-retry-<n>-<hex id>`), fix credentials or `NEO4J_DATABASE`, restart with the same `--run-id`. |
| `stage=monitor run_id=<id> lease server exceeded 5 restarts` | The lease server kept dying (more than 5 restarts without a healthy stretch of `restart_count_reset_ms` in between). Check its container log. Raise `loading.bucket_lease.max_consecutive_restarts` only after fixing the cause. |
| `reason=neo4j_outage` ERROR, lag rising, fleet still up | Expected during a Neo4j outage (B7); nothing is lost. Fix Neo4j and ingestion resumes. `reason=neo4j_fatal` followed by exit 1 means the credentials or database name failed for 10 minutes. |
| Edges are parked sooner than expected | The default retry window is about 16 s (B4). Raise `dead_letter.max_retry_attempts` and `retry.max_delay_seconds`. Then `replay-dlq --parked` the ones already parked. |
| `GraphLoaderRetryStalled` / `reason=retry_stalled` | The retry consumer's writes of a type fail with nothing written (read-only or unavailable database, unknown error). Rows wait; fix the cause. |
| DLQ keeps growing; ratio of envelopes per consumed record rises | Endpoints are not arriving, or blocked copies are republished because a partition's backlog exceeds the hold bounds. See DLQ sizing in B4. |
| `reason=parked_pair_index_lagging` repeating | A loader holds new partitions until its parked-pair index view catches up; if it never does, the index topic is unreadable. The partitions stay paused on purpose. |
| `reason=invalid_on_retry` for a multi-pair type | A DLQ row's `original_topic` is no longer one of the type's topics, or its key names another pair. It parks after `max_retry_attempts`; fix the schema or replay the row to the right topic. |
| `reason=stale_resurrected_copy_skipped` | Informational: a redelivered flush resurrected an already-resolved row and the retry consumer skipped it. |
| After a restart everything is reloaded and the DLQ replays | Only after an upgrade from run-scoped groups, or after the group offsets expired (`offsets.retention.minutes`, 7 days). On upgrade, start once with `--run-id <old run id>` so the CLI copies the legacy offsets (B10). |
| Start fails with a container name conflict after a crash | Containers of the previous run (killed with `kill -9`) are still present: remove them (`docker ps -a --filter label=app=graph-loader`) then restart. |
| Container exits `137` on stop during an outage | A blocked driver connect outlasted Docker's 10-second stop grace. Nothing was committed; the batch is redelivered. |
| Lag series missing from Prometheus | `graph_loader_consumer_lag_records` is exported only by the CLI on `127.0.0.1`. Set `GRAPH_LOADER_METRICS_BIND_ADDR=0.0.0.0` (MONITORING.md section 7). |
| A loader exits with `... over the 8388608-byte DLQ envelope limit` | A malformed record's DLQ envelope exceeds the cap; that partition stalls fail-closed. Stop the producer from sending oversized records, or advance the group's committed offset past it after capturing the record. |
| Newer records on a recreated topic never appear | A source topic was recreated while a retry consumer ran (B5). Stop the whole fleet, recreate, restart. |

## 7. Worked examples

### Example A: two incremental bulk batches

The schema is the minimal one from [BULK_LOAD.md section 3](BULK_LOAD.md#3-the-schema) (`Person`, `City`, `LIVES_IN`, group `my-graph`, topics `person-events`, `city-events`, `lives-in-events`), loaded in Pattern 2 style. **Batch 1** is the load from BULK_LOAD.md section 4: Person 7 `{"person_id": 7, "name": "Ada", "joined": "2024-01-31"}`, City `Paris`, and `LIVES_IN` 7 -> Paris with `since: 2020-05-01`.

```bash
# Environment as in BULK_LOAD.md section 2 (NEO4J_*, KAFKA_BOOTSTRAP_SERVERS, LOADER_*), NEO4J_DATABASE=mygraph.

# --- Batch 2: reset the inputs (Neo4j data is kept) ---------------------------------------------
for t in person-events city-events lives-in-events; do
  docker exec graph-loader-kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic "$t"
done
until ! docker exec graph-loader-kafka kafka-topics --bootstrap-server localhost:9092 --list \
        | grep -qxE 'person-events|city-events|lives-in-events'; do sleep 1; done       # deletion is asynchronous
for l in Person City; do
  docker exec graph-loader-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --delete --group "my-graph-$l" || true
done

# --- Publish ONLY batch 2 (recreate the topics, then send these records) ------------------------
#   person-events    : {"person_id": 7, "name": "Ada Lovelace"}          (new name; joined is not carried, so it is kept)
#                      {"person_id": 8, "name": "Grace"}
#   city-events      : {"name": "Lyon"}
#   lives-in-events  : {"source": {"person_id": 7}, "target": {"name": "Paris"}, "properties": {}}
#                      {"source": {"person_id": 8}, "target": {"name": "Lyon"}, "properties": {"since": "2021-03-04"}}
# (the producer sketch in BULK_LOAD.md section 4, with these records; hold the producer still afterwards)

# --- Load batch 2 ------------------------------------------------------------------------------------
python -m src.cli start --mode bulk --config my_graph.yaml --run-id inc-20261001-02 --skip-image-build

# --- Check ---------------------------------------------------------------------------------------------
docker exec graph-loader-neo4j cypher-shell -u neo4j -p "$NEO4J_PASSWORD" -d "$NEO4J_DATABASE" \
  "MATCH (p:Person)-[r:LIVES_IN]->(c:City) RETURN p.person_id, p.name, p.joined, c.name, r.since ORDER BY p.person_id"
```

Expected from the `MERGE`/`+=` semantics (not executed): two rows. Person 7 is `Ada Lovelace` with `joined` still `2024-01-31` and `since` still `2020-05-01` (batch 2 carried neither, and `+=` keeps what it does not carry); Person 8 is `Grace` with no `joined`, living in `Lyon` since `2021-03-04`. `Paris` stays because batch 2 never deleted it. The `LIVES_IN` count is 2, not 3: the repeated 7 -> Paris pair merged into the existing relationship. The planned run reads only the three batch-2 topics (`graph-loader-edge-chunks-inc-20261001-02` is created and deleted) and the node groups start fresh. The wait loop uses `grep -qxE` because Kafka deletes topics asynchronously.

### Example B: a stream with edges before nodes, a restart, and a replay

This uses the repository's dead-letter example, [`examples/multi_topic/graph_schema_dead_letter.yaml`](../examples/multi_topic/graph_schema_dead_letter.yaml): a stream-mode `RATED` type fed by two topics (`mt-rated-app`, `mt-rated-import`), `order_by: rated_at`, one DLQ `RATED.dlq`, retry delays 1 s doubling to 10 s, `max_retry_attempts: 30`, group `multi-topic-dlq-example`. Its README documents the exact graph match for this flow (and the final smoke recorded `exact_match: true`, `RATED.dlq.parked` empty and all lags 0 on 1,000 customers).

```bash
# Environment as in BULK_LOAD.md section 2; a database to load into (CREATE DATABASE mtstream IF NOT EXISTS WAIT).
export NEO4J_DATABASE=mtstream

# 1. Publish the relationships FIRST. This also creates every topic (the stream preflight requires them to exist).
python examples/multi_topic/multi_topic_graph.py publish --customers 1000 --only edges

# 2. Start the fleet in the background (--run-id is optional; it names containers and lets the CLI migrate legacy offsets).
python -m src.cli start --mode stream --config examples/multi_topic/graph_schema_dead_letter.yaml --run-id mt-dlq &
STREAM_PID=$!
#    Expect: "ensured dead-letter topic RATED.dlq", "ensured parking-lot topic RATED.dlq.parked"
#    (no parked-pairs topic: RATED has order_by). Every rating lands in RATED.dlq because no node exists yet.

# 3. Now publish the nodes. The retry consumer writes the ratings as the customers and products arrive.
python examples/multi_topic/multi_topic_graph.py publish --customers 1000 --only nodes
python examples/multi_topic/multi_topic_graph.py verify  --customers 1000 --types RATED    # repeat until "exact_match": true

# 4. Look at the state.
docker exec graph-loader-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group multi-topic-dlq-example-RATED   # edge loaders
docker exec graph-loader-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group multi-topic-dlq-example-retry-consumer     # retry consumer
docker exec graph-loader-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group multi-topic-dlq-example-Customer      # a node group
python -m src.cli replay-dlq --config examples/multi_topic/graph_schema_dead_letter.yaml --edge-type RATED --source-topic mt-rated-import --dry-run
python -m src.cli replay-dlq --config examples/multi_topic/graph_schema_dead_letter.yaml --edge-type RATED --parked --dry-run

# 5. Stop gracefully, then restart: the stable groups resume from the committed offsets.
kill -TERM $STREAM_PID; wait $STREAM_PID; echo "exit=$?"      # expect 0 within a few seconds
python -m src.cli start --mode stream --config examples/multi_topic/graph_schema_dead_letter.yaml --run-id mt-dlq &
```

Things to confirm while it runs: the group names are `<consumer_group_id>-<type>` for the edge loaders and `<consumer_group_id>-retry-consumer` for the retry consumer; every group's lag returns to 0 once the nodes have arrived; the `--parked --dry-run` replay matches 0 records on a healthy run; and the SIGTERM exit code is `0`. Restarting without `--run-id` resumes the same stable groups from their committed offsets just the same (B10). Clean up with `python examples/multi_topic/multi_topic_graph.py reset`, and delete the DLQ topics (`RATED.dlq`, `RATED.dlq.parked`) and the groups if you will reload.

## 8. Notes / open questions

* **Stream groups are stable.** Edge groups are `<prefix>-<type>` and the retry consumer's is `<prefix>-retry-consumer`, independent of the run id (B10), with a one-time migration of legacy offsets when `--run-id` names the earlier run. Not exercised end to end in this guide.
* **Pattern 3 (reused `--run-id` for lease/classic)** is derived from the code (deterministic derived group names, static assignment resumes from committed offsets) and is not tested as a procedure. The control-topic replay and offset-expiry caveats are listed with it. The bulk-to-stream hand-over pre-seeding of the stable groups is untested.
* **No incremental timings.** REG-04 in `plan/v5-v6-functional-test-plan.md` specifies the check and `plan/performance-review-20260927.md` says it is still to be done. Local benchmark output exists outside the repository and is not quoted. The reports in `examples/imdb/reports` are single full loads.
* **Lease-server restart limit.** The cap counts consecutive restarts: the count resets after `bucket_lease.restart_count_reset_ms` (default 600000) of healthy uptime plus the quiet period.
* **DLQ partitions.** Adding partitions remaps pair keys; alter only with the fleet stopped and the DLQ drained. `.parked` and `.parked-pairs` are not resized.
* **`GRAPH_LOADER_LEASE_MERGE_ROWS`** is forwarded to stream edge loaders and the retry consumer only, never to bulk containers.
* **Stream throughput at scale** has no documented benchmark; the one figure quoted is a small indicative run.
* **Stream node-loader replicas** come only from the YAML, so a stream label that omits `replicas` runs one loader.
* **The worked examples were not executed** (the shared local stack was in use). Example B follows `examples/multi_topic/README.md` plus a fixed `--run-id`; Example A's expected output is derived from the write semantics.
