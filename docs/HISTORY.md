# History and design background

Why graph-loader exists, the original design decisions, and what each became.
This page is background. The current behaviour is documented in
[USAGE](USAGE.md) and [ARCHITECTURE](ARCHITECTURE.md); changes between releases
are in the [CHANGELOG](../CHANGELOG.md). Back to the [README](../README.md).

## Background and initial ask

The project was born out of the need for a distributed, highly concurrent
pipeline that loads Kafka topics into a Neo4j graph.

**The challenge.** Loading nodes in parallel is straightforward. Loading
relationships concurrently is prone to database deadlocks. In a star-shaped
model (highly connected central nodes), concurrent threads try to acquire write
locks on the same nodes in different orders and the transactions fail.

**The goal.** A deadlock-free loading mechanism where:

* a central orchestrator triggers the loading jobs in Docker containers;
* node types load without contention;
* relationship types are scheduled so they cannot deadlock;
* both bulk and continuous streaming data are handled;
* the whole data model is generic and defined by a YAML schema.

Reference material studied during design: the Neo4j
[mix-and-batch relationship load](https://neo4j.com/blog/developer/mix-and-batch-relationship-load/)
article and the
[Cypher Manual](https://neo4j.com/docs/cypher-manual/current/introduction/).

## Original design decisions and what became of them

The first plan (Phases 1-6, kept under [`plan/`](../plan/)) made three
decisions. They were later superseded for the default paths; the original
techniques remain available as the classic bulk mode.

### 1. Dual-layer deadlock prevention

* **Layer 1, thread isolation:** partition edges in Python with directional
  mix-and-batch. Forward edges (`src_id < tgt_id`) and backward edges are split
  into separate slots, so `A->B` and `B->A` never share a batch.
* **Layer 2, global atomic locking:** before writing a batch, aggregate its
  distinct nodes, sort them by internal id and lock them with
  `apoc.lock.nodes()` to eliminate circular waits inside the batch.

*Today:* this is `--edge-classic`. It prevents deadlocks but not lock waits
between loaders. The default bulk mode (planned) and the lease modes avoid even
the waits; see [ARCHITECTURE](ARCHITECTURE.md#how-zero-lock-waits-is-achieved).

### 2. Multi-loader synchronization (global batch clock)

When several relationship types share a node label (for example `WORKS_AT` and
`BOUGHT` both touching `Person`), running them blindly in parallel causes
inter-loader deadlocks. The first decision was a Kafka-based **global batch
clock** in which an orchestrator assigns node hash buckets to loaders in
rotating time slots, with self-referencing types such as `Person-KNOWS-Person`
flagged to run in isolation.

*Today:* the clock is **not** the stream default. Streaming uses a fleet of
edge loaders under a bucket-lease coordinator (leases replace the rotating
slots). The clock and rotation scheduling remain reachable only through the
deprecated `--stream-clock-rotation` flag (`src/orchestrator/clock_runner.py`).
Bulk classic still isolates self-references into a final phase unless
`--bulk-edge-overlap shared` is used.

### 3. Real-time streaming and resilience

* **Edge before node:** an edge that arrives before its nodes is detected and
  routed to a dead-letter queue for retry with backoff.
* **Kafka eviction:** avoid exceeding `max.poll.interval.ms` during long Neo4j
  writes by keeping the consumer polling while writes run.
* **Rebalance safety:** wait for in-flight work and commit offsets before a
  partition is handed over.

*Today:* implemented as the opt-in `dead_letter` block (DLQ, retry consumer,
parking lot, parked-pair index), the commit-on-revoke behaviour, eviction prevention (a `LoadingConfig`
validator bounds the write budget against `max_poll_interval_ms`, and loaders
pause partitions while they retry or back off, instead of a dual-thread model), and a stream outage ride-out. See
[INCREMENTAL_LOAD](INCREMENTAL_LOAD.md) and USAGE's
[Dead-lettering](USAGE.md#dead-lettering-malformed-records-and-missing-endpoints).

## Project plan

The work was divided into six phases, with per-slice stories and completion
reports under [`plan/`](../plan/):

1. **Phase 1:** Foundation and infrastructure (Docker, schema initializer).
2. **Phase 2:** Node data ingestion.
3. **Phase 3:** High-throughput edge loading (the classic deadlock engine).
4. **Phase 4:** Multi-loader concurrency (the global batch clock).
5. **Phase 5:** Real-time streaming and resilience (DLQ, leases, retry).
6. **Phase 6:** Observability, metrics and hardening.

Later work (planned mode, multi-topic and multi-pair relationship types, stable
stream consumer groups) is described in the [CHANGELOG](../CHANGELOG.md) and
`plan/v2` through `plan/v7`. The plan documents are design records written
while the work was in progress: where they disagree with the code or with
USAGE, the code and USAGE win.

## Direct single-topic runs (Phase 2 and Phase 3 entry points)

The CLI is the normal entry point. The per-component modules still run on the
host for debugging against the local stack, with the connection variables
exported (`NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`,
`KAFKA_BOOTSTRAP_SERVERS`; `NEO4J_USER` is a legacy fallback):

```bash
# one node label, stop after 10 messages
python -m src.loader.node_loader --config config/graph_schema.yaml \
  --node-label Person --max-messages 10

# one relationship type, stop after 10 messages
python -m src.loader.edge_loader --config config/graph_schema.yaml \
  --edge-type WORKS_AT --max-messages 10
```

Both consume only the selected entity's configured topic. Omit
`--max-messages` for a continuous run. Run `--help` on either module for the
full flag list (`--mode`, `--topic`, `--run-id`, `--replica-id`; the edge loader
also takes `--topics`, `--replica-count` and the clock-path flags).

Properties that hold for the loaders in every mode:

* **Writes** use the `loading` section of the schema: `unwind_batch_size` caps
  records per `UNWIND` transaction, `flush_interval_ms` finalizes an idle
  partial batch, and `retry_max_attempts` / `retry_base_delay_ms` /
  `retry_max_delay_ms` apply bounded exponential retry only to Neo4j errors the
  driver marks retryable.
* **Offsets** are committed as explicit *next offsets* only after the whole
  relevant Neo4j batch is durable. Each topic-partition has a
  contiguous-resolution barrier, so an unresolved or malformed record cannot
  let a later offset skip ahead. During a retry the loader pauses its
  partitions, polls for group heartbeats, and fails closed if a rebalance
  revokes unresolved work.
* **Rejections.** Without a `dead_letter` block, malformed JSON or
  schema-invalid records are written with `flush()` and `fsync()` to the
  rejection log (`loading.rejection_log_path`; container replicas get their own
  JSONL file under `var/rejections/`) and only then become eligible for their
  offset commit. The entry holds topic, partition, offset, label or edge type
  and reason, and deliberately omits the raw payload. With `dead_letter`
  enabled, such records go to the DLQ instead.
* **Relationship envelope.** A relationship loader merges only the
  schema-declared labels and type. In classic mode it preflights both endpoint
  nodes inside the same Neo4j transaction, so a missing endpoint or a database
  failure leaves the offset uncommitted.
