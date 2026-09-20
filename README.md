# Neo4j Distributed Graph Loader

## Background & Initial Ask
This project was born out of the need to build a distributed, highly-concurrent pipeline to load data from Kafka topics into a Neo4j graph database. 

**The Challenge:** Loading nodes in parallel is straightforward, but loading relationships (edges) concurrently is notoriously prone to database deadlocks. If a data model resembles a star schema (highly connected central nodes), concurrent threads will inevitably try to acquire write locks on the same nodes in different sequences, crashing the transactions.

**The Goal:** Build a deadlock-free loading mechanism where:
- A central Docker container triggers different loading jobs.
- Node types load directly without contention.
- Edge types safely map dependencies.
- The system handles both Bulk and Continuous Streaming data.
- The entire data model is generic and defined via a YAML schema.

## Reference Materials
During the design phase, the following resources were shared and analyzed:
1. **Mix-and-Batch Technique**: [https://neo4j.com/blog/developer/mix-and-batch-relationship-load/](https://neo4j.com/blog/developer/mix-and-batch-relationship-load/)
2. **Neo4j Cypher Manual**: [https://neo4j.com/docs/cypher-manual/current/introduction/](https://neo4j.com/docs/cypher-manual/current/introduction/)

## Architectural Discussions & Key Decisions

The planned architecture addresses the following distributed-system edge cases. These capabilities are scheduled in later phases; Slice 1 delivers only the local development environment.

### 1. Planned Deadlock Prevention Strategy
The loader will use a **Dual-Layer Deadlock Prevention Engine**:
- **Layer 1 (Thread Isolation):** Edges will be partitioned in Python using directional Mix-and-Batch. Forward edges (`src_id < tgt_id`) and backward edges will be split into distinct slots, preventing circular dependencies such as `A->B` and `B->A` in a batch.
- **Layer 2 (Global Atomic Locking):** Before writing a batch of 1,000 edges, Cypher will aggregate distinct nodes, sort them by internal ID, and lock them with `apoc.lock.nodes()` to eliminate intra-batch circular waits.

### 2. Planned Multi-Loader Synchronization (Global Batch Clock)
When multiple edge types (e.g., `WORKS_AT` and `BOUGHT`) share a common node type (`Person`), running them blindly in parallel causes inter-loader deadlocks.
- **Decision:** A Kafka-based "Global Batch Clock" will let a central orchestrator assign node hash buckets to loaders in rotating time slots.
- **Self-Referencing Edges:** The schema parser will flag relationships such as `Person-KNOWS-Person` to run in isolation.

### 3. Planned Real-Time Streaming & Resilience
- **Edge-Before-Node:** An edge that arrives before its nodes will be detected with `OPTIONAL MATCH` and routed to a **Dead Letter Queue (DLQ)** for exponential-backoff retry.
- **Kafka Eviction Flaw:** To avoid exceeding Kafka's `max.poll.interval.ms` during Neo4j bulk writes, the loader will use a **Dual-Thread Model**: one thread will poll Kafka for heartbeats while a background thread executes Neo4j queries.
- **Rebalance Safety:** An `on_revoke` listener will wait for in-flight Neo4j transactions and explicitly commit offsets before partition hand-off.

## Project Plan
The execution plan has been divided into 6 detailed Agile EPICs, which can be found in the `/plan/` directory:
1. **Phase 1:** Foundation & Infrastructure (Docker, Schema Initializer)
2. **Phase 2:** Node Data Ingestion
3. **Phase 3:** High-Throughput Edge Loading (Core Deadlock Engine)
4. **Phase 4:** Multi-Loader Concurrency (Global Batch Clock)
5. **Phase 5:** Real-Time Streaming & Resilience (DLQ)
6. **Phase 6:** Observability, Metrics & Hardening

## Local Development Environment

Prerequisite: Docker Desktop (or Docker Engine) with Docker Compose v2. Use the platform's standard shell: Bash on Linux/macOS/WSL/Git Bash, or PowerShell on Windows.

On Linux, macOS, WSL, or Git Bash:

```bash
cp .env.example .env
bash scripts/start_dev.sh
```

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
.\scripts\start_dev.ps1
```

The startup command waits for both services to become healthy, then exposes Neo4j Browser at `http://localhost:7474`, Neo4j Bolt at `bolt://localhost:7687`, Kafka at `localhost:9092`, and Kafka UI at `http://localhost:8080`.

To run the environment smoke tests on Bash-based systems:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pytest tests/test_dev_environment.py -v -m smoke
```

On Windows PowerShell:

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pytest tests/test_dev_environment.py -v -m smoke
```

Use `docker compose down -v` to stop the stack and remove its local volumes.

## Node Loader Operations (Phase 2)

With the local stack running, a single configured node topic can be loaded directly:

```bash
python -m src.loader.node_loader --config config/graph_schema.yaml --node-label Person --max-messages 10
```

Set `NEO4J_URI`, `NEO4J_USERNAME`, and `NEO4J_PASSWORD` for Neo4j, plus
`KAFKA_BOOTSTRAP_SERVERS` for Kafka. `NEO4J_USER` remains a legacy fallback.
The command consumes only the selected label's configured topic. `--max-messages`
is useful for bounded bulk runs and tests; omit it for a continuous stream.

Node writes use the `loading` section in `config/graph_schema.yaml`:

- `unwind_batch_size` caps records per Neo4j `UNWIND` transaction.
- `flush_interval_ms` finalizes an idle partial batch.
- `retry_max_attempts`, `retry_base_delay_ms`, and `retry_max_delay_ms` apply
  bounded exponential retry only to Neo4j errors marked retryable by the driver.

The loader commits explicit Kafka *next offsets* only after the whole relevant
Neo4j batch is durable. Each topic-partition has a contiguous-resolution barrier,
so an uncommitted or malformed record cannot let a later offset skip ahead. During
retry it pauses assigned partitions, polls for group heartbeats, and fails closed
if a rebalance revokes unresolved work.

Malformed JSON or schema-invalid records are written with `flush()` and `fsync()`
to the configured `rejection_log_path`, then—and only then—are eligible for their
contiguous offset commit. This is an intentional interim Phase 2 log-and-skip sink,
not a Kafka DLQ. **Phase 5 will replace it with DLQ routing.** The rejection entry
contains topic, partition, offset, node label, and reason; it deliberately omits
the raw payload.

For container fleets, start the pipeline with:

```bash
python -m src.cli start --mode stream --config config/graph_schema.yaml
```

Each node-loader replica receives an isolated JSONL path under `/app/rejections`.
Docker bind-mounts the host `var/rejections/` directory there, so logs survive
container removal. Inspect the host directory when investigating rejected records.

## Relationship Loader Operations (Phase 3 / Slice 1)

Load one configured relationship topic directly with the same durable-offset
guarantees as node ingestion:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
python -m src.loader.edge_loader --config config/graph_schema.yaml \
  --edge-type WORKS_AT --max-messages 10
```

Relationship events use an explicit endpoint envelope; endpoint key names are
declared by the edge configuration:

```json
{
  "source": {"personId": "p-001"},
  "target": {"companyId": "c-001"},
  "properties": {"since": "2020-01-02", "role": "Engineer"}
}
```

The loader merges only the schema-declared labels and relationship type. It
preflights both endpoint nodes inside the same Neo4j transaction, so a missing
endpoint or database failure leaves the Kafka offset uncommitted. Malformed
events are instead written and fsync'd to the rejection log with `edge_type`
before their offsets can advance; Phase 5 will route those events to a DLQ.
Relationship fleet scheduling, control acknowledgements, and parallel edge
lanes are intentionally not part of this direct Slice 1 command.

Bulk CLI runs additionally schedule declared relationship types after the node
fleet has reached its durable drain. Types with disjoint endpoint labels share
a stage; types sharing any label are drained and zero-lag verified in separate
stages. Keep every relationship topic quiescent for the complete run: an input
arrival after a stage captures its boundary fails that stage rather than
claiming a successful load.

### Finite bulk runs

Use bulk mode for a finite, quiescent input set:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
python -m src.cli start --mode bulk --config config/graph_schema.yaml \
  --bulk-timeout-seconds 300
```

The CLI creates a run-scoped fleet and waits until every replica proves its Kafka
assignment (surplus replicas explicitly report that they are idle). It captures a
fixed end-offset boundary, rejects records that arrive after that boundary, waits
for committed offsets to reach it, and then sends `SIGTERM` only to the exact
containers it created. Each loader flushes and acknowledges its drain before the
CLI performs a final zero-lag check. A nonzero result leaves no claim of a
successful bulk load; inspect the run's logs and rejection files before retrying.

Stream mode is intentionally open-ended: it starts the configured fleet and does
not stop it merely because current Kafka lag is zero.

The CLI runs on the host and therefore uses `localhost` endpoints from `.env`.
Node-loader containers use the Docker-network defaults `bolt://neo4j:7687` and
`kafka:29092`; set `LOADER_NEO4J_URI` or
`LOADER_KAFKA_BOOTSTRAP_SERVERS` only when the container network differs.
