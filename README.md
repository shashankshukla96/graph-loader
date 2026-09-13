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

The startup command waits for both services to become healthy, then exposes Neo4j Browser at `http://localhost:7474`, Neo4j Bolt at `bolt://localhost:7687`, and Kafka at `localhost:9092`.

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
