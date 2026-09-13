# Phase 4: Technical Details

## Components & Architecture (Python, Kafka, Docker)

### 1. Dual-Thread Consumer Architecture
- **The Kafka Eviction Flaw:** If Neo4j takes 6 minutes to process a massive batch, a single-threaded Python app cannot call `poll()`, causing Kafka to evict the consumer (exceeding `max.poll.interval.ms`).
- **The Fix:**
  - **Main Thread:** Continuously calls `kafka_consumer.poll()`, maintains heartbeats, handles coordination messages, and fills a ThreadSafe Queue.
  - **Worker Thread:** Pulls from the queue and executes the blocking Neo4j `session.run()` queries.

### 2. Coordination Service (`src/orchestrator/coordination.py`)
- Uses a dedicated Kafka topic: `graph.loader.coordination`.
- Implements slot timeouts (e.g., 60 seconds) and Docker SDK heartbeat monitoring to prevent clock stalls if a container hangs.

### 3. Self-Referencing Handling
- Safely handled by Phase 3's directional logic. `Person-KNOWS-Person` edges where `id1 == id2` naturally fall into the Backward Pass slots, guaranteeing safe acyclic processing.
