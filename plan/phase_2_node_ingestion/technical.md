# Phase 2: Technical Details

## Components & Architecture

### 1. Node Loader Component (src/loader/node_loader.py)
- Uses confluent-kafka Consumer to read from topics.
- Buffers messages up to unwind_batch_size (e.g., 2000-5000).
- Uses UNWIND Cypher block for writes:
  `cypher
  UNWIND $rows AS row
  MERGE (n:Label {key: row.id})
  SET n += row.props
  `
- Commits Kafka offsets only after successful Neo4j transaction.

### 2. Docker SDK Integration (src/orchestrator/container_manager.py)
- Use the docker Python package.
- loader.py start --mode=bulk maps to starting container instances of 
ode_loader.py.
- Pass environment variables: LOADER_ROLE=node, ENTITY_LABEL=Person, KAFKA_TOPIC=graph.nodes.person.

### 3. Bulk Mode Synchronization
- CLI monitors consumer group lag via Kafka Admin Client.
- The bulk phase is considered "Node Complete" when all node consumer groups report 0 lag.

### Constraints & Considerations
- Retries on Neo4j transient errors are implemented here.
- Deadlocks are rare on node inserts but can happen on index locks under extreme concurrency; retry logic (Exponential backoff) will handle this gracefully.

### Detailed Implementation: Consumer Loop & Batching
**Kafka Consumer Configuration:**
- `enable.auto.commit`: `False` (We strictly commit offsets only *after* Neo4j successfully writes the batch).
- `auto.offset.reset`: `earliest`.

**Neo4j Connection Pool:**
Ensure the Neo4j driver is configured for high throughput:
`driver = GraphDatabase.driver(uri, auth=(user, pwd), max_connection_pool_size=50)`

**Write Optimization (UNWIND):**
```python
query = """
UNWIND $batch AS record
MERGE (n:Person {personId: record.personId})
SET n += record.properties
"""
session.execute_write(lambda tx: tx.run(query, batch=batch))
```
