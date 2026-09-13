# Phase 6: Technical Details

## Components & Architecture

### 1. Prometheus Metrics (src/utils/metrics.py)
- Use prometheus_client Python package.
- Spin up an HTTP server on port 9090 inside each container.
- Metrics to track:
  - graph_loader_records_consumed_total
  - graph_loader_neo4j_write_duration_seconds
  - graph_loader_dlq_routed_total
  - graph_loader_deadlocks_recovered_total

### 2. Integration & Stress Testing (	ests/test_integration.py)
- pytest with 	estcontainers-python (Kafka + Neo4j).
- **Test Case 1:** Star topology deadlock generation. Verify zero failures with Mix & Batch enabled.
- **Test Case 2:** Edge-before-node simulation. Emit edge, verify DLQ, emit node, verify edge loads successfully upon DLQ retry.

### 3. Graceful Shutdown
- Catch SIGTERM/SIGINT.
- Ensure Kafka consumers flush offsets and Neo4j driver connection pools close cleanly.
- Ensure Orchestrator halts container spin-ups and shuts down active workers.

### 4. CI/CD (Optional)
- Add GitHub Actions or GitLab CI yaml to build the Docker image, run pytest, and lint using lake8 / lack.

### Detailed Implementation: Metrics & Hardening
**Prometheus Instrumentation (`prometheus_client`):**
```python
from prometheus_client import Counter, Histogram, start_http_server

RECORDS_PROCESSED = Counter('graph_loader_records_processed', 'Records written to Neo4j', ['loader_name', 'entity_type'])
WRITE_DURATION = Histogram('graph_loader_write_duration_seconds', 'Time spent executing Neo4j transaction')

# Start server in main thread
start_http_server(9090)
```

**Testing Strategy (`testcontainers-python`):**
Use `Neo4jContainer` and `KafkaContainer` in `pytest` fixtures to spin up isolated environments for CI.
Generate a star topology dataset programmatically (e.g., 100 threads trying to connect to a single `Company` node). Run the Mix-and-Batch edge loader and assert that no `DeadlockDetectedException` leaks to the top level, and the final relationship count matches the input exactly.
