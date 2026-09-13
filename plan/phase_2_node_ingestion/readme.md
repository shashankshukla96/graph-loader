# Phase 2 (Epic): Node Data Ingestion

## Business Objective
Enable high-throughput, parallel loading of foundational entities (Nodes) into the graph database. Nodes (like Person, Company, Product) must exist before relationships can be safely mapped between them.

## Value Proposition
- **Independent Scaling:** Node topics have no deadlock risks associated with them (due to unique constraints). We can scale node loaders horizontally.
- **Idempotency:** Data can be re-run safely without creating duplicates.
- **Foundation for Edges:** Sets up the prerequisite data for the more complex edge loading phases.

## Features (High-Level)
1. **Node Kafka Consumers:** Dedicated workers subscribing to node-specific topics.
2. **Batched Neo4j Writes:** Aggregating messages and writing them efficiently into Neo4j to maximize throughput.
3. **Container Orchestration (Phase 1 of CLI):** The CLI script can spawn isolated Docker containers for each Node type defined in the YAML.

## Acceptance Criteria
- [ ] Node loaders successfully consume JSON messages from Kafka topics.
- [ ] The system batches up to `unwind_batch_size` (e.g., 2000) records before flushing to Neo4j.
- [ ] `MERGE` is used based on the node's `key_property` to ensure idempotency.
- [ ] Running the same Kafka topic twice results in no duplicate nodes in Neo4j.
- [ ] Orchestrator detects when lag reaches 0 across all node consumer groups in `bulk` mode.
