# Phase 1 (Epic): Foundation & Infrastructure

## Business Objective
Establish the foundational infrastructure and declarative framework for the Graph Loader. Before we can ingest high-velocity data, we need a reliable environment (Kafka + Neo4j), a standard way to define our graph schema, and automated setup of database constraints to guarantee data integrity.

## Value Proposition
- **Declarative Management:** Data engineers can define the graph model (nodes, edges, properties, indexes) via a simple YAML file without writing code.
- **Data Integrity:** The system will automatically ensure Neo4j constraints (like uniqueness) are present, preventing duplicate node creation during concurrent loads.
- **Infrastructure as Code:** Local development and testing become seamless through containerized infrastructure.

## Features (High-Level)
1. **YAML Schema Registry:** A centralized configuration file (graph_schema.yaml) driving the entire pipeline.
2. **Schema Initializer:** A component that reads the YAML and applies CREATE CONSTRAINT / INDEX IF NOT EXISTS to Neo4j.
3. **Local Dev Environment:** Docker Compose setup for Neo4j (Enterprise) and Kafka (Confluent).
4. **Base CLI:** The entry point (loader.py) skeleton for managing the pipeline.

## Acceptance Criteria
- [ ] A `docker-compose.yaml` is provided that successfully spins up Neo4j 5.x and a Kafka broker.
- [ ] The `graph_schema.yaml` can define nodes, properties, constraints, and relationships.
- [ ] The `schema_initializer.py` connects to Neo4j on startup, parses the YAML, and idempotently creates all declared `UNIQUE` constraints and indexes.
- [ ] The core CLI (`loader.py`) parses arguments (e.g., `--mode bulk`) without crashing.
