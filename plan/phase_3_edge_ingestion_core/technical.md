# Phase 3: Technical Details

## Components & Architecture (Python, Neo4j, Kafka)

### 1. The Ultimate Deadlock Prevention Engine
To guarantee zero deadlocks, we layer two mathematical principles:
- **Mix-and-Batch Grid (Thread Isolation):** Python partitions edges so that parallel worker threads never touch the same nodes concurrently.
- **Global Atomic Batch Locking (Intra-Thread):** Even within a single thread, we force Neo4j to acquire locks for all nodes in the batch in a strict global sequence (lowest internal ID first) using APOC, before any `MERGE` happens.

### 2. Neo4j Writer (`src/loader/neo4j_writer.py`)
- We use a standard `UNWIND` transaction instead of `CALL IN CONCURRENT TRANSACTIONS`. Python already handles thread-level parallelism.
```cypher
UNWIND $rows AS row
MATCH (s:Src {id: row.s_id}), (t:Tgt {id: row.t_id})
WITH collect({s: s, t: t, props: row.props}) AS rels, collect(s) + collect(t) AS all_nodes
CALL {
    WITH all_nodes
    UNWIND all_nodes AS n
    WITH DISTINCT n AS dist_n
    ORDER BY id(dist_n)
    RETURN collect(dist_n) AS sorted_nodes
}
CALL apoc.lock.nodes(sorted_nodes)
UNWIND rels AS rel
WITH rel.s AS s, rel.t AS t, rel.props AS props
MERGE (s)-[r:REL]->(t)
SET r += props
```

### 3. Dependency Manager (`src/orchestrator/dependency_manager.py`)
- Parses YAML to build the Conflict Graph.
- Conflicting Edge loaders run sequentially via Docker orchestration.
