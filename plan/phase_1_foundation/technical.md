# Phase 1: Technical Details

## Components & Architecture

### 1. Project Scaffolding
- Initialize Python environment with required dependencies: confluent-kafka, 
eo4j, pydantic, docker, pyyaml.
- Set up directory structure (src/, config/, 	ests/, etc.).

### 2. Infrastructure Setup (docker-compose.yaml)
- **Neo4j:** Use Neo4j 5 Enterprise image (required for CONCURRENT TRANSACTIONS optimal performance). Set NEO4J_ACCEPT_LICENSE_AGREEMENT=yes.
- **Kafka:** Use Confluent Community image or Redpanda for a lightweight Kafka-compatible broker.

### 3. Pydantic Models for YAML (src/models/schema.py)
- Define strictly typed models for NodeConfig, EdgeConfig, MixAndBatchConfig, etc.
- Validate required fields (e.g., node keys, topic names).

### 4. Schema Initializer (src/orchestrator/schema_initializer.py)
- Parse the YAML and generate Cypher dynamically.
- Connect via official Neo4j Python Driver.
- Execute constraints sequentially:
  `cypher
  CREATE CONSTRAINT IF NOT EXISTS FOR (n:Label) REQUIRE n.key IS UNIQUE;
  `
- **Definition of Done:** Pipeline successfully boots, parses YAML, and Neo4j reflects the requested constraints/indexes.

### Detailed Implementation: Schema Validation & Initializer
**YAML to Pydantic Example:**
```python
class NodeConfig(BaseModel):
    label: str
    topic: str
    key_property: str
    constraints: List[Dict[str, str]] = []

class GraphSchema(BaseModel):
    nodes: List[NodeConfig]
    edges: List[EdgeConfig]
```
**Schema Initializer Logic:**
The initializer must iterate through `schema.nodes` and construct Cypher queries:
```cypher
// Generated on the fly
CREATE CONSTRAINT person_id_uniq IF NOT EXISTS FOR (n:Person) REQUIRE n.personId IS UNIQUE;
```
*Crucial*: Wait for indexes to become `ONLINE` before starting loaders. Neo4j population can block if indexes are still `POPULATING`.
