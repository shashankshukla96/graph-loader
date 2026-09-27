# Implementation Plan: Story 1 — Node DDL Generator Core

## Objective
Implement a pure Python function that takes a `GraphSchema` object and generates a list of idempotent Cypher DDL strings (`CREATE CONSTRAINT` and `CREATE INDEX`) for all node properties.

## Files to Create
| File | Change |
|---|---|
| `src/utils/cypher_generator.py` | Create file with `generate_node_ddl` function |
| `tests/test_cypher_generator.py` | Unit tests for node DDL generation |

## Key Logic

**Function Signature:**
```python
from src.models.schema import GraphSchema

def generate_node_ddl(schema: GraphSchema) -> list[str]:
    """
    Generate Cypher DDL (constraints and indexes) for all nodes in the schema.
    """
```

**Implementation Details:**
1. Initialize an empty list `statements = []`.
2. Iterate over `schema.nodes`.
3. For each node, iterate over `properties.items()`. Let `prop_name` and `prop_config` be the key and value.
4. If `prop_config.constraint` is set:
   - Extract type (`uniqueness` or `not_null`).
   - Map suffix: `"unique"` if type is `"uniqueness"` else `"not_null"`.
   - Format name: `{node.label.lower()}_{prop_name.lower()}_{suffix}`
   - Append appropriate Cypher string:
     - `uniqueness`: `CREATE CONSTRAINT {name} IF NOT EXISTS FOR (n:{node.label}) REQUIRE n.{prop_name} IS UNIQUE`
     - `not_null`: `CREATE CONSTRAINT {name} IF NOT EXISTS FOR (n:{node.label}) REQUIRE n.{prop_name} IS NOT NULL`
5. If `prop_config.index` is set:
   - Extract type (`range` or `text`).
   - Suffix is directly the type (`"range"` or `"text"`).
   - Format name: `{node.label.lower()}_{prop_name.lower()}_{type}`
   - Append appropriate Cypher string:
     - `range`: `CREATE RANGE INDEX {name} IF NOT EXISTS FOR (n:{node.label}) ON (n.{prop_name})`
     - `text`: `CREATE TEXT INDEX {name} IF NOT EXISTS FOR (n:{node.label}) ON (n.{prop_name})`
6. Return `statements`.

## Unit Tests (`tests/test_cypher_generator.py`)
Create `TestNodeDDLGenerator` class with tests:
- `test_uniqueness_constraint_cypher`: Verifies `IS UNIQUE` string and naming (`_unique`).
- `test_not_null_constraint_cypher`: Verifies `IS NOT NULL` string and naming (`_not_null`).
- `test_range_index_cypher`: Verifies `RANGE INDEX` string and naming (`_range`).
- `test_text_index_cypher`: Verifies `TEXT INDEX` string and naming (`_text`).
- `test_idempotency_and_lowercase`: Verifies `IF NOT EXISTS` is in all generated statements and names are strictly lowercase.

Use the canonical `config/graph_schema.yaml` via `load_schema()` to test output logic.

## Validation Commands
```bash
# Run tests
PYTHONPATH=. .venv/bin/pytest tests/test_cypher_generator.py -v

# Check coverage
PYTHONPATH=. .venv/bin/pytest tests/test_cypher_generator.py --cov=src.utils.cypher_generator --cov-report=term-missing
```

## Rollback
Delete `src/utils/cypher_generator.py`, `tests/test_cypher_generator.py`, and `plan/v1/slice-3/impl_plan_story_1.md`.
