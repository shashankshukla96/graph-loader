# Implementation Plan: Story 2 — Edge DDL Generator

## Objective
Extend the Cypher generator to produce idempotent `CREATE INDEX` statements for Edge (relationship) properties. Relationship constraints are ignored as they require Neo4j Enterprise Edition features not specified in this slice.

## Files to Modify
| File | Change |
|---|---|
| `src/utils/cypher_generator.py` | Add `generate_edge_ddl` function |
| `tests/test_cypher_generator.py` | Add `TestEdgeDDLGenerator` class |

## Key Logic

**Function Signature:**
```python
def generate_edge_ddl(schema: GraphSchema) -> list[str]:
    """
    Generate Cypher DDL (indexes) for all edges in the schema.
    """
```

**Implementation Details:**
1. Initialize an empty list `statements = []`.
2. Iterate over `schema.edges`. Let `etype = edge.type`.
3. For each edge, iterate over `properties.items()`. Let `prop_name` and `prop_config` be the key and value.
4. Check if `prop_config.index` is set. (Explicitly skip `prop_config.constraint` processing).
5. If `prop_config.index` is set:
   - Extract type (`range` or `text`).
   - Format name: `{etype.lower()}_{prop_name.lower()}_{type}`
   - Append appropriate Cypher string:
     - `range`: `CREATE RANGE INDEX {name} IF NOT EXISTS FOR ()-[r:{etype}]-() ON (r.{prop_name})`
     - `text`: `CREATE TEXT INDEX {name} IF NOT EXISTS FOR ()-[r:{etype}]-() ON (r.{prop_name})`
6. Return `statements`.

## Unit Tests (`tests/test_cypher_generator.py`)
Add `TestEdgeDDLGenerator` class with tests:
- `test_edge_range_index_cypher`: Validates `RANGE INDEX` format for edges. Since the canonical schema doesn't have an edge index by default, we'll construct a minimal `GraphSchema` inline with an edge index to test it.
- `test_edge_text_index_cypher`: Validates `TEXT INDEX` format for edges.
- `test_edge_constraint_ignored`: Verifies that if an edge property has a constraint (like `since` in `WORKS_AT`), it does NOT produce any DDL in this function.

## Validation Commands
```bash
PYTHONPATH=. .venv/bin/pytest tests/test_cypher_generator.py -v
PYTHONPATH=. .venv/bin/pytest tests/test_cypher_generator.py --cov=src.utils.cypher_generator --cov-report=term-missing
```

## Rollback
Revert `src/utils/cypher_generator.py` and `tests/test_cypher_generator.py` to their Story 1 states. Delete `impl_plan_story_2.md`.
