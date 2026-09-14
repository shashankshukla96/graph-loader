# Implementation Plan: Story 3 — Combined Generator & Test Suite

## Objective
Create a single entry point `generate_all_ddl(schema)` that aggregates the outputs from the node and edge DDL generators. 

## Files to Modify
| File | Change |
|---|---|
| `src/utils/cypher_generator.py` | Add `generate_all_ddl` function |
| `tests/test_cypher_generator.py` | Add tests for `generate_all_ddl` and ensure overall test coverage is >80% |

## Key Logic

**Function Signature:**
```python
def generate_all_ddl(schema: GraphSchema) -> list[str]:
    """
    Generate all Cypher DDL (constraints and indexes) for the entire schema.
    """
```

**Implementation Details:**
1. Call `generate_node_ddl(schema)` and store as `node_statements`.
2. Call `generate_edge_ddl(schema)` and store as `edge_statements`.
3. Return `node_statements + edge_statements`.

## Unit Tests (`tests/test_cypher_generator.py`)
Add `test_generate_all_ddl` which will:
- Use the `canonical_schema` fixture.
- Verify that the output of `generate_all_ddl` is exactly the concatenation of the outputs from `generate_node_ddl` and `generate_edge_ddl`.
- Confirm that the total count of generated statements equals the sum of node and edge statements.

## Validation Commands
```bash
PYTHONPATH=. .venv/bin/pytest tests/test_cypher_generator.py -v
PYTHONPATH=. .venv/bin/pytest tests/test_cypher_generator.py --cov=src.utils.cypher_generator --cov-report=term-missing
```

## Rollback
Revert `src/utils/cypher_generator.py` and `tests/test_cypher_generator.py` to their Story 2 states. Delete `impl_plan_story_3.md`.
