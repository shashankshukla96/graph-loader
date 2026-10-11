# Implementation Plan: Story 2 — DDL Execution & ONLINE Polling

## Objective
Implement `apply_schema` to run the Cypher statements generated from the schema and wait for Neo4j to confirm that all indexes and constraints are fully `ONLINE`.

## Files to Modify
| File | Change |
|---|---|
| `src/orchestrator/schema_initializer.py` | Add `apply_schema` function |
| `tests/test_schema_initializer.py` | Add `TestSchemaApplication` unit tests |

## Key Logic

**Function Signature:**
```python
from src.models.schema import GraphSchema
from src.utils.cypher_generator import generate_all_ddl

def apply_schema(driver: Driver, schema: GraphSchema, timeout_seconds: int = 120) -> None:
    """
    Applies the schema DDL to Neo4j and waits for all indexes to become ONLINE.
    """
```

**Implementation Details (`apply_schema`):**
1. Get the statements: `statements = generate_all_ddl(schema)`
2. Execute all statements using a Neo4j session:
   - `with driver.session() as session: for stmt in statements: session.run(stmt)`
3. Enter polling loop: `start_time = time.time(); while True:`
4. Inside loop, open a new session for reading states:
   - Query FAILED: `SHOW INDEXES YIELD name, state WHERE state = 'FAILED' RETURN name`
     - If results exist, `raise SchemaInitializationError(f"Index creation failed for: {failed}")`
   - Query POPULATING: `SHOW INDEXES YIELD name, state WHERE state = 'POPULATING' RETURN name`
     - If NO results exist, `logger.info("All constraints and indexes are ONLINE.")` and `return`
5. Timeout check:
   - If `time.time() - start_time > timeout_seconds`, `raise SchemaInitializationError(...)`
6. Sleep:
   - `logger.info(f"Waiting for indexes to populate: {populating}")`
   - `time.sleep(5)`

## Unit Tests (`tests/test_schema_initializer.py`)
Add `TestSchemaApplication` class mocking `driver.session()` and `generate_all_ddl`:
- `test_apply_schema_success`: Mock returns no `FAILED` and no `POPULATING` immediately. Asserts `session.run()` was called with DDL.
- `test_apply_schema_waits_and_succeeds`: Mock returns `POPULATING` on first call, empty on second. Asserts `time.sleep(5)` was called once.
- `test_apply_schema_index_failed`: Mock returns a `FAILED` index. Asserts `SchemaInitializationError` is raised immediately.
- `test_apply_schema_timeout`: Mock always returns `POPULATING`. Provide a fast timeout (`timeout_seconds=0.1`) and mock `time.sleep`. Asserts `SchemaInitializationError` due to timeout.

## Validation Commands
```bash
PYTHONPATH=. .venv/bin/pytest tests/test_schema_initializer.py -v
PYTHONPATH=. .venv/bin/pytest tests/test_schema_initializer.py --cov=src.orchestrator.schema_initializer --cov-report=term-missing
```

## Rollback
Remove `apply_schema` and `TestSchemaApplication`. Delete `plan/v1/slice-4/impl_plan_story_2.md`.
