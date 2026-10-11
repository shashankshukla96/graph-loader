# Implementation Plan: Story 3 — Integration Test with Real Neo4j Container

## Objective
Write a live integration test that applies the canonical `graph_schema.yaml` to the local Neo4j dev container using the initializer. This ensures `apply_schema` functions correctly against the actual Neo4j database engine.

## Files to Modify
| File | Change |
|---|---|
| `tests/test_schema_initializer_int.py` | Create integration test file |

## Key Logic

**Integration Test (`tests/test_schema_initializer_int.py`):**
1. Get connection settings from environment or default to localhost dev settings (`bolt://localhost:7687`, `neo4j/password`).
2. Attempt to get a driver via `get_neo4j_driver(max_retries=1)`. If this fails (e.g., container not running), `pytest.skip()` the test instead of failing it.
3. Load `config/graph_schema.yaml` via `load_schema()`.
4. Run a setup/cleanup phase: use `generate_all_ddl(schema)` to parse out the generated names and execute `DROP CONSTRAINT <name> IF EXISTS` and `DROP INDEX <name> IF EXISTS` to guarantee a clean slate.
5. Invoke `apply_schema(driver, schema, timeout_seconds=30)`. This should block until `ONLINE`.
6. Assert correctness by running `SHOW CONSTRAINTS YIELD name` and `SHOW INDEXES YIELD name`. Verify that specific constraints (like `person_personid_unique`) and indexes (`person_age_range`) are present.
7. Close driver.

## Validation Commands
```bash
# Run the integration test against live Neo4j
PYTHONPATH=. .venv/bin/pytest tests/test_schema_initializer_int.py -v
```

## Rollback
Delete `tests/test_schema_initializer_int.py` and `plan/v1/slice-4/impl_plan_story_3.md`.
