# Implementation Plan: Story 2 — Wiring 'Start' to Schema Initializer

## Objective
Update the `start` command to read `.env` configuration, connect to the Neo4j database, and apply the schema constraints via the previously built Schema Initializer before starting the ingestion process (which is currently a stub).

## Files to Modify
| File | Change |
|---|---|
| `src/cli.py` | Add logic to `handle_start` |
| `tests/test_cli.py` | Update and expand tests for `handle_start` |

## Key Logic

**Code Changes (`src/cli.py`):**
1. Add imports:
   ```python
   import os
   from src.utils.schema_loader import load_schema
   from src.orchestrator.schema_initializer import (
       get_neo4j_driver,
       apply_schema,
       SchemaInitializationError
   )
   ```
2. Rewrite `handle_start`:
   ```python
   def handle_start(args: argparse.Namespace) -> int:
       logger.info(f"Starting pipeline in {args.mode} mode with config {args.config}")
       
       # 1. Load schema
       try:
           schema = load_schema(args.config)
       except Exception as e:
           logger.error(f"Failed to load schema: {e}")
           return 1
           
       # 2. Get DB config
       uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
       user = os.environ.get("NEO4J_USER", "neo4j")
       password = os.environ.get("NEO4J_PASSWORD", "changeme")
       
       # 3. Apply schema
       try:
           driver = get_neo4j_driver(uri, user, password)
           try:
               apply_schema(driver, schema)
           finally:
               driver.close()
           logger.info("Schema initialization complete. Ready for ingestion.")
       except SchemaInitializationError as e:
           logger.error(f"Schema initialization failed: {e}")
           return 1
           
       return 0
   ```

## Unit Tests (`tests/test_cli.py`)
Replace `test_handle_start_stub` with three new tests:
- `test_handle_start_success`: Mock `load_schema`, `get_neo4j_driver`, and `apply_schema`. Assert return code is `0` and `driver.close()` is called.
- `test_handle_start_schema_load_failure`: Mock `load_schema` to raise an `Exception`. Assert return code is `1` and no driver logic is executed.
- `test_handle_start_init_failure`: Mock `apply_schema` (or `get_neo4j_driver`) to raise `SchemaInitializationError`. Assert return code is `1` and `logger.error` catches the message gracefully instead of dumping a stack trace.

## Validation Commands
```bash
PYTHONPATH=. .venv/bin/pytest tests/test_cli.py -v --cov=src.cli --cov-report=term-missing
```

## Rollback
Delete `plan/v1/slice-5/impl_plan_story_2.md`. Revert `src/cli.py` and `tests/test_cli.py` to their Story 1 states.
