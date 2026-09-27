# Implementation Plan: Story 1 — Neo4j Connection & Retries

## Objective
Create the schema initializer entry point with a robust Neo4j connection factory that uses exponential backoff to handle transient database unavailability.

## Files to Modify/Create
| File | Change |
|---|---|
| `requirements.txt` | Add `neo4j>=5.14.1` |
| `src/orchestrator/schema_initializer.py` | Create file with connection factory and error classes |
| `tests/test_schema_initializer.py` | Create file with mocked unit tests |

## Key Logic

**Classes & Functions (`src/orchestrator/schema_initializer.py`):**
```python
import time
import logging
from neo4j import GraphDatabase, Driver
from neo4j.exceptions import ServiceUnavailable, AuthError

logger = logging.getLogger(__name__)

class SchemaInitializationError(Exception):
    """Raised when the schema initializer fails permanently."""
    pass

def get_neo4j_driver(uri: str, user: str, password: str, max_retries: int = 5, base_delay: float = 2.0) -> Driver:
    """
    Creates and verifies a Neo4j driver connection with exponential backoff.
    """
```

**Implementation Details (`get_neo4j_driver`):**
1. Enter a loop `for attempt in range(max_retries + 1):`
2. Inside loop:
   - Instantiate driver: `driver = GraphDatabase.driver(uri, auth=(user, password))`
   - Try to verify connection: `driver.verify_connectivity()`
   - If successful, return `driver`.
   - Except `AuthError` as e: immediately `driver.close()` and `raise SchemaInitializationError(f"Authentication failed: {e}") from e`.
   - Except `ServiceUnavailable` as e:
     - `driver.close()`
     - If `attempt == max_retries`, `raise SchemaInitializationError(f"Failed to connect to Neo4j after {max_retries} retries.") from e`
     - Else, calculate `delay = base_delay * (2 ** attempt)`
     - Log a warning indicating the retry and sleep duration.
     - `time.sleep(delay)`
   - Except `Exception` as e: catch-all for other driver creation errors. `driver.close()` if created, then `raise SchemaInitializationError(f"Unexpected connection error: {e}") from e`.

## Unit Tests (`tests/test_schema_initializer.py`)
Use `unittest.mock.patch` to mock `neo4j.GraphDatabase.driver`.
Tests for `TestNeo4jConnection`:
- `test_connect_success`: `verify_connectivity` succeeds on first try.
- `test_connect_retries_and_succeeds`: `verify_connectivity` raises `ServiceUnavailable` twice, then succeeds. Check `time.sleep` call count and args (mock `time.sleep`).
- `test_connect_exhausts_retries`: `verify_connectivity` always raises `ServiceUnavailable`. Must raise `SchemaInitializationError`.
- `test_connect_auth_error_no_retry`: `verify_connectivity` raises `AuthError`. Must immediately raise `SchemaInitializationError` without calling `time.sleep`.

## Validation Commands
```bash
# Update requirements
.venv/bin/pip install -r requirements.txt

# Run tests
PYTHONPATH=. .venv/bin/pytest tests/test_schema_initializer.py -v --cov=src.orchestrator.schema_initializer --cov-report=term-missing
```

## Rollback
Remove `neo4j` from `requirements.txt`. Delete `src/orchestrator/schema_initializer.py` and `tests/test_schema_initializer.py`. Delete `plan/v1/slice-4/impl_plan_story_1.md`.
