# Implementation Plan: Story 5 - Automated Environment Smoke Test

## Files to Create/Modify
- `J:\Graph Loader\tests\test_dev_environment.py` (Create)
- `J:\Graph Loader\requirements.txt` (Create)
- `J:\Graph Loader\pyproject.toml` (Create)

## New Classes and Methods
- `TestNeo4jConnectivity` class with `test_neo4j_is_reachable` and `test_apoc_is_installed`
- `TestKafkaConnectivity` class with `test_kafka_is_reachable` and `test_kafka_can_create_and_delete_topic`

## Key Logic / Implementation Details
- **`tests/test_dev_environment.py`**:
  - Contains pytest functions marked with `@pytest.mark.smoke`.
  - Load `.env` from the repository root derived from `Path(__file__).resolve()`, never from the caller's working directory and never by executing its contents. The parser ignores blank lines and full-line comments; accepts `KEY=value`; removes matching single or double quotes around a value; strips an inline comment only when `#` is preceded by whitespace (so quoted or unspaced `#` values are preserved); and raises a clear error for missing `=`, an invalid key, or an unclosed quote. Explicitly supplied process environment values take precedence over `.env`, which in turn takes precedence over the documented defaults. This ensures a copied-and-customized `.env` works when pytest is invoked directly.
  - Connect to Neo4j using the configured Bolt URI, username, password, and database; use driver context managers and turn connection/APOC errors into clear failures that instruct the developer to start the stack.
  - Asserts that APOC is installed by executing `RETURN apoc.version()`.
  - Connects to Kafka via `AdminClient`, checks metadata, and turns Kafka connection failures into actionable test failures.
  - Verifies topic creation with a UUID-suffixed smoke-test topic. Always delete the topic in a `finally` block, require the delete future to resolve within 10 seconds, then poll metadata for at most 10 seconds for the topic to disappear; surface an actionable cleanup failure if it remains.
- **`requirements.txt`**:
  - Adds test dependencies: `pytest`, `testcontainers[neo4j,kafka]`, `neo4j`, `confluent-kafka`.
- **`pyproject.toml`**:
  - Registers the `smoke` marker to avoid pytest warnings.

## Tests to Write
- The entire task is writing a four-test suite for the local environment setup: Neo4j Bolt reachability, APOC availability, Kafka metadata reachability, and topic create/verify/delete.
- Add one focused non-smoke unit check for the settings helper using `monkeypatch`: an explicit process `NEO4J_URI` must override the otherwise supplied dotenv value. It does not change the required four-test `-m smoke` result.
- Install the declared requirements into the ignored local virtual environment, run `python -m pytest tests/test_dev_environment.py -v -m smoke`, and require all four tests to pass against the stack started by Story 4.
- Confirm `pyproject.toml` registers the `smoke` marker and verify no smoke-test topic remains after the run.

## Rollback / Cleanup
- Delete the created files if the implementation fails.
