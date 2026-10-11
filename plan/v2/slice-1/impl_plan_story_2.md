# Implementation Plan — Story 2: Write Generic Idempotent Nodes to Neo4j

## Scope and Boundaries

Implement only Story 2 from `plan/v2/stories_slice_1.md`. Add a generic Neo4j writer for a `NodeRecord` already validated by Story 1. It uses a singleton `$batch` through the Phase 2 `UNWIND` pattern and has PATCH semantics for omitted optional properties. It does not create a Kafka consumer, commit offsets, batch multiple Kafka records, retry writes, route to a DLQ, or modify `src/cli.py`.

## Files

### Create

- `tests/conftest.py` — isolated, session-scoped Neo4j Testcontainers fixture and deterministic graph cleanup for this slice's integration tests.
- `tests/test_node_loader_integration.py` — Neo4j integration tests for Person and Company upserts.

### Modify

- `src/loader/node_loader.py` — add query construction and `NodeWriter` after the existing pure normalization API.
- `tests/test_node_loader.py` — add query-shape and mocked driver/session lifecycle tests.

## Design

### 1. Generic Cypher builder

Add this public function to `src/loader/node_loader.py`:

```python
def build_node_upsert_query(node_config: NodeConfig) -> str:
    """Return schema-derived, parameterized UNWIND/MERGE Cypher for one node label."""
```

Use the Phase 2 technical plan's pattern verbatim as the basis:

```python
query = """
UNWIND $batch AS record
MERGE (n:Person {personId: record.personId})
SET n += record.properties
"""
session.execute_write(lambda tx: tx.run(query, batch=batch))
```

Its generic output will interpolate only Story 1-validated schema identifiers, quoting them with Cypher backticks:

```cypher
UNWIND $batch AS record
MERGE (n:`<label>` {`<key_property>`: record.key})
SET n += record.properties
```

The query will have no payload-derived string interpolation and accepts exactly one parameter, `$batch`.

### 2. Node writer

Add:

```python
class NodeWriter:
    """Write normalized records for one ``NodeConfig`` through a Neo4j driver.

    The caller owns and closes the injected driver; ``write`` owns only its
    session.
    """

    def __init__(self, driver: Driver, node_config: NodeConfig) -> None: ...

    def write(self, record: NodeRecord) -> None:
        """Execute one idempotent singleton-batch node upsert."""
```

- Cache the schema-derived query in `__init__`.
- `write()` opens `with self._driver.session() as session:` and invokes `session.execute_write(...)`; the transaction lambda calls `tx.run(self._query, batch=[{"key": record.key, "properties": record.properties}])`.
- Do not catch or translate Neo4j exceptions: the upcoming Kafka runner must decide whether to commit, stop, or retry.
- Do not mutate `record.properties` or close the injected driver.
- PATCH contract: because Story 1 omits optional nulls, a replay without an optional property leaves its prior Neo4j value untouched through `SET n += record.properties`.

### 3. Integration fixture

In `tests/conftest.py`, create a session-scoped `neo4j_driver` fixture using `testcontainers.neo4j.Neo4jContainer` with a Neo4j 5.x image. It must:

- expose an authenticated driver usable by `NodeWriter`;
- yield the driver and close it in `finally`;
- remove all nodes and relationships both before and after each integration test (a function-scoped cleanup fixture may depend on the session driver);
- avoid APOC and the application’s live Docker Compose state;
- skip only when Docker/Testcontainers cannot start, with an actionable reason.

`tests/test_node_loader_integration.py` will use the canonical schema through `load_schema()` and apply it through the existing `apply_schema(driver, schema)` path before writes, exercising the real schema-initialization contract rather than hand-building only a subset of DDL. Each integration test explicitly requests the cleanup fixture, which deletes only test-container graph data before and after that test; it must not be `autouse`, so unit-only test runs never start Docker.

## Tests

### `tests/test_node_loader.py`

Add unit tests that prove:

- `build_node_upsert_query()` produces distinct Person and Company labels/keys and the expected `UNWIND`, backticked `MERGE`, and `SET n += record.properties` clauses.
- A malicious node identifier cannot reach query construction because model validation rejects it (covered by Story 1's schema tests; this test documents the writer boundary using a valid config only).
- `NodeWriter.write()` supplies a singleton `batch` with `key` and `properties`, calls `execute_write`, closes its session, and does not close the driver.
- A transaction exception propagates and the session still closes.

### `tests/test_node_loader_integration.py`

Add integration coverage for:

- Person: first write creates the node; second write with the same `personId` updates a supplied value without creating a second node.
- Company: the same writer API works with `companyId` and produces exactly one Company node.
- PATCH semantics: write Person with optional `age`, then replay the same key without `age`; Neo4j retains the initial age.

Mark each test in this module with `@pytest.mark.integration`. The cleanup and Neo4j Testcontainers fixtures are requested only by these marked tests.

Run:

```bash
.venv/bin/python -m pytest tests/test_node_loader.py -v
.venv/bin/python -m pytest tests/test_node_loader_integration.py -v -m integration
```

## Rollback and Cleanup

- The Testcontainers fixture always stops its container and closes its driver; test cleanup deletes only test-container graph data.
- If a testcontainer is unavailable, unit tests remain mandatory and the integration test reports a clear skip rather than silently targeting the developer’s Compose database.
- If implementation is abandoned, remove only Story 2's writer/query/fixture/integration-test additions and retain Story 1's validated record contract. Do not reset unrelated working-tree changes.
