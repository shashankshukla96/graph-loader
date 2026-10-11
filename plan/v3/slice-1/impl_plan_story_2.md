# Implementation Plan — Phase 3 / Slice 1 / Story 2

## Story

**Write a Generic Idempotent Relationship Transaction**

## Scope and non-goals

This story adds a sequential, schema-derived Neo4j writer for normalized
`EdgeRecord` values. It neither consumes Kafka nor adds concurrency/locking,
retry, or DLQ policy. Those are deferred to Story 3 and later Phase 3 slices.

## Files

| Action | Path | Purpose |
| --- | --- | --- |
| Modify | `src/loader/edge_loader.py` | Add the relationship query builders, missing-endpoint exception, and writer. |
| Modify | `tests/test_edge_loader.py` | Add mock-driver transaction, endpoint preflight, error, and schema-generic writer tests. |

## New API

```python
class MissingRelationshipEndpointError(RuntimeError):
    """Raised when a record cannot resolve both configured endpoint nodes."""

def build_edge_upsert_query(edge_config: EdgeConfig) -> str:
    """Return validated-schema Cypher for idempotent relationship upserts."""

class EdgeWriter:
    def __init__(self, driver: Driver, edge_config: EdgeConfig) -> None: ...
    def write(self, record: EdgeRecord) -> None: ...
    def write_batch(self, records: Sequence[EdgeRecord]) -> None: ...
```

`build_edge_upsert_query` will interpolate only the schema identifiers already
validated by Pydantic and will pass all event values as `$rows` parameters:

```cypher
UNWIND $rows AS row
MATCH (s:`<source-label>` {`<source-key>`: row.source_key})
MATCH (t:`<target-label>` {`<target-key>`: row.target_key})
MERGE (s)-[r:`<relationship-type>`]->(t)
SET r += row.properties
```

## Endpoint safety within one transaction

The merge query alone would silently drop rows with a missing endpoint. The
writer will use a private schema-derived preflight query in the *same explicit
transaction* before the merge:

```cypher
UNWIND $rows AS row
OPTIONAL MATCH (s:`<source-label>` {`<source-key>`: row.source_key})
WITH row, count(s) AS source_matches
OPTIONAL MATCH (t:`<target-label>` {`<target-key>`: row.target_key})
RETURN row.source_key AS source_key,
       row.target_key AS target_key,
       source_matches,
       count(t) AS target_matches
```

Algorithm:

1. Return immediately for an empty batch.
2. Materialize each record as `{source_key, target_key, properties}`.
3. Open one context-managed session and explicit transaction.
4. Run the endpoint preflight; consume every returned row, then `consume()` the
   result. Any row whose source or target count is not exactly one raises
   `MissingRelationshipEndpointError` with edge type and endpoint presence
   (never record values). No merge is run and the context exits uncommitted.
5. Run the upsert query, call `consume()` to surface server errors, then call
   `commit()` exactly once.
6. Let driver/transaction exceptions propagate. The writer never closes the
   caller-owned driver.

Both relationship declarations are exercised without type-specific branches:
`WORKS_AT` (`Person.personId` → `Company.companyId`) and self-referencing
`KNOWS` (`Person.personId` → `Person.personId`). This slice is intentionally
single-transaction/sequential—APOC locks, ordering, and parallel lanes remain
out of scope.

## Tests

- exact query identifiers for `WORKS_AT` and `KNOWS`; record values never appear
  in query text;
- singleton `write()` delegates to one-row `write_batch()`;
- valid batch runs preflight then merge in one session/transaction, consumes
  both results, commits only after merge consumption, and closes the session;
- replay uses `MERGE` (not `CREATE`) and supports two configurations without
  branching;
- missing source, missing target, and duplicate endpoint match count each raise
  `MissingRelationshipEndpointError`, issue no merge, and never commit;
- preflight/merge execution failure propagates and never commits;
- empty batch makes no session/transaction;
- writer does not close the injected driver.

Run:

```bash
.venv/bin/python -m pytest tests/test_edge_loader.py tests/test_schema_validation.py -v
```

## Rollback / cleanup

The writer creates no resources beyond caller-owned Neo4j sessions. A failed
preflight or query exits without `commit()`, so the driver rolls back the
uncommitted transaction. No Kafka offsets exist in this story.
