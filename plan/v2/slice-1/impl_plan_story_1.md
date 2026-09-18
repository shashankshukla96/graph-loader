# Implementation Plan — Story 1: Define the Generic Node Record Contract

## Scope and Boundaries

Implement only Story 1 from `plan/v2/stories_slice_1.md`. This establishes safe schema identifiers and converts a top-level JSON-equivalent mapping into a validated `NodeRecord`. It does not open Kafka consumers, connect to Neo4j, generate write Cypher, commit offsets, batch records, retry failures, or publish a DLQ message.

## Files

### Create

- `src/loader/__init__.py` — package marker with a short package docstring.
- `src/loader/node_loader.py` — `NodeRecord`, `NodeRecordValidationError`, and record normalization helpers only.
- `tests/test_node_loader.py` — unit tests for normalizing node records.

### Modify

- `src/models/schema.py` — validate Cypher identifiers throughout the existing schema and require node keys to be declared required.
- `tests/test_schema_validation.py` — schema validation coverage for invalid node/edge identifiers and optional node keys.

## Design

### 1. Safe schema identifiers

In `src/models/schema.py`:

```python
CYPHER_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def _validate_cypher_identifier(value: str, field_name: str) -> str:
    """Return a safe Cypher identifier or raise ValueError with field context."""
```

- Import `re` and Pydantic `field_validator`.
- Add field validators for `NodeConfig.label`, `NodeConfig.key_property`, `NodeConfig.properties` mapping keys, `EdgeConfig.type`, `EdgeConfig.properties` mapping keys, and `SourceTargetConfig.source`/`.target`. Endpoint names must be safe independently of whether a matching `NodeConfig` label currently exists, because later edge Cypher will interpolate them.
- Retain `NodeConfig.at_least_one_property()` before `key_property_must_exist()`.
- Extend `key_property_must_exist()` to reject a key property whose `PropertyConfig.required` is false, with an error mentioning `key_property` and `required`.
- Preserve the current Pydantic model API and all valid canonical YAML.

### 2. Node record API and normalization

In `src/loader/node_loader.py`:

```python
@dataclass(frozen=True)
class NodeRecord:
    """A validated Neo4j-ready record for one schema-declared node."""
    key: object
    properties: dict[str, object]

class NodeRecordValidationError(ValueError):
    """Raised when a raw node record violates its NodeConfig contract."""

def normalize_node_record(
    raw_record: Mapping[str, object], node_config: NodeConfig
) -> NodeRecord:
    """Validate a top-level record and normalize it to Neo4j-supported values."""
```

Helpers will be private (`_normalize_property_value` and `_raise_record_error`) and receive the label/property name needed for concise errors. No exception may include `repr(raw_record)` or raw payload values.

Algorithm:

1. Reject keys in `raw_record` that are not in `node_config.properties`.
2. Iterate every declared property in configuration order.
3. If it is absent or `None`:
   - raise `NodeRecordValidationError` when it is the key or is required;
   - otherwise omit it from the normalized `properties` mapping.
4. For a present non-null value, normalize by `PropertyConfig.type`:
   - `string`: require `str`.
   - `integer`: require `int` and reject `bool`.
   - `float`: accept `int` or `float`, reject `bool`, convert inside `try/except (OverflowError, ValueError)`, and use `math.isfinite()` to reject overflow, `NaN`, `Infinity`, and `-Infinity` as `NodeRecordValidationError`.
   - `date`: require `str`; use `date.fromisoformat()` and translate `ValueError` to `NodeRecordValidationError`.
   - `datetime`: require `str`; require an ISO/RFC3339 `T` separator between date and time before parsing, convert only a terminal `Z` to `+00:00`, parse with `datetime.fromisoformat()`, and require a non-`None` UTC offset (`tzinfo` and `utcoffset()`).
5. Ensure the normalized mapping contains the configured key property and return `NodeRecord(key=properties[key_property], properties=properties)`.

The optional-null behavior is intentionally PATCH-compatible for Story 2: omitted values are absent from `NodeRecord.properties`, so a later `SET n += record.properties` will preserve an existing Neo4j property.

## Tests

### `tests/test_schema_validation.py`

Add focused tests that prove:

- Invalid node label, node key name, node property name, edge type, edge property name, source label, and target label each raise `ValidationError` mentioning the affected field.
- YAML-backed parameterized cases for every unsafe identifier class call `load_schema()` and assert `SchemaLoadError` with a chained `ValidationError`; retain focused direct model tests where they improve diagnostic coverage.
- A declared key with `required: false` raises `ValidationError` mentioning the key property requirement.
- Existing valid Person/Company/edge fixtures and canonical YAML still validate.

### `tests/test_node_loader.py`

Use `load_schema()` for canonical Person and Company configs where appropriate. Cover:

- A fully populated Person record, including a date and offset-aware datetime (`Z`), produces native `date` and aware `datetime` values.
- A Company record validates through the same function with `companyId` as its key.
- Optional `None` is omitted while a configured required/key field being `None` or absent fails.
- Unknown fields fail without echoing payload contents.
- String/integer/float type boundaries reject `bool`; floats reject non-finite values and an oversized integer that overflows float conversion.
- Invalid dates, invalid datetimes, datetimes with a non-`T` separator, and naïve datetimes fail.
- The returned `NodeRecord.key` equals the configured key property and that property remains in `NodeRecord.properties`.

Run:

```bash
python -m pytest tests/test_schema_validation.py tests/test_node_loader.py -v
```

## Rollback and Cleanup

- This story has no external services or persistent state.
- If validation behavior breaks an existing canonical schema or test fixture, correct the smallest in-scope validator/test mismatch; do not weaken identifier validation or alter unrelated schema semantics.
- If implementation must be abandoned, remove only the new `src/loader/` package and `tests/test_node_loader.py`, then revert the Story 1 changes to `src/models/schema.py` and `tests/test_schema_validation.py` through the normal code-review workflow. Do not reset unrelated working-tree changes.
