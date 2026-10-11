# Implementation Plan — Phase 3 / Slice 1 / Story 1

## Story

**Validate Relationship Endpoints and Event Contracts**

## Scope and non-goals

This story makes every declared relationship self-describing and introduces a
pure, schema-driven event normalizer. It does not connect to Kafka or Neo4j,
resolve real endpoint nodes, create relationships, or add retry/DLQ policy.
Those concerns are reserved for Stories 2 and 3 (and Phase 5).

## Files

| Action | Path | Purpose |
| --- | --- | --- |
| Modify | `src/models/schema.py` | Declare endpoint-key fields and validate graph-wide edge references. |
| Modify | `config/graph_schema.yaml` | Make the canonical `WORKS_AT` and `KNOWS` declarations explicit about their endpoint keys. |
| Create | `src/loader/edge_loader.py` | Define the pure relationship record error, immutable normalized record, and normalizer. |
| Modify | `tests/test_schema_validation.py` | Update fixtures and prove cross-schema endpoint validation. |
| Create | `tests/test_edge_loader.py` | Prove contract validation and normalization across two edge types. |

## Schema changes

1. Add required `source_key_property: str` and `target_key_property: str` to
   `EdgeConfig`, each with the existing Cypher-identifier validator.
2. Add one `GraphSchema` after-model validator that builds a `{label:
   NodeConfig}` lookup after duplicate-label/type validation. For each edge:
   - require `nodes.source` and `nodes.target` to name declared nodes;
   - require `source_key_property == source_node.key_property` and
     `target_key_property == target_node.key_property`;
   - assert each corresponding node property exists and is required (defensive
     guarantees even if a model was built through future extensions).
3. Add the explicit keys to canonical YAML:
   - `WORKS_AT`: `source_key_property: personId`,
     `target_key_property: companyId`;
   - `KNOWS`: both keys `personId`.

The model-level validator produces messages containing the edge type and the
invalid label/key so configuration errors are actionable without exposing data.

## New contract API

Create `src/loader/edge_loader.py` with:

```python
class EdgeRecordValidationError(ValueError):
    """Raised when a relationship event violates its declared schema."""

@dataclass(frozen=True)
class EdgeRecord:
    """Normalized keys and allowlisted properties for one relationship."""
    source_key: object
    target_key: object
    properties: dict[str, object]

def normalize_edge_record(
    raw_record: Mapping[str, object], edge_config: EdgeConfig
) -> EdgeRecord:
    """Validate the canonical envelope and normalize declared scalar values."""
```

Algorithm:

1. Require exactly the top-level keys `source`, `target`, and `properties`;
   reject unknown, missing, non-mapping, or `None` sections.
2. For `source` and `target`, allow only the respectively configured endpoint
   key, require it to be non-null, and normalize it using the declared
   `PropertyConfig` from the resolved node configuration. `EdgeConfig` will
   retain the resolved endpoint property configs as internal, non-serialized
   Pydantic `PrivateAttr` bindings (defaulting to `None`). A documented
   binder/resolver method is invoked by `GraphSchema` only after complete
   graph validation. This avoids a second schema lookup and avoids a
   type-specific branch.
3. `normalize_edge_record` first verifies both internal bindings exist. A
   standalone/directly constructed `EdgeConfig` therefore fails deterministically
   with `EdgeRecordValidationError("Edge '<TYPE>' has unresolved endpoint schema
   bindings")`, never an attribute error or unconstrained endpoint validation.
   Application callers must obtain configurations through `load_schema()`.
4. Allowlist relationship `properties`: reject unknown entries; require every
   required property and omit optional nulls, mirroring node PATCH semantics.
5. Reuse the exact scalar conversion implementation from `node_loader` by
   extracting it to a small shared private helper in `src/loader/validation.py`
   **only if necessary to avoid import-cycle or duplicated behavior**. If the
   helper extraction is used, it is included in this story's files/tests and
   both node and edge validation must retain their current behavior.
6. Error text identifies only edge type/field/reason, never raw values.

To avoid scope expansion, the expected implementation will use a small shared
normalization helper (`src/loader/record_validation.py`) parameterized by
entity name and property config. `node_loader.py` will be updated to delegate
to it, preserving its `NodeRecordValidationError` public error type by passing
an error factory. This is a mechanical refactor required to guarantee that
node and edge scalar rules cannot drift.

## Tests

`tests/test_schema_validation.py`:

- amend minimal edge fixtures with endpoint key fields;
- valid canonical edge declarations;
- unknown source label and unknown target label;
- source/target endpoint keys that differ from their declared node key;
- missing/optional endpoint key (through schema mutation); and
- invalid endpoint identifiers.

`tests/test_edge_loader.py`:

- valid `WORKS_AT` envelope with normalized date relationship property;
- valid self-referencing `KNOWS` envelope;
- missing, null, unknown, and non-mapping top-level sections;
- unknown/missing/null endpoint fields;
- unknown/missing/invalid relationship properties;
- endpoint scalar type failures and optional-null omission;
- error text must not echo a sentinel raw value.
- passing a standalone, unbound `EdgeConfig` fails with the documented
  binding error; all normal successful cases use an edge obtained through
  `GraphSchema`/`load_schema()`.

Run:

```bash
python -m pytest tests/test_schema_validation.py tests/test_node_loader.py tests/test_edge_loader.py -v
```

## Rollback / cleanup

This story creates no infrastructure or persistent data. If the refactor
changes node validation behavior, revert only the shared-helper extraction and
keep the new edge API isolated until both contract suites pass. No Kafka offsets
or Neo4j transactions exist in this story.
