# Implementation Plan: Story 5 — Unit Test Suite (v2)

## Objective
Create `tests/test_schema_validation.py` — a comprehensive pytest suite covering
`src/models/schema.py` and `src/utils/schema_loader.py`. Tests use `load_schema()`
exclusively in `TestSchemaLoader`; no test in that class imports `GraphSchema` directly.

## Files to Create
| File | Notes |
|---|---|
| `tests/test_schema_validation.py` | Main deliverable |

No other files are modified.

## Module-level constants
```python
from pathlib import Path
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
```
This follows the established project convention from `tests/test_dev_environment.py`
and makes all path-dependent tests working-directory-independent.

## Test Structure (8 classes)

### TestPropertyConfig
- `test_valid_string_property` — `PropertyConfig(type="string")` succeeds; `required` defaults False
- `test_all_valid_types_accepted` — parametrize over all 5 types; each must not raise
- `test_invalid_type_raises` — `type="blob"` raises `ValidationError` matching "type"
- `test_constraint_embedded` — nested `ConstraintConfig` parses correctly
- `test_index_embedded` — nested `IndexConfig` parses correctly

### TestConstraintConfig
- `test_valid_constraint_types` — parametrize over `["uniqueness", "not_null"]`
- `test_invalid_constraint_type_raises` — `type="foreign_key"` raises `ValidationError`

### TestNodeConfig (uses `_minimal_node()`)
- `test_valid_node` — happy path
- `test_missing_key_property_raises` — `key_property="nonexistent"` raises `ValidationError` matching "key_property"
- `test_empty_properties_raises` — `properties={}` raises `ValidationError` matching "at least one property"
- `test_missing_topic_raises` — missing `topic` raises `ValidationError`

### TestSourceTargetConfig
- `test_self_referencing_detected` — `source == target` → `is_self_referencing is True`
- `test_non_self_referencing` — `source != target` → `is_self_referencing is False`

### TestEdgeConfig (uses `_minimal_edge()`)
- `test_valid_edge` — happy path
- `test_empty_properties_raises` — `properties={}` raises `ValidationError` matching "at least one property"
- `test_mix_and_batch_defaults` — `batch_size == 1000`
- `test_self_referencing_edge_detectable` — `KNOWS` edge → `nodes.is_self_referencing is True`

### TestRetryConfig  ← NEW CLASS (was incorrectly placed in TestGraphSchema in v1)
- `test_cross_field_guard` — `RetryConfig(base_delay_seconds=30.0, max_delay_seconds=1.0)` raises `ValidationError`
- `test_valid_retry_config` — `RetryConfig(base_delay_seconds=1.0, max_delay_seconds=60.0)` succeeds

### TestGraphSchema (uses `_valid_schema_dict()`)
- `test_valid_schema_parses` — 2 nodes, 2 edges
- `test_duplicate_node_labels_raises` — raises `ValidationError` matching "Duplicate node labels"
- `test_duplicate_edge_types_raises` — raises `ValidationError` matching "Duplicate edge types"
- `test_loading_config_defaults` — `mode == "stream"`, `consumer_group_id == "graph-loader"`

### TestSchemaLoader (uses `load_schema()` ONLY — no direct `GraphSchema` import in this class)
- `test_loads_canonical_yaml` — uses `_PROJECT_ROOT / "config" / "graph_schema.yaml"` → 2 nodes, 2 edges
- `test_missing_file_raises_schema_load_error` — `SchemaLoadError` with "not found"
- `test_invalid_yaml_raises_schema_load_error` — bad YAML (`"key: [\n  - unclosed\n"`) → `SchemaLoadError`
  matching "parse" AND `isinstance(exc_info.value.__cause__, yaml.YAMLError)`
- `test_pydantic_failure_raises_schema_load_error` — missing `key_property` → `SchemaLoadError`
  matching "validation failed" AND `isinstance(exc_info.value.__cause__, ValidationError)`
- `test_non_mapping_yaml_raises` — list-root YAML → `SchemaLoadError` with "mapping"

## Helper Functions (module-level)
```python
def _minimal_node(label="Person", key="pid") -> dict:
    return {"label": label, "topic": f"{label.lower()}-events",
            "key_property": key, "properties": {key: {"type": "string", "required": True}}}

def _minimal_edge(etype="KNOWS") -> dict:
    return {"type": etype, "topic": f"{etype.lower()}-events",
            "nodes": {"source": "Person", "target": "Person"},
            "properties": {"since": {"type": "date", "required": True}}}

def _valid_schema_dict() -> dict:
    return {"nodes": [_minimal_node("Person", "personId"), _minimal_node("Company", "companyId")],
            "edges": [_minimal_edge("WORKS_AT"), _minimal_edge("KNOWS")]}
```

## Key Implementation Notes

### Bad YAML fixture (for `test_invalid_yaml_raises_schema_load_error`)
Use `"key: [\n  - unclosed\n"` — an unclosed flow sequence guaranteed to produce
`yaml.YAMLError` across all PyYAML 6.x releases:
```python
bad_yaml.write_text("key: [\n  - unclosed\n", encoding="utf-8")
```
Must also assert the `__cause__` is chained:
```python
assert isinstance(exc_info.value.__cause__, yaml.YAMLError)
```

### Pydantic failure fixture (for `test_pydantic_failure_raises_schema_load_error`)
```python
import yaml
bad_schema.write_text(
    yaml.dump({
        "nodes": [{"label": "X", "topic": "t", "key_property": "MISSING",
                   "properties": {"id": {"type": "string"}}}],
        "edges": [],
    }),
    encoding="utf-8",
)
```
Must also assert the `__cause__` is chained:
```python
assert isinstance(exc_info.value.__cause__, ValidationError)
```

## Coverage Target
```bash
.venv/bin/python -m pytest tests/test_schema_validation.py -v \
    --cov=src/models/schema --cov=src/utils/schema_loader \
    --cov-report=term-missing
```
Target: ≥80% on both modules.

## Validation Commands
```bash
# New tests only:
.venv/bin/python -m pytest tests/test_schema_validation.py -v --tb=short
# Full suite (must not regress):
.venv/bin/python -m pytest tests/ -v --tb=short
```

## Rollback
Delete `tests/test_schema_validation.py`. No other files are modified.
