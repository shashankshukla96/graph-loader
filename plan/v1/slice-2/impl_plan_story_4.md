# Implementation Plan: Story 4 — Schema Loader Utility (v2)

## Objective
Create `src/utils/schema_loader.py` with a single public function `load_schema(path)`
that reads the YAML file, validates it through Pydantic, and returns a typed `GraphSchema`.
This is the single authorised entry point for all pipeline components — no other code
should call `GraphSchema.model_validate()` directly.

## Files to Create
| File | Notes |
|---|---|
| `src/utils/schema_loader.py` | Main deliverable |

No other files are modified.

## Public API

```python
class SchemaLoadError(Exception):
    """Raised when the schema YAML cannot be read or fails Pydantic validation."""

def load_schema(path: str | os.PathLike) -> GraphSchema:
    ...
```

## Implementation Logic (step by step)

### 1. Resolve the path
```python
resolved = Path(path).resolve()
```
Converts relative paths (e.g. `"config/graph_schema.yaml"`) to absolute paths so error
messages always show the full path regardless of the caller's working directory.

### 2. File existence check
```python
if not resolved.is_file():
    raise SchemaLoadError(f"Schema file not found: {resolved}")
```
Raises immediately with a clear message before attempting to open the file.

### 3. YAML parse
```python
try:
    with resolved.open("r", encoding="utf-8") as fh:
        raw: dict = yaml.safe_load(fh)   # typed annotation — keeps mypy/Pyright happy
except yaml.YAMLError as exc:
    raise SchemaLoadError(f"Failed to parse YAML at {resolved}: {exc}") from exc
```
Uses `yaml.safe_load` (no arbitrary Python object construction).
Chains the original `YAMLError` via `from exc`.
`raw: dict` annotation ensures downstream code is typed correctly; without it
type checkers would infer `Any`, defeating type-safety on `model_validate(raw)`.

### 4. Root type guard
```python
if not isinstance(raw, dict):
    raise SchemaLoadError(
        f"Schema file must be a YAML mapping at the root level, got: {type(raw).__name__}"
    )
```
Catches files that are valid YAML but not a mapping — including:
- Plain lists (`got: list`)
- Empty files (`yaml.safe_load` returns `None` → `got: NoneType`)
- Bare scalars (`got: str`, `got: int`, etc.)

### 5. Pydantic validation
```python
try:
    return GraphSchema.model_validate(raw)
except ValidationError as exc:
    raise SchemaLoadError(
        f"Schema validation failed for {resolved}:\n{exc}"
    ) from exc
```
Chains the original `ValidationError` so callers can inspect field-level errors.

## Error Message Examples
| Scenario | Error |
|---|---|
| File not found | `SchemaLoadError: Schema file not found: /abs/path/config/graph_schema.yaml` |
| Bad YAML syntax | `SchemaLoadError: Failed to parse YAML at ...: <yaml error>` |
| Non-mapping root (list) | `SchemaLoadError: Schema file must be a YAML mapping at the root level, got: list` |
| Empty file | `SchemaLoadError: Schema file must be a YAML mapping at the root level, got: NoneType` |
| Pydantic failure | `SchemaLoadError: Schema validation failed for ...:\n<pydantic error with field paths>` |

## Validation Commands
```bash
# 1. Happy path — must print schema summary
.venv/bin/python -c "
from src.utils.schema_loader import load_schema
schema = load_schema('config/graph_schema.yaml')
print(f'Loaded {len(schema.nodes)} nodes, {len(schema.edges)} edges')
print(f'Mode: {schema.loading.mode}')
"

# 2. File not found
.venv/bin/python -c "
from src.utils.schema_loader import load_schema, SchemaLoadError
try:
    load_schema('nonexistent.yaml')
    raise AssertionError('should have raised')
except SchemaLoadError as e:
    assert 'not found' in str(e), f'wrong message: {e}'
    print('file-not-found guard: OK')
"

# 3. Non-mapping YAML root
.venv/bin/python -c "
import tempfile, os
from src.utils.schema_loader import load_schema, SchemaLoadError
with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
    f.write('- item1\n- item2\n')
    name = f.name
try:
    load_schema(name)
    raise AssertionError('should have raised')
except SchemaLoadError as e:
    assert 'mapping' in str(e), f'wrong message: {e}'
    print('non-mapping guard: OK')
finally:
    os.unlink(name)
"

# 4. Bad YAML syntax — must chain yaml.YAMLError
.venv/bin/python -c "
import tempfile, os, yaml
from src.utils.schema_loader import load_schema, SchemaLoadError
with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
    f.write('nodes: [\n  - unclosed\n')
    name = f.name
try:
    load_schema(name)
    raise AssertionError('should have raised')
except SchemaLoadError as e:
    assert isinstance(e.__cause__, yaml.YAMLError), f'YAMLError must be chained, got: {type(e.__cause__)}'
    print('bad-yaml chain guard: OK')
finally:
    os.unlink(name)
"

# 5. Pydantic ValidationError — must chain pydantic.ValidationError
.venv/bin/python -c "
import tempfile, os, yaml
from src.utils.schema_loader import load_schema, SchemaLoadError
from pydantic import ValidationError
with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
    # Valid YAML mapping but missing required 'key_property' field
    yaml.dump({'nodes': [{'label': 'X', 'topic': 't', 'properties': {'id': {'type': 'string'}}}], 'edges': []}, f)
    name = f.name
try:
    load_schema(name)
    raise AssertionError('should have raised')
except SchemaLoadError as e:
    assert isinstance(e.__cause__, ValidationError), f'ValidationError must be chained, got: {type(e.__cause__)}'
    print('pydantic-chain guard: OK')
finally:
    os.unlink(name)
"
```

## Rollback
Delete `src/utils/schema_loader.py`. No other files are modified.
