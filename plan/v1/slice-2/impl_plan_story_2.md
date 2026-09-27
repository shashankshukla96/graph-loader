# Implementation Plan: Story 2 — Pydantic v2 Schema Models (v2)

## Objective
Replace the `src/models/schema.py` stub with a complete, strictly-typed Pydantic v2 model
hierarchy covering every construct in the YAML schema. No YAML parsing here — pure model
definitions and validators only.

## Files to Modify
| File | Change |
|---|---|
| `src/models/schema.py` | Replace stub with full model implementation |

## Class Hierarchy (dependency order)

### Primitives
```
ConstraintConfig   — type: Literal["uniqueness", "not_null"]
IndexConfig        — type: Literal["range", "text"]
PropertyConfig     — type: Literal[5 types], required, constraint?, index?
```

### Node & Edge sub-models
```
NodeConfig         — label, topic, key_property, properties dict
                     Validators (in this order):
                       1. at_least_one_property  ← FIRST (independently reachable)
                       2. key_property_must_exist ← SECOND (properties guaranteed non-empty)
SourceTargetConfig — source, target; .is_self_referencing property
MixAndBatchConfig  — batch_size, forward_slot_ms, backward_slot_ms (all >0)
RetryConfig        — max_attempts, base_delay_seconds, max_delay_seconds
                     Validator: max_delay_seconds >= base_delay_seconds
DeadLetterConfig   — topic, enabled
LoadingConfig      — mode, consumer_group_id, max_poll_interval_ms, session_timeout_ms
EdgeConfig         — type, topic, nodes, properties, mix_and_batch, retry, dead_letter?
                     Validator: at_least_one_property
```

### Root
```
GraphSchema        — nodes: List[NodeConfig], edges: List[EdgeConfig], loading: LoadingConfig
                     Validator: no duplicate node labels; no duplicate edge types
```

## Key Implementation Notes

### Pydantic v2 idioms
- Use `model_validator(mode="after")` — receives `self` (already-constructed model instance).
- Use `Field(default_factory=...)` for mutable defaults (dicts, nested models).
- Use `Literal[...]` for enums — no separate Enum classes needed.
- `from __future__ import annotations` at the top for forward-reference safety.
- Return type annotations on validators use quoted form `-> "NodeConfig"` (safe with PEP 563).

### Validator logic

#### NodeConfig — validator ORDER matters
`at_least_one_property` MUST be defined before `key_property_must_exist` so that
an empty `properties={}` surfaces the semantically correct error independently:

```python
@model_validator(mode="after")
def at_least_one_property(self) -> "NodeConfig":
    if not self.properties:
        raise ValueError(f"Node '{self.label}' must have at least one property.")
    return self

@model_validator(mode="after")
def key_property_must_exist(self) -> "NodeConfig":
    if self.key_property not in self.properties:
        raise ValueError(
            f"key_property '{self.key_property}' not found in properties of node '{self.label}'"
        )
    return self
```

#### RetryConfig — cross-field guard (new — not in original stories.md spec)
Prevents nonsensical configs where the backoff ceiling is below the starting delay:

```python
@model_validator(mode="after")
def max_delay_exceeds_base(self) -> "RetryConfig":
    if self.max_delay_seconds < self.base_delay_seconds:
        raise ValueError(
            f"max_delay_seconds ({self.max_delay_seconds}) must be >= "
            f"base_delay_seconds ({self.base_delay_seconds})"
        )
    return self
```

#### EdgeConfig — single validator
```python
@model_validator(mode="after")
def at_least_one_property(self) -> "EdgeConfig":
    if not self.properties:
        raise ValueError(f"Edge '{self.type}' must have at least one property.")
    return self
```

#### GraphSchema — duplicate check
```python
@model_validator(mode="after")
def no_duplicate_labels(self) -> "GraphSchema":
    labels = [n.label for n in self.nodes]
    if len(labels) != len(set(labels)):
        raise ValueError("Duplicate node labels detected in schema.")
    types = [e.type for e in self.edges]
    if len(types) != len(set(types)):
        raise ValueError("Duplicate edge types detected in schema.")
    return self
```

### `SourceTargetConfig.is_self_referencing`
A `@property` (not a Pydantic field) — returns `self.source == self.target`.
Defined after the Pydantic fields, annotated with `@property` only (no `Field`).

## Validation Commands
```bash
.venv/bin/python -c "
from src.models.schema import GraphSchema, NodeConfig, EdgeConfig, SourceTargetConfig, RetryConfig
print('imports OK')
st = SourceTargetConfig(source='Person', target='Person')
print('is_self_referencing:', st.is_self_referencing)
# Verify RetryConfig cross-field guard
import sys
try:
    RetryConfig(base_delay_seconds=30.0, max_delay_seconds=1.0)
    print('ERROR: should have raised')
    sys.exit(1)
except Exception:
    print('RetryConfig cross-field guard: OK')
"
```

## Rollback
Restore `src/models/schema.py` to the stub (docstring-only) state from Story 1.
