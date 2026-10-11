# Story 2 implementation plan: Schema — Node Dead-Letter Config and Topic Naming

## Files

- Edit: `src/models/schema.py`
- Edit: `src/loader/dead_letter.py` (add `dead_letter_topic_name`)
- Edit: `config/graph_schema.yaml`
- Edit: `docs/USAGE.md` (§3)
- New/edit: `tests/test_schema_validation.py` and/or `tests/test_auto_settings.py`
  (the existing topic-collision parametrized test lives in
  `test_auto_settings.py::test_schema_contract_errors_are_clear`; add cases
  there, plus new focused tests in `test_schema_validation.py`)

## Design

### `src/models/schema.py`

1. `DeadLetterConfig.topic`: change from required `str` to `Optional[str] =
   None`. Add a `field_validator("topic")` that only runs the existing
   blank-string rejection when `topic is not None`:
   ```python
   @field_validator("topic")
   @classmethod
   def topic_is_not_blank_when_set(cls, value: str | None) -> str | None:
       if value is not None and not value.strip():
           raise ValueError("dead_letter topic must not be blank when set")
       return value
   ```
2. `NodeConfig`: add `dead_letter: Optional[DeadLetterConfig] = None`,
   identical in shape/position to `EdgeConfig.dead_letter`.
3. `GraphSchema.validate_edge_endpoints` (the existing single model validator
   that already builds the `owners: dict[str, str]` topic-ownership map):
   extend it to also collect every entity's *effective* DLQ topic (via the
   new `dead_letter_topic_name` helper) into a second map
   `dlq_owners: dict[str, str]`, and validate:
   - No two entities' effective DLQ topics collide (explicit or defaulted) —
     checked the same way as `owners` (insert-and-detect-duplicate).
   - No effective DLQ topic collides with any entry already in `owners`
     (a node/edge source topic or the coordination topic) — check membership
     in `owners` before inserting into `dlq_owners`.
   - An entity whose *own* source topic equals its own explicit
     `dead_letter.topic` is rejected (this is already implied by the
     previous rule since the entity's own topic is already in `owners`
     before its DLQ topic is checked, but state it explicitly in the error
     message so it's diagnosable).
   This runs inside the existing `model_validator(mode="after")` immediately
   after the `owners` loop and before endpoint validation, so it fails at
   `load_schema()` time like every other schema error.

### `src/loader/dead_letter.py`

Add pure helper (no Kafka, no pydantic import — takes the config object
structurally so it doesn't need to import `NodeConfig`/`EdgeConfig`):

```python
def dead_letter_topic_name(entity_topic: str, dead_letter) -> str | None:
    """Return the effective DLQ topic name, or None when DLQ routing is off.

    ``dead_letter`` is an object with ``.enabled`` and ``.topic`` attributes
    (``DeadLetterConfig`` or ``None``). ``None`` and ``enabled=False`` both
    mean "DLQ routing is off, use the file-sink fallback" and must return
    the identical ``None`` so every downstream caller treats them the same.
    """
    if dead_letter is None or not dead_letter.enabled:
        return None
    return dead_letter.topic or f"{entity_topic}.dlq"
```

### `config/graph_schema.yaml`

Add a commented-out node-level `dead_letter` example under `Person`'s block
(commented so it stays a zero-behavior-change regression baseline), plus a
comment next to the two existing edge `dead_letter` blocks (`WORKS_AT`,
`KNOWS`) noting they go from dead code to live routing in this slice.

### `docs/USAGE.md`

- §3 `nodes[]` fields table: add a `dead_letter` row mirroring the edges
  table's description, referencing the default-naming rule.
- §3 Validation bullet list: add "a node or relationship type's dead-letter
  topic (explicit or the `<topic>.dlq` default) collides with another
  entity's dead-letter topic, or with any node/edge/coordination topic."
- §10 gets Story 6's migration subsection (added when Story 6 lands); this
  story only touches §3.

## Tests

`tests/test_schema_validation.py` (new, self-contained fixtures, not
depending on the social schema fixture):
- A node schema with no `dead_letter` block validates identically to today
  (regression — construct a minimal 1-node 1-edge schema and confirm
  `node.dead_letter is None`).
- A node `dead_letter` block validates the same shape as an edge one
  (`enabled`, optional `topic`).
- `topic: None` (omitted) defers naming — `dead_letter_topic_name` returns
  the default.
- `dead_letter.enabled=False` and `dead_letter=None` both make
  `dead_letter_topic_name` return `None` (identical treatment).
- Default-name collision: two node labels whose topics are `a` and `a.dlq`
  (so the second label's *source* topic literally collides with the first's
  *defaulted* DLQ topic) is rejected by schema loading.
- Explicit-topic collision: an edge's explicit `dead_letter.topic` equals
  another node's source topic — rejected.
- An entity's explicit `dead_letter.topic` equals its own source topic —
  rejected, with a clear message.

`tests/test_auto_settings.py::test_schema_contract_errors_are_clear`: add
two more `(change, message)` cases using the existing `SOCIAL` schema
fixture — one node-level and one edge-level DLQ-topic collision — since
that test already proves the end-to-end `load_schema()` error surface for
this class of validation.

## Dependencies

Depends on Story 1 (`dead_letter_topic_name` lives in `dead_letter.py`
beside the envelope model). Blocks Stories 3-6.
