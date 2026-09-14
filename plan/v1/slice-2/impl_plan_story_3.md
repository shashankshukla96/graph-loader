# Implementation Plan: Story 3 — Canonical `config/graph_schema.yaml`

## Objective
Create the fully-commented, canonical YAML schema file that exercises every feature
of the Pydantic models built in Story 2. This file is the single source of truth for
the graph model and the primary input to all downstream components.

## Files to Create
| File | Notes |
|---|---|
| `config/graph_schema.yaml` | Main deliverable — fully worked example |

`config/.gitkeep` is **left in place** for this story. It may be removed in a future
cleanup commit once `graph_schema.yaml` is committed — both files track the directory.

## Required Coverage (from Slice 2 DoD)
| Requirement | Satisfied by |
|---|---|
| ≥ 2 node types | `Person`, `Company` |
| ≥ 2 edge types | `WORKS_AT`, `KNOWS` |
| 1 self-referencing edge | `KNOWS` (Person → Person) |
| All 5 property types | `string`, `integer`, `float`, `date`, `datetime` |
| Both constraint types | `uniqueness`, `not_null` |
| Both index types | `range`, `text` |
| Every field has inline comment | Throughout |

## YAML Structure
```
loading:        ← LoadingConfig fields
nodes:
  - Person      ← NodeConfig (key: personId/string/uniqueness, name/not_null/text-index,
                               age/integer/range-index, salary/float, birthDate/date,
                               lastLogin/datetime)
  - Company     ← NodeConfig (key: companyId/string/uniqueness, name/not_null/text-index,
                               foundedYear/integer/range-index)
edges:
  - WORKS_AT    ← EdgeConfig (Person→Company; since/date/not_null, role/string;
                               mix_and_batch, retry, dead_letter)
  - KNOWS       ← EdgeConfig (Person→Person self-ref; since/date/not_null,
                               strength/float; mix_and_batch, retry, dead_letter)
```

## Validation Commands
```bash
# 1. Raw YAML parse — must not raise
.venv/bin/python -c "
import yaml
with open('config/graph_schema.yaml') as f:
    raw = yaml.safe_load(f)
print('YAML parse OK — top-level keys:', list(raw.keys()))
"

# 2. Pydantic validation — must return GraphSchema with 2 nodes, 2 edges
# NOTE: Direct GraphSchema.model_validate() is used here ONLY because
# schema_loader.py (Story 4) does not exist yet. The test suite (Story 5)
# MUST use load_schema() exclusively — never import GraphSchema directly in tests.
.venv/bin/python -c "
import yaml
from src.models.schema import GraphSchema
with open('config/graph_schema.yaml') as f:
    raw = yaml.safe_load(f)
schema = GraphSchema.model_validate(raw)
assert len(schema.nodes) == 2, f'Expected 2 nodes, got {len(schema.nodes)}'
assert len(schema.edges) == 2, f'Expected 2 edges, got {len(schema.edges)}'
knows = next(e for e in schema.edges if e.type == 'KNOWS')
assert knows.nodes.is_self_referencing, 'KNOWS must be self-referencing'
print(f'Nodes: {[n.label for n in schema.nodes]}')
print(f'Edges: {[e.type for e in schema.edges]}')
print(f'KNOWS self-referencing: {knows.nodes.is_self_referencing}')
print('All assertions passed.')
"
```

## Rollback
Delete `config/graph_schema.yaml`. No other files are modified in this story.
