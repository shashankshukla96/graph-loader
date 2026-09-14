# Implementation Plan: Story 1 — Project Scaffold & Dependency Wiring

## Objective
Turn the empty `src/` directory into a properly structured, importable Python package tree and add the two new runtime dependencies required for Slice 2.

## Files to Create
| File | Content |
|---|---|
| `src/__init__.py` | empty |
| `src/models/__init__.py` | empty |
| `src/models/schema.py` | empty (module-level docstring only — populated in Story 2) |
| `src/utils/__init__.py` | empty |
| `src/orchestrator/__init__.py` | empty |

**Note:** `src/.gitkeep` will be deleted — the `__init__.py` files make it unnecessary.  
**Note:** `config/.gitkeep` is left **untouched** — `config/graph_schema.yaml` is created in Story 3.

## Files to Modify
| File | Change |
|---|---|
| `requirements.txt` | Append `pydantic>=2.7` and `pyyaml>=6.0` (preserve all existing entries) |
| `pyproject.toml` | Add `testpaths = ["tests"]` inside the **existing** `[tool.pytest.ini_options]` section |

### `pyproject.toml` target state for `[tool.pytest.ini_options]`
```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
markers = [
    "smoke: environment smoke tests (require docker compose up)",
]
```

## Key Logic
1. Create four empty `__init__.py` files — makes `src`, `src.models`, `src.utils`, `src.orchestrator` importable packages.
2. Create `src/models/schema.py` as an empty file (docstring only) so that `from src.models import schema` succeeds immediately.
3. Append to `requirements.txt` (do NOT replace — preserve existing entries).
4. Add `testpaths = ["tests"]` into the **existing** `[tool.pytest.ini_options]` section in `pyproject.toml` (the section header already exists — do not add a duplicate header).
5. Delete `src/.gitkeep`.

## Validation Commands
```bash
pip install -r requirements.txt
python -c "from src.models import schema; print('OK')"   # must exit code 0
python -c "import src.models; import src.utils; import src.orchestrator; print('OK')"
python -c "import pydantic; import yaml; print(pydantic.__version__)"
python -m pytest tests/test_dev_environment.py -v -m smoke   # must still pass
```

## Rollback
If anything breaks, delete the created `__init__.py` and `schema.py` files, restore `src/.gitkeep`, and revert `requirements.txt` and `pyproject.toml` changes.
