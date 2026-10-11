# Implementation Plan: Story 1 - Repo Skeleton & Environment Variable Contract

## Files to Create/Modify
- `J:\Graph Loader\.env.example` (Create)
- `J:\Graph Loader\.gitignore` (Create)
- `J:\Graph Loader\config\.gitkeep` (Create)
- `J:\Graph Loader\src\.gitkeep` (Create)
- `J:\Graph Loader\tests\.gitkeep` (Create)
- `J:\Graph Loader\scripts\.gitkeep` (Create)

## New Classes and Methods
- N/A

## Key Logic / Implementation Details
- **`.env.example`**: Create with the exact following 8 environment variables and comments. (The parent story incorrectly calls this a 9-variable contract; its own required block contains 8 assignments.)
  - `NEO4J_URI=bolt://localhost:7687`
  - `NEO4J_USERNAME=neo4j`
  - `NEO4J_PASSWORD=changeme          # REQUIRED: min 8 characters for Neo4j Enterprise`
  - `NEO4J_DATABASE=neo4j`
  - `NEO4J_ACCEPT_LICENSE_AGREEMENT=yes`
  - `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`
  - `KAFKA_CONSUMER_GROUP_PREFIX=graph-loader`
  - `LOADER_LOG_LEVEL=INFO`
- **`.gitignore`**: Ignore `.env` (to prevent secrets leakage), Python cache directories (`__pycache__/`, `*.pyc`, `*.pyo`), test caches (`.pytest_cache/`, `.mypy_cache/`), builds (`dist/`, `build/`, `*.egg-info/`), and virtual environments (`.venv/`, `venv/`).
- **Directories**: Initialize `config/`, `src/`, `tests/`, and `scripts/` directories. Add an empty `.gitkeep` file to each to ensure they are tracked by Git.

## Tests to Write
- N/A (Pure repository scaffolding). Verify file presence and the eight required variable names without displaying `.env` values.
- Git precondition: this workspace must be a Git checkout or be initialized with `git init` before Git-based DoD checks can be completed.
- After the Git precondition is met, verify `.env` is ignored with `git check-ignore .env`, verify it is untracked with `git ls-files --error-unmatch .env` (expected to exit non-zero), and inspect `git status --short`.

## Rollback / Cleanup
- Remove the created files and directories if the implementation fails.
