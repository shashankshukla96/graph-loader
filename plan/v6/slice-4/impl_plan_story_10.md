# Story 10 implementation plan: edge-before-node DLQ scaffold (auto-activating)

New files: `tests/integration/test_edge_before_node_dlq.py`,
`tests/test_edge_before_node_dlq_probes.py`.

## Design (Decision 4 in `stories.md`)

`_row_level_dlq_still_missing(write_call)` and `_retry_consumer_available()`
are the two independent capability probes -- behaviour-detected, not a
hardcoded date/branch check. The writer is built with
`explicit_locks=False, sort_rows=False` (planned mode's exact query
branch) so this scaffold runs against the plain `neo4j_driver`/`clean_neo4j`
shared fixtures (no APOC, no `CALL (vars) { ... }` syntax -- Story 6's
version finding doesn't apply here, keeping this test on the same 5.21
fixture every other pre-existing integration test uses).

`_retry_consumer_available()` uses `importlib.util.find_spec(...)` (never
an actual `import` statement) wrapped in a broad `except Exception: return
False`, so it can never turn a legitimate skip into a collection error, per
the story's own DoD line.

## Acceptance criteria covered

- Collection never errors regardless of whether Phase 5 lands (no
  Phase-5-specific name at module scope).
- Today (pre-Slice-3): reports skipped with the documented reason; suite
  exit code unaffected.
- Both probes unit-tested in isolation (no Docker) against a fake write
  call / fake module path.
- Only currently-real `src/` names imported at module scope.
