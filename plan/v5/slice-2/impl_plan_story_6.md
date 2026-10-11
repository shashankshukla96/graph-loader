# Story 6 implementation plan: Orchestrator Wiring — Topic Provisioning and Migration Path

## Files

- Edit: `src/cli.py`
- Edit: `docs/USAGE.md` (§10 migration subsection)
- New: `tests/test_cli_dead_letter.py` (keeps the addition isolated from the
  already-large `tests/test_cli.py`, matching the repo's convention of
  topic-scoped test files, e.g. `test_dead_letter.py` itself)

## Design

### `src/cli.py`

```python
def _ensure_dead_letter_topics(schema, bootstrap_servers: str) -> None:
    """Create every configured DLQ topic before any container launches.

    Fail-closed (unlike _ensure_control_topic's warn-and-continue): a run
    that cannot durably DLQ a malformed record must not start, matching
    bulk mode's existing "fail closed on setup" philosophy (DDL
    application). Deduplicates by resolved topic name -- Story 2's schema
    validation already rejects two entities sharing one, so this is a
    defensive assertion, not a real branch in practice.
    """
    from src.loader.dead_letter import ensure_dead_letter_topic

    seen: set[str] = set()
    for entity_topic, dead_letter in (
        *((node.topic, node.dead_letter) for node in schema.nodes),
        *((edge.topic, edge.dead_letter) for edge in schema.edges),
    ):
        resolved_topic = dead_letter_topic_name(entity_topic, dead_letter)
        if resolved_topic is None or resolved_topic in seen:
            continue
        seen.add(resolved_topic)
        ensure_dead_letter_topic(bootstrap_servers, resolved_topic, source_topic=entity_topic)
```

Call site: in `_handle_start`, immediately beside the existing
`_ensure_control_topic(...)` call (same line grouping), before any
container-launch call. `_handle_start` has no blanket exception handler
(each of its other failure points explicitly catches and returns `1`), so
this call is wrapped the same way: `try: _ensure_dead_letter_topics(...)
except Exception as exc: logger.error(...); return 1` -- giving a clean
fail-closed exit and a log line instead of an unhandled traceback, while
still aborting before `docker_service.run_node_loader`/`run_edge_loader`
is ever called (they run later in the function).

Import `dead_letter_topic_name` from `src.models.schema` at the top of
`cli.py` (it already imports several names from that module).

### `src/orchestrator/docker_service.py`

No change. Confirmed by inspection: `run_node_loader`/`run_edge_loader`
already pass `KAFKA_BOOTSTRAP_SERVERS` (from `LOADER_KAFKA_BOOTSTRAP_SERVERS`)
and mount the full schema YAML at `/app/runtime_schema.yaml` unconditionally,
so a DLQ-enabled container recomputes its own entity's DLQ topic name
locally via `dead_letter_topic_name` (Story 2) without needing a new env
var. `REJECTION_LOG_PATH` and `host_rejection_dir` stay exactly as they are
-- required for file-mode entities, harmless (unused) for DLQ-mode ones,
and the mount stays unconditional (no conditional-per-replica mount path).

### `docs/USAGE.md` §10

New subsection "Migrating a rejection-file entity to a DLQ topic" per the
story's exact 4-step text (add `dead_letter: {enabled: true}`, rerun
`start`, the JSONL-payload replay limitation, reversibility).

## Tests (`tests/test_cli_dead_letter.py`)

- `_ensure_dead_letter_topics` calls `ensure_dead_letter_topic` exactly
  once per distinct resolved DLQ topic across a schema with mixed
  node/edge `dead_letter` entities (mock `src.loader.dead_letter.
  ensure_dead_letter_topic` via `patch("src.cli.ensure_dead_letter_topic")`
  after importing it at call time -- or patch the deferred import target
  `src.loader.dead_letter.ensure_dead_letter_topic` directly, whichever
  the actual import shape ends up using).
- A schema with no `dead_letter` blocks anywhere makes zero calls to
  `ensure_dead_letter_topic`.
- A simulated `ensure_dead_letter_topic` failure inside `_handle_start`
  aborts before `DockerService`/any launch call is made -- mock
  `DockerService.build_image`/`run_node_loader` and assert never called,
  and assert `_handle_start` returns `1`.
- `_ensure_dead_letter_topics` on its own (unit-level, not through
  `_handle_start`) with a schema whose two entities share... (not
  reachable: Story 2 already rejects that at load time) -- instead test
  the defensive dedup branch directly by constructing two `NodeConfig`/
  `EdgeConfig`-like inputs that would resolve to the same topic only if
  Story 2's guard were bypassed, proving the loop's `seen` set does not
  double-call even if it somehow received one.

## Dependencies

Depends on Stories 2, 3, 4, 5. Blocks nothing further in this slice.
