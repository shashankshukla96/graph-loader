# Phase 6 / Slice 1 / Story 1 — Implementation Plan

## Story

Pin a zero-reformat ruff lint baseline (`plan/v6/slice-1/stories.md`, Story 1).

## Constraint (binding, from the orchestrator's parallel-work note)

A parallel developer (Phase 5 Slice 2) is editing: `src/cli.py`,
`src/loader/edge_loader.py`, `src/loader/node_loader.py`,
`src/models/schema.py`, `src/orchestrator/docker_service.py`,
`tests/test_edge_loader.py`, `tests/test_node_loader.py`,
`tests/test_schema_validation.py`, `tests/test_cli.py`,
`tests/test_docker_service.py`, `docs/USAGE.md`. None of the fixes below in
those files may be applied in this slice. They are recorded as deferred
instead of fixed, and no per-file `noqa`/ignore is added to hide them — the
lint job is expected to fail on exactly those findings until P5-S2 merges.

## Files to add/edit

1. **`requirements-dev.txt`** (new) — pins `ruff==0.16.9` and
   `actionlint-py==1.7.12.25`, with a header comment noting these are not
   installed into any loader image.
2. **`pyproject.toml`** — append `[tool.ruff]` (`target-version = "py312"`)
   and `[tool.ruff.lint]` with the narrow `select`/`ignore` table from
   `stories.md` (F, E4, E7, E9 selected; E402/E701/E702/E731/E741 ignored,
   each with the hit-count rationale as a comment). Existing
   `[tool.pytest.ini_options]` table untouched.
3. **`Makefile`** — add `.PHONY: lint` / `lint: ruff check src tests examples`.
4. **Genuine fixes, applied (not in the excluded-file list):**
   - `examples/imdb/publish_and_load_nodes.py:37` — drop unused
     `DEFAULT_CHUNK_ROWS, DEFAULT_HUB_DEGREE, DEFAULT_PLAN_BUCKETS` from the
     `from src.loader.edge_plan import ...` line. (`DEFAULT_PLAN_WRITERS` on
     line 40 is a separate, still-used import — untouched.)
   - `src/loader/edge_execution.py:623` — drop unused `EdgeWriter` from the
     local `from src.loader.edge_loader import EdgeWriter,
     MissingRelationshipEndpointError`; keep `MissingRelationshipEndpointError`.
   - `src/orchestrator/lease_server.py:47` — drop unused `field` from
     `from dataclasses import dataclass, field`.
   - `src/orchestrator/planned_dispatcher.py:51` — same fix, same import line.
   - `src/orchestrator/relationship_bulk_monitor.py:16` — drop unused
     `ClockProtocolError` from the `coordination` import line; keep
     `ClockLease`, `decode_clock_lease`.
   - `tests/test_auto_settings.py:6` — drop unused `timedelta` from the
     `datetime` import line.
   - `tests/test_bucket_lease_execution.py:11` — drop unused `SimpleNamespace`
     import line entirely (sole name on that line).
   - `tests/test_dev_environment.py:17` — drop unused `Consumer` from the
     `confluent_kafka` import; keep `KafkaException`.
   - `tests/test_planned_dispatcher.py:316` — rename `first = running.client("w1")`
     to `_first = ...` (keeps the reference alive for the `with
     pytest.raises` block below it; ruff's dummy-variable pattern ignores
     leading-underscore names).
   - `tests/test_planned_edges_cli.py:6-7` — delete the unused `import os` and
     `from pathlib import Path` lines.
5. **Deferred (excluded files — left unfixed on purpose, recorded here and in
   the eventual `plan/v6/completion_slice-1.md`):**
   - `src/cli.py:94` — unused `DEFAULT_PLAN_WRITERS` import (F401).
   - `src/cli.py:1511` — `f"Stopping loader(s)..."` with no placeholder (F541).
   - `src/loader/edge_loader.py:30` — unused `PropertyConfig` import (F401).
   - `src/loader/edge_loader.py:728` — unused `suffix` local (F841).
   - `tests/test_edge_loader.py:33` — unused `ControlDeliveryError` import (F401).
   - `tests/test_schema_validation.py:25,27` — unused `IndexConfig`,
     `MixAndBatchConfig` imports (F401 x2).

## Validation

- Throwaway venv under `/tmp`: `pip install -r requirements-dev.txt` then
  `ruff check src tests examples`. Expect exactly the 7 deferred findings
  above and nothing else once the 13 non-deferred fixes land.
- `make lint` runs the same command.
- Spot-revert one fix (`tests/test_planned_dispatcher.py`) and confirm ruff
  reports exactly that one new finding.
- Run `tests/test_planned_dispatcher.py::test_duplicate_writer_ids_are_rejected`
  after the `_first` rename to confirm behavior is unchanged.
- `ruff format --check` is never invoked anywhere in this story.

## Reviewer touchpoint

Send the reviewer this plan plus the diff before marking the story done.
