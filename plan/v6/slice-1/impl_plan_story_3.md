# Phase 6 / Slice 1 / Story 3 — Implementation Plan

## Story

Wire the advisory nightly/manual integration workflow
(`plan/v6/slice-1/stories.md`, Story 3).

## Files to add/edit

1. **`.github/workflows/ci-integration.yml`** (new) — a separate workflow
   file, not a job appended to `ci.yml`, so "never blocks a PR" is true by
   construction (no shared `on:` trigger, no `needs:` edge to `ci.yml`):
   - `on: schedule` (nightly cron `"0 3 * * *"`) + `workflow_dispatch: {}`.
     No `push`/`pull_request` trigger anywhere in this file.
   - Single `integration` job, `timeout-minutes: 45`: checkout, setup-python
     (cache keyed on `requirements.txt` only — this job does not install
     `requirements-dev.txt` or the imdb extras, just the production deps
     the `testcontainers` suite needs), `pip install -r requirements.txt`,
     `pytest tests -m integration --junitxml=integration-results.xml`, then
     an `if: always()` `actions/upload-artifact@v4` step uploading the
     JUnit XML with `retention-days: 14` regardless of pass/fail.
   - No `services:` block — confirmed the repo's `tests/conftest.py`
     fixtures (`neo4j_connection`, `kafka_bootstrap`) each start their own
     `testcontainers` container against the runner's own Docker daemon and
     `pytest.skip(...)` cleanly if Docker isn't reachable; declaring GitHub
     Actions `services:` would just run a second, unused stack.
   - No `GRAPH_LOADER_IT_*` env vars set anywhere — confirmed
     `tests/test_social_planned_integration.py`'s module-level
     `pytestmark` has `pytest.mark.skipif(not
     os.environ.get("GRAPH_LOADER_IT_KAFKA"), reason="GRAPH_LOADER_IT_*
     environment not set")`, so it reports `skipped` in the JUnit XML, not
     `error`.

## Not in scope

- `tests/test_docker_build.py::test_docker_build_node_loader` is
  `@pytest.mark.integration` and will run here too, redundantly with
  Story 2's dedicated `build-images` job — intentional, left as-is per the
  story text (Story 2's job is the deliberate entrypoint check for all
  three images; this pytest test comes along for free and is node-loader-
  only).
- No excluded-file edits; this workflow doesn't touch `src/` or `tests/*`
  at all.

## Validation plan

- YAML syntax check (`yaml.safe_load`) immediately.
- `actionlint` (from the /tmp venv used for Story 1) against this file:
  confirm it validates `schedule`/`workflow_dispatch` syntax, the cron
  string, and `actions/upload-artifact@v4`'s `with:` keys.
- Cannot exercise `schedule`/`workflow_dispatch` triggers without pushing to
  GitHub (explicitly out of scope — never push). Record in the completion
  doc that the "one recorded manual dispatch run" DoD item is not
  satisfiable from this sandboxed environment and is a known gap, matching
  this story's own acknowledgment that "there is nothing to link to... since
  nothing is pushed from this environment" doesn't apply verbatim (that
  line was about Story 4's validation being local instead of a link) —
  I will call this out explicitly as an open item in the completion report
  rather than fabricate a dispatch run.
- Manually sanity-check the `pytest tests -m integration` collection step
  locally (repo .venv, Docker available here) to confirm
  `test_social_planned_integration.py`'s tests collect and report
  `skipped`, and that other integration tests either run for real (Docker
  is available in this sandbox) or skip cleanly — without running the full
  45-minute suite if it's large; sample first with `--collect-only` and a
  short subset.

## Reviewer touchpoint

Send the reviewer the workflow YAML plus this plan; confirm especially:
non-blocking by construction (no trigger/needs overlap with `ci.yml`), no
`services:` block, and that `test_social_planned_integration.py` will show
`skipped` not `error`.
