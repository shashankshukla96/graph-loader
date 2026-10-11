# Story 8 implementation plan: CI wiring

Additive-only changes:

- `pyproject.toml`: the `stress` marker (already added during Story 4's
  fixes, confirmed present -- this story only verifies it, per the
  reviewer's sign-off that adding it early was fine).
- `.github/workflows/ci-integration.yml`: the existing `integration` job's
  `timeout-minutes` bumped from 45 to 60 (measured: the 5 new 100k-tier
  tests add ~110-125s; the bump keeps the original budget for the rest of
  the suite plus generous headroom); one new job,
  `star-topology-stress-1m`, `if: always()` (independent of `integration`),
  `timeout-minutes: 25` (measured 1M run: 79.07s locally; CI runners are
  markedly slower and shared, hence the large multiple), running only
  `test_1m_edges_into_one_hub_planned_mode_zero_lock_waits` with
  `GRAPH_LOADER_STRESS_SCALE=1000000` and the same "Fail if Docker-backed
  tests were skipped" guard Slice 1 already established for `integration`.
  No new trigger block: this job inherits the file's existing `schedule`/
  `workflow_dispatch` triggers, so it never gates a PR (P6-Q2).
- `Makefile`: `stress-100k`/`stress-1m` targets appended at the end (after
  `validate-workflows`); `.PHONY` line gains both names.

## Local validation (no push, no shared/named stack)

1. `make validate-workflows` -- YAML parse + `actionlint` against every
   file under `.github/workflows/`.
2. Teeth-check: introduce a bad `needs:` value in the new job
   (`needs: this-job-does-not-exist`), confirm `actionlint` reports
   `job-needs`, then revert.
3. `pytest --collect-only -m stress` (no Docker) -- lists all 6 Story 4-7
   tests, no collection error.
4. `make stress-100k` against scratch containers.
5. `make stress-1m` against scratch containers, once, for a real measured
   wall-clock (already obtained while implementing Story 7: 79.07s).
6. `git diff .github/workflows/ci-integration.yml` -- confirm the
   `integration` job's only changed line is `timeout-minutes`.
7. `make -n stress-100k` / `make -n stress-1m` -- each prints exactly one
   recipe line, no name collision with an existing target.

## Residual note

`tests/conftest.py::kafka_bootstrap` still `pytest.skip`s on a Kafka
startup `TimeoutError` (pre-existing; this slice's file-ownership rule
forbids editing it) -- flagged, not fixed, as a green-by-skip risk for the
`integration`/`star-topology-stress-1m` jobs specifically if Kafka itself
ever fails to start on a runner. Recorded in
`plan/v6/completion_slice-4.md`.
