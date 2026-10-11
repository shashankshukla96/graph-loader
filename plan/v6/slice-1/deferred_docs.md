# Deferred: `docs/USAGE.md` §13 — Continuous integration

Phase 6 / Slice 1 / Story 5 (`plan/v6/slice-1/stories.md`) asks for a new
`## 13. Continuous integration` section appended after `docs/USAGE.md`'s
existing `## 12. Limitations` (which currently ends at line 631).
`docs/USAGE.md` is on the parallel Phase 5 Slice 2 developer's excluded-file
list for this slice, so this story is deferred entirely rather than edited
in place. The text below is ready to paste in as-is once P5-S2 merges (or
once the orchestrator otherwise clears `docs/USAGE.md` for this slice); no
further drafting should be needed, only insertion and a final proof-read
against whatever else P5-S2 added to the file in the meantime.

## Section text to add

```markdown
## 13. Continuous integration

Every push and pull request runs `.github/workflows/ci.yml`: a `lint` job
gates two independent jobs, `unit-tests` and `build-images` (a fail-fast:false
matrix building the node, edge and clock images — checks named
`build-images (node)`, `build-images (edge)`, `build-images (clock)` — and
verifying each one's `ENTRYPOINT`). A lint failure skips both dependent
jobs rather than failing them, so CI minutes aren't spent running a suite
whose lint gate has already failed. `make lint` needs
`pip install -r requirements-dev.txt` (not just `requirements.txt`).

Separately, `.github/workflows/ci-integration.yml` runs the
`testcontainers`-backed integration suite nightly (03:00 UTC) and on manual
`workflow_dispatch` only — it is advisory and never blocks a pull request.
It needs nothing beyond what a stock GitHub-hosted runner already provides:
no scratch Kafka/Neo4j stack, no secrets, no `services:` block (the suite's
own fixtures start and tear down their own containers). Locally, those
fixtures skip cleanly if Docker is unreachable; in the nightly job that
same skip is treated as a failure (a `docker info` step runs first, and a
guard step fails the job if any test's result contains "Docker is
required"), so a green nightly run means the containers actually ran, not
that they were silently skipped. Results are uploaded as the
`integration-results.xml` artifact (14-day retention) regardless of
pass/fail.

Every job has a matching local command, so a red check tells you exactly
what to run before pushing again:

| CI job | Local command |
| --- | --- |
| `lint` | `make lint` |
| `unit-tests` | `make test-unit` |
| `build-images` | `make build-images` |
| `integration` (advisory) | `python3 -m pytest tests -m integration` (needs Docker) |
| workflow YAML validity | `make validate-workflows` (also needs `requirements-dev.txt`) |

`tests/test_social_planned_integration.py` keeps skipping in the advisory
job exactly as it does locally without a named `GRAPH_LOADER_IT_*` scratch
stack — see `README.md`'s "Worked example: `examples/social`" section and
`examples/social/README.md`'s "Comparing modes" section; the exact
variables are read in `tests/test_social_planned_integration.py`.
```

## Notes for whoever applies this

- Confirm the section number is still `13` (i.e. no other slice has appended
  a `##` section between `## 12. Limitations` and this insertion in the
  meantime); renumber if needed.
- The local-command table above should be re-verified against whatever
  Makefile targets exist at merge time — as of this slice, `make lint`,
  `make test-unit`, `make build-images` and `make validate-workflows` all
  exist and were exercised locally (see `plan/v6/completion_slice-1.md`).
- `git diff` for the eventual real edit should touch only new lines
  appended after the existing `## 12. Limitations` section, per the
  story's own acceptance criterion that no existing content changes.
