# Phase 6 / Slice 1 / Story 2 — Implementation Plan

## Story

Wire the blocking CI workflow — lint, unit tests, image builds
(`plan/v6/slice-1/stories.md`, Story 2).

## Files to add/edit

1. **`.github/workflows/ci.yml`** (new) — three jobs per the story spec:
   - `lint`: checkout, `actions/setup-python@v5` with
     `python-version-file: .python-version`, pip cache keyed on
     `requirements.txt` + `requirements-dev.txt`, `pip install -r
     requirements-dev.txt`, `make lint`.
   - `unit-tests` (`needs: lint`): checkout, setup-python cache keyed on
     `requirements.txt` + `examples/imdb/requirements.txt`, `pip install -r
     requirements.txt -r examples/imdb/requirements.txt`, `make test-unit`.
   - `build-images` (`needs: lint`): matrix of node/edge/clock, each building
     its Dockerfile with `docker/build-push-action@v6` (`push: false, load:
     true`, tag `graph-loader-<name>:ci`, GHA layer cache scoped per image),
     then a "Verify entrypoint" step running `docker inspect ... --format
     '{{json .Config.Entrypoint}}'` and comparing to the expected
     entrypoint array for that image — this is the load-bearing check the
     story calls out (a broken `ENTRYPOINT` would otherwise not fail CI).
   - Top-level `on: [push, pull_request]` and a `concurrency` block
     (`group: ci-${{ github.workflow }}-${{ github.ref }}`,
     `cancel-in-progress: true`).
   - No `permissions:` block needed beyond default (read-only checkout); no
     secrets used anywhere in this workflow.
2. **`Makefile`** — add `test-unit` and `build-images` targets whose commands
   are character-for-character what `ci.yml` runs, so local and CI never
   drift:
   - `test-unit: pytest tests examples/imdb/tests -m "not integration and not smoke"`
   - `build-images`: three `docker build -f Dockerfile.<x> -t
     graph-loader-<x>:local .` lines (local tag, distinct from CI's `:ci`
     tag and from my own manual `:ci-local` validation tag — never `:latest`
     anywhere).

## Not in scope for this story (excluded files / other stories)

- No change to `docs/USAGE.md` (Story 5, deferred).
- No change to `src/cli.py`, `src/loader/edge_loader.py`,
  `src/loader/node_loader.py`, `src/models/schema.py`,
  `src/orchestrator/docker_service.py`, or any of the excluded test files.
- `.github/workflows/ci-integration.yml` is Story 3, not touched here.

## Validation plan

- `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))"`
  — cheap syntax sanity check now; full `actionlint` run is Story 4's
  Makefile target, but I'll run the pinned `actionlint-py` ad hoc here too
  since it's already installed in the /tmp lint venv, to catch mistakes
  immediately rather than waiting for Story 4.
- Run `make test-unit` and `make build-images` locally (repo .venv for the
  former; `docker build` with the `:ci-local` tag substituted in place of
  `:local` for a one-off local check, removed afterward + `docker image
  prune -f`, per the setup constraints — the `:local` tag itself is only
  ever produced by the real `make build-images` a contributor runs, not by
  me in this sandboxed validation pass).
- Confirm the "Verify entrypoint" logic against the three Dockerfiles' actual
  `ENTRYPOINT` lines (already read: node -> `["python","-m",
  "src.loader.node_loader"]`, edge -> `["python","-m",
  "src.loader.edge_loader"]`, clock -> `["python","-m",
  "src.orchestrator.clock_runner"]`).
- Send the reviewer the workflow YAML in full plus this plan before marking
  the story done; check especially: is `needs: lint` on both dependent jobs
  correct so a lint failure skips both, does the matrix's `fail-fast: false`
  keep legs independent, is the concurrency group correct so superseded
  pushes on the same PR ref get cancelled, is caching keyed on the files
  that actually determine the resolved dependency set.
