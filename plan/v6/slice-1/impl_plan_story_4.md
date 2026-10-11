# Phase 6 / Slice 1 / Story 4 — Implementation Plan

## Story

Validate both workflows locally without pushing to GitHub
(`plan/v6/slice-1/stories.md`, Story 4).

## Files to add/edit

1. **`Makefile`** — add `validate-workflows`:
   ```makefile
   .PHONY: validate-workflows
   validate-workflows:
   	python3 -c "import yaml, glob; [yaml.safe_load(open(f, encoding='utf-8')) for f in glob.glob('.github/workflows/*.yml')]"
   	actionlint
   ```

## Validation plan (this story IS validation)

- Use the `/tmp` venv from Story 1 (already has `actionlint-py` pinned) —
  never the repo `.venv`, per setup constraints.
- Negative check: copy `ci.yml` to a scratch file with a deliberately broken
  reference (`needs: lintt`) and confirm `actionlint` reports the specific
  error and a non-zero exit — proves the check has teeth. Delete the scratch
  file after.
- Positive check: run `actionlint` against the real `.github/workflows/*.yml`
  as authored in Stories 2-3; must exit 0 with no output.
- Run `make lint && make test-unit && make build-images` in a freshly
  created `/tmp` venv (`python3.12 -m venv` + `pip install -r
  requirements.txt -r requirements-dev.txt -r examples/imdb/requirements.txt`)
  with Docker available and no `.env` file in view. `build-images`'s
  `docker build` calls use the Makefile's own `:local` tags per Story 2 —
  after confirming they succeed I will `docker rmi` them and run `docker
  image prune -f` immediately (disk is ~5GB free), matching the setup
  instructions to use `:ci-local` for *my own* ad hoc manual validation
  builds and to always clean up, not to leave `:local`-tagged images
  sitting on the runner's disk.
- Confirm deleting `actionlint-py`'s pin and rerunning `make
  validate-workflows` fails loudly (ModuleNotFoundError / command not
  found), not silently — then restore the pin.
- Capture the exact terminal output (ruff clean, pytest's final summary
  line, actionlint's silent-success exit code) to paste into the completion
  doc, since there is no CI run URL available from this sandboxed
  environment.

## Reviewer touchpoint

Send the reviewer the captured output and the Makefile diff; ask for a final
sign-off that local validation substitutes credibly for a real Actions run
given the "never push" constraint.
