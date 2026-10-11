# Phase 6 / Slice 1 Completion — CI Pipeline: Lint, Unit Tests, Image Builds

## Summary

All 5 stories in `plan/v6/slice-1/stories.md` are implemented on
`feat/p6-s1-ci` (branched from `feat/streaming-observability`, which already
carried the "plan: phase 6 slice 1 stories" commit). Every story was
reviewed by a dedicated, independent Opus reviewer subagent before being
marked done; every finding it raised as required was fixed and re-verified.
Nothing was pushed to GitHub at any point (per the binding "never push"
constraint); all workflow validation is local (`actionlint`, `yaml.safe_load`,
and running each Makefile target's commands — and, after closing a sandbox
gap identified mid-slice, the literal `make <target>` invocations
themselves).

This slice runs in parallel with Phase 5 Slice 2 (malformed-record DLQ),
which is editing `src/cli.py`, `src/loader/edge_loader.py`,
`src/loader/node_loader.py`, `src/models/schema.py`,
`src/orchestrator/docker_service.py`, `tests/test_edge_loader.py`,
`tests/test_node_loader.py`, `tests/test_schema_validation.py`,
`tests/test_cli.py`, `tests/test_docker_service.py`, and `docs/USAGE.md`.
Per the orchestrator's parallel-work constraint, none of those files were
touched in this slice; violations inside them were left unfixed and
un-hidden (Story 1), and the docs story (Story 5) was deferred entirely to
a plan file instead of editing `docs/USAGE.md`.

## Post-slice update (2026-09-29)

Two items below were open at the time this report was first written and
have since been resolved on `feat/streaming-observability`:

- **Story 5 (`docs/USAGE.md` §13) is now delivered.** P5-S2 (malformed-record
  DLQ) merged, clearing `docs/USAGE.md` off the parallel-work exclusion list.
  The section drafted in `plan/v6/slice-1/deferred_docs.md` was inserted
  verbatim as `## 13. Continuous integration` (with the table of contents
  updated to list it) on 2026-09-29, after P5-S2's merge — see "Story 5"
  under "Approved scope delivered" below and item 2 in "Resolved deferred
  items" below, both updated in place rather than duplicated here.
- **The 7 ruff findings deferred in P5-S2's excluded files (item 1 in
  "Resolved deferred items" below) are fixed.** P5-S2's own commit
  `2575aef` ("style: fix ruff lint findings after rebasing onto
  feat/streaming-observability") applied the fixes. `make lint` now passes
  on `feat/streaming-observability`; Story 1's AC1 and Story 2's AC6 (both
  called out as unmet in "Acceptance criteria status" below) are satisfied
  as of that commit.

## Commits (on `feat/p6-s1-ci`, ahead of `feat/streaming-observability`)

```
0f2b20b feat(ci): pin a zero-reformat ruff lint baseline (P6-S1 story 1)
282d142 feat(ci): wire the blocking CI workflow — lint, unit tests, image builds (P6-S1 story 2)
59287b0 feat(ci): wire the advisory nightly/manual integration workflow (P6-S1 story 3)
77958de fix(ci): run pytest via python -m so tests can import the src package
7a0ab27 feat(ci): validate both workflows locally without pushing to GitHub (P6-S1 story 4)
f21a8a6 docs(ci): defer P6-S1 story 5 (CI docs pointer) to a plan file
```

`77958de` is not one of the five stories; it is a fix, discovered by
Story 4's own local validation, for a real bug that Stories 2 and 3
introduced (see "Bug caught mid-slice" below). It is recorded as its own
commit rather than folded silently into Story 2 or 3's history.

## Approved scope delivered

1. **Story 1 — Ruff lint baseline.** `requirements-dev.txt` (new; ruff,
   actionlint-py, pyyaml — dev/CI tooling only, not installed into any
   loader image). `pyproject.toml`'s `[tool.ruff]`/`[tool.ruff.lint]` table:
   explicit `select = ["E4","E7","E9","F"]`, a justified `ignore` list
   (E402/E701/E702/E731/E741, each with a measured hit count and
   rationale), `target-version = "py311"` (matching `Dockerfile.clock`'s
   `python:3.11-slim`). 13 genuine, behavior-preserving lint fixes applied
   in files outside the P5-S2 exclusion list. `make lint` added.
2. **Story 2 — Blocking CI workflow.** `.github/workflows/ci.yml`: `lint`
   gates (`needs: lint`) two independent jobs, `unit-tests` and
   `build-images` (a `fail-fast: false` matrix over node/edge/clock, each
   leg verifying its image's `ENTRYPOINT` via `docker inspect` after the
   build). Top-level `permissions: contents: read`; concurrency cancels
   only in-progress `pull_request` runs. Matrix legs carry an explicit
   `name: build-images (${{ matrix.name }})` for stable branch-protection
   check names. `make test-unit` / `make build-images` added, matching the
   workflow's commands exactly.
3. **Story 3 — Advisory nightly/manual integration workflow.**
   `.github/workflows/ci-integration.yml`: triggered only by `schedule`
   (nightly) and `workflow_dispatch`, no `push`/`pull_request` trigger, no
   `needs:`/`workflow_run` link to `ci.yml` — non-blocking by construction.
   No `services:` block (the repo's own `testcontainers` fixtures bring up
   and tear down their own containers). A `docker info` step plus a
   post-pytest guard that fails the job if any test's JUnit output contains
   "Docker is required" (the exact skip message `tests/conftest.py` emits
   when Docker itself is unreachable), so a Docker-infra failure can't
   silently show green by skipping every container-backed test.
4. **Story 4 — Local workflow validation.** `make validate-workflows`
   (`yaml.safe_load` + `actionlint`). Verified to have real teeth against
   two independently deliberate breakages (a `needs:` reference to a
   nonexistent job; an unknown `runs-on` label) and to fail loudly, not
   silently, when its own dependency (`actionlint-py`) is missing.
5. **Story 5 — Docs pointer (delivered 2026-09-29, after P5-S2 merged).**
   `docs/USAGE.md` was on the P5-S2 exclusion list at the time this slice's
   other four stories were implemented, so this story was deferred to
   `plan/v6/slice-1/deferred_docs.md` rather than edited in place. Once
   P5-S2 (malformed-record DLQ) merged, the drafted `## 13. Continuous
   integration` section was inserted into `docs/USAGE.md` as-is (table of
   contents updated to list it too); no existing section's content changed.

## Bug caught mid-slice (not one of the 5 stories, but load-bearing)

Story 4's fresh-venv local validation caught a real defect in the
already-committed Story 2 and Story 3 workflows before it could ever reach
a real GitHub Actions run: this repo has no `tests/__init__.py`, no root
`conftest.py`, and no installed/editable package, so `from src...` imports
inside test modules only resolve when the repo root is on `sys.path`.
`python -m pytest` gets that for free (`-m` prepends cwd); a bare `pytest`
invocation — the literal command both the Makefile's `test-unit` target and
`ci-integration.yml`'s pytest step used — does not, because pytest's
default import mode only puts each test file's own containing directory
(`tests/`) on `sys.path`, not the repo root.

Verified against the repo's own pre-existing `.venv` (not a scratch venv,
to rule out an environment fluke): bare `pytest tests examples/imdb/tests
-m "not integration and not smoke"` produced **36 collection errors**, all
`ModuleNotFoundError: No module named 'src'`; `python -m pytest` with the
same arguments passed clean (904 passed, 17 deselected). Same pattern for
`pytest tests -m integration`. Unfixed, this would have failed the blocking
`unit-tests` job and the advisory `integration` job on a real runner with
zero tests actually run — a worse outcome than the deferred lint findings,
since it is not advisory.

Fixed in `77958de`, two layers: `Makefile`'s `test-unit` and
`ci-integration.yml`'s pytest step now invoke `python3 -m pytest`
explicitly, and `pyproject.toml`'s `[tool.pytest.ini_options]` gained
`pythonpath = ["."]` as defense in depth (the reviewer's suggestion), so a
future bare-`pytest` invocation — in a Makefile edit, a CI step, or a
contributor's shell — can't silently reintroduce the same failure. Both
bare `pytest` and `python -m pytest` now behave identically. Re-verified
against the repo `.venv` and a fresh venv after the fix: 904 passed, 17
deselected either way; `ruff check` still reports exactly the 7 findings
deferred below (unaffected); `actionlint` still exits 0 on both workflow
files.

## Acceptance criteria status

- **Now met (was "unmet until deferred item 1 resolves"; see "Resolved
  deferred items" below):** Story 1's AC1
  (`ruff check` exits 0 on the tree as of this story's own commit) and
  Story 2's AC6 ("`main`'s current tree passes lint"). Both were red because
  of the 7 findings in P5-S2's excluded files; P5-S2's commit `2575aef`
  fixed them, and `make lint` now passes on `feat/streaming-observability`
  (see "Post-slice update" above).
- **Checked by construction and local equivalents, not by a real GitHub
  Actions run** (nothing was pushed): Story 2's ACs 1-5 (skip-on-lint-
  failure, `unit-tests`/`build-images` not serialized with each other,
  independent matrix legs, an `ENTRYPOINT` change failing only that leg,
  superseded-push cancellation) and all of Story 3's ACs. `actionlint`
  plus the literal `make` runs are strong evidence the YAML is correct, but
  they are not a substitute for GitHub's own scheduler, matrix runner, and
  concurrency-group behavior actually executing it.
- **Now met (was "unmet by design"):** Story 5's DoD line "`docs/USAGE.md`
  §13 exists" — deliberately not done in this slice at the time, applied
  2026-09-29 after P5-S2 merged; see "Resolved deferred items" below and
  "Post-slice update" above.

## Deferred items (binding, orchestrator-accepted, not hidden)

1. **Story 3's DoD asks for "one recorded manual dispatch run"** (screenshot
   or run URL). Not obtainable from this sandboxed environment, since
   nothing is ever pushed to GitHub — and, independent of that, GitHub only
   honors a workflow's `schedule` trigger and only offers manual dispatch
   once the workflow file is on the repository's default branch, so this
   can only happen after `ci-integration.yml` reaches `main`. Recorded as
   an open item rather than fabricated.

2. **Repo-admin follow-ups (cannot be set from a workflow file):**
   - Branch protection on `main` must list `lint`, `unit-tests`, and each
     `build-images (node|edge|clock)` matrix leg as required status checks,
     or Story 2's gate is advisory in practice despite the YAML.
   - `integration` (the advisory workflow's job) must **never** be added as
     a required status check — it never runs on `push`/`pull_request`, so a
     required check would leave every PR pending indefinitely. This is the
     one way this workflow could end up blocking a PR, and it happens in
     admin settings, not YAML.
   - GitHub auto-disables scheduled workflows after 60 days with no repository
     activity (public repos) — worth a reminder if the repo goes quiet.

3. **Story 3's Docker-availability guard was validated against synthetic
   JUnit XML fixtures, not a real integration run.** Running the real
   `pytest tests -m integration` suite was intentionally skipped in this
   sandbox: it would pull `neo4j:5.21-enterprise`, and this environment has
   only ~4.8GB free disk. The guard logic (`! grep -q "Docker is required"
   integration-results.xml`) was instead verified against two hand-built
   XML fixtures — one containing the Docker-unreachable skip message
   (guard correctly fails), one containing only the legitimate
   `GRAPH_LOADER_IT_*`-unset skip message (guard correctly passes).

4. **`ruff format` / a repo-wide reformat is deliberately not configured**
   (no `[tool.ruff.format]`, no CI step) — `ruff format --check` reports 83
   files would be reformatted today; enabling it is exactly the
   repository-wide reformat `plan/v5-v6-orchestration.md` rules out for
   this slice.

## Resolved deferred items

Both items below were originally listed in "Deferred items" above; they are
recorded here instead, with their resolution, rather than left in that list.

1. **7 ruff findings in P5-S2's excluded files** — originally left unfixed
   and un-suppressed (no `noqa`, no per-file ignore) in `src/cli.py:94`
   (F401, `DEFAULT_PLAN_WRITERS`), `src/cli.py:1511` (F541, no-placeholder
   f-string), `src/loader/edge_loader.py:30` (F401, `PropertyConfig`),
   `src/loader/edge_loader.py:728` (F841, unused `suffix`),
   `tests/test_edge_loader.py:33` (F401, `ControlDeliveryError`), and
   `tests/test_schema_validation.py:25,27` (F401, `IndexConfig` /
   `MixAndBatchConfig`). This meant `ruff check src tests examples` — and
   therefore `ci.yml`'s `lint` job — would fail on every push until fixed,
   with `unit-tests`/`build-images` showing as **skipped** (both carry
   `needs: lint`) for the same window.
   **Resolved:** P5-S2's own commit `2575aef` ("style: fix ruff lint
   findings after rebasing onto feat/streaming-observability") applied all
   7 fixes. `make lint` now passes on `feat/streaming-observability`
   (verified 2026-09-29). Story 1's AC1 and Story 2's AC6 are satisfied.

2. **Story 5 (`docs/USAGE.md` §13) was fully deferred** to
   `plan/v6/slice-1/deferred_docs.md`; no edit was made to the real
   `docs/USAGE.md` in this slice because it was on P5-S2's excluded-file
   list at the time.
   **Resolved:** P5-S2 (malformed-record DLQ) merged, clearing
   `docs/USAGE.md`. On 2026-09-29, after that merge, the drafted `## 13.
   Continuous integration` section was inserted into `docs/USAGE.md`
   verbatim (table of contents updated to list it), with no change to any
   existing section's content.

## Verification (evidence)

All commands below were run locally; nothing was pushed to GitHub.

```text
# Story 1 — ruff, throwaway /tmp venv, requirements-dev.txt only
$ ruff check src tests examples
Found 7 errors.   # exactly the 7 deferred findings above

# Independence of the 13 fixes: ruff against the pre-slice tree (d666144's
# src/tests/examples) with the *new* pyproject.toml ruff config applied
# reports exactly 20 findings; the current tree reports exactly 7. The
# difference is exactly the 13 fixed findings, each a distinct name or
# line, which is what shows they are independent rather than one fix
# accidentally masking another. (Only one fix, the `first` -> `_first`
# rename in tests/test_planned_dispatcher.py, was individually spot-
# reverted as a smoke test: reverting it alone reintroduces exactly the
# one F841 finding on `first` and nothing else.)

# Story 2/4 — literal `make` invocations (env PATH=<venv>/bin:/usr/bin:/bin)
$ make lint
... (7 deferred findings) ...
make: *** [Makefile:13: lint] Error 1        # exit 2, expected

$ make test-unit
===================== 904 passed, 17 deselected in 15.27s ======================
# (exit 0)

$ make build-images
... three docker build sequences complete ...
# (exit 0); docker inspect confirmed each image's ENTRYPOINT:
#   graph-loader-node:local  -> ["python","-m","src.loader.node_loader"]
#   graph-loader-edge:local  -> ["python","-m","src.loader.edge_loader"]
#   graph-loader-clock:local -> ["python","-m","src.orchestrator.clock_runner"]
# then: docker rmi <all three> && docker image prune -f (disk restored to ~4.8G free)

$ make validate-workflows
# (exit 0, silent on both the yaml.safe_load step and actionlint)

# Story 4 — actionlint teeth checks (throwaway scratch dirs, deleted after)
$ actionlint   # against ci.yml with `needs: lint` -> `needs: lintt`
.github/workflows/ci.yml:26:3: job "unit-tests" needs job "lintt" which does not exist ... [job-needs]
.github/workflows/ci.yml:41:3: job "build-images" needs job "lintt" which does not exist ... [job-needs]
# exit 1

$ actionlint   # against ci.yml with `runs-on: ubuntu-latest-typo`
.github/workflows/ci.yml:16:14: label "ubuntu-latest-typo" is unknown ... [runner-label]
(same at lines 31, 47)
# exit 1

$ actionlint   # against the real ci.yml + ci-integration.yml as committed
# (exit 0, no output)

# Story 4 — missing-dependency check
$ pip install pyyaml   # only, no actionlint-py
$ make validate-workflows
/bin/bash: actionlint: No such file or directory
# exit 127 — loud failure, not a silent skip

# Full unit suite, repo .venv, after all fixes
$ python -m pytest tests examples/imdb/tests -m "not integration and not smoke" -q
904 passed, 17 deselected in ~15s
```

## Reviewer

A single dedicated, independent Opus reviewer subagent was launched at the
start of this slice and stayed engaged for its entire lifespan, reviewing
every story's plan and diff before it was marked done. Every one of its
required findings was applied and re-verified before commit:

- Story 1: `target-version` corrected from `py312` to `py311` (matches
  `Dockerfile.clock`); three inaccurate/stale `pyproject.toml` comments
  corrected (select-comment wording, E402 hit count/rationale, E741 line
  reference).
- Story 2: job-level `name: build-images (${{ matrix.name }})` added (branch
  protection check-name stability); `permissions: contents: read` added;
  `cancel-in-progress` scoped to `pull_request` only.
- Story 3: a post-pytest guard added to fail the job if any test was
  skipped because Docker itself was unreachable; `permissions: contents:
  read` added.
- Story 4: `pyyaml>=6.0` added to `requirements-dev.txt` so the dev-tooling
  target doesn't silently depend on the production requirements file; the
  literal-`make` reproducibility gap closed.
- Story 5: the drafted section's `docs/USAGE.md` §2/§4/§10 cross-references
  (copied from the story spec itself) pointed at sections that don't
  actually contain the referenced content — corrected to point at
  `README.md` and `examples/social/README.md`'s actual sections instead;
  the "skips cleanly if Docker is unreachable" wording was corrected to
  also state the opposite behavior in CI (a Docker-unreachable skip is
  treated as a job failure by Story 3's guard, not silently accepted).
- Cross-story: the bare-`pytest` `sys.path` bug (see above) was missed in
  the reviewer's own Story 2 and Story 3 reviews — both passed with the
  bug present. It was Story 4's fresh-venv local validation that caught
  it. Once raised, the reviewer independently reproduced it against the
  repo's `.venv` and suggested the `pythonpath = ["."]` defense-in-depth
  addition alongside the `python3 -m pytest` fix.

No **required** finding was overridden. The one point initially raised as
"blocking" (the 7 deferred lint findings keeping `lint` red) was, on
discussion, an orchestrator-accepted, explicitly instructed tradeoff rather
than something in this First Mate's discretion to resolve — the reviewer
accepted that framing once given the same brief and asked only that it be
recorded prominently here, which it is (see "Deferred items" §1).

Two **recommendations** were declined rather than applied, and are recorded
as known costs rather than silently dropped:
- Decoupling `unit-tests`/`build-images` from `needs: lint` (so they would
  still produce a signal while lint is red). Declined because Story 2's own
  AC1 requires the skip-on-lint-failure behavior; the consequence is the
  same one already covered in "Deferred items" §1.
- Scoping the `push:` trigger to specific branches to avoid a same-repo PR
  branch running CI twice (once for the push, once for the PR, since they
  land in different concurrency groups). Declined because the story text
  says "every push" without qualification; left as an optional, known
  double-run cost.

## Known follow-ups / out of scope

- `requirements.txt` pins minimum versions only (`>=`), so each CI run can
  resolve newer package versions than whatever happens to be installed in
  a given local `.venv`, creating a possible drift source. This predates
  this slice and is not something Slice 1 changes.
- The `unit-tests` job (and this slice's local validation) runs against
  Python 3.12 only (`.python-version`), while `Dockerfile.clock` builds on
  `python:3.11-slim`. Story 1's `target-version = "py311"` covers the lint
  rule set's version-awareness, but nothing in this slice actually runs
  the unit suite under 3.11 to catch a 3.12-only construct that would
  break the clock image at import time.

## Sign-off

All 5 stories implemented, reviewed, and locally validated; the one
cross-cutting defect found during validation (bare-`pytest` import failure)
was fixed and re-verified before this report was written. Deferred items
are listed above with their resolution owners. Nothing was pushed to
GitHub.
