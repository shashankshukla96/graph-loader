# Next-agent handoff — prerequisite evidence and release decisions

Prepared 2026-10-16. Copy the prompt below into the next agent's task. All paths
inside the prompt are relative to the repository root unless stated otherwise.

---

Work in `/Users/shashank/Desktop/Code/Graph/graph-loader`. Complete the next
bounded library implementation slice: **E00 (baseline and DLQ proof)** and
**E01 (user-approved release decisions)**. Follow the SDLC workflow below,
including independent review and correction loops. Do not start E02–E12 in
this assignment. Prepare an evidence-based handoff for E02 when prerequisites
are complete; if a decision or environment remains unavailable, state the exact
remaining gate and continue work that does not depend on it.

1. **Establish context and preserve the working tree.**

   Read applicable `AGENTS.md` instructions first, then inspect the actual current
   branch, HEAD, staged/unstaged diffs and relevant untracked files. At handoff,
   branch is `codex/pypi-library-checklist`, HEAD is
   `d53a92cfef652b7aae5aca6472736c10e851db42`. This is a dirty checkout containing
   required work. Do not reset, discard, clean, stash away or overwrite it.
   A checkout of HEAD alone omits necessary prerequisite fixes, fixtures and plan.
   Stay in this checkout unless isolation is needed; a new worktree does not copy
   this uncommitted state, so account for all relevant contents before moving.

   Read these authoritative records:

   - `plan/library-implementation/README.md` and `backlog.json`;
   - `epics/e00.md`, `epics/e01.md` and `decisions.md` under that directory;
   - `cancellation-contract.md`, `ordering-matrix.md`, `test-inventory.json`;
   - `reviews/fresh-audit-01.md` and `reviews/fresh-audit-02.md`;
   - `docs/PYPI_LIBRARY_CHECKLIST.md`, `docs/PYPI_CHECKLIST_REVIEW.md`;
   - the current source/tests referenced by E00/E01, not just the prose claims.

   The canonical plan has 13 epics, 26 features, 54 stories and covers 48 checklist
   IDs. All epic decompositions are independently approved. Implementation
   statuses remain not started. Review approval is not execution evidence or
   acceptance of product decisions. D01–D07 are unresolved.

2. **Understand the changes already present.**

   Twelve tracked files have uncommitted prerequisite changes:
   `.gitignore`, `docs/PYPI_CHECKLIST_REVIEW.md`,
   `docs/PYPI_LIBRARY_CHECKLIST.md`, `docs/USAGE.md`, `src/cli.py`,
   `src/loader/edge_planner.py`, `src/orchestrator/planned_edges.py`,
   `src/utils/schema_loader.py`,
   `tests/integration/test_multi_topic_relationship.py`,
   `tests/test_auto_settings.py`, `tests/test_cli.py`,
   `tests/test_external_planner.py`.

   Preserve the untracked `tests/test_library_preflight.py`,
   `tests/fixtures/__init__.py`, `tests/fixtures/multi_topic_arrival.py` and the
   entire `plan/` deliverable, including this prompt. Recheck this inventory;
   it is a starting snapshot, not permission to overwrite newer user work.

   Existing behavior changes include unique planner spill directories, exclusive
   output locks, owned stale-output reset, closed-planner rejection, startup
   cleanup, source/settings validation before Kafka side effects, convenience
   topic ownership/cleanup, preserved rejection evidence, wrapped schema I/O
   errors and moving logging setup into the CLI entry function. The arrival
   dataset moved out of a removed planning script into maintained test fixtures.
   A CLI unit test now mocks its Neo4j connection attempt.

   The proposed nonblocking framed worker transport and public cancellation API
   are **not implemented**. They belong to E04 after D02 approval. Fresh-review
   fixes FA01–FA04 are plan corrections; do not confuse them with runtime fixes.
   Preserve the S5 private-transport → S3 public-integration → S4 cleanup test
   allocation when later work reaches E04.

3. **Plan the slice and define acceptance before coding.**

   Make a small execution checklist using the exact E00/E01 story IDs. Record
   prerequisites, acceptance criteria, affected files, risks, test/build commands
   and rollback/cleanup ownership. Inspect current implementation and fixture
   behavior to identify gaps before changing code. Avoid repeating fixes already
   present or enlarging public scope to resolve a prerequisite defect.

   E00 can proceed immediately. Begin E01's read-only identity/dependency/scope
   investigation alongside it. Prepare concrete, sourced choices before asking
   the user for the unresolved decisions. Ask early enough to avoid idle time;
   continue independent E00 work while waiting. User approval of D01–D07 is
   required by the approved E01 exit gate; proposed defaults and silence are
   not approval. Routine reversible implementation/test repairs do not need
   repeated confirmation.

   If discovery changes scope, guarantees, dependency ordering or story sizing,
   update `backlog.json` and affected contracts, regenerate the plan and obtain
   independent approval of affected epics before the expanded work. Do not
   weaken acceptance or silently drop a failure case to fit an estimate.

4. **Execute E00.F1.S1: establish a reproducible baseline.**

   Create a safe source/environment manifest as specified by the story: HEAD,
   staged/unstaged binary diff hashes and allowlisted per-file path/size/SHA-256
   covering relevant tracked source/config/tests/build inputs AND the untracked
   preflight tests and fixture package. Include applicable example tests and
   requirements. Exclude `.env`, secrets, ignored datasets and private logs;
   do not recursively archive all untracked files or hash evidence into itself.
   Use a temporary copy to demonstrate that changing an included untracked
   fixture changes the manifest identity without altering the real fixture.

   Record interpreter, dependency versions, relevant nonsecret configuration,
   exact commands, exit codes, counts, skip reasons and warnings. Historical
   results cannot be rebound to today's identity by assumption. Reuse only
   demonstrably identical source/environment evidence; otherwise rerun affected
   suites, including a fresh full baseline where identity cannot be established.

   `.venv/bin/python` exists. Verify it rather than assuming it remains usable.
   If rebuilding the environment, use the repository's actual
   `requirements-dev.txt` (which includes runtime and
   `examples/imdb/requirements.txt` dependencies). Keep credentials out of output.

   Required baseline commands from the repository root:

   ```bash
   .venv/bin/python -m ruff check src tests examples
   .venv/bin/python -m pip check
   .venv/bin/python -m pytest tests examples/imdb/tests -m "not integration and not smoke" -q
   git diff --check
   ```

   Explain each inherited unit skip individually. Record warnings and ownership
   of deferred work rather than hiding them. If fixing a defect, first reproduce
   it with a focused regression, apply the smallest repair, rerun that test and
   affected regressions, and refresh source identity/full evidence as required.

5. **Execute E00.F2.S1: build/run the isolated real-service DLQ proof.**

   Inspect the exact test and its fixture dependency closure first. Verify local
   Docker availability and provision the throwaway Neo4j/Kafka services using
   the existing testcontainers fixtures. Record actual image identities and
   environment prerequisites. Audit any image/build inputs affected by repairs;
   rebuild and smoke-test affected runtime images if the change requires them.
   Do not claim a package build: distribution metadata and installed artifacts
   are future E02+ work. Mark a build step N/A with its reason when this slice
   changes no build output; fixture service images still need recorded identity.

   Run:

   ```bash
   .venv/bin/python -m pytest tests/integration/test_multi_topic_relationship.py::test_dead_letter_arrival_stream_type_wide_dlq_drained_by_the_retry_consumer -m integration -v
   ```

   This exact test must execute and pass using the maintained arrival fixture,
   without restoring `plan/v7` helpers. A Docker/service skip is an unmet gate.
   Diagnose failures, add meaningful focused coverage, repair, rerun the exact
   case and affected tests. Use bounded supervision for hangs and collect safe
   diagnostics. Clean up only test-owned children, containers, topics and files
   on success/failure. If infrastructure prevents execution, record the precise
   blocker and leave E00/B3 open; unit tests and the IMDb run cannot replace it.

6. **Execute E01: resolve the release contract with the user.**

   Follow its story dependencies: E01.F1.S1 → E01.F1.S2 → E01.F2.S1 → E01.F2.S2.
   Verify dated PyPI/TestPyPI availability and local executable collisions,
   inspect current API/wire-format/retry behavior, inventory eager imports and
   dependency boundaries, and prepare a feasible tested-platform proposal.
   Browse official/primary sources for external facts and current availability;
   an availability check does not reserve a name or prove account ownership.

   Present concrete choices for D01 identity/public ownership, D02 API/CLI scope
   and ordering/cancellation guarantees, D03 alpha/version/compatibility policy,
   D04 supported environments, D05 base/extras/Rust/version pairs, D06 image
   delivery/ownership, and D07 backend/version source/manual publication.
   Record explicit user decisions, date, owner, evidence and affected stories
   in `decisions.md`. Do not obtain public contact details from local Git config
   or store private contacts/tokens in tracked evidence. Broader promises need
   new bounded stories and independent review, not silent implementation.

7. **Review, repair, verify, and keep evidence current.**

   For this assignment, use an independent internal subagent to review the
   actual final code/test/docs diff and E00/E01 evidence. Give it a zero-history
   start (`fork_turns: "none"` when supported), repository location, scope,
   canonical plan and evidence paths. Ask it to form its own findings from code
   and acceptance criteria. Have it check correctness, ownership/cleanup,
   test adequacy, hidden skips, source identity and decision provenance. Do not
   ask it to approve based only on summaries or existing review labels.

   Resolve findings, update focused tests, rerun the appropriate tests/builds,
   refresh manifests and evidence, and request re-review of the final revision.
   Preserve original reports and record resolutions. Repeat until no blocking
   findings remain; optional findings need an explicit disposition. Reviewer
   approval does not replace actual execution or user decisions. An unavailable
   service or missing user decision stays open even if the reviewer approves
   the changes that could be completed.

   After plan/status updates, run:

   ```bash
   python3 plan/library-implementation/tools/render_plan.py
   git diff --check
   ```

   Validate local document links and generated-file consistency. Keep generated
   epic/traceability content synchronized through the canonical backlog. Mark
   stories complete only after their evidence and prerequisites pass. Avoid
   broad/repeated tests once applicable checks pass unless further changes or
   unresolved findings require them.

8. **Close with documentation, acceptance, and the next handoff.**

   Create safe evidence records at
   `plan/library-implementation/evidence/E00.F1.S1.md`,
   `E00.F2.S1.md` and one record per executed E01 story. Link manifests/log
   summaries and review reports; retain large operational logs outside tracked
   evidence. Update B3/checklist statuses only when the exact gate passes.

   Report what changed, why, test/build results, skipped/blocked gates, reviewer
   verdict and final source/environment identity. Explain remaining risk and
   how to resume. If E00 and all user-approved E01 decisions pass, prepare the
   next handoff for E02 (distribution foundation), listing resolved parameters
   and acceptance commands. Do not implement E02 as part of this assignment.
   If prerequisites remain open, hand off those exact tasks instead of claiming
   packaging readiness. No release/deployment is applicable to E00/E01.

Persistent constraints and known evidence:

- **Do not push anything.** Do not upload packages/images, create accounts,
  publish, merge, add release workflows or alter the existing no-workflows
  policy. This assignment requests no commit or release tag; leave changes
  reviewable in the working tree. Later external actions require their own
  explicit user instruction under E11/E12.
- Preserve the loaded remote IMDb graph and services at `192.168.1.75`. Do not
  clear that database, run tests against it, prune shared Docker resources or
  repeat the large IMDb load. Existing `.env` is ignored and may point at remote
  systems; do not print it, bulk-source it for testcontainers or let its service
  settings override isolated fixtures. Current reachability is not verified by
  this handoff. Never remove a planner lock without confirming its owner stopped.
- Historical 2026-10-15 evidence: 3,366 unit/example passes, 4 skips, 49 deselected;
  406 focused passes; 19 existing fork warnings. B3 was unrun. The successful
  IMDb seed-10,000/depth-4 run had 1,247,026 nodes, 4,510,991 distinct relationships
  and 1,780 verified chunks / 4,644,708 input rows. The resumed load took 422.618
  seconds, excluding preparation/troubleshooting. Local ignored detail:
  `examples/imdb/reports/imdb-preflight-verification-20261015.md`.
- Fresh handoff checks on 2026-10-16: `tests/test_library_preflight.py` **15 passed**;
  Ruff, `pip check`, plan renderer and `git diff --check` passed. The full suite,
  real-service B3 test, package builds and remote deployment were **not rerun**
  for this handoff. These narrow checks do not satisfy the full E00 gate.
- Keep useful progress updates concise and regular. Ask only for missing
  decisions or genuinely required information; continue independent work while
  waiting. Never imply that review of a plan proves its future runtime behavior.
