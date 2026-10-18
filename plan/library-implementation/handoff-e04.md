# E04 next-agent handoff — public API and bounded planner lifecycle

Updated 2026-10-18. Give this entire document to the next implementation agent.
**Execute E04 only after the linked fresh E03 verification is accepted.** Follow
the complete workflow and acceptance gates below; do not implement E05–E12.
No upload, GitHub push or release is authorized.

## Current prerequisite verification

[E03 completion verification on 2026-10-18](reviews/e03-completion-verification-20261018.md)
accepts the actual committed migration after fresh tests/builds and the
[zero-context source review](reviews/e03-revalidation-independent-20261018.md).
Fresh results: **3,387 tests passed**, four inherited skips, 49 deselections and
19 known warnings; exact B3 **one pass without a skip**; isolated original/
rebuilt artifacts, two clean installs and all four new-image matrices passed.
The [exact evidence](evidence/e03-revalidation-20261018.json) binds these results
to source/environment and artifact IDs. The
[independent handoff/tracker review](reviews/e04-handoff-tracker-review-20261018.md)
must also approve before this prompt is delivered as accepted. A pending or
changes-requested review is an open gate, not authorization to claim completion.

## Preserve and inspect the actual checkout

Repository `/Users/shashank/Desktop/Code/Graph/graph-loader`, branch
`codex/pypi-library-checklist`. Read current HEAD/status; E03 began at `6de3562`
with required E00–E02 dirty/untracked implementation. The E03 entry and final
manifests distinguish inherited fixes from the namespace move. Preserve all
source/test/fixture/plan work; never clean/stash/reset to HEAD alone. No
AGENTS.md was present at E03 entry; recheck before work. Keep `.env` private,
never source remote settings into isolated tests, never touch IMDb at
`192.168.1.75`, never globally prune Docker or remove foreign locks/resources.

E03 implementation is committed at
`bf5471e9243dbda90cdf066a5d811486119dba7c`; postcommit content-equality evidence
is in `ffb862537b83ef7be053fb96e05d12fcbcfdd355`. That implementation commit also
first committed inherited E00–E02 work; those phases do not have invented
separate commit hashes. Read the [change tracker](CHANGE_TRACKER.md) and maintain
it for every E04 implementation, repair and delivery commit. Re-read current
HEAD/status; later documentation-only commits may follow this validated baseline.

## E03 state and development workflow

Runtime Python modules now live under `src/neo4j_graph_loader`; no generic
`src` compatibility shim exists. Static/dynamic/test imports, process targets,
DockerService module commands, examples/docs and all Dockerfiles migrated.
The package root exports only the version: **no E04 public API exists yet**.
Metrics imports `.._version`; the one literal authority remains `0.1.0a1.dev0`.
D03's E11 release target remains `0.1.0a1`. Dependency bounds/no-extras policy
are unchanged; matched Neo4j/Rust remains mandatory.

Use CPython 3.12, a fresh owned venv, `pip install -r requirements-dev.txt`, then
`pip install -e .`; run `pip check`, requirements parity, Ruff and full suites.
Pytest's root-path workaround was removed. Surviving repository path insertions
serve only example/test helpers. New code imports require the installation.

Restricted setuptools discovery and the sdist allowlist ship all 49 Python
runtime modules and reviewed metadata/license inputs. Config/resources remain
external; there is no installed `graph-loader` console script (E05). Full CLI
orchestration still builds images from the checkout (E06). All four Dockerfiles
install generated runtime requirements and then the package `--no-deps` with
isolated setuptools; no PYTHONPATH rescue or SCM version fallback is needed.

[E03 execution](execution-e03.md), [F1.S1](evidence/E03.F1.S1.md),
[F2.S1](evidence/E03.F2.S1.md), [F2.S2](evidence/E03.F2.S2.md) and their linked
JSON results/reviews contain exact identities, counts and limitations. Do not
substitute E02 scaffold proof or historical source hashes for current E03 bytes.
Re-capture current source/environment and rebuild affected artifacts after edits.
The E03 archive/origin validator is `tools/verify_e03_artifacts.py`; the old E02
validator intentionally still rejects migrated artifacts. E07's installed full
regression runner and E08/E09 platform/service matrices remain future gates.

## Required E04 order and boundaries

Read canonical [E04](epics/e04.md), backlog, [decisions](decisions.md),
[cancellation contract](cancellation-contract.md), [ordering matrix](ordering-matrix.md)
and [test ownership](test-inventory.json) before planning or coding.
Keep E01 approvals and no expanded retry/fleet API guarantees in force.

1. **E04.F1.S1:** explicit supported schema/planning modules, functions, models,
   errors and `__all__`; document input/source shapes and Kafka requirement.
   Do not claim structural Protocol semantics for a retained concrete base.
2. **E04.F2.S1:** public one-shot/closed/ownership and cleanup behavior. Preserve
   E00 locks, overlapping outputs, stale markers, partial-startup and caller-owned
   topic guarantees. An explicit lifecycle alone does not establish cancellation.
3. **E04.F2.S5 private transport first:** owned complete-frame nonblocking read
   AND write adapter, codec/frame/resource bounds and prompt endpoint closure.
   Test partial header/body, oversize/corrupt/EOF frames and write backpressure
   with deterministic clocks plus supervised real process fixtures. The parent
   must not block on recv/send/sendall after the deadline starts. No helper
   thread/process; ownership/budget failures need explicit diagnostics.
4. **E04.F2.S3 public integration second:** integrate the adapter through public
   cancellation/timeout errors and one operation-wide absolute deadline, with
   supplied-signal checks during all gather/broadcast/read/write waits. Public
   supervised blocked-source, mid-frame-reply and later-command/stop backpressure
   tests cover timeout AND cancellation. Preserve submission/serialization,
   arbitrary caller next()/callbacks and OS-uninterruptible exclusions; inline
   cooperative semantics are separate and start the scan clock as approved.
5. **E04.F2.S4 cleanup third:** one aggregate approved 10-second cleanup budget,
   signal-all then bounded join/terminate/kill, failure markers and output/topic/
   spill ownership. Complete public test assertions for total poll+cleanup time,
   child/endpoint/helper liveness and reclaimed/retained resources. Preserve live
   resources and name the worker if the OS cannot reap it. Private adapter tests
   alone do not satisfy public integration or aggregate cleanup gates.
6. **E04.F2.S2 last:** prove public/CLI imports preserve signals, logging,
   environment and threads/sockets; repeated independent library calls and CLI
   shutdown regressions. Avoid unapproved optional-dependency boundaries.

Do not implement E05–E12 during E04. If implementation needs a broader transport
or ownership design, revise canonical plan and independently re-review first.

## Continuing SDLC and evidence requirements

Plan first; use independent zero-history internal reviewers after every
implementation step and a fresh final adversarial reviewer. Review actual code,
source/artifact identity and executed checks, not prior approval summaries.
Reproduce every actionable finding, fix/retest/rebuild and independently re-review
until all findings close; preserve reports and negative controls. Infrastructure
skips leave required gates open. Never weaken assertions or collection for counts.

Run focused tests and fresh full nonintegration suite, schema/planner/CLI imports,
spawn/process regressions, requirements/lint/dependency checks, isolated sdist/
wheel rebuild and clean noneditable origin probes, affected image and live checks.
The exact owned B3 regression remains
`tests/integration/test_multi_topic_relationship.py::test_dead_letter_arrival_stream_type_wide_dlq_drained_by_the_retry_consumer`;
use `tools/run_dlq_proof.py --python <fresh-dev-python> <owned-output-dir>`
with an independent hard bound and check recorded interpreter/no-skip result.
Do not rerun the large IMDb benchmark as a substitute.

Capture safe explicit tracked/untracked manifests (include new public modules,
transport fixtures and executable tools) and environment before/after final
checks, precise commands/exits/import origins, every skip/warning and immutable
artifact/image hashes. Four inherited skip reasons and A5/E08 fork-warning
ownership remain recorded in E03 evidence. Mark canonical statuses complete only
after actual acceptance; render plan, validate links and whitespace. Prepare E05
handoff and follow the user's local-commit instruction only after testing and
independent acceptance. Never push, publish, tag or add workflows without a new
explicit authorization.

## Executable workflow and independent acceptance loop

1. **Plan the actual changes before coding.** Read applicable `AGENTS.md`,
   canonical E04 stories, D01–D07 and the complete approved contracts. Create
   `execution-e04.md` with story IDs, scope, dependency order, affected modules,
   acceptance criteria, tests/build commands, time budgets and owned cleanup.
   Capture starting source/environment and current commit/diff. Add a pending
   entry to `CHANGE_TRACKER.md`. Do not reopen approved choices as unanswered
   questions; investigate implementation details and ask only about real missing
   decisions or scope expansion. A materially different transport/guarantee needs
   revised bounded stories and independent plan review before implementation.

2. **Use the approved story order and deliver checks at each step.** Complete
   `E04.F1.S1 → E04.F2.S1 → E04.F2.S5 → E04.F2.S3 → E04.F2.S4 → E04.F2.S2`.
   The numerical story suffixes are not execution order. Keep schema/planning
   wrappers explicit; root wildcard exports, worker/fleet internals and embedded
   ingestion are not part of the approved API. Preserve concrete source-class
   identity/spawn pickling and existing exception causes. Audit the actual
   `_ProcessWorker`, `_planner_worker_main`, gather/broadcast/start/finish/close,
   inline worker and convenience topic/output ownership paths before edits.
   Tests for supported public behavior must import the public modules; private
   adapter controls belong to S5 and cannot replace public integration proofs.

3. **Write focused failure controls and implement narrowly.** Reproduce actual
   defects before repairing them where applicable. Preserve working ownership,
   replay/wire-format, Kafka grouping and CLI behavior; avoid unrelated algorithm
   changes. Implement frame/resource limits and a codec whose parent-side work
   obeys the approved bounds/callback exclusions; do not silently substitute a
   blocking receive or unsafe arbitrary callback path. Split/re-review discovery
   that exceeds the approved design or story size. Do not write a hanging test
   to demonstrate an unsupported blocking boundary.

4. **Keep the three transport/lifecycle validation gates distinct.**

   | Story | Required proof before accepting that story |
   | --- | --- |
   | S5 private transport | Partial header/body, frame larger than socket buffers, EOF/truncation/corruption/oversize, nonblocking send backpressure, supplied signal and absolute deadline. Real child faults are externally supervised; fixture cleanup is independently owned. No public errors or future cleanup-budget behavior is required yet. |
   | S3 public integration | Public timeout AND cancellation on blocked source, partial-large-reply stall and later-command send backpressure; approved errors and prompt coordinator observation using existing abort/terminate behavior. Preserve inline-record, phase-accumulation and start/finish-gap clock controls. |
   | S4 final cleanup | Same public cases plus stop-send backpressure, ordinary source failure, partial startup and termination-resistant fake. Assert total observation-plus-aggregate-cleanup bound, child/endpoint/helper liveness, diagnostics, failure markers and reclaimed/retained owned state. Unreaped workers retain possibly live resources. |

   Every process fault test needs its own external supervisor hard deadline and
   emergency cleanup so a broken implementation cannot hang pytest. S3's tests
   must not depend on S4's future cleanup guarantees; E04 closes only when all
   three gates actually pass. Private transport faults cannot be replaced with
   fake-only public tests. Retain normal source results and failure diagnostics.

5. **Preserve the exact clock and ownership semantics.** One monotonic absolute
   planning deadline spans scan/hub/emit/results. Process mode anchors once after
   initial serialization/submission, including a later start-to-finish gap;
   inline mode anchors before its first record and checks between returning
   records. Wait slices target at most 100 ms; readiness is not a complete frame.
   The aggregate cleanup target is a separate 10-second budget, not a reset or
   one budget per worker. Document all approved excluded blocking operations and
   supported-platform assumptions. Check deadline before/after bounded codec
   computation without claiming CPU preemption or OS-universal child termination.
   Preserve caller topics, live locks/spill files and caller-owned signals/start
   methods. Imports must not create servers, connections, containers, process
   configuration or global metrics state; verify this in fresh processes.

6. **Run code tests in an installed development environment.** Use CPython 3.12
   and a fresh owned virtualenv, install declared development dependencies, then
   `pip install -e .`. Record exact interpreter/dependencies and code origins.
   Run each new story's focused tests and inherited planner/schema/lifecycle/
   spawn/CLI regressions; then the final full suite:

   ```sh
   python -m pytest tests examples/imdb/tests -m "not integration and not smoke" -q -rs
   python -m ruff check src tests examples scripts/sync_requirements.py
   python -m pip check
   python scripts/sync_requirements.py --check
   git diff --check
   ```

   Here `python` means that fresh environment's interpreter, not an assumed
   system Python. Run newly added executable validation tools through lint too.
   The E03 baseline is 3,387 passes, four explained skips, 49 deselections and
   19 known A5/E08 fork warnings; do not preserve a count by dropping assertions
   or collection. Explain each skip and every new warning/failure. A required
   process, Docker or service case skipped for infrastructure remains open.

7. **Build and test final artifacts and affected images.** Public modules,
   transport and lifecycle edits invalidate their wheel/sdist/image evidence.
   Build in isolation; run strict Twine; inspect complete runtime contents,
   metadata/license/dependencies and excluded private/planning/test files.
   Explicitly rebuild from extracted sdist without Git. Install original/rebuilt
   wheels in separate clean noneditable environments away from checkout, with no
   PYTHONPATH/PYTHONHOME or source-path insertion. Verify site-packages origins,
   public schema/planning success/error examples, metadata/version, no legacy
   `src`, import inertness, `pip check` and component commands. Extend the E03
   inventory checker for E04's actual runtime modules; do not loosen leak checks
   or present smoke probes as E07's full installed regression suite.
   Rebuild affected local component images and run probes/helps using exact image
   IDs, no host source mounts and no network rescue for import/help. Preserve
   truthful E04 scope in documentation; the installed console script is E05.

8. **Execute affected real-service regressions with owned resources.** Rerun
   the maintained exact B3 case through `tools/run_dlq_proof.py` with explicit
   fresh `--python`; verify it truly passes without skipping and that graph
   properties, offsets, DLQ state and zero-wait evidence remain meaningful.
   Lifecycle changes also need their supervised real-child tests; B3 does not
   substitute for cancellation/transport controls. Testcontainers own scratch
   services and tests own their children/topics/files. Capture logs before
   cleanup and clean only owned resources. Do not rerun or clear remote IMDb.

9. **Require independent subagent review after every implementation step.**
   Spawn an internal reviewer with `fork_turns: "none"` when supported so it has
   zero inherited conversation. Give it the repository, exact story and approved
   contracts, real diff/untracked files and safe source/artifact/test evidence.
   Ask it to inspect code and tests independently, reproduce concerns and report
   file/line, impact and missing assertions. It must not approve by accepting
   author summaries or old approval labels, and must not edit implementation
   files. Keep reviewer and implementer separate. If internal review is
   unavailable, record the gate as open rather than calling self-review
   independent approval. Do not create user-visible chats for these subtasks.

10. **Repeat fix → test/build → independent review until every actionable issue
    is resolved.** Preserve original reports and link each finding to the repair,
    focused reproduction, retest/rebuild results and independent acceptance of
    the exact revised state. There is no fixed round limit. Do not waive a
    lower-priority actionable defect to finish sooner, suppress a failing gate,
    weaken tests or declare a prose response a fix. A false positive requires
    reproducible evidence and reviewer agreement; true scope expansion requires
    replanning. After all stories pass, use a fresh zero-history final reviewer
    for the complete E04 delta and evidence. Apply the same repair/retest/review
    loop to its findings. Repeat checks only when a change/failure/concern
    invalidates prior results; do not endlessly rerun unchanged passing suites.

11. **Record final acceptance, commits and handoff.** Save evidence for each
    E04 story under `evidence/<story-id>.md`, safe source/environment manifests,
    exact commands/exits/results and immutable artifact/image identities. Keep
    logs/data/secrets outside tracked evidence and leave excluded/platform gates
    explicit. Update canonical implementation statuses only after actual checks
    and final independent acceptance. Render the plan and validate local links.
    Follow the existing local-commit instruction after staged-diff inspection;
    update `CHANGE_TRACKER.md` with full commit SHA, date, story scope, behavior,
    test results, review/resolutions and evidence links. Use a follow-up local
    tracker/evidence commit to record a newly created delivery hash; do not try
    to embed a tracker's own enclosing hash recursively. Preserve per-file
    tested identity across documentation-only commits. Prepare the E05 handoff
    without implementing it. **Never push to GitHub unless the user specifically
    asks; this assignment supplies no push or publication authorization.**
