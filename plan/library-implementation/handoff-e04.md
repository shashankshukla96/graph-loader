# E04 next-agent handoff — public API and bounded planner lifecycle

Prepared 2026-10-17 during E03. **Do not execute E04 from this handoff alone**;
this assignment implements E03 only. Read E03's final review/status and actual
checkout before treating E03 as accepted. No upload/push/release is authorized.

## Preserve and inspect the actual checkout

Repository `/Users/shashank/Desktop/Code/Graph/graph-loader`, branch
`codex/pypi-library-checklist`. Read current HEAD/status; E03 began at `6de3562`
with required E00–E02 dirty/untracked implementation. The E03 entry and final
manifests distinguish inherited fixes from the namespace move. Preserve all
source/test/fixture/plan work; never clean/stash/reset to HEAD alone. No
AGENTS.md was present at E03 entry; recheck before work. Keep `.env` private,
never source remote settings into isolated tests, never touch IMDb at
`192.168.1.75`, never globally prune Docker or remove foreign locks/resources.

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
