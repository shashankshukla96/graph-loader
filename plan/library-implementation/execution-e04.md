# E04 execution — 2026-10-18

Scope is E04 only. E03's fresh completion and handoff/tracker reviews are accepted.
Actual entry is clean HEAD `98cd267` on `codex/pypi-library-checklist`; no applicable
AGENTS.md found. [Entry source/environment](evidence/e04-entry.json) captures the
actual baseline, not a historical HEAD assumption. Preserve `.env`, remote IMDb,
all prior code/fixtures/evidence, dependency bounds and literal `0.1.0a1.dev0`.
No publication/push/tag/workflow is authorized. E05–E12 are not implementation scope.

## Ordered changes and checkpoints

1. **E04.F1.S1 public contracts.** Add explicit `schema` and `planning` reexport
   modules with approved finite __all__, canonical existing class identities,
   no root wildcard exports or internal worker/fleet API. Public tests import
   those modules for schema success/read/YAML/validation failures, settings,
   source mapping/sequence, multi-pair input errors and result types. Preserve
   exception diagnostics/causes; source base remains a concrete class. Document
   Kafka requirement and external config/console boundaries. Independent review.
2. **E04.F2.S1 ownership/lifecycle.** Document one-shot start/finish/close,
   idempotence where existing, no concurrent lifecycle calls, explicit try/finally,
   exclusive output locks/manual stale recovery, topic retain/delete ownership
   and best-effort deletion diagnostics. Public tests cover overlapping paths,
   stale files, partial startup, source failure and caller/convenience topics;
   retain inherited B1. No cancellation claim yet. Independent review.
3. **E04.F2.S5 private transport.** Owned socketpair, length-framed messages,
   nonblocking parent incremental read/write via selector slices <=100ms. Child
   may block. Initial source/task serialization and submission keep their
   explicit exclusion; only child decodes those initial pickles. Later parent
   IPC uses a closed tagged codec for exact builtins, numeric 1D NumPy counters
   and the owned ChunkSpec dataclass, no arbitrary pickle constructors/callbacks.
   Proposed bounds: 64MiB encoded frame, 1MiB user scalar string/bytes, numeric
   counter buffer <=32MiB, 1M codec nodes, depth64; reject invalid/oversized,
   truncated/EOF/corrupt payloads explicitly. Check cancellation/deadline before,
   during bounded traversals and after codec work, without CPU preemption claims.
   Codec tests round-trip every actual scan/counter/degrees/emit/stop message,
   maximum 2^22 uint32 counter (16MiB), packed .5MiB candidates, ChunkSpec and
   scalar hub values. Reject subclasses/object/unsupported dtypes, unknown tags,
   invalid shapes/nesting/lengths; prove no __reduce__/constructor/property
   callbacks in parent. Test exact/over frame, scalar, depth, node and cumulative
   allocation limits with configurable small test bounds plus real maximum
   counter/frame controls. Preserve runtime result types and external formats.
   Close duplicate endpoints
   after spawn, no receiver helper. Stop delivery uses bounded writes. Private
   tests: fake clock/readiness + externally supervised real child partial header/
   body, >socket-buffer stall, EOF/corruption/oversize and send backpressure,
   both timeout and signal. Gate only private behavior here. Independent review.
4. **E04.F2.S3 public integration.** Add approved keyword-only cancel_event and
   finite positive timeout_seconds; public EdgePlanCancelledError/TimeoutError
   under EdgePlanError. One absolute monotonic deadline: process anchor after
   all initial submissions, inline immediately before iteration; no reset through
   start/finish gap, gather, counter/degrees/emit and parent result phases. Check
   between inline returning records and around excluded callbacks. Public tests
   run in separately supervised client processes with emergency group cleanup:
   blocked source, real partial-large-reply stall and later-command backpressure,
   timeout AND cancellation. Assert error and prompt observation using abort/
   terminate behavior; aggregate final cleanup is the following gate. Fake clocks
   prove ten-record inline expiry, phase accumulation and start/finish gap.
   Preserve cause/remote failure diagnostics. Independent review.
5. **E04.F2.S4 cleanup.** Single internal 10s absolute aggregate cleanup deadline,
   signal all, bounded stop writes/joins, terminate then kill under shared phase
   cutoffs, not per-worker budgets. Heartbeat/endpoint cleanup shares the budget.
   Expose diagnostics for unreaped PIDs; retain possibly live spill/output locks
   and owned topic if children remain. Preserve original public error plus cleanup
   notes. Public real-process cases above add total observation+cleanup bounds,
   child/endpoint/helper liveness, marker and owned-state assertions, stop-send
   backpressure, <=4 workers, ordinary source error and partial startup. A fake
   termination-resistant process proves retention/diagnostics and aggregate time.
   Fixtures have separate hard deadlines and emergency cleanup. Independent review.
6. **E04.F2.S2 process effects.** Fresh public/CLI imports preserve logging,
   signals, environment, threads and process start method; forbid sockets/servers/
   children/containers during import and assert metrics uninitialized. Repeated
   independent planner calls plus inherited CLI supervision/shutdown tests. Keep
   all base dependencies mandatory. Independent review.

Each step is change → focused tests → zero-history independent reviewer → repair,
retest and re-review until all actionable findings close. Reviewer edits reports
only, independently inspects actual diff/tests and executed evidence. No interim
story completion before its own gate. Final fresh zero-history adversarial review
covers the whole delta and final frozen proof, with the same repair loop.

## Environments, test/build/live proof and evidence

Owned work: ignored `var/e04-validation-20261018`, fresh CPython3.12 dev venv;
install requirements-dev.txt, then pip install -e ., pip check and parity. Preserve
repository venv. Focused commands and full suite use the fresh interpreter:
`python -m pytest tests examples/imdb/tests -m "not integration and not smoke" -q -rs`;
Ruff source/tests/examples/sync plus all new executable tools; git diff --check.
Baseline is 3387 passes, four inherited skips,49deselections,19 A5/E08 fork warnings.
Record exact IDs/counts/skips/warnings, origins, commands/exits and safe log hashes.

Extend safe manifest named untracked inventory for public modules, private adapter,
new tests/fixtures/supervisor/tools. Capture before/after final proofs. Isolated
wheel/sdist+strict Twine, exact inventories/private exclusions and no-Git extracted
rebuild, compare every member byte, original/rebuilt into separate clean envs
away from source, python -I with PYTHONPATH/PYTHONHOME stripped. Public success/error,
origins/inertness/version/old imports/base pip checks and component helps required.
E03 validator dynamically covers all source modules; retain its exact leak checks.
Four Dockerfiles include changed planner code, so rebuild all unique local tags
and test immutable IDs: all module bytes/site-packages origins, public examples,
eight component helps/default help and pip check, no network/mount/PYTHONPATH.

Run exact B3 through actual `plan/library-implementation/tools/run_dlq_proof.py`
with explicit fresh --python and owned output. Verify no skip and proof summary,
properties/commits/DLQ/lock-wait assertions, bounded child group and fixture-only
cleanup. Never access/reload remote IMDb or globally prune containers/locks.

Per-story `evidence/E04.*.md`, safe JSON identities/results and independent reports
under reviews/e04-*. Preserve failures and fixes. Final freeze/full/build/image/
live checks must match source/environment. Complete canonical backlog only after
final acceptance, render plan and validate links. Prepare E05 handoff only.
Maintain CHANGE_TRACKER for implementation/repair/delivery commits; inspect stage,
commit locally after acceptance, then use a tracker/evidence commit to record
actual delivery SHA and compare tested file contents across HEAD changes. Never
recursively hash evidence into itself. Rollback is the reviewed E04 delta relative
to clean entry commit, without touching unrelated files/resources or `.env`.
