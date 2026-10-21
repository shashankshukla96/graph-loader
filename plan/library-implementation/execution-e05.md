# E05 execution plan — 2026-10-21

Scope: E05 only, no E06–E12 implementation/publication/push/tag/workflow. Entry
clean HEAD `14fe493` on `codex/pypi-library-checklist`; E04-R1 delivery and fresh
[revalidation](reviews/e04-completion-verification-20261021.md) accepted. Preserve
all prior code/tests/evidence, literal0.1.0a1.dev0, mandatory dependencies/no extras,
`.env` and remote IMDb at192.168.1.75. No private environment loading/disclosure,
foreign lock/topic/resource deletion or global Docker prune. No applicable
AGENTS.md found. Fresh owned CPython3.12 environment and safe entry manifest:
ignored `var/e05-validation-20261021`; do not alter earlier environments.

## Approved scope translated into three ordered stories

### E05.F1.S1 — command entry and version

Add `[project.scripts] graph-loader = neo4j_graph_loader.cli:main`, guarded
`neo4j_graph_loader.__main__`, stable parser program name and file-authority
`--version`. Keep existing no-argument-compatible `main(args_list=None)`, modes,
flags, return/parser error codes and explicit process environment policy; never
load `.env` on import or implicitly at command entry. Help/version/no-args/invalid
command behavior must agree for executable, package module and existing CLI
module from unrelated cwd with services forbidden. Imports remain inert, including
importing __main__ through runtime inventory discovery. Reinstall editable metadata
for the new executable. Extend exact artifact checker for the sole console entry
and its wheel/sdist metadata rather than relaxing leak checks. Independent review.

### E05.F1.S2 — finite resources and service-free schema helpers

Explicit resource allowlist: byte copies of reviewed `config/graph_schema.yaml`
(12KB canonical default) and `examples/social/graph_schema.yaml` (3.5KB social
sample), under `neo4j_graph_loader/data/graph_schema.yaml` and
`data/social_schema.yaml`. No datasets, tests, logs, server XML or other config
files ship. Exact package-data/MANIFEST entries and strict inventory/hash checks.
Use a private resource helper based on importlib.resources.files/as_file; hold
materialization lifetime through the entire command, including its shutdown.
An explicit --config always names a caller filesystem path; omitted config uses
the bundled canonical default without a root-config/cwd fallback. Provide bounded
service-free `schema-example [--name default|social]` (stdout, suitable for caller
redirection) and `validate-schema [--config PATH | --sample default|social]`.
These inspect schema files only, adding no ingestion mode or new public library
API. Validate both packaged samples and external YAML, parse failures and missing
resource negative controls. Test resource lifetime after materialization and
read-only package/cwd. Update user/package guides truthfully. Independent review.

### E05.F2.S1 — explicit owned writable run state

At the installed CLI/main start boundary, validate cheap numeric/contradictory
flags before preparing state. Materialize the source under its resource lifetime,
then copy it into a private persistent runtime schema. Create an exclusive0700
attempt directory using OS temporary storage by default, or a new unique child
of explicit `--run-dir BASE`; log its absolute location. Never write beside the
package or require writable cwd/home. No automatic deletion on completion/failure:
diagnostics and potentially live E04 lock/spill owners must remain safe.

Private runtime paths cover schema, rejections, plan/ledger/progress/failure/spill
and timings/metrics. Preflight real writable probes for every output target before
clients/DDL/ingestion. Explicit existing --plan-dir/--plan-spill-dir/--edge-plan
and GRAPH_LOADER_TIMING_DIR inputs retain priority and ownership; do not unlink
foreign files or relax E04 output locks. Defaults isolate independent attempts,
even with the same explicit run identity. Caller-selected shared override paths
remain caller responsibility; existing ownership guards still reject collisions.
Generated schema is a copy, never mutation/chmod of caller/package files.

Carry this context explicitly on CLI args and per-instance DockerService state,
not process-global environment rewrites. Internal direct handler/service call
hooks may retain their existing caller-cwd defaults when no CLI runtime context
is supplied; the supported executable/module entry always prepares context.
Thread rejections into planner and node/edge/retry launchers, plan/spill defaults
into planner (ledger/progress beside owned plan), and timing/snapshot paths into
all relevant CLI/helpers/container launchers. Node/edge overrides, retry fallback
and CLI periodic/final metrics use the same explicit path. Preserve existing
signal restoration, cleanup ordering, supervision, source errors and R1 controls.
No scheduling, Kafka identity, retry/ordering or image-selection redesign.

Audit all six container roles: node, edge, retry, clock, lease server and planned
dispatcher/writers. Bind host absolute schema/plan paths read-only and owned
rejection/timing directories read-write at established /app container paths.
Do not confuse container paths with daemon-side bind sources: the CLI requires
those host paths visible to the Docker daemon (local daemon/shared filesystem);
no new remote-daemon filesystem translation is promised by E05. Keep explicit
--skip-image-build/local image overrides and existing checkout build strategy;
checkout-free image selection is E06. Tests must distinguish this limit from
installed help/resources/state independence.

Test actual public main preflight/dispatch plus every Docker mount/env argument
with controlled clients; internal scheduling tests keep their useful existing
scope. Do not weaken assertions or broadly bypass runtime preflight in tests.
Check two independent attempts, caller-source immutability, explicit override
priority, missing/unwritable paths before service access, read-only/unrelated cwd
and package, persistent evidence on failure and protection of unrelated files.
Use actual read-only container rootfs/tmpfs proof to avoid chmod tests passing
under root. New default state does not load ambient .env. Independent review.

## Proof, reviewers and delivery

Plan review precedes coding. After each ordered story use a zero-history internal
reviewer inspecting actual tracked/untracked changes and meaningful executed
controls, then reproduce/fix/retest/re-review every actionable finding. Fresh final
adversarial reviewer covers all stories and exact final proof, not approval labels.
Never create user-visible chats for these reviews. Required infrastructure skips
leave their gates open; preserve failed attempts and negative controls.

Fresh installed dev interpreter runs focused schema/public/planner/R1 transport/
lifecycle/CLI/metrics/spawn tests and final complete suite:
`python -m pytest tests examples/imdb/tests -m "not integration and not smoke" -q -rs`.
Run Ruff source/tests/examples/parity plus new executable tools, pip check,
requirements parity and staged/unstaged whitespace. Baseline3,516 passes, four
inherited skips,49 deselections,19 known A5/E08 fork warnings; explain every
changed count/new warning without collection/claim weakening.

Capture allowlisted tracked/relevant-untracked entry and before/after source/
environment identities, including new modules/data/tests/docs/tools. Do not hash
secret/ignored datasets or evidence recursively. Build isolated sdist/wheel,
strict Twine, exact module/resource/entrypoint/license/dependency/private-exclusion
checks and omitted-file negative controls. Rebuild extracted sdist without Git and
compare every wheel member byte. Two clean noneditable installs outside checkout,
no PYTHONPATH rescue/dev tools: verify purelib origins/bytes, metadata/version,
sole executable, three invocation forms help/version/parser errors, default/social
and external validation/examples, read-only package/cwd and private run state.
Retain public E04/R1 semantics, not just parser smoke. These do not replace E07.

Rebuild all four affected images by new owned tags/immutable IDs; import/help/
version/resource checks have no source mounts/network rescue. Runtime bind tests
may mount only owned schema/plan/state data, never application code; inspect
absolute host source versus container destination. Read-only-rootfs checks allow
only owned tempfs/state outputs. Rerun exact no-skip B3 through run_dlq_proof.py
with explicit fresh --python and unique owned outputs, independently bounded;
retain graph/offset/DLQ/zero-wait semantics and fixture-only cleanup. No IMDb load.

Keep per-story evidence, original findings/resolutions, exact commands/exits,
skips/warnings and immutable artifact/image IDs. Complete canonical statuses only
after actual acceptance; render plan and validate local links. Prepare E06 handoff
without implementing it. Inspect staging and commit locally after acceptance,
then record full delivery SHA/per-file committed equality in a documentation-only
follow-up. Follow tracker rule5 to avoid recursively embedding its own hash.
