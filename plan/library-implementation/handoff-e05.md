# E05 next-agent handoff — installed command, resources and writable state

**E04-R1 closed by independent repair acceptance, 2026-10-21:**
The [original finding](reviews/e04-completion-verification-20261020.md) remains
historical. [New repair evidence](evidence/e04-r1-repair.md) and
[fresh final acceptance](reviews/e04-r1-final-review.md) restore E04 S3/S4.
Use the actual repair delivery and postcommit equality in the change tracker
before starting E05; this repair task does not implement E05.

Prepared 2026-10-20. Execute **E05 only after E04 final independent acceptance
and local delivery are recorded** in the [change tracker](CHANGE_TRACKER.md)
and [E04 final evidence](evidence/e04-final-validation.md). E04 local delivery is
`55dd4938188768090c10d851f8f7330706975ea5`; [postcommit equality](evidence/e04-postcommit.json)
verifies the259 inputs. The tracker follow-up records this actual hash. This
handoff does not start E05. No upload, push, tag, publication or workflow is authorized.

## Preserve the actual state

Repository `/Users/shashank/Desktop/Code/Graph/graph-loader`, branch
`codex/pypi-library-checklist`. Read actual HEAD/status and applicable AGENTS.md;
E04 began clean at `98cd267db5c70d3517a89ec68c1a4cedf2c254fd` and its tracked
manifests distinguish exact tested inputs from later documentation commits.
Preserve all code/tests/fixtures/plans, `.env` and foreign resources. Never source
private remote settings, contact IMDb at `192.168.1.75`, remove foreign locks or
globally prune Docker. Use new owned environments/services/output directories.

## E04 behavior to preserve

Supported public modules are `neo4j_graph_loader.schema` and
`neo4j_graph_loader.planning`, with explicit finite exports and canonical model/
source identities. Root exports the version only; no `src` alias exists. Concrete
`EdgeRecordSource` is not a Protocol. Schema loading wraps read/decode/YAML/model
failures in `SchemaLoadError` with causes; direct models keep Pydantic validation.
Producer planning requires Kafka. The convenience mapping call defaults inline;
the planner factory and source sequences default spawn. Public one-shot planners
own output locks/spill, retain caller topics, and use instance metrics registries.

Cancellation/deadline controls are parent-owned/cooperative. Process timing
anchors once after initial serialization/submission; inline timing begins before
iteration. Initial submission, blocked caller callbacks/next and OS-uninterruptible
work remain excluded. Closed bounded socket framing covers complete reads and
later writes; initial pickle is decoded only by the child, later traffic uses a
closed codec. Preserve frame/scalar/array/node/depth/allocation bounds, no IPC
helper, public errors and remote diagnostic/local causes. Cleanup uses a separate
aggregate10-second budget, signals all workers before joins, closes endpoints,
and retains potentially live lock/spill/topic state with PID/path diagnostics.
Individual cleanup errors must not bypass remaining worker cleanup; outer failure
handling must not overwrite startup cleanup evidence. See
[public guide](../../docs/LIBRARY_API.md), [contract](cancellation-contract.md),
[E04 execution](execution-e04.md) and retained per-story/review reports.

## E05 scope and order

Read canonical [E05](epics/e05.md), backlog, D01–D07, ordering matrix and test
inventory before planning. Keep dependencies mandatory, no extras, version literal
`0.1.0a1.dev0`; release target remains `0.1.0a1`. E06 image delivery and E07–E12
work are not E05 scope.

1. E05.F1.S1: add the approved installed executable/no-argument-compatible main,
   module invocation and metadata-consistent version. Help/version/parser errors
   work from unrelated directories without services; preserve command semantics
   and explicit `.env` policy. Imports remain inert.
2. E05.F1.S2: package only approved small samples, use `importlib.resources` with
   correct temporary-path lifetimes and preserve explicit caller paths. Wheel and
   no-Git rebuilt sdist deliver the same resource set; read-only installation works.
3. E05.F2.S1: thread caller/documented writable run directories through schema,
   rejection/plan/ledger/spill/progress/timing paths and launchers. Never write
   beside installed code; distinguish host paths from Docker-visible paths.
   Test two independent runs, unrelated/read-only cwd/package, permission errors
   before ingestion and every generated config/bind mount.

## Required workflow and proof

Create an execution plan before coding, capture safe explicit tracked/untracked
source/environment and current diff, add a pending tracker entry. Use CPython3.12,
fresh dev venv, install requirements-dev then editable package. Every implementation
step requires a zero-history independent internal reviewer; preserve/reproduce
findings and repeat repair/test/re-review until all actionable findings close.
Use a fresh final adversarial reviewer for the complete delta and actual evidence.
Do not create user-visible chats for internal review.

Run focused public/schema/planner/transport/lifecycle/CLI/spawn checks, full fresh
nonintegration suite, Ruff (including new executable tools), dependency/parity and
whitespace checks. Preserve the four inherited skips and19 known fork warnings
with ownership; new failures/warnings need explanation. Required infrastructure
skips leave gates open. E04's final evidence records exact current counts rather
than promising a fixed count for E05.

Build isolated sdist/wheel, strict Twine, inspect exact modules/resources/metadata/
license/dependencies/private exclusions, explicitly rebuild from extracted sdist
without Git and compare member bytes. Install original/rebuilt wheels into two
fresh noneditable environments outside checkout; verify site-packages origins,
public examples, executable/module help/version/parser parity, resource loading
and read-only package with no PYTHONPATH rescue or dev tools. Extend the strict
`tools/verify_e03_artifacts.py` validator deliberately for E05 entry points and
resources; do not weaken leak checks merely to accept new files. The E02 verifier
remains historical and intentionally rejects migrated artifacts.

Rebuild affected component images and test immutable IDs with no source mounts/
network rescue for import/help. Rerun exact owned B3 via
`tools/run_dlq_proof.py --python <fresh-dev-python> <new-owned-output>` and verify
one pass without skip; retain semantic graph/offset/DLQ/zero-wait assertions.
Each real process fault has its own independent hard-deadline supervisor and
owned emergency cleanup. Do not substitute the IMDb benchmark or private codec
checks for public lifecycle guarantees.

Capture before/after safe manifests, precise commands/exits/origins, all skips/
warnings and artifact/image hashes. Complete statuses only after actual final
acceptance; render canonical plan and validate local links/whitespace. Prepare
E06 handoff without implementing it. Inspect staging, commit locally only after
acceptance, then record actual full delivery SHA in a follow-up tracker/evidence
commit with per-file tested-content equality. A tracker does not recursively
contain its own enclosing hash. Never push or publish without explicit new user
authorization.


## Preserve the R1 failure-cleanup regression

A received ordinary worker failure must raise immediately with its existing
cause and enter shared cleanup before waiting for unrelated blocked workers.
Preserve the externally supervised public mixed-source default/finite-timeout
controls and deterministic post-reply timeout/cancellation cause controls in
`tests/test_public_cleanup.py`, plus `PeerReadyFailingSource` readiness coordination.
Receive order remains sequential: do not claim observation of a later-lane error
while an earlier receive is blocked. The repair evidence includes12 checkout-free
spawned mixed controls across both rebuilt wheel installs and all four new images.
