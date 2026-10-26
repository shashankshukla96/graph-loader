# E07 delivered-code completion verification — 2026-10-26

**Independently accepted: both repairs and complete fresh E07 proof.**
[Fresh final approval](e07-final-revalidation-20261026.md) finds no actionable issue.
Local delivery is `2927a847c6f27f86086191b87773af4edae4026a`; [all307 equality](../evidence/e07-revalidation-postcommit-20261026.json) passes. No E08 implementation or publication.

Entry was clean `91d765d18d5711402764bb5be51e3cd9c3bbed89`; original E07
implementation is `9b1ddd2ff24f3d86b1a19aa35ea188c66e6155d7`, E06 baseline
`5c3f72b6cd16d0e14ac931f220a3f73eb7e5a02d`. Read actual E07 stories, runner,
guard, pytest receipt plugin, inventory freezer, test adaptations and original
artifacts/receipts. [Revalidation plan](../execution-e07-revalidation.md) limits
changes to delivered E07 tooling; application source remains byte-identical to
accepted E06. No runtime metadata/API/version/dependency/ordering change.

## Findings and bounded repair loop

The [independent runner review](e07-runner-revalidation-20261026.md) reproduces
and closes two P2 defects introduced by E07:

1. **Interruption leaked an owned command and lost receipts.** SIGINT during
   `run()` escaped `process.wait()` before process-group cleanup, receipt-bound
   descendant cleanup and command receipt append. An actual owned sleeping child
   remained alive after its supervisor exited. Outer `main()` caught only
   Exception, so KeyboardInterrupt/SystemExit also omitted the owned failure
   manifest. The repair performs bounded cleanup and records the command on all
   termination paths, retains cleanup errors, then propagates the original
   interruption unchanged. Main saves the owned failure manifest before that
   propagation. Rejected/existing foreign outputs retain their ownership rules.
2. **Unsupported forkpty children escaped accounting.** With an actual target
   wheel and guard, `os.forkpty()` could create and immediately terminate a Python
   child while the provenance log contained only parent/module records. Every
   final expected-child predicate incorrectly passed. The guard now rejects the
   actual `os.forkpty` audit event before child creation. No extra supported child
   API, universal sandbox or broader origin guarantee is claimed.

The reviewer approved the narrow plan and manifest addendum before author edits.
Five new controls fail on original delivered bytes: **five failed, 24 deselected
in0.93s**. All **29** repaired runner controls pass, including actual SIGINT,
cleanup failure, outer main KeyboardInterrupt/SystemExit manifests and real OS
forkpty rejection. The independent reviewer repeats the original leak probe:
child gone, exit-15, retained receipt, original type preserved; actual wheel/guard
forkpty exits86 with a durable violation before any child, ordinary Popen passes,
and `-S` remains rejected. Independent runner suite: **29 passed in1.47s**.
Its first custom pytest temp directory was inside Git and correctly failed two
outside-Git ownership checks; that harness error is retained, then the normal
outside-Git run passes. No product assertion was weakened to resolve it.

[Independent inventory review](e07-inventory-revalidation-20261026.md) freshly
collects every ID, recomputes every disposition/closure/hash and compares source
assertions against E06. No baseline test function or behavioral assertion is
lost. Refreeze preserves all **3,799** original IDs, their order/dispositions,
all eight lane objects and original assertion baseline. It adds only **five
checkout-tool IDs**: now **3,804 IDs/124files**, **3,650 installed IDs**, **48
service IDs** and **106 checkout development IDs**. Payload and frozen wheel
metadata are unchanged. Independent repaired runner/inventory: **37 passed**;
collection/unit audit observes zero private dotenv or remote network attempts.
Historical16-run receipts remain bound to their old inventory hash and are not
relabeled as current execution.

A [fresh zero-history final auditor](e07-final-revalidation-20261026.md) separately
reviews the actual repair and inventory, executes its own37 controls and freshly
collects3,804IDs, and audits final artifacts, receipts, images and documents.
Whole-proof acceptance and local delivery/equality are complete.

## Identity and retained original evidence

Initial safe entry freeze contains307 inputs; parent independently reads all307
actual original Git blobs and compares them to tested/entry bytes, sizes and
executable modes. All **154** original raw receipt hashes match. Both old wheels,
all65 members, the71-file sdist and all16 original receipts are independently
reconciled. Application source and all74 image build inputs are unchanged;
actual daemon image inspection confirms original E07 immutable IDs/labels/
entrypoints/platform. This is a direct audit of historical proof, not a new
service or image-build execution. Original acceptance is qualified in the tracker,
README, historical validation and E08 handoff.

Parent output is ignored `var/e07-revalidation-20261026/`; final output is its
`final/` child. The entry3,746-pass run is retained separately; the final repaired
source is tested anew. Red reproductions and original/superseded results remain.

Repaired307-input freeze:
`995e8d9cf5a89fc31eddf8d5e8c80b633e91114483097d0627fa0997df726317`;
CPython3.12.14 dev environment
`ae2f84a14eebdcb4c6fef629ef822b72fcb62658806808a5e2bc8c43794a5cc3`;
refreshed inventory
`ea0a618d1f62df26ddb616ceb7db162c6aa85fc63a85bfdcbb73a066c5fbbf93`.

## Fresh final proof on unchanged repaired bytes

[Machine evidence](../evidence/e07-revalidation-20261026.json) retains all307
inputs, exact commands/receipts, **348** raw final proof hashes and original red/
superseded proof identities. Before/after source and environment match.

- Full checkout/example suite: **3,751 passed, four inherited skips,
  49 deselected, 19 known warnings in202.16s**. The private dotenv/remote-address
  audit was installed before pytest; zero blocked attempts. Skips retain readonly
  bash SHELLOPTS, two historical Git-slice checks and intentional multi-pair
  fixture reasons. Warnings remain15 IMDb subset/four planner multi-threaded-fork
  deprecations under A5/E08; no new skip/warning or filtering.
- Ruff across source/tests/examples/scripts/maintained tools, requirements
  parity, pip check, exact3,804-ID inventory and whitespace pass. Plan render
  validates all canonical story/dependency/coverage records.
- Fresh isolated wheel/sdist and true outside-Git extracted-sdist rebuild,
  strict Twine/inspection, all **65 member bytes equal** and **11 actual archive
  mutations rejected**. Exactly57 runtime modules/two YAML resources/eight
  mandatory dependencies/no extras/root version only remain. Two fresh bare
  noneditable runtime-only installations pass import/API/CLI/resource/example/
  origin/parser/pip checks outside checkout.
- **All16 installed lanes** run in distinct fresh target environments: core454,
  safety1335, CLI636, public247, writer540, orchestration282, examples115 and
  service-helper41 per wheel. All **7,298 passes**, **two approved intentional
  core skips**, **396 actual child launches**, **6,015 module-origin receipts**
  reconcile with exact full/selected IDs/exclusions/outcomes, current runner/guard/
  plugin bytes and owned cleanup. Parent rehashes all staged/generated files and
  all59 payload files in each target environment. Target environments include
  separately declared test tools; the two bare/runtime artifacts do not.
- Reuse actual immutable E07 images only after current daemon identity/labels/
  entrypoints and all **74 build-input** hashes match. Historical build commands/
  logs are explicitly labeled, without a new build/tag claim. All **76 fresh
  image checks**, **four fresh real RO-input/RW-output bind probes** and **12
  separately supervised R1 cases** pass. Their exact client bytes, first-received
  causes, default/finite timeouts and child/endpoint/lock/spill cleanup remain.
- Exact owned **B3: one pass, no skip/timeout**, exit0,22.705s supervisor; its
  actual graph/DLQ commits/parking/zero-lock-wait assertions remain unchanged.
  Before/after Docker inventories are empty; no image probe ran concurrently.
- New owned Neo4j5.26 Enterprise/cp-kafka7.7.0 service proof invokes the installed
  CLI outside checkout, without source mounts: normal, padded named-database and
  external-plan loads pass; whitespace-default actual preflight passes. Verify
  **13 nodes/12 relationships/all properties**, node offsets12/1, edge boundary
  `[0,0,12]`, complete12-row ledgers, default database zero nodes, host monitor/
  container DNS coordinator readiness, shared-parent foreign sentinel and spill
  cleanup. All **seven** actual auth/database/address/network/metadata/missing-bind
  negatives fail safely; all **12 commands** and exact enhanced helper are hashed.
  Owned applications/infrastructure containers/network are removed; final Docker
  inventory is empty. Retained librdkafka disconnect diagnostics are expected
  during negatives/owned-broker teardown; they are not hidden or new pytest warnings.

Fresh final auditor independently rechecks all16 raw execution receipts, every
staged/generated file, current tools, all944 installed payload files, actual
child startup/create/PID/origin/exec evidence and zero matching owned survivors.
It also verifies exact image/live/B3 receipts and absence of owned service
objects. Final independent document/machine consistency acceptance and local delivery/
equality are complete.

## Scope and delivery

Private `.env`, remote IMDb192.168.1.75 and foreign resources are untouched.
No global Docker cleanup, push/upload/tag/publication or E08–E12 implementation.
Native macOS CPython3.12/local Linux arm64 proof does not close E08 support or
general E09 service guarantees. Tiny inputs do not force spill; the padded load
reuses node consumer groups and the blank-default case is preflight only.
No malicious-code sandbox or maximum-backlog throughput guarantee is added.
Docker Desktop was initially stopped; opening it restored the local daemon
without settings changes. All service resources belong to the new proof run.

Fresh independent final approval and staging inspection pass. Local repair
commit is `2927a847c6f27f86086191b87773af4edae4026a`. [Postcommit proof](../evidence/e07-revalidation-postcommit-20261026.json)
checks all 307 actual committed blobs against tested/current content, size
and executable mode; environment remains unchanged. The documentation-only
follow-up records actual identity under tracker rule5, with its own SHA from
Git history. The conditioned E08 handoff is ready and preserves all new controls
and unsupported/unproved boundaries; E08 execution still requires user request.
