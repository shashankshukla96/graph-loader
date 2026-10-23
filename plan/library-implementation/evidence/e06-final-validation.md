# E06 final validation — 2026-10-22

**Historical delivery proof:** the 2026-10-23 delivered-code revalidation found
and repaired additional issues. [Fresh completion](../reviews/e06-completion-verification-20261023.md)
and [independent acceptance](../reviews/e06-final-audit-resume-20261023.md) now
control the gate; current local delivery/equality are in the
[tracker](../CHANGE_TRACKER.md). Results below describe the original delivery.

**Accepted by the fresh final independent adversarial review on 2026-10-23.**
[Review](../reviews/e06-final-adversarial-review.md), [independent checks](e06-final-independent.json).
Validation executions below retain their actual October 22 dates.
Only E06 is executed; no push/upload/release tag/publication/workflow or E07–E12.

Final279-input source `39cb146ce8e5891c04c1ea10b02e83d2bb06582c817f043599fba757f2851d13`; fresh CPython3.12.14 dev
editable environment `96b072066210abc50c4d8cf2bf48460f11e2ebbea959f87983d47328e1d5135d`. [Tested manifest](e06-tested-manifest.json)
and before/after captures match. No private .env was loaded/disclosed, remote
IMDb contacted, foreign resources removed or Docker globally pruned.

The full suite passed **3624 tests**, four inherited skips,49deselections and
19known fork warnings. Exact log summary: `3624 passed, 4 skipped, 49 deselected, 19 warnings in 196.88s (0:03:16)`.
Skips retain their original reasons: bash-only readonly SHELLOPTS, two inherited
checkout guards and intentional multi-pair fixture. Warnings are15 IMDb subset
and4 IMDb planner fork deprecations, still A5/E08; no new pytest warnings/skips.
Lint across source/tests/examples/tools, requirements parity, pip check and
whitespace pass. [Structured results](e06-results.json) retain commands/exits and
log identities. Raw final proof is `var/e06-validation-20261022/finalb/`.

[Strict artifacts](e06-artifacts.json):57Python modules, exactly2YAML resources,
sole console entry/license/all8mandatory dependencies/no extras/private inputs.
Isolated build/Twine strict pass. Extracted sdist is truly outside Git discovery
(exit128), its rebuilt wheel matches all65 member bytes. Two fresh outside-
checkout noneditable installs pass origins/hashes/inert imports/metadata/help/
three-entry parity/public examples/resources/runtime ownership and pip checks.
All11 actual [archive mutation controls](e06-archive-negative-controls.json) reject.

[Four rebuilt immutable images](e06-images.json) and [candidate manifest](e06-candidates.json)
record actual local IDs/entrypoints/platform/version/component/contract labels
and complete build inputs, with proposed GHCR tags and no registry digests.
Every image passes all57module origin/hash checks and component/default help,
entry/version/resources/public/runtime fixtures/pip, outside /app with no source
mount rescue. [Four actual data-bind probes](e06-data-binds.json) prove schema/
plan RO, rejection/timing RW and canonical resource hashes under read-only rootfs.
All [12 externally supervised R1 controls](e06-mixed.json) across both installs
and all4images pass default/finite-timeout first-received causes, bounded failure,
child/endpoint/lock/spill ownership and cleanup. Each process has its own hard
supervisor and exact owned emergency cleanup.

[Actual installed service proof](e06-service.json) uses Neo4j5.26 Enterprise and
Kafka3.7/cp-kafka7.7.0, unrelated cwd and no checkout application source mounts.
Two loads prove normal planning plus explicit external plan with shared parent/
spill/timing and preserved foreign sentinel. Exact13nodes/12relationships/all
properties, node offsets12/1, source edge boundary[0,0,12], two completed ledger
entries/12rows, scratch DB/default DB separation, persistent diagnostics and
planner spill child reclamation pass. Tiny rows do not force spill. Actual
constructed monitor/default host readiness client and container DNS PING pass.
Seven actual credential/database/container address/network/advertised metadata/
missing-bind negatives fail clearly with no leaked credential markers. All owned
applications, three infrastructure containers and their network are removed.

Exact independently supervised **B3:1passed, no skip/timeout**, exit0. Finalb
before/after Docker inventories are both empty. Its unchanged actual graph,
source/DLQ commit/parking and zero-lock-wait assertions remain. Original final
B3 overlapped one owned image probe; its separate isolated repeat passed too,
but R3 required the current finalb repeat. No IMDb substitute.

A late development-guide paragraph still described old cwd builds; it is the
sole source-manifest delta from the first green3624-pass frozen run. Its corrected
279-input snapshot receives a fresh full run above. All59package payload members
and74candidate COPY inputs remain byte-identical; affected runtime artifacts
were not changed/relabelled. [Retained resolutions](../reviews/e06-resolutions.md)
preserve every failed attempt, independent finding, bounded repair and causal
limit. E05 five symlink-loop controls, private persistent state/resource lifetime,
E04 public ownership/transport/cancellation/10-second cleanup/R1 contracts,
version-only root/no src alias/version/dependencies and D02 ordering restrictions
remain. Native macOS/local Linux arm64 does not certify E08's wider matrix or
E09's service/ordering guarantees. No publication occurred.

All three stories and E06 are independently accepted. Local main delivery is
`a034420f98a57d17c7591b23eb1011d7bc346d25`. [Postcommit proof](e06-postcommit.json)
verifies all 279 tested/committed/current input bytes, sizes and modes and the
unchanged environment. This documentation-only follow-up records the actual
full delivery SHA; its own hash comes from Git history. Temporary outside working directories from October 22 have
become unavailable; hashed raw receipts/artifacts remain, and the final independent
report records fresh October 23 installation/image checks without relabelling
the earlier service executions.
[Future E07 handoff](../handoff-e07.md) does not authorize E07 execution.
