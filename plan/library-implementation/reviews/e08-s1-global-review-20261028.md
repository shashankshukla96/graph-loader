# Independent global E08.F1.S1 acceptance review — 2026-10-28

**Disposition: accept E08.F1.S1 for the approved CPython 3.12 / Linux arm64 and amd64 minimum/current dependency scope. No actionable finding remains in this story review. The E08.F1.S1 dependency of E08.F2.S1 is satisfied; ordered S2 work may now begin. This does not accept S2, the complete support matrix, E08, a release, delivery, publication or any later epic.**

The reviewer began with a task-only brief and no implementation conversation. Actual HEAD is `4b246f3ab953bd9f7d266a682ac9fff39bd32032`; S1 implementation remains uncommitted. No applicable AGENTS.md was found in the checkout or its ancestor chain. Reviewed the [execution plan](../execution-e08.md), [decisions](../decisions.md), [canonical epic](../epics/e08.md), [ordering matrix](../ordering-matrix.md), [story evidence](../evidence/E08.F1.S1.md), prior plan/floor/runner/arm64/workspace reviews, current implementation and local retained raw evidence. Only this report and ignored audit outputs were written. No SSH, Docker operation, service/data access, private environment-file read, publication, staging or commit was performed.

## Implementation and preserved contracts

The current metadata and actual tested wheel implement the independently reviewed PyYAML 6.0.1 and paired Neo4j 6.0 / Rust 6.0 floors. The other five direct floors, eight mandatory dependencies, matched acceleration, driver/Rust `<7` fences, package `0.1.0a1.dev0`, Python `>=3.12`, version-only root exports and two schema resources are preserved. There are no extras or advertised fallback. Original PyYAML `cython_sources` build failures and the installed driver 5.20 `ConnectionPoolError` import failure remain retained; their successful replacements do not rewrite those observations.

Independent comparison to HEAD verifies every original 3,804 source ID in order, every original per-ID disposition, every original assertion/source-baseline entry, all existing lane definitions (including all eight installed lanes), and all 59 application payload hashes. Existing test files also match their HEAD bytes. The 37 added IDs belong to checkout tooling; current source collection has 3,841 IDs. The explicit pytest 9.1.1 tooling pin preserves the collector's parameter IDs after the retained 9.2 mismatch; no original selection, assertion or skip allowance was relaxed.

The constraints/wheelhouse implementation accepts finite exact pins and manifested regular wheel files, rejects directives/paths/URLs/normalized duplicates/symlinks/oversized or changed members, and snapshots verified bytes. Application and test-tool installation share the constraints and offline wheelhouse. Actual installed versions match the full locks. Existing default runner behavior, hard supervision, interruption/forkpty ownership controls, phase receipts and parent/child provenance remain. The container controller binds removal to full CID, immutable image, owner label and observed creation identity, preserves original failures and cleanup errors, and verifies absence. The aggregate native caller requires all 18 cells to succeed.

## Independently checked native results

Both [arm64 evidence](../evidence/e08-s1-arm64-20261027.json) and [amd64 evidence](../evidence/e08-s1-amd64-20261028.json) are supported by their local raw results, rather than inferred from intended runner behavior. The older arm64 report's unavailable-amd64 statements remain historical observations; the later actual native execution closes that S1 gap.

| Native target | Dependency set | Installed environments | Successful calls | Intentional skips | Separate bare environment |
| --- | --- | --- | --- | --- | --- |
| Linux arm64 | Minimum | 8 | 3,649 | 1 | Pass |
| Linux arm64 | Current | 8 | 3,649 | 1 | Pass |
| Linux amd64 | Minimum | 8 | 3,649 | 1 | Pass |
| Linux amd64 | Current | 8 | 3,649 | 1 | Pass |

This is 14,596 successful installed calls, four inherited intentional skips, 32 fresh installed environments and four separate bare environments across 36 observations. Each dependency set preserves lane counts: core 453/1, safety 1,335/0, CLI 636/0, public contracts 247/0, writer 540/0, orchestration 282/0, examples 115/0 and service-helper unit 41/0. The sole skip in each set is the original intentionally multi-pair schema example.

All 32 installed receipts independently match frozen full/selected IDs and order, explicit exclusions, staged and generated-stage hashes, distribution contracts and actual setup/call/teardown outcomes. No failed phase, xfail, unexpected skip, hidden deselection or receipt error is counted. Provenance independently verifies installed module origins/hashes, target environment/site, child PID/creation and parent identities, startup after launch, and post-exec startup where applicable, with no violations. Arm64 retains 6,002 module and 428 startup records; amd64 retains 5,976 module and 427 startup records. These measured counts need not be identical across machines. The installed result's process list predates its final `pip check`; the additional recorded startup is that successful dependency-check command.

All four bare environments establish isolation and absence of pytest, testcontainers, pandas, dotenv, build, twine and psutil before public probes. They verify all mandatory runtime distributions, all 59 application payload members, matched active native Rust codec, 57 imported application modules, public schema/planner/resource examples, CLI/help/version/parser parity and `pip check`. Bare runtime installs do not borrow test-rich target environments.

The minimum remains Neo4j 6.0.0 / Rust 6.0.0.0, Kafka 2.5.0, Pydantic 2.7.0, PyYAML 6.0.1, Docker 7.1.0, NumPy 1.26.0 and Prometheus 0.20.0. The coherent dated current lock remains distinct. Both native amd64 preparations reproduce the full recorded locks exactly and retain actual target-specific artifacts and complete resolution reports. Independently recomputed all wheelhouse hashes (30 minimum and 31 current per architecture), executable bundle manifests and 758 frozen context-file hashes across the four images. Original wheel metadata matches the refreshed inventory and current project requirements.

The amd64 readiness/launch/daemon/base/image/target evidence distinguishes host CPython 3.12.3 / x86_64 / kernel `7.0.0-38-generic` from target CPython 3.12.14 / x86_64 / Linuxkit `6.10.14`. Daemon and image report native Linux amd64. Arm64 targets independently report CPython 3.12.14 / Linux aarch64. Actual target controllers prove nonroot execution, disabled network, read-only root and owned evidence mounts without checkout/application-source mounts. The retained Docker wrapper selects the explicit local Desktop socket; launch environments use the owned isolated HOME and finite PATH.

## Native workspace, exports and retention

The [reviewed native-volume adapter](e08-native-workspace-review-20261028.md) leaves the original resource and installed assertions unchanged. The former host-share failure and partial interrupted attempt remain under `var/e08-validation-20261028/restart-failed-proof-v2/`, with their archive/hash references intact. No result is inferred from that failed attempt.

The deliberate interrupted smoke remains an expected failed/timed-out controller, followed by successful identity-bound container removal and recovery from the retained volume. Its actual recovered bytes record nonroot chmod write rejection and survival after removal. This proves that bounded interruption/recovery path, not an OOM or reboot experiment.

For every one of the 18 completed amd64 cells, independently checked the normal wrapper's zero exit, the separately executed read-only retained-volume verifier, actual verifier stdout, snapshot hashes, original six wheel metadata hashes, 59 installed payload hashes, five unchanged installed metadata hashes, and the verifier's executed validation of 64 installed RECORD member hashes/sizes. Pip's rewritten installed RECORD is checked separately from original wheel RECORD bytes. Raw initial/cleanup inspect logs match full CID/image/owner/creation identities, removal succeeds, and subsequent raw inspection reports absence. All target and export controllers have no cleanup errors. Both independent watchdogs exit zero with no emergency cleanup or errors.

The local audit recomputed 285 path/hash/size references, all 2,415 indexed archive-member hashes/sizes against both the actual archive and extracted files, and 785 retained controller/target/setup command receipt-log pairs. The archive is 94,174,736 bytes, SHA-256 `e6fe906832a24c544a5ecb04e5f1da4eabbdfa7d49258a99894458caeeb24a3b`. Unique actual wheel bytes are retained in immutable contexts; duplicate preparation/installed wheelhouse copies were verified before exclusion, with manifests and equality receipts retained. All executable/context manifests agree. Nonzero normal receipts are the expected parser exits or post-removal absence inspections; the separately retained intentional interruption is not a passing target cell.

The archive's empty `collection.log` is its snapshot before the collector printed its final result. Independently checked the separate actual zero-exit collection receipt and nonempty final log under `var/e08-validation-20261028/native-collection/`; their reported archive hash, size and member count match the actual archive. No successful collection exit is invented from the earlier empty snapshot.

Physical target virtualenv directories are excluded from the finite local archive. Actual native volumes retain those environments, while the executed origin/hash guards and separate volume verifier retain their checks. This review uses local receipts and bytes only; it does not claim to have independently reopened the remote volumes.

## Current state, validation and ordered continuation

The [317-input capture](../evidence/e08-s1-current-manifest-20261028.json) matches current reviewed inputs and differs from the accepted arm64 capture only in `docs/SUPPORT_MATRIX_VALIDATION.md`. Environment identity is unchanged (`9d146d496c52570cadd6bc1ae0eb8a8cbe7d06a201c9cf8efdf2a6a0f704b661`). The arm64 immutable v4 images preserve their original helper bytes; the previously reviewed retry-log naming repair is covered by current controls and actual portable/native preparation. No tested/committed/current equality is claimed at this story gate.

Retained audited checkout proof is 3,788 passes, four inherited skips, 49 deselections and 19 existing fork warnings. Its audit records installation before pytest with no private-dotenv/IMDb attempt. Those warnings remain explicitly assigned to S2. This review reran only isolated tooling controls:

```text
var/e08-validation-20261026/dev/bin/python -B -m pytest --noconftest -p no:cacheprovider -q tests/test_support_matrix.py tests/test_installed_runner.py
66 passed in 1.52s
python3 -B scripts/sync_requirements.py --check
Runtime requirements match project.dependencies
```

The S1 evidence's initially stale opening/open-requirements sentences were reconciled during review to distinguish completed native observations from pending independent acceptance; reinspection confirms the correction. No executable change accompanied it. This report now supplies the requested combined S1 acceptance. Canonical S2 explicitly depends on S1, so that dependency is satisfied without an ordering override.

S2's external-source pickling, spawn/inline, context preservation, framed transport, worker-count/cleanup envelope and 19-warning repair remain required. Final audited checkout/lint/parity, strict original/rebuilt archives, dual-wheel installed lanes, four immutable production images and read-only probes, all 12 uninstrumented R1 controls, exact no-skip B3, actual installed Neo4j 5.26 Enterprise / Kafka 7.7 proof, fresh final adversarial review, local delivery and postcommit equality remain open. Historical E07 evidence and this accepted dependency story do not close them. No E09–E12 execution, tag, push, upload, workflow or publication is authorized by this disposition.
