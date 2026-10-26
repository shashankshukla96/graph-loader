# Independent E07.F1.S8 review — 2026-10-25

**Disposition: accepted for the frozen orchestration-runtime lane. No actionable S8 finding remains.** S9 may proceed in the approved dependency order. This does not accept the remaining installed lanes, rebuilt-sdist parity, E09 service guarantees or final E07 completion.

This fresh zero-history review read canonical E07, the execution plan, approved S3 overflow boundaries and exclusions, accepted S7 review, frozen inventory, S8 evidence and raw receipts, runner/receipt/guard implementation, and orchestration tests with their recursive helper/schema/golden closure. Review took place on `codex/pypi-library-checklist`, HEAD `5c3f72b6cd16d0e14ac931f220a3f73eb7e5a02d`. Only this report, reviewer-owned proof under `var/e07-validation-20261024/s8-review/`, and a fresh outside-Git temporary output/environment were written. No application, test, runner or inventory source was edited.

## Selection, preservation and nonvacuity

All **18 selected test files** independently match both actual HEAD bytes and their original frozen pre-port baseline. Their complete **434 original assertion ASTs** match in order. Application source has no HEAD diff, and the source/development-environment identities are identical before and after this review.

The selection retains **284 full IDs and 282 installed IDs**, with identity source-to-installed mappings. The only exclusions are the S3-approved historical Git-slice checks:

- `tests/test_multi_pair_leases.py::test_the_slice_does_not_touch_the_lease_server_or_client`
- `tests/test_multi_pair_orchestration.py::test_the_slice_does_not_touch_the_bulk_monitor_or_docker_service`

Both unchanged tests run in the separate checkout selection and retain their original `not a git checkout` skips because the pinned historical revisions are unavailable. Those two skipped historical comparisons supply no application-behavior pass. Every behavioral ID in both mixed files executes installed, including real local lease-server/client exchanges, simultaneous pair holdings, shared drain budgets, scheduling readiness, precedence launch ordering and refusal before side effects. No additional difficult runtime case was reclassified or omitted.

I recomputed the recursive import/fixture closure from the 18 seeds and matched all **43 staged files** and exact hashes. The closure contains cross-test planner/retry/index/monitor/metrics/lease helpers, explicit Docker configuration doubles, the multi-pair graph helper, ancestor fixtures, finite schemas and golden files. The previously accepted location helper accounts for the increase from the original S3 table's 42 files; no new scope split or application source is involved. No application tree, Git tree, dataset or private dotenv is staged.

Original tests exercise actual installed CLI, monitor, migration, retry, lease, rotation, precedence, metrics and isolation methods with their existing explicit fake infrastructure boundaries. The single-pair compatibility fingerprint computes actual Cypher, DLQ/index bytes, plans and chunks and compares the full finite golden; its separate nonvacuity assertions execute. The migration wiring check uses `inspect.getsource` on actual imported installed modules, whose paths and bytes independently match the wheel. No empty staged source scan, reduced reference glob, test-only workaround or source-path adaptation supplies a pass.

## Independent execution and retained receipts

A fresh invocation of the prescribed runner against the explicit wheel passed in **11.98 seconds overall**, with pytest reporting **282 passed in 4.07 seconds**. Full collection/order, selected IDs/order and both documented exclusions exactly match the frozen inventory. Every selected ID has successful setup/call/teardown reports. There are no failed phases, skips, xfails, unapproved deselections, collection errors or pytest warnings. Pip check passes.

The independent raw-provenance audit verifies **zero actual Python children**, **one regression process identity** and **50 application module-origin receipts**. The later pip-check startup adds the second raw process record; it is not a regression child. Every module receipt resolves within the fresh environment and matches actual installed bytes and the explicit wheel. I additionally hash-checked **all 59 installed application payload members**, including resources, against both the wheel and frozen inventory. No provenance violation, supervised timeout, leftover owned process or cleanup error occurs. Complete raw receipts were copied persistently to `s8-review/receipts/`, with retained hashes and sizes.

The separate guarded checkout selection passes **282 original IDs with exactly the two inherited historical skips in 3.51 seconds**. Its complete 284-ID collection/order matches the inventory, with no deselection, xfail, warning or failed phase. The audit hook runs before pytest/source imports, blocks repository private-dotenv reads and remote socket connections, and records zero blocked attempts. Its 31 socket events are seven local lease-server binds, corresponding local client exchanges, and one import-time IPv6 loopback port-zero availability probe. These exercise local unit fixtures; no external Neo4j/Kafka/Docker service contract is certified.

All eight retained raw receipt files declared by the author's S8 machine evidence independently match their hashes. The independent execution corroborates those receipts rather than relying on the author's summary. The accepted S11 startup repair remains the unchanged runner input; no S8 source change or new child-launch concern required repeating the full S1 control matrix.

## Exact accepted inputs and remaining gates

- Source: **307 inputs**, SHA-256 `f94b1e9e68d6bcb750752782d15c253a4b4fa28325d52ae09129c56e1e5adcf6`.
- Development environment: `ae2f84a14eebdcb4c6fef629ef822b72fcb62658806808a5e2bc8c43794a5cc3`.
- Inventory: `770da5b51b6ec05d59d219516b5fac1f2fddef60d74f2394db5c34c3e9f1ee08`.
- Wheel: `cdd73ac361c99a8c73a15c929d150ce7d5e75d9446e1229dba5a1614cd494feb`.
- Runner: `bb79a159ee3958b66b218227f738cdf35f8a9d4ad91246e6b2a081412a5afdcb`.
- Guard: `02deb3760f24718cf19165de005b0dbabc7efbbcce6034b0d0889a51cdd91ca5`.
- Pytest receipt tool: `38f1feb53fae9b0f552c24837c42c471140469f403795a4efbe4c000158cf62d`.

Commands, exact collection/outcomes, closure/assertion audit, before/after captures, raw installed receipts, guarded checkout socket stacks and reconciliation are retained in `s8-review/review.py`, `static.json`, `installed-command.json`, `installed-verification.json`, `receipts/`, `checkout-command.json`, `checkout-receipt.json`, `retained-identities.json` and `final.json`.

S9–S10, F2.S1, final full checkout/service/image/B3 gates, uninstrumented bare/image R1 controls and final adversarial acceptance remain open. The root's separately retained full-checkout result is independent evidence and is not certified or claimed as final completion by this story review. No private environment file was read, remote IMDb endpoint contacted, developer smoke/live external service executed, foreign resource changed, Docker cleanup performed, publication action taken or E08+ work undertaken.
