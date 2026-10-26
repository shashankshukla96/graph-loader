# Independent E07.F1.S7 review — 2026-10-25

**Disposition: accepted for the frozen writer-runtime lane. No actionable S7 finding remains.** S8 may proceed in the approved dependency order. This accepts neither the remaining installed lanes, rebuilt-sdist parity, E09 service guarantees nor final E07 completion.

This fresh zero-history review read canonical E07, the execution plan, approved S3 overflow boundaries and dispositions, accepted S6 review, frozen inventory, S7 evidence and raw receipts, runner/guard/receipt implementation, and writer tests with their helper/schema closure. Review took place on `codex/pypi-library-checklist`, HEAD `5c3f72b6cd16d0e14ac931f220a3f73eb7e5a02d`. Only this report, reviewer-owned files under `var/e07-validation-20261024/s7-review/`, and a new outside-Git temporary output/environment were written. No application, test, runner or inventory source was edited.

## Selection, preservation and closure

All **16 selected test files** independently match actual HEAD bytes and the original frozen pre-port baseline. Their complete **1,057 original assertion ASTs** match in order. The application source has no HEAD diff; the source and development-environment identities remain identical before and after review.

The exact selection retains **541 full IDs and 540 installed IDs**, with identity source-to-installed mappings. Its sole exclusion is the S3-approved `tests/test_metrics_overhead.py::test_prometheus_client_call_cost_ceiling`, a direct third-party wall-clock microbenchmark. Its original source and integration marker remain unchanged. The separate checkout unit selection retains that existing marker deselection; this review does not claim to have executed the excluded benchmark. All six deterministic tests from the same file execute installed, including the real node/edge write-path batch-count instrumentation checks, deadlock recovery accounting, metrics bind-failure handling and independent registries. No additional difficult runtime case was reclassified or omitted.

I recomputed the recursive closure and import edges from the 16 seeds and matched all **36 staged files** and their exact hashes. This includes the transitive CLI-shutdown/planned-edge helpers, fake bucket-lease hub, Docker configuration doubles, multi-pair graph helper, ancestor fixtures, all finite schema YAML and golden fixtures. No application source, Git tree, dataset or private dotenv is staged. Schema-driven record/DDL, multi-pair routing/write, lease/coordinator, retry/deadline, dispatcher and metrics assertions run against actual installed methods with their original explicit fake boundaries. No empty staged source scan or reduced reference glob supplies a pass.

## Independent execution and receipts

A fresh invocation of the prescribed installed runner against the explicit wheel passed in **46.85 seconds overall**, with pytest reporting **540 passed in 38.72 seconds**. Exact full collection/order, selected IDs/order and the one documented exclusion match the frozen inventory. Every selected ID has successful setup/call/teardown reports. There are no failed phases, skips, xfails, unapproved deselections, collection errors or pytest warnings. Pip check passes.

The independent raw-provenance audit verifies **zero actual Python children**, **one regression process identity** and **51 application module-origin receipts**. The later pip-check startup adds the second raw process record; it is not a regression child. Every module receipt resolves inside the new environment and matches actual installed file bytes and the explicit wheel. I also independently hash-checked **all 59 installed application payload members**, including resources, against the wheel and frozen inventory. No provenance violation, supervised timeout, leftover owned process or cleanup error occurs. The complete raw receipts were copied persistently into `s7-review/receipts/`, with retained hashes and sizes.

The separate guarded checkout selection passes **540 original IDs, one approved integration-marker deselection in 38.30 seconds**. Its full/selected collection and exact deselected ID match the inventory; all executed phases pass with no skips, xfails or warnings. The audit hook runs before pytest/source imports, blocks repository private-dotenv reads and external socket connections, and records zero blocked attempts. Its 50 socket events consist of the existing local dispatcher/metrics unit fixtures and one urllib3 import-time IPv6 port-zero availability probe. Local TCP unit fixtures are exercised; Neo4j/Kafka/Docker services and supported-mode E09 guarantees are not certified by these fakes or loopback checks.

All eight raw receipt files declared by the author's S7 machine evidence independently match their retained hashes. The independent run corroborates the author evidence rather than relying on its summary. The previously accepted S11 startup repair remains the runner input; no S7 source change or new child-launch concern required repeating the full S1 control matrix.

## Exact accepted inputs and remaining gates

- Source: **307 inputs**, SHA-256 `f94b1e9e68d6bcb750752782d15c253a4b4fa28325d52ae09129c56e1e5adcf6`.
- Development environment: `ae2f84a14eebdcb4c6fef629ef822b72fcb62658806808a5e2bc8c43794a5cc3`.
- Inventory: `770da5b51b6ec05d59d219516b5fac1f2fddef60d74f2394db5c34c3e9f1ee08`.
- Wheel: `cdd73ac361c99a8c73a15c929d150ce7d5e75d9446e1229dba5a1614cd494feb`.
- Runner: `bb79a159ee3958b66b218227f738cdf35f8a9d4ad91246e6b2a081412a5afdcb`.
- Guard: `02deb3760f24718cf19165de005b0dbabc7efbbcce6034b0d0889a51cdd91ca5`.
- Pytest receipt tool: `38f1feb53fae9b0f552c24837c42c471140469f403795a4efbe4c000158cf62d`.

Commands, exact collection/outcomes, static closure/assertion audit, before/after captures, raw installed receipts, checkout socket stacks and final reconciliation are retained in `s7-review/review.py`, `static.json`, `installed-command.json`, `installed-verification.json`, `receipts/`, `checkout-command.json`, `checkout-receipt.json`, `retained-identities.json` and `final.json`.

S8–S10, F2.S1, final full checkout/service/image/B3 gates, uninstrumented bare/image R1 controls and final adversarial acceptance remain open. No private environment file was read, remote IMDb endpoint contacted, developer smoke/live external service executed, foreign resource changed, Docker cleanup performed, publication action taken or E08+ work undertaken. The separately running full checkout proof was not interrupted or claimed by this story review.
