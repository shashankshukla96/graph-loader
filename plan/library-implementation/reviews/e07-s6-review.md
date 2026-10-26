# Independent E07.F1.S6 review — 2026-10-25

**Disposition: accepted for the frozen public-contracts lane. No actionable S6 finding remains.** S7 may proceed in the approved dependency order. This accepts neither S7–S10, rebuilt-sdist parity, E09 service guarantees nor final E07 completion.

This fresh zero-history review read canonical E07, the execution plan, frozen inventory and approved S3 splits, S6 evidence and original rejection, the accepted S11 repair review, runner/guard/receipt implementation, public tests and their helper/reference closure. Review took place on `codex/pypi-library-checklist`, HEAD `5c3f72b6cd16d0e14ac931f220a3f73eb7e5a02d`. Only this report, reviewer-owned files under `var/e07-validation-20261024/s6-review/`, and a new outside-Git temporary output/environment were written. No application, test, runner or inventory source was edited.

## Frozen contracts and preservation

All **14 public test files** independently match actual HEAD bytes and the original frozen pre-port baseline. Their complete original AST assertion lists also match. The application source has no HEAD diff. The current source identity is identical before and after review and identical to the independently accepted S11 inputs; no cancellation deadline, cleanup budget, process-startup assertion or application implementation was changed to obtain the pass.

The selection retains **250 full IDs and 247 installed IDs**, with identity source-to-installed mappings. The only three exclusions are the individually approved `test_distribution_foundation.py` dependency-tool cases:

- `test_requirement_check_detects_drift_without_rewriting_the_file`
- `test_requirement_check_fails_when_generated_input_is_missing`
- `test_repository_runtime_requirements_are_current`

These test the checkout dependency-generation tool and inputs; all three remain in the separate checkout run. The three parametrized metrics/version runtime IDs from that same file execute installed and retain their process-info label and override assertions. No additional exclusion, skip or service disposition was introduced.

I recomputed the recursive closure and import edges from the 14 seeds and matched all **37 staged files**, their exact bytes, finite references and declared purposes. This includes `test_edge_planner`, `multi_pair_support`, Docker fakes, ancestor fixtures, E04 fault/import/public-example fixtures, the maintained fault supervisor, schema references and golden fixtures. Staging contains no application source, Git tree, private dotenv or developer dataset. The namespace walk imports the real installed package; the resource inventory iterates the actual installed data directory. The runner checks the complete frozen application payload before tests and each child startup, and module receipts match target-wheel bytes. These checks do not gain a pass from an empty staged source tree or reduced reference glob.

## Independent execution and receipt audit

A fresh invocation of the prescribed runner against the explicit wheel passed in **32.64 seconds overall**, with pytest reporting **247 passed in 23.82 seconds**. Exact full collection/order, selected IDs/order and all three documented exclusions match the frozen inventory. Every selected ID has successful setup/call/teardown reports; there are no failed phases, skips, xfails, unapproved deselections, collection errors or pytest warnings. Pip check passes.

The independent raw-provenance audit verifies **137 actual Python children** against matching PID/OS creation identities, parents, pre-creation launch anchors and startup receipts. There are **138 regression process identities**, plus the subsequent pip-check startup identity, and **1,698 module-origin receipts**. Every module receipt resolves within this new environment and matches both its actual file bytes and the explicit wheel inventory. Fresh post-exec receipts also reconcile. No provenance violation, supervised timeout, leftover owned process or cleanup error occurs. Complete raw receipts were copied persistently into `s6-review/receipts/` before any temporary output can expire; their hashes/sizes are retained.

The independent checkout run separately passes **all 250 original IDs in 23.49 seconds**, including the three tool cases, with exact frozen full collection/order and no skipped/xfail/failed phase. Its audit hook is installed before pytest/source imports and blocks repository private-dotenv opens and service socket connections. The final run has zero blocked attempts. One port-zero IPv6 bind is explicitly attributed to urllib3's import-time `HAS_IPV6` availability probe; it creates no service connection. An earlier overly broad reviewer audit blocked that probe while all tests still passed; both receipts remain retained, and the final run permits the probe with its exact stack recorded.

The original S6 pytest-green/runner-red rejection, original S11 interruption rejection and accepted repair review remain preserved. S6 uses the independently accepted repaired guard; it does not reinterpret the earlier missing-child result as success. All eight raw receipt files declared by the author's final S6 machine evidence independently match their retained hashes. The new S6 result corroborates the author evidence rather than relying on its summary.

## Exact accepted inputs and limits

- Source: **307 inputs**, SHA-256 `f94b1e9e68d6bcb750752782d15c253a4b4fa28325d52ae09129c56e1e5adcf6`.
- Development environment: `ae2f84a14eebdcb4c6fef629ef822b72fcb62658806808a5e2bc8c43794a5cc3`.
- Inventory: `770da5b51b6ec05d59d219516b5fac1f2fddef60d74f2394db5c34c3e9f1ee08`.
- Wheel: `cdd73ac361c99a8c73a15c929d150ce7d5e75d9446e1229dba5a1614cd494feb`.
- Runner: `bb79a159ee3958b66b218227f738cdf35f8a9d4ad91246e6b2a081412a5afdcb`.
- Guard: `02deb3760f24718cf19165de005b0dbabc7efbbcce6034b0d0889a51cdd91ca5`.
- Pytest receipt tool: `38f1feb53fae9b0f552c24837c42c471140469f403795a4efbe4c000158cf62d`.

Reviewer commands, static closure/assertion audit, before/after captures, raw installed receipts, exact checkout outcomes and final reconciliation are retained in `s6-review/review.py`, `verify-final.py`, `static.json`, `installed-command.json`, `receipts/`, `checkout-final-command.json`, `checkout-final-receipt.json`, `retained-identities.json` and `final.json`. The earlier reviewer-only `/var` versus `/private/var` path-canonicalization assertion and the broad bind audit are retained and corrected in the final verification; neither reflects a runner failure or source repair.

S11's test-only startup synchronization is part of this installed proof. Final uninstrumented bare/image R1 timing controls, all remaining installed lanes, rebuilt artifact parity, full final checkout/service/image/B3 gates and final adversarial acceptance remain open. No private environment file was read, remote IMDb endpoint contacted, developer smoke/live service executed, foreign resource changed, Docker cleanup performed, publication action taken or E08+ work undertaken.
