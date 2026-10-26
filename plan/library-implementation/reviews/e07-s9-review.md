# Independent E07.F1.S9 review — 2026-10-25

**Disposition: accepted for the frozen example-contracts lane. No actionable S9 finding remains.** S10 may proceed in the approved dependency order. This does not accept remaining installed lanes, rebuilt-sdist parity, E09 service guarantees or final E07 completion.

This fresh zero-history review read canonical E07, its execution plan, the accepted S3 overflow boundaries, S8 acceptance, inventory, S9 evidence and raw receipts, runner/receipt/guard code, and the example tests and finite helper/reference closure. Review took place on `codex/pypi-library-checklist`, HEAD `5c3f72b6cd16d0e14ac931f220a3f73eb7e5a02d`. Only this report, reviewer-owned evidence under `var/e07-validation-20261024/s9-review/`, and a fresh outside-Git output/environment were written. No application, test, runner or inventory source was edited.

## Selection, assertion preservation and closure

All **eight selected test files** match actual HEAD and their original frozen pre-port bytes. Their complete **353 original assertion ASTs** match in order. All **115 full collected IDs** are retained with identity source-to-installed mappings; there are no exclusions or approved skips. Application source has no HEAD diff, and independent before/after captures preserve the exact source and development-environment identities.

Recomputed recursive local imports, ancestor fixtures, explicit reference targets and finite schemas/goldens match all **32 staged files**, their hashes and frozen import edges. The closure explicitly includes the IMDb builder/planner/publisher helpers, README YAML reference, social and multi-topic generators, star-topology helper and cross-test planner helpers. It contains no application source tree, Git tree, dataset or private environment file. The example test modules' existing path shims resolve only within staging during the installed run; installed application provenance proves they supply no checkout rescue.

The unchanged tests generate small owned TSV/JSON fixtures. They retain actual vectorized-versus-row-reference graph comparisons across chunk and process boundaries, exact publisher bytes and key/shard ordering, failure propagation, planner record multiplicity and pair order, hub/resource outcomes, byte budgets, graph normalization/comparison and README schema behavior. Service-facing operations use their existing explicit producers/admin/driver/process doubles. `load_dotenv` is patched in the tests that enter example `main`; importing the helper does not load credentials. No real IMDb data or remote database is needed for this lane. `pandas` and `python-dotenv` remain explicit lane-only test requirements; independent wheel metadata inspection confirms neither became a runtime dependency or extra.

## Independent execution and provenance

A fresh invocation of the prescribed installed runner against the explicit wheel passed in **19.82 seconds overall**, with pytest reporting **115 passed in 4.26 seconds**. Full and selected collection/order exactly match the frozen inventory. Every ID has successful setup/call/teardown phases. There are no skips, xfails, deselections, failed phases, collection errors or pytest warnings. Pip check passes, and every supervised command has no timeout, leftover owned process or cleanup error.

The independent raw audit reconciles **43 actual Python children**, **44 regression process identities**, and **820 application module-origin receipts**. The extra raw process record is the subsequent pip check. Each launch has a matching child PID, OS creation identity, actual parent PID, fresh startup tick and target environment. All module origins and bytes match the explicit target wheel, and all **59 application payload members**, including both resources, match independently whether imported or not. No violation or unaccounted child appears. The actual launches comprise 35 forks and eight Python subprocesses; subprocess targets are the staged builder with owned synthetic input paths.

The author's retained run independently reconciles **42 children / 43 regression processes / 779 module receipts**, with all eight declared raw receipt hashes verified. These are valid run-specific counts: the failing-source parallel derivation can start a different number of pending workers before reporting the original expected failure. The review therefore accounts for every actual launch rather than requiring a fixed worker count. The initial reviewer harness's fixed-count assumption was corrected and retained in `assumption-correction.json`; neither runtime evidence nor source inputs changed.

The separate guarded checkout selection reports **115 passed in 3.71 seconds**, with exact full collection/order and no skip, xfail, deselection, warning or failed phase. Its parent/fork audit hook starts before pytest/source imports, blocks private dotenv/dataset reads and prohibited service connections, and permits genuine local unit IPC and port-zero availability probes. It records zero blocked attempts and one socket event: urllib3's import-time IPv6 loopback port-zero probe. Separately launched builder scripts consume only the explicit synthetic paths, as shown by the original tests and installed launch receipts. No live Neo4j/Kafka/Docker or real-dataset guarantee is inferred.

## Exact accepted inputs and remaining gates

- Source: **307 inputs**, SHA-256 `f94b1e9e68d6bcb750752782d15c253a4b4fa28325d52ae09129c56e1e5adcf6`.
- Development environment: `ae2f84a14eebdcb4c6fef629ef822b72fcb62658806808a5e2bc8c43794a5cc3`.
- Inventory: `770da5b51b6ec05d59d219516b5fac1f2fddef60d74f2394db5c34c3e9f1ee08`.
- Wheel: `cdd73ac361c99a8c73a15c929d150ce7d5e75d9446e1229dba5a1614cd494feb`.
- Runner: `bb79a159ee3958b66b218227f738cdf35f8a9d4ad91246e6b2a081412a5afdcb`.
- Guard: `02deb3760f24718cf19165de005b0dbabc7efbbcce6034b0d0889a51cdd91ca5`.
- Pytest receipt tool: `38f1feb53fae9b0f552c24837c42c471140469f403795a4efbe4c000158cf62d`.

Commands, before/after captures, source/assertion/closure audit, copied installed receipts and hashes, exact checkout outcomes and socket stack, and author reconciliation are retained in `s9-review/review.py`, `before.json`, `after.json`, `static.json`, `installed-command.json`, `installed-verification.json`, `receipts/`, `retained-identities.json`, `checkout-command.json`, `checkout-receipt.json`, `reconcile_author.py`, `author-reconciliation.json` and `final.json`.

S10, F2.S1 and all final E07 gates remain incomplete. The root's separate full-checkout and exact B3 results are independent evidence, not final acceptance supplied by this story review. Accepted S11 startup protections are unchanged; no new concern required repeating the full S1 matrix. No private environment was read, remote IMDb endpoint contacted, developer smoke/live service executed, Docker resource changed or pruned, publication action taken or E08+ work undertaken.
