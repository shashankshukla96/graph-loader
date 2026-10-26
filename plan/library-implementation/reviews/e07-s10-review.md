# Independent E07.F1.S10 review — 2026-10-25

**Disposition: accepted for the frozen service-helper-unit lane. No actionable S10 finding remains.** F2.S1 may proceed in the approved dependency order. This does not accept rebuilt-sdist parity, E09 service guarantees or final E07 completion.

This fresh zero-history review read canonical E07, the execution plan, approved S3 splits, S9 acceptance, the frozen inventory, S10 evidence and raw receipts, runner/receipt/closure code, all six selected test files and their maintained helper/reference closure, and explicit E09 ownership. Review took place on `codex/pypi-library-checklist`, HEAD `5c3f72b6cd16d0e14ac931f220a3f73eb7e5a02d`. Only this report, reviewer-owned evidence under `var/e07-validation-20261024/s10-review/`, and a new outside-Git output/environment were written. No application, test, runner or inventory source was edited.

## Selection, assertions and closure

All **six selected test files**, including their live tests, match actual HEAD and frozen original bytes. Their **61 original assertion ASTs** remain identical in order. All **47 full collected IDs** remain; exactly **41 unit IDs** have identity source-to-installed mappings. There are no approved unit skips. Application source has no HEAD diff, and independent before/after captures preserve the exact source and development-environment identities.

The six approved exclusions retain their original integration markers, assertions and explicit `service` / `E09` dispositions:

- `tests/integration/test_relationship_correctness_regressions.py`: `test_planned_classic_and_lease_modes_all_match_the_expected_graph`; `test_missing_endpoint_fails_closed_by_default_in_every_mode`.
- `tests/test_planned_harness.py`: `test_run_planned_mode_matches_the_independently_computed_expected_graph`; `test_run_planned_mode_raises_on_a_writer_failure_instead_of_a_partial_result`; `test_run_planned_mode_twice_leaves_no_leftover_chunk_topic`.
- `tests/test_self_reference_isolation_e2e.py`: `test_public_bulk_cli_isolates_person_knows_after_shared_works_at`.

These are the exact accepted S3 mixed-file service exclusions, not runtime omissions introduced by S10. They were collected but never executed in either reviewer run. The source assertions still cover actual graph/property/ordering behavior, writer failure, topic cleanup and shared-then-isolated public CLI behavior; those guarantees remain open under E09.

Recomputed recursive local imports, ancestor fixtures and finite schemas/goldens match all **27 staged files**, their hashes and frozen import edges. This includes the scratch-database guard, lock-wait and planned-harness helpers, imported DLQ integration probe module, star-topology helper and social example generator. There is no staged application source, Git tree, dataset or private environment file. The planned-harness source check reads the complete nonempty staged maintained helper; it does not substitute an empty file.

The unchanged unit tests retain forbidden/malformed database rejection, explicit credential precedence and missing-credential skip/fail exceptions, retry-module capability outcomes, real writer retry-log counting, query-log shape/offset/error checks, exception propagation, metric parsing and closed-port behavior, real social last-write evidence and schema validation. Tests asserting expected `pytest.skip.Exception` remain passing unit assertions, not skipped tests. Container/log/HTTP operations use their original explicit doubles except the original owned loopback closed-port probe. None proves live Neo4j lock waits or actual service correctness. There are no extra lane test dependencies or runtime dependency changes.

## Independent execution and provenance

The prescribed installed runner against the explicit wheel passed in **9.48 seconds overall**; pytest reports **41 passed in 0.41 seconds**. Full collection/order exactly matches all 47 frozen IDs, selected collection/order matches all 41 unit IDs, and approved exclusions match the exact six IDs and reasons. Every selected ID has passing setup/call/teardown phases. There are no skips, xfails, pytest warnings, collection errors, failed phases, unapproved deselections or receipt errors. Pip check passes. All supervised commands exit successfully without timeout, leftover owned processes or cleanup errors.

Raw provenance independently reconciles **one regression process, zero actual Python children and 50 application module-origin receipts**. The second raw process record belongs to the subsequent pip check. All module origins and bytes match the explicit wheel inside the new environment. All **59 application payload members**, including both resources, and immutable distribution files match wheel bytes independently whether imported or not. Both author and reviewer installs are noneditable explicit-wheel installations; all recorded installed RECORD hashes and sizes validate. The initial reviewer assumption that installed RECORD itself should equal archive RECORD bytes was corrected for pip's normal install-time rewrite; this harness-only correction is retained in `assumption-correction.json`.

The author's retained raw receipts independently reconcile the same **41 selected / 47 full / six exclusions**, **one regression process / zero children / 50 module receipts**, with all eight declared receipt hashes verified and no process/provenance/cleanup error.

The separate guarded checkout run uses the original `-m 'not integration and not smoke and not stress'` selection and reports **41 passed, six deselected in 0.22 seconds**. Full/selected IDs, exact excluded IDs and all three successful phases match the frozen inventory. It has no warning, skip, xfail, collection error or blocked attempt. The audit hook starts before pytest/source imports and blocks private dotenv/dataset reads and prohibited service connections. Its three socket events are urllib3's import-time IPv6 loopback port-zero probe and the original unit test's loopback port-zero bind followed by its closed-port connection attempt. No live service or Docker operation was performed.

## Exact accepted inputs and remaining gates

- Source: **307 inputs**, SHA-256 `f94b1e9e68d6bcb750752782d15c253a4b4fa28325d52ae09129c56e1e5adcf6`.
- Development environment: `ae2f84a14eebdcb4c6fef629ef822b72fcb62658806808a5e2bc8c43794a5cc3`.
- Inventory: `770da5b51b6ec05d59d219516b5fac1f2fddef60d74f2394db5c34c3e9f1ee08`.
- Wheel: `cdd73ac361c99a8c73a15c929d150ce7d5e75d9446e1229dba5a1614cd494feb`.
- Runner: `bb79a159ee3958b66b218227f738cdf35f8a9d4ad91246e6b2a081412a5afdcb`.
- Guard: `02deb3760f24718cf19165de005b0dbabc7efbbcce6034b0d0889a51cdd91ca5`.
- Pytest receipt tool: `38f1feb53fae9b0f552c24837c42c471140469f403795a4efbe4c000158cf62d`.

Commands, before/after captures, source/assertion/closure audit, copied installed receipts and hashes, exact checkout outcomes and socket stacks, and author reconciliation are retained in `s10-review/review.py`, `before.json`, `after.json`, `static.json`, `installed-command.json`, `installed-verification.json`, `receipts/`, `retained-identities.json`, `checkout-command.json`, `checkout-receipt.json`, `reconcile_author.py`, `author-reconciliation.json` and `final.json`.

F2.S1 and all final E07 gates remain incomplete. The root's separate full-checkout, four-image and exact B3 results remain independent retention gates; this review did not alter them. Accepted S11 startup protections remain unchanged; no new concern required repeating the full S1 matrix. No private environment was read, remote IMDb endpoint contacted, developer smoke/live service executed, foreign resource changed or Docker globally pruned, publication action taken or E08+ work undertaken.
