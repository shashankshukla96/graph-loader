# Independent E07.F1.S3 review — 2026-10-24

**Accepted after repair and independent re-review. All five proposed S6–S10
splits are accepted for implementation. No actionable S3 finding remains.**
This accepts the frozen inventory, fixture/reference closure, mixed-ID filtering
and story boundaries. It does not accept a legacy installed runtime lane, E09
service behavior, rebuilt-artifact parity or final E07 completion. S2 may begin
in the approved dependency order.

The reviewer started without the author's conversation history and read the
actual supplied handoff, canonical E07 and execution plan, inventory, accepted
plan/S1 reviews, S3 evidence and amended evidence, freeze/runner/receipt code,
mechanism tests, developer-smoke repair, relevant remaining tests/helpers,
packaging contract and E09 ownership. HEAD remained
`5c3f72b6cd16d0e14ac931f220a3f73eb7e5a02d` on
`codex/pypi-library-checklist`. Only this review and new reviewer-owned proof
under `var/e07-validation-20261024/s3-review/` were written by this reviewer.
No legacy test port, application change, service/smoke execution, private dotenv
read, remote IMDb contact, Docker mutation, publication or later-epic execution
was performed.

## Retained finding and repair

**[P2, resolved] Three metrics runtime IDs were incorrectly excluded as
packaging-development tests.** The original `CHECKOUT['distribution_foundation']`
classification applied to the whole file. Its three parametrized
`test_checkout_metrics_report_package_version_or_explicit_override` IDs actually
exercise packaged runtime behavior in real children: default, empty and explicit
version overrides, import side effects, metrics initialization and process-info
labels. Their historical test name does not justify losing installed coverage.
The existing installed namespace/import checks did not retain all three override
and label outcomes.

The reviewer reproduced the wrong dispositions directly from the original
inventory and policy; `s3-review/audit.json` retains them. The author preserved
the rejected inventory at `s3-inventory-before-distribution-finding.json`, ran
all three original tests unchanged through the actual isolated wheel runner,
and amended the mixed file: all three runtime IDs now belong to
public-contracts; the three dependency-generation IDs retain their explicit
checkout disposition. `disposition_amendments` records the exact IDs and
retained evidence. Review of the real three-pass receipt, child provenance and
unchanged source bytes confirms the repair. The amended public lane separately
collects exactly 250 full IDs and 247 selected IDs. The three authoring IDs are
explicit approved exclusions. The first draft is not acceptance evidence.

The author also strengthened repeat freezing during review: it now refuses
removal/reordering of prior IDs and changes to prior dispositions. Its new
regression passes. Existing original source baselines remain preserved; the
extra developer regression accounts for the one-ID increase in collection.

## Inventory, closure and approved splits

The amended inventory accounts for **3,798 exact parametrized IDs in 124 files**:
**3,650 installed, 48 service and 100 checkout-development**. The reviewer
independently collected all repository/example test IDs with private dotenv
opens and service connections blocked before collection. The IDs/order match
the frozen selection. Every selected ID has an identity mapping; mixed files
retain their full source collection and explicit exclusion reasons.

| Lane / story | Selected files | Selected IDs | Full IDs | Staged files | Days |
| --- | ---: | ---: | ---: | ---: | ---: |
| core / S2 | 12 | 454 | 454 | 32 | 1 |
| safety / S4 | 18 | 1,335 | 1,335 | 40 | 1.5 |
| cli-runtime / S5 | 11 | 636 | 636 | 29 | 1.5 |
| public-contracts / S6 | 14 | 247 | 250 | 37 | 1 |
| writer-runtime / S7 | 16 | 540 | 541 | 36 | 1.5 |
| orchestration-runtime / S8 | 18 | 282 | 284 | 42 | 1.5 |
| example-contracts / S9 | 8 | 115 | 115 | 32 | 1.5 |
| service-helper-unit / S10 | 6 | 41 | 47 | 27 | 0.5 |

**S6, S7, S8, S9 and S10 are individually approved as these bounded selections**,
after S2/S4/S5 and before F2.S1. Their estimates are reasonable for preserving
existing assertions and adapting locations/launchers, rather than rewriting the
parameter matrix. Scope growth or a materially larger launcher redesign needs a
further reviewed split before that port. Each story still requires its own
fresh independent runtime review.

The reviewer recomputed every lane's recursive closure and import edges. The
finite closure includes `reference_edge_planner`, cross-test helpers,
`multi_pair_support`, `docker_support`, ancestor package/conftest files, dynamic
fixture/script targets, eight schema YAMLs and four golden JSON files. The E04
fault supervisor is the sole finite plan-tree Python reference; maintained
example Python/README references are individually hashed. No application source,
Git tree, dataset or private dotenv is staged. Pytest, psutil and testcontainers
are runner/development inputs; pandas/python-dotenv are confined to example test
environments. Strict runtime metadata retains the eight mandatory dependencies
and no extras.

The remaining checkout classifications are justified by their actual subjects:
dependency/image-building tools, smoke-setting helpers, proof-harness ownership,
authoring docs/monitoring/classifiers and the direct third-party wall-clock
microbenchmark. Its six deterministic runtime checks stay installed. Historical
Git slice checks retain checkout-only dispositions while behavioral IDs from
the same files remain installed. The 48 actual-service IDs retain E09 ownership,
including the source Docker-build case and six live IDs in mixed helper files;
none supplies an installed pass through a skip. The intentional multi-pair
schema skip is recorded by exact ID and reason.

Original test source/assertion records are retained. Independent comparison
matched the 123 unchanged test files; only the deliberately extended inventory
mechanism-test file differs from its preserved earlier baseline. The developer
smoke repair adds the import regression and moves credential loading into
actual smoke execution while preserving existing precedence/password controls.

Literal scans remain material port obligations: the three metrics scans, two
lease-name/reference scans and outage importer/construction guard must inspect a
nonempty complete installed package with the same checks. The tracked-schema
Git guard must use the complete frozen schema set. Real fault supervision,
minimal child environments and fake Kafka import timing must retain their
assertions under the accepted runner. Collection does not prove these runtime
adaptations; no staged empty `src` tree or checkout rescue is approved.

## Independent validation and boundaries

- Development mechanism selection: **34 passed, four smoke IDs deselected**;
  retained at `s3-review/mechanism-tests.log`. The actual pytest mixed-file
  inclusion/filter controls exercise Receipt, rather than only comparing lists.
- Guarded full source collection: **3,798 IDs, 124 files, exact IDs/order match**;
  `s3-review/source-collection-result.json`. An actual `-k` reduction collects
  3,643 IDs; the inventory check rejects it against 3,798 with the expected
  disappearance/deselection error (`s3-review/deselection.json`). Missing files,
  changed helpers, removed/reordered IDs and refreeze reclassification controls
  also pass.
- Independently checked all eight installed collection receipts against current
  staged bytes, full/selected IDs, approved exclusions and current runner hashes.
  Seven unchanged lanes reuse byte/ID-identical retained receipts; public has a
  fresh amended receipt. All have no collection errors or runtime outcomes.
  Recomputed retained receipt hashes and copied reviewed receipts into
  `s3-review/receipts/`. These are collection proofs, **not runtime lane passes**.
- Inspected the actual unchanged three-metrics runtime proof and eight-ID
  accepted S1 parent/child regression receipt under the current tools. They
  retain successful calls and actual provenance; they do not certify later
  legacy lanes.
- Re-ran strict wheel/sdist inspection and matched the frozen artifact contract,
  including source payload bytes, metadata, two resources, namespace, finite
  archive members and dependency policy. This is the S3 input contract, not
  F2.S1's future rebuilt-sdist proof.
- Reproduced original developer-test eager dotenv loading from Git source with
  the private open blocked **before reading**. The repaired import and full
  source collection succeed under the block. The author's earlier actual
  constraint breach remains disclosed and cannot be undone by the repair.

No full checkout regression run, legacy installed runtime lane, image/service
proof or final gate is certified here. All accepted S1 protections and E04–E06
behavioral regressions remain obligations of the ordered subsequent gates.

## Exact accepted inputs

Independent recapture matches [amended machine evidence](../evidence/e07-s3-final.json):

- Source: **297 inputs**, SHA-256
  `e8a239983444be85837d9747567a16f618a2e9864479ff2fe124e3cec6690ee8`.
- Development environment: SHA-256
  `ae2f84a14eebdcb4c6fef629ef822b72fcb62658806808a5e2bc8c43794a5cc3`.
- Frozen inventory: SHA-256
  `bffd54d7ac72fac127a5e26920839e9fb5e5ebbcbf7ad09f5a1f15ddab571917`.
- Amended machine evidence: SHA-256
  `a181aecb6bce03a596bf922379277e128887dd5a4058c7ffb9602c4802fca365`.
- `freeze_installed_inventory.py`:
  `b771d00add4c60064c2a5cbb904dc3264d0cdd175b1c743a15003c138381ee7c`.
- `verify_installed.py`:
  `f8decb54fe444e4a3ab3a7ceb09c453ee76506065e4233cb9841d0957fd18ca5`.
- `installed_pytest.py`:
  `38f1feb53fae9b0f552c24837c42c471140469f403795a4efbe4c000158cf62d`.
- `installed_guard.py`:
  `bcca55cf4c4e34e2f9921cd0d8682e596ee7d6102494fd20ebd665a1126d5b0f`.

Reviewer commands/results, full input identities and artifact hashes are in
`var/e07-validation-20261024/s3-review/final-verification.json`,
`current-source.json` and their adjacent retained scripts/logs. The original
S3 evidence and rejected inventory remain intact.
