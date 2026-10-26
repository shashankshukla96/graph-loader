# E07 inventory and assertion revalidation — 2026-10-26

**Result: no actionable finding in this review scope.** The delivered inventory,
staged fixture closure, selected/excluded ID reconciliation, assertion adaptations
and bounded story partition satisfy the E07 requirements examined below. The
[delivered-code revalidation plan](../execution-e07-revalidation.md) is suitable:
it separates direct historical proof inspection from fresh execution, requires
all eight lanes for both freshly built artifacts, and keeps E09 and E08 open.
Final acceptance still depends on the parent's fresh execution and final audit.

## Identity and method

This reviewer independently inspected actual code, inventory, plan, baseline diff
and retained raw receipts, without inheriting prior reviewer conclusions. Entry
HEAD is `91d765d18d5711402764bb5be51e3cd9c3bbed89`; delivered implementation is
`9b1ddd2ff24f3d86b1a19aa35ea188c66e6155d7`; comparison baseline is
`5c3f72b6cd16d0e14ac931f220a3f73eb7e5a02d`. The inventory SHA-256 is
`770da5b51b6ec05d59d219516b5fac1f2fddef60d74f2394db5c34c3e9f1ee08`.

Fresh checks used the existing development Python, with bytecode writes and
pytest's cache provider disabled. Independent source collection and inventory
unit checks installed audit hooks that reject private dotenv reads and network
connections. Both completed with **zero blocked/attempted private/network events**.
This reviewer changed only this report and owned ignored scratch under
`var/e07-inventory-revalidation-20261026/`; no product/test file, environment,
service, image or network object was mutated.

## Complete classification and exact outcomes

Fresh source collection produced **3,799 IDs in 124 files**, in exactly frozen
order. Each disposition was recomputed from actual collected markers/file policy
and matched the committed per-ID record. Independent checks reconciled every
file's ID list and category counts against the complete source collection:

- 3,650 IDs belong to eight installed unit lanes.
- 48 IDs explicitly remain actual-service work owned by E09.
- 101 IDs explicitly remain checkout development checks.

Every selected ID appears exactly once in the installed partition. Each lane's
full collection equals selected IDs plus its explicit, nonoverlapping excluded
IDs. Actual retained pytest receipts for all sixteen original/rebuilt runs have
exact full/selected ID order, exact approved exclusion reasons, no unapproved
pytest deselection, no receipt errors, no xfail, and no failed phase. They account
for **7,298 passing calls and two approved skips**, one intentional multi-pair
schema-example skip per wheel. The exact skipped ID and required reason are
recorded in the core lane; every other installed ID has a successful call.

The mixed-file exclusions are explicit and bounded: three dependency-generation
checks; one Prometheus wall-clock microbenchmark; two historical Git-slice checks;
and six live service checks. Runtime metrics version/override/import-side-effect
IDs remain installed in public-contracts. All deterministic metrics-overhead
checks remain installed. Behavioral assertions in the two historical mixed files
remain installed. The service-helper mixed files retain their independent unit
checks while live graph/writer/cleanup proofs remain E09-owned. Checkout-only
files otherwise concern developer configuration/docs, image-building/proof tools,
and the installed-runner/inventory mechanisms. These rationales match their
actual code; no difficult application regression was found reassigned by default.

Fresh `tests/test_installed_inventory.py` execution reports **8 passed**. It
exercises disappearance/reordering, changed fixture bytes, recursive helpers,
unknown-runtime classification refusal, full mixed-file partition validation,
re-freeze refusal, and actual pytest approved exclusion/unapproved filtering.

## Fixture/config closure

The recursive closure was recomputed independently for all eight lanes. Its
staged path sets, recursive import-edge maps, and every staged file hash match
the inventory. Counts are:

| Lane | IDs | Selected files | Closure files |
| --- | ---: | ---: | ---: |
| core | 454 | 12 | 33 |
| safety | 1,335 | 18 | 41 |
| cli-runtime | 636 | 11 | 30 |
| public-contracts | 247 | 14 | 37 |
| writer-runtime | 540 | 16 | 36 |
| orchestration-runtime | 282 | 18 | 43 |
| example-contracts | 115 | 8 | 32 |
| service-helper-unit | 41 | 6 | 27 |

The union contains 139 maintained files. `tests/conftest.py`, cross-test imports,
`reference_edge_planner`, `multi_pair_support`, package ancestors, dynamic child
fixtures, scratch/lock/planned helpers, all eight maintained schema YAMLs and all
four golden JSON files are covered explicitly. The external E04 fault supervisor
is an explicit reference exception. Generated pytest configuration clears
checkout pythonpath/addopts; staging does not include application `src`, Git,
private settings or datasets. Example pandas/dotenv dependencies stay test-only.

## Assertion and regression preservation

All 122 pre-existing test files remain represented; two new files contain E07
runner/inventory development controls. AST comparison found **no lost pre-existing
test function**. Baseline-to-delivery inspection found no assertion loss except
the equivalent package-relative expected-path literal rewrite in the stream
outage source scanner. That rewrite preserves the exact two importer expectation
and adds a nonempty target check. Lease and metrics scanners inspect the actual
nonempty imported package. Single-topic reference enumeration verifies the frozen
staged fixture hashes. The early-stop child moves to an owned temporary cwd and
retains its fake Kafka path without adding checkout application imports.

The CLI initialization-failure case now additionally proves the intended image
build/database initialization branch and exact failure log. The private dotenv
collection repair defers settings reads to actual smoke execution and adds an
import regression. Original assertion ASTs are preserved in the frozen baseline;
the developer dotenv file's saved baseline includes that explicitly recorded
pre-freeze privacy repair. No original assertion is lost there. E04 application,
public cancellation/deadline/cleanup and child fault-test bytes are unchanged.

The five overflow lanes have explicit finite file/ID/helper selections, owners,
1–1.5-day bounds (service-helper 0.5 day), dependencies and validation. Their plan
stories precede runtime acceptance; the S11 repair is a separate bounded story
before public-contracts acceptance. Completion labels are supported by exact
receipts rather than being treated as proof themselves.

## Archive exclusion, parity and retained proof

Direct strict inspection of both retained wheels against the retained sdist
passed: **65 members per wheel, 71 sdist files**, exact current runtime/module,
resource and metadata contract. Both wheels have identical members and member
bytes, SHA-256
`ac3500e2b6f6a3907b41e47c8acee0cf22f7fb4685414b046a6596ba80919700`.
The exact package and distribution metadata hashes equal the installed inventory.
The finite archive allowlist excludes tests, runner, plan/reviews, private
settings, datasets and operational state. The runtime payload contains 57 Python
modules and precisely two schema YAML resources. Extra test dependencies are not
runtime metadata. All **154 retained raw receipt files** independently match
their recorded size and SHA-256.

These archive and all-sixteen receipt inspections are direct audits of retained
historical proof, not claims of a fresh build or fresh installed execution. The
parent's revalidation plan correctly requires those fresh checks separately.
Docker/images/service runtime behavior is outside this reviewer's execution
scope. No E09 live-service or E08 cleanup completion is inferred from unit lanes.

## Reproduction records

Owned ignored evidence:

- `var/e07-inventory-revalidation-20261026/audit.py` and `static-audit.json`:
  full partition/closure/hash/assertion and strict archive/receipt reconciliation.
- `collect.py`, `collection.log`, `collection.json`, `collection-audit.json`:
  3,799 exact collected IDs, actual markers/fixtures, exit 0, no audit events.
- `unit.py`, `unit.log`, `unit-audit.json`: 8 passing controls, exit 0, no audit events.
- `receipt_hashes.py`, `receipt-hashes.json`: all 154 historical proof hashes/sizes.
- `test-functions.json`: 122 baseline files, two new files, zero lost test functions.

## Amended inventory acceptance after the runner repairs

The earlier sections describe the initial delivered snapshot and retained
historical proof. After the runner reviewer reproduced the SIGINT cleanup/receipt
and unsupported `forkpty` provenance gaps, the parent applied the separately
reviewed narrow tooling repair. **The refreshed inventory is accepted within this
review scope.** Runner lifecycle acceptance remains owned by its independent
reviewer, and all affected fresh final proof remains required.

Current inventory SHA-256 is
`ea0a618d1f62df26ddb616ceb7db162c6aa85fc63a85bfdcbb73a066c5fbbf93`.
Fresh independent collection now reports **3,804 IDs in the same 124 files**:
3,650 installed, 48 E09 service, and 106 checkout development. This is precisely
five added checkout tooling IDs, from three new parametrized/test functions in
`tests/test_installed_runner.py`: interrupted supervisor cleanup/receipt with and
without a controlled cleanup error; main's preserved KeyboardInterrupt/SystemExit
failure manifest; and pre-child rejection of unsupported `os.forkpty`.

Direct structured comparison against entry HEAD proves:

- All original 3,799 IDs remain in their original relative order; each original
  disposition is exactly unchanged. Only the five named tooling IDs are added.
- All eight installed lane objects are exactly unchanged, including selected and
  full collected IDs, explicit exclusions, allowed skip, stage hashes, recursive
  imports, references, requirements and effort. Recomputed closures/hashes match.
- The complete `original_source_baseline` is exactly unchanged. The old runner
  test module's entire AST remains an exact prefix of the amended module; existing
  test functions, fixtures, assertions and parametrizations were not rewritten.
- All other file classifications are unchanged. The runner-control file alone
  grows from 24 to 29 collected checkout IDs. No runtime coverage is reclassified.
- Package payload, distribution metadata, finite archive member lists, version,
  resources, all 57 module hashes and artifact contract fields other than actual
  archive path/size/hash receipts are exactly unchanged. Strict inspection of the
  freshly referenced wheel/sdist equals the refreshed artifact contract.
- Application `src` remains byte-identical to the E06 baseline. Actual changes
  are the two runner tooling files, additive controls and explicit inventory
  extension; this is not a silent baseline replacement.

The independent collection recomputed every actual disposition from the current
markers/file policy and passed inventory validation. The eight inventory controls
and all 29 runner controls pass together: **37 passed in 1.66 seconds**, no skip.
Both source collection and control execution used private-read/network audit
hooks and recorded zero events. Existing historical sixteen-lane receipts remain
historical proof bound to the prior inventory hash; the parent's fresh final gate
must bind new receipts to the current runner tools and inventory hash.

Amendment evidence is preserved separately under
`var/e07-inventory-revalidation-20261026/repair/`: `audit.py`/`audit.json`,
`collect.py`/`collection.json`/`collection.log`/`collection-audit.json`, and
`unit.py`/`unit.log`/`unit-audit.json`. Initial review evidence remains intact.
