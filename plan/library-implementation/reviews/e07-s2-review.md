# Independent E07.F1.S2 review — 2026-10-24

**Accepted. No actionable S2 finding remains.** This accepts the frozen core
installed regression port only. S4 and subsequent installed lanes, rebuilt-sdist
parity, live service proofs, final adversarial review and E07 completion remain
open. The approved dependency order may proceed to S4.

This reviewer started without author conversation history. Actual HEAD is
`5c3f72b6cd16d0e14ac931f220a3f73eb7e5a02d` on
`codex/pypi-library-checklist`. Reviewed the supplied handoff, canonical E07,
execution plan, accepted plan/S1/S3 reviews, S2 narrative/machine evidence,
frozen inventory/baselines, actual core tests/reference helpers/fixtures,
runner/guard/receipt/freezer and safe source-capture changes. Only this report
and new reviewer-owned evidence under
`var/e07-validation-20261024/s2-review/` were written. No author source, frozen
inventory, application code or prior proof was changed.

## Coverage and adaptation

All **454 exact core IDs across 12 files** retain their identity mapping and full
collection; there are no exclusions. Independently compared every original
assertion AST with both the frozen baseline and actual HEAD: **519 assertion
expressions match**. Eleven complete test files retain their original bytes.
The sole legacy port changes the tracked-schema location lookup to
`tests/installed_support.py`; its original assertion remains unchanged.

Independently recomputed the complete recursive closure and import edges. The
core now stages **33 files**, reflecting the one new location adapter in addition
to S3's 32-file freeze. Eight maintained schema YAMLs, four golden JSONs,
`reference_edge_planner.py`, cross-test helpers, `multi_pair_support.py`,
`docker_support.py`, conftest and package support remain explicit and hashed.
Reference comparison, deterministic/spilled and process-based plans, source
validation, encoding, single/multi-type/topic/pair assertions remain executable.
No overflow, altered parameter matrix, replacement golden or runtime change was
introduced.

The adapter uses the runner-generated manifest only in installed staging;
checkout still independently uses `git ls-files config examples` (27 files in
the current checkout). Its returned reference bytes are verified against their
frozen hashes. The actual installed manifest equals the full frozen staging
mapping. All staged bytes and generated pytest config/manifest hashes were
independently checked. There is no staged application tree or Git metadata;
actual `git rev-parse --show-toplevel` from the accepted stage exits **128**.
Thus the schema guard does not rely on Git rescue or an empty staged source scan.

## Independent execution and controls

The actual prescribed runner was independently invoked from a new outside-Git
temporary directory with `--lane core`, the explicit candidate wheel, current
inventory, a new owned output and `--timeout 600`. It exits **0** and reports
**453 passed, one exact inherited intentional skip**. The skip is only:

`tests/test_schema_multi_pair.py::test_every_shipped_schema_stays_single_pair[examples/multi_topic/graph_schema_multi_pair.yaml]`

Its reason remains `the multi-pair example is multi-pair on purpose`. All 454
collected/executed IDs and their order match the frozen selection; no failure,
xfail, deselection, added skip or approved exclusion appears. Pip check passes.
The independent checkout run separately reports **453 passed and the same skip**
with identical IDs/order. Its audit block was installed before pytest or source
imports and records no private-dotenv or service-connection attempt.

The real installed run binds **five actual Python children** to matching startup
and OS creation identities, **six test processes**, and **171 module receipts**.
Every application module receipt resolves to the target wheel environment and
has the exact frozen payload hash. Planner workers and the logging-import child
are covered; the multiprocessing resource tracker starts the guard but imports
no application module. The later pip-check interpreter has its own additional
startup record and is distinct from the six regression processes. No provenance
violation or leftover-owned-process/cleanup error appears.

Independent actual controls reject empty, missing, altered and escaping
reference inputs. Running the unchanged original schema-coverage assertion with
the adapter and a real owned newly uncovered schema also rejects, proving that
the adapted assertion remains nonvacuous. A full runner invocation with an owned
inventory copy containing an altered golden hash exits **1** before test setup,
naming the exact mismatched golden. The author receipts and hashes, including
the first successful run and final path-hardened run, were independently checked.
No prior evidence was overwritten.

Additional reviewer commands:

- `var/e07-validation-20261023/dev/bin/python -I var/e07-validation-20261024/s2-review/checkout-safe.py`: exit 0, 453 passed / 1 exact skip.
- `var/e07-validation-20261023/dev/bin/python -I var/e07-validation-20261024/s2-review/mechanism.py`: exit 0, **34 passed**, four smoke cases deselected.
- Targeted Ruff on runner/freezer/adapter/ported test/capture tool: exit 0.
- `git diff --check`: exit 0.
- Independent safe source/environment capture before and after: exact equality.

Full installed command, selected/collected/outcome receipts, module/PID origins,
fixture/control commands and exits, copied logs/generated stage files and input
hashes are retained in `s2-review/verification.json`, `reference-controls.json`,
`final-verification.json` and adjacent files. Outside run:
`/private/var/folders/5p/84cp5b7s72q_ymcsd4qlvq8r0000gn/T/graph-loader-e07-s2-independent-au7zern0/run`.
The first reviewer checkout launcher incorrectly ran pytest at module top level,
causing spawn-bootstrap reentry; that reviewer harness failure and exact-owned
process-tree cleanup are retained separately. The guarded `__main__` launcher
then passed unchanged product/test inputs. This is not a product regression.

## Exact accepted inputs and limits

Independent before/after capture matches the S2 author proof:

- **298 source inputs**: `8a8b604b92b1641e044e701473d7737fb13286936fdafff5564ab9add68f9282`.
- Environment: `ae2f84a14eebdcb4c6fef629ef822b72fcb62658806808a5e2bc8c43794a5cc3`.
- Inventory: `d37c56d3bbada9d2c4d2bf00100c1388fe70c4c9b3ba924f0a8b95ba150e4a50`.
- Wheel: `cdd73ac361c99a8c73a15c929d150ce7d5e75d9446e1229dba5a1614cd494feb`.
- Machine evidence: `d0551be5bd62398386e43d4e7e8c9e283c1ea3b8d90726beba28f89b39863f2d`.
- Runner: `6ccdf40e7c2397e3f7c65a163a7cdce2d2cee87ad4bf5fc9383c6ae3dcd029b9`.
- Guard: `bcca55cf4c4e34e2f9921cd0d8682e596ee7d6102494fd20ebd665a1126d5b0f`.
- Receipt: `38f1feb53fae9b0f552c24837c42c471140469f403795a4efbe4c000158cf62d`.
- Freezer: `b771d00add4c60064c2a5cbb904dc3264d0cdd175b1c743a15003c138381ee7c`.
- Adapter: `bcf605f4824864c8ae49dc6b2bce614a92d4c8ea0c01e517288781b0a717db23`.

No private dotenv was read, no remote IMDb endpoint contacted, and no live
service, Docker mutation, foreign cleanup, publication/push/tag or E08+ work was
performed. This review does not certify later stories or replace their required
fresh independent gates.
