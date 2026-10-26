# Independent E07.F1.S5 review — 2026-10-24

**Accepted. No actionable S5 finding remains.** This accepts the frozen
CLI/runtime installed port only. S6–S10, F2 rebuilt-sdist parity, required final
checkout/artifact/image/service proofs, final adversarial review and E07
completion remain open. The approved dependency order may proceed to S6.

This reviewer started without author conversation history. Actual HEAD is
`5c3f72b6cd16d0e14ac931f220a3f73eb7e5a02d` on
`codex/pypi-library-checklist`. Reviewed the supplied handoff, canonical E07,
execution plan, accepted plan/S1/S3/S2/S4 reviews, current inventory and preserved
baselines, S5 narrative/machine evidence and retained first-green/red proof,
actual test diff, location adapter and runner/guard/receipt/freezer. Only this
report and new reviewer-owned outputs under
`var/e07-validation-20261024/s5-review/` and new outside temporary directories
were written. Author inputs and earlier evidence were preserved.

## Coverage and behavior

The selection retains **636 exact IDs across 11 files**, in the frozen order,
with identity source-to-installed mapping, no excluded IDs and no allowed skips.
Recomputed recursive closure/import edges and every staged hash: **30 files**,
the S3 29-file selection plus the accepted shared location adapter. Config,
golden/reference fixtures, cross-test helpers, `docker_support.py`, conftest
and package support remain explicitly staged. The live Docker-build ID retains
its independently accepted E09 disposition; it does not supply a unit pass.

Independently compared actual HEAD, original-source baseline and current ASTs:
**all 775 original assert expressions remain**, as do original mock assertions.
Eight complete files retain their original bytes. The only three changed files
are metrics scan locations, early-stop child launch locations and the repaired
database initialization failure test. No application source change is present.
No runtime dependency or warning-filter change supplies this result.

The unchanged shutdown tests deliver real SIGTERM/SIGINT to the test process,
verify the shared shutdown event and restored handlers, exercise all eight
launch/drain/plan/dispatcher/stream shutdown phases, require an actually started
fake fleet and exactly one effective stop per container, and retain rollback,
failed-stop retry, concurrent cleanup, watcher and parseable metrics-snapshot
assertions. Stop-readiness tests retain the deterministic exit-137 reproduction,
single-stop graceful exit, bounded readiness wait and foreign-container behavior.
Metrics retains catalogue/type/label/bucket checks, real loopback exposition,
fresh-process import side effects and all three static guards.

The early-stop helper now gives its real target-Python children only PATH and
the owned fake-Kafka directory on PYTHONPATH, with that directory as cwd. The
existing signal-during-import exit-0/diagnostic/no-continued-import assertions
and no-stop exit-7 control remain intact. The installed guard lives in the
target environment, so it requires no checkout or environment-variable rescue.

## Independent execution and controls

The prescribed runner was executed from a new outside-Git directory:

```text
var/e07-validation-20261023/dev/bin/python scripts/verify_installed.py --lane cli-runtime --wheel var/e07-validation-20261023/s1-artifacts/neo4j_graph_loader-0.1.0a1.dev0-py3-none-any.whl --inventory plan/library-implementation/test-inventory.json --output /var/folders/5p/84cp5b7s72q_ymcsd4qlvq8r0000gn/T/graph-loader-e07-s5-independent-plrzqjht/run --timeout 600
```

The recorded actual command uses absolute repository/input paths and cwd
`/var/folders/5p/84cp5b7s72q_ymcsd4qlvq8r0000gn/T/graph-loader-e07-s5-independent-plrzqjht`.
It exits **0** in 92.28 seconds: **636 passed**, no skips, deselections, xfails
or warnings. Pip check passes. No failed/timed-out phase, leftover owned process
or cleanup error appears. Actual Git discovery from its stage exits **128**.

The independent checkout command
`var/e07-validation-20261023/dev/bin/python -I var/e07-validation-20261024/s5-review/checkout-safe.py`
separately exits **0**, **636 passed** without warnings. Its audit hook was
installed before pytest/source imports and blocks private dotenv reads and
nonloopback service connections; it records **zero blocked attempts**. Exact
collected/full/executed IDs and all three successful phases per ID match the
frozen inventory in both executions.

Independent receipt verification binds **three actual Python children** to
matching OS creation/startup identities, **four regression processes** and
**57 application module receipts**. Two children execute the installed
`loader.node_loader`; one imports installed `utils.metrics`. Every application
origin resolves into the target environment and matches the frozen wheel bytes.
The subsequent pip-check interpreter accounts for one further startup. All
**59 installed payload members**, including the complete **57 Python files**,
match the frozen inventory. The actual Python file set equals that inventory.
All staged inputs and generated config/manifest hashes also match. Thus the
three adapted scans cover the complete installed package, with no empty `src`
tree or selected-critical-file-only completeness argument.

Six independently executed controls reject: an empty actual application root,
an unsafe metric label, unsafe metrics-entrypoint name, per-record metric
emission, an actual wheel missing `cli.py`, and an actual wheel missing
`data/graph_schema.yaml`. Each metrics defect is injected into an additional
module in a complete owned textual package copy after its healthy scan passes;
the original scan function then fails. Those copies are never imported or staged
as application code. Both omitted-member wheels fail the real runner preflight
with exit **1**, naming missing/unexpected members.

The author-reported original initialization-test hole is independently confirmed.
Executing the exact preserved original function with an owned earlier Docker
failure satisfies **all original assertions with zero driver calls**. Executing
the repaired function through fakes satisfies all assertions with **one image
preparation call, one driver call** and the exact diagnostic
`Schema initialization failed: DB offline`. The added DockerService fake reaches
the intended branch. Original source/red replay/first green remain retained;
the first green warning is not accepted as final evidence. Both complete final
author reruns and both independent reruns are warning-free.

Additional independent commands, all exit 0:

- `var/e07-validation-20261023/dev/bin/python -I var/e07-validation-20261024/s5-review/static-checks.py`: IDs, closure, hashes and assertion preservation.
- `var/e07-validation-20261023/dev/bin/python -I var/e07-validation-20261024/s5-review/controls.py`: all six controls and old/repaired failure-branch replay.
- `var/e07-validation-20261023/dev/bin/python -I var/e07-validation-20261024/s5-review/verify-receipts.py`: exact outcomes, stage/payload bytes, process/module origins and author receipt hashes.
- `var/e07-validation-20261023/dev/bin/python -I var/e07-validation-20261024/s5-review/mechanism.py`: **34 passed, four smoke IDs deselected**, zero blocked private/service attempts.
- Targeted Ruff on the three modified tests, adapter and runner/guard/receipt/freezer; `git diff --check`.

Exact commands/exits, full logs, copied installed receipts, verification and
control results remain under the reviewer-owned directory. No reviewer harness
failure or product failure occurred.

## Exact accepted identities and limits

Independent safe before/after captures equal each other and the author proof:

- **298 source inputs**: `aa118604964e8b58cfc58310c966056ecf5d33e1002eaf8822f63fe46e33ea5c`.
- Environment: `ae2f84a14eebdcb4c6fef629ef822b72fcb62658806808a5e2bc8c43794a5cc3`.
- Inventory: `f137761bf412bf09c524564d5570560ee1a83ec59261b3723b9384c59fcda3a6`.
- Wheel: `cdd73ac361c99a8c73a15c929d150ce7d5e75d9446e1229dba5a1614cd494feb`.
- S5 machine evidence: `2d1fede39ffbcdb170ea869e58d84a7fb6ac1049e0bd8b06a209f41bdc99a94c`.
- Runner: `6ccdf40e7c2397e3f7c65a163a7cdce2d2cee87ad4bf5fc9383c6ae3dcd029b9`.
- Guard: `bcca55cf4c4e34e2f9921cd0d8682e596ee7d6102494fd20ebd665a1126d5b0f`.
- Receipt: `38f1feb53fae9b0f552c24837c42c471140469f403795a4efbe4c000158cf62d`.
- Freezer: `b771d00add4c60064c2a5cbb904dc3264d0cdd175b1c743a15003c138381ee7c`.
- Adapter: `8d12d5a220ede61113ca7deef1a2169d86f8f287bbe3bc83920c94f3764ebdf5`.

No private dotenv was read, remote IMDb endpoint contacted, live developer
smoke/database test executed, Docker mutation/foreign cleanup performed, or
push/upload/tag/publication/workflow/E08+ work undertaken. External services were
faked; real-service guarantees remain owned by E09. This review does not certify
subsequent stories or the final E07 gates.
