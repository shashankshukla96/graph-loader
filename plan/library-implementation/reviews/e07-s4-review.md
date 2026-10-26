# Independent E07.F1.S4 review — 2026-10-24

**Accepted. No actionable S4 finding remains.** This accepts the frozen safety
installed port only. S5–S10, rebuilt-sdist parity, required final checkout/artifact/
image/service gates, final adversarial review and E07 completion remain open.
The approved dependency order may proceed to S5.

This reviewer started without author conversation history. Actual HEAD is
`5c3f72b6cd16d0e14ac931f220a3f73eb7e5a02d` on
`codex/pypi-library-checklist`. Reviewed the supplied user handoff, canonical E07,
execution plan, accepted plan/S1/S3/S2 reviews, S4 narrative/machine evidence,
frozen inventory/original baselines, actual test/helper changes and runner/
guard/receipt/freezer implementation. Only this report and new reviewer-owned
outputs under `var/e07-validation-20261024/s4-review/` and new outside temporary
directories were written. Author inputs and previous evidence were preserved.

## Coverage and assertions

The safety lane retains **1,335 exact IDs across 18 files**, in the same frozen
order, with identity source-to-installed mapping and no exclusions or allowed
skips. Independently compared actual HEAD, preserved original source baselines
and current assertion ASTs: **all 1,827 original assertions remain**, after only
the two documented expected module-path string normalizations. Sixteen full
test files retain their original bytes. In `test_lease_server.py`, only the roots
of two existing static scans change. In `test_stream_outage_resilience.py`, the
importer/construction scan uses the actual package root, displays paths relative
to that root and additionally asserts every named scan target exists. This adds
one assertion, removing none. No application source change is present.

This preserves outcome/order/fence/rebalance/offset assertions, real ten-child
SIGTERM storm and real SIGTERM-after-loop-close regression. The unchanged parked
index tests include acknowledged add/remove/bulk operations, older/newer replay,
compacted echoes and unrelated progress, malformed same-key values, registration
before producer I/O without holding the state lock, duplicate-key caller lifetime,
metadata validation, partial confirmation and publication/install failure cleanup.
Retry fuzz sensitivity controls, resurrection/tombstone assertions and finite
LRU/revoke memory boundaries retain their complete IDs and original bytes;
no broader ordering promise or live-service guarantee is introduced.

Recomputed the recursive closure and import edges: **41 staged files**, matching
the complete current inventory and every hash. This is S3's 40-file safety closure
plus the shared location adapter introduced in accepted S2. It explicitly retains
`multi_pair_support.py`, cross-test helpers, `docker_support.py`, conftest,
package support, schemas and golden fixtures. The bounded 1.5-day selection did
not expand or require a new split.

## Independent execution and scan controls

The prescribed runner was invoked from a new outside-Git temporary directory
with `--lane safety`, the explicit candidate wheel, current inventory, new owned
output and `--timeout 600`. It exits **0** and reports **1,335 passed**, with no
skip, deselection, xfail, warning or failed phase. Independently checked every
collected/executed ID and all three successful phase reports for each ID. Pip
check passes; no timeout, leftover owned process or cleanup error appears.

Outside output:
`/private/var/folders/5p/84cp5b7s72q_ymcsd4qlvq8r0000gn/T/graph-loader-e07-s4-independent-1gfebfzd/run`.
The exact command/cwd/exit is retained in `s4-review/commands.json` and the runner
result; the complete run took 51.43 seconds. `git rev-parse --show-toplevel` from
its actual stage exits **128**. The staged inputs and generated pytest config/
manifest match their hashes; only normal pytest caches are additional generated
files. No application source or Git tree is staged.

The independent checkout command
`var/e07-validation-20261023/dev/bin/python -I var/e07-validation-20261024/s4-review/checkout-safe.py`
separately exits **0**, **1,335 passed**, with identical ordered IDs and successful
phase receipts. Its audit hook was installed before pytest/source imports;
private dotenv opens and connections beyond the loopback unit coordinator are
blocked. It records **zero blocked attempts**. The original tests still exercise
actual loopback lease coordination and operating-system SIGTERM.

The installed result binds **ten actual Python children**, matching OS creation
identities/startup receipts, and **11 regression processes**. Each child executes
the actual installed lease-server module. All **111 module-origin receipts**
resolve into the target environment and match the frozen wheel payload bytes.
The separate subsequent pip-check interpreter accounts for one additional process
startup. Independently checked all **59 installed application payload members**,
including the complete **57-file Python scan set**, against frozen hashes. Thus
the adapted recursive scans inspect the complete installed package; neither an
empty staged `src` tree nor selected critical-file presence alone supplies the
completeness proof.

Five independently executed negative controls reject: an empty actual package
root, a deprecated lease alias in an additional module, an unexpected deprecated
lease-address reference, an extra outage importer, and a missing named outage
scan target. Each defect control first passes the unchanged original scan against
a complete owned text copy, then fails after its defect is injected. The copies
are read as text only; they are never imported or staged as application code.
The empty-root control changes only the already-imported package's location
attribute temporarily. No author file is changed. Controls and retained scratch
path are recorded in `s4-review/scan-controls.json`.

Additional independent commands:

- `var/e07-validation-20261023/dev/bin/python -I var/e07-validation-20261024/s4-review/static-checks.py`: exit 0; exact closure/IDs/original assertion preservation.
- `var/e07-validation-20261023/dev/bin/python -I var/e07-validation-20261024/s4-review/verify-receipts.py`: exit 0; exact outcomes, stage/payload bytes, module/PID provenance, owned cleanup and author receipt hashes.
- `var/e07-validation-20261023/dev/bin/python -I var/e07-validation-20261024/s4-review/scan-controls.py`: exit 0; all five actual controls reject.
- `var/e07-validation-20261023/dev/bin/python -I var/e07-validation-20261024/s4-review/mechanism.py`: exit 0; **34 passed, four smoke IDs deselected**, no blocked private/service attempt.
- Targeted Ruff on the adapter, two ported tests, runner/freezer/guard/receipt: exit 0.
- `git diff --check`: exit 0.

The first reviewer receipt verifier stopped because its post-run file-set check
incorrectly counted pytest's generated `.pytest_cache` as staged author input.
Excluding that generated cache allowed the unchanged proof to verify; the first
script and explanation remain retained. This was a reviewer harness issue,
not a test/product failure. All author receipt-file hashes and the copied author
result match the S4 machine evidence; copied independent receipts and exact
commands/results are retained under `s4-review/receipts/`, `verification.json`
and adjacent files.

## Exact accepted inputs and limits

Independent safe captures before and after match each other and the author proof:

- **298 source inputs**: `3d391c8bb3fed6929ac86e64ae6fcdf837c1c4156f528d0d40df840f3ed08cb8`.
- Environment: `ae2f84a14eebdcb4c6fef629ef822b72fcb62658806808a5e2bc8c43794a5cc3`.
- Inventory: `838251e1ff9044b0e29faa0a8404aa040f37d25ffc520b9c53ab4c71ce0e4518`.
- Wheel: `cdd73ac361c99a8c73a15c929d150ce7d5e75d9446e1229dba5a1614cd494feb`.
- S4 machine evidence: `03e071ade0a767ab52bc974732895fd354b12039b62b373bc6de30b37db67382`.
- Runner: `6ccdf40e7c2397e3f7c65a163a7cdce2d2cee87ad4bf5fc9383c6ae3dcd029b9`.
- Guard: `bcca55cf4c4e34e2f9921cd0d8682e596ee7d6102494fd20ebd665a1126d5b0f`.
- Receipt: `38f1feb53fae9b0f552c24837c42c471140469f403795a4efbe4c000158cf62d`.
- Freezer: `b771d00add4c60064c2a5cbb904dc3264d0cdd175b1c743a15003c138381ee7c`.
- Adapter: `8d12d5a220ede61113ca7deef1a2169d86f8f287bbe3bc83920c94f3764ebdf5`.

No private dotenv was read, no remote IMDb endpoint contacted, and no developer
smoke, live database, Docker mutation/foreign cleanup, publication/push/tag,
workflow or E08+ work was performed. E09 retains real-service ownership. This
review does not certify subsequent stories or replace their independent gates.
