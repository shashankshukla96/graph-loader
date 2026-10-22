# E05 fresh final adversarial revalidation — 2026-10-21

**Disposition: accepted. No open actionable E05 finding.**
R1 is closed by the narrow resolution repair and independent controls. R2 is
closed by the corrected fixture and its independent regression runs. The final
full suite, frozen source identity and tracked proof support acceptance before
local delivery; a future commit identity remains the parent delivery check.

## Independent scope

Read the E05 epic, execution/revalidation plan, actual E04-baseline-to-E05 changes,
current runtime/test patch, resource/entry/preflight code, and runtime mounts/timing
threading. Reviewed entry HEAD `0638322f3b84471f63ee867861fb75b13c97760b`,
implementation `2265e4fd1d9f76eb2813856157dea89a80b9f216`, baseline
`14fe4934976f62f421086e541e6bc00dbc1ceec8`. Prior approvals were not acceptance
evidence. Scoped reports were read after independent code/controls, and their
specific claims were checked against the actual repaired artifacts and receipts.

## P3 E05-R2 — closed: external-plan loop fixture portability

The new external `--edge-plan` regression uses a symlink loop at the plan filename
and requires `main` to fail before its handler. [Python 3.13 documentation](https://docs.python.org/3.13/library/pathlib.html#pathlib.Path.resolve)
specifies that non-strict `Path.resolve` returns the unresolved remainder for loops.
The sibling ledger is then an ordinary file, so preparation may legitimately
complete and dispatch to later plan validation. Other four settings still fail
when creating the unusable directory. Package metadata allows Python >=3.12;
D04 explicitly approves 3.12 proof and defers broader matrix evidence.

A narrow semantic control substitutes `Path(os.path.realpath(path, strict=False))`
for `Path.resolve`: the leaf-loop external plan reaches the controlled handler,
while `<loop>/plan.json` fails before it. This is explicitly an emulation of the
documented newer behavior, not an executed Python 3.13 support claim. Actual native
Python 3.13 is not available. Evidence: ignored
`var/e05-final-revalidation-20261021/python313-semantics.json` and executable probe.

The parent applied the recommended correction: use `<loop>/plan.json` only for the `edge-plan` parameter.
The existing looping ancestor makes its sibling ledger unusable on both behaviors,
still exercises the RuntimeError repair on 3.12, preserves the caller link, and
requires no product change, skip or expanded platform claim. The reviewer reran
all **35 runtime tests (0.30s)** on actual Python 3.12, and the actual **five new
tests passed (0.11s)** under the documented 3.13 semantic emulation, closing R2.
Logs: `runtime-final-r2.log` and `runtime-python313-semantics-r2.log`.

## Executed controls and proof audit

- **197 focused tests passed in 44.48s**, using the populated development environment:
  runtime paths, CLI, planned-edge CLI, resources and installed entry cases.
- **14 independent controls passed**: five real two-node cycles; OSError,
  PermissionError and FileNotFoundError pass through; resolution RuntimeError
  becomes ValueError with the original cause; RuntimeError from preparation,
  handler and directory creation remains observable; valid caller directory links,
  file bytes, locks and permissions are preserved; all 270 frozen inputs match.
- **90 actual command negatives passed**: five path settings × three entry forms ×
  two fresh noneditable installations/four new immutable images. Exit 1, runtime
  diagnostic and affected path, no traceback, and retained symlink in every case.
  Images use network none, read-only rootfs, no host mounts and owned /tmp only.
- Independently reinspected the actual wheel, sdist and rebuilt wheel against the
  current source with the strict inspector: exact 55 modules/two resources,
  metadata/entry/license/dependency closure, byte-equal rebuilt members. Checked
  no-Git discovery failures, clean site origins, installed probes and image hashes.
- Rehashed **76 image check logs** and **12 mixed real-spawn case logs**, checked
  immutable IDs, deadlines, source causes, children/endpoints/owned-state reclamation.
  Audited exact owned B3: one pass in 23.39s, no skip/timeout, empty before/after
  inventories. Read the complete **3571 passed, 4 skipped, 49 deselected, 19 known
  warnings in 195.69s** full-suite log.
- Rehashed the initial **210 tracked raw receipts**, recomputed source/environment manifest
  hashes, checked all 270 source bytes/sizes/executable bits and before/after
  identity, and reviewed the completion report, tracker, E06 handoff, execution
  plan, evidence JSON and original-proof qualification. Pending-final status was
  accurate at that review; fresh data-bind/fleet-ingestion or wider matrix claims
  were not made.

Own ignored outputs: `var/e05-final-revalidation-20261021/`, particularly
`focused-final.log`, `controls.json`, `installed-negatives.json`,
`evidence-audit.json`, `tracked-evidence-audit.json`, and individual negative logs.
First reviewer attempts used the base environment without the source import path
and then without its absent console entry; these harness failures are retained in
`focused.log`/`focused-corrected.log`. The populated environment rerun passed.
The initial audit compared macOS /var and /private/var spellings; canonicalizing
only the probe paths corrected that harness check. No product failure is inferred.

## Boundaries

No product/test edits, commit, push, existing .env reads, remote services or foreign
cleanup by this reviewer. Native macOS/Python 3.12 and Linux arm64 image proof
remain bounded; no E07 installed full-suite lane, E08/E09 wider platform/service
matrix, E06 image-delivery work or release authorization is implied. Runtime
attempts persist and explicit shared overrides retain caller ownership. The helper
catches only errors from resolution; main's broader error boundary is unchanged.

The final fixture-only change alters one of 270 source-input rows. All other
269 rows and the recorded environment remain identical; packaged runtime bytes
remain `2f968ec88bc4abee8645ba4228bfdb98e3ebe3139abc2dd994592a6276c15969`.
The reviewer re-audited the existing repaired archives/images/mixed/B3 receipts
against that final source identity. Those proof executions predate the fixture
adjustment and are valid for the unchanged package; they are not claimed as
newly rebuilt images or reexecuted ingestion after the test-only change.
`final-identity-audit.json` and `final-evidence-audit.json` record this check.

## Final acceptance audit

The corrected-fixture complete acceptance run exits 0: **3571 passed, four
inherited skips, 49 deselected, 19 known warnings in 196.01s**. Independently
read the final log, rehashed all **217** final tracked raw receipts, recomputed
manifest digests, and checked **270** final-before/final-after/current input
bytes, sizes and executable flags. Recorded environment identity is equal.
Final source manifest SHA-256:
`b9b8faec4c88ff142f9d74bbd679411f06a8322712143f91232cee88d3fe6511`.

Re-reviewed the final completion report, exact evidence JSON, execution plan,
tracker, E06 handoff and original-proof qualification. They retain R1/R2 history,
distinguish the 195.69s first-repair run from the 196.01s acceptance run, and
explicitly attribute reused package proof after the one-file fixture change.
The broader platform, service and installed-full-regression limitations remain
stated accurately. `git diff --check` passes. No further correction is required.

Final audit outputs: `final-tracked-evidence-audit.json`,
`final-evidence-audit.json`, `final-identity-audit.json`; prior controls and
failed reviewer harness attempts remain preserved.
