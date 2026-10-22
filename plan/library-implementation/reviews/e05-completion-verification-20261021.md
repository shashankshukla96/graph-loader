# E05 delivered-code verification and repair — 2026-10-21

**Disposition: accepted. E05-R1 and E05-R2 are closed; no actionable finding remains.**
[Fresh final adversarial acceptance](e05-final-revalidation-20261021.md) verifies
the repaired code, final fixture, complete proof and all 270 input identities.

Clean entry `0638322f3b84471f63ee867861fb75b13c97760b`, original implementation
`2265e4fd1d9f76eb2813856157dea89a80b9f216`, E04 baseline
`14fe4934976f62f421086e541e6bc00dbc1ceec8`. No E06 implementation, remote IMDb
access, private environment loading, push or publication. Original E05 evidence,
findings and approvals remain historical; this report covers the delivered code
and the additional repair described below.

## Review finding and repair

**P3 E05-R1 — closed by code/test repair and independent re-review.**
On CPython 3.12, a symlink loop passed through `--run-dir`, `--plan-dir`,
`--plan-spill-dir`, `GRAPH_LOADER_TIMING_DIR` or external `--edge-plan` caused
`Path.resolve()` to raise RuntimeError outside the CLI's handled path-error
boundary. Both the independent reviewer and parent reproduced all five cases.
All three installed entry forms emitted traceback. Exit status 1 and early stop
were preserved; ingestion did not start and caller symlinks were not removed.
This was a diagnostic/programmatic-main error-handling gap, not data loss.

The [narrow execution plan](../execution-e05-revalidation.md) was independently
approved before repair. Five new public-main filesystem tests failed before the
fix. A private `_resolve_path` now translates only RuntimeError raised by
Path.resolve into ValueError, retaining its cause. Directory and external-plan
resolution use that helper. Main's existing boundary and unrelated exceptions
remain unchanged. The tests require return 1, path-bearing runtime diagnostic,
zero handler/Docker dispatch and preserved caller symlink; assertions use the
affected path rather than interpreter-specific exception wording. Initial focused checks:
**85 passed in 14.99 seconds**. No public API, schema sample, dependency, version,
image-selection or ingestion semantics changed.

**P3 E05-R2 — closed by a test-fixture correction and independent re-review.**
The fresh final reviewer found that Python 3.13's documented non-strict resolution
can leave a looping leaf unresolved; the sibling external-plan ledger may still
be usable until later handler validation. The new regression now places the
external plan below a looping ancestor, which also makes its sibling output
unusable. This preserves a meaningful pre-dispatch failure without a new skip or
product policy change. Actual native 3.12 runtime tests pass (35 cases); the
reviewer's newer-semantics emulation passes all five cases. The emulation is
explicitly not Python 3.13 support evidence. Only that one test file changes from
the first repaired freeze; all other 269 inputs and the runtime package payload
remain identical. Archives/images/B3/mixed proofs were re-audited for reuse;
the final complete-suite run uses the corrected fixture and a separate freeze.

## Fresh proof

The unchanged delivered baseline first passed **3,566 tests**, four inherited
skips, 49 deselections and 19 known fork warnings in 198.10 seconds. All **270**
baseline input bytes/sizes/modes match the tested manifest and committed HEAD;
environment identity matches. Rehashed seven retained raw logs, nine artifact
command logs and 80 image command logs, and checked the 12 retained installed
R1 cases. Exact original/rebuilt inventories match 55 runtime modules and two
approved resources, metadata, sole entry, license and dependency restrictions.

After repair, an isolated source copy outside Git produces a new wheel/sdist;
the extracted-sdist build also explicitly fails Git discovery with exit 128.
Strict Twine and exact archive checks pass, and every rebuilt wheel member byte
equals the original. Two fresh noneditable installations outside checkout pass
all module origins/bytes, inert imports, executable/module help/version/parser
parity, public examples, resources, enforced read-only package/cwd and runtime
preflight/launcher-argument probes without PYTHONPATH or development-tool rescue.

The first repaired complete suite passes **3,571 tests**, four inherited skips,
49 deselections and 19 known fork warnings in **195.69 seconds**, exit 0.
The five added passes are the new path-error regressions. Skip ownership remains
the bash-only shell control, two historical Git-object guards and intentional
multi-pair schema exclusion; 15 IMDb subset/four planned-edge fork warnings remain
A5/E08 work. No new skips or warnings. Ruff, dependency parity, pip check,
whitespace and plan rendering pass. All 270 repaired inputs and the environment
are unchanged before/after proof. [Exact results and raw receipts](../evidence/e05-revalidation-20261021.json)
distinguish baseline and repaired executions.

After the final fixture correction, the complete suite again passes **3,571 tests,
four inherited skips, 49 deselections and 19 known warnings in 196.01 seconds**,
exit 0. The separate final-before/final-after manifests match all 270 input rows
and the environment. This final run is the acceptance run; the 195.69-second
first repair run remains historical. Unchanged runtime artifacts/images/B3/mixed
proofs are reused only after their payload hashes and the other 269 input rows
were independently confirmed equal.

All four component images were rebuilt locally and tested by immutable IDs.
**76 fresh probes** pass with networking disabled, no source mounts and read-only
root filesystems plus owned temporary storage. The earlier helper attempt assumed
that all historical default-help commands already requested read-only rootfs;
that harness assertion failed and is retained. The repaired-proof helper
strengthens those four default-help invocations to the same read-only boundary;
no product/Dockerfile correction was needed for this harness failure.

**12 fresh separately supervised R1 mixed-source cases** pass across both clean
installs and all four new images, with default and finite deadlines. Source
diagnostics/causes remain observable, both workers/endpoints are reclaimed,
lock/spill ownership is released and no supervisor cutoff is needed. Exact owned
B3 passes **one test in 23.39 seconds**, no skip or timeout, empty before/after
container inventories. The original four data-bind proofs are retained/audited;
the unchanged seven-role launcher code is independently exercised again, not
claimed as a fresh complete fleet-ingestion proof.

## Independent review

[Runtime review](e05-runtime-revalidation-20261021.md) initially ran **665 focused
tests**, concurrent attempts, two-instance seven-role mounts and 21 invalid-output
controls. It found R1, independently re-reviewed the actual fix, reran the final
35 runtime tests, all original loop cases, ordinary symlink success and unrelated
exception controls, and closed R1.

[Distribution review](e05-distribution-revalidation-20261021.md) ran **151 focused
tests**, 13 archive mutations, 16 initial installed parity cases, all 55 inert
module imports and real ZIP resource lifetime checks. Its repaired-artifact
addendum independently checks both new archives/installs and all five loop settings
through direct main and all three actual command forms: 10 direct and 30 command
cases return handled status 1 without traceback or service dispatch.

The fresh final reviewer additionally executes **90 actual command negatives**:
all five loop settings through three entry forms on both repaired clean installs
and all four new images. Each exits 1 with a runtime/path diagnostic, no traceback
and preserved caller symlink. Images run without networking, with read-only
rootfs and no host mounts. Its [final whole-proof verdict](e05-final-revalidation-20261021.md) accepts after
197 focused tests, final runtime tests and complete report/evidence audit.

Raw parent outputs: `var/e05-revalidation-20261021/`; repaired proof is under
`repaired/`. Earlier baseline builds and negative controls are preserved.
Native macOS/CPython 3.12 and local Linux arm64 images do not close E07's installed
full-suite lane or E08/E09's wider support/service matrices. Host binds still
require daemon visibility; image delivery remains E06. Persistent attempts and
caller-owned shared overrides keep their documented ownership rules.

Local repair delivery: `c35a6045ef39a822dcbcfcfc2c20d6b9c2227a28`.
[Postcommit proof](../evidence/e05-revalidation-postcommit-20261021.json) confirms all 270 tested input contents,
sizes and executable flags equal committed blobs and current files; environment
identity is unchanged. A following documentation-only commit records actual
delivery identity, without recursively embedding its own hash.
