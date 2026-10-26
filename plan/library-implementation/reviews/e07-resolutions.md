# E07 retained findings and resolutions — 2026-10-23

Current disposition: [S1 final independent re-review](e07-s1-review.md) accepts
all five repairs; no S1 finding remains. Earlier pending statements below are
retained author response history, not the current verdict.

[Plan](e07-plan-review.md) is accepted. S1's [initial independent review](e07-s1-review.md)
requests four repairs; author responses are not acceptance. Original scripts,
actual red reproductions and rejected proof remain retained.

- S1-R1, P1: env/POSIX/multiprocessing launcher bypass. Parent reproduces exact
  original scripts accepting three counterfeit imports. Supported launchers now
  preflight target interpreter/flags; every supported creation API declares PID
  and OS creation identity, and post-exec receipts must be fresh. Actual positive
  and negative launch/fork/spawn/handshake controls pass; re-review pending.
- S1-R2, P1: detached child survives success. Parent independently reproduces
  full runner leak, kills only the exact owned PID and retains evidence. Repaired
  guarded test commands reclaim matching receipt-bound identities; leftovers or
  cleanup errors fail the run. Actual detached control and no-foreign-identity
  safety regression pass; re-review pending.
- S1-R3, P2: output under unrelated Git ancestry. Parent independently reproduces
  zero Git discovery exit in accepted staging. Resolved ancestry now rejects
  directory/pointer/default-temp cases before staging; actual ordinary repo and
  linked-worktree controls pass. Re-review pending.
- S1-R4, P2: missing mandatory metadata masked by tool dependencies. Parent
  independently reproduces the actual wheel missing both Neo4j declarations
  passing. Freeze all six distribution member hashes, independently strict-check
  canonical metadata before forming that contract. Actual modified-metadata,
  missing-module and missing-resource archives reject. Re-review pending.
- Author S1-A1: process0 without final pytest receipt. Exact old code accepts a
  real os._exit(0) test; parent now requires the final receipt. Actual negative
  control rejects; independent re-review pending.

Other retained author failures: initial collection script missed checkout root
for cross-test imports (20 errors); corrected checkout-only discovery collects
3766 baseline IDs without changing installed paths. First repaired probe used
an incorrect expected console-version format (7pass/1fail); corrected fixture
retains the actual established command contract. No product behavior was changed
for either harness repair. Initial S1 green17-test/probe evidence is superseded
by repaired21-test/eight-positive/14-negative proof pending independent acceptance.

Raw directories: `var/e07-validation-20261023/s1-review/`, `s1-author-red/`,
`s1-original-source/`, `s1-repaira-failed/`, `s1-repairb-owned-receipts/`.

Additional independent S1-R5, P2: rejected existing --output still overwrote its
prior result.json. Parent reproduced the overwrite only on an owned sentinel;
explicit owned flag now gates every failure write and is set only after
exclusive creation. Actual directory/symlink/checkout sentinel bytes and modes
are preserved, including independent rechecks. Final C reruns all mechanism
controls:8positive IDs/17actualnegative invocations/23development tests, with
new exact294-input/environment capture. All preceding B/first-repair figures
are historical. Final independent re-review is pending.

## S3 acceptance — 2026-10-24

[Independent S3 review](e07-s3-review.md) accepts the inventory/closure, runner
extensions and five bounded S6–S10 splits. Resolved P2: three metrics version/
label/import runtime IDs were initially checkout-only; author reproduced all
three unchanged in the isolated runner and moved them to public-contracts.
Original inventory/proof retained. The earlier private dotenv discovery read
is disclosed in S3 evidence; lazy smoke-only settings and blocked-read regression
prevent collection/unit reads. Re-freezing now refuses silent losses or changes
to existing dispositions. All later runtime/final gates remain outstanding.

## S2/S4 acceptance and retained S5 finding — 2026-10-24

[Core](e07-s2-review.md) and [safety](e07-s4-review.md) are independently accepted.
The current CLI gate is prepared for review. Author S5-A1: the initial green
CLI run emitted a Docker SDK credential-helper warning because the intended
database-init failure test did not fake DockerService. The exact original test
passes all its assertions after an injected earlier image failure, with zero
driver calls. The original source, replay, first green/warning and checkout run
remain retained. A missing fake and three additional image/driver/cause assertions
now require the intended database failure. Both complete 636-ID reruns are green
without warnings; original assertions remain. Independent S5 disposition is
pending. No runtime change or warning suppression was used.

[S5 independent acceptance](e07-s5-review.md) closes the author init-failure
finding after fresh original/fixed branch controls and both 636-case reruns.
No actionable S5 issue remains. Additional runtime ports and final gates remain open.

## S11 repair and S6–S8 acceptance — 2026-10-25

The original S6 run passed pytest247 but correctly failed runner provenance for
three immediately terminated real children. The independently approved bounded
[S11 plan](e07-startup-plan-review.md) retained all original E04 test/application
bytes and introduced a test-only bounded startup-receipt wait. The initial
[implementation review](e07-s11-review.md) found a caught BaseException could
escape without a durable violation, allowing a delayed genuine receipt to erase
failure. The author independently reproduced the exact KeyboardInterrupt false
success before repair. The guard now durably records every interrupted wait and
preserves interruption types; all three maintained interruption controls have
successful pytest receipts yet correctly fail the runner after genuine delayed
child receipts arrive. Original red/C proof remains retained.

The [fresh repair re-review](e07-s11-repair-review.md) independently accepts the
307-input repair, all14 positive IDs/24 negative invocations and unchanged
public247selection. Separate [S6](e07-s6-review.md), [S7](e07-s7-review.md) and
[S8](e07-s8-review.md) reviews accept their exact frozen lanes and original
assertions. No application code or runtime dependency was changed.

Retained final-suite harness rejection: the first owned script lacked a main
process guard, so multiprocessing spawn re-entered pytest. Only the exact owned
process group was stopped; all recorded identities are verified gone. The
original script/log and `full-suite-harness-rejection.json` remain under
`var/e07-validation-20261025/final/`. Adding the main guard fixes only the proof
script. The corrected suite actually passes3746 cases, with4 inherited skips,
49 deselections and19 known fork warnings; source/environment before and after
match. No source test or assertion was changed. A Darwin EPERM after duplicate
emergency cleanup of that already-empty group is retained; the newer generic
supervisor accepts that error only after verifying no live group members remain.
The successful full-suite run itself exits0 without this cleanup exception.


## Final independent acceptance — 2026-10-25

[S9](e07-s9-review.md), [S10](e07-s10-review.md) and [F2](e07-f2-review.md)
are independently accepted. The separate fresh
[final adversarial review](e07-final-adversarial-review.md) accepts E07 for local
delivery without actionable findings. Its new1971 installed regressions,
14launcher controls/35developer controls, interruption/detached/post-exec negatives,
all154 raw hashes and complete gate reconciliation pass at the same307-input
source/environment. Local commit and actual per-file equality documentation
remain separate required steps; no future SHA or publication is claimed.
