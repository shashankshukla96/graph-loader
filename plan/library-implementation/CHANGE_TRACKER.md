# Library implementation change tracker

Track the library/PyPI program from checklist preparation through E12. Dates
below use America/New_York and describe the actual commit or validation date.
This is a history index, not a replacement for Git diffs or acceptance evidence.
No entry authorizes a GitHub push, image/package upload or publication.

**Current gate — E06 independently accepted, 2026-10-23:**
Clean entry `a2bc7bbd6e82350a25e9a127454452c360c1ceda`. User authorized only
[E06 execution](execution-e06.md). [Fresh final acceptance](reviews/e06-final-adversarial-review.md)
closes all three stories and the retained bounded repairs. [Final proof](evidence/e06-final-validation.md)
records 3,624 full-suite passes, four inherited skips, 49 deselections and 19 known
warnings, strict artifacts/two installs/four immutable images/two actual loads/
12 supervised R1 controls and exact no-skip B3. Local delivery/equality will be
recorded after the authorized commit using its actual full Git SHA.
No E07–E12 implementation, push/upload/release tag/publication/workflow.

**Prior gate — E05 repaired and independently accepted, 2026-10-21:**
[Fresh E05 verification](reviews/e05-completion-verification-20261021.md) found
and repaired low-priority E05-R1 symlink-loop diagnostics. The original delivery
below remains historical. The narrow repair, five regression cases and
[independent runtime closure](reviews/e05-runtime-revalidation-20261021.md) are
complete. Repaired full suite: 3,571 passed, four inherited skips, 49 deselections,
19 known warnings. [Final adversarial acceptance](reviews/e05-final-revalidation-20261021.md)
closes R1/R2 and verifies all 270 final inputs. Local repair delivery is `c35a6045ef39a822dcbcfcfc2c20d6b9c2227a28`;
[postcommit equality](evidence/e05-revalidation-postcommit-20261021.json) confirms tested/committed/current identity. E06 handoff is ready.

**Original E05 delivery acceptance — 2026-10-21:**
[Final review](reviews/e05-final-adversarial-review.md) accepts all three stories
and closes the retained product/proof/documentation findings. [Exact proof](evidence/e05-final-validation.md)
records3566passes, four inherited skips,49 deselections,19 known warnings,
no-skip B3, two clean wheel installs, true outside-Git rebuild, four immutable
images and12public R1 controls. E04-R1 remains closed. All270 finald tested inputs
and environment are unchanged. Local delivery is `2265e4fd1d9f76eb2813856157dea89a80b9f216`.
[Postcommit equality](evidence/e05-postcommit.json) verifies all270 tested,
committed and current bytes/sizes/modes plus the unchanged environment. This
documentation-only follow-up records the actual hash; its own hash is in Git log
under rule5.
E06–E12, upload, push, tag and publication remain unstarted/unauthorized.

## Committed changes

| Date | Epic / change | Exact local commit | What changed | Verification / evidence |
| --- | --- | --- | --- | --- |
| 2026-10-21 | E05.F1.S1, E05.F1.S2, E05.F2.S1 local delivery | `2265e4fd1d9f76eb2813856157dea89a80b9f216` | Installed command/module/version, two finite schema resources, private persistent runtime paths and early preflight through planner/metrics/seven launcher roles. | [Final acceptance](reviews/e05-final-adversarial-review.md), [exact proof](evidence/e05-final-validation.md), [postcommit equality](evidence/e05-postcommit.json):3566passes,4 inherited skips,49 deselections,19 known warnings; artifacts/true no-Git rebuild/two installs/four images/12R1/B3 pass. |
| 2026-10-11 | Checklist foundation | `d53a92cfef652b7aae5aca6472736c10e851db42` | Added the library/PyPI checklist and independent checklist validation. No distribution implementation was claimed. | [Checklist review](../../docs/PYPI_CHECKLIST_REVIEW.md), [checklist](../../docs/PYPI_LIBRARY_CHECKLIST.md). |
| 2026-10-17 | E03 handoff preparation | `6de3562083cc7079091d056b89d66f18b69931e8` | Added the migration handoff and independent handoff review, including full SDLC, test/build gates and repeat-until-resolved independent review. Earlier implementation remained uncommitted then. | [Handoff review](reviews/handoff-e03-review-20261017.md). |
| 2026-10-17 | E00 prerequisites | `bf5471e9243dbda90cdf066a5d811486119dba7c` | First committed the inherited planner ownership/recovery fixes, schema/logging fixes, service-independent test repairs and maintained DLQ fixture/harness. Preserved them during the E03 migration. | [E00 baseline](evidence/E00.F1.S1.md), [DLQ proof](evidence/E00.F2.S1.md). |
| 2026-10-17 | E01 release decisions | `bf5471e9243dbda90cdf066a5d811486119dba7c` | First committed the approved D01–D07 identities, API/compatibility/support targets, mandatory dependencies, image ownership/delivery and manual-publication decisions, along with plans and reviews. | [Decision register](decisions.md), [direct approval record](evidence/e01-user-decisions-20261016.md). |
| 2026-10-17 | E02 distribution foundation | `bf5471e9243dbda90cdf066a5d811486119dba7c` | First committed the version/metadata scaffold, authoritative runtime dependency generation, development build inputs, metrics version authority and associated tests/evidence. | [E02 metadata](evidence/E02.F1.S1.md), [dependencies](evidence/E02.F1.S2.md), [version](evidence/E02.F2.S1.md). |
| 2026-10-17 | E03 atomic namespace migration | `bf5471e9243dbda90cdf066a5d811486119dba7c` | Moved all runtime modules to `neo4j_graph_loader`; migrated imports, patch/subprocess/pickle targets, examples/docs and four Docker entry paths; adopted editable development installs and complete restricted package discovery; added namespace tests and artifact verification. No generic `src` shim. | [E03 completion](evidence/e03-completion.md), [independent final review](reviews/e03-final-adversarial.md), [resolution history](reviews/e03-resolutions.md). Full suite 3,387 passed; exact B3 passed; artifacts/images verified. |
| 2026-10-17 | E03 postcommit evidence | `ffb862537b83ef7be053fb96e05d12fcbcfdd355` | Recorded content/size/mode equality of the 245 existing tested source inputs after the implementation commit. Documentation/evidence only; no new runtime behavior. | [Postcommit comparison](evidence/e03-postcommit.json). |
| 2026-10-18 | E03 verification / E04 handoff | `06ae36c05d724c47f99fa0d4d98dbf1f76aa9902` | Added fresh E03 verification and independent review, refreshed the executable E04 handoff, introduced this tracker and linked current status. Documentation/evidence only; no E04 implementation or runtime change. | [Current verification](reviews/e03-completion-verification-20261018.md), [independent source acceptance](reviews/e03-revalidation-independent-20261018.md), [handoff/tracker acceptance](reviews/e04-handoff-tracker-review-20261018.md). |
| 2026-10-18 | E03 tracker bookkeeping | `98cd267db5c70d3517a89ec68c1a4cedf2c254fd` | Recorded the actual E03 verification-delivery SHA in this tracker; tracker-only change. | Git file history and `git show` preserve the exact bookkeeping diff. |
| 2026-10-20 | E04 public API and bounded planner lifecycle | `55dd4938188768090c10d851f8f7330706975ea5` | Delivered all six ordered stories: explicit schema/planning API, ownership, closed framed IPC, operation cancellation/deadline, aggregate cleanup with retention diagnostics and independent metrics/import effects. | [Final3512-pass/B3/artifact/image proof](evidence/e04-final-validation.md), [per-story resolutions](reviews/e04-resolutions.md), [fresh final acceptance](reviews/e04-final-adversarial-review.md), [259-input postcommit equality](evidence/e04-postcommit.json). |
| 2026-10-20 | E04 delivery evidence / E05 draft handoff | `786260bec3cceb8ed14990896380640cb52115d2` | Recorded E04's actual delivery identity, postcommit input equality and the conditioned E05 handoff. No runtime source change. | [Postcommit proof](evidence/e04-postcommit.json), [E05 handoff](handoff-e05.md). Its gate was subsequently blocked by E04-R1; the accepted repair is recorded below. |
| 2026-10-20 | E04 revalidation: changes requested | `d987ad8d16eedfc70b45fcccb2a4fd6b088f2cb9` | Recorded independently reproduced P1 E04-R1, fresh passing baseline/artifact/image/service checks, reopened E04 S3/S4 and gated E05. Review/status/evidence only; no product fix. | [Current report](reviews/e04-completion-verification-20261020.md), [independent lifecycle finding](reviews/e04-lifecycle-revalidation-20261020.md), [exact evidence](evidence/e04-revalidation-20261020.json). |
| 2026-10-21 | E04-R1 repair / S3-S4 acceptance restored | `aa03f2c0d71409a7ed46534ec57780e5bef966bf` | Propagate a received worker failure before waiting on peers or applying a post-reply interruption check; retain its cause and enter shared cleanup. Add supervised default/finite mixed-source and deterministic cause-priority regressions. | [Fresh3516-pass/full/B3/artifact/image/mixed proof](evidence/e04-r1-repair.md), [final independent acceptance](reviews/e04-r1-final-review.md), [259-input committed equality](evidence/e04-r1-postcommit.json). |
| 2026-10-21 | E04-R1 delivery evidence | `1871898ea350d0e17c0e926b3b87595fbde0c2ef` | Recorded the actual repair commit and equality of all 259 tested, committed and current inputs. Documentation/evidence only. | [Postcommit proof](evidence/e04-r1-postcommit.json). |
| 2026-10-21 | E04 delivered repair revalidation | `369cb0d43c6ca94ebe1c8d7a7557f5d1d31fb41d` | Recorded fresh full-suite verification, original reproduction, source/evidence audit and another independent approval. Four review/evidence/tracker files only. | [Completion report](reviews/e04-completion-verification-20261021.md), [results](evidence/e04-revalidation-20261021.json), [independent review](reviews/e04-r1-independent-revalidation-20261021.md). |
| 2026-10-21 | E05 original delivery evidence | `0638322f3b84471f63ee867861fb75b13c97760b` | Recorded original E05 delivery identity and equality of all 270 tested/committed/current inputs before the later review repair. Documentation/evidence only. | [Original postcommit proof](evidence/e05-postcommit.json). |
| 2026-10-21 | E05-R1 repair / R2 fixture correction and fresh acceptance | `c35a6045ef39a822dcbcfcfc2c20d6b9c2227a28` | Handle symlink-loop path errors through the existing CLI boundary; add five public-main regressions with a portable looping-ancestor plan fixture. Retain all findings, independent reviews and fresh proof. | [Completion report](reviews/e05-completion-verification-20261021.md), [final independent acceptance](reviews/e05-final-revalidation-20261021.md), [postcommit equality](evidence/e05-revalidation-postcommit-20261021.json): 3,571 passes; no open finding. |

**Commit boundaries matter:** E00, E01 and E02 were completed in a dirty checkout
and first committed together with E03 in `bf5471e`. The repeated hash above is
intentional. There are no separate historical implementation commits for those
epics; do not invent them. The entry/mechanical manifests distinguish inherited
prerequisites from the migration itself. Historical validation dates can precede
the date those contents were committed.

## E04 delivered locally — 2026-10-20

This section records the original delivery. The later R1 revalidation reopened
its acceptance, and the subsequent repair below restores it. That revalidation
itself made no product repair; original delivery remains preserved in Git.

Entry HEAD `98cd267db5c70d3517a89ec68c1a4cedf2c254fd`, clean checkout.
All six E04 stories completed in approved order; [execution plan](execution-e04.md),
[entry manifest](evidence/e04-entry.json), [final evidence](evidence/e04-final-validation.md),
[resolution index](reviews/e04-resolutions.md) and
[final independent acceptance](reviews/e04-final-adversarial-review.md).
Fresh3,512 tests passed, four inherited skips,49 deselections,19 known warnings;
exact B3 passed without skip; rebuilt artifacts/two clean installs/four immutable
image matrices passed. Implementation/delivery commit: `55dd4938188768090c10d851f8f7330706975ea5`. The
[postcommit proof](evidence/e04-postcommit.json) verifies all259 delivery inputs
against both committed blobs and current contents/size/mode; environment identity
is unchanged. This following tracker/evidence commit changes documentation only
and does not recursively embed its own enclosing hash.
E05–E12 and publication remain out of scope; [E05 handoff](handoff-e05.md) prepared.

## Current validation and delivery

| Date | Reviewed source commit | Work | Delivery commit / state |
| --- | --- | --- | --- |
| 2026-10-18 | `ffb862537b83ef7be053fb96e05d12fcbcfdd355` | [Fresh E03 completion verification](reviews/e03-completion-verification-20261018.md): 3,387 test passes, exact no-skip DLQ proof, rebuilt artifacts/two clean installs and four new-image matrices. [Independent source review](reviews/e03-revalidation-independent-20261018.md) accepts E03; [handoff/tracker review](reviews/e04-handoff-tracker-review-20261018.md) approved after H1 was fixed and rechecked. Added this tracker and refreshed [E04 handoff](handoff-e04.md); no product/E04 implementation. | Delivered locally in `06ae36c05d724c47f99fa0d4d98dbf1f76aa9902`; source reviewed at `ffb8625` is unchanged. This tracker-only follow-up records the actual new hash. |
| 2026-10-20 | `55dd4938188768090c10d851f8f7330706975ea5` | [E04 final validation](evidence/e04-final-validation.md):3,512 passes, exact no-skip B3, rebuilt artifacts/two clean installs/four immutable image matrices and [independent final acceptance](reviews/e04-final-adversarial-review.md). | Local implementation delivered at that SHA; [postcommit equality](evidence/e04-postcommit.json) verifies259 inputs, with this documentation-only follow-up recording the hash. |
| 2026-10-20 | `786260bec3cceb8ed14990896380640cb52115d2` | [Current E04 revalidation](reviews/e04-completion-verification-20261020.md): fresh 3,512 passes, no-skip B3, clean artifacts/52 image checks, but **E04-R1 reproduced independently and by the primary agent**. [Transport review](reviews/e04-transport-revalidation-20261020.md) found no transport defect; [lifecycle review](reviews/e04-lifecycle-revalidation-20261020.md) requests correction. S3/S4 reopened; E05 blocked. Product code unchanged. | Recorded locally in `d987ad8d16eedfc70b45fcccb2a4fd6b088f2cb9`. This tracker-only follow-up records the actual reporting commit; R1 remains unfixed. |
| 2026-10-21 | `aa03f2c0d71409a7ed46534ec57780e5bef966bf` | [R1 repair validation](evidence/e04-r1-repair.md):3,516 passes, exact no-skip B3, isolated original/rebuilt artifacts, clean installs, four new immutable image matrices and12 checkout-free mixed cases; [fresh final review](reviews/e04-r1-final-review.md) accepts. | Local repair delivered at that SHA; this documentation-only follow-up records actual identity and [all259-input equality](evidence/e04-r1-postcommit.json). |
| 2026-10-21 | `1871898ea350d0e17c0e926b3b87595fbde0c2ef` | [Delivered repair revalidation](reviews/e04-completion-verification-20261021.md): fresh 3,516 passes, four inherited skips, 49 deselections, 19 known warnings; original reproduction and static checks pass, all 259 tested/committed/current inputs agree, retained artifact/image/B3 proof audited. [Fresh independent review](reviews/e04-r1-independent-revalidation-20261021.md) approves with 221 focused passes and 7 extra public probes. | Delivered locally in `369cb0d43c6ca94ebe1c8d7a7557f5d1d31fb41d`; final report/evidence/tracker independently approved. E04 accepted and E05 handoff ready. No product change or E05 implementation. |
| 2026-10-21 | `0638322f3b84471f63ee867861fb75b13c97760b` plus narrow E05-R1 repair | [Fresh E05 verification](reviews/e05-completion-verification-20261021.md): 3,571 passes, four inherited skips, 49 deselections, 19 known warnings; strict no-Git builds/two new installs/four new images/76 probes/12 R1 mixed cases/exact B3 passed. Independent runtime/distribution reviews and [fresh final acceptance](reviews/e05-final-revalidation-20261021.md) close R1/R2; 90 installed/image command negatives pass. | Delivered locally in `c35a6045ef39a822dcbcfcfc2c20d6b9c2227a28`; [all 270 inputs verified after commit](evidence/e05-revalidation-postcommit-20261021.json). Only runtime error normalization and five regression cases change product/test inputs; E06 remains unstarted. |

E00–E05 have original accepted deliveries; E06 is independently accepted as
recorded at the top. E07–E12 remain unstarted. Historical
green runs did not waive findings; new source, regressions and independent proof
close each repair before further acceptance.

## Update this tracker with every change

1. Before work, read current HEAD/status and the relevant epic/story. Keep
   inherited changes separate from the new task. Add a pending entry with date,
   story IDs, scope and expected evidence; do not supply a guessed commit hash.
2. Run required code tests, artifact/image/service checks and independent review.
   Preserve each finding, fix, retest and re-review. Link concrete results and
   list skips, limitations and unresolved gates. Do not equate a skip with a pass.
3. After a local implementation/fix/delivery commit, obtain its full SHA with
   `git rev-parse HEAD`. Confirm its actual files using `git show --stat <sha>`
   and `git show --name-status <sha>`. Record the date, exact hash, affected
   epic/stories, concrete behavior change, test/build results and reviewer verdict.
   Multiple stories may share a commit; multiple fix commits require separate
   rows. Keep superseded results and later corrections distinct.
4. Update the pending row in a following local tracker/evidence commit. Such
   bookkeeping must not silently change tested product inputs. Record source
   contents/environment independently of HEAD so a documentation commit does not
   falsely invalidate or falsely preserve a combined HEAD-based manifest.
5. The tracker cannot embed the hash of its own enclosing commit without changing
   that hash. Its maintenance commits are authoritatively available through
   `git log --follow --format='%H %aI %s' -- plan/library-implementation/CHANGE_TRACKER.md`.
   Do not create an endless chain of commits just to put a file's own hash inside
   itself. All implementation/delivery hashes must still be recorded explicitly.
6. Never amend history merely to disguise test failures or timing. Preserve local
   commit history and append corrections. **Never push to GitHub unless the user
   specifically asks.** Package/image publication and release tags retain their
   separate approval gates.

For the full repository history before the library checklist, use `git log`.
To inspect a particular change, use `git show <exact-commit>`; the table above
identifies the relevant local commits without relying on a remote branch.


## E04-R1 delivered locally — 2026-10-21

Clean entry `6508bd31779c49c0d1dd78f0ee242c91b9f9a9fc`; only the
[R1 repair plan](execution-e04-r1.md) was executed. Received failures propagate
with causes immediately; mixed-source default/finite-timeout and post-reply
interruption regressions pass. Fresh3,516-test/B3/artifact/image checks and
[final independent acceptance](reviews/e04-r1-final-review.md) restore S3/S4.
[Exact repair proof](evidence/e04-r1-repair.md) preserves negative controls,
transient infrastructure failure and immutable tested identities. Repair delivery: `aa03f2c0d71409a7ed46534ec57780e5bef966bf`.
[Postcommit proof](evidence/e04-r1-postcommit.json) verifies all259 tested delivery
inputs against actual Git blobs and current content/size/mode, with unchanged
environment. This following documentation/evidence commit records the actual
repair SHA; its own enclosing hash is available through Git history under rule5.
E05 is not implemented.


## E05 delivered locally — 2026-10-21

Clean entry `14fe493`; executing only [E05](execution-e05.md) after accepted
E04-R1 revalidation. Ordered console entry, finite samples and owned writable
runtime stories require independent reviews and fresh full/artifact/image/B3
proof before completion/local delivery. Actual delivery is `2265e4fd1d9f76eb2813856157dea89a80b9f216`; no E06 implementation, publication or push is authorized.

All ordered stories and the fresh final adversarial review now accept the exact
finald candidate. See [resolutions](reviews/e05-resolutions.md),
[final validation](evidence/e05-final-validation.md) and [E06 handoff](handoff-e06.md).
Actual delivery is `2265e4fd1d9f76eb2813856157dea89a80b9f216`;
[postcommit proof](evidence/e05-postcommit.json) verifies all270 accepted inputs
against committed/current content and modes. Rule5 applies to this follow-up
bookkeeping commit; no tested product input changes.


## E06 execution — 2026-10-22

Approved prebuilt strategy only: local candidates/versioned selection/exact
compatibility, per-instance immutable launches, local daemon/shared mount/service
preflight, host/container coordinator paths and narrow repeated-stop lifecycle
repair. Every actionable independent finding and original failed proof remains
in [resolutions](reviews/e06-resolutions.md). All three stories are accepted;
[Fresh final adversarial acceptance](reviews/e06-final-adversarial-review.md) is
complete on 2026-10-23; actual local delivery/equality follows the authorized commit.
