# Library implementation change tracker

Track the library/PyPI program from checklist preparation through E12. Dates
below use America/New_York and describe the actual commit or validation date.
This is a history index, not a replacement for Git diffs or acceptance evidence.
No entry authorizes a GitHub push, image/package upload or publication.

## Committed changes

| Date | Epic / change | Exact local commit | What changed | Verification / evidence |
| --- | --- | --- | --- | --- |
| 2026-10-11 | Checklist foundation | `d53a92cfef652b7aae5aca6472736c10e851db42` | Added the library/PyPI checklist and independent checklist validation. No distribution implementation was claimed. | [Checklist review](../../docs/PYPI_CHECKLIST_REVIEW.md), [checklist](../../docs/PYPI_LIBRARY_CHECKLIST.md). |
| 2026-10-17 | E03 handoff preparation | `6de3562083cc7079091d056b89d66f18b69931e8` | Added the migration handoff and independent handoff review, including full SDLC, test/build gates and repeat-until-resolved independent review. Earlier implementation remained uncommitted then. | [Handoff review](reviews/handoff-e03-review-20261017.md). |
| 2026-10-17 | E00 prerequisites | `bf5471e9243dbda90cdf066a5d811486119dba7c` | First committed the inherited planner ownership/recovery fixes, schema/logging fixes, service-independent test repairs and maintained DLQ fixture/harness. Preserved them during the E03 migration. | [E00 baseline](evidence/E00.F1.S1.md), [DLQ proof](evidence/E00.F2.S1.md). |
| 2026-10-17 | E01 release decisions | `bf5471e9243dbda90cdf066a5d811486119dba7c` | First committed the approved D01–D07 identities, API/compatibility/support targets, mandatory dependencies, image ownership/delivery and manual-publication decisions, along with plans and reviews. | [Decision register](decisions.md), [direct approval record](evidence/e01-user-decisions-20261016.md). |
| 2026-10-17 | E02 distribution foundation | `bf5471e9243dbda90cdf066a5d811486119dba7c` | First committed the version/metadata scaffold, authoritative runtime dependency generation, development build inputs, metrics version authority and associated tests/evidence. | [E02 metadata](evidence/E02.F1.S1.md), [dependencies](evidence/E02.F1.S2.md), [version](evidence/E02.F2.S1.md). |
| 2026-10-17 | E03 atomic namespace migration | `bf5471e9243dbda90cdf066a5d811486119dba7c` | Moved all runtime modules to `neo4j_graph_loader`; migrated imports, patch/subprocess/pickle targets, examples/docs and four Docker entry paths; adopted editable development installs and complete restricted package discovery; added namespace tests and artifact verification. No generic `src` shim. | [E03 completion](evidence/e03-completion.md), [independent final review](reviews/e03-final-adversarial.md), [resolution history](reviews/e03-resolutions.md). Full suite 3,387 passed; exact B3 passed; artifacts/images verified. |
| 2026-10-17 | E03 postcommit evidence | `ffb862537b83ef7be053fb96e05d12fcbcfdd355` | Recorded content/size/mode equality of the 245 existing tested source inputs after the implementation commit. Documentation/evidence only; no new runtime behavior. | [Postcommit comparison](evidence/e03-postcommit.json). |
| 2026-10-18 | E03 verification / E04 handoff | `06ae36c05d724c47f99fa0d4d98dbf1f76aa9902` | Added fresh E03 verification and independent review, refreshed the executable E04 handoff, introduced this tracker and linked current status. Documentation/evidence only; no E04 implementation or runtime change. | [Current verification](reviews/e03-completion-verification-20261018.md), [independent source acceptance](reviews/e03-revalidation-independent-20261018.md), [handoff/tracker acceptance](reviews/e04-handoff-tracker-review-20261018.md). |

**Commit boundaries matter:** E00, E01 and E02 were completed in a dirty checkout
and first committed together with E03 in `bf5471e`. The repeated hash above is
intentional. There are no separate historical implementation commits for those
epics; do not invent them. The entry/mechanical manifests distinguish inherited
prerequisites from the migration itself. Historical validation dates can precede
the date those contents were committed.

## Current validation and delivery

| Date | Reviewed source commit | Work | Delivery commit / state |
| --- | --- | --- | --- |
| 2026-10-18 | `ffb862537b83ef7be053fb96e05d12fcbcfdd355` | [Fresh E03 completion verification](reviews/e03-completion-verification-20261018.md): 3,387 test passes, exact no-skip DLQ proof, rebuilt artifacts/two clean installs and four new-image matrices. [Independent source review](reviews/e03-revalidation-independent-20261018.md) accepts E03; [handoff/tracker review](reviews/e04-handoff-tracker-review-20261018.md) approved after H1 was fixed and rechecked. Added this tracker and refreshed [E04 handoff](handoff-e04.md); no product/E04 implementation. | Delivered locally in `06ae36c05d724c47f99fa0d4d98dbf1f76aa9902`; source reviewed at `ffb8625` is unchanged. This tracker-only follow-up records the actual new hash. |

E00–E03 are recorded complete in the canonical plan. E04–E12 remain unstarted;
plan approval is not implementation. A new failed gate must be recorded rather
than hidden by an older pass. This verification does not itself start E04.

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
