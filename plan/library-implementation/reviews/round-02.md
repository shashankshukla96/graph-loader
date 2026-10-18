# Independent implementation-plan review — round 02

Date: 2026-10-15. Reviewer task identity: `/root/review_library_epics`, independently delegated by `/root`. This is the reviewer's original re-review of the round-02 submission, not an author approval or implementation report.

**Overall verdict: CHANGES REQUESTED. Twelve epics APPROVED; E04 CHANGES REQUESTED for R08 below.** The seven round-01 findings have substantive resolutions. One precise timeout-clock inconsistency remains in the new cancellation contract. Resolving it requires a small plan correction, not product implementation.

Approval concerns executable decomposition only. It does not resolve D01–D07, mark stories implemented, establish release readiness, or authorize services, account changes, uploads, pushes, yanks or user-data operations.

## Reviewed material and validation

Re-read all current plan prose and every epic, including previously approved and downstream epics:

- `plan/library-implementation/README.md`
- `plan/library-implementation/decisions.md`
- `plan/library-implementation/backlog.json` (entire structure parsed; every change/acceptance/validation string compared with generated pages)
- `plan/library-implementation/epics/e00.md`, `e01.md`, `e02.md`, `e03.md`, `e04.md`, `e05.md`, `e06.md`, `e07.md`, `e08.md`, `e09.md`, `e10.md`, `e11.md`, `e12.md`
- `plan/library-implementation/cancellation-contract.md`
- `plan/library-implementation/ordering-matrix.md`
- `plan/library-implementation/test-inventory.json`
- `plan/library-implementation/traceability.md`
- `plan/library-implementation/tools/render_plan.py`
- `plan/library-implementation/reviews/README.md`
- `plan/library-implementation/reviews/resolutions-01.md`
- The existing `plan/library-implementation/reviews/round-01.md` header and findings were used to verify resolutions. Its contents were preserved.

The source checklist, original checklist review and code/test surfaces named in round 01 remain the unchanged feasibility reference. In particular, R08 is grounded in the previously inspected `_InlineWorker.send()`, `_run_command()`/scan and public planner start/finish paths in `src/loader/edge_planner.py`; inline submission executes the scan synchronously rather than merely queuing it.

Read-only validation independently confirmed **13 epics, 26 features, 53 stories, all 48 checklist IDs covered**, valid numeric 0.5–2-day bounds, matching canonical/rendered story prose, existing current/reference/helper inventory paths, valid inventory owner IDs and an acyclic combined dependency graph including inherited epic prerequisites. No product tests or service probes were run. The renderer was inspected rather than executed because it writes generated pages. Only this new reviewer report was written by this review task.

Submission identifiers (SHA-256):

| File | Hash |
| --- | --- |
| `backlog.json` | `983f91cbbdac147caae71e761ea10abab687a63c9796c5d9eec4496769f06d09` |
| `cancellation-contract.md` | `098c4a7b507007669b362f2bb1698b17d5ee755098ab38aa9d3d27886e054811` |
| `ordering-matrix.md` | `b02d4e87f8ee13ac03fec2a256ad0e3fae3902f19523eaa9ded9a3747c532058` |
| `test-inventory.json` | `4023247d18a3464daad1e3d6fca80c928947cc9829b6aa8d1a9ef96776b527bc` |

Arithmetic note requested by the author: an initial round-01 draft used five/eight, but the submitted file currently has **six approved/seven changes requested** in both its opening and table, matching the reviewer's final message. No further change to round 01 is needed. `resolutions-01.md`'s description of a still-incorrect opening is stale; the correct count is six/seven.

## Verification of R01–R07

| Finding | Resolution assessment |
| --- | --- |
| R01 — Exact prerequisite baseline | **Resolved.** E00 now names staged/unstaged identity, a safe path/size/hash content manifest including untracked prerequisite tests/fixtures, dependency/interpreter identity, equivalence conditions for historical evidence and explained unit skips. The B3 case separately requires an actual non-skipped pass. |
| R02 — Source-layout bootstrap | **Resolved.** E03 explicitly covers pytest/development setup, an editable checkout install, every Dockerfile, SCM versions without `.git`, clean installed-wheel proof and actual module/import/help execution inside built images. Editable development evidence is explicitly separate from artifact evidence. |
| R03 — Bounded cancellation decomposition | **Substantially resolved.** E04 separates inherited lifecycle ownership, bounded process waits, bounded cleanup and process-wide effects into concrete stories. D02 must approve Event/deadline/errors; blocked inline code is explicitly outside interruption guarantees. R08 corrects one remaining ambiguity in the newly proposed clock definition. |
| R04 — Bounded installed regression selection | **Resolved.** The inventory names concrete core/safety/CLI-runtime test files, helper/config closure and separate service lanes. An explicit inventory/classification story precedes the bounded porting lanes, preserves the checkout suite and requires re-review for overflow rather than silently dropping assertions. The common runner and sdist parity gate remain coherent. |
| R05 — Service/ordering slices | **Resolved.** A shared actual-service adapter is followed by distinct bulk, stream, classic, DLQ and outage/supervision stories; the exact maintained arrival case remains required. O1–O8 now separately assign memory loss, cross-topic/partition identity, replicas, replay and recreation. Supported claims require live proofs; approved exclusions have explicit counterexamples/preconditions and cannot masquerade as safety passes. |
| R06 — Final image freshness | **Resolved.** README freshness rules invalidate affected package/image evidence after source changes. E11 explicitly orders clean/tagged package freeze/build → final image/context build → combined final attestation → separately authorized publication. Source-copy equivalence, versions/dependencies and bundled-context identity are checked; E12 consumes the frozen manifest rather than development images. |
| R07 — Recovery before production authorization | **Resolved.** E10.F2.S2 owns the pre-production runbook and hypothetical broken-release exercise. E12.F1.S1 explicitly depends on and links to it. Post-release work only finalizes actual references and grants no standing destructive authority. |

## Explicit per-epic verdicts

| Epic | Verdict | Basis and practical limit |
| --- | --- | --- |
| E00 | **APPROVED** | Content/environment identity and the exact B3 no-skip gate now establish an executable baseline story without overwriting current work or reusing the IMDb graph as a fixture. |
| E01 | **APPROVED** | D01–D07 and cancellation/ordering choices remain explicit user gates. The proposed contract can be corrected before D02 execution; plan approval chooses none of these product promises. |
| E02 | **APPROVED** | The development-only scaffold, authoritative dependencies and sdist-safe version foundation remain bounded and correctly precede the complete migration. |
| E03 | **APPROVED** | Atomic migration now explicitly preserves checkout and built-image bootstrapping, with installed provenance checked separately. It may remain one mechanical integration story. |
| E04 | **CHANGES REQUESTED** | R08: define an effective inline timeout start and one non-resetting deadline across phases before implementing the newly explicit API. |
| E05 | **APPROVED** | Installed entry point, resources and writable state remain concrete small changes; the normal dependency on completed E04 remains mandatory. |
| E06 | **APPROVED** | Checkout-free local image/context delivery and topology validation remain coherent, with release-image freshness explicitly handled later. |
| E07 | **APPROVED** | Concrete test inventory, recursive closure/classification, bounded porting lanes and parent/child provenance prevent the earlier undefined regression bucket. |
| E08 | **APPROVED** | Paired min/current dependency and start-method proofs remain tied to the approved matrix; the new cleanup-budget evidence is explicitly inherited from E04 and cannot be inferred from unit mocks alone. |
| E09 | **APPROVED** | The shared real-service adapter and separate mode/failure/ordering lanes provide executable owners and prevent unit/in-process proofs being mistaken for installed-image service evidence. See the nonblocking wording clarification below. |
| E10 | **APPROVED** | Public examples and scope statements remain tied to verified artifacts, and recovery is now an explicit completed pre-production deliverable. |
| E11 | **APPROVED** | Final package/image/context immutability and ordered attestation now close the stale-image gap while retaining separate TestPyPI/registry authorization. |
| E12 | **APPROVED** | Production approval depends on the rehearsed candidate and already exercised recovery procedure; exact bytes/references, clean retrieval and separate destructive-action authorization remain required. |

These verdicts evaluate each epic's decomposition. E04's unapproved plan detail still blocks dependent implementation acceptance through the existing prerequisite chain. Approving E05–E12 does not waive that prerequisite.

## R08 — Define timeout start consistently for inline and process execution

Priority: P2. Affected plan surfaces: `cancellation-contract.md` and E04.F2.S3; E04.F2.S4 consumes the resulting timeout/cancellation outcome.

The new contract says the elapsed deadline begins when **“parent task submission is complete and worker result processing starts”**, while also requiring inline cancellation/deadline checks between records and phases. Existing `_InlineWorker.send()` executes `_run_command()` synchronously, including scanning caller records. For inline execution, initial task submission therefore does not finish until the scan finishes. A deadline created only after that point cannot expire during a slow but cooperative inline scan, contradicting the intended between-record timeout behavior. The contract also should explicitly say that the deadline is one absolute monotonic deadline across phases rather than being reset at every gather/phase.

**Required fix:** define the clock anchors for both execution modes. The process-mode deadline can begin at the explicitly chosen parent wait/submission boundary, with serialization/client-operation exclusions retained. The inline deadline must begin before the first source iteration whose cooperative duration is meant to count. State whether time between a separately called `start()` and `finish()` counts, or explicitly define this as a finish/wait budget if that is the chosen contract. Preserve a single absolute deadline across all covered phases. D02 must approve the chosen semantics; no universal interruption promise for a blocked `next()` is required.

**Approval criterion:** the contract and E04 story identify where the deadline is created and passed, what time it covers, and how it is checked without resets. Add a deterministic planned validation with an inline source yielding multiple individually returning slow records: timeout occurs during the covered scan rather than after all input is consumed. Retain the real blocked-child and cooperative cancellation cases and the distinct cleanup budget.

This is a precise contract correction within the already bounded cancellation story. It does not ask for another API feature or an expanded implementation epic.

## Nonblocking consistency clarification

E09.F1.S6 acceptance item 2 says **“final candidate image IDs”**, although E09 precedes E11's final release-image freeze. The README and E11 stories correctly and explicitly distinguish development images from release identities, so there is no actual required backward dependency in the reviewed hierarchy. Change that E09 wording to “current identified development-candidate image IDs,” with a note that E11 repeats the gate on frozen release identities, to avoid accidental execution-time confusion. This is a wording clarification, not a separate release blocker.

Preserve both original review reports, record R08's resolution, regenerate/validate any changed canonical pages and return the affected contract/epic for independent confirmation. No implementation or external action is authorized by this report.
