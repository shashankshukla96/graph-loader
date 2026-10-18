# Fresh independent implementation-plan audit 02

Date: 2026-10-16. Reviewer: the independent agent that wrote fresh-audit-01.
Reviewed HEAD: `d53a92cfef652b7aae5aca6472736c10e851db42` plus the existing
working-tree changes. This is a plan re-review, not implementation verification.

**Overall verdict: APPROVED. All E00–E12 epics are approved as implementation
plans.** The proposed transport design addresses FA01's technical concern.
During this re-review I identified a semantic dependency in the first S5 test
allocation (FA04); the author corrected it before this report was finalized,
and I re-read and validated the regenerated canonical records. No required
plan correction remains. Implementation and release approval remain separate.

I read the author response in `fresh-resolutions-01.md`, then checked the
canonical stories, contract, current code and dependencies myself. The response
was not treated as approval. Earlier round reports/resolutions were not used as
authority; fresh-audit-01 and all other reports are preserved.

## Final per-epic verdicts for this reviewed revision

| Epic | Verdict | Reason and downstream consequence |
| --- | --- | --- |
| E00 | **APPROVED** | Content-identified historical baseline plus the exact no-skip B3 integration gate remain appropriate. No service evidence is inferred from this review. |
| E01 | **APPROVED** | D02 now explicitly approves complete-frame transport and codec boundaries before E04. Existing scope/identity/support/image/publisher decisions and expanded-scope replanning remain in force. |
| E02 | **APPROVED** | Development-only scaffold, metadata/dependency authority and single sdist-safe version source remain coherent. |
| E03 | **APPROVED** | The cohesive namespace migration still includes executable imports, test targets, examples, source-layout bootstrap and working image entry points; no partial installed-runtime claim was introduced. |
| E04 | **APPROVED** | Nonblocking incremental framing, bounded sends, limits, endpoint ownership and cleanup separation solve the technical design gap. The corrected S5-private / S3-public-integration / S4-final-cleanup test allocation resolves FA04 and makes each stage independently completable. |
| E05 | **APPROVED** | Installed CLI/resources and writable-path tests remain concrete. Its prerequisite E04 must be implemented and verified before E05 acceptance. |
| E06 | **APPROVED** | Checkout-free image selection, overrides, mounts, service addresses and tiny-load validation remain downstream of E05. Publication is still separately gated. |
| E07 | **APPROVED** | FA03 is adopted: the inventory freeze records recursive closure, parametrized IDs and revised effort before any port, with overflow split/re-reviewed first. Isolation, negative controls and independent sdist-built-wheel validation remain intact. |
| E08 | **APPROVED** | FA02 is adopted with genuinely minimal base-only environments and standalone absence probes. The supported-platform/start-method gate now includes framed transport, partial transfer/backpressure and cleanup proofs. It cannot close before E04's completed contract and E07's installed runner exist. |
| E09 | **APPROVED** | Real owned services, supported-mode/failure/offset checks, positive-control lock-wait detection and O1–O8 ordering dispositions remain explicit; the E08 prerequisite prevents unsupported transport/platform assumptions from bypassing this gate. |
| E10 | **APPROVED** | Installed examples, precise guarantees, migration/support material and pre-production recovery rehearsal remain evidence-dependent. |
| E11 | **APPROVED** | Frozen source/version, fresh exact artifacts/images and combined final E07/E08/E09 attestation remain required before specifically authorized external rehearsal. The new transport proofs are inherited by that attestation. |
| E12 | **APPROVED** | Concrete production approval, exact rehearsed bytes, compatible images and fresh public-install/load verification remain appropriately ordered after E11. |

These are verdicts on each epic's decomposition. They do not waive failed
prerequisites, mark stories implemented or authorize publication. Clerical
mirroring of these verdicts into review-status fields, summary status and
report links is permitted without a further substantive review. Implementation
statuses must remain not started. Substantive changes after this reviewed
revision require review of the affected epics again.

## Prior findings rechecked

**FA01 — technical design concern addressed in the proposed plan.** S5 and the
revised contract require a parent-owned nonblocking socket stream adapter with
incremental length/header/body handling, deadline/cancellation-aware selector
waits and bounded writes. Readiness cannot be followed by blocking `recv`,
`send` or `sendall`. Later broadcasts and stop delivery are covered. Endpoint
ownership is explicit, and there is no receiver helper to strand. Oversize,
corrupt, truncated and EOF frames have explicit limits/errors; codec computation
has before/after deadline checks and maximum-frame support evidence rather
than a hidden universal CPU-preemption promise. Initial submission/source
pickling retains its pre-existing explicit exclusion. D02 approves that surface;
D04/E08 must prove every advertised transport/platform or restrict support.

The required large partial-frame and later-send-backpressure negative controls
now cover the important missing boundaries. External supervision prevents a
regression from hanging the test runner. Blocked-source, phase-clock and inline
clock tests remain distinct. S4 explicitly includes partial replies and full
stop-send buffers in its separate shared cleanup budget. This is a technically
reasonable bounded design to implement; this review does not prove its future
implementation correct. The corrected allocation described under FA04 below
makes the tests executable in the stated story order.

**FA02 — adopted and closed as a plan-wording improvement.** E08.F1.S1 now
requires a fresh environment without system-site packages, inherited PYTHONPATH,
development/testcontainers dependencies, shared conftest or optional plugins.
It inventories the environment and checks D05-optional packages are absent
before standalone public probes. Dependencies retained as mandatory by D05 are
not incorrectly required to disappear. This directly avoids the current shared
conftest's eager Docker/testcontainers imports masking an absent extra.

**FA03 — adopted and closed as a plan-wording improvement.** E07.F1.S3 now owns
staged file counts, recursive fixture/config/helper closure, all collected IDs
including parametrization, and an effort reassessment before porting. The
initial inventory includes the additional helper files and social/IMDb schemas.
The AST counts are correctly labeled discovery inputs, not pass evidence, and
overflow must be split and reviewed before execution. Full closure is still a
future deliverable rather than falsely claimed complete now.

## Sequencing finding resolved before finalization

### FA04 — P2 / medium, resolved: predecessor tests required successor behavior

**Affected stories/files:** E04.F2.S5, E04.F2.S3, E04.F2.S4 in `backlog.json`,
generated `epics/e04.md` and test ownership in `cancellation-contract.md`.
E08.F2.S1 and E11.F1.S4 inherit the final combined proof.

The first revision submitted for this re-review declared S1 → S5 → S3 → S4,
but required S5 to pass public cancellation/timeout and final cleanup assertions
before S3/S4 implemented that behavior. Structural graph validation could not
detect this semantic dependency. I reported it to the author while the report
was still a draft. The author revised the canonical backlog and contract;
I then re-read the exact regenerated story bodies rather than treating the
response as evidence of resolution.

The final allocation is now explicit in both places:

- **S5:** private adapter framing/limits/ownership, fake-clock readiness controls
  and real-process partial-frame/backpressure fixtures. It exercises private
  signal/deadline exits and prompt adapter interruption. An external supervisor
  owns the independent hard deadline and fixture child/endpoint cleanup. It
  explicitly leaves public errors and full-operation cleanup to S3/S4.
- **S3:** approved public signatures/errors and operation-wide signal/deadline
  integration, tested with partial-large-reply stalls and later-command send
  backpressure under both timeout and cancellation. It asserts public errors
  and prompt coordinator observation using the existing abort/terminate path;
  final total timing and complete cleanup assertions belong to S4. The distinct
  blocked-source, multi-phase, inline-clock and start/finish-gap tests remain.
- **S4:** final public error/diagnostic, total poll-plus-cleanup elapsed budget,
  worker/endpoint liveness, no helpers, failure markers and retained/reclaimed
  owned resources across those public cases. Stop-send backpressure, ordinary
  source failure, partial startup and the unreapable-worker fake remain covered.
  Independent emergency supervisors remain in place throughout.

This supplies independently satisfiable predecessor checks without dropping
the final public contract. Private controls cannot substitute for S3/S4 proof;
E04 closes only after all three stages pass. E08 repeats supported transport
and platform behavior and E11 binds final artifacts to the combined gates.
**FA04 is closed as a planning issue.** No remaining required or optional
plan correction is proposed by this re-review.

## Checks performed and limits

- Re-read the revised author response, canonical E01/E04/E07/E08 stories,
  cancellation contract, README, decisions and complete test inventory; checked
  the other epics' current gates/dependencies and relevant acceptance/validation
  requirements against the existing checklist and the first audit's code review.
- Re-inspected the actual worker construction/send/receive/stop/close loop,
  worker entry, parent gather/broadcast and producer-planner/convenience entry
  points. They remain the pre-implementation baseline: the new transport and
  public cancellation behavior are future work. Rechecked the development
  requirements and shared conftest imports relevant to FA02.
- Ran the existing structural renderer with `Path.write_text` intercepted in
  memory: **13 epics, 26 features, 54 stories, 48 checklist items, acyclic declared
  dependency graphs**. All 14 generated bodies matched current files; nothing
  was regenerated on disk. Story estimates remain within 0.5–2 days and total
  48.5 focused engineering days. Existence checks for listed current test/helper
  resources passed. Semantic test dependencies were checked manually as well
  as validating the declared graph, including after the FA04 correction.
- `git diff --check` passed. Repository `AGENTS.md` inventory remained empty;
  the ancestor instruction search from the original audit found none applicable.
- No product tests, real-process fault probes, services, artifact builds,
  deployments, accounts, uploads, pushes or external connections were run. This
  audit verifies plan sufficiency and sequencing, not the proposed design's
  actual runtime, platform, resource-limit or performance behavior.
- Only this new report was written. No earlier report, product code or other
  plan content was changed by the reviewer. No blanket approval is inferred
  from author claims or from the structurally valid dependency graph.

Final substantive reviewed-file SHA-256 identities, before permitted clerical
verdict/status mirroring:

| File | SHA-256 |
| --- | --- |
| `backlog.json` | `7304a85c9bb1fdc9b2168ed63bbfb18bfab89197d8a06fce750a029c6a2ed9c9` |
| `cancellation-contract.md` | `8c7fd624d8f2f9287efc164f2d881be3563cc7dffe52cefe0b9ce41187de5692` |
| Preserved `reviews/fresh-audit-01.md` | `2af16b76e56d22fdd075634a8bb3aa1d5d2138ce9f3ff14ec9be44fb9307296c` |
