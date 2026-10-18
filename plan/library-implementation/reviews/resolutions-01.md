# Author resolutions of round 01

Revision: round-02 submission, 2026-10-15. These are author responses, not
independent approvals. The original reviewer report is preserved. Its submitted per-epic table and
corrected opening summary list six approved and seven changes-requested verdicts.
The reviewer corrected an earlier draft count before final submission; no
outstanding arithmetic erratum remains.
All 13 epics are pending independent round-02 review, including changed earlier
approvals and downstream gates.

| Finding | Plan change | Re-review evidence / location |
| --- | --- | --- |
| R01 | Baseline identity now includes an explicit safe content manifest for tracked changes AND untracked prerequisite tests/fixtures, staged/unstaged patch identity, interpreter/dependency environment, evidence-reuse conditions and explained unit skips. Only the required B3 service case has the strict no-skip gate. | E00.F1.S1 and E00 exit gate |
| R02 | Atomic migration explicitly updates pytest/development bootstrap and every Dockerfile; documented editable checkout install is separate from wheel proof. Both clean checkout tests and actual built-image import/module help checks are required. | E03.F1.S1 / E03.F2.S2; README freshness rules |
| R03 | Existing ownership work is separated from process wait/cancellation and overall bounded cleanup. D02 must choose the proposed Event/deadline/error surface; inline blocked caller code is expressly outside universal interruption guarantees. | E01.F1.S2; E04.F2.S1/S3/S4; cancellation-contract.md |
| R04 | Added the concrete current test/fixture/reference inventory, a whole-repository classification/closure story, bounded core/safety/CLI-runtime port slices, and a declared isolated-runner interface. | test-inventory.json; E07.F1.S1–S5 and final parity gate |
| R05 | Added shared real-service bootstrap, separate normal stream/classic/DLQ/outage stories, and two bounded ordering slices. O1–O8 explicitly cover memory loss, cross-stream offsets, replicas, manual replay and topic recreation, with D02 dispositions and supported-proof versus excluded-counterexample evidence. | E09.F1.S6/S1–S5, E09.F2.S1/S2, ordering-matrix.md; service lanes in inventory |
| R06 | Final package build, final image/context build and final combined attestation are distinct ordered stories after the clean/tagged version freeze. Every build input/version/image identity is recorded, development IDs are superseded, bundled-context branch is explicit, and only the frozen manifest can be uploaded. Earlier source changes invalidate affected artifact/image evidence. | E11.F1.S2/S3/S4; E11 publication/retrieval consumers; README freshness rules |
| R07 | Recovery runbook and hypothetical broken-release dry run now belong to an explicit E10 story and production approval depends on it. Post-release maintenance fills real references into the existing procedure. | E10.F2.S2 → E12.F1.S1; E12.F2.S2 |

The plan now contains 13 epics, 26 features, 53 stories. The renderer validates
all 48 checklist mappings, inherited epic/story references, sizing, dependency
cycles and current inventory/helper paths. Markdown local-link checks and
`git diff --check` pass. This revision makes no product/source/service changes.

Round-02 in-progress review refinements: E09 service bootstrap now explicitly
uses current development-candidate image IDs, while E11 alone freezes final
release identities. E04 now defines separate process/inline clock anchors and
one operation-wide absolute deadline across all phases, with inline scan time
included and deterministic cumulative-budget tests. These changes were returned
to the independent reviewer before the round-02 verdict.
