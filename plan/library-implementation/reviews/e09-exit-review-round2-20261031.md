# E09 exit-gate independent review, round 2 — 2026-10-31

Reviewer: Codex CLI session 01a19269 (read-only), same session as [round 1](e09-exit-review-20261031.md). Output reproduced verbatim below.

**Verdict: ACCEPT WITH CHANGES.** F1–F6 are repaired. Two holes in the new additive-image policy must be fixed before E09 closes.

1. **Major — inconsistent image snapshots can hide an excluded addition.**  
   [service_lane.py:35](/Users/shashank/Desktop/Code/Graph/graph-loader/scripts/service_lane.py:35) calls `images()` separately for IDs and tags. [service_lane.py:61](/Users/shashank/Desktop/Code/Graph/graph-loader/scripts/service_lane.py:61) inspects only additions from the first list.

   Fake-client reproduction: the first call returned existing A plus benign B; the second also returned candidate-repository C. Postflight returned:
   ```text
   clean=True
   foreign_difference_tolerated=True
   foreign_images_added=[B]
   ```
   C appeared in the recorded tag difference but escaped inspection and the additions receipt.

   **Required:** derive IDs and tags from one API response, reject inconsistent identity sets, and account for every added metadata row. Add a changing-list control.

2. **Major — digest-only candidate-repository images are tolerated.**  
   [service_lane.py:62](/Users/shashank/Desktop/Code/Graph/graph-loader/scripts/service_lane.py:62) checks only `RepoTags`; snapshots omit `RepoDigests`. A fake image with empty tags and a candidate-repository digest (`ghcr.io/shashankshukla96/graph-loader-edge@sha256:…`) produced `clean=True` and `foreign_difference_tolerated=True`.

   **Required:** check candidate repositories in both tags and repository digests, retain that metadata, and add a digest-only control.

These are defects in the chosen policy, independent of D08’s accepted unfenced-substrate limitations. The ordinary removal, retagging, owner-label, inspection-error and container/network/volume controls pass.

The round-1 repairs verify as follows:

| Finding | Status | Evidence |
|---|---|---|
| F1 | Fixed | [Bulk failure cases:25](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/test_service_kill_bulk_modes.py:25): classic loader, lease loader and lease-server kills all exit 1; replay exits 0 with exact graph and committed offsets. Retained partial graphs contain 200, 854 and 421 relationships. |
| F2 | Fixed | [Bulk ordering cases:251](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/test_service_last_value_order.py:251): newest values are stored before older records are published; all three modes preserve them, with offset/ledger checks. Timestamp ordering remains explicitly unexecuted at [F2 evidence:65](/Users/shashank/Desktop/Code/Graph/graph-loader/plan/library-implementation/evidence/E09.F2.S1.md:65). |
| F3 | Fixed | [Unique-row inspection:72](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/inspect_strict.py:72) rejects duplicate pairs before dictionary projection. Both inspector controls pass. S1–S3 retain whole-list comparisons. |
| F4 | Fixed | Metadata/offset errors propagate; absence assertions require an existing topic. [DLQ reader:132](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/dlq_fixture.py:132) rejects incomplete watermark coverage. Fake inspection controls pass. |
| F5 | Fixed | [Bulk assertion:300](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/test_service_load_bulk.py:300) now claims counter consistency only. The cited [dispatcher proof:204](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/test_planned_dispatcher.py:204) exists and passes locally and in the retained 540-case installed lane. |
| F6 | Fixed | [Outage cases:180](/Users/shashank/Desktop/Code/Graph/graph-loader/tests/installed_service/test_service_kill_and_outage.py:180) preserve the original ID and add two executions of the same body. All three pass in `r1/final-2`. |

I verified `r1/final-2` against current files:

- **150 selected = 150 collected**, all 450 phase outcomes passed; no skips, xfails, exclusions or errors. Duration **1,546.84 seconds**.
- Inventory SHA matches: `11a1dd505b171e479928fbbd480029d0d4bb9f6d8c8f713e7ce18a7b909618a3`.
- **47/47 staged files, 6/6 runner tools and 131/131 frozen inputs** match.
- Foreign snapshots are byte-identical; postflight is clean, with tolerance unused.
- All **4,165 pre-round IDs** and dispositions remain unchanged and ordered. Additions are exactly **16 service IDs and 14 checkout controls**; only `service-harness` changes. HEAD itself contains 4,132 IDs, also preserved.
- `final-1` is retained red: all 150 tests passed, but postflight rejected the single added image.

Local focused verification: **72 passed; ruff clean**. No live-host verification was performed.

Before closure, repair the two image-policy findings, freeze the added controls and retain a complete passing run bound to the resulting runner bytes.