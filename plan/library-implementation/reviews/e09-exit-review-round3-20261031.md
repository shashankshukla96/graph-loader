# E09 exit-gate independent review, round 3 — 2026-10-31

Reviewer: Codex CLI session 01a19269 (read-only), same session as rounds [1](e09-exit-review-20261031.md) and [2](e09-exit-review-round2-20261031.md). Output reproduced verbatim below.

**Verdict: ACCEPT WITH CHANGES.** Both prior majors are repaired, but one minor validation gap remains.

1. **Minor — duplicate digest rows and duplicate image IDs are accepted.**  
   [service_lane.py:63](/Users/shashank/Desktop/Code/Graph/graph-loader/scripts/service_lane.py:63) converts digest rows to a dictionary before checking duplicates. Line 65 checks cardinality only for tag rows.

   Fake-client reproductions showed:
   ```text
   duplicate digest rows: candidate digest followed by [] → tolerated=True
   duplicate image IDs → tolerated=True
   ```
   The first case hides the candidate digest through dictionary overwrite. The current single-response snapshot producer does not generate this inconsistent shape, so this is a malformed-input validation gap, not a demonstrated ordinary runtime bypass.

   **Required:** check uniqueness/cardinality of image IDs and digest rows before conversion, and add a duplicate-digest control.

Otherwise verified:

- Snapshot IDs, tags and digests now derive from **one `images()` response**. Candidate detection covers tags and digests; existing tag/digest changes fail; receipts retain digests.
- **All 67 service-lane controls passed**, including the four new controls and their positive/negative branches. Ruff passed.
- `r1/final-3`: **150 selected = 150 collected**, all 450 phase outcomes passed; no skips, xfails, exclusions or errors. Duration **1,543.66 seconds**.
- Inventory SHA matches current bytes: `8c7319e7ade4a414df31fb18169f5e9cc1e4524ffab4479c6a74f6193ad704e2`.
- **47 staged files, six runner tools and 131 frozen inputs** all match the working tree.
- Foreign snapshots are byte-identical; postflight is clean and tolerance was unused.
- Inventory contains **4,199 IDs**. All 4,195 round-2 IDs and dispositions remain unchanged and ordered; exactly four checkout controls were added, with no lane changes.

Before closure, fix the duplicate-table check and refresh the byte-bound evidence after that change.