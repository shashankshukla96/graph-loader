# E05 findings and resolutions — 2026-10-21

Every review used a newly initialized internal reviewer, with no inherited author
history. [Plan](e05-plan-review.md) and [S1](e05-s1-review.md) accepted without
findings. Three actionable findings were preserved, repaired and independently
re-reviewed:

| Finding | Original observation | Repair and independent closure |
| --- | --- | --- |
| S2 read errors | schema-example traceback for missing or invalidUTF8 resource | Read completely inside OSError/UnicodeError guard, log/exit1 before stdout. Actual installed original reproduction, repaired wheels, real ZIP lifetime success/failure and corruption/omission controls accepted in [S2](e05-s2-review.md). |
| S3 hidden outputs | hidden ledger/progress temp directory fails late; symlink could truncate sentinel | Preflight final and deterministic hidden siblings for external/automatic plan, reject special files/symlinks, probe readonly without truncation. Nine negative cases preserve sentinel/link/files; [S3](e05-s3-review.md) accepted. |
| S3 early flags | planner ranges/bulk-only flags created state and metrics before rejection; first partial repair still missed stream edge-leases | Shared schema-independent planner/lease numeric validators and early mode gates; schema-selected default bulk lease checks occur before state. Eight public-main controls forbid schema/state/metrics, plus original independent cases; [S3](e05-s3-review.md) closed finding. |

All original reports/reproductions and intermediate failures remain. Author
initial full suite3547pass2fail came from the intentionally changed internal
None/default sentinel assertion and a constructor-bypassing test fixture. Tests
now verify actual no-context legacy directory and explicit priority and construct
a real service with its controlled client. Final3566passes restore all existing
coverage. A metrics-disabled author test was explicitly enabled for its snapshot
assertion; negative-keyed-max test was corrected for existing argparse exit2.
No source checks, skips, warning expectations or required proof were weakened.

The first final-review turn was interrupted by an account usage limit before any
verdict. It was recorded as incomplete, never acceptance. After the user's
continue instruction the same independently initialized reviewer resumed against
the unchanged frozen candidate. Its eventual report is authoritative for final
acceptance, separate from the per-story reports above.

Final adversarial review also found two delivery issues. The original extracted
sdist lived below the checkout, so its missing local .git did not establish
failed Git discovery. New author and independent rebuilds use paths outside all
checkouts, explicitly require discovery exit128, and compare all63 member bytes.
The original build attempt and claim are retained and qualified in both story
checkpoints. The other issue was stale current-path/default/retention guidance,
including two parser help strings; targeted guide/help corrections were reviewed
before the finald270-input freeze. Runtime logic did not change. Finald repeats
the complete proof on the corrected bytes; superseded attempts remain available.
