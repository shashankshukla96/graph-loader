# E05 delivered-code revalidation and path-error repair — 2026-10-21

Entry clean `0638322f3b84471f63ee867861fb75b13c97760b`, implementation
`2265e4fd1d9f76eb2813856157dea89a80b9f216`. Preserve all earlier evidence,
private environment settings and remote services. No E06 implementation or push.

Fresh full baseline: 3,566 passed, four inherited skips, 49 deselections,
19 known fork warnings, 198.10 seconds. Fresh isolated build/no-Git rebuild,
strict archives and two clean install probes passed; exact owned B3 passed.
All 270 baseline inputs/environment remain unchanged through those checks.

Independent runtime review and parent reproduction found P3 E05-R1: CPython
3.12 Path.resolve raises RuntimeError for a symlink loop. Five runtime path
settings bypass the CLI's handled path-failure boundary. Actual installed entry
forms still exit 1 before ingestion, but emit traceback; direct main raises.

Narrow repair plan, independently approved by the runtime reviewer before coding:

1. Add five real filesystem public-main regression cases for run/plan/spill/timing
   overrides and external plan paths. Require return 1, runtime diagnostic,
   no handler or Docker dispatch, and preserved caller symlink. Run red first.
2. Normalize only Path.resolve RuntimeError to ValueError in a private helper,
   retaining its cause. Use for directory resolution and external plan resolution.
   Keep the existing main boundary; do not catch arbitrary runtime bugs broadly.
3. Run focused and complete suites, lint/dependency/plan checks. Capture a new
   270-input freeze. Rebuild isolated artifacts outside Git, two clean installs,
   installed symlink-loop negatives and affected immutable images/probes. Repeat
   exact owned B3 and preserve E04/R1 source-failure cleanup controls.
4. Independently re-review actual code/tests and final proof; repair/test/re-review
   until no actionable finding remains. Retain initial failing repros/baseline,
   update tracker and status from final evidence, commit locally only after review,
   then record actual SHA and tested/committed equality. No publication or push.

All raw runs are owned under ignored `var/e05-revalidation-20261021`; build/install
environments are outside checkout. Clean only owned fixture resources.

Final-review P3 E05-R2: non-strict resolve behavior changed in Python 3.13.
The new external-plan test used a looping leaf, which may remain unresolved until
handler validation on newer Python. Use a looping ancestor (`loop/plan.json`)
instead: its sibling ledger is also unusable, so the expected pre-dispatch failure
is meaningful across versions. The final reviewer proposed/approved this narrow
fixture correction; retain its semantic emulation as an emulation, not actual
3.13 platform proof. Runtime bytes remain unchanged. Refresh the full-suite and
final input freeze; reuse independently matching artifact/image/service proof
with explicit attribution rather than rebuilding unchanged runtime payloads.
