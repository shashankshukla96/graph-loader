# E06 delivered-code revalidation — 2026-10-23

Entry clean `e78970cb310a7a265ae6af5db958edc613f1d923`, implementation
`a034420f98a57d17c7591b23eb1011d7bc346d25`, E05 baseline `a2bc7bb`.
Evaluate E06 only, preserve earlier evidence, private environment settings and
remote IMDb; no E07 implementation, push or publication.

Fresh baseline full suite: one failed, 3,623 passed, four inherited skips,
49 deselections and 19 known warnings, 196.88 seconds. Failure is the unchanged
parked-index contains-after-remove control; separate independent investigation
must establish cause and a bounded repair before claiming a green gate. All 279
baseline inputs/environment remain unchanged through that run and initial fresh
isolated artifacts/two clean installs. Retained 127 raw logs, 74 candidate input
hashes, four real immutable images and prior service commands independently match.

## Required narrow database-preflight repair

Independent topology review and parent controls establish P2 E06-R1: the new
probe uses raw NEO4J_DATABASE while existing sessions strip it. Whitespace-only
and padded existing names fail preflight despite valid established behavior.

The topology reviewer independently approved this plan before coding:

1. Add probe-level tests for unset, empty, whitespace-only, padded and explicit
   database settings. Use realistic controlled drivers/mounts/metadata; require
   successful Kafka metadata completion and database selection parity. Run red
   before fixing; preserve credential/bad-database negatives.
2. Import and use shared `open_session(driver)` in the probe. Keep all other
   preflight, cleanup and redaction behavior unchanged.
3. Re-review actual source/tests independently. Freeze final inputs; run focused
   and full suites, lint/parity/pip checks, strict isolated/no-Git rebuild and two
   clean installs, new four candidate images/read-only probes, 12 supervised R1
   mixed controls and exact owned B3. Run real installed service proof, additionally
   exercising padded existing database and whitespace-default session selection.
4. Resolve every full-gate finding without hiding failed attempts or attributing
   an inherited race to E06. A fresh final independent reviewer must approve
   complete source, tests and exact proof. Record local delivery and all tested/
   committed/current input equality after commit; never push or publish.

Owned output root: ignored `var/e06-revalidation-20261023/`. Independent reports
retain separate scopes and reproductions. No global Docker cleanup or foreign
resource mutation is permitted.

## Inherited full-gate repair: acknowledged index mutation ordering

Independent deterministic diagnosis establishes a pre-existing parked-index race,
not an E06-introduced change. Lagging replay can undo a newer confirmed local
mutation; conversely a local post-flush update can overwrite a newer foreign
record already consumed. Six event-gated desired-contract regressions fail on
the unchanged baseline. The image reviewer independently approved this plan:

- Use validated successful delivery coordinates from the index topic. Preserve
  acks/flush/error checks before installing local state. Missing/invalid callback
  metadata fails closed; update positive client doubles to report real coordinates.
- Under the existing state lock, install a local mutation only if the poller has
  not consumed past it and it is not older than a pending local same-key fence.
  Replay ignores older same-key offsets while genuinely advancing consumed
  position. Matching/newer records and EOF/compaction positions retire fences.
  Never advance caught-up position from a producer acknowledgement, skip other
  keys, compare unrelated partitions, or hold the state lock during producer I/O.
- Cover add/remove/bulk, inverse stale tombstones, foreign newer records before
  flush returns, reversed local installs, unrelated-key barriers, fence pruning,
  metadata validation and partial delivery failures. Keep wire bytes/order policy.
- The diagnosing agent authors this narrow fix; a different reviewer must assess
  the actual source/tests, followed by the fresh final whole-proof reviewer.
  Preserve original full failure, all red controls and precise inherited attribution.

The first repaired candidate passes 3,674 full tests, but the fresh final reviewer
finds a new compaction gap: partition progress past a local acknowledgement does
not prove a record for that key was consumed. A compacted-away add can therefore
be dropped after only an unrelated key is consumed. Its green proof is explicitly
superseded and retained under `repaired/`; no acceptance is inferred. Amend the
design with bounded in-flight per-key mutation tracking before producer I/O,
actual same-key applied evidence and newer local installs, retaining state until
all in-flight same-key operations finish. Preserve pending echo fences and prune
metadata without a lifetime history of every tombstone. Independently review
this amendment and its regressions before freezing/rerunning final product gates.

## Amended freeze and fresh final execution

The bounded amendment was separately approved before its implementation.
Amended author checks: 123 index / 779 focused passes and clean targeted Ruff;
actual product SHA-256
`9e7e20cb91c4cbca576df9e5f6b0ae25badcc0d2763ad8ac537cf7c91a36ff07`.
A fresh zero-history auditor accepts the actual source after 159 independent
index/compaction/boundary cases and 290 focused E06 cases. Author checks and the
older rejected repair's green outputs remain distinct.

Final279-input freeze:
`2ee3cb59488a8ee9a4f3d7e93eb7773c4de3d9bff1303585db313e6758f30fd5`,
unchanged environment `96b072066210abc50c4d8cf2bf48460f11e2ebbea959f87983d47328e1d5135d`.
Fresh final run passes3713 tests,4 inherited skips,49 deselections,19 known
warnings. Strict no-Git build/rebuild65-member equality/two clean installs,
11 fresh archive negatives, four new immutable candidates/76checks/four binds,
12 supervised mixed cases, exact isolated no-skip B3 and actual installed service
proof all pass. Service proof adds a padded named-database load and whitespace-
default actual preflight; three load invocations,13nodes/12relationships and
seven actual negatives complete. All owned services removed, final Docker
inventory empty. Source/environment before/after match. See the
[completion report](reviews/e06-completion-verification-20261023.md) and
[machine receipts](evidence/e06-revalidation-20261023.json).
[Fresh final independent whole-proof/document acceptance](reviews/e06-final-audit-resume-20261023.md)
now passes with no actionable issue. Local repair delivery is `c15c43c5c4eb7c6fe6920489fd5835a91bf72377`;
[all279 tested/committed/current equality](evidence/e06-revalidation-postcommit-20261023.json) and unchanged environment
are verified. No E07 execution or publication is inferred.
