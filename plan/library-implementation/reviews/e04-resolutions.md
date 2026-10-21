# E04 independent review and resolution index

Reviews use zero inherited conversation, inspect the actual working tree and run
independent controls. Original findings and retests remain in each linked report;
this index does not replace their evidence. Original E04 implementation/repairs were delivered together from entry
`98cd267db5c70d3517a89ec68c1a4cedf2c254fd`; the additional R1 repair is a separate
local delivery tracked below.

| Gate | Actionable finding and concrete repair | Independent disposition |
| --- | --- | --- |
| [Plan](e04-plan-review.md) | Explicitly enumerate actual codec messages, max counters/candidates and frame/scalar/depth/node/allocation boundaries, callback/subclass/dtype rejection and externally supervised controls. Execution plan expanded before implementation. | Accepted. |
| [E04.F1.S1](e04-s1-review.md) | Wrap stat/read-boundary failures with causes; add public multi-pair mapping/missing/wrong-topic rejection and endpoint-pair assertions. | Repaired, focused recheck accepted. |
| [E04.F2.S1](e04-ownership-review.md) | Prove partial startup after the first worker is owned, rather than failure before any worker exists. Added first-worker closure and owned lock/spill/marker assertions. | Repaired, independently accepted. |
| [E04.F2.S5](e04-transport-review.md) | Constructor setup failures could leak socketpair endpoints. Protected allocation/setup and tested RawArray/Process/FrameStream/start failures with captured real sockets. | Repaired, independently accepted. |
| [E04.F2.S3](e04-cancellation-review.md) | No actionable finding. Public control/error/clock and real partial-reply/later-write controls inspected and rerun. | Accepted. |
| [E04.F2.S4](e04-cleanup-review.md) | Progress unlink could abort remaining cleanup and permit topic deletion; outer finish overwrote startup retention diagnostics. Cleanup now isolates action failures and retains ownership until cleanup succeeds; outer marker append preserves diagnostics. Added progress/endpoint/spill fault cases and resistant partial-startup combinations. | Both reproduced, repaired, independently rechecked and accepted. Original report retained. |
| [E04.F2.S2](e04-import-review.md) | Guide incorrectly implied factory mappings default inline. Qualified automatic inline selection to convenience function and documented factory spawn/default and explicit inline option. | Reproduced generator pickling failure, repaired wording/contexts independently accepted. |
| [Final adversarial](e04-final-adversarial-review.md) | No actionable finding after independent runtime, codec, identity and service-proof controls. | Accepted for exact final tested E04 state. |

Author's full fresh run passes3,512 tests with four inherited skips,49 deselections
and19 known fork warnings. Per-story passing counts are historical checkpoints,
not substitutes for final source/artifact identities in
[whole-E04 evidence](../evidence/e04-final-validation.md). Initial Docker-stopped
attempts and the one incorrectly scoped test injection are preserved in raw logs;
required image/B3 retries and corrected fault checks passed. No acceptance is
inferred from an infrastructure skip or author-only review.


## E04-R1 closure — 2026-10-21

[Revalidation](e04-completion-verification-20261020.md) reopened S3/S4 when an
already received source error waited on a blocked peer and could be replaced by
timeout. `_gather` now raises that error immediately with its existing cause,
before another receive or post-reply interruption check. New supervised real
mixed-source controls and deterministic cause-priority controls cover the gap.
[Plan review](e04-r1-plan-review.md), [source/test review](e04-r1-repair-review.md)
and [fresh final review](e04-r1-final-review.md) accept the repair. Original
negative findings remain unchanged; [new proof](../evidence/e04-r1-repair.md)
records3,516 passes and fresh artifact/image/service/mixed-installed checks.
