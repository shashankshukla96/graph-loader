# E04-R1 repair execution — 2026-10-20

Entry clean HEAD `6508bd3` on `codex/pypi-library-checklist`. Current
[revalidation](reviews/e04-completion-verification-20261020.md) supersedes E04
completion for one P1: an already observed source failure waits behind a blocked
peer and can be replaced by timeout. E04 S3/S4 stay reopened and E05 blocked until
repair/testing/independent acceptance. Preserve historical reports and all prior
source/evidence. No E05, publication, push, tag, workflow, remote IMDb access,
`.env` sourcing/disclosure or foreign-resource cleanup is authorized.

## Narrow repair and acceptance

1. Reproduce both default/no-timeout and finite-timeout outcomes using the retained
   independently bounded public source-error reproduction, a new owned output and
   fresh installed CPython3.12 environment. Capture safe entry source/environment.
2. Once `_gather` receives a non-ok worker response, immediately raise its ordinary
   error with the existing inline-original or remote-diagnostic cause. Do not wait
   for more replies, and do not replace this received error with a following
   cancellation/deadline check. Successful replies retain all existing checks,
   result order and phase behavior. This is not a receive-multiplexing redesign:
   a failure behind an earlier blocked lane is not observed until that lane returns.
3. Add real spawned mixed-source public fault cases with no timeout and finite
   timeout, independently supervised with emergency cleanup. Coordinate the
   failing source with a readiness file so the peer is demonstrably blocked before
   failure. Observation-only receive wrapping records the error without changing
   protocol behavior. Assert ordinary error/cause, prompt cleanup entry, total
   observation+aggregate10s cleanup with scheduling tolerance, all child/endpoint
   death, no heartbeat/helper, source-bearing marker and released owned lock/spill.
   Keep a caller-owned sentinel/signal unchanged. Add deterministic public controls
   for cancellation/deadline becoming true immediately after the error is returned.
   Preserve existing ownership and all partial-frame/backpressure/cancel controls.
4. Run focused public API/lifecycle/transport/CLI regressions, Ruff including tools,
   dependency/parity/whitespace checks, then a fresh complete nonintegration suite.
   Four inherited skips,49 deselections and19 known fork warnings retain ownership;
   no new failure/skip/warning may be hidden or claimed as a pass.
5. Rebuild isolated sdist/wheel and no-Git extracted-sdist wheel, strict Twine,
   exact inventory/member equality and two new noneditable installed environments.
   Rebuild all four affected component images and probe immutable IDs without
   mounts/network rescue:52 module bytes/origins, public/import examples,8 component
   helps/default help/pip check. Re-run exact B3 through owned supervisor with the
   explicit fresh interpreter; require no-skip pass. No remote IMDb substitute.
6. Keep plan review, post-repair independent zero-history source/test review and
   fresh final adversarial evidence review separate. Reproduce/repair/retest/review
   every actionable finding until closed. Record exact commands/exits/identities
   and safe before/after manifests. Restore E04 S3/S4 completion and E05 handoff gate
   only after actual acceptance; render canonical plan, validate links/whitespace.
   Inspect staging, commit repair locally after tests/review, then record actual
   full repair SHA and per-file content equality in a documentation-only follow-up.
   Do not recursively embed that follow-up's own hash.

Owned raw outputs/environments/scripts: ignored `var/e04-r1-20261020`. Existing
supervisor owns only its client process group and testcontainers own scratch
services. Never globally prune Docker or remove foreign locks/topics/files.
