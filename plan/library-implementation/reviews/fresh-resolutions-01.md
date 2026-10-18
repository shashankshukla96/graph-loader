# Author response to fresh independent audit 01

Date: 2026-10-16. This response is not independent approval. Original audits and
prior reviews are preserved; FA01 supersedes the earlier E04 approval until the
new reviewer accepts the correction.

- **FA01 (required):** Added E04.F2.S5 as a separate 1.5-day private framed
  transport story. The parent uses an owned nonblocking socket stream adapter
  with incremental header/body reads and bounded writes/selector waits. It
  cannot equate readiness with full-frame completion or block on recv/sendall.
  There is no receiver helper to strand; endpoint and worker ownership, frame
  limits, later parent sends, stop backpressure and codec boundaries are explicit.
  E04.F2.S3 depends on this transport before integrating public deadlines/errors;
  S4 includes partial-reply/full-send-buffer cleanup in its separate budget.
  Required tests add a supervised real-process partial-large-frame stall through
  public timeout and cancellation, plus a worker refusing later command/stop
  delivery. Assertions cover time, errors, liveness, helpers, markers and
  retained/reclaimed owned resources. The test supervisor has its own hard
  deadline/cleanup. Existing blocked-source/phase/inline tests remain.
- **FA04 (re-review sequencing correction):** S5 gates private adapter behavior
  and real-process transport fixtures with independently supervised fixture
  cleanup. S3 owns public timeout/cancellation partial-frame and later-command
  backpressure integration and prompt signal/deadline observation. S4 owns the
  final total elapsed-budget, worker/endpoint liveness, helper, marker and owned
  resource assertions across all public controls. This removes the acceptance
  dependency on successor implementation without dropping any negative control.
- **FA02 (optional, adopted):** E08.F1.S1 now explicitly provisions a minimal
  base-only virtualenv without system-site packages, inherited PYTHONPATH,
  developer/testcontainers/shared-conftest dependencies or optional plugins.
  Standalone public probes first assert selected optional packages are absent;
  test-rich regression environments remain separate. E08.F2.S1 now also tests
  the framed transport on each advertised platform.
- **FA03 (optional, adopted):** E07.F1.S3 must record staged files/closure,
  collected test IDs and revised lane effort before porting. Overflow must be
  split/re-reviewed first. The inventory now includes the additionally identified
  helper files/social/IMDb schemas and records the current AST counts explicitly
  as discovery facts, never test pass evidence.

The plan remains 13 epics / 26 features and now has 54 stories. All 48 checklist
IDs are mapped; metadata/dependency/link validation passes. No product code,
services, account/deployment operations, builds, uploads or pushes were run.
