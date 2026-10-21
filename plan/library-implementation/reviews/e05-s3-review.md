# E05.F2.S1 independent implementation review — 2026-10-21

**Verdict: accepted after repairs. No remaining actionable S3 finding.**

This separately initialized review inspected actual tracked/untracked changes at
HEAD `14fe4934976f62f421086e541e6bc00dbc1ceec8`, the execution plan, handoff,
canonical E05 and prior independent S1/S2 acceptances. Scope is S3 runtime paths,
preflight, Docker launcher mounts/environment, planner output ownership and
CLI/container evidence paths. No implementation source was edited, private
settings loaded, remote IMDb contacted, foreign state deleted, image selection
redesigned or publication performed. All adversarial files belong to this review.
Final rebuilt artifacts, four-image read-only proof and B3 are separate open
full-E05 gates; pending concurrent proof is not acceptance.

## Findings retained for repair and re-review

**[P2, closed after repair] Preflight omitted deterministic temporary ledger/progress outputs.**
`_runtime.prepare_run` probes final ledger/progress paths but omits the hidden
siblings actually opened by `planned_edges.persist_ledger` and
`edge_planner._Heartbeat.beat`. Public `main` with an external plan and an existing
`.caller-plan.json.ledger.json.tmp` directory dispatches successfully, then the
real ledger writer raises `IsADirectoryError`. The same temporary path as a
symlink to an owned foreign sentinel is accepted; the real writer truncates that
sentinel before replacing the link with the ledger. These failures occur after
ingestion rather than during promised output preflight. Preflight the actual
fixed temporary siblings for external/automatic ledger and automatic progress,
rejecting nonregular/symlink/unwritable outputs while preserving existing bytes.
Original controlled public-entry and real-writer reproduction is retained in
`var/e05-validation-20261021/s3-independent/original-temp-sibling-finding.log`.
No real ingestion or external service was used.

The author added checks for ledger final/hidden temporary siblings and automatic
progress hidden temporary output. Independent re-review exercised all9 combinations
of external ledger/automatic ledger/automatic progress and directory/symlink/
read-only blockers. Every case returned1 before dispatch and preserved sentinel,
link and existing file bytes. Evidence: `s3-independent/first-repair-rereview.log`.

**[P2, closed after repair] Cheap planner numeric/mode errors created persistent state and started metrics.**
The new early checks do not include planner numeric bounds or the bulk-only
`--edge-plan` requirement. Real public `main` with `--mode bulk --edge-plan
--plan-writers 0`, `--plan-workers 0`, or `--mode stream --edge-plan` creates a
persistent attempt/schema and calls the CLI metrics starter before returning1.
The execution plan and CLI guide explicitly promise cheap invalid numeric/mode
flags fail before state creation. Move schema-independent planner range/mode
checks into the early phase, retaining schema-dependent checks at their required
boundary. Add public-entry assertions that invalid flags do not create output,
start metrics or contact services. Original real-handler reproduction, with
services forbidden and metrics start recorded, is retained in
`var/e05-validation-20261021/s3-independent/original-invalid-flags.log`.

The first repair extracts planner flag checks into early validation. Independent
re-review confirms the three original planner controls now create no state and
start no metrics. However, `--mode stream --edge-leases` still creates persistent
state and starts metrics before the bulk-only rejection; this same mode-validation
finding was not closed at that first re-review. Evidence:
`s3-independent/first-repair-rereview.log`.

The second repair adds the early bulk-only lease-mode gate and reuses extracted
lease numeric checks at the early boundary for explicit bulk lease/default stream
paths; schema-selected default bulk lease checks run after schema read and before
state creation. Independent second re-review confirms all original planner cases
and the remaining stream/lease incompatibility return1 with no attempt and no
metrics start. All9 hidden-output protection controls still pass. Evidence:
`s3-independent/final-repair-rereview.log`.

## Independent checks completed before repairs

- Fresh dev CPython3.12 ran runtime, CLI, resources, shutdown, planned-edge CLI,
  DockerService, metrics and public cleanup/R1 tests: **330 passed in93.09s**,
  no skips or warnings. The initial invocation named nonexistent
  `tests/test_planned_edges.py` and collected nothing; corrected invocation uses
  `tests/test_planned_edges_cli.py`. Both logs are retained as
  `s3-independent-targeted.log` and `s3-independent-targeted-corrected.log`.
- Independently exercised actual retry-consumer main with shared controlled
  service fixtures: an absent `REJECTION_LOG_PATH` retains the legacy standalone
  fallback; an explicit path with spaces reaches the malformed-record sink
  exactly. Both cases returned0. Evidence:
  `s3-independent/retry-fallback.log`.
- Independently executed the new runtime fixture under isolated Python: two
  distinct attempts (one explicit parent, one OS default), all7 launch roles each,
  absolute owned host paths, read-only config/plan versus writable output mounts,
  unchanged environment and public-entry preflight controls passed. This is a
  controlled client test, not actual Docker read-only-rootfs proof. Evidence:
  `s3-independent/runtime-fixture.log`. The adapted legacy rejection-default test
  also independently passed (**1 passed in0.10s**).
- Source review confirms per-attempt0700 directories and0600 schema copies,
  explicit directory override priority, no process-environment rewrite,
  per-instance timing/rejection mount state, all seven container-role bindings,
  planner-owned spill/plan/rejection paths and explicit CLI periodic/final metrics
  routing. E04 transport and planner cleanup source remains unchanged.

Repair-focused runtime/planned-edge CLI/CLI tests independently passed: **168 passed
in37.97s**, no skips or warnings (`s3-independent/repaired-tests.log`). Ruff on
all touched runtime modules, new runtime tests/fixture and strict artifact verifier
passed; `git diff --check` passed.

This report retains both original findings, their first re-review and the verified
final repairs. The adjusted existing tests deliberately track the new optional
rejection-directory default while asserting legacy resolution/explicit priority,
and construct DockerService with a controlled client instead of bypassing its
constructor. Their semantic assertions remain intact.

## Final repaired checks

The final independent broad targeted invocation ran runtime/CLI/shutdown/
resources/planned-edge CLI/Docker/metrics/public-cleanup R1/default-mode/multi-topic/
environment controls against the repaired source. Result: **437 passed in 93.48s (0:01:33)**,
no skips or warnings. Evidence: `s3-independent/final-targeted-corrected.log`.
An initial invocation named nonexistent default-mode/multi-topic test filenames
and collected nothing; `s3-independent/final-targeted.log` retains that attempt.
Final Ruff and `git diff --check` passed. Reviewed input hashes were checked again
after execution and remained equal.

## Reviewed input identities

The repaired S3 acceptance is bound to these SHA-256 inputs and the complete
machine-readable mapping in `s3-independent/reviewed-hashes.json`. Later product
edits require re-review. Final full-suite/artifact/image/B3 acceptance remains
separate from this story review.

| File | SHA-256 |
| --- | --- |
| `src/neo4j_graph_loader/_runtime.py` | `da731749af45c524c1e9e86b5437ed0e79cb58660ba1551d6c460065a58cb3e8` |
| `src/neo4j_graph_loader/cli.py` | `7502bebc56e039cbef38dcc5f57c08f5da6a2ffb18183848871e5815c6935b6e` |
| `src/neo4j_graph_loader/orchestrator/docker_service.py` | `2ae0db315a24a0b72c01f95ce2b314234ab1f1e29bc7f37cd2fd0655f942c445` |
| `src/neo4j_graph_loader/orchestrator/planned_edges.py` | `315a3271f275692f215ab2a3349170f8414560857374b539e6b46a18e76e5cc5` |
| `src/neo4j_graph_loader/loader/retry_consumer.py` | `bc4a85d832982bbef1b045967e4fecfd9a1fc13e2ab568a97d8007de2c695034` |
| `src/neo4j_graph_loader/utils/metrics.py` | `6fb61ce5cb818b495be08e97a5f8871d1d30e4d5733841d8c4e5f13fb7717e7c` |
| `tests/test_runtime_paths.py` | `427427715207c240abfa41ba89cef0d59fe2c8eab79bc1a8c0e73fcfd4582aae` |
| `tests/fixtures/e05_runtime.py` | `9a76c67f6598810769b0c57eb44db83eaa5bff9599b525a7d41219cab89f4cd3` |
| `tests/test_auto_settings.py` | `76994bf3faf4a4b79a9bb919b9f55693e9ab4479bd12f8c3d6a03228c174018d` |
| `tests/test_multi_topic_stream_bulk.py` | `7768ab3393a43bfa2aef1406b4c61629090eb46e58f275fc50d6ee86c8b35d5a` |
| `docs/CLI_RUNTIME.md` | `dcb5c4422b144b27a99c7351ca66835da6d3f29b01b9099911167439b27c8519` |
| `plan/library-implementation/tools/verify_e03_artifacts.py` | `03aaedca9f42d13d7b62f5efcabd2e60164f5bcb5d4c41e63b4804e4cb512867` |
