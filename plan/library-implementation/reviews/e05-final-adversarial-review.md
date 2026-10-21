# E05 independent final adversarial review — 2026-10-21

**Verdict: accepted for E05 completion. No open actionable finding.**

Acceptance binds only the finald candidate: entry HEAD
`14fe4934976f62f421086e541e6bc00dbc1ceec8`, branch
`codex/pypi-library-checklist`, **270 inputs**, source identity
`4485190c3f87ba5c7f03e933716ebc21aafbbe06d749a4d88a946418d79f2d76`,
environment identity
`e49ae7c075909a04df1d517f3fec72030e944b95829f4d7591ee877d7072c9c4`.
The reviewer independently compared every current input's bytes, size and executable
mode with the frozen manifest and checked exact before/after source/environment
equality. The complete mapping is the [tested manifest](../evidence/e05-tested-manifest.json).
Later product/input changes require renewed review. Completion records, staging
and delivery remain a separate check; this report does not certify a commit.

This was a separately initialized reviewer, not the implementation author. The
review read the handoff, execution plan, canonical E05, decisions, story reports,
resolutions, actual tracked/untracked source and test changes, packaging and user
guides. No applicable AGENTS.md was found. The earlier account-limit interruption
produced no verdict. This resumed review executed its own controls and preserved
its earlier attempts. Only reviewer evidence/report files and owned ignored proof
artifacts were written; the author made the requested repairs.

## Findings preserved and closed

The three earlier product findings remain visible in the [story reports and
resolutions](e05-resolutions.md): S2 missing/undecodable resource errors, S3 hidden
ledger/progress output siblings, and S3 incomplete early planner/lease validation.
The first partial S3 repair and its remaining stream/lease failure are retained.
Independent finald controls re-exercised the repaired behavior, including additional
special-file and concurrent-run cases. None of these findings was waived.

**Final-F1 — closed: the original “no-Git” rebuild could discover the parent checkout.**
The finalb driver extracted below the repository and checked only for a local
`.git`. Independently running `git rev-parse --show-toplevel` from that source
resolved this repository. Thus that attempt did not establish its claimed proof.
The reviewer rebuilt outside all checkouts, explicitly observed discovery exit128,
ran an isolated build and strict Twine, and compared every wheel-member byte.
The author corrected its driver and repeated finald packaging with the same
negative discovery check. The reviewer then independently repeated that complete
control on finald: **63 member contents equal**, including metadata/resources.
Original attempts and qualified earlier claims remain historical; finald and the
independent repeat close the gate. Exact commands and hashes are in
[independent evidence](../evidence/e05-final-independent.json).

**Final-F2 — closed: current guides/help retained obsolete state/default/retention claims.**
The author identified stale README/CLI/checklist statements. Review also found the
old spill default and rejection-directory/deletion claims in operational guides,
plus timing prose implying diagnostics required an environment override. Targeted
corrections now document private attempt plans/spill/rejections/timings, persistent
evidence and worker-owned cleanup, explicit overrides and direct-component legacy
contexts. Both parser help strings now agree. Eight affected documentation files
and the two help strings were reviewed before the finald freeze. A comparison
with the earlier candidate confirms no functional runtime logic changed in these
last repairs. All packaging/image/full-suite proof was regenerated for finald.

## Independently executed checks

Raw reviewer proof is under `var/e05-validation-20261021/final-independent/`;
**finald/** contains the accepted final-byte checks. Its command arrays, script/log
hashes, origins and artifact identities are bound in
[e05-final-independent.json](../evidence/e05-final-independent.json), SHA-256
`eb1bbb90e6e35ff954854194317e5b143219dfd99855b96a480e579f49754fad`.

- Broad source review controls passed **354 tests in99.14s**, without skips or
  warnings. After the help/guide corrections, the exact finald entry/resource/runtime
  suite passed **50 tests in7.71s**, without skips or warnings. The broad invocation
  covered CLI/shutdown/resources/planned CLI/Docker/metrics/public cleanup, including
  E04/R1. Finald used:
  `var/e05-validation-20261021/dev/bin/python -m pytest tests/test_runtime_paths.py tests/test_cli_resources.py tests/test_installed_cli.py -q -rs`.
- The independently installed finald public `main` exercised **11 public-main
  early-error cases**, forbidding schema/state/metrics side effects. **70 output
  controls** covered final/hidden plan and timing files blocked by directories,
  ordinary/dangling symlinks, FIFOs and read-only files. Every case rejected before
  dispatch, preserved sentinel bytes/types/links and contacted no service.
- **14 interleaved launcher calls** across two service instances checked all seven
  roles, absolute host paths, read-only schema/plan versus writable rejection/timing
  binds and per-container output paths. **Eight concurrent public-main calls** with
  the same explicit identity produced eight distinct 0700 roots and 0600 schema copies.
  Environment equality held. These were controlled dispatch/clients, not service fleets.
- The real handler's failure path used one explicit periodic/final metrics snapshot
  path. Genuine ZIP traversables and `as_file` tested materialization through handler
  cleanup and exception cleanup; temporary paths disappeared afterwards. Missing and
  invalidUTF8 sample controls returned1 with no partial stdout.
- Two additional clean, noneditable finald wheel installs were created under
  `/var/folders/5p/84cp5b7s72q_ymcsd4qlvq8r0000gn/T/graph-loader-e05-final-review-c4vkkvzh/finald/`.
  Original/rebuilt probes independently passed all55 module-byte/site-packages origins,
  inert imports, sole entry/version metadata, executable/package/CLI-module parser parity,
  public examples, resource/caller schemas, actual read-only package/cwd and runtime
  fixtures. No PYTHONPATH or development-tool rescue was used. Both pip checks passed.
- Strict archive inspection passed for both finald wheels/sdist. **Eleven independently
  mutated archives** were actually rejected: resource/entry-point omissions and injected
  non-allowlisted members. Strict Twine passed on all finald artifacts and the reviewer's
  separate outside-Git rebuilt wheel. No leak/metadata/dependency check was weakened.
- **28 separately supervised finald process controls** passed: four image module/origin
  checks, four read-only resource fixtures, four runtime fixtures, four actual data-bind
  tests and **12 public spawned R1 mixed-source cases**. The R1 matrix covers default/
  finite timeouts in both independent installs and all four immutable images, preserving
  the LookupError cause, prompt shared cleanup, dead children, closed endpoints and
  reclaimed owned state before fixture emergency cleanup. All exited0 without timeout.
  Image commands used `--network none --read-only`, owned `/tmp` tmpfs and no source
  mounts. Actual binds used only owned schema/plan/rejection/timing data and proved
  denied input writes plus successful output writes. Final inventory was empty.
- Independent final Ruff, requirement parity, staged/unstaged whitespace and clean
  install dependency checks passed. The executable commands for the packaging,
  process and identity audits are the `candidate-check.py`, `negative-controls.py`,
  `process-controls.py`, `no-git-rebuild.py` and `identity-check.py` scripts in the raw
  reviewer finald directory. Exact installed `-I -c` commands are retained as JSON.

The implementation agent's final full run was independently inspected rather than
counted as a reviewer execution: **3566 passed,4 inherited skips,49 deselected,
19 known warnings in196.08s**, exit0. Raw log SHA-256:
`f0a3cff1fd29abc9becdac4766148f32b4a4eba5985a7e941d17da6c6ec40696`.
The four skip reasons and the15 subset/four planner fork warnings retain their
existing ownership; no added skip/warning is hidden. The exact owned B3 command,
raw supervisor result and unchanged semantic assertions were inspected: **1 passed
in26.79s**, no skip/timeout, before/after container inventories empty. B3 log SHA-256:
`be8be838a42f41f5cea69ce23fe4b45ff3da9b4fcd336698963287facbd015d3`.
The final evidence JSON was compared with raw finald proof and its log hashes.

## Accepted identities and boundaries

| Input/artifact | SHA-256 |
| --- | --- |
| `src/neo4j_graph_loader/cli.py` | `da8ae9ef959737ecf93ed90cf0654cf49b3b7eac44b024d664741485889ec8e8` |
| `src/neo4j_graph_loader/_runtime.py` | `da731749af45c524c1e9e86b5437ed0e79cb58660ba1551d6c460065a58cb3e8` |
| `src/neo4j_graph_loader/_resources.py` | `c19cf2cdebc7f4c311aa80b9799682b17a9426b675cc71f97590f2d003692551` |
| `src/neo4j_graph_loader/orchestrator/docker_service.py` | `2ae0db315a24a0b72c01f95ce2b314234ab1f1e29bc7f37cd2fd0655f942c445` |
| `src/neo4j_graph_loader/orchestrator/planned_edges.py` | `315a3271f275692f215ab2a3349170f8414560857374b539e6b46a18e76e5cc5` |
| Original finald wheel | `99a88f8b88aa5e80495e6089c74ff4e9e5c44a0e77cba288e55922531c7ca434` |
| Finald sdist | `3fe448722e6d15d3c674dfad6db840648846d5f48f3d23813f746d2d9555df59` |
| Author's outside-Git rebuilt wheel | `66d6bf439175d844ea2d42415d5e3b7a2c55d798f59bf8f3f665c6737c6d51ad` |
| Reviewer's outside-Git rebuilt wheel | `7616d88c50c5c33996a508eb3aa892d0b3758375044f2859b9e004ba3d25b979` |

All63 member contents agree across those wheels; whole ZIP hashes may differ.
Accepted immutable images are CLI
`sha256:082adbfbdce03674c8683317c5b9e7d3d6ed462fbbd9fc0a1b845bee7ddad867`,
node `sha256:4856edbddfc0faf3062f5b3ba80e92bf2bae403a5618f28cf6aaa68167284144`,
edge `sha256:3dd1dcfc532c282cb1f4a7762930e19e293eef8033b1e93b5d3c23d55bfc4ae3`,
clock `sha256:d43ce61eac26a5567299e6fe8ca71cef5eef0b0fbe85659e542072a08b0fa831`.

This accepts E05's installed entry/resources/writable-state contract. E06 image
selection and E07–E12/platform/release/service-matrix gates remain open. Host bind
sources still require daemon visibility. Explicit shared paths and unchanged
Kafka/container identities require caller coordination; default attempt evidence
is persistent. No complete checkout-free service fleet is claimed. Repository
`.env` was neither read nor sourced; no remote IMDb/private settings, foreign
resource deletion, global Docker prune, publication, push, tag or commit was used.
