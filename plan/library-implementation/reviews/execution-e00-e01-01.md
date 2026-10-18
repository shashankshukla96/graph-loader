# Independent E00/E01 execution review 01

Date: 2026-10-16. Reviewer: independent agent with no inherited conversation
history. Scope: bounded E00 prerequisite repairs/evidence and E01 decision
preparation. No E02–E12 implementation or release approval is given.

**Final verdict: E00 APPROVED as completed execution at the identified source
and environment below. Both E00.F1.S1 and E00.F2.S1 meet their gates.** No
required correction remains within this bounded execution. E01 preparation is
reviewable, but **E01 remains incomplete**: every D01–D07 entry still requires
the human decision specified by its story. No execution approval is inferred
from earlier plan-review approval.

## Independent inspection and correction loop

I read E00/E01, the plan README, decision register, cancellation contract,
ordering matrix, existing prerequisite source/tests and dirty changes. No
applicable repository/ancestor `AGENTS.md` was found. I preserved existing work
and did not edit product source, start services, inspect credentials/private
operational logs/`.env`, push or publish.

The following findings were sent to the implementing agent, repaired there,
and re-inspected here:

| Finding | Verified correction |
| --- | --- |
| `KafkaEdgePlanner.finish()` wrote a foreign failure marker after an output-lock conflict | Finish paths are validated; early failures obtain the output lock exclusively; instantiated planners own their markers. I independently reproduced the original corruption using only inline workers, fake Kafka and a temporary directory. Added conflict and occupied/free early-failure regressions pass. |
| DLQ fleet and retry child leased different label schemes | Fleet derives labels from its actual schema; the retry child shares those labels; the DLQ lease server pins the same scheme. The new label regression passes. |
| Fleet shutdown assertions skipped remaining cleanup; killed retry child was not reaped; log handle had no explicit owner | `ExitStack` attempts every closer, the retry child is waited after kill, and its log is closed explicitly. Failure-continuation and killed-child regressions pass. |
| Lease-server startup failure leaked its loop; driver close failure skipped container removal | Failed server startup invokes cleanup, stopped loops close, and Neo4j teardown nests driver/container cleanup. Both focused failure regressions pass. |
| Graph equality alone did not prove DLQ drain; hard timeout bypassed Python cleanup | Test also requires retry committed offsets to reach observed DLQ high watermarks. An external process-group supervisor terminates only its owned process group; testcontainers/Ryuk retains Docker ownership. |

I independently ran `.venv/bin/python -m pytest tests/test_library_preflight.py
tests/test_dlq_proof_harness.py -q` after the final repairs: **23 passed in
0.57s**. These unit regressions support the bounded corrections; they do not
replace the live B3 test.

## Source/environment identity

I invoked `capture_baseline.capture()` independently in memory and compared its
result with `evidence/baseline-accepted-before.json`. All **224** allowlisted
file entries and the environment identity match. Source SHA-256:

`ba6647d27bc384b5d9ff3ecb67566adc89c18142917fcd056d5c3cb0a7cb80e0`

The inventory includes the relevant tracked source/config/build/test helpers,
IMDb example tests, and all three explicit untracked test/fixture inputs. It
records staged/unstaged binary diff hashes and interpreter/platform/dependency
versions, excludes ignored datasets, secrets and generated plan evidence, and
rejects symlink inputs. Its temporary fixture-copy mutation changes the source
identity while preserving the real fixture. I checked that behavior myself.

Review-tool provenance:

| Tool | SHA-256 |
| --- | --- |
| `tools/capture_baseline.py` | `22d96b82b07ce0aca0021904f6c768d4e325ef10bc37a6b8b54e5a7efbb545fa` |
| `tools/run_dlq_proof.py` | `d5996ef95d4ddd9f574a54d243e271d6103995e0944117c50c22ce9740c91c62` |

## Required service evidence inspected

I read the owned test artifacts in
`/tmp/graph-loader-e00-dlq-20261016-02/`: `result.json`, `pytest.log`,
`proof-summary.json`, and the throwaway Neo4j query log. I also hashed the owned
retry log without relying on its contents as proof. The exact named B3 test
ran with `-m integration -v`: **1 passed, 0 skipped, 23.45 seconds**. Supervisor
exit code is 0, timeout is false, and before/after container inventories are
empty. The test obtains service endpoints from its throwaway fixtures.

The executed assertions establish 3,000 input rows, an exact graph of 783
expected pairs, 582 pairs shared across topics, four type-wide DLQ partitions,
4,059 DLQ records and committed offsets, no parked records and no per-topic
DLQs. I independently summed the recorded partition high watermarks to 4,059
and parsed the multiline query log: **561 completed LIKES entries, total lock
waiting 0 ms**. These are the restricted single-execution assertions in this
test, not new restart/replica/durability guarantees.

| Artifact | SHA-256 |
| --- | --- |
| `result.json` | `d7d70083a603500bd09f1c95f0ad100b8d69ba907fa8ccc44f8226639090b6e7` |
| `pytest.log` | `bba05cfbbce7e5ed866fdb256196f8795c0f978d77e8867117d7b6f817b97134` |
| `proof-summary.json` | `e2b91c2817144f361e2da40ee50d4223fe3b2f0c414934f1de02861f035bf470` |
| `neo4j-query.log` | `60f2134a56a55976bed6ae207b9f32ccf0ee50095b1931e84d600ac837314672` |
| `retry-consumer.log` | `fd1c1c8dfa32850bc4c45af0bd285ab736d298e1cb4081bb205913a89a78b965` |

## E01 decision preparation

I checked `decision-brief.md` and all four E01 story evidence records. Proposed
exports, CLI modes, errors/types, cancellation/framing limits, O1–O8 boundaries,
version/deprecation/format policy, dependency/platform/Rust options, image
delivery and publication ownership remain explicitly proposed. Current plan,
chunk, DLQ and consumer-group identities agree with the inspected source.

I independently repeated the D01 checks on 2026-10-16: canonical PyPI and
TestPyPI JSON endpoints both returned HTTP 404; neither proposed command exists
on the current PATH. Availability checks reserve nothing and establish no
ownership. Public owner/contact/index/registry/release responsibility still
needs the human response. No implicit decision, name change, packaging work,
workflow or publication authorization is accepted by this review.

## Final acceptance verification

After the preliminary report, I read the final `E00.F1.S1.md`, `E00.F2.S1.md`,
`baseline-results.json`, `dlq-proof.json`, both accepted before/after manifests,
and the owned full-suite/focused logs. The actual full log reports **3,374
passed, 4 skipped, 49 deselected, 19 warnings in 171.41 seconds**; its four
individual skip lines and 15+4 warning split agree with the story record.
The actual focused log reports **417 passed in 3.23 seconds**. I verified every
recorded log's size and SHA-256 against its bytes, including the preserved
failure-before-repair logs. I independently reran Ruff, `pip check` and
`git diff --check`; all passed.

I independently confirmed the skip explanations against test source and local
read-only checks: the shipped multi-pair example is deliberately excluded from
the single-pair invariant; both historical Git objects `8748274` and `239deb4`
are absent; `sh` and `bash` resolve to distinct files. These are explained
skips, not pass evidence. The misleading historical-guard skip wording and
installed/shell portability remain downstream inventory/support concerns.
The 19 multithreaded-fork deprecations remain explicit A5/E08 work. The named B3
service gate has no skip or warning. Historical 3,366/406/IMDb and intermediate
runs have not been substituted for this fresh final baseline.

I recomputed the canonical source/environment digests and all current inputs
again after the final full-suite run. Before, after and current source entries
and canonical environment contents are identical:

- Source: `ba6647d27bc384b5d9ff3ecb67566adc89c18142917fcd056d5c3cb0a7cb80e0`.
- Environment: `048a4d2d18ada968b28398105c6086473679ae25eb88c7a6e5d175dad965bf89`.
- Combined baseline: `bd92b0db11bb1b462f25f2b804376c10d3f7cf729e54e1d0cfbbf83eec2c4f78`.

The accepted before/after staged and unstaged binary diff hashes also match
exactly. Later clerical documentation changes may alter the unstaged diff hash;
they must remain distinguishable from the tested source inventory.

I also independently inspected the three explicitly named local service images
with output restricted to IDs, repository digests and OS/architecture. Neo4j
`5.26-enterprise`, `confluentinc/cp-kafka:7.6.0` and Ryuk `0.8.1` all match
`dlq-proof.json` and are Linux arm64; Docker server version is 29.8.0. No service
was started by this reviewer.

E00 can close at this identified snapshot. Clerical status/evidence-link updates
are permitted without treating them as product changes. Any subsequent relevant
source/environment change invalidates affected execution evidence and needs the
appropriate rerun/review. E01 and dependent E02–E12 remain gated on explicit
D01–D07 decisions; this verdict authorizes no namespace/package scaffold,
broader public guarantees, push, registry operation or publication.
I also reviewed `handoff-e02.md`: it carries the identified evidence and current
limitations, preserves dirty/untracked work, and explicitly requires E01 human
decisions, any necessary independent plan review, and a separately scoped E02
assignment before work begins. It introduces no conflicting authorization.
