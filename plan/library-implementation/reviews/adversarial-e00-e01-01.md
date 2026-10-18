# Independent adversarial E00/E01 review 01

Date: 2026-10-16. Scope: the finished E00 prerequisite implementation and
evidence, and the latest E01 decision records. This reviewer started without
inherited conversation history and inspected actual source, dirty diffs,
canonical stories, contracts, evidence bytes and local test results. Earlier
review verdicts were not substituted for this review.

**Verdict: no new actionable findings in this bounded slice. E00 is accepted
at the identified snapshot. E01.F1.S1, E01.F1.S2 and E01.F2.S1 are accepted as
completed decisions; E01.F2.S2 and E01 remain incomplete for the explicitly
missing ownership details. E02 is not authorized by this review.**

## Independently executed checks

No repository or ancestor AGENTS.md was present. The dirty checkout was
preserved; no reset, stash, staging, commit, push, product-source edit, service
startup, remote IMDb operation or credential/private-log inspection occurred.
Only the named owned test logs were inspected. Plan validation regenerated
the canonical Markdown mirrors; the reviewer added this report.

| Check | Result |
| --- | --- |
| Fresh `capture_baseline.py` capture | All 224 file entries and environment contents match accepted-before, accepted-after, decision-update and handoff manifests; independently recomputed digests match |
| Focused prerequisite/planner/orchestration lane, exact seven files from E00 evidence | **417 passed in 3.30 seconds** |
| `python -m ruff check src tests examples` | Pass |
| `python -m pip check` | Pass |
| `git diff --check` | Pass |
| Canonical plan renderer | 13 epics, 26 features, 54 stories, all 48 checklist IDs covered; dependency graphs acyclic |
| Recorded baseline/DLQ log and tool hashes | Every recorded file size/hash verified against its bytes |
| Approved brief/cancellation/ordering technical bodies | All three original approval-time SHA-256 values independently reconstructed exactly |

The exact focused command was:

```sh
.venv/bin/python -m pytest tests/test_library_preflight.py tests/test_dlq_proof_harness.py tests/test_edge_planner.py tests/test_external_planner.py tests/test_planner_abort.py tests/test_planned_edges_cli.py tests/test_auto_settings.py -q
```

Source SHA-256:
`ba6647d27bc384b5d9ff3ecb67566adc89c18142917fcd056d5c3cb0a7cb80e0`.
Environment SHA-256:
`048a4d2d18ada968b28398105c6086473679ae25eb88c7a6e5d175dad965bf89`.
Combined baseline:
`bd92b0db11bb1b462f25f2b804376c10d3f7cf729e54e1d0cfbbf83eec2c4f78`.

The capture script uses tracked inventories and narrowly named untracked test
inputs, excludes secrets/ignored datasets/generated evidence, rejects symlinks,
records missing files/executable flags and proves that changing a temporary
fixture copy changes identity without changing the real fixture. I inspected
its exclusion inventory: omitted tracked paths are documentation, environment
examples/version selection and placeholder files, rather than missing runtime
or test source. Interpreter identity is recorded separately. No generated
evidence is recursively included in its own identity.

## E00 implementation and evidence

I inspected the planner, schema-loader, CLI and orchestration changes and their
regressions. Output conflicts do not delete the owning planner's output,
progress, lock or failure marker; finish-path mismatches fail before modifying
either output. Spill directories use distinct owned temporary paths. The
Kafka wrapper's early-failure marker requires exclusive lock ownership, and an
instantiated planner remains responsible for its own failure evidence. The
focused lane exercises conflicts, occupied/free startup failures, stale-state
replacement, closed-planner reuse, invalid producer inputs, topic ownership,
schema read/decode failures and import-time logging behavior.

The maintained arrival fixture contains no plan/v7 import. I inspected the
isolated service fixture and live assertion path. Fleet and retry share schema
labels and the pinned lease scheme; the graph must equal every expected pair
and value and retry commits must reach observed DLQ high watermarks. The proof
checks four type-wide DLQ partitions, sufficient DLQ envelopes, no parked rows,
no per-topic DLQs and completed LIKES query-log writes with zero lock waiting.
The cleanup path attempts all registered closers after failures, waits after
killing the retry child, closes its log, stops/closes the lease loop, and stops
the owned Neo4j container even after driver errors. The negative controls ran
in the independent focused lane. The outer supervisor signals only its own
process group; Docker cleanup remains testcontainers/Ryuk-owned.

I inspected the actual owned full-suite and focused logs rather than accepting
their summaries: **3,374 passed, 4 skipped, 49 deselected, 19 warnings in
171.41 seconds**, and **417 passed in 3.23 seconds**, respectively. Every skip
is individually explained in E00.F1.S1. I checked the historical guard source,
confirmed both referenced Git objects are absent, and confirmed `sh` and `bash`
resolve differently. The deliberately multi-pair schema exclusion agrees with
its test. The 15+4 fork warnings remain explicit A5/E08 work. Skips and warnings
are not converted into successful support claims.

I inspected and hashed the named owned service artifacts under
`/tmp/graph-loader-e00-dlq-20261016-02/`. The exact named B3 test reports
**1 passed, no skip, 23.45 seconds**. Its supervisor reports exit 0, no timeout,
and empty before/after container inventories. The summary reports 3,000 input
rows, 783 exact expected/actual pairs, 582 shared pairs, four DLQ partitions
and 4,059 envelopes/commits. I independently summed the partition high
watermarks to 4,059 and parsed the multiline query log: **561 completed LIKES
entries, total lock waiting 0 ms**. Recorded tools and all associated owned
log files match their recorded SHA-256 hashes.

The full and real-service runs were not repeated blindly: their actual log
bytes and identical source/environment binding support reuse. The independent
417-test lane was rerun. E00's macOS checkout and cp-kafka:7.6.0 execution
remain development/B3 evidence; installed Linux support, the 7.7.0 target,
broader ordering/failure matrices and new public cancellation remain downstream.

## E01 approvals, immutable contracts and ownership

I compared all four E01 story records, the canonical backlog, decision register,
brief, cancellation contract, ordering matrix and conditional E02 handoff.
The supplied trusted direct human replies approve the named distribution,
import and command plus Shashank Shukla's public maintainer/index ownership
and supplied public contact; approve the full brief's D02–D05/D07 technical
policy; and choose recommended prebuilt images for now while deferring further
discussion. The records correctly distinguish that consent from tested support
or live publication/push authority.

For each contract, I replaced only its updated introductory status header with
the supplied original approval-time header and independently hashed the result.
All three match `e01-approved-input-hashes.json` exactly:

| Document | Reconstructed approval SHA-256 |
| --- | --- |
| decision-brief.md | `f68a27ded2f6a3447f7b7167f7f5692f7f2a81ee1b2637fdac6f39133eeb36a6` |
| cancellation-contract.md | `8c7fd624d8f2f9287efc164f2d881be3563cc7dffe52cefe0b9ce41187de5692` |
| ordering-matrix.md | `b02d4e87f8ee13ac03fec2a256ad0e3fae3902f19523eaa9ded9a3747c532058` |

Thus the public exports/types/errors/CLI modes, compatibility surfaces, complete
framed nonblocking transport and codec bounds, operation-wide deadline anchors,
inline/process exclusions, 100-ms observation slices, aggregate cleanup target,
O1–O8 restrictions and conditional order_by assumptions are unchanged. The
records retain the approved platform/minimum-dependency/Rust policies as proof
targets and preserve later correction/review requirements. Source inspection
agrees with the existing plan/chunk/DLQ/group identities and dependency inventory.
The dated name-check evidence makes no reservation/account-access claim; an
independent PATH check still finds neither proposed executable.

The ownership interpretation is appropriately conservative. The first reply
answers a question explicitly requesting public maintainer/index ownership;
the second says approval without supplying the additionally requested release
owner; the third supplies neither registry namespace nor authorized image
owner. Retaining those concrete missing fields avoids inventing operational
roles. It does not reopen or understate already granted technical approvals.
No additional permission to push or publish follows from any of these replies.

## Remaining gates and handoff

E01.F2.S2 remains open for **public registry namespace, authorized image owner
and designated release owner/operator**. The same person or organization may
hold multiple roles once designated. No repeat approval of D01–D05 or D07 policy
is needed. E01 cannot yet close under its canonical exit gate.

The handoff preserves the dirty/untracked checkout, exact tested identities,
individual skip/warning limits, conditional platform/service claims, E04 story
allocation and clean-candidate/exact-byte/publication boundaries. E02–E12 remain
not started in the canonical backlog. Completing this bounded review grants
neither E02 execution nor broader API/ordering/support scope. A separately
scoped successor assignment must respect the remaining E01 gate.
