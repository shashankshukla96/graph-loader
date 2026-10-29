# Independent E08 matrix and dependency evidence revalidation — 2026-10-28

**Disposition: historical E08.F1.S1 dependency observations are supported by the retained bytes and receipts. Complete E08 acceptance is not supported: the current implementation and its latest evidence stop at partial E08.F2.S1.** No dependency-floor or historical S1 pass-count defect was found in this audit.

This review used local files, archives and retained receipts only. No SSH, remote connection, Docker command, Linux host command, Linux container/test, service/data access or private environment-file read occurred. No tests were rerun. Only this report and ignored files under `var/e08-matrix-revalidation-20261028/` were written. Entry/current HEAD is `4b246f3ab953bd9f7d266a682ac9fff39bd32032`; user-owned E08 changes were preserved. No applicable ancestor `AGENTS.md` was found. The user's prohibition on Linux reruns remains binding; the missing proof below does not authorize obtaining it remotely.

## Findings affecting a completion claim

### E08-M1 — P1: Required S2 worker/transport/native proof is unfinished

The canonical [epic](../epics/e08.md:42), [S2 story evidence](../evidence/E08.F2.S1.md:77), [machine evidence](../evidence/e08-s2-initial-20261028.json) and [remaining design](../e08-process-matrix-design.md) consistently describe S2 as partial. This is corroborated by executable code: [the fault supervisor](../tools/run_e04_fault.py:33) explicitly rejects worker/context arguments for legacy fault scenarios; [the current public client tests](../../../tests/test_public_cancellation.py:76) exercise only worker counts 1 and 4; the installed runner has no remaining-design process-envelope option. The machine record explicitly says native S2 has not executed.

The retained new installed runs are macOS observations: 261 public-contract and 116 example-contract selections. I independently checked their zero exits, exact collected IDs, all setup/call/teardown outcomes, and their staged source hashes against current files. These passes substantiate the focused repair; they cannot prove the required native Linux arm64/amd64 process envelope. Normal counts 1–8/16/32/64, failure counts 1/2/4/8/64, the explicit prepared/cold framing/backpressure matrix and per-case provenance restoration remain unproved by the supplied records. Preserve the incomplete disposition until the required implementation and matching retained proof exist, or the support contract is explicitly revised. Do not infer authorization for Linux execution from this finding.

### E08-M2 — P1: Historical S1 cells and partial S2 capture do not satisfy final E08 retention/delivery gates

The [execution plan's final gates](../execution-e08.md:190) require final audited checkout/lint/parity/pip checks, original/rebuilt strict archives, two bare installs, all eight installed lanes for both final wheels, four fresh production-image probes, 12 separately supervised R1 controls, exact no-skip B3, actual installed Neo4j 5.26 Enterprise/Kafka 7.7 service proof, final adversarial acceptance and delivery/equality records. The latest [S2 evidence](../evidence/E08.F2.S1.md:85) keeps these open. I found no later E08 final evidence in the local E08 evidence/proof trees examined.

The latest 318-input S2 source manifest matches current captured file hashes and sizes, but it is explicitly an initial partial snapshot, not evidence that all final checks ran on those inputs. Both architecture S1 bundles contain earlier versions of six current example/client/fault files; arm64 also retains the earlier `scripts/matrix_docker.py`. The historical S1 inventory has 3,650 installed IDs, while current inventory has 3,665. Thus substituting the 14,596 historical S1 passes for a final-current full-matrix result would be incorrect. The canonical records correctly avoid that substitution. HEAD remains the entry commit; no E08 delivery or postcommit equality record is present. A completion label would need actual final proof and its concrete input/delivery identity, not only bookkeeping changes.

## Independently substantiated dependency/S1 results

I read D01, D04, D05 and D07, both floor-decision reviews, metadata, runtime pins/full locks, runner/probe implementation, native proof records and actual cell receipts. The actual retained wheel's eight `Requires-Dist` entries semantically equal current `pyproject.toml`: Neo4j/Rust `>=6.0,<7`, PyYAML `>=6.0.1,<7`, and unchanged Kafka/Pydantic/Docker/NumPy/Prometheus floors. Both remain mandatory coherent pairs; no extras or pure-driver fallback are advertised. Name/import/CLI identity, `0.1.0a1.dev0`, Python `>=3.12`, version-only root exports and two schema resources are preserved. No publication/workflow behavior was introduced by these dependency changes.

The retained actual macOS and Linux arm64 PyYAML 6.0 logs contain `cython_sources` isolated-build failures. The original installed Neo4j 5.20 bare probe contains the `ConnectionPoolError` import failure. These original failures remain red; the corrected metadata is present in the actual tested wheel. Exact minimum direct pins are Neo4j 6.0.0/Rust 6.0.0.0, Kafka 2.5.0, Pydantic 2.7.0, PyYAML 6.0.1, Docker 7.1.0, NumPy 1.26.0 and Prometheus 0.20.0. Current direct pins are dated inputs rather than a claim about today's upstream latest release. Both full target locks preserve pytest 9.1.1 and paired runtime versions.

Independent replay of the retained receipt data validates all 36 S1 observations, including four bare probes and 32 installed environments:

| Native architecture | Dependency set | Installed passes | Approved original skips | Bare runtime probe |
| --- | --- | ---: | ---: | --- |
| arm64 | minimum | 3,649 | 1 | supported by retained receipt |
| arm64 | current | 3,649 | 1 | supported by retained receipt |
| amd64 | minimum | 3,649 | 1 | supported by retained receipt |
| amd64 | current | 3,649 | 1 | supported by retained receipt |

For every installed lane, I independently compared actual collected IDs to the immutable bundle inventory and installed result; checked all phases and permitted skips; checked staged bytes; replayed actual child creation/post-exec provenance; and checked all recorded application-module hashes/origins against the wheel payload. All four bundle manifests and their application payloads match retained bytes. Each bare receipt proves the seven author/test tools absent and records a matching active architecture-specific Rust extension. Observation command log hashes, timeout/owned-cleanup receipts and native amd64 read-only-volume export equality records agree. This is receipt revalidation, not a new execution or independent reopening of retained remote volumes.

The actual amd64 archive is 94,174,736 bytes with SHA-256 `e6fe906832a24c544a5ecb04e5f1da4eabbdfa7d49258a99894458caeeb24a3b`. Every one of its 2,415 indexed proof/context files matches both the archive member and extracted local bytes; the archive additionally carries its own index. This supports the prior S1 review without relying on its approval label.

Current inventory comparison to HEAD preserves every original 3,804 per-ID disposition, every original source-baseline entry and every original installed ID in order, with unchanged allowed skips. Current additions are 37 authoring controls plus 15 runtime controls. The actual S1 wheel is SHA-256 `a804d26a8130ee7fcc06256327257b7a94476896bac6b151b68f1136c6b52391`; all 59 payload members and six recorded original metadata members agree with current inventory, and all 59 payload members agree with current `src/` files.

## Retention limits and reproducible audit output

The historical no-Git rebuild receipt reports 65 equal wheel members and retains their manifests, but its physical rebuilt wheel at `/private/tmp/graph-loader-e08-no-git-rebuild-20261027-v1/artifacts/neo4j_graph_loader-0.1.0a1.dev0-py3-none-any.whl` is absent at that recorded path. The original wheel and sdist are retained. This review cannot independently reopen the rebuilt wheel bytes or treat that historical receipt as final dual-wheel execution proof. No recreation was attempted.

The independent audit scripts/results are retained under `var/e08-matrix-revalidation-20261028/`: `audit.py`, `audit.json`, `artifact_audit.py` and `artifact-audit.json`. The reference scan checked 644 distinct path/hash/size triples from E08 evidence, with no missing referenced file. Its 18 mismatches belong to historical source snapshots whose files changed during documented E08 work; the latest 318-file source capture has zero hash/size mismatches. Those historical mismatches are not evidence of corrupted immutable target receipts. The absent rebuilt wheel is recorded inside the nested historical no-Git result, rather than among those 644 direct references.

No finding here establishes a runtime regression. The actionable gap is the attempted promotion of completed dependency/focused-repair proof into complete E08 acceptance. The current canonical in-progress state is consistent with the implementation and retained evidence.
