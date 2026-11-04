# Release decision register

Updated 2026-10-16 from the user's direct replies to the three approval questions.
Decision owner: **Shashank Shukla**, authorized public maintainer/index owner;
public contact: **shashankshukla96@gmail.com** (supplied for this public role).
No private account identifiers, tokens, credentials or private contact details belong here. Public registry/repository names below are intentional release identities.

**D01–D07 are resolved; all four E01 stories are complete.** The original
technical approvals remain unchanged. Following the user's instructions to use
the recommendation and then “finish the rest”, the agent selected the concrete
registry and assigned image/release accountability to the already approved
public maintainer. These are delegated choices, not literal registry strings
supplied by the user. [Independent checkpoint 1](reviews/e01-completion-checkpoints.md)
approved that interpretation and the read-only identity/registry investigation.
Completion-record and final validation reviews follow in the same report.

See [direct approval evidence](evidence/e01-user-decisions-20261016.md), the
[approved input hashes](evidence/e01-approved-input-hashes.json) and per-story
evidence. Approval selects future contracts; it does not claim they are already
implemented/tested or authorize E02–E12 execution, uploads or pushes.

| ID | Status / decision on 2026-10-16 | Resolving story |
| --- | --- | --- |
| D01 | **Approved.** Distribution `neo4j-graph-loader`, import `neo4j_graph_loader`, command `graph-loader`; Shashank Shukla is the authorized public maintainer/index owner, public contact above. Dated index/PATH checks are in evidence; availability is not reservation or account-access proof. | E01.F1.S1 |
| D02 | **Approved in full as briefed.** Explicit schema/planning modules, types/errors and existing installed CLI modes; producer planning requires Kafka; embedded fleet/retry/index/worker APIs internal. Full [cancellation contract](cancellation-contract.md), including complete-frame nonblocking reads/writes, codec bounds, operation-wide deadlines, process/inline exclusions, approved errors and 10-second aggregate cleanup target. O1–O8 [restricted ordering dispositions](ordering-matrix.md) and conditional `order_by` assumptions; no new exactly-once/restart-safe arrival promise. Implementation/proof remains E04/E08/E09. | E01.F1.S2 |
| D03 | **Approved.** Target `0.1.0a1`, freeze in E11; no legacy `src` shim; alpha API changes documented, deprecated names/flags retained through at least one subsequent alpha except documented urgent fixes. Preserve schema, plan/chunk, DLQ v1/v2 decode and v2 encode, index and group identities as specified in the brief's compatibility table. | E01.F1.S2 |
| D04 | **Approved proof targets.** CPython 3.12, Linux arm64/amd64 for APIs and CLI/container operations; `requires-python >=3.12`, no speculative upper cap. Neo4j 5.26 Enterprise, Kafka 3.7 via cp-kafka:7.7.0; classic APOC requirements. Every advertised platform/start-method/worker-bound cell still requires E08/E09 proof. Current macOS checkout and E00 cp-kafka:7.6.0 are not that matrix. | E01.F2.S1 |
| D05 | **Approved.** All eight current runtime dependencies mandatory, including matched Neo4j/Rust extension; no extras or advertised pure-driver fallback. E08's independently reviewed corrections set paired Neo4j/Rust floors to6.0 (exact minimum6.0.0/6.0.0.0) for the existing typed pool-error contract, and PyYAML to6.0.1 for CPython3.12 isolated installation. Original driver5.20/YAML6.0 failed proofs remain; [driver review](reviews/e08-driver-floor-review.md), [YAML review](reviews/e08-dependency-floor-review.md). Full matrix remains open. **E15 amendment (2026-11-03, user decision D15-4: "use PG, which can be configured"; "Either json or a sql db"):** the base dependency list is unchanged and still mandatory; two OPTIONAL extras are approved, finite and explicit: `sql` (`SQLAlchemy>=2.0.30,<3`) and `postgres` (`SQLAlchemy>=2.0.30,<3`, `psycopg[binary]>=3.1.12,<4`; the floors are the lowest versions the SQL tests pass on). No other extra is allowed: the strict artifact verifier (`plan/library-implementation/tools/verify_e03_artifacts.py`, `APPROVED_EXTRAS`) and the installed runner (`scripts/verify_installed.py`) fail a wheel whose `Provides-Extra` or extra `Requires-Dist` differ from the list. The one module that needs an extra at import time (`neo4j_graph_loader/status/sql_store.py`, SQLAlchemy) is listed explicitly (`OPTIONAL_MODULES`); a base install imports every other module and must fail to import that one with a `ModuleNotFoundError` for exactly `sqlalchemy`. | E01.F2.S1; E08 correction2026-10-27; E15.F2.S11 |
| D06 | **Resolved under delegated completion; namespace amended 2026-11-03 by the user (D19-1, E19.F1.S1): the images are `ghcr.io/neburyx/...` and the repository `Neburyx/graph-loader`; the owner stays Shashank Shukla, through the Neburyx organization. The `0.1.0a1` records keep the original `ghcr.io/shashankshukla96` namespace.** Original text: versioned prebuilt images in `ghcr.io/shashankshukla96`, image owner **Shashank Shukla**; planned repositories `graph-loader-cli`, `graph-loader-node`, `graph-loader-edge`, `graph-loader-clock`. All components use the matching package version (initial target `0.1.0a1`); explicit compatible overrides, no implicit checkout builds or `latest` release identity. Public visibility and anonymous pulls required. Local image IDs before publication; actual registry digests only after separately authorized E11 push and re-pull verification. | E01.F2.S2 |
| D07 | **Resolved.** setuptools, one file-based version source, manual Twine with external credentials, no new workflows; T3 N/A. **Shashank Shukla** is the accountable release owner/operator and index/image owner under delegated completion. Preserve clean candidate/exact bytes and separate TestPyPI/image/production/Git-push approvals. | E01.F2.S2 |
| D08 | **Requested by the user 2026-10-31 ("keep only what we need to keep this library production ready"); specific cuts chosen by the orchestrator and independently verified ([review](reviews/plan-reset-review-20261031.md)).** Keep only work required by the release checklist (V6 real-service tests for planned bulk, streaming leases, classic, DLQ, failure/restart and lock-wait; V7 restricted ordering; U1–U5 docs; T1/T4–T6 TestPyPI; R1–R5). The threat model of a hostile workload escaping to the host Docker daemon is out of scope: tests run trusted code on the user's native Docker Engine with owned, labelled resources, cleanup by recorded ID and an unchanged snapshot of all foreign resources. Removed: E09.F1.S6 bootstrap fixture and the shared-gate protocol (fence, broker/settlement, retained namespaces). Merged: E09.F2.S2→E09.F2.S1, E11.F1.S4→E11.F1.S3, E12.F2.S2→E12.F2.S1; E10.F2.S2 is a runbook without a dry run; E11.F1.S1 is T1 only (T2 decided, T3 N/A); the unused bundled-context branch (D06 chose prebuilt images) is dropped; arm64 is covered by an installed-CLI smoke on the final arm64 images in E11.F1.S3. Per-epic independent review replaces per-story review rounds. | Plan reset |

E01's decision/ownership exit gate is satisfied. Selected ownership is an
accountability decision, not verified authenticated account control, package
reservation or permission to upload. Actual account access/security, package
creation/visibility, candidate image IDs/digests, public anonymous pulls and
installed/service compatibility remain their original E06/E09/E11/E12 gates.
E02 execution remains outside this assignment.

`<import_name>` and `<distribution>` resolve to approved D01 identities. The
four registry repository names resolve as listed above. Candidate versions,
wheel/sdist filenames and immutable digests remain actual E11 manifest inputs;
no fake digest or artifact has been substituted. E02–E10 builds are development
candidates; E11 creates the clean frozen candidate and records filenames/hashes.

Any later broader embedded API, stronger retry/order promise, automation or
larger platform matrix must be split/reviewed before expanded implementation.
No accepted gate, story scope, guarantee or dependency has been weakened here.
