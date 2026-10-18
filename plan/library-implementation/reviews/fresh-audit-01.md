# Fresh independent implementation-plan audit 01

Date: 2026-10-16. Reviewer: independent audit agent, starting without the parent
conversation. Reviewed checkout HEAD: `d53a92cfef652b7aae5aca6472736c10e851db42`
plus the existing tracked and untracked working-tree changes.

**Overall verdict: CHANGES REQUESTED.** E04 needs one correction to its proposed
bounded IPC-wait design and associated validation before I approve its
decomposition. The other twelve epics are approved as implementation plans,
subject to their stated decisions, prerequisites and execution gates. These
verdicts do not approve implementation, service execution or publication.

I formed these verdicts without reading previous round reports or resolutions.
The existing `approved` fields and README approval claim were treated as claims,
not review evidence. I have not changed those fields or any plan/product content.

## Per-epic verdicts

| Epic | Verdict | Independent assessment |
| --- | --- | --- |
| E00 | **APPROVED** | Distinguishes historical B1/B2 evidence from a content-identified baseline, includes relevant untracked fixtures, explains skips, and requires the exact B3 real-service case to pass. Protects the existing IMDb deployment. |
| E01 | **APPROVED** | D01–D07 cover identity, API/CLI scope, persisted formats, ordering, platforms, imports/extras, Rust, images and publishing. Broader choices trigger replanning rather than silently enlarging stories. Resolve FA01 when approving the proposed cancellation design. |
| E02 | **APPROVED** | The explicitly incomplete scaffold avoids claiming that packaging metadata alone supplies a usable loader. Dependency authority, version source, sdist-safe behavior and development-only status are sensibly separated. |
| E03 | **APPROVED** | The cohesive namespace move includes import strings, test hooks, module/subprocess commands, examples, bootstrap and all existing Dockerfiles. It preserves wire/group identities and immediately validates imports inside rebuilt images. Two days is aggressive but plausible for a mechanical migration with the stated overflow/review rule. |
| E04 | **CHANGES REQUESTED** | Ownership, public errors, inert imports, cooperative inline execution and separate cleanup work are sound divisions. FA01 identifies an incomplete mechanism for the newly promised bounded process waits. Correct that mechanism and its negative-control tests before approving this epic. |
| E05 | **APPROVED** | Entry points, resources and writable state are concrete separate surfaces. Unrelated/read-only directories, mount ownership, permission failures and concurrent runs have meaningful checks. |
| E06 | **APPROVED** | Implements the chosen checkout-free strategy, includes compatibility/overrides and local image identity, and tests host/container addressing, daemon mounts, credentials/database selection and `--skip-image-build`. Publication remains separately gated. |
| E07 | **APPROVED** | Explicit runner, fixture/inventory freeze, bounded porting lanes and independent sdist-built-wheel checks cover the important import-masking risks. The inventory/overflow rule prevents silently dropping difficult regressions. See optional FA03 about the amount of discovery left to the freeze story. |
| E08 | **APPROVED** | Minimum/current paired dependencies, base/extras, architecture/interpreter/OS cells, source pickling/start methods and fork warnings have appropriate gates. Execution remains downstream of the corrected E04 contract. See optional FA02 about avoiding extra-dependency contamination in absence proofs. |
| E09 | **APPROVED** | A real owned-service adapter precedes supported-mode, DLQ, failure and ordering proofs. Positive-control lock-wait detection, graph/offset/ledger checks and known-counterexample labeling prevent several common false passes. O1–O8 have explicit owners and stronger guarantees require new work. |
| E10 | **APPROVED** | Installed examples, self-contained index documentation, support/guarantee statements, migration notes and a pre-approval recovery exercise cover the checklist. Public wording closes against verified behavior. |
| E11 | **APPROVED** | Clean version/source freeze, fresh package/image builds, final combined attestation, specific TestPyPI/image authorization and retrieved-install checks form a realistic candidate sequence. The exact bytes survive into production and development image evidence cannot substitute. |
| E12 | **APPROVED** | The user reviews concrete evidence and recovery material before production authorization; file/SCM tag timing is distinguished; retrieved public installation and release references are verified. Actual yanking/data migration requires a separate instruction. |

Approval of an epic's own decomposition does not bypass a failed prerequisite.
In particular, downstream acceptance remains blocked until E04's corrected
implementation and verification pass.

## Required correction

### FA01 — P1 / high: polling readiness does not bound receipt of a complete worker message

**Affected stories:** E04.F2.S3 and its cleanup interaction with E04.F2.S4.
D02 in E01.F1.S2 must approve the corrected contract; E08.F2.S1 should exercise
it on supported process transports.

**Plan references:** `cancellation-contract.md:33–38` and
`epics/e04.md:60–75` / the corresponding E04.F2.S3 fields in `backlog.json`.

The plan prescribes replacing unbounded `Pipe.recv()` waits with `poll()` slices
of at most 100 ms, and the proposed contract then promises bounded coordinator
waits. This misses a second blocking boundary. On the inspected Python 3.12.14
POSIX implementation, `Connection.poll()` indicates that input is available;
`Connection._recv_bytes()` subsequently reads the header and then calls
`_recv(size)`, whose blocking reads continue until the whole payload arrives.
A successful poll is not proof that the entire framed message is available.
Checking cancellation around an otherwise unchanged `recv()` can therefore
leave the parent stuck after the first bytes arrive.

This is relevant to the actual code, not just a hypothetical different IPC
protocol. `src/loader/edge_planner.py:1077–1078` sends whole result objects with
`connection.send(("ok", result))`; `degree_counter()` at line 782 returns a
NumPy array, candidate degrees are dictionaries, and emit returns the entire
chunk-spec list. The counter configuration in `edge_spill.py` permits 2^22
uint32 entries (approximately 16 MiB before framing). Those replies can span
multiple transport writes. A child stalled or suspended after starting such a
reply can leave a poll-then-recv parent waiting past cancellation or timeout.
The required blocked-`records()` test blocks before any reply is sent and does
not cover this failure mode. Ordinary interruptible IPC waiting is not the
contract's exclusion for OS-uninterruptible work.

**Required correction:** make the design explicitly cover complete-message
transfer, rather than equating readiness with completion. Select a bounded
framing/read strategy or an owned receiver arrangement that keeps the calling
thread responsive and can itself be shut down within the cleanup contract.
If helper receivers are used, specify their ownership and termination so the
fix does not leave a blocked helper behind. Audit later parent sends and stop
delivery for the same bounded-wait assumption. Keep the existing documented
initial submission/pickling exclusions precise instead of silently excluding
all IPC after the deadline begins. Update the canonical story and contract,
and split the story if the chosen transport work exceeds its estimate.

**Required validation:** add an externally bounded real-process test in which
a worker starts a reply larger than the pipe/socket buffer, exposes partial
frame readiness, then stalls before completing it. Exercise timeout and
cancellation through the approved public surface. Assert the expected public
error, elapsed bound, worker/receiver liveness, failure marker and retained or
reclaimed owned resources. Give the test its own supervisor deadline so a
regression cannot hang the test runner. Retain the existing pre-reply blocked
source, phase-clock and inline-clock tests; they check different boundaries.

This is a blocker for E04 plan approval, not a claim that cancellation has
already been implemented incorrectly: it has not been implemented yet.

## Optional improvements

### FA02 — P3 / low: make absence-proof environments explicit

**Affected story:** E08.F1.S1; `epics/e08.md:25–34`.

The acceptance criterion correctly requires base-only APIs to work without
optional packages. Preserve that as an actual absence proof when writing the
runner: the existing `tests/conftest.py:10–13` imports Docker and testcontainers
at collection time, and `requirements-dev.txt` installs testcontainers, which
can install Docker even if Docker becomes an optional runtime extra. A normal
development environment could therefore mask a missing lazy import or fallback.

**Suggested improvement:** name a minimal base-only subprocess/environment
smoke lane that installs no optional extra indirectly, asserts optional-package
absence, and exercises supported public imports/calls plus missing-extra errors.
Run the larger regression lanes separately with their test dependencies. This
is optional plan wording because the current acceptance criterion already
requires the correct outcome.

### FA03 — P3 / low: use the freeze story to re-estimate porting before execution

**Affected stories:** E07.F1.S3/S2/S4/S5 and `test-inventory.json`.

The initial inventory is intentionally not a frozen recursive fixture closure.
Read-only AST inspection found 253 test functions across the 12 core files,
688 across the 18 safety files, and 308 across the 12 CLI/runtime files, before
parametrization. For example, safety tests additionally import helpers from
`test_edge_execution_writer`, `test_multi_pair_leases` and `test_planned_writer`;
core/CLI tests read the social and IMDb schema YAML files outside the initial
closure list. E07.F1.S3 explicitly owns this discovery, so these omissions are
not a blocker or evidence that required resources will be omitted.

**Suggested improvement:** record staged file counts, collected test IDs,
fixture closure and a revised effort check at E07.F1.S3's exit. Use the existing
split/re-review rule before beginning any porting lane that no longer fits its
1–1.5 day estimate. The listed numerical sizes should be treated as current
inventory facts, not evidence that all tests have run or must be rewritten.

## Structural and requirements checks

- Loaded all canonical backlog data and all E00–E12 generated epic documents,
  README, decisions, cancellation contract, ordering matrix, test inventory,
  traceability and source checklist.
- Executed the existing renderer's validation with `Path.write_text` replaced
  by an in-memory capture. It reported **13 epics, 26 features, 53 stories,
  48 checklist items covered and acyclic dependency graphs**. All 14 generated
  file bodies matched their current files; none were rewritten.
- Confirmed the validator's current-test/reference/helper paths exist. The
  dependency direction checks pass and all story estimates are 0.5–2 days.
  Total estimated focused effort is 47 engineering days; external decisions,
  unavailable matrix environments and publication waits are excluded.
- `git diff --check` passed. No product tests, integration cases or release
  gates were executed in this audit.
- Source checklist requirements have mapped stories. Mapping alone was not
  treated as behavioral proof: the review checked the corresponding concrete
  acceptance/validation text, including B3, V5/V6/V7 and exact-artifact release
  sequencing. Original checklist requirements remain inherited; for example,
  U4's exclusion of development plans from `main` still applies as well as the
  explicitly repeated artifact exclusion.

The plan accurately describes the inspected starting state: the runtime is
the generic `src` package, pyproject has pytest/Ruff configuration without
distribution metadata, requirements are separate files, Docker builds use
the checkout, and no installed public distribution or cancellation API exists.
The working tree contains the prerequisite fixes and untracked maintained
fixture/preflight tests; E00 is right to bind evidence to their contents rather
than treating HEAD or earlier test counts as sufficient identity.

## Inspection scope and verification limits

No applicable `AGENTS.md` was found in the repository's ancestor paths; a
repository inventory found none below it. Product/config/test inspection was
targeted rather than an assertion that every line of the large application was
audited. It included:

- `pyproject.toml`, requirements files, Dockerfile and three component
  Dockerfiles, `.dockerignore`, `.gitignore`, Makefile and relevant executable
  references;
- schema loader/model surfaces, planner settings/source interfaces, worker
  startup/send/receive/cleanup, convenience-topic ownership, result processing,
  planned orchestration, Docker launch/image/mount paths, metrics/version
  behavior, CLI/component entry and process-control boundaries;
- retry resolution-memory behavior, fencing assumptions, public-preflight and
  external-planner tests, shared conftest, maintained arrival fixture references,
  the exact B3 integration case, installed-lane helper imports, Docker-build,
  streaming/planned harness and positive-control lock-wait test excerpts;
- the local Python standard-library implementation of `Connection.poll`,
  `_poll`, `_recv_bytes` and `_recv`, inspected with `inspect.getsource` to
  establish FA01 without starting a service or executing an implementation story.

I did not inspect prior review findings, run the claimed 3,366/406 baselines,
execute B3, build/install artifacts, run containers, access the remote IMDb
deployment, inspect credentials/accounts or verify external package-name/image
availability. No web access, service connection, upload, push or account change
was performed. Only this requested audit report was created. Historical pass
counts, proposed support promises and existing approval labels remain
unverified until their owning execution gates supply evidence.
