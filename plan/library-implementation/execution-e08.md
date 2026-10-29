# E08 execution plan — 2026-10-26

**Plan prepared before implementation; independently approved after the worker-bound
amendment in [plan review](reviews/e08-plan-review.md).** Execute
E08 only. User supplied the E08 next-agent handoff as the current assignment.
Clean entry is `4b246f3ab953bd9f7d266a682ac9fff39bd32032` on
`codex/pypi-library-checklist`. Latest accepted E07 repair is
`2927a847c6f27f86086191b87773af4edae4026a`, with307-input committed/tested/current
equality and unchanged environment. Preserve all original and revalidation
proof, including the five new interruption/forkpty checkout controls. No applicable
AGENTS.md was found. No E09–E12 execution, push, upload, Git tag, publication or workflow.

## Decisions and exact exit boundaries

[D01–D07](decisions.md), [ordering](ordering-matrix.md),
[cancellation](cancellation-contract.md) and the current E07 inventory govern.
D05 makes all eight dependencies mandatory: no extras, optional Rust absence,
pure-driver fallback or missing-extra API is offered. Interpret E08's generic
conditional extra/fallback clauses as not applicable; clarify the canonical
story wording without changing that decision. Keep version0.1.0a1.dev0, target
0.1.0a1, root version-only exports, no src alias, two resources and Neo4j/Rust
less-than-7 fence. A necessary dependency-floor correction requires actual red
resolution/install evidence and independent review before changing metadata.

D04 targets CPython3.12, Linux arm64 and amd64. Record exact Python patch, kernel,
image, daemon architecture and execution mode per cell. The present Docker daemon
is Linux/aarch64 (18CPUs/25GB); buildx also lists emulated amd64. Native Linux
amd64 access is unavailable; the user explicitly requested keeping that proof
open on2026-10-27. Emulated observations may inform
compatibility, but must be labeled and cannot silently close a native timing/
transport cell. Missing native infrastructure remains an open release blocker,
never a skip/pass or an invented remote machine. Prepare a portable finite
runner bundle so a subsequently authorized native runner can execute identical
inputs. No change to D04's advertised platforms is inferred from infrastructure.

On2026-10-28 the user supplied SSH access to the existing machine at
`shashank@192.168.1.75` for the missing runner. Read-only discovery confirms
Linux/x86_64 host and Docker daemon,12CPUs and about15GB daemon memory. This
supersedes the earlier unavailable-runner observation, not the ban on IMDb work.
Use SSH/SCP only for finite E08 runner inputs, owned private directories and
new labeled test containers/images; never inspect or contact existing IMDb
services/data, load private environment files, change global Docker configuration
or prune/remove foreign resources. Keep credentials out of commands, files and
evidence. The explicit local Docker socket is per-controller, with an isolated
HOME; no persistent context rewrite. Reproduce the frozen minimum/current locks
under the actual native amd64 interpreter/image, retain complete raw proof and
obtain fresh independent S1 acceptance before dependent S2 work. No publication,
push, Git tag or workflow is authorized by remote runner access.

After both machines restarted on2026-10-28, the prior controller/watchdog were
absent. Retain the interrupted attempt and verify the recorded CID/image/owner/
creation identity before cleaning its stopped container. The prior standalone
resource probe also exposes Docker Desktop shared-folder chmod semantics: the
original assertion correctly rejects the still-writable package. Keep every
probe unchanged and execute new cells in unique labeled native Linux volumes,
with finite snapshots exported to an owned host path. Retain each volume until
export verification or recovery; timeouts/container removal must not erase raw
proof. A deliberately interrupted nonroot control confirms chmod rejection and
recovery after container removal. [Independent adapter review](reviews/e08-native-workspace-review-20261028.md)
accepts this bounded repair; actual cells/global S1 acceptance remain pending.

Process support targets the already approved default spawn plus cooperative
inline; do not advertise new start methods as a side effect of this work.
Compatibility observations for other available contexts remain explicitly
unadvertised unless a revised support decision is obtained. Validate normal
planning for worker counts1–8 plus16,32,64 and full-failure cleanup at1,2,4,8,64.
The existing CLI documents and validates1–64;64 is the required maximum, not8.
Keep untested interior counts explicit rather than claiming exhaustive timing
proof. The API accepts positive integer counts without a hard upper bound;
describe measured counts separately from accepted settings. Publish an
evidence-backed support envelope only after the cells pass; do not impose a new
constructor limit or claim arbitrary larger worker counts. Preserve default4,
the existing1–64 CLI range and accepted settings/serialized formats. The initial
proposed8-worker envelope was rejected during plan review before implementation.

## Entry and discovery, before source edits

Create a new CPython3.12 editable development environment under a new owned E08
output tree, install declared development requirements, pip-check and capture
safe tracked+relevant-untracked inputs/environment. Do not reuse an E07 env as
E08 development proof. Entry has307 inputs; initial new environment resolves
current compatible packages and has its own recorded identity. Capture public
PyPI version/Requires-Dist/wheel metadata with URLs/hashes and actual resolver
reports, never private configuration. A new base-only throwaway discovery env
may attempt the declared minimum set to retain real failure evidence before
floor decisions. These discovery scripts/output are not implementation or a
claimed successful matrix.

Minimum direct targets start from approved floors: Neo4j5.20.0 paired with the
published matching Rust extension5.20.0.0, Kafka2.5.0, Pydantic2.7.0, PyYAML6.0,
Docker7.1.0, NumPy1.26.0, Prometheus0.20.0. Resolve them jointly, not independently
minimized incompatible transitives. Latest current metadata is a dated discovery
input; the actual coherent current install/report determines the pinned lane.
Universal Python wheels count as compatible; absence of a cp312-specific filename
alone is not a failure. Record source-built pure-Python dependencies explicitly.
PyYAML6.0 has no cp312 wheel in the upstream release; test its actual default
isolated build/install before proposing a correction. No floor is changed merely
to avoid recording an unavailable wheel or source build.

Discovery retained two independently reviewed corrections before metadata edits:
[PyYAML6.0.1](reviews/e08-dependency-floor-review.md) installs on CPython3.12;
[Neo4j6.0.0/Rust6.0.0.0](reviews/e08-driver-floor-review.md) supplies the existing
typed connection-pool errors absent from the inspected5.x line. The original
5.20 installed application imports fail; generic ClientError aliases or empty
placeholder classes would not preserve that taxonomy. The corrected minimum
keeps all other five direct floors unchanged. New exact constraints, artifacts
and every target lane must be rebuilt/retested; discovery successes are not
story or matrix acceptance.

Primary references: [Neo4j Rust pairing](https://pypi.org/project/neo4j-rust-ext/),
[PyYAML6.0 files](https://pypi.org/project/PyYAML/6.0/), and
[CPython3.12 contexts](https://docs.python.org/3.12/library/multiprocessing.html#contexts-and-start-methods).
Rust's patch segment follows the exact driver version. Python warns about fork
from a multithreaded parent; explicit private contexts and guarded importable
entry points are required. Use actual indexed metadata and execution for claims.

## Ordered story E08.F1.S1 — dependency and architecture cells

1. Add finite reviewed minimum/current constraints/lock inputs, actual dependency
   and upstream artifact reports, a maintained support-matrix runner and standalone
   public probes. No optional groups are invented. If declared minima fail,
   retain the original error, propose the smallest justified floor correction,
   independently review it before changing pyproject/requirements, then rebuild
   strict artifacts and refresh the immutable artifact inventory explicitly.
2. Extend the checkout installed runner only as needed to accept explicit strict
   constraints and a finite recorded wheelhouse. Preserve its prescribed existing
   interface/default behavior and every E07 repair. Reject requirement-file
   directives/URLs/path injection or unsafe wheelhouse members. Record constraint
   and wheelhouse bytes, actual resolution/install reports and versions. Apply
   constraints to application AND test-tool installation; tools must not silently
   upgrade the base lane. No global environment rewrite. Unit tests target the
   actual parsing/install/receipt contracts, not duplicate implementation details.
3. Use new owned test-only platform harness images/containers with exact observed
   upstream base IDs/digests. Finite contexts contain only public wheels,
   maintained runner/probe/test closure and explicit locks, never src, private
   files, Git or datasets. This is matrix tooling, not a speculative shipped Docker
   build context or a change to D06 production delivery. Run as an unprivileged
   user. Build/download dependencies in separate owned preparation steps; execute
   probes/regressions offline where practical. Record source-built wheel inputs.
4. For each available native architecture and minimum/current lane, create NEW
   bare noneditable venvs without system-site/user/developer/test packages. Use
   plain isolated client scripts, never shared conftest, before test-rich lanes.
   Prove all eight runtime distributions/constraints and Neo4j/Rust pairing with
   active accelerator; assert pytest/testcontainers/pandas/dotenv/build/twine/
   psutil authoring tools are absent. Verify all application module/resource bytes,
   public schema/producer planner examples, imports and CLI/help/version/parser.
   No real Kafka or full service promise follows from a producer fake.
5. Run the relevant full eight frozen installed lanes separately in fresh target
   environments for each tested minimum/current platform cell. Preserve exact
   full/selected IDs, explicit exclusions, stage/assertion baselines, actual
   parent/child origins and PID/ctime/startup/post-exec receipts. Additive E08
   authoring tests are classified honestly; no difficult runtime ID is dropped.
6. Per-process external hard deadlines and CID/label/creation-bound emergency
   cleanup own only new resources. Retain interruptions, original causes and
   malformed receipts. Review every failure, repair/retest/re-review actionable
   issues. A fresh zero-history reviewer accepts S1 before S2.

## Ordered story E08.F2.S1 — process/transport/warning cells

1. Add guarded importable installed external-source clients for spawn and inline,
   picklable success, clearly non-picklable failure, source errors and real worker
   failure. Verify caller-selected global method is unchanged. Capture actual
   worker identities/contexts, deterministic plans/chunks/spill output and scope.
   Exercise the recorded worker envelope without changing E04 semantics.
2. Reuse and parameterize maintained supervised transport/public fault fixtures
   only through explicit context/worker inputs with old defaults/IDs/assertions
   retained. At representative and maximum workers exercise partial headers/body,
   read backpressure, later-command/stop write backpressure, cancellation/timeout,
   operation-wide phase clocks, failure markers, liveness, endpoints, locks and
   spill ownership. Preserve the aggregate10-second cleanup and original hard
   supervisor. Do not replace framed reads with Connection.poll/readiness alone.
   Include boundary codec/resource checks under the actual target interpreter.
3. Reproduce the19 existing project-owned multithreaded-fork warnings before
   repair. Investigate the example subset's explicit fork closures and explicit
   fork test context. Prefer a bounded refactor to importable/picklable task and
   selector descriptions with an explicit safe spawn context, preserving output
   bytes, chunking, dedup/collision behavior, source-failure causes, original test
   IDs and assertions. Pass necessary per-call configuration to workers; do not
   rely on inherited patched globals or add cloudpickle/runtime dependencies.
   If the original injected local failure needs an importable fixture, retain
   its actual intended worker error assertion and add controls proving it is
   reached. Record copying/serialization/performance limits; no real IMDb run.
   No thread-based silent change to the public planner or receiver helpers.
   Any larger warning fix requires a bounded amended plan/review before code.
4. Keep other contexts explicitly unadvertised or test documented restrictions;
   never suppress warnings, globally set the embedding method, skip hard cases
   or weaken original timing assertions to obtain green output. Report every
   unavailable native/platform/transport cell as open. Independently review S2.

## Full final retention and delivery

After actual ordered acceptance, freeze safe current inputs/environment, execute
full checkout/example suite with private dotenv/remote-IMDb audit installed before
pytest and a main-guarded external supervisor. Full lint/requirements parity/pip/
whitespace; original and rebuilt strict wheel/sdist outside Git/member equality;
two new bare installs; all eight installed lanes for both final wheels; four
fresh immutable production images with real read-only/no-source-mount probes;
all12 separately hard-supervised uninstrumented R1 controls; exact no-skip B3;
and actual installed tiny Neo4j5.26Enterprise/Kafka7.7 service proof remain gates.
Use new owned outputs/tags/services, retain all previous E07/E08 red and superseded
proof. Explain actual skips/warnings, never substitute historical totals or IMDb.

A fresh zero-history final adversarial reviewer accepts the exact implementation,
matrix scope and complete proof. E08 cannot be complete while an advertised
native cell or required infrastructure remains unproved. Then render plan/check
links/inspect staging/commit locally; record full actual SHA and every tested/
committed/current input byte/size/Git mode plus unchanged environment in a doc-only
follow-up, obtaining that follow-up identity from Git history without recursion.
Prepare only a conditioned E09 handoff. No E09–E12 or publication execution.

## S2 remaining harness detail, prepared2026-10-28

S1 is [globally accepted](reviews/e08-s1-global-review-20261028.md). The warning
repair and initial external clients have separate focused reviews; these do not
accept S2. [Remaining matrix design](e08-process-matrix-design.md) records the
reviewed opt-in harness, case provenance/restoration, prepared-vs-cold clock
limits, packed-send payload sizing and aggregate private cleanup before code.
No new worker cap or unsupported process context is advertised.

## Revalidation continuation authorization,2026-10-28

The independent matrix revalidation remains historical evidence of two open
completion blockers. Its Linux prohibition described that read-only review.
The user subsequently answered the scope question: “Resume implementation and
required Linux validation.” This explicitly authorizes the remaining S2 and
required final native runs under the existing owned-resource boundaries. It
does not authorize existing IMDb service/data access, global Docker changes,
push, tag, upload, publication or later epics. S1 results remain historical
snapshots; current S2/final proof must be obtained without substituting them.
