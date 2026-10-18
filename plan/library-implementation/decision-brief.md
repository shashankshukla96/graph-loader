# D01–D07 approval brief — 2026-10-16

Status updated 2026-10-16: D01–D07 are resolved. The user approved D01–D05
and D07 policy exactly as briefed and selected recommended prebuilt images.
Following “finish the rest”, delegated completion selects public release-image
namespace `ghcr.io/shashankshukla96`; **Shashank Shukla** is public maintainer,
index/image owner and release owner/operator, contact shashankshukla96@gmail.com.
See [decision register](decisions.md), [direct replies](evidence/e01-user-decisions-20261016.md)
and [independent completion checkpoints](reviews/e01-completion-checkpoints.md).
The original proposal text below remains approval context; actual account access,
image visibility/digests and publication require their later evidence/approvals.
Work remains bounded to E00/E01.

## D01: names and accountable owner

Recommend distribution `neo4j-graph-loader`, import `neo4j_graph_loader`, command
`graph-loader`. Both PyPI and TestPyPI JSON endpoints returned HTTP 404 on
2026-10-16 from direct HTTPS requests; neither local command `graph-loader` nor
`neo4j-graph-loader` exists on PATH. This does not reserve a name or establish
authorization/trademark rights. Per [PyPA normalization](https://packaging.python.org/en/latest/specifications/name-normalization/),
case and runs of `-_.` normalize to the same name, so these spelling variants
are not separate projects. Endpoints:
[PyPI](https://pypi.org/pypi/neo4j-graph-loader/json),
[TestPyPI](https://test.pypi.org/pypi/neo4j-graph-loader/json).
Need the authorized public owner/maintainer name and public contact (email or
public contact URL), plus who will own the index projects. No local Git email
or account credentials have been used. Registry/owner information must be public.

## D02: explicit alpha scope and limits

Propose these public modules under the chosen import namespace; names here are
the future supported surface, not already available imports:

| Module / exports | Inputs and results / errors |
| --- | --- |
| `schema`: `load_schema`, `GraphSchema`, `LoadingConfig`, `NodeConfig`, `EdgeConfig`, `PropertyConfig`, `SourceTargetConfig`, `DeadLetterConfig`, `RetryConfig`, `SchemaLoadError`, `MultiTopicEdgeError`, `MultiPairEdgeError` | `load_schema(str or os.PathLike) -> GraphSchema`; YAML/read/decode/model failures become `SchemaLoadError` with cause; direct model validation raises Pydantic `ValidationError` |
| `planning`: `PlanSettings`, `EdgeRecordSource`, `IterableEdgeSource`, `ParallelEdgePlanner`, `PlanResult`, `EdgePlan`, `producer_edge_planner`, `plan_and_publish_edges`, `EdgePlanError`, `EdgePlanInputError` | Source mapping type name -> iterable, or picklable `EdgeRecordSource` sequence; `PlanSettings`, broker string, plan path; `producer_edge_planner -> ParallelEdgePlanner`; `finish`/convenience -> `PlanResult` containing plan/digest/path/timings; construction `ValueError`, input/plan errors, ordinary source/client causes preserved |
| New cancellation errors in `planning`: `EdgePlanCancelledError`, `EdgePlanTimeoutError` under `EdgePlanError` | Keyword-only `cancel_event=None`, `timeout_seconds=None`; one-shot planner, no concurrent lifecycle calls; cancellation/deadline contract below |
| Installed CLI | Existing `start` bulk (planned/default and classic) and stream, `stop`, `status`, `replay-dlq`; retain operational behavior and exit codes. Embedded ingestion, fleet, retry, index, metrics and worker internals have no supported Python API. |

Approve [cancellation-contract.md](cancellation-contract.md) **in full**: parent
observes caller signal, one operation-wide monotonic deadline (process anchor
after initial submission; inline before first record), <=100 ms wait slices,
nonblocking complete-frame reads AND writes, explicit frame/resource/codec
limits and before/after codec checks, no hidden arbitrary codec callbacks or
receiver helpers. Proposed cleanup budget is 10 seconds across all workers,
plus <=100 ms observation and documented scheduling slack. Normal children are
reaped; OS-unreapable children/resources must be reported and ownership retained.
Construction/pickling, Kafka/admin/client callbacks, blocked inline `next()` and
OS-uninterruptible work keep their explicitly documented limits. E04 must prove
the behavior; it is not implemented here. Maximum workers require E08 evidence.

Approve [ordering-matrix.md](ordering-matrix.md) O1–O8's restricted dispositions:
no universal arrival order across restart, rebalance, 100,000-entry resolution
LRU eviction, topics/partitions, replicas or manual replay. Stop fleet/retry
before topic recreation. `order_by` is a conditional property-version rule,
not exactly once: comparable schema-validated non-null versions, unique endpoint
keys, no external writers violating the rule, and replay retaining the property.
Only newer versions may replace older ones; equal versions do not establish a
deterministic cross-source winner, and missing/null versions carry no newest-value
promise. Current equal/missing handling must be captured and proved in E09 before
advertising any stronger rule. Known counterexamples are limitations, not safety
passes. Producer planning publishes Kafka chunks and requires Kafka.

## D03: alpha and compatibility

Propose initial target `0.1.0a1` (candidate frozen only in E11), no generic `src`
compatibility shim, and release notes for every alpha API change. Deprecated
public Python names/CLI flags get a warning and remain for at least one subsequent
alpha before removal, except a documented urgent correctness/security fix.
Existing CLI aliases retain their existing warning behavior. No stable 1.0 API
promise is made during alpha.

| Stored/wire surface | Proposed compatibility policy / current source |
| --- | --- |
| Schema YAML | Preserve existing fields/defaults/validation; additive changes documented; incompatible meaning requires explicit migration (`models/schema.py`) |
| Plan JSON | Retain `graph-loader-edge-plan/1`; incompatible changes require a new format and rejection/migration policy (`loader/edge_plan.py`) |
| Kafka chunk | Retain format `1`, resource/key/chunk encodings and chain identities; incompatible changes versioned (`loader/edge_plan.py`) |
| DLQ envelopes | Continue decoding v1 and v2, emit v2; preserve existing promise that stored old versions remain decodable (`loader/dead_letter.py`) |
| Parked-pair index | Preserve keys, labels/source identity, sequencing and tombstones; no implicit reset or topic renaming (`loader/parked_pair_index.py`) |
| Consumer groups | Keep stable stream `<prefix>-<type>`, `<prefix>-retry-consumer`, run-scoped bulk `<prefix>-<type>-<run_id>`, and explicit legacy migration (`utils/consumer_groups.py`) |

## D04: support targets, conditional on executed evidence

Propose CPython 3.12; Linux arm64 and amd64 for schema/planner and all CLI/container
operations, each gated by E08/E09. Current macOS arm64 checkout execution is
development evidence only; macOS/Windows and other Python releases are unadvertised
until separately tested. `requires-python >=3.12`, no speculative upper cap.
Use spawn/process and cooperative inline; every advertised start method/maximum
worker setting requires its own transport/cleanup proof.

Server proof targets: Neo4j **5.26 Enterprise**, Kafka broker **3.7** via
`confluentinc/cp-kafka:7.7.0` (pin actual harness image/version in evidence).
Current Cypher uses scoped subqueries (code/docs minimum 5.23); 5.23 is not
advertised as tested by this slice. APOC is required for classic execution;
lease/planned modes use native writes. Enterprise is needed for query-log and
concurrency proofs; do not claim Community validation. E00's default testcontainers
Kafka image may differ from this future support target and must be recorded.

## D05: dependency authority and Rust

Propose initially keeping all eight runtime dependencies mandatory; no extras
until separation/absence tests justify them. Rust extension remains mandatory
for the first alpha (smallest change to current behavior); matching Neo4j driver
must resolve, pure-driver support is unadvertised until tested. This avoids
inventing an unproved optional fallback. [Extension documentation](https://pypi.org/project/neo4j-rust-ext/)
requires a matching driver; unsupported wheel platforms need Rust/source tools.
The Python code uses the normal driver, so optional acceleration is a later
choice requiring separation and paired tests.

| Dependency | Current allowed range | Proposed minimum lane / current observed lane | Eager import path |
| --- | --- | --- | --- |
| neo4j | >=5.20,<7 | 5.20.0 / 6.3.1 | CLI -> schema initializer; writers |
| neo4j-rust-ext | >=5.20,<7 | compatible 5.20.x extension / 6.3.1.0 | driver discovers installed accelerator; paired resolver constraint |
| confluent-kafka | >=2.5,<3 | 2.5.0 / 2.15.1 | CLI -> DLQ/lease orchestration; planner factory imports |
| pydantic | >=2.7,<3 | 2.7.0 / 2.13.5 | schema -> models |
| PyYAML | >=6.0,<7 | 6.0 / 6.0.3 | schema loader |
| docker | >=7.1.0,<8 | 7.1.0 / 7.2.0 | CLI -> docker service |
| numpy | >=1.26,<3 | 1.26.0 / 2.5.3 | planner -> NumPy/spill |
| prometheus-client | >=0.20,<1 | 0.20.0 / 0.26.0 | planner -> metrics |

The schema import closure is currently YAML/Pydantic/control; planner also
eagerly requires NumPy/Prometheus. CLI crosses Docker/Kafka/Neo4j as well. Retaining
the union as base is deliberate. The minimum extension patch must be chosen
from published paired wheels during E08; failed minimum resolution raises the
floor through an explicitly reviewed constraint correction, not a claimed pass.
Current lane is observed local versions, not proof of every supported cell.
Dev requirements include runtime, pytest/testcontainers/Ruff and IMDb's
pandas/python-dotenv; examples remain outside production base.

## D06: delivery choice and image responsibility

Recommended: matching versioned prebuilt CLI/node/edge/clock images in a
user-owned registry namespace; immutable release references/digests recorded
after separately authorized push. Explicit image overrides are allowed with
version compatibility checks; no implicit checkout builds. Need the public
registry namespace and authorized image owner. Alternative: bundle a versioned
build context for checkout-free local builds (no registry owner needed); same
component version/compatibility and E06/E09/E11 validation gates apply. Choosing
either path authorizes no push.

## D07: build and publication

Recommend setuptools (`setuptools.build_meta`, modern PEP 639 support), a single
file-based version source read by metadata/API/CLI/metrics/images, manual build
and Twine upload with credentials kept external. See [setuptools configuration](https://setuptools.pypa.io/en/latest/userguide/pyproject_config.html).
No new workflows; T3 N/A for manual publication. E11 freezes a clean candidate
and exact wheel/sdist hashes before explicit TestPyPI/image instructions; E12
requires separate production/index/Git-push approval and creates the file-based
release tag at the tested commit. Need the authorized public release owner;
the owner may be the same person/organization as D01.
