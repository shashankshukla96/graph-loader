# Checkout-free Docker execution

`graph-loader start` resolves prebuilt images and never builds from cwd. Defaults
are `ghcr.io/shashankshukla96/graph-loader-{node,edge,clock}:<package-version>`;
the current development package version is `0.1.0a1.dev0`. The CLI image follows
the same naming policy. These names are planned registry identities: local E06
candidates do not establish publication or anonymous-pull availability.

Resolution inspects the local daemon first. Only an absent reference is pulled;
an incompatible local image fails immediately. `--skip-image-build` keeps its
existing spelling and requires preloaded images, with no pull or build. A missing
local image, denied pull, incompatible metadata or unavailable daemon returns a
clear failure. Installation, imports and schema/help commands start no services.
The operator owns Kafka, Neo4j, Docker network and image preload/pull access.

`GRAPH_LOADER_NODE_IMAGE`, `GRAPH_LOADER_EDGE_IMAGE` and
`GRAPH_LOADER_CLOCK_IMAGE` select trusted operator overrides (tag, digest or local
ID). Every selected image, including overrides, must have these exact labels:

| Label | Required value |
| --- | --- |
| `org.opencontainers.image.version` | Installed CLI package version, including development suffix |
| `io.graph-loader.component` | `node`, `edge` or `clock`, matching the requested role |
| `io.graph-loader.image-contract` | `1` |

The contract means the matching package and existing entrypoint/mount/environment
conventions. It does not authenticate arbitrary operator images; the operator is
responsible for their provenance. No unlabeled/version-incompatible bypass is
provided. Each DockerService snapshots references and pins selected immutable
local IDs for its lifetime; retagging or changing environment settings cannot
change later replica/coordinator/retry launches in that service.

Development maintainers explicitly build from the checkout:

```sh
make build-images CANDIDATE_SUFFIX=my-fresh-candidate
. var/candidate-images-my-fresh-candidate/image-overrides.sh
```

The new `var/candidate-images-my-fresh-candidate/images.json` records all four
local IDs, exact input hashes, labels/platform/entrypoints and proposed registry
tags. Existing candidate tags/output directories are refused. The generated public
`image-overrides.sh` exports the manifest references; source it explicitly, then run the
installed CLI anywhere with `--skip-image-build`. The builder derives the version
from the single file authority and checks installed metadata during Docker build.
Ad-hoc Dockerfile builds without the version argument have an empty version label
and cannot be selected. Nothing here pushes or publishes an image. Registry
publication and immutable registry digests require a separate release operation.

Runtime state and mount ownership are described in [CLI runtime](CLI_RUNTIME.md).
The supported topology uses a local Docker socket/Desktop daemon and an existing
user-defined bridge network (`--network`). The CLI runs on the host. It reaches
Neo4j/Kafka through published host addresses; containers use their own network
DNS names. Set both sides explicitly when they differ:

| Setting | Where it must be reachable |
| --- | --- |
| `NEO4J_URI` | CLI host (`bolt://localhost:<published-port>` is typical) |
| `LOADER_NEO4J_URI` | Loader network (`bolt://neo4j:7687` is typical) |
| `KAFKA_BOOTSTRAP_SERVERS` | CLI host; advertised host-listener metadata must also be reachable |
| `LOADER_KAFKA_BOOTSTRAP_SERVERS` | Loader network; every advertised broker address must be reachable there |
| `NEO4J_USERNAME` / `NEO4J_USER`, `NEO4J_PASSWORD` | Explicit credentials passed to host/container drivers |
| `NEO4J_DATABASE` | Existing database selected consistently by host and container sessions |

A container's `localhost` refers to that container. Docker Desktop does not make
container bridge IPs or user-defined network DNS available on the host. For a
host-provided service, use a reachable host gateway address for containers (Docker
Desktop commonly provides `host.docker.internal`); Linux host-gateway setup is
operator configuration. Kafka must advertise addresses for the listener each
client uses, not just accept an initial bootstrap connection. TCP connectivity to
a bootstrap alone is insufficient.

Before DDL, Kafka provisioning or planning, supported `main` runs check network,
container Neo4j connectivity/database/credentials and Kafka metadata reachability
in a temporary selected-image probe. Mount checks compare schema bytes and unique
host markers, and confirm container writes return to the same rejection/timing
paths. The probe retains its created container before attempting start, and tracks
each exclusively opened marker before writing. Cleanup failures prevent success.
The probe has a read-only rootfs, uses strict bind mounts (missing daemon
paths are not created), has a bounded wait and removes only its own container and
markers. Diagnostics report safe phase names without driver/daemon bodies,
credentials or service URIs. Failures exit1 and leave the private attempt for
inspection; no ingestion fleet is launched. Host-side initialization and planner
also verify their respective service paths during normal execution.

Schema and edge-plan inputs are bound read-only at `/app/runtime_schema.yaml`
and `/app/edge-plan.json`. Rejection/timing outputs are read-write at
`/app/rejections` and `/app/write-timings`. The planner creates plan, progress,
lock and spill files on the CLI host. The CLI persists the verified completion
ledger beside the host plan; ledger/spill directories are not mounted into
writer containers. Preflight verifies their daemon-visible directory identity,
while the existing host output preflight checks actual host write permission.
An external `--edge-plan` checks its actual parent (including a future input),
and an existing input is additionally mounted read-only and compared by hash.
Per-role markers allow explicit output directories to share a parent safely.
Explicit plan/spill/timing overrides retain their existing ownership rules.

Dispatcher and lease coordinators publish ephemeral control ports on host
loopback. Host control/readiness clients use those published ports. Writers and
retry consumers use network addresses; streaming lease users use a stable DNS
name across coordinator restart. These are distinct paths. A local daemon
and published host-loopback ports are required for this topology.

All bind sources are absolute host paths that the daemon must see. On Docker
Desktop, put runtime/source inputs in a shared host directory; `--run-dir BASE`
selects a suitable shared parent. Remote TCP/SSH daemons, remote path translation,
Docker host/default-bridge networks and automatic network/infrastructure creation
are unsupported by installed orchestration and fail explicitly. Use an external
orchestrator for another topology. No remote filesystem/port proxy is implied.

E06's tiny owned installed-wheel proof exercises these paths with Neo4j5.26
Enterprise and Kafka3.7/cp-kafka7.7.0. It does not establish E08's wider platform
matrix or E09's full service/ordering guarantees. Publication remains separate.
