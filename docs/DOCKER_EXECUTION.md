# Checkout-free Docker execution

This page describes the default `--runtime docker`: one container per role. To
run the roles as processes of the command instead (no Docker daemon, no images),
see [Process runtime](PROCESS_RUNTIME.md); image options and
`GRAPH_LOADER_*_IMAGE` do not apply there. To run all roles inside one CLI-image
container, see [Single container](SINGLE_CONTAINER.md).

`graph-loader start` resolves prebuilt images and never builds from cwd. Defaults
are `ghcr.io/neburyx/graph-loader-{node,edge,clock}:<package-version>`;
the current package version is `0.1.0a2`. The CLI image follows
the same naming policy.

**Your own registry.** (Behind a network without internet access: [Restricted networks](RESTRICTED_NETWORKS.md).) Set `GRAPH_LOADER_IMAGE_REGISTRY=<prefix>` (for example `registry.example.com:5000/team`) and every
default reference, `cli` included, becomes `<prefix>/graph-loader-<role>:<package-version>`; copy the four images there first.
The prefix is `host[:port]` and lowercase path components only: a scheme, credentials, a tag or digest, spaces, an empty
component or a trailing slash is refused before anything starts, and the message never repeats the value. The per-role
variables still win for their role, and an image reference that carries credentials (`user:password@host/image`) is refused:
sign in with `docker login` instead. `--runtime process` runs no images and ignores the setting. These are the registry identities used at release; a local
candidate build does not establish publication or anonymous-pull availability.

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

Development maintainers explicitly build from the checkout (Docker with buildx; the dependency lock of the daemon's platform is used, the
dependencies come from the public PyPI unless `CANDIDATE_WHEELHOUSE=<dir>` or `CANDIDATE_INDEX=<url>` names another source):

```sh
make build-images CANDIDATE_SUFFIX=my-fresh-candidate
. var/candidate-images-my-fresh-candidate/image-overrides.sh
```

The new `var/candidate-images-my-fresh-candidate/images.json` records all four
local IDs, the package wheel and the dependency lock they were built from (file names and sha256), the Dockerfile and install-helper
hashes, the resolved base image digest, the build arguments without secrets, the Debian package versions of the CLI image,
labels/platform/entrypoints and proposed registry tags. To build images for your own registry from the installed package instead, see
[`graph-loader images build`](CLI.md#images-build). Existing candidate tags/output directories are refused. The generated public
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

These paths are exercised with Neo4j 5.26 Enterprise and Kafka 3.7
(`cp-kafka:7.7.0`) on Linux amd64; what is and is not covered is listed in
[Support and limits](../README.md#support-and-limits).
