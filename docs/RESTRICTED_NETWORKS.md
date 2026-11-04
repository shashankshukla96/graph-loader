# Restricted networks

How to run the loader where the hosts that run it cannot reach the public internet: no `ghcr.io`, no `pypi.org`, no `deb.debian.org`, no
Docker Hub. The loader itself never needs the internet; only getting its **software** into the network does. There are three routes, and you
can mix them. This is an alpha (`0.1.0a2`): [what was tested and what was not](#what-was-tested) is at the end.

| Route | You run the loader as | You bring into the network | Needs |
| --- | --- | --- | --- |
| [1. Process runtime from your PyPI mirror](#route-1-the-process-runtime-from-your-pypi-mirror) | processes of one `graph-loader start --runtime process` command | the package and its locked dependencies, as wheels on your index | Linux, CPython 3.12, no Docker |
| [2. Copy the released images](#route-2-copy-the-released-images-by-digest) | containers (`--runtime docker`) | the four released images, by digest | Docker; a registry you control |
| [3. Build the images yourself](#route-3-build-the-images-with-graph-loader-images-build) | containers (`--runtime docker`) | a base image, and either a PyPI mirror or a wheelhouse | Docker with `buildx`; a registry you control |

Whatever the route, **you provide Neo4j, Kafka and (for routes 2 and 3) the Docker daemon**; the loader never creates or removes them
([Quick start](QUICKSTART.md#what-you-need)). Credentials never go on a command line, in a build argument or in an image: every option below takes a
file or an environment variable, or reads your Docker login.

## Route 1: the process runtime from your PyPI mirror

The [process runtime](PROCESS_RUNTIME.md) runs every role as a child process of one `graph-loader start` command, so nothing but the Python
package has to enter the network.

1. **Put the package and its dependencies on your index.** The package is `neo4j-graph-loader`. The exact dependency set the project tests with
   is the hashed lock that ships inside the package, one file per platform:

   ```bash
   python3.12 -c "from importlib.resources import files; print(files('neo4j_graph_loader.image_build') / 'locks' / 'py312-linux-amd64.txt')"
   ```

   Mirror every wheel that file lists (each line is `name==version` with the `sha256` of one wheel) and the package's own wheel. Use the lock of
   your platform (`linux-amd64` or `linux-arm64`); both ship with the package. All dependencies are binary wheels, so the index needs no
   compiler or source distribution.
2. **Install from the mirror.** Keep the credentials out of the command line (pip reads `PIP_INDEX_URL` from the environment) and trust your private
   CA with `PIP_CERT` when the index uses one:

   ```bash
   python3.12 -m venv .venv && . .venv/bin/activate
   export PIP_INDEX_URL="$(cat pip-index-url.txt)"     # one line: your index URL, with its user and password if it needs them
   export PIP_CERT=/path/to/your-ca.pem                # only for a private CA
   python -m pip install "neo4j-graph-loader==0.1.0a2"
   unset PIP_INDEX_URL
   ```

3. **Run.** The roles reach Neo4j and Kafka at the command's own `NEO4J_URI` and `KAFKA_BOOTSTRAP_SERVERS`:

   ```bash
   export NEO4J_URI=bolt://neo4j.internal.example:7687 NEO4J_USERNAME=neo4j NEO4J_PASSWORD=...
   export KAFKA_BOOTSTRAP_SERVERS=kafka.internal.example:9092
   graph-loader start --runtime process --mode bulk --config my-graph.yaml --run-dir ./runs
   ```

   Do not set `LOADER_NEO4J_URI` or `LOADER_KAFKA_BOOTSTRAP_SERVERS` for this runtime: they are for container roles and, when set, win over the two variables above.
   Requirements and behaviour of this runtime (Linux 5.3 or newer, the run directory, health endpoints) are in [Process runtime](PROCESS_RUNTIME.md).

## Route 2: copy the released images by digest

If a machine that can reach the registry that publishes the images (`ghcr.io/neburyx`) is allowed to copy to your registry, copy the four images
(`graph-loader-cli`, `-node`, `-edge`, `-clock`, tag = package version) and point the loader at your registry. **Copy by digest** and keep the digest
you copied, so the image you run is the image you reviewed:

```bash
# on the machine that can reach both registries; docker login to each registry first
for role in cli node edge clock; do
  src=ghcr.io/neburyx/graph-loader-$role
  digest=$(docker buildx imagetools inspect "$src:0.1.0a2" --format '{{.Manifest.Digest}}')   # sha256:... of the release you reviewed
  docker pull "$src@$digest"
  docker tag "$src@$digest" registry.example.com:5000/team/graph-loader-$role:0.1.0a2
  docker push registry.example.com:5000/team/graph-loader-$role:0.1.0a2                        # prints "digest: sha256:..."
  echo "$role $digest" >> copied-digests.txt                                                   # the pushed digest must equal this one
done
```

Any tool that copies a registry object works and keeps the manifest byte for byte: `skopeo copy --all docker://SOURCE docker://DESTINATION`,
`crane copy SOURCE DESTINATION`, or an offline transfer with `docker save` on one side and `docker load` on the other (then `docker tag` and `docker push`).
**Compare the digest the destination reports with the one you recorded.** With `docker pull`/`tag`/`push` and with `docker save`/`load` the digest was unchanged in the proof; a tool that re-encodes the manifest, or a registry that rewrites it, would change it.
The loader does not depend on the digest: it requires the three image labels (below), so a copy that keeps the labels is accepted.

Then name your registry once, and the loader resolves `<prefix>/graph-loader-<role>:<package version>` for every role (the CLI image too):

```bash
export GRAPH_LOADER_IMAGE_REGISTRY=registry.example.com:5000/team       # host[:port] and lowercase path parts; no scheme, no credentials
docker login registry.example.com:5000                                  # the loader uses your Docker login to pull; it never asks for a password
graph-loader start --mode bulk --config my-graph.yaml --network my-net
```

`start --runtime docker` must run on the Docker host itself, through a local socket: the roles publish their control ports on that host's loopback and the bind-mounted paths are not translated for a remote daemon (the same rule as everywhere in [Docker execution](DOCKER_EXECUTION.md)).

To pin one role to a digest, set its override (a tag or a digest reference, never a URL or credentials): `GRAPH_LOADER_NODE_IMAGE`,
`GRAPH_LOADER_EDGE_IMAGE`, `GRAPH_LOADER_CLOCK_IMAGE` ([Docker execution](DOCKER_EXECUTION.md)). `--skip-image-build` makes `start` refuse to pull and use
only images already on the daemon. The `--runtime process` runtime ignores all of this.

## Route 3: build the images with `graph-loader images build`

Build the four images on a machine inside your network from the **installed package**, your base image and either your PyPI mirror or a
wheelhouse. The command and every option are in [`graph-loader images build`](CLI.md#images-build); this section is the preparation.

**You need** Docker with the `buildx` plugin (BuildKit) on the build machine, a registry you control, and:

| Input | Cell A: your index | Cell B: offline |
| --- | --- | --- |
| Base image | `python:3.12-slim` (CPython 3.12 on Debian) copied into your registry; the build resolves it to a digest first | the same, **plus `tini` 0.19.0 already installed** in it |
| Python dependencies | your PyPI mirror (`--pip-index-secret FILE`, or `--pip-index-url` for an index without credentials), optionally `--pip-ca-bundle` | a wheelhouse (`--wheelhouse DIR`), nothing from any index |
| `tini` for the CLI image | your Debian mirror (`--debian-mirror URL`) | none: a base image that already has it skips apt |
| Package wheel | `--package-wheel FILE` (its hashes must equal the installed package's) | taken from the wheelhouse |

**Cell A, a mirror:**

```bash
docker login registry.example.com:5000                       # the base image and the push both use this login
printf '%s\n' "$INDEX_URL_WITH_CREDENTIALS" > pip-index-url.txt && chmod 600 pip-index-url.txt
graph-loader images build --registry registry.example.com:5000/team \
    --base-image registry.example.com:5000/mirror/python:3.12-slim \
    --pip-index-secret pip-index-url.txt --pip-ca-bundle your-ca.pem \
    --debian-mirror http://debian.internal.example/debian \
    --package-wheel neo4j_graph_loader-0.1.0a2-py3-none-any.whl --push --output ./build-manifest
```

**Cell B, offline.** Prepare the wheelhouse **inside the target base image** on the target platform, so that pip chooses the wheels the build will install;
then copy the package wheel next to them. The directory must hold exactly the locked wheels and the package wheel (a missing, extra, changed or
wrong-architecture wheel is refused before anything is built):

```bash
LOCK=$(python3.12 -c "from importlib.resources import files; print(files('neo4j_graph_loader.image_build') / 'locks' / 'py312-linux-amd64.txt')")
mkdir wheelhouse
docker run --rm --platform linux/amd64 -v "$LOCK:/lock.txt:ro" -v "$PWD/wheelhouse:/wheelhouse" --user "$(id -u):$(id -g)" -e HOME=/tmp \
    python:3.12-slim pip download --no-deps --only-binary=:all: --require-hashes -r /lock.txt -d /wheelhouse
cp neo4j_graph_loader-0.1.0a2-py3-none-any.whl wheelhouse/
graph-loader images build --registry registry.example.com:5000/team \
    --base-image registry.example.com:5000/mirror/python-tini:3.12-slim --wheelhouse ./wheelhouse --push --output ./build-manifest
```

Do not prepare the wheelhouse with `pip download --platform manylinux2014_...` from another machine: current `confluent-kafka` and NumPy wheels need
newer `manylinux` tags. Build and prepare for each architecture you run (the build is for the architecture of the Docker daemon); the dependency lock of both
ships with the package. A base image with `tini` is made once, from your Debian mirror or any machine that has `tini`, for example:

```dockerfile
FROM registry.example.com:5000/mirror/python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends tini && rm -rf /var/lib/apt/lists/* && tini --version
```

then push it under the name you give to `--base-image`.

**Your Debian mirror** must be in the archive layout (`dists/<suite>/...`, `pool/...`) of the Debian release of the base image (`trixie` for
`python:3.12-slim`): the build keeps Debian's own signing keys (`Signed-By` the archive keyring), so the mirror must serve Debian's **signed**
`InRelease` and the index files **by hash** (the archive sets `Acquire-By-Hash`). A mirror that carries only `tini` is enough if it keeps the real signed
indices; an unsigned or re-signed flat repository is not accepted.

**What the build writes.** `--output DIR` writes `images.json` (the manifest: the package wheel and its sha256, the lock and every dependency artifact
sha256, the Dockerfile and helper hashes, the resolved base image digest, the build arguments without secrets, the Debian package versions of the CLI image,
every image id and, with `--push`, the registry digest) and `image-overrides.sh`. Keep both with your records. Nothing in them is a credential.

## Registry login

The loader and `images build` use **your Docker login**, never a password option. `images build` copies only the logins of the base image's registry and
of the target registry into a throwaway Docker configuration (`--docker-config DIR` chooses the source). `start` pulls with the Docker login of the user that
runs it. Observed behaviour against an authenticated registry:

* a build whose base image is in a private registry and that has **no login** stops at the base image lookup with `cannot resolve the base image to a digest;
  check the name and the registry login` (the registry answered `401 Unauthorized`; `docker buildx imagetools inspect` with the same configuration shows it);
* `docker push` **without a login** is refused (`push access denied ... authorization failed: no basic auth credentials`); with the login it succeeds;
* a login is a client-side file of the build user: it is not written into the images, the manifest or the build output.

**Kubernetes** (documentation only, not tested here): create a `kubernetes.io/dockerconfigjson` Secret for your registry and reference it as
`imagePullSecrets` of the pod that runs the images. The loader's CLI image can run as a pod only in the process runtime (see [Single container](SINGLE_CONTAINER.md)).

## Checking what you have

```bash
docker image inspect --format '{{json .Config.Labels}}' registry.example.com:5000/team/graph-loader-node:0.1.0a2
docker run --rm --entrypoint tini registry.example.com:5000/team/graph-loader-cli:0.1.0a2 --version      # tini version 0.19.0
```

The loader requires three labels on every role image, and refuses an image without them: `org.opencontainers.image.version` equal to the package
version, `io.graph-loader.component` equal to the role (`cli`, `node`, `edge`, `clock`) and `io.graph-loader.image-contract` equal to `1`. The manifest
of a build you made records the same labels for each image.

## Network notes for restricted networks

* **Name resolution.** Build steps (`RUN`) use the Docker daemon's DNS settings (`dns` in `daemon.json` or `--dns`), not the resolver that a container
  network gives to ordinary containers. A mirror host name that works for `docker pull` must also resolve for the daemon's build steps. In an **inner**
  user-defined Docker network, a single-label name (`registry`) that only your DNS knows is not forwarded to it: use fully qualified names.
* **How a blocked route looks.** A direct connect to an address with no route fails at once with "network is unreachable"; a connection from inside a build
  step can instead end in a connect timeout (apt after about two minutes). Treat both as "denied". A pull from a blocked registry reports `dial tcp ...:
  connect: network is unreachable`, which is a network failure, not an authentication one, even when the message also says "failed to authorize".
* **`apt-get update` exits 0 when every source fails to fetch**; read its warnings, not the exit status, when you test a mirror.
* **pip prints no network message unless it retries.** With `--retries 0` the only message is "No matching distribution found"; use at least one retry
  when you diagnose connectivity.
* A registry reached over plain HTTP only through the daemon's `insecure-registries` was **not** tried: `images build` resolves the base image with
  `docker buildx imagetools inspect`, which runs in the client and needs a TLS certificate the client trusts.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| `cannot resolve the base image to a digest; check the name and the registry login` | the base image name is wrong, or the client has no login, or does not trust the registry's certificate |
| the build stops at `FROM` with "no basic auth credentials" | the scoped login is missing for the base image's registry (`--docker-config`) |
| the CLI image build fails in `apt-get` | the Debian mirror is unreachable, unsigned, lacks the by-hash index files, or does not match the base image's release |
| `a wheelhouse that does not satisfy the lock` | a wheel is missing, extra or for another architecture; prepare it inside the base image on that platform |
| `start` stops with `cannot resolve <role> image` | the image is not at `<prefix>/graph-loader-<role>:<version>`, the login is missing, or the labels are wrong |
| `pip` reports "No matching distribution" in a build | the index does not carry every wheel of the lock, or the CA is not given (`--pip-ca-bundle`) |

## What was tested

The restricted-network proof ran an isolated topology on a x86_64 Linux host: an internal Docker network with no route out; an authenticated registry
with TLS from a throwaway CA, an authenticated PyPI index behind a TLS proxy and a Debian mirror, all owned and on that network; an inner Docker daemon
(Docker 29.9, BuildKit 0.34) with only those services reachable; and a runner container that had the Docker CLI, `buildx` and the package. Public hosts
(`docker.io`, `ghcr.io`, `pypi.org`, `deb.debian.org`) answered DNS but every connection failed with a network error, in the runner, in a daemon pull, in a
build step and in a role container, while a control container on a normal bridge network reached them. In that topology a build from the authenticated
index, CA bundle and Debian mirror (cell A) and an offline build from a wheelhouse with a `tini` base image (cell B, during which the owned index and mirror received
no request) produced images that were pushed to the owned registry; a bulk load and a short stream load per cell ran with `GRAPH_LOADER_IMAGE_REGISTRY` pointing at that
registry and the role images pulled from it; route 2 (copy by digest, once by `docker save`/`load`) and a load from the copy, and route 1 (the package installed into a fresh
virtual environment from the owned index and a load with `--runtime process`) ran as written above. The runner was the Docker CLI's host: it shared the inner daemon's
network namespace, because the docker runtime's dispatcher publishes its control port on the loopback of the CLI's host.

**Not tested:** linux/arm64 builds and images; a plain-HTTP registry; Kubernetes `imagePullSecrets`; IPv6; a registry that requires a proxy; Windows and macOS
hosts (the process runtime is Linux only). The proof used small loads (minutes), not a soak. This guide is for users; the maintainers' export checks are a
development tool that is not shipped and have no effect on your installation.
