# Single container: run graph-loader in one container

By default `graph-loader start` runs every role as its own Docker container and
needs the Docker socket. With `--runtime process` the roles are **child
processes of the one `graph-loader start` command**. Put that command in the CLI
image and the whole loader is **one container**: `tini` is PID 1, the
`graph-loader` command is its child, and every role (node loaders, relationship
loaders, lease server, retry consumer, planner workers) is a child of the
command. This page covers building and running that container, stopping it,
sizing it and its limits.

Related: [Process runtime](PROCESS_RUNTIME.md) (the runtime itself: options,
run directory, recovery, sizing) | [CLI reference](CLI.md) |
[Docker execution](DOCKER_EXECUTION.md) | [Incremental load](INCREMENTAL_LOAD.md) |
[Bulk load](BULK_LOAD.md) | [Monitoring](MONITORING.md)

Contents: [What it is](#1-what-it-is-and-when-to-use-it) | [The image](#2-the-image) |
[Quick start](#3-quick-start-with-docker-run) | [Docker Compose](#4-docker-compose) |
[Stopping and restarting](#5-stopping-and-restarting) | [Resources](#6-resources) |
[Kubernetes](#7-kubernetes-untested) | [Limits](#8-limits)

> **Verification status of this page.** The container shape was run live on a
> Linux host (Ubuntu, kernel 7.0, Docker, cgroup v2, amd64) in story
> **E14.F3.S8**: the cells and their results are in
> `plan/library-implementation/evidence/E14.F3.S8.md`. Statements that were
> observed there say so. Statements marked **untested** below were not covered by
> a cell (the arm64 image, Compose, restart policies other than `on-failure`,
> `docker stop` with the default 10 s grace, `--cpus`/`LOADER_CPUSET`, a shared
> PID namespace, Kubernetes, cgroup v1); read those as "designed and derived from
> the source, not yet measured". Statements about the process runtime itself
> (flags, `/readyz` semantics, the ordered stop, the cgroup budget) come from the
> source and from the host-process evidence in
> [Process runtime](PROCESS_RUNTIME.md). Numbers are quoted only where a
> measurement exists.

## 1. What it is and when to use it

| | Container per role (default) | Single container (this page) | Host process or service |
| --- | --- | --- | --- |
| Command | `graph-loader start` (`--runtime docker`) | `docker run <CLI image> start ... --runtime process` | `graph-loader start --runtime process` in a shell or systemd |
| Needs | A Docker daemon and its socket on the machine that runs the CLI, a user-defined network, four images | One container from the CLI image; no Docker socket, no `--privileged`, no network creation, no other images | Linux, the installed wheel |
| A role is | its own container | a child process in the one container | a child process on the host |
| Memory limit | one per role container | one shared limit for the command and all roles | the shell's, scope's or unit's limit |
| Restart owner | you | the container restart policy or the orchestrator | systemd, or you |
| Roles can span hosts | yes (any Docker daemon the CLI talks to) | no: one container, one machine | no |

Use the single container when you deploy by container image (Docker, Compose,
Kubernetes) and do not want to give a loader the Docker socket, or when the
platform can run a container but not start containers of its own. Use the
default when you already run Docker on the load host and want the tested default
shape, or the host process when you have a Linux machine and no container
requirement ([choosing a runtime](PROCESS_RUNTIME.md#1-choosing-a-runtime)).

Requirements: a **Linux** container host, kernel 5.3 or newer (a container shares
the host kernel, and the runtime signals roles through `pidfd`), on **amd64** or
**arm64**. cgroup v2 is needed for the memory budget and the OOM report
([section 6](#6-resources)). Docker Desktop on macOS or Windows is not a
supported load platform. Neo4j 5.26 Enterprise and a Kafka broker are yours to
provide and must be reachable from the container.

What stays the same as the host process: the roles, their arguments, Kafka and
Neo4j semantics, the supervision policy (a role that dies fails the run, the
command stops the rest and exits `1`), the exit codes, the ordered stop, the
`/healthz` and `/readyz` semantics, the run directory layout. What changes is
only who starts and stops the command: Docker (or Kubernetes) instead of you or
systemd.

## 2. The image

The CLI image is built from the repository's `Dockerfile`
(`io.graph-loader.component="cli"`). Its entry point is now

```text
ENTRYPOINT ["tini", "--", "python", "-m", "neo4j_graph_loader.cli"]
```

so everything after the image name in `docker run` is a `graph-loader`
sub-command: `docker run <image> start --mode stream ...`. `tini` (the Debian
package, version checked at build time against the `TINI_VERSION` argument) is
PID 1 and does three things: forwards `SIGTERM` (`docker stop`, pod termination)
to the command, reaps orphaned processes (planner workers, role grandchildren)
so no zombie accumulates, and exits with the command's status. It is started
without `-g`: the command signals its own process groups and must run its
ordered stop first. The image contract is unchanged: the labels
`org.opencontainers.image.version`, `io.graph-loader.component="cli"` and
`io.graph-loader.image-contract="1"`, the package-version check, `WORKDIR /app`,
no `USER`, no `CMD`. Docker mode never creates a container from this image, so
it is unaffected.

**Which image to use.** The image naming policy is
`ghcr.io/shashankshukla96/graph-loader-cli:<package version>`
([Docker execution](DOCKER_EXECUTION.md)). The `0.1.0a1` release images were
frozen before `tini` was added to the CLI image (the change alters the image
bytes), so **do not assume that a published `graph-loader-cli` tag contains
`tini`**; check any image first:

```bash
docker run --rm --entrypoint tini <image> --version         # prints "tini version 0.19.0"
```

Until a release that includes it is published, build the image from a checkout
of the repository (the build needs the repository files, not the wheel):

```bash
docker build --build-arg GRAPH_LOADER_VERSION=0.1.0a1 -t graph-loader-cli:local .
```

`scripts/build_candidate_images.py --output <new dir> --suffix <suffix>` builds
all four images locally with the tag `ghcr.io/shashankshukla96/graph-loader-cli:<version>-<suffix>`
and records a manifest; it never pushes. Use your own registry name when you
push a build you made yourself. For the other architecture build with
`docker buildx build --platform linux/arm64 --load .` (or `linux/amd64`); the
Debian `tini` package makes the install identical for both. **Untested:**
the arm64 image has not been run. The S8 host is x86_64 without `qemu-aarch64`
emulation, so the smoke was skipped; only the amd64 image was run.

Do not add `--init` (Compose `init: true`) on top: Docker's init would be PID 1
and `tini` its child, which makes `tini` warn that it is not PID 1 and cannot
re-parent orphans unless `TINI_SUBREAPER` is set ([section 8](#8-limits)).

## 3. Quick start with docker run

Prepare, on the Docker host:

* a **schema** and, for a stream, the **topics** ([Incremental load](INCREMENTAL_LOAD.md));
  for a bulk load, publish the records first ([Bulk load](BULK_LOAD.md));
* a **work directory** that will be mounted as `/work`, holding `schema.yaml` and
  an empty `runs/` directory; the run directory (`--run-dir`) lives under it;
* an **env file**, mode `0600`, outside the image and the repository. The
  command does not read `.env`; it reads the environment of the container:

  ```text
  NEO4J_URI=bolt://neo4j.example.internal:7687
  NEO4J_USERNAME=neo4j
  NEO4J_PASSWORD=change-me
  NEO4J_DATABASE=neo4j
  KAFKA_BOOTSTRAP_SERVERS=kafka.example.internal:9092
  ```

  `KEY=value` lines, no quotes (Docker keeps quotes literally; the same lines work
  for `systemd`'s `EnvironmentFile`). Only the CLI's own variables matter in
  process mode; roles get the connection settings from the command
  ([environment](PROCESS_RUNTIME.md#4-options-and-environment)). The password
  never appears on the `docker run` command line, but it is visible to anyone
  who can `docker inspect` the container; in an orchestrator use its secret
  mechanism to fill the same variables. The command reads secrets from the
  environment only.

```bash
mkdir -p work/runs && cp my_graph.yaml work/schema.yaml

docker run -d --name graph-loader \
  --env-file ./graph-loader.env -e HOME=/tmp \
  --user "$(id -u):$(id -g)" \
  -v "$PWD/work:/work" -w /work \
  --network <network that reaches Neo4j and Kafka> \
  -p 127.0.0.1:8081:8081 \
  --memory 6g --memory-swap 6g \
  --stop-timeout 120 \
  --restart on-failure:5 \
  --health-cmd "python -c \"import urllib.request; urllib.request.urlopen('http://127.0.0.1:8081/readyz', timeout=3)\"" \
  --health-interval 30s --health-timeout 5s --health-retries 3 --health-start-period 120s \
  graph-loader-cli:local \
  start --mode stream --config /work/schema.yaml --runtime process \
        --run-dir /work/runs --health-addr 0.0.0.0:8081 --child-logs relay
```

The S8 live cells ran this shape (no socket mount, no `--privileged`, a bind
mount of the work directory, `--user` of the invoking user with `HOME=/tmp`,
`--health-addr 0.0.0.0:8081`, `docker stop -t 60`): `/readyz` answered 200 and
the exact fixture graph loaded. The health command above was also run inside the
container by the extras cell (through `docker exec`, exit 0 while the fleet was
ready). It uses `python -c` with `urllib` because the slim image has no `curl`
or `wget`.

What each part does:

| Part | Why |
| --- | --- |
| `--runtime process` | Run roles as children of the command. Required: without it the command looks for a Docker socket. `GRAPH_LOADER_RUNTIME=process` in the environment is equivalent. |
| `--network <net>` (Docker's) | Put the container where Neo4j and Kafka are reachable. Not graph-loader's `--network` option, which is **refused** with `--runtime process`; do not pass it after the image name. |
| `NEO4J_URI`, `KAFKA_BOOTSTRAP_SERVERS` | Must resolve and connect **from inside the container**. Kafka brokers hand back their *advertised* listener addresses, which must also be reachable from the container. Roles reuse the command's values; `LOADER_*` overrides are not needed. |
| `-v $PWD/work:/work` and `--run-dir /work/runs` | Each start creates a private attempt directory `graph-loader-<random>` under `--run-dir` with `registry.jsonl`, per-child `logs/`, `work/<name>/`, planner files and `metrics/targets.json` ([run directory](PROCESS_RUNTIME.md#5-the-run-directory)). Without a volume the default is the container's temporary directory, in the writable layer: it survives a restart of the same container, not `docker rm`. With a volume, check that the container's user can write it (`--user` as above). Attempt directories are never removed automatically. |
| `--health-addr 0.0.0.0:8081` | Serve `/healthz` and `/readyz` (off unless given). Inside a container `0.0.0.0` is your choice, and it is only needed when something **outside** the container's network namespace probes it (a published port, a Kubernetes kubelet). A `HEALTHCHECK` that runs inside the container can use `127.0.0.1`, and then `--health-addr 127.0.0.1:8081` is enough. The endpoint is unauthenticated; do not publish it on a public interface (the example publishes to the host's loopback). |
| `--child-logs relay` | The default. Each role's output is also printed to the command's stdout with a `[graph-loader-<role>-...]` prefix, so `docker logs graph-loader` shows the whole fleet. The files in `<attempt>/logs/` stay the source of truth (size-capped; if stdout stalls, relay lines are dropped and counted, the files stay complete). Docker's default `json-file` log driver does not rotate by itself; set `--log-opt max-size=... --log-opt max-file=...` for a long-running stream (not tested). |
| `--memory 6g --memory-swap 6g` | One limit for everything in the container, swap off ([section 6](#6-resources)). `6g` is only an example; size it from the [formula](PROCESS_RUNTIME.md#9-sizing). |
| `--stop-timeout 120` | `docker stop` waits this long before `SIGKILL`; the default of 10 s is too short ([section 5](#5-stopping-and-restarting)). |
| `--restart on-failure:5` | Restart when the command exits non-zero (a role was lost); a clean stop is not restarted ([section 5](#5-stopping-and-restarting)). |
| `-e HOME=/tmp` | A role gets `HOME` from the command's environment. A `--user` id has no home directory in the image, so the S8 cells set `HOME=/tmp`. |

Check it from the Docker host:

```bash
docker logs -f graph-loader                                  # command and [role] lines
curl -fsS http://127.0.0.1:8081/readyz                       # 200 once every role is ready (curl is on the host, not in the image)
docker inspect --format '{{.State.Health.Status}}' graph-loader
```

**Health semantics** (from the source, the same as the host process;
[details](PROCESS_RUNTIME.md#6-logs-metrics-and-health)). `/healthz` is 200 while
the supervision loop has ticked within `--health-stale-after` (default 30 s) and
the registry is writable; it is a liveness signal. `/readyz` is 200 only when at
least one role is running, every launched role has reported ready, none has
failed and the run is not stopping; otherwise 503 with a JSON body listing
`roles`, `running`, `pending`, `failed` and `stopping`. A role that finished with
exit 0 on its own counts as neither ready nor failed, so **a finished bulk run
reports not ready** (and the container is about to exit anyway). Use `/healthz`
for liveness checks and `/readyz` for "the fleet is up". The image has no `curl`;
the `HEALTHCHECK` above uses Python's standard library, which exits non-zero on a
503. Plain Docker only *records* an unhealthy status; it does not restart the
container for it (Compose `depends_on: condition: service_healthy` and
orchestrators can act on it). After an abrupt end a stream restart waits for the
old consumer-group members to expire before it is ready (`session_timeout_ms`
+ 5 s by default, [details](PROCESS_RUNTIME.md#7-stopping-and-recovery)), so give
`--health-start-period` several minutes if the container is restarted abruptly.

**Metrics.** `--metrics-addr HOST[:PORT]` is where every **role** binds its
Prometheus endpoint: the default is `127.0.0.1` with an OS-assigned port per
role, written to `<attempt>/metrics/targets.json`. `--metrics-addr 0.0.0.0`
makes the role endpoints reachable from other containers or pods on the
network (unauthenticated: keep it on a private network). The ports are chosen
at run time, so they cannot be published with `-p` in advance, and a fixed
port (`0.0.0.0:9464`) suits a single role only; see [section 8](#8-limits) for
the `targets.json` caveat. The runtime hands each role its address through
`GRAPH_LOADER_METRICS_LISTEN`; that variable is how `--metrics-addr` is applied,
not a setting for you. The command's **own** endpoint (consumer lag, planner
metrics) is separate: `GRAPH_LOADER_METRICS_BIND_ADDR` (default `127.0.0.1`,
port `9464` or `GRAPH_LOADER_METRICS_PORT`; a non-default address is logged as
a warning). `GRAPH_LOADER_METRICS_ENABLED=0` turns the endpoints off. The main S8
cells run with metrics **disabled**; two extra cells ran with metrics enabled on
a user-defined network. With `--metrics-addr 0.0.0.0` the **role** endpoints
answered `200` from another container (by the container's address), and
`targets.json` still named `127.0.0.1` for every target, as stated above. The
command's own endpoint on port `9464` did **not** answer another container: it
keeps its loopback default until you set `GRAPH_LOADER_METRICS_BIND_ADDR`. With
no `--metrics-addr` the role endpoints also refused another container. Metric names are in [Monitoring](MONITORING.md).

## 4. Docker Compose

The same container as a Compose service. Neo4j and Kafka are assumed to be
reachable on an existing network named `data-net` (replace it with yours):

```yaml
services:
  graph-loader:
    image: graph-loader-cli:local        # your build, see section 2
    command:
      - start
      - --mode=stream
      - --config=/work/schema.yaml
      - --runtime=process
      - --run-dir=/work/runs
      - --health-addr=0.0.0.0:8081
      - --child-logs=relay
    env_file: ./graph-loader.env                 # mode 0600, outside version control
    environment:
      HOME: /tmp
    # user: "1000:1000"                          # the owner of ./work
    volumes:
      - ./work:/work
    working_dir: /work
    networks: [data-net]
    ports:
      - "127.0.0.1:8081:8081"
    mem_limit: 6g                                # example; see section 6
    memswap_limit: 6g                            # equal to mem_limit: swap off
    stop_grace_period: 120s                      # must exceed the ordered-stop budget, section 5
    restart: "on-failure:5"                      # not "always" / "unless-stopped" for a bulk job
    healthcheck:
      test:
        - CMD
        - python
        - -c
        - "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8081/readyz', timeout=3)"
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 120s

networks:
  data-net:
    external: true
```

`docker compose stop` and `docker compose down` use `stop_grace_period`;
`docker compose up --abort-on-container-exit` is not a substitute for a restart
policy. For a **bulk** load use `restart: "no"` (a finished bulk run exits `0`;
a stopped one exits `1`) so that a finished load is never started again, and keep
`/work` on a volume so the attempt directory outlives the container. **Untested:** the Compose
file has not been run; it expresses the same settings as the `docker run`
example.

## 5. Stopping and restarting

### `docker stop`: the grace must exceed the ordered-stop budget

`docker stop -t <grace>` sends `SIGTERM` to PID 1 (`tini`), which forwards it to
the command; the command then performs the same ordered stop as in every other
shape. Docker sends `SIGKILL` to PID 1 when the grace runs out, and the kernel
kills everything else in the container with it, so the ordered stop is cut
short. **Docker's default grace is 10 s, which is shorter than the ordered stop
of a loaded fleet.** The budget, from the source (stream with relationships;
each step waits for the previous one, the roles inside a step stop together):

| Step | Roles | `SIGTERM`, then `SIGKILL` after |
| --- | --- | --- |
| 1 | edge loaders and retry consumer | 10 s |
| 2 | node loaders | 30 s |
| 3 | lease server | 10 s |

So the worst case is **50 s** (10 + 30 + 10), if every role needs its full grace.
Add up to 5 s if the stop arrives while a role is still starting (a stop sent to
a role before it has reported ready is held until it is ready, at most 5 s after
its launch), and a little for the command's own exit. A stream **without**
relationship types has only the node loader step (30 s). A planned bulk run
additionally stops the planner workers before any role is signalled; that stop
was not derived separately. Set the grace to at least 60 s, and **120 s** for
margin (`--stop-timeout 120`, `docker stop -t 120`, Compose
`stop_grace_period: 120s`, Kubernetes `terminationGracePeriodSeconds: 120`); it
is a ceiling, not a delay. An idle fleet stopped in about a second in the host
measurement under systemd (1.0 s, same ordered stop). A loaded fleet in a
container was timed by the S8 cell: `docker stop -t 60` returned in **1.06 s**
with container exit code `0`, all six child exit receipts `0`, in the order edge
loaders, then node loaders, then the lease server. The worst case of 50 s above
was derived from the source (edges and retry 10 s, node loaders 30 s, lease
server 10 s) and not provoked.

A stream run stopped this way exits `0`; a stopped bulk run exits `1` (the
[exit codes](CLI.md#exit-codes) are the same as everywhere else). A stop
cut short by the default 10 s grace ends like `docker kill` (below): safe, not
clean, exit code 137. **Untested:** the default-grace case was not run (the
loaded fleet stopped in about 1 s, well inside 10 s); the `docker kill` outcome
below was.

A second-shell stop is also possible and does the same ordered stop; it ends the
command with exit `0` and **is not a `docker stop`**, which matters for the
restart policy below:

```bash
docker exec graph-loader python -m neo4j_graph_loader.cli stop --runtime process --run-dir /work/runs
```

The extras cell ran this with the fleet loaded: the command printed
`Stopping loader(s)` and exited `0` in 1.8 s, the container exit code was `0`,
the restart policy `on-failure` did not restart it (`RestartCount` 0, one `die`
event with code `0`), every child exit receipt was `0` in the ordered sequence,
and the graph was exact.

### Restart policy

| Event | `no` | `on-failure[:N]` | `always` | `unless-stopped` |
| --- | --- | --- | --- | --- |
| A role is lost: the command stops the fleet and exits `1` | stays down | **restarted** | restarted | restarted |
| `docker stop` (stream: exit `0`; bulk: exit `1`) | down | down | down until the daemon restarts | down |
| Clean exit `0` that was not a `docker stop` (a finished bulk load, the `docker exec ... stop` above) | down | **down** | restarted | restarted |
| The command itself is killed (for example by the kernel's OOM killer): the container exits `137` | down | restarted | restarted | restarted |

The rows come from Docker's documented restart-policy behaviour combined with
the command's exit codes. The live cell covers `on-failure`
with a role killed inside (the command fails the run, the container exits
`1`, the policy restarts it, exactly one new fleet runs, `RestartCount` is `1`,
the old attempt's children are all receipted and the graph is exact). The extras
cell also confirmed that `on-failure` does not restart after an ordered stop.
**Untested:** `always` and `unless-stopped`.

Use **`on-failure`** (with a retry limit) for a stream, so that a lost role is
retried and an intentional stop stays stopped. Do not use `always` or
`unless-stopped` for a bulk load or with `docker exec ... stop`: they restart the
container after a clean exit `0` and the load would start again. A restart
creates a **new attempt directory**; the command does not restart anything by
itself, and the old directories stay in the volume until you remove them.

### `docker kill`, a crash, a host restart

Whether a restart policy also restarts a container that you ended with
`docker kill` depends on Docker; do not rely on it (the live cell below starts
the container by hand). `docker kill` (or an OOM kill of the command, a power
loss, a daemon restart without live restore) ends `tini` abruptly; when PID 1 of a container dies the kernel kills
every other process in it. Nothing is left running, so **`stop --recover` is not
needed**: that command exists for processes that outlive a dead command, which
cannot happen in a container whose PID 1 is gone. What is left is what was
already on disk or in Kafka:

* **Stream:** start again. The new run waits for the old consumer-group members
  to expire, then resumes from the committed offsets. Redelivery is possible;
  every write is an idempotent `MERGE`, so the graph ends exact with no duplicate
  relationships ([stream restart](PROCESS_RUNTIME.md#7-stopping-and-recovery)).
  The live cell ran `docker kill` mid-load (246 relationships present, 131 in
  flight): exit `137`, the first attempt's children had no exit receipts (the
  end was abrupt), and after a manual `docker start` there was one fleet, the
  exact oracle graph with no duplicates, and offsets at the log ends.
* **Planned bulk:** the run's chunk topic `graph-loader-edge-chunks-<run id>` (or
  your `--plan-topic`) stays in Kafka and **nothing deletes it**. The attempt
  directory in the volume keeps the planner's lock and progress files, the
  registry and the logs (`stop --recover` would list them as retained and delete
  nothing). Delete the chunk topic and rerun as described in [Process runtime
  section 7](PROCESS_RUNTIME.md#7-stopping-and-recovery) (steps 2 to 5; step 1
  is the part a container makes unnecessary). A random `--run-id` is always safe;
  with an explicit one use `--replace-stale-plan-topic` after deleting the topic.

If the run directory is not on a volume and the container is removed (`docker
rm`, `--rm`, a rescheduled pod), the registry, logs and planner files go with
it; the Kafka and Neo4j side is unaffected.

## 6. Resources

**Memory.** Use `--memory <n> --memory-swap <n>` (equal values: no swap), or
Compose `mem_limit` with an equal `memswap_limit`. The limit is shared by the
command, the planner workers and every role, so size it for the sum with the
[formula](PROCESS_RUNTIME.md#9-sizing): about 200 MiB for the command, plus the
role reserves, plus the planner (workers x at least 256 MiB). With swap on, an
over-limit process slows down instead of being killed and the budget is harder
to reason about; with it off the kernel's OOM killer acts at the limit.

**The planner is sized from the container's limit.** Before a planned bulk
load the command reads the cgroup v2 limits of its own cgroup and every
ancestor, and sizes the planner (workers and per-worker memory) from the smaller
of the available memory and what is left under the limit; the run is refused
before any role starts (`stage=sizing reason=...`, exit `1`) when not even one
256 MiB worker fits. Inside a container the container's limit is the file the
command sees as `memory.max` of its own (root) cgroup. The budget was measured
in a systemd scope on a host (a `1400M` limit: planner reduced from 4 to 1
worker, peak 405 MiB, completed; `900M`: refused in seconds); the same
arithmetic with a **container's** limit was run by the extras cell: a planned
bulk load in a container with `--memory 1400m` logged
`stage=sizing planner_workers=1 planner_memory_mb=324 headroom_mb=1325 ... roles=5`
(concurrency reduced from 4 to 1 worker) and completed with exit `0`. It needs
**cgroup v2** on the host. When no cgroup v2 memory limit is visible to the
command (cgroup v1, or no memory controller), automatic sizing **refuses**
(`no cgroup v2 memory limit is visible ...`, exit `1`) instead of guessing from
the host's `MemAvailable`: give both `--plan-workers` and `--plan-memory-mb`
explicitly (the way to size by hand anyway). That refusal was proved by unit
tests, not in a cgroup v1 container. Streams have no planner and are bounded by the limit itself.

**An OOM kill is reported at group level.** The kernel kills one process of the
container's cgroup when the limit is hit; which one is not knowable from inside
(a role, a planner worker, or the command itself). A role that dies of `SIGKILL`
is exit `137`, exactly like a manual `kill -9`. When the cgroup's `oom_kill`
counter in `memory.events` has risen, the command prints one line and writes an
`oom` record to `registry.jsonl`:

```text
ERROR stage=oom cgroup OOM kill observed; victim unknown (memory.events oom_kill <before> -> <after>)
```

It never names a role or a pid. Then the run fails as for any role loss (exit
`1`, the restart policy applies). No rise (a manual kill) prints nothing. If the
kernel chose the command itself, there is nobody left to print it: the container
exits `137` and you see only that. **Docker's `State.OOMKilled` flag is not
a dependable signal for this shape** (it was `true` in the live run although only
a child was killed and the container exited `1`, and it says nothing about which
process died), so look for the `stage=oom` line in
`docker logs` and for `oom_kill` in the container's cgroup `memory.events`.
The live cells ran `--memory 1536m --memory-swap 1536m`, raised
the intended victim's `oom_score_adj` and filled the limit until `oom_kill` rose
by one: the run failed, the container survived and exited `1`, the output had the
`victim unknown` line and nothing named a child or pid; a concurrent manual
`SIGKILL` of a second role still yielded one group-level report; a manual kill
alone yielded none. **Observed, and different from the earlier wording:** Docker's
`State.OOMKilled` was `true` after such a child-only kill even though the
container itself kept running and exited `1` by the command's own decision, so it
does not mean "the container's main process was killed" here; do not rely on it
either way.

**CPU.** `--cpus` is a CFS quota (throttling) and `--cpuset-cpus` restricts the
CPUs the container may run on. Neither changes what the command *counts*: the
default planner concurrency is `min(4, os.cpu_count())`, and Python reports the
host's CPUs in a container, so set `--plan-workers` yourself when you give the
container fewer CPUs than that. `LOADER_CPUSET=<cpus>` (for example `4-7`) pins
each *role* with `sched_setaffinity` to keep roles off cores that Neo4j or Kafka
use; the command and the planner workers are not pinned, an invalid value stops
that role from starting, and the numbers must be CPUs the container is allowed
to use (with `--cpuset-cpus 4-7` give `LOADER_CPUSET` a subset of `4-7`, host
numbering). `--cpuset-cpus` alone confines the whole container; `LOADER_CPUSET`
only arranges the roles inside it. **Untested:** neither `--cpus`
nor `LOADER_CPUSET` has been run in a container.

## 7. Kubernetes (untested)

**No manifest on this page has been run on a cluster.** One pod, one container
from the CLI image, the same command as the `docker run` example. The mapping:

* `args:` replaces Docker's `CMD`, so the image's `tini` entry point is kept;
  do not set `command:`.
* `/healthz` is the liveness probe, `/readyz` the readiness probe, on the port
  given to `--health-addr` (bind `0.0.0.0`: the kubelet reaches the pod IP, not
  the container's loopback).
* `terminationGracePeriodSeconds` at least the stop budget of
  [section 5](#5-stopping-and-restarting); 120 is the value used throughout.
  Kubernetes sends `SIGTERM` to PID 1 and `SIGKILL` when the period ends.
* A memory `limits` value on the container is the shared limit of
  [section 6](#6-resources) (cgroup v2 nodes for the planner budget and the OOM
  report). Kubernetes runs without swap by default.
* Run **one** pod per schema: use `strategy: Recreate` so a rollout does not run
  two fleets of the same schema at once (the command refuses a second live
  fleet).
* A role loss makes the command exit `1` and the pod restart; for a **bulk**
  load use a `Job` with `restartPolicy: Never` or `OnFailure`, and do not use
  the readiness probe there (a finished run reports not ready).
* If the pod sets `shareProcessNamespace: true`, `tini` is not PID 1; set
  `TINI_SUBREAPER=1` ([section 8](#8-limits)).

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: graph-loader
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels: {app: graph-loader}
  template:
    metadata:
      labels: {app: graph-loader}
    spec:
      terminationGracePeriodSeconds: 120
      containers:
        - name: graph-loader
          image: registry.example.com/graph-loader-cli:0.1.0a1       # your build
          args: [start, --mode=stream, --config=/work/schema.yaml, --runtime=process,
                 --run-dir=/work/runs, --health-addr=0.0.0.0:8081]
          envFrom:
            - secretRef: {name: graph-loader-env}     # NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD, KAFKA_BOOTSTRAP_SERVERS
          env:
            - {name: HOME, value: /tmp}
          ports:
            - {name: health, containerPort: 8081}
          livenessProbe:
            httpGet: {path: /healthz, port: health}
            initialDelaySeconds: 30
            periodSeconds: 15
            failureThreshold: 3
          readinessProbe:
            httpGet: {path: /readyz, port: health}
            periodSeconds: 10
          resources:
            requests: {memory: 6Gi}
            limits: {memory: 6Gi}
          volumeMounts:
            - {name: work, mountPath: /work}
      volumes:
        - name: work
          persistentVolumeClaim: {claimName: graph-loader-work}      # schema.yaml and runs/
```

The schema file must be placed on that volume (or mounted from a ConfigMap
at a different path, with `--config` pointing at it).

## 8. Limits

* **Stream start-up rebalance (known, both runtimes).** A stream run can exit `1`
  shortly after a start when a replica is revoked a partition while it has writes
  in flight. In a container the restart policy brings it back and the next start
  recovers with no data impact; the trigger and mitigations (keep the broker's
  `group.initial.rebalance.delay.ms` at `3000`, run under a restart manager) are in
  [Process runtime section 10](PROCESS_RUNTIME.md#10-limits-and-what-is-not-covered).
  A later version is planned to fix it in the product.
* **`--metrics-addr 0.0.0.0` and `targets.json`.** The role endpoints bind
  `0.0.0.0`, but `<attempt>/metrics/targets.json` lists them as `127.0.0.1:<port>`
  (the runtime writes loopback for a wildcard address). A Prometheus in another
  container or pod cannot use the file as is; it works only for a scraper in the
  same network namespace (a sidecar that reads the file from the volume). There is
  no aggregated endpoint. See [Process runtime section 6](PROCESS_RUNTIME.md#6-logs-metrics-and-health).
* **`TINI_SUBREAPER` when the PID namespace is shared.** `tini` reaps orphans
  because it is PID 1. If the container does not own its PID namespace
  (`docker run --pid=host`, `--pid=container:<other>`, Compose `init: true`, a
  Kubernetes pod with `shareProcessNamespace: true`), `tini` is not PID 1, warns,
  and does not receive orphaned processes: planner workers and other
  grandchildren can then stay as zombies. Set `TINI_SUBREAPER=1` in the
  container's environment so `tini` registers as a child subreaper and reaps
  them. Avoid `--pid=host` for this container in any case: it puts the
  loader's processes in the host's PID namespace. The live cell showed
  no zombie after 20 orphaned child exits with `tini` as PID 1, and a control
  container whose PID 1 is not an init left exactly 20, so the check bites.
  **Untested:** the shared-namespace case (`TINI_SUBREAPER`) itself.
* **One container, one machine.** Roles cannot span hosts, and there are no
  per-role memory limits: one shared limit plus the planner budget.
* **Not run in a container (so far):** a Compose file, any Kubernetes manifest,
  the arm64 image, cgroup v1 hosts, Docker Desktop, `--cpus`/`LOADER_CPUSET`.
  The container was exercised with stream loads, plus one planned bulk load under
  a memory limit (sizing only; no bulk kill or OOM cell in a container); the bulk
  code is the same process runtime as the host shape.
* Linux only, kernel 5.3 or newer, amd64 and arm64
  ([platform](PROCESS_RUNTIME.md#1-choosing-a-runtime)).
