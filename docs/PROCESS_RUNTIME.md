# Process runtime: run graph-loader on a host or as a service

By default `graph-loader start` runs every role (node loaders, relationship
loaders, planned dispatcher and writers, lease server, retry consumer) as its
own Docker container. With `--runtime process` the same roles run as **child
processes of the one `graph-loader start` command**: no Docker daemon, no
network, no images. This page covers that runtime on a Linux host, from a shell
or as a systemd service.

Related: [CLI reference](CLI.md) | [Installed runtime state](CLI_RUNTIME.md) |
[Docker execution](DOCKER_EXECUTION.md) | [Bulk load](BULK_LOAD.md) |
[Incremental load](INCREMENTAL_LOAD.md) | [Monitoring](MONITORING.md) |
[systemd example](../examples/systemd/README.md)

Contents: [Choosing a runtime](#1-choosing-a-runtime) |
[Prerequisites and install](#2-prerequisites-and-install) |
[Quick start](#3-quick-start) | [Options and environment](#4-options-and-environment) |
[The run directory](#5-the-run-directory) | [Logs, metrics and health](#6-logs-metrics-and-health) |
[Stopping and recovery](#7-stopping-and-recovery) | [systemd service](#8-systemd-service) |
[Sizing](#9-sizing) | [Limits](#10-limits-and-what-is-not-covered)

## 1. Choosing a runtime

| Shape | How it runs | Choose it when |
| --- | --- | --- |
| Container per role (default) | `graph-loader start` on a host with a Docker daemon; `--runtime docker`. Every role is a container from the versioned images. | You already run Docker, or you are on macOS (Docker Desktop is not a supported load platform, see [Support and limits](../README.md#support-and-limits)) and want the tested default. |
| Single container | The CLI image runs `graph-loader start --runtime process`, every role a child process in that one container. | You deploy by container image and will not mount the Docker socket: [Single container](SINGLE_CONTAINER.md). |
| Host process or service | `pip install` plus `graph-loader start --runtime process` in a shell, or as a systemd service ([section 8](#8-systemd-service)). | You have a Linux machine or VM and no Docker daemon, or you want systemd to supervise the command. |

Select the runtime with `--runtime {docker,process}` on `start`, `stop`,
`status` and `replay-dlq`, or with the environment variable
`GRAPH_LOADER_RUNTIME` (the flag wins). The default is `docker`. Any other value
is refused (`stage=runtime reason=GRAPH_LOADER_RUNTIME must be one of docker,
process`, exit 1). `replay-dlq` only talks to Kafka and behaves the same in both
runtimes.

**Platform.** The process runtime is supported on **Linux only**, kernel 5.3 or
newer (it signals roles through `pidfd`), with `/proc` mounted. On macOS (or any
other system) it is refused before anything starts:
`the process runtime is supported on Linux only (this is Darwin); use --runtime
docker`. Windows is not supported natively. WSL2 runs a Linux kernel, so the
process runtime may work there; that is untested.

What stays the same: Kafka and Neo4j semantics, the schema, consumer groups,
the dead-letter queue, ordering, the roles' commands and arguments, and the
supervision policy (a role that dies fails the run exactly as it does in Docker
mode; the only in-run replacement is the stream lease-server restart, which
rebinds the same port). Restarting a failed run is up to whoever runs the
command: you, or systemd.

| | `--runtime docker` | `--runtime process` |
| --- | --- | --- |
| A role is | a container (`init` as PID 1) | a child process in its own session, started `python -I -u -m neo4j_graph_loader._child <role module>` |
| Needs | Docker daemon, a user-defined network, the four images | Linux >= 5.3, the installed package |
| Role addresses | container DNS names and published loopback ports | `127.0.0.1` ports reserved per run |
| Neo4j and Kafka addresses seen by roles | `LOADER_NEO4J_URI` (`bolt://neo4j:7687`), `LOADER_KAFKA_BOOTSTRAP_SERVERS` (`kafka:29092`) | the CLI's own `NEO4J_URI` and `KAFKA_BOOTSTRAP_SERVERS`; `LOADER_*` still wins when you set it |
| Roles' memory | one limit per container | one shared limit for the command and all roles; planner sized from it ([section 9](#9-sizing)) |
| `kill -9` of the CLI | every container keeps running | the roles stop on their own within seconds ([section 7](#7-stopping-and-recovery)) |
| `stop` | stops node-loader containers | signals the run's CLI, which performs its ordered stop |
| Image options and `GRAPH_LOADER_*_IMAGE` | used | refused (`--network`, `--skip-image-build`) or not read |

## 2. Prerequisites and install

* Linux (kernel 5.3 or newer, `/proc` mounted) and CPython 3.12.
* Neo4j 5.26 Enterprise and a Kafka broker that the host can reach. The roles
  run on the same host and use the same addresses as the command. Docker is not
  needed. (`pip` still installs the `docker` Python package, because it is one of
  the eight mandatory dependencies; it is not used in process mode.)
* A directory the command may write to for run state (`--run-dir BASE`).
* Optional: cgroup v2 with the memory controller, so the planner can size
  itself inside a memory limit ([section 9](#9-sizing)).

Install the wheel into a virtual environment:

```bash
python3.12 -m venv ~/graph-loader/venv
~/graph-loader/venv/bin/python -m pip install "neo4j-graph-loader==<version>"     # or ./neo4j_graph_loader-<version>-py3-none-any.whl
~/graph-loader/venv/bin/graph-loader --version
```

Children are started with the same interpreter as the command
(`sys.executable`), in isolated mode (`-I`), so a stray `PYTHONPATH` or user
site cannot change what a role imports. A role is a module of the installed
package; nothing is copied or built.

## 3. Quick start

Export the services and credentials (the command does not read `.env`). In
process mode only the host-side variables matter:

```bash
export KAFKA_BOOTSTRAP_SERVERS=kafka.example.internal:9092
export NEO4J_URI=bolt://neo4j.example.internal:7687
export NEO4J_USERNAME=neo4j NEO4J_PASSWORD=...           # NEO4J_DATABASE=... optional
```

**Bulk load.** Publish your node and relationship records first
([Bulk load guide](BULK_LOAD.md)), then:

```bash
graph-loader validate-schema --config my_graph.yaml
graph-loader start --mode bulk --runtime process --config my_graph.yaml --run-dir ~/graph-loader/runs
```

The command logs `stage=runtime directory=<attempt>` (save it), applies the
schema, runs the planner and the roles as children, verifies the load and exits
`0`. The bulk default is planned mode (a schema that enables relationship
`dead_letter` runs lease mode instead, see [CLI](CLI.md#edge-mode-selection-bulk));
in planned mode the process runtime logs a `stage=sizing ...` line before the
planner starts ([section 9](#9-sizing)). Exit
codes are the same as in Docker mode ([CLI reference](CLI.md#exit-codes)).

**Stream.** Create the topics first ([Incremental load guide](INCREMENTAL_LOAD.md)), then keep the
command in the foreground:

```bash
graph-loader start --mode stream --runtime process --config my_graph.yaml \
    --run-dir ~/graph-loader/runs --health-addr 127.0.0.1:8081
```

From a second shell:

```bash
curl -fsS http://127.0.0.1:8081/readyz                      # 200 once every role is ready
graph-loader status --runtime process --run-dir ~/graph-loader/runs
graph-loader stop   --runtime process --run-dir ~/graph-loader/runs      # ordered stop, the start command exits 0
```

`Ctrl-C` or `kill -TERM <pid of the start command>` does the same as `stop`. A
stream run that was supervising exits `0` when stopped.

A stream schema that declares **no relationship types** behaves differently by
runtime. In Docker mode `start --mode stream` launches the node loaders and
returns `0`, leaving the containers running unsupervised. In process mode the
loaders are children of the command and cannot outlive it, so the command stays
in the foreground as their supervisor: it exits `0` when stopped (`stop`,
`SIGTERM`, `Ctrl-C`; the loaders are stopped in order) and `1` when a node
loader dies (it stops the rest), like every other stream run. This suits
`Restart=on-failure` as well.

## 4. Options and environment

Flags added by the process runtime (verify with `graph-loader start --help`).
Process-only flags are refused with `--runtime docker`
(`... applies to --runtime process only`); `--network` (even with its default
value) and `--skip-image-build` are refused with `--runtime process`
(`... applies to Docker containers and is not accepted with --runtime process`).

| Flag | Default | Applies to | Meaning |
| --- | --- | --- | --- |
| `--runtime {docker,process}` | `docker`, or `GRAPH_LOADER_RUNTIME` | `start`, `stop`, `status`, `replay-dlq` | How roles run. |
| `--run-dir BASE` | OS temporary directory | `start`; `stop` and `status` (process) | `start`: where the attempt directory is created. `stop`/`status`: the attempt directory, or the base given to `start`. Give `start` a directory you keep (a service needs one). |
| `--metrics-addr HOST[:PORT]` | `127.0.0.1`, an OS-assigned port per role | `start` (process) | Where every role binds its Prometheus endpoint. Use `0.0.0.0` only when something outside the host must scrape it; the endpoint is unauthenticated. A fixed port suits a single role only. |
| `--health-addr HOST:PORT` | off | `start` (process) | Serve `/healthz` and `/readyz` there (`:PORT` and `PORT` bind `127.0.0.1`; port `0` picks one, logged as `stage=health addr=... port=...`). |
| `--health-stale-after SECONDS` | `30` | `start` (process) | `/healthz` turns unhealthy when the supervision loop has not ticked for this long. A healthy tick can take several seconds when the brokers are slow, so do not set it below about 10. |
| `--child-logs {relay,files}` | `relay` | `start` (process) | `relay` also prints each role's output to the command's stdout with a `[name]` prefix; `files` keeps it only in `<attempt>/logs/`. |
| `stop --recover` | off | `stop` (process) | Stop what an abruptly killed command left behind ([section 7](#7-stopping-and-recovery)). Refused with `--runtime docker`. |

All other `start` flags, the planned-mode tuning, the `--run-id` and the stream
options behave as documented in the [CLI reference](CLI.md).

**Environment.** Roles do not inherit the command's environment. A child gets
only `PATH`, `HOME`, `LANG`, `LC_ALL`, `TZ` and `TMPDIR` from it, plus exactly
the variables the CLI builds for that role (the same ones Docker mode passes:
`NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`, `NEO4J_DATABASE` when set,
`KAFKA_BOOTSTRAP_SERVERS`, the forwarded `GRAPH_LOADER_METRICS_ENABLED`,
`GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S`, `GRAPH_LOADER_VERSION` and
`GRAPH_LOADER_EDGE_EXPLICIT_LOCKS`, and role specifics). Anything else you export
(proxy settings, extra Kafka client variables) does not reach the roles. No
`PYTHON*` variable ever does.

| Variable | In process mode |
| --- | --- |
| `NEO4J_URI`, `KAFKA_BOOTSTRAP_SERVERS` | Used by the command and handed to every role. Roles keep the value they started with, so a Neo4j restart needs the same address afterwards (a fixed port or DNS name, not a re-published random port). |
| `LOADER_NEO4J_URI`, `LOADER_KAFKA_BOOTSTRAP_SERVERS` | Not needed. If set, the roles use them instead of the two variables above. |
| `LOADER_CPUSET` (for example `12-15,18`) | Each role pins itself to these CPUs (`sched_setaffinity`) before it starts. The command itself and the planner workers are not pinned. An invalid value stops that role from starting. |
| `GRAPH_LOADER_RUNTIME` | Default for `--runtime`. |
| `GRAPH_LOADER_METRICS_ENABLED=0` | Disables the roles' metrics endpoints; their `targets.json` entry has no target ([section 6](#6-logs-metrics-and-health)). |
| `GRAPH_LOADER_METRICS_LISTEN` | Set by the runtime for each role from `--metrics-addr` (`host:port`, port `0` = OS-assigned). It replaces `GRAPH_LOADER_METRICS_PORT` for roles. Not an operator setting. |
| `GRAPH_LOADER_METRICS_BIND_ADDR` | Unchanged: the command's own endpoint, loopback unless you set it. |
| `GRAPH_LOADER_NODE_IMAGE`, `GRAPH_LOADER_EDGE_IMAGE`, `GRAPH_LOADER_CLOCK_IMAGE` | Not read. |

## 5. The run directory

Each `start` creates a private attempt directory (mode 0700) named
`graph-loader-<random>` under `--run-dir BASE`. Besides the files described in
[CLI runtime](CLI_RUNTIME.md) it holds the process runtime's state:

| Path | Purpose |
| --- | --- |
| `registry.jsonl` | Append-only record of the command and every child it started: pid, start time, boot id, ports, labels, exit receipts. Each line is checksummed and `fsync`ed. `stop`, `status` and `stop --recover` work only through it. |
| `logs/<name>.log` | Each child's stdout and stderr, size-capped at 8 MiB with one rotated predecessor `<name>.log.1`. This is the source of truth; `<name>` looks like `graph-loader-node-Citizen-0`. |
| `work/<name>/` | The child's working directory. **A relative path that a role opens (for example a schema's `rejection_log_path: var/rejections/...`) resolves under `<attempt>/work/<name>/`**, not under the directory you started the command from. The outputs the CLI manages (`rejections/`, `plans/`, `spill/`, `timings/`) are absolute paths in the attempt directory. |
| `metrics/targets.json` | Prometheus `file_sd` file ([section 6](#6-logs-metrics-and-health)). |

Attempt directories are never removed automatically, after success or failure
(registry, logs, retained planner files). Remove old ones yourself once no
process of that run is left.

## 6. Logs, metrics and health

**Logs.** With `--child-logs relay` (default) each complete line a child prints
also goes to the command's stdout as `[<name>] <line>`, through a bounded queue
and one writer thread. If stdout is stuck the queue fills and further relay
lines are dropped (and counted, reported at close); the child is never blocked
and the log file stays complete. Under systemd stdout is the journal.

**Metrics.** Every role binds its endpoint on `--metrics-addr` with an
OS-assigned port, never the shared `9464`. The command rewrites
`<attempt>/metrics/targets.json` (temp file, `fsync`, rename, so a reader never
sees a partial file) every time a role becomes ready, exits or is replaced. It is
a Prometheus [`file_sd`](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#file_sd_config) file:

```yaml
scrape_configs:
  - job_name: graph-loader-process
    file_sd_configs:
      - files: ["/home/me/graph-loader/runs/graph-loader-*/metrics/targets.json"]
```

One group per live role with labels `exporter`, `runtime="process"`, and the
role's own labels (`component`, `kind`, `run_id`, `node_label`, `edge_type`,
`replica_id`, ...); the command's own endpoint is listed as `exporter="cli"`. A
role whose endpoint is disabled or could not bind gets a group with no targets
and a `metrics_state` label (`disabled`, `failed`); the role keeps writing. Exited
roles disappear. Targets are written as `<metrics host>:<port>` (`127.0.0.1` when
you pass `0.0.0.0`), so a Prometheus on another host or in another pod cannot use
the file as is. The metric names are in [Monitoring](MONITORING.md).

**Health.** With `--health-addr` the command serves two paths and 404 for
anything else (JSON bodies):

| Path | 200 when | 503 when |
| --- | --- | --- |
| `/healthz` | the supervision loop has ticked within `--health-stale-after` (or has not started yet) and the registry is writable | the loop stalled or the registry is broken: `{"status": "unhealthy"}` |
| `/readyz` | at least one role is running, every launched role has reported ready, none has failed, and the run is not stopping | no role yet, a role still starting (`pending`), a role exited non-zero without being asked (`failed`), or a stop is in progress (`stopping`) |

`/readyz` bodies carry `roles`, `running`, `pending`, `failed` and `stopping`.
A role that finished with exit 0 on its own (bulk work done) counts as neither
ready nor failed. Bind health to loopback unless a probe outside the host needs
it.

## 7. Stopping and recovery

**Graceful stop.** `Ctrl-C`, `SIGTERM` to the `start` command, or
`graph-loader stop --runtime process` from another shell. The command performs
the same ordered stop as in Docker mode (edge loaders and retry consumer, then
node loaders, then the lease server) with the same grace per role (30 s for node
loaders, 10 s for the lease server, then `SIGKILL`, reported as exit `137`). A
role that gets its stop before it has reported ready has the stop held until it
is ready (at most 5 seconds after its launch), so the signal cannot be lost. Planner workers are stopped before any role is
signalled. Exit codes are those of [Docker mode](CLI.md#exit-codes): a supervising
stream run exits `0`, a stopped bulk run `1`.

**Second shell.** `stop` opens a pidfd to the run's registered command,
checks its start time and boot id against `registry.jsonl`, and signals it with
`SIGTERM`; it never signals a role directly, and refuses a pid that has been
reused by something else. `stop --loader LABEL` signals only that label's
registered node loaders. Because the supervisor notices a node loader that
exits during a stream run, stopping one label of a stream run with relationships
stops the whole run with exit `1`. `status` lists the node loaders from the
registry in the Docker-mode format (`Loader <name> (<id>): running [Node: <label>]`).
Pass `--run-dir` pointing at the attempt directory, or at the base given to
`start`: with the base, exactly one live run under it is chosen, otherwise the
command lists the ambiguity and asks for the attempt directory. Without
`--run-dir` the base is the OS temporary directory.

**Lifeline: the command dies.** Each child holds a control socket to the command.
If the command disappears without cleaning up (`kill -9`, the OOM killer), the
child sees end-of-file, points its stdout and stderr at `/dev/null`, sends itself
`SIGTERM` so the role's ordinary graceful shutdown runs, and exits hard (exit
`137`) 30 seconds later if it has not finished. Planner workers and the
multiprocessing helper watch their own parent-death pipe and exit at once, even in
the middle of a Kafka scan. Measured after a `SIGKILL` of the command: a fleet of 6
children was gone in 0.155 s, a planned bulk run in 0.484 s and 1.025 s (the
test asserts a 30 s ceiling). The lifeline is a guarantee of the bare host shape.
Under a manager the manager's own rules apply (systemd: [section 8](#8-systemd-service)).

**`stop --recover`.** For a run whose command died abruptly:

```bash
graph-loader stop --runtime process --recover --run-dir ~/graph-loader/runs     # or the attempt directory
```

It refuses while the run's command is alive (`use graph-loader stop (without
--recover)`). Otherwise it signals every process the registry shows as started,
not exited and still having the same pid, start time and boot id (children,
planner workers and helper) with `SIGTERM` through a pidfd, `SIGKILL` after 15
seconds, appends a `recovered` record, and logs:

```text
stage=recover attempt=<dir> signalled=[...] killed=[...] remaining=[...]
stage=recover retained (never deleted) files=<n>: plans/edge-plan.json.lock, plans/edge-plan.json.progress, ...
```

Normally `signalled=[]` because the lifeline already stopped everything. It
**deletes nothing**: spill, plan, ledger, progress, lock and rejection files stay
and are only listed (first 50). It exits `0`, or `1` if a process is still alive
after the `SIGKILL`.

**After `kill -9` of a planned bulk run.** The processes are gone within seconds,
but the run's leftovers remain:

* in the attempt directory: `registry.jsonl`, `logs/`, and the planner's files
  (`plans/edge-plan.json.lock`, `plans/edge-plan.json.progress`, and
  `plans/edge-plan.json` once planning finished); a killed planner cannot write a
  failure marker or clean its spill directory;
* in Kafka: the run's chunk topic `graph-loader-edge-chunks-<run id>` (or your
  `--plan-topic`), created with unlimited retention. Nothing deletes it for you.

Recover in this order:

1. Run `stop --recover` as above and check that `remaining=[]`.
2. Find the dead run's chunk topic: the command logged it as
   `stage=edge-plan-start plan_id=... topic=<topic>` (journal or your terminal), or
   list the candidates:

   ```bash
   kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list | grep graph-loader-edge-chunks
   ```

3. Delete that one topic (not the ones that may belong to a live run). On the
   repository's Compose stack the tool is inside the broker container:

   ```bash
   kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --delete --topic graph-loader-edge-chunks-<run id>
   docker exec graph-loader-kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic graph-loader-edge-chunks-<run id>   # Compose stack
   ```

   Wait until Kafka has finished deleting it before reusing the name.
4. Rerun. A random `--run-id` is always safe: every write is an idempotent
   `MERGE`, planned mode re-plans from scratch, and each start has a fresh attempt
   directory. To reuse an explicit `--run-id`, add `--replace-stale-plan-topic`;
   in process mode its "no container of that run is alive" check covers the roles
   of the current command **and** any live, identity-checked process of that run id
   recorded in the registries of the other attempt directories under the same
   `--run-dir` base. It refuses and names them, and the message gives the
   `graph-loader stop` / `stop --recover` command to use. A command started with a
   different `--run-dir` base is not visible to the check, so step 1 is still what
   proves nothing is left.
5. When `remaining=[]`, the old attempt directory is yours to keep for diagnosis
   or remove.

A stream run after `kill -9` needs no topic cleanup: start the same command again;
it waits for any old consumer-group members to expire (up to
`session_timeout_ms` + 5 s by default, or `--stream-group-member-wait-seconds`)
and resumes from the committed offsets. Redelivery is possible; writes are idempotent.

**Docker and process mode after `kill -9` of the command.**

| | `--runtime docker` | `--runtime process` |
| --- | --- | --- |
| What happens to the roles | Every container keeps running; nothing cleans them up. | Each child stops itself within seconds (lifeline); `stop --recover` handles anything it could not reach, for example a process frozen with `SIGSTOP`. |
| Recovery | List, then remove only the run's containers ([USAGE: stopping a run](USAGE.md#stopping-a-run-safely-sigterm-sigint-sigkill-and-container-cleanup)). | `stop --recover`, which acts only through the registry after identity checks. |
| Left behind (planned bulk) | the chunk topic | the chunk topic, plus the attempt directory's planner files |
| Chunk topic | delete it ([Bulk load: re-running](BULK_LOAD.md#14-re-running-and-recovering)) | delete it (steps above) |

## 8. systemd service

[`examples/systemd/graph-loader.service`](../examples/systemd/graph-loader.service) runs one
`graph-loader start --mode stream --runtime process` as a systemd **user**
service; systemd supervises the command and the command supervises the roles.
It was exercised on a real systemd 255 user manager. Files:
[`graph-loader.service`](../examples/systemd/graph-loader.service),
[`graph-loader.env.example`](../examples/systemd/graph-loader.env.example),
[`README.md`](../examples/systemd/README.md).

```bash
python3 -m venv ~/graph-loader/venv && ~/graph-loader/venv/bin/pip install "neo4j-graph-loader==<version>"
install -d -m 700 ~/.config/graph-loader
install -m 600 examples/systemd/graph-loader.env.example ~/.config/graph-loader/graph-loader.env
$EDITOR ~/.config/graph-loader/graph-loader.env ~/graph-loader/schema.yaml
install -D -m 644 examples/systemd/graph-loader.service ~/.config/systemd/user/graph-loader.service
systemctl --user daemon-reload && systemctl --user start graph-loader
curl -fsS http://127.0.0.1:8081/readyz
journalctl --user -u graph-loader -f
loginctl enable-linger "$USER"        # keep it running after you log out (your administrator must allow it)
```

What the unit does:

| Setting | Why |
| --- | --- |
| `ExecStart=.../graph-loader start --mode stream --config ... --runtime process --run-dir %h/graph-loader/runs --health-addr 127.0.0.1:8081` | Foreground, no daemonizing, no PID file. `--run-dir` keeps every attempt in one place; `--health-addr` gives `/readyz`. |
| `Type=exec` | The unit counts as started once the command is executed; use `/readyz` for readiness. |
| `EnvironmentFile=%h/.config/graph-loader/graph-loader.env` | Connection settings and the password, mode `0600` (`KEY=value`, no quotes, no `export`). The password never appears on the command line. |
| `KillMode=mixed`, `KillSignal=SIGTERM` | `systemctl stop` sends `SIGTERM` to the `graph-loader` process only, which runs the ordered stop and exits `0`. After it exits, systemd `SIGKILL`s whatever is left in the unit's cgroup. |
| `TimeoutStopSec=120`, `SendSIGKILL=yes` | The ceiling for the whole stop before `SIGKILL` of the unit. An idle fleet stops in about a second (measured 1.0 s). |
| `Restart=on-failure`, `RestartSec=10`, `StartLimitBurst=5` in `StartLimitIntervalSec=600` | A role that dies makes the command stop the fleet and exit non-zero; systemd starts it again, at most 5 times in 10 minutes. |
| `OOMPolicy=continue` | When the kernel's OOM killer kills one of the unit's processes, systemd's default (`stop`) stops the **whole unit** at once and the command never reports it. With `continue` the command sees the killed role, logs `stage=oom ... victim unknown` once, fails the run (exit `1`) and `Restart=on-failure` starts it again ([section 9](#9-sizing)). |
| `#MemoryMax=6G`, `#MemorySwapMax=0` (commented) | Optional hard limit for the command, the roles and the planner together; see [section 9](#9-sizing). |

**Do not change `KillMode=mixed`.** With systemd's default `KillMode=control-group`
every process receives `SIGTERM` at once and the ordered stop is skipped: in the
host test the lease server exited before the last edge loader. `KillMode=process`
would leave the roles running. This unit form is the only one supported.

**Restart semantics.**

| Event | Result |
| --- | --- |
| `systemctl --user stop graph-loader` or `kill -s TERM` | Ordered stop, exit `0`, no restart, nothing left in the cgroup. Start it again yourself with `systemctl --user start graph-loader`. |
| A role dies | The command stops the fleet, exits non-zero; systemd restarts after `RestartSec`. |
| A role is killed by the OOM killer (`MemoryMax=` too low) | With the unit's `OOMPolicy=continue`: one `stage=oom` line in the journal, exit `1`, a restart; no duplicate fleet. With systemd's default `OOMPolicy=stop` the unit is stopped before the command can report. |
| `kill -9` of the command | systemd removes what is left in the unit's cgroup and restarts it. The new run waits for the old consumer-group members to expire and resumes from the committed offsets: about 48 s from the kill to `/readyz` in the host test (10 s `RestartSec` plus the member wait). There is exactly one fleet; the load completes with no duplicate relationships (idempotent writes, safe redelivery). |
| Clean exit `0` for another reason | Not restarted (`Restart=on-failure`). |

systemd is the only restart owner; the command never restarts itself. Old
attempt directories under `--run-dir` are kept; remove them yourself.
`stop --recover` is not needed under systemd, which cleans the cgroup.

**Logging.** With the default `--child-logs relay` every role's output reaches the
journal with its `[graph-loader-<role>-...]` prefix, next to the command's own
lines (`journalctl --user -u graph-loader`); the password is not logged. Per-child
files are also kept in `<attempt>/logs/`. Journal rate limits and retention for
very chatty fleets were not tested.

**User or system service.** The tested form is the user unit above
(`systemctl --user`, `~/.config/systemd/user/`, `WantedBy=default.target`). For a
system-wide unit install the same file under `/etc/systemd/system/`, replace the
`%h/...` paths with absolute ones, set `User=` (a dedicated account that owns the
schema, the env file and the run directory), use `WantedBy=multi-user.target`, and
drop `--user` from the commands. That adaptation is **untested**. Keep
`KillMode=mixed` in either form.

## 9. Sizing

Every process of the run (the command, the planner workers and all roles)
shares whatever memory limit the shell, container or unit has, so size the limit
for the sum.

**Per role.** The runtime reserves these sizes per role when it plans the
planner's budget (rounded-up peak resident sizes measured in live runs):

| Process | Reserve (MiB) | Measured peak RSS in the memory-limit run (MiB) |
| --- | --- | --- |
| `graph-loader` command | 200 | 170 |
| node loader | 150 | 97 |
| edge loader, retry consumer | 150 each | not measured separately |
| planned edge writer | 150 | 98 |
| planned dispatcher | 120 | 26 |
| lease server, clock | 100 each | not measured separately |
| planner worker | at least 256, at most 4096 (budget, not a peak) | 144 (budget 327) |

Rule of thumb: **required memory is about 200 + the sum of the role reserves +
the planner (workers x at least 256 MiB)**. In the measured planned bulk run
(1,400 MiB limit, 5 roles) the whole scope peaked at 405 MiB while the formula
total was 1,247 MiB; the formula is conservative on purpose (reserves are peaks,
and a 120-relationship fixture does not stress the planner), so large inputs
should be sized from your own measurement. Roles are reserved per schema
`replicas`; bulk auto-replicas derived from topic sizes can add node loaders
beyond the reserve, and then the limit itself bounds the run. Stream runs have no
planner; use the same reserves for `MemoryMax`.

**Planner budget (planned bulk, process mode only).** Before the planner starts
the command reads the cgroup v2 limits of its own cgroup and of every ancestor
(`/proc/self/cgroup`, `memory.max` and `memory.current` at each level; a
`systemd-run --user --scope -p MemoryMax=...` scope or a unit with `MemoryMax=`
shows up this way) and computes:

```text
headroom = min( MemAvailable, min over each level with a finite memory.max of max(0, memory.max - memory.current) )
reserved = 200 + sum of the role reserves above (nodes by schema replicas, dispatcher, planned writers)
planner  = max(0, headroom - reserved) * 0.8
workers  = --plan-workers (default min(4, CPUs)), reduced to floor(planner / 256) when 256 MiB each do not fit
per worker = min(4096, planner / workers) MiB    (replaces the automatic --plan-memory-mb)
```

It logs `stage=sizing planner_workers=... planner_memory_mb=... headroom_mb=...
reserved_mb=... planner_mb=... roles=...` and a note when it reduced the workers.
The run is **refused before any role starts** (`stage=sizing reason=...`, exit 1)
when not even one 256 MiB worker fits, when explicit `--plan-workers` x
`--plan-memory-mb` need more than `planner`, when a limit file exists but cannot
be read, or when **no cgroup v2 memory limit is visible at all** (a cgroup v1 host
or container, or no memory controller for the process: host `MemAvailable` would
size the planner for the whole machine, and a container with a limit would be
OOM-killed) and you did not give both settings. Giving both `--plan-workers` and
`--plan-memory-mb` is how you size by hand. A v2 hierarchy whose leaf says
`memory.max` = `max` (a bare host without a limit) is still bounded by
`MemAvailable`. Docker mode keeps its own, unchanged planner defaults.

In the measured limit runs a deliberately undersized `MemoryMax=900M` was refused
in seconds with no role launched and no OOM kill, an explicit 8 x 1024 MiB request
under `MemoryMax=1400M` was refused, and a `1400M` run reduced the planner from 4
to 1 worker and completed.

**OOM.** Process mode does not claim which process an out-of-memory kill hit. A
child that dies of `SIGKILL` is exit `137`, exactly as after a manual `kill -9`.
When the `oom_kill` counter in `memory.events` of the command's cgroup rose since
the last look, the command adds one group-level line to its output,
`ERROR stage=oom cgroup OOM kill observed; victim unknown (memory.events oom_kill
<before> -> <after>)`, and an `oom` record to `registry.jsonl`; it never names a
child. No rise (a manual kill), no cgroup v2 or no memory controller: nothing is
reported. Under systemd note the unit's `OOMPolicy=`: its default, `stop`, stops
the **whole unit** when the kernel kills one of its processes (the journal then
says a process of the unit was killed by the OOM killer, and the command is
stopped before it can report); the shipped unit sets `OOMPolicy=continue`, which
lets the command see the failure and apply its usual supervision (report, exit
`1`, restart). Keep that line if you copy the unit. Size the unit's `MemoryMax=` with
[the formula above](#9-sizing) so the planner and roles fit.

**CPU.** `LOADER_CPUSET=<cpus>` pins each role ([section 4](#4-options-and-environment)),
keeping the loaders off the cores that Neo4j and Kafka use. It is read per role;
the planner is not pinned.

## 10. Limits and what is not covered

* **Stream start-up revoke (both runtimes; seen in process-mode soaks).** A stream run can exit `1` shortly after a start
  when one replica of a label or type is revoked a partition while it has writes in flight
  (`Kafka partitions revoked with unresolved work`, then `node loader exited while the stream run was active`). Trigger:
  a replica joins its consumer group after the sibling already holds partitions, likely after a restart that waited for
  old members while a backlog built up, and when the broker's `group.initial.rebalance.delay.ms` is `0`. Effect: exit
  `1`, the next start recovers, no data impact. Mitigations: keep the broker default of `3000` ms and run the command
  under a restart manager ([section 8](#8-systemd-service)). A later version is planned to fix it in the product.
* Linux only (kernel >= 5.3). No macOS, no native Windows. One command, one
  machine: roles do not span hosts.
* No per-process memory limits; one shared limit and the planner budget above.
* Prometheus discovery by `file_sd` is host-local. A monitoring stack on another
  host or in another pod needs an aggregated endpoint; that is outside this page.
* The single container (the CLI image running `--runtime process`) is covered
  in [Single container](SINGLE_CONTAINER.md); its live verification is pending.
* **Kubernetes: untested.** The same command can run inside any Linux container
  that has Python 3.12 and the wheel. If you try it, map `/healthz` and `/readyz`
  to the liveness and readiness probes (`--health-addr 0.0.0.0:<port>`), set the
  termination grace period to at least 120 seconds, and set the container memory
  limit from [section 9](#9-sizing). Treat a role death (non-zero exit of the
  command) as the pod's cue to restart. No manifest has been run on a real
  cluster.
* Not tested: `Restart=on-failure` crash loops against `StartLimitBurst`, a
  `MemoryMax=` set in the unit itself (the limit mechanism was tested in a
  systemd user scope), the system (root) unit, and journald rate limits.
