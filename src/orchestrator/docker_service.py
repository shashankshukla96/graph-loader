import hashlib
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
import docker
from typing import Callable, List, Dict, Optional, Sequence
from src.loader.lease_client import (
    BUCKET_LEASE_ADDRESS_ENV, EDGE_OUTER_BATCH_ENV, LEASE_ADDRESS_ENV, LEASE_BUCKETS_ENV, LEASE_KEYED_LABELS_ENV,
    MAX_EDGE_OUTER_BATCH_SIZE, lease_scheme_token, parse_keyed_labels, parse_lease_address,
    parse_lease_bucket_count,
)
from src.orchestrator.fleet_contract import parse_fleet_edge_types, select_shared_fleet_edges
from src.orchestrator.lease_server import DEFAULT_BUCKET_LEASE_PORT
from src.orchestrator.planned_dispatcher import DEFAULT_DISPATCHER_PORT
from src.utils.consumer_groups import edge_group, stream_retry_group
from src.utils.metrics import DEFAULT_METRICS_PORT

logger = logging.getLogger(__name__)

_PLAN_MOUNT = "/app/edge-plan.json"
# Read by the lease coordinator in the edge loader and the retry consumer
# (src/loader/edge_execution.py LEASE_MERGE_ROWS_ENV). Not imported from there:
# that module pulls in the Neo4j driver stack the launcher does not need.
LEASE_MERGE_ROWS_ENV = "GRAPH_LOADER_LEASE_MERGE_ROWS"
from src.utils.schema_loader import SchemaLoadError, load_schema


def _metrics_port(env: Optional[Dict[str, str]] = None) -> int:
    """The metrics port this launcher's containers should publish.

    When ``env`` is given, it is treated as *authoritative* — it is the
    final, per-launch environment dict a caller is about to hand to
    ``containers.run``, after any explicit ``environment=`` override has
    already been applied, so it is exactly what the container will
    actually see. A missing key in a *given* ``env`` means the container
    will see no value at all (i.e. its own in-container default, 9464 /
    enabled) — it must NOT fall back to the host's environment in that
    case, since a caller-supplied ``environment=`` override deliberately
    does not forward the host's own vars (P6-S2 review finding F2: an
    earlier version of this function fell back to the host env even when
    an explicit, host-var-omitting ``environment=`` was given, so the
    published port/label could read the host's value while the container
    itself bound the unrelated default). Only when ``env`` itself is
    ``None`` (no per-launch dict available at all) does this fall back to
    the host's own environment.

    Never raises, and never returns a value Docker's own port-mapping API
    would reject: an unparseable *or* out-of-range (must be 1-65535; ``0``
    is rejected too — it would silently ask Docker/the container to pick a
    random port while every Docker label and env var still says ``"0"``)
    value falls back to :data:`DEFAULT_METRICS_PORT` (9464 — an
    orchestrator correction, not Prometheus's own default port 9090; see
    `plan/v6/completion_slice-2.md`) with a logged warning (P6-S2 review
    finding F1: a typo'd port used to be able to fail every container
    launch in a run).
    """
    raw = env.get("GRAPH_LOADER_METRICS_PORT") if env is not None else os.environ.get("GRAPH_LOADER_METRICS_PORT")
    if not raw:
        return DEFAULT_METRICS_PORT
    try:
        port = int(raw)
    except ValueError:
        logger.warning(
            "stage=metrics reason=invalid_port_env value=%s falling back to default=%s",
            raw, DEFAULT_METRICS_PORT,
        )
        return DEFAULT_METRICS_PORT
    if not 1 <= port <= 65535:
        logger.warning(
            "stage=metrics reason=out_of_range_port_env value=%s falling back to default=%s",
            raw, DEFAULT_METRICS_PORT,
        )
        return DEFAULT_METRICS_PORT
    return port


def _neo4j_username() -> str:
    """The username the CLI itself connects with: ``NEO4J_USERNAME``, else
    ``NEO4J_USER``, else ``neo4j`` (same chain as ``src.cli.get_neo4j_credentials``),
    so containers authenticate as the same user as the CLI."""
    return os.environ.get("NEO4J_USERNAME", os.environ.get("NEO4J_USER", "neo4j"))


def _metrics_env_passthrough() -> Dict[str, str]:
    """``GRAPH_LOADER_METRICS_PORT``/``GRAPH_LOADER_METRICS_ENABLED``, when
    set on the host running the CLI, forwarded into a launched container's
    own environment (plus ``GRAPH_LOADER_VERSION``, the version label, and
    ``GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S``; the snapshot
    *path* is deliberately not forwarded -- each container gets its own) — the same "if set on the host, thread it through"
    pattern already used for ``GRAPH_LOADER_EDGE_EXPLICIT_LOCKS``."""
    env: Dict[str, str] = {}
    if os.environ.get("GRAPH_LOADER_METRICS_ENABLED") is not None:
        env["GRAPH_LOADER_METRICS_ENABLED"] = os.environ["GRAPH_LOADER_METRICS_ENABLED"]
    if os.environ.get("GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S"):
        env["GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S"] = os.environ["GRAPH_LOADER_METRICS_SNAPSHOT_INTERVAL_S"]
    if os.environ.get("GRAPH_LOADER_METRICS_PORT"):
        env["GRAPH_LOADER_METRICS_PORT"] = os.environ["GRAPH_LOADER_METRICS_PORT"]
    if os.environ.get("GRAPH_LOADER_VERSION"):
        env["GRAPH_LOADER_VERSION"] = os.environ["GRAPH_LOADER_VERSION"]
    return env


def _metrics_enabled(env: Optional[Dict[str, str]] = None) -> bool:
    """Whether this container's metrics server will actually be listening,
    mirroring ``start_metrics_server``'s own check: unset means enabled;
    ``"0"`` and ``"false"`` (case-insensitive, whitespace-stripped) disable.
    ``env``, when given, is authoritative (see :func:`_metrics_port`'s
    docstring for why a missing key must not fall back to the host
    environment once a real per-launch dict exists) — only a ``None``
    ``env`` falls back to the host's own environment."""
    raw = env.get("GRAPH_LOADER_METRICS_ENABLED") if env is not None else os.environ.get("GRAPH_LOADER_METRICS_ENABLED")
    return (raw if raw is not None else "1").strip().lower() not in ("0", "false")


def _metrics_labels(kind: str, env: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """Docker labels Slice 3's Prometheus Docker-SD config will key off of.
    Labels only here — no ``docker_sd_configs``, no compose file, no
    Prometheus config; that is entirely Slice 3's job, per P6-S2's explicit
    scope boundary. Pass the launcher's final per-launch ``env`` dict (see
    :func:`_metrics_port`) so the label always matches what the container
    will actually bind, even when a caller passed an explicit
    ``environment=`` override.

    Includes ``graph_loader.metrics_enabled`` (P6-S2 review finding F3):
    with metrics disabled, the container still publishes the port mapping
    (harmless — nothing listens there) but a Slice 3 Docker-SD relabel rule
    needs a way to skip these targets instead of discovering one that will
    always read as "down".
    """
    return {
        "graph_loader.metrics_port": str(_metrics_port(env)),
        "graph_loader.kind": kind,
        "graph_loader.metrics_enabled": "1" if _metrics_enabled(env) else "0",
    }


def _metrics_ports_entry(env: Optional[Dict[str, str]] = None) -> Dict[str, tuple]:
    """One ``ports={...}`` entry: Docker-assigned ephemeral host port,
    exactly the ``bucket_lease_address``/``planned_dispatcher_addresses``
    precedent — two replicas of the same kind never collide on a host
    port no matter how many share one Docker host. Pass the launcher's
    final per-launch ``env`` dict; see :func:`_metrics_port`."""
    return {f"{_metrics_port(env)}/tcp": ("127.0.0.1", None)}


# Host directory of the relationship rejection logs: the classic and lease edge loaders and the
# planner (``<run id>-planner-<worker>.jsonl``) all write here.
REJECTION_DIR = "var/rejections"

def node_image() -> str:
    """Node-loader image tag (``GRAPH_LOADER_NODE_IMAGE``, default ``graph-loader-node:latest``)."""
    return os.environ.get("GRAPH_LOADER_NODE_IMAGE") or "graph-loader-node:latest"


def edge_image() -> str:
    """Edge/dispatcher/writer image tag (``GRAPH_LOADER_EDGE_IMAGE``, default ``graph-loader-edge:latest``)."""
    return os.environ.get("GRAPH_LOADER_EDGE_IMAGE") or "graph-loader-edge:latest"


def clock_image() -> str:
    """Global-clock image tag (``GRAPH_LOADER_CLOCK_IMAGE``, default ``graph-loader-clock:latest``)."""
    return os.environ.get("GRAPH_LOADER_CLOCK_IMAGE") or "graph-loader-clock:latest"


def _dns_safe_run_token(run_id: str) -> str:
    """Deterministic, restart-stable, DNS-label-safe token for ``run_id``.

    Story 10 E2E finding: the lease server is the one container other
    containers resolve *by name* (``bucket_lease_address(...,
    prefer_name=True)``), since its restart-transparent addressing (Story
    5) depends on the name staying stable across a mid-run restart. Every
    other loader container's own name is never looked up by anyone, so
    its existing ``run_id.encode().hex()`` suffix (used for readability
    and for the label-based orphan sweep) is harmless no matter how long
    ``run_id`` is. The lease server's name is different: a real DNS
    label is capped at 63 octets, and a normal ``uuid.uuid4().hex`` run
    id, once ``.encode('utf-8').hex()``-doubled, already exceeds that
    budget on its own -- confirmed against a real Docker network, where
    resolving such a name raises ``UnicodeError: label too long`` from
    the client's own IDNA encoding, not a Docker-side error, so nothing
    server-side catches it. A truncated digest keeps the name short and
    still deterministic per run id (so a supervised restart reuses the
    exact same name), at a collision probability irrelevant for one
    coordinator per concurrent run.
    """
    return hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:24]


def _mount_write_timing_directory(volumes: dict) -> bool:
    """Expose the optional host timing directory to loader containers."""
    host_directory = os.environ.get("GRAPH_LOADER_TIMING_DIR")
    if not host_directory:
        return False
    host_directory = os.path.abspath(host_directory)
    os.makedirs(host_directory, exist_ok=True)
    volumes[host_directory] = {"bind": "/app/write-timings", "mode": "rw"}
    return True

def _loader_resource_options() -> Dict[str, str]:
    """Pin loader containers to LOADER_CPUSET so they do not contend with Kafka or Neo4j."""
    cpuset = os.environ.get("LOADER_CPUSET", "").strip()
    return {"cpuset_cpus": cpuset} if cpuset else {}


# Each ``containers.run`` is a create + start + inspect round trip (~0.35s on
# the benchmark host); replicas are independent, so issue them concurrently.
_MAX_PARALLEL_LAUNCHES = 16


def _launch_all(
    launchers: Sequence[Callable[[], object]],
    rollback: Callable[[object], None],
) -> list:
    """Run every replica launcher concurrently, preserving replica order.

    If any launch fails, every replica that did start is rolled back and the
    lowest-index failure is re-raised, so a partial launch never leaks.
    """
    if len(launchers) <= 1:
        results = []
        try:
            for launch in launchers:
                results.append(launch())
        except Exception:
            for container in results:
                rollback(container)
            raise
        return results

    def attempt(launch: Callable[[], object]) -> tuple[object, Exception | None]:
        try:
            return launch(), None
        except Exception as exc:  # collected so siblings can be rolled back
            return None, exc

    with ThreadPoolExecutor(
        max_workers=min(len(launchers), _MAX_PARALLEL_LAUNCHES),
        thread_name_prefix="container-launch",
    ) as executor:
        outcomes = list(executor.map(attempt, launchers))
    errors = [error for _, error in outcomes if error is not None]
    if errors:
        for container, error in outcomes:
            if error is None:
                rollback(container)
        raise errors[0]
    return [container for container, _ in outcomes]


# F1: ``docker stop`` sends SIGTERM exactly once, then SIGKILLs after the
# grace period.  Sent the moment a container has started, that one SIGTERM can
# be lost: until the container's init (docker-init/tini, PID 1 of its PID
# namespace) has blocked signals for forwarding, the kernel discards a signal
# with the default disposition sent to that init.  The loader then never
# learns of the stop, runs on, and is SIGKILLed (exit 137).  A SIGTERM that
# does get through while the loader's interpreter is still starting kills it
# with the default action (exit 143).  Only launch-phase stops hit this (P0
# shut03/shut06; a probe of 300 launch-then-stop containers on one busy CPU
# gave 2 lost SIGTERMs and 8 exit-143s, and none of either with this hold).
#
# So the stop of a container this process launched is held until
#   * it has been up for STOP_SIGNAL_READY_SECONDS (the floor: the window in
#     which the interpreter installs its SIGTERM handler), and
#   * ``container.top()`` shows the init's child process: tini blocks every
#     signal before it forks, so a visible child proves the init forwards the
#     SIGTERM instead of losing it (a container without an init is ready
#     once its process is listed),
# capped at STOP_SIGNAL_READY_MAX_SECONDS after the launch.  If ``top()``
# fails, the floor alone applies.  Containers this process did not launch, and
# any older than the cap (every steady-state stop), are stopped exactly as
# before, with the same single SIGTERM and no extra API call.
STOP_SIGNAL_READY_SECONDS = 1.0
STOP_SIGNAL_READY_MAX_SECONDS = 5.0
_STOP_READY_POLL_SECONDS = 0.1
_LAUNCH_RECORD_TTL_SECONDS = 300.0
_INIT_COMMANDS = ("docker-init", "tini")
_launch_lock = threading.Lock()
_launched_at: Dict[str, float] = {}
_signal_ready: set = set()  # ids whose init was seen forwarding (child listed)
# Indirections so tests can drive the hold with a fake clock.
_monotonic: Callable[[], float] = time.monotonic
_sleep: Callable[[float], None] = time.sleep


def _record_launch(container: object) -> object:
    """Remember when this process started ``container`` (its Docker id)."""
    container_id = getattr(container, "id", None)
    if isinstance(container_id, str) and container_id:
        now = _monotonic()
        with _launch_lock:
            for stale in [key for key, at in _launched_at.items() if now - at > _LAUNCH_RECORD_TTL_SECONDS]:
                del _launched_at[stale]
                _signal_ready.discard(stale)
            _launched_at[container_id] = now
    return container


def _init_forwards_signals(container: object) -> bool:
    """Whether ``docker top`` shows the container ready to take a SIGTERM (raises if top fails)."""
    listing = container.top()
    titles = list(listing.get("Titles") or [])
    rows = [row for row in listing.get("Processes") or [] if row]
    column = titles.index("CMD") if "CMD" in titles else titles.index("COMMAND") if "COMMAND" in titles else -1
    commands = [str(row[column]) for row in rows]
    has_init = any(name in command for command in commands for name in _INIT_COMMANDS)
    return len(rows) >= 2 if has_init else len(rows) >= 1


def wait_until_stop_signal_ready(container: object) -> float:
    """Hold a stop until ``container`` can take its SIGTERM (see above); return the seconds held."""
    container_id = getattr(container, "id", None)
    if not isinstance(container_id, str) or not container_id:
        return 0.0
    with _launch_lock:
        launched = _launched_at.get(container_id)
        ready = container_id in _signal_ready
    if launched is None:
        return 0.0
    started = _monotonic()
    if started - launched >= STOP_SIGNAL_READY_MAX_SECONDS:
        return 0.0
    floor = STOP_SIGNAL_READY_SECONDS - (started - launched)
    if floor > 0:
        _sleep(floor)
    while not ready:
        try:
            ready = _init_forwards_signals(container)
        except Exception:
            break  # no process listing: the floor alone applies
        if ready:
            with _launch_lock:
                _signal_ready.add(container_id)
            break
        remaining = STOP_SIGNAL_READY_MAX_SECONDS - (_monotonic() - launched)
        if remaining <= 0:
            break
        _sleep(min(_STOP_READY_POLL_SECONDS, remaining))
    return _monotonic() - started


def _stop_quietly(container: object) -> None:
    try:
        wait_until_stop_signal_ready(container)
        container.stop()
    except Exception:
        pass


def _stop_and_remove_quietly(container: object) -> None:
    _stop_quietly(container)
    try:
        container.remove(force=True)
    except Exception:
        pass


class DockerService:
    def __init__(self, client: docker.DockerClient = None):
        self.client = client or docker.from_env()

    def _run_container(self, **kwargs):
        """``containers.run`` that records the launch time for a later stop (F1)."""
        return _record_launch(self.client.containers.run(**kwargs))

    def build_image(self, tag: str | None = None, dockerfile: str = "Dockerfile.node_loader"):
        self.client.images.build(path=".", dockerfile=dockerfile, tag=tag or node_image(), rm=True)

    def build_edge_image(self, tag: str | None = None, dockerfile: str = "Dockerfile.edge_loader"):
        """Build the dedicated relationship-loader image."""
        self.client.images.build(path=".", dockerfile=dockerfile, tag=tag or edge_image(), rm=True)

    def build_clock_image(self, tag: str | None = None, dockerfile: str = "Dockerfile.clock"):
        """Build the dedicated run-scoped global-clock image."""
        self.client.images.build(path=".", dockerfile=dockerfile, tag=tag or clock_image(), rm=True)

    def run_global_batch_clock(self, *, run_id: str, config_path: str,
                               fleet_edge_types: str, coordination_topic: str,
                               network: str = "graph-loader-net",
                               environment: Optional[Dict[str, str]] = None):
        """Launch one exact clock container; callers own only its returned object."""
        if not isinstance(run_id, str) or not run_id.strip() or not isinstance(coordination_topic, str) or not coordination_topic.strip():
            raise ValueError("global clock requires nonblank run_id and coordination_topic")
        parse_fleet_edge_types(fleet_edge_types)
        env = dict(environment) if environment is not None else {
            "KAFKA_BOOTSTRAP_SERVERS": os.environ.get("LOADER_KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        }
        if environment is None:
            env.update(_metrics_env_passthrough())
        encoded = run_id.encode("utf-8").hex()
        if not encoded:
            raise ValueError("global clock run_id must not be empty")
        volumes = {os.path.abspath(config_path): {"bind": "/app/runtime_schema.yaml", "mode": "ro"}}
        capture_timings = _mount_write_timing_directory(volumes)
        if capture_timings:
            env["GRAPH_LOADER_METRICS_SNAPSHOT_PATH"] = f"/app/write-timings/metrics-clock-{encoded}.prom"
        return self._run_container(
            image=clock_image(), name=f"graph-loader-clock-{encoded}",
            command=["--config", "/app/runtime_schema.yaml", "--run-id", run_id,
                     "--fleet-edge-types", fleet_edge_types, "--coordination-topic", coordination_topic],
            environment=env, network=network,
            labels={
                "app": "graph-loader", "component": "global-batch-clock", "run_id": run_id,
                **_metrics_labels("clock", env),
            },
            volumes=volumes,
            ports=_metrics_ports_entry(env),
            detach=True, init=True, remove=False,
        )

    def run_bucket_lease_server(
        self, *, run_id: str, bucket_count: int, network: str = "graph-loader-net",
        port: int = DEFAULT_BUCKET_LEASE_PORT, keyed_labels: Sequence[str] = (),
        quiet_period_seconds: float = 0.0, fence_seconds: float = 0.0, restart_attempt: int = 0,
    ):
        """Launch the run-scoped bucket-lease coordinator from the edge image.

        The coordinator rejects any client whose bucket count or keyed-label
        scheme differs, so every loader maps nodes to resources identically.

        ``quiet_period_seconds`` (P5-S1 Story 5) is nonzero only when
        relaunching after a restart (the P5-Q1 amendment's ``Q``);
        ``fence_seconds`` (review finding B4-1) is passed on every launch,
        including the first, since a mid-run disconnect is not
        restart-specific. ``restart_attempt`` (review finding C2-1) makes
        each relaunch's exit-time metrics snapshot path distinct, so a
        supervised restart never overwrites the previous generation's
        final counters.
        """
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("bucket lease server requires a nonblank run_id")
        parse_lease_bucket_count(bucket_count)
        scheme = lease_scheme_token(keyed_labels)
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError("bucket lease server port must be 1-65535")
        if quiet_period_seconds < 0 or fence_seconds < 0:
            raise ValueError("quiet_period_seconds and fence_seconds must be nonnegative")
        encoded_run_id = run_id.encode("utf-8").hex()
        env = {"PYTHONUNBUFFERED": "1", **_metrics_env_passthrough()}
        volumes: Dict[str, Dict[str, str]] = {}
        capture_timings = _mount_write_timing_directory(volumes)
        if capture_timings:
            env["GRAPH_LOADER_METRICS_SNAPSHOT_PATH"] = (
                f"/app/write-timings/metrics-lease-server-{encoded_run_id}-r{restart_attempt}.prom"
            )
        command = [
            "--host", "0.0.0.0", "--port", str(port), "--buckets", str(bucket_count),
            "--scheme", scheme,
        ]
        if quiet_period_seconds > 0:
            command.extend(["--quiet-period-seconds", str(quiet_period_seconds)])
        if fence_seconds > 0:
            command.extend(["--fence-seconds", str(fence_seconds)])
        return self._run_container(
            image=edge_image(),
            name=f"graph-loader-lease-{_dns_safe_run_token(run_id)}",
            entrypoint=["python", "-m", "src.orchestrator.lease_server"],
            command=command,
            environment=env, network=network,
            labels={
                "app": "graph-loader", "component": "bucket-lease-server", "run_id": run_id,
                **_metrics_labels("lease-server", env),
            },
            volumes=volumes,
            ports=_metrics_ports_entry(env),
            detach=True, init=True, remove=False,
            **_loader_resource_options(),
        )

    @staticmethod
    def bucket_lease_address(container, *, network: str, port: int = DEFAULT_BUCKET_LEASE_PORT,
                             prefer_name: bool = False) -> str:
        """Return ``ip:port`` of a launched lease server on ``network``.

        An IP works on Docker's default ``bridge`` network, which has no
        container-name DNS; the container name is the fallback.
        ``prefer_name=True`` (P5-S1 Story 5) skips the IP lookup entirely
        and returns the container's name directly, relying on the
        run's own user-defined network's embedded DNS -- required for a
        streaming lease server, whose *restarted* replacement is a new
        container with a new IP; only the streaming launcher passes this
        (bulk's existing call site is unchanged and keeps IP-preference,
        since bulk never restarts this container mid-run).
        """
        if prefer_name:
            if not isinstance(container.name, str) or not container.name:
                raise RuntimeError("bucket lease server has no name")
            return f"{container.name}:{port}"
        container.reload()
        networks = (container.attrs.get("NetworkSettings") or {}).get("Networks") or {}
        address = (networks.get(network) or {}).get("IPAddress")
        if not address:
            address = container.name
        if not isinstance(address, str) or not address:
            raise RuntimeError("bucket lease server has no network address")
        return f"{address}:{port}"

    def run_planned_dispatcher(
        self, *, run_id: str, plan_path: str, network: str = "graph-loader-net",
        port: int = DEFAULT_DISPATCHER_PORT, ready_labels: Sequence[str] = (),
    ):
        """Launch the run-scoped planned-edge dispatcher from the edge image.

        The plan file is mounted read-only; the port is also published on
        the host loopback (ephemeral) for the orchestrator's control client.
        """
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("planned dispatcher requires a nonblank run_id")
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError("planned dispatcher port must be 1-65535")
        host_plan = os.path.abspath(plan_path)
        if not os.path.isfile(host_plan):
            raise ValueError(f"edge plan file does not exist: {host_plan}")
        labels = sorted(set(ready_labels))
        encoded_run_id = run_id.encode("utf-8").hex()
        env = {"PYTHONUNBUFFERED": "1", **{
            key: os.environ[key] for key in ("GRAPH_LOADER_PLAN_NODE_PHASE_WRITERS",) if os.environ.get(key)
        }, **_metrics_env_passthrough()}
        volumes = {host_plan: {"bind": _PLAN_MOUNT, "mode": "ro"}}
        capture_timings = _mount_write_timing_directory(volumes)
        if capture_timings:
            env["GRAPH_LOADER_METRICS_SNAPSHOT_PATH"] = (
                f"/app/write-timings/metrics-planned-dispatcher-{encoded_run_id}.prom"
            )
        ports = {f"{port}/tcp": ("127.0.0.1", None)}
        ports.update(_metrics_ports_entry(env))
        return self._run_container(
            image=edge_image(),
            name=f"graph-loader-planned-dispatcher-{encoded_run_id}",
            entrypoint=["python", "-m", "src.orchestrator.planned_dispatcher"],
            command=[
                "--plan", _PLAN_MOUNT, "--host", "0.0.0.0", "--port", str(port),
                "--ready-labels", ",".join(labels),
            ],
            environment=env, network=network,
            labels={
                "app": "graph-loader", "component": "planned-dispatcher", "run_id": run_id,
                **_metrics_labels("planned-dispatcher", env),
            },
            volumes=volumes,
            ports=ports,
            detach=True, init=True, remove=False,
            **_loader_resource_options(),
        )

    @staticmethod
    def planned_dispatcher_addresses(container, *, network: str,
                                     port: int = DEFAULT_DISPATCHER_PORT) -> tuple[str, str]:
        """Return ``(address for writers on network, address for the host)``."""
        container.reload()
        settings = container.attrs.get("NetworkSettings") or {}
        networks = settings.get("Networks") or {}
        internal = (networks.get(network) or {}).get("IPAddress") or container.name
        if not isinstance(internal, str) or not internal:
            raise RuntimeError("planned dispatcher has no network address")
        bindings = (settings.get("Ports") or {}).get(f"{port}/tcp") or []
        host_port = next((item.get("HostPort") for item in bindings if item.get("HostPort")), None)
        host = f"127.0.0.1:{host_port}" if host_port else f"{internal}:{port}"
        return f"{internal}:{port}", host

    def run_planned_edge_writers(
        self, *, run_id: str, plan_path: str, config_path: str, dispatcher_address: str,
        writers: int, network: str = "graph-loader-net", environment: Optional[Dict[str, str]] = None,
        prefetch: bool = True,
    ) -> List[docker.models.containers.Container]:
        """Launch ``writers`` planned-edge writer containers (all edge types)."""
        if isinstance(writers, bool) or not isinstance(writers, int) or not 1 <= writers <= 64:
            raise ValueError("planned writers must be 1-64")
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("planned writers require a nonblank run_id")
        host, _, port = dispatcher_address.rpartition(":")
        if not host or not port.isdigit():
            raise ValueError("dispatcher address must be host:port")
        host_plan = os.path.abspath(plan_path)
        env = dict(environment) if environment is not None else {
            "NEO4J_URI": os.environ.get("LOADER_NEO4J_URI", "bolt://neo4j:7687"),
            "NEO4J_USERNAME": _neo4j_username(),
            "NEO4J_PASSWORD": os.environ.get("NEO4J_PASSWORD", "changeme"),
            "KAFKA_BOOTSTRAP_SERVERS": os.environ.get("LOADER_KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        }
        if environment is None and os.environ.get("NEO4J_DATABASE"):
            env["NEO4J_DATABASE"] = os.environ["NEO4J_DATABASE"]
        env["PYTHONUNBUFFERED"] = "1"
        if environment is None:
            env.update(_metrics_env_passthrough())
        volumes = {
            os.path.abspath(config_path): {"bind": "/app/runtime_schema.yaml", "mode": "ro"},
            host_plan: {"bind": _PLAN_MOUNT, "mode": "ro"},
        }
        capture_timings = _mount_write_timing_directory(volumes)
        encoded_run_id = run_id.encode("utf-8").hex()
        launchers = []
        for index in range(writers):
            writer_id = f"w{index}"
            command = [
                "--config", "/app/runtime_schema.yaml", "--plan", _PLAN_MOUNT,
                "--dispatcher", dispatcher_address, "--writer-id", writer_id,
            ]
            if not prefetch:
                command.append("--no-prefetch")
            replica_environment = dict(env)
            if capture_timings:
                replica_environment["GRAPH_LOADER_TIMING_LOG_PATH"] = (
                    f"/app/write-timings/edge-planned-{writer_id}.log"
                )
                replica_environment["GRAPH_LOADER_METRICS_SNAPSHOT_PATH"] = (
                    f"/app/write-timings/metrics-planned-writer-{writer_id}.prom"
                )
            launch_kwargs = dict(
                image=edge_image(),
                name=f"graph-loader-edge-planned-{writer_id}-{encoded_run_id}",
                entrypoint=["python", "-m", "src.loader.planned_writer"],
                command=command, environment=replica_environment, network=network,
                labels={
                    "app": "graph-loader", "component": "planned-edge-writer",
                    "writer_id": writer_id, "run_id": run_id,
                    **_metrics_labels("planned-writer", replica_environment),
                },
                volumes=volumes, detach=True, init=True, remove=False,
                ports=_metrics_ports_entry(replica_environment),
                **_loader_resource_options(),
            )
            launchers.append(lambda kwargs=launch_kwargs: self._run_container(**kwargs))
        return _launch_all(launchers, _stop_and_remove_quietly)

    def run_edge_loader(
        self,
        *,
        edge_type: str,
        mode: str,
        config_path: str,
        topic: Optional[str] = None,
        topics: Optional[Sequence[str]] = None,
        replicas: int = 1,
        network: str = "graph-loader-net",
        environment: Optional[Dict[str, str]] = None,
        rejection_dir: str = REJECTION_DIR,
        run_id: Optional[str] = None,
        consumer_group_prefix: str = "graph-loader",
        slot_gating: bool = False,
        coordination_topic: Optional[str] = None,
        fleet_edge_types: Optional[str] = None,
        lease_address: Optional[str] = None,
        lease_buckets: Optional[int] = None,
        outer_batch_size: Optional[int] = None,
        lease_keyed_labels: Sequence[str] = (),
        timing_tag: Optional[str] = None,
    ) -> List[docker.models.containers.Container]:
        """Launch exact relationship loader replicas, rolling back only this call.

        ``timing_tag`` (a precedence sub-stage, e.g. ``t1``) is inserted after the edge type in the
        timing and metric file names so sequential sub-stages do not overwrite each other.

        ``lease_address`` (``host:port``) enables bucket-lease writes with
        ``lease_buckets`` buckets per label, leasing ``lease_keyed_labels``
        per node key; ``outer_batch_size`` overrides the schema's edge outer
        flush size.
        """
        if replicas < 1:
            raise ValueError("replicas must be positive")
        timed_type = f"{edge_type}.{timing_tag}" if timing_tag else edge_type
        # v7-S2: a relationship type may read several topics in one group.
        if topics is not None:
            topic_list = list(topics)
        elif topic is not None:
            topic_list = [topic]
        else:
            raise ValueError("run_edge_loader requires topic or topics")
        if (
            not topic_list or topic is not None and topics is not None
            or any(not isinstance(item, str) or not item.strip() for item in topic_list)
            or len(set(topic_list)) != len(topic_list)
        ):
            raise ValueError("edge loader topics must be a nonempty list of unique nonblank names (and not both topic and topics)")
        topic_args = ["--topic", topic_list[0]] if len(topic_list) == 1 else ["--topics", ",".join(topic_list)]
        if lease_address is not None:
            parse_lease_address(lease_address)
            if slot_gating:
                # P5-S1 Story 6: leases and clock-slot-gating are mutually
                # exclusive within one EdgeLoader in *any* mode -- not
                # "bucket leases are bulk-only" (they are also streaming's
                # default now, with lease_address set and slot_gating
                # unset, exactly what this guard already permits).
                raise ValueError("bucket leases cannot combine with slot_gating")
        if (lease_buckets is not None or lease_keyed_labels) and lease_address is None:
            raise ValueError("lease_buckets and lease_keyed_labels require lease_address")
        keyed_labels = parse_keyed_labels(lease_keyed_labels)
        if lease_buckets is not None:
            parse_lease_bucket_count(lease_buckets)
        if outer_batch_size is not None and (
            isinstance(outer_batch_size, bool) or not isinstance(outer_batch_size, int)
            or not 1 <= outer_batch_size <= MAX_EDGE_OUTER_BATCH_SIZE
        ):
            raise ValueError(f"outer_batch_size must be an integer from 1 through {MAX_EDGE_OUTER_BATCH_SIZE}")
        if coordination_topic is not None and not slot_gating:
            raise ValueError("coordination_topic requires slot_gating")
        if fleet_edge_types is not None and not slot_gating:
            raise ValueError("fleet_edge_types requires slot_gating")
        if slot_gating:
            if not isinstance(coordination_topic, str) or not coordination_topic.strip():
                raise ValueError("slot_gating requires a nonblank coordination_topic")
            if not isinstance(run_id, str) or not run_id.strip():
                raise ValueError("slot_gating requires a nonblank run_id")
            try:
                canonical_fleet_types = parse_fleet_edge_types(fleet_edge_types)
            except ValueError as exc:
                raise ValueError(
                    f"stage=launch run_id={run_id} edge={edge_type} "
                    f"reason=invalid shared fleet contract: {exc}"
                ) from exc
            if edge_type not in canonical_fleet_types:
                raise ValueError(
                    f"stage=launch run_id={run_id} edge={edge_type} "
                    "reason=edge type is absent from fleet_edge_types"
                )
            try:
                schema = load_schema(config_path)
                selected_edge_types = {
                    edge.type for edge in select_shared_fleet_edges(schema.edges, canonical_fleet_types)
                }
            except (SchemaLoadError, ValueError) as exc:
                raise ValueError(
                    f"stage=launch run_id={run_id} edge={edge_type} "
                    f"reason=invalid shared fleet contract: {exc}"
                ) from exc
            if edge_type not in selected_edge_types:
                raise ValueError(
                    f"stage=launch run_id={run_id} edge={edge_type} "
                    "reason=edge type is absent from selected shared fleet"
                )
        env = dict(environment) if environment is not None else {
            "NEO4J_URI": os.environ.get("LOADER_NEO4J_URI", "bolt://neo4j:7687"),
            "NEO4J_USERNAME": _neo4j_username(),
            "NEO4J_PASSWORD": os.environ.get("NEO4J_PASSWORD", "changeme"),
            "KAFKA_BOOTSTRAP_SERVERS": os.environ.get(
                "LOADER_KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
            ),
        }
        if environment is None and os.environ.get("NEO4J_DATABASE"):
            env["NEO4J_DATABASE"] = os.environ["NEO4J_DATABASE"]
        if environment is None and os.environ.get("GRAPH_LOADER_EDGE_EXPLICIT_LOCKS"):
            # The writer that reads this toggle runs in the edge containers.
            env["GRAPH_LOADER_EDGE_EXPLICIT_LOCKS"] = os.environ["GRAPH_LOADER_EDGE_EXPLICIT_LOCKS"]
        if environment is None and mode == "stream" and os.environ.get(LEASE_MERGE_ROWS_ENV):
            # The lease coordinator that reads this runs in the edge containers.
            # Stream only: bulk edge containers keep their default merge budget.
            env[LEASE_MERGE_ROWS_ENV] = os.environ[LEASE_MERGE_ROWS_ENV]
        if lease_address is not None:
            # P5-S1 Story 1: set both the current and the deprecated env var
            # name to the same value for one release, so an edge-loader
            # image built from a slightly older commit (still reading only
            # the deprecated name) in a mixed-version fleet never silently
            # loses its lease address.
            env[BUCKET_LEASE_ADDRESS_ENV] = lease_address
            env[LEASE_ADDRESS_ENV] = lease_address
            env[LEASE_BUCKETS_ENV] = str(parse_lease_bucket_count(lease_buckets))
            env[LEASE_KEYED_LABELS_ENV] = ",".join(sorted(keyed_labels))
        if outer_batch_size is not None:
            env[EDGE_OUTER_BATCH_ENV] = str(outer_batch_size)
        if environment is None:
            env.update(_metrics_env_passthrough())
        host_config_path = os.path.abspath(config_path)
        host_rejection_dir = os.path.abspath(rejection_dir)
        os.makedirs(host_rejection_dir, exist_ok=True)
        volumes = {
            host_config_path: {"bind": "/app/runtime_schema.yaml", "mode": "ro"},
            host_rejection_dir: {"bind": "/app/rejections", "mode": "rw"},
        }
        capture_timings = _mount_write_timing_directory(volumes)
        # Finite bulk replicas all start from this one launch: each owns the
        # partitions ``p % replicas == replica_id`` via ``assign()`` so a
        # staggered start never rebalances (and revokes) a writing sibling.
        # Slot-gated fleets and stream loaders keep group subscription.
        static_assignment = mode == "bulk" and not slot_gating
        launchers = []
        for index in range(replicas):
            replica_id = str(index)
            command = [
                "--config", "/app/runtime_schema.yaml", "--edge-type", edge_type,
                "--mode", mode, *topic_args, "--replica-id", replica_id,
            ]
            labels = {
                "app": "graph-loader", "component": "edge-loader",
                "edge_type": edge_type, "replica_id": replica_id,
                **_metrics_labels("edge", env),
            }
            name = f"graph-loader-edge-{edge_type}-{replica_id}"
            replica_environment = dict(env)
            rejection_suffix = ""
            if run_id is not None:
                encoded_run_id = run_id.encode("utf-8").hex()
                if not encoded_run_id:
                    raise ValueError("run_id must not be empty")
                name = f"{name}-{encoded_run_id}"
                command.extend(["--run-id", run_id])
                labels["run_id"] = run_id
                replica_environment["GRAPH_LOADER_RUN_ID"] = run_id
                rejection_suffix = f"-{encoded_run_id}"
            if run_id is not None or mode == "stream":
                # Bulk: run-scoped ``<prefix>-<type>-<run id>``. Stream: the
                # stable ``<prefix>-<type>``, so a restart resumes.
                replica_environment["KAFKA_GROUP_ID"] = edge_group(
                    consumer_group_prefix, edge_type, mode=mode, run_id=run_id,
                )
            if static_assignment:
                command.extend(["--replica-count", str(replicas)])
            if slot_gating:
                command.extend([
                    "--slot-gating", "--coordination-topic", coordination_topic,
                    "--fleet-edge-types", fleet_edge_types,
                ])
                replica_environment["KAFKA_COORDINATION_GROUP_ID"] = (
                    f"{consumer_group_prefix}-{edge_type}-{run_id}-clock-{replica_id}"
                )
            replica_environment["REJECTION_LOG_PATH"] = (
                f"/app/rejections/{edge_type}-{replica_id}{rejection_suffix}.jsonl"
            )
            if capture_timings:
                replica_environment["GRAPH_LOADER_TIMING_LOG_PATH"] = (
                    f"/app/write-timings/edge-{timed_type}-{replica_id}.log"
                )
                replica_environment["GRAPH_LOADER_METRICS_SNAPSHOT_PATH"] = (
                    f"/app/write-timings/metrics-edge-{timed_type}-{replica_id}.prom"
                )
            launch_kwargs = dict(
                image=edge_image(), name=name, command=command,
                environment=replica_environment, network=network, labels=labels,
                volumes=volumes, detach=True, init=True, remove=False,
                ports=_metrics_ports_entry(replica_environment),
                **_loader_resource_options(),
            )
            launchers.append(
                lambda kwargs=launch_kwargs: self._run_container(**kwargs)
            )
        return _launch_all(launchers, _stop_and_remove_quietly)

    def run_retry_consumer(
        self, *, run_id: str, config_path: str, lease_address: str,
        dlq_topics: Sequence[str], replicas: int = 1,
        lease_buckets: Optional[int] = None, lease_keyed_labels: Sequence[str] = (),
        network: str = "graph-loader-net", environment: Optional[Dict[str, str]] = None,
        consumer_group_prefix: str = "graph-loader",
    ) -> List[docker.models.containers.Container]:
        """Launch ``replicas`` retry-consumer replicas from the edge image,
        one Kafka consumer group across every DLQ topic in ``dlq_topics``
        (P5-S4 Story 6). The group is the stable
        ``<consumer_group_prefix>-retry-consumer``
        (``src.utils.consumer_groups.stream_retry_group``), not run-scoped.

        ``lease_address`` is required: a retry consumer with no live lease
        server to fence against must not be launched by this path
        (Decisions §4) -- construct one directly (module-level, no
        Docker) for the standalone/manual-bulk-backlog use case
        (``python -m src.loader.retry_consumer`` with no ``--lease-address``)
        instead. ``lease_buckets``/``lease_keyed_labels`` must match the
        run's live edge-loader fleet exactly (the same values passed to
        every ``run_edge_loader`` call), so this replica's own
        ``BucketCellRouter`` maps nodes to lease resources identically.
        """
        if not dlq_topics:
            raise ValueError("retry consumer requires at least one DLQ topic")
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("retry consumer requires a nonblank run_id")
        if isinstance(replicas, bool) or not isinstance(replicas, int) or not 1 <= replicas <= 64:
            raise ValueError("retry consumer replicas must be 1-64")
        parse_lease_address(lease_address)  # fail fast on a malformed address, same guard run_edge_loader uses
        keyed_labels = parse_keyed_labels(lease_keyed_labels)
        if lease_buckets is not None:
            parse_lease_bucket_count(lease_buckets)
        env = dict(environment) if environment is not None else {
            "NEO4J_URI": os.environ.get("LOADER_NEO4J_URI", "bolt://neo4j:7687"),
            "NEO4J_USERNAME": _neo4j_username(),
            "NEO4J_PASSWORD": os.environ.get("NEO4J_PASSWORD", "changeme"),
            "KAFKA_BOOTSTRAP_SERVERS": os.environ.get("LOADER_KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        }
        if environment is None and os.environ.get("NEO4J_DATABASE"):
            env["NEO4J_DATABASE"] = os.environ["NEO4J_DATABASE"]
        if environment is None and os.environ.get(LEASE_MERGE_ROWS_ENV):
            env[LEASE_MERGE_ROWS_ENV] = os.environ[LEASE_MERGE_ROWS_ENV]
        env["PYTHONUNBUFFERED"] = "1"
        env[BUCKET_LEASE_ADDRESS_ENV] = lease_address
        env[LEASE_ADDRESS_ENV] = lease_address
        if lease_buckets is not None:
            env[LEASE_BUCKETS_ENV] = str(parse_lease_bucket_count(lease_buckets))
        env[LEASE_KEYED_LABELS_ENV] = ",".join(sorted(keyed_labels))
        env["GRAPH_LOADER_RUN_ID"] = run_id
        # Stable and namespaced by the schema prefix: a restart resumes the
        # DLQ where it stopped, and two deployments never share the group.
        env["KAFKA_GROUP_ID"] = stream_retry_group(consumer_group_prefix)
        if environment is None:
            env.update(_metrics_env_passthrough())
        host_config_path = os.path.abspath(config_path)
        volumes = {host_config_path: {"bind": "/app/runtime_schema.yaml", "mode": "ro"}}
        capture_timings = _mount_write_timing_directory(volumes)
        encoded_run_id = run_id.encode("utf-8").hex()
        launchers = []
        for index in range(replicas):
            replica_id = str(index)
            replica_environment = dict(env)
            if capture_timings:
                replica_environment["GRAPH_LOADER_METRICS_SNAPSHOT_PATH"] = (
                    f"/app/write-timings/metrics-retry-consumer-{replica_id}-{encoded_run_id}.prom"
                )
            launch_kwargs = dict(
                image=edge_image(),
                name=f"graph-loader-retry-{replica_id}-{encoded_run_id}",
                entrypoint=["python", "-m", "src.loader.retry_consumer"],
                command=[
                    "--config", "/app/runtime_schema.yaml", "--run-id", run_id,
                    "--dlq-topics", ",".join(dlq_topics),
                ],
                environment=replica_environment, network=network,
                labels={
                    "app": "graph-loader", "component": "retry-consumer",
                    "run_id": run_id, "replica_id": replica_id,
                    **_metrics_labels("retry-consumer", replica_environment),
                },
                volumes=volumes, detach=True, init=True, remove=False,
                ports=_metrics_ports_entry(replica_environment),
                **_loader_resource_options(),
            )
            launchers.append(lambda kwargs=launch_kwargs: self._run_container(**kwargs))
        return _launch_all(launchers, _stop_and_remove_quietly)

    def run_node_loader(
        self,
        node_label: str,
        topic: str,
        mode: str,
        config_path: str,
        replicas: int = 1,
        network: str = "graph_loader_default",
        environment: Optional[Dict[str, str]] = None,
        rejection_dir: str = REJECTION_DIR,
        run_id: Optional[str] = None,
        lease_address: Optional[str] = None,
        lease_buckets: Optional[int] = None,
        lease_keyed_labels: Sequence[str] = (),
    ) -> List[docker.models.containers.Container]:
        # ``lease_address`` (stream mode): node writes take the same bucket
        # leases as relationship writes, so the settings must match the
        # edge loaders' (bucket count, keyed labels).

        # The CLI runs on the host, whereas node loaders run on the Docker
        # network. Do not leak host endpoints such as ``localhost:9092`` into a
        # loader container: there, localhost is the loader itself. Deployments
        # with a non-default internal network can explicitly use the LOADER_*
        # variables or pass ``environment``.
        env = dict(environment) if environment is not None else {
            "NEO4J_URI": os.environ.get("LOADER_NEO4J_URI", "bolt://neo4j:7687"),
            "NEO4J_USERNAME": _neo4j_username(),
            "NEO4J_PASSWORD": os.environ.get("NEO4J_PASSWORD", "changeme"),
            "KAFKA_BOOTSTRAP_SERVERS": os.environ.get(
                "LOADER_KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
            ),
        }
        if environment is None and os.environ.get("NEO4J_DATABASE"):
            env["NEO4J_DATABASE"] = os.environ["NEO4J_DATABASE"]
        if environment is None and os.environ.get("GRAPH_LOADER_EDGE_EXPLICIT_LOCKS"):
            env["GRAPH_LOADER_EDGE_EXPLICIT_LOCKS"] = os.environ["GRAPH_LOADER_EDGE_EXPLICIT_LOCKS"]
        if environment is None:
            env.update(_metrics_env_passthrough())
        if lease_address is not None:
            parse_lease_address(lease_address)
            env[BUCKET_LEASE_ADDRESS_ENV] = lease_address
            env[LEASE_ADDRESS_ENV] = lease_address
            env[LEASE_BUCKETS_ENV] = str(parse_lease_bucket_count(lease_buckets))
            env[LEASE_KEYED_LABELS_ENV] = ",".join(sorted(parse_keyed_labels(lease_keyed_labels)))
        elif lease_buckets is not None or lease_keyed_labels:
            raise ValueError("lease_buckets and lease_keyed_labels require lease_address")

        labels = {
            "app": "graph-loader",
            "component": "node-loader",
            "node_label": node_label,
            **_metrics_labels("node", env),
        }

        command = ["--config", "/app/runtime_schema.yaml", "--node-label", node_label, "--mode", mode, "--topic", topic]

        # Use an absolute path for the volume mount
        host_config_path = os.path.abspath(config_path)
        host_rejection_dir = os.path.abspath(rejection_dir)
        os.makedirs(host_rejection_dir, exist_ok=True)
        volumes = {
            host_config_path: {"bind": "/app/runtime_schema.yaml", "mode": "ro"},
            host_rejection_dir: {"bind": "/app/rejections", "mode": "rw"},
        }
        capture_timings = _mount_write_timing_directory(volumes)
        # Bulk loaders are finite and run-scoped, and their orchestrator
        # removes them after the run: keep them after exit so a crashed
        # replica's exit code and logs remain inspectable, and so Docker need
        # not also delete each container while it serializes stop teardown.
        # Long-lived stream loaders keep auto-remove.
        auto_remove = mode != "bulk"

        launchers = []
        for i in range(replicas):
            replica_id = str(i)
            container_name = f"graph-loader-node-{node_label}-{replica_id}"
            replica_environment = dict(env)
            replica_environment["REJECTION_LOG_PATH"] = (
                f"/app/rejections/{node_label}-{replica_id}.jsonl"
            )
            if capture_timings:
                replica_environment["GRAPH_LOADER_TIMING_LOG_PATH"] = (
                    f"/app/write-timings/node-{node_label}-{replica_id}.log"
                )
                replica_environment["GRAPH_LOADER_METRICS_SNAPSHOT_PATH"] = (
                    f"/app/write-timings/metrics-node-{node_label}-{replica_id}.prom"
                )
            replica_command = [*command, "--replica-id", replica_id]
            replica_labels = {**labels, "replica_id": replica_id}
            if run_id is not None:
                # Hex encoding preserves every byte of the external id while
                # restricting the Docker name suffix to a safe alphabet. In
                # particular, ``a-b`` and ``ab`` cannot collapse to one name.
                encoded_run_id = run_id.encode("utf-8").hex()
                if not encoded_run_id:
                    raise ValueError("run_id must not be empty")
                container_name = f"{container_name}-{encoded_run_id}"
                replica_environment["GRAPH_LOADER_RUN_ID"] = run_id
                replica_command.extend(["--run-id", run_id])
                replica_labels["run_id"] = run_id
            launch_kwargs = dict(
                image=node_image(),
                name=container_name,
                command=replica_command,
                environment=replica_environment,
                network=network,
                labels=replica_labels,
                volumes=volumes,
                detach=True,
                init=True,
                remove=auto_remove,
                ports=_metrics_ports_entry(replica_environment),
                **_loader_resource_options(),
            )
            launchers.append(
                lambda kwargs=launch_kwargs: self._run_container(**kwargs)
            )

        # Roll back every started replica of this node_label if any replica fails.
        return _launch_all(launchers, _stop_quietly if auto_remove else _stop_and_remove_quietly)

    def stop_node_loaders(self, node_label: Optional[str] = None):
        labels = ["component=node-loader"]
        if node_label:
            labels.append(f"node_label={node_label}")
        filters = {"label": labels}

        containers = self.client.containers.list(all=True, filters=filters)
        for container in containers:
            wait_until_stop_signal_ready(container)
            container.stop()

    def list_run_containers(self, *, label_filters: Sequence[str] = ("app=graph-loader",)) -> list:
        """Every graph-loader container matching every given label filter, any kind.

        ``app=graph-loader`` is the only label every launcher applies
        unconditionally (``run_id`` is best-effort: node/edge loaders only
        carry it when one was passed), so the default is the reliable
        orphan check.  Includes stopped containers (``all=True``).
        """
        return self.client.containers.list(all=True, filters={"label": list(label_filters)})

    def list_node_loaders(self) -> List[Dict]:
        filters = {"label": "component=node-loader"}
        containers = self.client.containers.list(all=True, filters=filters)
        result = []
        for container in containers:
            result.append({
                "id": container.short_id,
                "name": container.name,
                "status": container.status,
                "node_label": container.labels.get("node_label")
            })
        return result
