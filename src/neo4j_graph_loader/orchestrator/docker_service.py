import logging
import os
import threading
import time
import docker
from docker.errors import NotFound
from neo4j_graph_loader._images import ImageSelection, ImageSelectionError, image_reference
from typing import Callable, List, Dict, Optional, Sequence
from neo4j_graph_loader.orchestrator.lease_server import DEFAULT_BUCKET_LEASE_PORT
from neo4j_graph_loader.orchestrator.planned_dispatcher import DEFAULT_DISPATCHER_PORT
from neo4j_graph_loader.orchestrator.runtime import REJECTION_DIR, HandleGone
from neo4j_graph_loader.utils.schema_loader import SchemaLoadError, load_schema  # noqa: F401  (load_schema is patched here by tests)
from neo4j_graph_loader.orchestrator.role_launch import (  # noqa: F401  (re-exported: the long-standing import path)
    LEASE_MERGE_ROWS_ENV, RoleLauncher, _MAX_PARALLEL_LAUNCHES, _PLAN_MOUNT, _dns_safe_run_token, _launch_all,
    _loader_resource_options, _metrics_enabled, _metrics_env_passthrough, _metrics_labels, _metrics_port,
    _metrics_ports_entry, _neo4j_username,
)

logger = logging.getLogger(__name__)




def node_image() -> str:
    return image_reference("node")


def edge_image() -> str:
    return image_reference("edge")


def clock_image() -> str:
    return image_reference("clock")



def _mount_write_timing_directory(volumes: dict, directory: str | None = None) -> bool:
    """Expose the optional host timing directory to loader containers."""
    host_directory = directory if directory is not None else os.environ.get("GRAPH_LOADER_TIMING_DIR")
    if not host_directory:
        return False
    host_directory = os.path.abspath(host_directory)
    os.makedirs(host_directory, exist_ok=True)
    volumes[host_directory] = {"bind": "/app/write-timings", "mode": "rw"}
    return True

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


class DockerHandle:
    """A Docker container seen through the backend-neutral ``Handle`` protocol.

    Delegates every operation to the SDK container unchanged and maps ``docker.errors.NotFound`` to
    :class:`HandleGone`; any other exception propagates as it was raised. ``container`` is the wrapped
    SDK object.
    """

    __slots__ = ("container",)

    def __init__(self, container: object):
        self.container = container

    def __repr__(self) -> str:
        return f"DockerHandle({self.container!r})"

    @property
    def name(self):
        return self.container.name

    @property
    def id(self):
        return self.container.id

    @property
    def short_id(self):
        return self.container.short_id

    @property
    def labels(self):
        return self.container.labels

    @property
    def status(self):
        return self.container.status

    @property
    def attrs(self):
        return self.container.attrs

    def reload(self):
        try:
            return self.container.reload()
        except NotFound as exc:
            raise HandleGone(str(exc)) from exc

    def logs(self, *args, **kwargs):
        try:
            return self.container.logs(*args, **kwargs)
        except NotFound as exc:
            raise HandleGone(str(exc)) from exc

    def stop(self, *args, **kwargs):
        try:
            return self.container.stop(*args, **kwargs)
        except NotFound as exc:
            raise HandleGone(str(exc)) from exc

    def remove(self, *args, **kwargs):
        try:
            return self.container.remove(*args, **kwargs)
        except NotFound as exc:
            raise HandleGone(str(exc)) from exc

    def top(self, *args, **kwargs):
        try:
            return self.container.top(*args, **kwargs)
        except NotFound as exc:
            raise HandleGone(str(exc)) from exc

    def hold_for_stop(self) -> float:
        return wait_until_stop_signal_ready(self)


class DockerService(RoleLauncher):
    def __init__(self, client: docker.DockerClient = None, *, runtime=None):
        try:
            self.client = client if client is not None else docker.from_env()
        except Exception as exc:
            raise ImageSelectionError("cannot access Docker daemon; check Docker endpoint and permissions") from exc
        self.runtime = runtime
        self.images = ImageSelection(self.client)

    def _mount_timings(self, volumes):
        return _mount_write_timing_directory(volumes, str(self.runtime.timings) if self.runtime is not None else None)

    def _rejection_directory(self, explicit):
        return explicit if explicit is not None else (str(self.runtime.rejections) if self.runtime is not None else REJECTION_DIR)

    def _schema(self, config_path):
        return load_schema(config_path)

    def _rollback(self, auto_remove: bool):
        return _stop_quietly if auto_remove else _stop_and_remove_quietly

    def _run_container(self, **kwargs):
        """``containers.run`` that records the launch time for a later stop (F1)."""
        return _record_launch(DockerHandle(self.client.containers.run(**kwargs)))

    def preflight_topology(self, network, *, plan_path=None):
        from neo4j_graph_loader._docker_preflight import check
        check(self, network, plan_path=plan_path)

    def prepare_images(self, roles, *, local_only=False):
        self.images.local_only = local_only
        return {role: self.images.resolve(role) for role in roles}

    def _image(self, role):
        return self.images.resolve(role)

    def build_image(self, tag: str | None = None, dockerfile: str | None = None):
        """Retained internal name: resolve a prebuilt node image, never build cwd."""
        return self._resolve_compat_image("node", tag, dockerfile)

    def build_edge_image(self, tag: str | None = None, dockerfile: str | None = None):
        return self._resolve_compat_image("edge", tag, dockerfile)

    def build_clock_image(self, tag: str | None = None, dockerfile: str | None = None):
        return self._resolve_compat_image("clock", tag, dockerfile)

    def _resolve_compat_image(self, role, tag, dockerfile):
        if dockerfile is not None:
            raise ImageSelectionError("runtime Dockerfile builds are unsupported; use the checkout candidate builder explicitly")
        return self.images.resolve(role, tag)

    @staticmethod
    def bucket_lease_address(container, *, network: str, port: int = DEFAULT_BUCKET_LEASE_PORT,
                             prefer_name: bool = False, host: bool = False) -> str:
        """Return ``ip:port`` of a launched lease server on ``network``.

        ``host=True`` uses the published loopback control port for a CLI
        readiness client. Container callers use an internal IP or stable DNS
        name. These addresses have different reachability on Docker Desktop.
        ``prefer_name=True`` (P5-S1 Story 5) skips the IP lookup entirely
        and returns the container's name directly, relying on the
        run's own user-defined network's embedded DNS -- required for a
        streaming lease server, whose *restarted* replacement is a new
        container with a new IP; only the streaming launcher passes this
        (bulk's existing call site is unchanged and keeps IP-preference,
        since bulk never restarts this container mid-run).
        """
        if host:
            container.reload()
            bindings = ((container.attrs.get("NetworkSettings") or {}).get("Ports") or {}).get(f"{port}/tcp") or []
            host_port = next((item.get("HostPort") for item in bindings if item.get("HostPort")), None)
            if not host_port:
                raise RuntimeError("lease coordinator has no published host control port")
            return f"127.0.0.1:{host_port}"
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

    def stop_node_loaders(self, node_label: Optional[str] = None):
        labels = ["component=node-loader"]
        if node_label:
            labels.append(f"node_label={node_label}")
        filters = {"label": labels}

        containers = [DockerHandle(c) for c in self.client.containers.list(all=True, filters=filters)]
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
        return [DockerHandle(c) for c in self.client.containers.list(all=True, filters={"label": list(label_filters)})]

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
