"""Backend-neutral runtime seam: how the CLI launches, inspects, stops and removes role processes.

``Runtime`` is the set of operations the CLI performs through ``DockerService`` today; ``Handle`` is what
a launched role looks like to its callers. Nothing here imports a container SDK: call sites depend on
these protocols and on :class:`HandleGone`, and the Docker backend maps ``docker.errors.NotFound`` to it.
"""
from __future__ import annotations

from typing import Any, Mapping, Protocol, Sequence, runtime_checkable

RUNTIME_BACKENDS = ("docker", "process")
DEFAULT_RUNTIME_BACKEND = "docker"
RUNTIME_ENV = "GRAPH_LOADER_RUNTIME"

# Host directory of the relationship rejection logs: the classic and lease edge loaders and the
# planner (``<run id>-planner-<worker>.jsonl``) all write here.
REJECTION_DIR = "var/rejections"


class HandleGone(Exception):
    """The role behind a handle no longer exists (the backend-neutral ``NotFound``)."""


def wait_until_stop_signal_ready(handle: object) -> float:
    """Hold a stop until ``handle`` can take its SIGTERM; return the seconds held (see the backend's hold).

    A handle class that defines ``hold_for_stop`` supplies its own hold (the Docker handle waits for its
    init to forward signals, a process handle for its readiness report). Anything else (test doubles) gets
    the Docker hold, which only reads ``id`` and ``top()``.
    """
    own = getattr(type(handle), "hold_for_stop", None)
    if callable(own):
        return own(handle)
    from neo4j_graph_loader.orchestrator.docker_service import wait_until_stop_signal_ready as docker_hold
    return docker_hold(handle)


@runtime_checkable
class Handle(Protocol):
    """One launched role (a container, or a child process).

    ``attrs["State"]`` carries ``Status`` (``created``/``running``/``restarting``/``exited``...),
    ``ExitCode`` and ``OOMKilled``. ``OOMKilled`` is a bool where the backend can attribute an OOM kill
    to this role and ``None`` ("unknown") where it cannot; callers must treat ``None`` as unknown.
    Every method raises :class:`HandleGone` when the role has disappeared.
    """

    name: str
    id: str
    labels: Mapping[str, str]
    status: str
    attrs: Mapping[str, Any]

    def reload(self) -> None: ...

    def logs(self, **kwargs: Any) -> bytes | str: ...

    def stop(self, *, timeout: int | None = None) -> None: ...

    def remove(self, *, force: bool = False) -> None: ...

    def top(self) -> Mapping[str, Any]: ...

    def hold_for_stop(self) -> float:
        """Block until a SIGTERM sent now would be delivered to the role; return the seconds held."""
        ...


@runtime_checkable
class Runtime(Protocol):
    """Launch and discovery operations the CLI needs from a backend."""

    def preflight_topology(self, network: str, *, plan_path: str | None = None) -> None: ...

    def prepare_images(self, roles: Sequence[str], *, local_only: bool = False) -> Mapping[str, str]: ...

    def build_image(self, tag: str | None = None, dockerfile: str | None = None) -> str: ...

    def build_edge_image(self, tag: str | None = None, dockerfile: str | None = None) -> str: ...

    def build_clock_image(self, tag: str | None = None, dockerfile: str | None = None) -> str: ...

    def run_global_batch_clock(self, **kwargs: Any) -> Handle: ...

    def run_bucket_lease_server(self, **kwargs: Any) -> Handle: ...

    def bucket_lease_address(self, handle: Handle, *, network: str, **kwargs: Any) -> str: ...

    def run_planned_dispatcher(self, **kwargs: Any) -> Handle: ...

    def planned_dispatcher_addresses(self, handle: Handle, *, network: str, **kwargs: Any) -> tuple[str, str]: ...

    def run_planned_edge_writers(self, **kwargs: Any) -> Sequence[Handle]: ...

    def run_edge_loader(self, **kwargs: Any) -> Sequence[Handle]: ...

    def run_retry_consumer(self, **kwargs: Any) -> Sequence[Handle]: ...

    def run_node_loader(self, **kwargs: Any) -> Sequence[Handle]: ...

    def stop_node_loaders(self, node_label: str | None = None) -> None: ...

    def list_run_containers(self, *, label_filters: Sequence[str] = ("app=graph-loader",)) -> Sequence[Handle]: ...

    def list_node_loaders(self) -> list[dict]: ...

