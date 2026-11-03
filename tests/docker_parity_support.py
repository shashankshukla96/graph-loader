"""Behavioural parity harness for Docker mode (E14.F1.S1).

A recording fake Docker client sits under the *real* ``DockerService``; the CLI, ``planned_edges`` and the
bulk/relationship monitors run unmodified above it. Every Docker call (client, image, network, api and
container-handle operations) is appended to a trace together with its arguments and the thread lane it
came from. ``run_all()`` replays each scenario and returns ``{scenario: {"trace": ..., "outcome": ...}}``.

The harness imports nothing that only exists after the E14 refactor, so the *same file* runs against
the frozen ``0.1.0a1`` source (commit 60ea29f) to produce ``tests/golden/docker_parity_0.1.0a1.json`` and
against the working tree to compare. ``python tests/docker_parity_support.py --emit PATH`` writes the trace.

Determinism devices (applied identically to both sources; they change when work runs, not what is called):

* D1 every ``ThreadPoolExecutor`` in ``cli``, ``docker_service`` and ``bulk_monitor`` is clamped to one worker, so a pool
  executes its tasks in submission order;
* D2 the stream node watcher polls hourly instead of every second (its polling is driven explicitly in the
  crash scenario);
* D3 Kafka, Neo4j and the monitors' Kafka consumers are replaced by scripted fakes; scenarios that need the
  real monitors construct them directly over ``MagicMock`` Kafka objects (as the unit tests do);
* D5 the working directory is the scratch directory, so relative paths such as ``var/rejections`` resolve to
  ``<TMP>/var/rejections`` (N2) instead of the checkout;
* D4 the process environment is reduced to ``PATH``, ``HOME`` and ``GRAPH_LOADER_METRICS_ENABLED=0`` so no
  host variable reaches a recorded container environment.

Written normalization (``normalize``; nothing else is rewritten):

* N1 the package version string becomes ``<VERSION>``;
* N2 the per-attempt run directory and its base become ``<ATTEMPT>`` and ``<TMP>``;
* N3 the thread name loses its trailing worker index (``container-launch_3`` -> ``container-launch``) and
  becomes the trace *lane*; ordering is compared per lane (cross-lane interleaving is scheduler noise);
* N4 image ids, container ids and published host ports are derived from the container name by the fake, so
  they need no rewriting;
* N5 ISO-8601 timestamps and ``elapsed_s=``/``wait_s=`` style durations become ``<TS>``/``<DUR>``;
* N7 (0.1.0a2) the values of the three run-status environment keys (``STATUS_ENV_KEYS``): the execution id, the
  incarnation and the snapshot directory are random per run, so they become ``<EXECUTION_ID>``, ``<INCARNATION>`` and
  ``/app/write-timings/<EXECUTION_ID>/<INCARNATION>``. The keys exist only from 0.1.0a2; ``strip_status_env`` removes
  them, which is how a 0.1.0a2 trace is compared with the frozen 0.1.0a1 golden;
* N6 the preflight probe's random marker (``.graph-loader-preflight-<32 hex>``) and secret token (a command
  argument, replaced by ``<PROBE-TOKEN>`` as the fake records it) are masked; probe ids are a counter.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import logging
import os
import re
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from docker.errors import APIError, ImageNotFound, NotFound

VERSION_PLACEHOLDER = "<VERSION>"
STATUS_ENV_KEYS = {
    "GRAPH_LOADER_EXECUTION_ID": "<EXECUTION_ID>",
    "GRAPH_LOADER_INCARNATION": "<INCARNATION>",
    "GRAPH_LOADER_STATUS_SNAPSHOT_DIR": "/app/write-timings/<EXECUTION_ID>/<INCARNATION>",
}
ROLES = ("cli", "node", "edge", "clock")


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def lane_of(thread_name: str) -> str:
    return re.sub(r"[_-]?\d+$", "", thread_name)


class Recorder:
    def __init__(self):
        self.events: list[dict] = []
        self._lock = threading.Lock()

    def add(self, subject: str, op: str, **detail):
        event = {"lane": lane_of(threading.current_thread().name), "subject": subject, "op": op, "args": detail}
        with self._lock:
            self.events.append(event)


def _plain(value):
    """Reduce a call argument to deterministic JSON-able data."""
    if isinstance(value, dict):
        return {str(k): _plain(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    if isinstance(value, (set, frozenset)):
        return sorted(_plain(v) for v in value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "__dict__") and type(value).__module__.startswith("docker"):
        return {"__type__": type(value).__name__, **_plain(dict(vars(value)))}
    return repr(value)


class FakeContainer:
    def __init__(self, client: "FakeDockerClient", kwargs: dict):
        self._client = client
        self.name = kwargs.get("name") or "anon-" + _sha(json.dumps(_plain(kwargs), sort_keys=True))[:8]
        self.id = _sha("container:" + self.name)
        self.short_id = self.id[:12]
        self.labels = dict(kwargs.get("labels") or {})
        self.auto_remove = bool(kwargs.get("remove"))
        host_port = 40000 + int(_sha("port:" + self.name)[:4], 16) % 20000
        networks = {}
        if kwargs.get("network"):
            networks[kwargs["network"]] = {"IPAddress": "172.18.%d.%d" % (int(_sha("ip:" + self.name)[:2], 16) % 250 + 1, int(_sha("ip:" + self.name)[2:4], 16) % 250 + 1)}
        ports = {}
        for container_port in (kwargs.get("ports") or {}):
            ports[container_port] = [{"HostIp": "127.0.0.1", "HostPort": str(host_port)}]
        self.attrs = {"State": {"Status": "running", "ExitCode": 0, "OOMKilled": False},
                      "NetworkSettings": {"Networks": networks, "Ports": ports}}
        self.log_bytes = b""
        self.gone = False

    @property
    def status(self):
        return self.attrs["State"]["Status"]

    def _check(self, op):
        if self.gone:
            raise NotFound(f"no such container: {self.name}")

    def reload(self):
        self._client.recorder.add(self.name, "reload")
        self._check("reload")

    def logs(self, *args, **kwargs):
        self._client.recorder.add(self.name, "logs", args=_plain(args), kwargs=_plain(kwargs))
        self._check("logs")
        return self.log_bytes

    def top(self, *args, **kwargs):
        self._client.recorder.add(self.name, "top")
        self._check("top")
        return {"Titles": ["PID", "CMD"], "Processes": [["1", "docker-init"], ["7", "python"]]}

    def stop(self, *args, **kwargs):
        self._client.recorder.add(self.name, "stop", args=_plain(args), kwargs=_plain(kwargs))
        self._check("stop")
        script = self._client.script.get(("stop", self.name))
        if script is not None:
            raise script
        self.attrs["State"].update(Status="exited", ExitCode=self._client.exit_codes.get(self.name, 0))
        if self.auto_remove:
            self.gone = True

    def remove(self, *args, **kwargs):
        self._client.recorder.add(self.name, "remove", args=_plain(args), kwargs=_plain(kwargs))
        if self.gone:
            raise NotFound(f"no such container: {self.name}")
        self.gone = True


class FakeImage:
    def __init__(self, role: str, labels: dict):
        self.id = "sha256:" + _sha("image:" + role)
        self.attrs = {"Config": {"Labels": labels}}


class _Namespace:
    pass


class FakeApi:
    base_url = "http+docker://localhost"

    def __init__(self, client):
        self._client = client
        self._probes = {}

    def create_host_config(self, **kwargs):
        self._client.recorder.add("api", "create_host_config", **_plain(kwargs))
        return {"__host_config__": kwargs}

    def create_container(self, **kwargs):
        recorded = dict(kwargs)
        recorded["command"] = [kwargs["command"][0], kwargs["command"][1], "<PROBE-TOKEN>"]   # N6: random secret
        self._client.recorder.add("api", "create_container", **_plain(recorded))
        container_id = f"probe-{len(self._probes) + 1}"
        self._probes[container_id] = kwargs
        return {"Id": container_id}

    def start(self, container_id):
        self._client.recorder.add("api", "start", container_id=container_id)
        kwargs = self._probes[container_id]
        # The probe container writes "<token>-container" into the two mounted directories it may write.
        token = kwargs["command"][2]
        for mount in kwargs["host_config"]["__host_config__"].get("mounts", []):
            target = mount["Target"]
            if target.startswith("/app/preflight/") and not mount.get("ReadOnly"):
                directory = Path(mount["Source"])
                for marker in directory.glob(".graph-loader-preflight-*"):
                    marker.write_text(token + "-container")

    def wait(self, container_id, timeout=None):
        self._client.recorder.add("api", "wait", container_id=container_id, timeout=timeout)
        return {"StatusCode": 0}

    def logs(self, container_id, **kwargs):
        self._client.recorder.add("api", "logs", container_id=container_id, **_plain(kwargs))
        return b'{"ok": true}'

    def remove_container(self, container_id, **kwargs):
        self._client.recorder.add("api", "remove_container", container_id=container_id, **_plain(kwargs))


class FakeDockerClient:
    """Duck-typed ``docker.DockerClient`` that records every call it receives."""

    def __init__(self, recorder: Recorder, *, version: str):
        self.recorder = recorder
        self.version = version
        self.containers_by_name: dict[str, FakeContainer] = {}
        self.script: dict = {}          # ("run", name) / ("stop", name) -> exception to raise
        self.exit_codes: dict[str, int] = {}
        self.api = FakeApi(self)
        self.containers = SimpleNamespace(run=self._run, list=self._list)
        self.images = SimpleNamespace(get=self._image_get, pull=self._image_pull)
        self.networks = SimpleNamespace(get=self._network_get)

    # containers ---------------------------------------------------------------------------------
    def _run(self, **kwargs):
        self.recorder.add("client", "containers.run", **_plain(kwargs))
        failure = self.script.get(("run", kwargs.get("name")))
        if failure is not None:
            raise failure
        container = FakeContainer(self, kwargs)
        self.containers_by_name[container.name] = container
        return container

    def _list(self, **kwargs):
        self.recorder.add("client", "containers.list", **_plain(kwargs))
        wanted = (kwargs.get("filters") or {}).get("label")
        wanted = [wanted] if isinstance(wanted, str) else list(wanted or [])
        selected = []
        for container in self.containers_by_name.values():
            labels = {f"{k}={v}" for k, v in container.labels.items()}
            if all(item in labels for item in wanted) and not container.gone:
                selected.append(container)
        return selected

    # images / networks ------------------------------------------------------------------------------
    def _image_get(self, reference):
        self.recorder.add("client", "images.get", reference=reference)
        role = next((r for r in ROLES if f"graph-loader-{r}:" in reference or reference.endswith(f"/graph-loader-{r}")), None)
        if role is None:
            raise ImageNotFound(reference)
        return FakeImage(role, {"org.opencontainers.image.version": self.version,
                                "io.graph-loader.component": role, "io.graph-loader.image-contract": "1"})

    def _image_pull(self, reference):
        self.recorder.add("client", "images.pull", reference=reference)
        return self._image_get(reference)

    def _network_get(self, name):
        self.recorder.add("client", "networks.get", name=name)
        return SimpleNamespace(attrs={"Driver": "bridge", "Name": name})


def normalize(value, *, version: str, attempt: str | None, tmp: str):
    """Apply N1, N2, N4, N5 to a JSON-able structure (N3 happens when lanes are built)."""
    def fix(text: str) -> str:
        text = text.replace(version, VERSION_PLACEHOLDER)
        if attempt:
            text = text.replace(attempt, "<ATTEMPT>")
        text = text.replace(tmp, "<TMP>")
        text = re.sub(r"\.graph-loader-preflight-[0-9a-f]{32}", "<PROBE-MARKER>", text)
        text = re.sub(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?", "<TS>", text)
        text = re.sub(r"((?:elapsed|wait|duration)_?s?=)\d+(?:\.\d+)?", r"\1<DUR>", text)
        return text
    if isinstance(value, str):
        return fix(value)
    if isinstance(value, list):
        return [normalize(v, version=version, attempt=attempt, tmp=tmp) for v in value]
    if isinstance(value, dict):
        return {fix(k): (STATUS_ENV_KEYS[k] if k in STATUS_ENV_KEYS and isinstance(v, str)
                         else normalize(v, version=version, attempt=attempt, tmp=tmp)) for k, v in value.items()}
    return value


def strip_status_env(record: dict) -> dict:
    """A scenario record without the three run-status environment keys (the only 0.1.0a2 addition to a trace)."""
    def strip(node):
        if isinstance(node, dict):
            return {key: strip(value) for key, value in node.items() if key not in STATUS_ENV_KEYS}
        if isinstance(node, list):
            return [strip(item) for item in node]
        return node
    return strip(record)


def lanes(events: list[dict]) -> dict:
    result: dict[str, list] = {}
    for event in events:
        result.setdefault(event["lane"], []).append({k: v for k, v in event.items() if k != "lane"})
    return dict(sorted(result.items()))


# ---------------------------------------------------------------------------------------------------
# Scripted Kafka-side doubles (D3)
# ---------------------------------------------------------------------------------------------------
class _ScriptedMonitorBase:
    """Stands in for ``BulkMonitor`` / ``RelationshipBulkMonitor``; touches no Kafka and no Docker."""

    script: dict = {}

    def __init__(self, *args, **kwargs):
        self._lease = kwargs.get("tracked_lease_server")
        self.closed = False

    @property
    def tracked_lease_server(self):
        return self._lease

    def close(self):
        self.closed = True

    def _maybe_fail(self, step):
        failure = self.script.get(step)
        if failure is not None:
            raise failure

    def wait_for_assignment_coverage(self, timeout):
        self._maybe_fail("coverage")

    def capture_boundary(self):
        return {}

    def wait_for_clock_lease(self, timeout):
        pass

    def wait_for_completion(self, timeout):
        self._maybe_fail("completion")

    def wait_for_rotating_completion(self, timeout):
        self._maybe_fail("completion")

    def wait_for_drain_complete(self, timeout):
        pass

    def verify_zero_lag(self):
        pass

    def wait_for_lease_server_ready(self, timeout, **kwargs):
        pass

    def supervise_stream(self, shutdown_requested, **kwargs):
        self._maybe_fail("supervise")
        shutdown_requested.set()          # the operator stops the run

    def drive_label_readiness(self, labels, timeout, *, stop_label, on_ready, should_abort):
        self._maybe_fail("readiness")
        for index, label in enumerate(sorted(labels)):
            stop_label(label)
            on_ready(label)
            if index == 0 and self.script.get("after_first_ready") is not None:
                self.script["after_first_ready"]()


class FakeDispatcherControl:
    """Replaces ``DispatcherClient``: completes the plan on the first status poll."""

    def __init__(self, address, *, digest, client=None):
        self.digest, self.sent, self.client = digest, set(), client

    def connect(self):
        pass

    def ready(self, labels):
        self.sent |= set(labels)

    def status(self):
        for container in (self.client.containers_by_name.values() if self.client else ()):
            if container.labels.get("component") == "planned-edge-writer":
                container.attrs["State"].update(Status="exited", ExitCode=0)   # writers finish with the plan
        return {"state": "complete", "done": 1, "granted": 0}

    def summary(self):
        return self.plan_summary

    def abort(self, reason):
        pass

    def close(self):
        pass


# ---------------------------------------------------------------------------------------------------
# Scenario runner
# ---------------------------------------------------------------------------------------------------
class _OneWorker(ThreadPoolExecutor):
    """D1: tasks run strictly in submission order."""

    def __init__(self, max_workers=None, *args, **kwargs):
        super().__init__(1, *args, **kwargs)


class Context:
    """One scenario's fake Docker client, scratch directory and patched collaborators."""

    def __init__(self, tmp: Path):
        from neo4j_graph_loader._version import __version__
        self.version = __version__
        self.tmp = tmp
        self.recorder = Recorder()
        self.client = FakeDockerClient(self.recorder, version=__version__)
        self.monitor_script: dict = {}
        self.log_records: list[str] = []

    # -- collaborators ----------------------------------------------------------------------------------------
    def service(self, runtime=None):
        from neo4j_graph_loader.orchestrator.docker_service import DockerService
        return DockerService(client=self.client, runtime=runtime) if runtime is not None else DockerService(client=self.client)

    def patches(self, extra=None):
        from neo4j_graph_loader import cli
        from neo4j_graph_loader.orchestrator import docker_service, planned_edges
        try:
            from neo4j_graph_loader.orchestrator import role_launch
        except ImportError:                       # the frozen 0.1.0a1 source has no such module
            role_launch = None
        ctx = self
        script = self.monitor_script

        class BulkMon(_ScriptedMonitorBase):
            pass
        BulkMon.script = script
        watcher_class = cli._NodeLoaderWatcher
        schema_plan_phase = planned_edges.run_planned_edge_phase

        def planned_phase(*args, **kwargs):
            kwargs.setdefault("control_factory", ctx.control_factory)
            kwargs.setdefault("verify_topic", lambda plan, bootstrap: None)
            return schema_plan_phase(*args, **kwargs)

        def service_factory(**kwargs):
            return ctx.service(kwargs.get("runtime"))

        def watcher_factory(containers, shutdown, **kwargs):
            return watcher_class(containers, shutdown, **{**kwargs, "interval": 3600.0})

        entries = [
            patch.object(cli, "DockerService", service_factory),
            patch.object(cli, "BulkMonitor", BulkMon),
            patch.object(cli, "RelationshipBulkMonitor", BulkMon),
            patch.object(cli, "run_planned_edge_phase", planned_phase),
            patch.object(cli, "_NodeLoaderWatcher", watcher_factory),
            patch.object(cli, "ThreadPoolExecutor", _OneWorker),
            # the launch pool lives in docker_service in the frozen source and in role_launch since E14.F2.S4
            patch(f"{(docker_service if role_launch is None else role_launch).__name__}.ThreadPoolExecutor", _OneWorker),
            patch("neo4j_graph_loader.orchestrator.bulk_monitor.ThreadPoolExecutor", _OneWorker),
            patch.object(docker_service, "_sleep", lambda seconds: None),
            patch.object(cli, "get_neo4j_driver", MagicMock()),
            patch.object(cli, "apply_schema", MagicMock()),
            patch.object(cli, "_missing_schema_topics", lambda schema, bootstrap: []),
            patch.object(cli, "_ensure_dead_letter_topics", MagicMock()),
            patch.object(cli, "_ensure_retry_topics", MagicMock()),
            patch.object(cli, "_ensure_control_topic", MagicMock()),
            patch.object(cli, "_prepare_stream_consumer_groups", MagicMock()),
            patch.object(cli, "_require_no_stale_dead_letter_topics", MagicMock()),
            patch.object(cli, "_require_no_dlq_leftovers", MagicMock()),
            patch.object(cli, "_node_topic_record_counts", lambda bootstrap, topics: {topic: 10 for topic in topics}),
            patch.object(cli, "start_kafka_edge_planner", MagicMock()),
            patch.object(planned_edges, "validate_plan_against_schema", lambda plan, schema: None),
            patch("neo4j_graph_loader.orchestrator.auto_settings.topic_sizes",
                  lambda bootstrap, topics, **kwargs: {topic: (10_000_000, 2) for topic in topics}),
        ]
        entries += list(extra or [])
        return entries

    def control_factory(self, address, *, digest):
        control = FakeDispatcherControl(address, digest=digest, client=self.client)
        control.plan_summary = getattr(self, "plan_summary", None)
        return control

    # -- running ----------------------------------------------------------------------------------------------
    @contextlib.contextmanager
    def patched(self, extra=None):
        with contextlib.ExitStack() as stack:
            for entry in self.patches(extra):
                stack.enter_context(entry)
            yield

    def main(self, argv, extra=None):
        from neo4j_graph_loader import cli
        with self.patched(extra):
            return cli.main(argv)

    def start(self, *flags, extra=None):
        base = ["start", "--run-dir", str(self.tmp / "runs"), "--run-id", "run-parity"]
        return self.main(base + list(flags), extra)


def _reset_module_state():
    from neo4j_graph_loader import cli
    from neo4j_graph_loader.orchestrator import docker_service
    cli._stop_locks.clear()
    cli._stop_initiated.clear()
    cli._stop_done.clear()
    docker_service._launched_at.clear()
    docker_service._signal_ready.clear()


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.lines: list[str] = []

    def emit(self, record):
        self.lines.append(f"{record.levelname} {record.getMessage()}")


def run_scenario(function) -> dict:
    """Run one scenario in a fresh scratch directory; return its normalized trace and outcome."""
    _reset_module_state()
    with tempfile.TemporaryDirectory(prefix="parity-") as raw:
        tmp = Path(raw).resolve()
        ctx = Context(tmp)
        capture = _Capture()
        root = logging.getLogger()
        root.addHandler(capture)
        previous_directory = os.getcwd()
        os.chdir(tmp)          # D5: relative paths (``var/rejections``) resolve inside the scratch directory
        clean = {"PATH": os.environ.get("PATH", ""), "HOME": os.environ.get("HOME", ""), "GRAPH_LOADER_METRICS_ENABLED": "0"}
        try:
            with patch.dict(os.environ, clean, clear=True):
                outcome = function(ctx)
        except BaseException as exc:  # an escaping exception is part of the observable outcome
            outcome = {"exception": type(exc).__name__, "message": str(exc)}
        finally:
            os.chdir(previous_directory)
            root.removeHandler(capture)
        attempts = sorted(path.name for path in (tmp / "runs").glob("*")) if (tmp / "runs").exists() else []
        attempt = attempts[0] if attempts else None
        record = {"trace": lanes(ctx.recorder.events), "outcome": outcome,
                  "problems": [line for line in capture.lines if line.startswith("ERROR")]}
        return normalize(json.loads(json.dumps(record, default=repr)), version=ctx.version, attempt=attempt, tmp=str(tmp))


# ---------------------------------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------------------------------
def _outcome(rc):
    return {"exit_code": rc}


def scenario_stream_start_stop(ctx):
    """Stream fleet starts (lease server, nodes, edges), runs until the operator stops it."""
    return _outcome(ctx.start("--mode", "stream"))


def _planned_plan(ctx, path):
    """Write a small two-type plan file at ``path`` plus the dispatcher summary that completes it."""
    from neo4j_graph_loader.loader.edge_plan import ChunkSpec, ResourceTable, assemble_plan, resource_pair, write_plan_file
    table = ResourceTable(["Person", "Title"], 2)
    specs = [
        ChunkSpec("A:0", "ACTED_IN", ("ACTED_IN", 0), 0, resource_pair(0, 2), 3, 0, 0),
        ChunkSpec("A:1", "ACTED_IN", ("ACTED_IN", 0), 1, resource_pair(0, 2), 2, 0, 1),
        ChunkSpec("E:0", "EPISODE_OF", ("EPISODE_OF", 1), 0, resource_pair(3, 3), 4, 1, 0),
    ]
    plan = assemble_plan(plan_id="plan-9", topic="chunks", partitions=2, resources=table,
                         types={"ACTED_IN": ("Person", "Title", 5), "EPISODE_OF": ("Title", "Title", 4)}, specs=specs)
    write_plan_file(path, plan)
    ledger = [[chunk.id, "w0", chunk.rows, 100.0 + chunk.id, 101.0 + chunk.id, 5.0] for chunk in plan.chunks]
    ctx.plan_summary = {"state": "complete", "plan_id": plan.plan_id, "failure": None, "ledger": ledger,
                        "types": {"ACTED_IN": {"rows": 5, "chunks": 2, "write_ms": 10.0, "first_grant": 100.0, "last_done": 102.0}},
                        "stats": {"grants": 3}}


def _planned(ctx, *flags):
    """The plan file appears after the first node label is ready and the second label waits for the dispatcher,
    so the scheduler thread's view of the ready labels (and therefore the trace) does not depend on timing."""
    import time
    holder = {}

    def publish_plan_then_wait_for_dispatcher():
        _planned_plan(ctx, holder["path"])
        deadline = time.monotonic() + 30
        while not any(c.labels.get("component") == "planned-dispatcher" for c in ctx.client.containers_by_name.values()):
            if time.monotonic() > deadline:
                raise TimeoutError("dispatcher never launched")
            time.sleep(0.01)

    plan = holder["path"] = ctx.tmp / "edge-plan.json"
    ctx.monitor_script["after_first_ready"] = publish_plan_then_wait_for_dispatcher
    return _outcome(ctx.start("--mode", "bulk", "--edge-plan", str(plan), "--plan-writers", "2", *flags))


def scenario_planned_bulk(ctx):
    """Planned bulk, default overlap: nodes, one dispatcher, two writers."""
    return _planned(ctx)


def scenario_planned_bulk_overlap_labels(ctx):
    """Planned bulk with --bulk-edge-overlap labels (classic-only option: ignored with a warning)."""
    return _planned(ctx, "--bulk-edge-overlap", "labels")


def scenario_planned_bulk_overlap_shared(ctx):
    """Planned bulk with --bulk-edge-overlap shared."""
    return _planned(ctx, "--bulk-edge-overlap", "shared")


def scenario_edge_classic_overlap_labels(ctx):
    """--edge-classic with the default serialization of edge types that share an endpoint label."""
    return _outcome(ctx.start("--mode", "bulk", "--edge-classic", "--bulk-edge-overlap", "labels"))


def scenario_edge_classic_overlap_shared(ctx):
    """--edge-classic with every type scheduled together."""
    return _outcome(ctx.start("--mode", "bulk", "--edge-classic", "--bulk-edge-overlap", "shared"))


def scenario_edge_leases(ctx):
    """--edge-leases: one global bucket-lease coordinator for the whole bulk edge fleet."""
    return _outcome(ctx.start("--mode", "bulk", "--edge-leases"))


def scenario_stream_clock_rotation(ctx):
    """Deprecated --stream-clock-rotation: a global batch clock plus rotating edge loaders (needs a schema
    without self-referencing edges, or the clock isolation policy refuses it)."""
    schema = Path(__file__).resolve().parent / "golden" / "clock_rotation_schema.yaml"
    return _outcome(ctx.start("--mode", "stream", "--stream-clock-rotation", "--config", str(schema)))


def scenario_partial_launch_rollback(ctx):
    """The second edge loader fails to launch: everything already started is stopped and removed."""
    ctx.client.script[("run", "graph-loader-edge-KNOWS-0-72756e2d706172697479")] = APIError("boom")
    return _outcome(ctx.start("--mode", "stream"))


def scenario_loader_crash_stream(ctx):
    """The relationship supervisor reports a crashed edge loader; the fleet is torn down (exit 1)."""
    from neo4j_graph_loader.orchestrator.bulk_monitor import LoaderCrashError
    ctx.monitor_script["supervise"] = LoaderCrashError("edge WORKS_AT replica 0 exited with code 1")
    return _outcome(ctx.start("--mode", "stream"))


def scenario_loader_crash_bulk(ctx):
    """A bulk node loader crashes before completion: the fleet is stopped, diagnostics read, containers removed."""
    from neo4j_graph_loader.orchestrator.bulk_monitor import LoaderCrashError
    ctx.monitor_script["completion"] = LoaderCrashError("node Person replica 0 exited with code 1")
    return _outcome(ctx.start("--mode", "bulk", "--edge-classic"))


def scenario_stop_during_startup(ctx):
    """SIGTERM arrives while the first node loader is being launched."""
    import signal
    sent = []

    class Raiser(Exception):
        pass

    original_run = ctx.client._run

    def run_and_signal(**kwargs):
        container = original_run(**kwargs)
        if kwargs.get("labels", {}).get("component") == "node-loader" and not sent:
            sent.append(kwargs["name"])
            os.kill(os.getpid(), signal.SIGTERM)
        return container

    ctx.client.containers.run = run_and_signal
    return _outcome(ctx.start("--mode", "stream"))


def scenario_stop_command(ctx):
    """`graph-loader stop` against a daemon that holds a node loader and another run's loader."""
    for label, replica in (("Person", "0"), ("Company", "0")):
        ctx.client._run(name=f"graph-loader-node-{label}-{replica}-x", labels={"app": "graph-loader", "component": "node-loader", "node_label": label})
    ctx.recorder.events.clear()
    with ctx.patched():
        from neo4j_graph_loader import cli
        rc = cli.main(["stop"])
        rc_one = cli.main(["stop", "--loader", "Person"])
    return {"exit_code": rc, "exit_code_single": rc_one}


def scenario_status_command(ctx):
    """`graph-loader status` lists node loaders by label filter."""
    ctx.client._run(name="graph-loader-node-Person-0-x", labels={"app": "graph-loader", "component": "node-loader", "node_label": "Person"})
    ctx.recorder.events.clear()
    with ctx.patched():
        from neo4j_graph_loader import cli
        return _outcome(cli.main(["status"]))


def scenario_replay_dlq(ctx):
    """`graph-loader replay-dlq --dry-run`: no Docker traffic at all."""
    from neo4j_graph_loader.loader import dead_letter_replay
    summary = dead_letter_replay.ReplaySummary(matched=0, replayed=0, dry_run=True)
    with ctx.patched([patch.object(dead_letter_replay, "run_replay", lambda **kwargs: summary)]):
        from neo4j_graph_loader import cli
        rc = cli.main(["replay-dlq", "--node-label", "Person", "--dry-run", "--dlq-topic", "person-events.dlq"])
    return _outcome(rc)


class _NeverShutdown:
    def is_set(self):
        return False


def _kafka_doubles(topics):
    admin, watermark, control = MagicMock(), MagicMock(), MagicMock()
    admin.list_topics.return_value = SimpleNamespace(topics={
        topic: SimpleNamespace(error=None, partitions={index: SimpleNamespace() for index in range(count)})
        for topic, count in topics.items()})
    watermark.get_watermark_offsets.return_value = (0, 3)
    control.poll.return_value = None
    return admin, watermark, control


def _schema():
    from neo4j_graph_loader._resources import schema_path
    from neo4j_graph_loader.utils.schema_loader import load_schema
    with schema_path(None, sample="default") as path:
        return load_schema(path)


def _relationship_monitor(ctx, tracked, **kwargs):
    from neo4j_graph_loader.orchestrator.relationship_bulk_monitor import RelationshipBulkMonitor
    schema = _schema()
    edge = next(item for item in schema.edges if item.type == "WORKS_AT")
    admin, watermark, control = _kafka_doubles({topic: 1 for topic in edge.topics})
    return RelationshipBulkMonitor(
        [edge], "run-parity", {("WORKS_AT", "0")}, {("WORKS_AT", "0"): tracked},
        bootstrap_servers="kafka:9092", consumer_group_prefix="graph-loader", admin_client=admin,
        watermark_consumer=watermark, control_consumer=control, poll_interval_seconds=0.01, **kwargs)


def _edge_loader(ctx, service):
    return service.run_edge_loader(edge_type="WORKS_AT", mode="stream", config_path="/app/runtime_schema.yaml",
                                   topic="works-at-events", run_id="run-parity", network="graph-loader-net")[0]


def scenario_lease_server_replacement(ctx):
    """The stream supervisor replaces a dead lease server (restart with backoff), then gives up after the cap."""
    from neo4j_graph_loader.models.schema import BucketLeaseStreamConfig
    service = ctx.service()
    lease = service.run_bucket_lease_server(run_id="run-parity", bucket_count=16, network="graph-loader-net",
                                            keyed_labels=["Person"])
    monitor = _relationship_monitor(ctx, _edge_loader(ctx, service), tracked_lease_server=lease,
                                    bucket_lease_config=BucketLeaseStreamConfig(
                                        max_consecutive_restarts=2, restart_backoff_base_ms=1, restart_backoff_max_ms=1),
                                    docker_service=service, lease_bucket_count=16, lease_keyed_labels=("Person",))
    monitor._sleep = lambda seconds: None
    outcomes = []
    for attempt in range(3):
        current = monitor.tracked_lease_server
        ctx.client.containers_by_name[current.name].attrs["State"].update(Status="exited", ExitCode=137)
        ctx.client.containers_by_name[current.name].log_bytes = b"coordinator killed"
        try:
            monitor._check_lease_server_health(_NeverShutdown())
            outcomes.append({"restarted": True, "tracked": monitor.tracked_lease_server.name})
        except Exception as exc:
            outcomes.append({"exception": type(exc).__name__, "message": str(exc)})
    monitor.close()
    return {"attempts": outcomes}


def scenario_crash_detection_real_monitors(ctx):
    """Real monitors, watcher and diagnostics over exited, removed and healthy containers."""
    from neo4j_graph_loader import cli
    from neo4j_graph_loader.orchestrator.bulk_monitor import BulkMonitor
    service = ctx.service()
    schema = _schema()
    nodes = service.run_node_loader(node_label="Person", topic="person-events", mode="bulk",
                                    config_path="/app/runtime_schema.yaml", replicas=2, run_id="run-parity",
                                    network="graph-loader-net")
    fake = ctx.client.containers_by_name
    outcomes = {}

    def attempt(label, call):
        try:
            outcomes[label] = {"returned": call()}
        except Exception as exc:
            outcomes[label] = {"exception": type(exc).__name__, "message": str(exc)}

    admin, watermark, control = _kafka_doubles({"person-events": 2, "company-events": 1})
    expected = {("Person", "0"), ("Person", "1")}
    bulk = BulkMonitor(schema, "run-parity", expected, {("Person", str(i)): node for i, node in enumerate(nodes)},
                       bootstrap_servers="kafka:9092", admin_client=admin, watermark_consumer=watermark,
                       control_consumer=control, poll_interval_seconds=0.01)
    attempt("bulk_healthy", bulk._check_container_health)
    fake[nodes[1].name].attrs["State"].update(Status="exited", ExitCode=1)
    fake[nodes[1].name].log_bytes = b"boom: write failed"
    attempt("bulk_exited", bulk._check_container_health)
    fake[nodes[0].name].gone = True
    attempt("bulk_removed", bulk._check_container_health)
    cli._log_failed_loader_diagnostics(nodes)
    watcher = cli._NodeLoaderWatcher(nodes, __import__("threading").Event(), interval=0.0)
    watcher._run()
    outcomes["watcher_failed"] = watcher.failed
    bulk.close()

    edge = _edge_loader(ctx, service)
    relationship = _relationship_monitor(ctx, edge)
    attempt("edge_healthy", relationship._check_health)
    fake[edge.name].attrs["State"].update(Status="exited", ExitCode=1)
    fake[edge.name].log_bytes = b"edge boom"
    attempt("edge_exited", relationship._check_health)
    fake[edge.name].gone = True
    attempt("edge_removed", relationship._check_health)
    relationship.close()
    return outcomes


SCENARIOS = {
    "stream_start_stop": scenario_stream_start_stop,
    "stream_clock_rotation": scenario_stream_clock_rotation,
    "planned_bulk": scenario_planned_bulk,
    "planned_bulk_overlap_labels": scenario_planned_bulk_overlap_labels,
    "planned_bulk_overlap_shared": scenario_planned_bulk_overlap_shared,
    "edge_classic_overlap_labels": scenario_edge_classic_overlap_labels,
    "edge_classic_overlap_shared": scenario_edge_classic_overlap_shared,
    "edge_leases": scenario_edge_leases,
    "partial_launch_rollback": scenario_partial_launch_rollback,
    "loader_crash_stream": scenario_loader_crash_stream,
    "loader_crash_bulk": scenario_loader_crash_bulk,
    "stop_during_startup": scenario_stop_during_startup,
    "stop_command": scenario_stop_command,
    "status_command": scenario_status_command,
    "replay_dlq": scenario_replay_dlq,
    "lease_server_replacement": scenario_lease_server_replacement,
    "crash_detection_real_monitors": scenario_crash_detection_real_monitors,
}


def help_texts() -> dict:
    """`--help` of the top-level parser and every subcommand, at a fixed width."""
    from neo4j_graph_loader import cli
    with patch.dict(os.environ, {"COLUMNS": "100"}):
        parser = cli.build_parser()
        choices = next(action.choices for action in parser._actions if isinstance(action, argparse._SubParsersAction))
        return {"": parser.format_help(), **{name: sub.format_help() for name, sub in sorted(choices.items())}}


def run_all(names=None) -> dict:
    return {name: run_scenario(function) for name, function in SCENARIOS.items() if not names or name in names}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--emit", required=True, help="write the normalized trace JSON here")
    parser.add_argument("--only", action="append")
    parser.add_argument("--source", default="", help="recorded in the file: what produced it (e.g. a commit)")
    options = parser.parse_args(argv)
    os.environ["GRAPH_LOADER_METRICS_ENABLED"] = "0"
    result = {"_source": options.source, "_help": help_texts(), "scenarios": run_all(options.only)}
    Path(options.emit).write_text(json.dumps(result, indent=1, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
