"""The aggregated ``/metrics`` endpoint of the process runtime (E15.F2.S8, D15-3).

``graph-loader start --runtime process --metrics-port N`` serves ONE endpoint that merges the CLI's own registry and a scrape of
every child's own endpoint, so a Prometheus (a ``static_configs`` target, a Kubernetes ``Service``/``PodMonitor``) needs one
stable address instead of the children's ephemeral ports. Per-child endpoints stay available through ``file_sd``; a deployment
scrapes EITHER the children OR the aggregate, never both (a shipped alert rule flags the same ``exporter`` seen from two jobs).

Merging: one ``# HELP`` / ``# TYPE`` per family (the first one seen; samples of a family whose type differs are dropped and
counted); samples are unique because every child's ``exporter`` label differs (a sample whose label set is already present is
dropped and counted, never emitted twice). A child that exits stops being listed (Prometheus staleness removes its series); a
restarted child is a new ``exporter``. Scrape health per child is ``graph_loader_exporter_up{exporter}``: 1 when this scrape
of the child worked, 0 when it failed (its series are then absent from this scrape). A child whose metrics are disabled by
choice has no endpoint and no ``exporter_up``; one whose endpoint failed to bind is 0.
"""
from __future__ import annotations

import logging
import threading
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable

from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.core import Metric
from prometheus_client.parser import text_string_to_metric_families
from prometheus_client.samples import Sample

logger = logging.getLogger(__name__)

EXPORTER_UP = "graph_loader_exporter_up"
EXPORTER_UP_HELP = "1 when the aggregated endpoint's scrape of this exporter succeeded, 0 when it failed (E15 run status)."
CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


def default_fetch(target: str, timeout: float) -> str:
    with urllib.request.urlopen(f"http://{target}/metrics", timeout=timeout) as response:      # noqa: S310 - loopback or pod-local
        return response.read().decode("utf-8")


class _Merged:
    def __init__(self, families: list[Metric]):
        self._families = families

    def collect(self):
        return iter(self._families)


def merge_expositions(parts: list[str], *, up: list[tuple[dict[str, str], int]] = ()) -> tuple[bytes, dict[str, int]]:
    """Merge Prometheus text expositions into one; ``up`` adds ``graph_loader_exporter_up`` rows ``(labels, 0|1)``.
    Returns the text and ``{"type_conflicts": n, "duplicates": n}``."""
    families: dict[str, Metric] = {}
    seen: dict[str, set] = {}
    stats = {"type_conflicts": 0, "duplicates": 0}
    for text in parts:
        for family in text_string_to_metric_families(text):
            target = families.get(family.name)
            if target is None:
                target = families[family.name] = Metric(family.name, family.documentation, family.type, family.unit)
                seen[family.name] = set()
            elif target.type != family.type:
                stats["type_conflicts"] += len(family.samples)
                continue
            for sample in family.samples:
                key = (sample.name, tuple(sorted(sample.labels.items())))
                if key in seen[family.name]:
                    stats["duplicates"] += 1
                    continue
                seen[family.name].add(key)
                target.samples.append(Sample(sample.name, dict(sample.labels), sample.value, sample.timestamp, sample.exemplar))
    ordered = list(families.values())
    if up:
        health = Metric(EXPORTER_UP, EXPORTER_UP_HELP, "gauge")
        health.samples = [Sample(EXPORTER_UP, dict(labels), float(value)) for labels, value in up]
        ordered = [family for family in ordered if family.name != EXPORTER_UP] + [health]
    registry = CollectorRegistry(auto_describe=False)
    registry.register(_Merged(ordered))
    return generate_latest(registry), stats


class AggregatedMetrics:
    """The merged endpoint: ``own()`` is the CLI's exposition (bytes); ``targets()`` the file_sd groups of the live children."""

    def __init__(self, host: str, port: int, *, own: Callable[[], bytes], targets: Callable[[], list[dict]], timeout: float = 2.0,
                 fetch: Callable[[str, float], str] = default_fetch, max_parallel: int = 16):
        self._host, self._port, self._own, self._targets = host, port, own, targets
        self._timeout, self._fetch, self._parallel = timeout, fetch, max_parallel
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self.scrapes = 0
        self.failures: dict[str, int] = {}

    # -- rendering ----------------------------------------------------------------------------------------------
    def render(self) -> bytes:
        groups = [group for group in self._targets() if (group.get("labels") or {}).get("component") != "cli"]
        scraped = [group for group in groups if group.get("targets")]
        results: dict[str, str | None] = {}

        def pull(group: dict) -> tuple[str, str | None]:
            exporter = group["labels"]["exporter"]
            try:
                return exporter, self._fetch(group["targets"][0], self._timeout)
            except Exception as exc:                                  # noqa: BLE001 - a child that cannot be scraped is exporter_up 0
                self.failures[exporter] = self.failures.get(exporter, 0) + 1
                logger.debug("stage=aggregated-metrics reason=scrape of %s failed: %s", exporter, exc)
                return exporter, None

        if scraped:
            with ThreadPoolExecutor(max_workers=min(self._parallel, len(scraped)), thread_name_prefix="aggregate-scrape") as pool:
                results = dict(pool.map(pull, scraped))
        up: list[tuple[dict[str, str], int]] = []
        for group in groups:
            labels = group.get("labels") or {}
            health = {key: str(labels[key]) for key in ("exporter", "run_id", "execution_id", "runtime") if key in labels}
            if group.get("targets"):
                up.append((health, 1 if results.get(labels["exporter"]) is not None else 0))
            elif labels.get("metrics_state") == "failed":
                up.append((health, 0))                                # it could not bind; "disabled" is a choice and has no row
        body, stats = merge_expositions([self._own().decode("utf-8"), *[text for text in results.values() if text is not None]], up=up)
        self.scrapes += 1
        if stats["type_conflicts"] or stats["duplicates"]:
            logger.warning("stage=aggregated-metrics reason=merge dropped %s sample(s) of a conflicting type and %s duplicate sample(s)",
                           stats["type_conflicts"], stats["duplicates"])
        return body

    # -- serving ------------------------------------------------------------------------------------------------
    def start(self) -> int:
        aggregator = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):                                         # noqa: N802
                if self.path.split("?", 1)[0] != "/metrics":
                    self.send_error(404)
                    return
                try:
                    body = aggregator.render()
                except Exception as exc:                              # noqa: BLE001
                    logger.warning("stage=aggregated-metrics reason=render failed: %s", exc)
                    self.send_error(500)
                    return
                self.send_response(200)
                self.send_header("Content-Type", CONTENT_TYPE)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):                             # no access log
                pass

        self._server = ThreadingHTTPServer((self._host, self._port), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, name="aggregated-metrics", daemon=True)
        self._thread.start()
        return self._server.server_port

    def close(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
