"""``graph-loader monitoring export``: write the monitoring bundle for the user's own Prometheus and Grafana (E15.F2.S10).

The shipped assets (``neo4j_graph_loader/monitoring/``) are the ones the bundled quick-start stack uses. The export turns them
into a self-contained directory for any Prometheus and Grafana:

``prometheus/scrape-config.yml``   the ``scrape_configs:`` snippet for ``--discovery {docker,file,kubernetes,static}``
``prometheus/prometheus.yml``      a minimal complete config around it (what ``promtool check config`` is run on)
``prometheus/alerts/*.yml``        the alert rules, with thresholds, ``for``, severities and labels from ``--values``
``prometheus/alert_tests/``        the shipped promtool tests (defaults only) and ``generated_test.yml`` (boundary tests of
                                   every overridable threshold, generated from the final values)
``grafana/dashboards/``            the overview dashboard for PROVISIONING (no ``__inputs``; its datasource variable is set from
                                   ``--datasource-uid``), ``$__rate_interval``; ``import/`` the same dashboard with ``__inputs``
                                   for the import dialog or HTTP API; ``dashboards-sql/`` the PostgreSQL run-history dashboard
``grafana/provisioning/``          datasource and dashboard providers (``--datasource-uid``, ``--prometheus-url``)
``kubernetes/``                    ``--discovery kubernetes``: Service, ServiceMonitor and PodMonitor for the aggregated port
``manifest.json``                  the options used and the sha256 of every file

Every sample carries ``run_id``, ``execution_id``, ``exporter`` and ``runtime`` (see ``utils/identity.py``), so the rules and
dashboards use metric labels only and work with any discovery. Every scrape configuration sets ``honor_labels: true``.
The four identity names are reserved and rejected by ``--extra-labels``.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
import re
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any

import yaml

from neo4j_graph_loader.utils.identity import RESERVED_LABELS, ReservedLabelError, validate_extra_labels

DISCOVERIES = ("docker", "file", "kubernetes", "static")
DEFAULT_TARGETS_FILE = "/etc/prometheus/graph-loader/targets.json"
DEFAULT_DOCKER_NETWORK = "graph-loader-net"
DEFAULT_PROMETHEUS_URL = "http://prometheus:9090"
SERVICE_NAME_LABEL = "app.kubernetes.io/name"
SERVICE_NAME = "graph-loader"
METRICS_PORT_NAME = "metrics"
DS_PROMETHEUS = "DS_PROMETHEUS"
DS_POSTGRES = "DS_POSTGRES"
POSTGRES_PLUGIN = "grafana-postgresql-datasource"
# label names an operator may not add: the identity names, Prometheus's own, and the metric labels an exporter already sets
EXTRA_LABEL_FORBIDDEN = frozenset(RESERVED_LABELS) | {"job", "instance", "kind", "name", "le", "quantile", "role"}
LABEL_NAME = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")
DURATION = re.compile(r"(\d+)(ms|s|m|h|d|w)")
JOB_NAME = re.compile(r"[A-Za-z0-9_.-]{1,64}")
ALERT_BLOCK = re.compile(r"^(\s*)- alert:\s*(\S+)\s*$")
GROUP_START = re.compile(r"^(\s*)- name:\s*\S+")
SEVERITIES = ("info", "warning", "critical")
IDENTITY = 'execution_id="x1",exporter="%s-aaaaaaaa",run_id="r1",runtime="process"'


class ExportError(ValueError):
    """The export options or values file are invalid; nothing was written."""


# -- shipped assets -------------------------------------------------------------------------------------------------------
def assets() -> Any:
    return resources.files("neo4j_graph_loader.monitoring")


def asset_text(*parts: str) -> str:
    node = assets()
    for part in parts:
        node = node / part
    return node.read_text(encoding="utf-8")


def asset_files() -> dict[str, bytes]:
    """Every shipped monitoring file by posix path relative to the package (data files only)."""
    found: dict[str, bytes] = {}

    def walk(node, prefix: str) -> None:
        for child in node.iterdir():
            if child.is_dir():
                if child.name != "__pycache__":
                    walk(child, f"{prefix}{child.name}/")
            elif not child.name.endswith((".py", ".pyc")):
                found[f"{prefix}{child.name}"] = child.read_bytes()
    walk(assets(), "")
    return found


# -- validation -----------------------------------------------------------------------------------------------------------
def parse_duration_seconds(text: str, what: str) -> float:
    match = DURATION.fullmatch(str(text).strip())
    if not match:
        raise ExportError(f"{what} must be a duration such as 15s, 1m or 2h, got {text!r}")
    return int(match.group(1)) * {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}[match.group(2)]


def parse_extra_labels(text: str | None) -> dict[str, str]:
    """``k=v,k2=v2`` -> dict; reserved, Prometheus-owned or malformed names are refused."""
    labels: dict[str, str] = {}
    for item in [part for part in (text or "").split(",") if part.strip()]:
        name, separator, value = item.partition("=")
        name = name.strip()
        if not separator:
            raise ExportError(f"--extra-labels expects name=value pairs, got {item!r}")
        labels[name] = value.strip()
    return validate_labels(labels)


def validate_labels(labels: dict[str, str]) -> dict[str, str]:
    try:
        validate_extra_labels(labels)
    except ReservedLabelError as exc:
        raise ExportError(str(exc)) from exc
    for name, value in labels.items():
        if not LABEL_NAME.fullmatch(name) or name.startswith("__"):
            raise ExportError(f"label name {name!r} is not a valid Prometheus label name")
        if name in EXTRA_LABEL_FORBIDDEN:
            raise ExportError(f"label name {name!r} is reserved (identity labels, or already set by Prometheus or the exporters)")
        if not isinstance(value, str) or not value or len(value) > 128 or any(ch in value for ch in "\n\r"):
            raise ExportError(f"label {name!r} needs a non-empty single-line value of at most 128 characters")
    return dict(labels)


def parse_host_port(text: str, what: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+:\d{1,5}", text) or not 0 < int(text.rsplit(":", 1)[1]) < 65536:
        raise ExportError(f"{what} must be host:port, got {text!r}")
    return text


# -- alert rules ----------------------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class Param:
    file: str
    default: float
    kind: str                       # gauge | rate | plan_wait | slope
    metric: str
    selector: str = ""             # extra series labels
    summary_literals: tuple[tuple[str, str], ...] = ()      # (text containing {v}, default text) replaced with the new value


ALERT_PARAMS: dict[str, Param] = {
    "GraphLoaderWriteOutage": Param("write_outage.yml", 120, "gauge", "graph_loader_write_outage_seconds", 'kind="edge",name="T"'),
    "GraphLoaderDlqBacklogHigh": Param("dlq.yml", 100, "gauge", "graph_loader_dlq_backlog", 'kind="edge",name="T",queue="dlq"'),
    "GraphLoaderRetryStalled": Param("dlq.yml", 600, "gauge", "graph_loader_retry_stalled_seconds", 'kind="edge",name="T"'),
    "GraphLoaderConsumerLagHigh": Param("lag_and_errors.yml", 100000, "gauge", "graph_loader_consumer_lag_records", 'kind="cli",name="T",topic="t"'),
    "GraphLoaderConsumerLagCritical": Param("lag_and_errors.yml", 1000000, "gauge", "graph_loader_consumer_lag_records", 'kind="cli",name="T",topic="t"'),
    "GraphLoaderRetryRateHigh": Param("lag_and_errors.yml", 1, "rate", "graph_loader_retry_total", 'kind="edge",name="T",reason="deadlock"',
                                      (("more than {v}/s", "more than 1/s"),)),
    "GraphLoaderConsumerLagNotDraining": Param("lag_and_errors.yml", 0, "slope", "graph_loader_consumer_lag_records", 'kind="cli",name="T",topic="t"'),
    "GraphLoaderPlanResourceWaitHigh": Param("concurrency.yml", 5, "plan_wait", "graph_loader_plan_resource_wait_seconds",
                                             'kind="planned-dispatcher",name="-"', (("exceeded {v}s", "exceeded 5s"),)),
}
VALUE_KEYS = {"job_name", "labels", "alerts", "disable"}
ALERT_KEYS = {"threshold", "for", "severity"}


def number(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else repr(float(value))


def alert_files() -> dict[str, str]:
    return {name: asset_text("prometheus", "alerts", name) for name in sorted(
        child.name for child in (assets() / "prometheus" / "alerts").iterdir() if child.name.endswith(".yml"))}


def shipped_alert_names() -> set[str]:
    return {rule["alert"] for text in alert_files().values() for group in yaml.safe_load(text)["groups"] for rule in group["rules"]}


def load_values(path: str | Path | None) -> dict:
    if path is None:
        return {}
    try:
        values = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise ExportError(f"cannot read the values file {path}: {exc}") from exc
    return check_values(values)


def check_values(values: Any) -> dict:
    """Validate a parsed values document (unknown keys, alerts or thresholds are errors, never ignored)."""
    if not isinstance(values, dict):
        raise ExportError("the values file must be a mapping")
    unknown = sorted(set(values) - VALUE_KEYS)
    if unknown:
        raise ExportError(f"unknown key(s) in the values file: {', '.join(unknown)} (allowed: {', '.join(sorted(VALUE_KEYS))})")
    known = shipped_alert_names()
    if "job_name" in values and not JOB_NAME.fullmatch(str(values["job_name"])):
        raise ExportError(f"job_name {values['job_name']!r} is not a valid job name")
    if "labels" in values:
        if not isinstance(values["labels"], dict):
            raise ExportError("labels must be a mapping of label name to value")
        validate_labels({str(k): v for k, v in values["labels"].items()})
    for name in values.get("disable") or []:
        if name not in known:
            raise ExportError(f"cannot disable unknown alert {name!r}")
    for name, spec in (values.get("alerts") or {}).items():
        if name not in known:
            raise ExportError(f"unknown alert {name!r} in the values file")
        if not isinstance(spec, dict) or set(spec) - ALERT_KEYS:
            raise ExportError(f"alert {name}: keys must be among {', '.join(sorted(ALERT_KEYS))}")
        if "threshold" in spec:
            if name not in ALERT_PARAMS:
                raise ExportError(f"alert {name} has no overridable threshold (overridable: {', '.join(sorted(ALERT_PARAMS))})")
            threshold = spec["threshold"]
            if isinstance(threshold, bool) or not isinstance(threshold, (int, float)) or not math.isfinite(threshold) or threshold <= 0:
                raise ExportError(f"alert {name}: threshold must be a positive number")
        if "for" in spec:
            parse_duration_seconds(spec["for"], f"alert {name}: for")
        if "severity" in spec and spec["severity"] not in SEVERITIES:
            raise ExportError(f"alert {name}: severity must be one of {', '.join(SEVERITIES)}")
    return values


def render_alert_files(values: dict, job_name: str, extra_labels: dict[str, str]) -> dict[str, str]:
    """The alert rule files after the values are applied. With no values and the default job the text equals the shipped files
    (comments included); an overridden alert gets one comment line saying so."""
    disabled = set(values.get("disable") or [])
    overrides = values.get("alerts") or {}
    rendered: dict[str, str] = {}
    for filename, text in alert_files().items():
        lines = text.splitlines()
        if filename == "health.yml" and job_name != "graph-loader":
            lines = [line.replace('job="graph-loader"', f'job="{job_name}"') for line in lines]
        starts = [index for index, line in enumerate(lines) if ALERT_BLOCK.match(line) or GROUP_START.match(line)]
        out: list[str] = lines[:starts[0]] if starts else list(lines)
        for position, begin in enumerate(starts):
            end = starts[position + 1] if position + 1 < len(starts) else len(lines)
            block = lines[begin:end]
            match = ALERT_BLOCK.match(block[0])
            if not match:
                out.extend(block)
                continue
            name = match.group(2)
            if name in disabled:
                continue
            out.extend(rewrite_alert(block, name, overrides.get(name) or {}, extra_labels))
        rendered[filename] = drop_empty_groups("\n".join(out) + "\n")
    return {name: text for name, text in rendered.items() if yaml.safe_load(text).get("groups")}


def rewrite_alert(block: list[str], name: str, spec: dict, extra_labels: dict[str, str]) -> list[str]:
    notes = []
    block = list(block)
    if "threshold" in spec:
        param = ALERT_PARAMS[name]
        old, new = number(param.default), number(spec["threshold"])
        pattern = re.compile(r"(>\s*)" + re.escape(old) + r"(?![\d.])")
        start = next((i for i, line in enumerate(block) if line.lstrip().startswith("expr:")), None)
        stop = next((i for i in range(start + 1, len(block)) if re.match(r"^\s+(for|labels|annotations):", block[i])), len(block)) if start is not None else 0
        hits = 0
        for index in range(start or 0, stop):
            block[index], count = pattern.subn(lambda m: m.group(1) + new, block[index])
            hits += count
        if start is None or hits == 0:
            raise ExportError(f"internal: the threshold of {name} ({old}) was not found in the shipped rule")
        for index, line in enumerate(block):
            for template, default_text in param.summary_literals:
                if default_text in line:
                    block[index] = line.replace(default_text, template.format(v=new))
        notes.append(f"threshold {new} (default {old})")
    if "for" in spec:
        block = substitute_line(block, r"^(\s+)for:\s*\S+", lambda m: f"{m.group(1)}for: {spec['for']}", name, "for")
        notes.append(f"for {spec['for']}")
    if "severity" in spec:
        block = substitute_line(block, r"^(\s+)severity:\s*\S+", lambda m: f"{m.group(1)}severity: {spec['severity']}", name, "severity")
        notes.append(f"severity {spec['severity']}")
    if extra_labels:
        for index, line in enumerate(block):
            match = re.match(r"^(\s+)severity:\s*\S+", line)
            if match:
                extras = [f"{match.group(1)}{label}: {json.dumps(value)}" for label, value in extra_labels.items()]
                block[index + 1:index + 1] = extras
                break
        else:
            raise ExportError(f"internal: no severity label in {name}")
    if notes:
        indent = re.match(r"^(\s*)", block[0]).group(1)
        block.insert(1, f"{indent}  # exported with {', '.join(notes)}")
    return block


def substitute_line(block: list[str], pattern: str, replace, name: str, what: str) -> list[str]:
    compiled = re.compile(pattern)
    for index, line in enumerate(block):
        match = compiled.match(line)
        if match:
            block[index] = replace(match)
            return block
    raise ExportError(f"internal: no {what} line in {name}")


def drop_empty_groups(text: str) -> str:
    lines = text.splitlines()
    starts = [index for index, line in enumerate(lines) if GROUP_START.match(line)]
    if not starts:
        return text
    out = lines[:starts[0]]
    for position, begin in enumerate(starts):
        end = starts[position + 1] if position + 1 < len(starts) else len(lines)
        segment = lines[begin:end]
        if any(ALERT_BLOCK.match(line) for line in segment):
            out.extend(segment)
    return "\n".join(out) + "\n"


def final_rules(files: dict[str, str]) -> dict[str, dict]:
    """alert name -> {file, rule} from rendered rule files."""
    rules = {}
    for filename, text in files.items():
        for group in yaml.safe_load(text)["groups"]:
            for rule in group["rules"]:
                rules[rule["alert"]] = {"file": filename, "rule": rule}
    return rules


# -- generated promtool tests ----------------------------------------------------------------------------------------------
def generated_tests(files: dict[str, str], values: dict) -> str | None:
    """Boundary tests for every overridable alert still present: at the threshold and below it nothing fires, above it fires
    exactly when ``for`` has elapsed. Written as ``promql_expr_test`` on ``ALERTS`` so they need no annotation text."""
    rules = final_rules(files)
    overrides = values.get("alerts") or {}
    tests = []
    used = []
    for name, param in ALERT_PARAMS.items():
        if name not in rules:
            continue
        threshold = float((overrides.get(name) or {}).get("threshold", param.default))
        for_value = rules[name]["rule"].get("for")
        for_seconds = parse_duration_seconds(str(for_value), f"{name}: for") if for_value not in (None, 0, "0") else 0
        for_minutes = int(math.ceil(for_seconds / 60))
        used.append(rules[name]["file"])
        tests.extend(cases(name, param, threshold, for_minutes))
    if not tests:
        return None
    document = {"rule_files": sorted({f"../alerts/{file}" for file in used}), "evaluation_interval": "1m", "tests": tests}
    header = ("# Generated by `graph-loader monitoring export` from the final alert values: boundary tests of every overridable\n"
              "# threshold (nothing fires at or below it, the alert fires above it once `for:` has elapsed, and not before).\n"
              "# Run: promtool test rules alert_tests/generated_test.yml\n")
    return header + yaml.safe_dump(document, sort_keys=False, width=1000)


def count_expectation(alert: str, eval_minutes: int, firing: bool) -> dict:
    return {"expr": f'count(ALERTS{{alertname="{alert}",alertstate="firing"}}) or vector(0)', "eval_time": f"{eval_minutes}m",
            "exp_samples": [{"labels": "{}", "value": 1 if firing else 0}]}


def cases(alert: str, param: Param, threshold: float, for_minutes: int) -> list[dict]:
    selector = param.selector
    labels = lambda extra="": "{" + ",".join(filter(None, [selector, IDENTITY % "x", extra])) + "}"      # noqa: E731
    horizon = for_minutes + 10
    result = []

    def gauge(value: float, checks: list[dict], note: str) -> None:
        result.append({"name": f"{alert}: {note}", "interval": "1m",
                       "input_series": [{"series": f"{param.metric}{labels()}", "values": f"{number(value)}x{horizon}"}],
                       "promql_expr_test": checks})

    if param.kind == "gauge":
        above, below = threshold * 1.1, threshold * 0.9
        fire_at = for_minutes + 2
        checks = [count_expectation(alert, fire_at, True)]
        if for_minutes >= 2:
            checks.insert(0, count_expectation(alert, for_minutes - 1, False))        # not before `for:` has elapsed
        gauge(above, checks, f"above {number(threshold)} fires after {for_minutes}m")
        gauge(threshold, [count_expectation(alert, horizon - 1, False)], f"exactly {number(threshold)} does not fire")
        gauge(below, [count_expectation(alert, horizon - 1, False)], f"below {number(threshold)} does not fire")
    elif param.kind == "rate":
        for factor, firing, note in ((1.5, True, "above"), (0.5, False, "below")):
            slope = threshold * 60 * factor
            result.append({"name": f"{alert}: {note} {number(threshold)}/s", "interval": "1m",
                           "input_series": [{"series": f"{param.metric}{labels()}", "values": f"0+{number(round(slope, 6))}x{horizon + 8}"}],
                           "promql_expr_test": [count_expectation(alert, for_minutes + 8, firing)]})
    elif param.kind == "slope":
        # deriv(lag[10m]) > threshold (records/s, default 0). Series step once a minute; a growth of k per step is k/60 per second.
        above = threshold * 1.5 if threshold > 0 else 0.1
        result.append({"name": f"{alert}: slope above {number(threshold)}/s fires after {for_minutes}m", "interval": "1m",
                       "input_series": [{"series": f"{param.metric}{labels()}", "values": f"0+{number(round(above * 60, 6))}x{horizon + 12}"}],
                       "promql_expr_test": ([count_expectation(alert, for_minutes - 1, False)] if for_minutes >= 2 else [])
                       + [count_expectation(alert, for_minutes + 12, True)]})
        flat = {"name": f"{alert}: a flat lag does not fire", "interval": "1m",
                "input_series": [{"series": f"{param.metric}{labels()}", "values": f"5x{horizon + 12}"}],
                "promql_expr_test": [count_expectation(alert, for_minutes + 12, False)]}
        result.append(flat)
        if threshold > 0:
            result.append({"name": f"{alert}: slope below {number(threshold)}/s does not fire", "interval": "1m",
                           "input_series": [{"series": f"{param.metric}{labels()}", "values": f"0+{number(round(threshold * 0.5 * 60, 6))}x{horizon + 12}"}],
                           "promql_expr_test": [count_expectation(alert, for_minutes + 12, False)]})
    elif param.kind == "plan_wait":
        for mass_le, firing, note in ((2 * threshold, True, "in the bucket above"), (threshold, False, "in the bucket at or below")):
            series = []
            for le in sorted({threshold, 2 * threshold}):
                series.append({"series": f"{param.metric}_bucket{labels('le=' + json.dumps(number(le)))}",
                               "values": f"0+100x{horizon + 8}" if le >= mass_le else f"0x{horizon + 8}"})
            infinity = 'le="+Inf"'
            series.append({"series": f"{param.metric}_bucket{labels(infinity)}", "values": f"0+100x{horizon + 8}"})
            result.append({"name": f"{alert}: p95 {note} {number(threshold)}s", "interval": "1m", "input_series": series,
                           "promql_expr_test": [count_expectation(alert, for_minutes + 8, firing)]})
    return result


# -- scrape configuration ---------------------------------------------------------------------------------------------------
def extra_relabelings(labels: dict[str, str]) -> list[dict]:
    return [{"target_label": name, "replacement": value} for name, value in labels.items()]


def docker_job(job_name: str, interval: str, network: str, labels: dict[str, str]) -> dict:
    shipped = next(job for job in yaml.safe_load(asset_text("prometheus", "prometheus.yml"))["scrape_configs"] if job["job_name"] == "graph-loader")
    job = copy.deepcopy(shipped)
    job["job_name"] = job_name
    job["scrape_interval"] = interval
    job["honor_labels"] = True
    for relabel in job["relabel_configs"]:
        if relabel.get("source_labels") == ["__meta_docker_network_name"]:
            relabel["regex"] = re.escape(network)
    job["relabel_configs"] = job["relabel_configs"] + extra_relabelings(labels)
    return job


def scrape_job(options: "ExportOptions", labels: dict[str, str]) -> dict:
    job_name, interval = options.job_name, options.scrape_interval
    if options.discovery == "docker":
        return docker_job(job_name, interval, options.docker_network, labels)
    job: dict[str, Any] = {"job_name": job_name, "scrape_interval": interval, "honor_labels": True}
    if options.discovery == "file":
        job["file_sd_configs"] = [{"files": [options.targets_file or DEFAULT_TARGETS_FILE], "refresh_interval": "5s"}]
    elif options.discovery == "static":
        job["static_configs"] = [{"targets": list(options.targets)}]
    else:
        job["kubernetes_sd_configs"] = [{"role": "endpoints", "namespaces": {"names": [options.namespace]}}]
        job["relabel_configs"] = [
            {"source_labels": ["__meta_kubernetes_service_label_" + SERVICE_NAME_LABEL.replace(".", "_").replace("/", "_")],
             "regex": re.escape(options.service_name), "action": "keep"},
            {"source_labels": ["__meta_kubernetes_endpoint_port_name"], "regex": METRICS_PORT_NAME, "action": "keep"},
            {"source_labels": ["__meta_kubernetes_namespace"], "target_label": "namespace"},
            {"source_labels": ["__meta_kubernetes_pod_name"], "target_label": "pod"},
        ]
    if options.discovery != "docker":
        job["relabel_configs"] = job.get("relabel_configs", []) + extra_relabelings(labels)
    if not job.get("relabel_configs"):
        job.pop("relabel_configs", None)
    return job


def kubernetes_manifests(options: "ExportOptions", labels: dict[str, str]) -> dict[str, dict]:
    selector = {SERVICE_NAME_LABEL: options.service_name}
    metadata = {"name": options.service_name, "namespace": options.namespace, "labels": dict(selector)}
    service = {"apiVersion": "v1", "kind": "Service", "metadata": copy.deepcopy(metadata),
               "spec": {"selector": {SERVICE_NAME_LABEL: options.service_name}, "ports": [
                   {"name": METRICS_PORT_NAME, "port": options.metrics_port, "targetPort": options.metrics_port, "protocol": "TCP"}]}}
    relabelings = [{"targetLabel": name, "replacement": value} for name, value in labels.items()]
    endpoint = {"port": METRICS_PORT_NAME, "path": "/metrics", "interval": options.scrape_interval, "honorLabels": True}
    if relabelings:
        endpoint["relabelings"] = relabelings
    service_monitor = {"apiVersion": "monitoring.coreos.com/v1", "kind": "ServiceMonitor", "metadata": {**copy.deepcopy(metadata), "name": options.job_name},
                       "spec": {"jobLabel": SERVICE_NAME_LABEL, "selector": {"matchLabels": dict(selector)},
                                "namespaceSelector": {"matchNames": [options.namespace]}, "endpoints": [endpoint]}}
    pod_endpoint = {"port": METRICS_PORT_NAME, "path": "/metrics", "interval": options.scrape_interval, "honorLabels": True}
    if relabelings:
        pod_endpoint["relabelings"] = relabelings
    pod_monitor = {"apiVersion": "monitoring.coreos.com/v1", "kind": "PodMonitor", "metadata": {**copy.deepcopy(metadata), "name": options.job_name},
                   "spec": {"selector": {"matchLabels": dict(selector)}, "namespaceSelector": {"matchNames": [options.namespace]},
                            "podMetricsEndpoints": [pod_endpoint]}}
    return {"service.yaml": service, "servicemonitor.yaml": service_monitor, "podmonitor.yaml": pod_monitor}


# -- dashboards -----------------------------------------------------------------------------------------------------------------
RATE_WINDOW = re.compile(r"\[(?:1m|5m)\]")


def _walk_targets(dashboard: dict) -> None:
    def walk(panels):
        for panel in panels:
            for target in panel.get("targets", []):
                target["expr"] = RATE_WINDOW.sub("[$__rate_interval]", target["expr"])
            walk(panel.get("panels") or [])
    walk(dashboard["panels"])


def _datasource_variable(dashboard: dict) -> dict:
    return next(v for v in dashboard["templating"]["list"] if v.get("type") == "datasource")


def overview_dashboard(datasource_uid: str | None = None) -> dict:
    """The PROVISIONING variant (``grafana/dashboards/``): no ``__inputs``; the datasource variable's ``current`` is the uid given by
    ``--datasource-uid`` (name ``Prometheus``, the name the exported provisioning file gives it). Without a uid ``current`` is left
    unset, so Grafana picks the default Prometheus datasource; with several Prometheus datasources give ``--datasource-uid``."""
    dashboard = json.loads(asset_text("grafana", "dashboards", "graph-loader-overview.json"))
    variable = _datasource_variable(dashboard)
    if datasource_uid is None:
        variable["current"] = {}
    else:
        variable["current"] = {"selected": False, "text": "Prometheus", "value": datasource_uid}
    _walk_targets(dashboard)
    return dashboard


def overview_import_dashboard() -> dict:
    """The IMPORT variant (``grafana/import/``): a Prometheus datasource INPUT (the import dialog or API fills ``${DS_PROMETHEUS}``),
    the datasource variable defaulting to it, and ``$__rate_interval`` in place of the fixed rate windows."""
    dashboard = json.loads(asset_text("grafana", "dashboards", "graph-loader-overview.json"))
    dashboard = {"__inputs": [{"name": DS_PROMETHEUS, "label": "Prometheus", "description": "The Prometheus that scrapes graph-loader",
                               "type": "datasource", "pluginId": "prometheus", "pluginName": "Prometheus"}],
                 "__requires": [{"type": "datasource", "id": "prometheus", "name": "Prometheus", "version": "1.0.0"},
                                {"type": "panel", "id": "timeseries", "name": "Time series", "version": ""},
                                {"type": "panel", "id": "table", "name": "Table", "version": ""}], **dashboard}
    _datasource_variable(dashboard)["current"] = {"selected": False, "text": "${%s}" % DS_PROMETHEUS, "value": "${%s}" % DS_PROMETHEUS}
    _walk_targets(dashboard)
    return dashboard


def run_history_dashboard() -> dict:
    return json.loads(asset_text("grafana", "dashboards-sql", "graph-loader-run-history.json"))


# -- the bundle ------------------------------------------------------------------------------------------------------------------
@dataclass
class ExportOptions:
    out: Path
    discovery: str
    scrape_interval: str = "15s"
    job_name: str = "graph-loader"
    extra_labels: dict[str, str] | None = None
    metrics_port: int | None = None
    targets: tuple[str, ...] = ()
    targets_file: str | None = None
    docker_network: str = DEFAULT_DOCKER_NETWORK
    namespace: str = "default"
    service_name: str = SERVICE_NAME
    prometheus_url: str = DEFAULT_PROMETHEUS_URL
    datasource_uid: str | None = None  # None: the provisioning file uses "prometheus" and the dashboard leaves its variable unset
    values: str | None = None
    force: bool = False


def validate_options(options: ExportOptions, values: dict) -> dict[str, str]:
    if options.discovery not in DISCOVERIES:
        raise ExportError(f"--discovery must be one of {', '.join(DISCOVERIES)}")
    seconds = parse_duration_seconds(options.scrape_interval, "--scrape-interval")
    if not 1 <= seconds <= 3600:
        raise ExportError("--scrape-interval must be between 1s and 1h")
    if not JOB_NAME.fullmatch(options.job_name):
        raise ExportError("--job-name may contain letters, digits, '_', '.', '-' (at most 64)")
    if options.metrics_port is not None and not 1 <= options.metrics_port <= 65535:
        raise ExportError("--metrics-port must be between 1 and 65535")
    if not re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", options.namespace) or not re.fullmatch(r"[a-z0-9]([-a-z0-9.]*[a-z0-9])?", options.service_name):
        raise ExportError("--namespace and --service-name must be Kubernetes names (lowercase letters, digits, '-')")
    if options.datasource_uid is not None and not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", options.datasource_uid):
        raise ExportError("--datasource-uid may contain letters, digits, '_' and '-' (at most 40)")
    if not re.fullmatch(r"https?://[^\s]+", options.prometheus_url):
        raise ExportError("--prometheus-url must be an http(s) URL")
    if options.discovery in ("static", "kubernetes") and options.metrics_port is None:
        raise ExportError(f"--discovery {options.discovery} scrapes the aggregated endpoint: give --metrics-port "
                          "(the port `graph-loader start --runtime process --metrics-port N` serves)")
    if options.discovery == "static":
        options.targets = tuple(parse_host_port(item, "--targets") for item in options.targets) or (f"localhost:{options.metrics_port}",)
    if options.discovery == "file":
        if options.targets_file is not None and not re.fullmatch(r"[^\s'\"]+", options.targets_file):
            raise ExportError("--targets-file must be a path without whitespace or quotes")
    labels = dict(options.extra_labels or {})
    labels.update({str(k): v for k, v in (values.get("labels") or {}).items()})
    return validate_labels(labels)


def build_bundle(options: ExportOptions) -> dict[str, bytes]:
    """The whole bundle as ``{relative path: bytes}`` (nothing is written)."""
    values = load_values(options.values)
    if "job_name" in values and options.job_name == "graph-loader":
        options.job_name = str(values["job_name"])
    labels = validate_options(options, values)
    files: dict[str, bytes] = {}

    def put(path: str, content: str | bytes) -> None:
        files[path] = content.encode("utf-8") if isinstance(content, str) else content

    job = scrape_job(options, labels)
    header = (f"# Generated by `graph-loader monitoring export --discovery {options.discovery}`.\n"
              "# honor_labels: true keeps the exporters' own identity labels (run_id, execution_id, exporter, runtime) authoritative.\n"
              "# Scrape the children (docker, file) OR the aggregated endpoint (static, kubernetes), never both for the same run.\n")
    put("prometheus/scrape-config.yml", header + yaml.safe_dump({"scrape_configs": [job]}, sort_keys=False, width=1000))
    rendered = render_alert_files(values, options.job_name, labels)
    for filename, text in rendered.items():
        put(f"prometheus/alerts/{filename}", text)
    interval = max(parse_duration_seconds(options.scrape_interval, "scrape interval"), 5)
    put("prometheus/prometheus.yml", header + yaml.safe_dump({
        "global": {"scrape_interval": options.scrape_interval, "evaluation_interval": f"{int(interval)}s"},
        "rule_files": ["alerts/*.yml"], "scrape_configs": [job]}, sort_keys=False, width=1000))
    if not values.get("alerts") and not values.get("disable") and not labels and options.job_name == "graph-loader":
        for name, data in asset_files().items():
            if name.startswith("prometheus/alert_tests/"):
                put(name, data)
    generated = generated_tests(rendered, values)
    if generated:
        put("prometheus/alert_tests/generated_test.yml", generated)
    put("grafana/dashboards/graph-loader-overview.json", json.dumps(overview_dashboard(options.datasource_uid), indent=2) + "\n")
    put("grafana/import/graph-loader-overview.json", json.dumps(overview_import_dashboard(), indent=2) + "\n")
    put("grafana/dashboards-sql/graph-loader-run-history.json", json.dumps(run_history_dashboard(), indent=2) + "\n")
    put("grafana/provisioning/datasources/prometheus.yml", yaml.safe_dump({"apiVersion": 1, "datasources": [{
        "name": "Prometheus", "uid": options.datasource_uid or "prometheus", "type": "prometheus", "access": "proxy", "url": options.prometheus_url,
        "isDefault": True, "editable": False, "jsonData": {"timeInterval": options.scrape_interval}}]}, sort_keys=False))
    put("grafana/provisioning/dashboards/graph-loader.yml", asset_text("grafana", "provisioning", "dashboards", "graph-loader.yml"))
    if options.discovery == "kubernetes":
        for name, document in kubernetes_manifests(options, labels).items():
            put(f"kubernetes/{name}", yaml.safe_dump(document, sort_keys=False))
    put("BUNDLE.md", bundle_readme(options))
    from neo4j_graph_loader._version import __version__
    manifest = {"graph_loader_version": __version__, "discovery": options.discovery, "job_name": options.job_name,
                "scrape_interval": options.scrape_interval, "metrics_port": options.metrics_port, "extra_labels": labels,
                "values": values, "files": {name: hashlib.sha256(data).hexdigest() for name, data in sorted(files.items())}}
    put("manifest.json", json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return files


def bundle_readme(options: ExportOptions) -> str:
    where = {"docker": "Prometheus must reach the Docker socket and the `%s` network (Docker service discovery)." % options.docker_network,
             "file": "Point `file_sd` at the run's `metrics/targets.json` (process runtime) at %s." % (options.targets_file or DEFAULT_TARGETS_FILE),
             "static": "Prometheus scrapes %s, the aggregated endpoint of `graph-loader start --runtime process --metrics-port %s`." % (
                 ", ".join(options.targets) or "?", options.metrics_port),
             "kubernetes": "Apply `kubernetes/service.yaml` and either `servicemonitor.yaml` or `podmonitor.yaml` (Prometheus Operator), or use the "
                           "`kubernetes_sd_configs` job; both reach the aggregated port %s." % options.metrics_port}[options.discovery]
    return (f"# graph-loader monitoring bundle ({options.discovery} discovery)\n\n{where}\n\n"
            "1. Merge `prometheus/scrape-config.yml` into your Prometheus `scrape_configs`, and add `prometheus/alerts/*.yml` to `rule_files`.\n"
            "   `prometheus/prometheus.yml` is a complete minimal configuration around the same pieces.\n"
            "2. Grafana, either way: PROVISION `grafana/provisioning/` with `grafana/dashboards/` (no inputs; its datasource variable is set from\n"
            "   `--datasource-uid`, so pass it when Grafana has several Prometheus datasources), OR IMPORT `grafana/import/graph-loader-overview.json`\n"
            "   (dialog or `POST /api/dashboards/import`; it asks for the Prometheus datasource). Do not import the provisioning file into a\n"
            "   Grafana that has no datasource with that uid. `grafana/dashboards-sql/graph-loader-run-history.json` needs a PostgreSQL datasource\n"
            "   with the run-status database (`--status-store sql`).\n"
            "3. Check: `promtool check config prometheus/prometheus.yml` and `promtool test rules prometheus/alert_tests/*_test.yml`.\n\n"
            "Every sample carries run_id, execution_id, exporter and runtime; alerts and dashboards use these metric labels only.\n"
            "`run_id` is reusable and can be empty; `execution_id` is unique per `graph-loader start`.\n")


def write_bundle(options: ExportOptions) -> list[str]:
    """Build and write the bundle under ``options.out``; refuses to overwrite existing files without ``force``. Returns the paths."""
    files = build_bundle(options)
    out = Path(options.out)
    clashes = sorted(name for name in files if (out / name).exists())
    if clashes and not options.force:
        raise ExportError(f"{len(clashes)} file(s) already exist under {out} (for example {clashes[0]}); use --force to overwrite")
    for name, data in files.items():
        target = out / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    return sorted(files)
