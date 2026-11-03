"""E15.F2.S10: the packaged monitoring assets and ``graph-loader monitoring export``.

Pure Python: nothing here starts Docker. The promtool cells run only with ``E15_PROMTOOL=1`` (an owned, network-less
``docker run --rm --network none --entrypoint promtool`` of ``E15_PROMTOOL_IMAGE``, read-only mount) and are recorded in the evidence.
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from neo4j_graph_loader import cli
from neo4j_graph_loader.monitoring import export
from neo4j_graph_loader.monitoring.export import ExportError, ExportOptions

REPO = Path(__file__).resolve().parent.parent
CHECKOUT = REPO / "monitoring"
PACKAGED = REPO / "src" / "neo4j_graph_loader" / "monitoring"
DISCOVERY_ARGS = {"docker": {}, "file": {}, "static": {"metrics_port": 9600}, "kubernetes": {"metrics_port": 9600, "namespace": "loaders"}}


def tree(root: Path, skip=(".py", ".pyc")) -> dict[str, bytes]:
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in sorted(root.rglob("*"))
            if p.is_file() and p.suffix not in skip and "__pycache__" not in p.parts}


def options(tmp_path, discovery="docker", **kw) -> ExportOptions:
    return ExportOptions(out=tmp_path / "bundle", discovery=discovery, **{**DISCOVERY_ARGS[discovery], **kw})


def alert_rules(files: dict[str, bytes]) -> dict[str, dict]:
    rules = {}
    for name, data in files.items():
        if name.startswith("prometheus/alerts/"):
            for group in yaml.safe_load(data)["groups"]:
                for rule in group["rules"]:
                    rules[rule["alert"]] = rule
    return rules


# -- package data ----------------------------------------------------------------------------------------------------------------
def test_packaged_assets_equal_the_checkout_monitoring_tree() -> None:
    assert tree(PACKAGED) == tree(CHECKOUT), "run: rsync -a --delete monitoring/ src/neo4j_graph_loader/monitoring/ (keeping the .py files)"


def test_the_asset_listing_matches_the_files_and_pyproject_names_every_pattern() -> None:
    listed = export.asset_files()
    assert set(listed) == set(tree(PACKAGED))
    import tomllib
    patterns = tomllib.loads((REPO / "pyproject.toml").read_text())["tool"]["setuptools"]["package-data"]["neo4j_graph_loader"]
    manifest = (REPO / "MANIFEST.in").read_text()
    for name in listed:
        assert any(Path(name).match(pattern.removeprefix("monitoring/")) or Path("monitoring/" + name).match(pattern) for pattern in patterns), name
        assert f"src/neo4j_graph_loader/monitoring/{name}" in manifest or any(
            line.startswith("recursive-include src/neo4j_graph_loader/monitoring") for line in manifest.splitlines()), name


# -- every discovery ------------------------------------------------------------------------------------------------------------
@pytest.mark.parametrize("discovery", sorted(DISCOVERY_ARGS))
def test_each_discovery_writes_a_consistent_bundle(tmp_path, discovery) -> None:
    files = export.build_bundle(options(tmp_path, discovery, extra_labels={"team": "data"}, scrape_interval="30s", job_name="gl"))
    config = yaml.safe_load(files["prometheus/prometheus.yml"])
    scrape = yaml.safe_load(files["prometheus/scrape-config.yml"])["scrape_configs"]
    assert config["scrape_configs"] == scrape and len(scrape) == 1
    job = scrape[0]
    assert job["job_name"] == "gl" and job["scrape_interval"] == "30s" and job["honor_labels"] is True
    assert config["global"]["scrape_interval"] == "30s" and config["rule_files"] == ["alerts/*.yml"]
    assert {"target_label": "team", "replacement": "data"} in job["relabel_configs"]
    key = {"docker": "docker_sd_configs", "file": "file_sd_configs", "static": "static_configs", "kubernetes": "kubernetes_sd_configs"}[discovery]
    assert key in job and sum(k.endswith("_sd_configs") or k == "static_configs" for k in job) == 1
    assert (discovery == "kubernetes") == any(name.startswith("kubernetes/") for name in files)
    manifest = json.loads(files["manifest.json"])
    assert manifest["discovery"] == discovery and manifest["extra_labels"] == {"team": "data"}
    for name, digest in manifest["files"].items():
        import hashlib
        assert hashlib.sha256(files[name]).hexdigest() == digest, name
    assert "manifest.json" not in manifest["files"]


def test_docker_discovery_keeps_the_shipped_relabelling_and_the_network_is_configurable(tmp_path) -> None:
    shipped = yaml.safe_load((CHECKOUT / "prometheus" / "prometheus.yml").read_text())["scrape_configs"][0]
    job = yaml.safe_load(export.build_bundle(options(tmp_path, docker_network="my-net"))["prometheus/scrape-config.yml"])["scrape_configs"][0]
    assert job["docker_sd_configs"] == shipped["docker_sd_configs"]
    networks = [r for r in job["relabel_configs"] if r.get("source_labels") == ["__meta_docker_network_name"]]
    assert networks[0]["regex"] == "my\\-net"
    assert len(job["relabel_configs"]) == len(shipped["relabel_configs"])


def test_file_discovery_points_at_the_targets_file(tmp_path) -> None:
    job = yaml.safe_load(export.build_bundle(options(tmp_path, "file", targets_file="/data/run/metrics/targets.json"))["prometheus/scrape-config.yml"])["scrape_configs"][0]
    assert job["file_sd_configs"][0]["files"] == ["/data/run/metrics/targets.json"]
    default = yaml.safe_load(export.build_bundle(options(tmp_path, "file"))["prometheus/scrape-config.yml"])["scrape_configs"][0]
    assert default["file_sd_configs"][0]["files"] == [export.DEFAULT_TARGETS_FILE]


def test_static_discovery_scrapes_the_aggregated_port(tmp_path) -> None:
    job = yaml.safe_load(export.build_bundle(options(tmp_path, "static"))["prometheus/scrape-config.yml"])["scrape_configs"][0]
    assert job["static_configs"] == [{"targets": ["localhost:9600"]}]
    job = yaml.safe_load(export.build_bundle(options(tmp_path, "static", targets=("graph-loader.svc:9600", "other:9601")))["prometheus/scrape-config.yml"])["scrape_configs"][0]
    assert job["static_configs"][0]["targets"] == ["graph-loader.svc:9600", "other:9601"]


def test_kubernetes_manifests_point_at_the_aggregated_port(tmp_path) -> None:
    files = export.build_bundle(options(tmp_path, "kubernetes", extra_labels={"team": "data"}, scrape_interval="20s", service_name="gl-agg"))
    service, service_monitor, pod_monitor = (yaml.safe_load(files[f"kubernetes/{n}.yaml"]) for n in ("service", "servicemonitor", "podmonitor"))
    assert service["apiVersion"] == "v1" and service["kind"] == "Service" and service["metadata"]["namespace"] == "loaders"
    port = service["spec"]["ports"][0]
    assert port["name"] == "metrics" and port["port"] == 9600 and port["targetPort"] == 9600
    labels = service["metadata"]["labels"]
    assert labels == {"app.kubernetes.io/name": "gl-agg"} and service["spec"]["selector"] == labels
    for monitor, kind, field in ((service_monitor, "ServiceMonitor", "endpoints"), (pod_monitor, "PodMonitor", "podMetricsEndpoints")):
        assert monitor["apiVersion"] == "monitoring.coreos.com/v1" and monitor["kind"] == kind
        assert monitor["spec"]["selector"]["matchLabels"] == labels and monitor["spec"]["namespaceSelector"]["matchNames"] == ["loaders"]
        endpoint = monitor["spec"][field][0]
        assert endpoint["port"] == "metrics" and endpoint["path"] == "/metrics" and endpoint["interval"] == "20s" and endpoint["honorLabels"] is True
        assert endpoint["relabelings"] == [{"targetLabel": "team", "replacement": "data"}]
    job = yaml.safe_load(files["prometheus/scrape-config.yml"])["scrape_configs"][0]
    keep = [r for r in job["relabel_configs"] if r.get("action") == "keep"]
    assert {r["regex"] for r in keep} == {"gl\\-agg", "metrics"}
    assert job["kubernetes_sd_configs"][0]["namespaces"]["names"] == ["loaders"]


@pytest.mark.parametrize("discovery", ["static", "kubernetes"])
def test_aggregated_discoveries_require_the_metrics_port(tmp_path, discovery) -> None:
    with pytest.raises(ExportError, match="--metrics-port"):
        export.build_bundle(ExportOptions(out=tmp_path / "b", discovery=discovery))


@pytest.mark.parametrize("bad", [
    {"scrape_interval": "soon"}, {"scrape_interval": "0s"}, {"job_name": "bad name"}, {"metrics_port": 70000},
    {"datasource_uid": "has space"}, {"prometheus_url": "ftp://x"}, {"namespace": "Bad_NS"}, {"discovery": "consul"},
])
def test_invalid_options_are_refused_before_anything_is_written(tmp_path, bad) -> None:
    opts = options(tmp_path, "static")
    for key, value in bad.items():
        setattr(opts, key, value)
    with pytest.raises(ExportError):
        export.write_bundle(opts)
    assert not (tmp_path / "bundle").exists()


# -- extra labels -----------------------------------------------------------------------------------------------------------------
@pytest.mark.parametrize("name", ["run_id", "execution_id", "exporter", "runtime", "job", "instance", "kind", "name", "le", "__x", "1bad", "has-dash"])
def test_extra_labels_reject_reserved_and_invalid_names(name) -> None:
    with pytest.raises(ExportError):
        export.parse_extra_labels(f"ok=1,{name}=v")


def test_extra_labels_parse_and_validate_values() -> None:
    assert export.parse_extra_labels("team=data, env=prod") == {"team": "data", "env": "prod"}
    assert export.parse_extra_labels("") == {}
    for bad in ("team", "team=", "team=a\nb"):
        with pytest.raises(ExportError):
            export.parse_extra_labels(bad)


def test_reserved_labels_in_the_values_file_are_refused(tmp_path) -> None:
    values = tmp_path / "v.yaml"
    values.write_text("labels: {run_id: x}\n")
    with pytest.raises(ExportError, match="reserved"):
        export.build_bundle(options(tmp_path, values=str(values)))


# -- alert rules from values ----------------------------------------------------------------------------------------------------------
def test_defaults_reproduce_the_shipped_alert_rules_exactly(tmp_path) -> None:
    files = export.build_bundle(options(tmp_path))
    for path in sorted((CHECKOUT / "prometheus" / "alerts").glob("*.yml")):
        assert files[f"prometheus/alerts/{path.name}"].decode() == path.read_text(), path.name
    shipped_tests = {p.name for p in (CHECKOUT / "prometheus" / "alert_tests").glob("*_test.yml")}
    assert {n.rsplit("/", 1)[1] for n in files if n.startswith("prometheus/alert_tests/")} == shipped_tests | {"generated_test.yml"}


def write_values(tmp_path, text: str) -> str:
    path = tmp_path / "values.yaml"
    path.write_text(text)
    return str(path)


def test_values_change_thresholds_for_severity_and_labels(tmp_path) -> None:
    values = write_values(tmp_path, """
labels: {team: data}
alerts:
  GraphLoaderWriteOutage: {threshold: 300, for: 3m, severity: warning}
  GraphLoaderRetryRateHigh: {threshold: 2.5}
  GraphLoaderPlanResourceWaitHigh: {threshold: 8}
  GraphLoaderConsumerLagCritical: {threshold: 2000000, for: 10m}
  GraphLoaderParkedLotGrowing: {severity: warning}
""")
    files = export.build_bundle(options(tmp_path, values=values))
    rules, shipped = alert_rules(files), alert_rules(export.asset_files())
    outage = rules["GraphLoaderWriteOutage"]
    assert "> 300" in outage["expr"] and outage["for"] == "3m" and outage["labels"] == {"severity": "warning", "team": "data"}
    assert "> 2.5" in rules["GraphLoaderRetryRateHigh"]["expr"] and "more than 2.5/s" in rules["GraphLoaderRetryRateHigh"]["annotations"]["summary"]
    assert "> 8" in rules["GraphLoaderPlanResourceWaitHigh"]["expr"] and "exceeded 8s" in rules["GraphLoaderPlanResourceWaitHigh"]["annotations"]["summary"]
    assert "> 2000000" in rules["GraphLoaderConsumerLagCritical"]["expr"] and rules["GraphLoaderConsumerLagCritical"]["for"] == "10m"
    assert rules["GraphLoaderParkedLotGrowing"]["labels"]["severity"] == "warning"
    for name, rule in rules.items():                             # nothing else moved except the extra label on every alert
        assert rule["labels"]["team"] == "data", name
        if name not in ("GraphLoaderWriteOutage", "GraphLoaderRetryRateHigh", "GraphLoaderPlanResourceWaitHigh", "GraphLoaderConsumerLagCritical",
                        "GraphLoaderParkedLotGrowing"):
            assert {k: v for k, v in rule.items() if k != "labels"} == {k: v for k, v in shipped[name].items() if k != "labels"}, name
    assert "# exported with threshold 300 (default 120), for 3m, severity warning" in files["prometheus/alerts/write_outage.yml"].decode()


def test_disabling_alerts_drops_them_and_their_empty_groups(tmp_path) -> None:
    values = write_values(tmp_path, "disable: [GraphLoaderWriteOutage, GraphLoaderParkedLotGrowing]\n")
    files = export.build_bundle(options(tmp_path, values=values))
    assert "prometheus/alerts/write_outage.yml" not in files
    rules = alert_rules(files)
    assert "GraphLoaderParkedLotGrowing" not in rules and "GraphLoaderDlqBacklogHigh" in rules
    generated = yaml.safe_load(files["prometheus/alert_tests/generated_test.yml"])
    assert "../alerts/write_outage.yml" not in generated["rule_files"]


def test_the_job_name_reaches_the_health_rules(tmp_path) -> None:
    files = export.build_bundle(options(tmp_path, job_name="my-job"))
    assert alert_rules(files)["GraphLoaderTargetDown"]["expr"] == 'up{job="my-job"} == 0'
    values = write_values(tmp_path, "job_name: from-values\n")
    files = export.build_bundle(options(tmp_path, values=values))
    assert alert_rules(files)["GraphLoaderTargetDown"]["expr"] == 'up{job="from-values"} == 0'
    assert yaml.safe_load(files["prometheus/scrape-config.yml"])["scrape_configs"][0]["job_name"] == "from-values"


@pytest.mark.parametrize("text, match", [
    ("surprise: 1\n", "unknown key"),
    ("alerts: {NoSuchAlert: {for: 1m}}\n", "unknown alert"),
    ("disable: [NoSuchAlert]\n", "unknown alert"),
    ("alerts: {GraphLoaderWriteOutage: {threshold: 0}}\n", "positive"),
    ("alerts: {GraphLoaderWriteOutage: {threshold: abc}}\n", "positive"),
    ("alerts: {GraphLoaderWriteOutage: {threshold: true}}\n", "positive"),
    ("alerts: {GraphLoaderParkedLotGrowing: {threshold: 3}}\n", "no overridable threshold"),
    ("alerts: {GraphLoaderWriteOutage: {for: soon}}\n", "duration"),
    ("alerts: {GraphLoaderWriteOutage: {severity: dire}}\n", "severity"),
    ("alerts: {GraphLoaderWriteOutage: {colour: red}}\n", "keys must be"),
    ("labels: [a]\n", "mapping"),
    ("- not a mapping\n", "mapping"),
    ("job_name: 'bad name'\n", "job_name"),
])
def test_invalid_values_are_errors_never_ignored(tmp_path, text, match) -> None:
    with pytest.raises(ExportError, match=match):
        export.build_bundle(options(tmp_path, values=write_values(tmp_path, text)))


def test_every_overridable_threshold_matches_the_shipped_rule() -> None:
    """The defaults in ALERT_PARAMS are the literals of the shipped rules (a drifted default would make an override miss)."""
    for name, param in export.ALERT_PARAMS.items():
        text = (CHECKOUT / "prometheus" / "alerts" / param.file).read_text()
        block = text[text.index(f"- alert: {name}"):]
        block = block[:block.index("- alert:", 10)] if "- alert:" in block[10:] else block
        assert re.search(r">\s*" + re.escape(export.number(param.default)) + r"(?![\d.])", block), name


def test_generated_tests_use_the_final_thresholds(tmp_path) -> None:
    values = write_values(tmp_path, "alerts: {GraphLoaderWriteOutage: {threshold: 300, for: 4m}, GraphLoaderDlqBacklogHigh: {threshold: 40}}\n")
    document = yaml.safe_load(export.build_bundle(options(tmp_path, values=values))["prometheus/alert_tests/generated_test.yml"])
    by_name = {t["name"]: t for t in document["tests"]}
    above = by_name["GraphLoaderWriteOutage: above 300 fires after 4m"]
    assert above["input_series"][0]["values"].startswith("330x") and [c["exp_samples"][0]["value"] for c in above["promql_expr_test"]] == [0, 1]
    assert by_name["GraphLoaderWriteOutage: exactly 300 does not fire"]["input_series"][0]["values"].startswith("300x")
    assert by_name["GraphLoaderWriteOutage: below 300 does not fire"]["input_series"][0]["values"].startswith("270x")
    assert "GraphLoaderDlqBacklogHigh: above 40 fires after 15m" in by_name
    assert set(document["rule_files"]) == {"../alerts/write_outage.yml", "../alerts/dlq.yml", "../alerts/lag_and_errors.yml", "../alerts/concurrency.yml"}
    names = {n.split(":")[0] for n in by_name}
    assert names == set(export.ALERT_PARAMS)


def test_the_lag_slope_threshold_is_overridable_and_its_default_is_todays_rule(tmp_path) -> None:
    """GraphLoaderConsumerLagNotDraining: `deriv(lag[10m]) > 0` by default (unchanged text); `threshold` raises the slope in records/s."""
    shipped = (CHECKOUT / "prometheus" / "alerts" / "lag_and_errors.yml").read_text()
    default = export.build_bundle(options(tmp_path))["prometheus/alerts/lag_and_errors.yml"].decode()
    assert default == shipped and "deriv(graph_loader_consumer_lag_records[10m]) > 0\n" in default
    values = write_values(tmp_path, "alerts: {GraphLoaderConsumerLagNotDraining: {threshold: 0.05}}\n")
    files = export.build_bundle(options(tmp_path, values=values))
    rule = alert_rules(files)["GraphLoaderConsumerLagNotDraining"]
    assert rule["expr"] == "deriv(graph_loader_consumer_lag_records[10m]) > 0.05" and rule["for"] == "15m"
    assert "# exported with threshold 0.05 (default 0)" in files["prometheus/alerts/lag_and_errors.yml"].decode()


def test_generated_tests_cover_the_lag_slope_default_and_a_non_zero_threshold(tmp_path) -> None:
    def tests(values=None):
        document = yaml.safe_load(export.build_bundle(options(tmp_path, values=values))["prometheus/alert_tests/generated_test.yml"])
        return {t["name"]: t for t in document["tests"] if t["name"].startswith("GraphLoaderConsumerLagNotDraining")}
    default = tests()
    assert set(default) == {"GraphLoaderConsumerLagNotDraining: slope above 0/s fires after 15m", "GraphLoaderConsumerLagNotDraining: a flat lag does not fire"}
    values = write_values(tmp_path, "alerts: {GraphLoaderConsumerLagNotDraining: {threshold: 0.05}}\n")
    custom = tests(values)
    assert set(custom) == {"GraphLoaderConsumerLagNotDraining: slope above 0.05/s fires after 15m", "GraphLoaderConsumerLagNotDraining: a flat lag does not fire",
                           "GraphLoaderConsumerLagNotDraining: slope below 0.05/s does not fire"}
    rising = custom["GraphLoaderConsumerLagNotDraining: slope above 0.05/s fires after 15m"]["input_series"][0]["values"]
    below = custom["GraphLoaderConsumerLagNotDraining: slope below 0.05/s does not fire"]["input_series"][0]["values"]
    assert rising.startswith("0+4.5x") and below.startswith("0+1.5x")        # 0.075/s and 0.025/s, per one-minute step
    # control: a generator that ignored the override would still test the default slope against a rule that now says 0.05
    assert default["GraphLoaderConsumerLagNotDraining: slope above 0/s fires after 15m"]["input_series"][0]["values"].startswith("0+6x")


def test_control_a_drifted_lag_slope_default_is_refused_not_ignored(tmp_path, monkeypatch) -> None:
    """Control: if the recorded default ever drifted from the shipped literal, the override must fail loudly, never silently do nothing."""
    drifted = dataclasses.replace(export.ALERT_PARAMS["GraphLoaderConsumerLagNotDraining"], default=3)
    monkeypatch.setitem(export.ALERT_PARAMS, "GraphLoaderConsumerLagNotDraining", drifted)
    values = write_values(tmp_path, "alerts: {GraphLoaderConsumerLagNotDraining: {threshold: 0.05}}\n")
    with pytest.raises(ExportError, match="was not found in the shipped rule"):
        export.build_bundle(options(tmp_path, values=values))


# -- the documentation shows what the command does -----------------------------------------------------------------------------------------
DOCS = Path(__file__).resolve().parents[1] / "docs"


def test_the_values_example_in_the_monitoring_guide_is_accepted_as_written(tmp_path) -> None:
    text = (DOCS / "MONITORING.md").read_text(encoding="utf-8")
    block = re.search(r"```text\njob_name: graph-loader.*?\n```", text, re.S)
    assert block, "the values example moved or changed its fence"
    values = write_values(tmp_path, block.group(0)[len("```text\n"):-4])
    files = export.build_bundle(options(tmp_path, values=values))
    rules = alert_rules(files)
    assert rules["GraphLoaderWriteOutage"]["expr"].endswith("> 300") and rules["GraphLoaderConsumerLagNotDraining"]["expr"].endswith("> 0.05")
    assert "GraphLoaderDlqRoutingSpike" not in rules


def test_the_threshold_table_in_the_monitoring_guide_lists_every_overridable_alert_with_its_default() -> None:
    text = (DOCS / "MONITORING.md").read_text(encoding="utf-8")
    rows = dict(re.findall(r"^\| `(GraphLoader\w+)` \| ([\d.]+) \|", text[text.index("Overridable thresholds"):], re.M))
    assert set(rows) == set(export.ALERT_PARAMS)
    assert {name: float(value) for name, value in rows.items()} == {name: float(param.default) for name, param in export.ALERT_PARAMS.items()}


def test_every_flag_of_the_export_command_is_documented_in_the_cli_reference() -> None:
    reference = (DOCS / "CLI.md").read_text(encoding="utf-8")
    section = reference[reference.index("## monitoring export"):reference.index("## replay-dlq")]
    parser = cli.build_parser()
    export_parser = next(action for action in parser._actions if isinstance(action, argparse._SubParsersAction)).choices["monitoring"]
    export_parser = next(action for action in export_parser._actions if isinstance(action, argparse._SubParsersAction)).choices["export"]
    flags = {option for action in export_parser._actions for option in action.option_strings if option.startswith("--") and option != "--help"}
    assert flags and all(flag in section for flag in flags), sorted(flag for flag in flags if flag not in section)


# -- dashboards -------------------------------------------------------------------------------------------------------------------------
def _expressions(dashboard: dict) -> list[str]:
    return [t["expr"] for p in dashboard["panels"] for t in p.get("targets", [])]


def _datasource_variable(dashboard: dict) -> dict:
    return next(v for v in dashboard["templating"]["list"] if v["type"] == "datasource")


def test_the_exported_overview_is_importable_with_a_datasource_input_and_rate_interval(tmp_path) -> None:
    dashboard = json.loads(export.build_bundle(options(tmp_path))["grafana/import/graph-loader-overview.json"])
    assert dashboard["__inputs"][0]["name"] == "DS_PROMETHEUS" and dashboard["__inputs"][0]["pluginId"] == "prometheus"
    assert _datasource_variable(dashboard)["current"]["value"] == "${DS_PROMETHEUS}"
    expressions = _expressions(dashboard)
    assert any("$__rate_interval" in e for e in expressions) and not any(re.search(r"\[(1m|5m)\]", e) for e in expressions)
    shipped = json.loads((CHECKOUT / "grafana" / "dashboards" / "graph-loader-overview.json").read_text())
    assert len(dashboard["panels"]) == len(shipped["panels"]) and dashboard["uid"] == shipped["uid"]


def test_the_provisioning_variant_has_no_input_and_no_unresolved_placeholder(tmp_path) -> None:
    for uid in (None, "my-prom"):
        files = export.build_bundle(options(tmp_path, datasource_uid=uid))
        text = files["grafana/dashboards/graph-loader-overview.json"].decode()
        dashboard = json.loads(text)
        assert "__inputs" not in dashboard and "DS_PROMETHEUS" not in text and "${DS_" not in text
        assert not any(re.search(r"\[(1m|5m)\]", e) for e in _expressions(dashboard))


def test_the_provisioning_variant_selects_the_datasource_uid_it_was_given(tmp_path) -> None:
    files = export.build_bundle(options(tmp_path, datasource_uid="my-prom"))
    current = _datasource_variable(json.loads(files["grafana/dashboards/graph-loader-overview.json"]))["current"]
    assert current["value"] == "my-prom" and current["text"] == "Prometheus"
    provisioned = yaml.safe_load(files["grafana/provisioning/datasources/prometheus.yml"])["datasources"][0]
    assert provisioned["uid"] == current["value"] and provisioned["name"] == current["text"]
    # the import variant does not follow --datasource-uid: it takes whatever datasource the importer picks
    imported = json.loads(files["grafana/import/graph-loader-overview.json"])
    assert "my-prom" not in json.dumps(imported)


def test_without_a_datasource_uid_the_variable_is_left_unset_and_the_datasource_keeps_the_default_uid(tmp_path) -> None:
    files = export.build_bundle(options(tmp_path))
    assert _datasource_variable(json.loads(files["grafana/dashboards/graph-loader-overview.json"]))["current"] == {}
    assert yaml.safe_load(files["grafana/provisioning/datasources/prometheus.yml"])["datasources"][0]["uid"] == "prometheus"


def test_the_bundled_stack_provisions_the_dashboard_variant_for_its_own_datasource() -> None:
    """The quick-start stack mounts monitoring/grafana/dashboards as is: its variable must already name the datasource that the same
    stack provisions, or a second Prometheus datasource would change what the panels show."""
    shipped = json.loads((CHECKOUT / "grafana" / "dashboards" / "graph-loader-overview.json").read_text())
    provisioned = yaml.safe_load((CHECKOUT / "grafana" / "provisioning" / "datasources" / "prometheus.yml").read_text())["datasources"][0]
    current = _datasource_variable(shipped)["current"]
    assert "__inputs" not in shipped and current["value"] == provisioned["uid"] and current["text"] == provisioned["name"]


def test_shipping_the_import_literal_in_the_provisioning_file_is_caught(tmp_path, monkeypatch) -> None:
    """Control: the earlier export wrote ``${DS_PROMETHEUS}`` into the provisioned dashboard, where nothing fills it."""
    monkeypatch.setattr(export, "overview_dashboard", lambda uid=None: export.overview_import_dashboard())
    text = export.build_bundle(options(tmp_path))["grafana/dashboards/graph-loader-overview.json"].decode()
    with pytest.raises(AssertionError):
        assert "${DS_" not in text and "__inputs" not in text


def test_the_run_history_dashboard_has_its_own_postgres_datasource_input(tmp_path) -> None:
    dashboard = json.loads(export.build_bundle(options(tmp_path))["grafana/dashboards-sql/graph-loader-run-history.json"])
    assert dashboard["__inputs"][0]["name"] == "DS_POSTGRES" and dashboard["__inputs"][0]["pluginId"] == "grafana-postgresql-datasource"
    variable = next(v for v in dashboard["templating"]["list"] if v["name"] == "pg_datasource")
    assert variable["current"]["value"] == "${DS_POSTGRES}"
    panels = [p for p in dashboard["panels"] if p["type"] != "row"]
    assert panels and all(p["datasource"]["uid"] == "${pg_datasource}" for p in panels)
    for panel in panels:
        for target in panel["targets"]:
            sql = target["rawSql"]
            assert target["datasource"]["uid"] == "${pg_datasource}" and sql.upper().startswith("SELECT") and ";" not in sql
            assert "$schema." in sql
    tables = set(re.findall(r"\$schema\.(\w+)", json.dumps(dashboard)))
    pytest.importorskip("sqlalchemy")
    from neo4j_graph_loader.status import sql_store
    assert tables <= set(sql_store.build_tables(None)[1]), tables


def test_provisioning_uses_the_configured_datasource(tmp_path) -> None:
    files = export.build_bundle(options(tmp_path, datasource_uid="my-prom", prometheus_url="http://prom.example:9090", scrape_interval="10s"))
    datasource = yaml.safe_load(files["grafana/provisioning/datasources/prometheus.yml"])["datasources"][0]
    assert datasource["uid"] == "my-prom" and datasource["url"] == "http://prom.example:9090" and datasource["jsonData"]["timeInterval"] == "10s"


# -- writing and the command ----------------------------------------------------------------------------------------------------------
def test_write_refuses_to_overwrite_without_force(tmp_path) -> None:
    written = export.write_bundle(options(tmp_path))
    assert "prometheus/prometheus.yml" in written and (tmp_path / "bundle" / "manifest.json").is_file()
    with pytest.raises(ExportError, match="--force"):
        export.write_bundle(options(tmp_path))
    export.write_bundle(options(tmp_path, force=True))


def test_the_command_writes_the_bundle_and_reports_errors(tmp_path, capsys, caplog) -> None:
    out = tmp_path / "out"
    assert cli.main(["monitoring", "export", "--out", str(out), "--discovery", "static", "--metrics-port", "9600", "--extra-labels", "team=data"]) == 0
    assert "prometheus/prometheus.yml" in capsys.readouterr().out and (out / "prometheus" / "alerts" / "health.yml").is_file()
    with caplog.at_level("ERROR"):
        assert cli.main(["monitoring", "export", "--out", str(tmp_path / "o2"), "--discovery", "static", "--metrics-port", "9600",
                         "--extra-labels", "run_id=x"]) == 1
    assert "reserved" in caplog.text and not (tmp_path / "o2").exists()
    assert cli.main(["monitoring", "export", "--out", str(out), "--discovery", "static", "--metrics-port", "9600"]) == 1     # exists
    with pytest.raises(SystemExit):
        cli.main(["monitoring", "export", "--out", str(tmp_path / "o3"), "--discovery", "consul"])


# -- promtool (opt-in; owned container, no network) --------------------------------------------------------------------------------------
PROMTOOL = pytest.mark.skipif(os.environ.get("E15_PROMTOOL") != "1" or not shutil.which("docker"),
                              reason="set E15_PROMTOOL=1 (and have Docker) to run promtool in an owned network-less container")


def promtool(directory: Path, *args: str) -> subprocess.CompletedProcess:
    image = os.environ.get("E15_PROMTOOL_IMAGE", "prom/prometheus:v2.55.1")
    return subprocess.run(["docker", "run", "--rm", "--network", "none", "--entrypoint", "promtool", "-v", f"{directory}:/etc/prometheus:ro", image, *args],
                          capture_output=True, text=True, timeout=300)


@PROMTOOL
@pytest.mark.parametrize("discovery", sorted(DISCOVERY_ARGS))
def test_promtool_accepts_every_discovery(tmp_path, discovery) -> None:
    export.write_bundle(options(tmp_path, discovery, extra_labels={"team": "data"}, values=write_values(
        tmp_path, "alerts: {GraphLoaderWriteOutage: {threshold: 300, for: 3m}, GraphLoaderRetryRateHigh: {threshold: 2}}\n")))
    directory = tmp_path / "bundle" / "prometheus"
    for command in (("check", "config", "/etc/prometheus/prometheus.yml"), ):
        result = promtool(directory, *command)
        assert result.returncode == 0, result.stdout + result.stderr
    tests = sorted(p.name for p in (directory / "alert_tests").glob("*_test.yml"))
    result = promtool(directory, "test", "rules", *[f"/etc/prometheus/alert_tests/{n}" for n in tests])
    assert result.returncode == 0, result.stdout + result.stderr


# -- the bundled quick-start stack (D15-2) ------------------------------------------------------------------------------------------------
COMPOSE = (REPO / "docker-compose.monitoring.yml").read_text()


def test_bundled_stack_binds_loopback_requires_a_password_and_makes_anonymous_access_opt_in() -> None:
    document = yaml.safe_load(COMPOSE)["services"]
    assert document["prometheus"]["ports"] == ["${PROMETHEUS_HOST_BIND:-127.0.0.1}:9090:9090"]
    assert document["grafana"]["ports"] == ["${GRAFANA_HOST_BIND:-127.0.0.1}:3000:3000"]
    environment = document["grafana"]["environment"]
    assert environment["GF_SECURITY_ADMIN_PASSWORD"].startswith("${GRAFANA_ADMIN_PASSWORD:?")
    assert "admin}" not in environment["GF_SECURITY_ADMIN_PASSWORD"]
    assert environment["GF_AUTH_ANONYMOUS_ENABLED"] == "${GRAFANA_ANONYMOUS_ENABLED:-false}"
    assert "0.0.0.0" not in json.dumps(document)
    volumes = json.dumps(document["prometheus"]["volumes"] + document["grafana"]["volumes"])
    assert volumes.count("${MONITORING_DIR:-./monitoring}") == 3


@pytest.mark.skipif(not shutil.which("make"), reason="make is not installed")
@pytest.mark.parametrize("password", ["", "admin", "short"])
def test_monitoring_up_rejects_a_missing_or_weak_password_before_any_side_effect(password, tmp_path) -> None:
    environment = {**os.environ, "GRAFANA_ADMIN_PASSWORD": password, "GRAPH_LOADER_DATA_DIR": str(tmp_path / "data")}
    result = subprocess.run(["make", "-C", str(REPO), "monitoring-up"], capture_output=True, text=True, env=environment, timeout=60)
    assert result.returncode != 0 and "GRAFANA_ADMIN_PASSWORD" in result.stderr
    assert not (tmp_path / "data").exists()


@pytest.mark.skipif(not shutil.which("make"), reason="make is not installed")
def test_monitoring_down_does_not_need_the_password_and_up_with_one_would_start_compose() -> None:
    environment = {key: value for key, value in os.environ.items() if key != "GRAFANA_ADMIN_PASSWORD"}
    down = subprocess.run(["make", "-C", str(REPO), "-n", "monitoring-down"], capture_output=True, text=True, env=environment, timeout=60)
    assert down.returncode == 0 and down.stdout.count("GRAFANA_ADMIN_PASSWORD=${GRAFANA_ADMIN_PASSWORD:-unused-for-shutdown}") == 2
    up = subprocess.run(["make", "-C", str(REPO), "-n", "monitoring-up"], capture_output=True, text=True, env={**environment, "GRAFANA_ADMIN_PASSWORD": "long-enough-pw"}, timeout=60)
    assert up.returncode == 0 and "docker compose" in up.stdout


def test_the_make_targets_can_append_one_more_compose_file() -> None:
    """E15.F2.S12: MONITORING_EXTRA_COMPOSE reaches up, stop and rm, so the quick start can run beside other stacks."""
    text = (REPO / "Makefile").read_text()
    assert text.count("${MONITORING_EXTRA_COMPOSE:+-f $$MONITORING_EXTRA_COMPOSE}") == 3
