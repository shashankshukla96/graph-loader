"""CLI/env polish: early argument validation, Docker-unreachable stop/status,
NEO4J_USER and GRAPH_LOADER_VERSION forwarding to containers."""
import argparse
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from src import cli
from src.orchestrator.docker_service import DockerService, _metrics_env_passthrough


def _run_start(argv):
    with patch("src.cli.DockerService") as docker, \
            patch("src.cli.load_schema") as load_schema, \
            patch("src.cli.get_neo4j_driver") as driver, \
            patch("src.cli.apply_schema") as ddl, \
            patch("src.cli._ensure_dead_letter_topics") as dlq, \
            patch("src.cli.start_kafka_edge_planner") as planner:
        rc = cli.main(["start", *argv])
    for m in (docker, load_schema, driver, ddl, dlq, planner):
        m.assert_not_called()
    return rc


@pytest.mark.parametrize("value", ["0", "-5"])
def test_bulk_timeout_rejected_before_any_side_effect(value, caplog):
    assert _run_start(["--mode", "bulk", "--bulk-timeout-seconds", value]) == 1
    assert "stage=launch reason=--bulk-timeout-seconds must be positive" in caplog.text


@pytest.mark.parametrize("value", ["0", "65", "-1"])
def test_retry_replicas_rejected_before_any_side_effect(value, caplog):
    assert _run_start(["--mode", "stream", "--retry-consumer-replicas", value]) == 1
    assert "stage=launch reason=--retry-consumer-replicas must be 1-64" in caplog.text


def test_early_validation_leaves_valid_and_other_mode_values_alone():
    v = cli._validate_early_numeric_args
    v(argparse.Namespace(mode="bulk", bulk_timeout_seconds=300.0, retry_consumer_replicas=1))
    v(argparse.Namespace(mode="stream", bulk_timeout_seconds=0, retry_consumer_replicas=64))
    v(argparse.Namespace(mode="bulk", bulk_timeout_seconds=0.5, retry_consumer_replicas=999))  # replicas: stream only
    v(argparse.Namespace(mode="bulk"))  # defaults when absent
    with pytest.raises(ValueError):
        v(argparse.Namespace(mode="bulk", bulk_timeout_seconds=0))


@pytest.mark.parametrize("handler,args", [
    (cli.handle_stop, argparse.Namespace(loader=None)),
    (cli.handle_status, argparse.Namespace(config="x.yaml")),
])
def test_stop_status_without_docker_exit_1(handler, args, caplog):
    with patch("src.cli.DockerService", side_effect=Exception("Error while fetching server API version")):
        assert handler(args) == 1
    assert "stage=docker reason=unreachable" in caplog.text


def _env_of_launches(monkeypatch, **env):
    for k in ("NEO4J_USERNAME", "NEO4J_USER", "GRAPH_LOADER_VERSION"):
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    client = MagicMock()
    svc = DockerService(client=client)
    with tempfile.TemporaryDirectory() as d:
        svc.run_node_loader(node_label="P", topic="t", mode="stream", config_path="c.yaml",
                            replicas=1, network="n", rejection_dir=d)
        svc.run_retry_consumer(run_id="r", config_path="c.yaml", dlq_topics=["a.dlq"], network="n",
                               lease_address="lease:1", replicas=1)
    return [c.kwargs["environment"] for c in client.containers.run.call_args_list]


@pytest.mark.parametrize("env,expected", [
    ({}, "neo4j"),
    ({"NEO4J_USER": "legacy"}, "legacy"),
    ({"NEO4J_USERNAME": "doc", "NEO4J_USER": "legacy"}, "doc"),
])
def test_containers_get_the_cli_resolved_username(monkeypatch, env, expected):
    envs = _env_of_launches(monkeypatch, **env)
    assert len(envs) == 2
    assert all(e["NEO4J_USERNAME"] == expected for e in envs)
    assert cli.get_neo4j_credentials()[1] == expected


def test_graph_loader_version_forwarded_only_when_set(monkeypatch):
    monkeypatch.delenv("GRAPH_LOADER_VERSION", raising=False)
    assert "GRAPH_LOADER_VERSION" not in _metrics_env_passthrough()
    monkeypatch.setenv("GRAPH_LOADER_VERSION", "v1.2.3")
    assert _metrics_env_passthrough()["GRAPH_LOADER_VERSION"] == "v1.2.3"
    envs = _env_of_launches(monkeypatch, GRAPH_LOADER_VERSION="v1.2.3")
    assert all(e["GRAPH_LOADER_VERSION"] == "v1.2.3" for e in envs)
