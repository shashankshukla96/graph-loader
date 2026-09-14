"""
tests/test_cli.py
─────────────────
Unit tests for the CLI parser and routing.
"""
import pytest
import argparse
from unittest.mock import patch, MagicMock

from src.cli import build_parser, get_neo4j_credentials, main, handle_start, handle_stop, handle_status
from src.orchestrator.schema_initializer import SchemaInitializationError


def test_parser_start_valid():
    parser = build_parser()
    args = parser.parse_args(["start", "--mode", "bulk"])
    assert args.command == "start"
    assert args.mode == "bulk"
    assert args.config == "config/graph_schema.yaml"
    assert hasattr(args, "func")


def test_parser_start_missing_mode(capsys):
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["start"])
    captured = capsys.readouterr()
    assert "the following arguments are required: --mode" in captured.err


def test_parser_stop():
    parser = build_parser()
    args = parser.parse_args(["stop", "--loader", "company_loader"])
    assert args.command == "stop"
    assert args.loader == "company_loader"


def test_parser_status():
    parser = build_parser()
    args = parser.parse_args(["status", "--config", "custom.yaml"])
    assert args.command == "status"
    assert args.config == "custom.yaml"


@patch("src.cli.handle_start")
def test_main_routes_to_start_handler(mock_handle_start):
    mock_handle_start.return_value = 0
    exit_code = main(["start", "--mode", "stream"])
    assert exit_code == 0
    mock_handle_start.assert_called_once()
    args = mock_handle_start.call_args[0][0]
    assert args.mode == "stream"


@patch("src.cli.handle_stop")
def test_main_routes_to_stop_handler(mock_handle_stop):
    mock_handle_stop.return_value = 0
    exit_code = main(["stop"])
    assert exit_code == 0
    mock_handle_stop.assert_called_once()


@patch("src.cli.handle_status")
def test_main_routes_to_status_handler(mock_handle_status):
    mock_handle_status.return_value = 0
    exit_code = main(["status"])
    assert exit_code == 0
    mock_handle_status.assert_called_once()


@patch("src.cli.DockerService")
def test_handle_stop_stub(mock_docker):
    args = argparse.Namespace(loader=None)
    mock_docker_instance = MagicMock()
    mock_docker.return_value = mock_docker_instance
    assert handle_stop(args) == 0
    mock_docker_instance.stop_node_loaders.assert_called_once_with(node_label=None)


@patch("src.cli.DockerService")
def test_handle_status_stub(mock_docker):
    args = argparse.Namespace(config="config/graph_schema.yaml")
    mock_docker_instance = MagicMock()
    mock_docker_instance.list_node_loaders.return_value = [{"name": "c1", "id": "1", "status": "running", "node_label": "P"}]
    mock_docker.return_value = mock_docker_instance
    assert handle_status(args) == 0
    mock_docker_instance.list_node_loaders.assert_called_once()


def test_neo4j_username_prefers_documented_variable(monkeypatch):
    monkeypatch.setenv("NEO4J_USERNAME", "documented")
    monkeypatch.setenv("NEO4J_USER", "legacy")
    assert get_neo4j_credentials()[1] == "documented"


def test_neo4j_username_uses_legacy_fallback(monkeypatch):
    monkeypatch.delenv("NEO4J_USERNAME", raising=False)
    monkeypatch.setenv("NEO4J_USER", "legacy")
    assert get_neo4j_credentials()[1] == "legacy"


@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_handle_start_success(mock_load, mock_get_driver, mock_apply_schema, mock_docker):
    args = argparse.Namespace(mode="bulk", config="config.yaml", network="test_net")
    mock_driver = MagicMock()
    mock_get_driver.return_value = mock_driver
    mock_schema = MagicMock()
    mock_node = MagicMock()
    mock_node.label = "Person"
    mock_node.topic = "person_topic"
    mock_schema.nodes = [mock_node]
    mock_load.return_value = mock_schema

    mock_docker_instance = MagicMock()
    mock_docker.return_value = mock_docker_instance
    
    exit_code = handle_start(args)
    
    assert exit_code == 0
    mock_load.assert_called_once_with("config.yaml")
    mock_get_driver.assert_called_once()
    mock_apply_schema.assert_called_once()
    mock_driver.close.assert_called_once()
    
    mock_docker_instance.build_image.assert_called_once()
    mock_docker_instance.run_node_loader.assert_called_once_with(
        node_label="Person",
        topic="person_topic",
        mode="bulk",
        config_path="config.yaml",
        replicas=mock_node.replicas,
        network="test_net"
    )


@patch("src.cli.load_schema")
def test_handle_start_schema_load_failure(mock_load):
    args = argparse.Namespace(mode="bulk", config="config.yaml")
    mock_load.side_effect = Exception("File not found")
    
    exit_code = handle_start(args)
    
    assert exit_code == 1


@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_handle_start_init_failure(mock_load, mock_get_driver, mock_apply_schema):
    args = argparse.Namespace(mode="bulk", config="config.yaml")
    mock_get_driver.side_effect = SchemaInitializationError("DB offline")
    
    exit_code = handle_start(args)
    
    assert exit_code == 1
    mock_load.assert_called_once()
    mock_apply_schema.assert_not_called()


@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_handle_start_docker_launch_failure(mock_load, mock_get_driver, mock_apply_schema, mock_docker):
    args = argparse.Namespace(mode="bulk", config="config.yaml", network="test_net")
    mock_schema = MagicMock()
    mock_node1 = MagicMock()
    mock_node2 = MagicMock()
    mock_schema.nodes = [mock_node1, mock_node2]
    mock_load.return_value = mock_schema

    mock_docker_instance = MagicMock()
    mock_docker.return_value = mock_docker_instance
    mock_container = MagicMock()
    
    # First call succeeds, second fails
    mock_docker_instance.run_node_loader.side_effect = [[mock_container], Exception("Launch failed")]

    exit_code = handle_start(args)

    assert exit_code == 1
    mock_container.stop.assert_called_once()
