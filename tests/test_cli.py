"""
tests/test_cli.py
─────────────────
Unit tests for the CLI parser and routing.
"""
import pytest
import argparse
from unittest.mock import patch, MagicMock

from src.cli import build_parser, main, handle_start, handle_stop, handle_status
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


def test_handle_stop_stub():
    args = argparse.Namespace(loader=None)
    assert handle_stop(args) == 0
    args_with_loader = argparse.Namespace(loader="my_loader")
    assert handle_stop(args_with_loader) == 0


def test_handle_status_stub():
    args = argparse.Namespace(config="config/graph_schema.yaml")
    assert handle_status(args) == 0


@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_handle_start_success(mock_load, mock_get_driver, mock_apply_schema):
    args = argparse.Namespace(mode="bulk", config="config.yaml")
    mock_driver = MagicMock()
    mock_get_driver.return_value = mock_driver
    
    exit_code = handle_start(args)
    
    assert exit_code == 0
    mock_load.assert_called_once_with("config.yaml")
    mock_get_driver.assert_called_once()
    mock_apply_schema.assert_called_once()
    mock_driver.close.assert_called_once()


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
