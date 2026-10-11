"""Stream-mode topic preflight: fail fast, launch nothing, never create topics."""
from __future__ import annotations

import argparse
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src import cli
from src.cli import _missing_schema_topics, _preflight_stream_topics, handle_start

_REAL_PREFLIGHT = _preflight_stream_topics


def _schema():
    return SimpleNamespace(
        nodes=[SimpleNamespace(label="Person", topic="person-events", replicas=1, dead_letter=None)],
        edges=[SimpleNamespace(type="KNOWS", topic="knows-events", topics=["knows-events"], dead_letter_entity="knows-events", is_multi_topic=False, replicas=1, dead_letter=None, order_by=None)],
    )


def _admin(topics: dict):
    admin = MagicMock()
    admin.list_topics.return_value = SimpleNamespace(topics=topics)
    return admin


def _meta(error=None):
    return SimpleNamespace(error=error)


def test_missing_topics_lists_only_absent_or_errored_topics() -> None:
    admin = _admin({"person-events": _meta(), "knows-events": _meta(error="UNKNOWN_TOPIC_OR_PART")})
    with patch("confluent_kafka.admin.AdminClient", return_value=admin):
        assert _missing_schema_topics(_schema(), "kafka:9092") == ["knows-events"]


def test_missing_topics_empty_when_all_exist() -> None:
    admin = _admin({"person-events": _meta(), "knows-events": _meta()})
    with patch("confluent_kafka.admin.AdminClient", return_value=admin):
        assert _missing_schema_topics(_schema(), "kafka:9092") == []
    admin.create_topics.assert_not_called()


def test_preflight_names_missing_topics_and_never_creates(caplog) -> None:
    admin = _admin({})
    with patch("confluent_kafka.admin.AdminClient", return_value=admin), caplog.at_level(logging.ERROR):
        assert _REAL_PREFLIGHT(_schema(), "kafka:9092") is False
    assert "person-events, knows-events" in caplog.text
    assert "Create or publish them first" in caplog.text
    admin.create_topics.assert_not_called()


def test_preflight_fails_closed_when_broker_unreachable() -> None:
    with patch("src.cli._missing_schema_topics", side_effect=RuntimeError("broker down")):
        assert _REAL_PREFLIGHT(_schema(), "kafka:9092") is False


@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
@patch("src.cli._ensure_dead_letter_topics")
def test_handle_start_stream_with_missing_topics_returns_1_and_launches_nothing(
    mock_dlq, mock_load, _driver, mock_apply, mock_docker, monkeypatch,
) -> None:
    monkeypatch.setenv("GRAPH_LOADER_METRICS_ENABLED", "0")
    monkeypatch.setattr(cli, "_preflight_stream_topics", _REAL_PREFLIGHT)
    mock_load.return_value = _schema()
    docker_instance = MagicMock()
    mock_docker.return_value = docker_instance
    args = argparse.Namespace(mode="stream", config="config.yaml", network="test_net")

    with patch("src.cli._missing_schema_topics", return_value=["knows-events"]):
        assert handle_start(args) == 1

    mock_dlq.assert_not_called()
    mock_apply.assert_not_called()
    assert docker_instance.method_calls == []


@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
@patch("src.cli._ensure_dead_letter_topics")
def test_handle_start_bulk_skips_the_preflight(mock_dlq, mock_load, _driver, mock_apply, mock_docker, monkeypatch) -> None:
    monkeypatch.setenv("GRAPH_LOADER_METRICS_ENABLED", "0")
    mock_load.return_value = _schema()
    mock_dlq.side_effect = RuntimeError("stop here")  # abort right after the preflight site
    preflight = MagicMock(return_value=False)
    monkeypatch.setattr(cli, "_preflight_stream_topics", preflight)

    assert handle_start(argparse.Namespace(mode="bulk", config="config.yaml", network="n")) == 1

    preflight.assert_not_called()
    mock_dlq.assert_called_once()
