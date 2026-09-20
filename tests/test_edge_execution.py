"""Unit tests for relationship execution-mode capability checks."""
from unittest.mock import MagicMock

import pytest
from neo4j.exceptions import ClientError, CypherSyntaxError, ServiceUnavailable

from src.loader.edge_execution import (
    ExecutionModeError, ServerCapabilities, probe_server_capabilities, require_execution_mode,
)


def test_capability_probe_consumes_query_and_closes_session() -> None:
    driver = MagicMock()
    driver.get_server_info.return_value.agent = "Neo4j/2026.06"
    session = driver.session.return_value.__enter__.return_value
    capabilities = probe_server_capabilities(driver)
    assert capabilities.cypher_25 is True
    assert capabilities.neo4j_version == "Neo4j/2026.06"
    session.run.assert_called_once_with("CYPHER 25 RETURN 1 AS supported")
    session.run.return_value.consume.assert_called_once_with()
    driver.session.return_value.__exit__.assert_called_once()


def test_capability_probe_maps_unsupported_syntax_to_false() -> None:
    driver = MagicMock()
    driver.get_server_info.return_value.agent = "Neo4j/5.21"
    session = driver.session.return_value.__enter__.return_value
    session.run.side_effect = CypherSyntaxError("unsupported")
    assert probe_server_capabilities(driver).cypher_25 is False


def test_capability_probe_propagates_connectivity_failure() -> None:
    driver = MagicMock()
    driver.session.side_effect = ServiceUnavailable("offline")
    with pytest.raises(ServiceUnavailable):
        probe_server_capabilities(driver)


def test_capability_probe_propagates_non_syntax_client_error() -> None:
    driver = MagicMock()
    driver.get_server_info.return_value.agent = "Neo4j/2026.06"
    session = driver.session.return_value.__enter__.return_value
    session.run.side_effect = ClientError("forbidden")
    with pytest.raises(ClientError, match="forbidden"):
        probe_server_capabilities(driver)


def test_native_mode_fails_closed_with_diagnostic() -> None:
    with pytest.raises(ExecutionModeError, match="native_disjoint.*5.21"):
        require_execution_mode("native_disjoint", ServerCapabilities("5.21", False))
    require_execution_mode("python_apoc", ServerCapabilities("5.21", False))
