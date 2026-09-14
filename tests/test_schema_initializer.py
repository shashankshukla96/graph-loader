"""
tests/test_schema_initializer.py
─────────────────────────────────
Unit tests for the schema initializer module.
"""
import pytest
from unittest.mock import patch, MagicMock
from neo4j.exceptions import ServiceUnavailable, AuthError

from src.orchestrator.schema_initializer import get_neo4j_driver, SchemaInitializationError


class TestNeo4jConnection:
    
    @patch("src.orchestrator.schema_initializer.GraphDatabase.driver")
    def test_connect_success(self, mock_driver_factory):
        mock_driver = MagicMock()
        mock_driver_factory.return_value = mock_driver
        
        driver = get_neo4j_driver("bolt://localhost:7687", "neo4j", "password")
        
        assert driver == mock_driver
        mock_driver.verify_connectivity.assert_called_once()
        
    @patch("src.orchestrator.schema_initializer.time.sleep")
    @patch("src.orchestrator.schema_initializer.GraphDatabase.driver")
    def test_connect_retries_and_succeeds(self, mock_driver_factory, mock_sleep):
        mock_driver = MagicMock()
        
        # Fail twice with ServiceUnavailable, then succeed
        mock_driver.verify_connectivity.side_effect = [
            ServiceUnavailable("db booting"),
            ServiceUnavailable("db booting"),
            None  # success
        ]
        mock_driver_factory.return_value = mock_driver
        
        driver = get_neo4j_driver("bolt://localhost:7687", "neo4j", "pass", base_delay=1.0)
        
        assert driver == mock_driver
        assert mock_driver.verify_connectivity.call_count == 3
        
        # Check backoff logic: attempt 0 (1.0s), attempt 1 (2.0s)
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(1.0)
        mock_sleep.assert_any_call(2.0)
        
        # driver should have been closed twice (on the two failures)
        assert mock_driver.close.call_count == 2
        
    @patch("src.orchestrator.schema_initializer.time.sleep")
    @patch("src.orchestrator.schema_initializer.GraphDatabase.driver")
    def test_connect_exhausts_retries(self, mock_driver_factory, mock_sleep):
        mock_driver = MagicMock()
        mock_driver.verify_connectivity.side_effect = ServiceUnavailable("db dead")
        mock_driver_factory.return_value = mock_driver
        
        with pytest.raises(SchemaInitializationError, match="Failed to connect to Neo4j after 2 retries"):
            get_neo4j_driver("bolt://localhost:7687", "neo4j", "pass", max_retries=2, base_delay=1.0)
            
        assert mock_driver.verify_connectivity.call_count == 3  # Initial try + 2 retries
        assert mock_sleep.call_count == 2
        assert mock_driver.close.call_count == 3
        
    @patch("src.orchestrator.schema_initializer.time.sleep")
    @patch("src.orchestrator.schema_initializer.GraphDatabase.driver")
    def test_connect_auth_error_no_retry(self, mock_driver_factory, mock_sleep):
        mock_driver = MagicMock()
        mock_driver.verify_connectivity.side_effect = AuthError("bad password")
        mock_driver_factory.return_value = mock_driver
        
        with pytest.raises(SchemaInitializationError, match="Authentication failed"):
            get_neo4j_driver("bolt://localhost:7687", "neo4j", "pass", max_retries=3)
            
        assert mock_driver.verify_connectivity.call_count == 1
        mock_sleep.assert_not_called()
        mock_driver.close.assert_called_once()
        
    @patch("src.orchestrator.schema_initializer.time.sleep")
    @patch("src.orchestrator.schema_initializer.GraphDatabase.driver")
    def test_connect_unexpected_error_no_retry(self, mock_driver_factory, mock_sleep):
        mock_driver = MagicMock()
        mock_driver.verify_connectivity.side_effect = ValueError("Something else broke")
        mock_driver_factory.return_value = mock_driver
        
        with pytest.raises(SchemaInitializationError, match="Unexpected connection error"):
            get_neo4j_driver("bolt://localhost:7687", "neo4j", "pass", max_retries=3)
            
        assert mock_driver.verify_connectivity.call_count == 1
        mock_sleep.assert_not_called()
        mock_driver.close.assert_called_once()

from src.orchestrator.schema_initializer import apply_schema
from neo4j.exceptions import Neo4jError

class TestSchemaApplication:
    
    @patch("src.orchestrator.schema_initializer.generate_all_ddl")
    def test_apply_schema_success(self, mock_generate):
        mock_generate.return_value = ["CREATE CONSTRAINT foo"]
        mock_driver = MagicMock()
        mock_session = mock_driver.session.return_value.__enter__.return_value
        # Return empty list for SHOW INDEXES (no failed, no populating)
        mock_session.run.return_value = []
        
        apply_schema(mock_driver, MagicMock())
        
        mock_generate.assert_called_once()
        # session.run should be called twice: once for DDL, once for SHOW INDEXES
        assert mock_session.run.call_count == 2
        
    @patch("src.orchestrator.schema_initializer.time.sleep")
    @patch("src.orchestrator.schema_initializer.generate_all_ddl")
    def test_apply_schema_waits_and_succeeds(self, mock_generate, mock_sleep):
        mock_generate.return_value = []
        mock_driver = MagicMock()
        mock_session = mock_driver.session.return_value.__enter__.return_value
        
        # First poll: one populating. Second poll: none.
        mock_session.run.side_effect = [
            [{"name": "idx1", "state": "POPULATING"}],
            []
        ]
        
        apply_schema(mock_driver, MagicMock(), poll_interval=1.0)
        
        mock_sleep.assert_called_once_with(1.0)
        assert mock_session.run.call_count == 2
        
    @patch("src.orchestrator.schema_initializer.generate_all_ddl")
    def test_apply_schema_index_failed(self, mock_generate):
        mock_generate.return_value = []
        mock_driver = MagicMock()
        mock_session = mock_driver.session.return_value.__enter__.return_value
        mock_session.run.return_value = [{"name": "idx1", "state": "FAILED"}]
        
        with pytest.raises(SchemaInitializationError, match="Index creation failed for: \\['idx1'\\]"):
            apply_schema(mock_driver, MagicMock())
            
    @patch("src.orchestrator.schema_initializer.time.monotonic")
    @patch("src.orchestrator.schema_initializer.time.sleep")
    @patch("src.orchestrator.schema_initializer.generate_all_ddl")
    def test_apply_schema_timeout(self, mock_generate, mock_sleep, mock_monotonic):
        mock_generate.return_value = []
        mock_driver = MagicMock()
        mock_session = mock_driver.session.return_value.__enter__.return_value
        mock_session.run.return_value = [{"name": "idx1", "state": "POPULATING"}]
        
        # mock_monotonic to simulate time passing beyond timeout
        mock_monotonic.side_effect = [0.0, 10.0, 150.0]
        
        with pytest.raises(SchemaInitializationError, match="Timeout waiting for indexes"):
            apply_schema(mock_driver, MagicMock(), timeout_seconds=120.0)
            
    @patch("src.orchestrator.schema_initializer.generate_all_ddl")
    def test_apply_schema_ddl_error(self, mock_generate):
        mock_generate.return_value = ["CREATE CONSTRAINT foo"]
        mock_driver = MagicMock()
        mock_session = mock_driver.session.return_value.__enter__.return_value
        mock_session.run.side_effect = Neo4jError("Syntax error")
        
        with pytest.raises(SchemaInitializationError, match="Failed to execute DDL"):
            apply_schema(mock_driver, MagicMock())
