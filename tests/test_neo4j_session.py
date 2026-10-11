from unittest.mock import MagicMock

from src.utils.neo4j_session import open_session


def test_open_session_uses_configured_database(monkeypatch):
    driver = MagicMock()
    monkeypatch.setenv("NEO4J_DATABASE", "latesttesttemp")

    open_session(driver)

    driver.session.assert_called_once_with(database="latesttesttemp")


def test_open_session_preserves_default_when_database_is_unset(monkeypatch):
    driver = MagicMock()
    monkeypatch.delenv("NEO4J_DATABASE", raising=False)

    open_session(driver)

    driver.session.assert_called_once_with()
