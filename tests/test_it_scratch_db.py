"""Unit tests for the scratch-database guard and the container-fixture env scrub."""
from __future__ import annotations

import pytest

from it_scratch_db import ENV_VAR, check_database_name, explicit_neo4j_auth, scratch_database_name


@pytest.fixture
def _glit(monkeypatch) -> None:
    monkeypatch.setenv(ENV_VAR, "glit")


@pytest.mark.parametrize("name", ["neo4j", "NEO4J", "latesttesttemp", "system"])
def test_forbidden_databases_are_refused(name, monkeypatch) -> None:
    with pytest.raises(ValueError, match="refused"):
        check_database_name(name)
    monkeypatch.setenv(ENV_VAR, name)
    with pytest.raises(pytest.fail.Exception):
        scratch_database_name()


@pytest.mark.parametrize("name", ["", "1abc", "a-b", "a`b", "x" * 64])
def test_malformed_names_are_refused(name) -> None:
    with pytest.raises(ValueError):
        check_database_name(name)


def test_unset_variable_skips_with_a_clear_message(monkeypatch) -> None:
    monkeypatch.delenv(ENV_VAR, raising=False)
    with pytest.raises(pytest.skip.Exception, match=ENV_VAR):
        scratch_database_name()


@pytest.mark.usefixtures("_glit")
def test_it_database_fixture_exports_the_cli_database(it_database) -> None:
    import os

    assert os.environ["NEO4J_DATABASE"] == it_database == "glit"


@pytest.mark.usefixtures("_glit")
def test_valid_name_is_returned() -> None:
    assert scratch_database_name() == "glit"


# -- explicit Neo4j credentials (never the default password) -------------------------------------

@pytest.fixture
def _no_neo4j_env(monkeypatch) -> None:
    for name in ("NEO4J_PASSWORD", "NEO4J_URI", "NEO4J_USERNAME", "NEO4J_USER", "RUN_X_E2E"):
        monkeypatch.delenv(name, raising=False)


@pytest.mark.usefixtures("_no_neo4j_env")
def test_no_password_skips_before_any_connection() -> None:
    with pytest.raises(pytest.skip.Exception, match="NEO4J_PASSWORD is not set.*changeme"):
        explicit_neo4j_auth(opted_in_by="RUN_X_E2E")


@pytest.mark.usefixtures("_no_neo4j_env")
def test_an_explicit_opt_in_without_a_password_fails_instead_of_skipping(monkeypatch) -> None:
    monkeypatch.setenv("RUN_X_E2E", "1")
    with pytest.raises(pytest.fail.Exception, match="RUN_X_E2E is set but NEO4J_PASSWORD is not set"):
        explicit_neo4j_auth(opted_in_by="RUN_X_E2E")


@pytest.mark.usefixtures("_no_neo4j_env")
@pytest.mark.parametrize("env, user", [
    ({"NEO4J_USERNAME": "alice", "NEO4J_USER": "bob"}, "alice"),
    ({"NEO4J_USER": "bob"}, "bob"),
    ({}, "neo4j"),
])
def test_credentials_come_from_the_environment(monkeypatch, env, user) -> None:
    monkeypatch.setenv("NEO4J_PASSWORD", "s3cret")
    monkeypatch.setenv("NEO4J_URI", "bolt://scratch:7687")
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    assert explicit_neo4j_auth() == ("bolt://scratch:7687", user, "s3cret")


@pytest.mark.usefixtures("_no_neo4j_env")
def test_alternative_variables_for_the_social_test(monkeypatch) -> None:
    monkeypatch.setenv("GRAPH_LOADER_IT_NEO4J_PASSWORD", "pw")
    monkeypatch.setenv("GRAPH_LOADER_IT_NEO4J_URI", "bolt://localhost:17687")
    assert explicit_neo4j_auth(
        password_var="GRAPH_LOADER_IT_NEO4J_PASSWORD", uri_var="GRAPH_LOADER_IT_NEO4J_URI",
    ) == ("bolt://localhost:17687", "neo4j", "pw")
