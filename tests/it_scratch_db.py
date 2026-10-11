"""Scratch-database guard for the opt-in end-to-end tests that target a developer-run Neo4j.

Those tests (``test_rotation_e2e``, ``test_streaming_lease_e2e``, ``test_relationship_bulk_e2e``,
``test_self_reference_isolation_e2e``, ``test_social_planned_integration`` and
``test_schema_initializer_int``) run against a stack the developer started, whose server default
database is ``neo4j``.  They must never write there, so each one needs an explicit scratch database
named by ``GRAPH_LOADER_IT_DATABASE`` (recommended name: ``glit``) and is skipped when it is unset.
"""
from __future__ import annotations

import os
import re

import pytest

ENV_VAR = "GRAPH_LOADER_IT_DATABASE"
#: Databases tests must never touch (compared case-insensitively).  ``system`` holds no user data.
FORBIDDEN_DATABASES = frozenset({"neo4j", "latesttesttemp", "system"})
_NAME = re.compile(r"[A-Za-z][A-Za-z0-9]{0,62}")


def check_database_name(name: str) -> str:
    """Return ``name`` if it is an acceptable scratch database, else raise ``ValueError``."""
    if name.lower() in FORBIDDEN_DATABASES:
        raise ValueError(
            f"{ENV_VAR}={name!r} is refused: tests must never use the {sorted(FORBIDDEN_DATABASES)} databases"
        )
    if not _NAME.fullmatch(name):
        raise ValueError(f"{ENV_VAR}={name!r} must be 1-63 letters/digits starting with a letter")
    return name


def scratch_database_name() -> str:
    """Return the validated scratch database name; skip the test if unset, fail if forbidden."""
    name = os.environ.get(ENV_VAR, "").strip()
    if not name:
        pytest.skip(f"set {ENV_VAR} to a scratch Neo4j database (e.g. glit); these tests never use the default database")
    try:
        return check_database_name(name)
    except ValueError as exc:
        pytest.fail(str(exc), pytrace=False)


def ensure_database(driver, name: str) -> str:
    """Create the scratch database ``name`` if missing (idempotent, waits until online)."""
    check_database_name(name)
    with driver.session(database="system") as session:
        session.run(f"CREATE DATABASE `{name}` IF NOT EXISTS WAIT").consume()
    return name


#: The password the repo's docker-compose ships with.  Tests never fall back to it.
DEFAULT_PASSWORD = "changeme"


def explicit_neo4j_auth(
    *, password_var: str = "NEO4J_PASSWORD", uri_var: str = "NEO4J_URI",
    default_uri: str = "bolt://localhost:7687", opted_in_by: str | None = None,
) -> tuple[str, str, str]:
    """Return ``(uri, user, password)`` from the environment, never trying a default password.

    A test that guesses the default ``changeme`` password against a shared Neo4j fails its login,
    and a few failed logins in a row trip Neo4j's ``AuthenticationRateLimit``, which then locks out
    every real client of that user for a while.  So the password must be exported explicitly.
    Without it the test is skipped before any connection, unless the run explicitly opted in to it
    (``opted_in_by`` names the opt-in variable, set): then a missing password is a setup error and
    the test fails, so an opted-in run is never reported green by a silent skip.

    The user is ``NEO4J_USERNAME``, else ``NEO4J_USER``, else ``neo4j`` (as ``src.cli``).
    """
    password = os.environ.get(password_var, "")
    uri = os.environ.get(uri_var, default_uri)
    if not password:
        reason = (
            f"{password_var} is not set: export the Neo4j credentials for {uri} explicitly; this test never "
            f"tries the default {DEFAULT_PASSWORD!r} password (failed logins trip AuthenticationRateLimit "
            "and lock out other clients of a shared Neo4j)"
        )
        if opted_in_by is not None and os.environ.get(opted_in_by, "").strip() not in ("", "0"):
            pytest.fail(f"{opted_in_by} is set but {reason}", pytrace=False)
        pytest.skip(reason)
    user = os.environ.get("NEO4J_USERNAME") or os.environ.get("NEO4J_USER") or "neo4j"
    return uri, user, password
