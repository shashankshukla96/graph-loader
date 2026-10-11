"""Explicit fixtures for integration tests that require isolated containers."""
from __future__ import annotations

from collections.abc import Iterator

import os

import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from docker.errors import DockerException
from neo4j import Driver
from testcontainers.community.neo4j import Neo4jContainer
from testcontainers.community.kafka import KafkaContainer
import uuid
import time


@pytest.fixture(autouse=True)
def _disable_metrics_server_by_default(monkeypatch):
    """Default the whole unit suite to ``GRAPH_LOADER_METRICS_ENABLED=0``.

    Every process kind's ``main()``/``serve()`` now calls
    ``start_metrics_server`` (P6-S2 Story 3), which binds a real TCP socket
    when enabled (the default). Hundreds of existing tests call a loader's
    ``main()`` directly and would otherwise each bind port 9464 (or
    whatever ``GRAPH_LOADER_METRICS_PORT`` is set to), which is slow, can
    flake under any port contention, and tests nothing about metrics
    themselves. ``tests/test_metrics.py`` explicitly re-enables
    (``monkeypatch.delenv``) for the handful of cases that need a live
    server.
    """
    monkeypatch.setenv("GRAPH_LOADER_METRICS_ENABLED", "0")


@pytest.fixture(autouse=True)
def _restore_sigterm_handler():
    """Loader ``main()`` functions deliberately leave their flag-only SIGTERM
    handler installed until process exit (F2); undo that between tests."""
    import signal

    before = signal.getsignal(signal.SIGTERM)
    yield
    signal.signal(signal.SIGTERM, before)


@pytest.fixture(autouse=True)
def _reset_cli_stop_registry():
    """The CLI stops each container at most once per process; forget that between tests."""
    import sys

    yield
    cli = sys.modules.get("src.cli")
    if cli is not None:
        with cli._stop_registry_lock:
            cli._stop_locks.clear()
            cli._stop_initiated.clear()
            cli._stop_done.clear()
    docker_service = sys.modules.get("src.orchestrator.docker_service")
    if docker_service is not None:
        with docker_service._launch_lock:
            docker_service._launched_at.clear()
            docker_service._signal_ready.clear()


def _stop_snapshotters(metrics_module) -> None:
    """Stop and clear any periodic snapshot threads a test left running."""
    with metrics_module._snapshotters_lock:
        snapshotters = list(metrics_module._snapshotters.values())
        metrics_module._snapshotters.clear()
    for snapshotter in snapshotters:
        snapshotter.stop()


@pytest.fixture(autouse=True)
def _reset_metrics_globals():
    """Reset every process-wide metrics global before AND after each test.

    ``src.utils.metrics.process_metrics()`` is a per-process singleton and
    ``install_dlq_routed_listener()``/``install_parked_listener()`` (P5-S4
    Story 7) permanently rebind ``src.loader.dead_letter.on_dlq_routed``/
    ``on_parked`` (P6-S2 review finding B3): once any test — today only
    ``tests/test_metrics.py``, soon also every
    ``main()``-calling test once Story 4/5 wires the loaders — calls either,
    the effect would otherwise leak into every later test in the same
    pytest process. Reset here, repo-wide, rather than only inside
    ``tests/test_metrics.py``'s own fixture, so a Story 4/5 test in a
    different file can never silently inherit contaminated state.
    """
    from src.loader import dead_letter
    from src.utils import metrics as metrics_module

    original_on_dlq_routed = dead_letter.on_dlq_routed
    original_on_parked = dead_letter.on_parked
    _stop_snapshotters(metrics_module)
    metrics_module._reset_process_metrics_for_tests()
    metrics_module._reset_dlq_routed_listener_for_tests()
    metrics_module._reset_parked_listener_for_tests()
    yield
    _stop_snapshotters(metrics_module)
    metrics_module._reset_process_metrics_for_tests()
    metrics_module._reset_dlq_routed_listener_for_tests()
    metrics_module._reset_parked_listener_for_tests()
    dead_letter.on_dlq_routed = original_on_dlq_routed
    dead_letter.on_parked = original_on_parked


@pytest.fixture(scope="session")
def neo4j_connection() -> Iterator[tuple[str, str, str]]:
    """Provide isolated Neo4j connection settings, or skip without Docker."""
    container = (
        # P6-S5 (found during P6-S4): bumped from 5.21 - 5.21 cannot parse
        # `CALL (vars) { ... }` scoped subqueries, which
        # build_apoc_locked_upsert_query (src/loader/edge_execution.py)
        # uses for the classic and lease relationship writers' queries
        # (the lease server itself runs no Cypher at all). 5.26 is
        # confirmed working; see docs/USAGE.md Sec 2 for the documented
        # minimum supported version (5.23, where the syntax first landed).
        Neo4jContainer("neo4j:5.26-enterprise", password="test-password")
        .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
    )
    try:
        container.start()
    except (DockerException, TimeoutError) as exc:
        try:
            container.stop()
        except Exception:
            pass
        pytest.skip(f"Docker is required for Neo4j integration tests: {exc}")
    try:
        yield (container.get_connection_url(), container.username, container.password)
    finally:
        container.stop()


@pytest.fixture(scope="session")
def neo4j_driver(neo4j_connection: tuple[str, str, str]) -> Iterator[Driver]:
    """Return a driver for the isolated Neo4j container."""
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver(neo4j_connection[0], auth=neo4j_connection[1:])
    driver.verify_connectivity()
    try:
        yield driver
    finally:
        driver.close()


def _reset_neo4j(driver: Driver) -> None:
    """Empty the session-scoped test database: data, constraints and (non-LOOKUP) indexes.

    The container is shared by every test of the session, so schema a test applies (for example
    ``config/graph_schema.yaml``'s ``Company.name IS NOT NULL``) must not leak into the next one.
    Constraints go first (they own their backing indexes), then the remaining indexes except the
    built-in LOOKUP ones; every statement is its own auto-commit transaction (schema and data
    changes cannot share one).
    """
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n").consume()
        for record in list(session.run("SHOW CONSTRAINTS YIELD name RETURN name")):
            session.run(f"DROP CONSTRAINT `{record['name']}` IF EXISTS").consume()
        for record in list(session.run("SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP' RETURN name")):
            session.run(f"DROP INDEX `{record['name']}` IF EXISTS").consume()


@pytest.fixture
def clean_neo4j(neo4j_driver: Driver) -> Iterator[None]:
    """Reset graph data and schema before and after one explicitly requesting test."""
    _reset_neo4j(neo4j_driver)
    try:
        yield
    finally:
        _reset_neo4j(neo4j_driver)


@pytest.fixture(scope="session")
def kafka_bootstrap() -> Iterator[str]:
    """Provide an isolated Kafka bootstrap server or skip without Docker."""
    container = KafkaContainer()
    try:
        container.start()
    except (DockerException, TimeoutError) as exc:
        try:
            container.stop()
        except Exception:
            pass
        pytest.skip(f"Docker is required for Kafka integration tests: {exc}")
    try:
        yield container.get_bootstrap_server()
    finally:
        container.stop()


@pytest.fixture
def kafka_topic(kafka_bootstrap: str) -> Iterator[str]:
    """Create and delete one UUID-suffixed single-partition Kafka topic."""
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    topic = f"node-loader-test-{uuid.uuid4().hex}"
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result(20)
    try:
        yield topic
    finally:
        admin.delete_topics([topic])[topic].result(20)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if topic not in admin.list_topics(timeout=5).topics:
                break
            time.sleep(0.25)
        else:
            raise AssertionError(f"Kafka did not delete test topic {topic!r} within 20 seconds")


# Tests that start their own testcontainers must build connection settings from
# the container fixtures only; a developer shell that sourced a local bench env
# (NEO4J_DATABASE=bench, ...) must not leak into them.  The rule follows fixture
# dependency, not file paths: any test whose fixture closure contains a
# Neo4j/Kafka container fixture (``neo4j_driver``, ``neo4j_connection``,
# ``clean_neo4j``, ``kafka_bootstrap``, ``kafka_topic`` and the per-module
# ``neo4j_logged``, ``stress_neo4j``, ``logged_apoc_neo4j`` ... -- every name
# containing "neo4j" or "kafka") is scrubbed.  The opt-in developer-stack e2e
# tests deliberately use none of these and keep their ambient endpoints.
_CONTAINER_FIXTURE_MARKERS = ("neo4j", "kafka")
_AMBIENT_PREFIXES = ("NEO4J_", "KAFKA_", "LOADER_NEO4J_", "LOADER_KAFKA_")


@pytest.fixture(autouse=True)
def _isolate_container_tests_from_ambient_env(request, monkeypatch):
    """Drop ambient NEO4J_*/KAFKA_*/LOADER_* connection variables for testcontainers-backed tests."""
    if any(marker in name for name in request.fixturenames for marker in _CONTAINER_FIXTURE_MARKERS):
        for name in [name for name in os.environ if name.startswith(_AMBIENT_PREFIXES)]:
            monkeypatch.delenv(name)


@pytest.fixture
def it_database(monkeypatch) -> str:
    """Scratch database for opt-in developer-stack e2e tests (``GRAPH_LOADER_IT_DATABASE``).

    Skips when the variable is unset and fails for ``neo4j``/``latesttesttemp``.  Also exports it as
    ``NEO4J_DATABASE`` so the CLI, its containers and any subprocess write there.  Tests create the
    database with ``it_scratch_db.ensure_database(driver, name)`` once they hold a driver.
    """
    from it_scratch_db import scratch_database_name

    name = scratch_database_name()
    monkeypatch.setenv("NEO4J_DATABASE", name)
    return name


@pytest.fixture(autouse=True)
def _no_control_topic_admin(request, monkeypatch):
    """Unit tests drive ``handle_start`` without a broker: skip ensuring the control topic and the stream topic preflight."""
    if request.node.get_closest_marker("integration") is None:
        monkeypatch.setattr("src.cli._ensure_control_topic", lambda _bootstrap: None)
        monkeypatch.setattr("src.cli._preflight_stream_topics", lambda _schema, _bootstrap: True)
        # v7-S4: the former-DLQ-name preflight lists topics too; tested directly with fakes.
        monkeypatch.setattr("src.cli._require_no_stale_dead_letter_topics", lambda _schema, _bootstrap, **_kw: None)
        # Final follow-ups S2: the index-leftover preflight reads index topics; tested directly with fakes.
        monkeypatch.setattr("src.cli._require_no_dlq_leftovers", lambda _schema, _bootstrap, **_kw: None)
        # Stable stream groups: the pre-launch member check and migration talk to Kafka;
        # tested directly with a fake admin (tests/test_stream_consumer_groups.py).
        # Tests marked ``real_stream_groups`` keep the real function.
        if request.node.get_closest_marker("real_stream_groups") is None:
            monkeypatch.setattr("src.cli._prepare_stream_consumer_groups", lambda *_a, **_kw: None)
