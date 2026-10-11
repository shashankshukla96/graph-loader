"""Integration test (real Docker) proving lock_wait.py detects a genuine wait.

Uses its own function-scoped Neo4jContainer, not the session-scoped
`neo4j_driver` fixture: query logging must be applied with `.with_env(...)`
before `.start()`, and mutating the shared fixture would change every other
integration test's assumption about default Neo4j config.
"""
from __future__ import annotations

import threading

import pytest
from docker.errors import DockerException
from neo4j import GraphDatabase

from tests.stress import lock_wait


@pytest.fixture
def logged_apoc_neo4j():
    from testcontainers.community.neo4j import Neo4jContainer

    container = lock_wait.with_apoc(lock_wait.with_verbose_query_log(
        Neo4jContainer("neo4j:5.21-enterprise", password="test-password")
        .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
    ))
    try:
        container.start()
    except (DockerException, TimeoutError) as exc:
        pytest.skip(f"Docker is required for this integration test: {exc}")
    driver = GraphDatabase.driver(container.get_connection_url(), auth=(container.username, container.password))
    try:
        driver.verify_connectivity()
        yield container, driver
    finally:
        driver.close()
        container.stop()


@pytest.mark.integration
def test_assert_zero_lock_wait_passes_with_no_contention(logged_apoc_neo4j) -> None:
    _container, driver = logged_apoc_neo4j
    with driver.session() as session:
        session.run("MERGE (n:Probe {id: 'uncontended'}) RETURN n").consume()
    lock_wait.assert_zero_lock_wait(logged_apoc_neo4j[0])


@pytest.mark.integration
def test_total_lock_wait_ms_detects_a_genuine_induced_wait(logged_apoc_neo4j) -> None:
    """Two concurrent transactions on the same node must produce a real, nonzero wait.

    This is the non-negotiable AC from Residual Risk 3 in
    plan/v6/slice-4/stories.md: the helper must be proven to detect a REAL
    wait, not just always return zero (a wrong regex or a misconfigured
    query log would make every "zero lock wait" assertion in this slice
    vacuously true).
    """
    container, driver = logged_apoc_neo4j
    with driver.session() as session:
        session.run("MERGE (n:Probe {id: 'contended'}) RETURN n").consume()

    holding = threading.Event()
    holder_error: list[BaseException] = []

    def hold_and_sleep() -> None:
        try:
            with driver.session() as session:
                def work(tx):
                    tx.run("MATCH (n:Probe {id: 'contended'}) SET n.x = 1 RETURN n").consume()
                    holding.set()  # the SET above has been sent and consumed: the lock is held
                    tx.run("CALL apoc.util.sleep(2500)").consume()
                session.execute_write(work)
        except BaseException as exc:  # noqa: BLE001 - re-raised on the main thread below
            holder_error.append(exc)

    holder = threading.Thread(target=hold_and_sleep)
    holder.start()
    try:
        assert holding.wait(10), "the holder thread never reported acquiring its lock (it may have died)"
        with driver.session() as session:
            session.run("MATCH (n:Probe {id: 'contended'}) SET n.y = 1 RETURN n").consume()
    finally:
        holder.join(10)
    assert not holder.is_alive(), "the holder thread did not finish within the join timeout"
    if holder_error:
        raise holder_error[0]

    total = lock_wait.total_lock_wait_ms(container)
    assert total > 0, "expected a real induced lock wait, measured 0 ms -- the helper cannot be trusted"
    first = lock_wait.any_lock_wait(container)
    assert first is not None and first > 0
    with pytest.raises(AssertionError):
        lock_wait.assert_zero_lock_wait(container)
