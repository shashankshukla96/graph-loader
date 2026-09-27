"""Explicit fixtures for integration tests that require isolated containers."""
from __future__ import annotations

from collections.abc import Iterator

import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from docker.errors import DockerException
from neo4j import Driver
from testcontainers.community.neo4j import Neo4jContainer
from testcontainers.community.kafka import KafkaContainer
import uuid
import time


@pytest.fixture(scope="session")
def neo4j_connection() -> Iterator[tuple[str, str, str]]:
    """Provide isolated Neo4j connection settings, or skip without Docker."""
    container = (
        Neo4jContainer("neo4j:5.21-enterprise", password="test-password")
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


@pytest.fixture
def clean_neo4j(neo4j_driver: Driver) -> Iterator[None]:
    """Remove graph data before and after one explicitly requesting test."""
    with neo4j_driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n").consume()
    try:
        yield
    finally:
        with neo4j_driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n").consume()


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
