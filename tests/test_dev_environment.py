"""Smoke tests for the local Neo4j and Kafka development environment.

The stack must already be running (``make dev`` or ``docker compose up -d``).
Run the integration checks with:

    python -m pytest tests/test_dev_environment.py -v -m smoke
"""

import os
import re
import time
import uuid
from pathlib import Path
from typing import Mapping

import pytest
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from neo4j import GraphDatabase


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOTENV_PATH = PROJECT_ROOT / ".env"
KEY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _parse_dotenv(path: Path) -> dict[str, str]:
    """Parse this project's simple dotenv contract without executing its contents."""
    if not path.is_file():
        return {}

    values: dict[str, str] = {}
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            raise ValueError(f"Malformed {path} line {line_number}: expected KEY=value")

        key, raw_value = stripped.split("=", maxsplit=1)
        key = key.strip()
        if not KEY_PATTERN.fullmatch(key):
            raise ValueError(f"Malformed {path} line {line_number}: invalid key {key!r}")

        raw_value = _strip_inline_comment(raw_value.strip())
        if raw_value.startswith(("'", '"')):
            quote = raw_value[0]
            if len(raw_value) < 2 or not raw_value.endswith(quote):
                raise ValueError(f"Malformed {path} line {line_number}: unclosed quoted value")
            value = raw_value[1:-1]
        else:
            value = raw_value
        values[key] = value
    return values


def _strip_inline_comment(raw_value: str) -> str:
    """Strip a whitespace-prefixed comment only when it occurs outside quotes."""
    quote: str | None = None
    for index, character in enumerate(raw_value):
        if character in {"'", '"'}:
            if quote is None:
                quote = character
            elif character == quote:
                quote = None
        elif character == "#" and quote is None and index > 0 and raw_value[index - 1].isspace():
            return raw_value[:index].rstrip()
    return raw_value


def _setting(name: str, dotenv_values: Mapping[str, str], default: str) -> str:
    """Return a process environment value, then dotenv value, then default."""
    environment_value = os.environ.get(name)
    if environment_value is not None:
        return environment_value
    return dotenv_values.get(name, default)


DOTENV_VALUES = _parse_dotenv(DOTENV_PATH)
NEO4J_URI = _setting("NEO4J_URI", DOTENV_VALUES, "bolt://localhost:7687")
NEO4J_USERNAME = _setting("NEO4J_USERNAME", DOTENV_VALUES, "neo4j")
NEO4J_PASSWORD = _setting("NEO4J_PASSWORD", DOTENV_VALUES, "changeme")
NEO4J_DATABASE = _setting("NEO4J_DATABASE", DOTENV_VALUES, "neo4j")
KAFKA_BOOTSTRAP = _setting("KAFKA_BOOTSTRAP_SERVERS", DOTENV_VALUES, "localhost:9092")


def _delete_topic_and_wait(admin: AdminClient, topic_name: str) -> None:
    """Delete a test topic and wait at most ten seconds for metadata to drop it."""
    metadata = admin.list_topics(timeout=10)
    if topic_name not in metadata.topics:
        return

    futures = admin.delete_topics([topic_name])
    futures[topic_name].result(timeout=10)

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        metadata = admin.list_topics(timeout=5)
        if topic_name not in metadata.topics:
            return
        time.sleep(0.5)

    raise AssertionError(f"Kafka did not delete smoke-test topic {topic_name!r} within 10 seconds")


def test_process_environment_overrides_dotenv(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Explicit environment settings win, and quotes can be followed by comments."""
    monkeypatch.setenv("NEO4J_URI", "bolt://override:7687")
    assert _setting("NEO4J_URI", {"NEO4J_URI": "bolt://dotenv:7687"}, "bolt://default:7687") == "bolt://override:7687"
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text('NEO4J_PASSWORD="changeme" # local password\n', encoding="utf-8")
    assert _parse_dotenv(dotenv_path)["NEO4J_PASSWORD"] == "changeme"


@pytest.mark.smoke
def test_neo4j_is_reachable() -> None:
    """Verify that Neo4j accepts Bolt connections and Cypher queries."""
    try:
        with GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD)) as driver:
            with driver.session(database=NEO4J_DATABASE) as session:
                assert session.run("RETURN 1 AS n").single()["n"] == 1
    except Exception as error:
        pytest.fail(f"Neo4j is not reachable via {NEO4J_URI}. Run `make dev`. Original error: {error}")


@pytest.mark.smoke
def test_apoc_is_installed() -> None:
    """Verify APOC is available for the planned apoc.lock.nodes() strategy."""
    try:
        with GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD)) as driver:
            with driver.session(database=NEO4J_DATABASE) as session:
                version = session.run("RETURN apoc.version() AS version").single()["version"]
                assert version, "APOC returned an empty version"
    except Exception as error:
        pytest.fail(
            "APOC is not installed or Neo4j is not reachable. "
            f"Run `make dev` and check NEO4J_PLUGINS. Original error: {error}"
        )


@pytest.mark.smoke
def test_kafka_is_reachable() -> None:
    """Verify that Kafka returns broker metadata to a host-side client."""
    try:
        metadata = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP}).list_topics(timeout=10)
        assert metadata.brokers, "Kafka returned no brokers"
    except KafkaException as error:
        pytest.fail(f"Kafka is not reachable at {KAFKA_BOOTSTRAP}. Run `make dev`. Original error: {error}")


@pytest.mark.smoke
def test_kafka_can_create_and_delete_topic() -> None:
    """Verify topic creation and bounded cleanup for the loader's Kafka broker."""
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    topic_name = f"graph-loader-smoke-{uuid.uuid4().hex}"

    try:
        futures = admin.create_topics([NewTopic(topic_name, num_partitions=1, replication_factor=1)])
        futures[topic_name].result(timeout=10)

        metadata = admin.list_topics(timeout=10)
        assert topic_name in metadata.topics, "Kafka did not report the newly created smoke-test topic"
    except KafkaException as error:
        pytest.fail(f"Kafka topic operations failed. Run `make dev`. Original error: {error}")
    finally:
        try:
            _delete_topic_and_wait(admin, topic_name)
        except (AssertionError, KafkaException) as error:
            pytest.fail(f"Kafka smoke-test topic cleanup failed for {topic_name!r}: {error}")
