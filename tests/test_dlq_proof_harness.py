"""Regressions for ownership and coordination in the isolated E00 DLQ proof."""
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock
import subprocess

import pytest

from tests.integration import test_multi_topic_relationship as proof
from neo4j_graph_loader.models.schema import GraphSchema


def test_dlq_fleet_uses_its_schema_labels_for_shared_leases(tmp_path):
    schema = GraphSchema.model_validate(proof._dlq_schema_dict("unit"))
    case = SimpleNamespace(schema=schema, tmp_path=tmp_path)
    fleet = proof.DlqFleet(case, topics=list(schema.edges[0].topics), edge=schema.edges[0],
                           mode="stream", lease=True, replicas=3, static=False,
                           group="unit", run_id="unit", lease_address="localhost:1234")
    assert set(fleet._environ()[proof.LEASE_KEYED_LABELS_ENV].split(",")) == {"DlqPerson", "DlqCompany"}


def test_dlq_cleanup_attempts_every_owner_after_fleet_failure(monkeypatch):
    child = Mock()
    child.poll.return_value = None
    fleet = Mock()
    fleet.stop.side_effect = AssertionError("failed replica")
    server, log = MagicMock(), Mock()
    deleted = Mock()
    monkeypatch.setattr(proof.std, "delete_topics", deleted)
    with pytest.raises(AssertionError, match="failed replica"):
        proof._cleanup_dlq_case(child, fleet, server, "owned-bootstrap", ["owned-topic"], log)
    child.wait.assert_called_once_with(timeout=60)
    server.__exit__.assert_called_once_with(None, None, None)
    deleted.assert_called_once_with("owned-bootstrap", ["owned-topic"])
    log.close.assert_called_once()


def test_dlq_cleanup_reaps_killed_retry_child(monkeypatch):
    child = Mock()
    child.poll.return_value = None
    child.wait.side_effect = [subprocess.TimeoutExpired("retry", 60), 0]
    monkeypatch.setattr(proof.std, "delete_topics", Mock())
    proof._cleanup_dlq_case(child, None, None, "owned-bootstrap", [], None)
    child.kill.assert_called_once()
    assert child.wait.call_args_list[-1].kwargs == {"timeout": 10}


def test_lease_server_start_failure_stops_its_loop(monkeypatch):
    class FailedServer:
        def __init__(self, **kwargs):
            pass

        async def start(self, *args):
            raise RuntimeError("start failed")

        async def close(self):
            pass

    monkeypatch.setattr(proof, "BucketLeaseServer", FailedServer)
    manager = proof.LeaseServer()
    try:
        with pytest.raises(RuntimeError, match="start failed"):
            manager.__enter__()
        assert not manager.thread.is_alive()
        assert manager.loop.is_closed()
    finally:
        # Keep the negative control from leaking even against the broken code.
        if manager.thread.is_alive():
            manager.loop.call_soon_threadsafe(manager.loop.stop)
            manager.thread.join(10)
        if not manager.loop.is_closed():
            manager.loop.close()


def test_neo4j_fixture_stops_owned_container_when_driver_close_fails(monkeypatch):
    container, driver = MagicMock(), Mock()
    container.with_env.return_value = container
    driver.close.side_effect = RuntimeError("close failed")
    monkeypatch.setattr("docker.from_env", Mock())
    monkeypatch.setattr("testcontainers.community.neo4j.Neo4jContainer", lambda *args, **kwargs: container)
    monkeypatch.setattr(proof.GraphDatabase, "driver", lambda *args, **kwargs: driver)
    fixture = proof.neo4j_logged.__wrapped__()
    next(fixture)
    with pytest.raises(RuntimeError, match="close failed"):
        next(fixture)
    container.stop.assert_called_once()
