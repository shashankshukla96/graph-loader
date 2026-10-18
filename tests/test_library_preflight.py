"""Library lifecycle failures must not affect another run or the caller."""
from pathlib import Path
import subprocess
import sys

import pytest

from neo4j_graph_loader.loader.edge_plan import EdgePlanError, plan_failure_marker, plan_progress_path
from neo4j_graph_loader.loader.edge_planner import KafkaEdgePlanner, PlanSettings, plan_and_publish_edges, producer_edge_planner
from neo4j_graph_loader.orchestrator.planned_edges import wait_for_plan
from neo4j_graph_loader.utils.schema_loader import SchemaLoadError, load_schema
from test_edge_planner import FakeCluster, FakeProducer, envelope, schema_with


def _planner(tmp_path, cluster, *, rows=(), output="plan.json"):
    return producer_edge_planner(
        schema_with("VISITED"), {"VISITED": rows},
        PlanSettings(topic="chunks", partitions=1, memory_mb=1, hub_degree=0), "unused",
        plan_id="same-id", context="inline", plan_path=tmp_path / output,
        producer_factory=lambda: FakeProducer(cluster),
    )


def test_same_id_planners_own_separate_spill_files(tmp_path):
    cluster = FakeCluster()
    first, second = _planner(tmp_path, cluster), _planner(tmp_path, cluster, output="other.json")
    try:
        first.start()
        sentinel = first.spill_dir / "active-file"
        sentinel.write_text("first planner")
        second.start()
        assert first.spill_dir != second.spill_dir
        second.close()
        assert sentinel.read_text() == "first planner"
    finally:
        first.close()
        second.close()
    assert not list(tmp_path.glob(".edge-plan-spill-*"))


def test_overlapping_output_is_rejected_without_touching_its_owner(tmp_path):
    cluster = FakeCluster()
    first, second = _planner(tmp_path, cluster), _planner(tmp_path, cluster)
    try:
        first.start()
        progress = plan_progress_path(first.plan_path)
        assert progress.exists()
        with pytest.raises(EdgePlanError, match="already in use"):
            second.finish(first.plan_path)
        assert progress.exists()
        assert not plan_failure_marker(first.plan_path).exists()
        assert first._plan_lock_path.exists()
    finally:
        first.close()
        second.close()
    assert not list(tmp_path.glob("*.lock"))


def test_successful_retry_replaces_failed_output_state(tmp_path):
    cluster = FakeCluster()
    cluster.topic("chunks", 1)
    path = tmp_path / "plan.json"
    with pytest.raises(RuntimeError, match="invalid VISITED"):
        _planner(tmp_path, cluster, rows=[{"invalid": True}]).finish(path)
    assert plan_failure_marker(path).exists()
    result = _planner(tmp_path, cluster, rows=[envelope("VISITED", 1, "City A", times=1)]).finish(path)
    wait_for_plan(path, 0.1)
    assert result.plan.total_rows == 1
    assert not plan_failure_marker(path).exists()
    assert not list(tmp_path.glob("*.lock"))


def test_new_attempt_hides_old_plan_until_replacement_is_ready(tmp_path):
    cluster = FakeCluster()
    path = tmp_path / "plan.json"
    path.write_text("old plan")
    planner = _planner(tmp_path, cluster)
    try:
        planner.start()
        assert not path.exists()
    finally:
        planner.close()


def test_start_failure_releases_output_and_spill_files(tmp_path, monkeypatch):
    cluster = FakeCluster()
    planner = _planner(tmp_path, cluster)
    from neo4j_graph_loader.loader import edge_planner
    monkeypatch.setattr(edge_planner, "_InlineWorker", lambda *_: (_ for _ in ()).throw(RuntimeError("startup")))
    with pytest.raises(RuntimeError, match="startup"):
        planner.start()
    assert not list(tmp_path.glob("*.lock"))
    assert not list(tmp_path.glob(".edge-plan-spill-*"))


@pytest.mark.parametrize("invalid", ["settings", "source"])
def test_invalid_inputs_do_not_create_a_topic(tmp_path, invalid):
    cluster = FakeCluster()
    with pytest.raises((ValueError, EdgePlanError)):
        plan_and_publish_edges(
            schema_with("VISITED"), {"UNKNOWN": []} if invalid == "source" else {},
            settings=PlanSettings(topic="chunks", buckets=0 if invalid == "settings" else 2),
            bootstrap_servers="unused", plan_path=tmp_path / "plan.json", admin_factory=cluster.admin,
        )
    assert cluster.created == []
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("create_topic", [True, False])
def test_failed_producer_call_deletes_only_its_own_topic(tmp_path, create_topic):
    cluster = FakeCluster()
    with pytest.raises(RuntimeError, match="invalid VISITED"):
        plan_and_publish_edges(
            schema_with("VISITED"), {"VISITED": [{"invalid": True}]},
            settings=PlanSettings(topic="chunks", memory_mb=1), bootstrap_servers="unused",
            plan_path=tmp_path / "plan.json", admin_factory=cluster.admin,
            producer_factory=lambda: FakeProducer(cluster), create_topic=create_topic,
        )
    assert cluster.deleted == (["chunks"] if create_topic else [])
    assert not list(tmp_path.glob("*.lock"))


def test_schema_decoding_error_uses_public_exception(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_bytes(b"\xff\xfe")
    with pytest.raises(SchemaLoadError) as error:
        load_schema(path)
    assert isinstance(error.value.__cause__, UnicodeDecodeError)


def test_schema_read_error_uses_public_exception(tmp_path, monkeypatch):
    path = tmp_path / "schema.yaml"
    path.touch()
    def denied(*args, **kwargs):
        raise PermissionError("denied")
    monkeypatch.setattr(Path, "open", denied)
    with pytest.raises(SchemaLoadError) as error:
        load_schema(path)
    assert isinstance(error.value.__cause__, PermissionError)


def test_public_imports_do_not_configure_logging():
    subprocess.run([sys.executable, "-c", """
import logging
root = logging.getLogger()
before = (root.level, list(root.handlers))
import neo4j_graph_loader.cli
import neo4j_graph_loader.loader.edge_planner
import neo4j_graph_loader.utils.schema_loader
assert (root.level, root.handlers) == before
"""], check=True, timeout=30)


def test_arrival_fixture_covers_shared_pairs_and_last_arrival():
    from tests.fixtures.multi_topic_arrival import arrival_dataset
    b, a, expected = arrival_dataset()
    assert len(a) == len(b) == 1500
    shared = {(p, c) for _, p, c, *_ in a} & {(p, c) for _, p, c, *_ in b}
    assert len(shared) > 100
    for p, c in shared:
        last = next(row for row in reversed(a) if row[1:3] == (p, c))
        assert expected[p, c] == last[3:]


def test_closed_planner_cannot_remove_its_successful_result(tmp_path):
    cluster = FakeCluster()
    cluster.topic("chunks", 1)
    planner = _planner(tmp_path, cluster)
    path = tmp_path / "plan.json"
    planner.finish(path)
    original = path.read_bytes()
    with pytest.raises(EdgePlanError, match="closed"):
        planner.finish(path)
    assert path.read_bytes() == original
    assert not plan_failure_marker(path).exists()


def test_wrong_finish_path_does_not_modify_either_output_or_stop_owner(tmp_path):
    cluster = FakeCluster()
    planner = _planner(tmp_path, cluster)
    other = tmp_path / "other.json"
    other.write_text("another run")
    try:
        planner.start()
        lock = planner._plan_lock_path
        with pytest.raises(EdgePlanError, match="finish path must match"):
            planner.finish(other)
        assert other.read_text() == "another run"
        assert not plan_failure_marker(other).exists()
        assert lock.exists()
        assert not planner._closed
    finally:
        planner.close()


def test_kafka_wrapper_lock_conflict_preserves_owner_failure_evidence(tmp_path):
    cluster = FakeCluster()
    schema = schema_with("VISITED")
    cluster.topic(schema.edges[0].topic, 1)
    path = tmp_path / "plan.json"
    first = _planner(tmp_path, cluster)
    second = KafkaEdgePlanner(
        schema, "unused", PlanSettings(topic="chunks", partitions=1, memory_mb=1),
        plan_id="contender", plan_path=path, context="inline", create_topic=False,
        consumer_factory=cluster.consumer, producer_factory=lambda: FakeProducer(cluster),
    )
    try:
        first.start()
        marker = plan_failure_marker(path)
        marker.write_text("owned evidence")
        with pytest.raises(EdgePlanError, match="already in use"):
            second.finish(path)
        assert marker.read_text() == "owned evidence"
        assert first._plan_lock_path.exists()
        assert plan_progress_path(path).exists()
    finally:
        second.close()
        first.close()


@pytest.mark.parametrize("occupied", [True, False])
def test_kafka_wrapper_early_failure_writes_only_unowned_marker(tmp_path, monkeypatch, occupied):
    from neo4j_graph_loader.loader import edge_planner

    path = tmp_path / "plan.json"
    lock = path.with_name(path.name + ".lock")
    marker = plan_failure_marker(path)
    if occupied:
        lock.write_text("another owner")
        marker.write_text("another failure")
    planner = KafkaEdgePlanner(
        schema_with("VISITED"), "unused", PlanSettings(topic="chunks"),
        plan_id="boundary-failure", plan_path=path,
    )
    def fail(*args, **kwargs):
        raise RuntimeError("boundary unavailable")
    monkeypatch.setattr(edge_planner, "capture_edge_boundary", fail)
    with pytest.raises(RuntimeError, match="boundary unavailable"):
        planner.finish(path)
    assert marker.read_text() == ("another failure" if occupied else "RuntimeError: boundary unavailable\n")
    assert lock.exists() == occupied
