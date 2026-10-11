"""v7-S7 story 7.5: orchestration treats a multi-pair type as touching every label of every pair."""
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
from threading import Event, Thread
from unittest.mock import MagicMock, patch

import pytest

from multi_pair_support import BACKFILL, COMMENTS, POSTS, likes, likes_schema, likes_schema_dict, nodes
from src import cli
from src.models.schema import GraphSchema, MultiPairEdgeError
from src.orchestrator.coordination import ClockPublishError, GlobalBatchClock
from src.orchestrator.dependency_manager import build_conflict_families, build_relationship_conflict_plan
from src.orchestrator.fleet_contract import select_shared_fleet_edges
from src.orchestrator.isolation import build_relationship_isolation_plan
from test_precedence_substages import _args, _docker, _monitor_cls

ROOT = Path(__file__).resolve().parents[1]


def _schema_with(*extra_edges: dict, likes_kwargs: dict | None = None, extra_nodes: list | None = None) -> GraphSchema:
    data = likes_schema_dict(**(likes_kwargs or {}))
    data["nodes"].extend(extra_nodes or [])
    data["edges"].extend(extra_edges)
    return GraphSchema.model_validate(data)


TAG = {"label": "Tag", "topic": "tags", "key_property": "tag_id",
       "properties": {"tag_id": {"type": "integer", "required": True}}}
WROTE = {"type": "WROTE", "topic": "wrote", "nodes": {"source": "Person", "target": "Post"},
         "source_key_property": "person_id", "target_key_property": "post_id", "properties": {"at": {"type": "date"}}}
TAGGED = {"type": "TAGGED", "topic": "tagged", "nodes": {"source": "Comment", "target": "Tag"},
          "source_key_property": "comment_id", "target_key_property": "tag_id", "properties": {"at": {"type": "date"}}}
SELF_PAIR = {"name": "likes-persons", "nodes": {"source": "Person", "target": "Person"},
             "source_key_property": "person_id", "target_key_property": "person_id"}


def _likes_with_self_pair() -> GraphSchema:
    data = likes_schema_dict()
    data["edges"][0]["topics"].append(dict(SELF_PAIR))
    return GraphSchema.model_validate(data)


# ── AC 1 ─────────────────────────────────────────────────────────────────────


def test_a_three_label_type_conflicts_with_every_type_touching_one_of_its_labels() -> None:
    schema = _schema_with(WROTE, TAGGED, extra_nodes=[TAG])
    plan = build_relationship_conflict_plan(schema.edges)
    assert plan.conflicts["LIKES"] == frozenset({"WROTE", "TAGGED"})
    assert plan.conflicts["TAGGED"] == frozenset({"LIKES"}) and plan.conflicts["WROTE"] == frozenset({"LIKES"})
    assert [stage.edge_types for stage in plan.stages] == [("LIKES",), ("TAGGED", "WROTE")]
    assert build_relationship_conflict_plan(list(reversed(schema.edges))).stages == plan.stages


# ── AC 2 ─────────────────────────────────────────────────────────────────────


@patch("src.cli._run_bulk_relationship_edge")
def test_the_scheduler_waits_for_every_label_of_every_pair(run_edge) -> None:
    edge = likes()
    started = Event()
    run_edge.side_effect = lambda *_a, **_k: started.set() or 0
    ready = {"Person", "Post"}
    condition = cli.Condition()
    result: list = []
    thread = Thread(target=lambda: result.append(cli._run_completion_driven_bulk_edges(
        (edge,), MagicMock(), argparse.Namespace(config="c.yaml", network="n", bulk_timeout_seconds=1),
        parent_run_id="bulk-run", consumer_group_prefix="loader", ready_labels=ready, ready_condition=condition,
    )))
    thread.start()
    try:
        assert not started.wait(0.5)  # Comment is not loaded yet
        with condition:
            ready.add("Comment")
            condition.notify_all()
        assert started.wait(5)
    finally:
        thread.join(5)
    assert result == [0]


# ── AC 3 ─────────────────────────────────────────────────────────────────────


def test_a_self_referencing_pair_isolates_the_type_in_classic_only() -> None:
    schema = _likes_with_self_pair()
    edge = schema.edges[0]
    assert edge.has_self_reference and cli._has_self_reference(edge) is True
    plan = build_relationship_isolation_plan(schema.edges)
    assert plan.shared_edge_types == ()
    [phase] = plan.isolated_phases
    assert (phase.edge_type, phase.label) == ("LIKES", "Person")
    # The opt-in shared overlap admits it like any python_apoc self-reference (the silent getattr is fixed).
    shared = argparse.Namespace(mode="bulk", bulk_edge_overlap="shared")
    assert cli._co_scheduled_self_references(schema.edges, shared) == frozenset({"LIKES"})
    assert cli._co_scheduled_self_references(schema.edges, argparse.Namespace(mode="bulk")) == frozenset()
    plain = likes_schema()
    assert build_relationship_isolation_plan(plain.edges).shared_edge_types == ("LIKES",)


# ── AC 4 ─────────────────────────────────────────────────────────────────────


def test_stream_clock_rotation_refuses_a_multi_pair_type_before_any_side_effect(caplog) -> None:
    args = argparse.Namespace(mode="stream", config="c.yaml", network="n", skip_image_build=True,
                              stream_clock_rotation=True)
    docker = MagicMock(side_effect=AssertionError("no container may be created"))
    with patch.object(cli, "load_schema", return_value=likes_schema()), patch.object(cli, "DockerService", docker), \
            patch.object(cli, "_ensure_dead_letter_topics", side_effect=AssertionError("no topic")):
        assert cli.handle_start(args) == 1
    docker.assert_not_called()
    assert "edge type(s) ['LIKES'] bind several label pairs; --stream-clock-rotation (deprecated)" in caplog.text


def test_deprecated_phase4_paths_refuse_a_multi_pair_type() -> None:
    edge = likes()
    with pytest.raises(MultiPairEdgeError):
        build_conflict_families([edge])
    with pytest.raises(ClockPublishError, match="several label pairs"):
        GlobalBatchClock(run_id="r", edges=[edge], config=MagicMock(), producer=MagicMock(), health_probe=MagicMock(),
                         wall_clock_ms=lambda: 0, sleeper=lambda _s: None)
    with pytest.raises(ValueError, match="several label pairs"):
        select_shared_fleet_edges([edge], ("LIKES",))
    assert cli._run_isolated_relationship_stream_phase(edge, MagicMock(), MagicMock(), run_id="r", phase_index=0,
                                                       consumer_group_prefix="g", parent_run_id="p") == 1


# ── AC 5 ─────────────────────────────────────────────────────────────────────


def _precedence_likes(**edge_kwargs):
    return likes(order_by=False, topic_conflict="precedence", **edge_kwargs)


def test_precedence_substages_of_a_multi_pair_type_are_single_pair_with_the_types_dlq() -> None:
    edge = _precedence_likes(dead_letter={"enabled": True})
    plan = cli._precedence_substage_plan(edge, "run1")
    assert [(rank, topic) for rank, topic, _, _ in plan] == [(0, POSTS), (1, BACKFILL), (2, COMMENTS)]
    for _rank, topic, _run_id, sub in plan:
        assert sub.is_multi_pair is False and sub.topics == [topic]
        assert sub.single_pair.labels == edge.pair_for_topic(topic).labels
        assert cli.edge_dead_letter_topic_name(sub) == "LIKES.dlq"
        assert cli.edge_parked_pair_index_topic_name(sub) == "LIKES.dlq.parked-pairs"


def test_precedence_substages_launch_one_loader_per_topic_in_rank_order() -> None:
    edge = _precedence_likes()
    events: list = []
    marks = {(t, p): (0, 10) for t in edge.topics for p in (0, 1)}
    with patch.object(cli, "RelationshipBulkMonitor", _monitor_cls(events)), \
            patch.object(cli, "_topic_watermarks", side_effect=lambda topics: {k: v for k, v in marks.items()
                                                                               if k[0] in topics}), \
            patch.object(cli, "_group_commits", side_effect=lambda group, parts: {}), \
            patch.object(cli, "_stop_exact_containers", return_value=[]), \
            patch.object(cli, "_remove_exact_containers"), \
            patch("test_precedence_substages.MARKS", marks):
        rc = cli._run_bulk_relationship_edge(edge, _docker(events), _args(), consumer_group_prefix="g", run_id="run1")
    assert rc == 0
    assert [e[1] for e in events if e[0] == "launch"] == [(POSTS,), (BACKFILL,), (COMMENTS,)]


def test_a_substage_is_checked_against_its_parents_self_reference() -> None:
    data = likes_schema_dict(order_by=False, topic_conflict="precedence")
    data["edges"][0]["topics"].append(dict(SELF_PAIR))
    edge = GraphSchema.model_validate(data).edges[0]
    sub = edge.topic_view(POSTS)
    assert cli._has_self_reference(sub) is False
    with patch.object(cli, "RelationshipBulkMonitor", side_effect=RuntimeError("stop here")):
        # isolated phase 0: the parent is self-referencing, so the check passes and the run reaches the monitor
        docker = MagicMock()
        docker.run_edge_loader.return_value = [MagicMock()]
        with patch.object(cli, "_stop_exact_containers", return_value=[]), patch.object(cli, "_remove_exact_containers"):
            cli._run_bulk_relationship_edge(sub, docker, _args(), run_id="r1", consumer_group_prefix="g",
                                            phase_index=0, parent_edge=edge)
        docker.run_edge_loader.assert_called_once()
        docker.reset_mock()
        assert cli._run_bulk_relationship_edge(sub, docker, _args(), run_id="r1", consumer_group_prefix="g",
                                               phase_index=0) == 1
        docker.run_edge_loader.assert_not_called()


# ── AC 6 ─────────────────────────────────────────────────────────────────────


def test_the_missing_loader_check_and_ready_labels_name_comment() -> None:
    edge = likes()
    assert cli._endpoint_labels_of(edge) == frozenset({"Person", "Post", "Comment"})
    data = likes_schema_dict()
    data["nodes"] = [n for n in nodes() if n["label"] != "Comment"]
    with pytest.raises(Exception, match="Comment"):
        GraphSchema.model_validate(data)  # a schema cannot even declare the type without the Comment node


# ── AC 7 ─────────────────────────────────────────────────────────────────────


def test_the_slice_does_not_touch_the_bulk_monitor_or_docker_service() -> None:
    # The v7-S7 slice is 8748274..239deb4 (end of feat/v7-s7-multi-pair). Later work may change these
    # files (the F1 launch-phase stop hold lives in docker_service), so the guard pins the slice's range.
    changed = subprocess.run(
        ["git", "diff", "--name-only", "8748274", "239deb4", "--",
         "src/orchestrator/relationship_bulk_monitor.py", "src/orchestrator/docker_service.py"], cwd=ROOT, capture_output=True, text=True,
    )
    if changed.returncode != 0:
        pytest.skip("not a git checkout")
    assert changed.stdout.strip() == ""
