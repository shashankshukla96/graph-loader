"""v7-S5: bulk topic precedence as sequential per-topic sub-stages (classic and lease bulk)."""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from threading import Event
from unittest.mock import MagicMock, patch

import pytest
import yaml

from src import cli
from src.models.schema import GraphSchema

ROOT = Path(__file__).resolve().parents[1]
TOPICS = ["works-at-a", "works-at-b", "works-at-c"]
RUN_ID = re.compile(r"^[A-Za-z0-9._-]{1,128}$")


def _schema(topics=TOPICS, rule="precedence", dead_letter=None) -> GraphSchema:
    data = yaml.safe_load((ROOT / "config" / "graph_schema.yaml").read_text())
    data.get("loading", {}).pop("mode", None)
    for edge in data["edges"]:
        if edge["type"] == "WORKS_AT":
            edge.pop("topic")
            edge.pop("dead_letter", None)
            edge["topics"] = list(topics)
            edge["topic_conflict"] = rule
            edge["replicas"] = 3
            edge.pop("order_by", None)
            if dead_letter:
                edge["dead_letter"] = dead_letter
    return GraphSchema.model_validate(data)


def _edge(**kw):
    return next(e for e in _schema(**kw).edges if e.type == "WORKS_AT")


def _args(**extra):
    return argparse.Namespace(config="c.yaml", network="n", bulk_timeout_seconds=1, **extra)


def _docker(events):
    docker = MagicMock()

    def launch(**kwargs):
        events.append(("launch", tuple(kwargs["topics"]), kwargs["run_id"], kwargs["replicas"]))
        return [MagicMock() for _ in range(kwargs["replicas"])]

    docker.run_edge_loader.side_effect = launch
    return docker


def _monitor_cls(events, fail_on=None):
    def build(edges, run_id, expected, tracked, **kwargs):
        topics = tuple(edges[0].topics)
        monitor = MagicMock()
        monitor.wait_for_assignment_coverage.side_effect = lambda *_: events.append(("coverage", topics))
        def boundary(*_):
            events.append(("boundary", topics))
            return {(edges[0].type, t, p): high for (t, p), (_low, high) in MARKS.items() if t in topics}

        monitor.capture_boundary.side_effect = boundary

        def complete(*_):
            events.append(("complete", topics))
            if topics == (fail_on,):
                raise RuntimeError("late record")

        monitor.wait_for_completion.side_effect = complete
        monitor.verify_zero_lag.side_effect = lambda *_: events.append(("zero-lag", topics))
        return monitor

    return MagicMock(side_effect=build)


MARKS = {(t, p): (0, 10) for t in TOPICS for p in (0, 1)}


def _run(edge, docker, events, marks=None, commits=None, **kw):
    """``marks``: a callable returning the watermarks (called once per read), default constant."""
    reads = marks or (lambda topics: {k: v for k, v in MARKS.items() if k[0] in topics})
    with patch.object(cli, "RelationshipBulkMonitor", kw.pop("monitor", _monitor_cls(events))), \
            patch.object(cli, "_topic_watermarks", side_effect=lambda topics: reads(list(topics))), \
            patch.object(cli, "_group_commits", side_effect=commits or (lambda group, parts: {})), \
            patch.object(cli, "_stop_exact_containers", return_value=[]) as stop, \
            patch.object(cli, "_remove_exact_containers"):
        rc = cli._run_bulk_relationship_edge(edge, docker, _args(), consumer_group_prefix="g", **kw)
    return rc, stop


def test_plan_runs_topics_lowest_rank_first_with_distinct_valid_run_ids() -> None:
    edge = _edge()
    plan = cli._precedence_substage_plan(edge, "run1")
    assert [(rank, topic) for rank, topic, _, _ in plan] == [(0, TOPICS[0]), (1, TOPICS[1]), (2, TOPICS[2])]
    ids = [sub_id for _, _, sub_id, _ in plan]
    assert len(set(ids)) == 3 and all(RUN_ID.match(i) for i in ids)
    for _, topic, _, sub in plan:
        assert sub.topics == [topic] and sub.replicas == edge.replicas == 3
        assert sub.conflict_rule == "single"
    assert edge.topics == TOPICS  # the schema edge is not mutated


def test_plan_follows_the_declared_order_not_the_alphabet() -> None:
    edge = _edge(topics=["z-first", "a-second"])
    assert [t for _, t, _, _ in cli._precedence_substage_plan(edge, "r")] == ["z-first", "a-second"]


def test_substages_are_sequential_each_with_boundary_completion_and_zero_lag() -> None:
    events: list = []
    rc, _ = _run(_edge(), _docker(events), events, run_id="run1")
    assert rc == 0
    launches = [e for e in events if e[0] == "launch"]
    assert [e[1] for e in launches] == [(t,) for t in TOPICS]
    assert all(e[3] == 3 for e in launches)
    # strictly one sub-stage at a time: a sub-stage's zero-lag precedes the next launch
    seq = [(e[0], e[1]) for e in events]
    assert seq == [(k, (t,)) for t in TOPICS for k in ("launch", "coverage", "boundary", "complete", "zero-lag")]
    assert len({e[2] for e in launches}) == 3  # distinct run ids -> distinct consumer groups


def test_a_failed_substage_stops_the_type_and_never_starts_later_topics() -> None:
    events: list = []
    rc, stop = _run(_edge(), _docker(events), events, run_id="run1", monitor=_monitor_cls(events, fail_on=TOPICS[1]))
    assert rc == 1
    assert [e[1] for e in events if e[0] == "launch"] == [(TOPICS[0],), (TOPICS[1],)]
    assert ("zero-lag", (TOPICS[1],)) not in events
    assert stop.call_count >= 1


def test_cancelled_run_launches_nothing() -> None:
    cancelled = Event()
    cancelled.set()
    docker = _docker([])
    rc, _ = _run(_edge(), docker, [], run_id="run1", cancelled=cancelled)
    assert rc == 1
    docker.run_edge_loader.assert_not_called()


def test_non_precedence_multi_topic_types_keep_one_parallel_stage() -> None:
    events: list = []
    rc, _ = _run(_edge(rule="arrival"), _docker(events), events, run_id="run1")
    assert rc == 0
    launches = [e for e in events if e[0] == "launch"]
    assert len(launches) == 1 and launches[0][1] == tuple(TOPICS) and launches[0][2] == "run1"


def test_substage_timing_rows_are_written_per_topic(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("GRAPH_LOADER_TIMING_DIR", str(tmp_path))
    events: list = []
    _run(_edge(), _docker(events), events, run_id="r")
    assert sorted(p.name for p in tmp_path.iterdir()) == [f"substage-WORKS_AT.t{i}.json" for i in range(3)]


def test_append_to_a_finished_topic_fails_the_type() -> None:
    events: list = []
    reads = {"n": 0}

    def marks(topics):
        reads["n"] += 1
        grown = reads["n"] > 2  # snapshot, then the check after sub-stage a; later reads see the append
        return {k: ((0, v[1] + 1) if grown and k[0] == TOPICS[0] else v) for k, v in MARKS.items() if k[0] in topics}

    rc, _ = _run(_edge(), _docker(events), events, marks=marks, run_id="run1")
    assert rc == 1
    assert [e[1] for e in events if e[0] == "launch"] == [(t,) for t in TOPICS[:2]]  # c never started


def test_append_after_the_last_substage_still_fails() -> None:
    events: list = []
    reads = {"n": 0}

    def marks(topics):
        reads["n"] += 1
        grown = reads["n"] > 2  # snapshot, check after a; only the final check (after the last sub-stage) sees it
        return {k: ((0, v[1] + 5) if grown and k[0] == TOPICS[0] else v) for k, v in MARKS.items() if k[0] in topics}

    rc, _ = _run(_edge(topics=TOPICS[:2]), _docker(events), events, marks=marks, run_id="run1")
    assert rc == 1 and len([e for e in events if e[0] == "launch"]) == 2


def test_boundary_that_differs_from_the_prerun_snapshot_fails() -> None:
    events: list = []
    seen = {"n": 0}

    def marks(topics):
        seen["n"] += 1
        # the snapshot is taken first (10); the monitor's boundary (from MARKS, 10) then differs from a bumped snapshot
        return {k: ((0, 9) if k[0] == TOPICS[0] else v) for k, v in MARKS.items() if k[0] in topics}

    rc, _ = _run(_edge(), _docker(events), events, marks=marks, run_id="run1")
    assert rc == 1 and len([e for e in events if e[0] == "launch"]) == 1


def test_rerun_with_a_reused_run_id_is_refused_when_a_higher_topic_already_committed() -> None:
    events: list = []

    def commits(group, parts):
        # topic b's group has commits, topic a's group has none (a still has work)
        return {k: 10 for k in parts} if "topic.1" in group else {}

    rc, _ = _run(_edge(topics=TOPICS[:2]), _docker(events), events, commits=commits, run_id="run1")
    assert rc == 1 and not [e for e in events if e[0] == "launch"]


def test_operator_stop_between_substages_is_a_warning_not_an_error(caplog) -> None:
    events: list = []
    docker = _docker(events)
    state = {"stop": False}

    def monitor_factory(*a, **k):
        monitor = _monitor_cls(events)(*a, **k)
        monitor.verify_zero_lag.side_effect = lambda *_: state.update(stop=True)
        return monitor

    with patch.object(cli, "_shutdown_requested", side_effect=lambda: state["stop"]):
        rc, _ = _run(_edge(), docker, events, monitor=MagicMock(side_effect=monitor_factory), run_id="run1")
    assert rc == 1
    assert len(docker.run_edge_loader.call_args_list) == 1
    assert "stopped by operator" in caplog.text
    assert not [r for r in caplog.records if r.levelname == "ERROR" and "stage=bulk-substage" in r.getMessage()]


def test_failure_stops_exactly_the_failing_substage_containers() -> None:
    events: list = []
    launched: list = []
    docker = MagicMock()

    def launch(**kwargs):
        containers = [MagicMock(name=f"{kwargs['topics'][0]}-{i}") for i in range(kwargs["replicas"])]
        launched.append((kwargs["topics"][0], containers))
        return containers

    docker.run_edge_loader.side_effect = launch
    rc, stop = _run(_edge(), docker, events, monitor=_monitor_cls(events, fail_on=TOPICS[1]), run_id="run1")
    assert rc == 1
    (_, a_containers), (_, b_containers) = launched
    # a's containers are never touched after b launched; b's failure path stops exactly b's containers
    after_b = [list(call.args[0]) for call in stop.call_args_list if list(call.args[0]) == b_containers]
    assert after_b, "failing sub-stage containers were not stopped"
    seen_b_launch = False
    for call in stop.call_args_list:
        group = list(call.args[0])
        if group == b_containers:
            seen_b_launch = True
        elif seen_b_launch:
            raise AssertionError(f"unexpected stop group {group}")
    assert all(not set(map(id, group)) & set(map(id, b_containers)) for group in [a_containers])


def test_precedence_runs_next_to_another_type_under_the_scheduler() -> None:
    import threading

    schema = _schema()
    works = next(e for e in schema.edges if e.type == "WORKS_AT")
    other = works.model_copy(update={"type": "EMPLOYED_AT", "topics": ["other-topic"], "topic_conflict": None})
    other_launched = threading.Event()
    events: list = []
    docker = MagicMock()

    def launch(**kwargs):
        events.append(("launch", kwargs["edge_type"], tuple(kwargs["topics"])))
        if kwargs["edge_type"] == "EMPLOYED_AT":
            other_launched.set()
        return [MagicMock() for _ in range(kwargs["replicas"])]

    docker.run_edge_loader.side_effect = launch

    def monitor_factory(edges, run_id, expected, tracked, **kw):
        monitor = MagicMock()
        topics = tuple(edges[0].topics)
        monitor.capture_boundary.return_value = {
            (edges[0].type, t, p): 10 for t in topics for p in (0, 1)
        }
        if topics == (TOPICS[0],):  # the first sub-stage waits until the other type is running
            monitor.wait_for_completion.side_effect = lambda *_: other_launched.wait(10) or None
        return monitor

    args = _args(bulk_edge_overlap="shared", max_concurrent_bulk_edges=0)
    with patch.object(cli, "RelationshipBulkMonitor", MagicMock(side_effect=monitor_factory)), \
            patch.object(cli, "_topic_watermarks", side_effect=lambda t: {k: v for k, v in MARKS.items() if k[0] in t}), \
            patch.object(cli, "_group_commits", return_value={}), \
            patch.object(cli, "_stop_exact_containers", return_value=[]), \
            patch.object(cli, "_remove_exact_containers"):
        rc = cli._run_completion_driven_bulk_edges(
            (works, other), docker, args, parent_run_id="parent", consumer_group_prefix="g",
            ready_labels={works.nodes.source, works.nodes.target},
        )
    assert rc == 0
    order = [(e[1], e[2]) for e in events]
    assert order.index(("EMPLOYED_AT", ("other-topic",))) < order.index(("WORKS_AT", (TOPICS[1],)))
    assert [t for et, t in order if et == "WORKS_AT"] == [(t,) for t in TOPICS]


def test_precedence_type_is_weighted_first_under_a_concurrency_cap() -> None:
    schema = _schema()
    works = next(e for e in schema.edges if e.type == "WORKS_AT")
    other = works.model_copy(update={"type": "AAA_FIRST", "topics": ["other-topic"], "topic_conflict": None})
    started: list[str] = []

    def fake(edge, *a, **k):
        started.append(edge.type)
        return 0

    args = _args(bulk_edge_overlap="shared", max_concurrent_bulk_edges=1)
    with patch.object(cli, "_run_bulk_relationship_edge", side_effect=fake):
        rc = cli._run_completion_driven_bulk_edges(
            (other, works), MagicMock(), args, parent_run_id="parent", consumer_group_prefix="g",
            ready_labels={works.nodes.source, works.nodes.target},
        )
    assert rc == 0 and started[0] == "WORKS_AT"


# --- start-time matrix: which (mode, rule) cells run -----------------------------------


@pytest.mark.parametrize("flags", [
    {"edge_leases": True, "edge_plan": None}, {"edge_classic": True, "edge_plan": None}, {"edge_plan": "p"},
])
def test_bulk_precedence_is_accepted_in_every_bulk_mode(flags) -> None:
    cli._require_supported_topic_conflicts(_schema(), argparse.Namespace(mode="bulk", **flags))


def test_stream_precedence_is_still_refused() -> None:
    with pytest.raises(ValueError, match="stream"):
        cli._require_supported_topic_conflicts(_schema(), argparse.Namespace(mode="stream", edge_plan=None))


@pytest.mark.parametrize("flags", [{"edge_leases": True}, {"edge_classic": True}])
def test_precedence_with_dead_letter_is_accepted_in_classic_and_lease(flags) -> None:
    """v7-S4 lifted the S2/S5 refusal: the type has one DLQ for all its topics."""
    schema = _schema(dead_letter={"enabled": True, "route_missing_endpoints_in_bulk": True})
    cli._require_supported_topic_conflicts(schema, argparse.Namespace(mode="bulk", edge_plan=None, **flags))


def test_precedence_dead_letter_is_provisioned_once_type_wide_from_the_parent_edge() -> None:
    schema = _schema(dead_letter={"enabled": True})
    with patch.object(cli, "ensure_dead_letter_topic") as ensure:
        cli._ensure_dead_letter_topics(schema, "kafka:9092")
    edge_calls = [c for c in ensure.call_args_list if c.args[1].startswith("WORKS_AT")]
    assert len(edge_calls) == 1
    assert edge_calls[0].args[1] == "WORKS_AT.dlq"
    assert edge_calls[0].kwargs["source_topics"] == TOPICS


def test_precedence_substages_never_provision_a_per_topic_dlq() -> None:
    """A sub-edge (model_copy(topics=[t])) would resolve `<t>.dlq`; sub-stages must not provision."""
    events: list = []
    edge = _edge(dead_letter={"enabled": True, "route_missing_endpoints_in_bulk": True})
    with patch.object(cli, "ensure_dead_letter_topic") as ensure, \
            patch.object(cli, "ensure_parked_pair_index_topic") as ensure_index:
        docker = _docker(events)
        rc, _ = _run(edge, docker, events, run_id="run1")
    assert rc == 0
    ensure.assert_not_called()
    ensure_index.assert_not_called()
    # every sub-stage container loads the parent edge type (and so its type-wide DLQ) by name
    assert [e[1] for e in events if e[0] == "launch"] == [(t,) for t in TOPICS]
    assert {c.kwargs["edge_type"] for c in docker.run_edge_loader.call_args_list} == {"WORKS_AT"}


def test_sub_stage_edges_keep_the_parents_dlq_parking_lot_and_index_names() -> None:
    """v7-S4 review S6: a one-topic copy must not resolve `<topic>.dlq`."""
    from src.models.schema import (
        edge_dead_letter_topic_name, edge_parked_pair_index_topic_name, edge_parked_topic_name,
    )

    edge = _edge(dead_letter={"enabled": True, "route_missing_endpoints_in_bulk": True})
    for _rank, topic, _run, sub in cli._precedence_substage_plan(edge, "run1"):
        assert sub.topics == [topic]
        assert edge_dead_letter_topic_name(sub) == "WORKS_AT.dlq"
        assert edge_parked_topic_name(sub) == "WORKS_AT.dlq.parked"
        assert edge_parked_pair_index_topic_name(sub) == "WORKS_AT.dlq.parked-pairs"
        assert sub.dead_letter.route_missing_endpoints_in_bulk is True
    no_dlq = _edge()
    assert all(sub.dead_letter is None for *_x, sub in cli._precedence_substage_plan(no_dlq, "r"))
