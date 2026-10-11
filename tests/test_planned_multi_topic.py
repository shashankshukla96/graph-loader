"""v7-S3: planned mode (``--edge-plan``) over every topic of a multi-topic relationship type.

Covers the boundary over all topics, the deterministic plan-chain order
``(topic index, first partition)`` that makes ``topic_conflict: precedence``
exact (``SET r += props`` merges; the later-listed topic wins), bounded memory
and per-worker assignment with many topics, hub detection over the merged
topics, the plan's topic record and its validation, and the ``order_by``
conditional SET in the planned writer.
"""
from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.loader import edge_planner
from src.loader.edge_execution import ApocLockedEdgeWriter, build_apoc_locked_upsert_query
from src.loader.edge_loader import MissingRelationshipEndpointError
from src.loader.edge_plan import EdgePlan
from src.loader.edge_planner import (
    EdgePlanInputError, KafkaEdgePlanner, PlannerWorkerState, PlanSettings, capture_edge_boundary, kafka_read_tasks,
)
from src.models.schema import EdgeConfig
from src.orchestrator.planned_edges import PlannedEdgeError, validate_plan_against_schema
from test_edge_planner import EDGES, SOCIAL, FakeCluster, FakeProducer, chains, envelope


def multi_edge(topics, rule="precedence", **extra) -> EdgeConfig:
    # model_copy keeps the key bindings GraphSchema validation attached to the loaded edge.
    return EDGES["FRIEND_OF"].model_copy(update={"topics": list(topics), "topic_conflict": rule, **extra})


def schema_of(edge) -> SimpleNamespace:
    return SimpleNamespace(nodes=SOCIAL.nodes, edges=[edge])


def publish_topic(cluster, topic, rows, partitions=3, edge=None) -> None:
    edge = edge or EDGES["FRIEND_OF"]
    if topic not in cluster.logs:
        cluster.topic(topic, partitions)
    for row in rows:
        source = row["source"][edge.source_key_property]
        cluster.append(topic, sum(json.dumps(source).encode()) % partitions, json.dumps(row).encode())


def friend(source, target, **properties):
    return envelope("FRIEND_OF", source, target, **properties)


def plan_kafka(cluster, edge, tmp_path, *, workers=1, buckets=3, hub=0, chunk_rows=3, name="plan.json",
               memory_mb=0, after_start=None):
    settings = PlanSettings(buckets=buckets, hub_degree=hub, chunk_rows=chunk_rows, topic="chunks", partitions=2,
                            workers=workers, memory_mb=memory_mb, spill_dir=str(tmp_path / "spill"))
    planner = KafkaEdgePlanner(schema_of(edge), "kafka:1", settings, plan_id="run-1", context="inline",
                               consumer_factory=cluster.consumer, producer_factory=lambda: FakeProducer(cluster),
                               admin_factory=cluster.admin)
    planner.start()
    if after_start is not None:
        after_start()
    return planner.finish(tmp_path / name)


def stored(plan, cluster) -> dict:
    """Replay the plan's chains like the planned writers: ``SET r += props`` per record, in chain order."""
    state: dict = {}
    for records in chains(plan, cluster).values():
        for record in records:
            pair = (record["source"]["person_id"], record["target"]["person_id"])
            state.setdefault(pair, {}).update(record["properties"])
    return state


def overlapping_cluster(first="topic-a", second="topic-b", partitions=3):
    """Pairs 0..39 appear in both topics (a: since=1 weight=1.5, b: since=2); 40..49 only in a."""
    cluster = FakeCluster()
    publish_topic(cluster, first, [friend(i % 10, i, since=1, weight=1.5) for i in range(50)], partitions)
    publish_topic(cluster, second, [friend(i % 10, i, since=2) for i in range(40)], partitions)
    return cluster


def expected_state(winner_since: int) -> dict:
    return {(i % 10, i): {"since": winner_since if i < 40 else 1, "weight": 1.5} for i in range(50)}


# -- Story 3.1: boundary over all topics ---------------------------------------------


def test_boundary_covers_every_topic_of_a_multi_topic_type_and_second_topic_changes_abort(tmp_path) -> None:
    edge = multi_edge(["topic-a", "topic-b"])
    cluster = overlapping_cluster()
    boundary = capture_edge_boundary([edge], "kafka:1", consumer_factory=cluster.consumer)
    assert sorted(boundary) == ["topic-a", "topic-b"]
    assert sum(high - low for _, low, high in boundary["topic-b"]) == 40

    def late_record():
        publish_topic(cluster, "topic-b", [friend(99, 100, since=3)])

    with pytest.raises(EdgePlanInputError, match=r"topic-b changed after the bulk boundary"):
        plan_kafka(cluster, edge, tmp_path, after_start=late_record)
    assert not (tmp_path / "plan.json").exists()


def test_single_topic_types_keep_one_topic_in_the_boundary_and_order_zero() -> None:
    cluster = FakeCluster()
    publish_topic(cluster, EDGES["FRIEND_OF"].topic, [friend(1, 2, since=1)])
    boundary = capture_edge_boundary([EDGES["FRIEND_OF"]], "kafka:1", consumer_factory=cluster.consumer)
    assert list(boundary) == [EDGES["FRIEND_OF"].topic]
    (task,) = kafka_read_tasks([EDGES["FRIEND_OF"]], boundary, 1, "kafka:1", "g")
    assert task.topic_index == 0 and task.order == min(p for p, _, _ in task.ranges)


# -- Story 3.2: read tasks and deterministic order -----------------------------------


def test_tasks_carry_topic_index_and_orders_are_distinct_and_topic_major() -> None:
    edge = multi_edge(["t0", "t1", "t2"])
    cluster = FakeCluster()
    for index, topic in enumerate(edge.topics):
        publish_topic(cluster, topic, [friend(i, i + 1, since=index) for i in range(60)], 4)
    boundary = capture_edge_boundary([edge], "kafka:1", consumer_factory=cluster.consumer)
    for workers in (1, 4):
        tasks = kafka_read_tasks([edge], boundary, workers, "kafka:1", "g")
        assert {task.topic: task.topic_index for task in tasks} == {"t0": 0, "t1": 1, "t2": 2}
        orders = [task.order for task in tasks]
        assert len(set(orders)) == len(orders)
        by_topic = {topic: [t.order for t in tasks if t.topic == topic] for topic in edge.topics}
        assert max(by_topic["t0"]) < min(by_topic["t1"]) and max(by_topic["t1"]) < min(by_topic["t2"])
    assert len(kafka_read_tasks([edge], boundary, 4, "kafka:1", "g")) > 3  # topics were split over workers


def test_task_order_rejects_out_of_range_indexes() -> None:
    task = edge_planner.KafkaReadTask(EDGES["FRIEND_OF"], "t", ((0, 0, 1),), "k", "g", topic_index=1 << 12)
    with pytest.raises(edge_planner.EdgePlanError, match="order range"):
        task.order


@pytest.mark.parametrize("workers, spill", [(1, False), (4, False), (1, True), (4, True)])
def test_later_listed_topic_wins_and_reversing_the_list_reverses_the_winner(monkeypatch, tmp_path, workers, spill) -> None:
    if spill:  # 1 MiB workers with tiny shares: every structure spills, cells stream from disk
        monkeypatch.setattr(edge_planner, "_ROWS_SHARE", 0.004)
        monkeypatch.setattr(edge_planner, "_EMIT_SHARE", 0.006)
        monkeypatch.setattr(edge_planner, "_WINDOW_SHARE", 0.0)
    memory = 1 if spill else 0
    cluster = overlapping_cluster()
    forward = plan_kafka(cluster, multi_edge(["topic-a", "topic-b"]), tmp_path, workers=workers,
                         memory_mb=memory, name="forward.json")
    assert stored(forward.plan, cluster) == expected_state(2)
    reverse_cluster = overlapping_cluster()
    reverse = plan_kafka(reverse_cluster, multi_edge(["topic-b", "topic-a"]), tmp_path, workers=workers,
                         memory_mb=memory, name="reverse.json")
    # b (since=2) is now listed first, a (since=1, weight) wins: merge keeps both properties.
    assert stored(reverse.plan, reverse_cluster) == expected_state(1)
    assert forward.plan.types["FRIEND_OF"].rows == 90
    if spill:
        assert any(lane.get("spilled_bytes", 0) > 0 for lane in forward.plan.stats["lanes"])


def test_plans_chains_and_chunk_bytes_do_not_depend_on_worker_count_or_budget(monkeypatch, tmp_path) -> None:
    def chunk_messages(plan, cluster):
        return {chunk.key: cluster.logs[plan.topic][chunk.partition][chunk.offset] for chunk in plan.chunks}

    edge = multi_edge(["topic-a", "topic-b"])
    results = []
    for workers in (1, 2, 4):
        cluster = overlapping_cluster()
        results.append((plan_kafka(cluster, edge, tmp_path, workers=workers, name=f"w{workers}.json").plan, cluster))
    reference_plan, reference_cluster = results[0]
    for plan, cluster in results[1:]:
        assert chains(plan, cluster) == chains(reference_plan, reference_cluster)
        assert plan.resources.names == reference_plan.resources.names
    monkeypatch.setattr(edge_planner, "_ROWS_SHARE", 0.004)
    monkeypatch.setattr(edge_planner, "_EMIT_SHARE", 0.006)
    monkeypatch.setattr(edge_planner, "_WINDOW_SHARE", 0.0)
    # Chunk boundaries follow the task split (worker count), but never the memory budget.
    in_memory_cluster, spilled_cluster = overlapping_cluster(), overlapping_cluster()
    in_memory = plan_kafka(in_memory_cluster, edge, tmp_path, workers=2, name="memory.json")
    spilled = plan_kafka(spilled_cluster, edge, tmp_path, workers=2, memory_mb=1, name="spilled.json")
    assert chains(spilled.plan, spilled_cluster) == chains(reference_plan, reference_cluster)
    assert chunk_messages(spilled.plan, spilled_cluster) == chunk_messages(in_memory.plan, in_memory_cluster)


def test_plan_records_the_topic_lists_in_schema_order(tmp_path) -> None:
    cluster = overlapping_cluster()
    plan = plan_kafka(cluster, multi_edge(["topic-a", "topic-b"]), tmp_path).plan
    assert plan.stats["type_topics"] == {"FRIEND_OF": ["topic-a", "topic-b"]}
    assert sorted(plan.stats["boundary"]) == ["topic-a", "topic-b"]


# -- Story 3.3: bounded memory with many topics, hubs across topics ------------------


def many_topic_cluster(topics=50, partitions=4, rows=40):
    edge = multi_edge([f"t{index:02d}" for index in range(topics)])
    cluster = FakeCluster()
    for index, topic in enumerate(edge.topics):
        publish_topic(cluster, topic, [friend((index * 7 + i) % 97, (i * 3 + index) % 89, since=index)
                                       for i in range(rows)], partitions)
    return edge, cluster


def test_fifty_topics_stay_within_the_memory_budget_and_assign_each_partition_once(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(edge_planner, "_ROWS_SHARE", 0.01)
    monkeypatch.setattr(edge_planner, "_EMIT_SHARE", 0.02)
    monkeypatch.setattr(edge_planner, "_WINDOW_SHARE", 0.0)
    edge, cluster = many_topic_cluster()
    boundary = capture_edge_boundary([edge], "kafka:1", consumer_factory=cluster.consumer)
    assert len(boundary) == 50 and sum(len(ranges) for ranges in boundary.values()) == 200
    tasks = kafka_read_tasks([edge], boundary, 1, "kafka:1", "g")
    # Single worker: one consumer, every non-empty partition assigned exactly once.
    assignments: list[list] = []
    base = cluster.consumer

    def recording_consumer(config):
        consumer = base(config)
        original = consumer.assign
        consumer.assign = lambda tps: (assignments.append([(tp.topic, tp.partition) for tp in tps]), original(tps))[1]
        return consumer

    state = PlannerWorkerState(3, hub_degree=0, memory_bytes=1 << 20, spill_dir=tmp_path / "spill", worker_index=0,
                               consumer_factory=recording_consumer)
    state.scan_lane(tasks)
    nonempty = sorted((topic, partition) for topic, ranges in boundary.items()
                      for partition, low, high in ranges if high > low)
    (assigned,) = assignments
    assert sorted(assigned) == nonempty and len(set(assigned)) == len(assigned)
    assert state.rows == 50 * 40
    largest = 160
    assert 0 < state.store.peak_buffered <= state.store.flush_bytes + state._step + largest
    assert state.store.flushes >= 1
    state.close()


def test_fifty_topic_plan_assigns_partitions_disjointly_over_workers_and_matches_one_worker(tmp_path) -> None:
    edge, cluster = many_topic_cluster(topics=50, partitions=4, rows=30)
    assignments: list[list] = []
    base = cluster.consumer

    def recording_consumer(config):
        consumer = base(config)
        original = consumer.assign
        consumer.assign = lambda tps: (assignments.append([(tp.topic, tp.partition) for tp in tps]), original(tps))[1]
        return consumer

    settings = PlanSettings(buckets=3, hub_degree=0, chunk_rows=5, topic="chunks", partitions=2, workers=4,
                            memory_mb=1, spill_dir=str(tmp_path / "spill"))
    planner = KafkaEdgePlanner(schema_of(edge), "kafka:1", settings, plan_id="run-1", context="inline",
                               consumer_factory=recording_consumer, producer_factory=lambda: FakeProducer(cluster),
                               admin_factory=cluster.admin)
    planner.start()
    result = planner.finish(tmp_path / "many.json")
    flat = [item for assigned in assignments for item in assigned if item[0] != "chunks"]
    assert len(flat) == len(set(flat))  # no partition read by two consumers
    lanes = [assigned for assigned in assignments if assigned and assigned[0][0] != "chunks"]
    assert 1 < len(lanes) <= 4 and max(len(lane) for lane in lanes) < len(flat)
    assert result.plan.types["FRIEND_OF"].rows == 50 * 30
    one_cluster = many_topic_cluster(topics=50, partitions=4, rows=30)[1]
    one = plan_kafka(one_cluster, edge, tmp_path, workers=1, buckets=3, chunk_rows=5, name="one.json")
    assert chains(one.plan, one_cluster) == chains(result.plan, cluster)


def test_node_below_hub_degree_in_each_topic_becomes_a_hub_when_topics_combine(tmp_path) -> None:
    def cluster_for(topic_b_rows):
        cluster = FakeCluster()
        # Person 7 has degree 6 in topic-a and 6 in topic-b (targets differ); hub_degree is 10.
        publish_topic(cluster, "topic-a", [friend(7, 100 + i, since=1) for i in range(6)]
                      + [friend(200 + i, 300 + i, since=1) for i in range(20)])
        publish_topic(cluster, "topic-b", topic_b_rows)
        return cluster

    edge = multi_edge(["topic-a", "topic-b"])
    combined = cluster_for([friend(7, 400 + i, since=2) for i in range(6)])
    table = plan_kafka(combined, edge, tmp_path, buckets=2, hub=10, name="hub.json").plan.resources
    assert table.hub_resource("Person", 7) is not None
    alone = cluster_for([friend(500 + i, 600 + i, since=2) for i in range(6)])
    plan = plan_kafka(alone, edge, tmp_path, buckets=2, hub=10, name="nohub.json").plan
    assert plan.resources.hub_resource("Person", 7) is None


# -- Story 3.5: plan validation -------------------------------------------------------


def _plan_for(tmp_path, topics=("topic-a", "topic-b")) -> EdgePlan:
    return plan_kafka(overlapping_cluster(), multi_edge(list(topics)), tmp_path).plan


def _strip_topics(plan: EdgePlan) -> EdgePlan:
    payload = plan.to_json()
    payload["stats"].pop("type_topics")
    return EdgePlan.from_json(json.loads(json.dumps(payload)))


def test_plan_validation_compares_the_per_type_topic_list_and_old_plans_still_load(tmp_path) -> None:
    plan = _plan_for(tmp_path)
    validate_plan_against_schema(plan, schema_of(multi_edge(["topic-a", "topic-b"])))
    with pytest.raises(PlannedEdgeError, match=r"FRIEND_OF was planned from topics \['topic-a', 'topic-b'\]"):
        validate_plan_against_schema(plan, schema_of(multi_edge(["topic-b", "topic-a"])))
    with pytest.raises(PlannedEdgeError, match="re-plan"):
        validate_plan_against_schema(plan, schema_of(multi_edge(["topic-a", "topic-b", "topic-c"])))
    with pytest.raises(PlannedEdgeError, match="re-plan"):
        validate_plan_against_schema(plan, schema_of(EDGES["FRIEND_OF"]))
    # A Kafka plan without the record predates multi-topic types: it read one topic per type.
    old = _strip_topics(plan)
    with pytest.raises(PlannedEdgeError, match="predates multi-topic types"):
        validate_plan_against_schema(old, schema_of(multi_edge(["topic-b", "topic-a"])))
    with pytest.raises(PlannedEdgeError, match="not planned from topic"):
        validate_plan_against_schema(old, schema_of(EDGES["FRIEND_OF"]))  # its topic is not in the boundary
    single_cluster = FakeCluster()
    publish_topic(single_cluster, EDGES["FRIEND_OF"].topic, [friend(1, 2, since=1)])
    single = plan_kafka(single_cluster, EDGES["FRIEND_OF"], tmp_path, name="single.json").plan
    old_single = _strip_topics(single)
    validate_plan_against_schema(old_single, schema_of(EDGES["FRIEND_OF"]))  # the old single-topic case loads
    with pytest.raises(PlannedEdgeError, match="predates multi-topic types"):
        validate_plan_against_schema(old_single, schema_of(multi_edge([EDGES["FRIEND_OF"].topic, "topic-b"])))
    # Only producer-source plans carry no topic information and skip the check.
    payload = old.to_json()
    payload["stats"]["source"] = "producer"
    producer_plan = EdgePlan.from_json(json.loads(json.dumps(payload)))
    validate_plan_against_schema(producer_plan, schema_of(multi_edge(["topic-b", "topic-a"])))


def test_type_topics_record_must_cover_every_type_with_a_list(tmp_path) -> None:
    plan = _plan_for(tmp_path)
    edge_schema = schema_of(multi_edge(["topic-a", "topic-b"]))
    for bad in ({}, {"FRIEND_OF": None}, {"FRIEND_OF": "topic-a"}, {"FRIEND_OF": 7}):
        payload = plan.to_json()
        payload["stats"]["type_topics"] = bad
        broken = EdgePlan.from_json(json.loads(json.dumps(payload)))
        with pytest.raises(PlannedEdgeError, match="records no topic list for edge type FRIEND_OF"):
            validate_plan_against_schema(broken, edge_schema)
    payload = plan.to_json()
    payload["stats"]["type_topics"] = ["FRIEND_OF"]
    with pytest.raises(PlannedEdgeError, match="malformed"):
        validate_plan_against_schema(EdgePlan.from_json(json.loads(json.dumps(payload))), edge_schema)


# -- extra ordering cases -------------------------------------------------------------


def test_precedence_holds_through_a_hub_unit_and_for_repeats_inside_and_across_topics(tmp_path) -> None:
    def build():
        cluster = FakeCluster()
        # Person 7 is a hub (degree 13 in topic a alone, hub_degree 10).
        a = [friend(7, 100 + i, since=1, weight=1.5) for i in range(12)]
        a += [friend(7, 100, since=3)]                       # repeat inside topic a
        a += [friend(300 + i, 400 + i, since=1, weight=2.5) for i in range(10)]
        a += [friend(300, 400, since=4)]                     # repeat inside a, merges over the first
        b = [friend(7, 100 + i, since=2) for i in range(0, 12, 2)]    # overlaps half the hub pairs
        b += [friend(300 + i, 400 + i, since=9) for i in range(0, 10, 2)]
        publish_topic(cluster, "topic-a", a)
        publish_topic(cluster, "topic-b", b)
        return cluster

    for workers in (1, 3):
        cluster = build()
        result = plan_kafka(cluster, multi_edge(["topic-a", "topic-b"]), tmp_path, workers=workers, buckets=2, hub=10,
                            name=f"hubprec{workers}.json")
        assert result.plan.resources.hub_resource("Person", 7) is not None
        state = stored(result.plan, cluster)
        assert state[(7, 100)] == {"since": 2, "weight": 1.5}       # a (1 then 3), b (2) wins
        assert state[(7, 101)] == {"since": 1, "weight": 1.5}       # only a
        assert state[(7, 102)] == {"since": 2, "weight": 1.5}
        assert state[(300, 400)] == {"since": 9, "weight": 2.5}     # a (1 then 4), b (9) wins
        assert state[(301, 401)] == {"since": 1, "weight": 2.5}
        reverse_cluster = build()
        reverse = plan_kafka(reverse_cluster, multi_edge(["topic-b", "topic-a"]), tmp_path, workers=workers, buckets=2,
                             hub=10, name=f"hubrev{workers}.json")
        reversed_state = stored(reverse.plan, reverse_cluster)
        assert reversed_state[(7, 100)] == {"since": 3, "weight": 1.5}  # a's last repeat wins now
        assert reversed_state[(300, 400)] == {"since": 4, "weight": 2.5}


def test_order_bound_is_topic_4095_partition_2_20_minus_1_and_one_more_fails() -> None:
    edge = EDGES["FRIEND_OF"]

    def task(index, partition):
        return edge_planner.KafkaReadTask(edge, "t", ((partition, 0, 1),), "k", "g", topic_index=index)

    assert task(4095, (1 << 20) - 1).order == (1 << 32) - 1
    assert task(0, 0).order == 0
    with pytest.raises(edge_planner.EdgePlanError, match="order range"):
        task(4095, 1 << 20).order
    with pytest.raises(edge_planner.EdgePlanError, match="order range"):
        task(4096, 0).order


def test_group_overhead_is_charged_against_the_row_budget(monkeypatch, tmp_path) -> None:
    from src.loader.edge_spill import GROUP_OVERHEAD_BYTES, GroupStore, SpillFile

    store = GroupStore(SpillFile(tmp_path / "rows.bin"), 100_000)
    assert store.limit == 100_000
    for _ in range(40):  # 21.8 KB, within the free allowance of a quarter of the budget: unchanged
        store.new_group(True, False)
    assert store.group_bytes == 40 * GROUP_OVERHEAD_BYTES and store.limit == 100_000
    for _ in range(40):  # 43.5 KB: the excess over 25 KB is charged
        store.new_group(True, False)
    assert store.limit == 100_000 - (80 * GROUP_OVERHEAD_BYTES - 25_000)
    for _ in range(1000):
        store.new_group(True, False)
    assert store.limit == 25_000  # floor: a quarter of the row budget

    monkeypatch.setattr(edge_planner, "_ROWS_SHARE", 0.01)
    monkeypatch.setattr(edge_planner, "_EMIT_SHARE", 0.02)
    monkeypatch.setattr(edge_planner, "_WINDOW_SHARE", 0.0)
    edge, cluster = many_topic_cluster(topics=50, partitions=2, rows=30)
    boundary = capture_edge_boundary([edge], "kafka:1", consumer_factory=cluster.consumer)
    tasks = kafka_read_tasks([edge], boundary, 1, "kafka:1", "g")
    state = PlannerWorkerState(8, hub_degree=0, memory_bytes=1 << 20, spill_dir=tmp_path / "spill", worker_index=0,
                               consumer_factory=cluster.consumer)
    state.scan_lane(tasks)
    assert state.store.group_bytes == GROUP_OVERHEAD_BYTES * len(state.store.groups) > 50 * GROUP_OVERHEAD_BYTES
    assert state.store.limit < state.store.flush_bytes
    assert state.store.peak_buffered <= state.store.flush_bytes + state._step + 200
    state.close()


# -- Story 3.4: order_by in the planned writer ---------------------------------------


def _planned_writer(driver, edge) -> ApocLockedEdgeWriter:
    """Built exactly like ``planned_writer.main`` does."""
    return ApocLockedEdgeWriter(driver, edge, explicit_locks=False, sort_rows=False,
                                missing_endpoint_error_factory=MissingRelationshipEndpointError)


def test_planned_writer_uses_the_conditional_set_and_binds_order_by_only_for_order_by_types() -> None:
    edge = multi_edge(["topic-a", "topic-b"], rule="order_by", order_by="since")
    ok = {"checked_rows": 1, "invalid_rows": 0, "merged_rows": 1, "skipped_as_not_newer_rows": 0}
    driver = MagicMock()
    transaction = driver.session.return_value.__enter__.return_value.begin_transaction.return_value.__enter__.return_value
    transaction.run.return_value.__iter__.side_effect = lambda: iter([ok])
    writer = _planned_writer(driver, edge)
    rows = [{"source_key": 1, "target_key": 2, "properties": {"since": 5}}]
    writer.write_rows(rows)
    query = transaction.run.call_args.args[0]
    assert query == build_apoc_locked_upsert_query(edge, explicit_locks=False, sort_rows=False)
    assert "should_apply_order_by" in query and "apoc" not in query and "ORDER BY elementId" not in query
    assert transaction.run.call_args.kwargs == {"rows": rows, "order_by": "since"}

    plain_edge = multi_edge(["topic-a", "topic-b"])
    driver = MagicMock()
    transaction = driver.session.return_value.__enter__.return_value.begin_transaction.return_value.__enter__.return_value
    transaction.run.return_value.__iter__.side_effect = lambda: iter([{**ok, "skipped_as_not_newer_rows": None}])
    _planned_writer(driver, plain_edge).write_rows(rows)
    assert "should_apply_order_by" not in transaction.run.call_args.args[0]
    assert transaction.run.call_args.kwargs == {"rows": rows}
    assert transaction.run.call_args.args[0].count("SET r += row.properties") == 1
