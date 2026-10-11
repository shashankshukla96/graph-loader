"""Unit tests for the library edge planner (Kafka input, producer sources, determinism)."""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
import json
import math
from pathlib import Path
from types import SimpleNamespace

from confluent_kafka import KafkaError
import pytest

from src.loader.edge_plan import (
    EdgePlanError, ResourceTable, decode_chunk_message, decode_key, encode_key, key_token, plan_failure_marker,
    read_plan_file,
)
from src.loader.edge_planner import (
    EdgePlanInputError, IterableEdgeSource, KafkaEdgePlanner, ParallelEdgePlanner, PlanSettings,
    PlannerWorkerState, capture_edge_boundary, kafka_read_tasks, plan_and_publish_edges, producer_edge_planner,
)
from src.loader.planned_writer import ChunkDecoder
from src.utils.schema_loader import load_schema

SOCIAL = load_schema(Path(__file__).resolve().parents[1] / "examples" / "social" / "graph_schema.yaml")
EDGES = {edge.type: edge for edge in SOCIAL.edges}


# -- fake Kafka --------------------------------------------------------------------


class FakeError:
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code

    def __str__(self):
        return f"FakeError({self._code})"


class FakeMessage:
    def __init__(self, topic, partition, offset, value, error=None):
        self._topic, self._partition, self._offset, self._value, self._error = topic, partition, offset, value, error

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def key(self):
        return None

    def error(self):
        return self._error


class FakeCluster:
    """Topic -> partition -> {offset: value}; ``low``/``high`` watermarks per partition."""

    def __init__(self):
        self.logs: dict[str, dict[int, dict[int, bytes | None]]] = {}
        self.low: dict[tuple[str, int], int] = {}
        self.high: dict[tuple[str, int], int] = {}
        self.created: list = []
        self.deleted: list = []

    def topic(self, name, partitions):
        self.logs[name] = {partition: {} for partition in range(partitions)}
        for partition in range(partitions):
            self.low[(name, partition)] = self.high[(name, partition)] = 0

    def append(self, name, partition, value):
        offset = self.high[(name, partition)]
        self.logs[name][partition][offset] = value
        self.high[(name, partition)] = offset + 1
        return offset

    def skip(self, name, partition, count=1):
        """Advance the high watermark without a readable record (control records, compaction)."""
        self.high[(name, partition)] += count

    def consumer(self, config):
        return FakeConsumer(self, config)

    def admin(self, config):
        cluster = self

        class Admin:
            def list_topics(self, timeout=None):
                return SimpleNamespace(topics=dict(cluster.logs))

            def create_topics(self, topics):
                futures = {}
                for item in topics:
                    cluster.created.append(item)
                    cluster.topic(item.topic, item.num_partitions)
                    futures[item.topic] = SimpleNamespace(result=lambda timeout=None: None)
                return futures

            def delete_topics(self, topics, operation_timeout=None):
                cluster.deleted.extend(topics)
                return {topic: SimpleNamespace(result=lambda timeout=None: None) for topic in topics}
        return Admin()


class FakeConsumer:
    """Round-robins partitions (one record each) so multi-partition reads interleave."""

    def __init__(self, cluster, config):
        self.cluster, self.config = cluster, config
        self.positions: dict[tuple[str, int], int] = {}
        self.paused: set = set()
        self.eof_sent: set = set()
        self.closed = False

    def list_topics(self, topic=None, timeout=None):
        topics = {
            name: SimpleNamespace(partitions={p: None for p in parts}, error=None)
            for name, parts in self.cluster.logs.items() if topic is None or name == topic
        }
        return SimpleNamespace(topics=topics)

    def get_watermark_offsets(self, tp, timeout=None):
        return self.cluster.low[(tp.topic, tp.partition)], self.cluster.high[(tp.topic, tp.partition)]

    def assign(self, tps):
        for tp in tps:
            key = (tp.topic, tp.partition)
            if tp.offset < self.cluster.low[key]:
                raise AssertionError("assigned below the low watermark")
            self.positions[key] = tp.offset

    def pause(self, tps):
        self.paused.update((tp.topic, tp.partition) for tp in tps)

    def consume(self, num_messages=1, timeout=None):
        batch = []
        progressed = True
        while progressed and len(batch) < num_messages:
            progressed = False
            for key in sorted(self.positions):
                if key in self.paused or len(batch) >= num_messages:
                    continue
                topic, partition = key
                position, high = self.positions[key], self.cluster.high[key]
                log = self.cluster.logs[topic][partition]
                while position < high and position not in log:
                    position += 1
                self.positions[key] = position
                if position < high:
                    batch.append(FakeMessage(topic, partition, position, log[position]))
                    self.positions[key] = position + 1
                    progressed = True
                elif self.config.get("enable.partition.eof") and (key, high) not in self.eof_sent:
                    self.eof_sent.add((key, high))
                    batch.append(FakeMessage(topic, partition, high, None, FakeError(KafkaError._PARTITION_EOF)))
                    progressed = True
        return batch

    def close(self):
        self.closed = True


class FakeProducer:
    def __init__(self, cluster):
        self.cluster, self._pending = cluster, []

    def produce(self, topic, *, value, partition, on_delivery):
        offset = self.cluster.append(topic, partition, value)
        self._pending.append((on_delivery, FakeMessage(topic, partition, offset, value)))

    def poll(self, timeout=0):
        pending, self._pending = self._pending, []
        for callback, message in pending:
            callback(None, message)

    def flush(self, timeout=0):
        self.poll()
        return 0


# -- helpers -----------------------------------------------------------------------


def envelope(edge_type, source, target, **properties):
    edge = EDGES[edge_type]
    return {"source": {edge.source_key_property: source}, "target": {edge.target_key_property: target},
            "properties": properties}


def publish(cluster, edge_type, rows, partitions=3):
    """Key rows by source like a real producer: ``hash(source) % partitions`` (stable)."""
    topic = EDGES[edge_type].topic
    if topic not in cluster.logs:
        cluster.topic(topic, partitions)
    for row in rows:
        source = row["source"][EDGES[edge_type].source_key_property]
        partition = sum(json.dumps(source).encode()) % partitions
        cluster.append(topic, partition, json.dumps(row).encode())


def schema_with(*edge_types):
    return SimpleNamespace(nodes=SOCIAL.nodes, edges=[EDGES[name] for name in edge_types])


def ensure_topics(cluster, schema, partitions=3):
    for edge in schema.edges:
        if edge.topic not in cluster.logs:
            cluster.topic(edge.topic, partitions)


def run_kafka_planner(cluster, schema, tmp_path, *, workers=1, buckets=2, hub=0, chunk_rows=4, name="plan.json",
                      max_chunk_bytes=1 << 20, after_start=None, rejection_dir=None):
    ensure_topics(cluster, schema)
    settings = PlanSettings(buckets=buckets, hub_degree=hub, chunk_rows=chunk_rows, max_chunk_bytes=max_chunk_bytes,
                            topic="chunks", partitions=2, workers=workers)
    planner = KafkaEdgePlanner(schema, "kafka:1", settings, plan_id="run-1", context="inline",
                               consumer_factory=cluster.consumer, producer_factory=lambda: FakeProducer(cluster),
                               admin_factory=cluster.admin, rejection_dir=rejection_dir)
    planner.start()
    if after_start is not None:
        after_start()
    result = planner.finish(tmp_path / name)
    return result


def chains(plan, cluster):
    """``unit -> records`` along each unit's chunk chain (one chain per unit)."""
    successors = {chunk.after: chunk for chunk in plan.chunks if chunk.after != -1}
    units = {}
    for chunk in plan.chunks:
        if chunk.after != -1:
            continue
        unit = tuple(chunk.key.split(":")[:3])
        assert unit not in units
        records, current = [], chunk
        while current is not None:
            header, rows = decode_chunk_message(cluster.logs[plan.topic][current.partition][current.offset])
            assert header["chunk"] == current.key and len(rows) == current.rows
            records.extend(rows)
            current = successors.get(current.id)
        units[unit] = records
    return units


def pair_sequences(records, edge_type):
    edge = EDGES[edge_type]
    sequences = defaultdict(list)
    for record in records:
        sequences[(json.dumps(record["source"][edge.source_key_property]),
                   json.dumps(record["target"][edge.target_key_property]))].append(record)
    return sequences


# -- tests -----------------------------------------------------------------------------


def test_reads_exactly_low_to_high_watermark_of_every_partition(tmp_path) -> None:
    cluster = FakeCluster()
    schema = schema_with("VISITED")
    cluster.topic("social-visited", 3)
    rows = [envelope("VISITED", person, f"City {chr(65 + person % 5)}", times=person) for person in range(40)]
    publish(cluster, "VISITED", rows)
    # Retention deleted partition 0's first two records: reading starts at the low watermark.
    del cluster.logs["social-visited"][0][0], cluster.logs["social-visited"][0][1]
    cluster.low[("social-visited", 0)] = 2
    # Partition 1 ends with control records (no readable message up to the high watermark).
    cluster.skip("social-visited", 1, 3)
    result = run_kafka_planner(cluster, schema, tmp_path)
    kept = sum(len(log) for log in cluster.logs["social-visited"].values())
    assert result.rows_read == kept == result.plan.types["VISITED"].rows == 38
    assert result.plan.stats["boundary_records"] == 38 + 3
    assert result.plan.stats["source"] == "kafka"
    assert result.plan.stats["boundary"]["social-visited"][1] == [1, 0, cluster.high[("social-visited", 1)]]


def test_records_after_the_boundary_fail_the_plan_closed(tmp_path) -> None:
    cluster = FakeCluster()
    schema = schema_with("VISITED")
    cluster.topic("social-visited", 2)
    publish(cluster, "VISITED", [envelope("VISITED", 1, "City A", times=1)], partitions=2)

    def late_record():
        publish(cluster, "VISITED", [envelope("VISITED", 2, "City B", times=2)], partitions=2)

    with pytest.raises(EdgePlanInputError, match="changed after the bulk boundary"):
        run_kafka_planner(cluster, schema, tmp_path, after_start=late_record)
    assert "changed after the bulk boundary" in plan_failure_marker(tmp_path / "plan.json").read_text()
    assert not (tmp_path / "plan.json").exists()


def test_missing_edge_topic_fails_with_a_clear_error() -> None:
    cluster = FakeCluster()
    with pytest.raises(EdgePlanInputError, match="social-visited has no partition metadata"):
        capture_edge_boundary([EDGES["VISITED"]], "kafka:1", consumer_factory=cluster.consumer)


@pytest.mark.parametrize("value, reason", [
    (b"{not json", "JSONDecodeError"),
    (None, "message payload is empty"),
    (b'{"source": {"person_id": 1}, "target": {"name": "City A"}, "properties": {"times": NaN}}', "NaN"),
    (b'{"source": {"person_id": "1"}, "target": {"name": "City A"}, "properties": {"times": 1}}', "integer"),
    (b'{"source": {"person_id": 1}, "target": {"name": "City A"}, "properties": {}}', "required"),
    (b'{"source": {"person_id": 1}, "target": {"name": "City A"}, "properties": {"times": 1, "x": 2}}',
     "not declared"),
    (b"\xff\xfe", "UnicodeDecodeError"),
])
def test_invalid_records_fail_closed_with_topic_partition_and_offset(tmp_path, value, reason) -> None:
    cluster = FakeCluster()
    schema = schema_with("VISITED")
    cluster.topic("social-visited", 2)
    cluster.append("social-visited", 1, json.dumps(envelope("VISITED", 1, "City A", times=1)).encode())
    cluster.append("social-visited", 1, value)
    with pytest.raises(RuntimeError) as failure:
        run_kafka_planner(cluster, schema, tmp_path)
    message = str(failure.value)
    assert "invalid VISITED record at social-visited partition 1 offset 1" in message and reason in message
    assert "Traceback" not in message
    assert "partition 1 offset 1" in plan_failure_marker(tmp_path / "plan.json").read_text()


BAD_RECORDS = [b"{not json", None, b"\xff\xfe",
               b'{"source": {"person_id": "1"}, "target": {"name": "City A"}, "properties": {"times": 1}}',
               b'{"source": {"person_id": 1}, "target": {"name": "City A"}, "properties": {"times": 1, "x": 2}}']


def _visited_cluster(with_bad: bool):
    """Two partitions of VISITED records; with ``with_bad`` invalid records are interleaved (and end partition 1)."""
    cluster = FakeCluster()
    cluster.topic("social-visited", 2)
    bad = iter(BAD_RECORDS * 2)
    for index in range(24):
        cluster.append("social-visited", index % 2,
                       json.dumps(envelope("VISITED", index, f"City {index % 3}", times=index)).encode())
        if with_bad and index % 5 == 0:
            cluster.append("social-visited", index % 2, next(bad))
    if with_bad:
        cluster.append("social-visited", 1, next(bad))  # the last record of partition 1 is invalid
    return cluster


@pytest.mark.parametrize("workers", [1, 2])
def test_planner_rejects_malformed_records_and_plans_exactly_the_valid_ones(tmp_path, workers) -> None:
    clean_cluster, dirty_cluster = _visited_cluster(False), _visited_cluster(True)
    schema = schema_with("VISITED")
    clean = run_kafka_planner(clean_cluster, schema, tmp_path, workers=workers, name="clean.json")
    rejections = tmp_path / "rej"
    dirty = run_kafka_planner(dirty_cluster, schema, tmp_path, workers=workers, name="dirty.json",
                              rejection_dir=rejections)
    assert dirty.plan.types["VISITED"].rows == clean.plan.types["VISITED"].rows == 24
    assert dirty.plan.stats["rejected"] == 6 and "rejected" not in clean.plan.stats
    assert dirty.rows_read == 24
    # exactly the plan of the valid records only: same chains of the same record bytes
    assert chains(dirty.plan, dirty_cluster) == chains(clean.plan, clean_cluster)
    entries = [json.loads(line) for path in sorted(rejections.glob("run-1-planner-*.jsonl"))
               for line in path.read_text().splitlines()]
    assert len(entries) == 6
    assert all(entry["edge_type"] == "VISITED" and entry["topic"] == "social-visited" for entry in entries)
    last = dirty_cluster.high[("social-visited", 1)] - 1
    assert any(entry["partition"] == 1 and entry["offset"] == last for entry in entries)
    assert sum(lane.get("rejected", 0) for lane in dirty.plan.stats["lanes"]) == 6


def test_rejection_dir_without_bad_records_leaves_no_rejected_key(tmp_path) -> None:
    result = run_kafka_planner(_visited_cluster(False), schema_with("VISITED"), tmp_path, workers=2,
                               rejection_dir=tmp_path / "rej")
    assert "rejected" not in result.plan.stats
    assert all("rejected" not in lane for lane in result.plan.stats["lanes"])
    assert not list((tmp_path / "rej").glob("*.jsonl"))


def test_unwritable_rejection_file_fails_the_plan_naming_path_and_record(tmp_path) -> None:
    blocker = tmp_path / "rej"
    blocker.write_text("a file, not a directory")
    with pytest.raises(EdgePlanInputError, match=r"rejection directory .*rej is unusable"):
        run_kafka_planner(_visited_cluster(True), schema_with("VISITED"), tmp_path, rejection_dir=blocker)
    state = PlannerWorkerState(2, rejection_path=blocker / "x-planner-0.jsonl")
    with pytest.raises(EdgePlanInputError, match=r"x-planner-0\.jsonl.*social-visited partition 1 offset 7"):
        state._reject(edge_type="VISITED", topic="social-visited", partition=1, offset=7, key=None,
                      value=b"x", reason="bad")
    state.close()


def test_planner_without_a_rejection_dir_still_fails_closed_on_the_same_data(tmp_path) -> None:
    with pytest.raises(RuntimeError, match="invalid VISITED record"):
        run_kafka_planner(_visited_cluster(True), schema_with("VISITED"), tmp_path)


def test_a_kafka_error_still_fails_closed_even_with_a_rejection_dir(tmp_path, monkeypatch) -> None:
    """Only ValueError records are rejected row-level; a broker error is not a record problem."""
    consume = FakeConsumer.consume

    def failing_consume(self, num_messages=1, timeout=None):
        if not getattr(self, "_failed", False):
            self._failed = True
            return [FakeMessage("social-visited", 0, 0, None, FakeError(KafkaError._ALL_BROKERS_DOWN))]
        return consume(self, num_messages, timeout)

    monkeypatch.setattr(FakeConsumer, "consume", failing_consume)
    rejections = tmp_path / "rej"
    with pytest.raises(RuntimeError, match=r"social-visited partition 0: Kafka error"):
        run_kafka_planner(_visited_cluster(True), schema_with("VISITED"), tmp_path, rejection_dir=rejections)
    assert not list(rejections.glob("*.jsonl"))


def test_rejected_last_record_finishes_its_partition(tmp_path) -> None:
    cluster = FakeCluster()
    cluster.topic("social-visited", 1)
    cluster.append("social-visited", 0, json.dumps(envelope("VISITED", 1, "City A", times=1)).encode())
    cluster.append("social-visited", 0, b"{not json")
    result = run_kafka_planner(cluster, schema_with("VISITED"), tmp_path, rejection_dir=tmp_path / "rej")
    assert result.plan.types["VISITED"].rows == 1 and result.plan.stats["rejected"] == 1


def test_pairs_keep_topic_partition_order_and_units_chain_in_partition_offset_order(tmp_path) -> None:
    cluster = FakeCluster()
    schema = schema_with("FRIEND_OF")
    cluster.topic("social-friend-of", 3)
    rows = []
    for step in range(60):
        source, target = step % 7, (step * 3) % 5
        rows.append(envelope("FRIEND_OF", source, target, since=step, **({"weight": step / 2} if step % 2 else {})))
    publish(cluster, "FRIEND_OF", rows)
    result = run_kafka_planner(cluster, schema, tmp_path, buckets=2, chunk_rows=3)
    units = chains(result.plan, cluster)
    for unit, records in units.items():
        # Along the chain, records follow (partition, offset) order.
        positions = []
        for record in records:
            text = json.dumps(record).encode()
            positions.append(next(
                (partition, offset) for partition, log in cluster.logs["social-friend-of"].items()
                for offset, value in log.items() if json.loads(value) == record and (partition, offset) not in positions
            ))
            del text
        assert positions == sorted(positions)
        for pair, sequence in pair_sequences(records, "FRIEND_OF").items():
            published = [row for row in rows if (json.dumps(row["source"]["person_id"]),
                                                 json.dumps(row["target"]["person_id"])) == pair]
            assert sequence == published
    assert sum(len(records) for records in units.values()) == 60
    # Self loops (source == target) use a single resource.
    loops = [chunk for chunk in result.plan.chunks if len(chunk.resources) == 1]
    assert loops


def test_parallel_planning_is_deterministic_and_worker_count_independent(tmp_path) -> None:
    def build():
        cluster = FakeCluster()
        schema = schema_with("BOUGHT", "LIVES_IN", "VISITED")
        for edge in schema.edges:
            cluster.topic(edge.topic, 4)
        rows_bought = [envelope("BOUGHT", i % 23, f"sku-{i % 11}", quantity=i) for i in range(300)]
        rows_lives = [envelope("LIVES_IN", i, "City A" if i % 3 else f"City {chr(66 + i % 4)}") for i in range(200)]
        rows_visited = [envelope("VISITED", i % 31, f"City {chr(65 + i % 6)}", times=i) for i in range(150)]
        publish(cluster, "BOUGHT", rows_bought, 4)
        publish(cluster, "LIVES_IN", rows_lives, 4)
        publish(cluster, "VISITED", rows_visited, 4)
        return cluster, schema

    results = {}
    for workers in (1, 3, 3):
        cluster, schema = build()
        result = run_kafka_planner(cluster, schema, tmp_path, workers=workers, buckets=3, hub=40, chunk_rows=7,
                                   name=f"plan-{workers}-{len(results)}.json")
        results.setdefault(workers, []).append((result, cluster))
    (one, one_cluster), = results[1]
    (first, first_cluster), (second, second_cluster) = results[3]
    # Same worker count: identical plans (apart from measured timings in the stats).
    def without_timings(plan):
        payload = plan.to_json()
        payload["stats"] = {key: value for key, value in payload["stats"].items()
                            if not key.endswith(("seconds", "second")) and key != "lanes"}
        return payload

    assert without_timings(first.plan) == without_timings(second.plan)
    # Any worker count: same resources/hubs and the same rows per unit in the same chain order.
    assert one.plan.resources.names == first.plan.resources.names
    assert first.plan.resources.hub_count >= 1
    assert chains(one.plan, one_cluster) == chains(first.plan, first_cluster)
    assert {name: item.rows for name, item in one.plan.types.items()} == {
        name: item.rows for name, item in first.plan.types.items()}


def test_kafka_tasks_split_large_topics_into_contiguous_balanced_partition_groups() -> None:
    edges = [EDGES["BOUGHT"], EDGES["VISITED"]]
    boundary = {
        "social-bought": tuple((p, 0, 100) for p in range(6)),
        "social-visited": ((0, 0, 10), (1, 5, 5)),
    }
    tasks = kafka_read_tasks(edges, boundary, 3, "kafka:1", "g")
    bought = [task for task in tasks if task.edge_type == "BOUGHT"]
    assert [[p for p, _, _ in task.ranges] for task in bought] == [[0, 1], [2, 3], [4, 5]]
    assert [task.order for task in bought] == [0, 2, 4] and all(task.multipart for task in bought)
    visited = [task for task in tasks if task.edge_type == "VISITED"]
    assert len(visited) == 1 and visited[0].ranges == boundary["social-visited"]
    assert not visited[0].multipart and visited[0].size_hint == 10


def test_every_schema_key_type_plans_and_decodes_through_the_writer(tmp_path) -> None:
    cluster = FakeCluster()
    schema = schema_with("ACTIVE_ON", "BOUGHT", "FRIEND_OF")
    for edge in schema.edges:
        cluster.topic(edge.topic, 2)
    days = [date(2024, 1, 1) + timedelta(days=i) for i in range(4)]
    active = [envelope("ACTIVE_ON", -5 + i, days[i % 4].isoformat(), sessions=i) for i in range(30)]
    bought = [envelope("BOUGHT", 2 ** 53 + i % 3, "sku-1", quantity=i, amount=float(i) + 0.5,
                       at=(datetime(2024, 5, 1, tzinfo=timezone(timedelta(hours=2))) + timedelta(minutes=i)).isoformat())
              for i in range(12)]
    friends = [envelope("FRIEND_OF", i % 4, (i + 1) % 4, since=2000 + i, weight=-0.0) for i in range(8)]
    publish(cluster, "ACTIVE_ON", active, 2)
    publish(cluster, "BOUGHT", bought, 2)
    publish(cluster, "FRIEND_OF", friends, 2)
    result = run_kafka_planner(cluster, schema, tmp_path, buckets=2, hub=8, chunk_rows=5)
    plan, digest = read_plan_file(result.path)
    assert digest == result.digest
    hubs = {(label, value) for label, kind, value in plan.resources.names if kind == "hub"}
    # Date hubs round-trip through the plan file as dates.
    assert ("Day", days[0]) in hubs and all(type(value) is date for label, value in hubs if label == "Day")
    decoder = ChunkDecoder(plan, EDGES)
    decoded = defaultdict(list)
    for chunk in plan.chunks:
        decoded[chunk.edge_type].extend(decoder.decode(chunk, cluster.logs["chunks"][chunk.partition][chunk.offset]))
    assert {row["target_key"] for row in decoded["ACTIVE_ON"]} == set(days)
    assert {type(row["source_key"]) for rows in decoded.values() for row in rows} == {int}
    assert {row["properties"]["at"].utcoffset() for row in decoded["BOUGHT"]} == {timedelta(hours=2)}
    assert all(type(row["properties"]["amount"]) is float for row in decoded["BOUGHT"])
    assert sum(len(rows) for rows in decoded.values()) == 50


def test_key_tokens_follow_python_equality_and_plan_keys_round_trip() -> None:
    assert key_token(-0.0) == key_token(0.0)
    noon_plus_one = datetime(2024, 1, 1, 12, tzinfo=timezone(timedelta(hours=1)))
    assert key_token(noon_plus_one) == key_token(datetime(2024, 1, 1, 11, tzinfo=timezone.utc))
    assert key_token(date(2024, 1, 1)) != key_token("2024-01-01") != key_token(datetime(2024, 1, 1, tzinfo=timezone.utc))
    for value in ("x", 7, -2.5, date(2024, 2, 29), noon_plus_one):
        assert decode_key(json.loads(json.dumps(encode_key(value)))) == value
    with pytest.raises(EdgePlanError):
        encode_key(math.nan)
    with pytest.raises(EdgePlanError):
        decode_key({"date": "not a date"})
    table = ResourceTable(["Day"], 4, {"Day": [date(2024, 1, 2)]})
    copy = ResourceTable.from_json(json.loads(json.dumps(table.to_json())))
    assert copy.names == table.names and copy.resource("Day", date(2024, 1, 2)) == table.hub_resource(
        "Day", date(2024, 1, 2))


def test_kafka_planner_creates_the_run_topic_derives_settings_and_deletes_the_topic(tmp_path) -> None:
    cluster = FakeCluster()
    schema = schema_with("VISITED", "WORKS_AT")
    ensure_topics(cluster, schema)
    publish(cluster, "VISITED", [envelope("VISITED", i, "City A", times=i) for i in range(10)])
    seen = {}

    def settings_for(records, edge_types):
        seen["records"], seen["types"] = records, edge_types
        return PlanSettings(buckets=2, hub_degree=0, chunk_rows=3, topic="run-chunks", partitions=3, workers=2)

    planner = KafkaEdgePlanner(schema, "kafka:1", settings_for, plan_id="p1", context="inline",
                               consumer_factory=cluster.consumer, producer_factory=lambda: FakeProducer(cluster),
                               admin_factory=cluster.admin)
    planner.start()
    thread = planner.run_in_background(tmp_path / "edge-plan.json")
    thread.join(10)
    assert planner.error is None and seen["records"] == 10 and seen["types"] == 1
    plan, _ = read_plan_file(tmp_path / "edge-plan.json")
    assert plan.topic == "run-chunks" and plan.partitions == 3 and plan.types["WORKS_AT"].rows == 0
    assert [(item.topic, item.num_partitions) for item in cluster.created] == [("run-chunks", 3)]
    assert int(cluster.created[0].config["max.message.bytes"]) > plan.settings["max_message_bytes"]
    planner.close(delete_topic=True)
    assert cluster.deleted == ["run-chunks"]


def test_kafka_planner_passes_boundary_self_referencing_records_to_a_keyword_callable(tmp_path) -> None:
    cluster = FakeCluster()
    schema = schema_with("VISITED", "FRIEND_OF", "WORKS_AT")
    ensure_topics(cluster, schema)
    publish(cluster, "VISITED", [envelope("VISITED", i, "City A", times=i) for i in range(10)])
    publish(cluster, "FRIEND_OF", [envelope("FRIEND_OF", i, i + 1, since=2020) for i in range(30)])
    seen = {}

    def settings_for(records, edge_types, *, self_ref_records):
        seen.update(records=records, types=edge_types, self_ref=self_ref_records)
        return PlanSettings(buckets=2, hub_degree=0, chunk_rows=3, topic="run-chunks-2", partitions=1, workers=1)

    planner = KafkaEdgePlanner(schema, "kafka:1", settings_for, plan_id="p2", context="inline",
                               consumer_factory=cluster.consumer, producer_factory=lambda: FakeProducer(cluster),
                               admin_factory=cluster.admin)
    planner.start()
    planner.close(delete_topic=True)
    assert seen == {"records": 40, "types": 2, "self_ref": 30}


def test_existing_chunk_topic_is_refused(tmp_path) -> None:
    cluster = FakeCluster()
    schema = schema_with("VISITED")
    ensure_topics(cluster, schema)
    cluster.topic("chunks", 1)
    with pytest.raises(EdgePlanError, match="already exists"):
        KafkaEdgePlanner(schema, "kafka:1", PlanSettings(topic="chunks"), plan_id="p", context="inline",
                         consumer_factory=cluster.consumer, admin_factory=cluster.admin).start()


def test_producer_api_plans_in_memory_rows_prepared_rows_and_rejects_bad_keys(tmp_path) -> None:
    cluster = FakeCluster()
    rows = {
        "VISITED": [envelope("VISITED", i, "City A", times=i) for i in range(6)]
        + [json.dumps(envelope("VISITED", 9, "City B", times=1))],
        "ACTIVE_ON": [(3, "2024-01-02", json.dumps(envelope("ACTIVE_ON", 3, "2024-01-02", sessions=1)))],
    }
    result = plan_and_publish_edges(
        SOCIAL, rows, bootstrap_servers="kafka:1", plan_path=tmp_path / "p.json",
        settings=PlanSettings(buckets=2, hub_degree=0, chunk_rows=4, topic="producer-chunks", partitions=1),
        admin_factory=cluster.admin, producer_factory=lambda: FakeProducer(cluster), plan_id="prod",
    )
    plan = result.plan
    assert plan.types["VISITED"].rows == 7 and plan.types["ACTIVE_ON"].rows == 1 and plan.types["BOUGHT"].rows == 0
    assert plan.stats["source"] == "producer" and cluster.created[0].topic == "producer-chunks"
    decoder = ChunkDecoder(plan, EDGES)
    decoded = [row for chunk in plan.chunks
               for row in decoder.decode(chunk, cluster.logs["producer-chunks"][chunk.partition][chunk.offset])]
    assert date(2024, 1, 2) in {row["target_key"] for row in decoded}
    with pytest.raises(RuntimeError, match="prepared row endpoint keys do not match"):
        producer_edge_planner(
            SOCIAL, {"ACTIVE_ON": [("3", "2024-01-02", "{}")]}, PlanSettings(topic="x"), "kafka:1",
            context="inline", producer_factory=lambda: FakeProducer(cluster),
        ).finish(tmp_path / "bad.json")
    with pytest.raises(RuntimeError, match=r"invalid VISITED row 2 of VISITED source 0"):
        producer_edge_planner(
            SOCIAL, [IterableEdgeSource("VISITED", [envelope("VISITED", 1, "City A", times=1), {"x": 1}])],
            PlanSettings(topic="x"), "kafka:1", context="inline", producer_factory=lambda: FakeProducer(cluster),
        ).finish(tmp_path / "bad2.json")
    with pytest.raises(EdgePlanError, match="not in the schema"):
        producer_edge_planner(SOCIAL, {"NOPE": []}, PlanSettings(topic="x"), "kafka:1")


def test_planner_rejects_duplicate_task_orders() -> None:
    sources = [IterableEdgeSource("VISITED", [], order=0), IterableEdgeSource("VISITED", [], order=0)]
    with pytest.raises(EdgePlanError, match="distinct orders"):
        producer_edge_planner(SOCIAL, sources, PlanSettings(topic="x"), "kafka:1")


def test_plan_settings_are_validated() -> None:
    with pytest.raises(ValueError, match="topic"):
        PlanSettings().validate()
    with pytest.raises(ValueError, match="buckets"):
        PlanSettings(buckets=0, topic="t").validate()
    with pytest.raises(ValueError, match="integer"):
        PlanSettings(workers=True, topic="t").validate()
    assert ParallelEdgePlanner([], PlanSettings(topic="t"), "k", types={}).lanes == [[]]


# ── Metrics instrumentation (P6-S2 Story 8) ──────────────────────────────────


def test_progress_heartbeat_increments_planner_counters_by_delta() -> None:
    """The heartbeat's own read function (_progress, called at most once per
    _Heartbeat.interval) is the existing progress call site this story
    wires - never a new per-row hook. Counters advance by the delta since
    the previous beat, since totals are running absolutes, not per-beat
    increments."""
    from types import SimpleNamespace
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    planner = ParallelEdgePlanner([], PlanSettings(topic="t"), "k", types={}, metrics=metrics)
    planner._workers = [SimpleNamespace(progress=[100, 2, 0])]

    result = planner._progress()
    assert result == {"phase": "starting", "rows": 100, "chunks": 2, "steps": 0}
    assert metrics.registry.get_sample_value(
        "graph_loader_planner_rows_read_total", {"kind": "cli"}
    ) == 100.0
    assert metrics.registry.get_sample_value(
        "graph_loader_planner_chunks_published_total", {"kind": "cli"}
    ) == 2.0

    # A second beat with more progress advances by the delta, not the
    # absolute total again.
    planner._workers = [SimpleNamespace(progress=[150, 5, 0])]
    planner._progress()
    assert metrics.registry.get_sample_value(
        "graph_loader_planner_rows_read_total", {"kind": "cli"}
    ) == 150.0
    assert metrics.registry.get_sample_value(
        "graph_loader_planner_chunks_published_total", {"kind": "cli"}
    ) == 5.0


def test_progress_heartbeat_stays_at_zero_when_no_progress_yet() -> None:
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    from src.utils import metrics as metrics_module

    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    metrics_module.initialize_zero_series(metrics, kind="cli", name="-")
    planner = ParallelEdgePlanner([], PlanSettings(topic="t"), "k", types={}, metrics=metrics)
    planner._progress()
    assert metrics.registry.get_sample_value(
        "graph_loader_planner_rows_read_total", {"kind": "cli"}
    ) == 0.0
    assert metrics.registry.get_sample_value(
        "graph_loader_planner_chunks_published_total", {"kind": "cli"}
    ) == 0.0


def test_close_reads_final_progress_after_heartbeat_stops(tmp_path) -> None:
    """P6-S2 review finding S8-3: close() must count any progress made
    since the last (~1s interval) heartbeat beat - otherwise a plan that
    finishes in under one heartbeat interval loses that trailing progress
    forever, and a very short plan would read 0 permanently."""
    from unittest.mock import MagicMock
    from prometheus_client import CollectorRegistry
    from src.utils.metrics import GraphLoaderMetrics

    metrics = GraphLoaderMetrics(registry=CollectorRegistry())
    planner = ParallelEdgePlanner(
        [], PlanSettings(topic="t"), "k", types={}, metrics=metrics,
        plan_path=tmp_path / "plan.json",
    )
    # Simulate a worker that made progress after the heartbeat's last beat
    # (e.g. the heartbeat's own 1s interval hadn't elapsed again before
    # the plan finished).
    worker = MagicMock()
    worker.progress = [42, 3, 0]
    planner._workers = [worker]
    planner.close()

    assert metrics.registry.get_sample_value(
        "graph_loader_planner_rows_read_total", {"kind": "cli"}
    ) == 42.0
    assert metrics.registry.get_sample_value(
        "graph_loader_planner_chunks_published_total", {"kind": "cli"}
    ) == 3.0
