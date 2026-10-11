"""External-memory planning: identical plans to the in-memory planner, bounded memory, cleanup.

The reference is the original in-memory planner (``tests/reference_edge_planner.py``,
frozen before the external planner replaced it).  Both plan the same randomized
inputs -- several edge types, a self-referencing type with self loops, repeated
``(source, target)`` pairs, heavy-tailed degrees (hubs), date keys, Kafka topics
split over several planner tasks -- and must produce the identical plan file
(chunks, ids, keys, resources, Kafka locations, predecessors, ranks) and
byte-identical chunk messages, whether the external planner fits in memory or
spills everything to disk.
"""
from __future__ import annotations

from datetime import date, timedelta
import json
from pathlib import Path
import random

import numpy as np
import pytest

from reference_edge_planner import plan_in_memory
from src.loader import edge_planner
from src.loader.edge_plan import (
    chunk_boundaries, decode_chunk_message, decode_key, decode_token, encode_chunk_message, encode_chunk_message_parts, key_token,
    plan_failure_marker, plan_progress_path,
)
from src.loader.edge_planner import (
    IterableEdgeSource, KafkaEdgePlanner, PlannerWorkerState, PlanSettings, capture_edge_boundary,
    chunk_topic_config, default_plan_memory_mb, kafka_read_tasks, producer_edge_planner, source_tasks,
)
from src.loader.edge_spill import unit_chunks
from test_edge_planner import EDGES, SOCIAL, FakeCluster, FakeProducer, envelope, publish, schema_with


TYPES = ("FRIEND_OF", "BOUGHT", "VISITED", "ACTIVE_ON", "LIVES_IN")


def random_rows(seed: int, rows: int) -> dict[str, list[dict]]:
    """Heavy-tailed endpoints (hubs), self loops, repeated pairs, date keys."""
    rng = random.Random(seed)
    persons = [rng.randint(-10**6, 10**6) for _ in range(max(20, rows // 6))]
    persons[0] = 2 ** 53 + 1
    products = [f"sku-{index:05d}" for index in range(max(10, rows // 10))]
    cities = [f"City {chr(65 + index)}" for index in range(12)]
    days = [date(2024, 1, 1) + timedelta(days=index) for index in range(30)]

    def skewed(items):
        return items[min(len(items) - 1, int(rng.paretovariate(1.2)) - 1)] if rng.random() < 0.6 else rng.choice(items)

    data = {name: [] for name in TYPES}
    for step in range(rows):
        kind = rng.choices(TYPES, weights=(40, 30, 10, 10, 10))[0]  # uneven: large topics split into tasks
        person = skewed(persons)
        if kind == "FRIEND_OF":
            other = person if rng.random() < 0.1 else skewed(persons)
            data[kind].append(envelope(kind, person, other, since=1990 + step % 30,
                                       **({"weight": step / 7} if step % 3 else {})))
        elif kind == "BOUGHT":
            data[kind].append(envelope(kind, person, skewed(products), quantity=step % 5 + 1,
                                       **({"amount": round(step * 0.37, 2)} if step % 2 else {})))
        elif kind == "VISITED":
            data[kind].append(envelope(kind, person, skewed(cities), times=step))
        elif kind == "LIVES_IN":
            data[kind].append(envelope(kind, person, rng.choice(cities)))
        else:
            data[kind].append(envelope(kind, person, skewed(days).isoformat(), sessions=step % 9))
        if rng.random() < 0.08:  # repeat an earlier pair (last write wins along the chain)
            data[kind].append(dict(data[kind][rng.randrange(len(data[kind]))]))
    return data


def build_cluster(data, partitions=4):
    cluster = FakeCluster()
    for name in TYPES:
        cluster.topic(EDGES[name].topic, partitions)
        publish(cluster, name, data[name], partitions)
    return cluster


def kafka_tasks(cluster, schema, workers):
    boundary = capture_edge_boundary(schema.edges, "kafka:1", consumer_factory=cluster.consumer)
    return kafka_read_tasks(schema.edges, boundary, workers, "kafka:1", "planner-test")


def types_of(schema):
    return {edge.type: (edge.nodes.source, edge.nodes.target) for edge in schema.edges}


def comparable(plan) -> dict:
    payload = plan.to_json()
    payload["stats"] = {key: value for key, value in payload["stats"].items()
                        if not key.endswith(("seconds", "second")) and key != "lanes"}
    return payload


def messages(plan, cluster) -> list[bytes]:
    return [cluster.logs[plan.topic][chunk.partition][chunk.offset] for chunk in plan.chunks]


def tiny_budget(monkeypatch) -> None:
    """1 MiB workers with tiny shares: every structure spills, cells stream from disk."""
    monkeypatch.setattr(edge_planner, "_ROWS_SHARE", 0.004)
    monkeypatch.setattr(edge_planner, "_EMIT_SHARE", 0.006)
    monkeypatch.setattr(edge_planner, "_WINDOW_SHARE", 0.0)


def plan_both(data, *, workers, buckets, hub, chunk_rows, max_chunk_bytes, memory_mb, tmp_path, name):
    schema = schema_with(*TYPES)
    settings = PlanSettings(buckets=buckets, hub_degree=hub, chunk_rows=chunk_rows, max_chunk_bytes=max_chunk_bytes,
                            topic="chunks", partitions=3, workers=workers, memory_mb=memory_mb,
                            spill_dir=str(tmp_path / "spill"))
    reference_cluster = build_cluster(data)
    reference_cluster.topic("chunks", 3)
    reference = plan_in_memory(
        kafka_tasks(reference_cluster, schema, workers), settings, types=types_of(schema), plan_id="run-1",
        producer_factory=lambda: FakeProducer(reference_cluster), consumer_factory=reference_cluster.consumer,
        extra_stats={"source": "kafka"},
    )
    cluster = build_cluster(data)
    cluster.topic("chunks", 3)
    planner = edge_planner.ParallelEdgePlanner(
        kafka_tasks(cluster, schema, workers), settings, "kafka:1", types=types_of(schema), plan_id="run-1",
        context="inline", producer_factory=lambda: FakeProducer(cluster), consumer_factory=cluster.consumer,
        extra_stats={"source": "kafka"},
    )
    result = planner.finish(tmp_path / name)
    return reference, reference_cluster, result, cluster


@pytest.mark.parametrize("seed, workers, buckets, hub, chunk_rows, max_chunk_bytes", [
    (1, 1, 3, 12, 7, 1 << 20),
    (2, 3, 2, 8, 14, 1100),     # byte cap binds; topics split over several tasks (multipart)
    (3, 2, 4, 0, 9, 1 << 20),   # hubs disabled
    (4, 4, 1, 20, 50, 3000),    # one bucket: big cells
])
@pytest.mark.parametrize("spill", [False, True])
def test_external_plan_is_identical_to_the_in_memory_plan(monkeypatch, tmp_path, seed, workers, buckets, hub,
                                                          chunk_rows, max_chunk_bytes, spill) -> None:
    if spill:
        tiny_budget(monkeypatch)
    data = random_rows(seed, 2500)
    reference, reference_cluster, result, cluster = plan_both(
        data, workers=workers, buckets=buckets, hub=hub, chunk_rows=chunk_rows, max_chunk_bytes=max_chunk_bytes,
        memory_mb=1 if spill else 64, tmp_path=tmp_path, name="plan.json",
    )
    assert comparable(result.plan) == comparable(reference)
    assert messages(result.plan, cluster) == messages(reference, reference_cluster)
    assert result.plan.total_rows == sum(len(rows) for rows in data.values())
    if hub:
        assert result.plan.resources.hub_count > 0
    spilled = sum(lane["spilled_bytes"] for lane in result.plan.stats["lanes"])
    assert (spilled > 0) == spill
    # The spill directory and the progress heartbeat are gone.
    assert not (tmp_path / "spill" / ".edge-plan-spill-run-1").exists()
    assert not plan_progress_path(tmp_path / "plan.json").exists()


def test_multipart_producer_sources_and_prepared_rows_are_identical(monkeypatch, tmp_path) -> None:
    tiny_budget(monkeypatch)
    data = random_rows(9, 3000)
    sources = []
    for name, rows in data.items():
        edge = EDGES[name]
        half = len(rows) // 2
        sources.append(IterableEdgeSource(name, [json.dumps(row) for row in rows[:half]], order=0, size_hint=half))
        sources.append(IterableEdgeSource(name, [
            (row["source"][edge.source_key_property], row["target"][edge.target_key_property], json.dumps(row))
            for row in rows[half:]
        ], order=1, size_hint=len(rows) - half))
    settings = PlanSettings(buckets=3, hub_degree=10, chunk_rows=16, max_chunk_bytes=1200, topic="chunks",
                            partitions=2, workers=3, memory_mb=1, spill_dir=str(tmp_path / "spill"))
    types = types_of(SOCIAL)
    reference_cluster, cluster = FakeCluster(), FakeCluster()
    reference_cluster.topic("chunks", 2)
    cluster.topic("chunks", 2)
    reference = plan_in_memory(source_tasks(SOCIAL, sources), settings, types=types, plan_id="p",
                               producer_factory=lambda: FakeProducer(reference_cluster))
    result = producer_edge_planner(SOCIAL, sources, settings, "kafka:1", plan_id="p", context="inline",
                                   producer_factory=lambda: FakeProducer(cluster)).finish(tmp_path / "p.json")
    reference.stats["source"] = "producer"
    assert comparable(result.plan) == comparable(reference)
    assert messages(result.plan, cluster) == messages(reference, reference_cluster)


def test_plans_and_chains_do_not_depend_on_worker_count_or_budget(monkeypatch, tmp_path) -> None:
    data = random_rows(5, 3000)
    chains = []
    for workers, spill in ((1, False), (4, False), (3, True), (1, True)):
        with monkeypatch.context() as patch:
            if spill:
                tiny_budget(patch)
            _, _, result, cluster = plan_both(
                data, workers=workers, buckets=3, hub=15, chunk_rows=8, max_chunk_bytes=1 << 20,
                memory_mb=1 if spill else 64, tmp_path=tmp_path, name=f"plan-{workers}-{spill}.json",
            )
        successors = {chunk.after: chunk for chunk in result.plan.chunks if chunk.after != -1}
        units = {}
        for chunk in result.plan.chunks:
            if chunk.after == -1:
                records, current = [], chunk
                while current is not None:
                    records.extend(decode_chunk_message(
                        cluster.logs["chunks"][current.partition][current.offset])[1])
                    current = successors.get(current.id)
                unit = chunk.key.rsplit(":", 2)[0]
                assert unit not in units  # one chain per unit
                units[unit] = records
        chains.append((result.plan.resources.names, units))
    # Hubs, resources and every unit's rows in chain order ((partition, offset) order) are the
    # same for any worker count and memory budget; only the chunk cuts follow the task split.
    assert all(names == chains[0][0] for names, _ in chains)
    assert all(units == chains[0][1] for _, units in chains)
    assert sum(len(records) for records in chains[0][1].values()) == sum(len(rows) for rows in data.values())


def test_memory_bound_is_respected_and_spill_files_are_removed(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(edge_planner, "_ROWS_SHARE", 0.01)
    monkeypatch.setattr(edge_planner, "_EMIT_SHARE", 0.02)
    monkeypatch.setattr(edge_planner, "_WINDOW_SHARE", 0.0)
    data = random_rows(6, 4000)
    rows = [(name, row) for name, items in data.items() for row in items]
    spill = tmp_path / "spill"
    state = PlannerWorkerState(1, hub_degree=10, memory_bytes=1 << 20, spill_dir=spill, worker_index=0)
    cluster = FakeCluster()
    cluster.topic("chunks", 2)
    tasks = source_tasks(SOCIAL, {name: [json.dumps(row) for row in items] for name, items in data.items()})
    state.scan_lane(tasks)
    budget = state.store.flush_bytes
    largest = max(len(json.dumps(row)) for _, row in rows) + 60
    assert state.store.flushes > 3
    assert 0 < state.store.peak_buffered <= budget + state._step + largest
    assert len(state.windows.windows) > 3 and state.windows.cap == 256
    files = sorted(path.name for path in spill.iterdir())
    assert files == ["worker-0-keys.bin", "worker-0-rows.bin"]
    counter = state.degree_counter()
    candidates = np.packbits(counter >= 10).tobytes()
    degrees = state.candidate_degrees(candidates)
    hubs = {label: [decode_token(token) for token, count in counts.items() if count >= 10]
            for label, counts in degrees.items()}
    from src.loader.edge_plan import ResourceTable
    table = ResourceTable({label for edge in SOCIAL.edges for label in (edge.nodes.source, edge.nodes.target)},
                          1, {label: keys for label, keys in hubs.items() if keys})
    loaded, streamed = [], []
    in_memory, on_disk = state._units_in_memory, state._units_streamed

    def spy(group, *args):
        loaded.append(state.store.total_bytes(group) + 48 * group.rows)
        return in_memory(group, *args)

    def spy_streamed(group, *args):
        streamed.append(group.rows)
        return on_disk(group, *args)
    monkeypatch.setattr(state, "_units_in_memory", spy)
    monkeypatch.setattr(state, "_units_streamed", spy_streamed)
    specs, _, stats = state.emit(table, run="r", topic="chunks", partitions=2, chunk_rows=50,
                                 max_chunk_bytes=1 << 20, producer_factory=lambda: FakeProducer(cluster),
                                 worker_index=0)
    assert sum(spec.rows for spec in specs) == len(rows)
    assert all(size <= state.emit_bytes for size in loaded) and streamed
    assert stats["spilled_bytes"] > 0 and stats["row_flushes"] == state.store.flushes
    state.close()
    assert not any(spill.iterdir())


def test_failed_plans_remove_spill_files_and_progress_and_write_the_marker(tmp_path) -> None:
    cluster = FakeCluster()
    schema = schema_with("VISITED")
    cluster.topic("social-visited", 2)
    publish(cluster, "VISITED", [envelope("VISITED", i, "City A", times=i) for i in range(50)], 2)
    cluster.append("social-visited", 1, b'{"source": {"person_id": "x"}}')
    settings = PlanSettings(buckets=2, hub_degree=3, chunk_rows=4, topic="chunks", partitions=1, workers=2,
                            memory_mb=1)
    planner = KafkaEdgePlanner(schema, "kafka:1", settings, plan_id="bad", context="inline",
                               consumer_factory=cluster.consumer, producer_factory=lambda: FakeProducer(cluster),
                               admin_factory=cluster.admin, plan_path=tmp_path / "edge-plan.json")
    planner.start()
    spill = tmp_path / ".edge-plan-spill-bad"
    assert spill.is_dir() and plan_progress_path(tmp_path / "edge-plan.json").is_file()
    with pytest.raises(RuntimeError, match="invalid VISITED record"):
        planner.finish(tmp_path / "edge-plan.json")
    assert not spill.exists() and not plan_progress_path(tmp_path / "edge-plan.json").exists()
    assert "invalid VISITED record" in plan_failure_marker(tmp_path / "edge-plan.json").read_text()


def test_process_workers_plan_identically_to_inline_workers(tmp_path) -> None:
    """Spawned planner processes (the production path) with a tiny budget match the reference."""
    data = random_rows(7, 1500)
    rows = {name: [json.dumps(row) for row in items] for name, items in data.items()}
    sources = [IterableEdgeSource(name, items, size_hint=len(items)) for name, items in rows.items()]
    settings = PlanSettings(buckets=2, hub_degree=6, chunk_rows=5, topic="chunks", partitions=2, workers=2,
                            memory_mb=1, spill_dir=str(tmp_path / "spill"))
    reference_cluster = FakeCluster()
    reference_cluster.topic("chunks", 2)
    reference = plan_in_memory(source_tasks(SOCIAL, sources), settings, types=types_of(SOCIAL), plan_id="proc",
                               producer_factory=lambda: FakeProducer(reference_cluster))
    result = producer_edge_planner(SOCIAL, sources, settings, "kafka:1", plan_id="proc",
                                   producer_class=_RecordingProducer).finish(tmp_path / "proc.json")
    # Offsets come from the real producers of two processes; compare everything else and the messages.
    by_key = {chunk.key: chunk for chunk in reference.chunks}
    assert [(c.id, c.key, c.resources, c.rows, c.after, c.rank, c.bytes) for c in result.plan.chunks] == [
        (c.id, c.key, c.resources, c.rows, c.after, c.rank, c.bytes) for c in reference.chunks]
    recorded = {}
    for path in (tmp_path / "spill").glob("produced-*.jsonl"):
        for line in path.read_text().splitlines():
            item = json.loads(line)
            recorded[(item["partition"], item["offset"])] = bytes.fromhex(item["value"])
    for chunk in result.plan.chunks:
        expected = reference_cluster.logs["chunks"][by_key[chunk.key].partition][by_key[chunk.key].offset]
        assert recorded[(chunk.partition, chunk.offset)] == expected
    assert result.plan.resources.names == reference.resources.names
    assert not (tmp_path / "spill" / ".edge-plan-spill-proc").exists()


class _RecordingProducer:
    """Picklable stand-in for confluent_kafka.Producer in spawned workers: logs messages to a file."""

    def __init__(self, config):
        import os
        import tempfile
        self._offsets = {}
        self._pending = []
        directory = Path(os.environ.get("GRAPH_LOADER_TEST_PRODUCED", tempfile.gettempdir()))
        self._file = open(directory / f"produced-{os.getpid()}.jsonl", "a", encoding="utf-8")

    def produce(self, topic, *, value, partition, on_delivery):
        # Offsets are unique per process: pid-scaled so two workers never collide.
        import os
        offset = os.getpid() * 1_000_000 + self._offsets.get(partition, 0)
        self._offsets[partition] = self._offsets.get(partition, 0) + 1
        self._file.write(json.dumps({"partition": partition, "offset": offset, "value": value.hex()}) + "\n")
        self._pending.append((on_delivery, partition, offset))

    def poll(self, timeout=0):
        pending, self._pending = self._pending, []
        for callback, partition, offset in pending:
            callback(None, _Delivered(partition, offset))

    def flush(self, timeout=0):
        self.poll()
        self._file.flush()
        return 0


class _Delivered:
    def __init__(self, partition, offset):
        self._partition, self._offset = partition, offset

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


@pytest.fixture(autouse=True)
def _produced_directory(tmp_path, monkeypatch):
    (tmp_path / "spill").mkdir(exist_ok=True)
    monkeypatch.setenv("GRAPH_LOADER_TEST_PRODUCED", str(tmp_path / "spill"))


# -- building blocks ------------------------------------------------------------------------


def test_unit_chunks_match_chunk_boundaries_for_any_piece_split() -> None:
    rng = random.Random(3)
    for _ in range(300):
        count = rng.randint(1, 120)
        sizes = [rng.choice((1, 5, 30, 200, 900)) for _ in range(count)]
        max_rows, max_bytes = rng.randint(1, 40), rng.choice((50, 300, 1000, 1 << 20))
        expected = chunk_boundaries(sizes, max_rows, max_bytes)
        cuts = sorted(rng.sample(range(1, count), min(count - 1, rng.randint(0, 5)))) if count > 1 else []
        bounds = [0, *cuts, count]
        records = [bytes([65 + index % 26]) * size + b"," for index, size in enumerate(sizes)]

        def pieces():
            for start, end in zip(bounds, bounds[1:]):
                yield (np.array(sizes[start:end], dtype=np.uint32),
                       lambda a, b, start=start: records[start + a:start + b])
        got = []
        position = 0
        for rows, buffers in unit_chunks(count, pieces(), max_rows, max_bytes):
            assert b"".join(buffers) == b"".join(records[position:position + rows])
            got.append((position, position + rows))
            position += rows
        assert got == expected


def test_chunk_messages_from_parts_are_byte_identical() -> None:
    records = ['{"a":1}', '{"b":"é"}', b'{"c":3}']
    parts = [b'{"a":1},', '{"b":"é"},'.encode(), b'{"c":3},']
    assert encode_chunk_message_parts("run", "T:1:2:0:0", "T", 3, parts) == encode_chunk_message(
        "run", "T:1:2:0:0", "T", records)


def test_key_tokens_decode_to_canonical_keys() -> None:
    from datetime import datetime, timezone
    for value in ("x", "é", 7, -3, 2 ** 70, 2.5, date(2024, 2, 29),
                  datetime(2024, 1, 1, 12, tzinfo=timezone(timedelta(hours=2)))):
        decoded = decode_token(key_token(value))
        assert decoded == value and key_token(decoded) == key_token(value)
        assert decode_key(json.loads(json.dumps(__import__("src.loader.edge_plan", fromlist=["encode_key"])
                                                .encode_key(decoded)))) == value
    assert str(decode_token(key_token(-0.0))) == "0.0"


def test_flush_waits_while_delivery_progresses_and_failed_workers_drop_their_queue() -> None:
    from src.loader.edge_planner import _abandon, _flush

    class Producer:
        def __init__(self, counts):
            self.counts, self.purged = list(counts), False

        def flush(self, timeout=0):
            return self.counts.pop(0) if len(self.counts) > 1 else self.counts[0]

        def purge(self):
            self.purged = True

    now = [0.0]

    def clock():
        now[0] += 10.0
        return now[0]
    # 30 flush rounds (300 s, beyond the 120 s stall limit) that each deliver something succeed.
    assert _flush(Producer(list(range(30, -1, -1))), clock=clock) == 0
    # No delivery for 120 s: give up and report what is left.
    assert _flush(Producer([5, 4, 4]), clock=clock) == 4
    stuck = Producer([3])
    _abandon(stuck)
    assert stuck.purged


def test_progress_heartbeat_changes_only_with_progress(tmp_path) -> None:
    from src.loader.edge_planner import _Heartbeat

    state = {"phase": "scan", "rows": 0}
    beat = _Heartbeat(tmp_path / "p.progress", "p", lambda: dict(state), interval=3600)
    beat.start()
    first = (tmp_path / "p.progress").read_text()
    beat.beat()
    assert (tmp_path / "p.progress").read_text() == first  # unchanged state: no new heartbeat
    state["rows"] = 10
    beat.beat()
    assert json.loads((tmp_path / "p.progress").read_text())["rows"] == 10
    beat.stop(remove=True)
    assert not (tmp_path / "p.progress").exists()


def test_default_memory_budget_and_chunk_topic_retention() -> None:
    assert default_plan_memory_mb(4, available=34 << 30) == 2176
    assert default_plan_memory_mb(4, available=1 << 30) == 256
    assert default_plan_memory_mb(1, available=512 << 30) == 4096
    config = chunk_topic_config(16 << 20)
    assert config["retention.ms"] == "-1" and config["retention.bytes"] == "-1"
    assert int(config["max.message.bytes"]) > 16 << 20
    with pytest.raises(ValueError, match="memory"):
        PlanSettings(topic="t", memory_mb=-1).validate()
