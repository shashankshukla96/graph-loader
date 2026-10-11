"""Unit coverage for the IMDb example's producer-side edge planning (``--edge-plan``)."""
from __future__ import annotations

import csv
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import multiprocessing
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pytest

from examples.imdb.planned_edges import TsvEdgeSource, imdb_edge_planner, imdb_edge_sources
from examples.imdb.publish_and_load_nodes import (
    EDGE_SOURCES, edge_payload, ensure_plan_topic, planned_timing_key, read_write_timings, write_run_report,
)
from src.loader.edge_plan import (
    ResourceTable, assemble_plan, decode_chunk_message, planned_bucket, read_plan_file, plan_failure_marker,
)
from src.loader.edge_planner import PLAN_PRODUCER_CONFIG, PlanSettings
from src.utils.schema_loader import load_schema

SCHEMA = load_schema(Path(__file__).resolve().parents[1] / "graph_schema.yaml")
EDGES = {edge.type: edge for edge in SCHEMA.edges}


class FakeMessage:
    def __init__(self, partition, offset):
        self._partition, self._offset = partition, offset

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


class FakeProducer:
    """Records produced chunk values; offsets are unique across processes."""

    produced = 0
    instances: list = []

    def __init__(self, config=None):
        self.config = config or {}
        self.values: dict[tuple[int, int], bytes] = {}
        self._pending = []
        FakeProducer.instances.append(self)

    def produce(self, topic, *, value, partition, on_delivery):
        FakeProducer.produced += 1
        offset = os.getpid() * 1_000_000 + FakeProducer.produced
        self.values[(partition, offset)] = value
        self._pending.append((on_delivery, FakeMessage(partition, offset)))

    def poll(self, timeout=0):
        pending, self._pending = self._pending, []
        for callback, message in pending:
            callback(None, message)

    def flush(self, timeout=0):
        self.poll()
        return 0


def write_tsv(path: Path, header, rows) -> Path:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)
    return path


def files_for(paths: dict[str, Path]) -> dict[str, tuple[str, str, str]]:
    return {edge_type: (str(path), EDGE_SOURCES[edge_type][2], EDGE_SOURCES[edge_type][3])
            for edge_type, path in paths.items()}


def reference_records(edge_type: str, path: Path) -> list[str]:
    _, _, source_column, target_column = EDGE_SOURCES[edge_type]
    with open(path, encoding="utf-8", newline="") as handle:
        return [
            json.dumps(edge_payload(row, EDGES[edge_type], source_column=source_column, target_column=target_column))
            for row in csv.DictReader(handle, delimiter="\t")
        ]


def plan_files(tmp_path, paths, *, buckets, hub, chunk_rows, max_chunk_bytes=1 << 20, workers=1):
    """Plan in-process with the library planner; return (plan, chunk values, table)."""
    settings = PlanSettings(buckets=buckets, hub_degree=hub, chunk_rows=chunk_rows, max_chunk_bytes=max_chunk_bytes,
                            topic="chunks", partitions=3, workers=workers)
    producers = []

    def factory():
        producer = FakeProducer()
        producers.append(producer)
        return producer

    planner = imdb_edge_planner(SCHEMA, files_for(paths), settings, "kafka:1", context="inline",
                                producer_factory=factory, plan_id="run-1")
    result = planner.finish(tmp_path / f"plan-{workers}-{buckets}-{hub}-{chunk_rows}.json")
    values = {}
    for producer in producers:
        values.update(producer.values)
    return result.plan, values, result.plan.resources


def chained_records(plan, values):
    """Per unit ``type:source:target``: the records of its chunk chain, in chain order."""
    successors = {chunk.after: chunk for chunk in plan.chunks if chunk.after != -1}
    units = {}
    for chunk in plan.chunks:
        if chunk.after != -1:
            continue
        key = chunk.key.rsplit(":", 2)[0]
        assert key not in units, "one chain per unit"
        chain, current = [], chunk
        while current is not None:
            header, records = decode_chunk_message(values[(current.partition, current.offset)])
            assert header["chunk"] == current.key and header["type"] == current.edge_type
            assert header["run"] == "run-1" and len(records) == current.rows
            assert current.key.rsplit(":", 2)[0] == key
            chain.extend(records)
            current = successors.get(current.id)
        units[key] = chain
    return units


@pytest.fixture
def imdb_files(tmp_path):
    played = write_tsv(tmp_path / "played.tsv", ["person_id", "character", "title_id"], [
        # nm1 plays "Self" repeatedly: one pair, last row must win.
        *[["nm1", "Self", f"tt{i}"] for i in range(6)],
        *[[f"nm{i}", "Self", f"tt{i}"] for i in range(2, 12)],
        *[[f"nm{i}", f"Char{i}", "tt1"] for i in range(20)],
    ])
    episode = write_tsv(tmp_path / "episode_of.tsv", ["episode_id", "series_id", "season_number", "episode_number"], [
        ["tt5", "tt5", "1", "1"],  # self loop: one resource
        *[[f"tt{i}", "tt100", "1", str(i)] for i in range(30)],
        ["tt7", "tt100", "2", "9"],  # repeated pair
    ])
    acted = write_tsv(tmp_path / "acted_in.tsv", ["person_id", "title_id", "category", "ordering"], [
        [f"nm{i % 13}", f"tt{i % 17}", "actor" if i % 3 else "self", str(i)] for i in range(120)
    ])
    return {"PLAYED": played, "EPISODE_OF": episode, "ACTED_IN": acted}


def test_every_row_is_planned_exactly_once_with_reference_records_and_pair_order(imdb_files, tmp_path) -> None:
    plan, values, _table = plan_files(tmp_path, imdb_files, buckets=2, hub=12, chunk_rows=4)
    units = chained_records(plan, values)
    for edge_type, path in imdb_files.items():
        expected = reference_records(edge_type, path)
        planned = [json.dumps(record) for key, chain in units.items() if key.startswith(edge_type + ":")
                   for record in chain]
        # Every row exactly once, identical to the per-type publisher's record.
        assert Counter(planned) == Counter(expected)
        assert plan.types[edge_type].rows == len(expected)
    # Types without rows are planned empty.
    assert plan.types["WROTE"].rows == 0 and plan.types["WROTE"].chunks == 0
    # Records of one (source, target) pair keep file order along their unit chain.
    for key, chain in units.items():
        edge_type = key.split(":", 1)[0]
        edge = EDGES[edge_type]
        pair_of = lambda record: (record["source"][edge.source_key_property], record["target"][edge.target_key_property])  # noqa: E731
        per_pair = defaultdict(list)
        for record in chain:
            per_pair[pair_of(record)].append(record)
        reference = defaultdict(list)
        for text in reference_records(edge_type, imdb_files[edge_type]):
            record = json.loads(text)
            if pair_of(record) in per_pair:
                reference[pair_of(record)].append(record)
        assert dict(per_pair) == dict(reference)
    assert all(chunk.rows <= 4 for chunk in plan.chunks)


def test_hubs_use_total_degree_across_types_and_get_their_own_resource(imdb_files, tmp_path) -> None:
    plan, _values, table = plan_files(tmp_path, imdb_files, buckets=2, hub=12, chunk_rows=50)
    hubs = {(label, value) for label, kind, value in table.names if kind == "hub"}
    # "Self": 16 PLAYED rows; tt100: 31 EPISODE_OF rows; nm1: 7 PLAYED + 10 ACTED_IN rows (degree
    # summed across types); nm3 has 2 + 9 < 12.
    assert {("Character", "Self"), ("Title", "tt100"), ("Person", "nm1")} <= hubs
    assert ("Person", "nm3") not in hubs
    self_resource = table.resource("Character", "Self")
    assert any(self_resource in chunk.resources for chunk in plan.chunks if chunk.edge_type == "PLAYED")
    # The EPISODE_OF self loop maps to a single resource.
    assert [chunk for chunk in plan.chunks if chunk.edge_type == "EPISODE_OF" and len(chunk.resources) == 1]


def test_distributed_hub_detection_is_exact_and_sharding_keeps_units_local(imdb_files, tmp_path) -> None:
    single, _, single_table = plan_files(tmp_path, imdb_files, buckets=2, hub=12, chunk_rows=5, workers=1)
    # Two workers: ACTED_IN (the largest file) is split into source-bucket shards.
    split, values, split_table = plan_files(tmp_path, imdb_files, buckets=2, hub=12, chunk_rows=5, workers=2)
    assert split_table.names == single_table.names
    assert {name: item.rows for name, item in split.types.items()} == {
        name: item.rows for name, item in single.types.items()}
    # No unit spans two shards: every (type, source, target) appears under one shard order.
    orders = defaultdict(set)
    for chunk in split.chunks:
        edge_type, source, target, order, _seq = chunk.key.split(":")
        orders[(edge_type, source, target)].add(order)
    assert all(len(found) == 1 for found in orders.values())
    assert any(order == {"1"} for key, order in orders.items() if key[0] == "ACTED_IN")
    units = chained_records(split, values)
    acted = [json.dumps(record) for key, chain in units.items() if key.startswith("ACTED_IN:") for record in chain]
    assert Counter(acted) == Counter(reference_records("ACTED_IN", imdb_files["ACTED_IN"]))


def test_invalid_rows_fail_the_plan_with_file_and_line(tmp_path) -> None:
    path = write_tsv(tmp_path / "acted_in.tsv", ["person_id", "title_id", "category", "ordering"],
                     [["nm1", "tt1", "actor", "1"], ["nm2", "", "actor", "2"]])
    with pytest.raises(RuntimeError, match=r"acted_in.tsv:3: missing endpoint"):
        plan_files(tmp_path, {"ACTED_IN": path}, buckets=2, hub=0, chunk_rows=10)
    assert "acted_in.tsv:3" in plan_failure_marker(tmp_path / "plan-1-2-0-10.json").read_text(encoding="utf-8")


def test_sources_shard_large_files_by_source_bucket(tmp_path) -> None:
    big = tmp_path / "credited_on.tsv"
    big.write_bytes(b"x" * 750)
    files = {"CREDITED_ON": (str(big), "person_id", "title_id")}
    for index, name in enumerate(("ACTED_IN", "PLAYED", "WROTE", "DIRECTED", "KNOWN_FOR")):
        small = tmp_path / f"{name}.tsv"
        small.write_bytes(b"x" * 50)
        files[name] = (str(small), "person_id", "title_id")
    assert [(source.order, source.shard_count) for source in imdb_edge_sources(files, 1)][:1] == [(0, 1)]
    sources = imdb_edge_sources(files, 4)
    sharded = [source for source in sources if source.edge_type == "CREDITED_ON"]
    # Fair share 1000 / 4 = 250 bytes: the 750-byte file becomes 3 shards.
    assert [(source.order, source.shard_count) for source in sharded] == [(0, 3), (1, 3), (2, 3)]
    assert all(source.size_hint == int(750 * (0.25 + 0.75 / 3)) for source in sharded)
    assert all(source.shard_count == 1 for source in sources if source.edge_type != "CREDITED_ON")


def test_shard_filter_uses_the_library_bucket(tmp_path) -> None:
    path = write_tsv(tmp_path / "acted.tsv", ["person_id", "title_id", "category", "ordering"],
                     [[f"nm{i}", "tt1", "actor", str(i)] for i in range(40)])
    edge = EDGES["ACTED_IN"]
    rows = [
        list(TsvEdgeSource("ACTED_IN", str(path), "person_id", "title_id", order=index, shard_count=3,
                           buckets=8).records(edge))
        for index in range(3)
    ]
    assert sum(len(shard) for shard in rows) == 40
    for index, shard in enumerate(rows):
        assert all(planned_bucket(source, 8) % 3 == index for source, _target, _record in shard)


def test_chunk_messages_respect_the_byte_budget(imdb_files, tmp_path) -> None:
    plan, values, _ = plan_files(tmp_path, {"ACTED_IN": imdb_files["ACTED_IN"]}, buckets=1, hub=0, chunk_rows=1000,
                                 max_chunk_bytes=2048)
    assert len(plan.chunks) > 1
    for chunk in plan.chunks:
        value = values[(chunk.partition, chunk.offset)]
        assert len(value) == chunk.bytes and len(value) < 2048 + 200


def test_process_planner_writes_a_valid_plan_or_a_failure_marker(imdb_files, tmp_path) -> None:
    settings = PlanSettings(buckets=2, hub_degree=12, chunk_rows=4, topic="chunks", partitions=2, workers=2)
    context = multiprocessing.get_context("fork")
    result = imdb_edge_planner(SCHEMA, files_for(imdb_files), settings, "kafka:1", context=context,
                               producer_class=FakeProducer).finish(tmp_path / "plan.json")
    plan, digest = read_plan_file(tmp_path / "plan.json")
    assert digest == result.digest
    assert plan.total_rows == sum(len(reference_records(name, path)) for name, path in imdb_files.items())
    assert plan.settings["buckets"] == 2 and plan.stats["chunks"] == len(plan.chunks)
    assert plan.stats["source"] == "producer"
    bad = write_tsv(tmp_path / "bad.tsv", ["person_id", "title_id", "category", "ordering"], [["nm1", "", "x", "1"]])
    with pytest.raises(RuntimeError, match="missing endpoint"):
        imdb_edge_planner(SCHEMA, files_for({"ACTED_IN": bad}), settings, "kafka:1", context=context,
                          producer_class=FakeProducer).finish(tmp_path / "p2.json")
    assert "missing endpoint" in plan_failure_marker(tmp_path / "p2.json").read_text(encoding="utf-8")
    assert not (tmp_path / "p2.json").exists()


def test_planner_producer_config_isolates_chunks_and_allows_large_messages(tmp_path) -> None:
    planner = imdb_edge_planner(SCHEMA, {}, PlanSettings(max_message_bytes=16 << 20, topic="t"), "kafka:1")
    assert planner.producer_config["message.max.bytes"] > 16 << 20
    assert PLAN_PRODUCER_CONFIG["batch.size"] == 1
    assert PLAN_PRODUCER_CONFIG["enable.idempotence"] is True
    with pytest.raises(ValueError, match="chunk rows"):
        PlanSettings(chunk_rows=0, topic="t").validate()




def test_ensure_plan_topic_creates_with_large_messages_and_checks_existing() -> None:
    admin = MagicMock()
    admin.list_topics.return_value = SimpleNamespace(topics={})
    future = MagicMock()
    admin.create_topics.return_value = {"chunks": future}
    with patch("examples.imdb.publish_and_load_nodes.AdminClient", return_value=admin):
        ensure_plan_topic("kafka:1", "chunks", 4, 16 << 20)
    (new_topic,), = admin.create_topics.call_args.args
    assert new_topic.topic == "chunks" and new_topic.num_partitions == 4
    assert new_topic.config["max.message.bytes"] == str(16 << 20)
    assert new_topic.config["retention.ms"] == "-1" and new_topic.config["retention.bytes"] == "-1"
    admin.list_topics.return_value = SimpleNamespace(topics={"chunks": SimpleNamespace(partitions={0: 1})})
    with patch("examples.imdb.publish_and_load_nodes.AdminClient", return_value=admin), \
            pytest.raises(ValueError, match="has 1 partition"):
        ensure_plan_topic("kafka:1", "chunks", 4, 16 << 20)
    admin.incremental_alter_configs.return_value = {"x": MagicMock()}
    with patch("examples.imdb.publish_and_load_nodes.AdminClient", return_value=admin):
        ensure_plan_topic("kafka:1", "chunks", 1, 16 << 20)
    admin.incremental_alter_configs.assert_called_once()


def test_planned_write_timings_are_keyed_per_edge_type_and_reported(tmp_path) -> None:
    timings = tmp_path / "timings"
    timings.mkdir()
    (timings / "edge-planned-w0.log").write_text(
        "stage=edge-write edge=ACTED_IN topic=imdb-edge-chunks planned=1 writer=w0 chunk=0 records=10 write_ms=5.0\n"
        "stage=edge-write edge=PLAYED topic=imdb-edge-chunks planned=1 writer=w0 chunk=1 records=4 write_ms=2.0\n"
        "stage=edge-write edge=ACTED_IN topic=imdb-edge-chunks planned=1 writer=w0 chunk=2 records=5 write_ms=1.0\n",
        encoding="utf-8",
    )
    summaries = read_write_timings(timings)
    assert summaries[planned_timing_key("imdb-edge-chunks", "ACTED_IN")].records == 15
    (timings / "planned-summary.json").write_text(json.dumps({
        "dispatch_seconds": 1.5, "first_grant_utc": "a", "last_done_utc": "b", "phase_seconds": 3.0,
        "dispatcher_stats": {"grants": 3}, "types": {"ACTED_IN": {"rows": 15, "chunks": 2, "write_ms": 6.0,
                                                                   "first_grant": 100.0, "last_done": 101.5}},
    }), encoding="utf-8")
    edge_plan = {
        "settings": {"buckets": 8, "hub_degree": 5000, "chunk_rows": 5000, "planner_workers": 4},
        "stats": {"hubs": 85, "resources": 165, "units": 3, "chunks": 3, "rows": 19, "scan_seconds": 1.0,
                  "hub_seconds": 0.1, "emit_seconds": 0.5, "max_chunk_message_bytes": 900,
                  "mean_chunk_message_bytes": 500},
        "types": {"ACTED_IN": {"rows": 15, "chunks": 2, "units": 1}, "PLAYED": {"rows": 4, "chunks": 1, "units": 1}},
        "writers": 10, "topic": "imdb-edge-chunks", "partitions": 4, "ready_seconds": 7.0,
    }
    report = write_run_report(
        tmp_path / "reports", started_at=datetime(2026, 9, 29, tzinfo=timezone.utc), total_seconds=9.0,
        partitions=4, publish_workers=4, consumer_workers=4, topic_setup_seconds=0.1, parallel_phase_seconds=1.0,
        image_build_seconds=None,
        topic_results=[("ACTED_IN", "imdb-edge-chunks", 15, 0.4), ("PLAYED", "imdb-edge-chunks", 4, 0.2)],
        consumer_lifecycle_seconds=5.0, consumer_exit_code=0, write_timings=summaries,
        timing_directory=timings, edge_plan=edge_plan,
    ).read_text(encoding="utf-8")
    assert "- Edge plan: enabled (8 buckets/label, hub degree >= 5000 (85 hubs)" in report
    assert "10 writers" in report and "first grant to last commit 1.500s" in report
    assert "| ACTED_IN | `imdb-edge-chunks` | 15 | 2 | 0.006s |" in report
    assert "| ACTED_IN | 15 | 15 | 2 | 1 | 0.006s |" in report
    assert "- Relationship records sent to Kafka: 19" in report


def test_main_plans_edges_and_passes_the_plan_to_the_bulk_cli(tmp_path) -> None:
    from concurrent.futures import ThreadPoolExecutor

    from examples.imdb import publish_and_load_nodes as module

    subset = tmp_path / "subset"
    (subset / "graph" / "nodes").mkdir(parents=True)
    (subset / "graph" / "relationships").mkdir(parents=True)
    for _label, (_topic, filename) in module.NODE_SOURCES.items():
        (subset / "graph" / "nodes" / filename).write_text("", encoding="utf-8")
    for _edge, (_topic, filename, _source, _target) in module.EDGE_SOURCES.items():
        (subset / "graph" / "relationships" / filename).write_text("", encoding="utf-8")
    published, captured = [], {}

    def no_publish(task, bootstrap, config):
        published.append(task.topic)
        return SimpleNamespace(label=task.label, topic=task.topic, count=0, started=0.0, completed=0.0,
                               shard_index=0, shard_count=1)

    class FakePlanner:
        def __init__(self, schema, files, settings, bootstrap, **kwargs):
            captured["planner"] = self
            self.files, self.settings, self.lanes = files, settings, [files]
            self.tasks = [SimpleNamespace(edge_type=edge.type, source_label=edge.nodes.source,
                                          target_label=edge.nodes.target) for edge in schema.edges]
            self.events = []

        def start(self):
            self.events.append("start")

        def finish(self, plan_path):
            self.events.append("finish")
            table = ResourceTable(["Person", "Title"], self.settings.buckets)
            plan = assemble_plan(
                plan_id="p", topic=self.settings.topic, partitions=self.settings.partitions, resources=table,
                types={task.edge_type: (task.source_label, task.target_label, 0) for task in self.tasks
                       if {task.source_label, task.target_label} <= {"Person", "Title"}},
                specs=[], settings={"buckets": self.settings.buckets},
            )
            return SimpleNamespace(plan=plan, digest="d", path=plan_path, total_seconds=0.1, type_seconds={})

        def close(self):
            self.events.append("close")

    class FakeProcess:
        def wait(self, timeout=None):
            captured["waited"] = True
            return 0

    def fake_start(command, environment, timing_directory):
        captured["command"], captured["timing_directory"] = command, timing_directory
        assert captured["planner"].events == ["start"]  # the CLI starts before planning finishes
        return FakeProcess()

    argv = [
        "publish_and_load_nodes.py", "--subset-dir", str(subset), "--skip-image-build", "--edge-plan",
        "--plan-writers", "12", "--plan-buckets", "4", "--report-dir", str(tmp_path / "reports"),
    ]
    with patch.object(sys, "argv", argv), patch.object(module, "load_dotenv"), \
            patch.object(module, "ensure_topics") as ensure_topics, patch.object(module, "ensure_plan_topic") as ensure_plan, \
            patch.object(module, "require_empty_topics") as require_empty, \
            patch.object(module, "run_publish_task", no_publish), \
            patch.object(module, "imdb_edge_planner", FakePlanner), \
            patch.object(module, "ProcessPoolExecutor", lambda max_workers, mp_context=None: ThreadPoolExecutor(max_workers)), \
            patch.object(module, "start_bulk_loader", fake_start):
        assert module.main() == 0
    command = captured["command"]
    plan_path = Path(command[command.index("--edge-plan") + 1])
    assert plan_path == captured["timing_directory"] / "edge-plan.json"
    assert command[command.index("--plan-writers") + 1] == "12"
    assert captured["planner"].events == ["start", "finish", "close"] and captured["waited"]
    assert captured["planner"].settings.buckets == 4
    # Only node topics are published record by record; per-type edge topics are untouched.
    edge_topics = {topic for topic, *_ in module.EDGE_SOURCES.values()}
    assert published and not edge_topics & set(published)
    assert not edge_topics & set(ensure_topics.call_args.args[1])
    ensure_plan.assert_called_once()
    assert "imdb-edge-chunks" in require_empty.call_args.args[1]
    report = next((tmp_path / "reports").glob("imdb-load-*.md")).read_text(encoding="utf-8")
    assert "- Edge plan: enabled (4 buckets/label" in report and "12 writers" in report
    assert "## Planned edge load by type" in report


def test_edge_plan_and_leases_are_mutually_exclusive(tmp_path, capsys) -> None:
    from examples.imdb import publish_and_load_nodes as module

    subset = tmp_path / "subset"
    (subset / "graph" / "nodes").mkdir(parents=True)
    (subset / "graph" / "relationships").mkdir(parents=True)
    for _label, (_topic, filename) in module.NODE_SOURCES.items():
        (subset / "graph" / "nodes" / filename).write_text("", encoding="utf-8")
    for _edge, (_topic, filename, _source, _target) in module.EDGE_SOURCES.items():
        (subset / "graph" / "relationships" / filename).write_text("", encoding="utf-8")
    argv = ["x", "--subset-dir", str(subset), "--edge-plan", "--edge-leases"]
    with patch.object(sys, "argv", argv), patch.object(module, "load_dotenv"), pytest.raises(SystemExit):
        module.main()
    assert "mutually exclusive" in capsys.readouterr().err


def test_kafka_plan_source_publishes_edge_topics_and_lets_the_loader_plan(tmp_path) -> None:
    from concurrent.futures import ThreadPoolExecutor

    from examples.imdb import publish_and_load_nodes as module

    subset = tmp_path / "subset"
    (subset / "graph" / "nodes").mkdir(parents=True)
    (subset / "graph" / "relationships").mkdir(parents=True)
    for _label, (_topic, filename) in module.NODE_SOURCES.items():
        (subset / "graph" / "nodes" / filename).write_text("", encoding="utf-8")
    for _edge, (_topic, filename, _source, _target) in module.EDGE_SOURCES.items():
        (subset / "graph" / "relationships" / filename).write_text("", encoding="utf-8")
    published, captured = [], {}

    def no_publish(task, bootstrap, config):
        published.append(task.topic)
        return SimpleNamespace(label=task.label, topic=task.topic, count=0, started=0.0, completed=0.0,
                               shard_index=0, shard_count=1)

    def fake_run(command, environment, report_directory):
        captured["command"] = command
        directory = tmp_path / "timings"
        directory.mkdir()
        table = ResourceTable(["Person", "Title"], 8)
        plan = assemble_plan(plan_id="p", topic="graph-loader-edge-chunks-p", partitions=4, resources=table,
                             types={"ACTED_IN": ("Person", "Title", 0)}, specs=[],
                             settings={"buckets": 8, "planner_workers": 4},
                             stats={"source": "kafka", "rows": 0, "scan_records_per_second": 0})
        from src.loader.edge_plan import write_plan_file
        write_plan_file(directory / "edge-plan.json", plan)
        return 0, {}, directory

    argv = [
        "publish_and_load_nodes.py", "--subset-dir", str(subset), "--skip-image-build", "--edge-plan",
        "--plan-source", "kafka", "--plan-chunk-rows", "700", "--report-dir", str(tmp_path / "reports"),
    ]
    with patch.object(sys, "argv", argv), patch.object(module, "load_dotenv"), \
            patch.object(module, "ensure_topics") as ensure_topics, patch.object(module, "ensure_plan_topic") as ensure_plan, \
            patch.object(module, "require_empty_topics"), patch.object(module, "run_publish_task", no_publish), \
            patch.object(module, "imdb_edge_planner") as planner, \
            patch.object(module, "ProcessPoolExecutor", lambda max_workers, mp_context=None: ThreadPoolExecutor(max_workers)), \
            patch.object(module, "run_bulk_loader_with_timings", fake_run):
        assert module.main() == 0
    command = captured["command"]
    # ``--edge-plan`` without a file: the loader plans from the edge topics.
    index = command.index("--edge-plan")
    assert index == len(command) - 1 or command[index + 1].startswith("--")
    assert command[command.index("--plan-chunk-rows") + 1] == "700" and "--plan-buckets" not in command
    planner.assert_not_called()
    ensure_plan.assert_not_called()
    edge_topics = {topic for topic, *_ in module.EDGE_SOURCES.values()}
    assert edge_topics <= set(published) and edge_topics <= set(ensure_topics.call_args.args[1])
    report = next((tmp_path / "reports").glob("imdb-load-*.md")).read_text(encoding="utf-8")
    assert "by the loader from the per-type edge topics" in report
