"""Unit tests for the planned-edge resource model, chunking, ranking and plan file."""
from __future__ import annotations

import json
import random
import zlib

import pytest

from src.loader.edge_plan import (
    ChunkSpec, EdgePlan, EdgePlanError, PlannedChunk, ResourceTable, assemble_plan, chunk_boundaries,
    chunk_cost, decode_chunk_message, encode_chunk_message, key_token, plan_failure_marker, planned_bucket,
    rank_chunks, read_plan_file, resource_pair, select_hubs, split_sizes, write_plan_file,
)


def test_key_tokens_are_typed_and_buckets_are_process_independent() -> None:
    assert key_token("1") != key_token(1) != key_token(1.0)
    assert key_token("tt1") == b"s:tt1"
    assert planned_bucket("tt1", 8) == zlib.crc32(b"s:tt1") % 8
    assert {planned_bucket(f"nm{i}", 8) for i in range(200)} == set(range(8))


def test_resource_table_gives_hubs_their_own_resource_and_buckets_the_rest() -> None:
    table = ResourceTable(["Title", "Genre"], 4, {"Genre": ["Drama", "Comedy"]})
    # Labels sorted: Genre buckets 0-3, Genre hubs (token order) 4-5, Title buckets 6-9.
    assert table.count == 10 and table.hub_count == 2
    assert table.names[4] == ("Genre", "hub", "Comedy") and table.names[5] == ("Genre", "hub", "Drama")
    assert table.resource("Genre", "Drama") == 5
    assert table.resource("Genre", "Horror") == planned_bucket("Horror", 4)
    assert table.resource("Title", "tt9") == 6 + planned_bucket("tt9", 4)
    assert table.label_of(table.resource("Title", "tt9")) == "Title"
    copy = ResourceTable.from_json(json.loads(json.dumps(table.to_json())))
    assert copy.names == table.names
    with pytest.raises(EdgePlanError, match="not in the plan"):
        table.resource("Person", "nm1")
    with pytest.raises(EdgePlanError, match="duplicate hub"):
        ResourceTable(["Genre"], 2, {"Genre": ["Drama", "Drama"]})
    with pytest.raises(EdgePlanError, match="not plan labels"):
        ResourceTable(["Genre"], 2, {"Title": ["tt1"]})
    with pytest.raises(EdgePlanError, match="bucket count"):
        ResourceTable(["Genre"], 0)


def test_select_hubs_uses_total_degree_threshold_and_zero_disables() -> None:
    degrees = {"Genre": {"Drama": 10, "Short": 3}, "Title": {"tt1": 9}}
    assert select_hubs(degrees, 9) == {"Genre": ["Drama"], "Title": ["tt1"]}
    assert select_hubs(degrees, 0) == {}


def test_split_sizes_and_byte_budget_boundaries() -> None:
    assert split_sizes(0, 5) == []
    assert split_sizes(11, 5) == [4, 4, 3]
    assert chunk_boundaries([1] * 11, 5, 10_000) == [(0, 4), (4, 8), (8, 11)]
    # Byte budget: 10 rows of 3+1 bytes, at most 8 bytes per chunk -> pairs.
    assert chunk_boundaries([3] * 10, 100, 8) == [(0, 2), (2, 4), (4, 6), (6, 8), (8, 10)]
    # A single oversized row still gets its own chunk.
    assert chunk_boundaries([1, 50, 1], 100, 10) == [(0, 1), (1, 2), (2, 3)]


def _spec(edge_type, unit, seq, resources, rows, location):
    return ChunkSpec(f"{edge_type}:{unit}:{seq}", edge_type, (edge_type, unit), seq, resources, rows, *location)


def test_rank_is_bottleneck_resource_first_and_keeps_unit_chains_consecutive() -> None:
    specs = [
        _spec("A", "light", 0, (0, 1), 10, (0, 0)),
        _spec("A", "hot", 1, (2, 3), 5000, (0, 1)),
        _spec("A", "hot", 0, (2, 3), 5000, (0, 2)),
        _spec("A", "hot2", 0, (3, 4), 100, (0, 3)),
    ]
    ranks = rank_chunks(specs)
    # Resource 3 carries the most work: both units touching it go first, the
    # heavier one before the lighter; the chain keeps seq order.
    assert ranks == [3, 1, 0, 2]
    with pytest.raises(EdgePlanError, match="disagree"):
        rank_chunks([_spec("A", "u", 0, (0,), 1, (0, 0)), _spec("A", "u", 1, (1,), 1, (0, 1))])


def _table():
    return ResourceTable(["Person", "Title"], 2)


def _plan_specs():
    table = _table()
    person, title = table.bucket_resource("Person", 0), table.bucket_resource("Title", 1)
    return table, [
        ChunkSpec("ACTED_IN:u1:0", "ACTED_IN", ("ACTED_IN", 1), 0, resource_pair(person, title), 3, 0, 5),
        ChunkSpec("ACTED_IN:u1:1", "ACTED_IN", ("ACTED_IN", 1), 1, resource_pair(person, title), 2, 1, 0),
        ChunkSpec("EPISODE_OF:u2:0", "EPISODE_OF", ("EPISODE_OF", 2), 0, resource_pair(title, title), 1, 0, 6),
    ]


def test_assemble_plan_orders_ids_by_rank_chains_units_and_round_trips(tmp_path) -> None:
    table, specs = _plan_specs()
    plan = assemble_plan(
        plan_id="run-1", topic="chunks", partitions=2, resources=table,
        types={"ACTED_IN": ("Person", "Title", 5), "EPISODE_OF": ("Title", "Title", 1), "EMPTY": ("Person", "Person", 0)},
        specs=specs,
    )
    assert [chunk.rank for chunk in plan.chunks] == [0, 1, 2]
    assert plan.chunks[0].key == "ACTED_IN:u1:0" and plan.chunks[1].after == 0 and plan.chunks[2].after == -1
    assert plan.chunks[2].resources == (table.bucket_resource("Title", 1),)
    assert plan.types["ACTED_IN"].chunks == 2 and plan.types["ACTED_IN"].units == 1
    assert plan.types["EMPTY"].rows == 0 and plan.total_rows == 6
    path = tmp_path / "plan.json"
    digest = write_plan_file(path, plan)
    loaded, loaded_digest = read_plan_file(path)
    assert loaded_digest == digest and loaded.to_json() == plan.to_json()
    assert plan_failure_marker(path).name == "plan.json.failed"


def test_assemble_plan_rejects_row_totals_that_differ_from_the_publisher_count() -> None:
    table, specs = _plan_specs()
    with pytest.raises(EdgePlanError, match="chunks hold 5 rows; plan declares 6"):
        assemble_plan(plan_id="r", topic="t", partitions=2, resources=table,
                      types={"ACTED_IN": ("Person", "Title", 6), "EPISODE_OF": ("Title", "Title", 1)}, specs=specs)
    with pytest.raises(EdgePlanError, match="unplanned edge type"):
        assemble_plan(plan_id="r", topic="t", partitions=2, resources=table,
                      types={"ACTED_IN": ("Person", "Title", 5)}, specs=specs)


def _valid_plan() -> EdgePlan:
    table, specs = _plan_specs()
    return assemble_plan(plan_id="r", topic="t", partitions=2, resources=table,
                         types={"ACTED_IN": ("Person", "Title", 5), "EPISODE_OF": ("Title", "Title", 1)}, specs=specs)


@pytest.mark.parametrize("mutate, message", [
    (lambda c: {"partition": 5}, "invalid Kafka location"),
    (lambda c: {"resources": (0,)}, "do not match"),
    (lambda c: {"resources": (9, 1)}, "sorted distinct"),
    (lambda c: {"rows": 0}, "empty"),
    (lambda c: {"after": 1}, "earlier chunk"),
])
def test_plan_validation_rejects_tampered_chunks(mutate, message) -> None:
    plan = _valid_plan()
    first = plan.chunks[0]
    plan.chunks[0] = PlannedChunk(**{**first.__dict__, **mutate(first)})
    with pytest.raises(EdgePlanError, match=message):
        plan.validate()


def test_plan_validation_rejects_duplicate_locations_ranks_and_branching_chains() -> None:
    plan = _valid_plan()
    plan.chunks[2] = PlannedChunk(**{**plan.chunks[2].__dict__, "partition": 0, "offset": 5})
    with pytest.raises(EdgePlanError, match="shares a Kafka location"):
        plan.validate()
    plan = _valid_plan()
    plan.chunks[2] = PlannedChunk(**{**plan.chunks[2].__dict__, "rank": 0})
    with pytest.raises(EdgePlanError, match="permutation"):
        plan.validate()
    payload = _valid_plan().to_json()
    payload["format"] = "other"
    with pytest.raises(EdgePlanError, match="not a"):
        EdgePlan.from_json(payload)


def test_chunk_message_carries_exact_records_and_header() -> None:
    records = ['{"source": {"k": "a"}, "target": {"k": "b"}, "properties": {"p": "x"}}'] * 3
    value = encode_chunk_message("run-1", "T:1:2:0:0", "T", records)
    header, decoded = decode_chunk_message(value)
    assert header == {"format": 1, "run": "run-1", "chunk": "T:1:2:0:0", "type": "T", "count": 3}
    assert decoded == [json.loads(item) for item in records]
    with pytest.raises(EdgePlanError, match="record count"):
        decode_chunk_message(value.replace(b'"count":3', b'"count":4'))
    with pytest.raises(EdgePlanError, match="not JSON"):
        decode_chunk_message(b"{")
    with pytest.raises(EdgePlanError, match="empty"):
        decode_chunk_message(None)


def test_chunk_cost_model() -> None:
    assert chunk_cost(5000) == pytest.approx(0.003 + 0.075)


def test_randomized_plans_assign_consistent_chains_and_ranks() -> None:
    rng = random.Random(7)
    table = ResourceTable(["A", "B"], 4)
    for _ in range(20):
        specs, offset = [], 0
        for unit in range(rng.randint(1, 30)):
            source = table.bucket_resource("A", rng.randrange(4))
            target = table.bucket_resource("B", rng.randrange(4))
            for seq in range(rng.randint(1, 4)):
                specs.append(ChunkSpec(f"T:{unit}:{seq}", "T", ("T", unit), seq, resource_pair(source, target),
                                       rng.randint(1, 50), 0, offset))
                offset += 1
        rng.shuffle(specs)
        plan = assemble_plan(plan_id="r", topic="t", partitions=1, resources=table,
                             types={"T": ("A", "B", sum(spec.rows for spec in specs))}, specs=specs)
        by_key = {chunk.key: chunk for chunk in plan.chunks}
        for spec in specs:
            chunk = by_key[spec.key]
            if spec.seq == 0:
                assert chunk.after == -1
            else:
                previous = plan.chunks[chunk.after]
                assert previous.key == f"T:{spec.unit[1]}:{spec.seq - 1}" and previous.rank < chunk.rank
