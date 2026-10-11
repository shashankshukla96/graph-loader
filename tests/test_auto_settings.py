"""Derived scheduling parameters, YAML contract checks and the auto-planned CLI path."""
from __future__ import annotations

import argparse
import copy
from datetime import date, datetime, timezone
import logging
import math
from pathlib import Path
import random
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import yaml

from multi_pair_support import likes_schema_dict
from src.cli import AUTO_EDGE_PLAN, _resolve_node_replicas, handle_start
from src.loader.edge_planner import self_referencing_rows
from src.loader.edge_records import EdgeRecordValidationError, normalize_edge_record
from src.models.schema import GraphSchema
from src.loader.planned_writer import compile_record_normalizer
from src.orchestrator import auto_settings
from src.orchestrator.planned_edges import start_kafka_edge_planner
from src.utils.cypher_generator import generate_node_ddl
from src.utils.schema_loader import SchemaLoadError, load_schema

SOCIAL_PATH = Path(__file__).resolve().parents[1] / "examples" / "social" / "graph_schema.yaml"
SOCIAL = load_schema(SOCIAL_PATH)
EDGES = {edge.type: edge for edge in SOCIAL.edges}


def test_writers_follow_neo4j_cores_with_a_documented_fallback() -> None:
    assert auto_settings.default_plan_writers(8) == 10
    assert auto_settings.default_plan_writers(4) == 6
    assert auto_settings.default_plan_writers(None) == auto_settings.DEFAULT_NEO4J_CORES + 2
    assert auto_settings.default_plan_writers(200) == 64
    assert auto_settings.default_plan_workers(2) == 2 and auto_settings.default_plan_workers(32) == 4


def test_plan_parameters_scale_with_data_volume_and_writers() -> None:
    # Small inputs: few buckets, so units still average >= 500 rows.
    small = auto_settings.derive_plan_parameters(10_000, 6)
    assert small == {"buckets": 4, "hub_degree": 500, "chunk_rows": 500}
    assert auto_settings.derive_plan_parameters(45_000, 10, edge_types=14)["buckets"] == 2
    medium = auto_settings.derive_plan_parameters(300_000, 6, edge_types=6)
    assert medium == {"buckets": 8, "hub_degree": 2500, "chunk_rows": 2500}
    large = auto_settings.derive_plan_parameters(20_000_000, 20, edge_types=14)
    assert large == {"buckets": 20, "hub_degree": 5000, "chunk_rows": 5000}
    assert auto_settings.derive_plan_parameters(0, 10) == {"buckets": 2, "hub_degree": 5000, "chunk_rows": 5000}
    explicit = auto_settings.derive_plan_parameters(10_000, 6, buckets=3, hub_degree=0, chunk_rows=77)
    assert explicit == {"buckets": 3, "hub_degree": 0, "chunk_rows": 77}


def _legacy_buckets(records: int, writers: int, edge_types: int) -> int:
    """The bucket rule before self-referencing rows counted (df2e5ea), verbatim."""
    writers = max(1, writers)
    by_writers = max(8, min(64, writers))
    by_volume = math.isqrt(max(0, records) // (max(1, edge_types) * 500))
    return max(2, min(by_writers, by_volume))


@pytest.mark.parametrize("records", [0, 1, 999, 10_000, 45_000, 300_000, 1_260_000, 8_895_248, 20_000_000, 10**9])
@pytest.mark.parametrize("writers", [1, 2, 6, 10, 18, 33, 64])
@pytest.mark.parametrize("edge_types", [1, 3, 6, 14])
def test_bipartite_loads_get_exactly_the_previous_buckets(records, writers, edge_types) -> None:
    legacy = _legacy_buckets(records, writers, edge_types)
    params = auto_settings.derive_plan_parameters(records, writers, edge_types=edge_types)
    assert params["buckets"] == legacy
    # No self-referencing rows, or a share at/below the threshold: unchanged too.
    for self_ref in (0, int(records * auto_settings.SELF_REF_SHARE_THRESHOLD)):
        assert auto_settings.derive_plan_parameters(
            records, writers, edge_types=edge_types, self_ref_records=self_ref) == params
        buckets, reason = auto_settings.derive_plan_buckets(records, writers, edge_types=edge_types,
                                                            self_ref_records=self_ref)
        assert buckets == legacy and reason.endswith("writers")


def test_imdb_boundary_keeps_its_buckets() -> None:
    """IMDb 20k (planned, Kafka): EPISODE_OF Title->Title is the only same-label type, 0.26% of the rows."""
    imdb = load_schema(Path(__file__).resolve().parents[1] / "examples" / "imdb" / "graph_schema.yaml")
    rows = {"ACTED_IN": 1314866, "AVAILABLE_IN_REGION": 835372, "CREDITED_ON": 2225639, "DIRECTED": 191304,
            "EPISODE_OF": 22683, "HAS_ALTERNATIVE_TITLE": 1037355, "HAS_GENRE": 352727, "HAS_PROFESSION": 495363,
            "HAS_RATING": 86626, "HAS_TITLE_TYPE": 200000, "IN_LANGUAGE": 289600, "KNOWN_FOR": 365890,
            "PLAYED": 1157718, "WROTE": 320105}
    per_topic = {edge.topic: rows[edge.type] for edge in imdb.edges}
    self_ref = self_referencing_rows(imdb.edges, per_topic)
    assert self_ref == 22683
    records = sum(per_topic.values())
    assert records == 8_895_248
    for writers in (6, 10, 18, 66):
        buckets, reason = auto_settings.derive_plan_buckets(records, writers, edge_types=14,
                                                            self_ref_records=self_ref)
        assert buckets == _legacy_buckets(records, writers, 14) and reason.endswith("writers")
    assert auto_settings.derive_plan_parameters(records, 10, edge_types=14, self_ref_records=self_ref) == {
        "buckets": 10, "hub_degree": 5000, "chunk_rows": 5000}


def test_self_referencing_share_raises_the_bucket_count() -> None:
    derive = auto_settings.derive_plan_buckets
    # One self-referencing type only: twice the writers, so ~K two-bucket chunks fit.
    assert derive(2_000_000, 10, edge_types=1, self_ref_records=2_000_000) == (20, "self_ref_share=1.00")
    # Self-reference-dominant social variant (FRIEND_OF ~79% of 1.26M rows over 6 types; cap isqrt(420) = 20).
    assert derive(1_260_000, 10, edge_types=6, self_ref_records=1_000_000) == (18, "self_ref_share=0.79")
    # Mixed: a third of the rows -> K * 1.33.
    assert derive(5_000_000, 10, edge_types=4, self_ref_records=1_650_000) == (13, "self_ref_share=0.33")
    # Just above the threshold: one bucket more (round half up of 11.05).
    assert derive(5_000_000, 10, edge_types=4, self_ref_records=525_000) == (11, "self_ref_share=0.10")
    # Only the bucket count moves: chunk rows and the hub degree follow R and K as before.
    plain = auto_settings.derive_plan_parameters(1_260_000, 10, edge_types=6)
    heavy = auto_settings.derive_plan_parameters(1_260_000, 10, edge_types=6, self_ref_records=1_000_000)
    assert (heavy["chunk_rows"], heavy["hub_degree"]) == (plain["chunk_rows"], plain["hub_degree"]) == (5000, 5000)
    assert (plain["buckets"], heavy["buckets"]) == (10, 18)


def test_self_referencing_buckets_keep_the_caps() -> None:
    derive = auto_settings.derive_plan_buckets
    assert derive(10**9, 64, edge_types=1, self_ref_records=10**9) == (64, "self_ref_share=1.00")  # max 64
    assert derive(10**9, 3, edge_types=1, self_ref_records=10**9) == (8, "self_ref_share=1.00")  # min 8
    # The volume cap still wins: social 20k (340k rows, 6 types, FRIEND_OF ~25%) stays at isqrt(113) = 10.
    assert derive(340_000, 10, edge_types=6, self_ref_records=84_000) == (10, "volume_cap=10,self_ref_share=0.25")
    assert derive(45_000, 10, edge_types=14, self_ref_records=45_000) == (2, "volume_cap=2,self_ref_share=1.00")
    assert derive(0, 10, self_ref_records=0) == (2, "volume_cap=2,writers")
    # More self-referencing records than records (inconsistent input) is clamped to a share of 1.
    assert derive(2_000_000, 10, self_ref_records=5_000_000) == (20, "self_ref_share=1.00")


def test_explicit_buckets_always_win_over_the_self_referencing_rule() -> None:
    assert auto_settings.derive_plan_parameters(
        2_000_000, 10, buckets=5, self_ref_records=2_000_000)["buckets"] == 5


def test_self_referencing_rows_count_same_label_pairs_only() -> None:
    assert self_referencing_rows(SOCIAL.edges, {edge.topic: 100 for edge in SOCIAL.edges}) == 100  # FRIEND_OF
    assert self_referencing_rows(SOCIAL.edges, {"social-bought": 7, "social-works-at": 9}) == 0
    # A multi-pair type: only its Person->Person topic counts, the Person->Post topic does not.
    data = likes_schema_dict()
    data["edges"][0]["topics"] = [
        {"name": "likes-posts", "nodes": {"source": "Person", "target": "Post"},
         "source_key_property": "person_id", "target_key_property": "post_id"},
        {"name": "likes-people", "nodes": {"source": "Person", "target": "Person"},
         "source_key_property": "person_id", "target_key_property": "person_id"},
        {"name": "likes-people-backfill", "nodes": {"source": "Person", "target": "Person"},
         "source_key_property": "person_id", "target_key_property": "person_id"},
    ]
    schema = GraphSchema.model_validate(data)
    (edge,) = schema.edges
    assert edge.is_multi_pair
    rows = {"likes-posts": 600, "likes-people": 300, "likes-people-backfill": 100}
    assert self_referencing_rows(schema.edges, rows) == 400
    # 400 of 1000 rows: K * 1.4 buckets (volume cap isqrt(1e6 * 1000 / 500) is far above).
    assert auto_settings.derive_plan_buckets(10**6, 10, edge_types=1,
                                             self_ref_records=400 * 1000) == (14, "self_ref_share=0.40")


def test_neo4j_core_probe_reads_the_jvm_and_tolerates_failures() -> None:
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    session.run.return_value.single.return_value = {"cores": 12}
    assert auto_settings.probe_neo4j_cores(driver) == 12
    session.run.side_effect = RuntimeError("no such procedure")
    assert auto_settings.probe_neo4j_cores(driver) is None


def test_node_replicas_use_yaml_values_else_topic_sizes(tmp_path) -> None:
    raw = yaml.safe_load(SOCIAL_PATH.read_text())
    raw["nodes"][1]["replicas"] = 3  # Company: explicit
    path = tmp_path / "schema.yaml"
    path.write_text(yaml.safe_dump(raw))
    schema = load_schema(path)
    sizes = {"social-person": (250_000, 12), "social-company": (5, 1), "social-product": (50_000, 2),
             "social-city": (25, 1)}
    replicas = auto_settings.auto_node_replicas(schema.nodes, sizes)
    assert replicas["Person"] == 8  # one per partition, capped
    assert replicas["Company"] == 3  # explicit YAML value wins
    assert replicas["Product"] == 2 and replicas["City"] == 1
    assert replicas["Day"] == 1  # topic unknown: YAML default
    assert auto_settings.auto_node_replicas(schema.nodes, sizes, min_records=0)["Person"] == 1
    args = argparse.Namespace(mode="bulk", node_replicas_min_records=10_000, max_node_replicas=4)
    with patch.object(auto_settings, "topic_sizes", return_value=sizes):
        assert _resolve_node_replicas(schema, args)["Person"] == 4
    with patch.object(auto_settings, "topic_sizes", side_effect=RuntimeError("down")):
        assert _resolve_node_replicas(schema, args)["Person"] == 1
    args.mode = "stream"
    assert _resolve_node_replicas(schema, args)["Person"] == 1


def _mutated(tmp_path, change):
    raw = yaml.safe_load(SOCIAL_PATH.read_text())
    change(raw)
    path = tmp_path / "bad.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


@pytest.mark.parametrize("change, message", [
    (lambda raw: raw["edges"][0].update(topic="social-bought"), "declared by both edge 'WORKS_AT' and edge 'BOUGHT'"),
    (lambda raw: raw["edges"][0].update(topic="social-person"), "node 'Person' and edge 'WORKS_AT'"),
    (lambda raw: raw["nodes"][1].update(topic="social-person"), "node 'Person' and node 'Company'"),
    (lambda raw: raw["edges"][0]["nodes"].update(target="Employer"), "does not name a declared node"),
    (lambda raw: raw["edges"][0].update(target_key_property="name"), "must match node 'Company' key_property"),
    (lambda raw: raw["nodes"][0]["properties"]["person_id"].update(type="uuid"), "person_id"),
    (lambda raw: raw["edges"][0].update(topic="  "), "topic must not be blank"),
    (lambda raw: raw.setdefault("loading", {}).update(coordination={"topic": "social-city"}),
     "Coordination topic 'social-city' is also the data topic of node 'City'"),
    (lambda raw: raw["nodes"][0].update(dead_letter={"topic": "social-company"}),
     "Dead-letter topic 'social-company' for node 'Person' collides with node 'Company'"),
    (lambda raw: (raw["nodes"][0].update(dead_letter={"enabled": True}),
                  raw["edges"][0].update(dead_letter={"topic": "social-person.dlq"})),
     "Dead-letter topic 'social-person.dlq' is declared by both node 'Person' and edge 'WORKS_AT'"),
])
def test_schema_contract_errors_are_clear(tmp_path, change, message) -> None:
    with pytest.raises(SchemaLoadError) as failure:
        load_schema(_mutated(tmp_path, change))
    assert message in str(failure.value)


def test_unindexed_node_keys_get_a_uniqueness_constraint() -> None:
    ddl = generate_node_ddl(SOCIAL)
    assert ("CREATE CONSTRAINT company_company_id_key_unique IF NOT EXISTS FOR (n:Company) "
            "REQUIRE n.company_id IS UNIQUE") in ddl
    # Keys that already declare a uniqueness constraint get no second one.
    assert not any("person_person_id_key_unique" in statement for statement in ddl)


def _outcome(function, raw):
    try:
        return "ok", function(raw)
    except EdgeRecordValidationError as exc:
        return "error", str(exc)


@pytest.mark.parametrize("edge_type", sorted(EDGES))
def test_typed_fast_normalizer_matches_the_reference(edge_type) -> None:
    edge = EDGES[edge_type]
    fast = compile_record_normalizer(edge)

    def reference(raw):
        record = normalize_edge_record(raw, edge)
        return record.source_key, record.target_key, record.properties

    samples = {
        "integer": [5, -3, 2 ** 60, "5", 5.0, True, None],
        "string": ["a", "", 5, None],
        "float": [1.5, 2, -0.0, float("inf"), 10 ** 400, "1.0", True, None],
        "date": ["2024-01-02", "2024-13-01", 20240102, None],
        "datetime": ["2024-01-02T03:04:05+02:00", "2024-01-02T03:04:05Z", "2024-01-02 03:04:05+00:00",
                     "2024-01-02T03:04:05", None],
    }
    rng = random.Random(edge_type)
    source_type, target_type = edge.source_key_config.type, edge.target_key_config.type
    for _ in range(400):
        raw = {
            "source": {edge.source_key_property: rng.choice(samples[source_type])},
            "target": {edge.target_key_property: rng.choice(samples[target_type])},
            "properties": {name: rng.choice(samples[config.type]) for name, config in edge.properties.items()
                           if rng.random() < 0.8},
        }
        if rng.random() < 0.05:
            raw["properties"]["undeclared"] = 1
        if rng.random() < 0.05:
            raw["source"]["extra"] = 1
        assert _outcome(fast, copy.deepcopy(raw)) == _outcome(reference, copy.deepcopy(raw)), raw
    valid = {
        "source": {edge.source_key_property: samples[source_type][0]},
        "target": {edge.target_key_property: samples[target_type][0]},
        "properties": {name: samples[config.type][0] for name, config in edge.properties.items()},
    }
    assert _outcome(fast, valid)[0] == "ok"


def test_fast_normalizer_returns_typed_values() -> None:
    fast = compile_record_normalizer(EDGES["BOUGHT"])
    source, target, properties = fast({"source": {"person_id": 3}, "target": {"sku": "s"},
                                       "properties": {"quantity": 2, "amount": 4, "at": "2024-01-02T03:04:05Z"}})
    assert (source, target) == (3, "s") and type(properties["amount"]) is float
    assert properties["at"] == datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    _, day, _ = compile_record_normalizer(EDGES["ACTIVE_ON"])(
        {"source": {"person_id": 1}, "target": {"day": "2024-02-29"}, "properties": {"sessions": 1}})
    assert day == date(2024, 2, 29)


def test_start_kafka_edge_planner_derives_settings_and_points_the_phase_at_its_plan(tmp_path, monkeypatch) -> None:
    created = {}
    rejections = tmp_path / "rej"
    rejections.mkdir()
    monkeypatch.setattr("src.orchestrator.planned_edges.REJECTION_DIR", str(rejections))
    (rejections / "r1-planner-0.jsonl").write_text("old\n")
    (rejections / "other-planner-0.jsonl").write_text("keep\n")

    class FakePlanner:
        def __init__(self, schema, bootstrap, settings_for, *, plan_id, plan_path, rejection_dir=None):
            created.update(schema=schema, bootstrap=bootstrap, settings_for=settings_for, plan_id=plan_id,
                           plan_path=plan_path, rejection_dir=rejection_dir)
            self.closed = []

        def start(self):
            created["settings"] = created["settings_for"](123_456, 3)

        def run_in_background(self, path):
            created["path"] = path

        def close(self, *, delete_topic):
            self.closed.append(delete_topic)

    stale = tmp_path / "edge-plan.json.failed"
    stale.write_text("old")
    args = argparse.Namespace(edge_plan=AUTO_EDGE_PLAN, plan_dir=str(tmp_path), plan_chunk_rows=None,
                              plan_buckets=None, plan_hub_degree=0, plan_workers=2, plan_partitions=None,
                              plan_topic=None, plan_memory_mb=512, plan_spill_dir=str(tmp_path / "spill"))
    planner = start_kafka_edge_planner(SOCIAL, args, run_id="r1", writers=6, bootstrap_servers="kafka:9",
                                       planner_factory=FakePlanner)
    settings = created["settings"]
    assert (settings.buckets, settings.hub_degree, settings.chunk_rows) == (8, 0, 1029)  # isqrt(123456/1500)=9
    assert settings.topic == "graph-loader-edge-chunks-r1" and settings.workers == 2 and settings.partitions == 4
    assert args.edge_plan == str(tmp_path / "edge-plan.json") == str(created["path"])
    assert created["plan_id"] == "r1" and not stale.exists() and isinstance(planner, FakePlanner)
    assert created["plan_path"] == tmp_path / "edge-plan.json"
    assert settings.memory_mb == 512 and settings.spill_dir == str(tmp_path / "spill")
    # the CLI wiring: the planner gets the one shared rejection directory; a rerun drops its stale files only
    assert created["rejection_dir"] == rejections
    assert not (rejections / "r1-planner-0.jsonl").exists() and (rejections / "other-planner-0.jsonl").exists()


@pytest.mark.parametrize("plan_buckets, expected, reason", [
    (None, 18, "buckets_reason=self_ref_share=0.79"),
    (7, 7, "buckets_reason=explicit"),  # --plan-buckets always wins
])
def test_start_kafka_edge_planner_logs_the_self_referencing_bucket_rule(tmp_path, monkeypatch, caplog,
                                                                        plan_buckets, expected, reason) -> None:
    monkeypatch.setattr("src.orchestrator.planned_edges.REJECTION_DIR", str(tmp_path / "rej"))
    created = {}

    class FakePlanner:
        def __init__(self, schema, bootstrap, settings_for, **_kwargs):
            created["settings_for"] = settings_for

        def start(self):
            created["settings"] = created["settings_for"](1_260_000, 6, self_ref_records=1_000_000)

        def run_in_background(self, path):
            pass

    args = argparse.Namespace(edge_plan=AUTO_EDGE_PLAN, plan_dir=str(tmp_path), plan_chunk_rows=None,
                              plan_buckets=plan_buckets, plan_hub_degree=None, plan_workers=2, plan_partitions=None,
                              plan_topic=None, plan_memory_mb=0, plan_spill_dir=None)
    with caplog.at_level(logging.INFO, logger="src.orchestrator.planned_edges"):
        start_kafka_edge_planner(SOCIAL, args, run_id="r1", writers=10, bootstrap_servers="kafka:9",
                                 planner_factory=FakePlanner)
    settings = created["settings"]
    assert (settings.buckets, settings.chunk_rows, settings.hub_degree) == (expected, 5000, 5000)
    line = next(record.getMessage() for record in caplog.records if "stage=edge-plan-settings" in record.getMessage())
    assert f"buckets={expected} " in line and "self_ref_records=1000000" in line and line.endswith(reason)
    # A caller that passes only (records, edge_types) gets the bipartite rule.
    assert created["settings_for"](1_260_000, 6).buckets == (10 if plan_buckets is None else 7)


def test_the_classic_and_planner_rejection_directories_are_one_constant() -> None:
    import inspect

    from src.orchestrator import docker_service, planned_edges
    assert planned_edges.REJECTION_DIR is docker_service.REJECTION_DIR
    for method in (docker_service.DockerService.run_edge_loader, docker_service.DockerService.run_node_loader):
        assert inspect.signature(method).parameters["rejection_dir"].default == docker_service.REJECTION_DIR


@patch("src.cli.start_kafka_edge_planner")
@patch("src.cli.run_planned_edge_phase")
@patch("src.cli.BulkMonitor")
@patch("src.cli.DockerService")
@patch("src.cli.apply_schema")
@patch("src.cli.get_neo4j_driver")
@patch("src.cli.load_schema")
def test_handle_start_with_edge_plan_alone_plans_from_kafka_and_cleans_up(
    mock_load, mock_driver, _apply, mock_docker, monitor_cls, planned, start_planner, tmp_path,
):
    schema = SimpleNamespace(
        nodes=[SimpleNamespace(label="Person", topic="p", replicas=1), SimpleNamespace(label="City", topic="c", replicas=1)],
        edges=[SimpleNamespace(type="LIVES_IN", topic="l", replicas=1, execution=SimpleNamespace(mode="python_apoc"),
                               nodes=SimpleNamespace(source="Person", target="City", is_self_referencing=False))],
        loading=SimpleNamespace(consumer_group_id="g"),
    )
    mock_load.return_value = schema
    docker = MagicMock()
    docker.run_node_loader.side_effect = lambda node_label, **_kwargs: [MagicMock(name=node_label)]
    mock_docker.return_value = docker
    monitor = MagicMock()
    monitor.drive_label_readiness.side_effect = lambda labels, timeout, **kwargs: [kwargs["on_ready"](l) for l in labels]
    monitor_cls.return_value = monitor
    fake_planner = MagicMock()

    def start(schema_arg, args, *, run_id, writers, bootstrap_servers, stop_requested=None):
        args.edge_plan = str(tmp_path / "edge-plan.json")
        start_planner.seen = (run_id, writers)
        start_planner.stop_requested = stop_requested
        return fake_planner

    start_planner.side_effect = start
    seen = {}

    def phase(schema_arg, docker_service, args, *, parent_run_id, **_kwargs):
        seen.update(plan=args.edge_plan, run=parent_run_id, writers=args.plan_writers)
        return 0

    planned.side_effect = phase
    with patch.object(auto_settings, "probe_neo4j_cores", return_value=6):
        args = argparse.Namespace(mode="bulk", config="c.yaml", network="net", skip_image_build=True,
                                  bulk_timeout_seconds=3, edge_plan=AUTO_EDGE_PLAN, plan_writers=None)
        assert handle_start(args) == 0
    assert seen["plan"] == str(tmp_path / "edge-plan.json") and seen["writers"] == 8
    assert start_planner.seen == (seen["run"], 8)
    # N2: the planner can tell an operator stop from a failure (it logs the abort as a WARNING).
    from src import cli as cli_module
    assert start_planner.stop_requested is cli_module._shutdown_requested
    fake_planner.close.assert_called_once_with(delete_topic=True)
    # A failing planner start fails the run before any loader launches.
    docker.reset_mock()
    start_planner.side_effect = RuntimeError("topic exists")
    args = argparse.Namespace(mode="bulk", config="c.yaml", network="net", skip_image_build=True,
                              bulk_timeout_seconds=3, edge_plan=AUTO_EDGE_PLAN, plan_writers=4)
    assert handle_start(args) == 1
    docker.run_node_loader.assert_not_called()
