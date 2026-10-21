"""Approved E04 public imports, input shapes and canonical class identities."""
from pathlib import Path
import pickle

from pydantic import ValidationError
import pytest
import yaml

from neo4j_graph_loader import planning, schema
from neo4j_graph_loader.planning import (
    EdgePlanError, EdgeRecordSource, IterableEdgeSource, ParallelEdgePlanner,
    PlanResult, PlanSettings, producer_edge_planner,
)
from neo4j_graph_loader.schema import GraphSchema, SchemaLoadError, load_schema

ROOT = Path(__file__).resolve().parents[1]


def test_public_schema_load_and_direct_model_contract():
    model = load_schema(ROOT / 'examples/social/graph_schema.yaml')
    assert isinstance(model, GraphSchema) and model.edges
    assert GraphSchema.model_validate(model.model_dump()) == model
    with pytest.raises(ValidationError):
        GraphSchema.model_validate({})


@pytest.mark.parametrize('content,cause', [(b'\xff', UnicodeDecodeError), (b': bad: yaml', yaml.YAMLError),
                                         (b'[]', ValueError), (b'nodes: wrong', ValidationError)])
def test_public_schema_error_keeps_cause(tmp_path, content, cause):
    p = tmp_path/'bad.yaml'
    p.write_bytes(content)
    with pytest.raises(SchemaLoadError) as error:
        load_schema(p)
    assert isinstance(error.value.__cause__, cause)


def test_public_missing_schema_keeps_read_cause(tmp_path):
    with pytest.raises(SchemaLoadError) as error:
        load_schema(tmp_path/'missing')
    assert isinstance(error.value.__cause__, FileNotFoundError)


def test_public_exports_are_explicit_and_keep_pickle_identity():
    assert set(schema.__all__) == {'load_schema','GraphSchema','LoadingConfig','NodeConfig','EdgeConfig',
        'PropertyConfig','SourceTargetConfig','DeadLetterConfig','RetryConfig','SchemaLoadError',
        'MultiTopicEdgeError','MultiPairEdgeError'}
    assert set(planning.__all__) == {'PlanSettings','EdgeRecordSource','IterableEdgeSource','ParallelEdgePlanner',
        'PlanResult','EdgePlan','producer_edge_planner','plan_and_publish_edges','EdgePlanError','EdgePlanInputError','EdgePlanCancelledError','EdgePlanTimeoutError'}
    for cls in (GraphSchema, PlanSettings, IterableEdgeSource):
        assert pickle.loads(pickle.dumps(cls)) is cls
    assert not getattr(EdgeRecordSource, '_is_protocol', False)
    with pytest.raises(NotImplementedError):
        EdgeRecordSource().records(None)


def test_public_settings_and_source_validation_happen_before_clients(tmp_path):
    model = load_schema(ROOT/'examples/social/graph_schema.yaml')
    with pytest.raises(ValueError, match='buckets'):
        producer_edge_planner(model, {}, PlanSettings(topic='chunks', buckets=0), 'unused')
    with pytest.raises(EdgePlanError, match='not in the schema'):
        producer_edge_planner(model, {'UNKNOWN': []}, PlanSettings(topic='chunks'), 'unused')
    name = model.edges[0].type
    mapping = producer_edge_planner(model, {name: []}, PlanSettings(topic='chunks'), 'unused', context='inline')
    sequence = producer_edge_planner(model, [IterableEdgeSource(name, [])], PlanSettings(topic='chunks'), 'unused')
    assert isinstance(mapping, ParallelEdgePlanner) and isinstance(sequence, ParallelEdgePlanner)
    assert type(mapping.tasks[0].source) is IterableEdgeSource
    mapping.close(); sequence.close()


def test_public_planner_returns_real_result_with_preserved_chunks(tmp_path):
    from test_edge_planner import FakeCluster, FakeProducer, envelope
    model = load_schema(ROOT/'examples/social/graph_schema.yaml')
    cluster = FakeCluster(); cluster.topic('chunks', 1)
    planner = producer_edge_planner(model, {'VISITED': [envelope('VISITED', 1, 'City A', times=2)]},
        PlanSettings(topic='chunks', partitions=1, memory_mb=1, hub_degree=0), 'unused',
        context='inline', producer_factory=lambda: FakeProducer(cluster))
    try:
        result = planner.finish(tmp_path/'plan.json')
        assert isinstance(result, PlanResult) and isinstance(result.plan, planning.EdgePlan)
        assert result.plan.total_rows == result.rows_read == 1
        assert result.path.exists() and len(result.digest) == 64
    finally:
        planner.close()


def test_public_schema_resolution_error_keeps_cause(monkeypatch):
    def fail(self): raise OSError('resolution denied')
    monkeypatch.setattr(Path, 'resolve', fail)
    with pytest.raises(SchemaLoadError) as error:
        load_schema('schema.yaml')
    assert isinstance(error.value.__cause__, OSError)


def test_public_schema_stat_error_keeps_read_cause(tmp_path, monkeypatch):
    def denied(self): raise PermissionError('stat denied')
    monkeypatch.setattr(Path, 'is_file', denied)
    with pytest.raises(SchemaLoadError) as error:
        load_schema(tmp_path/'schema.yaml')
    assert isinstance(error.value.__cause__, PermissionError)


def test_public_multi_pair_sources_must_identify_a_valid_topic():
    from multi_pair_support import likes_schema_dict
    model = GraphSchema.model_validate(likes_schema_dict())
    settings = PlanSettings(topic='chunks')
    with pytest.raises(EdgePlanError, match='several label pairs'):
        producer_edge_planner(model, {'LIKES': []}, settings, 'unused')
    for invalid in (None, 'wrong'):
        with pytest.raises(EdgePlanError, match='must name its topic'):
            producer_edge_planner(model, [IterableEdgeSource('LIKES', topic=invalid)], settings, 'unused')
    topic = model.edges[0].topics[-1]
    planner = producer_edge_planner(model, [IterableEdgeSource('LIKES', topic=topic)], settings, 'unused')
    expected = model.edges[0].pair_view(model.edges[0].pair_for_topic(topic).index)
    assert planner.tasks[0].edge.nodes == expected.nodes
    assert planner.tasks[0].edge.nodes != model.edges[0].pair_view(0).nodes
    planner.close()


def test_public_ordinary_inline_source_failure_retains_actual_cause(tmp_path):
    failure = LookupError('source unavailable')
    def rows():
        raise failure
        yield
    model = load_schema(ROOT/'examples/social/graph_schema.yaml')
    planner = producer_edge_planner(model, {'VISITED': rows()},
        PlanSettings(topic='chunks', memory_mb=1), 'unused', context='inline')
    with pytest.raises(RuntimeError, match='source unavailable') as error:
        planner.finish(tmp_path/'plan.json')
    assert error.value.__cause__ is failure
