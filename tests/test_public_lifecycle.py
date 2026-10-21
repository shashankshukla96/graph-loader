"""Ownership/lifecycle guarantees called through approved public modules."""
from pathlib import Path

import pytest

from neo4j_graph_loader.planning import EdgePlanError, PlanSettings, plan_and_publish_edges, producer_edge_planner
from neo4j_graph_loader.schema import load_schema
from test_edge_planner import FakeCluster, FakeProducer, envelope

ROOT = Path(__file__).resolve().parents[1]


def make_planner(path, cluster, rows=()):
    return producer_edge_planner(load_schema(ROOT/'examples/social/graph_schema.yaml'), {'VISITED': rows},
        PlanSettings(topic='chunks', partitions=1, memory_mb=1, hub_degree=0), 'unused',
        context='inline', plan_path=path, producer_factory=lambda: FakeProducer(cluster))


def test_public_planner_is_one_shot_start_and_close_are_idempotent(tmp_path):
    cluster = FakeCluster(); cluster.topic('chunks', 1)
    path = tmp_path/'plan.json'; planner = make_planner(path, cluster)
    try:
        planner.start(); owned = planner.spill_dir
        planner.start(); assert planner.spill_dir == owned
        result = planner.finish(path); original = path.read_bytes()
        assert result.path == path
        for action in (planner.start, lambda: planner.finish(path)):
            with pytest.raises(EdgePlanError, match='closed'): action()
        assert path.read_bytes() == original
    finally:
        planner.close(); planner.close()
    assert not list(tmp_path.glob('*.lock')) and not list(tmp_path.glob('.edge-plan-spill-*'))


def test_public_live_owner_and_stale_lock_are_never_automatically_reclaimed(tmp_path):
    path=tmp_path/'plan.json'; cluster=FakeCluster()
    owner=make_planner(path,cluster); contender=make_planner(path,cluster)
    try:
        owner.start(); progress=path.with_name(path.name+'.progress'); saved=progress.read_bytes()
        with pytest.raises(EdgePlanError,match='already in use'): contender.finish(path)
        assert progress.read_bytes()==saved
        assert not path.with_name(path.name+'.failed').exists()
        assert owner._plan_lock_path.exists()
    finally: contender.close(); owner.close()
    lock=path.with_name(path.name+'.lock'); lock.write_text('test-owned stale sentinel; manual recovery required')
    retry=make_planner(path,cluster)
    try:
        with pytest.raises(EdgePlanError,match='owner has stopped'): retry.finish(path)
        assert 'sentinel' in lock.read_text()
    finally: retry.close(); lock.unlink()


def test_public_wrong_output_does_not_end_owner_and_new_attempt_replaces_stale_files(tmp_path):
    path=tmp_path/'plan.json'; other=tmp_path/'other.json'; other.write_text('other owner')
    path.write_text('stale plan'); path.with_name(path.name+'.failed').write_text('old failure')
    planner=make_planner(path,FakeCluster())
    try:
        planner.start()
        assert not path.exists() and not path.with_name(path.name+'.failed').exists()
        with pytest.raises(EdgePlanError,match='finish path must match'): planner.finish(other)
        assert other.read_text()=='other owner' and planner._plan_lock_path.exists()
    finally: planner.close()


@pytest.mark.parametrize('create_topic',[False,True])
@pytest.mark.parametrize('success',[False,True])
def test_public_convenience_topic_ownership_and_failure_markers(tmp_path,create_topic,success):
    cluster=FakeCluster(); model=load_schema(ROOT/'examples/social/graph_schema.yaml'); path=tmp_path/'plan.json'
    if not create_topic: cluster.topic('chunks',1)
    rows=[envelope('VISITED',1,'City A',times=3)] if success else [{'bad':True}]
    def call():
        return plan_and_publish_edges(model,{'VISITED':rows},bootstrap_servers='unused',plan_path=path,
            settings=PlanSettings(topic='chunks',partitions=1,memory_mb=1,hub_degree=0),
            create_topic=create_topic,admin_factory=cluster.admin,producer_factory=lambda:FakeProducer(cluster))
    if success:
        assert call().plan.total_rows==1
        assert path.exists() and not path.with_name(path.name+'.failed').exists()
    else:
        with pytest.raises(RuntimeError,match='invalid VISITED'): call()
        assert path.with_name(path.name+'.failed').exists()
    assert cluster.deleted==(['chunks'] if create_topic and not success else [])
    assert not list(tmp_path.glob('*.lock')) and not list(tmp_path.glob('.edge-plan-spill-*'))


def test_public_topic_cleanup_failure_is_visible_without_hiding_source_failure(tmp_path,caplog):
    cluster=FakeCluster(); model=load_schema(ROOT/'examples/social/graph_schema.yaml')
    original=cluster.admin
    def admin(config):
        instance=original(config)
        def fail(*args,**kwargs): raise RuntimeError('delete unavailable')
        instance.delete_topics=fail
        return instance
    with pytest.raises(RuntimeError,match='invalid VISITED'):
        plan_and_publish_edges(model,{'VISITED':[{'bad':True}]},bootstrap_servers='unused',
            plan_path=tmp_path/'plan.json',settings=PlanSettings(topic='chunks',memory_mb=1),
            admin_factory=admin,producer_factory=lambda:FakeProducer(cluster))
    assert 'delete unavailable' in caplog.text


def test_public_partial_startup_reclaims_the_first_owned_worker(tmp_path,monkeypatch):
    from neo4j_graph_loader.planning import IterableEdgeSource
    from neo4j_graph_loader.loader import edge_planner as implementation
    original=implementation._InlineWorker
    handles=[]
    def constructor(*args,**kwargs):
        if handles: raise RuntimeError('second worker startup')
        handle=original(*args,**kwargs); handles.append(handle)
        return handle
    monkeypatch.setattr(implementation,'_InlineWorker',constructor)
    path=tmp_path/'plan.json'
    planner=producer_edge_planner(load_schema(ROOT/'examples/social/graph_schema.yaml'),
        [IterableEdgeSource('VISITED',order=0),IterableEdgeSource('VISITED',order=1)],
        PlanSettings(topic='chunks',workers=2,memory_mb=1),'unused',context='inline',plan_path=path)
    with pytest.raises(RuntimeError,match='second worker startup'): planner.start()
    assert len(handles)==1 and handles[0].state is None
    assert planner._closed and not planner._workers
    assert not path.with_name(path.name+'.lock').exists()
    assert 'second worker startup' in path.with_name(path.name+'.failed').read_text()
    assert not list(tmp_path.glob('.edge-plan-spill-*'))
