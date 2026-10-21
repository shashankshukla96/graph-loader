"""Fresh installed imports and independent public planner metrics ownership."""
from pathlib import Path
import subprocess
import sys

from neo4j_graph_loader import planning
from neo4j_graph_loader.schema import load_schema
from neo4j_graph_loader.utils import metrics
from test_edge_planner import FakeCluster,FakeProducer,envelope

ROOT=Path(__file__).resolve().parents[1]


def test_public_and_cli_imports_preserve_caller_process_configuration(tmp_path):
    result=subprocess.run([sys.executable,'-I',str(ROOT/'tests/fixtures/e04_imports.py')],
        cwd=tmp_path,capture_output=True,text=True,timeout=30)
    assert result.returncode==0,result.stdout+result.stderr
    assert '"imports": "inert"' in result.stdout


def test_repeated_public_calls_have_independent_default_metrics(tmp_path):
    schema=load_schema(ROOT/'examples/social/graph_schema.yaml');cluster=FakeCluster();cluster.topic('chunks',1)
    planners=[]
    for index in range(2):
        planner=planning.producer_edge_planner(schema,
            {'VISITED':[envelope('VISITED',index,'City A',times=1)]},
            planning.PlanSettings(topic='chunks',memory_mb=1,hub_degree=0),'unused',context='inline',
            producer_factory=lambda:FakeProducer(cluster))
        planners.append(planner)
        try:assert planner.finish(tmp_path/f'plan-{index}.json').plan.total_rows==1
        finally:planner.close()
    a,b=planners
    assert a._metrics.registry is not b._metrics.registry
    assert metrics._process_metrics is None
    for planner in planners:
        assert planner._metrics.registry.get_sample_value('graph_loader_planner_rows_read_total',{'kind':'cli'})==1
    a._metrics.planner_rows_read.labels(kind='cli').inc(10)
    assert b._metrics.registry.get_sample_value('graph_loader_planner_rows_read_total',{'kind':'cli'})==1


def test_self_contained_public_installed_example(tmp_path):
    result=subprocess.run([sys.executable,'-I',str(ROOT/'tests/fixtures/e04_public_example.py')],
        cwd=tmp_path,capture_output=True,text=True,timeout=30)
    assert result.returncode==0,result.stdout+result.stderr
    assert '"owned_resources": "reclaimed"' in result.stdout
