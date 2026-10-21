"""Public operation clocks and externally supervised process fault integration."""
import json
from pathlib import Path
import subprocess
import sys
import threading

import pytest

from neo4j_graph_loader.planning import (
    EdgePlanCancelledError,EdgePlanError,EdgePlanTimeoutError,PlanSettings,producer_edge_planner,
)
from neo4j_graph_loader.schema import load_schema
from test_edge_planner import FakeCluster,FakeProducer,envelope

ROOT=Path(__file__).resolve().parents[1]


def planner_for(rows,tmp_path,**options):
    cluster=FakeCluster();cluster.topic('chunks',1)
    return producer_edge_planner(load_schema(ROOT/'examples/social/graph_schema.yaml'),{'VISITED':rows},
        PlanSettings(topic='chunks',memory_mb=1,hub_degree=0),'unused',context='inline',
        producer_factory=lambda:FakeProducer(cluster),plan_path=tmp_path/'plan.json',**options)


@pytest.mark.parametrize('invalid',[True,False,0,-1,float('nan'),float('inf'),'1',object()])
def test_invalid_public_timeouts_fail_before_side_effects(tmp_path,invalid):
    with pytest.raises(ValueError,match='positive and finite'):planner_for([],tmp_path,timeout_seconds=invalid)
    assert not list(tmp_path.iterdir())


def test_cooperative_inline_clock_starts_before_first_returning_record(tmp_path):
    now=[0.];seen=[]
    def rows():
        for index in range(10):
            now[0]+=.1;seen.append(index)
            yield envelope('VISITED',index,'City A',times=1)
    planner=planner_for(rows(),tmp_path,timeout_seconds=.25);planner._clock=lambda:now[0]
    with pytest.raises(EdgePlanTimeoutError):planner.finish(tmp_path/'plan.json')
    assert 0<len(seen)<10 and planner._operation_started==0.


def test_inline_signal_is_checked_between_returning_records(tmp_path):
    signal=threading.Event();seen=[]
    def rows():
        for index in range(10):
            seen.append(index)
            if index==2:signal.set()
            yield envelope('VISITED',index,'City A',times=1)
    planner=planner_for(rows(),tmp_path,cancel_event=signal)
    with pytest.raises(EdgePlanCancelledError):planner.finish(tmp_path/'plan.json')
    assert seen==[0,1,2]


def test_one_deadline_accumulates_over_scan_gap_callback_and_emit(tmp_path):
    now=[0.]
    planner=planner_for([],tmp_path,timeout_seconds=1);planner._clock=lambda:now[0]
    planner.start();anchor=planner._deadline
    now[0]=.4
    def callback():now[0]+=.4
    planner.after_scan=callback
    original=planner._broadcast
    def broadcast(command,payloads):
        now[0]+=.3
        return original(command,payloads)
    planner._broadcast=broadcast
    with pytest.raises(EdgePlanTimeoutError):planner.finish(tmp_path/'plan.json')
    assert planner._deadline==anchor==1. and now[0]>1.


def test_public_errors_remain_in_the_approved_plan_family():
    assert issubclass(EdgePlanCancelledError,EdgePlanError)
    assert issubclass(EdgePlanTimeoutError,EdgePlanError)


@pytest.mark.parametrize('scenario',['public-blocked','public-partial','public-send'])
@pytest.mark.parametrize('mode',['timeout','cancel'])
def test_public_process_waits_observe_errors_under_an_external_supervisor(tmp_path,scenario,mode):
    output=tmp_path/'fault'
    command=[sys.executable,str(ROOT/'plan/library-implementation/tools/run_e04_fault.py'),scenario,
        '--mode',mode,'--python',sys.executable,'--output',str(output)]
    result=subprocess.run(command,cwd=ROOT,capture_output=True,text=True,timeout=30)
    assert result.returncode==0,result.stdout+result.stderr+(output/'client.log').read_text()
    record=json.loads((output/'supervisor.json').read_text());client=json.loads((output/'client.json').read_text())
    assert not record['timeout'] and record['exit_code']==0
    assert client['error']==('EdgePlanTimeoutError' if mode=='timeout' else 'EdgePlanCancelledError')
    assert 0<=client['observed_lateness']<.3
    if scenario=='public-send':assert client['blocked_command']=='degrees'


def test_process_submission_anchor_and_later_start_finish_gap_are_supervised(tmp_path):
    output=tmp_path/'gap'
    result=subprocess.run([sys.executable,str(ROOT/'plan/library-implementation/tools/run_e04_fault.py'),
        'public-gap','--mode','timeout','--python',sys.executable,'--output',str(output)],
        cwd=ROOT,capture_output=True,text=True,timeout=30)
    assert result.returncode==0,result.stdout+result.stderr+(output/'client.log').read_text()
    client=json.loads((output/'client.json').read_text())
    assert client['error']=='EdgePlanTimeoutError' and client['clock_anchor']==5.
