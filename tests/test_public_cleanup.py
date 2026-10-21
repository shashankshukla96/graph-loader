"""Public cleanup assertions; every real process fault has an external owner."""
import json
from pathlib import Path
import subprocess
import sys

import pytest

ROOT=Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('scenario,mode',[
    (scenario,mode) for scenario in ('public-blocked','public-partial','public-send','public-many')
    for mode in ('timeout','cancel')
]+[('public-stop','timeout'),('public-source-error','timeout'),('public-startup','timeout')])
def test_public_fault_reaps_every_child_and_releases_owned_resources(tmp_path,scenario,mode):
    output=tmp_path/'fault'
    result=subprocess.run([sys.executable,str(ROOT/'plan/library-implementation/tools/run_e04_fault.py'),
        scenario,'--mode',mode,'--python',sys.executable,'--output',str(output)],
        cwd=ROOT,capture_output=True,text=True,timeout=30)
    assert result.returncode==0,result.stdout+result.stderr+(output/'client.log').read_text()
    supervisor=json.loads((output/'supervisor.json').read_text())
    client=json.loads((output/'client.json').read_text())
    assert not supervisor['timeout'] and supervisor['exit_code']==0
    # .8 s planning + aggregate 10 s cleanup + 3 s process scheduling slack.
    # Initial process construction/submission remains an approved exclusion.
    assert client['elapsed_seconds']<13.8
    assert client['production_children_dead'] and client['production_endpoints_closed']
    assert not client['heartbeat_alive']
    assert not client['lock_exists'] and not client['spill_exists']
    if scenario!='public-stop':assert client['error'].split('.')[-1] in client['failure_marker']
    if scenario=='public-many':assert len(client['child_pids'])==4
    if scenario=='public-startup':assert len(client['child_pids'])==2


@pytest.mark.parametrize('failure',['timeout','cancel','source','startup'])
@pytest.mark.parametrize('create_topic',[None,False,True])
def test_unreapable_fake_workers_share_one_budget_and_keep_live_owned_state(tmp_path,monkeypatch,failure,create_topic):
    import threading
    from neo4j_graph_loader import planning
    from neo4j_graph_loader.schema import load_schema
    from neo4j_graph_loader.loader import edge_planner as implementation
    clock=[0.];operation=[0.];handles=[];actions=[];signal=threading.Event()
    class Process:
        pid=None
        def start(self):
            self.pid=900000+len(handles)
            if failure=='startup' and len(handles)==2:raise RuntimeError('second process started then failed')
        def is_alive(self):return True
        def terminate(self):actions.append(('terminate',self.pid))
        def kill(self):actions.append(('kill',self.pid))
        def join(self,timeout):actions.append(('join',self.pid,timeout));clock[0]+=timeout
    class Connection:
        closed=False
        def send_initial(self,value):pass
        def send(self,value,**kwargs):pass
        def close(self):self.closed=True
        def recv(self):
            if failure=='cancel':signal.set()
            if failure=='timeout':operation[0]=2.
            return ('error','ordinary source failure') if failure=='source' else ('ok',{})
    class Worker(implementation._ProcessWorker):
        def __init__(self,*args,**kwargs):
            self.process=Process();self.connection=Connection();self.progress=[0,0,0]
            self._child=None;self._stop_sent=False;self._submitted=False;handles.append(self)
    monkeypatch.setattr(implementation,'_ProcessWorker',Worker)
    path=tmp_path/'plan.json'
    planner=planning.producer_edge_planner(load_schema(ROOT/'examples/social/graph_schema.yaml'),
        [planning.IterableEdgeSource('VISITED',order=i) for i in range(4)],
        planning.PlanSettings(topic='chunks',workers=4,memory_mb=1),'unused',plan_path=path,
        cancel_event=signal,timeout_seconds=1)
    planner._clock=lambda:operation[0];planner._cleanup_clock=lambda:clock[0]
    error_type={'timeout':planning.EdgePlanTimeoutError,'cancel':planning.EdgePlanCancelledError,'source':RuntimeError,'startup':RuntimeError}[failure]
    from test_edge_planner import FakeCluster
    cluster=FakeCluster()
    def call():
        if create_topic is None:return planner.finish(path)
        monkeypatch.setattr(implementation,'producer_edge_planner',lambda *a,**k:planner)
        return planning.plan_and_publish_edges(load_schema(ROOT/'examples/social/graph_schema.yaml'),[],
            bootstrap_servers='unused',plan_path=path,settings=planner.settings,
            create_topic=create_topic,admin_factory=cluster.admin)
    try:
        with pytest.raises(error_type) as captured:call()
        assert cluster.deleted==[]
        assert clock[0]==10. and sum(a[2] for a in actions if a[0]=='join')==10.
        assert [a[0] for a in actions[:len(handles)]]==['terminate']*len(handles)
        assert len([a for a in actions if a[0]=='kill'])==len(handles)
        assert all(h.connection.closed for h in handles)
        assert planner._cleanup_incomplete and len(planner._workers)==len(handles)
        assert planner.spill_dir.is_dir() and path.with_name(path.name+'.lock').exists()
        marker=path.with_name(path.name+'.failed').read_text()
        assert type(captured.value).__name__ in marker and 'retained spill=' in marker
        assert all(str(h.process.pid) in marker for h in handles)
        assert any('cleanup incomplete' in note for note in captured.value.__notes__)
        planner.close(terminate=True);assert clock[0]==10. # no fresh cleanup budget
        contender=planning.producer_edge_planner(load_schema(ROOT/'examples/social/graph_schema.yaml'),
            {},planning.PlanSettings(topic='chunks'),'unused',plan_path=path)
        with pytest.raises(planning.EdgePlanError,match='already in use'):contender.start()
        contender.close()
    finally:
        # These are inert fakes, with no OS child or live file users. Only this
        # test's retained directory/lock are manually recovered after assertions.
        import shutil
        shutil.rmtree(planner.spill_dir,ignore_errors=True)
        path.with_name(path.name+'.lock').unlink(missing_ok=True)


@pytest.mark.parametrize('broken',['progress','endpoint','spill'])
def test_cleanup_action_failure_still_reaps_children_and_retains_owned_topic(tmp_path,monkeypatch,broken):
    from neo4j_graph_loader import planning
    from neo4j_graph_loader.schema import load_schema
    from neo4j_graph_loader.loader import edge_planner as implementation
    from test_edge_planner import FakeCluster
    handles=[];actions=[]
    class Process:
        pid=None;alive=True
        def start(self):self.pid=900001
        def is_alive(self):return self.alive
        def terminate(self):actions.append('terminate');self.alive=False
        def kill(self):actions.append('kill');self.alive=False
        def join(self,timeout):actions.append('join')
    class Connection:
        closed=False
        def send_initial(self,value):pass
        def recv(self):return 'error','ordinary source failure'
        def close(self):
            self.closed=True
            if broken=='endpoint':raise OSError('endpoint unavailable')
    class Worker(implementation._ProcessWorker):
        def __init__(self,*args,**kwargs):
            self.process=Process();self.connection=Connection();self.progress=[0,0,0]
            self._child=None;self._stop_sent=False;self._submitted=False;handles.append(self)
    monkeypatch.setattr(implementation,'_ProcessWorker',Worker)
    path=tmp_path/'plan.json';cluster=FakeCluster()
    original_unlink=Path.unlink
    def unlink(self,*args,**kwargs):
        if broken=='progress' and handles and self==path.with_name(path.name+'.progress'):raise PermissionError('progress unavailable')
        return original_unlink(self,*args,**kwargs)
    monkeypatch.setattr(Path,'unlink',unlink)
    if broken=='spill':
        def rmtree(*args,**kwargs):raise PermissionError('spill unavailable')
        monkeypatch.setattr(implementation.shutil,'rmtree',rmtree)
    captured=[];factory=implementation.producer_edge_planner
    def record(*args,**kwargs):
        planner=factory(*args,**kwargs);captured.append(planner);return planner
    monkeypatch.setattr(implementation,'producer_edge_planner',record)
    with pytest.raises(RuntimeError,match='ordinary source failure') as error:
        planning.plan_and_publish_edges(load_schema(ROOT/'examples/social/graph_schema.yaml'),
            [planning.IterableEdgeSource('VISITED')],bootstrap_servers='unused',plan_path=path,
            settings=planning.PlanSettings(topic='chunks',memory_mb=1),admin_factory=cluster.admin)
    planner=captured[0]
    assert 'terminate' in actions and not handles[0].process.is_alive()
    assert handles[0].connection.closed
    assert planner._cleanup_incomplete and cluster.deleted==[]
    assert planner.spill_dir.exists() and path.with_name(path.name+'.lock').exists()
    assert any('cleanup incomplete' in note for note in error.value.__notes__)
    assert 'cleanup incomplete' in path.with_name(path.name+'.failed').read_text()
    planner.close();assert actions.count('terminate')==1
    # No actual children exist. Restore injected failures before test-owned recovery.
    monkeypatch.undo()
    import shutil
    shutil.rmtree(planner.spill_dir)
    path.with_name(path.name+'.lock').unlink()


@pytest.mark.parametrize('timeout',[None,.8],ids=['no-timeout','finite-timeout'])
def test_observed_source_error_does_not_wait_for_blocked_peer(tmp_path,timeout):
    scenario='public-mixed-error-default' if timeout is None else 'public-mixed-error-timeout'
    output=tmp_path/'mixed'
    result=subprocess.run([sys.executable,str(ROOT/'plan/library-implementation/tools/run_e04_fault.py'),
        scenario,'--mode','timeout','--python',sys.executable,'--output',str(output),'--hard-timeout','15'],
        cwd=ROOT,capture_output=True,text=True,timeout=25)
    assert result.returncode==0,result.stdout+result.stderr+(output/'client.log').read_text()
    supervisor=json.loads((output/'supervisor.json').read_text())
    client=json.loads((output/'client.json').read_text())
    assert not supervisor['timeout'] and supervisor['exit_code']==0
    assert client['configured_timeout']==timeout
    assert client['error']=='RuntimeError' and 'LookupError: ordinary child source failure' in client['ordinary_cause']
    assert len(client['received_errors'])==1 and 'LookupError: ordinary child source failure' in client['received_errors'][0][1]
    assert client['blocked_peer_entered'] and len(client['child_pids'])==2
    assert 0<=client['failure_to_cleanup_seconds']<.3
    assert client['observation_to_raise_seconds']<11.3 # aggregate10s + poll/scheduling slack
    assert client['elapsed_seconds']<13.8
    assert client['production_children_dead'] and client['production_endpoints_closed']
    assert not client['active_child_pids'] and not client['heartbeat_alive']
    assert not client['lock_exists'] and not client['spill_exists']
    assert 'ordinary child source failure' in client['failure_marker'] and 'LookupError' in client['failure_marker']
    assert 'EdgePlanTimeoutError' not in client['failure_marker']
    assert client['caller_sentinel_preserved'] and client['caller_signal_preserved']


@pytest.mark.parametrize('interruption',['timeout','cancel'])
def test_received_inline_source_error_keeps_cause_before_post_receive_check(tmp_path,monkeypatch,interruption):
    import threading
    from neo4j_graph_loader.loader import edge_planner as implementation
    from test_public_cancellation import planner_for
    signal=threading.Event();clock=[0.];failure=LookupError('already received source failure')
    def rows():
        raise failure
        yield
    planner=planner_for(rows(),tmp_path,cancel_event=signal,timeout_seconds=1)
    planner._clock=lambda:clock[0]
    original_recv=implementation._InlineWorker.recv
    def recv(worker):
        reply=original_recv(worker)
        assert reply[0]=='error'
        if interruption=='timeout':clock[0]=2.
        else:signal.set()
        return reply
    monkeypatch.setattr(implementation._InlineWorker,'recv',recv)
    with pytest.raises(RuntimeError,match='already received source failure') as captured:
        planner.finish(tmp_path/'plan.json')
    assert captured.value.__cause__ is failure
    assert planner._closed and not planner._workers
    assert not planner._cleanup_incomplete
    assert 'already received source failure' in (tmp_path/'plan.json.failed').read_text()
