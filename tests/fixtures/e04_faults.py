"""Real E04 fault clients. The separate run_e04_fault supervisor owns emergency cleanup."""
import json
import multiprocessing as mp
import os
from pathlib import Path
import socket
import sys
import threading
import time

from neo4j_graph_loader.loader._planner_transport import (
    FrameStream, HEADER, MAGIC, WaitCancelled, WaitControl, WaitExpired, encode,
)


def _partial_reply(sock,ready):
    content=encode(('ok',b'x'*(512<<10)))
    sock.sendall(HEADER.pack(MAGIC,len(content))+content[:64])
    ready.set()
    while True: time.sleep(10)


def _not_reading(sock,ready):
    ready.set()
    while True: time.sleep(10)


def _reap(process):
    if process.is_alive(): process.terminate()
    process.join(2)
    if process.is_alive(): process.kill(); process.join(2)
    assert not process.is_alive()


def private_fault(scenario,mode):
    context=mp.get_context('spawn'); parent,child=socket.socketpair()
    parent.setsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF,4096)
    child.setsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF,4096)
    ready=context.Event()
    process=context.Process(target=_partial_reply if scenario=='private-recv' else _not_reading,args=(child,ready))
    process.start(); child.close()
    signal=threading.Event(); timer=None
    try:
        assert ready.wait(3)
        started=time.monotonic()
        if mode=='cancel': timer=threading.Timer(.15,signal.set);timer.start()
        control=WaitControl(deadline=started+.15 if mode=='timeout' else None,cancel_event=signal)
        stream=FrameStream(parent,check=control.check)
        try:
            if scenario=='private-recv': stream.recv()
            else: stream.send(('command',b'x'*(512<<10)))
        except (WaitExpired,WaitCancelled) as error:
            assert isinstance(error,WaitExpired if mode=='timeout' else WaitCancelled)
            observed=time.monotonic()-started
            assert .1 <= observed < .75,observed
        else: raise AssertionError('fault did not interrupt the private adapter')
    finally:
        parent.close(); _reap(process)
        if timer: timer.cancel(); timer.join(1)
    return {'scenario':scenario,'mode':mode,'observed_seconds':observed,'child_pids':[process.pid],
            'children_dead':not process.is_alive(),'endpoints_closed':parent.fileno()==-1,
            'fixture_timer_dead':timer is None or not timer.is_alive(),'client_pid':os.getpid()}




# Public integration targets are importable under spawn; only these intentional
# fault workers touch the private protocol. Caller behavior uses public modules.
from neo4j_graph_loader.planning import EdgeRecordSource


class BlockedSource(EdgeRecordSource):
    edge_type='VISITED'
    size_hint=0
    def __init__(self,ready,order=0):self.ready=ready;self.order=order
    def records(self,edge):
        Path(self.ready).touch()
        while True:time.sleep(10)
        yield


class FailingSource(EdgeRecordSource):
    edge_type='VISITED';size_hint=0;order=0
    def records(self,edge):
        raise LookupError('ordinary child source failure')
        yield


def _public_partial_worker(sock,worker,bootstrap,config,producer=None,consumer=None,progress=None,parent_duplicate=None):
    if parent_duplicate is not None:parent_duplicate.close()
    stream=FrameStream(sock); stream.recv(initial=True)
    payload=encode(('ok',{'tasks':[],'seconds':0.,'padding':b'x'*(512<<10)}))
    sock.sendall(HEADER.pack(MAGIC,len(payload))+payload[:64])
    (Path(worker['spill_dir']).parent/'child-reply-started').touch()
    while True:time.sleep(10)


def _public_nonreading_worker(sock,worker,bootstrap,config,producer=None,consumer=None,progress=None,parent_duplicate=None):
    import numpy as np
    if parent_duplicate is not None:parent_duplicate.close()
    stream=FrameStream(sock); stream.recv(initial=True)
    stream.send(('ok',{'tasks':[],'seconds':0.}))
    command,payload=stream.recv();assert command=='counter'
    stream.send(('ok',np.ones(1<<22,dtype=np.uint32)))
    while True:time.sleep(10)


def public_fault(scenario,mode,output):
    import selectors
    from neo4j_graph_loader import planning
    from neo4j_graph_loader.schema import load_schema
    from neo4j_graph_loader.loader import edge_planner as implementation
    root=Path(__file__).resolve().parents[2]
    directory=Path(output).parent; ready=directory/'fault-ready';path=directory/'plan.json'
    signal=threading.Event();stop_setter=threading.Event();set_times=[];observations=[];handles=[]
    timer=None
    fake_clock=[0.]
    original_worker=implementation._ProcessWorker
    class RecordingWorker(original_worker):
        def __init__(self,*args,**kwargs):
            super().__init__(*args,**kwargs)
            self.connection.socket.setsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF,4096)
            if scenario=='public-partial':
                original_socket=self.connection.socket
                class PartialReadProbe:
                    def __getattr__(self,name):return getattr(original_socket,name)
                    def recv(self,count):
                        data=original_socket.recv(count)
                        if count>HEADER.size and data:ready.touch()
                        return data
                self.connection.socket=PartialReadProbe()
            handles.append(self)
        def start(self):
            super().start()
            if scenario=='public-startup' and len(handles)==2:
                raise RuntimeError('second process started then failed')
        def send(self,command,payload):
            self.connection.test_command=command
            if scenario=='public-gap' and command=='scan':fake_clock[0]+=5
            return super().send(command,payload)
    implementation._ProcessWorker=RecordingWorker
    if scenario=='public-partial':implementation._planner_worker_main=_public_partial_worker
    if scenario in {'public-send','public-stop'}:
        if scenario=='public-send':implementation._planner_worker_main=_public_nonreading_worker
        original_wait=FrameStream._wait
        def wait(self,event,check):
            if event==selectors.EVENT_WRITE and getattr(self,'test_command',None) in {'degrees','stop'}:ready.touch()
            return original_wait(self,event,check)
        FrameStream._wait=wait
    sources=[BlockedSource(str(ready))] if scenario=='public-blocked' else [planning.IterableEdgeSource('VISITED')]
    if scenario in {'public-stop','public-many','public-startup'}:
        sources=[BlockedSource(str(ready),order=index) for index in range(4 if scenario=='public-many' else 2 if scenario=='public-startup' else 1)]
    if scenario=='public-source-error':sources=[FailingSource()]
    planner=planning.producer_edge_planner(load_schema(root/'examples/social/graph_schema.yaml'),sources,
        planning.PlanSettings(topic='unused',workers=len(sources),memory_mb=1,hub_degree=1 if scenario=='public-send' else 0),
        'unused',plan_path=path,cancel_event=signal,timeout_seconds=.8 if mode=='timeout' and scenario!='public-stop' else None)
    if scenario=='public-gap':planner._clock=lambda:fake_clock[0]
    original_check=planner._check
    def check():
        try:return original_check()
        except (planning.EdgePlanCancelledError,planning.EdgePlanTimeoutError):
            observations.append(time.monotonic());raise
    planner._check=check
    def cancel_when_ready():
        end=time.monotonic()+4
        while not stop_setter.is_set() and time.monotonic()<end:
            if ready.exists():set_times.append(time.monotonic());signal.set();return
            time.sleep(.005)
    if mode=='cancel' and scenario!='public-stop':timer=threading.Thread(target=cancel_when_ready);timer.start()
    started=time.monotonic()
    try:
        try:
            planner.start()
            if scenario=='public-stop':
                end=time.monotonic()+4
                while not ready.exists() and time.monotonic()<end:time.sleep(.005)
                assert ready.exists()
                ready.unlink()
                connection=handles[0].connection
                while True:
                    try:connection.socket.send(b'x'*65536)
                    except BlockingIOError:break
                connection.test_command='stop'
                planner.close()
                assert ready.exists(),'stop never reached write backpressure'
            if scenario=='public-gap':
                assert planner._operation_started==5. and planner._deadline==5.8
                fake_clock[0]=6.;ready.touch()
            if scenario!='public-stop':planner.finish(path)
        except (planning.EdgePlanCancelledError,planning.EdgePlanTimeoutError) as error:
            assert isinstance(error,planning.EdgePlanTimeoutError if mode=='timeout' else planning.EdgePlanCancelledError)
            assert ready.exists(),'fault was never activated'
            reference=planner._deadline if mode=='timeout' else set_times[0]
            lateness=0. if scenario=='public-gap' else observations[0]-reference
            assert 0 <= lateness < .3,lateness
            error_name=type(error).__name__
        except RuntimeError as error:
            assert (scenario=='public-source-error' and 'ordinary child source failure' in str(error)) or (scenario=='public-startup' and 'second process started then failed' in str(error))
            if scenario=='public-source-error':assert error.__cause__ is not None
            lateness=0.;error_name=type(error).__name__
        else:
            assert scenario=='public-stop','public fault returned successfully'
            lateness=0.;error_name='explicit-close'
        return {'scenario':scenario,'mode':mode,'error':error_name,'observed_lateness':lateness,'clock_anchor':planner._operation_started,
            'blocked_command':handles[0].connection.test_command if handles else None,
            'elapsed_seconds':time.monotonic()-started,'child_pids':[h.process.pid for h in handles],
            'production_children_dead':all(not h.process.is_alive() for h in handles),
            'production_endpoints_closed':all(h.connection.socket.fileno()==-1 for h in handles),
            'lock_exists':path.with_name(path.name+'.lock').exists(),
            'spill_exists':planner.spill_dir.exists(),
            'failure_marker':path.with_name(path.name+'.failed').read_text() if path.with_name(path.name+'.failed').exists() else None,
            'heartbeat_alive':any(t.name=='edge-plan-heartbeat' for t in threading.enumerate())}
    finally:
        stop_setter.set()
        for handle in handles:
            handle.connection.close();_reap(handle.process)
        planner.close(terminate=True)
        if timer:timer.join(1)


if __name__=='__main__':
    scenario,mode,output=sys.argv[1:]
    result=private_fault(scenario,mode) if scenario.startswith('private-') else public_fault(scenario,mode,output)
    Path(output).write_text(json.dumps(result,indent=2)+'\n')
