"""Real E04 fault clients. The separate run_e04_fault supervisor owns emergency cleanup."""
import json
import multiprocessing as mp
import os
from pathlib import Path
import socket
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


def reap_resource_tracker(timeout=5.):
    """Stop and wait for this client's multiprocessing resource tracker.

    The spawn context starts one tracker child in the client's session. Some CPython releases (the host's 3.12.3; 3.12.14 stops it in ResourceTracker.__del__) never wait
    for it at exit: it noticed EOF on its pipe only after the client had died, so
    the supervisor's group probe (run_e04_fault.py, right after client.wait()) still found it and
    reported emergency_group_cleanup. Closing its pipe and reaping it here is deterministic. All
    fault children are already reaped, so no other process holds the pipe; the wait is bounded.
    """
    from multiprocessing import resource_tracker
    tracker=resource_tracker._resource_tracker
    with tracker._lock:
        fd,pid=tracker._fd,tracker._pid
        if fd is None or pid is None:return
        os.close(fd);tracker._fd=None
        deadline=time.monotonic()+timeout
        while not os.waitpid(pid,os.WNOHANG)[0]:
            if time.monotonic()>=deadline:raise AssertionError('resource tracker did not exit after its pipe closed')
            time.sleep(.002)
        tracker._pid=None


def private_fault(scenario,mode,*,workers=None,context=None):
    if workers is not None:
        from tests.fixtures.e08_fault_matrix import private_fault as matrix_fault
        return matrix_fault(scenario,mode,workers=workers,context=context)
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
    def __init__(self,ready,order=0,phase_marker=None):self.ready=ready;self.order=order;self.phase_marker=phase_marker
    def records(self,edge):
        if self.phase_marker is not None:Path(self.phase_marker).touch()
        Path(self.ready).touch()
        while True:time.sleep(10)
        yield


class FailingSource(EdgeRecordSource):
    edge_type='VISITED';size_hint=0;order=0
    def records(self,edge):
        raise LookupError('ordinary child source failure')
        yield


class PeerReadyFailingSource(FailingSource):
    """Ensure the unrelated real child is blocked before reporting a source error."""
    def __init__(self,ready):self.ready=ready
    def records(self,edge):
        deadline=time.monotonic()+3
        while not Path(self.ready).exists() and time.monotonic()<deadline:time.sleep(.005)
        if not Path(self.ready).exists():raise AssertionError('blocked peer never entered records')
        yield from super().records(edge)


def _public_partial_worker(sock,worker,bootstrap,config,producer=None,consumer=None,progress=None,parent_duplicate=None):
    if parent_duplicate is not None:parent_duplicate.close()
    stream=FrameStream(sock); stream.recv(initial=True)
    payload=encode(('ok',{'tasks':[],'seconds':0.,'padding':b'x'*(512<<10)}))
    sock.sendall(HEADER.pack(MAGIC,len(payload))+payload[:64])
    (Path(worker['spill_dir']).parent/'child-reply-started').touch()
    if worker.get('e08_partial_marker') is not None:Path(worker['e08_partial_marker']).touch()
    while True:time.sleep(10)


def _public_nonreading_worker(sock,worker,bootstrap,config,producer=None,consumer=None,progress=None,parent_duplicate=None):
    import numpy as np
    if parent_duplicate is not None:parent_duplicate.close()
    stream=FrameStream(sock); stream.recv(initial=True)
    stream.send(('ok',{'tasks':[],'seconds':0.}))
    command,payload=stream.recv();assert command=='counter'
    stream.send(('ok',np.ones(worker.get('e08_counter_entries',1<<22),dtype=np.uint32)))
    while True:time.sleep(10)


def public_fault(scenario,mode,output,*,workers=None,context=None):
    import selectors
    from neo4j_graph_loader import planning
    from neo4j_graph_loader.schema import load_schema
    from neo4j_graph_loader.loader import edge_planner as implementation
    root=Path(__file__).resolve().parents[2]
    directory=Path(output).parent; ready=directory/'fault-ready';path=directory/'plan.json'
    signal=threading.Event();stop_setter=threading.Event();set_times=[];observations=[];handles=[]
    timer=None
    mixed=scenario in {'public-mixed-error-default','public-mixed-error-timeout'}
    received_errors=[];cleanup_entries=[];ordinary_cause=None
    sentinel=directory/'caller-owned';sentinel.write_text('caller sentinel')
    fake_clock=[0.]
    parameterized=workers is not None
    prepared=parameterized and scenario not in {'public-cold','public-startup'}
    preparation=[];write_sizes=[]
    original_worker=implementation._ProcessWorker
    class RecordingWorker(original_worker):
        def __init__(self,*args,**kwargs):
            if parameterized and scenario=='public-send':args[1]['e08_counter_entries']=1<<18
            if parameterized and scenario=='public-partial':
                args[1]['e08_partial_marker']=str(directory/f"partial-reply-ready-{args[1]['worker_index']}")
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
        def recv(self):
            reply=super().recv()
            if mixed and reply[0]!='ok':received_errors.append((time.monotonic(),str(reply[1])))
            return reply
        def start(self):
            super().start()
            if scenario=='public-startup' and len(handles)==2:
                raise RuntimeError('second process started then failed')
        def send(self,command,payload):
            self.connection.test_command=command
            if scenario=='public-gap' and command=='scan':fake_clock[0]+=5
            if parameterized and scenario=='public-send' and command=='degrees':
                size=len(encode((command,payload)));buffer=self.connection.socket.getsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF)
                assert size>buffer,(size,buffer)
                write_sizes.append({'encoded_frame_bytes':size,'socket_send_buffer':buffer})
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
    if mixed:sources=[PeerReadyFailingSource(str(ready)),BlockedSource(str(ready),order=1)]
    if parameterized:
        if mixed:
            assert workers>=2,'mixed peer failures require at least two workers'
            sources=[PeerReadyFailingSource(str(ready)),*[BlockedSource(str(ready),order=i) for i in range(1,workers)]]
        elif scenario=='public-source-error':sources=[FailingSource(),*[BlockedSource(str(ready),order=i) for i in range(1,workers)]]
        elif scenario in {'public-blocked','public-many','public-stop','public-startup','public-cold'}:
            assert scenario!='public-startup' or workers>=2,'second-start failure requires two workers'
            sources=[BlockedSource(str(ready),order=i) for i in range(workers)]
        else:sources=[planning.IterableEdgeSource('VISITED',order=i) for i in range(workers)]
        blocked_markers=[]
        partial_markers=[directory/f'partial-reply-ready-{i}' for i in range(workers)] if scenario=='public-partial' else []
        for source in sources:
            if isinstance(source,BlockedSource):
                marker=directory/f'blocked-peer-{source.order}'
                source.phase_marker=str(marker);blocked_markers.append(marker)
        if prepared:
            from tests.fixtures.e08_fault_matrix import ready_worker,ready_partial_worker,ready_nonreading_worker
            implementation._planner_worker_main=(ready_partial_worker if scenario=='public-partial' else
                ready_nonreading_worker if scenario=='public-send' else ready_worker)
    options={} if not parameterized else {'context':mp.get_context(context)}
    planner=planning.producer_edge_planner(load_schema(root/'examples/social/graph_schema.yaml'),sources,
        planning.PlanSettings(topic='unused',workers=len(sources),memory_mb=1,hub_degree=1 if scenario=='public-send' else 0),
        'unused',plan_path=path,cancel_event=signal,timeout_seconds=.8 if mode=='timeout' and scenario not in {'public-stop','public-mixed-error-default'} else None,**options)
    if prepared:
        original_begin=planner._begin_operation
        def begin():
            started=time.monotonic();deadline=started+60
            while (not all((directory/f'child-boot-ready-{i}').exists() for i in range(workers))
                   or not all(marker.exists() for marker in blocked_markers)):
                if time.monotonic()>=deadline:raise AssertionError('prepared worker boot deadline exceeded')
                time.sleep(.005)
            while not all(marker.exists() for marker in partial_markers):
                if time.monotonic()>=deadline:raise AssertionError('prepared partial-reply deadline exceeded')
                time.sleep(.005)
            preparation.append(time.monotonic()-started)
            return original_begin()
        planner._begin_operation=begin
    if mixed:
        assert isinstance(planner.lanes[0][0].source,PeerReadyFailingSource)
        original_close=planner.close
        def close(*args,**kwargs):
            if planner._cleanup_started is None:cleanup_entries.append(time.monotonic())
            return original_close(*args,**kwargs)
        planner.close=close
    if scenario=='public-gap':planner._clock=lambda:fake_clock[0]
    original_check=planner._check
    def check():
        try:return original_check()
        except (planning.EdgePlanCancelledError,planning.EdgePlanTimeoutError):
            observations.append(time.monotonic());raise
    planner._check=check
    def cancel_when_ready():
        if parameterized:
            while planner._operation_started is None and not stop_setter.wait(.005):pass
            if stop_setter.is_set():return
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
                if parameterized:
                    assert planner._operation_started==5.*workers and planner._deadline==5.*workers+.8
                    fake_clock[0]=5.*workers+1.;ready.touch()
                else:
                    assert planner._operation_started==5. and planner._deadline==5.8
                    fake_clock[0]=6.;ready.touch()
            if scenario!='public-stop':planner.finish(path)
        except (planning.EdgePlanCancelledError,planning.EdgePlanTimeoutError) as error:
            assert not mixed,'received source error was replaced by cancellation/timeout'
            assert isinstance(error,planning.EdgePlanTimeoutError if mode=='timeout' else planning.EdgePlanCancelledError)
            if scenario!='public-cold':assert ready.exists(),'fault was never activated'
            reference=planner._deadline if mode=='timeout' else set_times[0]
            lateness=0. if scenario=='public-gap' else observations[0]-reference
            assert 0 <= lateness < .3,lateness
            error_name=type(error).__name__
        except RuntimeError as error:
            assert ((scenario=='public-source-error' or mixed) and 'ordinary child source failure' in str(error)) or (scenario=='public-startup' and 'second process started then failed' in str(error))
            if scenario=='public-source-error' or mixed:
                assert error.__cause__ is not None
                ordinary_cause=str(error.__cause__)
            lateness=0.;error_name=type(error).__name__
        else:
            assert scenario=='public-stop','public fault returned successfully'
            lateness=0.;error_name='explicit-close'
        if parameterized:
            assert all(not h.process.is_alive() for h in handles),'production cleanup left live children'
            assert all(h.connection.socket.fileno()==-1 for h in handles),'production endpoints remain open'
            assert not planner.spill_dir.exists() and not path.with_name(path.name+'.lock').exists()
            assert not any(t.name=='edge-plan-heartbeat' for t in threading.enumerate())
            assert not mp.active_children()
            assert time.monotonic()-planner._cleanup_started<10,'aggregate production cleanup exceeded10 seconds'
            assert len(handles)==(2 if scenario=='public-startup' else workers)
            if prepared and scenario!='public-startup':assert all(marker.exists() for marker in blocked_markers)
            if prepared:assert all(marker.exists() for marker in partial_markers)
            if scenario=='public-send':assert write_sizes and ready.exists()
        return {'scenario':scenario,'mode':mode,'error':error_name,'observed_lateness':lateness,'clock_anchor':planner._operation_started,
            'client_pid':os.getpid(),
            'requested_workers':workers,'actual_workers':len(handles),'context':context,
            'prepared_transport':prepared,'excluded_fixture_preparation_seconds':sum(preparation),
            'preparation_scope':'boot and every intended blocked/partial-reply peer' if prepared else None,
            'degrees_write_sizes':write_sizes,'fault_activated':ready.exists(),
            'blocked_peer_count':sum(marker.exists() for marker in blocked_markers) if parameterized else None,
            'expected_blocked_peers':len(blocked_markers) if parameterized else None,
            'partial_reply_peer_count':sum(marker.exists() for marker in partial_markers) if parameterized else None,
            'expected_partial_reply_peers':len(partial_markers) if parameterized else None,
            'blocked_command':handles[0].connection.test_command if handles else None,
            'elapsed_seconds':time.monotonic()-started,'child_pids':[h.process.pid for h in handles],
            'production_children_dead':all(not h.process.is_alive() for h in handles),
            'production_endpoints_closed':all(h.connection.socket.fileno()==-1 for h in handles),
            'lock_exists':path.with_name(path.name+'.lock').exists(),
            'spill_exists':planner.spill_dir.exists(),
            'failure_marker':path.with_name(path.name+'.failed').read_text() if path.with_name(path.name+'.failed').exists() else None,
            'heartbeat_alive':any(t.name=='edge-plan-heartbeat' for t in threading.enumerate()),
            'received_errors':received_errors,'ordinary_cause':ordinary_cause,
            'failure_to_cleanup_seconds':cleanup_entries[0]-received_errors[0][0] if mixed else None,
            'observation_to_raise_seconds':time.monotonic()-received_errors[0][0] if mixed else None,
            'blocked_peer_entered':ready.exists(),'configured_timeout':planner.timeout_seconds,
            'active_child_pids':[child.pid for child in mp.active_children()],
            'caller_sentinel_preserved':sentinel.read_text()=='caller sentinel',
            'caller_signal_preserved':not signal.is_set()}
    finally:
        stop_setter.set()
        for handle in handles:
            handle.connection.close();_reap(handle.process)
        planner.close(terminate=True)
        if timer:
            timer.join(1)
            if parameterized:assert not timer.is_alive(),'fixture cancellation thread remains alive'


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('scenario');parser.add_argument('mode',choices=('timeout','cancel'));parser.add_argument('output')
    parser.add_argument('--context',choices=('spawn',));parser.add_argument('--workers',type=int)
    args=parser.parse_args()
    if (args.context is None)!=(args.workers is None) or (args.workers is not None and not 1<=args.workers<=64):
        parser.error('explicit spawn context and1–64 workers must be provided together')
    scenario,mode,output=args.scenario,args.mode,args.output
    result=(private_fault(scenario,mode,workers=args.workers,context=args.context) if scenario.startswith('private-') else
            public_fault(scenario,mode,output,workers=args.workers,context=args.context))
    Path(output).write_text(json.dumps(result,indent=2)+'\n')
    reap_resource_tracker()  # leave no helper in the client's group for the supervisor's probe
