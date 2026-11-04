"""Private S5 codec, incremental-frame and externally bounded real process controls."""
from dataclasses import replace
import json
from pathlib import Path
import socket
import struct
import subprocess
import sys

import numpy as np
import pytest

from neo4j_graph_loader.loader._planner_transport import (
    CodecLimits, FrameProtocolError, FrameStream, HEADER, MAGIC, WaitControl, WaitExpired, decode, encode,
)
from neo4j_graph_loader.loader.edge_plan import ChunkSpec

ROOT=Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('value',[None,True,False,-(2**127),2**128-1,.25,'héllo',b'bytes',
    ('stop',None),('counter',None),('degrees',b'\x00'*(1<<19)),
    ('ok',{'tasks':[('VISITED#0','VISITED',2,.01)],'seconds':.01,'rejected':0}),
    {'Person':{b'i:1':3,b's:key':2}},
    ('emit',{'resources':{'buckets':2,'labels':['Person'],'hubs':{'Person':[1,1.5,{'date':'2020-01-01'}]}}}),
    ('ok',([ChunkSpec('T:0','T',('T',1,2),0,(1,2),3,0,1,100,0)],{'T':.1},{'peak_rss_mb':4.5})),
])
def test_closed_codec_round_trips_actual_shapes(value):
    assert decode(encode(value))==value


def test_closed_codec_round_trips_maximum_actual_counter():
    counter=np.arange(1<<22,dtype=np.uint32)
    result=decode(encode(('ok',counter)))[1]
    assert result.dtype==counter.dtype and np.array_equal(result,counter)


class CallbackTrap:
    def __reduce__(self): raise AssertionError('reducer invoked')
    @property
    def value(self): raise AssertionError('property invoked')


@pytest.mark.parametrize('value',[CallbackTrap(),type('CustomStr',(str,),{})('x'),
    np.array([object()],dtype=object),np.array([1.],dtype='f8'),np.zeros((2,2),dtype='u4'),float('nan')])
def test_codec_rejects_unapproved_objects_without_callbacks(value):
    with pytest.raises(FrameProtocolError): encode(value)


@pytest.mark.parametrize('wire',[b'?',b's\0\0\0\x05a',b'r'+struct.pack('!d',float('inf')),
    b'a\x04b\0\0\0\0',b'a\0b\0\0\0\x03abc',b'nextra',b'l\xff\xff\xff\xff'])
def test_codec_rejects_corruption(wire):
    with pytest.raises(FrameProtocolError): decode(wire)


def test_codec_never_unpickles_parent_replies(monkeypatch):
    import pickle
    monkeypatch.setattr(pickle,'loads',lambda *a:(_ for _ in ()).throw(AssertionError('pickle callback')))
    assert decode(encode(('ok',{'normal':1})))==('ok',{'normal':1})
    with pytest.raises(FrameProtocolError): decode(b'\x80\x05malicious pickle')


def test_scalar_depth_node_and_cumulative_limits_are_effective():
    limits=CodecLimits(scalar=3,depth=2,nodes=5,allocation=4)
    for value in [b'1234',[[[1]]],[1,2,3,4,5],[b'123',b'456']]:
        with pytest.raises(FrameProtocolError): encode(value,limits=limits)
        with pytest.raises(FrameProtocolError): decode(encode(value),limits=limits)
    assert decode(encode([[1]],limits=limits),limits=limits)==[[1]]


def test_frame_limit_exact_and_over():
    value=('ok',b'123')
    payload=encode(value)
    limits=replace(CodecLimits(),frame=len(payload))
    assert decode(encode(value,limits=limits),limits=limits)==value
    for function,argument in [(encode,value),(decode,payload)]:
        with pytest.raises(FrameProtocolError): function(argument,limits=replace(limits,frame=len(payload)-1))


def test_actual_maximum_frame_and_cumulative_allocation():
    limits=CodecLimits()
    counter=np.zeros(1<<22,dtype=np.uint32)
    values=[counter]*3+[b'x'*(1<<20)]*15
    remaining=limits.frame-len(encode(values))-5
    values.append(b'x'*remaining)
    payload=encode(values)
    assert len(payload)==limits.frame
    result=decode(payload)
    assert len(result)==19 and np.array_equal(result[0],counter)
    values[-1]+=b'x'
    with pytest.raises(FrameProtocolError): encode(values)


@pytest.mark.parametrize('prefix',[b'GL',HEADER.pack(MAGIC,10)+b'short',HEADER.pack(MAGIC,(64<<20)+1)])
def test_truncated_header_body_and_oversize_fail_closed(prefix):
    parent,peer=socket.socketpair()
    try:
        peer.sendall(prefix);peer.close()
        with pytest.raises(FrameProtocolError): FrameStream(parent).recv()
    finally:parent.close();peer.close()


def test_partial_progress_and_waits_recheck_absolute_deadline(monkeypatch):
    from neo4j_graph_loader.loader import _planner_transport as transport
    now=[0.]; waits=[]
    class Sock:
        def setblocking(self,value): assert value is False
        def recv(self,count): raise BlockingIOError
    class Selector:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def register(self,*args):pass
        def select(self,timeout):waits.append(timeout);now[0]+=timeout
    monkeypatch.setattr(transport.selectors,'DefaultSelector',Selector)
    control=WaitControl(deadline=.25,clock=lambda:now[0])
    with pytest.raises(WaitExpired):FrameStream(Sock(),check=control.check).recv()
    assert now[0]==pytest.approx(.25) and all(0<x<=.1 for x in waits)


@pytest.mark.parametrize('scenario',['private-recv','private-send'])
@pytest.mark.parametrize('mode',['timeout','cancel'])
def test_real_private_fault_has_an_independent_supervisor(tmp_path,scenario,mode):
    output=tmp_path/'fault'
    result=subprocess.run([sys.executable,str(ROOT/'tests/fixtures/run_fault.py'),
        scenario,'--mode',mode,'--output',str(output),'--python',sys.executable],
        cwd=ROOT,capture_output=True,text=True,timeout=30)
    assert result.returncode==0,result.stdout+result.stderr+(output/'client.log').read_text()
    supervisor=json.loads((output/'supervisor.json').read_text())
    client=json.loads((output/'client.json').read_text())
    assert not supervisor['timeout'] and supervisor['exit_code']==0
    assert client['children_dead'] and client['endpoints_closed'] and client['fixture_timer_dead']


@pytest.mark.parametrize('dtype',['<u4','<u8','>u4','>u8'])
def test_supported_counter_dtypes_preserve_values_and_dtype(dtype):
    array=np.arange(10,dtype=dtype)
    result=decode(encode(array))
    assert result.dtype==array.dtype and np.array_equal(result,array)


def test_incremental_read_write_handles_partial_progress_without_losing_bytes(monkeypatch):
    from neo4j_graph_loader.loader import _planner_transport as transport
    value=('ok',{'bytes':b'body'})
    payload=encode(value);wire=HEADER.pack(MAGIC,len(payload))+payload
    waits=[];now=[0.]
    class Sock:
        def __init__(self):self.input=wire;self.sent=bytearray();self.block=True
        def setblocking(self,value):assert value is False
        def recv(self,count):
            self.block=not self.block
            if self.block:raise BlockingIOError
            part=self.input[:min(3,count)];self.input=self.input[len(part):];return part
        def send(self,part):
            self.block=not self.block
            if self.block:raise BlockingIOError
            part=part[:3];self.sent.extend(part);return len(part)
    class Selector:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def register(self,*args):pass
        def select(self,timeout):waits.append(timeout);now[0]+=.001
    monkeypatch.setattr(transport.selectors,'DefaultSelector',Selector)
    sock=Sock();stream=FrameStream(sock,check=WaitControl(deadline=1,clock=lambda:now[0]).check)
    assert stream.recv()==value
    stream.send(value)
    assert bytes(sock.sent)==wire and len(waits)>2 and all(0<x<=.1 for x in waits)


@pytest.mark.parametrize('function,value',[(encode,[1]*2048),(decode,encode([1]*2048))])
def test_codec_checks_interruptions_during_bounded_work(function,value):
    from neo4j_graph_loader.loader._planner_transport import WaitCancelled
    checks=[]
    def check():
        checks.append(True)
        if len(checks)==2:raise WaitCancelled('during codec')
        return .1
    with pytest.raises(WaitCancelled):function(value,check=check)
    assert len(checks)==2


@pytest.mark.parametrize('function,value',[(encode,1),(decode,encode(1))])
def test_codec_checks_the_deadline_after_computation(function,value):
    now=[0.]
    control=WaitControl(deadline=.15,clock=lambda:now[0])
    def check():now[0]+=.1;return control.check()
    with pytest.raises(WaitExpired):function(value,check=check)


@pytest.mark.parametrize('stage',['RawArray','Process','FrameStream','start'])
def test_partial_worker_construction_closes_both_owned_sockets(monkeypatch,stage):
    from unittest.mock import MagicMock
    from neo4j_graph_loader.loader import edge_planner as implementation
    real_pair=socket.socketpair;owned=[];failure=RuntimeError('constructor failure')
    def pair():
        sockets=real_pair();owned.extend(sockets);return sockets
    monkeypatch.setattr(implementation.socket,'socketpair',pair)
    context=MagicMock()
    if stage in ('RawArray','Process'):getattr(context,stage).side_effect=failure
    elif stage=='FrameStream':
        def fail(*args,**kwargs):raise failure
        monkeypatch.setattr(implementation,'FrameStream',fail)
    else:context.Process.return_value.start.side_effect=failure
    with pytest.raises(RuntimeError) as caught:
        worker=implementation._ProcessWorker(context,{},'unused',{},None,None)
        if stage=='start':worker.start()
    assert caught.value is failure and len(owned)==2
    assert all(sock.fileno()==-1 for sock in owned)


def test_interrupted_outbound_frame_cannot_be_followed_by_stop(monkeypatch):
    from neo4j_graph_loader.loader import _planner_transport as transport
    sent=bytearray()
    class Sock:
        def setblocking(self,value):pass
        def send(self,data):
            if sent:raise BlockingIOError
            sent.extend(data[:2]);return 2
    stream=transport.FrameStream(Sock())
    def interrupted(*args):raise WaitExpired('interrupted write')
    monkeypatch.setattr(stream,'_wait',interrupted)
    with pytest.raises(WaitExpired):stream.send(('degrees',b'large'))
    partial=bytes(sent)
    with pytest.raises(FrameProtocolError,match='interrupted'):stream.send(('stop',None))
    assert bytes(sent)==partial and len(sent)==2
