"""Safe preflight failures and separate host/container coordinator addresses."""
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from neo4j_graph_loader import cli
from neo4j_graph_loader._docker_preflight import check, DockerPreflightError, probe
from neo4j_graph_loader.orchestrator.docker_service import DockerService
from tests.docker_support import configure_images


def service(tmp_path):
    for name in ('rejections','plans','spill','timings'):
        (tmp_path/name).mkdir()
    schema=tmp_path/'schema.yaml';schema.write_text('nodes: []')
    runtime=SimpleNamespace(root=tmp_path,schema=schema,**{n:tmp_path/n for n in ('rejections','plans','spill','timings')})
    client=configure_images(MagicMock())
    client.api.base_url='http+docker://localhost'
    client.api.create_container.return_value={'Id':'a'*64}
    client.api.create_host_config.side_effect=lambda **kwargs:kwargs
    client.networks.get.return_value.attrs={'Driver':'bridge','Name':'owned-net'}
    return DockerService(client=client,runtime=runtime)


def probe_call(s):
    call=s.client.api.create_container.call_args.kwargs
    return call | call['host_config']


@pytest.mark.parametrize('endpoint',['http://remote:2375','https://remote:2376','http+docker://ssh'])
def test_unsupported_remote_daemon_rejected_before_network_or_launch(tmp_path,endpoint):
    s=service(tmp_path);s.client.api.base_url=endpoint
    with pytest.raises(DockerPreflightError,match='unsupported Docker endpoint'):
        check(s,'owned-net')
    s.client.networks.get.assert_not_called();s.client.api.create_container.assert_not_called()


@pytest.mark.parametrize('attrs',[{'Driver':'host','Name':'host'},{'Driver':'bridge','Name':'bridge'}])
def test_requires_user_defined_bridge_dns(tmp_path,attrs):
    s=service(tmp_path);s.client.networks.get.return_value.attrs=attrs
    with pytest.raises(DockerPreflightError,match='user-defined'):
        check(s,'bad-net')
    s.client.api.create_container.assert_not_called()


def test_missing_network_error_does_not_echo_daemon_body(tmp_path):
    s=service(tmp_path);s.client.networks.get.side_effect=RuntimeError('network-secret-marker')
    with pytest.raises(DockerPreflightError,match='network unavailable') as error:
        check(s,'bad-net')
    assert 'network-secret-marker' not in str(error.value)


@pytest.mark.parametrize('kind',['create','start','wait','bad-json','service','wrong-marker'])
def test_probe_failures_remove_only_owned_container_and_markers(tmp_path,kind):
    s=service(tmp_path);container=s.client.api
    container.wait.return_value={'StatusCode':1}
    container.logs.return_value=b'{"ok":false,"phase":"neo4j-auth"}'
    if kind=='create':s.client.api.create_container.side_effect=RuntimeError('daemon-secret-marker')
    elif kind=='start':container.start.side_effect=RuntimeError('start-secret-marker')
    elif kind=='wait':container.wait.side_effect=TimeoutError('timeout-secret-marker')
    elif kind=='bad-json':container.logs.return_value=b'secret-marker invalid'
    elif kind=='wrong-marker':
        container.wait.return_value={'StatusCode':0};container.logs.return_value=b'{"ok":true}'
    with pytest.raises(DockerPreflightError) as error:
        check(s,'owned-net')
    assert 'secret-marker' not in str(error.value)
    if kind!='create':container.remove_container.assert_called_once_with('a'*64,force=True)
    assert not list(tmp_path.rglob('.graph-loader-preflight-*'))


def test_probe_mounts_are_strict_and_checks_shared_writes(tmp_path):
    s=service(tmp_path);container=s.client.api
    def wait(*args,**kwargs):
        command=probe_call(s)['command']
        for name in ('rejections','timings'):
            (Path(getattr(s.runtime,name))/(command[1]+'-'+name)).write_text(command[2]+'-container')
        return {'StatusCode':0}
    container.wait.side_effect=wait;container.logs.return_value=b'{"ok":true}'
    check(s,'owned-net')
    call=probe_call(s)
    assert call['read_only'] is True
    assert len(call['mounts'])==5 and all(m['Type']=='bind' for m in call['mounts'])
    assert {m['Target']:m['ReadOnly'] for m in call['mounts']} == {
        '/app/runtime_schema.yaml':True, '/app/preflight/rejections':False,
        '/app/preflight/plans':True, '/app/preflight/spill':True, '/app/preflight/timings':False}
    container.start.assert_called_once()
    container.remove_container.assert_called_once_with('a'*64,force=True)
    assert not list(tmp_path.rglob('.graph-loader-preflight-*'))


def test_lease_host_control_port_is_separate_from_container_dns(tmp_path):
    s=service(tmp_path);s.run_bucket_lease_server(run_id='owned',bucket_count=4)
    call=s.client.containers.run.call_args.kwargs
    assert call['ports']['7700/tcp']==('127.0.0.1',None)
    c=MagicMock();c.name='lease-owned';c.attrs={'NetworkSettings':{
        'Networks':{'owned-net':{'IPAddress':'172.20.0.3'}},
        'Ports':{'7700/tcp':[{'HostIp':'127.0.0.1','HostPort':'40001'}]}}}
    assert s.bucket_lease_address(c,network='owned-net',host=True)=='127.0.0.1:40001'
    assert s.bucket_lease_address(c,network='owned-net',prefer_name=True)=='lease-owned:7700'
    assert s.bucket_lease_address(c,network='owned-net')=='172.20.0.3:7700'
    c.attrs['NetworkSettings']['Ports']={}
    with pytest.raises(RuntimeError,match='published host control port'):
        s.bucket_lease_address(c,network='owned-net',host=True)


def test_public_start_preflight_failure_precedes_ddl(tmp_path,monkeypatch,caplog):
    s=MagicMock();s.preflight_topology.side_effect=DockerPreflightError('container preflight failed (neo4j-auth)')
    monkeypatch.setattr(cli,'DockerService',lambda **kwargs:s)
    monkeypatch.setattr(cli,'get_neo4j_driver',lambda *args:pytest.fail('DDL reached'))
    assert cli.main(['start','--mode','bulk','--skip-image-build','--run-dir',str(tmp_path/'runs')])==1
    assert 'neo4j-auth' in caplog.text


def test_container_probe_reports_mount_failure_without_exception_text(monkeypatch,capsys):
    monkeypatch.setattr(Path,'read_bytes',lambda self:(_ for _ in ()).throw(OSError('sensitive-path-marker')))
    assert probe('hash','marker','token')==1
    assert capsys.readouterr().out.strip()=='{"ok": false, "phase": "mount"}'


def test_probe_cleanup_failure_prevents_success(tmp_path):
    s=service(tmp_path);container=s.client.api
    def wait(*args,**kwargs):
        command=probe_call(s)['command']
        for name in ('rejections','timings'):
            (Path(getattr(s.runtime,name))/(command[1]+'-'+name)).write_text(command[2]+'-container')
        return {'StatusCode':0}
    container.wait.side_effect=wait;container.logs.return_value=b'{"ok":true}'
    container.remove_container.side_effect=RuntimeError('cleanup-secret-marker')
    with pytest.raises(DockerPreflightError,match='cleanup failed') as error:
        check(s,'owned-net')
    assert 'cleanup-secret-marker' not in str(error.value)
    assert not list(tmp_path.rglob('.graph-loader-preflight-*'))


@pytest.mark.parametrize('present',[False,True])
def test_external_plan_and_shared_override_directories_preserve_identity_and_ownership(tmp_path,present):
    import hashlib
    s=service(tmp_path);parent=tmp_path/'external';parent.mkdir()
    plan=parent/'input.json'
    if present:plan.write_text('{}')
    sentinel=parent/'foreign';sentinel.write_text('preserve')
    s.runtime.spill=parent;s.runtime.timings=parent
    container=s.client.api
    def wait(*args,**kwargs):
        call=probe_call(s);marker,token=call['command'][1:]
        by_target={m['Target']:m for m in call['mounts']}
        assert by_target['/app/preflight/plans']['Source']==str(parent)
        assert (parent/(marker+'-plans')).read_text()==token
        assert (parent/(marker+'-spill')).read_text()==token
        assert (parent/(marker+'-timings')).read_text()==token
        if present:
            assert by_target['/app/preflight/edge-plan.json']['Source']==str(plan)
            assert by_target['/app/preflight/edge-plan.json']['ReadOnly']
            assert call['environment']['GRAPH_LOADER_PREFLIGHT_PLAN_SHA256']==hashlib.sha256(b'{}').hexdigest()
        else:assert '/app/preflight/edge-plan.json' not in by_target
        for name in ('rejections','timings'):
            (Path(getattr(s.runtime,name))/(marker+'-'+name)).write_text(token+'-container')
        return {'StatusCode':0}
    container.wait.side_effect=wait;container.logs.return_value=b'{"ok":true}'
    check(s,'owned-net',plan_path=str(plan))
    assert plan.exists() is present and sentinel.read_text()=='preserve'
    assert not list(tmp_path.rglob('.graph-loader-preflight-*'))


@pytest.mark.parametrize("failure",["write","close"])
def test_marker_write_failure_removes_exclusively_created_path(tmp_path,monkeypatch,failure):
    s=service(tmp_path);original=Path.open
    class FailingWrite:
        def __init__(self,stream):self.stream=stream
        def __enter__(self):return self
        def write(self,value):
            if failure=='write':raise OSError('write-secret-marker')
            self.stream.write(value)
        def __exit__(self,*args):
            self.stream.close()
            if failure=='close':raise OSError('close-secret-marker')
    def opened(path,*args,**kwargs):
        stream=original(path,*args,**kwargs)
        return FailingWrite(stream) if path.name.startswith('.graph-loader-preflight-') else stream
    monkeypatch.setattr(Path,'open',opened)
    with pytest.raises(DockerPreflightError) as error:check(s,'owned-net')
    assert 'write-secret-marker' not in str(error.value)
    assert not list(tmp_path.rglob('.graph-loader-preflight-*'))
    s.client.api.create_container.assert_not_called()


def test_probe_retains_id_without_sdk_inspection(tmp_path):
    s=service(tmp_path)
    s.client.containers.get.side_effect=RuntimeError('inspect-secret-marker')
    s.client.api.start.side_effect=RuntimeError('start-secret-marker')
    with pytest.raises(DockerPreflightError):check(s,'owned-net')
    s.client.api.remove_container.assert_called_once_with('a'*64,force=True)
    s.client.containers.create.assert_not_called()
    s.client.containers.get.assert_not_called()
