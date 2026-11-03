"""Runner safety and exact-inventory controls; development tooling only."""
import copy
import json
from pathlib import Path
from types import SimpleNamespace
import sys
import subprocess
import time

import pytest
import psutil

from scripts.installed_pytest import Receipt
from scripts.verify_installed import check_git_ancestry, cleanup_owned, main, run, runtime_environment, safe_relative, validate_inventory


def test_malformed_receipt_fails_without_losing_owned_cleanup(tmp_path):
    process = subprocess.Popen([sys.executable, '-c', 'import time;time.sleep(60)'], start_new_session=True)
    child = psutil.Process(process.pid)
    parent = psutil.Process()
    log = tmp_path/'provenance.jsonl'
    log.write_bytes(json.dumps({'kind': 'python_child', 'pid': parent.pid,
        'created': parent.create_time(), 'child_pid': child.pid,
        'child_created': child.create_time()}).encode() + b'\ninvalid complete JSON record\n\xff\n')
    try:
        result = cleanup_owned(log)
        assert result['cleanup_errors'] == ['invalid provenance record during owned cleanup'] * 2
        assert result['leftover_owned'] == [{'pid': child.pid, 'created': child.create_time()}]
        process.wait(timeout=5)
        assert not child.is_running() or child.status() == psutil.STATUS_ZOMBIE
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


def inventory():
    return {'schema_version': 1, 'package_payload': {n:'a'*64 for n in (
        'neo4j_graph_loader/__init__.py', 'neo4j_graph_loader/data/graph_schema.yaml',
        'neo4j_graph_loader/data/social_schema.yaml')},
        'lanes': {'probe': {'kind': 'installed unit regression',
            'stage_files': {'tests/test_probe.py': 'b'*64},
            'current_test_files': ['tests/test_probe.py'],
            'test_ids': ['tests/test_probe.py::test_probe']}}}


@pytest.mark.parametrize('path', [
    '../tests/x.py', '/tests/x.py', 'tests/../x.py', 'tests//x.py', '.env',
    'tests/.env.copy', 'src/neo4j_graph_loader/cli.py', 'tests/src/cli.py',
    'examples/imdb/datasets/data.csv', '.git/config', 'examples/imdb/tool.py',
])
def test_staged_paths_reject_sources_secrets_and_escape(path):
    with pytest.raises(ValueError):
        safe_relative(path)


def test_inventory_rejects_silent_file_or_id_reduction():
    original = inventory()
    assert validate_inventory(original,'probe')
    for key in ('current_test_files','test_ids','stage_files'):
        data = copy.deepcopy(original)
        data['lanes']['probe'][key].clear()
        with pytest.raises(ValueError):
            validate_inventory(data,'probe')


def test_live_service_lane_is_not_an_installed_unit_pass():
    data=inventory();data['lanes']['probe']['kind']='proposed actual-service harness/probe lane'
    with pytest.raises(ValueError,match='E09'):
        validate_inventory(data,'probe')


def test_runtime_environment_drops_inherited_paths_plugins_and_services(monkeypatch):
    for name in ('PYTHONPATH','PYTHONHOME','PYTEST_ADDOPTS','PYTEST_PLUGINS','NEO4J_URI','KAFKA_BOOTSTRAP_SERVERS','GRAPH_LOADER_NODE_IMAGE'):
        monkeypatch.setenv(name,'inherited-sentinel')
    env=runtime_environment(Path(sys.executable))
    assert 'inherited-sentinel' not in env.values()
    assert env['PYTEST_DISABLE_PLUGIN_AUTOLOAD']=='1'


def test_collection_disappearance_and_deselection_fail_closed(tmp_path):
    receipt=Receipt(['tests/x.py::test_one'],{},tmp_path/'receipt.json',True)
    receipt.pytest_collection_finish(SimpleNamespace(items=[]))
    receipt.pytest_deselected([SimpleNamespace(nodeid='tests/x.py::test_one')])
    session=SimpleNamespace(exitstatus=0)
    receipt.pytest_sessionfinish(session,0)
    assert session.exitstatus==1
    assert receipt.errors==['collected test IDs/order differ from frozen inventory','selected tests were deselected']


def test_unapproved_skip_is_not_a_pass(tmp_path):
    node='tests/x.py::test_one'
    receipt=Receipt([node],{},tmp_path/'receipt.json')
    receipt.collected=[node]
    receipt.pytest_runtest_logreport(SimpleNamespace(nodeid=node,when='setup',outcome='skipped',longrepr='new skip',skipped=True,failed=False))
    session=SimpleNamespace(exitstatus=0);receipt.pytest_sessionfinish(session,0)
    assert session.exitstatus==1
    assert receipt.errors==['unapproved skip/xfail: '+node]


def test_external_process_timeout_is_retained_and_bounded(tmp_path):
    receipts=[];start=time.monotonic()
    with pytest.raises(RuntimeError,match='retained output'):
        run([sys.executable,'-c','import time;time.sleep(60)'],cwd=tmp_path,
            env=runtime_environment(Path(sys.executable)),log=tmp_path/'child.log',timeout=.05,receipts=receipts)
    assert time.monotonic()-start<7
    assert receipts[0]['timeout'] and receipts[0]['exit_code']!=0


@pytest.mark.parametrize('pointer',[False,True])
def test_resolved_git_ancestor_rejects_before_staging(tmp_path,pointer):
    git=tmp_path/'.git'
    if pointer:
        git.write_text('gitdir: /owned/elsewhere\n')
    else:
        git.mkdir()
    with pytest.raises(ValueError,match='every Git'):
        check_git_ancestry(tmp_path/'new'/'output')


def test_cleanup_failure_cannot_yield_a_successful_command(tmp_path,monkeypatch):
    monkeypatch.setattr('scripts.verify_installed.cleanup_owned',lambda _path:{'leftover_owned':[],'cleanup_errors':['owned cleanup failed']})
    receipts=[]
    with pytest.raises(RuntimeError):
        run([sys.executable,'-c','pass'],cwd=tmp_path,env=runtime_environment(Path(sys.executable)),
            log=tmp_path/'child.log',timeout=10,receipts=receipts)
    assert receipts[0]['exit_code']==0 and receipts[0]['owned_cleanup']['cleanup_errors']


def test_cleanup_never_signals_a_reused_or_foreign_identity(tmp_path,monkeypatch):
    import json
    class Reused:
        def __init__(self,pid):
            self.pid=pid
        def create_time(self):
            return 99
        def terminate(self):
            raise AssertionError('must not signal a mismatching identity')
    path=tmp_path/'provenance.jsonl'
    path.write_text(json.dumps({'pid':12345,'created':1,'kind':'process'})+'\n')
    monkeypatch.setattr('scripts.verify_installed.psutil.Process',Reused)
    assert cleanup_owned(path)=={'leftover_owned':[],'cleanup_errors':[]}


@pytest.mark.parametrize('symlink',[False,True])
def test_rejected_existing_output_preserves_previous_result(tmp_path,symlink):
    import hashlib
    import json
    import zipfile
    data=inventory();data['package_version']='0.1.0a1.dev0'
    payload={path:b'# fixture\n' for path in data['package_payload']}
    dist='neo4j_graph_loader-0.1.0a1.dev0.dist-info/'
    metadata={dist+name:b'' for name in ('WHEEL','top_level.txt','RECORD','licenses/LICENSE','entry_points.txt')}
    metadata[dist+'METADATA']=b'Name: neo4j-graph-loader\nVersion: 0.1.0a1.dev0\nProvides-Extra: postgres\nProvides-Extra: sql\n'
    sha=lambda content:hashlib.sha256(content).hexdigest()
    data['package_payload']={path:sha(content) for path,content in payload.items()}
    data['distribution_files']={path:sha(content) for path,content in metadata.items()}
    wheel=tmp_path/'fixture.whl'
    with zipfile.ZipFile(wheel,'w') as archive:
        for path,content in (payload|metadata).items():
            archive.writestr(path,content)
    inventory_path=tmp_path/'inventory.json';inventory_path.write_text(json.dumps(data))
    existing=tmp_path/'previous';existing.mkdir();sentinel=existing/'result.json'
    sentinel.write_text('previous evidence sentinel\n');before=sentinel.read_bytes()
    output=existing
    if symlink:
        output=tmp_path/'alias';output.symlink_to(existing,target_is_directory=True)
    assert main(['--lane','probe','--wheel',str(wheel),'--inventory',str(inventory_path),'--output',str(output)])==1
    assert sentinel.read_bytes()==before


@pytest.mark.parametrize('cleanup_raises', [False, True])
def test_interrupted_supervisor_reaps_owned_child_and_retains_receipt(tmp_path, cleanup_raises):
    import os
    import signal

    root = Path(__file__).resolve().parents[1]
    identity = tmp_path/'child.json'
    result_path = tmp_path/'interrupted.json'
    code = '''import json,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from scripts import verify_installed as runner
if sys.argv[4]=='True':
 def fail_cleanup(_path):raise RuntimeError('controlled cleanup failure')
 runner.cleanup_owned=fail_cleanup
receipts=[]
worker='import json,os,time,psutil;from pathlib import Path;Path('+repr(sys.argv[2])+').write_text(json.dumps({"pid":os.getpid(),"created":psutil.Process().create_time()}));time.sleep(60)'
try:
 runner.run([sys.executable,'-c',worker],cwd=Path(sys.argv[2]).parent,env=runner.runtime_environment(Path(sys.executable)),log=Path(sys.argv[2]).with_suffix('.log'),timeout=20,receipts=receipts)
except KeyboardInterrupt:
 Path(sys.argv[3]).write_text(json.dumps({'interrupt_preserved':True,'receipts':receipts}))
'''
    supervisor = subprocess.Popen([sys.executable, '-I', '-c', code, str(root), str(identity),
                                    str(result_path), str(cleanup_raises)], start_new_session=True)
    child = None
    created = None
    try:
        deadline = time.monotonic()+5
        while not identity.exists() and time.monotonic()<deadline:
            time.sleep(.01)
        row = json.loads(identity.read_text())
        child = psutil.Process(row['pid'])
        created = row['created']
        assert child.create_time() == created
        os.kill(supervisor.pid, signal.SIGINT)
        supervisor.wait(timeout=10)
        assert supervisor.returncode == 0
        result = json.loads(result_path.read_text())
        assert result['interrupt_preserved']
        assert not child.is_running() or child.status() == psutil.STATUS_ZOMBIE
        [receipt] = result['receipts']
        assert receipt['interruption'] == 'KeyboardInterrupt'
        assert receipt['exit_code'] is not None and receipt['exit_code'] != 0
        assert bool(receipt['owned_cleanup']['cleanup_errors']) == cleanup_raises
    finally:
        if child is not None and child.is_running() and child.create_time() == created:
            try:
                child.kill()
                child.wait(timeout=5)
            except psutil.NoSuchProcess:
                pass
        if supervisor.poll() is None:
            supervisor.kill()
            supervisor.wait(timeout=5)


@pytest.mark.parametrize('interruption', [KeyboardInterrupt, SystemExit])
def test_main_persists_owned_failure_manifest_and_preserves_interruption(tmp_path, monkeypatch, interruption):
    import hashlib
    import zipfile

    from scripts import verify_installed as runner

    root = tmp_path/'checkout'
    (root/'tests').mkdir(parents=True)
    fixture = b'def test_probe(): pass\n'
    (root/'tests/test_probe.py').write_bytes(fixture)
    monkeypatch.setattr(runner, 'ROOT', root)
    data = inventory()
    data['package_version'] = '0.1.0a1.dev0'
    data['lanes']['probe']['stage_files']['tests/test_probe.py'] = hashlib.sha256(fixture).hexdigest()
    payload = {path: b'# fixture\n' for path in data['package_payload']}
    dist = 'neo4j_graph_loader-0.1.0a1.dev0.dist-info/'
    metadata = {dist+name: b'' for name in ('WHEEL', 'top_level.txt', 'RECORD', 'licenses/LICENSE', 'entry_points.txt')}
    metadata[dist+'METADATA'] = b'Name: neo4j-graph-loader\nVersion: 0.1.0a1.dev0\nProvides-Extra: postgres\nProvides-Extra: sql\n'
    data['package_payload'] = {path: hashlib.sha256(value).hexdigest() for path, value in payload.items()}
    data['distribution_files'] = {path: hashlib.sha256(value).hexdigest() for path, value in metadata.items()}
    wheel = tmp_path/'fixture.whl'
    with zipfile.ZipFile(wheel, 'w') as archive:
        for path, value in (payload | metadata).items():
            archive.writestr(path, value)
    inventory_path = tmp_path/'inventory.json'
    inventory_path.write_text(json.dumps(data))
    output = tmp_path/'owned-output'
    def interrupt(*_args, **kwargs):
        kwargs['receipts'].append({'interruption': interruption.__name__})
        raise interruption('controlled interruption')
    monkeypatch.setattr(runner, 'run', interrupt)
    with pytest.raises(interruption, match='controlled interruption'):
        runner.main(['--lane', 'probe', '--wheel', str(wheel), '--inventory', str(inventory_path), '--output', str(output)])
    result = json.loads((output/'result.json').read_text())
    assert result['status'] == 'failed' and result['interruption'] == interruption.__name__
    assert result['commands'] == [{'interruption': interruption.__name__}]


def test_installed_guard_rejects_forkpty_before_creating_an_untracked_child(tmp_path):
    import os

    if not hasattr(os, 'forkpty'):
        pytest.skip('forkpty is unavailable on this platform')
    root = Path(__file__).resolve().parents[1]
    log = tmp_path/'provenance.jsonl'
    (tmp_path/'_e07_guard_config.json').write_text(json.dumps({
        'prefix': sys.prefix, 'payload': {}, 'provenance_log': str(log)}))
    guard = tmp_path/'guard.py'
    guard.write_bytes((root/'scripts/installed_guard.py').read_bytes())
    code = '''import os,runpy,signal,sys,sysconfig
from pathlib import Path
site=Path(sys.argv[1]);get_paths=sysconfig.get_paths
sysconfig.get_paths=lambda *a,**kw:{**get_paths(*a,**kw),'purelib':str(site)}
runpy.run_path(str(site/'guard.py'))
pid,fd=os.forkpty()
if pid==0:os._exit(0)
os.kill(pid,signal.SIGTERM);os.waitpid(pid,0);os.close(fd)
'''
    result = subprocess.run([sys.executable, '-I', '-c', code, str(tmp_path)],
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 86, result.stderr
    rows = [json.loads(line) for line in log.read_text().splitlines()]
    assert any(row['kind'] == 'violation' and 'forkpty' in row['message'] for row in rows)
    assert {row['pid'] for row in rows} == {rows[0]['pid']}
    assert not any(row['kind'] == 'python_child' for row in rows)
