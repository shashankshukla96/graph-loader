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
    metadata[dist+'METADATA']=b'Name: neo4j-graph-loader\nVersion: 0.1.0a1.dev0\n'
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
