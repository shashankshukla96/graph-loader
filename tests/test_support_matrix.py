"""Checkout matrix tooling: finite dependency inputs and version drift controls."""
import ast
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import zipfile

import pytest

from scripts.installed_dependencies import read_constraints, read_wheelhouse, verify_versions
from scripts import support_matrix
from scripts import matrix_docker
from scripts import verify_installed
from scripts.verify_installed import run, runtime_environment


def test_partial_startup_keeps_real_startup_identity_without_inventing_imports():
    observed = {'child_pids': [11, 12], 'client_pid': 10, 'clock_anchor': None, 'error': 'RuntimeError',
                'failure_marker': 'RuntimeError: second process started then failed\n'}
    case = {'scenario': 'public-startup', 'workers': 64}
    rows = [{'kind': 'python_child', 'pid': 10, 'child_pid': pid} for pid in (11, 12)]
    rows.append({'kind': 'module', 'pid': 11, 'module': 'neo4j_graph_loader.planning'})
    result = verify_installed.validate_fault_children(observed, case, rows, {10, 11, 12}, {11, 12})
    assert result['partial_second_start'] and result['actual_child_imports']['12'] == []
    with pytest.raises(RuntimeError, match='launch/startup'):
        verify_installed.validate_fault_children(observed, case, rows, {10, 11}, {11, 12})
    with pytest.raises(RuntimeError, match='import provenance'):
        verify_installed.validate_fault_children(observed, {'scenario': 'public-many', 'workers': 2}, rows, {10, 11, 12}, {11, 12})
    with pytest.raises(RuntimeError, match='intended second-start failure'):
        verify_installed.validate_fault_children({**observed, 'clock_anchor': 1.}, case, rows, {10, 11, 12}, {11, 12})
    cold = {**observed, 'clock_anchor': 1., 'prepared_transport': False,
            'error': 'EdgePlanTimeoutError', 'configured_timeout': .8}
    result = verify_installed.validate_fault_children(cold, {'scenario': 'public-cold', 'workers': 2}, rows, {10, 11, 12}, {11, 12})
    assert result['cold_timeout'] and result['children_without_application_import_receipt'] == [12]
    with pytest.raises(RuntimeError, match='unmodified operation clock'):
        verify_installed.validate_fault_children({**cold, 'prepared_transport': True}, {'scenario': 'public-cold', 'workers': 2}, rows, {10, 11, 12}, {11, 12})


@pytest.mark.parametrize('fault', ['parent', 'launch', 'prefix', 'site', 'origin', 'hash'])
def test_case_provenance_rejects_unbound_children_and_application_origins(tmp_path, fault):
    site = tmp_path/'environment/lib/site-packages'
    config = {'prefix': str(tmp_path/'environment'), 'payload': {'neo4j_graph_loader/planning.py': 'a'*64}}
    def startup(pid, parent, created, tick):
        return dict(kind='process', pid=pid, ppid=parent, created=created, tick=tick,
                    prefix=config['prefix'], site=str(site))
    def module(pid, created, tick):
        return dict(kind='module', pid=pid, created=created, tick=tick,
                    module='neo4j_graph_loader.planning', origin=str(site/'neo4j_graph_loader/planning.py'), sha256='a'*64)
    rows = [startup(10, 5, 10., 100), module(10, 10., 110),
            dict(kind='python_child', pid=10, created=10., child_pid=11, child_created=11., launch_tick=115, tick=120),
            startup(11, 10, 11., 130), module(11, 11., 140)]
    path = tmp_path/'provenance.jsonl'
    path.write_text(''.join(json.dumps(row)+'\n' for row in rows))
    assert verify_installed.validate_provenance(path, config=config, site=site) == ({10, 11}, {11})
    if fault == 'parent': rows[3]['ppid'] = 999
    elif fault == 'launch': rows[2]['launch_tick'] = 135
    elif fault == 'prefix': rows[3]['prefix'] = str(tmp_path/'foreign-environment')
    elif fault == 'site': rows[3]['site'] = str(tmp_path/'foreign-site')
    elif fault == 'origin': rows[4]['origin'] = str(tmp_path/'source/neo4j_graph_loader/planning.py')
    else: rows[4]['sha256'] = 'b'*64
    path.write_text(''.join(json.dumps(row)+'\n' for row in rows))
    with pytest.raises(RuntimeError):
        verify_installed.validate_provenance(path, config=config, site=site)


@pytest.mark.parametrize('error', [RuntimeError('case failed'), KeyboardInterrupt()])
def test_process_case_failure_restores_exact_guard_and_retains_receipt(tmp_path, monkeypatch, error):
    stage = tmp_path/'stage'; stage.mkdir()
    fixture = stage/'client.py'; fixture.write_bytes(b'owned fixture\n')
    site = tmp_path/'site'; site.mkdir()
    work = tmp_path/'work'; work.mkdir()
    config = {'prefix': str(tmp_path/'environment'), 'payload': {}, 'provenance_log': 'original-log'}
    guard = site/'_e07_guard_config.json'
    original = (json.dumps(config, indent=2)+'\n').encode(); guard.write_bytes(original)
    monkeypatch.setattr(verify_installed, 'process_cases', lambda: [dict(scenario='external-success', context='spawn', workers=1, mode='timeout')])
    def failed_run(*args, **kwargs):
        assert guard.read_bytes() != original
        kwargs['receipts'].append({'failed_control': True})
        raise error
    monkeypatch.setattr(verify_installed, 'run', failed_run)
    with pytest.raises(type(error)) as captured:
        verify_installed.run_process_envelope(Path(sys.executable), stage, work, {}, site, config,
                                             {'client.py': hashlib.sha256(fixture.read_bytes()).hexdigest()})
    assert captured.value is error and guard.read_bytes() == original
    record = json.loads((work/'process-envelope/result.json').read_text())
    assert not record['complete'] and record['cases'][0]['status'] == 'failed'
    assert record['cases'][0]['commands'] == [{'failed_control': True}]
    assert json.loads((work/'process-envelope/guard-configuration.json').read_text())['restored']


@pytest.mark.parametrize('value', [
    '-r other.txt', '--index-url https://example.invalid', 'thing @ https://example.invalid/x.whl',
    '../private.whl', 'thing>=1', 'thing==1; python_version>"3"', 'thing==*',
    'a_b==1\na-b==1', 'thing==1 --hash=sha256:abc', '',
])
def test_constraints_reject_directives_paths_ranges_and_normalized_duplicates(tmp_path, value):
    path = tmp_path/'constraints.txt'
    path.write_text(value)
    with pytest.raises(ValueError):
        read_constraints(path)


def test_constraint_snapshot_retains_exact_bytes_and_pins(tmp_path):
    path = tmp_path/'constraints.txt'
    raw = b'# dated resolution\nPyYAML==6.0.1\nneo4j_rust_ext==5.20.0.0\n'
    path.write_bytes(raw)
    result = read_constraints(path)
    assert result == {'content': raw, 'sha256': hashlib.sha256(raw).hexdigest(),
                      'pins': {'pyyaml': '6.0.1', 'neo4j-rust-ext': '5.20.0.0'}}


def wheelhouse(tmp_path):
    directory = tmp_path/'wheels'
    directory.mkdir()
    content = io.BytesIO()
    with zipfile.ZipFile(content, 'w') as archive:
        archive.writestr('thing-1.dist-info/METADATA', 'Name: thing\nVersion: 1\n')
    wheel = directory/'thing-1-py3-none-any.whl'
    wheel.write_bytes(content.getvalue())
    manifest = tmp_path/'manifest.json'
    manifest.write_text(json.dumps({'schema_version': 1, 'files': {wheel.name: hashlib.sha256(wheel.read_bytes()).hexdigest()}}))
    return directory, manifest, wheel


@pytest.mark.parametrize('fault', ['extra', 'changed', 'symlink', 'unconstrained', 'wrong-version', 'escape'])
def test_offline_manifest_rejects_unrecorded_changed_or_unconstrained_wheels(tmp_path, fault):
    directory, manifest, wheel = wheelhouse(tmp_path)
    pins = {'thing': '1'}
    if fault == 'extra':
        (directory/'unrecorded.whl').write_bytes(b'extra')
    elif fault == 'changed':
        wheel.write_bytes(b'changed')
    elif fault == 'symlink':
        original = tmp_path/'original.whl'
        wheel.rename(original)
        wheel.symlink_to(original)
    elif fault == 'unconstrained':
        pins.clear()
    elif fault == 'wrong-version':
        pins['thing'] = '2'
    else:
        manifest.write_text(json.dumps({'schema_version': 1, 'files': {'../escape.whl': 'a'*64}}))
    with pytest.raises(ValueError):
        read_wheelhouse(directory, manifest, pins)


def test_offline_input_is_copied_from_verified_bytes(tmp_path):
    directory, manifest, wheel = wheelhouse(tmp_path)
    snapshot = read_wheelhouse(directory, manifest, {'thing': '1'})
    before = wheel.read_bytes()
    wheel.write_bytes(b'changed after snapshot')
    assert snapshot['contents'][wheel.name] == before
    assert snapshot['files'][wheel.name] == hashlib.sha256(before).hexdigest()


@pytest.mark.parametrize('actual', [
    {'neo4j-graph-loader': '0.1.0a1.dev0', 'PyYAML': '6.0.2'},
    {'neo4j-graph-loader': '0.1.0a1.dev0', 'unconstrained': '1'},
    {'neo4j-graph-loader': '9', 'PyYAML': '6.0.1'},
])
def test_tool_installation_cannot_silently_drift_runtime_or_introduce_unpinned_dependency(actual):
    with pytest.raises(ValueError):
        verify_versions(actual, {'pyyaml': '6.0.1'}, '0.1.0a1.dev0')


def test_version_verification_allows_only_bootstrap_exceptions_and_absent_unused_pins():
    actual = {'neo4j-graph-loader': '0.1.0a1.dev0', 'PyYAML': '6.0.1', 'pip': '25.0.1'}
    assert verify_versions(actual, {'pyyaml': '6.0.1', 'unused-lane-tool': '1'}, '0.1.0a1.dev0')['pyyaml'] == '6.0.1'


@pytest.mark.parametrize('fault', ['missing', 'changed', 'extra', 'symlink', 'source', 'secret'])
def test_portable_bundle_rejects_corruption_and_source_rescue(tmp_path, fault):
    root = tmp_path/'bundle'
    root.mkdir()
    wheel = root/'application-1-py3-none-any.whl'
    wheel.write_bytes(b'owned fixture')
    files = {wheel.name: hashlib.sha256(wheel.read_bytes()).hexdigest()}
    if fault == 'missing':
        wheel.unlink()
    elif fault == 'changed':
        wheel.write_bytes(b'changed')
    elif fault == 'extra':
        (root/'extra.py').write_text('unrecorded')
    elif fault == 'symlink':
        outside = tmp_path/'outside'
        wheel.rename(outside)
        wheel.symlink_to(outside)
    else:
        relative = 'src/rescue.py' if fault == 'source' else '.env.private'
        path = root/relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'synthetic forbidden fixture')
        files[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    (root/'bundle.json').write_text(json.dumps({'schema_version': 1, 'wheel': wheel.name, 'files': files}))
    with pytest.raises(ValueError):
        support_matrix.verify_bundle(root)


def sibling_tool_modules(source):
    """Modules of scripts/ that a tool imports or loads by file name (relative or bare import, or ``with_name('x.py')``)."""
    available = {path.stem for path in Path(support_matrix.__file__).parent.glob('*.py')}
    found = set()
    for node in ast.walk(ast.parse(source.read_text())):
        if isinstance(node, ast.ImportFrom):
            found.update([node.module] if node.module else [alias.name for alias in node.names])
        elif isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value.endswith('.py'):
            found.add(node.value[:-3])
    return {name.split('.')[0] for name in found} & available


def test_bundled_tools_ship_every_sibling_module_they_load():
    """A tool copied into a portable bundle must find every sibling it imports there (verify_installed loads service_lane)."""
    scripts = Path(support_matrix.__file__).parent
    bundled = {name[:-3] for name in support_matrix.TOOLS}
    needed, queue = set(), [name[:-3] for name in support_matrix.TOOLS]
    while queue:
        name = queue.pop()
        for sibling in sibling_tool_modules(scripts/f'{name}.py') - {name}:
            if sibling not in needed:
                needed.add(sibling)
                queue.append(sibling)
    assert {'service_lane', 'installed_guard_load'} <= needed, 'the derivation must see what verify_installed loads'
    # Loaded only by the actual-service lane branch (image preflight and Docker host check); a portable bundle observes
    # unit lanes only, so these are deliberately not bundled.
    service_lane_only = {'installed_service', 'build_candidate_images'}
    assert needed - service_lane_only <= bundled, f'bundle omits sibling modules: {sorted(needed - service_lane_only - bundled)}'
    assert not service_lane_only & bundled


def test_matrix_output_guard_preserves_existing_evidence(tmp_path):
    evidence = tmp_path/'prior'
    evidence.mkdir()
    sentinel = evidence/'result.json'
    sentinel.write_bytes(b'prior retained evidence')
    with pytest.raises(ValueError):
        support_matrix.new_output(evidence)
    assert sentinel.read_bytes() == b'prior retained evidence'


@pytest.mark.parametrize('kind', ['fifo', 'oversize', 'directory'])
def test_constraint_input_is_bounded_and_regular_before_reading(tmp_path, kind):
    path = tmp_path/'input'
    if kind == 'fifo':
        os.mkfifo(path)
    elif kind == 'oversize':
        path.write_bytes(b'x'*65537)
    else:
        path.mkdir()
    with pytest.raises(ValueError, match='bounded regular'):
        read_constraints(path)


def test_supervised_parser_error_retains_exact_exit_and_separate_stream_receipts(tmp_path):
    receipts = []
    stdout = tmp_path/'stdout'
    stderr = tmp_path/'stderr'
    run([sys.executable, '-c', 'import sys;print("out");print("err",file=sys.stderr);sys.exit(2)'],
        cwd=tmp_path, env=runtime_environment(Path(sys.executable)), log=stdout, stderr_log=stderr,
        timeout=10, receipts=receipts, allowed_exit_codes=(0, 2))
    assert stdout.read_bytes() == b'out\n' and stderr.read_bytes() == b'err\n'
    assert receipts[0]['exit_code'] == 2 and receipts[0]['allowed_exit_codes'] == [0, 2]
    assert receipts[0]['stderr_sha256'] == hashlib.sha256(b'err\n').hexdigest()
    assert not receipts[0]['owned_cleanup']['cleanup_errors']


@pytest.mark.parametrize('fault', ['create-interrupt', 'initial-inspect', 'final-inspect', 'foreign'])
def test_container_cleanup_recovers_owned_creation_and_preserves_failures(tmp_path, monkeypatch, fault):
    cid = 'a'*64
    calls = []
    removed = False
    failed_inspect = False
    def fake_run(command, *, log, stderr_log, receipts, **kwargs):
        nonlocal removed, failed_inspect
        calls.append(command)
        code, stdout, stderr = 0, '', ''
        log.write_text('')
        stderr_log.write_text('')
        if command[1] == 'create':
            Path(command[command.index('--cidfile')+1]).write_text(cid)
            if fault == 'create-interrupt':
                receipts.append({'interruption':'KeyboardInterrupt'})
                raise KeyboardInterrupt('controlled original interruption')
        elif command[1] == 'inspect':
            if not failed_inspect and ((fault == 'initial-inspect' and log.name == 'initial-inspect.log')
                    or (fault == 'final-inspect' and log.name == 'final-inspect.log')):
                failed_inspect = True
                raise RuntimeError('controlled original inspect failure')
            if removed:
                code, stderr = 1, 'Error: No such object: '+cid
            else:
                stdout = json.dumps([{'Id':cid,'Image':'sha256:owned','Created':'original-creation',
                    'Config':{'Labels':{'owner':'foreign' if fault=='foreign' else 'unique'}},
                    'State':{'Running':False,'ExitCode':0}}])
        elif command[1] == 'rm':
            removed = True
        log.write_text(stdout)
        stderr_log.write_text(stderr)
        receipts.append({'command':command,'exit_code':code})
    monkeypatch.setattr(matrix_docker.supervisor, 'run', fake_run)
    output = tmp_path/'controller'
    expected = KeyboardInterrupt if fault == 'create-interrupt' else RuntimeError
    with pytest.raises(expected):
        matrix_docker.run_container('/owned/docker',['sha256:owned'],output=output,
            label_key='owner',label_value='unique',image='sha256:owned',timeout=10)
    result = json.loads((output/'controller.json').read_text())
    assert result['status'] == 'failed' and result['exception'] == expected.__name__
    if fault == 'foreign':
        assert not removed and not any(c[1]=='rm' for c in calls)
        assert result['cleanup_errors']
    else:
        assert removed and result['cleanup']['absent']
        assert 'controlled original' in result['error']
