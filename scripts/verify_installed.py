#!/usr/bin/env python3
"""Run an explicit frozen test lane in a fresh, checkout-free wheel environment."""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from email.parser import BytesParser
import hashlib
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import signal
import subprocess
import sys
import tempfile
import time
import zipfile

import psutil

if __package__:
    from .installed_dependencies import read_constraints, read_wheelhouse, verify_versions
else:
    # Also support existing supervisors loading this file through a module spec.
    _spec = importlib.util.spec_from_file_location('installed_dependencies', Path(__file__).with_name('installed_dependencies.py'))
    _dependencies = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_dependencies)
    read_constraints, read_wheelhouse, verify_versions = (
        _dependencies.read_constraints, _dependencies.read_wheelhouse, _dependencies.verify_versions)

ROOT = Path(__file__).resolve().parents[1]
TOOLS = Path(__file__).resolve().parent


def sha(content):
    return hashlib.sha256(content).hexdigest()


def safe_relative(value, *, payload=False, references=()):
    path = PurePosixPath(value)
    if path.is_absolute() or str(path) != value or any(p in {'..', '.', '.git', '__pycache__', 'src', 'datasets', 'reports', 'logs'} or p.startswith('.env') for p in path.parts):
        raise ValueError('unsafe staged path: ' + value)
    if payload:
        if not value.startswith('neo4j_graph_loader/') or path.suffix not in {'.py', '.yaml'}:
            raise ValueError('unexpected application payload: ' + value)
    elif value in references:
        if path.suffix not in {'.py', '.yaml', '.yml', '.json', '.csv', '.md'}:
            raise ValueError('unexpected reference fixture: ' + value)
        if path.parts[0] not in {'tests', 'config', 'examples'} and value != 'plan/library-implementation/tools/run_e04_fault.py':
            raise ValueError('unapproved reference location: ' + value)
    elif path.parts[0] not in {'tests', 'config', 'examples'} or path.suffix not in {'.py', '.yaml', '.yml', '.json', '.csv'}:
        raise ValueError('not an approved fixture/test path: ' + value)
    elif path.suffix == '.py' and path.parts[0] != 'tests':
        raise ValueError('only maintained test helpers may stage Python: ' + value)
    return path


def validate_inventory(data, lane):
    if data.get('schema_version') != 1 or lane not in data.get('lanes', {}):
        raise ValueError('lane needs an executable version1 inventory')
    selected = data['lanes'][lane]
    if selected.get('kind') != 'installed unit regression':
        raise ValueError('actual-service lanes require the separate E09 adapter')
    files = selected.get('stage_files', {})
    ids = selected.get('test_ids', [])
    references = selected.get('reference_files', {})
    if not set(references) <= set(files) or any(not reason for reason in references.values()):
        raise ValueError('reference fixtures need staged hashes and explicit purposes')
    if not files or not ids or len(ids) != len(set(ids)):
        raise ValueError('empty or duplicate frozen test/fixture inventory')
    for path, digest in files.items():
        safe_relative(path, references=references)
        if not isinstance(digest, str) or len(digest) != 64:
            raise ValueError('missing fixture hash: ' + path)
    if any(nodeid.split('::')[0] not in files or not nodeid.startswith(('tests/', 'examples/imdb/tests/')) for nodeid in ids):
        raise ValueError('test ID is outside staged test files')
    collected = selected.get('collected_test_ids', ids)
    excluded = selected.get('excluded_test_ids', {})
    if (len(collected) != len(set(collected)) or set(collected) != set(ids) | set(excluded)
            or set(ids) & set(excluded) or any(not reason for reason in excluded.values())
            or any(i.split('::')[0] not in {n.split('::')[0] for n in ids} for i in collected)):
        raise ValueError('full collection differs from selected and explicitly excluded IDs')
    tests = selected.get('current_test_files', [])
    if not tests or set(tests) != {nodeid.split('::')[0] for nodeid in ids}:
        raise ValueError('selected files differ from frozen test IDs')
    payload = data.get('package_payload', {})
    if not payload or 'neo4j_graph_loader/__init__.py' not in payload or not {
        'neo4j_graph_loader/data/graph_schema.yaml', 'neo4j_graph_loader/data/social_schema.yaml'
    } <= set(payload):
        raise ValueError('incomplete application payload inventory')
    for path, digest in payload.items():
        safe_relative(path, payload=True)
        if not isinstance(digest, str) or len(digest) != 64:
            raise ValueError('invalid application hash: ' + path)
    return selected


def runtime_environment(python):
    # Do not inherit pytest config, developer paths or ambient service settings.
    keep = {'HOME', 'TMPDIR', 'TEMP', 'TMP', 'SYSTEMROOT', 'LANG', 'LC_ALL', 'SSL_CERT_FILE', 'SSL_CERT_DIR'}
    env = {key: value for key, value in os.environ.items() if key in keep}
    env.update(PATH=str(python.parent) + os.pathsep + os.defpath,
               PYTEST_DISABLE_PLUGIN_AUTOLOAD='1', GRAPH_LOADER_METRICS_ENABLED='0')
    return env


def check_git_ancestry(path):
    resolved = path.resolve()
    if any((parent/'.git').exists() or (parent/'.git').is_symlink()
           for parent in (resolved, *resolved.parents)):
        raise ValueError('staging must be outside every Git checkout/worktree')


def cleanup_owned(provenance):
    """Signal only receipt-bound PIDs whose OS creation identity still matches."""
    if provenance is None or not provenance.exists():
        return {'leftover_owned': [], 'cleanup_errors': []}
    rows = []
    errors = []
    for line in provenance.read_bytes().splitlines():
        try:
            row = json.loads(line)
            if not isinstance(row, dict) or not isinstance(row.get('pid'), int) or not isinstance(row.get('created'), (int, float)):
                raise ValueError('invalid process identity')
            rows.append(row)
        except (ValueError, TypeError):
            errors.append('invalid provenance record during owned cleanup')
    identities = {(r['pid'], r['created']) for r in rows}
    for row in rows:
        if row.get('kind') == 'python_child' and row.get('child_created') is not None:
            if isinstance(row.get('child_pid'), int) and isinstance(row['child_created'], (int, float)):
                identities.add((row['child_pid'], row['child_created']))
            else:
                errors.append('invalid child identity during owned cleanup')
    def live():
        result=[]
        for pid, created in identities:
            if pid == os.getpid():
                continue
            try:
                process=psutil.Process(pid)
                if process.create_time()==created and process.status()!=psutil.STATUS_ZOMBIE:
                    result.append(process)
            except psutil.NoSuchProcess:
                pass
        return result
    deadline=time.monotonic()+.5
    while live() and time.monotonic()<deadline:
        time.sleep(.025)
    remaining=live();leftover=[{'pid':p.pid,'created':p.create_time()} for p in remaining]
    for process in remaining:
        try:
            process.terminate()
        except psutil.NoSuchProcess:
            pass
        except psutil.Error as exc:
            errors.append(type(exc).__name__)
    _, alive=psutil.wait_procs(remaining,timeout=2)
    for process in alive:
        try:
            process.kill()
        except psutil.NoSuchProcess:
            pass
        except psutil.Error as exc:
            errors.append(type(exc).__name__)
    _, alive=psutil.wait_procs(alive,timeout=2)
    if alive:
        errors.append('owned process survived cleanup')
    return {'leftover_owned':leftover,'cleanup_errors':errors}


def run(command, *, cwd, env, log, timeout, receipts, provenance=None, stderr_log=None, allowed_exit_codes=(0,)):
    if not allowed_exit_codes or any(type(code) is not int or not 0 <= code <= 255 for code in allowed_exit_codes):
        raise ValueError('explicit finite nonnegative exit codes are required')
    started = time.monotonic()
    interruption = None
    timed_out = False
    cleanup_errors = []
    with ExitStack() as streams:
        output = streams.enter_context(log.open('wb'))
        errors = streams.enter_context(stderr_log.open('wb')) if stderr_log else subprocess.STDOUT
        process = subprocess.Popen(command, cwd=cwd, env=env, stdout=output,
                                   stderr=errors, start_new_session=True)
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
        except BaseException as exc:
            interruption = exc
        finally:
            def attempt(operation):
                nonlocal interruption
                try:
                    operation()
                except ProcessLookupError:
                    pass
                except BaseException as exc:
                    cleanup_errors.append(type(exc).__name__)
                    if not isinstance(exc, Exception) and interruption is None:
                        interruption = exc

            if timed_out or interruption is not None:
                attempt(lambda: os.killpg(process.pid, signal.SIGTERM))
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass  # The unconditional group kill below supplies escalation.
                except BaseException as exc:
                    cleanup_errors.append(type(exc).__name__)
                    if interruption is None:
                        interruption = exc
            # Reclaim grandchildren even when the direct command already exited.
            attempt(lambda: os.killpg(process.pid, signal.SIGKILL))
            attempt(lambda: process.wait(timeout=5))
            try:
                cleanup = cleanup_owned(provenance)
            except BaseException as exc:
                cleanup = {'leftover_owned': [], 'cleanup_errors': [type(exc).__name__]}
                if not isinstance(exc, Exception) and interruption is None:
                    interruption = exc
            cleanup['cleanup_errors'].extend(cleanup_errors)
            row = {'command': command, 'cwd': str(cwd), 'exit_code': process.returncode,
                   'timeout': timed_out, 'elapsed_seconds': time.monotonic() - started,
                   'log': str(log), 'log_sha256': sha(log.read_bytes()), 'owned_cleanup': cleanup}
            if interruption is not None:
                row['interruption'] = type(interruption).__name__
            if stderr_log is not None:
                row.update(stderr_log=str(stderr_log), stderr_sha256=sha(stderr_log.read_bytes()))
            if allowed_exit_codes != (0,):
                row['allowed_exit_codes'] = list(allowed_exit_codes)
            receipts.append(row)
    if interruption is not None:
        raise interruption
    if timed_out or process.returncode not in allowed_exit_codes or cleanup['leftover_owned'] or cleanup['cleanup_errors']:
        raise RuntimeError('command failed; retained output: ' + str(log))


def validate_wheel(wheel, data):
    """Require the same frozen application/distribution contract in every lane."""
    if wheel.suffix != '.whl':
        raise ValueError('an explicit wheel file is required')
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)):
            raise ValueError('duplicate wheel member')
        actual = {n: sha(archive.read(n)) for n in names if n.startswith('neo4j_graph_loader/')}
        version = data['package_version']
        dist = f'neo4j_graph_loader-{version}.dist-info'
        allowed = set(data['package_payload']) | {f'{dist}/{n}' for n in (
            'METADATA', 'WHEEL', 'top_level.txt', 'RECORD', 'licenses/LICENSE', 'entry_points.txt')}
        if set(names) != allowed:
            raise ValueError('wheel contains missing or unexpected archive members')
        metadata = BytesParser().parsebytes(archive.read(f'{dist}/METADATA'))
        if metadata['Name'] != 'neo4j-graph-loader' or metadata['Version'] != version or metadata.get_all('Provides-Extra'):
            raise ValueError('unexpected wheel identity or extras')
        distribution={n:sha(archive.read(n)) for n in names if n.startswith(dist+'/')}
        if distribution != data.get('distribution_files'):
            raise ValueError('wheel metadata differs from frozen distribution contract')
    if actual != data['package_payload']:
        raise ValueError('wheel payload differs from frozen application inventory')
    return actual, version, distribution


def validate_provenance(path, *, config=None, site=None):
    """Require receipt-bound startup/import/child and post-exec identities."""
    provenance = [json.loads(line) for line in path.read_text().splitlines()]
    processes = {row['pid'] for row in provenance if row['kind'] == 'process'}
    expected = {row['child_pid'] for row in provenance if row['kind'] == 'python_child'}
    if expected - processes or any(row['kind'] == 'violation' for row in provenance):
        raise RuntimeError('missing or violated Python child provenance')
    last_start = {}
    for row in provenance:
        if row['kind'] == 'process':
            last_start[(row['pid'], row['created'])] = row['tick']
    if any(row['kind'] == 'exec' and row['tick'] >= last_start.get((row['pid'], row['created']), -1) for row in provenance):
        raise RuntimeError('missing fresh post-exec Python provenance')
    if any(row['kind'] == 'python_child' and row['child_created'] is not None and
           (row['child_pid'], row['child_created']) not in last_start for row in provenance):
        raise RuntimeError('child process creation identity differs from provenance')
    if not any(row['kind'] == 'module' for row in provenance):
        raise RuntimeError('no application import provenance observed')
    if config is not None:
        for row in provenance:
            if row['kind'] == 'process' and (Path(row['prefix']).resolve() != Path(config['prefix']).resolve()
                                           or Path(row['site']).resolve() != site.resolve()):
                raise RuntimeError('case startup differs from target environment/site')
            if row['kind'] == 'module':
                origin = Path(row['origin'])
                if (not origin.is_relative_to(site) or
                        config['payload'].get(origin.relative_to(site).as_posix()) != row['sha256'] or
                        (row['pid'], row['created']) not in last_start):
                    raise RuntimeError('case application origin/hash/identity differs from installed payload')
            if row['kind'] == 'python_child' and not any(
                    start['kind'] == 'process' and start['pid'] == row['child_pid']
                    and start['ppid'] == row['pid'] and start['tick'] >= row['launch_tick']
                    and (row['child_created'] is None or start['created'] == row['child_created'])
                    for start in provenance):
                raise RuntimeError('case child startup is not bound to its parent/launch/creation identity')
    return processes, expected


def process_cases():
    """Explicit measured points; no claim about unmeasured interior counts."""
    normal = [1, 2, 3, 4, 5, 6, 7, 8, 16, 32, 64]
    failure = [1, 2, 4, 8, 64]
    cases = []
    for context in ('spawn', 'inline'):
        cases.extend(dict(scenario='external-success', context=context, workers=n, mode='timeout') for n in normal)
    for n in failure:
        for scenario in ('external-nonpicklable', 'external-source-error', 'external-worker-exit'):
            cases.append(dict(scenario=scenario, context='spawn', workers=n, mode='timeout'))
        cases.append(dict(scenario='external-source-error', context='inline', workers=n, mode='timeout'))
        for scenario in ('private-recv', 'private-send', 'public-blocked', 'public-partial', 'public-send', 'public-many'):
            for mode in ('timeout', 'cancel'):
                cases.append(dict(scenario=scenario, context='spawn', workers=n, mode=mode))
        for scenario in ('public-cold', 'public-stop', 'public-source-error', 'public-gap'):
            cases.append(dict(scenario=scenario, context='spawn', workers=n, mode='timeout'))
        if n >= 2:
            for scenario in ('public-startup', 'public-mixed-error-default', 'public-mixed-error-timeout'):
                cases.append(dict(scenario=scenario, context='spawn', workers=n, mode='timeout'))
    return cases


def validate_fault_children(observed, case, rows, processes, expected):
    """Bind actual workers; retain truthful import evidence during early cleanup."""
    children = observed['child_pids']
    actual = 2 if case['scenario'] == 'public-startup' else case['workers']
    if len(children) != actual or len(set(children)) != actual:
        raise RuntimeError('fault child identities differ from actual launched workers')
    partial_start = case['scenario'] == 'public-startup'
    cold_timeout = case['scenario'] == 'public-cold'
    if partial_start and (observed['clock_anchor'] is not None or observed['error'] != 'RuntimeError'
                          or 'second process started then failed' not in observed['failure_marker']):
        raise RuntimeError('partial-startup observation does not establish the intended second-start failure')
    if cold_timeout and (observed['clock_anchor'] is None or observed['prepared_transport']
                         or observed['error'] != 'EdgePlanTimeoutError' or observed['configured_timeout'] != .8):
        raise RuntimeError('cold timeout observation does not establish its unmodified operation clock')
    imports = {}
    for pid in children:
        if pid == observed['client_pid'] or pid not in processes or pid not in expected or not any(
                item['kind'] == 'python_child' and item['pid'] == observed['client_pid'] and item['child_pid'] == pid
                for item in rows):
            raise RuntimeError('fault child identities lack bound target launch/startup provenance')
        modules = [item['module'] for item in rows if item['kind'] == 'module' and item['pid'] == pid]
        # The second start intentionally raises immediately after its audited
        # Python startup. Cleanup can kill either child before an application
        # import. Prefix/site and complete payload were verified at startup;
        # never manufacture an import record for code it did not execute.
        # The separate cold-deadline case can likewise terminate a booting
        # child; its ordinary clock/error contract is checked above.
        if 'neo4j_graph_loader.planning' not in modules and not (partial_start or cold_timeout):
            raise RuntimeError('fault child lacks actual installed planning-module import provenance')
        imports[str(pid)] = modules
    return {'actual_child_imports': imports, 'partial_second_start': partial_start, 'cold_timeout': cold_timeout,
            'children_without_application_import_receipt': [int(pid) for pid, modules in imports.items()
                                                            if 'neo4j_graph_loader.planning' not in modules]}


def run_process_envelope(python, stage, work, env, site, config, stage_files):
    """Execute frozen clients with isolated per-case guard logs, restoring config."""
    directory = work/'process-envelope'
    directory.mkdir(mode=0o700)
    config_path = site/'_e07_guard_config.json'
    original = config_path.read_bytes()
    results = []
    failure = None
    try:
        for case in process_cases():
            label = f"{case['scenario']}-{case['context']}-{case['workers']}-{case['mode']}"
            output = directory/label
            output.mkdir(mode=0o700)
            provenance = output/'provenance.jsonl'
            case_config = {**config, 'provenance_log': str(provenance)}
            commands = []
            row = {**case, 'label': label}
            try:
                if any(sha((stage/name).read_bytes()) != digest or (stage/name).is_symlink()
                       for name, digest in stage_files.items()):
                    raise RuntimeError('process client stage differs from frozen fixture closure')
                config_path.write_text(json.dumps(case_config))
                run([str(python), str(stage/'plan/library-implementation/tools/run_e04_fault.py'),
                     case['scenario'], '--context', case['context'], '--workers', str(case['workers']),
                     '--mode', case['mode'], '--hard-timeout', '180', '--python', str(python),
                     '--output', str(output/'client')], cwd=stage, env=env,
                    log=output/'command.log', timeout=200, receipts=commands, provenance=provenance)
                controller = json.loads((output/'client/supervisor.json').read_text())
                observed = json.loads((output/'client/client.json').read_text())
                if controller['exit_code'] or controller['timeout'] or controller['emergency_group_cleanup']:
                    raise RuntimeError('case required emergency supervision or failed')
                processes, expected = validate_provenance(provenance, config=case_config, site=site)
                provenance_rows = [json.loads(line) for line in provenance.read_text().splitlines()]
                client_pid = observed['client_pid']
                if client_pid not in processes:
                    raise RuntimeError('case client lacks an actual target startup identity')
                if case['scenario'].startswith('external-'):
                    if observed['status'] != 'observed' or observed['workers'] != case['workers'] or observed['context'] != case['context']:
                        raise RuntimeError('external client observation differs from explicit cell')
                    successful = case['scenario'] == 'external-success'
                    if observed['caller_method'] != observed['caller_method_after'] or observed['operation_cleanup_upper_seconds'] >= 10:
                        raise RuntimeError('external caller method or cleanup contract failed')
                    if successful and (len(observed['source_identities']) != 64 or observed['rows'] != 192 or observed['planner_workers'] != case['workers']):
                        raise RuntimeError('external success lacks complete source/data/worker observation')
                    identities = [observed['client_pid'], *[source['pid'] for source in observed['source_identities']]]
                    created_workers = sorted({item['child_pid'] for item in provenance_rows
                        if item['kind'] == 'python_child' and item['pid'] == client_pid
                        and '--multiprocessing-fork' in item.get('command', [])})
                    if (case['context'] == 'inline' and created_workers) or len(created_workers) > case['workers']:
                        raise RuntimeError('external client created workers outside its explicit context/capacity')
                    if case['context'] == 'spawn' and case['scenario'] != 'external-nonpicklable' and len(created_workers) != case['workers']:
                        raise RuntimeError('external process worker count differs from requested capacity')
                    row.update(requested_parallelism=case['workers'], actual_created_process_workers=len(created_workers),
                               actual_process_worker_pids=created_workers,
                               initial_submission_failure=case['scenario'] == 'external-nonpicklable')
                    if any(pid not in processes or not any(
                            item['kind'] == 'module' and item['pid'] == pid and item['module'] == 'neo4j_graph_loader.planning'
                            for item in provenance_rows) for pid in identities):
                        raise RuntimeError('external source identity lacks actual installed planning-module provenance')
                else:
                    actual = 2 if case['scenario'] == 'public-startup' else case['workers']
                    if observed['actual_workers'] != actual or observed['context'] != 'spawn':
                        raise RuntimeError('fault observation differs from actual launched workers')
                    row['worker_provenance'] = validate_fault_children(observed, case, provenance_rows, processes, expected)
                    if case['scenario'].startswith('public-'):
                        if not observed['production_children_dead'] or not observed['production_endpoints_closed'] or observed['active_child_pids']:
                            raise RuntimeError('fault case left production resources alive')
                        if observed['prepared_transport'] and (observed['blocked_peer_count'] != observed['expected_blocked_peers']
                                or observed['partial_reply_peer_count'] != observed['expected_partial_reply_peers']):
                            raise RuntimeError('prepared fault peer activation incomplete')
                    elif not observed['children_dead'] or not observed['endpoints_closed'] or not observed['fixture_timer_dead']:
                        raise RuntimeError('private case cleanup incomplete')
                row.update(status='passed', observation=observed, processes=sorted(processes), expected_children=sorted(expected))
            except BaseException as error:
                row.update(status='failed', error=str(error), interruption=type(error).__name__)
                raise
            finally:
                row['commands'] = commands
                results.append(row)
                (output/'result.json').write_text(json.dumps(row, indent=2)+'\n')
                (directory/'result.json').write_text(json.dumps({'cases': results, 'complete': False}, indent=2)+'\n')
    except BaseException as error:
        failure = error
        raise
    finally:
        try:
            config_path.write_bytes(original)
            if config_path.read_bytes() != original:
                raise RuntimeError('target provenance configuration was not restored')
            (directory/'guard-configuration.json').write_text(json.dumps({'restored': True, 'sha256': sha(original)})+'\n')
        except BaseException as error:
            if failure is None or not isinstance(error, Exception):
                raise
            failure.add_note('target guard restoration failed: '+str(error))
    result = {'cases': results, 'complete': True, 'guard_configuration_restored': True}
    (directory/'result.json').write_text(json.dumps(result, indent=2)+'\n')
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--lane', required=True)
    parser.add_argument('--wheel', type=Path, required=True)
    parser.add_argument('--inventory', type=Path, required=True)
    parser.add_argument('--output', type=Path, help='new output directory, outside the checkout')
    parser.add_argument('--collect-only', action='store_true')
    parser.add_argument('--timeout', type=float, default=900)
    parser.add_argument('--constraints', type=Path, help='exact finite name==version pins for runtime and test tools')
    parser.add_argument('--wheelhouse', type=Path, help='offline directory containing only manifested dependency wheels')
    parser.add_argument('--wheelhouse-manifest', type=Path, help='version1 files/hash JSON; required with --wheelhouse')
    parser.add_argument('--process-envelope', action='store_true', help='explicit full E08 process matrix after public-contracts regression')
    args = parser.parse_args(argv)
    if not 0 < args.timeout <= 3600:
        parser.error('--timeout must be positive and at most3600 seconds')
    if args.process_envelope and (args.lane != 'public-contracts' or args.collect_only):
        parser.error('--process-envelope requires an executed public-contracts lane')
    receipts = []
    work = None
    owned = False
    try:
        if bool(args.wheelhouse) != bool(args.wheelhouse_manifest) or (args.wheelhouse and not args.constraints):
            raise ValueError('offline wheelhouse requires constraints and its explicit hash manifest')
        constraints = read_constraints(args.constraints) if args.constraints else None
        wheelhouse = read_wheelhouse(args.wheelhouse, args.wheelhouse_manifest, constraints['pins']) if args.wheelhouse else None
        data = json.loads(args.inventory.read_text())
        lane = validate_inventory(data, args.lane)
        wheel = args.wheel.resolve(strict=True)
        actual, version, distribution = validate_wheel(wheel, data)
        if args.output:
            requested = args.output.absolute()
            work = requested.parent.resolve() / requested.name
            if work.is_relative_to(ROOT) or requested.exists() or requested.is_symlink():
                raise ValueError('output must be a new directory outside checkout')
            check_git_ancestry(work)
            work.mkdir(mode=0o700, parents=True)
            owned = True
            work = work.resolve()
        else:
            temporary_parent=Path(tempfile.gettempdir()).resolve()
            check_git_ancestry(temporary_parent)
            work = Path(tempfile.mkdtemp(prefix='graph-loader-installed-',dir=temporary_parent))
            owned = True
        if work.is_relative_to(ROOT):
            raise ValueError('temporary staging must be outside checkout')
        check_git_ancestry(work)
        stage = work / 'stage'
        stage.mkdir(mode=0o700)
        for relative, digest in lane['stage_files'].items():
            source = ROOT / relative
            if any(p.is_symlink() for p in [source, *source.parents] if p != ROOT and p.is_relative_to(ROOT)) or not source.resolve().is_relative_to(ROOT):
                raise ValueError('symlink/escaped fixture: ' + relative)
            content = source.read_bytes()
            if sha(content) != digest:
                raise ValueError('fixture differs from frozen inventory: ' + relative)
            target = stage / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
        (stage / 'pytest.ini').write_text('[pytest]\naddopts =\npythonpath =\nmarkers =\n    integration: live services\n    smoke: live services\n    stress: heavyweight services\n    real_stream_groups: retain real group preflight\n')
        (stage / 'e07-stage-files.json').write_text(json.dumps(lane['stage_files'], sort_keys=True) + '\n')
        environment = work / 'environment'
        python = environment / 'bin/python'
        env = runtime_environment(python)
        run([sys.executable, '-I', '-m', 'venv', str(environment)], cwd=stage, env=env,
            log=work/'venv.log', timeout=120, receipts=receipts)
        # Test requirements belong to this runner, never to package metadata.
        requirements = data.get('test_requirements', []) + lane.get('extra_test_requirements', [])
        if not requirements or any(not isinstance(r, str) or r.startswith('-') for r in requirements):
            raise ValueError('explicit runner test requirements are required')
        install_options = []
        dependency_inputs = None
        if constraints:
            frozen_constraints = work/'constraints.txt'
            frozen_constraints.write_bytes(constraints['content'])
            install_options = ['--constraint', str(frozen_constraints), '--report', str(work/'install-report.json')]
            dependency_inputs = {k: v for k, v in constraints.items() if k != 'content'}
            if wheelhouse:
                frozen_wheels = work/'wheelhouse'
                frozen_wheels.mkdir(mode=0o700)
                for name, content in wheelhouse['contents'].items():
                    (frozen_wheels/name).write_bytes(content)
                install_options += ['--no-index', '--find-links', str(frozen_wheels), '--only-binary=:all:']
                dependency_inputs['wheelhouse'] = {k: v for k, v in wheelhouse.items() if k != 'contents'}
        run([str(python), '-m', 'pip', *(['--isolated'] if constraints else []), 'install',
             *install_options, str(wheel), *requirements],
            cwd=stage, env=env, log=work/'install.log', timeout=600, receipts=receipts)
        if constraints:
            inventory_code = '''import importlib.metadata,json,sys
from pathlib import Path
Path(sys.argv[1]).write_text(json.dumps({d.metadata['Name']:d.version for d in importlib.metadata.distributions()},sort_keys=True))
'''
            run([str(python), '-I', '-c', inventory_code, str(work/'installed-versions.json')],
                cwd=stage, env=env, log=work/'versions.log', timeout=30, receipts=receipts)
            dependency_inputs['installed_versions'] = verify_versions(
                json.loads((work/'installed-versions.json').read_text()), constraints['pins'], version)
        site_result = subprocess.check_output([str(python), '-I', '-c',
            'import sysconfig;print(sysconfig.get_paths()["purelib"])'], cwd=stage, env=env, text=True, timeout=30)
        site = Path(site_result.strip())
        config = {'prefix': str(environment), 'payload': data['package_payload'],
                  'provenance_log': str(work/'provenance.jsonl')}
        (site/'_e07_guard_config.json').write_text(json.dumps(config))
        (site/'sitecustomize.py').write_bytes((TOOLS/'installed_guard.py').read_bytes())
        (site/'e07_pytest.py').write_bytes((TOOLS/'installed_pytest.py').read_bytes())
        launch = """import json,sys
from pathlib import Path
import pytest
from e07_pytest import Receipt
stage=Path(sys.argv[1]);sys.path[:0]=[str(stage),str(stage/'tests')]
data=json.loads(Path(sys.argv[2]).read_text())
lane=data['lane'];collect=data['collect_only']
plugin=Receipt(lane['test_ids'],lane.get('allowed_skips',{}),data['receipt'],collect,
    full_expected=lane.get('collected_test_ids'),excluded=lane.get('excluded_test_ids',{}))
args=['-c',str(stage/'pytest.ini'),'--rootdir',str(stage),'--confcutdir',str(stage),'--import-mode=importlib','-q','-rs',*lane['current_test_files']]
if collect:args.append('--collect-only')
raise SystemExit(pytest.main(args,plugins=[plugin]))
"""
        run_config = work/'run.json'
        run_config.write_text(json.dumps({'lane': lane, 'collect_only': args.collect_only,
                                          'receipt': str(work/'pytest.json')}))
        run([str(python), '-I', '-c', launch, str(stage), str(run_config)],
            cwd=stage, env=env, log=work/'pytest.log', timeout=args.timeout, receipts=receipts,
            provenance=work/'provenance.jsonl')
        pytest_receipt=json.loads((work/'pytest.json').read_text())
        if pytest_receipt['errors'] or pytest_receipt['collected']!=lane['test_ids'] or pytest_receipt['pytest_exit_code']:
            raise RuntimeError('missing or failed final pytest receipt')
        processes, expected = validate_provenance(work/'provenance.jsonl')
        envelope = run_process_envelope(python, stage, work, env, site, config, lane['stage_files']) if args.process_envelope else None
        run([str(python), '-I', '-m', 'pip', 'check'], cwd=stage, env=env,
            log=work/'pip-check.log', timeout=60, receipts=receipts)
        result = {'status': 'passed', 'lane': args.lane, 'wheel': str(wheel),
                  'wheel_sha256': sha(wheel.read_bytes()), 'inventory_sha256': sha(args.inventory.read_bytes()),
                  'stage_files': lane['stage_files'], 'test_ids': lane['test_ids'],
                  'python': str(python), 'work': str(work), 'commands': receipts,
                  'provenance_processes': sorted(processes), 'expected_python_children': sorted(expected),
                  'runner_tools': {p.name: sha(p.read_bytes()) for p in (Path(__file__), TOOLS/'installed_guard.py', TOOLS/'installed_pytest.py', TOOLS/'installed_dependencies.py')},
                  'payload_members': len(actual), 'collect_only': args.collect_only}
        if dependency_inputs is not None:
            result['dependency_inputs'] = dependency_inputs
        if envelope is not None:
            path = work/'process-envelope/result.json'
            result['process_envelope'] = {'path': str(path), 'sha256': sha(path.read_bytes()),
                                          'cases': len(envelope['cases']), 'complete': envelope['complete']}
        result['generated_stage_files'] = {name: sha((stage/name).read_bytes())
                                           for name in ('pytest.ini', 'e07-stage-files.json')}
        result['distribution_files']=distribution
        (work/'result.json').write_text(json.dumps(result, indent=2)+'\n')
        print(json.dumps({'status': 'passed', 'result': str(work/'result.json'), 'test_ids': len(lane['test_ids'])}))
        return 0
    except BaseException as exc:
        result = {'status': 'failed', 'error': str(exc), 'commands': receipts}
        if not isinstance(exc, Exception):
            result['interruption'] = type(exc).__name__
        if owned and work and work.is_dir():
            (work/'result.json').write_text(json.dumps(result, indent=2)+'\n')
        if not isinstance(exc, Exception):
            raise
        print('installed runner failed: ' + str(exc), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
