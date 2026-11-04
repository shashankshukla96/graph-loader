#!/usr/bin/env python3
"""Exact-tree validation (private): runs the product's own checks inside a copy of a materialized public tree, with development artifacts built from it.

    export_validate.py TREE WORKDIR [--run-lane core] [--keep-venv]

The tree (the quarantine clone's working tree or any materialized public tree) is copied into ``WORKDIR/tree`` so it stays pristine; ``python -m build`` builds
a wheel and an sdist from that copy; a fresh venv gets the tree's development requirements and the wheel (non-editable). Then, all inside the copy and against
its own files: test collection (and the ids against the tree's frozen inventory), the offline unit suite, the strict artifact verifier (archive checks and the
installed public probe), the documentation link check, and the installed-runner checks: the tree's inventory is re-frozen into a scratch file for these
artifacts and every installed unit lane runs ``verify_installed.py --collect-only`` (wheel payload against the inventory, staged files, collected ids); with
``--run-lane`` a lane is executed completely. The report keeps the numbers per step. Nothing here changes the tree, the quarantine or any repository.

A frozen inventory is only valid for the exact bytes it was frozen from, so the tree's own inventory is compared (hashes of staged files) and reported, and the
scratch re-freeze is what the runner checks use; a platform-dependent test id (the signal number in ``test_exit_codes_after_ready``) differs between the Linux
freeze and a macOS collection and is the only id difference accepted.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time

PLATFORM_ID = re.compile(r'(tests/test_process_runtime\.py::test_exit_codes_after_ready\[self-signal-args3-)(\d+)\]')
SUITE_FLAGS = ['--ignore=tests/installed_service', '--ignore=tests/integration', '--ignore=tests/stress', '-m', 'not integration and not smoke and not stress',
               '-q', '-p', 'no:cacheprovider', '-rs']
LINK_FILES = ['README.md', 'PACKAGE_README.md', 'SECURITY.md', 'CHANGELOG.md']


def default_signals():
    """A tool started in the background by a shell inherits an ignored SIGINT, which Python then never turns into KeyboardInterrupt (the installed-runner
    supervisor tests depend on it); give every step the default disposition."""
    signal.signal(signal.SIGINT, signal.SIG_DFL)


def run(command, cwd, log: Path, timeout=3600, env=None):
    started = time.time()
    with open(log, 'wb') as handle:
        done = subprocess.run(command, cwd=cwd, stdout=handle, stderr=subprocess.STDOUT, timeout=timeout, env=env, preexec_fn=default_signals)
    return done.returncode, round(time.time() - started, 1)


def tail(path: Path, lines=3) -> str:
    return '\n'.join(path.read_text(errors='replace').strip().splitlines()[-lines:])


def parse_pytest_summary(text: str) -> dict:
    last = [line for line in text.splitlines() if re.search(r'\d+ (passed|failed|error)', line)]
    counts = {name: int(number) for number, name in re.findall(r'(\d+) (passed|failed|skipped|deselected|errors?)', last[-1])} if last else {}
    return {'passed': counts.get('passed', 0), 'failed': counts.get('failed', 0) + counts.get('error', 0) + counts.get('errors', 0), 'skipped': counts.get('skipped', 0),
            'deselected': counts.get('deselected', 0)}


def compare_collection(frozen: list[str], collected: list[str]) -> dict:
    """Ids lost, added and reordered against a frozen list; the platform signal id is the only tolerated difference."""
    def norm(ids):
        return [PLATFORM_ID.sub(r'\1<platform>]', i) for i in ids]
    f, c = norm(frozen), norm(collected)
    in_f, in_c = set(f), set(c)
    return {'frozen': len(frozen), 'collected': len(collected), 'lost': [i for i in f if i not in in_c], 'added': len([i for i in c if i not in in_f]),
            'order_preserved': [i for i in c if i in in_f] == [i for i in f if i in in_c],
            'platform_id_differs': [i for i in frozen if PLATFORM_ID.search(i)] != [i for i in collected if PLATFORM_ID.search(i)]}


def stale_stage_files(tree: Path, inventory: dict) -> list[str]:
    stale = set()
    for lane in inventory['lanes'].values():
        for path, digest in lane.get('stage_files', {}).items():
            file = tree / path
            if not file.is_file() or hashlib.sha256(file.read_bytes()).hexdigest() != digest:
                stale.add(path)
    return sorted(stale)


def validate(tree: Path, work: Path, builder_python: str = sys.executable, run_lanes: list[str] | None = None, keep_venv: bool = False) -> dict:
    tree, work = Path(tree).resolve(), Path(work).resolve()
    if work.exists():
        raise SystemExit('the work directory must be new')
    work.mkdir(parents=True)
    logs = work / 'logs'
    logs.mkdir()
    copy = work / 'tree'
    shutil.copytree(tree, copy, ignore=shutil.ignore_patterns('__pycache__', '*.pyc', '.pytest_cache'))      # .git stays: tests read `git ls-files` of a repository
    report: dict = {'tree': str(tree), 'files': sum(1 for p in copy.rglob('*') if p.is_file()), 'steps': {}}

    def step(name, ok, **info):
        report['steps'][name] = {'ok': bool(ok), **info}
        print(f'{"PASS" if ok else "FAIL"}  {name}  {json.dumps(info, default=str)[:400]}', flush=True)
        return ok

    code, seconds = run([builder_python, '-m', 'build', '--no-isolation', '--outdir', str(work / 'dist'), str(copy)], copy, logs / 'build.log')
    wheels, sdists = list((work / 'dist').glob('*.whl')), list((work / 'dist').glob('*.tar.gz'))
    if not step('build development artifacts', code == 0 and len(wheels) == 1 and len(sdists) == 1, seconds=seconds, wheel=wheels[0].name if wheels else None,
                sdist=sdists[0].name if sdists else None, log=str(logs / 'build.log')):
        return report
    wheel, sdist = wheels[0], sdists[0]
    shutil.rmtree(copy / 'build', ignore_errors=True)
    for egg in (copy / 'src').glob('*.egg-info'):
        shutil.rmtree(egg, ignore_errors=True)

    venv = work / 'venv'
    code, seconds = run([builder_python, '-m', 'venv', str(venv)], work, logs / 'venv.log')
    py = str(venv / 'bin' / 'python')
    code2, seconds2 = run([py, '-m', 'pip', 'install', '-q', '-r', 'requirements-dev.txt', 'pytest==9.1.1', 'SQLAlchemy>=2.0.30,<3', 'build', 'setuptools'], copy, logs / 'pip-dev.log')
    code3, _ = run([py, '-m', 'pip', 'install', '-q', '--no-deps', str(wheel)], copy, logs / 'pip-wheel.log')
    where = subprocess.run([py, '-c', 'import neo4j_graph_loader, sys; print(neo4j_graph_loader.__file__)'], cwd=work, capture_output=True, text=True).stdout.strip()
    if not step('fresh environment with the wheel (non-editable)', code == code2 == code3 == 0 and 'site-packages' in where and str(copy) not in where,
                seconds=round(seconds + seconds2, 1), package=where):
        return report

    inventory_path = copy / 'tests' / 'inventory' / 'test-inventory.json'
    inventory = json.loads(inventory_path.read_text())
    code, seconds = run([py, '-m', 'pytest', '--collect-only', '-q', '-p', 'no:cacheprovider', '-o', 'addopts=', 'tests', 'examples/imdb/tests'], copy, logs / 'collect.log')
    collected = [line.strip() for line in (logs / 'collect.log').read_text(errors='replace').splitlines() if '::' in line]
    comparison = compare_collection(inventory['source_collection']['test_ids'], collected)
    step('collection', code == 0 and collected and not comparison['lost'] and comparison['order_preserved'], seconds=seconds, **comparison)
    stale = stale_stage_files(copy, inventory)
    report['steps']['frozen inventory hashes'] = {'ok': not stale, 'stale_staged_files': len(stale), 'files': stale[:40],
                                                  'note': 'the inventory is frozen from other bytes and is re-frozen at the 0.1.0a2 freeze'}
    print(f'INFO  frozen inventory hashes: {len(stale)} staged file(s) differ from the frozen hashes (re-frozen at the freeze)', flush=True)

    code, seconds = run([py, '-m', 'pytest', *SUITE_FLAGS], copy, logs / 'suite.log')
    summary = parse_pytest_summary((logs / 'suite.log').read_text(errors='replace'))
    step('offline unit suite', code == 0 and summary['failed'] == 0 and summary['passed'] > 0, seconds=seconds, **summary, log=str(logs / 'suite.log'))

    # the installed probe needs a clean base installation: the wheel and its required dependencies only (no test tools, no optional extras)
    probe_cwd, probe_venv = work / 'probe', work / 'probe-venv'
    probe_cwd.mkdir()
    run([builder_python, '-m', 'venv', str(probe_venv)], work, logs / 'probe-venv.log')
    probe_py = str(probe_venv / 'bin' / 'python')
    code0, _ = run([probe_py, '-m', 'pip', 'install', '-q', str(wheel)], work, logs / 'probe-pip.log')
    code, seconds = run([py, 'scripts/verify_artifacts.py', str(wheel), str(sdist), '--python', probe_py, '--cwd', str(probe_cwd), '--output', str(work / 'artifact-record.json')], copy,
                        logs / 'artifacts.log')
    step('strict artifact verifier (archives and installed probe)', code0 == 0 and code == 0, seconds=seconds, result=tail(logs / 'artifacts.log', 1))
    shutil.rmtree(probe_venv, ignore_errors=True)

    docs = sorted(p.relative_to(copy).as_posix() for p in (copy / 'docs').glob('*.md'))
    code, seconds = run([py, 'scripts/check_doc_links.py', str(copy), *LINK_FILES, *docs], copy, logs / 'links.log')
    step('documentation link check', code == 0, seconds=seconds, result=(tail(logs / 'links.log', 3).splitlines() or [''])[0])

    rename = []
    frozen_platform = [i for i in inventory['source_collection']['test_ids'] if PLATFORM_ID.search(i)]
    now_platform = [i for i in collected if PLATFORM_ID.search(i)]
    if frozen_platform and now_platform and frozen_platform != now_platform:
        rename = ['--rename', frozen_platform[0], now_platform[0], 'platform signal number of this collection']
    scratch = work / 'scratch-inventory.json'
    code, seconds = run([py, 'scripts/freeze_installed_inventory.py', '--migrate-from', str(inventory_path), '--inventory', str(scratch), '--collection-output', str(work / 'collection.json'),
                         '--wheel', str(wheel), '--sdist', str(sdist), *rename, '--reviewed-by', 'scratch validation', '--report', str(work / 'migration-report.json')],
                        copy, logs / 'refreeze.log')
    migration = json.loads((work / 'migration-report.json').read_text()) if (work / 'migration-report.json').exists() else {}
    if not step('scratch re-freeze for these artifacts', code == 0 and scratch.exists(), seconds=seconds, ids=migration.get('migrated_ids'), baseline=migration.get('baseline_ids'),
                added=len(migration.get('added_ids', [])), dispositions_changed=migration.get('dispositions_changed')):
        return report
    lanes = sorted(name for name, lane in json.loads(scratch.read_text())['lanes'].items() if lane['kind'] == 'installed unit regression')
    lane_results = {}
    for lane in lanes:
        out = work / 'lanes' / lane
        out.parent.mkdir(exist_ok=True)
        code, seconds = run([py, 'scripts/verify_installed.py', '--lane', lane, '--wheel', str(wheel), '--inventory', str(scratch), '--collect-only', '--output', str(out)], copy,
                            logs / f'lane-{lane}-collect.log')
        result = json.loads((out / 'result.json').read_text()) if (out / 'result.json').exists() else {}
        lane_results[lane] = {'ok': code == 0 and result.get('status') == 'passed', 'status': result.get('status'), 'seconds': seconds}
    step('installed-runner checks: every installed unit lane, collect-only', all(r['ok'] for r in lane_results.values()) and bool(lane_results), lanes=lane_results)
    for lane in run_lanes or []:
        out = work / 'lanes' / f'{lane}-run'
        code, seconds = run([py, 'scripts/verify_installed.py', '--lane', lane, '--wheel', str(wheel), '--inventory', str(scratch), '--output', str(out)], copy, logs / f'lane-{lane}-run.log',
                            timeout=3600)
        result = json.loads((out / 'result.json').read_text()) if (out / 'result.json').exists() else {}
        step(f'installed-runner lane {lane}, executed', code == 0 and result.get('status') == 'passed', seconds=seconds, status=result.get('status'), result=tail(logs / f'lane-{lane}-run.log', 2))
    report['ok'] = all(s['ok'] for name, s in report['steps'].items() if name != 'frozen inventory hashes')
    if not keep_venv:
        shutil.rmtree(venv, ignore_errors=True)
    (work / 'report.json').write_text(json.dumps(report, indent=2, default=str))
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('tree', type=Path)
    parser.add_argument('workdir', type=Path)
    parser.add_argument('--run-lane', action='append', default=[], help='execute this installed unit lane completely (repeatable)')
    parser.add_argument('--keep-venv', action='store_true')
    args = parser.parse_args(argv)
    result = validate(args.tree, args.workdir, run_lanes=args.run_lane, keep_venv=args.keep_venv)
    print('EXACT-TREE VALIDATION ' + ('PASSED' if result.get('ok') else 'FAILED'))
    return 0 if result.get('ok') else 1


if __name__ == '__main__':
    raise SystemExit(main())
