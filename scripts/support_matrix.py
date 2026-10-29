#!/usr/bin/env python3
"""Prepare finite dependency cells and execute checkout-free E08 observations.

This tool records observations, never release/matrix acceptance. Native runner
availability and Docker ownership supervision belong to the invoking controller.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import platform
import subprocess
import sys

# Portable manifests are immutable inputs, even when their directory is writable.
# Set this before local imports so an ordinary invocation cannot add pyc members.
sys.dont_write_bytecode = True

if __package__:
    from . import verify_installed as runner
    from .installed_dependencies import normalized, read_constraints, read_wheelhouse, verify_versions
else:
    import verify_installed as runner
    from installed_dependencies import normalized, read_constraints, read_wheelhouse, verify_versions

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = {'neo4j', 'neo4j-rust-ext', 'confluent-kafka', 'pydantic', 'pyyaml', 'docker', 'numpy', 'prometheus-client'}
TOOLS = ('verify_installed.py', 'installed_dependencies.py', 'installed_guard.py', 'installed_pytest.py', 'support_matrix.py', 'matrix_docker.py')
PROBES = ('e04_imports.py', 'e04_public_example.py', 'e05_resources.py', 'e05_runtime.py', 'e08_base_probe.py')


def sha(content):
    return hashlib.sha256(content).hexdigest()


def new_output(path):
    requested = path.absolute()
    resolved = requested.parent.resolve()/requested.name
    runner.check_git_ancestry(resolved)
    if requested.exists() or requested.is_symlink() or resolved.is_relative_to(ROOT):
        raise ValueError('output must be new and outside tooling/input root')
    resolved.mkdir(parents=True, mode=0o700)
    return resolved


def execute(command, output, name, receipts, *, timeout=600):
    runner.run(command, cwd=output, env=runner.runtime_environment(Path(command[0])),
               log=output/(name+'.log'), timeout=timeout, receipts=receipts)


def prepare(args, output, receipts):
    """Resolve once, freeze the joint closure, then build/download exact wheels."""
    if (ROOT/'bundle.json').exists():
        verify_bundle(ROOT)
    pins = read_constraints(args.pins)
    if set(pins['pins']) != RUNTIME:
        raise ValueError('runtime lane must pin exactly the eight mandatory distributions')
    (output/'runtime-pins.txt').write_bytes(pins['content'])
    resolution_pins = read_constraints(args.resolution_constraints) if args.resolution_constraints else None
    resolution_options = []
    if resolution_pins:
        if any(resolution_pins['pins'].get(name) != version for name, version in pins['pins'].items()):
            raise ValueError('full resolution constraints differ from runtime lane pins')
        (output/'resolution-constraints.txt').write_bytes(resolution_pins['content'])
        resolution_options = ['--constraint', str(output/'resolution-constraints.txt')]
    python = output/'resolver/bin/python'
    execute([sys.executable, '-I', '-m', 'venv', str(python.parent.parent)], output, 'venv', receipts, timeout=120)
    inventory = json.loads(args.inventory.read_text())
    runner.validate_wheel(args.wheel, inventory)
    requirements = list(dict.fromkeys(inventory['test_requirements'] + [
        r for lane in inventory['lanes'].values() if lane['kind'] == 'installed unit regression'
        for r in lane.get('extra_test_requirements', [])]))
    report = output/'resolution.json'
    execute([str(python), '-m', 'pip', '--isolated', 'install', '--dry-run', '--ignore-installed',
             '--report', str(report), '--constraint', str(output/'runtime-pins.txt'),
             *resolution_options, str(args.wheel.resolve(strict=True)), *requirements], output, 'resolve', receipts)
    resolution = json.loads(report.read_text())
    versions = {normalized(row['metadata']['name']): row['metadata']['version'] for row in resolution['install']}
    if any(versions.get(name) != version for name, version in pins['pins'].items()):
        raise ValueError('resolver changed a runtime lane pin')
    # The explicit application wheel is carried separately and remains frozen by
    # the inventory's payload AND distribution-member hashes.
    versions.pop('neo4j-graph-loader')
    if resolution_pins and any(resolution_pins['pins'].get(name) != version for name, version in versions.items()):
        raise ValueError('resolved closure differs from explicit full constraints')
    constraint = output/'constraints.txt'
    constraint.write_text(''.join(f'{name}=={version}\n' for name, version in sorted(versions.items())))
    wheels = output/'wheelhouse'
    wheels.mkdir(mode=0o700)
    execute([str(python), '-m', 'pip', '--isolated', 'wheel', '--no-deps', '--wheel-dir', str(wheels),
             '--constraint', str(constraint), *[f'{n}=={v}' for n, v in sorted(versions.items())]], output, 'wheels', receipts)
    manifest = {'schema_version': 1, 'files': {p.name: sha(p.read_bytes()) for p in sorted(wheels.iterdir())}}
    (output/'wheelhouse.json').write_text(json.dumps(manifest, indent=2)+'\n')
    read_wheelhouse(wheels, output/'wheelhouse.json', versions)
    return {'runtime_pins_sha256': pins['sha256'], 'resolution_constraints_sha256':resolution_pins['sha256'] if resolution_pins else None,
            'versions': versions,
            'resolution_sha256': sha(report.read_bytes()), 'wheelhouse': manifest}


def bundle(args, output, receipts):
    """Copy only hashed maintained test/reference files and public artifacts."""
    data = json.loads(args.inventory.read_text())
    runner.validate_wheel(args.wheel, data)
    files = {}
    for name, lane in data['lanes'].items():
        if lane['kind'] != 'installed unit regression':
            continue
        runner.validate_inventory(data, name)
        for path, digest in lane['stage_files'].items():
            if path in files and files[path] != digest:
                raise ValueError('inconsistent shared fixture hashes')
            files[path] = digest
    for name in TOOLS:
        files['scripts/'+name] = sha((ROOT/'scripts'/name).read_bytes())
    for name in PROBES:
        files['tests/fixtures/'+name] = sha((ROOT/'tests/fixtures'/name).read_bytes())
    artifact_tool = 'plan/library-implementation/tools/verify_e03_artifacts.py'
    files[artifact_tool] = sha((ROOT/artifact_tool).read_bytes())
    for relative, digest in files.items():
        source = ROOT/relative
        if any(p.is_symlink() for p in (source, *source.parents) if p.is_relative_to(ROOT)):
            raise ValueError('symlink in finite tooling/test closure')
        content = source.read_bytes()
        if sha(content) != digest:
            raise ValueError('source differs from frozen inventory: '+relative)
        target = output/relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    (output/'inventory.json').write_bytes(args.inventory.read_bytes())
    # Preserve the real wheel filename for pip.
    (output/args.wheel.name).write_bytes(args.wheel.read_bytes())
    for name in ('minimum-runtime.txt', 'current-runtime-20261026.txt',
                 'minimum-linux-arm64-20261027.txt', 'current-linux-arm64-20261027.txt'):
        if not (ROOT/'config/support-matrix'/name).exists():
            continue  # Initial resolution precedes the first recorded full lock.
        target = output/'config/support-matrix'/name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((ROOT/'config/support-matrix'/name).read_bytes())
    if args.preparation_only:
        if any((args.constraints, args.wheelhouse, args.wheelhouse_manifest)):
            raise ValueError('preparation-only bundle cannot claim resolved architecture wheels')
    else:
        if not all((args.constraints, args.wheelhouse, args.wheelhouse_manifest)):
            raise ValueError('executable cell bundle requires constraints and manifested wheelhouse')
        pins = read_constraints(args.constraints)
        wheels = read_wheelhouse(args.wheelhouse, args.wheelhouse_manifest, pins['pins'])
        (output/'constraints.txt').write_bytes(pins['content'])
        (output/'wheelhouse.json').write_bytes(args.wheelhouse_manifest.read_bytes())
        (output/'wheelhouse').mkdir()
        for name, content in wheels['contents'].items():
            (output/'wheelhouse'/name).write_bytes(content)
    # Freeze every portable byte; result/command receipts live outside this root.
    files = {p.relative_to(output).as_posix(): sha(p.read_bytes()) for p in sorted(output.rglob('*')) if p.is_file()}
    manifest = {'schema_version': 1, 'wheel': args.wheel.name, 'preparation_only':args.preparation_only, 'files': files}
    (output/'bundle.json').write_text(json.dumps(manifest, indent=2)+'\n')
    return {'bundle_manifest_sha256': sha((output/'bundle.json').read_bytes()), 'members': len(files)}


def verify_bundle(root):
    runner.check_git_ancestry(root)
    data = json.loads((root/'bundle.json').read_text())
    files = data.get('files', {})
    if data.get('schema_version') != 1 or not files:
        raise ValueError('missing finite portable bundle manifest')
    for relative in files:
        path = Path(relative)
        if (path.is_absolute() or path.as_posix() != relative or any(
                p in {'.git', 'src', 'datasets', '__pycache__', '..', '.'} or p.startswith('.env') for p in path.parts)):
            raise ValueError('unsafe portable bundle member')
    paths = list(root.rglob('*'))
    if any(p.is_symlink() for p in paths):
        raise ValueError('portable bundle contains symlinks')
    actual = {p.relative_to(root).as_posix(): sha(p.read_bytes()) for p in paths
              if p.is_file() and p != root/'bundle.json'}
    if actual != files:
        raise ValueError('portable bundle has missing, changed or unexpected members')
    if data['wheel'] not in files or Path(data['wheel']).name != data['wheel'] or not data['wheel'].endswith('.whl'):
        raise ValueError('portable bundle has no explicit application wheel')
    return data


def observe(args, output, receipts):
    root = args.bundle.resolve(strict=True)
    manifest = verify_bundle(root)
    if manifest.get('preparation_only'):
        raise ValueError('preparation inputs do not establish an executable platform cell')
    machine = platform.machine()
    expected = {'arm64': 'aarch64', 'amd64': 'x86_64'}[args.architecture]
    if sys.version_info[:2] != (3, 12) or platform.system() != 'Linux' or machine != expected:
        raise ValueError('observation interpreter/OS/architecture differs from explicit matrix cell')
    wheel = root/manifest['wheel']
    runner.validate_wheel(wheel, json.loads((root/'inventory.json').read_text()))
    if args.lane:
        execute([sys.executable, '-B', str(root/'scripts/verify_installed.py'), '--lane', args.lane,
                 '--wheel', str(wheel), '--inventory', str(root/'inventory.json'), '--constraints', str(root/'constraints.txt'),
                 '--wheelhouse', str(root/'wheelhouse'), '--wheelhouse-manifest', str(root/'wheelhouse.json'),
                 '--output', str(output/'installed'), '--timeout', '1800',
                 *(['--process-envelope'] if args.process_envelope else [])], output, 'installed', receipts,
                timeout=14400 if args.process_envelope else 2400)
        observed = json.loads((output/'installed/result.json').read_text())
    else:
        python = output/'bare/bin/python'
        execute([sys.executable, '-I', '-m', 'venv', str(python.parent.parent)], output, 'venv', receipts, timeout=120)
        execute([str(python), '-m', 'pip', '--isolated', 'install', '--no-index', '--find-links', str(root/'wheelhouse'),
                 '--only-binary=:all:', '--constraint', str(root/'constraints.txt'), '--report', str(output/'install-report.json'),
                 str(wheel)], output, 'install', receipts)
        execute([str(python), '-I', str(root/'tests/fixtures/e08_base_probe.py'), str(root/'inventory.json'),
                 str(output/'base.json')], output, 'base', receipts, timeout=60)
        observed = json.loads((output/'base.json').read_text())
        verify_versions(observed['versions'], read_constraints(root/'constraints.txt')['pins'], observed['application_version'])
        spec = importlib.util.spec_from_file_location('artifact_probe', root/'plan/library-implementation/tools/verify_e03_artifacts.py')
        probe = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(probe)
        inventory = json.loads((root/'inventory.json').read_text())
        def supervised_probe(command, *, cwd, env, capture_output, text, timeout):
            log = output/f'public-{len(receipts)}.log'
            errors = log.with_suffix('.stderr.log')
            runner.run(command, cwd=cwd, env=env, log=log, stderr_log=errors, timeout=timeout,
                       receipts=receipts, allowed_exit_codes=(0, 2))
            # The unchanged helper asserts each command's expected exit and
            # stdout/stderr separately, including intentional parser errors.
            return subprocess.CompletedProcess(command, receipts[-1]['exit_code'],
                                               stdout=log.read_text(), stderr=errors.read_text())
        observed['public'] = probe.probe(python, {k:v for k,v in inventory['package_payload'].items() if k.endswith('.py')},
                                         output, command_runner=supervised_probe)
        execute([str(python), '-I', '-m', 'pip', 'check'], output, 'pip-check', receipts, timeout=60)
    return {'execution_mode': args.execution_mode, 'architecture': args.architecture,
            'python': sys.version, 'platform': platform.platform(), 'machine': machine,
            'bundle_manifest_sha256': sha((root/'bundle.json').read_bytes()),
            'observation': observed, 'release_acceptance': False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    p = sub.add_parser('prepare')
    p.add_argument('--pins', type=Path, required=True)
    p.add_argument('--resolution-constraints', type=Path, help='exact previously resolved runtime AND test closure for another native runner')
    p.add_argument('--wheel', type=Path, required=True)
    p.add_argument('--inventory', type=Path, required=True)
    b = sub.add_parser('bundle')
    for name in ('wheel', 'inventory'):
        b.add_argument('--'+name, type=Path, required=True)
    for name in ('constraints', 'wheelhouse', 'wheelhouse-manifest'):
        b.add_argument('--'+name, type=Path)
    b.add_argument('--preparation-only', action='store_true', help='portable native-runner inputs; no resolved architecture claim')
    o = sub.add_parser('observe')
    o.add_argument('--bundle', type=Path, required=True)
    o.add_argument('--architecture', choices=['arm64', 'amd64'], required=True)
    o.add_argument('--execution-mode', choices=['native', 'emulated'], required=True)
    o.add_argument('--lane', help='explicit frozen installed lane; omit for bare runtime probe')
    o.add_argument('--process-envelope', action='store_true', help='explicit full process matrix; requires public-contracts lane')
    for command in (p, b, o):
        command.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    if getattr(args, 'process_envelope', False) and args.lane != 'public-contracts':
        parser.error('--process-envelope requires --lane public-contracts')
    receipts = []
    output = None
    try:
        output = new_output(args.output)
        result = {'prepare': prepare, 'bundle': bundle, 'observe': observe}[args.command](args, output, receipts)
        result.update(status='observed', command=sys.argv, commands=receipts)
        # Bundle receipts cannot be written inside their own immutable manifest.
        if args.command != 'bundle':
            (output/'result.json').write_text(json.dumps(result, indent=2)+'\n')
        print(json.dumps({k:v for k,v in result.items() if k not in {'observation', 'commands', 'wheelhouse', 'versions'}}))
        return 0
    except BaseException as exc:
        if output is not None:
            (output/'failure.json').write_text(json.dumps({'status':'failed', 'error':str(exc),
                'interruption':type(exc).__name__, 'commands':receipts}, indent=2)+'\n')
        if not isinstance(exc, Exception):
            raise
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
