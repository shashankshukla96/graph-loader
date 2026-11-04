"""Rerun the E08 support-matrix cells (minimum and current) for the machine's own architecture, natively.

    python e11_matrix.py ARCH SRC_DIR WHEEL WORKDIR [--cells minimum current]

ARCH is arm64 or amd64 and must match the Docker daemon. SRC_DIR is a clean `git archive` export of the candidate
commit; WHEEL is the candidate wheel. Per cell: with network, `support_matrix.py prepare` resolves and downloads the
wheelhouse and `bundle` freezes the portable bundle; then, offline with a read-only root, non-root user and an
owned named volume, `observe` runs the bare runtime probe, each of the eight installed unit lanes, and the full
process envelope with `public-contracts`. Every container and the volume carry an owner label and are removed by
recorded ID only; nothing is pruned. Results are exported to WORKDIR.
"""
import json
import secrets
import signal
import subprocess
import sys
import time
from pathlib import Path

IMAGE = 'python:3.12-slim'
LANES = ['core', 'safety', 'cli-runtime', 'public-contracts', 'writer-runtime', 'orchestration-runtime',
         'example-contracts', 'service-helper-unit']
LOCKS = {'minimum': 'minimum-linux-arm64-20261027.txt', 'current': 'current-linux-arm64-20261027.txt'}
PINS = {'minimum': 'minimum-runtime.txt', 'current': 'current-runtime-20261026.txt'}
USER = '10001:10001'


def docker(*args, input=None, timeout=900, check=True):
    result = subprocess.run(['docker', *args], input=input, capture_output=True, timeout=timeout,
                            preexec_fn=lambda: signal.signal(signal.SIGINT, signal.SIG_DFL))
    if check and result.returncode:
        raise RuntimeError(f'docker {" ".join(args[:3])} failed: {result.stderr.decode()[-500:]}')
    return result


def main():
    arch, src, wheel, workdir = sys.argv[1], Path(sys.argv[2]).resolve(), Path(sys.argv[3]).resolve(), Path(sys.argv[4]).resolve()
    cells = sys.argv[sys.argv.index('--cells') + 1:] if '--cells' in sys.argv else ['minimum', 'current']
    expected = {'arm64': 'aarch64', 'amd64': 'x86_64'}[arch]
    daemon = docker('info', '--format', '{{.Architecture}}').stdout.decode().strip()
    if daemon != expected:
        raise SystemExit(f'daemon architecture {daemon} is not {expected}')
    workdir.mkdir(parents=True, exist_ok=False)
    token = secrets.token_hex(8)
    label = f'io.graph-loader.e11-owner={token}'
    volume = f'e11-{token}-matrix-{arch}'
    registry = workdir / 'registry.jsonl'
    summary = {'arch': arch, 'owner': token, 'image': IMAGE, 'cells': {}}

    def record(kind, **values):
        with registry.open('a') as handle:
            handle.write(json.dumps({'kind': kind, **values}) + '\n')

    def run_container(name, command, *, network, timeout, user=USER, mounts=()):
        cidfile = workdir / f'{name}.cid'
        cidfile.unlink(missing_ok=True)
        args = ['run', '--rm', '--label', label, '--cidfile', str(cidfile), '--user', user, '--init',
                '--read-only', '--tmpfs', '/tmp:rw,exec,size=2g', '-e', 'HOME=/tmp', '-e', 'PIP_NO_CACHE_DIR=1',
                '-v', f'{volume}:/work', *mounts]
        if not network:
            args += ['--network', 'none']
        started = time.time()
        result = docker(*args, IMAGE, *command, timeout=timeout, check=False)
        if cidfile.exists():
            record('container', name=name, id=cidfile.read_text().strip())
        (workdir / f'{name}.log').write_bytes(result.stdout + b'\n--- stderr ---\n' + result.stderr)
        return {'exit': result.returncode, 'seconds': round(time.time() - started, 1)}

    try:
        docker('volume', 'create', '--label', label, volume)
        record('volume', name=volume)
        tarball = workdir / 'inputs.tar'
        subprocess.run(['tar', '-c', '-C', str(src.parent), '--exclude=var', '-f', str(tarball), src.name], check=True,
                       env={'COPYFILE_DISABLE': '1', 'PATH': '/usr/bin:/bin'})
        docker('run', '--rm', '--label', label, '-i', '-v', f'{volume}:/work', '--user', '0', IMAGE, 'sh', '-c',
               'mkdir -p /work/cand /work/out && tar -x -C /work -f - && chown -R 10001:10001 /work',
               input=tarball.read_bytes())
        docker('run', '--rm', '--label', label, '-i', '-v', f'{volume}:/work', '--user', '0', IMAGE, 'sh', '-c',
               f'cat > /work/cand/{wheel.name} && chown -R 10001:10001 /work', input=wheel.read_bytes())
        root = f'/work/{src.name}'
        for kind in cells:
            cell = summary['cells'][kind] = {}
            # Controller venv per cell: docker and urllib3 pinned to the repository's own locks (docker 7.1.0 minimum /
            # 7.2.0 current, urllib3 2.8.0), the full lock used as a constraint so no other dependency drifts.
            docker_pin = {'minimum': '7.1.0', 'current': '7.2.0'}[kind]
            setup = run_container(f'{kind}-controller', ['sh', '-c',
                f'python -m venv /work/ctl-{kind} && /work/ctl-{kind}/bin/pip install -q -c {root}/config/support-matrix/{LOCKS[kind]} '
                f'psutil==7.2.2 packaging==26.3 docker=={docker_pin} urllib3==2.8.0 '
                f'&& /work/ctl-{kind}/bin/pip freeze > /work/out/controller-{kind}-freeze.txt'], network=True, timeout=600)
            cell['controller'] = setup
            if setup['exit']:
                continue
            ctl = f'/work/ctl-{kind}/bin/python'
            lock = ['--resolution-constraints', f'{root}/config/support-matrix/{LOCKS[kind]}'] if arch == 'arm64' else []
            common = ['--wheel', f'/work/cand/{wheel.name}', '--inventory', f'{root}/tests/inventory/test-inventory.json']
            prepare = run_container(f'{kind}-prepare', [ctl, f'{root}/scripts/support_matrix.py', 'prepare',
                '--pins', f'{root}/config/support-matrix/{PINS[kind]}', *lock, *common, '--output', f'/work/out/{kind}-prepare'],
                network=True, timeout=1800)
            cell['prepare'] = prepare
            if prepare['exit']:
                continue
            bundle = run_container(f'{kind}-bundle', [ctl, f'{root}/scripts/support_matrix.py', 'bundle',
                *common, '--constraints', f'/work/out/{kind}-prepare/constraints.txt',
                '--wheelhouse', f'/work/out/{kind}-prepare/wheelhouse', '--wheelhouse-manifest', f'/work/out/{kind}-prepare/wheelhouse.json',
                '--output', f'/work/out/{kind}-bundle'], network=False, timeout=600)
            cell['bundle'] = bundle
            if bundle['exit']:
                continue
            for lane in [None, *LANES]:
                name = f'{kind}-{lane or "bare"}'
                command = [ctl, f'/work/out/{kind}-bundle/scripts/support_matrix.py', 'observe',
                           '--bundle', f'/work/out/{kind}-bundle', '--architecture', arch, '--execution-mode', 'native',
                           '--output', f'/work/out/{name}']
                if lane:
                    command += ['--lane', lane]
                if lane == 'public-contracts':
                    command += ['--process-envelope']
                cell[lane or 'bare'] = run_container(name, command, network=False, timeout=14400 if lane == 'public-contracts' else 3000)
                print(kind, lane or 'bare', cell[lane or 'bare'], flush=True)
                (workdir / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        exporter = docker('run', '--rm', '--label', label, '-v', f'{volume}:/work:ro', '--user', '0', IMAGE, 'tar', '-c', '-C', '/work/out', '.',
                          timeout=1800)
        (workdir / 'out.tar').write_bytes(exporter.stdout)
        summary['export_bytes'] = len(exporter.stdout)
    finally:
        (workdir / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        leftover = docker('ps', '-a', '-q', '--filter', f'label={label}', check=False).stdout.decode().split()
        for cid in leftover:
            docker('rm', '-f', cid, check=False)
        owned = docker('volume', 'ls', '-q', '--filter', f'label={label}', check=False).stdout.decode().split()
        summary['removed_volumes'] = [v for v in owned if v == volume and docker('volume', 'rm', v, check=False).returncode == 0]
        summary['removed_leftover_containers'] = leftover
        (workdir / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    ok = all(c.get(k, {}).get('exit') == 0 for c in summary['cells'].values() for k in c)
    print(json.dumps({'ok': ok, 'cells': {k: {s: v['exit'] for s, v in c.items()} for k, c in summary['cells'].items()}}))
    return 0 if ok else 1


sys.exit(main())
