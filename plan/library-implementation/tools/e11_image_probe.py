"""Probe the four candidate images of one architecture against the frozen wheel payload.

    python e11_image_probe.py IMAGES_JSON INVENTORY_JSON OUTPUT_JSON

Runs `docker run --rm` (owner-labelled, offline, read-only root, no mounts) with `--entrypoint python` and
records, per image: the labels, the installed distribution version, the sha256 of every installed
`neo4j_graph_loader/**` file compared with the inventory's `package_payload` (frozen from the wheel), the metrics
process version, `pip check` and the full dependency list, plus the image entrypoint's `--help`/`--version`
output. Only containers created here are touched, each removed by its recorded ID.
"""
import json
import secrets
import subprocess
import sys
from pathlib import Path

PROBE = r'''
import hashlib, importlib.metadata as m, json, os, platform, subprocess, sys
from pathlib import Path
import neo4j_graph_loader
pkg = Path(neo4j_graph_loader.__file__).parent
files = {}
for p in sorted(pkg.rglob('*')):
    if p.is_file() and '__pycache__' not in p.parts:
        files['neo4j_graph_loader/' + str(p.relative_to(pkg))] = hashlib.sha256(p.read_bytes()).hexdigest()
from neo4j_graph_loader.utils import metrics
check = subprocess.run([sys.executable, '-m', 'pip', 'check'], capture_output=True, text=True)
print(json.dumps({
    'version': m.version('neo4j-graph-loader'), 'root_export': neo4j_graph_loader.__version__,
    'metrics_process_version': metrics.PROCESS_VERSION, 'env_version': os.environ.get('GRAPH_LOADER_VERSION'),
    'python': platform.python_version(), 'machine': platform.machine(), 'location': str(pkg),
    'files': files, 'pip_check': [check.returncode, check.stdout.strip()],
    'distributions': sorted(f'{d.metadata["Name"]}=={d.version}' for d in m.distributions()),
}))
'''


def run(command, **kwargs):
    return subprocess.run(command, capture_output=True, text=True, timeout=300, **kwargs)


def main():
    images_path, inventory_path, output_path = map(Path, sys.argv[1:4])
    images = json.loads(images_path.read_text())
    frozen = json.loads(inventory_path.read_text())['package_payload']
    token = secrets.token_hex(8)
    label = f'io.graph-loader.e11-owner={token}'
    report = {'owner_label': label, 'version': images['version'], 'images': {}, 'created': [], 'removed': []}

    def container(image, *arguments, entrypoint=None):
        command = ['docker', 'run', '--rm', '--network', 'none', '--read-only', '--label', label,
                   '--cidfile', str(cidfile)]
        if entrypoint:
            command += ['--entrypoint', entrypoint]
        return run(command + [image, *arguments])

    for row in images['images']:
        role, identity = row['component'], row['id']
        cidfile = output_path.with_suffix(f'.{role}.cid')
        cidfile.unlink(missing_ok=True)
        probe = container(identity, '-c', PROBE, entrypoint='python')
        data = json.loads(probe.stdout.strip().splitlines()[-1]) if probe.returncode == 0 else {'error': probe.stderr[-400:]}
        entry = {'id': identity, 'platform': row['platform'], 'labels': row['labels'],
                 'entrypoint': row['entrypoint'], 'probe_exit': probe.returncode}
        if 'error' not in data:
            files = data.pop('files')
            entry['payload_equal_to_frozen_wheel'] = files == frozen
            entry['payload_only_in_image'] = sorted(set(files) - set(frozen))
            entry['payload_missing_in_image'] = sorted(set(frozen) - set(files))
            entry['payload_differing'] = sorted(k for k in set(files) & set(frozen) if files[k] != frozen[k])
            entry['payload_files'] = len(files)
        entry.update(data)
        for flag in ('--help',) if role != 'cli' else ('--help', '--version'):
            cidfile.unlink(missing_ok=True)
            out = container(identity, flag)
            entry[f'entrypoint{flag}'] = {'exit': out.returncode, 'stdout': out.stdout.strip()[:200], 'stderr': out.stderr.strip()[-200:]}
        report['images'][role] = entry
        if cidfile.exists():
            report['created'].append(cidfile.read_text().strip())
    leftover = run(['docker', 'ps', '-a', '-q', '--filter', f'label={label}']).stdout.split()
    report['leftover_containers'] = leftover
    output_path.write_text(json.dumps(report, indent=2) + '\n')
    ok = all(e['probe_exit'] == 0 and e.get('payload_equal_to_frozen_wheel') and e['pip_check'][0] == 0 and not leftover
             and e['version'] == images['version'] == e['metrics_process_version'] == e['root_export']
             and e['labels']['org.opencontainers.image.version'] == images['version'] for e in report['images'].values())
    print(json.dumps({'ok': ok, 'leftover': leftover}))
    return 0 if ok else 1


sys.exit(main())
