#!/usr/bin/env python3
"""Build development candidates locally; never push, pull release tags or publish."""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path
import re
import shlex
import subprocess

ROOT = Path(__file__).resolve().parents[1]
NAMESPACE = "ghcr.io/shashankshukla96"
DOCKERFILES = {"cli": "Dockerfile", "node": "Dockerfile.node_loader",
               "edge": "Dockerfile.edge_loader", "clock": "Dockerfile.clock"}


def package_version(root=ROOT):
    tree = ast.parse((root / "src/neo4j_graph_loader/_version.py").read_text())
    return next(ast.literal_eval(n.value) for n in tree.body if isinstance(n, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == "__version__" for t in n.targets))


def build_inputs(root=ROOT):
    """Hash the complete COPY closure, excluding only Docker-ignored bytecode."""
    paths = [root / n for n in (*DOCKERFILES.values(), 'pyproject.toml', 'requirements.txt',
                                'MANIFEST.in', 'PACKAGE_README.md', 'LICENSE', '.dockerignore',
                                'scripts/build_candidate_images.py', '.env.example')]
    for directory in ('src/neo4j_graph_loader', 'config'):
        for path in sorted((root / directory).rglob('*')):
            if '__pycache__' in path.parts or path.suffix in {'.pyc', '.pyo'}:
                continue
            if path.is_symlink() or path.name.startswith('.env'):
                raise ValueError('unsafe candidate build input: ' + str(path.relative_to(root)))
            if path.is_file():
                paths.append(path)
    for path in paths:
        if path.is_symlink():
            raise ValueError('symlink candidate build input: ' + str(path.relative_to(root)))
    return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path, help="New evidence directory")
    parser.add_argument("--suffix", required=True, help="Unique local candidate suffix (no release tag overwrite)")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,63}", args.suffix):
        parser.error("suffix must be 1-64 lowercase tag-safe characters")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    version = package_version()
    rows = []
    inputs = build_inputs()
    for role, dockerfile in DOCKERFILES.items():
        proposed = f"{NAMESPACE}/graph-loader-{role}:{version}"
        tag = f"{proposed}-{args.suffix}"
        existing = subprocess.run(['docker', 'image', 'inspect', tag], capture_output=True)
        if existing.returncode == 0:
            raise SystemExit(f'refusing to overwrite existing local candidate tag: {tag}')
        iidfile = output / f"{role}.iid"
        command = ['docker', 'build', '--file', str(ROOT / dockerfile), '--build-arg',
                   f'GRAPH_LOADER_VERSION={version}', '--tag', tag, '--iidfile', str(iidfile), str(ROOT)]
        with (output / f'{role}-build.log').open('w') as log:
            result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
        assert result.returncode == 0, f"{role} build failed; inspect owned build log"
        identity = iidfile.read_text().strip()
        inspected = json.loads(subprocess.check_output(['docker', 'image', 'inspect', identity]))[0]
        labels = inspected['Config']['Labels']
        assert labels['org.opencontainers.image.version'] == version
        assert labels['io.graph-loader.component'] == role
        assert labels['io.graph-loader.image-contract'] == '1'
        rows.append({'component': role, 'id': identity, 'local_tag': tag,
                     'proposed_registry_tag': proposed, 'registry_digest': None,
                     'platform': f"{inspected['Os']}/{inspected['Architecture']}",
                     'labels': labels, 'entrypoint': inspected['Config']['Entrypoint'],
                     'command': command, 'exit_code': result.returncode})
        (output / 'images.json').write_text(json.dumps({'version': version, 'input_sha256': inputs,
            'images': rows, 'published': False, 'compatibility': 'exact package version, component and image-contract=1'}, indent=2) + '\n')
    (output / 'image-overrides.sh').write_text('# Public local candidate references; no credentials.\n' + ''.join(
        f"export GRAPH_LOADER_{row['component'].upper()}_IMAGE={shlex.quote(row['local_tag'])}\n" for row in rows))
    print(f"Built {len(rows)} local candidates; manifest: {output / 'images.json'}; overrides: {output / 'image-overrides.sh'}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
