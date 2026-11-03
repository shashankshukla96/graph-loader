"""Remove what one soak run owns, by recorded ID, after the harness itself died (or to verify a normal close left nothing).

    python -m soak.cleanup <run directory> --manifest manifest.json [--execute]

Without ``--execute`` it only lists. Ownership follows ``tests/installed_service/ownership.py``: a container is ours if it carries the
run's owner label, or carries no owner label and is attached to the run's own network, was created from one of the four frozen
candidate images, and is not in the pre-run foreign snapshot. Volumes and the network need the owner label. Nothing is selected by a
name pattern or a broad label filter, and nothing is pruned.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import docker

from tests.installed_service.ownership import OWNER_LABEL

NETWORK_PREFIX = 'e13-'


def plan(client, run_dir: Path, candidate_images: set) -> dict:
    registry = [json.loads(line) for line in (run_dir / 'registry.jsonl').read_text().splitlines() if line.strip()]
    owner = registry[0]['owner']
    before = json.loads((run_dir / 'out' / 'foreign-before.json').read_text()) if (run_dir / 'out' / 'foreign-before.json').exists() else {}
    preexisting = set(before.get('containers', {}))
    networks = [r['id'] for r in registry if r['kind'] == 'created' and r.get('resource') == 'network']
    volumes = [r['name'] for r in registry if r['kind'] == 'created' and r.get('resource') == 'volume']
    verified_networks = []
    for network_id in networks:
        try:
            attrs = client.api.inspect_network(network_id)
        except docker.errors.NotFound:
            continue
        if (attrs.get('Labels') or {}).get(OWNER_LABEL) == owner:
            verified_networks.append(attrs['Id'])
    containers, skipped = [], []
    for row in client.api.containers(all=True):
        cid = row['Id']
        labels = row.get('Labels') or {}
        if cid in preexisting:
            continue
        if labels.get(OWNER_LABEL) == owner:
            containers.append(cid)
            continue
        if labels.get(OWNER_LABEL) not in (None, owner):
            continue
        on_network = any(n.get('NetworkID') in verified_networks for n in (row.get('NetworkSettings') or {}).get('Networks', {}).values())
        if on_network and row.get('ImageID') in candidate_images:
            containers.append(cid)
        elif on_network:
            skipped.append({'id': cid, 'reason': 'on the owned network but not from a candidate image'})
    owned_volumes = []
    for name in volumes:
        try:
            attrs = client.api.inspect_volume(name)
        except docker.errors.NotFound:
            continue
        if (attrs.get('Labels') or {}).get(OWNER_LABEL) == owner:
            owned_volumes.append(name)
    return {'owner': owner, 'containers': containers, 'volumes': owned_volumes, 'networks': verified_networks, 'skipped': skipped}


def execute(client, found: dict) -> dict:
    receipt = {'removed': [], 'failed': []}
    for cid in found['containers']:
        try:
            client.api.remove_container(cid, force=True, v=True)
            receipt['removed'].append(('container', cid))
        except docker.errors.NotFound:
            pass
        except Exception as error:
            receipt['failed'].append(('container', cid, type(error).__name__))
    for name in found['volumes']:
        try:
            client.api.remove_volume(name, force=True)
            receipt['removed'].append(('volume', name))
        except docker.errors.NotFound:
            pass
        except Exception as error:
            receipt['failed'].append(('volume', name, type(error).__name__))
    for nid in found['networks']:
        try:
            client.api.remove_network(nid)
            receipt['removed'].append(('network', nid))
        except docker.errors.NotFound:
            pass
        except Exception as error:
            receipt['failed'].append(('network', nid, type(error).__name__))
    return receipt


def main(argv=None):
    parser = argparse.ArgumentParser(prog='python -m soak.cleanup')
    parser.add_argument('run_dir')
    parser.add_argument('--manifest', default='', help='frozen-candidate manifest (container shapes); the host shapes own no images')
    parser.add_argument('--docker-host', default='unix:///var/run/docker.sock')
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args(argv)
    candidates = set()
    if args.manifest:
        manifest = json.loads(Path(args.manifest).read_text())
        candidates = {row['id'] for row in manifest['images']['amd64']['rows'].values()}
    client = docker.DockerClient(base_url=args.docker_host, timeout=60)
    run_dir = Path(args.run_dir).expanduser()
    found = plan(client, run_dir, candidates)
    print(json.dumps({'found': found}, indent=1))
    if args.execute:
        receipt = execute(client, found)
        (run_dir / 'out').mkdir(exist_ok=True)
        (run_dir / 'out' / 'cleanup-receipt.json').write_text(json.dumps({'found': found, 'receipt': receipt}, indent=1))
        print(json.dumps(receipt, indent=1))
        return 0 if not receipt['failed'] else 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
