"""Outer-controller checks around a live service lane (native shared Docker Engine substrate).

The pytest process registers every owned Docker resource it creates in an
fsynced registry. This module runs outside that process: it snapshots every
resource before and after, reads the registry and removes only recorded IDs
that still exist and still carry the run's owner label (or, for application
containers the CLI created, are attached to the recorded owned network).
"""
from __future__ import annotations

import json
from pathlib import Path

import docker
import urllib3

OWNER_LABEL = 'io.graph-loader.e09-owner'
MAX_REGISTRY = 4 * 1024 * 1024
FAILURES = (docker.errors.DockerException, OSError, urllib3.exceptions.HTTPError)


def snapshot(client):
    """Names, IDs, state and published ports of every container, network, volume and image."""
    containers = []
    for row in client.api.containers(all=True):
        attrs = client.api.inspect_container(row['Id'])
        containers.append({'id': row['Id'], 'names': sorted(row['Names']), 'image': attrs['Image'],
                           'state': attrs['State']['Status'], 'started': attrs['State']['StartedAt'],
                           'restarts': attrs['RestartCount'],
                           'ports': sorted((p.get('IP', ''), p.get('PrivatePort'), p.get('PublicPort'), p.get('Type'))
                                           for p in row.get('Ports', []))})
    return {'containers': sorted(containers, key=lambda c: c['id']),
            'networks': sorted((n['Id'], n['Name']) for n in client.api.networks()),
            'volumes': sorted(v['Name'] for v in (client.api.volumes().get('Volumes') or [])),
            'images': sorted(i['Id'] for i in client.api.images(all=True))}


def difference(before, after):
    result = {}
    for kind in before:
        old, new = before[kind], after[kind]
        if old != new:
            keys = (lambda x: x['id']) if kind == 'containers' else (lambda x: str(x))
            old_keys, new_keys = {keys(x) for x in old}, {keys(x) for x in new}
            result[kind] = {'added': sorted(new_keys - old_keys), 'removed': sorted(old_keys - new_keys),
                            'changed': sorted(k for k in old_keys & new_keys
                                              if next(x for x in old if keys(x) == k) != next(x for x in new if keys(x) == k))}
    return result


def read_registry(path):
    path = Path(path)
    if not path.is_file() or path.is_symlink() or path.stat().st_size > MAX_REGISTRY:
        raise ValueError('service registry is missing, not a regular file, or exceeds its bound')
    raw = path.read_bytes()
    lines = raw.splitlines()
    truncated = bool(raw) and not raw.endswith(b'\n')
    if truncated:
        lines.pop()
    rows, corrupt = [], 0
    for line in lines:
        # A damaged interior line (a partial write followed by later rows) must not hide the valid rows around it.
        try:
            row = json.loads(line)
        except ValueError:
            row = None
        if isinstance(row, dict):
            rows.append(row)
        else:
            corrupt += 1
    owners = {row.get('owner') for row in rows}
    if len(owners) != 1 or None in owners or any(not isinstance(row.get('kind'), str) for row in rows):
        raise ValueError('service registry names no single owner')
    owner = next(iter(owners))
    if truncated:
        rows.append({'kind': 'truncated', 'owner': owner})
    rows.extend({'kind': 'corrupt', 'owner': owner} for _ in range(corrupt))
    return owner, rows


def recorded(rows):
    """IDs created or observed and not recorded as removed, by resource kind."""
    created = {kind: {} for kind in ('container', 'network', 'volume')}
    for row in rows:
        kind = row.get('resource')
        if row['kind'] in ('created', 'observed') and kind in created and row.get('id'):
            created[kind][row['id']] = row
        elif row['kind'] == 'removed' and kind in created:
            created[kind].pop(row.get('id'), None)
    return created


def ever_created(rows):
    created = {kind: set() for kind in ('container', 'network', 'volume')}
    for row in rows:
        if row['kind'] in ('created', 'observed') and row.get('resource') in created and row.get('id'):
            created[row['resource']].add(row['id'])
    return created


def reap(client, owner, rows, *, images=None, protected=frozenset()):
    """Remove still-present recorded resources after verifying ownership; report all outcomes.

    Unlabelled containers (the CLI's own) are removed only if they were created from one of ``images`` (when
    given), are attached to a network whose owner label was verified, and are not in ``protected`` (the IDs
    that existed before the run).

    A Docker or transport failure on one resource is recorded as ``(id, step, error)`` and
    cleanup continues with the others, so partial results are never lost.
    """
    pending = recorded(rows)
    removed, refused, errors = [], [], []
    name_prefix = f'e09-{owner}-'

    def attempt(identifier, step, call):
        """(done, value): a vanished resource is not an error; any other failure is recorded."""
        try:
            return True, call()
        except docker.errors.NotFound:
            return False, None
        except FAILURES as error:
            errors.append((identifier, step, type(error).__name__))
            return False, None

    # An intent without a matching created row may still have produced a resource: look it up by its exact name.
    for row in rows:
        if row['kind'] == 'intent' and row.get('resource') in pending and str(row.get('name', '')).startswith(name_prefix):
            if not any(created.get('name') == row['name'] for created in rows if created['kind'] == 'created'):
                lookup = {'container': client.api.inspect_container, 'network': client.api.inspect_network,
                          'volume': client.api.inspect_volume}[row['resource']]
                found, inspect = attempt(row['name'], 'intent-lookup', lambda: lookup(row['name']))
                if found:
                    pending[row['resource']].setdefault(inspect.get('Id') or inspect.get('Name'), row)
    # A network is authority for unlabelled containers only when its own owner label is verified.
    trusted, network_labels = set(), {}
    for network in pending['network']:
        found, attrs = attempt(network, 'inspect-network', lambda: client.api.inspect_network(network))
        if found:
            network_labels[network] = attrs.get('Labels') or {}
            if network_labels[network].get(OWNER_LABEL) == owner:
                trusted.add(network)

    for container_id in pending['container']:
        found, attrs = attempt(container_id, 'inspect-container', lambda: client.api.inspect_container(container_id))
        if not found:
            continue
        label = (attrs['Config'].get('Labels') or {}).get(OWNER_LABEL)
        if container_id in protected or (label != owner if label is not None else
                                          (images is not None and attrs.get('Image') not in images
                                           or not any(n['NetworkID'] in trusted for n in attrs['NetworkSettings']['Networks'].values()))):
            refused.append(container_id)
            continue
        done, _ = attempt(container_id, 'remove-container', lambda: client.api.remove_container(container_id, force=True, v=True))
        if done:
            removed.append(container_id)
    for volume in pending['volume']:
        found, attrs = attempt(volume, 'inspect-volume', lambda: client.api.inspect_volume(volume))
        if not found:
            continue
        if (attrs.get('Labels') or {}).get(OWNER_LABEL) != owner:
            refused.append(volume)
            continue
        done, _ = attempt(volume, 'remove-volume', lambda: client.api.remove_volume(volume, force=True))
        if done:
            removed.append(volume)
    for network in network_labels:
        # Labels are read again immediately before removal; the earlier read only decided container authority.
        found, attrs = attempt(network, 'inspect-network', lambda: client.api.inspect_network(network))
        if not found:
            continue
        if (attrs.get('Labels') or {}).get(OWNER_LABEL) != owner:
            refused.append(network)
            continue
        done, _ = attempt(network, 'remove-network', lambda: client.api.remove_network(network))
        if done:
            removed.append(network)
    return {'removed': removed, 'refused': refused, 'errors': errors}
