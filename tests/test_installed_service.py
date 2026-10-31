"""Checks for the installed service runner's inventory, socket and image-manifest boundaries."""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from scripts.installed_service import local_docker_host, validate_images
from scripts import build_candidate_images, installed_service, verify_installed
from scripts.verify_installed import main as installed_main, validate_service_inventory


OWNER = '1' * 32
IMAGE = 'sha256:' + '2' * 64
IDENTITY = '3' * 64


def service_inventory():
    ids = ['tests/installed_service/test_harness.py::test_one',
           'tests/installed_service/test_harness.py::test_two']
    return {'schema_version': 1,
            'package_payload': {name: 'a'*64 for name in (
                'neo4j_graph_loader/__init__.py', 'neo4j_graph_loader/data/graph_schema.yaml',
                'neo4j_graph_loader/data/social_schema.yaml')},
            'source_collection': {'test_ids': list(ids),
                'sha256': hashlib.sha256(json.dumps(ids, sort_keys=True, separators=(',', ':')).encode()).hexdigest()},
            'test_dispositions': {item: {'kind': 'service', 'reason': 'explicit service proof'} for item in ids},
            'lanes': {'service-harness': {'owner': 'E09.F1.S7', 'kind': 'installed service regression',
                'stage_files': {'tests/installed_service/test_harness.py': 'b'*64},
                'current_test_files': ['tests/installed_service/test_harness.py'], 'test_ids': ids,
                'service_dispositions': {item: {'owner': 'E09.F1.S7', 'reason': 'owned scenario'} for item in ids}}}}


def test_service_inventory_requires_frozen_owned_selection():
    data = service_inventory()
    assert validate_service_inventory(data, 'service-harness') == data['lanes']['service-harness']
    for mutate in (
        lambda d: d['lanes']['service-harness'].update(kind='proposed actual-service harness/probe lane'),
        lambda d: d['lanes']['service-harness'].update(owner='E09.F1.S1'),
        lambda d: d['source_collection']['test_ids'].reverse(),
        lambda d: d['source_collection']['test_ids'].pop(),
        lambda d: d['test_dispositions'][d['source_collection']['test_ids'][0]].update(kind='installed'),
        lambda d: d['lanes']['service-harness'].update(allowed_skips={'test': 'infrastructure'}),
        lambda d: d['lanes']['service-harness'].update(excluded_test_ids={'test': 'infrastructure'}),
        lambda d: d['lanes']['service-harness'].update(allowed_skips=None),
        lambda d: d['lanes']['service-harness'].update(excluded_test_ids=[]),
        lambda d: d['lanes']['service-harness'].update(stage_files=[]),
        lambda d: d['lanes']['service-harness'].update(reference_files=[]),
        lambda d: d['lanes']['service-harness'].update(current_test_files=None),
        lambda d: d.update(package_payload=[]),
        lambda d: d['lanes']['service-harness']['stage_files'].update({'.': 'b'*64}),
        lambda d: d['lanes']['service-harness']['service_dispositions'][d['source_collection']['test_ids'][0]].update(owner='E09.F1.S1'),
        lambda d: d['lanes']['service-harness']['stage_files'].update({'tests/installed_service/test_harness.py': 'z'*64}),
    ):
        broken = copy.deepcopy(data)
        mutate(broken)
        with pytest.raises(ValueError):
            validate_service_inventory(broken, 'service-harness')
    with pytest.raises(ValueError):
        validate_service_inventory(data, 'unknown')


@pytest.mark.parametrize('change', ['remove', 'reorder'])
def test_service_inventory_rejects_source_id_changes_even_with_updated_hash(change):
    data = service_inventory()
    ids = data['source_collection']['test_ids']
    if change == 'remove':
        ids.pop()
    else:
        ids.reverse()
    data['source_collection']['sha256'] = hashlib.sha256(
        json.dumps(ids, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    with pytest.raises(ValueError, match='absent or reordered'):
        validate_service_inventory(data, 'service-harness')


def test_service_cli_remains_closed_until_controller_and_image_preflight_exist(tmp_path, capsys, monkeypatch):
    """Name kept so the frozen test ID stays; since E09.F1.S7 the controller exists and only a failed preflight stops the lane."""
    inventory_path = tmp_path/'inventory.json'
    inventory_path.write_text(json.dumps(service_inventory()))
    common = ['--lane', 'service-harness', '--wheel', str(tmp_path/'unused.whl'),
              '--inventory', str(inventory_path), '--output', str(tmp_path/'output')]
    assert installed_main(common) == 1
    assert 'require an image manifest and explicit local Docker socket' in capsys.readouterr().err
    assert installed_main(common+['--images', str(tmp_path/'missing.json'),
                                  '--docker-host', 'tcp://192.0.2.1:2375']) == 1
    assert 'explicit local Docker socket required' in capsys.readouterr().err
    (tmp_path/'unused.whl').touch()
    calls = []
    client = SimpleNamespace(close=lambda: calls.append('close'))
    monkeypatch.setitem(sys.modules, 'docker', SimpleNamespace(DockerClient=lambda **kwargs: client))
    monkeypatch.setattr(verify_installed, 'validate_wheel', lambda wheel, data: ({}, '0.1.0a1.dev0', {}))
    def failing_preflight(path, **kwargs):
        calls.append(('images', path, kwargs['version']))
        raise ValueError('candidate image preflight rejected')
    monkeypatch.setattr(verify_installed, 'preflight_service_images', failing_preflight)
    assert installed_main(common+['--images', str(tmp_path/'missing.json'),
                                  '--docker-host', 'unix:///tmp/unused.sock']) == 1
    assert 'candidate image preflight rejected' in capsys.readouterr().err
    assert calls == [('images', tmp_path/'missing.json', '0.1.0a1.dev0'), 'close']
    assert not (tmp_path/'output').exists()


@pytest.mark.parametrize('change', ['none', 'daemon', 'inputs', 'version', 'payload', 'extra_source'])
def test_service_image_preflight_rechecks_daemon_and_source(monkeypatch, tmp_path, change):
    calls = []
    def info():
        calls.append('daemon')
        return {'ID': 'second' if change == 'daemon' and calls.count('daemon') > 1 else 'first'}
    def inputs():
        calls.append('inputs')
        result = {'src/neo4j_graph_loader/__init__.py': 'b'*64 if change == 'inputs' and calls.count('inputs') > 1 else 'a'*64}
        if change == 'extra_source':
            result['src/neo4j_graph_loader/extra.py'] = 'e'*64
        return result
    monkeypatch.setattr(build_candidate_images, 'package_version',
                        lambda: 'different' if change == 'version' else '0.1.0a1.dev0')
    monkeypatch.setattr(build_candidate_images, 'build_inputs', inputs)
    monkeypatch.setattr(installed_service, 'validate_images', lambda path, **kwargs: {'cli': {'id': IMAGE}})
    if change == 'none':
        result = verify_installed.preflight_service_images(tmp_path/'manifest.json',
                                                           version='0.1.0a1.dev0', payload={'neo4j_graph_loader/__init__.py': 'a'*64},
                                                           client=SimpleNamespace(info=info))
        assert result['daemon_id'] == 'first' and result['images']['cli']['id'] == IMAGE
    else:
        with pytest.raises(ValueError):
            verify_installed.preflight_service_images(tmp_path/'manifest.json',
                                                      version='0.1.0a1.dev0',
                                                      payload={'neo4j_graph_loader/__init__.py': 'c'*64 if change == 'payload' else 'a'*64},
                                                      client=SimpleNamespace(info=info))


@pytest.mark.parametrize('url', ['tcp://192.168.1.75:2375', 'ssh://host', 'unix://host/socket',
                                 'unix:///tmp/../socket', 'unix:///tmp/sock?x=1', 'unix:///tmp/%2esock'])
def test_remote_or_ambiguous_docker_hosts_are_rejected(url):
    with pytest.raises(ValueError):
        local_docker_host(url)


def test_explicit_unix_socket_is_accepted():
    assert local_docker_host('unix:///tmp/owned.sock') == 'unix:///tmp/owned.sock'


def manifest(tmp_path):
    rows, images = [], {}
    for index, role in enumerate(('cli', 'node', 'edge', 'clock')):
        identity = 'sha256:' + str(index + 1) * 64
        tag = f'ghcr.io/shashankshukla96/graph-loader-{role}:0.1.0a1.dev0-test'
        labels = {'org.opencontainers.image.version': '0.1.0a1.dev0',
                  'io.graph-loader.component': role, 'io.graph-loader.image-contract': '1'}
        rows.append({'component': role, 'id': identity, 'local_tag': tag, 'labels': labels,
                     'registry_digest': None, 'exit_code': 0, 'platform': 'linux/arm64'})
        images[tag] = SimpleNamespace(id=identity, attrs={'Id': identity, 'Config': {'Labels': labels.copy()},
                                                           'Os': 'linux', 'Architecture': 'arm64'})
    data = {'version': '0.1.0a1.dev0', 'published': False, 'input_sha256': {'source': 'hash'}, 'images': rows}
    path = tmp_path / 'images.json'
    path.write_text(json.dumps(data))
    return path, data, images


def image_client(images):
    return SimpleNamespace(images=SimpleNamespace(get=lambda tag: images[tag]))


def test_manifest_binds_all_four_actual_images(tmp_path):
    path, _, images = manifest(tmp_path)
    result = validate_images(path, version='0.1.0a1.dev0', inputs={'source': 'hash'}, client=image_client(images))
    assert set(result) == {'cli', 'node', 'edge', 'clock'}


@pytest.mark.parametrize('change', ['role', 'image', 'digest', 'source', 'tag', 'label', 'platform'])
def test_manifest_or_daemon_identity_disagreement_is_rejected(tmp_path, change):
    path, data, images = manifest(tmp_path)
    first = data['images'][0]
    if change == 'role':
        first['component'] = 'node'
    elif change == 'image':
        images[first['local_tag']].id = 'sha256:' + 'a' * 64
    elif change == 'digest':
        first['registry_digest'] = 'sha256:' + 'a' * 64
    elif change == 'source':
        data['input_sha256'] = {}
    elif change == 'tag':
        first['local_tag'] = 'other:latest'
    elif change == 'label':
        images[first['local_tag']].attrs['Config']['Labels']['io.graph-loader.component'] = 'node'
    else:
        first['platform'] = 'linux/amd64'
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError):
        validate_images(path, version='0.1.0a1.dev0', inputs={'source': 'hash'}, client=image_client(images))


def test_manifest_symlink_is_rejected(tmp_path):
    path, _, images = manifest(tmp_path)
    link = tmp_path / 'link.json'
    link.symlink_to(path)
    with pytest.raises(ValueError):
        validate_images(link, version='0.1.0a1.dev0', inputs={'source': 'hash'}, client=image_client(images))


def test_fifo_evidence_is_rejected_without_waiting_for_a_writer(tmp_path):
    fifo = tmp_path / 'fifo'
    os.mkfifo(fifo)
    root = Path(__file__).resolve().parents[1]
    code = ('import sys; sys.path.insert(0, sys.argv[1]); '
            'from scripts.installed_service import _read_regular; _read_regular(sys.argv[2])')
    result = subprocess.run([sys.executable, '-I', '-c', code, str(root), str(fifo)],
                            capture_output=True, text=True, timeout=3)
    assert result.returncode != 0
    assert 'regular file' in result.stderr
