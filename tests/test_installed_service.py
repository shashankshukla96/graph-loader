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
from scripts import installed_service, verify_installed
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


BINDING_DIGEST = 'a' * 64


def fake_binding(dockerfile_digest=BINDING_DIGEST, version='0.1.0a1.dev0', extra=False):
    """What ``wheel_binding`` returns for a wheel: its hash, version, the Dockerfile and lock hashes and the lock's artifact hashes."""
    dockerfiles = {name: dockerfile_digest for name in installed_service.DOCKERFILE_NAMES}
    if extra:
        dockerfiles['Dockerfile.extra'] = 'e' * 64
    return {'sha256': 'f' * 64, 'version': version, 'dockerfiles': dockerfiles, 'install_helper_sha256': 'd' * 64,
            'locks': {name: {'sha256': 'c' * 64, 'hashes': ['1' * 64, '2' * 64]} for name in sorted(installed_service.LOCK_NAMES)}}


def payload_for(binding):
    payload = {installed_service.IMAGE_BUILD + 'dockerfiles/' + n: d for n, d in binding['dockerfiles'].items() if n != 'Dockerfile.extra'}
    payload |= {installed_service.IMAGE_BUILD + 'locks/' + n: row['sha256'] for n, row in binding['locks'].items()}
    payload[installed_service.IMAGE_BUILD + 'install.py'] = binding['install_helper_sha256']
    return payload


@pytest.mark.parametrize('change', ['none', 'daemon', 'inputs', 'version', 'payload', 'extra_source'])
def test_service_image_preflight_rechecks_daemon_and_source(monkeypatch, tmp_path, change):
    """Name kept so the frozen test ID stays. The images are bound to the wheel under test: the daemon identity, the wheel's own version, the
    wheel's image build files against its frozen payload, and the wheel itself must be the same before and after the inspection."""
    calls = []

    def info():
        calls.append('daemon')
        return {'ID': 'second' if change == 'daemon' and calls.count('daemon') > 1 else 'first'}

    def binding(wheel):
        calls.append('binding')
        return fake_binding('b' * 64 if change == 'inputs' and calls.count('binding') > 1 else BINDING_DIGEST,
                            version='different' if change == 'version' else '0.1.0a1.dev0', extra=change == 'extra_source')

    payload = payload_for(fake_binding())
    if change == 'payload':
        payload[installed_service.IMAGE_BUILD + 'dockerfiles/Dockerfile'] = 'c' * 64
    monkeypatch.setattr(installed_service, 'wheel_binding', binding)
    monkeypatch.setattr(installed_service, 'validate_images', lambda path, **kwargs: {'cli': {'id': IMAGE}})
    arguments = dict(version='0.1.0a1.dev0', payload=payload, client=SimpleNamespace(info=info), wheel=tmp_path / 'w.whl')
    if change == 'none':
        result = verify_installed.preflight_service_images(tmp_path / 'manifest.json', **arguments)
        assert result['daemon_id'] == 'first' and result['images']['cli']['id'] == IMAGE and result['package_wheel_sha256'] == 'f' * 64
    else:
        with pytest.raises(ValueError):
            verify_installed.preflight_service_images(tmp_path / 'manifest.json', **arguments)


@pytest.mark.parametrize('url', ['tcp://192.0.2.75:2375', 'ssh://host', 'unix://host/socket',
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
        tag = f'ghcr.io/neburyx/graph-loader-{role}:0.1.0a1.dev0-test'
        labels = {'org.opencontainers.image.version': '0.1.0a1.dev0',
                  'io.graph-loader.component': role, 'io.graph-loader.image-contract': '1'}
        rows.append({'component': role, 'id': identity, 'local_tag': tag, 'labels': labels,
                     'registry_digest': None, 'exit_code': 0, 'platform': 'linux/arm64'})
        images[tag] = SimpleNamespace(id=identity, attrs={'Id': identity, 'Config': {'Labels': labels.copy()},
                                                           'Os': 'linux', 'Architecture': 'arm64'})
    binding = fake_binding()
    lock = sorted(binding['locks'])[0]
    data = {'schema': installed_service.MANIFEST_SCHEMA, 'version': '0.1.0a1.dev0', 'published': False, 'platform': 'linux/arm64',
            'package_wheel': {'file': 'neo4j_graph_loader-0.1.0a1.dev0-py3-none-any.whl', 'sha256': binding['sha256']},
            'recipe': {'version': 1, 'dockerfiles': dict(binding['dockerfiles']), 'install_helper_sha256': binding['install_helper_sha256']},
            'lock': {'file': lock, 'sha256': binding['locks'][lock]['sha256']}, 'dependency_sha256': list(binding['locks'][lock]['hashes']),
            'base_image': {'requested': 'python:3.12-slim', 'resolved': 'python@sha256:' + '9' * 64}, 'images': rows}
    path = tmp_path / 'images.json'
    path.write_text(json.dumps(data))
    return path, data, images


def image_client(images):
    return SimpleNamespace(images=SimpleNamespace(get=lambda tag: images[tag]))


def test_manifest_binds_all_four_actual_images(tmp_path):
    path, _, images = manifest(tmp_path)
    result = validate_images(path, version='0.1.0a1.dev0', wheel=fake_binding(), client=image_client(images))
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
        data['package_wheel']['sha256'] = 'e' * 64
    elif change == 'tag':
        first['local_tag'] = 'other:latest'
    elif change == 'label':
        images[first['local_tag']].attrs['Config']['Labels']['io.graph-loader.component'] = 'node'
    else:
        first['platform'] = 'linux/amd64'
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError):
        validate_images(path, version='0.1.0a1.dev0', wheel=fake_binding(), client=image_client(images))


@pytest.mark.parametrize('control', ['substituted_wheel_same_version', 'changed_dockerfile', 'changed_install_helper', 'changed_lock', 'wrong_dependency_hashes',
                                     'missing_dependency_hash', 'unresolved_base', 'old_manifest_schema', 'no_wheel_record'])
def test_control_the_manifest_must_match_the_wheel_it_claims_to_come_from(tmp_path, control):
    """Adversarial controls of the wheel-hash binding: each one is a manifest that no longer describes the wheel under test."""
    path, data, images = manifest(tmp_path)
    if control == 'substituted_wheel_same_version':
        data['package_wheel']['sha256'] = hashlib.sha256(b'another build of the same version').hexdigest()
    elif control == 'changed_dockerfile':
        data['recipe']['dockerfiles']['Dockerfile.node_loader'] = '0' * 64
    elif control == 'changed_install_helper':
        data['recipe']['install_helper_sha256'] = '0' * 64
    elif control == 'changed_lock':
        data['lock']['sha256'] = '0' * 64
    elif control == 'wrong_dependency_hashes':
        data['dependency_sha256'] = ['3' * 64, '2' * 64]
    elif control == 'missing_dependency_hash':
        data['dependency_sha256'] = data['dependency_sha256'][:1]
    elif control == 'unresolved_base':
        data['base_image']['resolved'] = 'python:3.12-slim'
    elif control == 'old_manifest_schema':
        data['schema'] = 'graph-loader-image-manifest/1'
    else:
        data.pop('package_wheel')
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError):
        validate_images(path, version='0.1.0a1.dev0', wheel=fake_binding(), client=image_client(images))


def test_wheel_binding_reads_the_build_files_from_the_wheel_and_refuses_a_wheel_with_other_files(tmp_path):
    import zipfile
    files = {installed_service.IMAGE_BUILD + 'dockerfiles/' + n: b'FROM x\n' + n.encode() for n in installed_service.DOCKERFILE_NAMES}
    files |= {installed_service.IMAGE_BUILD + 'locks/' + n: ('x==1 \\\n    --hash=sha256:' + (str(i + 1) * 64) + '\n').encode()
              for i, n in enumerate(sorted(installed_service.LOCK_NAMES))}
    files[installed_service.IMAGE_BUILD + 'install.py'] = b'print(1)\n'
    files['neo4j_graph_loader-9.9.dist-info/METADATA'] = b'Name: x\n'
    wheel = tmp_path / 'neo4j_graph_loader-9.9-py3-none-any.whl'
    with zipfile.ZipFile(wheel, 'w') as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    binding = installed_service.wheel_binding(wheel)
    assert binding['version'] == '9.9' and binding['sha256'] == hashlib.sha256(wheel.read_bytes()).hexdigest()
    assert set(binding['dockerfiles']) == installed_service.DOCKERFILE_NAMES and binding['install_helper_sha256'] == hashlib.sha256(b'print(1)\n').hexdigest()
    assert binding['locks']['py312-linux-amd64.txt']['hashes'] == ['1' * 64] and binding['locks']['py312-linux-arm64.txt']['hashes'] == ['2' * 64]
    for broken in ('missing lock', 'extra dockerfile', 'no helper'):
        other = tmp_path / (broken.replace(' ', '_') + '.whl')
        with zipfile.ZipFile(other, 'w') as archive:
            for name, content in files.items():
                if (broken == 'missing lock' and name.endswith('amd64.txt')) or (broken == 'no helper' and name.endswith('install.py')):
                    continue
                archive.writestr(name, content)
            if broken == 'extra dockerfile':
                archive.writestr(installed_service.IMAGE_BUILD + 'dockerfiles/Dockerfile.extra', b'FROM x')
        with pytest.raises(ValueError, match='exactly the image build files'):
            installed_service.wheel_binding(other)


def test_manifest_symlink_is_rejected(tmp_path):
    path, _, images = manifest(tmp_path)
    link = tmp_path / 'link.json'
    link.symlink_to(path)
    with pytest.raises(ValueError):
        validate_images(link, version='0.1.0a1.dev0', wheel=fake_binding(), client=image_client(images))


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
