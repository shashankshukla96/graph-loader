"""Identity checks for installed service lanes: a local Docker socket and the four candidate images."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
from urllib.parse import urlsplit
import zipfile


ROLES = frozenset({'cli', 'node', 'edge', 'clock'})
ID_RE = re.compile(r'sha256:[0-9a-f]{64}\Z')


def local_docker_host(value):
    """Reject network daemons, relative paths, encoded paths, and URL options."""
    if not isinstance(value, str):
        raise ValueError('explicit local Docker socket required')
    parsed = urlsplit(value)
    if (parsed.scheme != 'unix' or parsed.netloc or not parsed.path.startswith('/')
            or parsed.query or parsed.fragment or '%' in parsed.path
            or '\x00' in parsed.path or '..' in Path(parsed.path).parts):
        raise ValueError('explicit local Docker socket required')
    return value


def _read_regular(path, limit=1024 * 1024):
    path = Path(path)
    # Resolve no parent symlinks: an inspected path must be the path opened.
    absolute = Path(os.path.abspath(path))
    for part in (absolute, *absolute.parents):
        if part.is_symlink():
            raise ValueError('symlink in evidence path')
    fd = os.open(absolute, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
            raise ValueError('evidence must be a bounded regular file')
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            content = stream.read(limit + 1)
        if len(content) > limit:
            raise ValueError('evidence exceeds size limit')
        return content
    finally:
        os.close(fd)


IMAGE_BUILD = 'neo4j_graph_loader/image_build/'
DOCKERFILE_NAMES = frozenset({'Dockerfile', 'Dockerfile.node_loader', 'Dockerfile.edge_loader', 'Dockerfile.clock'})
LOCK_NAMES = frozenset({'py312-linux-amd64.txt', 'py312-linux-arm64.txt'})
MANIFEST_SCHEMA = 'graph-loader-image-manifest/2'
DIGEST_RE = re.compile(r'sha256:[0-9a-f]{64}\Z')


def _sha(content):
    return hashlib.sha256(content).hexdigest()


def wheel_binding(wheel):
    """What a wheel carries for image builds, read from the wheel itself: its own sha256, the sha256 of each Dockerfile and of the install helper,
    and of each dependency lock with the artifact hashes that lock names. The image manifest is bound to these, not to a checkout."""
    path = Path(wheel)
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        dockerfiles = {n[len(IMAGE_BUILD + 'dockerfiles/'):]: _sha(archive.read(n)) for n in names if n.startswith(IMAGE_BUILD + 'dockerfiles/')}
        locks = {n[len(IMAGE_BUILD + 'locks/'):]: archive.read(n) for n in names if n.startswith(IMAGE_BUILD + 'locks/')}
        helper = archive.read(IMAGE_BUILD + 'install.py') if IMAGE_BUILD + 'install.py' in names else b''
        dist = [n.split('/')[0] for n in names if n.endswith('.dist-info/METADATA')]
    if set(dockerfiles) != DOCKERFILE_NAMES or set(locks) != LOCK_NAMES or not helper or len(dist) != 1:
        raise ValueError('the wheel does not carry exactly the image build files')
    return {'sha256': _sha(path.read_bytes()), 'version': dist[0].removeprefix('neo4j_graph_loader-').removesuffix('.dist-info'), 'dockerfiles': dockerfiles,
            'install_helper_sha256': _sha(helper),
            'locks': {name: {'sha256': _sha(text), 'hashes': sorted(re.findall(r'--hash=sha256:([0-9a-f]{64})', text.decode('utf-8')))} for name, text in locks.items()}}


def validate_images(path, *, version, wheel, client):
    """Bind the four manifest candidates to the wheel under test (``wheel`` is ``wheel_binding``) and to locally inspected immutable images."""
    data = json.loads(_read_regular(path))
    if (not isinstance(data, dict) or data.get('schema') != MANIFEST_SCHEMA or data.get('version') != version
            or data.get('published') is not False):
        raise ValueError('image manifest schema, version or publication differs')
    package, recipe, lock = data.get('package_wheel'), data.get('recipe'), data.get('lock')
    if not isinstance(package, dict) or package.get('sha256') != wheel['sha256']:
        raise ValueError('the images were built from a different wheel than the one under test')
    if (not isinstance(recipe, dict) or recipe.get('dockerfiles') != wheel['dockerfiles']
            or recipe.get('install_helper_sha256') != wheel['install_helper_sha256']):
        raise ValueError('the image build recipe differs from the one in the wheel')
    if (not isinstance(lock, dict) or lock.get('file') not in wheel['locks'] or lock.get('sha256') != wheel['locks'][lock['file']]['sha256']
            or data.get('dependency_sha256') != wheel['locks'][lock['file']]['hashes']):
        raise ValueError('the image dependencies differ from the lock in the wheel')
    base = data.get('base_image')
    if not isinstance(base, dict) or not re.search(r'@sha256:[0-9a-f]{64}\Z', str(base.get('resolved'))):
        raise ValueError('the base image was not resolved to a digest')
    rows = data.get('images')
    if not isinstance(rows, list) or len(rows) != len(ROLES):
        raise ValueError('four image roles required')
    result = {}
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError('invalid image entry')
        role, identity, tag = row.get('component'), row.get('id'), row.get('local_tag')
        if (not isinstance(role, str) or role not in ROLES or role in result or not isinstance(identity, str)
                or not ID_RE.fullmatch(identity) or not isinstance(tag, str)
                or not tag.startswith(f'ghcr.io/neburyx/graph-loader-{role}:{version}-')
                or not re.fullmatch(r'[a-zA-Z0-9._:/-]+', tag)
                or row.get('registry_digest') is not None or row.get('exit_code') != 0):
            raise ValueError('invalid local candidate identity')
        labels = {'org.opencontainers.image.version': version,
                  'io.graph-loader.component': role, 'io.graph-loader.image-contract': '1'}
        if not isinstance(row.get('labels'), dict) or any(row['labels'].get(k) != v for k, v in labels.items()):
            raise ValueError('candidate manifest labels differ')
        image = client.images.get(tag)
        actual = image.attrs
        if (image.id != identity or actual.get('Id') != identity
                or any((actual.get('Config', {}).get('Labels') or {}).get(k) != v for k, v in labels.items())
                or f"{actual.get('Os')}/{actual.get('Architecture')}" != row.get('platform')):
            raise ValueError('candidate tag or inspected image differs')
        result[role] = {'id': identity, 'tag': tag, 'platform': row['platform']}
    if set(result) != ROLES or len({row['id'] for row in result.values()}) != len(ROLES):
        raise ValueError('candidate roles or image IDs are not unique')
    return result
