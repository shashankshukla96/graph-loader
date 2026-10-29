"""Finite dependency inputs for the checkout-only installed regression runner."""
from __future__ import annotations

from email.parser import BytesParser
import hashlib
import io
import json
from pathlib import Path
import re
import stat
import zipfile


def normalized(name):
    return re.sub(r'[-_.]+', '-', name).lower()


def regular_path(path):
    path = Path(path).absolute()
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError('dependency inputs must not use symlinks')
    return path


def bounded_file(path, maximum):
    path = regular_path(path)
    if not stat.S_ISREG(path.stat().st_mode) or path.stat().st_size > maximum:
        raise ValueError('dependency input must be a bounded regular file')
    with path.open('rb') as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise ValueError('dependency input exceeds finite bound')
    return raw


def read_constraints(path):
    raw = bounded_file(path, 65536)
    pins = {}
    for line in raw.decode('ascii').splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        match = re.fullmatch(r'([A-Za-z0-9][A-Za-z0-9._-]*)==([0-9][A-Za-z0-9.!+_-]*)', line)
        if match is None:
            raise ValueError('constraints require exact name==version pins; no directives, paths or URLs')
        name, version = match.groups()
        name = normalized(name)
        if name in pins:
            raise ValueError('duplicate normalized constraint: ' + name)
        pins[name] = version
    if not pins:
        raise ValueError('constraints must contain exact pins')
    return {'sha256': hashlib.sha256(raw).hexdigest(), 'pins': pins, 'content': raw}


def read_wheelhouse(directory, manifest, pins):
    directory = regular_path(directory)
    raw = bounded_file(manifest, 262144)
    data = json.loads(raw)
    files = data.get('files')
    if data.get('schema_version') != 1 or not isinstance(files, dict) or not 0 < len(files) <= 256:
        raise ValueError('wheelhouse needs a finite version1 hash manifest')
    if set(p.name for p in directory.iterdir()) != set(files):
        raise ValueError('wheelhouse members differ from explicit manifest')
    contents = {}
    for name, digest in files.items():
        if (not isinstance(name, str) or Path(name).name != name or '/' in name or '\\' in name
                or not name.endswith('.whl') or not isinstance(digest, str)
                or re.fullmatch('[a-f0-9]{64}', digest) is None):
            raise ValueError('unsafe wheelhouse member or digest')
        path = regular_path(directory/name)
        if not path.is_file() or path.stat().st_size > 512 * 1024 * 1024:
            raise ValueError('wheelhouse member must be a bounded regular wheel')
        content = bounded_file(path, 512 * 1024 * 1024)
        if hashlib.sha256(content).hexdigest() != digest:
            raise ValueError('wheelhouse hash differs: ' + name)
        with zipfile.ZipFile(io.BytesIO(content)) as wheel:
            metadata = [n for n in wheel.namelist() if n.endswith('.dist-info/METADATA')]
            if len(metadata) != 1:
                raise ValueError('wheelhouse member needs one distribution identity')
            identity = BytesParser().parsebytes(wheel.read(metadata[0]))
            package = identity.get('Name')
            version = identity.get('Version')
            if not package or not version or pins.get(normalized(package)) != version:
                raise ValueError('wheelhouse identity is not exactly constrained: ' + name)
        contents[name] = content
    return {'manifest_sha256': hashlib.sha256(raw).hexdigest(), 'files': files, 'contents': contents}


def verify_versions(versions, pins, application_version):
    """Tool installation must not upgrade any runtime or introduce an unpinned one."""
    actual = {normalized(name): version for name, version in versions.items()}
    if actual.get('neo4j-graph-loader') != application_version:
        raise ValueError('installed application version differs from explicit wheel')
    for name, version in actual.items():
        if name == 'neo4j-graph-loader':
            continue
        if name in {'pip', 'setuptools'} and name not in pins:
            continue  # venv bootstrap tools, never application/test resolution inputs.
        if pins.get(name) != version:
            raise ValueError('installed distribution is missing/different from constraints: ' + name)
    return actual
