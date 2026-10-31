"""Identity checks for installed service lanes: a local Docker socket and the four candidate images."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import stat
from urllib.parse import urlsplit


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


def validate_images(path, *, version, inputs, client):
    """Bind the four manifest candidates to locally inspected immutable images."""
    data = json.loads(_read_regular(path))
    if (not isinstance(data, dict) or data.get('version') != version
            or data.get('published') is not False or data.get('input_sha256') != inputs):
        raise ValueError('image manifest version, publication, or source differs')
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
                or not tag.startswith(f'ghcr.io/shashankshukla96/graph-loader-{role}:{version}-')
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
