"""Hashed per-platform dependency locks and the check of a wheelhouse against one. Standard library only.

A lock is a pip requirements file (``name==version`` with ``--hash=sha256:...`` lines, usable with ``pip install --require-hashes``) preceded by a
comment header. The header names the platform, the Python version, the base image and the sha256 of the requirements it was resolved from, and
``content-sha256`` binds the body, so a hand edit of a pin or a hash is detected without a network.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import re

PLATFORMS = {'linux/amd64': 'x86_64', 'linux/arm64': 'aarch64'}
PYTHON = '3.12'
HEADER_KEYS = ('platform', 'python', 'base-image', 'requirements-sha256', 'generator', 'content-sha256')
_ENTRY = re.compile(r'(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)==(?P<version>[A-Za-z0-9][A-Za-z0-9.!+_-]*)')
_HASH = re.compile(r'--hash=sha256:(?P<digest>[0-9a-f]{64})')
_WHEEL = re.compile(r'(?P<name>[^-]+)-(?P<version>[^-]+)(?:-(?P<build>[0-9][^-]*))?-(?P<python>[^-]+)-(?P<abi>[^-]+)-(?P<platform>[^-]+)\.whl')


class LockError(ValueError):
    """A lock file or a wheelhouse is not what the lock says."""


def canonical_name(name: str) -> str:
    return re.sub(r'[-_.]+', '-', name).lower()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


@dataclass(frozen=True)
class Entry:
    name: str
    version: str
    hashes: tuple[str, ...]


@dataclass(frozen=True)
class Lock:
    header: dict
    entries: tuple[Entry, ...]
    body: str

    @property
    def platform(self) -> str:
        return self.header['platform']

    @property
    def machine(self) -> str:
        return PLATFORMS[self.platform]

    def versions(self) -> dict:
        return {entry.name: entry.version for entry in self.entries}


def render_body(entries) -> str:
    """The canonical body: entries sorted by canonical name, hashes sorted, pip-style continuation lines."""
    lines = []
    for entry in sorted(entries, key=lambda e: e.name):
        lines.append(f'{entry.name}=={entry.version} \\')
        lines.extend(f'    --hash=sha256:{digest}' + (' \\' if index < len(entry.hashes) - 1 else '') for index, digest in enumerate(sorted(entry.hashes)))
    return '\n'.join(lines) + '\n'


def render(header: dict, entries) -> str:
    """A complete lock file; ``content-sha256`` is computed here."""
    body = render_body(entries)
    full = {**header, 'content-sha256': sha256_bytes(body.encode())}
    missing = [key for key in HEADER_KEYS if key not in full]
    if missing:
        raise LockError('lock header lacks ' + ', '.join(missing))
    return ('# Hashed dependency lock for the loader images (generated; do not edit).\n'
            '# Regenerate: make lock    Check: make check-lock\n'
            + ''.join(f'# {key}: {full[key]}\n' for key in HEADER_KEYS) + '\n' + body)


def parse_lock(text: str) -> Lock:
    """Parse and fully validate a lock; raises ``LockError`` for any malformed, edited or ambiguous content."""
    header, body_lines, seen_body = {}, [], False
    for number, line in enumerate(text.splitlines(), 1):
        if not seen_body and (line.startswith('#') or not line.strip()):
            match = re.fullmatch(r'# ([a-z0-9-]+): (.+)', line)
            if match and match.group(1) in HEADER_KEYS:
                header[match.group(1)] = match.group(2)
            continue
        seen_body = True
        body_lines.append(line)
    missing = [key for key in HEADER_KEYS if key not in header]
    if missing:
        raise LockError('lock header lacks ' + ', '.join(missing))
    if header['platform'] not in PLATFORMS:
        raise LockError('lock platform must be one of ' + ', '.join(PLATFORMS))
    if header['python'] != PYTHON:
        raise LockError(f'lock is for CPython {header["python"]}, not {PYTHON}')
    body = '\n'.join(body_lines) + '\n'
    entries, current, hashes = [], None, []

    def close():
        if current is None:
            return
        if not hashes or len(set(hashes)) != len(hashes):
            raise LockError(f'lock entry {current[0]} needs one or more distinct sha256 hashes')
        entries.append(Entry(current[0], current[1], tuple(hashes)))

    for line in body_lines:
        stripped = line.strip().removesuffix('\\').strip()
        if not stripped:
            raise LockError('blank line inside the lock body')
        hash_match = _HASH.fullmatch(stripped)
        if hash_match and line.startswith('    '):
            if current is None:
                raise LockError('hash before any requirement')
            hashes.append(hash_match.group('digest'))
            continue
        entry = _ENTRY.fullmatch(stripped)
        if not entry or line != line.lstrip() or not line.endswith(' \\'):
            raise LockError('only exact pins with hashes are allowed in a lock: ' + stripped[:60])
        close()
        current, hashes = (entry.group('name'), entry.group('version')), []
    close()
    names = [entry.name for entry in entries]
    if not entries or len(set(map(canonical_name, names))) != len(names) or any(name != canonical_name(name) for name in names):
        raise LockError('lock entries must be non-empty, unique and in canonical form')
    if render_body(entries) != body:
        raise LockError('lock body is not in canonical order or form')
    if sha256_bytes(body.encode()) != header['content-sha256']:
        raise LockError('lock content digest does not match: the file was edited by hand; run make lock')
    return Lock(header, tuple(entries), body)


def wheel_fields(filename: str) -> dict:
    match = _WHEEL.fullmatch(filename)
    if not match:
        raise LockError('not a wheel file name: ' + filename[:80])
    return match.groupdict()


def wheel_fits(filename: str, machine: str) -> bool:
    """True when the wheel can run on CPython 3.12 on glibc Linux with this machine architecture (``x86_64`` or ``aarch64``)."""
    fields = wheel_fields(filename)
    pythons, abis, platforms = fields['python'].split('.'), fields['abi'].split('.'), fields['platform'].split('.')
    stable = 'abi3' in abis
    python_ok = any(p in ('py3', 'py312', 'cp312') or (stable and p.startswith('cp3') and p[3:].isdigit() and int(p[3:]) <= 12) for p in pythons)
    abi_ok = any(a in ('none', 'abi3', 'cp312') for a in abis)
    platform_ok = any(p == 'any' or (p.startswith(('manylinux', 'linux_')) and p.endswith('_' + machine)) for p in platforms)
    return python_ok and abi_ok and platform_ok


def verify_wheelhouse(directory, lock: Lock, *, package_wheel: str | None = None) -> dict:
    """Check a wheelhouse against a lock and return ``{file name: sha256}``; raise ``LockError`` otherwise.

    Every lock entry must be satisfied by exactly one wheel whose name and version match and whose sha256 is one of the entry's hashes; the
    wheel must also fit this lock's platform (a wheel built for another architecture is refused even if somebody added its hash). Nothing else
    may be in the directory except the one package wheel named by ``package_wheel``: no sdist, no other wheel, no symlink, no subdirectory.
    """
    root = Path(directory)
    if not root.is_dir():
        raise LockError('wheelhouse is not a directory')
    found = {}
    for path in sorted(root.iterdir()):
        if path.is_symlink() or not path.is_file():
            raise LockError('wheelhouse may hold plain files only: ' + path.name)
        found[path.name] = path
    known = {}
    for entry in lock.entries:
        matches = []
        for name, path in found.items():
            if not name.endswith('.whl'):
                continue
            fields = wheel_fields(name)
            if canonical_name(fields['name']) == entry.name and fields['version'].lower() == entry.version.lower():
                matches.append(path)
        if not matches:
            raise LockError(f'wheelhouse is missing {entry.name}=={entry.version}')
        if len(matches) > 1:
            raise LockError(f'wheelhouse holds more than one wheel of {entry.name}=={entry.version}')
        path = matches[0]
        if not wheel_fits(path.name, lock.machine):
            raise LockError(f'{path.name} is not a CPython {PYTHON} wheel for {lock.platform}')
        digest = sha256_file(path)
        if digest not in entry.hashes:
            raise LockError(f'{path.name} does not match any hash in the lock')
        known[path.name] = digest
    extra = sorted(set(found) - set(known) - ({package_wheel} if package_wheel else set()))
    if extra:
        raise LockError('wheelhouse holds files the lock does not name: ' + ', '.join(extra[:3]))
    if package_wheel:
        if package_wheel not in found:
            raise LockError('wheelhouse is missing the package wheel ' + package_wheel)
        known[package_wheel] = sha256_file(found[package_wheel])
    return known
