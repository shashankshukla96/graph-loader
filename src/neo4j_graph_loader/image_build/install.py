"""Install the loader package and its locked dependencies inside an image build, with pip started in a fresh environment.

Run by the image Dockerfiles as ``python -I install.py ...``; standard library only. The point of the helper is that the index URL, which may
carry credentials, never appears in any process's argument list, in an image layer or in a log, and that nothing but the intended source can
influence pip:

* pip starts with an environment holding only ``PATH``, ``PIP_CONFIG_FILE=/dev/null`` (pip then loads no global, site or user configuration
  file) and, for an index install, ``PIP_INDEX_URL`` (and ``PIP_CERT`` for a private CA). No inherited ``PIP_*`` or proxy variable survives.
* before installing, ``pip config list`` runs in that same environment and must show exactly the expected ``:env:`` keys and values.
* the URL is read from a BuildKit secret file (``--index-secret``) or given plainly (``--index-url``, for a mirror without credentials) and
  is redacted from everything pip prints. ``--wheelhouse`` installs offline from a directory with ``--no-index``.
* dependencies come from a hashed lock with ``--require-hashes --only-binary=:all: --no-deps``; the package wheel is checked against its
  expected sha256 and installed with its hash, never from an index.
"""
from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import threading
import time
from urllib.parse import urlsplit

CONFIG_FILE = os.devnull
PATH = '/usr/local/bin:/usr/bin:/bin'
EXIT_USAGE, EXIT_CONFIG, EXIT_WHEEL = 2, 3, 4


class InstallError(Exception):
    def __init__(self, message: str, code: int = EXIT_USAGE):
        super().__init__(message)
        self.code = code


def read_index_url(path: Path) -> str:
    """The index URL from a secret file: the file's only content, one line, an http(s) URL."""
    try:
        text = path.read_text(encoding='utf-8')
    except OSError:
        raise InstallError('the pip index secret is not available (pass it as a BuildKit secret)') from None
    return validate_url(text.strip(), 'the pip index secret')


def validate_url(url: str, what: str) -> str:
    if not url or any(c.isspace() for c in url):
        raise InstallError(f'{what} must hold exactly one URL and nothing else')
    parts = urlsplit(url)
    if parts.scheme not in ('http', 'https') or not parts.hostname:
        raise InstallError(f'{what} must be an http(s) URL')
    return url


def sanitized_environment(*, index_url: str | None = None, ca_bundle: str | None = None) -> dict:
    """Exactly the environment pip is started with."""
    env = {'PATH': PATH, 'PIP_CONFIG_FILE': CONFIG_FILE}
    if index_url is not None:
        env['PIP_INDEX_URL'] = index_url
    if ca_bundle:
        env['PIP_CERT'] = ca_bundle
    return env


def expected_config(env: dict) -> dict:
    """``pip config list`` keys (as pip prints them) and values the fresh environment must produce."""
    return {key[len('PIP_'):].lower().replace('_', '-'): value for key, value in env.items() if key.startswith('PIP_')}


def parse_config_list(text: str) -> dict:
    """``{key: value}`` of ``pip config list`` output; any line that is not an ``:env:`` entry raises ``InstallError`` (no value is echoed)."""
    found = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        match = re.fullmatch(r":env:\.([a-z0-9-]+)='(.*)'", line)
        if not match:
            raise InstallError('pip reads configuration from somewhere other than the prepared environment', EXIT_CONFIG)
        found[match.group(1)] = match.group(2)
    return found


def check_configuration(python: str, env: dict, expected: dict | None = None) -> None:
    """``pip config list`` in ``env`` must show exactly ``expected`` (default: what ``env`` itself prepares) and nothing from any file."""
    done = subprocess.run([python, '-I', '-m', 'pip', 'config', 'list'], env=env, capture_output=True, text=True, timeout=120)
    if done.returncode:
        raise InstallError('pip config list failed', EXIT_CONFIG)
    found = parse_config_list(done.stdout)
    expected = expected_config(env) if expected is None else expected
    if set(found) != set(expected):
        raise InstallError('pip sees configuration keys other than ' + ', '.join(sorted(expected)) + ': ' + ', '.join(sorted(set(found) ^ set(expected))),
                           EXIT_CONFIG)
    if any(found[key] != value for key, value in expected.items()):
        raise InstallError('a pip configuration value differs from the prepared environment', EXIT_CONFIG)


def secrets_of(url: str | None) -> list[str]:
    """Strings to keep out of logs and out of the process list: the URL, its network location and its credentials."""
    if not url:
        return []
    parts = urlsplit(url)
    pieces = {url, parts.netloc, parts.username or '', parts.password or ''}
    if parts.username is not None:
        pieces.add(parts.netloc.rsplit('@', 1)[0])
    return sorted((piece for piece in pieces if len(piece) >= 4), key=len, reverse=True)


def redact(text: str, secrets: list[str]) -> str:
    for secret in secrets:
        text = text.replace(secret, '***')
    return re.sub(r'(://)[^/@\s]*@', r'\1***@', text)


class CommandLineProbe:
    """While pip runs, scan every process's command line for the secrets; ``found`` lists the processes (by pid) that showed one."""

    def __init__(self, secrets: list[str]):
        self.secrets, self.found, self.scans = [s.encode() for s in secrets], [], 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self):
        if not Path('/proc/self/cmdline').exists():
            raise InstallError('--probe-cmdline needs /proc')
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join(timeout=5)

    def _run(self):
        while not self._stop.is_set():
            for entry in Path('/proc').iterdir():
                if entry.name.isdigit():
                    try:
                        command = (entry / 'cmdline').read_bytes()
                    except OSError:
                        continue
                    if any(secret in command for secret in self.secrets):
                        self.found.append(int(entry.name))
            self.scans += 1
            time.sleep(0.02)


def run_pip(python: str, args: list[str], env: dict, secrets: list[str], probe: bool) -> int:
    command = [python, '-I', '-m', 'pip', *args]
    if probe and secrets:
        with CommandLineProbe(secrets) as watcher:
            code = _stream(command, env, secrets)
        print(f'probe: scanned the process list {watcher.scans} times during the install')
        if watcher.found:
            raise InstallError('the index URL was visible in a process command line', EXIT_CONFIG)
        return code
    return _stream(command, env, secrets)


def _stream(command: list[str], env: dict, secrets: list[str]) -> int:
    process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors='replace')
    for line in process.stdout:
        print(redact(line.rstrip('\n'), secrets), flush=True)
    return process.wait()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


COMMON = ['--no-cache-dir', '--disable-pip-version-check', '--no-input', '--root-user-action=ignore']


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--lock', type=Path, required=True)
    parser.add_argument('--wheel', type=Path, required=True)
    parser.add_argument('--wheel-sha256', required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--wheelhouse', type=Path)
    source.add_argument('--index-url')
    source.add_argument('--index-secret', type=Path)
    parser.add_argument('--ca-bundle', type=Path, help='private CA bundle for an https index; ignored when the file is empty')
    parser.add_argument('--python', default=sys.executable)
    parser.add_argument('--target', type=Path, help='install into this directory (development checks)')
    parser.add_argument('--probe-cmdline', action='store_true', help='fail if the index URL is ever visible in a command line during the install')
    args = parser.parse_args(argv)
    try:
        if not re.fullmatch(r'[0-9a-f]{64}', args.wheel_sha256):
            raise InstallError('--wheel-sha256 must be the 64 hex characters of the package wheel digest')
        if not args.wheel.is_file() or not args.lock.is_file():
            raise InstallError('the package wheel and the lock must be files of the build context')
        if sha256_file(args.wheel) != args.wheel_sha256:
            raise InstallError('the package wheel does not match the expected sha256', EXIT_WHEEL)
        if args.wheelhouse is not None and not args.wheelhouse.is_dir():
            raise InstallError('the wheelhouse is not a directory')
        url = (validate_url(args.index_url, '--index-url') if args.index_url is not None
               else read_index_url(args.index_secret) if args.index_secret is not None else None)
        ca = str(args.ca_bundle.resolve()) if args.ca_bundle and args.ca_bundle.is_file() and args.ca_bundle.stat().st_size else None
        env = sanitized_environment(index_url=url, ca_bundle=ca)
        secrets = secrets_of(url)
        check_configuration(args.python, env)
        print('pip configuration check: ' + ', '.join(sorted(expected_config(env))))
        where = ['--no-index', '--find-links', str(args.wheelhouse)] if args.wheelhouse is not None else []
        target = ['--target', str(args.target)] if args.target else []
        code = run_pip(args.python, ['install', '--no-deps', '--require-hashes', '--only-binary=:all:', *COMMON, *where, *target,
                                     '-r', str(args.lock)], env, secrets, args.probe_cmdline)
        if code:
            return code
        with tempfile.TemporaryDirectory() as work:
            requirement = Path(work, 'package.txt')
            requirement.write_text(f'{args.wheel.resolve()} --hash=sha256:{args.wheel_sha256}\n', encoding='utf-8')
            code = run_pip(args.python, ['install', '--no-deps', '--no-index', '--require-hashes', *COMMON, *target, '-r', str(requirement)],
                           sanitized_environment(), [], False)
        if code or args.target:
            return code
        return run_pip(args.python, ['check', '--disable-pip-version-check'], sanitized_environment(), [], False)
    except InstallError as exc:
        print(f'install failed: {exc}', file=sys.stderr)
        return exc.code


if __name__ == '__main__':
    raise SystemExit(main())
