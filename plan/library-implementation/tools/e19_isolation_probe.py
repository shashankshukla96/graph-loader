#!/usr/bin/env python3
"""E19.F4.S10 isolation probe (private, stdlib only): one denied-public and one allowed-internal set of checks, run unchanged in every place.

    e19_isolation_probe.py --place NAME [--public denied|reached] [--internal-http registry,pypi,debian] [--internal-tcp host:port,...]
                           [--secrets-dir DIR | --pypi-url-file FILE] [--ca FILE] [--apt-public] [--apt-mirror URL] [--pip-public] [--pip-index URL-FILE]

Every check prints one line ``PROBE {json}``. A public check records the DNS answer, a TCP connect to each answer and an HTTP request, and
classifies each failure: ``network`` (unreachable, no route, refused, timeout), ``dns``, ``tls``, ``http`` (a status came back: the host was
reached), ``other``. A denial counts only when DNS answered and every transport attempt failed with ``network``: a DNS failure alone, an
authentication error or a missing resource is not a denial.
"""
from __future__ import annotations

import argparse
import base64
import errno
import json
import os
import socket
import ssl
import subprocess
import sys
import urllib.error
import urllib.request
from urllib.parse import urlsplit

NETWORK_ERRNOS = {errno.ENETUNREACH, errno.EHOSTUNREACH, errno.ECONNREFUSED, errno.ETIMEDOUT, errno.ENETDOWN, errno.EHOSTDOWN}
PUBLIC_TCP = [('1.1.1.1', 443), ('8.8.8.8', 443)]
PUBLIC_NAMES = [('docker.io registry', 'registry-1.docker.io', 443, 'https', '/v2/'), ('ghcr.io', 'ghcr.io', 443, 'https', '/v2/'),
                ('pypi.org', 'pypi.org', 443, 'https', '/simple/'), ('deb.debian.org', 'deb.debian.org', 80, 'http', '/debian/dists/trixie/InRelease')]
APT_NETWORK = ('Network is unreachable', 'Could not connect', 'onnection timed out', 'Connection refused', 'No route to host', 'Unable to connect')
APT_DNS = ('Temporary failure resolving', 'Could not resolve', 'Name or service not known')


def classify(exc: BaseException) -> str:
    if isinstance(exc, urllib.error.HTTPError):
        return 'http'
    if isinstance(exc, urllib.error.URLError) and isinstance(exc.reason, BaseException):
        return classify(exc.reason)
    if isinstance(exc, socket.gaierror):
        return 'dns'
    if isinstance(exc, (socket.timeout, TimeoutError)):
        return 'network'
    if isinstance(exc, ssl.SSLError):
        return 'tls'
    if isinstance(exc, OSError) and exc.errno in NETWORK_ERRNOS:
        return 'network'
    return 'other'


def errno_name(exc: BaseException) -> str:
    inner = exc.reason if isinstance(exc, urllib.error.URLError) and isinstance(exc.reason, BaseException) else exc
    if isinstance(inner, OSError) and inner.errno:
        return errno.errorcode.get(inner.errno, str(inner.errno))
    return type(inner).__name__


def emit(place: str, kind: str, name: str, ok: bool, **detail) -> bool:
    print('PROBE ' + json.dumps({'place': place, 'kind': kind, 'name': name, 'ok': ok, **detail}, sort_keys=True), flush=True)
    return ok


def tcp(host: str, port: int) -> dict:
    try:
        with socket.create_connection((host, port), timeout=5):
            return {'to': f'{host}:{port}', 'class': 'connected'}
    except OSError as exc:
        return {'to': f'{host}:{port}', 'class': classify(exc), 'error': errno_name(exc)}


def http(url: str, headers=None, context=None) -> dict:
    request = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=8, context=context) as response:
            return {'class': 'reached', 'status': response.status}
    except urllib.error.HTTPError as exc:
        return {'class': 'reached', 'status': exc.code}
    except Exception as exc:                                                    # noqa: BLE001  every failure is classified, none is hidden
        return {'class': classify(exc), 'error': errno_name(exc)}


def public_checks(place: str, expect: str) -> bool:
    ok = True
    for host, port in PUBLIC_TCP:
        result = tcp(host, port)
        good = result['class'] == ('network' if expect == 'denied' else 'connected')
        ok &= emit(place, f'public-{expect}', f'tcp {host}:{port}', good, **result)
    for label, host, port, scheme, path in PUBLIC_NAMES:
        try:
            answers = sorted({info[4][0] for info in socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)})
            dns = {'answered': True, 'answers': answers}
        except socket.gaierror as exc:
            answers, dns = [], {'answered': False, 'error': errno_name(exc)}
        connects = [tcp(ip, port) for ip in answers[:2]]
        fetched = http(f'{scheme}://{host}{path}')
        if expect == 'denied':
            good = bool(answers) and all(c['class'] == 'network' for c in connects) and fetched['class'] == 'network'
        else:
            good = bool(answers) and all(c['class'] == 'connected' for c in connects) and fetched['class'] == 'reached'
        ok &= emit(place, f'public-{expect}', label, good, dns=dns, connects=connects, request=fetched)
    return ok


def credentials(args) -> dict:
    found = {}
    if args.secrets_dir:
        users = json.load(open(os.path.join(args.secrets_dir, 'users.json')))
        for key in ('registry', 'pypi'):
            found[key] = (users[key], open(os.path.join(args.secrets_dir, f'{key}.password')).read())
    elif args.pypi_url_file:
        parts = urlsplit(open(args.pypi_url_file).read().strip())
        found['pypi'] = (parts.username, parts.password)
    return found


def basic(user_password) -> dict:
    return {'Authorization': 'Basic ' + base64.b64encode(f'{user_password[0]}:{user_password[1]}'.encode()).decode()}


def internal_checks(place: str, args) -> bool:
    ok = True
    creds = credentials(args)
    context = ssl.create_default_context(cafile=args.ca) if args.ca else None
    sfx = f'.{args.internal_domain}' if args.internal_domain else ''
    urls = {'registry': (f'https://registry{sfx}:5000/v2/', True), 'pypi': (f'https://pypi{sfx}:8443/simple/', True),
            'debian': (f'http://debian{sfx}/debian/dists/trixie/InRelease', False)}
    for key in filter(None, (args.internal_http or '').split(',')):
        url, needs_auth = urls[key]
        headers = basic(creds[key]) if needs_auth and key in creds else {}
        result = http(url, headers, context if url.startswith('https') else None)
        good = result['class'] == 'reached' and result.get('status') == 200
        ok &= emit(place, 'internal-allowed', f'http {key}', good, url=url, authenticated=bool(headers), **result)
    for item in filter(None, (args.internal_tcp or '').split(',')):
        host, port = item.rsplit(':', 1)
        result = tcp(host, int(port))
        ok &= emit(place, 'internal-allowed', f'tcp {item}', result['class'] == 'connected', **result)
    return ok


def run_apt(place: str, kind: str, name: str, command: list[str], expect: str) -> bool:
    done = subprocess.run(command, capture_output=True, text=True, timeout=180)
    text = done.stdout + done.stderr
    failed = done.returncode != 0 or 'Failed to fetch' in text or any(line.startswith('Err:') for line in text.splitlines())
    if not failed:                  # apt-get update exits 0 when a source cannot be fetched: the warnings are the result
        cls = 'reached'
    elif any(m in text for m in APT_NETWORK):
        cls = 'network'
    elif any(m in text for m in APT_DNS):
        cls = 'dns'
    else:
        cls = 'other'
    lines = [line for line in text.splitlines() if line.startswith(('Err:', 'Ign:', 'E:', 'W:', 'Get:', 'Hit:')) or 'IP:' in line][:6]
    return emit(place, kind, name, cls == expect, **{'class': cls, 'exit': done.returncode, 'lines': lines})


def apt_sources(mirror: str) -> list[str]:
    os.makedirs('/tmp/e19-sources', exist_ok=True)
    os.makedirs('/tmp/e19-lists/partial', exist_ok=True)
    codename = dict(line.split('=', 1) for line in open('/etc/os-release').read().splitlines() if '=' in line)['VERSION_CODENAME'].strip('"')
    with open('/tmp/e19-sources/debian.sources', 'w') as handle:
        handle.write(f'Types: deb\nURIs: {mirror}\nSuites: {codename} {codename}-updates\nComponents: main\nSigned-By: /usr/share/keyrings/debian-archive-keyring.gpg\n')
    return ['-o', 'Dir::Etc::sourcelist=/dev/null', '-o', 'Dir::Etc::sourceparts=/tmp/e19-sources', '-o', 'Dir::State::lists=/tmp/e19-lists']


def pip_check(place: str, args) -> bool:
    ok = True
    base = ['python', '-m', 'pip', 'download', '--no-deps', '--disable-pip-version-check', '--no-cache-dir', '--retries', '1', '--timeout', '6', '-d', '/tmp/e19-pip']
    env = {**os.environ, 'PIP_NO_INPUT': '1'}
    if args.pip_public:
        done = subprocess.run([*base, '--index-url', 'https://pypi.org/simple/', 'six'], capture_output=True, text=True, env=env, timeout=120)
        text = done.stdout + done.stderr
        if done.returncode == 0:
            cls = 'reached'
        elif any(m in text for m in ('Network is unreachable', 'Connection refused', 'timed out', 'No route to host', 'NewConnectionError', 'Max retries exceeded')):
            cls = 'network'
        elif any(m in text for m in ('Temporary failure in name resolution', 'Name or service not known')):
            cls = 'dns'
        else:
            cls = 'other'
        ok &= emit(place, 'public-denied', 'pip pypi.org', cls == 'network', **{'class': cls, 'exit': done.returncode, 'evidence': [line.strip()[:220] for line in text.splitlines() if 'unreachable' in line or 'timed out' in line or 'refused' in line][:2]})
    if args.pip_index:
        url = open(args.pip_index).read().strip()
        done = subprocess.run([*base, '--index-url', url, '--cert', args.ca, 'annotated-types'], capture_output=True, text=True, env=env, timeout=120)
        ok &= emit(place, 'internal-allowed', 'pip owned index', done.returncode == 0, exit=done.returncode,
                   tail=('downloaded' if done.returncode == 0 else (done.stdout + done.stderr).strip().splitlines()[-1][:200]))
    return ok


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--place', required=True)
    parser.add_argument('--public', choices=('denied', 'reached'))
    parser.add_argument('--internal-http')
    parser.add_argument('--internal-domain', help='suffix for the owned service names (single-label names do not resolve in an inner user-defined network)')
    parser.add_argument('--internal-tcp')
    parser.add_argument('--secrets-dir')
    parser.add_argument('--pypi-url-file')
    parser.add_argument('--ca')
    parser.add_argument('--apt-public', action='store_true')
    parser.add_argument('--apt-mirror')
    parser.add_argument('--pip-public', action='store_true')
    parser.add_argument('--pip-index')
    args = parser.parse_args(argv)
    ok = True
    if args.public:
        ok &= public_checks(args.place, args.public)
    if args.internal_http or args.internal_tcp:
        ok &= internal_checks(args.place, args)
    if args.apt_public:
        ok &= run_apt(args.place, 'public-denied', 'apt deb.debian.org', ['apt-get', 'update', '-o', 'Acquire::Retries=0', '-o', 'Acquire::http::Timeout=6'], 'network')
    if args.apt_mirror:
        ok &= run_apt(args.place, 'internal-allowed', 'apt owned mirror',
                      ['apt-get', 'update', '-o', 'Acquire::Retries=0', '-o', 'Acquire::http::Timeout=10', *apt_sources(args.apt_mirror)], 'reached')
    if args.pip_public or args.pip_index:
        ok &= pip_check(args.place, args)
    print('PROBE-SUMMARY ' + json.dumps({'place': args.place, 'ok': bool(ok)}), flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
