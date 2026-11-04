"""Owned local servers for the E19 build proofs: a PEP 503 index (https, optional basic auth) and a logging Debian mirror proxy.

Every request is logged as a JSON line (time, method, path, whether authentication was presented and whether it was right); credentials are
never logged. Used only by the private proof harnesses.
"""
from __future__ import annotations

import base64
import json
import re
import ssl
import subprocess
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def make_ca_and_cert(directory: Path, names: list[str]) -> tuple[Path, Path, Path]:
    """A throwaway CA and a server certificate for ``names``; returns (ca.pem, server.pem, server.key)."""
    directory.mkdir(parents=True, exist_ok=True)
    ca_key, ca_pem, key, csr, pem, ext = (directory / n for n in ('ca.key', 'ca.pem', 'server.key', 'server.csr', 'server.pem', 'ext.cnf'))
    run = lambda *a: subprocess.run(a, check=True, capture_output=True)       # noqa: E731
    run('openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', str(ca_key), '-out', str(ca_pem), '-subj', '/CN=e19 throwaway CA', '-days', '2')
    run('openssl', 'req', '-newkey', 'rsa:2048', '-nodes', '-keyout', str(key), '-out', str(csr), '-subj', '/CN=e19 index')
    ext.write_text('subjectAltName=' + ','.join(f'DNS:{n}' for n in names) + '\nbasicConstraints=CA:FALSE\n')
    run('openssl', 'x509', '-req', '-in', str(csr), '-CA', str(ca_pem), '-CAkey', str(ca_key), '-CAcreateserial', '-out', str(pem), '-days', '2', '-extfile', str(ext))
    return ca_pem, pem, key


class _Base(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def record(self, **extra):
        entry = {'t': round(time.time(), 3), 'method': self.command, 'path': self.path.split('?')[0], **extra}
        with self.server.lock:
            self.server.entries.append(entry)
        if self.server.log_file:
            with open(self.server.log_file, 'a') as handle:
                handle.write(json.dumps(entry) + '\n')


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, handler, log_file=None):
        super().__init__(address, handler)
        self.entries, self.lock, self.log_file = [], threading.Lock(), log_file
        if log_file and Path(log_file).exists():
            Path(log_file).unlink()          # one run per log

    def start(self):
        threading.Thread(target=self.serve_forever, daemon=True).start()
        return self

    def stop(self):
        self.shutdown()
        self.server_close()

    @property
    def port(self):
        return self.server_address[1]


class _Index(_Base):
    def do_GET(self):
        server = self.server
        presented = self.headers.get('Authorization')
        ok = True
        if server.credentials is not None:
            expected = 'Basic ' + base64.b64encode(server.credentials.encode()).decode()
            ok = presented == expected
        self.record(auth='none' if presented is None else ('right' if ok else 'wrong'))
        if not ok:
            self.send_response(401)
            self.send_header('WWW-Authenticate', 'Basic realm="e19"')
            self.end_headers()
            return
        path = self.path.split('?')[0]
        if path in ('/simple/', '/simple'):
            names = sorted({server.normalize(f.name.split('-')[0]) for f in server.files})
            return self.page(''.join(f'<a href="/simple/{n}/">{n}</a>\n' for n in names))
        match = re.fullmatch(r'/simple/([^/]+)/', path)
        if match:
            wanted = match.group(1)
            links = ''.join(f'<a href="/files/{f.name}#sha256={server.digest(f)}">{f.name}</a>\n' for f in server.files
                            if server.normalize(f.name.split('-')[0]) == wanted)
            return self.page(links) if links else self.send_error(404)
        match = re.fullmatch(r'/files/([^/]+)', path)
        if match and (server.directory / match.group(1)).is_file():
            data = (server.directory / match.group(1)).read_bytes()
            self.send_response(200)
            self.send_header('Content-Type', 'application/octet-stream')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        self.send_error(404)

    def page(self, body):
        data = f'<!DOCTYPE html><html><body>{body}</body></html>'.encode()
        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def index_server(directory: Path, *, tls: tuple[Path, Path] | None = None, credentials: str | None = None, log_file: Path | None = None,
                 bind: str = '127.0.0.1') -> _Server:
    import hashlib
    server = _Server((bind, 0), _Index, str(log_file) if log_file else None)
    server.directory, server.credentials = Path(directory), credentials
    server.files = sorted(p for p in server.directory.iterdir() if p.name.endswith('.whl'))
    server.normalize = lambda n: re.sub(r'[-_.]+', '-', n).lower()
    cache = {}
    server.digest = lambda f: cache.setdefault(f, hashlib.sha256(f.read_bytes()).hexdigest())
    if tls:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(str(tls[0]), str(tls[1]))
        server.socket = context.wrap_socket(server.socket, server_side=True)
    return server.start()


class _Mirror(_Base):
    def do_GET(self):
        self.record(upstream=self.server.upstream)
        try:
            with urllib.request.urlopen(urllib.request.Request(self.server.upstream + self.path, headers={'User-Agent': 'e19-mirror'}), timeout=60) as reply:
                data = reply.read()
                self.send_response(reply.status)
                for key in ('Content-Type', 'Last-Modified'):
                    if reply.headers.get(key):
                        self.send_header(key, reply.headers[key])
        except urllib.error.HTTPError as exc:
            self.send_error(exc.code)
            return
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def mirror_server(upstream: str = 'https://deb.debian.org', *, log_file: Path | None = None, bind: str = '127.0.0.1') -> _Server:
    server = _Server((bind, 0), _Mirror, str(log_file) if log_file else None)
    server.upstream = upstream
    return server.start()


def logging_sink(log_file: Path | None = None, bind: str = '127.0.0.1') -> _Server:
    """A server that answers 404 to everything and logs it: the planted fallback index or proxy that must never be contacted."""
    class Sink(_Base):
        def do_GET(self):
            self.record(auth='n/a')
            self.send_error(404)
        do_CONNECT = do_POST = do_GET
    return _Server((bind, 0), Sink, str(log_file) if log_file else None).start()
