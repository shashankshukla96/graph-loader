"""Checkout-free resource and caller-schema proof; execute in an installed child."""
import contextlib
import errno
import hashlib
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading


def forbidden(*args, **kwargs):
    raise AssertionError('service access during schema command')


socket.socket.connect = socket.socket.connect_ex = socket.socket.bind = forbidden
subprocess.Popen = threading.Thread.start = forbidden
from neo4j_graph_loader import _resources, cli

@contextlib.contextmanager
def readonly_package():
    package = Path(_resources.__file__).parent
    paths = [package, *package.rglob('*')]
    modes = [(path, path.stat().st_mode & 0o777) for path in paths if not path.is_symlink()]
    changed = []
    try:
        for path, mode in modes:
            try:
                path.chmod(mode & ~0o222)
                changed.append((path, mode))
            except OSError as exc:
                if exc.errno != errno.EROFS:
                    raise
                break  # The immutable image mount itself enforces read-only access.
        try:
            with (package / 'forbidden-state').open('x'):
                pass
        except OSError as exc:
            assert exc.errno in (errno.EACCES, errno.EROFS), exc
        else:
            raise AssertionError('package writable')
        yield
    finally:
        for path, mode in changed:
            path.chmod(mode)



with readonly_package():
    records = []
    with tempfile.TemporaryDirectory(prefix='graph-loader-resource-proof-') as temporary:
        root = Path(temporary)
        for name in ('default', 'social'):
            with _resources.schema_path(sample=name) as resource:
                content = resource.read_bytes()
            stream = io.StringIO()
            with contextlib.redirect_stdout(stream):
                assert cli.main(['schema-example', '--name', name]) == 0
                assert stream.getvalue().encode() == content
                assert cli.main(['validate-schema', '--sample', name]) == 0
            external = root / f'{name}.yaml'
            external.write_bytes(content)
            with contextlib.redirect_stdout(io.StringIO()):
                assert cli.main(['validate-schema', '--config', str(external)]) == 0
            records.append({'sample': name, 'sha256': hashlib.sha256(content).hexdigest()})
        original = dict(os.environ)
        (root / '.env').write_text('GRAPH_LOADER_ENTRY_SENTINEL=private\n')
        previous = Path.cwd()
        root.chmod(0o555)
        try:
            os.chdir(root)
            with contextlib.redirect_stdout(io.StringIO()):
                assert cli.main(['validate-schema']) == 0
            assert dict(os.environ) == original
        finally:
            os.chdir(previous)
            root.chmod(0o700)
print(json.dumps({'resources': records, 'explicit_paths': True, 'no_env_loading': True}))
