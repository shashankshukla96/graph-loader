"""E19.F2.S5: the image install helper starts pip in a fresh environment and keeps the index URL out of argv, logs and layers.

These tests run the real helper with the interpreter's own pip against local wheels and a local index on 127.0.0.1 (which pip accepts over http);
the Dockerfile and BuildKit side is proved by the image build harness, not here.
"""
import base64
import hashlib
import http.server
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import zipfile

import pytest

from neo4j_graph_loader.image_build import install

SENTINEL = 'Zq7Xk2Sentinel'
USER = 'tester'


def sha(path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def credentialed(url, user, password):
    """``url`` with credentials in it, assembled at run time so no source line holds a credentialed URL."""
    scheme, rest = url.split('://', 1)
    return scheme + '://' + user + ':' + password + '@' + rest


def make_wheel(directory: Path, name: str, version: str = '1.0', body: str = 'VALUE = 1\n') -> Path:
    path = directory / f'{name}-{version}-py3-none-any.whl'
    info = f'{name}-{version}.dist-info'
    with zipfile.ZipFile(path, 'w') as archive:
        archive.writestr(f'{name}/__init__.py', body)
        archive.writestr(f'{info}/METADATA', f'Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n')
        archive.writestr(f'{info}/WHEEL', 'Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n')
        archive.writestr(f'{info}/RECORD', f'{name}/__init__.py,,\n{info}/METADATA,,\n{info}/WHEEL,,\n{info}/RECORD,,\n')
    return path


def lock_for(path: Path, *wheels) -> Path:
    lines = []
    for wheel in wheels:
        name, version = wheel.name.split('-')[:2]
        lines.append(f'{name}=={version} \\\n    --hash=sha256:{sha(wheel)}\n')
    path.write_text(''.join(lines))
    return path


@pytest.fixture
def parts(tmp_path):
    house = tmp_path / 'wheelhouse'
    house.mkdir()
    dep = make_wheel(house, 'depone')
    package = make_wheel(tmp_path, 'pkgmain', '2.0', 'import depone\n')
    return {'house': house, 'dep': dep, 'package': package, 'lock': lock_for(tmp_path / 'lock.txt', dep), 'target': tmp_path / 'site', 'tmp': tmp_path}


def run(parts, *extra, source=None, capsys=None):
    args = ['--lock', str(parts['lock']), '--wheel', str(parts['package']), '--wheel-sha256', sha(parts['package']), '--python', sys.executable,
            '--target', str(parts['target']), *(source or ['--wheelhouse', str(parts['house'])]), *extra]
    return install.main(args)


class Index:
    """A PEP 503 index on 127.0.0.1 with optional basic authentication; ``hits`` counts requests and ``auth`` what was presented."""

    def __init__(self, directory: Path, credentials: str | None = None):
        self.hits, self.auth, outer = 0, [], self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                outer.hits += 1
                presented = self.headers.get('Authorization')
                outer.auth.append('none' if presented is None else 'present')
                if credentials and presented != 'Basic ' + base64.b64encode(credentials.encode()).decode():
                    self.send_response(401)
                    self.send_header('WWW-Authenticate', 'Basic realm="t"')
                    self.end_headers()
                    return
                path = self.path.split('?')[0]
                if path.startswith('/files/'):
                    data = (directory / path[len('/files/'):]).read_bytes()
                    ctype = 'application/octet-stream'
                else:
                    name = path.strip('/').split('/')[-1]
                    links = ''.join(f'<a href="/files/{w.name}#sha256={sha(w)}">{w.name}</a>' for w in directory.glob('*.whl') if w.name.split('-')[0] == name)
                    data, ctype = f'<html><body>{links}</body></html>'.encode(), 'text/html'
                self.send_response(200)
                self.send_header('Content-Type', ctype)
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    @property
    def url(self):
        return f'http://127.0.0.1:{self.server.server_address[1]}/simple/'

    def with_credentials(self, user, password):
        return credentialed(self.url, user, password)

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def index(parts):
    server = Index(parts['house'], credentials=f'{USER}:{SENTINEL}')
    yield server
    server.close()


def installed(parts, name):
    return any(parts['target'].glob(f'{name}*'))


# -- the sanitized environment and pip's view of it ---------------------------------------------------------------------------------------
def test_the_environment_pip_gets_holds_only_the_prepared_keys():
    assert install.sanitized_environment() == {'PATH': install.PATH, 'PIP_CONFIG_FILE': os.devnull}
    assert install.sanitized_environment(index_url='https://i/simple/') == {'PATH': install.PATH, 'PIP_CONFIG_FILE': os.devnull, 'PIP_INDEX_URL': 'https://i/simple/'}
    with_ca = install.sanitized_environment(index_url='https://i/simple/', ca_bundle='/ctx/ca.pem')
    assert set(with_ca) == {'PATH', 'PIP_CONFIG_FILE', 'PIP_INDEX_URL', 'PIP_CERT'}
    assert install.expected_config(with_ca) == {'config-file': os.devnull, 'index-url': 'https://i/simple/', 'cert': '/ctx/ca.pem'}
    assert not any('proxy' in key.lower() or key in ('HOME', 'SSL_CERT_FILE', 'REQUESTS_CA_BUNDLE') for key in with_ca)


def test_pip_config_list_output_must_be_env_entries_only_and_no_value_is_echoed():
    assert install.parse_config_list(":env:.config-file='/dev/null'\n:env:.index-url='" + credentialed('https://h/simple/', 'u', 'p') + "'\n") == {
        'config-file': '/dev/null', 'index-url': credentialed('https://h/simple/', 'u', 'p')}
    for text in ("global.index-url='https://x:" + SENTINEL + "@h/simple/'\n", "user.extra-index-url='https://h/'\n", "site.cert='/x'\n", "garbage\n"):
        with pytest.raises(install.InstallError) as error:
            install.parse_config_list(":env:.config-file='/dev/null'\n" + text)
        assert SENTINEL not in str(error.value) and error.value.code == install.EXIT_CONFIG


def test_planted_user_and_site_style_configuration_is_ignored_and_planted_environment_is_detected(tmp_path, monkeypatch):
    for place in (tmp_path / '.config' / 'pip', tmp_path / '.pip', tmp_path / 'xdg' / 'pip'):
        place.mkdir(parents=True)
        (place / 'pip.conf').write_text('[global]\nextra-index-url = http://127.0.0.1:9/simple/\nindex-url = http://127.0.0.1:9/simple/\n')
    env = {**install.sanitized_environment(index_url='https://i.example/simple/'), 'HOME': str(tmp_path), 'XDG_CONFIG_HOME': str(tmp_path / 'xdg'),
           'XDG_CONFIG_DIRS': str(tmp_path / 'xdg')}
    install.check_configuration(sys.executable, env)                      # PIP_CONFIG_FILE=/dev/null: no file is read
    plain = subprocess.run([sys.executable, '-I', '-m', 'pip', 'config', 'list'], env={'PATH': install.PATH, 'HOME': str(tmp_path)}, capture_output=True, text=True)
    assert 'extra-index-url' in plain.stdout, 'control: without the prepared environment the planted file is read'
    for planted in ('PIP_EXTRA_INDEX_URL', 'PIP_FIND_LINKS', 'PIP_TRUSTED_HOST'):         # something the helper did not prepare reaches pip: refused
        with pytest.raises(install.InstallError, match='other than'):
            install.check_configuration(sys.executable, {**env, planted: 'http://127.0.0.1:9/'}, install.expected_config(env))


def test_the_configuration_check_refuses_a_value_that_differs_from_the_prepared_one(monkeypatch):
    env = install.sanitized_environment(index_url='https://i.example/simple/')
    monkeypatch.setattr(install, 'expected_config', lambda e: {'config-file': os.devnull, 'index-url': 'https://other/'})
    with pytest.raises(install.InstallError, match='differs'):
        install.check_configuration(sys.executable, env)


# -- reading and hiding the URL -----------------------------------------------------------------------------------------------------------
def test_the_secret_file_holds_one_http_url_and_nothing_else(tmp_path):
    good = tmp_path / 'good'
    good.write_text(credentialed('https://index.example/simple/', USER, SENTINEL) + '\n')
    assert install.read_index_url(good) == credentialed('https://index.example/simple/', USER, SENTINEL)
    cases = {'two lines': 'https://a/simple/\nhttps://b/\n', 'empty': '\n', 'spaces': 'https://a/ b\n', 'ftp': 'ftp://a/x\n', 'no host': 'https:///x\n', 'text': 'token\n'}
    for name, text in cases.items():
        path = tmp_path / name.replace(' ', '_')
        path.write_text(text)
        with pytest.raises(install.InstallError) as error:
            install.read_index_url(path)
        assert SENTINEL not in str(error.value)
    with pytest.raises(install.InstallError, match='not available'):
        install.read_index_url(tmp_path / 'absent')


def test_everything_that_could_expose_the_credentials_is_redacted():
    url = credentialed('https://index.example:8443/simple/', USER, SENTINEL)
    secrets = install.secrets_of(url)
    assert url in secrets and SENTINEL in secrets and f'{USER}:{SENTINEL}@index.example:8443' in secrets
    text = f'Looking in indexes: {url}\nGET {url}numpy/ failed for {USER}:{SENTINEL}\nplain ' + credentialed('https://host/x', 'u', 'other') + f' and {SENTINEL}'
    cleaned = install.redact(text, secrets)
    assert SENTINEL not in cleaned and 'other@' not in cleaned and 'index.example' not in cleaned.split('\n')[0]
    assert install.secrets_of(None) == [] and install.secrets_of('https://index.example/simple/') == ['https://index.example/simple/', 'index.example']
    assert install.redact('user ab@host', install.secrets_of(credentialed('https://h/', 'ab', 'cd'))) == 'user ab@host', 'short fragments are not blanked out of unrelated text'


@pytest.mark.skipif(not Path('/proc/self/cmdline').exists(), reason='needs /proc')
def test_the_probe_sees_a_secret_on_a_command_line_and_nothing_else():
    with install.CommandLineProbe([SENTINEL]) as watcher:
        child = subprocess.Popen(['sleep', '1', SENTINEL])
        child.wait()
    assert watcher.found and watcher.scans > 0
    with install.CommandLineProbe([SENTINEL]) as quiet:
        subprocess.run(['sleep', '0.3'], check=True)
    assert quiet.found == []


# -- the install itself ---------------------------------------------------------------------------------------------------------------------
def test_an_offline_wheelhouse_install_puts_the_dependency_and_the_package_in_place(parts, capsys):
    assert run(parts) == 0
    out = capsys.readouterr().out
    assert installed(parts, 'depone') and installed(parts, 'pkgmain')
    assert 'pip configuration check: config-file' in out


def test_an_index_secret_install_uses_only_that_index_and_never_prints_the_url(parts, index, tmp_path, capsys, monkeypatch):
    decoy = Index(parts['house'])
    try:
        for name, value in (('PIP_EXTRA_INDEX_URL', decoy.url), ('PIP_INDEX_URL', decoy.url), ('PIP_FIND_LINKS', decoy.url), ('HTTP_PROXY', 'http://127.0.0.1:9'),
                            ('HTTPS_PROXY', 'http://127.0.0.1:9'), ('ALL_PROXY', 'http://127.0.0.1:9')):
            monkeypatch.setenv(name, value)
        monkeypatch.setenv('HOME', str(tmp_path / 'home'))
        (tmp_path / 'home' / '.config' / 'pip').mkdir(parents=True)
        (tmp_path / 'home' / '.config' / 'pip' / 'pip.conf').write_text(f'[global]\nextra-index-url = {decoy.url}\n')
        secret = tmp_path / 'pip_index'
        secret.write_text(index.with_credentials(USER, SENTINEL) + '\n')
        assert run(parts, source=['--index-secret', str(secret)]) == 0
        out = capsys.readouterr().out
        assert installed(parts, 'depone') and installed(parts, 'pkgmain')
        assert decoy.hits == 0, 'the planted fallback index was contacted'
        assert index.hits > 0 and set(index.auth) <= {'none', 'present'} and 'present' in index.auth
        assert SENTINEL not in out and f'{USER}:' not in out and 'http://' + '***@127.0.0.1' in out        # the credentials, not the host, are hidden
        assert 'pip configuration check: config-file, index-url' in out
    finally:
        decoy.close()


def test_control_the_plain_url_form_also_installs_and_keeps_credentials_out_of_the_output(parts, index, capsys):
    assert run(parts, source=['--index-url', index.with_credentials(USER, SENTINEL)]) == 0
    assert SENTINEL not in capsys.readouterr().out and installed(parts, 'depone')


def test_control_wrong_credentials_fail_without_leaking_either_password(parts, index, tmp_path, capsys):
    secret = tmp_path / 'pip_index'
    secret.write_text(index.with_credentials(USER, 'wrongpassword1') + '\n')
    assert run(parts, source=['--index-secret', str(secret)]) != 0
    out = capsys.readouterr().out
    assert 'wrongpassword1' not in out and SENTINEL not in out and not installed(parts, 'pkgmain')


def test_control_a_substituted_package_wheel_is_refused_before_pip_runs(parts, capsys):
    parts['package'].write_bytes(parts['package'].read_bytes() + b'0')
    args = ['--lock', str(parts['lock']), '--wheel', str(parts['package']), '--wheel-sha256', '0' * 64, '--wheelhouse', str(parts['house']),
            '--python', sys.executable, '--target', str(parts['target'])]
    assert install.main(args) == install.EXIT_WHEEL
    assert 'does not match the expected sha256' in capsys.readouterr().err and not parts['target'].exists()


def test_control_a_tampered_lock_or_a_different_wheel_in_the_wheelhouse_stops_the_install(parts, capsys):
    other = make_wheel(parts['tmp'], 'depone', '1.0', 'VALUE = 2\n')                    # same name and version, other bytes
    parts['dep'].write_bytes(other.read_bytes())
    assert run(parts) != 0
    assert 'DO NOT MATCH THE HASHES' in capsys.readouterr().out and not installed(parts, 'pkgmain')


def test_control_an_unpinned_or_hashless_lock_is_refused_by_pip(parts, capsys):
    parts['lock'].write_text('depone>=1.0\n')
    assert run(parts) != 0
    assert not installed(parts, 'pkgmain')


@pytest.mark.parametrize('content', ['https://a/simple/\nhttps://b/simple/\n', 'not a url\n', ''])
def test_control_a_bad_index_secret_is_refused_with_the_usage_code(parts, tmp_path, content, capsys):
    secret = tmp_path / 'pip_index'
    secret.write_text(content)
    assert run(parts, source=['--index-secret', str(secret)]) == install.EXIT_USAGE
    assert not parts['target'].exists()


def test_control_a_missing_secret_is_refused_and_the_message_says_how_to_pass_it(parts, tmp_path, capsys):
    assert run(parts, source=['--index-secret', str(tmp_path / 'absent')]) == install.EXIT_USAGE
    assert 'BuildKit secret' in capsys.readouterr().err


def test_arguments_must_name_exactly_one_source_and_real_files(parts, capsys):
    with pytest.raises(SystemExit):
        install.main(['--lock', str(parts['lock']), '--wheel', str(parts['package']), '--wheel-sha256', sha(parts['package'])])
    with pytest.raises(SystemExit):
        install.main(['--lock', str(parts['lock']), '--wheel', str(parts['package']), '--wheel-sha256', sha(parts['package']), '--wheelhouse', '.',
                      '--index-url', 'https://x/'])
    assert install.main(['--lock', str(parts['lock']), '--wheel', 'missing.whl', '--wheel-sha256', 'a' * 64, '--wheelhouse', str(parts['house'])]) == install.EXIT_USAGE
    assert install.main(['--lock', str(parts['lock']), '--wheel', str(parts['package']), '--wheel-sha256', 'xyz', '--wheelhouse', str(parts['house'])]) == install.EXIT_USAGE
    assert re.search('sha256', capsys.readouterr().err)
