"""E19.F2.S6: ``graph-loader images build`` and the shared build core, against a recording stand-in for the docker CLI.

Nothing here runs Docker. The builds themselves (BuildKit, the context, the secret) are proved by the live harness; these tests pin the refusals,
the sanitized environment, the scoped registry logins, the generated context, the manifest and the dry run.
"""
import hashlib
import json
from pathlib import Path
import zipfile

import pytest

from neo4j_graph_loader import cli
from neo4j_graph_loader.image_build import builder, lockfile

SENTINEL = 'Zq7Xk2Sentinel'
BASE_DIGEST = 'sha256:' + '7' * 64
PUSHED = 'sha256:' + '8' * 64
ROOT = Path(__file__).resolve().parents[1]


def credentialed(url, user, password):
    """``url`` with credentials in it, assembled at run time so no source line holds a credentialed URL."""
    scheme, rest = url.split('://', 1)
    return scheme + '://' + user + ':' + password + '@' + rest


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def make_wheel(directory: Path, name: str, version: str = '1.0', machine: str | None = None) -> Path:
    tags = f'cp312-cp312-manylinux_2_28_{machine}' if machine else 'py3-none-any'
    path = directory / f'{name}-{version}-{tags}.whl'
    with zipfile.ZipFile(path, 'w') as archive:
        archive.writestr(f'{name}/__init__.py', f'# {name} {machine}')
    return path


class Fake:
    """The docker CLI: every call is recorded with its environment; ``state`` tells it what exists and what a build should produce."""
    calls: list = []
    state: dict = {}
    contexts: list = []

    def __init__(self, executable, env):
        self.executable, self.env = executable, dict(env)

    def run(self, *args, timeout=0, input_text=None):
        Fake.calls.append((args, dict(self.env)))
        out = lambda text='', code=0: type('Done', (), {'returncode': code, 'stdout': text, 'stderr': ''})()    # noqa: E731
        state = Fake.state
        if args[:2] == ('context', 'inspect'):
            return out(state.get('endpoint', 'unix:///var/run/docker.sock') + '\n')
        if args[:1] == ('info',):
            return out(json.dumps([{'Name': 'buildx', 'Path': state['plugin']}] if state.get('plugin') else []))
        if args[:1] == ('version',):
            return out(state.get('platform', 'linux/arm64') + '\n', 0 if state.get('daemon', True) else 1)
        if args[:2] == ('image', 'inspect'):
            tag = args[2]
            if tag in state.get('built', {}):
                return out(json.dumps([state['built'][tag]]))
            if tag in state.get('existing', ()):
                return out('[{}]')
            return out('', 1)
        if args[:3] == ('buildx', 'imagetools', 'inspect'):
            return out(BASE_DIGEST + '\n') if not state.get('base_fails') else out('', 1)
        if args[:2] == ('buildx', 'build'):
            file = Path(args[args.index('--file') + 1])
            context = file.parent
            Fake.contexts.append({str(p.relative_to(context)): hashlib.sha256(p.read_bytes()).hexdigest() for p in context.rglob('*') if p.is_file()})
            tag = args[args.index('-t') + 1]
            role = tag.split('graph-loader-')[1].split(':')[0]
            if state.get('build_fails') == role:
                return out(f'step failed for {state.get("secret_url", "")}', 1)
            state.setdefault('built', {})[tag] = {
                'Id': 'sha256:' + hashlib.sha256(tag.encode()).hexdigest(), 'Os': 'linux', 'Architecture': state.get('platform', 'linux/arm64').split('/')[1],
                'Config': {'Labels': {'org.opencontainers.image.version': builder.__version__, 'io.graph-loader.component': role,
                                      'io.graph-loader.image-contract': '1', 'org.opencontainers.image.source': 'https://github.com/Neburyx/graph-loader',
                                      **state.get('label_override', {})},
                           'Entrypoint': state.get('entrypoint_override', {}).get(role, builder.EXPECTED_ENTRYPOINTS[role])}}
            return out()
        if args[:1] == ('run',):
            return out('base-files=13.8+deb13u7\ntini=0.19.0-3+b8\n')
        if args[:1] == ('push',):
            return out(f'0.1: digest: {PUSHED} size: 1234\n') if not state.get('push_fails') else out('denied', 1)
        raise AssertionError(f'unexpected docker call {args}')


@pytest.fixture
def world(tmp_path, monkeypatch):
    """A synthetic lock and a wheelhouse that satisfies it for linux/arm64, the package data otherwise real, and a fake docker."""
    house = tmp_path / 'house'
    house.mkdir()
    dep = make_wheel(house, 'depone')
    binary = make_wheel(house, 'binlib', '3.4.5', 'aarch64')
    package = make_wheel(house, 'neo4j_graph_loader', builder.__version__)
    header = {'platform': 'linux/arm64', 'python': '3.12', 'base-image': 'python:3.12-slim@sha256:' + 'a' * 64, 'requirements-sha256': 'b' * 64, 'generator': 't'}
    text = lockfile.render(header, [lockfile.Entry('depone', '1.0', (sha(dep),)), lockfile.Entry('binlib', '3.4.5', (sha(binary),))])
    real = builder.package_bytes

    def package_bytes(*parts):
        return text.encode() if parts[:1] == ('locks',) else real(*parts)

    plugin = tmp_path / 'docker-buildx'
    plugin.write_text('#!/bin/sh\n')
    config = tmp_path / 'user-docker'
    config.mkdir()
    monkeypatch.setattr(builder, 'package_bytes', package_bytes)
    monkeypatch.setattr(builder, 'installed_binding', lambda wheel, version: None)
    for key in list(__import__('os').environ):
        if key.upper().endswith('_PROXY') or key.startswith(('DOCKER', 'PIP_')):
            monkeypatch.delenv(key)
    monkeypatch.setenv('PATH', '/usr/local/bin:/usr/bin')
    monkeypatch.setenv('HTTP_PROXY', 'http://proxy.internal:3128')
    monkeypatch.setenv('PIP_EXTRA_INDEX_URL', 'https://elsewhere/simple/')
    Fake.calls, Fake.contexts = [], []
    Fake.state = {'plugin': str(plugin)}
    settings = lambda **kw: builder.Settings(**{'registry': 'registry.example.com:5000/team', 'wheelhouse': house, 'docker_config': config, **kw})   # noqa: E731
    return {'house': house, 'package': package, 'text': text, 'settings': settings, 'config': config, 'tmp': tmp_path, 'dep': dep}


def run(world, **kw):
    return builder.build(world['settings'](**kw), docker_factory=Fake)


def verbs():
    return [call[0] for call in Fake.calls]


# -- refusals, all before anything is built ------------------------------------------------------------------------------------------------------
def refused(world, message, **kw):
    with pytest.raises(builder.BuildError, match=message):
        run(world, **kw)
    assert not any(call[0][:2] == ('buildx', 'build') for call in Fake.calls), 'a refused build must not start building'


@pytest.mark.parametrize('registry', ['https://registry.example.com', f'user:{SENTINEL}@registry.example.com', 'registry.example.com/Team', 'registry.example.com/', ''])
def test_control_a_bad_registry_prefix_is_refused_without_echo(world, registry):
    with pytest.raises(builder.BuildError, match='invalid --registry') as error:
        run(world, registry=registry)
    assert SENTINEL not in str(error.value) and registry not in str(error.value) or not registry


def test_control_a_version_that_is_not_the_installed_one_is_refused(world):
    refused(world, 'builds images for its own version', version='9.9.9')


def test_control_exactly_one_dependency_source_is_required(world):
    refused(world, 'exactly one of', wheelhouse=None)
    refused(world, 'exactly one of', index_url='https://index.example/simple/')


@pytest.mark.parametrize('url', [credentialed('https://index.example/simple/', 'user', SENTINEL), 'ftp://index.example/', 'https://index.example/ simple', 'index.example'])
def test_control_an_index_url_with_credentials_or_a_bad_scheme_is_refused_without_echo(world, url):
    with pytest.raises(builder.BuildError) as error:
        run(world, wheelhouse=None, index_url=url, package_wheel=world['package'])
    assert SENTINEL not in str(error.value)


@pytest.mark.parametrize('field,value', [('proxy', credentialed('http://proxy.example:3128', 'user', 'pw')), ('proxy', 'socks5://proxy.example'), ('debian_mirror', 'ftp://deb.example/debian'),
                                         ('debian_mirror', credentialed('http://deb.example/debian', 'u', 'p')), ('tag_suffix', 'Bad Suffix'), ('tag_suffix', 'x' * 80)])
def test_control_unsafe_optional_values_are_refused(world, field, value):
    refused(world, '.', **{field: value})


def test_control_a_suffix_marks_a_local_build_and_cannot_be_pushed(world):
    refused(world, 'cannot be pushed', tag_suffix='dev1', push=True)


def test_control_a_missing_buildx_fails_closed_with_a_clear_message(world):
    Fake.state.pop('plugin')
    refused(world, 'docker buildx \\(BuildKit\\) is required')


@pytest.mark.parametrize('endpoint', ['tcp://192.0.2.9:2375', 'ssh://host', 'unix://host/socket', 'unix:///var/run/../docker.sock'])
def test_control_only_a_local_unix_socket_is_accepted(world, endpoint):
    Fake.state['endpoint'] = endpoint
    refused(world, 'local Docker daemon')


@pytest.mark.parametrize('platform', ['linux/s390x', 'windows/amd64'])
def test_control_other_platforms_are_refused(world, platform):
    Fake.state['platform'] = platform
    refused(world, 'linux/amd64 or linux/arm64')


def test_control_an_unreachable_daemon_is_a_clear_refusal(world):
    Fake.state['daemon'] = False
    refused(world, 'cannot reach the Docker daemon')


def test_control_a_wheelhouse_that_does_not_satisfy_the_lock_is_refused(world):
    next(world['house'].glob('binlib*')).unlink()
    refused(world, 'does not satisfy the lock: wheelhouse is missing binlib')
    make_wheel(world['house'], 'binlib', '3.4.5', 'x86_64')                  # the other architecture
    refused(world, 'does not satisfy the lock')
    make_wheel(world['house'], 'binlib', '3.4.5', 'aarch64')                 # right name, other bytes
    (world['house'] / 'surplus-1-py3-none-any.whl').write_bytes(b'x')
    refused(world, 'does not satisfy the lock')


def test_control_the_package_wheel_must_be_found_and_agree(world, tmp_path):
    world['package'].unlink()
    refused(world, 'missing the package wheel')
    other = make_wheel(tmp_path, 'neo4j_graph_loader', builder.__version__)
    other.write_bytes(b'different')
    refused(world, 'put neo4j_graph_loader-', package_wheel=other)
    refused(world, 'give --package-wheel', wheelhouse=None, index_url='https://index.example/simple/')
    refused(world, 'must be a wheel file', wheelhouse=None, index_url='https://index.example/simple/', package_wheel=tmp_path / 'nope.tgz')


def test_control_an_existing_tag_is_refused_unless_replace_is_given(world):
    tag = f'registry.example.com:5000/team/graph-loader-node:{builder.__version__}'
    Fake.state['existing'] = {tag}
    refused(world, 'already exists locally')
    manifest = run(world, replace=True)
    assert len(manifest['images']) == 4


def test_control_the_output_directory_must_be_new(world, tmp_path):
    (tmp_path / 'out').mkdir()
    refused(world, 'new directory', output=tmp_path / 'out')


def test_control_an_image_that_does_not_carry_the_contract_is_rejected(world):
    Fake.state['label_override'] = {'io.graph-loader.image-contract': '2'}
    with pytest.raises(builder.BuildError, match='three image-contract labels'):
        run(world)
    Fake.state = {'plugin': Fake.state['plugin'], 'entrypoint_override': {'node': ['sh']}}
    with pytest.raises(builder.BuildError, match='unexpected entrypoint'):
        run(world)


def test_control_a_failed_build_reports_the_step_without_the_credentials(world, tmp_path):
    secret = tmp_path / 'pip_index'
    secret.write_text(credentialed('https://index.example/simple/', 'user', SENTINEL) + '\n')
    Fake.state |= {'build_fails': 'node', 'secret_url': credentialed('https://index.example/simple/', 'user', SENTINEL)}
    with pytest.raises(builder.BuildError, match='node image build failed') as error:
        run(world, wheelhouse=None, index_secret=secret, package_wheel=world['package'])
    assert SENTINEL not in str(error.value) and 'index.example' not in str(error.value).replace('***@index.example', '')


# -- dry run -------------------------------------------------------------------------------------------------------------------------------------
def test_a_dry_run_validates_everything_prints_the_plan_and_builds_nothing(world, tmp_path):
    plan = run(world, dry_run=True, output=tmp_path / 'never')
    assert plan['dry_run'] is True and plan['platform'] == 'linux/arm64' and plan['version'] == builder.__version__
    assert [row['local_tag'] for row in plan['images']] == [f'registry.example.com:5000/team/graph-loader-{r}:{builder.__version__}' for r in builder.ROLES]
    assert not any(call[0][:2] in (('buildx', 'build'), ('push',)) or call[0][:1] == ('run',) for call in Fake.calls)
    assert not (tmp_path / 'never').exists() and plan['package_wheel']['sha256'] == sha(world['package'])
    assert not any(call[0][:3] == ('buildx', 'imagetools', 'inspect') for call in Fake.calls), 'no registry is contacted'


def test_a_dry_run_still_applies_every_refusal(world):
    next(world['house'].glob('binlib*')).unlink()
    with pytest.raises(builder.BuildError, match='does not satisfy the lock'):
        run(world, dry_run=True)


# -- the environment docker runs in and the logins it sees -----------------------------------------------------------------------------------
def test_docker_runs_with_a_sanitized_environment_and_a_fresh_scoped_config(world):
    run(world)
    build_calls = [env for args, env in Fake.calls if args[:2] == ('buildx', 'build')]
    assert len(build_calls) == 4
    env = build_calls[0]
    assert set(env) == {'PATH', 'HOME', 'DOCKER_CONFIG', 'DOCKER_HOST', 'DOCKER_BUILDKIT'} or set(env) == {'PATH', 'DOCKER_CONFIG', 'DOCKER_HOST', 'DOCKER_BUILDKIT'}
    assert not any('proxy' in key.lower() or key.startswith('PIP_') for key in env), 'no proxy and no pip variable of the caller reaches docker'
    assert env['DOCKER_CONFIG'] != str(world['config']) and env['DOCKER_HOST'] == 'unix:///var/run/docker.sock'
    assert not Path(env['DOCKER_CONFIG']).exists(), 'the temporary config (with copied logins) is removed afterwards'


def test_only_the_logins_of_the_base_registry_and_the_target_registry_are_copied(world):
    (world['config'] / 'config.json').write_text(json.dumps({
        'auths': {'registry.example.com:5000': {'auth': 'target'}, 'mirror.example.com': {'auth': 'base'}, 'other.example.com': {'auth': 'unrelated'},
                  'https://index.docker.io/v1/': {'auth': 'hub'}},
        'credHelpers': {'registry.example.com:5000': 'target-helper', 'unrelated.example.com': 'secret-helper'}, 'credsStore': 'osxkeychain',
        'proxies': {'default': {'httpProxy': 'http://proxy:3128'}}, 'currentContext': 'x', 'plugins': {'a': {}}, 'auxKey': 1}))
    seen = {}
    original = Fake.run

    def spy(self, *args, **kw):
        if args[:2] == ('buildx', 'build') and not seen:
            seen.update(json.loads((Path(self.env['DOCKER_CONFIG']) / 'config.json').read_text()))
        return original(self, *args, **kw)
    Fake.run = spy
    try:
        run(world, base_image='mirror.example.com/mirror/python:3.12-slim')
    finally:
        Fake.run = original
    assert set(seen['auths']) == {'registry.example.com:5000', 'mirror.example.com'} and 'unrelated' not in json.dumps(seen) and 'hub' not in json.dumps(seen)
    assert seen['credHelpers'] == {'registry.example.com:5000': 'target-helper', 'mirror.example.com': 'osxkeychain'}
    assert set(seen) == {'auths', 'credHelpers'}, 'no proxy, plugin, context or global store setting is carried over'


def test_scoped_config_cases():
    source = {'auths': {'https://index.docker.io/v1/': {'auth': 'a'}, 'ghcr.io': {'auth': 'g'}, 'https://registry.example.com': {'auth': 'r'}},
              'credsStore': 'desktop', 'credHelpers': {'ecr.example.com': 'ecr-login'}}
    assert builder.scoped_config(source, {'docker.io'}) == {'auths': {'https://index.docker.io/v1/': {'auth': 'a'}},
                                                            'credHelpers': {k: 'desktop' for k in ('docker.io', 'index.docker.io', 'registry-1.docker.io', 'https://index.docker.io/v1/')}}
    assert builder.scoped_config(source, {'registry.example.com', 'ecr.example.com'}) == {
        'auths': {'https://registry.example.com': {'auth': 'r'}}, 'credHelpers': {'registry.example.com': 'desktop', 'ecr.example.com': 'ecr-login'}}
    assert builder.scoped_config({}, {'ghcr.io'}) == {}
    assert [builder.registry_host(r) for r in ('python:3.12-slim', 'library/python', 'ghcr.io/neburyx/x', 'localhost:5000/x', 'reg.example.com:5000/a/b:1', 'team/x')] == [
        'docker.io', 'docker.io', 'ghcr.io', 'localhost:5000', 'reg.example.com:5000', 'docker.io']


def test_a_proxy_reaches_the_build_only_when_it_is_given_explicitly(world):
    seen = []
    original = Fake.run

    def spy(self, *args, **kw):
        if args[:2] == ('buildx', 'build'):
            seen.append(args)
        return original(self, *args, **kw)
    Fake.run = spy
    try:
        run(world)
        without = [a for a in seen if any('PROXY' in part.upper() for part in a)]
        seen.clear()
        manifest = run(world, proxy='http://proxy.example:3128', replace=True)
        with_proxy = [a for a in seen if any(part.startswith('HTTPS_PROXY=') for part in a)]
    finally:
        Fake.run = original
    assert without == [] and len(with_proxy) == 4
    assert 'proxy.example' not in json.dumps(manifest) and manifest['proxy'] is True


# -- the context, the secret, the manifest -----------------------------------------------------------------------------------------------------
def test_the_generated_context_holds_package_data_the_wheel_and_the_wheelhouse_only(world):
    run(world)
    context = Fake.contexts[0]
    assert set(context) == {*builder.DOCKERFILES.values(), 'install.py', 'lock.txt', world['package'].name, 'pip-ca.pem',
                            *(f'wheelhouse/{p.name}' for p in world['house'].iterdir())}
    assert context['lock.txt'] == hashlib.sha256(world['text'].encode()).hexdigest()
    assert context['Dockerfile.node_loader'] == hashlib.sha256((ROOT / 'src/neo4j_graph_loader/image_build/dockerfiles/Dockerfile.node_loader').read_bytes()).hexdigest()
    assert context['pip-ca.pem'] == hashlib.sha256(b'').hexdigest()


def test_a_ca_bundle_is_copied_into_the_context_and_a_missing_one_is_refused(world, tmp_path):
    ca = tmp_path / 'ca.pem'
    ca.write_text('-----PEM-----\n')
    run(world, ca_bundle=ca)
    assert Fake.contexts[0]['pip-ca.pem'] == hashlib.sha256(ca.read_bytes()).hexdigest()
    Fake.calls.clear()
    refused(world, 'must be a file', ca_bundle=tmp_path / 'absent.pem', replace=True)


def test_the_index_secret_goes_to_buildkit_as_a_file_and_nowhere_else(world, tmp_path):
    secret = tmp_path / 'pip_index'
    url = credentialed('https://index.example/simple/', 'user', SENTINEL)
    secret.write_text(url + '\n')
    manifest = run(world, wheelhouse=None, index_secret=secret, package_wheel=world['package'], output=tmp_path / 'out')
    commands = [args for args, _ in Fake.calls if args[:2] == ('buildx', 'build')]
    assert all(f'id=pip_index,src={secret.resolve()}' in args for args in commands)
    everything = json.dumps([[list(a), e] for a, e in Fake.calls]) + json.dumps(manifest) + (tmp_path / 'out' / 'images.json').read_text() \
        + (tmp_path / 'out' / 'image-overrides.sh').read_text() + json.dumps(Fake.contexts)
    assert SENTINEL not in everything and 'user:' not in everything and url not in everything
    assert all(any(part == 'DEPENDENCY_SOURCE=index-secret' for part in args) for args in commands)
    assert not any(part.startswith('INDEX_URL=') for args in commands for part in args)
    assert all('<secret file>' in ' '.join(row['command']) for row in manifest['images'])


def test_a_plain_index_url_is_a_build_argument_but_is_not_recorded(world, tmp_path):
    manifest = run(world, wheelhouse=None, index_url='https://index.example/simple/', package_wheel=world['package'])
    commands = [args for args, _ in Fake.calls if args[:2] == ('buildx', 'build')]
    assert all('INDEX_URL=https://index.example/simple/' in args and 'DEPENDENCY_SOURCE=index-url' in args for args in commands)
    assert 'index.example' not in json.dumps(manifest) and 'INDEX_URL' not in manifest['build_args']


def test_the_manifest_records_the_provenance_and_nothing_secret(world, tmp_path):
    out = tmp_path / 'out'
    manifest = run(world, output=out, base_image='python:3.12-slim')
    assert json.loads((out / 'images.json').read_text()) == manifest
    assert manifest['schema'] == builder.MANIFEST_SCHEMA and manifest['version'] == builder.__version__ and manifest['published'] is False
    assert manifest['platform'] == 'linux/arm64' and manifest['base_image'] == {'requested': 'python:3.12-slim', 'resolved': f'python@{BASE_DIGEST}'}
    assert manifest['package_wheel'] == {'file': world['package'].name, 'sha256': sha(world['package'])}
    assert manifest['lock'] == {'file': 'py312-linux-arm64.txt', 'sha256': hashlib.sha256(world['text'].encode()).hexdigest()}
    assert manifest['dependency_sha256'] == sorted([sha(world['dep']), sha(next(world['house'].glob('binlib*')))])
    assert set(manifest['recipe']['dockerfiles']) == set(builder.DOCKERFILES.values()) and manifest['recipe']['version'] == builder.RECIPE_VERSION
    assert manifest['recipe']['install_helper_sha256'] == sha(ROOT / 'src/neo4j_graph_loader/image_build/install.py')
    assert manifest['build_args']['BASE_IMAGE'] == f'python@{BASE_DIGEST}' and manifest['build_args']['DEPENDENCY_SOURCE'] == 'wheelhouse'
    assert manifest['debian_packages'] == {'base-files': '13.8+deb13u7', 'tini': '0.19.0-3+b8'}
    rows = {row['component']: row for row in manifest['images']}
    assert set(rows) == set(builder.ROLES)
    for role, row in rows.items():
        assert row['local_tag'] == f'registry.example.com:5000/team/graph-loader-{role}:{builder.__version__}' and row['registry_digest'] is None
        assert row['labels']['io.graph-loader.component'] == role and row['entrypoint'] == builder.EXPECTED_ENTRYPOINTS[role] and row['exit_code'] == 0
    assert 'GRAPH_LOADER_NODE_IMAGE=' in (out / 'image-overrides.sh').read_text()
    assert len({row['id'] for row in rows.values()}) == 4


def test_a_base_image_with_a_digest_is_used_as_given_and_a_tag_is_resolved_first(world):
    given = f'mirror.example.com/mirror/python@sha256:{"5" * 64}'
    manifest = run(world, base_image=given)
    assert manifest['base_image']['resolved'] == given and not any(call[0][:3] == ('buildx', 'imagetools', 'inspect') for call in Fake.calls)
    Fake.state = {'plugin': Fake.state['plugin']}
    manifest = run(world, base_image='mirror.example.com:5443/mirror/python:3.12-slim')
    assert manifest['base_image']['resolved'] == f'mirror.example.com:5443/mirror/python@{BASE_DIGEST}'
    Fake.state = {'plugin': Fake.state['plugin'], 'base_fails': True}
    with pytest.raises(builder.BuildError, match='cannot resolve the base image'):
        run(world, base_image='mirror.example.com/mirror/python:3.12-slim')


def test_push_uses_docker_push_and_records_each_digest_and_a_failed_push_is_an_error(world):
    manifest = run(world, push=True)
    assert [call[0] for call in Fake.calls if call[0][:1] == ('push',)] == [('push', row['local_tag']) for row in manifest['images']]
    assert all(row['registry_digest'] == PUSHED for row in manifest['images'])
    Fake.state['push_fails'] = True
    with pytest.raises(builder.BuildError, match='pushing .* failed'):
        run(world, push=True, replace=True)


def test_the_docker_sdk_is_never_used_to_build_or_push():
    source = (ROOT / 'src/neo4j_graph_loader/image_build/builder.py').read_text()
    assert 'import docker' not in source and 'images.build' not in source and 'images.push' not in source


# -- the installed-package binding -----------------------------------------------------------------------------------------------------------
class FakeDist:
    def __init__(self, files, direct=None):
        self.files, self._direct = files, direct

    def read_text(self, name):
        return self._direct if name == 'direct_url.json' else None


def record_entry(path: str, content: bytes):
    import base64
    digest = base64.urlsafe_b64encode(hashlib.sha256(content).digest()).rstrip(b'=').decode()
    return type('F', (), {'__str__': lambda self: path, 'hash': type('H', (), {'value': digest})()})()


def test_the_package_wheel_must_be_the_installed_package(tmp_path, monkeypatch):
    version = builder.__version__
    wheel = tmp_path / f'neo4j_graph_loader-{version}-py3-none-any.whl'
    with zipfile.ZipFile(wheel, 'w') as archive:
        archive.writestr('neo4j_graph_loader/__init__.py', 'x = 1\n')
        archive.writestr(f'neo4j_graph_loader-{version}.dist-info/METADATA', 'Name: x\n')
    good = FakeDist([record_entry('neo4j_graph_loader/__init__.py', b'x = 1\n')])
    monkeypatch.setattr(builder.importlib.metadata, 'distribution', lambda name: good)
    builder.installed_binding(wheel, version)
    monkeypatch.setattr(builder.importlib.metadata, 'distribution', lambda name: FakeDist([record_entry('neo4j_graph_loader/__init__.py', b'x = 2\n')]))
    with pytest.raises(builder.BuildError, match='not the installed package'):
        builder.installed_binding(wheel, version)                 # same version, other bytes: a substituted wheel
    monkeypatch.setattr(builder.importlib.metadata, 'distribution', lambda name: FakeDist(good.files, '{"dir_info": {"editable": true}}'))
    with pytest.raises(builder.BuildError, match='editable'):
        builder.installed_binding(wheel, version)
    monkeypatch.setattr(builder.importlib.metadata, 'distribution', lambda name: (_ for _ in ()).throw(builder.importlib.metadata.PackageNotFoundError()))
    with pytest.raises(builder.BuildError, match='not installed'):
        builder.installed_binding(wheel, version)
    monkeypatch.setattr(builder.importlib.metadata, 'distribution', lambda name: good)
    with pytest.raises(builder.BuildError, match='is not a wheel of'):
        builder.installed_binding(wheel, '0.0.1')


# -- the command ---------------------------------------------------------------------------------------------------------------------------------
def test_the_command_needs_a_registry_and_one_dependency_source(monkeypatch, capsys):
    monkeypatch.delenv('GRAPH_LOADER_IMAGE_REGISTRY', raising=False)
    with pytest.raises(SystemExit):
        cli.main(['images', 'build'])
    with pytest.raises(SystemExit):
        cli.main(['images', 'build', '--wheelhouse', 'a', '--pip-index-url', 'https://i/'])
    assert cli.main(['images', 'build', '--wheelhouse', 'a']) == 1


def test_the_command_reports_a_refusal_with_exit_one_and_prints_the_plan_for_a_dry_run(world, monkeypatch, capsys, caplog):
    real = builder.build
    seen = []

    def fake_build(settings):
        seen.append(settings)
        return real(settings, docker_factory=Fake)

    monkeypatch.setattr(builder, 'build', fake_build)
    monkeypatch.setenv('GRAPH_LOADER_IMAGE_REGISTRY', 'registry.example.com:5000/team')
    assert cli.main(['images', 'build', '--wheelhouse', str(world['house']), '--dry-run']) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan['dry_run'] is True and seen[0].registry == 'registry.example.com:5000/team' and seen[0].dry_run is True
    assert cli.main(['images', 'build', '--wheelhouse', str(world['house']), '--package-version', '9.9.9']) == 1
    assert 'builds images for its own version' in caplog.text and 'stage=images' in caplog.text
    assert cli.main(['images', 'build', '--registry', 'https://bad', '--wheelhouse', str(world['house'])]) == 1
    assert 'invalid --registry' in caplog.text
