"""Missing-only pulls, exact compatibility and immutable per-instance selection."""
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from docker.errors import DockerException, ImageNotFound

from neo4j_graph_loader import cli
from neo4j_graph_loader._images import ImageSelection, ImageSelectionError, image_reference
from neo4j_graph_loader._version import __version__
from neo4j_graph_loader.orchestrator.docker_service import DockerService
from tests.docker_support import configure_images, IMAGE_IDS


def compatible(role='node', identity=None):
    return SimpleNamespace(id=identity or IMAGE_IDS[role], attrs={'Config': {'Labels': {
        'org.opencontainers.image.version': __version__, 'io.graph-loader.component': role,
        'io.graph-loader.image-contract': '1'}}})


@pytest.mark.parametrize('role', ['cli','node','edge','clock'])
def test_default_reference_matches_package_and_namespace(role, monkeypatch):
    monkeypatch.delenv(f'GRAPH_LOADER_{role.upper()}_IMAGE', raising=False)
    assert image_reference(role) == f'ghcr.io/neburyx/graph-loader-{role}:{__version__}'


@pytest.mark.parametrize('role', ['node','edge','clock'])
def test_override_has_same_compatibility_policy(role, monkeypatch):
    monkeypatch.setenv(f'GRAPH_LOADER_{role.upper()}_IMAGE','local/operator:chosen')
    client=MagicMock();client.images.get.return_value=compatible(role)
    assert ImageSelection(client).resolve(role) == IMAGE_IDS[role]
    client.images.get.assert_called_once_with('local/operator:chosen')
    client.images.pull.assert_not_called();client.images.build.assert_not_called()


def test_only_missing_reference_pulls():
    client=MagicMock();client.images.get.side_effect=ImageNotFound('absent')
    client.images.pull.return_value=compatible()
    assert ImageSelection(client).resolve('node') == IMAGE_IDS['node']
    client.images.pull.assert_called_once_with(image_reference('node'))
    client.images.build.assert_not_called()


def test_local_only_never_pulls():
    client=MagicMock();client.images.get.side_effect=ImageNotFound('absent')
    with pytest.raises(ImageSelectionError,match='missing locally'):
        ImageSelection(client,local_only=True).resolve('node')
    client.images.pull.assert_not_called();client.images.build.assert_not_called()


@pytest.mark.parametrize('key,value',[
    ('org.opencontainers.image.version',None),('org.opencontainers.image.version','0.1.0a1'),
    ('io.graph-loader.component','edge'),('io.graph-loader.component',None),
    ('io.graph-loader.image-contract','2'),('io.graph-loader.image-contract',None)])
def test_mismatches_fail_without_pull_or_build(key,value):
    if value==__version__:  # the id keeps the literal release string; its pre-release is still a near-miss mismatch
        value=f'{value}.dev0'
    image=compatible();image.attrs['Config']['Labels'][key]=value
    client=MagicMock();client.images.get.return_value=image
    with pytest.raises(ImageSelectionError,match='incompatible node image'):
        ImageSelection(client).resolve('node')
    client.images.pull.assert_not_called();client.images.build.assert_not_called()


@pytest.mark.parametrize('operation',['get','pull'])
def test_daemon_or_registry_failure_is_clear_without_exception_secret(operation):
    client=MagicMock()
    if operation=='pull':client.images.get.side_effect=ImageNotFound('absent')
    getattr(client.images,operation).side_effect=DockerException('sensitive-error-marker')
    with pytest.raises(ImageSelectionError,match='Docker daemon access') as error:
        ImageSelection(client).resolve('node')
    assert 'sensitive-error-marker' not in str(error.value)
    assert isinstance(error.value.__cause__,DockerException)


def test_resolution_pins_id_across_retag_environment_change_and_threads(monkeypatch):
    client=MagicMock();client.images.get.return_value=compatible()
    selection=ImageSelection(client)
    with ThreadPoolExecutor(max_workers=8) as pool:
        assert list(pool.map(lambda _:selection.resolve('node'),range(32))) == [IMAGE_IDS['node']]*32
    monkeypatch.setenv('GRAPH_LOADER_NODE_IMAGE','other:later')
    client.images.get.return_value=compatible(identity='sha256:'+'f'*64)
    assert selection.resolve('node') == IMAGE_IDS['node']
    client.images.get.assert_called_once()
    with pytest.raises(ImageSelectionError,match='already pinned'):
        selection.resolve('node','other:later')


@pytest.mark.parametrize('reference',['https://user:secret@example/image','bad space',''])
def test_invalid_reference_is_not_echoed(reference):
    with pytest.raises(ImageSelectionError,match='invalid node image reference') as error:
        ImageSelection(MagicMock()).resolve('node',reference)
    assert 'secret' not in str(error.value)


def test_build_compatibility_names_do_not_build_from_cwd():
    client=configure_images(MagicMock());service=DockerService(client=client)
    assert service.build_image() == IMAGE_IDS['node']
    assert service.build_edge_image() == IMAGE_IDS['edge']
    assert service.build_clock_image() == IMAGE_IDS['clock']
    with pytest.raises(ImageSelectionError,match='Dockerfile builds are unsupported'):
        service.build_image(dockerfile='Dockerfile')
    client.images.build.assert_not_called()


def test_public_main_reports_image_failure_before_service_access(tmp_path,monkeypatch,capsys):
    schema=tmp_path/'schema.yaml'
    schema.write_text('nodes: []\nedges: []\n')
    # Use actual bundled schema validation/state, then stop at image selection.
    service=MagicMock();service.prepare_images.side_effect=ImageSelectionError('node image is missing locally')
    monkeypatch.setattr(cli,'DockerService',lambda **kwargs:service)
    monkeypatch.setattr(cli,'get_neo4j_credentials',lambda:pytest.fail('service config reached'))
    assert cli.main(['start','--mode','bulk','--skip-image-build','--run-dir',str(tmp_path/'runs')]) == 1
    service.prepare_images.assert_called_once()


def test_public_main_redacts_daemon_constructor_error(tmp_path,monkeypatch,caplog):
    import docker
    def unavailable():
        raise DockerException('constructor-sensitive-marker')
    monkeypatch.setattr(docker,'from_env',unavailable)
    assert cli.main(['start','--mode','bulk','--skip-image-build','--run-dir',str(tmp_path/'runs')]) == 1
    assert 'cannot access Docker daemon' in caplog.text
    assert 'constructor-sensitive-marker' not in caplog.text


# -- E19.F2.S3: GRAPH_LOADER_IMAGE_REGISTRY -------------------------------------------------------------------------------------------
REGISTRY = 'GRAPH_LOADER_IMAGE_REGISTRY'


@pytest.fixture
def no_overrides(monkeypatch):
    for role in ('cli', 'node', 'edge', 'clock'):
        monkeypatch.delenv(f'GRAPH_LOADER_{role.upper()}_IMAGE', raising=False)
    monkeypatch.delenv(REGISTRY, raising=False)


@pytest.mark.parametrize('prefix', ['registry.example.com', 'registry:5000', 'registry.example.com:5000/team',
                                    'localhost:5000/a/b_c.d-e', '192.0.2.3:443/x', 'harbor.example.com/proj/sub'])
@pytest.mark.parametrize('role', ['cli', 'node', 'edge', 'clock'])
def test_registry_prefix_makes_every_default_reference(prefix, role, monkeypatch, no_overrides):
    monkeypatch.setenv(REGISTRY, prefix)
    assert image_reference(role) == f'{prefix}/graph-loader-{role}:{__version__}'


@pytest.mark.parametrize('value', [None, ''])
def test_unset_or_empty_registry_keeps_the_default_namespace(value, monkeypatch, no_overrides):
    if value is not None:
        monkeypatch.setenv(REGISTRY, value)
    assert image_reference('node') == f'ghcr.io/neburyx/graph-loader-node:{__version__}'


def test_a_per_role_override_wins_over_the_registry(monkeypatch, no_overrides):
    monkeypatch.setenv(REGISTRY, 'registry.example.com/team')
    monkeypatch.setenv('GRAPH_LOADER_EDGE_IMAGE', 'local/edge:chosen')
    assert image_reference('edge') == 'local/edge:chosen'
    assert image_reference('node') == f'registry.example.com/team/graph-loader-node:{__version__}'
    assert ImageSelection(MagicMock()).references == {
        'cli': f'registry.example.com/team/graph-loader-cli:{__version__}', 'node': f'registry.example.com/team/graph-loader-node:{__version__}',
        'edge': 'local/edge:chosen', 'clock': f'registry.example.com/team/graph-loader-clock:{__version__}'}


def test_a_per_role_override_is_not_validated_against_the_prefix_grammar(monkeypatch, no_overrides):
    """An invalid registry setting is irrelevant for a role whose own variable is set; the other roles still refuse it."""
    monkeypatch.setenv(REGISTRY, 'https://nope')
    monkeypatch.setenv('GRAPH_LOADER_NODE_IMAGE', 'local/node:chosen')
    assert image_reference('node') == 'local/node:chosen'
    with pytest.raises(ImageSelectionError):
        image_reference('edge')


SENTINEL = 'Zq7Xk2Sentinel'


def credentialed(url, user, password):
    """``url`` with credentials in it, assembled at run time so no source line holds a credentialed URL."""
    scheme, rest = url.split('://', 1)
    return scheme + '://' + user + ':' + password + '@' + rest
BAD_PREFIXES = {
    'scheme': 'https://registry.example.com', 'scheme-plain': 'http://registry.example.com:5000/team',
    'userinfo': f'user:{SENTINEL}@registry.example.com', 'userinfo-only-user': f'{SENTINEL}@registry.example.com/team',
    'tag': f'registry.example.com/team:{SENTINEL}', 'tag-on-host': f'registry.example.com:{SENTINEL}',
    'digest': f'registry.example.com/team@sha256:{"a" * 64}', 'uppercase-path': f'registry.example.com/{SENTINEL}',
    'trailing-slash': 'registry.example.com/team/', 'double-slash': 'registry.example.com//team', 'leading-slash': '/registry.example.com',
    'empty-component-at-end': 'registry.example.com/', 'space': f'registry.example.com/team {SENTINEL}', 'tab': 'registry.example.com\t/team',
    'newline': 'registry.example.com\n', 'only-space': ' ', 'port-too-long': 'registry.example.com:123456', 'port-letters': 'registry.example.com:port',
    'path-edge-hyphen': 'registry.example.com/-team', 'path-triple-underscore': 'registry.example.com/a___b', 'too-long': 'r.example.com/' + 'a' * 300,
    'query': f'registry.example.com/team?x={SENTINEL}', 'fragment': f'registry.example.com/team#{SENTINEL}',
}


@pytest.mark.parametrize('name', sorted(BAD_PREFIXES))
def test_invalid_registry_prefixes_are_rejected_without_echo(name, monkeypatch, no_overrides):
    monkeypatch.setenv(REGISTRY, BAD_PREFIXES[name])
    with pytest.raises(ImageSelectionError, match=f'invalid {REGISTRY}') as error:
        image_reference('node')
    assert SENTINEL not in str(error.value)
    assert not BAD_PREFIXES[name].strip() or BAD_PREFIXES[name] not in str(error.value)      # a blank value is a substring of any message


def test_control_the_bad_prefix_table_really_contains_what_it_names():
    """A guard against a vacuous table: each entry is wrong in the way its name says."""
    assert BAD_PREFIXES['scheme'].startswith('https://') and '@' in BAD_PREFIXES['userinfo']
    assert ':' in BAD_PREFIXES['tag'].split('/')[-1] and '@sha256:' in BAD_PREFIXES['digest']
    assert any(c.isupper() for c in BAD_PREFIXES['uppercase-path']) and BAD_PREFIXES['trailing-slash'].endswith('/')
    assert any(c.isspace() for c in BAD_PREFIXES['space'])


def test_the_docker_runtime_refuses_an_invalid_registry_before_touching_the_daemon(monkeypatch, no_overrides):
    monkeypatch.setenv(REGISTRY, f'user:{SENTINEL}@registry.example.com')
    client = MagicMock()
    with pytest.raises(ImageSelectionError, match=f'invalid {REGISTRY}') as error:
        DockerService(client=client)
    assert SENTINEL not in str(error.value)
    client.images.get.assert_not_called()
    client.images.pull.assert_not_called()


def test_a_registry_prefix_decides_what_is_pulled_and_checked(monkeypatch, no_overrides):
    monkeypatch.setenv(REGISTRY, 'registry.example.com:5000/team')
    client = MagicMock()
    client.images.get.side_effect = ImageNotFound('absent')
    client.images.pull.return_value = compatible('edge')
    assert ImageSelection(client).resolve('edge') == IMAGE_IDS['edge']
    client.images.get.assert_called_once_with(f'registry.example.com:5000/team/graph-loader-edge:{__version__}')
    client.images.pull.assert_called_once_with(f'registry.example.com:5000/team/graph-loader-edge:{__version__}')


def test_the_single_container_cli_image_follows_the_registry(monkeypatch, no_overrides):
    monkeypatch.setenv(REGISTRY, 'registry.example.com/team')
    assert image_reference('cli') == f'registry.example.com/team/graph-loader-cli:{__version__}'
    monkeypatch.setenv('GRAPH_LOADER_CLI_IMAGE', 'mirror.example.com/cli:pinned')
    assert image_reference('cli') == 'mirror.example.com/cli:pinned'


def test_the_public_main_reports_an_invalid_registry_without_echo(tmp_path, monkeypatch, caplog, no_overrides):
    import docker
    monkeypatch.setattr(docker, 'from_env', lambda: MagicMock())
    monkeypatch.setenv(REGISTRY, credentialed('https://registry.example.com', 'user', SENTINEL))
    assert cli.main(['start', '--mode', 'bulk', '--skip-image-build', '--run-dir', str(tmp_path / 'runs')]) == 1
    assert f'invalid {REGISTRY}' in caplog.text and SENTINEL not in caplog.text and 'registry.example.com' not in caplog.text


def test_the_process_runtime_ignores_the_registry(tmp_path, monkeypatch, no_overrides):
    from neo4j_graph_loader import _runtime
    from neo4j_graph_loader.orchestrator.process_service import ProcessService
    root = tmp_path / 'attempt'
    for name in ('rejections', 'plans', 'spill', 'timings'):
        (root / name).mkdir(parents=True)
    schema = root / 'runtime_schema.yaml'
    schema.write_text('x: 1\n')
    paths = _runtime.RunPaths(root, schema, root / 'rejections', root / 'plans', root / 'spill', root / 'timings')
    monkeypatch.setenv(REGISTRY, 'https://not-even-a-valid-prefix')
    service = ProcessService(paths)
    assert service.prepare_images(['node', 'edge']) == {'node': 'role:node', 'edge': 'role:edge'}


# per-role references: userinfo is refused (the old pattern accepted ``user:password@host/image``), digests and tags still work
@pytest.mark.parametrize('reference', [pytest.param(f'user:{SENTINEL}@registry.example.com/img:1', id='user-and-secret-before-the-host'),
                                       pytest.param(f'{SENTINEL}@registry.example.com/img', id='secret-before-the-host'),
                                       f'registry.example.com/img@{SENTINEL}', f'a@b@sha256:{"a" * 64}', f'registry.example.com/img@sha256:{SENTINEL}',
                                       f'@sha256:{"a" * 64}'])
def test_per_role_references_with_userinfo_are_rejected_without_echo(reference):
    with pytest.raises(ImageSelectionError, match='invalid node image reference') as error:
        ImageSelection(MagicMock()).resolve('node', reference)
    assert SENTINEL not in str(error.value)


@pytest.mark.parametrize('reference', ['local/operator:chosen', 'registry.example.com:5000/team/img:1.2', f'registry.example.com/img@sha256:{"b" * 64}',
                                       f'registry.example.com/img:1@sha256:{"c" * 64}'])
def test_per_role_tags_and_digests_still_resolve(reference):
    client = MagicMock()
    client.images.get.return_value = compatible()
    assert ImageSelection(client).resolve('node', reference) == IMAGE_IDS['node']
    client.images.get.assert_called_once_with(reference)
