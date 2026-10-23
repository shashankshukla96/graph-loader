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
    assert image_reference(role) == f'ghcr.io/shashankshukla96/graph-loader-{role}:{__version__}'


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
