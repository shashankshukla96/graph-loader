"""Resource lifetime, caller paths, schema helpers and finite sample inventory."""
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest

from neo4j_graph_loader import _resources, cli

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('name,source', [('default','config/graph_schema.yaml'),
                                         ('social','examples/social/graph_schema.yaml')])
def test_bundled_schema_is_exact_valid_small_copy(name, source, capsys):
    with _resources.schema_path(sample=name) as path:
        assert path.read_bytes() == (ROOT/source).read_bytes()
        assert path.stat().st_size < 16_000
    assert cli.main(['schema-example','--name',name]) == 0
    assert capsys.readouterr().out == (ROOT/source).read_text()
    assert cli.main(['validate-schema','--sample',name]) == 0
    assert 'Schema valid:' in capsys.readouterr().out


def test_resource_inventory_is_finite():
    data = Path(_resources.__file__).parent/'data'
    assert {p.name for p in data.iterdir()} == set(_resources.SCHEMAS.values())


def test_explicit_path_never_opens_bundled_resource(tmp_path, monkeypatch, capsys):
    path = tmp_path/'external.yaml'
    path.write_bytes((ROOT/'examples/social/graph_schema.yaml').read_bytes())
    monkeypatch.chdir(tmp_path)
    with patch.object(_resources,'files',side_effect=AssertionError('resource accessed')):
        assert cli.main(['validate-schema','--config',path.name]) == 0
    assert 'Schema valid:' in capsys.readouterr().out


def test_cwd_config_and_env_are_not_implicit_inputs(tmp_path, monkeypatch, capsys):
    (tmp_path/'config').mkdir(); (tmp_path/'config/graph_schema.yaml').write_text('bad: schema')
    (tmp_path/'.env').write_text('GRAPH_LOADER_RESOURCE_SECRET=loaded\n')
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv('GRAPH_LOADER_RESOURCE_SECRET',raising=False)
    assert cli.main(['validate-schema']) == 0
    assert 'Schema valid:' in capsys.readouterr().out
    import os
    assert 'GRAPH_LOADER_RESOURCE_SECRET' not in os.environ


@pytest.mark.parametrize('content', ['[invalid', 'nodes: bad', '[]', ''])
def test_invalid_explicit_yaml_fails_without_services(tmp_path, content, caplog):
    path=tmp_path/'bad.yaml';path.write_text(content)
    with patch.object(cli,'DockerService',side_effect=AssertionError('service accessed')):
        assert cli.main(['validate-schema','--config',str(path)]) == 1
    assert 'stage=schema' in caplog.text


def test_missing_resource_reports_failure(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(_resources,'files',lambda *_: tmp_path)
    assert cli.main(['validate-schema']) == 1
    assert 'stage=schema' in caplog.text


def test_materialized_path_lives_through_handler_cleanup(tmp_path, monkeypatch):
    materialized=tmp_path/'materialized.yaml'
    @contextmanager
    def temporary_path(resource):
        materialized.write_bytes((ROOT/'examples/social/graph_schema.yaml').read_bytes())
        try: yield materialized
        finally: materialized.unlink()
    monkeypatch.setattr(_resources,'as_file',temporary_path)
    def dispatch(args):
        try:
            assert Path(args.config).exists()
            return 7
        finally:
            assert Path(args.config).read_bytes() == materialized.read_bytes()
    monkeypatch.setattr(cli,'handle_status',dispatch)
    assert cli.main(['status']) == 7
    assert not materialized.exists()


@pytest.mark.parametrize('content', [None, b'\xff'])
def test_example_read_failure_is_reported(tmp_path, monkeypatch, content, caplog):
    (tmp_path/'data').mkdir()
    if content is not None:
        (tmp_path/'data/graph_schema.yaml').write_bytes(content)
    monkeypatch.setattr(_resources,'files',lambda *_: tmp_path)
    assert cli.main(['schema-example']) == 1
    assert 'stage=schema' in caplog.text
