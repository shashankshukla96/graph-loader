"""Executable inventory disappearance, helper-closure and mixed-file controls."""
import copy
import json
from pathlib import Path
import subprocess
import sys

import pytest

from scripts import freeze_installed_inventory as inventory
from scripts.installed_pytest import Receipt
from scripts.verify_installed import validate_inventory
from test_installed_runner import inventory as runner_inventory


def test_missing_id_and_unapproved_deselection_fail():
    frozen = {'source_collection': {'test_ids': ['tests/test_one.py::test_one', 'tests/test_one.py::test_two']}}
    for ids in ([], ['tests/test_one.py::test_one'], list(reversed(frozen['source_collection']['test_ids']))):
        with pytest.raises(ValueError, match='disappeared, changed or were deselected'):
            inventory.check(frozen, {'items': [{'id': i} for i in ids]})


def test_missing_file_and_changed_fixture_fail(tmp_path, monkeypatch):
    monkeypatch.setattr(inventory, 'ROOT', tmp_path)
    (tmp_path/'tests').mkdir()
    test = tmp_path/'tests/test_one.py'
    test.write_text('def test_one(): pass\n')
    frozen = {'source_collection': {'test_ids': ['tests/test_one.py::test_one']},
              'file_dispositions': {'tests/test_one.py': {}},
              'lanes': {'unit': {'kind': 'installed unit regression', 'stage_files': {'tests/test_one.py': inventory.sha(test.read_bytes())}}}}
    collection = {'items': [{'id': 'tests/test_one.py::test_one'}]}
    inventory.check(frozen, collection)
    test.unlink()
    with pytest.raises(ValueError, match='test files differ'):
        inventory.check(frozen, collection)
    test.write_text('def test_one(): assert False\n')
    with pytest.raises(ValueError, match='staged file missing/changed'):
        inventory.check(frozen, collection)


def test_recursive_local_dynamic_and_package_closure(tmp_path, monkeypatch):
    monkeypatch.setattr(inventory, 'ROOT', tmp_path)
    contents = {
        'tests/conftest.py': 'from helpers import support\n',
        'tests/test_one.py': 'from tests.fixtures import local\nSCRIPT="child.py"\n',
        'tests/helpers.py': 'import tests.fixtures.local\n',
        'tests/fixtures/__init__.py': '',
        'tests/fixtures/local.py': 'from tests import deeper\n',
        'tests/deeper.py': 'VALUE=1\n',
        'tests/fixtures/child.py': 'from tests import deeper\n',
    }
    for name, value in contents.items():
        path = tmp_path/name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value)
    staged, edges = inventory.closure(['tests/test_one.py'])
    assert set(staged) == set(contents)
    assert 'tests/deeper.py' in edges['tests/fixtures/local.py']


def test_unknown_runtime_test_cannot_be_checkout_default():
    with pytest.raises(ValueError, match='unclassified runtime test'):
        inventory.disposition({'id': 'tests/test_unknown.py::test_hard', 'file': 'tests/test_unknown.py', 'markers': []}, {})
    live = inventory.disposition({'id': 'tests/test_unknown.py::test_live', 'file': 'tests/test_unknown.py', 'markers': ['integration']}, {})
    assert live['kind'] == 'service' and live['owner'] == 'E09'


def test_mixed_file_requires_exact_full_partition():
    data = runner_inventory()
    lane = data['lanes']['probe']
    live = 'tests/test_probe.py::test_live'
    lane['collected_test_ids'] = [*lane['test_ids'], live]
    lane['excluded_test_ids'] = {live: 'E09 actual service; retained checkout'}
    validate_inventory(data, 'probe')
    changed = copy.deepcopy(data)
    changed['lanes']['probe']['excluded_test_ids'].clear()
    with pytest.raises(ValueError, match='full collection differs'):
        validate_inventory(changed, 'probe')


def test_refreeze_cannot_erase_or_reclassify_reviewed_ids(tmp_path, monkeypatch):
    import zipfile

    monkeypatch.setattr(inventory, 'ROOT', tmp_path)
    (tmp_path/'tests').mkdir()
    (tmp_path/'tests/test_installed_inventory.py').write_text('def test_one(): pass\n')
    (tmp_path/'tests/conftest.py').write_text('')
    wheel = tmp_path/'owned.whl'
    with zipfile.ZipFile(wheel, 'w') as archive:
        archive.writestr('neo4j_graph_loader/__init__.py', '')
    one = {'id': 'tests/test_installed_inventory.py::test_one', 'file': 'tests/test_installed_inventory.py', 'markers': []}
    data = {'lanes': {name: {'kind': 'installed unit regression', 'current_test_files': []} for name in ('core', 'safety', 'cli-runtime')},
            'source_collection': {'test_ids': [one['id'], one['id'] + '_lost']},
            'test_dispositions': {one['id']: {'kind': 'installed', 'lane': 'core', 'mapping': one['id']}}}
    with pytest.raises(ValueError, match='cannot remove/reorder'):
        inventory.freeze(data, {'items': [one]}, wheel)
    data['source_collection']['test_ids'].pop()
    with pytest.raises(ValueError, match='cannot silently change'):
        inventory.freeze(data, {'items': [one]}, wheel)


@pytest.mark.parametrize('deselect', [False, True])
def test_actual_pytest_mixed_disposition_and_unapproved_filter(tmp_path, deselect):
    (tmp_path/'test_mixed.py').write_text('def test_one(): pass\ndef test_live(): raise AssertionError("service ID executed")\n')
    plugin = Path(sys.modules[Receipt.__module__].__file__).resolve()
    receipt = tmp_path/'receipt.json'
    code = '''import importlib.util,sys,pytest
spec=importlib.util.spec_from_file_location('receipt_plugin',sys.argv[1]);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
one='test_mixed.py::test_one';live='test_mixed.py::test_live'
plugin=module.Receipt([one],{},sys.argv[2],full_expected=[one,live],excluded={live:'E09 real service'})
raise SystemExit(pytest.main(['-q','--rootdir=.', '-c', 'pytest.ini','test_mixed.py',*sys.argv[3:]],plugins=[plugin]))
'''
    (tmp_path/'pytest.ini').write_text('[pytest]\n')
    result = subprocess.run([sys.executable, '-I', '-c', code, str(plugin), str(receipt), *(['-k', 'one'] if deselect else [])],
                            cwd=tmp_path, capture_output=True, text=True, timeout=30)
    record = json.loads(receipt.read_text())
    if deselect:
        assert result.returncode == 1
        assert record['errors'] and record['deselected'] == ['test_mixed.py::test_live']
    else:
        assert result.returncode == 0, result.stdout + result.stderr
        assert record['collected'] == ['test_mixed.py::test_one']
        assert record['full_collected'] == ['test_mixed.py::test_one', 'test_mixed.py::test_live']
        assert record['approved_exclusions'] == {'test_mixed.py::test_live': 'E09 real service'}
