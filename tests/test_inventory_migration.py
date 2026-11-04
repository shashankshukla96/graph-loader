"""E19.F1.S2: the inventory is portable and its migration keeps every ID, disposition and order.

Covers ``compare_inventories`` (the migration comparison), ``make_portable`` / ``portability_problems`` (no machine or private
path in the public file) and the frozen documentation snippet IDs, which contain line numbers.
"""
import copy
import json
from pathlib import Path
import re

import pytest

from scripts import freeze_installed_inventory as inventory

ROOT = Path(__file__).resolve().parents[1]
PRIVATE_FOLDER = 'plan' + '/library-implementation'     # a maintainers' folder; built from parts so no file here names it
PUBLIC_INVENTORY = ROOT / 'tests' / 'inventory' / 'test-inventory.json'
FILE = 'tests/test_x.py'
HOME, USERS = '/' + 'home' + '/someone', '/' + 'Users' + '/someone'      # machine paths of the shapes the portability check refuses


def disposition(identifier, kind='installed', lane='core'):
    return {'kind': kind, 'lane': lane, 'mapping': identifier} if kind == 'installed' else {'kind': kind, 'reason': 'r'}


def baseline():
    ids = [f'{FILE}::test_a', f'{FILE}::test_snippet[README.md:79]', f'{FILE}::test_b', f'{FILE}::test_live']
    return {'source_collection': {'test_ids': ids, 'command': ['/venv/bin/python', '-I', '-c', 'code', '/work/tree', '/work/c.json']},
            'test_dispositions': {ids[0]: disposition(ids[0]), ids[1]: disposition(ids[1], 'checkout-development'),
                                  ids[2]: disposition(ids[2]), ids[3]: disposition(ids[3], 'service')},
            'lanes': {'core': {'kind': 'installed unit regression', 'test_ids': [ids[0], ids[2]],
                               'command': f'python scripts/verify_installed.py --lane core --inventory {PRIVATE_FOLDER}/test-inventory.json',
                               'stage_files': {FILE: 'h1'}},
                      'service-harness': {'kind': 'installed service regression', 'test_ids': [ids[3]], 'stage_files': {}}},
            'artifact_contract': {'artifacts': [{'path': '/work/dist/p.whl', 'size': 1, 'sha256': 'x'}]},
            'disposition_amendments': [{'date': 'd', 'previous_inventory': 'var/old.json', 'ids': []}]}


def migrated_from(old, *, add=(), rename=None):
    new = copy.deepcopy(old)
    rename = rename or {}
    ids = [rename.get(i, i) for i in old['source_collection']['test_ids']]
    for position, identifier in add:
        ids.insert(position, identifier)
    new['source_collection']['test_ids'] = ids
    new['test_dispositions'] = {}
    for identifier in ids:
        origin = next((o for o, n in rename.items() if n == identifier), identifier)
        if origin in old['test_dispositions']:
            was = old['test_dispositions'][origin]
            new['test_dispositions'][identifier] = {**was, 'mapping': identifier} if 'mapping' in was else was
        else:
            new['test_dispositions'][identifier] = disposition(identifier)
    for lane in new['lanes'].values():
        lane['test_ids'] = [rename.get(i, i) for i in lane['test_ids']]
    return new


def test_an_identical_migration_with_additions_and_a_reviewed_rename_passes_and_is_reported():
    old = baseline()
    renames = {f'{FILE}::test_snippet[README.md:79]': f'{FILE}::test_snippet[README.md:92]'}
    new = migrated_from(old, add=[(0, f'{FILE}::test_first'), (5, f'{FILE}::test_last')], rename=renames)
    new['lanes']['core']['stage_files'] = {FILE: 'h2'}
    report = inventory.compare_inventories(old, new, renames)
    assert report['baseline_ids'] == 4 and report['migrated_ids'] == 6 and report['order_preserved'] is True
    assert report['dispositions_compared'] == 4 and report['dispositions_changed'] == 0
    assert report['added_ids'] == [f'{FILE}::test_first', f'{FILE}::test_last'] and report['renamed'] == renames
    assert any(change.startswith('lanes.core.stage_files') for change in report['changed_fields'])
    # the same migration, with the rename recorded only by the digest of the old ID: the comparison derives it and the old string is gone
    (old_id, new_id), = renames.items()
    new['reviewed_id_renames'] = {old_id: {'new_id': new_id, 'reviewed': 'r', 'reason': 'why'}}
    hidden = inventory.conceal_renames(new, [old_id])
    assert hidden == {inventory.id_digest(old_id): old_id} and old_id not in json.dumps(new['reviewed_id_renames'])
    assert inventory.concealed_problems(new) == []
    assert inventory.resolve_renames(new['reviewed_id_renames'], old['source_collection']['test_ids']) == renames
    assert inventory.compare_inventories(old, new)['renamed'] == renames
    for broken in (lambda e: e.update(old_id_sha256='0' * 64), lambda e: e.update(new_id='not collected'), lambda e: e.pop('reason')):
        bad = copy.deepcopy(new)
        broken(next(iter(bad['reviewed_id_renames'].values())))
        assert inventory.concealed_problems(bad), 'a damaged concealed rename record must be reported'
    still_there = copy.deepcopy(new)
    still_there['source_collection']['test_ids'].append(old_id)
    assert inventory.concealed_problems(still_there), 'the concealed old ID must not be collected any more'


def test_control_a_lost_id_fails():
    old = baseline()
    new = migrated_from(old)
    new['source_collection']['test_ids'].remove(f'{FILE}::test_b')
    with pytest.raises(ValueError, match='lost 1 IDs'):
        inventory.compare_inventories(old, new)


def test_control_an_unlisted_rename_fails_even_when_the_replacement_is_in_the_same_position():
    """Snippet IDs hold line numbers; moving one without a reviewed entry looks like a loss plus an addition."""
    old = baseline()
    renames = {f'{FILE}::test_snippet[README.md:79]': f'{FILE}::test_snippet[README.md:92]'}
    new = migrated_from(old, rename=renames)
    with pytest.raises(ValueError, match='lost 1 IDs'):
        inventory.compare_inventories(old, new)
    inventory.compare_inventories(old, new, renames)


def test_control_a_reorder_fails():
    old = baseline()
    new = migrated_from(old)
    ids = new['source_collection']['test_ids']
    ids[0], ids[2] = ids[2], ids[0]
    with pytest.raises(ValueError, match='reordered'):
        inventory.compare_inventories(old, new)


def test_control_a_changed_disposition_fails():
    old = baseline()
    new = migrated_from(old)
    new['test_dispositions'][f'{FILE}::test_a'] = disposition(f'{FILE}::test_a', 'checkout-development')
    with pytest.raises(ValueError, match='changed 1 dispositions'):
        inventory.compare_inventories(old, new)


def test_control_a_changed_lane_selection_fails():
    old = baseline()
    new = migrated_from(old)
    new['lanes']['core']['test_ids'].reverse()
    with pytest.raises(ValueError, match='lane core'):
        inventory.compare_inventories(old, new)


def test_make_portable_moves_private_paths_to_the_provenance_and_is_idempotent():
    old = baseline()
    assert inventory.portability_problems(old), 'the baseline carries private paths'
    public, provenance = inventory.make_portable(old)
    assert inventory.portability_problems(public) == []
    assert public['source_collection']['command'] == ['<python>', '-I', '-c', 'code', '<repo-root>', '<collection-output>']
    assert public['artifact_contract']['artifacts'] == [{'path': 'p.whl', 'size': 1, 'sha256': 'x'}]
    assert 'previous_inventory' not in public['disposition_amendments'][0]
    assert public['lanes']['core']['command'].endswith('--inventory tests/inventory/test-inventory.json')
    assert provenance['source_collection.command'][0] == '/venv/bin/python'
    assert provenance['artifact_contract.artifacts'][0]['path'] == '/work/dist/p.whl'
    assert provenance['disposition_amendments[0]'] == {'previous_inventory': 'var/old.json'}
    again, nothing = inventory.make_portable(public)
    assert again == public and nothing == {}
    assert old['source_collection']['command'][0] == '/venv/bin/python', 'the input is not modified'


def test_the_original_source_baseline_goes_to_the_private_provenance_and_comes_back_for_a_refreeze(tmp_path):
    old = baseline()
    old['original_source_baseline'] = {'tests/test_a.py': {'sha256': 'a', 'assertions': ['original']}}
    public, provenance = inventory.make_portable(old)
    assert 'original_source_baseline' not in public
    assert provenance['original_source_baseline'] == {'tests/test_a.py': {'sha256': 'a', 'assertions': ['original']}}
    sidecar = tmp_path / 'provenance.json'
    sidecar.write_text(json.dumps(provenance))
    restored = inventory.with_private_baseline(public, sidecar)
    assert restored['original_source_baseline'] == provenance['original_source_baseline'] and 'original_source_baseline' not in public
    # a path the public file still carries is the same original record and wins over the sidecar; the sidecar fills in the others
    mixed = inventory.with_private_baseline({**public, 'original_source_baseline': {'tests/test_b.py': {'sha256': 'b'}}}, sidecar)
    assert set(mixed['original_source_baseline']) == {'tests/test_a.py', 'tests/test_b.py'}
    assert inventory.with_private_baseline(public, tmp_path / 'missing.json') == public and inventory.with_private_baseline(public, None) == public


@pytest.mark.parametrize('where,value', [pytest.param(*case, id=f'{case[0]}-{index}') for index, case in enumerate([
    ('artifact', HOME + '/dist/p.whl'), ('artifact', 'C:/x/../p.whl'), ('stage', f'{PRIVATE_FOLDER}/tools/x.py'),
    ('stage', '../outside.py'), ('command', f'python run.py --inventory {PRIVATE_FOLDER}/test-inventory.json'),
    ('amendment', 'var/e07-validation/x.json'), ('collection', USERS + '/tree')])])
def test_control_each_private_path_shape_is_reported(where, value):
    public, _ = inventory.make_portable(baseline())
    if where == 'artifact':
        public['artifact_contract']['artifacts'][0]['path'] = value
    elif where == 'stage':
        public['lanes']['core']['stage_files'][value] = 'h'
    elif where == 'command':
        public['lanes']['core']['command'] = value
    elif where == 'amendment':
        public['disposition_amendments'][0]['evidence'] = value
    else:
        public['source_collection']['command'][4] = value
    assert inventory.portability_problems(public), (where, value)


def test_the_check_refuses_an_inventory_with_a_private_path(monkeypatch, tmp_path):
    public, _ = inventory.make_portable(baseline())
    monkeypatch.setattr(inventory, 'ROOT', tmp_path)
    (tmp_path / 'tests').mkdir()
    (tmp_path / FILE).write_text('def test_a(): pass\n')
    public['file_dispositions'] = {FILE: {}}
    public['reviewed_id_renames'] = {}
    public['lanes']['core']['stage_files'] = {FILE: inventory.sha((tmp_path / FILE).read_bytes())}
    collection = {'items': [{'id': i} for i in public['source_collection']['test_ids']]}
    inventory.check(public, collection)
    public['artifact_contract']['artifacts'][0]['path'] = HOME + '/p.whl'
    with pytest.raises(ValueError, match='private or machine paths'):
        inventory.check(public, collection)
    public['artifact_contract']['artifacts'][0]['path'] = 'p.whl'
    inventory.check(public, collection)
    public['disposition_amendments'][0]['note'] = 'host ' + '192.' + '168.1.75'     # a private LAN address anywhere in the file
    with pytest.raises(ValueError, match='rename records are not clean'):
        inventory.check(public, collection)


# -- the committed public inventory ------------------------------------------------------------------------------------
def public():
    return json.loads(PUBLIC_INVENTORY.read_text(encoding='utf-8'))


def test_the_public_inventory_is_where_the_freeze_tool_writes_it_and_carries_no_private_path():
    assert inventory.INVENTORY == PUBLIC_INVENTORY and PUBLIC_INVENTORY.is_file()
    data = public()
    assert inventory.portability_problems(data) == []
    assert data['source_collection']['command'][0] == '<python>'
    assert all('/' not in artifact['path'] for artifact in data['artifact_contract']['artifacts'])
    text = PUBLIC_INVENTORY.read_text(encoding='utf-8')
    assert 'run_e04' + '_fault' not in text and 'verify_e03' + '_artifacts' not in text
    assert not inventory.LAN_ADDRESS.search(text), 'no private 192.168.x.x address in the public inventory'
    assert inventory.concealed_problems(data) == []
    assert 'original_source_baseline' not in data, 'the original assertion dumps stay in the private provenance'
    # the one reviewed rename of the migration is recorded by the digest of its old ID
    old_id = ('tests/test_installed_service.py::test_remote_or_ambiguous_docker_hosts_are_rejected[tcp://192.' + '168.1.75:2375]')
    entry = data['reviewed_id_renames'][inventory.CONCEALED + inventory.id_digest(old_id)]
    assert entry['new_id'] == old_id.replace('192.' + '168.1.75', '192.0.2.75') and old_id not in data['source_collection']['test_ids']
    assert entry['reviewed'] and entry['reason'] and entry['old_id_sha256'] == inventory.id_digest(old_id)


def test_the_documentation_snippet_ids_are_exactly_the_frozen_ones():
    """Docs edits must keep the yaml fences on their lines: the IDs hold line numbers and are never renamed."""
    import test_docs_yaml_snippets as snippets
    prefix = 'tests/test_docs_yaml_snippets.py::test_yaml_snippet_loads['
    current = {f'{prefix}{relative}:{line}]' for relative, line, _ in snippets.SNIPPETS}
    frozen = {i for i in public()['source_collection']['test_ids'] if i.startswith(prefix)}
    assert current == frozen


def test_the_moved_tools_exist_at_their_public_paths_and_the_old_ones_are_not_used():
    for path in ('scripts/verify_artifacts.py', 'scripts/check_doc_links.py', 'tests/fixtures/run_fault.py'):
        assert (ROOT / path).is_file(), path
    frozen_lane_files = {path for lane in public()['lanes'].values() for path in lane.get('stage_files', {})}
    assert 'tests/fixtures/run_fault.py' in frozen_lane_files
    assert not any(path.startswith('plan/') for path in frozen_lane_files)


def test_the_documented_dependency_free_scripts_run_from_the_repository_root():
    import subprocess
    import sys
    result = subprocess.run([sys.executable, str(ROOT / 'scripts' / 'check_doc_links.py'), str(ROOT), 'README.md'],
                            capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr
    assert re.search(r'checked 1 files; 0 broken', result.stdout)
