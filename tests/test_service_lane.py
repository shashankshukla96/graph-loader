"""Outer-controller snapshot, difference and reaper controls for live service lanes (fake Docker client)."""
from __future__ import annotations

import json
import os

import docker
import pytest
import requests

from scripts import service_lane, verify_installed
from tests.installed_service import live_stack, ownership

OWNER = 'a' * 32
OTHER = 'b' * 32
LABEL = service_lane.OWNER_LABEL


def rows(*items):
    return [{'owner': OWNER, 'at': 0, **item} for item in items]


class FakeApi:
    def __init__(self, containers=None, volumes=None, networks=None):
        self.containers, self.volumes, self.networks = containers or {}, volumes or {}, networks or {}
        self.removed, self.fail, self.raises = [], set(), {}

    def _maybe_raise(self, method, identifier):
        if (method, identifier) in self.raises:
            raise self.raises[method, identifier]

    def inspect_container(self, identifier):
        self._maybe_raise('inspect_container', identifier)
        if identifier not in self.containers:
            raise docker.errors.NotFound('gone')
        return self.containers[identifier]

    def inspect_volume(self, name):
        self._maybe_raise('inspect_volume', name)
        if name not in self.volumes:
            raise docker.errors.NotFound('gone')
        return {'Labels': self.volumes[name]}

    def inspect_network(self, identifier):
        self._maybe_raise('inspect_network', identifier)
        if identifier not in self.networks:
            raise docker.errors.NotFound('gone')
        return {'Labels': self.networks[identifier]}

    def remove_container(self, identifier, **kwargs):
        if identifier in self.fail:
            raise docker.errors.APIError('boom')
        self.removed.append(identifier)

    def remove_volume(self, name, **kwargs):
        self._maybe_raise('remove_volume', name)
        self.removed.append(name)

    def remove_network(self, identifier):
        self._maybe_raise('remove_network', identifier)
        self.removed.append(identifier)


class FakeClient:
    def __init__(self, api):
        self.api = api


IMAGE = 'sha256:' + 'c' * 64


def container(labels=None, networks=(), image=IMAGE):
    return {'Image': image, 'Config': {'Labels': labels or {}},
            'NetworkSettings': {'Networks': {f'n{i}': {'NetworkID': n} for i, n in enumerate(networks)}}}


def test_recorded_drops_removed_resources_and_keeps_observed_ones():
    log = rows({'kind': 'created', 'resource': 'container', 'id': 'c1'}, {'kind': 'observed', 'resource': 'container', 'id': 'c2'},
               {'kind': 'removed', 'resource': 'container', 'id': 'c1'}, {'kind': 'intent', 'resource': 'network', 'name': 'x'})
    assert set(service_lane.recorded(log)['container']) == {'c2'}
    assert service_lane.ever_created(log)['container'] == {'c1', 'c2'}


def test_reap_removes_only_recorded_owned_resources():
    api = FakeApi(containers={'owned': container({LABEL: OWNER}), 'attached': container(networks=['net1']),
                              'foreign-label': container({LABEL: 'b' * 32}), 'unrelated': container()},
                  volumes={'vol': {LABEL: OWNER}}, networks={'net1': {LABEL: OWNER}})
    log = rows(*[{'kind': 'created', 'resource': 'container', 'id': i} for i in ('owned', 'attached', 'foreign-label', 'gone')],
               {'kind': 'created', 'resource': 'volume', 'id': 'vol'}, {'kind': 'created', 'resource': 'network', 'id': 'net1'})
    result = service_lane.reap(FakeClient(api), OWNER, log)
    assert sorted(result['removed']) == ['attached', 'net1', 'owned', 'vol']
    assert result['refused'] == ['foreign-label'] and result['errors'] == []
    assert 'unrelated' not in api.removed and 'gone' not in api.removed


def test_reap_refuses_a_network_or_volume_with_another_owner():
    api = FakeApi(volumes={'vol': {LABEL: 'b' * 32}}, networks={'net1': {}})
    log = rows({'kind': 'created', 'resource': 'volume', 'id': 'vol'}, {'kind': 'created', 'resource': 'network', 'id': 'net1'})
    result = service_lane.reap(FakeClient(api), OWNER, log)
    assert result == {'removed': [], 'refused': ['vol', 'net1'], 'errors': []} and api.removed == []


def test_reap_refuses_another_owners_container_even_when_attached_to_the_owned_network():
    api = FakeApi(containers={'other': container({LABEL: 'b' * 32}, networks=['net1'])}, networks={'net1': {LABEL: OWNER}})
    log = rows({'kind': 'created', 'resource': 'container', 'id': 'other'}, {'kind': 'created', 'resource': 'network', 'id': 'net1'})
    assert service_lane.reap(FakeClient(api), OWNER, log)['refused'] == ['other']


def test_reap_reports_docker_errors_instead_of_raising():
    api = FakeApi(containers={'c1': container({LABEL: OWNER})})
    api.fail.add('c1')
    result = service_lane.reap(FakeClient(api), OWNER, rows({'kind': 'created', 'resource': 'container', 'id': 'c1'}))
    assert result['errors'] == [('c1', 'remove-container', 'APIError')] and result['removed'] == []


def test_reap_finds_a_resource_whose_create_was_never_recorded_by_exact_name():
    name = f'e09-{OWNER}-neo'
    api = FakeApi(containers={name: {**container({LABEL: OWNER}), 'Id': 'abc'}}, volumes={})
    api.containers['abc'] = api.containers[name]
    log = rows({'kind': 'intent', 'resource': 'container', 'name': name}, {'kind': 'intent', 'resource': 'container', 'name': 'e09-other-neo'})
    result = service_lane.reap(FakeClient(api), OWNER, log)
    assert result['removed'] == ['abc'] and result['refused'] == []


def created(*resources):
    return rows(*[{'kind': 'created', 'resource': kind, 'id': identifier} for kind, identifier in resources])


def test_reap_continues_after_a_failed_inspection_and_keeps_earlier_results():
    api = FakeApi(containers={'first': container({LABEL: OWNER}), 'second': container({LABEL: OWNER}), 'third': container({LABEL: OWNER})},
                  volumes={'vol': {LABEL: OWNER}}, networks={'net1': {LABEL: OWNER}})
    api.raises['inspect_container', 'second'] = docker.errors.APIError('boom')
    log = created(('container', 'first'), ('container', 'second'), ('container', 'third'), ('volume', 'vol'), ('network', 'net1'))
    result = service_lane.reap(FakeClient(api), OWNER, log)
    assert result['errors'] == [('second', 'inspect-container', 'APIError')]
    assert sorted(result['removed']) == ['first', 'net1', 'third', 'vol'] and 'second' not in api.removed


@pytest.mark.parametrize('method, resource, step', [
    ('inspect_volume', 'volume', 'inspect-volume'), ('inspect_network', 'network', 'inspect-network'),
    ('remove_volume', 'volume', 'remove-volume'), ('remove_network', 'network', 'remove-network'),
    ('inspect_container', 'container', 'inspect-container')])
@pytest.mark.parametrize('failure', [docker.errors.APIError('boom'), requests.exceptions.ConnectionError('socket closed'), OSError('transport')])
def test_reap_records_each_kind_of_docker_or_transport_failure_without_raising(method, resource, step, failure):
    api = FakeApi(containers={'c1': container({LABEL: OWNER})}, volumes={'v1': {LABEL: OWNER}}, networks={'n1': {LABEL: OWNER}})
    identifier = {'container': 'c1', 'volume': 'v1', 'network': 'n1'}[resource]
    api.raises[method, identifier] = failure
    result = service_lane.reap(FakeClient(api), OWNER, created(('container', 'c1'), ('volume', 'v1'), ('network', 'n1')))
    assert result['errors'] == [(identifier, step, type(failure).__name__)]
    assert sorted(result['removed']) == sorted({'c1', 'v1', 'n1'} - {identifier})


def test_reap_reports_a_failed_intent_lookup_and_goes_on():
    name = f'e09-{OWNER}-neo'
    api = FakeApi(containers={'c1': container({LABEL: OWNER})})
    api.raises['inspect_container', name] = requests.exceptions.ConnectionError('socket closed')
    log = rows({'kind': 'intent', 'resource': 'container', 'name': name}, {'kind': 'created', 'resource': 'container', 'id': 'c1'})
    result = service_lane.reap(FakeClient(api), OWNER, log)
    assert result['errors'] == [(name, 'intent-lookup', 'ConnectionError')] and result['removed'] == ['c1']


def test_reap_does_not_trust_an_unlabelled_network_as_authority_for_unlabelled_containers():
    api = FakeApi(containers={'c1': container(networks=['net1'])}, networks={'net1': {}})
    result = service_lane.reap(FakeClient(api), OWNER, created(('container', 'c1'), ('network', 'net1')))
    assert result['removed'] == [] and sorted(result['refused']) == ['c1', 'net1'] and api.removed == []


@pytest.mark.parametrize('labels, networks, trusted, expected', [
    ({LABEL: OWNER}, (), (), True), ({LABEL: OWNER}, ('n',), ('n',), True),
    ({LABEL: OTHER}, (), (), False), ({LABEL: OTHER}, ('n',), ('n',), False),
    ({}, ('n',), ('n',), True), ({}, ('n',), (), False), ({}, (), ('n',), False), ({'x': 'y'}, ('n', 'm'), ('m',), True)])
def test_the_runner_and_the_in_process_cleanup_share_one_ownership_policy(labels, networks, trusted, expected):
    attrs = container(labels, networks)
    assert ownership.container_owned(attrs, OWNER, set(trusted)) is expected
    api = FakeApi(containers={'c1': attrs}, networks={n: {LABEL: OWNER} for n in trusted})
    result = service_lane.reap(FakeClient(api), OWNER, created(('container', 'c1'), *(('network', n) for n in networks)))
    assert ('c1' in result['removed']) is expected


@pytest.mark.parametrize('labels, image, expected', [
    ({}, IMAGE, True), ({}, 'sha256:' + 'f' * 64, False), ({LABEL: OWNER}, 'sha256:' + 'f' * 64, True), ({LABEL: OTHER}, IMAGE, False)])
def test_both_ownership_implementations_apply_the_candidate_image_rule_alike(labels, image, expected):
    attrs = container(labels, ('n',), image)
    assert ownership.container_owned(attrs, OWNER, {'n'}, {IMAGE}) is expected
    api = FakeApi(containers={'c1': attrs}, networks={'n': {LABEL: OWNER}})
    result = service_lane.reap(FakeClient(api), OWNER, created(('container', 'c1'), ('network', 'n')), images={IMAGE})
    assert ('c1' in result['removed']) is expected


def test_reap_never_removes_a_container_that_existed_before_the_run():
    api = FakeApi(containers={'old': container({LABEL: OWNER}), 'old-unlabelled': container(networks=['n1'])},
                  networks={'n1': {LABEL: OWNER}})
    result = service_lane.reap(FakeClient(api), OWNER, created(('container', 'old'), ('container', 'old-unlabelled'), ('network', 'n1')),
                               images={IMAGE}, protected=frozenset({'old', 'old-unlabelled'}))
    assert sorted(result['refused']) == ['old', 'old-unlabelled'] and result['removed'] == ['n1']


def test_reap_reads_a_networks_label_again_just_before_removing_it():
    api = FakeApi(containers={'c1': container(networks=['n1'])}, networks={'n1': {LABEL: OWNER}})
    first = api.inspect_network
    calls = []

    def inspect(identifier):
        calls.append(identifier)
        return first(identifier) if len(calls) == 1 else {'Labels': {LABEL: OTHER}}

    api.inspect_network = inspect
    result = service_lane.reap(FakeClient(api), OWNER, created(('container', 'c1'), ('network', 'n1')), images={IMAGE})
    assert result['removed'] == ['c1'] and result['refused'] == ['n1'] and 'n1' not in api.removed


@pytest.mark.parametrize('extra', [['--wheel', 'w.whl', '--inventory', 'missing.json'],
                                   ['--wheel', 'w.whl', '--inventory', 'missing.json', '--wheelhouse', 'only-one-half']])
def test_early_validation_failures_report_their_own_error_for_a_service_lane(tmp_path, capsys, extra):
    argv = ['--lane', 'service-harness', *[str(tmp_path / a) if a.endswith(('.whl', '.json')) else a for a in extra]]
    assert verify_installed.main(argv) == 1
    assert 'UnboundLocalError' not in capsys.readouterr().err


def test_difference_reports_added_removed_and_changed():
    base = {'containers': [{'id': 'a', 'state': 'running'}, {'id': 'b', 'state': 'running'}], 'networks': [('n1', 'x')],
            'volumes': ['v'], 'images': ['i']}
    after = {'containers': [{'id': 'a', 'state': 'exited'}, {'id': 'c', 'state': 'running'}], 'networks': [('n1', 'x')],
             'volumes': ['v', 'w'], 'images': ['i']}
    diff = service_lane.difference(base, after)
    assert diff['containers'] == {'added': ['c'], 'removed': ['b'], 'changed': ['a']}
    assert diff['volumes']['added'] == ['w'] and 'networks' not in diff and 'images' not in diff


@pytest.mark.parametrize('content', [b'', b'{"kind": "x", "owner": "a"}\n{"kind": "y", "owner": "b"}\n', b'{"owner": "a"}\n'])
def test_registry_must_name_exactly_one_owner(tmp_path, content):
    path = tmp_path / 'registry.jsonl'
    path.write_bytes(content)
    with pytest.raises(ValueError):
        service_lane.read_registry(path)


def test_registry_tolerates_a_truncated_final_line_and_marks_it(tmp_path):
    path = tmp_path / 'registry.jsonl'
    path.write_bytes((json.dumps({'kind': 'created', 'owner': OWNER, 'id': 'x'}) + '\n').encode() + b'{"kind": "cre')
    owner, loaded = service_lane.read_registry(path)
    assert owner == OWNER and [row['kind'] for row in loaded] == ['created', 'truncated']


def test_registry_reads_a_single_owner(tmp_path):
    path = tmp_path / 'registry.jsonl'
    path.write_text(json.dumps({'kind': 'created', 'owner': OWNER}) + '\n')
    assert service_lane.read_registry(path)[0] == OWNER


def short_write(fraction=2):
    real = os.write

    def write(fd, data):
        return real(fd, data[:len(data) // fraction])
    return write


def test_a_short_registry_write_is_cut_back_so_later_rows_stay_readable(tmp_path, monkeypatch):
    registry = live_stack.Registry(tmp_path / 'registry.jsonl', OWNER)
    registry.write('intent', resource='volume', name='first')
    with monkeypatch.context() as patched:
        patched.setattr(live_stack.os, 'write', short_write())
        with pytest.raises(RuntimeError, match='short registry write'):
            registry.write('created', resource='volume', name='first', id='first')
    registry.write('close', failed=[])
    owner, rows = service_lane.read_registry(tmp_path / 'registry.jsonl')
    assert owner == OWNER and [row['kind'] for row in rows] == ['intent', 'close']


def test_a_registry_that_cannot_be_cut_back_refuses_every_later_write(tmp_path, monkeypatch):
    registry = live_stack.Registry(tmp_path / 'registry.jsonl', OWNER)
    registry.write('intent', resource='volume', name='first')
    with monkeypatch.context() as patched:
        patched.setattr(live_stack.os, 'write', short_write())
        patched.setattr(live_stack.os, 'ftruncate', lambda *args: (_ for _ in ()).throw(OSError('no')))
        with pytest.raises(RuntimeError, match='short registry write'):
            registry.write('created', resource='volume', name='first', id='first')
    with pytest.raises(RuntimeError, match='unusable'):
        registry.write('close', failed=[])
    # The damaged tail has no newline: recovery still reads the valid row before it and marks the run unclean.
    owner, rows = service_lane.read_registry(tmp_path / 'registry.jsonl')
    assert [row['kind'] for row in rows] == ['intent', 'truncated']


def test_recovery_keeps_valid_rows_around_a_damaged_interior_line_and_marks_the_run_unclean(tmp_path):
    path = tmp_path / 'registry.jsonl'
    good = lambda kind, **values: json.dumps({'kind': kind, 'owner': OWNER, **values}) + '\n'  # noqa: E731
    path.write_bytes((good('intent', resource='volume', name='v') + '{"kind": "creat' + good('close')).encode())
    owner, rows = service_lane.read_registry(path)
    assert owner == OWNER and [row['kind'] for row in rows] == ['intent', 'corrupt']


# -- a shared engine: images pulled by another workload during a lane run -------------------------------------------------------

def snap(images=(('sha256:a', ('base:1',)),), containers=(), networks=(), volumes=()):
    """Images as (id, tags) or (id, tags, digests)."""
    rows = [(i[0], tuple(i[1]), tuple(i[2]) if len(i) > 2 else ()) for i in images]
    return {'containers': list(containers), 'networks': list(networks), 'volumes': list(volumes),
            'images': sorted(i for i, _, _ in rows), 'image_tags': sorted([i, list(t)] for i, t, _ in rows),
            'image_digests': sorted([i, list(d)] for i, _, d in rows)}


class ImageApi:
    def __init__(self, labels=None, fail=False):
        self.labels, self.fail = labels or {}, fail

    def inspect_image(self, identifier):
        if self.fail:
            raise docker.errors.NotFound('gone')
        return {'Config': {'Labels': self.labels.get(identifier, {})}}


def tolerated(before, after, **kwargs):
    return service_lane.tolerated_foreign_images(FakeClient(ImageApi(**kwargs)), before, after, service_lane.difference(before, after))


EXISTING = (('sha256:a', ('base:1',)),)


def test_an_image_added_by_another_workload_is_tolerated_and_recorded():
    before = snap(EXISTING)
    after = snap(EXISTING + (('sha256:b', ('node:24',)), ('sha256:c', ())))
    ok, added, reason = tolerated(before, after)
    assert ok and added == [{'id': 'sha256:b', 'tags': ['node:24'], 'digests': []}, {'id': 'sha256:c', 'tags': [], 'digests': []}], reason


@pytest.mark.parametrize('case', ['removed image', 'removed and added', 'tag moved to a new image', 'tag removed', 'tag added to an old image'])
def test_a_removed_replaced_or_retagged_image_is_not_tolerated(case):
    before = snap(EXISTING)
    after = {'removed image': snap(()), 'removed and added': snap((('sha256:b', ('node:24',)),)),
             'tag moved to a new image': snap((('sha256:a', ()), ('sha256:b', ('base:1',)))),
             'tag removed': snap((('sha256:a', ()),)),
             'tag added to an old image': snap((('sha256:a', ('base:1', 'other:2')),))}[case]
    ok, added, reason = tolerated(before, after)
    assert not ok and added == [], reason


@pytest.mark.parametrize('change', ['container', 'network', 'volume'])
def test_an_added_image_plus_any_container_network_or_volume_change_is_not_tolerated(change):
    before = snap(EXISTING, containers=[{'id': 'c', 'state': 'running'}], networks=[('n', 'x')], volumes=['v'])
    after = snap(EXISTING + (('sha256:b', ('node:24',)),), containers=[{'id': 'c', 'state': 'running'}], networks=[('n', 'x')], volumes=['v'])
    after = {'container': {**after, 'containers': [{'id': 'c', 'state': 'exited'}]},
             'network': {**after, 'networks': [('n', 'x'), ('m', 'y')]},
             'volume': {**after, 'volumes': ['v', 'w']}}[change]
    ok, _, reason = tolerated(before, after)
    assert not ok and 'container, network, volume' in reason


def test_an_added_image_with_the_owner_label_or_a_candidate_tag_is_not_tolerated():
    before = snap(EXISTING)
    labelled_image = snap(EXISTING + (('sha256:b', ('node:24',)),))
    ok, _, reason = tolerated(before, labelled_image, labels={'sha256:b': {LABEL: OWNER}})
    assert not ok and 'owner label' in reason
    candidate = snap(EXISTING + (('sha256:b', ('ghcr.io/neburyx/graph-loader-edge:0.1.0a1.dev0-x',)),))
    ok, _, reason = tolerated(before, candidate)
    assert not ok and 'candidate repository' in reason
    ok, _, reason = tolerated(before, labelled_image, fail=True)
    assert not ok and 'could not be inspected' in reason


def test_a_snapshot_without_image_tags_never_tolerates():
    before, after = snap(EXISTING), snap(EXISTING + (('sha256:b', ('node:24',)),))
    del before['image_tags']
    ok, _, reason = tolerated(before, after)
    assert not ok and 'no image tags' in reason


class SnapshotApi:
    def __init__(self, images):
        self.images_now = images

    def containers(self, all=False):
        return []

    def networks(self):
        return []

    def volumes(self):
        return {'Volumes': []}

    def images(self, all=False):
        return [{'Id': row[0], 'RepoTags': list(row[1]), 'RepoDigests': list(row[2]) if len(row) > 2 else []} for row in self.images_now]

    def inspect_image(self, identifier):
        return {'Config': {'Labels': {}}}


def postflight(tmp_path, before_images, after_images):
    (tmp_path / 'service-run').mkdir()
    (tmp_path / 'service-run' / 'registry.jsonl').write_text(json.dumps({'kind': 'close', 'owner': OWNER, 'at': 0}) + '\n')
    client = FakeClient(SnapshotApi(after_images))
    service = {'client': client, 'before': service_lane.snapshot(FakeClient(SnapshotApi(before_images))), 'preflight': {'images': {}}}
    return verify_installed.service_postflight(service, tmp_path)


def test_the_postflight_records_foreign_images_added_during_the_run_and_stays_clean(tmp_path):
    result = postflight(tmp_path, EXISTING, EXISTING + (('sha256:b', ('node:24',)),))
    assert result['clean'] and result['foreign_unchanged'] is False and result['foreign_difference_tolerated'] is True
    assert result['foreign_images_added'] == [{'id': 'sha256:b', 'tags': ['node:24'], 'digests': []}]
    assert result['difference']['images']['added'] == ['sha256:b']


def test_the_postflight_still_fails_on_a_removed_image_and_reports_nothing_added(tmp_path):
    result = postflight(tmp_path, EXISTING + (('sha256:b', ('node:24',)),), EXISTING)
    assert not result['clean'] and result['foreign_difference_tolerated'] is False and 'foreign_images_added' not in result


def test_the_postflight_without_any_difference_is_clean_without_a_tolerance_record(tmp_path):
    result = postflight(tmp_path, EXISTING, EXISTING)
    assert result['clean'] and result['foreign_unchanged'] is True and result['foreign_difference_tolerated'] is False


CANDIDATE = 'ghcr.io/neburyx/graph-loader-edge'


def test_a_digest_only_image_in_a_candidate_repository_is_not_tolerated():
    before = snap(EXISTING)
    ok, _, reason = tolerated(before, snap(EXISTING + (('sha256:b', (), (CANDIDATE + '@sha256:' + 'd' * 64,)),)))
    assert not ok and 'candidate repository' in reason
    ok, added, reason = tolerated(before, snap(EXISTING + (('sha256:b', (), ('docker.io/library/node@sha256:' + 'e' * 64,)),)))
    assert ok and added == [{'id': 'sha256:b', 'tags': [], 'digests': ['docker.io/library/node@sha256:' + 'e' * 64]}], reason


def test_an_existing_image_that_gains_or_loses_a_digest_is_not_tolerated():
    before = snap(EXISTING)
    gained = snap((('sha256:a', ('base:1',), (CANDIDATE + '@sha256:' + 'd' * 64,)), ('sha256:b', ('node:24',))))
    ok, _, reason = tolerated(before, gained)
    assert not ok and 'digest' in reason
    with_digest = snap((('sha256:a', ('base:1',), ('docker.io/x@sha256:' + 'f' * 64,)),))
    ok, _, reason = tolerated(with_digest, snap((('sha256:a', ('base:1',)), ('sha256:b', ('node:24',)))))
    assert not ok and 'digest' in reason


def test_a_snapshot_whose_image_tables_disagree_is_rejected_and_every_added_id_is_accounted_for():
    before = snap(EXISTING)
    after = snap(EXISTING + (('sha256:b', ('node:24',)),))
    # An image (C, in a candidate repository) present in the tag rows but not in the id list: the old two-call race.
    hidden = {**after, 'image_tags': sorted(after['image_tags'] + [['sha256:c', [CANDIDATE + ':1']]])}
    ok, added, reason = tolerated(before, hidden)
    assert not ok and added == [] and 'inconsistent' in reason
    hidden_digest = {**after, 'image_digests': sorted(after['image_digests'] + [['sha256:c', [CANDIDATE + '@sha256:' + 'a' * 64]]])}
    ok, _, reason = tolerated(before, hidden_digest)
    assert not ok and 'inconsistent' in reason
    duplicated = {**after, 'image_tags': after['image_tags'] + [after['image_tags'][0]]}
    ok, _, reason = tolerated(before, duplicated)
    assert not ok and 'inconsistent' in reason


def test_a_snapshot_reads_the_image_list_once_so_a_changing_list_cannot_split_ids_from_tags():
    class Changing(SnapshotApi):
        def __init__(self):
            self.calls = 0

        def images(self, all=False):
            self.calls += 1
            rows = [('sha256:a', ('base:1',)), ('sha256:b', ('node:24',))]
            if self.calls > 1:
                rows.append(('sha256:c', (CANDIDATE + ':1',)))  # appears between two reads
            return [{'Id': i, 'RepoTags': list(t), 'RepoDigests': []} for i, t in rows]
    api = Changing()
    snapshot = service_lane.snapshot(FakeClient(api))
    assert api.calls == 1
    assert snapshot['images'] == [i for i, _ in snapshot['image_tags']] == [i for i, _ in snapshot['image_digests']] == ['sha256:a', 'sha256:b']


def test_duplicate_image_ids_or_duplicate_tag_or_digest_rows_are_rejected_before_they_can_collapse():
    before = snap(EXISTING)
    good = snap(EXISTING + (('sha256:b', ('node:24',)),))
    candidate_digest = CANDIDATE + '@sha256:' + 'd' * 64
    cases = {
        # a candidate digest row followed by an empty one for the same image: collapsing to a dict would keep the empty one
        'duplicate digest rows': {**good, 'image_digests': sorted(good['image_digests'] + [['sha256:b', [candidate_digest]]])},
        'duplicate tag rows': {**good, 'image_tags': sorted(good['image_tags'] + [['sha256:b', [CANDIDATE + ':1']]])},
        'duplicate image ids': {**good, 'images': sorted(good['images'] + ['sha256:b'])},
        'duplicate ids everywhere': {**good, 'images': sorted(good['images'] + ['sha256:b']),
                                     'image_tags': sorted(good['image_tags'] + [['sha256:b', ['x:1']]]),
                                     'image_digests': sorted(good['image_digests'] + [['sha256:b', []]])},
    }
    for name, after in cases.items():
        ok, added, reason = tolerated(before, after)
        assert not ok and added == [] and 'inconsistent' in reason and 'repeat' in reason, (name, reason)
    bad_before = {**before, 'image_digests': before['image_digests'] + before['image_digests']}
    ok, _, reason = tolerated(bad_before, good)
    assert not ok and 'before snapshot is inconsistent' in reason
    assert tolerated(before, good)[0]
