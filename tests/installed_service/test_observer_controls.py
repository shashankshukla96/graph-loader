"""Synthetic controls for the live lane's ownership and observation machinery (fake Docker objects).

The application-container observer must adopt only the run's own containers and
must prove, through an event-stream barrier, that it has seen everything the CLI
did; cleanup paths must refuse anything that does not carry the run's owner label.
"""
from __future__ import annotations

import queue
import threading
import time

import docker
import pytest

from tests.installed_service import ownership
from tests.installed_service.live_cli import AppObserver, project
from tests.installed_service.live_stack import LiveStack

pytestmark = pytest.mark.integration

OWNER = 'a' * 32
OTHER = 'b' * 32
NETWORK = 'n' * 64
CANDIDATE = 'sha256:' + 'c' * 64
LABEL = ownership.OWNER_LABEL


class Stream:
    """An ordered event stream whose delivery can be slowed or severed."""

    def __init__(self, delay=0):
        self.queue = queue.Queue()
        self.delay = delay

    def __iter__(self):
        return self

    def __next__(self):
        item = self.queue.get()
        if item is None:
            raise StopIteration
        time.sleep(self.delay)
        return item

    def push(self, event):
        self.queue.put(event)

    def close(self):
        self.queue.put(None)


class FakeApi:
    def __init__(self, containers, stream=None, *, network_labels=None, drop_barrier=False, existing=()):
        self.known, self.stream = containers, stream
        self.existing = list(existing)
        self.network_labels = {LABEL: OWNER} if network_labels is None else network_labels
        self.drop_barrier = drop_barrier
        self.volumes, self.removed, self.killed = {}, [], []

    def inspect_container(self, identifier):
        if identifier not in self.known:
            raise docker.errors.NotFound('gone')
        return self.known[identifier]

    def kill(self, identifier, signal):
        self.killed.append((identifier, signal))
        self.known[identifier]['State'] = {'Status': 'exited', 'OOMKilled': False}

    def wait(self, identifier, timeout):
        if identifier not in self.known:
            raise docker.errors.NotFound('gone')
        return {'StatusCode': 137}

    def containers(self, all=False):
        return [{'Id': identifier} for identifier in self.existing]

    def inspect_network(self, identifier):
        return {'Labels': self.network_labels}

    def inspect_volume(self, name):
        if name not in self.volumes:
            raise docker.errors.NotFound('gone')
        return {'Name': name, 'Labels': self.volumes[name]}

    def remove_container(self, identifier, **kwargs):
        self.removed.append(identifier)
        self.known.pop(identifier, None)

    def create_volume(self, name, labels):
        if name in self.volumes:
            return {'Name': name, 'Labels': self.volumes[name]}
        self.volumes[name] = labels
        if not self.drop_barrier:
            self.stream.push({'Type': 'volume', 'Action': 'create', 'Actor': {'ID': name, 'Attributes': {}}})
        return {'Name': name, 'Labels': labels}

    def remove_volume(self, name, **kwargs):
        self.removed.append(name)
        del self.volumes[name]
        if not self.drop_barrier:
            self.stream.push({'Type': 'volume', 'Action': 'destroy', 'Actor': {'ID': name, 'Attributes': {}}})


class FakeClient:
    def __init__(self, api):
        self.api, self.stream = api, api.stream

    def events(self, **kwargs):
        return self.stream


class Registry:
    def __init__(self):
        self.rows = []
        self.owner = OWNER

    def write(self, kind, **values):
        self.rows.append((kind, values))


def attrs(identifier, *, networks=(NETWORK,), labels=None):
    return {'Id': identifier, 'Name': '/app-' + identifier[:4], 'Image': CANDIDATE, 'Created': 't',
            'Config': {'Image': CANDIDATE, 'Labels': labels or {'component': 'node-loader', 'secret-label': 'x'}},
            'HostConfig': {'Privileged': False, 'NetworkMode': 'net', 'AutoRemove': True},
            'Mounts': [{'Type': 'bind', 'Source': '/run/x', 'Destination': '/app/x', 'RW': True}],
            'NetworkSettings': {'Networks': {f'n{i}': {'NetworkID': n} for i, n in enumerate(networks)}}}


def observer(containers, *, delay=0, **kwargs):
    api_options = {key: kwargs.pop(key) for key in ('network_labels', 'drop_barrier', 'existing') if key in kwargs}
    registry = Registry()
    api = FakeApi(containers, Stream(delay), **api_options)
    return AppObserver(FakeClient(api), registry, NETWORK, owner=OWNER, candidate_images={CANDIDATE}, **kwargs), registry, api


def container_event(identifier, action, **attributes):
    return {'Type': 'container', 'Action': action, 'Actor': {'ID': identifier, 'Attributes': attributes}}


def network_event(network, identifier, action='connect'):
    return {'Type': 'network', 'Action': action, 'Actor': {'ID': network, 'Attributes': {'container': identifier}}}


def test_foreign_container_events_are_never_adopted():
    watcher, registry, _ = observer({'f' * 64: attrs('f' * 64, networks=('x' * 64,))})
    for action in ('exec_create', 'health_status', 'start', 'die'):
        watcher._handle(container_event('f' * 64, action, name='postgres', image='postgres:latest'))
    watcher._handle(network_event('x' * 64, 'f' * 64))
    assert watcher.containers == {} and registry.rows == []


def test_owned_network_attachment_adopts_and_inspects_with_an_allowlisted_projection():
    identifier = 'a' * 64
    watcher, registry, _ = observer({identifier: attrs(identifier)})
    watcher._handle(network_event(NETWORK, identifier))
    row = watcher.containers[identifier]['inspected']
    assert row['labels'] == {'component': 'node-loader'} and row['networks'] == [NETWORK] and row['auto_remove'] is True
    assert 'Env' not in str(row) and registry.rows[0][1]['inspected'] is True and registry.rows[0][1]['name'] == 'app-aaaa'


def test_container_that_vanishes_before_inspection_is_recorded_as_uninspectable():
    watcher, registry, _ = observer({})
    watcher._handle(network_event(NETWORK, 'b' * 64))
    assert watcher.containers['b' * 64]['inspected'] is False and registry.rows[0][1]['inspected'] is False


def test_candidate_creations_not_seen_on_the_owned_network_are_reported_unobserved():
    watcher, _, _ = observer({})
    watcher._handle(container_event('d' * 64, 'create', name='stray-node', image=CANDIDATE))
    watcher._handle(container_event('e' * 64, 'create', name='other', image='postgres:latest'))
    assert watcher.unobserved_candidates() == ['stray-node']


def test_attempt_label_adopts_only_matching_containers():
    watcher, _, _ = observer({'g' * 64: attrs('g' * 64, networks=())}, attempt='graph-loader-run1')
    watcher._handle(container_event('g' * 64, 'create', name='x', image=CANDIDATE, attempt='graph-loader-other'))
    assert watcher.containers == {}
    watcher._handle(container_event('g' * 64, 'create', name='x', image=CANDIDATE, attempt='graph-loader-run1'))
    assert list(watcher.containers) == ['g' * 64]


def test_projection_excludes_environment_and_command():
    value = attrs('h' * 64)
    value['Config']['Env'] = ['NEO4J_PASSWORD=hunter2']
    value['Config']['Cmd'] = ['--password', 'hunter2']
    assert 'hunter2' not in str(project(value))


@pytest.mark.parametrize('repeat', range(5))
def test_barrier_waits_for_a_slow_stream_to_deliver_everything_before_it(repeat):
    late = 'c' * 64
    watcher, _, api = observer({late: attrs(late)}, delay=0.05)
    watcher.start()
    try:
        api.stream.push(container_event(late, 'create', name='late-node', image=CANDIDATE))
        api.stream.push(network_event(NETWORK, late))
        api.stream.push(container_event(late, 'die', name='late-node', image=CANDIDATE))
        watcher.barrier(timeout=10)
    finally:
        assert watcher.stop() is True
    assert late in watcher.containers and watcher.unobserved_candidates() == [] and late in watcher.ended
    assert api.volumes == {} and [row[0] for row in watcher.registry.rows if row[1].get('resource') == 'volume'] == ['intent', 'created', 'removed']


@pytest.mark.parametrize('repeat', range(5))
def test_stopping_without_a_barrier_loses_events_from_a_slow_stream(repeat):
    """Positive control for the race the barrier closes: a plain stop discards undelivered events."""
    late = 'd' * 64
    watcher, _, api = observer({late: attrs(late)}, delay=0.3)
    watcher.start()
    api.stream.push(network_event(NETWORK, late))
    assert watcher.stop() is True
    assert late not in watcher.containers


def test_barrier_fails_when_the_stream_never_delivers_it_and_still_removes_its_volume():
    watcher, registry, api = observer({}, drop_barrier=True)
    watcher.start()
    try:
        with pytest.raises(TimeoutError):
            watcher.barrier(timeout=0.3)
    finally:
        watcher.stop()
    assert api.volumes == {} and registry.rows[-1][0] == 'removed'


def test_barrier_fails_when_the_stream_was_lost_before_it():
    watcher, _, api = observer({})
    watcher.start()
    api.stream.close()
    watcher._thread.join(timeout=5)
    assert watcher.closed_early is True
    try:
        with pytest.raises(RuntimeError, match='ended before the barrier'):
            watcher.barrier(timeout=5)
    finally:
        watcher.stop()
    assert api.volumes == {}


def test_stop_reports_a_reader_thread_that_did_not_end():
    class Stuck(Stream):
        def __next__(self):
            threading.Event().wait()

    watcher, _, _ = observer({})
    watcher.client.stream = Stuck()
    watcher.join_timeout = 0.2
    watcher.start()
    assert watcher.stop() is False and watcher.snapshot()['stopped'] is False


def test_start_refuses_a_network_without_the_run_owner_label():
    watcher, _, _ = observer({}, network_labels={LABEL: OTHER})
    with pytest.raises(RuntimeError, match='owner label'):
        watcher.start()
    assert watcher._thread is None


def test_a_container_with_another_owner_label_on_the_owned_network_is_refused_and_never_removed():
    identifier = 'e' * 64
    watcher, registry, api = observer({identifier: attrs(identifier, labels={LABEL: OTHER, 'component': 'x'})})
    watcher._handle(network_event(NETWORK, identifier))
    watcher._handle(network_event(NETWORK, identifier, 'disconnect'))
    assert watcher.containers == {} and watcher.refused == {identifier: 'owner label ' + OTHER}
    assert [kind for kind, _ in registry.rows] == ['refused']
    assert watcher.reap() == {'removed': [], 'failed': []} and api.removed == []


def test_reap_rechecks_the_owner_label_just_before_removing():
    identifier = 'f' * 64
    watcher, _, api = observer({identifier: attrs(identifier)})
    watcher._handle(network_event(NETWORK, identifier))
    api.known[identifier] = attrs(identifier, labels={LABEL: OTHER})
    result = watcher.reap()
    assert result['removed'] == [] and result['failed'][0][0] == identifier and api.removed == []


def test_reap_removes_an_unlabelled_container_attached_to_the_owned_network():
    identifier = 'g' * 64
    watcher, _, api = observer({identifier: attrs(identifier)})
    watcher._handle(network_event(NETWORK, identifier))
    assert watcher.reap() == {'removed': [identifier], 'failed': []} and api.removed == [identifier]


class FakeResource:
    def __init__(self, identifier, labels, *, config=True):
        self.id = self.name = identifier
        self.status = 'running'
        self.attrs = {'Config': {'Labels': labels}} if config else {'Labels': labels}
        self.calls = []

    def reload(self):
        pass

    def kill(self, **kwargs):
        self.calls.append('kill')

    def wait(self, timeout):
        return {'StatusCode': 137}

    def remove(self, **kwargs):
        self.calls.append('remove')


def stack_with(**resources):
    stack = LiveStack(None, Registry(), deadline=time.monotonic() + 60)
    for name, value in resources.items():
        setattr(stack, name, value)
    return stack


def test_kill_and_remove_refuse_a_container_without_the_run_owner_label():
    foreign = FakeResource('x', {LABEL: OTHER})
    stack = stack_with()
    with pytest.raises(RuntimeError, match='owner label'):
        stack.kill_owned(foreign)
    with pytest.raises(RuntimeError, match='owner label'):
        stack.remove_owned(foreign)
    assert foreign.calls == [] and stack.registry.rows == []


def test_kill_owned_records_the_action_and_reads_back_the_terminal_state():
    owned = FakeResource('y', {LABEL: OWNER})
    owned.attrs['State'] = {'OOMKilled': False}
    owned.status = 'exited'
    assert stack_with().kill_owned(owned, timeout=5) == {'exit_code': 137, 'status': 'exited', 'oom_killed': False}
    assert owned.calls == ['kill']


def test_close_removes_only_resources_that_still_carry_the_run_owner_label():
    neo, kafka = FakeResource('neo', {LABEL: OWNER}), FakeResource('kafka', {LABEL: OTHER})
    owned_volume = FakeResource('vol1', {LABEL: OWNER}, config=False)
    foreign_volume = FakeResource('vol2', {LABEL: OTHER}, config=False)
    network = FakeResource('net', {LABEL: OTHER}, config=False)
    network.remove = lambda: network.calls.append('remove')
    stack = stack_with(neo=neo, kafka=kafka, network=network)
    stack.volumes = {('neo', '/data'): owned_volume, ('kafka', '/k'): foreign_volume}
    result = stack.close()
    assert sorted(result['removed']) == ['neo', 'vol1']
    assert sorted(item[0] for item in result['failed']) == ['kafka', 'net', 'vol2']
    assert kafka.calls == [] and foreign_volume.calls == [] and network.calls == []


def test_barrier_never_removes_an_existing_volume_that_lacks_the_run_owner_label():
    watcher, registry, api = observer({})
    name = f'e09-{OWNER}-barrier-1'
    api.volumes[name] = {LABEL: OTHER}
    watcher.start()
    try:
        with pytest.raises(RuntimeError, match='already exists'):
            watcher.barrier(timeout=0.3)
    finally:
        watcher.stop()
    assert api.volumes == {name: {LABEL: OTHER}} and api.removed == []
    assert [row[0] for row in registry.rows] == ['intent', 'refused']


def test_barrier_rechecks_the_volume_label_just_before_removing_it():
    watcher, registry, api = observer({}, drop_barrier=True)
    name = f'e09-{OWNER}-barrier-1'
    original = api.create_volume

    def create_then_replace(name, labels):
        result = original(name, labels)
        api.volumes[name] = {LABEL: OTHER}
        return result

    api.create_volume = create_then_replace
    watcher.start()
    try:
        with pytest.raises(RuntimeError, match='no longer carries'):
            watcher.barrier(timeout=0.2)
    finally:
        watcher.stop()
    assert api.removed == [] and api.volumes[name] == {LABEL: OTHER}
    assert registry.rows[-1][0] == 'refused'


def test_barrier_removes_its_volume_even_when_the_created_row_cannot_be_written():
    watcher, registry, api = observer({})
    write = registry.write

    def failing(kind, **values):
        if kind == 'created':
            raise RuntimeError('short registry write')
        write(kind, **values)

    registry.write = failing
    watcher.start()
    try:
        with pytest.raises(RuntimeError, match='short registry write'):
            watcher.barrier(timeout=0.5)
    finally:
        watcher.stop()
    assert api.volumes == {} and api.removed == [f'e09-{OWNER}-barrier-1']


def test_an_unlabelled_container_from_a_non_candidate_image_on_the_owned_network_is_refused_and_never_removed():
    identifier = 'a1' * 32
    value = attrs(identifier)
    value['Image'] = 'sha256:' + 'e' * 64
    watcher, registry, api = observer({identifier: value})
    watcher._handle(network_event(NETWORK, identifier))
    assert watcher.containers == {} and watcher.refused == {identifier: 'unlabelled and not created from a candidate image'}
    api.known[identifier] = value
    assert watcher.reap() == {'removed': [], 'failed': []} and api.removed == []


def test_a_container_that_existed_before_the_run_is_never_adopted_or_removed():
    identifier = 'b1' * 32
    watcher, registry, api = observer({identifier: attrs(identifier)}, existing=[identifier])
    watcher.start()
    try:
        watcher._handle(network_event(NETWORK, identifier))
    finally:
        watcher.stop()
    assert identifier in watcher.protected and watcher.containers == {} and watcher.refused == {identifier: 'existed before the run'}
    watcher.containers[identifier] = {'id': identifier, 'inspected': None}
    assert watcher.reap()['removed'] == [] and api.removed == []


def adopted(identifier, **kwargs):
    value = attrs(identifier)
    value['State'] = {'Status': 'running', 'OOMKilled': False}
    watcher, registry, api = observer({identifier: value}, **kwargs)
    watcher.start()
    watcher._handle(network_event(NETWORK, identifier))
    return watcher, registry, api


@pytest.mark.parametrize('repeat', range(5))
def test_kill_observed_terminates_an_adopted_container_and_reads_its_state_back(repeat):
    identifier = 'c1' * 32
    watcher, registry, api = adopted(identifier)
    try:
        assert watcher.kill_observed(identifier, timeout=7) == {'exit_code': 137, 'status': 'exited', 'oom_killed': False}
    finally:
        watcher.stop()
    assert api.killed == [(identifier, 'SIGKILL')]
    assert [v for k, v in registry.rows if k == 'control'] == [{'action': 'kill', 'id': identifier, 'signal': 'SIGKILL', 'deadline': 7}]


@pytest.mark.parametrize('case', ['not adopted', 'refused', 'pre-existing', 'label changed', 'image changed', 'network changed'])
def test_kill_observed_refuses_anything_not_provably_ours(case):
    identifier = 'd1' * 32
    watcher, registry, api = adopted(identifier, **({'existing': [identifier]} if case == 'pre-existing' else {}))
    try:
        if case == 'not adopted':
            watcher.containers.clear()
        elif case == 'refused':
            watcher.refused[identifier] = 'owner label ' + OTHER
        elif case == 'pre-existing':
            watcher.protected = frozenset({identifier})
        elif case == 'label changed':
            api.known[identifier]['Config']['Labels'][LABEL] = OTHER
        elif case == 'image changed':
            api.known[identifier]['Image'] = 'sha256:' + 'e' * 64
        else:
            api.known[identifier]['NetworkSettings']['Networks'] = {'n0': {'NetworkID': 'x' * 64}}
        with pytest.raises(RuntimeError, match='refusing to terminate'):
            watcher.kill_observed(identifier)
    finally:
        watcher.stop()
    assert api.killed == [] and not [k for k, _ in registry.rows if k == 'control']


def test_kill_observed_reports_a_container_the_cli_already_removed():
    identifier = 'e1' * 32
    watcher, _, api = adopted(identifier)
    try:
        api.kill = lambda *a, **k: api.known.pop(identifier)
        assert watcher.kill_observed(identifier) == {'exit_code': None, 'status': 'removed', 'oom_killed': None}
    finally:
        watcher.stop()


def test_observers_of_one_owner_use_distinct_barrier_volumes_when_tagged():
    first, _, _ = observer({}, barrier_tag='one-')
    second, _, _ = observer({}, barrier_tag='two-')
    names = []
    for watcher in (first, second):
        watcher.start()
        try:
            watcher.barrier(timeout=10)
        finally:
            watcher.stop()
        names.extend(v['name'] for k, v in watcher.registry.rows if k == 'intent' and v.get('resource') == 'volume')
    assert names == [f'e09-{OWNER}-barrier-one-1', f'e09-{OWNER}-barrier-two-1']
