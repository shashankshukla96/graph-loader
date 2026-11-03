"""The recorded-ID cleanup removes what the run owns and nothing else."""
import json

import docker

from soak import cleanup

OWNER = 'ab' * 16
OTHER = 'cd' * 16
CANDIDATE = 'sha256:' + '1' * 64
LABEL = 'io.graph-loader.e09-owner'


class FakeApi:
    def __init__(self, containers, networks, volumes):
        self.rows, self.nets, self.vols = containers, networks, volumes
        self.removed = []

    def containers(self, all=True):
        return list(self.rows)

    def inspect_network(self, nid):
        if nid not in self.nets:
            raise docker.errors.NotFound('no')
        return {'Id': nid, 'Labels': self.nets[nid]}

    def inspect_volume(self, name):
        if name not in self.vols:
            raise docker.errors.NotFound('no')
        return {'Labels': self.vols[name]}

    def remove_container(self, cid, force=False, v=False):
        self.removed.append(('container', cid))

    def remove_volume(self, name, force=False):
        self.removed.append(('volume', name))

    def remove_network(self, nid):
        self.removed.append(('network', nid))


class FakeClient:
    def __init__(self, api):
        self.api = api


def row(cid, labels=None, image=CANDIDATE, network='net-1'):
    return {'Id': cid, 'Labels': labels or {}, 'ImageID': image, 'NetworkSettings': {'Networks': {'n': {'NetworkID': network}}}}


def test_only_provably_owned_resources_are_selected_and_removed(tmp_path):
    (tmp_path / 'out').mkdir()
    registry = [{'kind': 'created', 'owner': OWNER, 'resource': 'network', 'name': 'e13-net', 'id': 'net-1'},
                {'kind': 'created', 'owner': OWNER, 'resource': 'volume', 'name': 'e13-vol', 'id': 'e13-vol'},
                {'kind': 'created', 'owner': OWNER, 'resource': 'volume', 'name': 'e13-vol-foreign-now', 'id': 'e13-vol-foreign-now'}]
    (tmp_path / 'registry.jsonl').write_text(''.join(json.dumps(r) + '\n' for r in registry))
    (tmp_path / 'out' / 'foreign-before.json').write_text(json.dumps({'containers': {'pre-existing': {}}}))
    api = FakeApi(
        [row('stack-labelled', {LABEL: OWNER}), row('fleet-unlabelled'),                         # ours: owner label / owned network + candidate image
         row('pre-existing'),                                                                      # existed before the run: never ours
         row('foreign-labelled', {LABEL: OTHER}),                                                  # another run's label wins
         row('other-image', image='sha256:' + '2' * 64),                                           # on our network, not a candidate image
         row('elsewhere', network='net-2')],                                                       # unlabelled, another network
        {'net-1': {LABEL: OWNER}}, {'e13-vol': {LABEL: OWNER}, 'e13-vol-foreign-now': {LABEL: OTHER}})
    found = cleanup.plan(FakeClient(api), tmp_path, {CANDIDATE})
    assert sorted(found['containers']) == ['fleet-unlabelled', 'stack-labelled']
    assert found['volumes'] == ['e13-vol'] and found['networks'] == ['net-1']
    assert found['skipped'] == [{'id': 'other-image', 'reason': 'on the owned network but not from a candidate image'}]
    receipt = cleanup.execute(FakeClient(api), found)
    assert receipt['failed'] == [] and ('container', 'pre-existing') not in api.removed and ('container', 'foreign-labelled') not in api.removed
    assert api.removed == [('container', 'stack-labelled'), ('container', 'fleet-unlabelled'), ('volume', 'e13-vol'), ('network', 'net-1')]
