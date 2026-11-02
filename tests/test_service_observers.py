"""Read-only service observer controls; no application-service acceptance."""
from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace

import pytest

from tests.installed_service import observers

OWNER = '1'*32
DATABASE = 'e09'+OWNER[:16]
PREFIX = 'e09-'+OWNER+'-'


class Result:
    """Iterable like a driver result; ``consume`` is recorded so an abandoned observation can be checked."""
    def __init__(self, rows, consumed):
        self.rows, self.consumed = iter(rows), consumed
    def __iter__(self): return self.rows
    def consume(self): self.consumed.append(True)


class Session:
    def __init__(self, nodes, edges):
        self.nodes, self.edges = nodes, edges
        self.calls, self.consumed = [], []
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def run(self, query, *, limit):
        self.calls.append((query.text, limit, query.timeout))
        return Result(self.edges if 'MATCH (a)-[r]' in query.text else self.nodes, self.consumed)


class Driver:
    def __init__(self, nodes, edges): self.value = Session(nodes, edges)
    def session(self, *, database, fetch_size):
        assert database == DATABASE and fetch_size == 1
        return self.value


def test_graph_observations_reads_bounded_properties_and_sorts():
    driver = Driver([{'labels':['Leaf'], 'properties':{'id':2}}, {'labels':['Leaf'], 'properties':{'id':1}},
                     {'labels':['Hub'], 'properties':{'id':0}}],
                    [{'type':'POINTS_AT', 'properties':{'seq':1}, 'source':{'id':1}, 'target':{'id':0},
                      'source_labels':['Leaf'], 'target_labels':['Hub']}])
    result = observers.graph_observations(driver, owner=OWNER, database=DATABASE, maximum=10)
    assert [row['properties']['id'] for row in result['nodes']] == [0, 1, 2]
    assert result['relationships'][0]['properties'] == {'seq':1}
    assert all(limit == 11 and timeout == 10 for _, limit, timeout in driver.value.calls)
    assert all('CREATE ' not in query and 'DELETE ' not in query for query, _, _ in driver.value.calls)


def test_graph_observations_rejects_foreign_database_overflow_and_nonfinite_values():
    with pytest.raises(ValueError):
        observers.graph_observations(Driver([], []), owner=OWNER, database='neo4j')
    with pytest.raises(RuntimeError, match='row bound'):
        observers.graph_observations(Driver([{'labels':['A'], 'properties':{'id':1}},
                                             {'labels':['A'], 'properties':{'id':2}}], []), owner=OWNER, database=DATABASE, maximum=1)
    with pytest.raises(RuntimeError, match='JSON'):
        observers.graph_observations(Driver([{'labels':['A'], 'properties':{'id':float('nan')}}], []), owner=OWNER, database=DATABASE)
    oversized = Driver([{'labels':['A'], 'properties':{'id':1, 'value':'x'*(2*1024*1024)}}], [])
    with pytest.raises(RuntimeError, match='byte bound'):
        observers.graph_observations(oversized, owner=OWNER, database=DATABASE)
    assert oversized.value.consumed == [True], 'an abandoned observation still completes its query'


def test_graph_observations_reject_duplicate_keys_and_canonicalize_labels():
    duplicate = [{'labels':['A'], 'properties':{'id':1}}, {'labels':['A'], 'properties':{'id':1}}]
    with pytest.raises(RuntimeError, match='not unique'):
        observers.graph_observations(Driver(duplicate, []), owner=OWNER, database=DATABASE)
    graph = observers.graph_observations(Driver([{'labels':['Z','A'], 'properties':{'id':1}}], []),
                                         owner=OWNER, database=DATABASE)
    assert graph['nodes'][0]['labels'] == ['A','Z']


def fixture(tmp_path):
    plan = {'plan_id':'one', 'chunks':[[0, 'EDGE', [], 2], [1, 'EDGE', [], 1]]}
    attempt = tmp_path/'graph-loader-test'; (attempt/'plans').mkdir(parents=True)
    path = attempt/'plans'/'edge-plan.json'; path.write_text(json.dumps(plan))
    ledger = {'plan_id':'one', 'plan_digest':hashlib.sha256(path.read_bytes()).hexdigest(),
              'state':'complete', 'chunks':2, 'rows':3, 'ledger':[[0, 'w0', 2], [1, 'w1', 1]]}
    ledger_path = attempt/'plans'/'edge-plan.json.ledger.json'; ledger_path.write_text(json.dumps(ledger))
    return attempt, path, ledger_path, ledger


def test_plan_ledger_checks_digest_exact_chunks_and_rows(tmp_path):
    attempt, plan, ledger_path, ledger = fixture(tmp_path)
    assert observers.plan_ledger(attempt, expected_rows=3, expected_digest=ledger['plan_digest'], expected_plan_id='one') == {
        'plan_digest':ledger['plan_digest'], 'chunks':2, 'rows':3}
    with pytest.raises(RuntimeError, match='plan ledger'):
        observers.plan_ledger(attempt, expected_rows=3, expected_digest='0'*64, expected_plan_id='one')
    with pytest.raises(RuntimeError, match='plan ledger'):
        observers.plan_ledger(attempt, expected_rows=3, expected_digest=ledger['plan_digest'], expected_plan_id='other')


@pytest.mark.parametrize('failure', ['digest', 'duplicate', 'missing', 'wrong_rows', 'nan', 'symlink'])
def test_plan_ledger_rejects_changed_or_ambiguous_evidence(tmp_path, failure):
    attempt, plan, ledger_path, ledger = fixture(tmp_path)
    expected_digest = ledger['plan_digest']
    if failure == 'digest': ledger['plan_digest'] = '0'*64
    if failure == 'duplicate': ledger['ledger'].append(ledger['ledger'][0])
    if failure == 'missing': ledger['ledger'].pop()
    if failure == 'wrong_rows': ledger['ledger'][0][2] = 3
    if failure == 'nan': ledger['other'] = float('nan')
    ledger_path.write_text(json.dumps(ledger))
    if failure == 'symlink':
        target = tmp_path/'target.json'; target.write_bytes(plan.read_bytes()); plan.unlink(); plan.symlink_to(target)
    with pytest.raises((RuntimeError, ValueError, OSError)):
        observers.plan_ledger(attempt, expected_rows=3, expected_digest=expected_digest, expected_plan_id='one')


def test_committed_offsets_use_exact_owned_group_and_partitions(monkeypatch):
    calls = []
    class Consumer:
        def __init__(self, config): calls.append(config)
        def committed(self, partitions, *, timeout):
            calls.append(timeout)
            return [SimpleNamespace(topic=p.topic, partition=p.partition, offset=7, error=None) for p in partitions]
        def close(self): calls.append('closed')
    monkeypatch.setattr(observers, 'Consumer', Consumer)
    rows = observers.committed_offsets(owner=OWNER, bootstrap='127.0.0.1:9093', group=PREFIX+'group',
                                       partitions=[(PREFIX+'topic', 0)], broker_guard=lambda: True)
    assert rows == [{'topic':PREFIX+'topic', 'partition':0, 'offset':7}]
    assert calls[0]['enable.auto.commit'] is False and calls[0]['enable.auto.offset.store'] is False
    assert calls[1:] == [5, 'closed']
    with pytest.raises(ValueError):
        observers.committed_offsets(owner=OWNER, bootstrap='127.0.0.1:9093', group='foreign',
                                    partitions=[(PREFIX+'topic', 0)], broker_guard=lambda: True)
    with pytest.raises(RuntimeError, match='ownership'):
        observers.committed_offsets(owner=OWNER, bootstrap='127.0.0.1:9093', group=PREFIX+'group',
                                    partitions=[(PREFIX+'topic', 0)], broker_guard=lambda: False)


@pytest.mark.parametrize('offset,expected', [(observers.OFFSET_INVALID, None), (0, 0)])
def test_offset_observer_distinguishes_uncommitted_from_zero(monkeypatch, offset, expected):
    class Consumer:
        def __init__(self, config): pass
        def committed(self, partitions, *, timeout):
            return [SimpleNamespace(topic=p.topic, partition=p.partition, offset=offset, error=None) for p in partitions]
        def close(self): pass
    monkeypatch.setattr(observers, 'Consumer', Consumer)
    rows = observers.committed_offsets(owner=OWNER, bootstrap='127.0.0.1:9093', group=PREFIX+'group',
                                       partitions=[(PREFIX+'topic', 0)], broker_guard=lambda: True)
    assert rows[0]['offset'] == expected


def test_offset_failure_preserves_primary_when_close_also_fails(monkeypatch):
    class Consumer:
        def __init__(self, config): pass
        def committed(self, partitions, *, timeout): raise RuntimeError('original failure')
        def close(self): raise OSError('close failure')
    monkeypatch.setattr(observers, 'Consumer', Consumer)
    with pytest.raises(RuntimeError, match='original failure') as captured:
        observers.committed_offsets(owner=OWNER, bootstrap='127.0.0.1:9093', group=PREFIX+'group',
                                    partitions=[(PREFIX+'topic', 0)], broker_guard=lambda: True)
    assert any('close also failed' in note for note in captured.value.__notes__)
