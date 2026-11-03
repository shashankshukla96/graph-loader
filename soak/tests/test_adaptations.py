"""The soak's adaptations of the E09 harness: the e13 namespace, deadlines, metrics, paginated graph reads, incremental query-log
reading across rotation and restarts, and sampling gaps as coverage failures."""
import json
import re
from datetime import datetime, timedelta, timezone

import pytest
from neo4j.time import Date, DateTime

from soak import budget, faults, spec
from soak.graph_read import Neo4jGraph
from soak.querylog import QueryLogReader, classify, make_rolling_config
from soak.sampler import Guard, Sampler, parse_metrics
from soak.tests import helpers as h

MARKER = 'e09' + 'ab' * 16
OWNER = 'cd' * 16


# ---- the e13 namespace ---------------------------------------------------------------------------------------------------------

class StubRegistry:
    owner = OWNER

    def write(self, *a, **k):
        pass


def test_owned_names_and_topics_live_in_the_e13_namespace():
    from soak.stack import SoakStack
    stack = SoakStack(None, StubRegistry(), deadline=1)
    assert stack.neo_name == f'e13-{OWNER}-neo' and stack.kafka_name == f'e13-{OWNER}-kafka'
    assert stack.topic_prefix == f'e13-{OWNER}-' and stack.labels['io.graph-loader.e13'] == '1'
    with pytest.raises(ValueError):
        stack.create_topics({f'e09-{OWNER}-customers': 1})
    with pytest.raises(ValueError):
        stack.create_topics({'customers': 1})
    assert all(t.startswith(f'e13-{OWNER}-') for t in spec.source_topics(OWNER))
    assert all(t.startswith(f'e13-{OWNER}-') and p.startswith(f'e13-{OWNER}-') for t, p, _ in spec.dlq_topics(OWNER).values())


def test_the_e09_offset_helper_refuses_e13_names_so_the_soak_has_its_own():
    from tests.installed_service.observers import committed_offsets
    with pytest.raises(ValueError):
        committed_offsets(owner=OWNER, bootstrap='127.0.0.1:9', group=f'e13-{OWNER}-Customer',
                          partitions=[(f'e13-{OWNER}-customers', 0)], broker_guard=lambda: True)
    from soak.checker import Checker
    groups = Checker(token=h.TOKEN, group_id=h.GROUP, journal=None, oracle=None, graph=None, kafka=None, prepare=None).groups()
    assert groups and all(g.startswith(f'e13-{h.TOKEN}-') for g in groups)
    assert all(t.startswith(f'e13-{h.TOKEN}-') for tps in groups.values() for t, _ in tps)


def test_the_stack_gets_memory_limits_and_a_rolling_query_log():
    from soak.stack import GiB, SoakStack
    stack = SoakStack(None, StubRegistry(), deadline=1)
    assert stack.memory_limits() == {'neo4j': 3 * GiB, 'kafka': int(1.5 * GiB)}          # the SoakStack constructor defaults (the epic's)
    from soak.runtime import SoakConfig
    assert SoakConfig().neo_memory < 3 * GiB and SoakConfig().kafka_memory < int(1.5 * GiB)  # the run's defaults fit the budget
    template = open('config/neo4j/server-logs.xml', 'rb').read()
    config = make_rolling_config(template, MARKER, size='64 MB', keep=50).decode()
    assert '<RollingFile' in config and 'SizeBasedTriggeringPolicy size="64 MB"' in config and 'fileIndex="max"' in config
    assert 'immediateFlush="true"' in config and f'value="{MARKER}"' in config and 'JsonLayout' in config
    with pytest.raises(ValueError):
        make_rolling_config(template + b' ', MARKER)


# ---- deadlines, metrics, cpuset -------------------------------------------------------------------------------------------------

def test_no_default_deadline_of_900_seconds_and_the_fleet_gets_metrics_and_a_cpuset(tmp_path, monkeypatch):
    import soak.fleet as fleet_mod
    from soak.runtime import SoakConfig
    assert SoakConfig().cli_deadline > 900 and SoakConfig().stack_deadline > 900 and SoakConfig().ready_timeout > 0
    seen = {}

    class FakeCli:
        def __init__(self, executable, args, **kw):
            seen.update(args=args, **kw)

    class FakeObserver:
        def __init__(self, *a, **k):
            pass

        def start(self):
            pass
    monkeypatch.setattr(fleet_mod, 'RunningCli', FakeCli)
    monkeypatch.setattr(fleet_mod, 'AppObserver', FakeObserver)

    class Net:
        id, name = 'n1', 'e13-net'

    class Stack:
        owner, password = OWNER, 'pw'
        network = Net
        neo_uri = container_neo_uri = 'bolt://x'
        database, bootstrap, container_bootstrap = 'db', 'k:1', 'k:2'

    class Ctx:
        client = registry = None
        stack = Stack
        config = {'images': {'cli': {'id': 'i1'}, 'node': {'id': 'i2'}, 'edge': {'id': 'i3'}, 'clock': {'id': 'i4'}},
                  'docker_host': 'unix:///x', 'venv_bin': '/v/bin'}
        run_dir, log_dir = tmp_path, tmp_path / 'logs'
        run_id, cpuset, cli_metrics_port, member_wait, cli_deadline, ready_timeout = 'e13abc', '4-11', 9555, 20.0, 43200.0, 300

    fleet_mod.Fleet(Ctx, 3, tmp_path / 's.yaml', expected={})
    assert seen['deadline'] == 43200.0
    assert seen['env']['GRAPH_LOADER_METRICS_ENABLED'] == '1' and seen['env']['LOADER_CPUSET'] == '4-11'
    assert seen['env']['GRAPH_LOADER_NODE_IMAGE'] == 'i2' and seen['env']['GRAPH_LOADER_METRICS_PORT'] == '9555'
    args = seen['args']
    assert args[:4] == ['start', '--mode', 'stream', '--config'] and '--run-id' in args and args[args.index('--run-id') + 1] == 'e13abc'
    assert '--skip-image-build' in args


# ---- paginated graph reads ------------------------------------------------------------------------------------------------------

class FakeRecord(dict):
    pass


class FakeSession:
    def __init__(self, store):
        self.store = store

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def run(self, query, **params):
        from neo4j import Query
        assert isinstance(query, Query)
        text = str(getattr(query, 'text', query))
        self.store['calls'].append((text, dict(params)))
        if 'count(' in text:
            return [FakeRecord(c=0)]
        if re.search(r'MATCH \(n:\w+\) WHERE n\.\w+ > \$last', text):
            keys = sorted(k for k in self.store['nodes'] if k > params['last'])[:params['n']]
            return [FakeRecord(k=k, p=self.store['nodes'][k]) for k in keys]
        if 'RETURN a.' in text and 'AS k ORDER BY k LIMIT' in text:
            keys = sorted({a for a, _ in self.store['rels'] if a > params['last']})[:params['n']]
            return [FakeRecord(k=k) for k in keys]
        rows = sorted((a, b) for a, b in self.store['rels'] if params['last'] < a <= params['upper'])
        return [FakeRecord(a=a, b=b, p=self.store['rels'][(a, b)]) for a, b in rows]


class FakeDriver:
    def __init__(self, store):
        self.store = store

    def session(self, **kw):
        assert kw['default_access_mode'] == 'READ'
        return FakeSession(self.store)


def test_graph_reads_are_paginated_without_a_row_cap_and_temporal_values_are_normalised():
    store = {'calls': [], 'nodes': {}, 'rels': {}}
    when = DateTime(2026, 1, 1, 10, 0, 0, tzinfo=timezone(timedelta(hours=2)))   # 08:00 UTC
    for i in range(4500):
        store['nodes'][f'c{i:07d}'] = {'customer_id': f'c{i:07d}', 'rid': 'r', 'name': 'n', 'updated_at': when}
    for i in range(3000):
        store['rels'][(f'c{i:07d}', f'c{(i + 1) % 3000:07d}')] = {'rid': 'r', 'since': Date(2020, 1, 1), 'strength': 2}
    graph = Neo4jGraph(lambda: FakeDriver(store), 'db', page=2000)
    nodes = list(graph.nodes('Customer'))
    assert len(nodes) == 4500 and [k for k, _ in nodes] == sorted(k for k, _ in nodes)
    assert nodes[0][1]['updated_at'] == '2026-01-01T08:00:00.000000Z'
    assert len([c for c in store['calls'] if 'MATCH (n:Customer)' in c[0]]) == 3          # 2000 + 2000 + 500: a short page ends the scan
    rels = list(graph.relationships('FOLLOWS', 'Customer', 'Customer'))
    assert len(rels) == 3000 and rels[0][2]['since'] == '2020-01-01'
    assert [(a, b) for a, b, _ in rels] == sorted((a, b) for a, b, _ in rels)


# ---- the incremental query-log reader ---------------------------------------------------------------------------------------------

def line(query_id, phase, *, query='MATCH (n) RETURN n', waiting=0):
    head = 'Query started: ' if phase == 'started' else ''
    message = (f'{head}id:{query_id} - transaction id:7 - 3 ms: (planning: 1, waiting: {waiting}) - 0 B - 0 page hits, 0 page faults - '
               f'bolt-session\tbolt\tpython/1\t\tclient/10.0.0.1:5\tserver/10.0.0.2:7687>\tdb - neo4j - {query} - runtime=pipelined - {{}}')
    return (json.dumps({'loggerName': 'QueryLogger', 'level': 'INFO', 'message': message, 'e09_marker': MARKER}) + '\n').encode()


def pair(query_id, **kw):
    return line(query_id, 'started', **kw) + line(query_id, 'completed', **kw)


class FakeLogHost:
    """/logs with files identified by inode: ``files`` maps a name to (inode, bytes)."""

    def __init__(self):
        self.files = {}
        self.next_inode = 100

    def create(self, name):
        self.next_inode += 1
        self.files[name] = [self.next_inode, b'']
        return self.next_inode

    def append(self, name, data):
        self.files[name][1] += data

    def rotate(self, archive):
        self.files[archive] = self.files.pop('/logs/query.log')
        self.create('/logs/query.log')

    def run(self, command):
        script = command[-1]
        if 'stat -c' in script:
            return 0, ''.join(f'{i} {len(d)} {n}\n' for n, (i, d) in sorted(self.files.items())).encode()
        match = re.search(r'tail -c \+(\d+) "([^"]+)" \| head -c (\d+)', script)
        offset, name, limit = int(match.group(1)) - 1, match.group(2), int(match.group(3))
        return 0, self.files[name][1][offset:offset + limit]


def reader_and_host():
    host = FakeLogHost()
    host.create('/logs/query.log')
    return QueryLogReader(host.run, MARKER), host


def test_the_reader_follows_a_rotation_without_losing_or_repeating_records():
    reader, host = reader_and_host()
    host.append('/logs/query.log', pair(1) + pair(2) + pair(3, query='MATCH (n) SET n.x = 1', waiting=0))
    assert reader.poll() == 6
    host.append('/logs/query.log', pair(4) + pair(5))
    host.rotate('/logs/query.log.1')                       # the segment we were reading is renamed
    host.append('/logs/query.log', pair(6, query='MATCH (n) SET n.x = 2', waiting=1531) + pair(7))
    assert reader.poll() == 8                              # the rest of the old segment, then the new one
    host.append('/logs/query.log', pair(8))
    reader.poll()
    for _ in range(4):
        reader.poll()
    assert reader.records == 16 and not reader.coverage and reader.max_id == 8
    assert [w['waiting_ms'] for w in reader.loader_waits()] == [1531]


def test_a_segment_lost_to_rotation_before_it_was_read_is_a_coverage_failure_not_a_zero():
    reader, host = reader_and_host()
    host.append('/logs/query.log', pair(1) + pair(2))
    reader.poll()
    host.append('/logs/query.log', pair(3) + pair(4))
    host.rotate('/logs/query.log.1')
    del host.files['/logs/query.log.1']                    # grew, rotated and deleted between two polls: unseen by size, seen by ids
    host.append('/logs/query.log', pair(5) + pair(6))
    for _ in range(5):
        reader.poll()
    text = ' '.join(reader.coverage)
    assert 'query id 3' in text and 'query id 4' in text


def test_a_segment_deleted_with_known_unread_bytes_is_reported_directly():
    reader, host = reader_and_host()
    host.append('/logs/query.log', pair(1) + pair(2)[:40])      # a partial line stays unread
    reader.poll()
    host.rotate('/logs/query.log.1')
    del host.files['/logs/query.log.1']
    host.append('/logs/query.log', pair(3))
    reader.poll()
    assert any('rotated away unread' in c for c in reader.coverage)


def test_a_neo4j_restart_starts_a_new_epoch_and_is_not_a_hole():
    reader, host = reader_and_host()
    host.append('/logs/query.log', pair(1) + pair(2) + pair(3))
    reader.poll()
    host.append('/logs/query.log', line(4, 'started'))     # in flight when Neo4j went down
    reader.poll()
    reader.mark_restart()
    host.append('/logs/query.log', pair(1) + pair(2))      # ids start again after the restart
    for _ in range(5):
        reader.poll()
    assert reader.epoch == 2 and reader.abandoned_open == 1 and not reader.coverage and reader.restarts == 1


def test_an_id_that_never_appears_is_reported_after_a_few_polls():
    reader, host = reader_and_host()
    host.append('/logs/query.log', pair(1) + pair(2) + pair(4))
    for _ in range(5):
        reader.poll()
    assert any('query id 3' in c for c in reader.coverage)


def test_ids_logged_out_of_order_are_not_holes():
    reader, host = reader_and_host()
    host.append('/logs/query.log', line(2, 'started') + line(1, 'started') + line(1, 'completed') + line(2, 'completed'))
    for _ in range(5):
        reader.poll()
    assert not reader.coverage and reader.max_id == 2


def test_a_partial_trailing_line_waits_for_its_newline():
    reader, host = reader_and_host()
    data = pair(1)
    host.append('/logs/query.log', data + pair(2)[:50])
    assert reader.poll() == 2
    host.append('/logs/query.log', pair(2)[50:])
    assert reader.poll() == 2 and not reader.coverage


def test_the_positive_control_is_seen_and_classified_apart_from_loader_writes():
    reader, host = reader_and_host()
    control = 'MATCH (n:LockProbe {id: 1}) SET n.v = 7001 RETURN n.v AS v'
    host.append('/logs/query.log', pair(1, query=control, waiting=1042) + pair(2, query='UNWIND $rows AS r MERGE (a:X {id: r}) SET a.v = 1', waiting=3))
    reader.poll()
    assert reader.control_seen(control) and classify(control) == 'control'
    assert len(reader.loader_waits()) == 1 and reader.summary()['controls_detected'] == 1


def test_an_unparseable_chunk_is_a_coverage_failure():
    reader, host = reader_and_host()
    host.append('/logs/query.log', b'this is not json\n')
    reader.poll()
    assert reader.coverage and 'unparseable' in reader.coverage[0]


# ---- sampling, guards, budget, verdicts ---------------------------------------------------------------------------------------------

def test_a_sampling_gap_over_sixty_seconds_is_reported_as_a_coverage_failure(tmp_path):
    ticks = iter([0.0, 15.0, 30.0, 120.0, 135.0])
    sampler = Sampler(tmp_path / 's.jsonl', {'x': lambda: 1}, clock=lambda: next(ticks))
    for _ in range(5):
        sampler.sample_once()
    assert sampler.gaps == [{'at': sampler.gaps[0]['at'], 'seconds': 90.0}] and sampler.coverage_failures() == ['sampling gap of 90.0 s']
    assert len((tmp_path / 's.jsonl').read_text().splitlines()) == 5


def test_a_failing_provider_is_recorded_as_an_error_never_as_zero(tmp_path):
    def broken():
        raise OSError('docker is gone')
    sampler = Sampler(tmp_path / 's.jsonl', {'ok': lambda: 1, 'bad': broken})
    row = sampler.sample_once()
    assert row['ok'] == 1 and 'OSError' in row['bad']['error'] and sampler.errors == {'bad': 1}


def test_the_reader_flags_its_own_polling_gap():
    reader, host = reader_and_host()
    reader.poll(now=0.0)
    reader.poll(now=100.0)
    assert any('sampling gap' in c for c in reader.coverage)


def test_the_guard_trips_below_1_5_gb_of_memory_and_below_30_gb_of_disk():
    tripped = []
    guard = Guard(tripped.append, mem=lambda: 1_400_000_000, disk=lambda: 100 * 1024 ** 3)
    assert guard.check() == ('host_memory', 1_400_000_000)
    guard = Guard(tripped.append, mem=lambda: 5 * 1024 ** 3, disk=lambda: 29 * 1024 ** 3)
    assert guard.check() == ('host_disk', 29 * 1024 ** 3)
    guard = Guard(tripped.append, mem=lambda: 5 * 1024 ** 3, disk=lambda: 100 * 1024 ** 3)
    assert guard.check() is None and guard.min_mem == 5 * 1024 ** 3


def test_the_guard_thread_trips_once_and_calls_back():
    tripped = []
    guard = Guard(tripped.append, mem=lambda: 1, period=0.01)
    guard.start()
    guard.join(timeout=5)
    assert guard.tripped['reason'] == 'host_memory' and len(tripped) == 1


def test_metrics_text_is_parsed():
    rows = parse_metrics('# HELP x\ngraph_loader_write_outage_seconds{kind="edge",name="PURCHASED"} 42.5\nother 3\nbroken line here\n')
    assert rows == [('graph_loader_write_outage_seconds', {'kind': 'edge', 'name': 'PURCHASED'}, 42.5), ('other', {}, 3.0)]


def test_the_memory_budget_inequality_for_the_largest_fleet():
    gib = budget.GiB
    peaks = {'node-loader:Customer': 150 * 1024 ** 2, 'node-loader:Product': 120 * 1024 ** 2, 'edge-loader:PURCHASED': 200 * 1024 ** 2,
             'retry-consumer': 180 * 1024 ** 2, 'bucket-lease-server': 90 * 1024 ** 2}
    fleet = budget.largest_fleet_peaks(peaks, {'Customer': 2, 'Product': 1, 'PURCHASED': 2}, {'PURCHASED': 3}, cli_peak=100 * 1024 ** 2)
    assert fleet['rows']['edge-loader:PURCHASED']['count'] == 3 and fleet['rows']['node-loader:Customer']['count'] == 2
    ok = budget.evaluate(neo4j_limit=3 * gib, kafka_limit=int(1.5 * gib), fleet=fleet, harness_peak=300 * 1024 ** 2, mem_available=12 * gib)
    assert ok['ok'] and ok['margin'] > 0
    tight = budget.evaluate(neo4j_limit=3 * gib, kafka_limit=int(1.5 * gib), fleet=fleet, harness_peak=300 * 1024 ** 2, mem_available=7 * gib)
    assert not tight['ok']
    assert budget.max_fleet_sum(3 * gib, int(1.5 * gib), 300 * 1024 ** 2, 7 * gib) < fleet['sum_with_overlap_and_cli']


def test_fault_verdicts():
    assert faults.judge_kill('edge-loader', 1, '... exited before drain acknowledgement ...', True)['ok']
    assert not faults.judge_kill('edge-loader', 0, 'nothing', True)['ok']
    assert not faults.judge_kill('retry-consumer', 1, 'retry consumer exited during stream supervision', False)['ok']
    assert faults.judge_outage(restarts=0, cli_running=True, outage_lines=3, recovered_lines=3, gauge_peak=31.0, gauge_after=0.0)['ok']
    assert not faults.judge_outage(restarts=1, cli_running=True, outage_lines=3, recovered_lines=3, gauge_peak=31.0, gauge_after=0.0)['ok']
    assert not faults.judge_outage(restarts=0, cli_running=True, outage_lines=3, recovered_lines=3, gauge_peak=0.0, gauge_after=0.0)['ok']
    assert faults.judge_sigterm_outage(0, {'a': 0, 'b': 137})['ok'] and not faults.judge_sigterm_outage(0, {'a': 1})['ok']
    assert faults.committed_not_behind({'a': 5, 'b': None}, {'a': 5, 'b': 1}) == []
    assert faults.committed_not_behind({'a': 5}, {'a': 4}) == [('a', 5, 4)]
