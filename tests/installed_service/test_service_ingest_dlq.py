"""E09.F1.S4 installed dead-letter routing, retry limits, parking and replay (native shared Docker Engine substrate).

Installed ``graph-loader start --mode stream`` (lease fleet plus the retry consumer container), installed images:

* the maintained arrival dataset (``tests/fixtures/multi_topic_arrival.py``, ported from
  ``test_dead_letter_arrival_stream_type_wide_dlq_drained_by_the_retry_consumer``): one multi-topic ``LIKES`` type with
  ``topic_conflict: arrival``; topic ``b`` is published and fully dead-lettered, then topic ``a``, then the endpoints; every
  row goes to the type's one DLQ, nothing is parked, no per-topic DLQ exists, every pair ends with its last-indexed value and
  every leased write waits 0 ms (repeated 3 times);
* a small single-topic case: missing endpoints are retried and parked after exactly ``max_retry_attempts``, a malformed record
  is terminal in the DLQ, offsets progress to the ends, ``replay-dlq --parked`` (dry run, then live) loads the parked rows
  once their endpoint exists.

Not covered: relationship types without ``dead_letter`` (S2 covers the fail-closed exit), DLQ in bulk modes, ambiguous
endpoints, retry-consumer scale or restart, DLQ envelope size limits, node DLQ, Neo4j outage while rows are parked (S5).
Publishing both arrival topics at once instead makes the result nondeterministic (the documented ``arrival`` warning), so the
test does not do that.
"""
from __future__ import annotations

from datetime import date
import json
from pathlib import Path

import pytest

from tests.installed_service import dlq_fixture as dq
from tests.installed_service.live_cli import run_cli
from tests.installed_service.inspect_strict import group_offsets, topic_total, unique
from tests.installed_service.stream_run import StreamRun

pytestmark = pytest.mark.integration

COMPONENTS = {'preflight': 1, 'bucket-lease-server': 1, 'node-loader': 2, 'edge-loader': 3, 'retry-consumer': 1}
WRITES = ('UNWIND range(0, size($rows) - 1) AS row_index', 'UNWIND $batch AS record MERGE (n:')


def clear(stack):
    with stack.driver.session(database=stack.database) as session:
        session.run('MATCH (n:DlqPerson|DlqCompany) DETACH DELETE n').consume()


def likes(stack):
    with stack.driver.session(database=stack.database) as session:
        rows = [dict(r) for r in session.run(
            'MATCH (p:DlqPerson)-[r:LIKES]->(c:DlqCompany) RETURN p.person_id AS p, c.company_id AS c, '
            'toString(r.since) AS since, r.role AS role')]
    unique(rows, lambda r: (r['p'], r['c']), 'LIKES')  # two physical relationships for one pair must fail, not be projected away
    return {(r['p'], r['c']): (r['since'], r['role']) for r in rows}


def total(stack, topic, partitions=None, *, exists=False):
    """Records ever written to ``topic``. A topic that is not there yet counts 0 only where a wait allows it (``exists=False``);
    an assertion about absence passes ``exists=True`` so a missing topic or a failed probe cannot read as "nothing"."""
    return topic_total(stack, topic, missing_ok=not exists)


def offsets(stack, group, topic, partitions):
    return group_offsets(stack, group, topic, partitions)


def consumed(stack, group, topic, partitions):
    return offsets(stack, group, topic, partitions) == stack.end_offsets(topic, partitions=partitions)


def check_stopped(stack, docker_client, out, cli, components):
    snapshot = out['snapshot']
    assert out['exit_code'] == 0, ('SIGTERM must end a supervising stream run with exit 0', cli.text()[-1500:])
    assert out['stop_seconds'] < 30 and cli.redactions == 0
    assert snapshot['errors'] == [] and not snapshot['closed_early'] and snapshot['stopped'] is True
    assert snapshot['refused'] == {} and out['leftovers'] == {'removed': [], 'failed': []}
    ids = set(snapshot['containers'])
    assert set(snapshot['created_from_candidates']) == ids and ids
    assert not [c for c in docker_client.containers.list(all=True) if c.id in ids], 'a container outlived the stop'
    seen = {}
    for row in snapshot['containers'].values():
        info = row['inspected']
        assert info and not info['privileged'] and info['pid_mode'] == '' and info['network_mode'] == stack.network.name
        assert all('docker.sock' not in source for _, source, _, _ in info['mounts'])
        seen[info['labels']['component']] = seen.get(info['labels']['component'], 0) + 1
    assert seen == components


def leased_writes(out):
    rows = [r for r in out['records'] if r['phase'] == 'completed' and ' '.join(r['query'].split()).startswith(WRITES)]
    assert rows and all(r['waiting_ms'] == 0 for r in rows), 'a leased write waited for a database lock'
    assert not [r for r in out['records'] if r['phase'] == 'failed']
    return len(rows)


@pytest.mark.parametrize('repeat', range(3))
def test_s4_arrival_dlq_is_drained_by_the_retry_consumer(repeat, docker_client, registry, stack, service_config):
    run = Path(service_config['run_dir']) / f's4-arrival-{repeat}'
    run.mkdir()
    clear(stack)
    prefix = f'e09-{stack.owner}-dlqa{repeat}-'
    group = f'e09-{stack.owner}-dlqa{repeat}-group'
    topic_a, topic_b, dlq, parked = prefix + 'likes-a', prefix + 'likes-b', prefix + 'likes.dlq', prefix + 'likes.dlq.parked'
    stack.create_topics([prefix + 'person', prefix + 'company'])
    stack.create_topics([topic_a], partitions=3)
    stack.create_topics([topic_b], partitions=4)
    b_rows, a_rows, want = dq.arrival()
    both = {(p, c) for _t, p, c, *_ in a_rows} & {(p, c) for _t, p, c, *_ in b_rows}
    assert len(both) > 100, 'the dataset must put many pairs in both topics'
    target = {k: (v[0].isoformat(), v[1]) for k, v in want.items()}
    stream = StreamRun(stack, docker_client, registry, service_config, run, 'start', dq.arrival_schema(prefix, group))
    try:
        dq.publish_likes(stack, topic_b, b_rows)  # first: every row is dead-lettered (no nodes yet)
        stream.wait('topic b consumed', lambda: consumed(stack, f'{group}-LIKES', topic_b, 4), 180)
        dq.publish_likes(stack, topic_a, a_rows)  # later, behind b's rows of the same pairs
        stream.wait('topic a consumed', lambda: consumed(stack, f'{group}-LIKES', topic_a, 3), 180)
        assert likes(stack) == {}, 'a relationship was written before its endpoints existed'
        sample = dq.envelopes(stack, dlq, 4)
        assert sample and {(e.entity_kind, e.entity_name, e.failure_class) for e in sample} == {('edge', 'LIKES', 'missing_endpoint')}
        reasons = {e.reason_code for e in sample}
        assert {e.original_topic for e in sample} == {topic_a, topic_b}
        # A row behind an earlier unresolved row of its pair is dead-lettered as blocked (documented reason code).
        assert 'missing_both' in reasons and reasons <= {'missing_both', 'blocked_by_earlier_unresolved_row'}, reasons
        persons = sorted({p for p, _c in want})
        companies = sorted({c for _p, c in want})
        stack.produce(prefix + 'person', [(p, {'person_id': p}) for p in persons])
        stack.produce(prefix + 'company', [(c, {'company_id': c}) for c in companies])
        stream.wait('every pair holds its last-arrival value', lambda: likes(stack) == target, 240)
        stream.wait('retry consumer drained the DLQ', lambda: consumed(stack, f'{group}-retry-consumer', dlq, 4), 120)
        assert total(stack, dlq, 4) >= len(a_rows) + len(b_rows), 'every row of both topics should have gone to the type DLQ'
        assert total(stack, parked, 4, exists=True) == 0, 'nothing should be parked'
        topics = stack.admin().list_topics(timeout=10).topics
        assert topic_a + '.dlq' not in topics and topic_b + '.dlq' not in topics, 'no per-topic DLQ'
        assert consumed(stack, f'{group}-LIKES', topic_a, 3) and consumed(stack, f'{group}-LIKES', topic_b, 4)
        assert stream.cli.running()
    finally:
        out = stream.finish()
    check_stopped(stack, docker_client, out, stream.cli, COMPONENTS)
    writes = leased_writes(out)
    assert likes(stack) == target
    (run / 'logs' / 'summary.json').write_text(json.dumps({'rows': len(a_rows) + len(b_rows), 'pairs': len(want), 'shared_pairs': len(both),
                                                          'leased_writes': writes, 'stop_seconds': out['stop_seconds']}) + '\n')
    clear(stack)


def test_s4_retry_limits_parking_and_replay(docker_client, registry, stack, service_config):
    run = Path(service_config['run_dir']) / 's4-limits'
    run.mkdir()
    clear(stack)
    prefix = f'e09-{stack.owner}-dlql-'
    group = f'e09-{stack.owner}-dlql-group'
    topic, dlq, parked = prefix + 'likes', prefix + 'likes.dlq', prefix + 'likes.dlq.parked'
    stack.create_topics([prefix + 'person', prefix + 'company', topic])
    stack.produce(prefix + 'person', [(p, {'person_id': p}) for p in range(1, 11)])
    stack.produce(prefix + 'company', [(c, {'company_id': c}) for c in ('c1', 'c2', 'c3')])
    good = [('t', p, f'c{p % 3 + 1}', date(2020, 1, p), f'r{p}') for p in range(1, 7)]
    missing = [('t', p, 'co-missing', date(2020, 2, p - 6), f'm{p}') for p in range(7, 11)]
    dq.publish_likes(stack, topic, good + missing)
    stack.produce(topic, [(99, {'source': {'person_id': 1}, 'target': {'company_id': 'c1'},
                                'properties': {'since': '2020-03-03', 'role': 'bad', 'bogus': 1}})])
    rows = len(good) + len(missing) + 1
    stream = StreamRun(stack, docker_client, registry, service_config, run, 'start', dq.limits_schema(prefix, group, attempts=3))
    expected_good = {(p, c): (d.isoformat(), r) for _t, p, c, d, r in good}
    expected_all = {**expected_good, **{(p, c): (d.isoformat(), r) for _t, p, c, d, r in missing}}
    try:
        stream.wait('resolvable relationships', lambda: likes(stack) == expected_good, 120)
        stream.wait('four rows parked', lambda: total(stack, parked, 1) >= 4, 120)
        assert likes(stack) == expected_good, 'a parked or malformed row reached the graph'
        parked_env = dq.envelopes(stack, parked, 1)
        assert len(parked_env) == 4
        assert {(e.entity_kind, e.entity_name, e.original_topic, e.failure_class, e.reason_code, e.attempt_count) for e in parked_env} == \
            {('edge', 'LIKES', topic, 'missing_endpoint', 'missing_target', 3)}, 'parked after exactly max_retry_attempts'
        dlq_env = dq.envelopes(stack, dlq, 1)
        malformed = [e for e in dlq_env if e.failure_class == 'malformed']
        assert len(malformed) == 1 and malformed[0].reason_code == 'undeclared_property' and malformed[0].attempt_count == 0
        assert {e.attempt_count for e in dlq_env if e.failure_class == 'missing_endpoint'} <= {0, 1, 2}
        assert {e.original_topic for e in dlq_env} == {topic}
        stream.wait('retry consumer at the DLQ end', lambda: consumed(stack, f'{group}-retry-consumer', dlq, 1), 60)
        assert offsets(stack, f'{group}-LIKES', topic, 1) == {0: rows}, 'the edge group must be past every record, malformed one included'
        assert total(stack, parked, 1, exists=True) == 4, 'the malformed record is terminal and never parked'

        cmd = ['replay-dlq', '--config', str(stream.schema_path), '--edge-type', 'LIKES', '--parked']
        dry = run_cli(stream.executable, [*cmd, '--dry-run'], env=stream.env, cwd=run, log_dir=run / 'logs', name='replay-dry',
                      timeout=120, redact=[stack.password])
        assert dry.exit_code == 0 and 'matched=4 replayed=0' in dry.stdout.read_text() + dry.stderr.read_text()
        assert likes(stack) == expected_good and total(stack, topic, 1) == rows, 'a dry run must change nothing'
        stack.produce(prefix + 'company', [('co-missing', {'company_id': 'co-missing'})])
        live = run_cli(stream.executable, cmd, env=stream.env, cwd=run, log_dir=run / 'logs', name='replay-live',
                       timeout=120, redact=[stack.password])
        assert live.exit_code == 0 and 'matched=4 replayed=4' in live.stdout.read_text() + live.stderr.read_text()
        assert dry.redactions == 0 and live.redactions == 0
        stream.wait('replayed rows loaded', lambda: likes(stack) == expected_all, 120)
        assert total(stack, topic, 1) == rows + 4, 'the replay republishes the parked rows onto their source topic'
        assert total(stack, parked, 1, exists=True) == 4, 'replay does not remove the parked copies'
        stream.wait('edge group past the replayed rows', lambda: offsets(stack, f'{group}-LIKES', topic, 1) == {0: rows + 4}, 60)
        assert stream.cli.running()
    finally:
        out = stream.finish()
    check_stopped(stack, docker_client, out, stream.cli, {**COMPONENTS, 'edge-loader': 1})
    leased_writes(out)
    assert likes(stack) == expected_all
    clear(stack)

