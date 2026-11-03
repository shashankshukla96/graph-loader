"""E14.F2.S7 dead-letter and restricted-ordering contracts under ``--runtime process`` (Linux host, owned Neo4j/Kafka).

The process-mode counterpart of ``test_service_ingest_dlq.py`` and ``test_service_last_value_order.py``: the same fixtures and
closed-form oracles, driven through the installed ``graph-loader`` with children as processes (``DOCKER_HOST`` at a missing
socket). Observation is the run's registry plus the graph and Kafka offsets.

* arrival DLQ with three edge replicas: relationships published before their nodes are drained by the retry consumer, every
  pair ends with its last-arrival value, nothing is parked;
* retry limits: rows with a missing endpoint are parked after exactly ``max_retry_attempts``, a malformed record is terminal,
  ``replay-dlq --parked`` (dry run, then live) loads the parked rows once the endpoint exists;
* O4/O5/O6: ``order_by`` over two topics x two partitions and three replicas; the newest version wins, an equal version keeps the
  stored value, an older one is ignored, a record without the property is applied unconditionally; without ``order_by`` the
  record applied last wins (documented counterexample);
* O7: a parked older value replayed after a newer one is protected by ``order_by`` and overwrites without it;
* O1/O2: ``order_by`` plus the DLQ across a fleet restart; O8: a source topic recreated with the fleet stopped;
* ``order_by`` in planned, lease and classic bulk when older records are read after newer ones are stored.

Not covered: the Docker-mode versions of these cells (E14 closes them last), LRU eviction and live rebalances (unit counterexamples).
"""
from __future__ import annotations

import json
import time
from datetime import date

import pytest

from tests.installed_service import dlq_fixture as dq
from tests.installed_service import order_fixture as of
from tests.installed_service.inspect_strict import group_offsets, topic_total
from tests.installed_service.live_cli import run_cli
from tests.installed_service.process_support import (  # noqa: F401  (memory_guard is an autouse fixture)
    CLI_TIMEOUT, Fleet, assert_clean, assert_no_secret, executable, finished, memory_guard, process_env, run_dir, run_to_completion,
)
from tests.installed_service.test_service_ingest_dlq import clear as dlq_clear
from tests.installed_service.test_service_ingest_dlq import consumed as dlq_consumed
from tests.installed_service.test_service_ingest_dlq import likes, total

pytestmark = pytest.mark.integration


def components(replicas, dead_letter):
    return {'bucket-lease-server': 1, 'node-loader': 2, 'edge-loader': replicas, **({'retry-consumer': 1} if dead_letter else {})}


def make_topics(stack, tag, edge_partitions):
    prefix = f'{stack.topic_prefix}{tag}-'
    stack.create_topics([prefix + 'person', prefix + 'item'])
    edge = []
    for name, partitions in edge_partitions.items():
        stack.create_topics([prefix + name], partitions=partitions)
        edge.append(prefix + name)
    return prefix, edge


def offsets_of(stack, group, topic, partitions):
    return group_offsets(stack, group, topic, partitions)


def consumed(stack, group, topics, partitions):
    return all(offsets_of(stack, f'{group}-RATED', topic, partitions[topic]) == stack.end_offsets(topic, partitions=partitions[topic])
               for topic in topics)


def order_total(stack, topic, *, exists=False):
    return topic_total(stack, topic, missing_ok=not exists)


def stop_ok(fleet, comps):
    code = fleet.stop()
    fleet.close()
    assert code == 0, (code, fleet.cli.text()[-2000:])
    finished(fleet.stack, fleet.cli, fleet.attempt, comps, code=0)


def replay(stack, config, run, schema_path, args, name):
    return run_cli(executable(config), ['replay-dlq', '--config', str(schema_path), *args], env=process_env(stack), cwd=run,
                   log_dir=run / 'logs', name=name, timeout=120, redact=[stack.password])


# -- dead letters ------------------------------------------------------------------------------------------------------
def test_p7_arrival_dlq_is_drained_by_the_retry_consumer(stack, service_config):
    run = run_dir(service_config, 'p7-arrival')
    dlq_clear(stack)
    prefix = f'{stack.topic_prefix}dlqa-'
    group = f'{prefix}group'
    topic_a, topic_b, dlq, parked = prefix + 'likes-a', prefix + 'likes-b', prefix + 'likes.dlq', prefix + 'likes.dlq.parked'
    stack.create_topics([prefix + 'person', prefix + 'company'])
    stack.create_topics([topic_a], partitions=3)
    stack.create_topics([topic_b], partitions=4)
    b_rows, a_rows, want = dq.arrival()
    target = {k: (v[0].isoformat(), v[1]) for k, v in want.items()}
    fleet = Fleet(stack, service_config, run, 'start', dq.arrival_schema(prefix, group))
    try:
        dq.publish_likes(stack, topic_b, b_rows)              # first: every row is dead-lettered (no nodes yet)
        fleet.wait('topic b consumed', lambda: dlq_consumed(stack, f'{group}-LIKES', topic_b, 4), 240)
        dq.publish_likes(stack, topic_a, a_rows)              # later, behind b's rows of the same pairs
        fleet.wait('topic a consumed', lambda: dlq_consumed(stack, f'{group}-LIKES', topic_a, 3), 240)
        assert likes(stack) == {}, 'a relationship was written before its endpoints existed'
        sample = dq.envelopes(stack, dlq, 4)
        assert sample and {(e.entity_kind, e.entity_name, e.failure_class) for e in sample} == {('edge', 'LIKES', 'missing_endpoint')}
        assert {e.original_topic for e in sample} == {topic_a, topic_b}
        persons = sorted({p for p, _c in want})
        companies = sorted({c for _p, c in want})
        stack.produce(prefix + 'person', [(p, {'person_id': p}) for p in persons])
        stack.produce(prefix + 'company', [(c, {'company_id': c}) for c in companies])
        fleet.wait('every pair holds its last-arrival value', lambda: likes(stack) == target, 300)
        fleet.wait('retry consumer drained the DLQ', lambda: dlq_consumed(stack, f'{group}-retry-consumer', dlq, 4), 180)
        assert total(stack, dlq, 4) >= len(a_rows) + len(b_rows)
        assert total(stack, parked, 4, exists=True) == 0, 'nothing should be parked'
        topics = stack.admin().list_topics(timeout=10).topics
        assert topic_a + '.dlq' not in topics and topic_b + '.dlq' not in topics, 'no per-topic DLQ'
        stop_ok(fleet, components(3, True))
    finally:
        fleet.close()
    assert likes(stack) == target
    dlq_clear(stack)


def test_p7_retry_limits_parking_and_replay(stack, service_config):
    run = run_dir(service_config, 'p7-limits')
    dlq_clear(stack)
    prefix = f'{stack.topic_prefix}dlql-'
    group = f'{prefix}group'
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
    fleet = Fleet(stack, service_config, run, 'start', dq.limits_schema(prefix, group, attempts=3))
    expected_good = {(p, c): (d.isoformat(), r) for _t, p, c, d, r in good}
    expected_all = {**expected_good, **{(p, c): (d.isoformat(), r) for _t, p, c, d, r in missing}}
    try:
        fleet.wait('resolvable relationships', lambda: likes(stack) == expected_good, 180)
        fleet.wait('four rows parked', lambda: total(stack, parked, 1) >= 4, 180)
        assert likes(stack) == expected_good, 'a parked or malformed row reached the graph'
        parked_env = dq.envelopes(stack, parked, 1)
        assert {(e.entity_kind, e.entity_name, e.original_topic, e.failure_class, e.reason_code, e.attempt_count) for e in parked_env} == \
            {('edge', 'LIKES', topic, 'missing_endpoint', 'missing_target', 3)} and len(parked_env) == 4
        malformed = [e for e in dq.envelopes(stack, dlq, 1) if e.failure_class == 'malformed']
        assert len(malformed) == 1 and malformed[0].reason_code == 'undeclared_property' and malformed[0].attempt_count == 0
        fleet.wait('retry consumer at the DLQ end', lambda: dlq_consumed(stack, f'{group}-retry-consumer', dlq, 1), 90)
        assert offsets_of(stack, f'{group}-LIKES', topic, 1) == {0: rows}, 'the edge group must be past every record'
        assert total(stack, parked, 1, exists=True) == 4

        args = ['--edge-type', 'LIKES', '--parked']
        schema = run / 'start.yaml'
        dry = replay(stack, service_config, run, schema, [*args, '--dry-run'], 'replay-dry')
        assert dry.exit_code == 0 and 'matched=4 replayed=0' in dry.stdout.read_text() + dry.stderr.read_text()
        assert likes(stack) == expected_good and total(stack, topic, 1) == rows, 'a dry run must change nothing'
        stack.produce(prefix + 'company', [('co-missing', {'company_id': 'co-missing'})])
        live = replay(stack, service_config, run, schema, args, 'replay-live')
        assert live.exit_code == 0 and 'matched=4 replayed=4' in live.stdout.read_text() + live.stderr.read_text()
        assert dry.redactions == 0 and live.redactions == 0
        fleet.wait('replayed rows loaded', lambda: likes(stack) == expected_all, 180)
        assert total(stack, topic, 1) == rows + 4 and total(stack, parked, 1, exists=True) == 4
        fleet.wait('edge group past the replayed rows', lambda: offsets_of(stack, f'{group}-LIKES', topic, 1) == {0: rows + 4}, 90)
        stop_ok(fleet, components(1, True) | {'node-loader': 2})
    finally:
        fleet.close()
    assert likes(stack) == expected_all
    dlq_clear(stack)


# -- ordering -----------------------------------------------------------------------------------------------------------
@pytest.mark.parametrize('repeat', range(2))
def test_p7_o4_o5_o6_order_by_newest_version_wins_across_topics_partitions_and_replicas(repeat, stack, service_config):
    run = run_dir(service_config, f'p7-order-by-{repeat}')
    of.clear(stack)
    prefix, topics = make_topics(stack, f'ob{repeat}', {'a': 2, 'b': 2})
    group = f'{prefix}group'
    partitions = {t: 2 for t in topics}
    want, _records = of.publish_conflicts(stack, topics)
    of.publish_nodes(stack, prefix)
    fleet = Fleet(stack, service_config, run, 'start', of.schema(prefix, group, topics=topics, order_by=True, replicas=3))
    ties = ('p3', 'i8')
    try:
        fleet.wait('newest version of every pair', lambda: of.relationships(stack) == want, 300)
        fleet.wait('every offset committed', lambda: consumed(stack, group, topics, partitions), 120)
        for step, row, expect in [
            ('first', (*ties, 500, 'first'), (500, 'first')),
            ('equal version', (*ties, 500, 'second'), (500, 'first')),
            ('older version', (*ties, 100, 'older'), (500, 'first')),
            ('no version', (*ties, None, 'unconditional'), (500, 'unconditional')),
        ]:
            of.publish(stack, topics[0], [row], partition=0)
            fleet.wait(f'{step} consumed', lambda: consumed(stack, group, topics, partitions), 120)
            assert of.relationships(stack)[ties] == expect, (step, of.relationships(stack)[ties])
        assert {k: v for k, v in of.relationships(stack).items() if k != ties} == want
        stop_ok(fleet, components(3, False))
    finally:
        fleet.close()
    of.clear(stack)


def test_p7_without_order_by_the_record_applied_last_wins_even_if_older(stack, service_config):
    run = run_dir(service_config, 'p7-arrival-order')
    of.clear(stack)
    prefix, topics = make_topics(stack, 'ar', {'a': 1})
    group = f'{prefix}group'
    of.publish_nodes(stack, prefix)
    fleet = Fleet(stack, service_config, run, 'start', of.schema(prefix, group, topics=topics, order_by=False))
    pair = ('p0', 'i0')
    try:
        of.publish(stack, topics[0], [(*pair, 400, 'newer')])
        fleet.wait('newer value written', lambda: of.relationships(stack).get(pair) == (400, 'newer'), 180)
        of.publish(stack, topics[0], [(*pair, 100, 'older')])
        fleet.wait('older value overwrote it', lambda: of.relationships(stack).get(pair) == (100, 'older'), 180)
        stop_ok(fleet, components(1, False))
    finally:
        fleet.close()
    of.clear(stack)


@pytest.mark.parametrize('ordered', [True, False], ids=['order_by-protects', 'arrival-is-overwritten'])
def test_p7_o7_replaying_a_parked_older_value(ordered, stack, service_config):
    run = run_dir(service_config, f'p7-replay-{"ob" if ordered else "plain"}')
    of.clear(stack)
    tag = 'rb' if ordered else 'rp'
    prefix, topics = make_topics(stack, tag, {'a': 1})
    group = f'{prefix}group'
    parked = prefix + 'rated.dlq.parked'
    of.publish_nodes(stack, prefix, items=of.ITEMS[:1])                    # item i9 ('late') is missing
    fleet = Fleet(stack, service_config, run, 'start', of.schema(prefix, group, topics=topics, order_by=ordered, dead_letter=True, attempts=3))
    late = ('p0', 'i9')
    try:
        of.publish(stack, topics[0], [(*late, 3, 'old')])
        fleet.wait('the older value is parked', lambda: order_total(stack, parked) >= 1, 180)
        stack.produce(prefix + 'item', [('i9', {'id': 'i9'})])
        of.publish(stack, topics[0], [(*late, 9, 'new')])
        fleet.wait('the newer value is written', lambda: of.relationships(stack).get(late) == (9, 'new'), 180)
        before = order_total(stack, topics[0], exists=True)
        done = replay(stack, service_config, run, run / 'start.yaml', ['--edge-type', 'RATED', '--parked'], 'replay')
        assert done.exit_code == 0 and 'matched=1 replayed=1' in done.stdout.read_text() + done.stderr.read_text()
        assert order_total(stack, topics[0], exists=True) == before + 1
        fleet.wait('the replayed record consumed', lambda: offsets_of(stack, f'{group}-RATED', topics[0], 1) == {0: before + 1}, 120)
        if ordered:
            assert of.relationships(stack)[late] == (9, 'new'), 'order_by must protect the newer value from the replay'
        else:
            fleet.wait('the older value overwrote the newer', lambda: of.relationships(stack).get(late) == (3, 'old'), 90)
        assert order_total(stack, parked, exists=True) == 1, 'replay does not remove the parked copy'
        stop_ok(fleet, components(1, True))
    finally:
        fleet.close()
    of.clear(stack)


def test_p7_o1_o2_order_by_with_the_dlq_survives_a_fleet_restart(stack, service_config):
    of.clear(stack)
    prefix, topics = make_topics(stack, 'rs', {'a': 1, 'b': 1})
    group = f'{prefix}group'
    partitions = {t: 1 for t in topics}
    dlq = prefix + 'rated.dlq'
    text = of.schema(prefix, group, topics=topics, order_by=True, dead_letter=True, attempts=200)
    pair = ('p0', 'i0')
    of.publish(stack, topics[0], [(*pair, 9, 'newest')])
    of.publish(stack, topics[1], [(*pair, 3, 'older')])               # neither endpoint exists yet: both go to the DLQ
    first = Fleet(stack, service_config, run_dir(service_config, 'p7-restart-1'), 'start-1', text)
    try:
        first.wait('both rows dead-lettered and consumed', lambda: order_total(stack, dlq) >= 2 and consumed(stack, group, topics, partitions), 180)
        assert of.relationships(stack) == {}
        stop_ok(first, components(1, True))                          # the retry consumer (and its in-memory resolution memory) goes away
    finally:
        first.close()
    second = Fleet(stack, service_config, run_dir(service_config, 'p7-restart-2'), 'start-2', text)
    try:
        of.publish_nodes(stack, prefix, items=of.ITEMS[:1])
        second.wait('the newest value after the restart', lambda: of.relationships(stack).get(pair) == (9, 'newest'), 240)
        second.wait('every offset committed', lambda: consumed(stack, group, topics, partitions), 120)
        second.wait('retry consumer at the DLQ end', lambda: offsets_of(stack, f'{group}-retry-consumer', dlq, 1) == stack.end_offsets(dlq, partitions=1), 120)
        assert of.relationships(stack) == {pair: (9, 'newest')}
        stop_ok(second, components(1, True))
    finally:
        second.close()
    of.clear(stack)


def test_p7_o8_recreating_a_source_topic_with_the_fleet_stopped_restarts_from_the_new_topics_start(stack, service_config):
    of.clear(stack)
    prefix, topics = make_topics(stack, 'rc', {'a': 1})
    group = f'{prefix}group'
    partitions = {topics[0]: 1}
    pairs = [(p, i) for p in of.PERSONS for i in of.ITEMS[:5]]
    text = of.schema(prefix, group, topics=topics, order_by=False)
    of.publish_nodes(stack, prefix, items=of.ITEMS[:5])
    of.publish(stack, topics[0], [(*pair, 1, f'gen1-{k}') for k, pair in enumerate(pairs[:6])])
    first = Fleet(stack, service_config, run_dir(service_config, 'p7-recreate-1'), 'start-1', text)
    try:
        first.wait('generation 1 loaded', lambda: len(of.relationships(stack)) == 6, 180)
        first.wait('offsets committed', lambda: consumed(stack, group, topics, partitions), 90)
        stop_ok(first, components(1, False))
    finally:
        first.close()
    assert offsets_of(stack, f'{group}-RATED', topics[0], 1) == {0: 6}
    admin = stack.admin()
    admin.delete_topics([topics[0]])[topics[0]].result(30)
    for _ in range(60):
        if topics[0] not in admin.list_topics(timeout=10).topics:
            break
        time.sleep(0.5)
    stack.create_topics([topics[0]], partitions=1)
    of.publish(stack, topics[0], [(*pair, 2, f'gen2-{k}') for k, pair in enumerate(pairs[6:15])])
    retained = offsets_of(stack, f'{group}-RATED', topics[0], 1)
    assert retained == {0: None}, ("Kafka kept the deleted topic's committed offsets", retained)
    second = Fleet(stack, service_config, run_dir(service_config, 'p7-recreate-2'), 'start-2', text)
    try:
        second.wait("all nine new records load from the new topic's start", lambda: len(of.relationships(stack)) == 15, 180)
        second.wait('offsets committed', lambda: consumed(stack, group, topics, partitions), 90)
        assert offsets_of(stack, f'{group}-RATED', topics[0], 1) == {0: 9}
        assert {pair: note for pair, (_v, note) in of.relationships(stack).items() if pair in set(pairs[6:15])} == \
            {pair: f'gen2-{k}' for k, pair in enumerate(pairs[6:15])}
        stop_ok(second, components(1, False))
    finally:
        second.close()
    (run_dir(service_config, 'p7-recreate-summary') / 'summary.json').write_text(json.dumps({'committed_after_delete': retained}) + '\n')
    of.clear(stack)


BULK_ARGS = {'planned': ['--plan-writers', '2'], 'lease': ['--edge-leases'], 'classic': ['--edge-classic']}


@pytest.mark.parametrize('mode', ['planned', 'lease', 'classic'])
def test_p7_order_by_newest_version_wins_in_planned_lease_and_classic_bulk(mode, stack, service_config):
    of.clear(stack)
    prefix, topics = make_topics(stack, f'bo{mode[0]}', {'a': 2, 'b': 2})
    group = f'{prefix}group'
    want, _records = of.publish_conflicts(stack, topics, phase='newest')
    of.publish_nodes(stack, prefix)
    text = of.schema(prefix, group, topics=topics, order_by=True, mode='bulk')
    run1 = run_dir(service_config, f'p7-bulk-{mode}-1')
    first, attempt1 = run_to_completion(stack, service_config, run1, 'start-1', text, extra=BULK_ARGS[mode], timeout=CLI_TIMEOUT)
    assert first.exit_code == 0, first.tail
    assert_no_secret(first, stack)
    assert_clean(attempt1)
    assert of.relationships(stack) == {pair: value for pair, value in want.items() if value[0] >= 400}, 'run 1 stores the newest versions'

    of.publish_conflicts(stack, topics, phase='older')                      # older versions (and noise) arrive after the newest is stored
    run2 = run_dir(service_config, f'p7-bulk-{mode}-2')
    second, attempt2 = run_to_completion(stack, service_config, run2, 'start-2', text, extra=BULK_ARGS[mode], timeout=CLI_TIMEOUT)
    assert second.exit_code == 0, second.tail
    assert_no_secret(second, stack)
    assert_clean(attempt2)
    assert of.relationships(stack) == want, 'the newest version of every pair must win in this mode'
    for label, topic in (('OrdPerson', prefix + 'person'), ('OrdItem', prefix + 'item')):
        assert group_offsets(stack, f'{group}-{label}', topic, 1) == {0: stack.end_offsets(topic)[0]}
    of.clear(stack)
