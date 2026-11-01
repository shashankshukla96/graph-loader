"""E09.F2.S1 restricted ordering contract on owned Neo4j/Kafka (native shared Docker Engine substrate).

Live probes through the installed CLI and images for what D02 supports and for the boundaries it excludes; the deterministic
unit counterexamples (O1-O3 resurrection memory, restart, revoke and LRU eviction) run in the ``safety`` lane of the same
installed runner and are mapped in ``evidence/E09.F2.S1.md``. Each test says which ordering-matrix row it covers:

* O4/O5/O6: ``order_by`` over two topics, two partitions each and three replicas, with offsets, topics and arrival order all
  disagreeing with the version order: the newest version wins; an equal version keeps the stored value, an older one is ignored, a
  record without the property is applied unconditionally (documented in ``docs/INCREMENTAL_LOAD.md`` B5).
* the same older-after-newer conflict in the bulk modes that advertise ``order_by`` (planned, lease bulk, classic): the newest version
  wins on every pair, the run-scoped relationship groups (or the planned ledger) end complete.
* counterexample: without ``order_by`` the record applied last wins, newer or not.
* O7: a parked older value replayed by ``replay-dlq --parked`` after a newer value was written: ``order_by`` protects the newer
  value, a type without it is overwritten (replay bypasses the index).
* O1/O2: ``order_by`` plus the DLQ across a fleet restart (the retry consumer's memory is gone): the newest value still wins.
* O8: a source topic deleted and recreated while the fleet is stopped: Kafka drops the deleted topic's committed offsets, so the
  restarted fleet reads the new topic from offset 0 and skips nothing (the documented stop-first precondition).

Not covered live: timestamp-typed (datetime) ``order_by``, recreating a topic under a RUNNING fleet (unsupported; the retry consumer's remembered offsets would skip the new
records), LRU eviction (O3, unit counterexample only, it needs 100,000 cache entries), a live partition revoke or rebalance
(O2 is covered by the unit test and by the restart probe), a stale parked-pair index on one replica (O6 without ``order_by``),
timestamp-typed ``order_by`` values, an uncomparable value type, external writers.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import time

import pytest

from tests.installed_service import order_fixture as of
from tests.installed_service.live_cli import run_cli
from tests.installed_service.inspect_strict import bulk_groups, group_offsets, list_groups, topic_total
from tests.installed_service.observers import plan_ledger
from tests.installed_service.stream_run import StreamRun
from tests.installed_service.test_service_kill_and_outage import bulk_cleanup, bulk_run, stopped_cleanly

pytestmark = pytest.mark.integration


def components(replicas, dead_letter):
    return {'preflight': 1, 'bucket-lease-server': 1, 'node-loader': 2, 'edge-loader': replicas,
            **({'retry-consumer': 1} if dead_letter else {})}


def make_topics(stack, tag, edge_partitions):
    prefix = f'e09-{stack.owner}-{tag}-'
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


def total(stack, topic, partitions=1, *, exists=False):
    return topic_total(stack, topic, missing_ok=not exists)


def start(stack, client, registry, config, run, name, text):
    return StreamRun(stack, client, registry, config, run, name, text, track_queries=False)


@pytest.mark.parametrize('repeat', range(3))
def test_o4_o5_o6_order_by_newest_version_wins_across_topics_partitions_and_replicas(repeat, docker_client, registry, stack, service_config):
    run = Path(service_config['run_dir']) / f'o-order-by-{repeat}'
    run.mkdir()
    of.clear(stack)
    prefix, topics = make_topics(stack, f'ob{repeat}', {'a': 2, 'b': 2})
    group = f'e09-{stack.owner}-ob{repeat}-group'
    partitions = {t: 2 for t in topics}
    want, _records = of.publish_conflicts(stack, topics)
    of.publish_nodes(stack, prefix)
    stream = start(stack, docker_client, registry, service_config, run, 'start',
                   of.schema(prefix, group, topics=topics, order_by=True, replicas=3))
    ties = ('p3', 'i8')
    try:
        stream.wait('newest version of every pair', lambda: of.relationships(stack) == want, 240)
        stream.wait('every offset committed', lambda: consumed(stack, group, topics, partitions), 120)
        # Equal version keeps the stored value; an older one is ignored; a record without the property is applied unconditionally.
        for step, row, expect in [
            ('first', (*ties, 500, 'first'), (500, 'first')),
            ('equal version', (*ties, 500, 'second'), (500, 'first')),
            ('older version', (*ties, 100, 'older'), (500, 'first')),
            ('no version', (*ties, None, 'unconditional'), (500, 'unconditional')),
        ]:
            of.publish(stack, topics[0], [row], partition=0)
            stream.wait(f'{step} consumed', lambda: consumed(stack, group, topics, partitions), 90)
            assert of.relationships(stack)[ties] == expect, (step, of.relationships(stack)[ties])
        assert {k: v for k, v in of.relationships(stack).items() if k != ties} == want
        assert stream.cli.running()
    finally:
        out = stream.finish()
    stopped_cleanly(stack, docker_client, out, stream.cli, components(3, False), exit_code=0)
    of.clear(stack)


def test_without_order_by_the_record_applied_last_wins_even_if_older(docker_client, registry, stack, service_config):
    """Counterexample (documented limit): no version rule, so a later-applied older value overwrites a newer one."""
    run = Path(service_config['run_dir']) / 'o-arrival'
    run.mkdir()
    of.clear(stack)
    prefix, topics = make_topics(stack, 'ar', {'a': 1})
    group = f'e09-{stack.owner}-ar-group'
    of.publish_nodes(stack, prefix)
    stream = start(stack, docker_client, registry, service_config, run, 'start',
                   of.schema(prefix, group, topics=topics, order_by=False))
    pair = ('p0', 'i0')
    try:
        of.publish(stack, topics[0], [(*pair, 400, 'newer')])
        stream.wait('newer value written', lambda: of.relationships(stack).get(pair) == (400, 'newer'), 120)
        of.publish(stack, topics[0], [(*pair, 100, 'older')])
        stream.wait('older value overwrote it', lambda: of.relationships(stack).get(pair) == (100, 'older'), 120)
    finally:
        out = stream.finish()
    stopped_cleanly(stack, docker_client, out, stream.cli, components(1, False), exit_code=0)
    of.clear(stack)


@pytest.mark.parametrize('ordered', [True, False], ids=['order_by-protects', 'arrival-is-overwritten'])
def test_o7_replaying_a_parked_older_value(ordered, docker_client, registry, stack, service_config):
    run = Path(service_config['run_dir']) / f'o-replay-{"ob" if ordered else "plain"}'
    run.mkdir()
    of.clear(stack)
    tag = 'rb' if ordered else 'rp'
    prefix, topics = make_topics(stack, tag, {'a': 1})
    group = f'e09-{stack.owner}-{tag}-group'
    parked = prefix + 'rated.dlq.parked'
    of.publish_nodes(stack, prefix, items=of.ITEMS[:1])  # item i9 ('late') is missing
    text = of.schema(prefix, group, topics=topics, order_by=ordered, dead_letter=True, attempts=3)
    stream = start(stack, docker_client, registry, service_config, run, 'start', text)
    late = ('p0', 'i9')
    try:
        of.publish(stack, topics[0], [(*late, 3, 'old')])
        stream.wait('the older value is parked', lambda: total(stack, parked) >= 1, 120)
        stack.produce(prefix + 'item', [('i9', {'id': 'i9'})])
        of.publish(stack, topics[0], [(*late, 9, 'new')])
        stream.wait('the newer value is written', lambda: of.relationships(stack).get(late) == (9, 'new'), 120)
        before = total(stack, topics[0], exists=True)
        replay = run_cli(stream.executable, ['replay-dlq', '--config', str(stream.schema_path), '--edge-type', 'RATED', '--parked'],
                         env=stream.env, cwd=run, log_dir=run / 'logs', name='replay', timeout=120, redact=[stack.password])
        assert replay.exit_code == 0 and 'matched=1 replayed=1' in replay.stdout.read_text() + replay.stderr.read_text()
        assert total(stack, topics[0], exists=True) == before + 1, 'the replay republishes the parked record onto its source topic'
        stream.wait('the replayed record consumed', lambda: offsets_of(stack, f'{group}-RATED', topics[0], 1) == {0: before + 1}, 90)
        if ordered:
            assert of.relationships(stack)[late] == (9, 'new'), 'order_by must protect the newer value from the replay'
        else:
            stream.wait('the older value overwrote the newer', lambda: of.relationships(stack).get(late) == (3, 'old'), 60)
        assert total(stack, parked, exists=True) == 1, 'replay does not remove the parked copy'
    finally:
        out = stream.finish()
    stopped_cleanly(stack, docker_client, out, stream.cli, components(1, True), exit_code=0)
    of.clear(stack)


def test_o1_o2_order_by_with_the_dlq_survives_a_fleet_restart(docker_client, registry, stack, service_config):
    run = Path(service_config['run_dir']) / 'o-restart'
    run.mkdir()
    of.clear(stack)
    prefix, topics = make_topics(stack, 'rs', {'a': 1, 'b': 1})
    group = f'e09-{stack.owner}-rs-group'
    partitions = {t: 1 for t in topics}
    dlq = prefix + 'rated.dlq'
    text = of.schema(prefix, group, topics=topics, order_by=True, dead_letter=True, attempts=200)
    pair = ('p0', 'i0')
    # Newest first, then an older copy from the other topic; neither endpoint exists yet, so both go to the DLQ.
    of.publish(stack, topics[0], [(*pair, 9, 'newest')])
    of.publish(stack, topics[1], [(*pair, 3, 'older')])
    first = start(stack, docker_client, registry, service_config, run, 'start-1', text)
    try:
        first.wait('both rows dead-lettered and consumed', lambda: total(stack, dlq, 1) >= 2 and consumed(stack, group, topics, partitions), 120)
        assert of.relationships(stack) == {}
    finally:
        out1 = first.finish()  # the retry consumer (and its in-memory resolution memory) goes away here
    stopped_cleanly(stack, docker_client, out1, first.cli, components(1, True), exit_code=0)
    second = start(stack, docker_client, registry, service_config, run, 'start-2', text)
    try:
        of.publish_nodes(stack, prefix, items=of.ITEMS[:1])
        second.wait('the newest value after the restart', lambda: of.relationships(stack).get(pair) == (9, 'newest'), 180)
        second.wait('every offset committed', lambda: consumed(stack, group, topics, partitions), 90)
        second.wait('retry consumer at the DLQ end', lambda: offsets_of(stack, f'{group}-retry-consumer', dlq, 1) == stack.end_offsets(dlq, partitions=1), 90)
        assert of.relationships(stack) == {pair: (9, 'newest')}
    finally:
        out2 = second.finish()
    stopped_cleanly(stack, docker_client, out2, second.cli, components(1, True), exit_code=0)
    of.clear(stack)


def test_o8_recreating_a_source_topic_with_the_fleet_stopped_restarts_from_the_new_topics_start(docker_client, registry, stack, service_config):
    """Stop the fleet, delete and recreate an owned source topic, restart: Kafka drops the deleted topic's committed offsets, so the
    restarted fleet reads the recreated topic from offset 0 and nothing is skipped. (A running fleet is unsupported: the retry
    consumer's remembered offsets would skip the new records; not exercised live.)"""
    run = Path(service_config['run_dir']) / 'o-recreate'
    run.mkdir()
    of.clear(stack)
    prefix, topics = make_topics(stack, 'rc', {'a': 1})
    group = f'e09-{stack.owner}-rc-group'
    partitions = {topics[0]: 1}
    pairs = [(p, i) for p in of.PERSONS for i in of.ITEMS[:5]]            # 20 pairs
    text = of.schema(prefix, group, topics=topics, order_by=False)
    of.publish_nodes(stack, prefix, items=of.ITEMS[:5])
    of.publish(stack, topics[0], [(*pair, 1, f'gen1-{k}') for k, pair in enumerate(pairs[:6])])
    first = start(stack, docker_client, registry, service_config, run, 'start-1', text)
    try:
        first.wait('generation 1 loaded', lambda: len(of.relationships(stack)) == 6, 120)
        first.wait('offsets committed', lambda: consumed(stack, group, topics, partitions), 60)
    finally:
        out1 = first.finish()
    stopped_cleanly(stack, docker_client, out1, first.cli, components(1, False), exit_code=0)
    assert offsets_of(stack, f'{group}-RATED', topics[0], 1) == {0: 6}

    admin = stack.admin()
    admin.delete_topics([topics[0]])[topics[0]].result(30)
    for _ in range(60):
        if topics[0] not in admin.list_topics(timeout=10).topics:
            break
        time.sleep(0.5)
    stack.create_topics([topics[0]], partitions=1)
    of.publish(stack, topics[0], [(*pair, 2, f'gen2-{k}') for k, pair in enumerate(pairs[6:15])])  # nine new records, offsets 0-8
    retained = offsets_of(stack, f'{group}-RATED', topics[0], 1)
    assert retained == {0: None}, ('Kafka kept the deleted topic\'s committed offsets', retained)
    second = start(stack, docker_client, registry, service_config, run, 'start-2', text)
    try:
        second.wait('all nine new records load from the new topic\'s start', lambda: len(of.relationships(stack)) == 15, 120)
        second.wait('offsets committed', lambda: consumed(stack, group, topics, partitions), 60)
        assert offsets_of(stack, f'{group}-RATED', topics[0], 1) == {0: 9}
        assert {pair: note for pair, (_v, note) in of.relationships(stack).items() if pair in set(pairs[6:15])} == \
            {pair: f'gen2-{k}' for k, pair in enumerate(pairs[6:15])}
    finally:
        out2 = second.finish()
    stopped_cleanly(stack, docker_client, out2, second.cli, components(1, False), exit_code=0)
    (run / 'logs' / 'summary.json').write_text(json.dumps({'committed_after_delete': retained, 'relationships_after_restart': 15}) + '\n')
    of.clear(stack)


BULK_ARGS = {'planned': ['--plan-writers', '2'], 'lease': ['--edge-leases'], 'classic': ['--edge-classic']}


@pytest.mark.parametrize('mode', ['planned', 'lease', 'classic'])
def test_order_by_newest_version_wins_in_planned_lease_and_classic_bulk(mode, docker_client, registry, stack, service_config):
    """Run 1 stores the newest versions. Run 2 (a full re-read of both topics, now holding the older versions as well) then applies
    older records after newer ones: the newest version must still win on every pair, offsets and the planned ledger end complete."""
    run = Path(service_config['run_dir']) / f'o-bulk-{mode}'
    run.mkdir()
    of.clear(stack)
    prefix, topics = make_topics(stack, f'bo{mode[0]}', {'a': 2, 'b': 2})
    group = f'e09-{stack.owner}-bo{mode[0]}-group'
    want, records = of.publish_conflicts(stack, topics, phase='newest')
    of.publish_nodes(stack, prefix)
    text = of.schema(prefix, group, topics=topics, order_by=True, mode='bulk')
    first = bulk_run(stack, docker_client, registry, service_config, run, 'start-1', text, BULK_ARGS[mode], poll_every=0.05)
    assert first['result'].exit_code == 0, first['result'].tail
    bulk_cleanup(stack, docker_client, first)
    assert of.relationships(stack) == {pair: value for pair, value in want.items() if value[0] >= 400}, 'run 1 stores the newest versions'

    of.publish_conflicts(stack, topics, phase='older')  # older versions (and noise) arrive after the newest is stored
    second = bulk_run(stack, docker_client, registry, service_config, run, 'start-2', text, BULK_ARGS[mode], poll_every=0.05)
    result = second['result']
    assert result.exit_code == 0, result.tail
    seen = bulk_cleanup(stack, docker_client, second)
    base = {'preflight': 1, 'node-loader': 2}
    if mode == 'planned':
        assert {k: v for k, v in seen.items() if k != 'planned-edge-writer'} == {**base, 'planned-dispatcher': 1}
        assert 1 <= seen['planned-edge-writer'] <= 2
    elif mode == 'lease':
        assert seen == {**base, 'bucket-lease-server': 1, 'edge-loader': 1}
    else:
        assert seen == {**base, 'edge-loader': 1}
    assert of.relationships(stack) == want, 'the newest version of every pair must win in this mode'
    ends = {t: stack.end_offsets(t, partitions=2) for t in topics}
    names = list_groups(stack)
    for label, topic in (('OrdPerson', prefix + 'person'), ('OrdItem', prefix + 'item')):
        assert group_offsets(stack, f'{group}-{label}', topic, 1) == {0: stack.end_offsets(topic)[0]}
    if mode == 'planned':
        assert not [n for n in names if n.startswith(f'{group}-RATED')], 'planned bulk commits no relationship group'
        plan_bytes = (second['attempt'] / 'plans' / 'edge-plan.json').read_bytes()
        plan = json.loads(plan_bytes)
        ledger = plan_ledger(second['attempt'], expected_rows=records, expected_digest=hashlib.sha256(plan_bytes).hexdigest(),
                             expected_plan_id=plan['plan_id'])
        assert ledger['rows'] == records
    else:
        groups = bulk_groups(second['snapshot'], group)
        assert list(groups) == ['RATED'] and groups['RATED'] in names, (groups, names)
        assert groups['RATED'] not in bulk_groups(first['snapshot'], group).values()
        for topic in topics:
            assert group_offsets(stack, groups['RATED'], topic, 2) == ends[topic], (topic, ends[topic])
        # Run 2 re-read the older versions with the newest already stored; the classic loader logs the rows it refused as not newer.
        logs = '\n'.join(path.read_text() for path in (run / 'logs').glob('start-2-container-graph-loader-edge-*.log'))
        superseded = sum(int(n) for n in re.findall(r'stage=edge-order-by-superseded .*?skipped_rows=(\d+)', logs))
        (run / 'logs' / 'summary.json').write_text(json.dumps({'superseded_rows': superseded, 'records': records}) + '\n')
        if mode == 'classic':  # the lease writer does not log superseded rows; there the unchanged final graph is the evidence
            assert superseded > 0, f'no older-after-newer conflict was seen by the loader (edge log sample: {logs[-300:]!r})'
    of.clear(stack)
