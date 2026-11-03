"""Quiet points, reconciliation checkpoints, pending and inconclusive handling, uncertain sends and the drain predicates."""
import pytest

from soak import spec
from soak.checker import QuietPointFailed
from soak.kafka_io import DlqFrontier, dlq_bracket_drained, resolve_uncertain, source_lagging
from soak.tests import helpers as h


def populate(journal):
    h.customer(journal, 'c1', name='Ada')
    h.product(journal, 'p1')
    h.store(journal, 's01')


def graph_with_model(oracle, graph, skip=()):
    h.mirror(oracle.journal, oracle, graph, skip=skip)


PURCHASED_C1P1 = ('R', 'PURCHASED', 'Customer', 'c1', 'Product', 'p1')
STOCKS_S1P1 = ('R', 'STOCKS', 'Store', 's01', 'Product', 'p1')


# ---- pending at a quiet point, full check at a checkpoint ---------------------------------------------------------------

def test_an_identity_with_a_parked_copy_is_pending_at_a_quiet_point_whatever_the_graph_holds(tmp_path):
    chk, journal, oracle, kafka, graph = h.checker(tmp_path)
    populate(journal)
    _, rid = h.purchased(journal, 'c1', 'p1', 10)
    h.park(kafka, 'PURCHASED', rid)
    graph_with_model(oracle, graph, skip={PURCHASED_C1P1})       # the graph lacks it: parked, not asserted absent or present
    result = chk.quiet_point()
    assert result['ok'] and result['pending'] == 1
    assert result['reports']['PURCHASED:Customer->Product']['pending_skipped'] == 1
    assert result['pending_by_type'] == {'PURCHASED': 1}


def test_a_parked_original_written_without_a_replay_is_not_modelled_as_absent(tmp_path):
    """Park published, index tombstone failed, retry consumer restarted: the retry consumer wrote the original after all."""
    def replay(rel):
        h.republish(kafka, journal, rid)   # the replay re-sends the same record; the product merges it idempotently
        return {'exit': 0, 'output': ''}
    chk, journal, oracle, kafka, graph = h.checker(tmp_path, replay=replay)
    populate(journal)
    _, rid = h.purchased(journal, 'c1', 'p1', 10)
    h.park(kafka, 'PURCHASED', rid)
    graph_with_model(oracle, graph)                              # the original IS in the graph
    assert chk.quiet_point()['ok']                               # pending: neither asserted present nor absent
    result = chk.checkpoint({'PURCHASED'})
    assert result['ok'] and result['reports']['PURCHASED:Customer->Product']['compared'] == 1
    assert result['journaled_replays'] and result['pending'] == 0


def test_at_a_checkpoint_a_parked_record_the_graph_lacks_is_a_failure(tmp_path):
    chk, journal, oracle, kafka, graph = h.checker(tmp_path, replay=lambda rel: {'exit': 0})
    populate(journal)
    _, rid = h.purchased(journal, 'c1', 'p1', 10)
    h.park(kafka, 'PURCHASED', rid)
    graph_with_model(oracle, graph, skip={PURCHASED_C1P1})
    assert chk.quiet_point()['ok']
    result = chk.checkpoint({'PURCHASED'})                       # the replay changed nothing: the record is still missing
    assert not result['ok'] and result['reports']['PURCHASED:Customer->Product']['counts']['missing'] == 1


def test_a_replayed_record_that_parks_again_before_the_replay_returns_fails_the_checkpoint(tmp_path):
    def replay(rel):
        h.republish(kafka, journal, rid)
        h.park(kafka, 'PURCHASED', rid)                          # it parked again while the command was still running
        return {'exit': 0}
    chk, journal, oracle, kafka, graph = h.checker(tmp_path, replay=replay)
    populate(journal)
    _, rid = h.purchased(journal, 'c1', 'p1', 10)
    h.park(kafka, 'PURCHASED', rid)
    graph_with_model(oracle, graph)
    result = chk.checkpoint({'PURCHASED'})
    assert not result['ok'] and result['reason'] == 'parked_after_boundary' and result['reparked'] == {'PURCHASED': [1, 0, 0, 0, 0, 0]}


def test_a_replayed_record_that_parks_again_during_the_final_drain_fails_the_checkpoint(tmp_path):
    def replay(rel):
        h.republish(kafka, journal, rid)
        kafka.on_committed = lambda: h.park(kafka, 'PURCHASED', rid, partition=2)   # appears while the final drain is polling
        return {'exit': 0}
    chk, journal, oracle, kafka, graph = h.checker(tmp_path, replay=replay)
    populate(journal)
    _, rid = h.purchased(journal, 'c1', 'p1', 10)
    h.park(kafka, 'PURCHASED', rid)
    graph_with_model(oracle, graph)
    result = chk.checkpoint({'PURCHASED'})
    assert not result['ok'] and result['reason'] == 'parked_after_boundary'


def test_a_purchased_checkpoint_keeps_an_unrelated_parked_stocks_identity_pending(tmp_path):
    def replay(rel):
        assert rel == 'PURCHASED'
        h.republish(kafka, journal, purchased_rid)
        return {'exit': 0}
    chk, journal, oracle, kafka, graph = h.checker(tmp_path, replay=replay)
    populate(journal)
    _, purchased_rid = h.purchased(journal, 'c1', 'p1', 10)
    _, stocks_rid = h.stocks(journal, 's01', 'p1', '2026-01-01T09:00:00+00:00')
    h.park(kafka, 'PURCHASED', purchased_rid)
    h.park(kafka, 'STOCKS', stocks_rid)
    graph_with_model(oracle, graph, skip={STOCKS_S1P1})          # STOCKS is parked and absent; PURCHASED is applied
    result = chk.checkpoint({'PURCHASED'})
    assert result['ok'], result['reports']
    assert result['reports']['STOCKS:Store->Product']['pending_skipped'] == 1
    assert result['reports']['PURCHASED:Customer->Product']['compared'] == 1
    assert result['pending'] == 1 and result['pending_by_type'] == {'STOCKS': 1}


def test_the_final_checkpoint_over_every_type_leaves_nothing_pending(tmp_path):
    chk, journal, oracle, kafka, graph = h.checker(tmp_path, replay=lambda rel: {'exit': 0})
    populate(journal)
    _, stocks_rid = h.stocks(journal, 's01', 'p1', '2026-01-01T09:00:00+00:00')
    h.park(kafka, 'STOCKS', stocks_rid)
    graph_with_model(oracle, graph, skip={STOCKS_S1P1})
    result = chk.checkpoint(set(spec.RELS))
    assert not result['ok'] and result['reports']['STOCKS:Store->Product']['counts']['missing'] == 1 and result['pending'] == 0


# ---- null-patch pairs ---------------------------------------------------------------------------------------------------------

def null_patch_pair(journal, a='c1', b='c2', window=3):
    h.follows(journal, a, b, {'since': '2020-01-01', 'strength': 1}, klass='null_patch', window=window)
    return h.follows(journal, a, b, {'since': None, 'strength': 2}, klass='null_patch', window=window)[1]


def cleared_graph(oracle, graph):
    graph_with_model(oracle, graph)
    graph.rels[('FOLLOWS', 'Customer', 'Customer')] = [('c1', 'c2', {'rid': 'x', 'strength': 2})]  # since lost: the product cleared it


def test_a_null_patch_pair_whose_own_record_entered_the_dlq_is_inconclusive_never_rechecked_as_complete(tmp_path):
    chk, journal, oracle, kafka, graph = h.checker(tmp_path, inconclusive_limit=1.0)
    populate(journal)
    h.customer(journal, 'c2', name='Bob')
    patch = null_patch_pair(journal)
    h.dead_letter(kafka, 'FOLLOWS', patch)                       # its own record visited the DLQ
    cleared_graph(oracle, graph)
    result = chk.quiet_point()
    assert result['ok'] and result['null_patch']['eligible'] == 1 and result['null_patch']['inconclusive'] == 1
    assert result['null_patch']['compared'] == 0
    assert result['reports']['FOLLOWS:Customer->Customer']['inconclusive_skipped'] == 1


def test_parking_of_an_unrelated_record_leaves_a_null_patch_pair_strict(tmp_path):
    chk, journal, oracle, kafka, graph = h.checker(tmp_path)
    populate(journal)
    h.customer(journal, 'c2', name='Bob')
    null_patch_pair(journal)
    _, other = h.purchased(journal, 'c1', 'p1', 10)
    h.park(kafka, 'PURCHASED', other)                            # unrelated parking
    cleared_graph(oracle, graph)
    result = chk.quiet_point()
    assert not result['ok'] and result['null_patch']['compared'] == 1 and result['null_patch']['inconclusive'] == 0
    assert result['reports']['FOLLOWS:Customer->Customer']['counts']['different'] == 1


def test_a_null_patch_pair_whose_window_saw_a_fault_is_inconclusive(tmp_path):
    chk, journal, oracle, kafka, graph = h.checker(tmp_path, inconclusive_limit=1.0)
    populate(journal)
    h.customer(journal, 'c2', name='Bob')
    null_patch_pair(journal, window=3)
    journal.flag_window(3, 'restart')
    cleared_graph(oracle, graph)
    result = chk.quiet_point()
    assert result['ok'] and result['null_patch']['inconclusive'] == 1


def test_a_run_with_no_strictly_compared_null_patch_pair_reports_zero(tmp_path):
    chk, journal, oracle, kafka, graph = h.checker(tmp_path)
    populate(journal)
    graph_with_model(oracle, graph)
    assert chk.quiet_point()['null_patch'] == {'eligible': 0, 'inconclusive': 0, 'compared': 0}


# ---- uncertain sends -----------------------------------------------------------------------------------------------------------

def header_msg(kafka, topic, send_id, partition=0, payload=b'{}'):
    return kafka.append(topic, partition, payload, headers={'e13-send': str(send_id)})


def test_uncertain_sends_resolve_only_by_a_positive_header_match(tmp_path):
    chk, journal, oracle, kafka, graph = h.checker(tmp_path)
    topic = spec.topic_name(h.TOKEN, 'customers')
    send, rid = h.customer(journal, 'c1', name='Ada', state='uncertain')
    other, _ = h.customer(journal, 'c9', name='Zed', state='uncertain')
    header_msg(kafka, topic, send, partition=1)
    header_msg(kafka, topic, 'not-a-send-id', partition=2)
    out = resolve_uncertain(journal, kafka, advance_floor=True)
    assert out == {'resolved': 1, 'still_uncertain': 1}
    row = journal.query('SELECT state, part, off FROM sends WHERE send_id = ?', (send,))[0]
    assert (row['state'], row['part'], row['off']) == ('ack', 1, 0)
    assert journal.query('SELECT state FROM sends WHERE send_id = ?', (other,))[0]['state'] == 'uncertain'


def test_an_uncertain_exact_resend_whose_rid_already_landed_is_not_resolved_by_the_rid(tmp_path):
    chk, journal, oracle, kafka, graph = h.checker(tmp_path)
    topic = spec.topic_name(h.TOKEN, 'customers')
    first, rid = h.customer(journal, 'c1', name='Ada', state='ack', offset=0)
    header_msg(kafka, topic, first, payload=b'{"rid": "' + rid.encode() + b'"}')   # the original landed
    resend = journal.new_send(rid)
    journal.mark_uncertain(resend)                               # the exact resend: delivery error, rid already in Kafka
    out = resolve_uncertain(journal, kafka, advance_floor=True)
    assert out == {'resolved': 0, 'still_uncertain': 1}          # a rid match proves nothing about THIS send
    assert journal.uncertain_identities() == {('N', 'Customer', 'Customer', 'c1', '', '')}


def test_a_send_appended_after_the_first_read_back_is_found_at_the_next_one(tmp_path):
    chk, journal, oracle, kafka, graph = h.checker(tmp_path)
    topic = spec.topic_name(h.TOKEN, 'customers')
    send, _ = h.customer(journal, 'c1', name='Ada', state='uncertain')
    assert resolve_uncertain(journal, kafka, advance_floor=True) == {'resolved': 0, 'still_uncertain': 1}   # not absent: unknown
    header_msg(kafka, topic, send, partition=3)                  # the append lands after the read
    assert resolve_uncertain(journal, kafka, advance_floor=True) == {'resolved': 1, 'still_uncertain': 0}


def test_the_scan_floor_does_not_move_when_a_send_was_still_in_flight(tmp_path):
    chk, journal, oracle, kafka, graph = h.checker(tmp_path)
    topic = spec.topic_name(h.TOKEN, 'customers')
    send, _ = h.customer(journal, 'c1', name='Ada', state='uncertain')
    header_msg(kafka, topic, 'someone-else', partition=3)
    resolve_uncertain(journal, kafka, advance_floor=False)
    assert journal.meta('scan_floor', {}) == {}
    resolve_uncertain(journal, kafka, advance_floor=True)
    assert journal.meta('scan_floor')[f'{topic}/3'] == 1


def test_a_quiet_point_lists_uncertain_identities_as_inconclusive_and_never_as_product_writes(tmp_path):
    chk, journal, oracle, kafka, graph = h.checker(tmp_path, inconclusive_limit=1.0)
    populate(journal)
    h.customer(journal, 'c2', name='Bob', state='uncertain')
    graph_with_model(oracle, graph)
    result = chk.quiet_point()
    assert result['ok'] and result['inconclusive'] == 1


# ---- drain predicates ------------------------------------------------------------------------------------------------------------

def test_the_source_drain_treats_an_empty_uncommitted_partition_as_drained_and_a_non_empty_one_as_not():
    assert source_lagging({('t', 0): (5, 5)}, {('t', 0): None}) == []                       # empty (low == high): nothing to commit
    assert source_lagging({('t', 0): (0, 0)}, {}) == []
    assert source_lagging({('t', 0): (0, 7)}, {('t', 0): None}) == [(('t', 0), None, 7)]     # data, no commit
    assert source_lagging({('t', 0): (0, 7)}, {('t', 0): 6}) == [(('t', 0), 6, 7)]
    assert source_lagging({('t', 0): (0, 7)}, {('t', 0): 7}) == []


def test_the_dlq_frontier_treats_an_empty_uncommitted_partition_as_drained_and_a_non_empty_one_as_not():
    tp = ('dlq', 0)
    assert dlq_bracket_drained({tp: 0}, {tp: 0}, {tp: None}, {tp: 0}) == []                 # empty, no commit
    assert dlq_bracket_drained({tp: 0}, {tp: 4}, {tp: None}, {tp: 4})[0][1] == 'no commit on a non-empty partition'
    assert dlq_bracket_drained({tp: 0}, {tp: 4}, {tp: 4}, {tp: 4}) == []
    assert dlq_bracket_drained({tp: 0}, {tp: 4}, {tp: 3}, {tp: 4})[0][1] == 'commit differs from end'


def test_the_dlq_frontier_is_not_fooled_by_a_re_dlq_past_the_first_end_snapshot():
    tp = ('dlq', 0)
    # end 100, a retry re-publishes the row at offset 100 (end becomes 101) and the old copy's commit lands at 100.
    bad = dlq_bracket_drained({tp: 0}, {tp: 100}, {tp: 100}, {tp: 101})
    assert bad and bad[0][1] == 'end moved'
    frontier = DlqFrontier(gap=30)
    assert frontier.observe(0, {tp: 0}, {tp: 100}, {tp: 100}, {tp: 101}, {}) is False


def test_the_dlq_frontier_needs_two_clean_reads_thirty_seconds_apart_with_parked_ends_unchanged():
    tp = ('dlq', 0)
    clean = dict(lows={tp: 0}, ends_before={tp: 10}, commits={tp: 10}, ends_after={tp: 10})
    frontier = DlqFrontier(gap=30)
    assert frontier.observe(0, parked_ends={'p': 1}, **clean) is False
    assert frontier.observe(10, parked_ends={'p': 1}, **clean) is False        # too soon
    assert frontier.observe(31, parked_ends={'p': 2}, **clean) is False        # a park happened between: restart the clock
    assert frontier.observe(70, parked_ends={'p': 2}, **clean) is True
    assert frontier.observe(80, parked_ends={'p': 2}, ends_before={tp: 11}, lows={tp: 0}, commits={tp: 11}, ends_after={tp: 12}) is False


def test_a_quiet_point_that_never_drains_fails_at_its_deadline_naming_the_lagging_partitions(tmp_path):
    chk, journal, oracle, kafka, graph = h.checker(tmp_path, autocommit=False, catchup_bound=60)
    topic = spec.topic_name(h.TOKEN, 'customers')
    kafka.append(topic, 0, {'customer_id': 'c1'})
    with pytest.raises(QuietPointFailed) as error:
        chk.quiet_point()
    assert error.value.reason == 'source_not_drained'
    assert any(topic in str(item) for item in error.value.detail)


def test_the_stable_stream_groups_only_are_checked(tmp_path):
    chk, *_ = h.checker(tmp_path)
    groups = chk.groups()
    assert set(groups) == {f'{h.GROUP}-{name}' for name in ('Customer', 'Product', 'Store', 'PURCHASED', 'STOCKS', 'FOLLOWS', 'VIEWED')}
    assert not any('bulk' in group for group in groups)


def test_more_than_a_tenth_of_a_percent_inconclusive_keys_is_a_harness_failure(tmp_path):
    chk, journal, oracle, kafka, graph = h.checker(tmp_path)
    populate(journal)
    h.customer(journal, 'c2', name='Bob', state='uncertain')
    graph_with_model(oracle, graph)
    result = chk.quiet_point()
    assert not result['ok'] and result['harness_failures']


def test_terminal_accounting_checks_malformed_relationships_in_the_dlq_and_malformed_nodes_in_the_rejection_files(tmp_path):
    topic = spec.topic_name(h.TOKEN, 'customers')
    chk, journal, oracle, kafka, graph = h.checker(tmp_path, rejected=lambda: {(topic, 0, 4)})
    populate(journal)
    h.rel(journal, 'PURCHASED', 'Customer', 'c1', 'Product', 'p1', 'purchases-web', {'qty': 'x', 'version': 20}, valid=False, rid_='bad-rel')
    h.put(journal, h.node_record('bad-node', 'Customer', 'c9', topic, {'customer_id': 'c9', 'rid': 'bad-node'}, valid=False), partition=0, offset=4)
    accounting = chk.accounting()
    assert accounting['malformed_relationships_not_in_dlq'] == ['bad-rel'] and accounting['malformed_nodes_not_rejected'] == []
    assert accounting['unaccounted'] == 1 and not accounting['ok']
    h.dead_letter(kafka, 'PURCHASED', 'bad-rel', failure_class='malformed')
    chk.sync_dlq()
    assert chk.accounting()['ok']
    chk.rejected_locations = lambda: set()                       # the node loaders never rejected it
    assert chk.accounting()['malformed_nodes_not_rejected'] == ['bad-node'] and not chk.accounting()['ok']
