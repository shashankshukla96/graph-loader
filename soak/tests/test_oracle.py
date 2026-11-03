"""The oracle: what the graph must hold, folded from the journal's physical sends."""
import pytest

from soak.journal import JournalError, node_record
from soak.oracle import AmbiguousJournal
from soak.tests import helpers as h


def test_a_null_update_keeps_the_stored_value(tmp_path):
    journal, oracle = h.new_journal(tmp_path)
    h.customer(journal, 'c1', name='Ada', tier='pro', city='Oslo')
    # The update sends tier as an explicit null and leaves city out: neither clears what is stored.
    r = h.rid()
    h.put(journal, node_record(r, 'Customer', 'c1', h.spec.topic_name(h.TOKEN, 'customers'),
                               {'customer_id': 'c1', 'rid': r, 'name': 'Ada L', 'tier': None}))
    oracle.refresh()
    entry = oracle.entry(('N', 'Customer', 'Customer', 'c1', '', ''))
    assert entry.props == {'customer_id': 'c1', 'rid': r, 'name': 'Ada L', 'tier': 'pro', 'city': 'Oslo'}


def test_order_by_picks_the_greatest_instant_whatever_the_arrival_order_and_offset_text(tmp_path):
    journal, oracle = h.new_journal(tmp_path)
    # 10:00+02:00 is 08:00Z: it is OLDER than 09:00Z although its text sorts later and it arrives later (offset 5 > 1).
    h.stocks(journal, 's01', 'p1', '2026-01-01T10:00:00+02:00', partition=2, offset=5, qty=1)
    _, newer = h.stocks(journal, 's01', 'p1', '2026-01-01T09:00:00+00:00', partition=2, offset=1, qty=2)
    oracle.refresh()
    entry = oracle.entry(('R', 'STOCKS', 'Store', 's01', 'Product', 'p1'))
    assert entry.winner == newer and entry.props['quantity'] == 2
    assert entry.props['updated_at'] == '2026-01-01T09:00:00.000000Z'


def test_multi_pair_identities_are_separated_by_endpoint_label(tmp_path):
    journal, oracle = h.new_journal(tmp_path)
    h.rel(journal, 'VIEWED', 'Customer', 'c1', 'Product', 'x1', 'views-product', {'seen_at': '2026-01-01T00:00:00+00:00', 'device': 'ios'})
    h.rel(journal, 'VIEWED', 'Customer', 'c1', 'Store', 'x1', 'views-store', {'seen_at': '2026-01-02T00:00:00+00:00', 'device': 'tv'})
    oracle.refresh()
    to_product = oracle.entry(('R', 'VIEWED', 'Customer', 'c1', 'Product', 'x1'))
    to_store = oracle.entry(('R', 'VIEWED', 'Customer', 'c1', 'Store', 'x1'))
    assert to_product.props['device'] == 'ios' and to_store.props['device'] == 'tv'
    assert oracle.counts() == {'VIEWED:Customer:Product': 1, 'VIEWED:Customer:Store': 1}


def test_a_journal_that_reuses_an_order_by_value_with_different_properties_is_rejected(tmp_path):
    journal, oracle = h.new_journal(tmp_path)
    h.purchased(journal, 'c1', 'p1', 10, topic='purchases-web')
    h.purchased(journal, 'c1', 'p1', 10, topic='purchases-app')  # a fresh rid with the same version: not an exact duplicate
    with pytest.raises(AmbiguousJournal):
        oracle.refresh()


def test_an_exact_duplicate_reuses_the_rid_and_the_payload_and_a_different_payload_is_refused(tmp_path):
    journal, oracle = h.new_journal(tmp_path)
    _, r = h.purchased(journal, 'c1', 'p1', 10)
    again = journal.record(r)
    journal.add_record(again)  # identical: accepted, nothing new
    again.payload = {**again.payload, 'properties': {**again.payload['properties'], 'qty': 99}}
    with pytest.raises(JournalError):
        journal.add_record(again)


def test_the_follows_patch_fold_keeps_since_and_takes_the_new_strength(tmp_path):
    journal, oracle = h.new_journal(tmp_path)
    h.follows(journal, 'c1', 'c2', {'since': '2020-01-01', 'strength': 1}, klass='null_patch')
    _, last = h.follows(journal, 'c1', 'c2', {'since': None, 'strength': 2}, klass='null_patch')
    oracle.refresh()
    entry = oracle.entry(('R', 'FOLLOWS', 'Customer', 'c1', 'Customer', 'c2'))
    assert entry.mode == 'exact' and entry.props == {'rid': last, 'since': '2020-01-01', 'strength': 2}


def test_a_complete_follows_pair_may_end_at_any_complete_record_it_was_sent(tmp_path):
    journal, oracle = h.new_journal(tmp_path)
    _, a = h.follows(journal, 'c1', 'c2', {'since': '2020-01-01', 'strength': 1}, klass='complete')
    _, b = h.follows(journal, 'c1', 'c2', {'since': '2021-01-01', 'strength': 2}, klass='complete')
    oracle.refresh()
    entry = oracle.entry(('R', 'FOLLOWS', 'Customer', 'c1', 'Customer', 'c2'))
    assert entry.mode == 'permitted' and entry.props['rid'] == b
    assert {p['rid'] for p in entry.permitted} == {a, b}


def test_an_exact_resend_at_a_later_offset_re_applies_old_values(tmp_path):
    journal, oracle = h.new_journal(tmp_path)
    _, old = h.customer(journal, 'c1', name='Old', tier='free', offset=0)
    h.customer(journal, 'c1', name='New', tier='pro', offset=1)
    send = journal.new_send(old, window=0)
    journal.mark_ack(send, h.spec.topic_name(h.TOKEN, 'customers'), 0, 2)  # the exact duplicate of the first record, later
    oracle.refresh()
    assert oracle.entry(('N', 'Customer', 'Customer', 'c1', '', '')).props['name'] == 'Old'


def test_a_late_acknowledgement_below_an_already_folded_offset_is_folded_in_offset_order(tmp_path):
    journal, oracle = h.new_journal(tmp_path)
    _, first = h.customer(journal, 'c1', name='First', tier='free', state='uncertain')
    h.customer(journal, 'c1', name='Second', tier='pro', offset=7)
    oracle.refresh()
    assert oracle.entry(('N', 'Customer', 'Customer', 'c1', '', '')).props['name'] == 'Second'
    send = journal.query("SELECT send_id FROM sends WHERE rid = ?", (first,))[0]['send_id']
    journal.mark_ack(send, h.spec.topic_name(h.TOKEN, 'customers'), 0, 3)  # it landed before offset 7 after all
    oracle.refresh()
    assert oracle.entry(('N', 'Customer', 'Customer', 'c1', '', '')).props['name'] == 'Second'  # offset 7 still last


def test_malformed_records_never_enter_the_model_and_are_accounted_as_rejected(tmp_path):
    journal, oracle = h.new_journal(tmp_path)
    h.purchased(journal, 'c1', 'p1', 10)
    h.rel(journal, 'PURCHASED', 'Customer', 'c1', 'Product', 'p1', 'purchases-web', {'qty': 'x', 'version': 20}, valid=False)
    oracle.refresh()
    assert oracle.entry(('R', 'PURCHASED', 'Customer', 'c1', 'Product', 'p1')).props['version'] == 10
    buckets = oracle.accounting()
    assert buckets['rejected'] == 1 and buckets['applied'] == 1


def test_accounting_separates_superseded_never_enqueued_and_uncertain(tmp_path):
    journal, oracle = h.new_journal(tmp_path)
    h.purchased(journal, 'c1', 'p1', 10)
    h.purchased(journal, 'c1', 'p1', 20)                       # the winner; the first is superseded
    h.purchased(journal, 'c2', 'p1', 10, state='never')
    h.purchased(journal, 'c3', 'p1', 10, state='uncertain')
    oracle.refresh()
    assert oracle.accounting() == {'applied': 1, 'superseded': 1, 'rejected': 0, 'never_enqueued': 1, 'uncertain': 1, 'unsent': 0}
