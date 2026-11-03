"""Negative controls: the checker must fail on every planted defect, and pass on the model's own graph."""
import copy

import pytest

from soak.checker import Checker, compare_all
from soak.tests import helpers as h


def healthy(tmp_path):
    journal, oracle = h.new_journal(tmp_path)
    h.customer(journal, 'c1', name='Ada', tier='pro')
    h.customer(journal, 'c2', name='Bob', city='Lyon')
    h.product(journal, 'p1')
    h.store(journal, 's01')
    h.purchased(journal, 'c1', 'p1', 10)
    h.purchased(journal, 'c1', 'p1', 20, topic='purchases-app')
    h.stocks(journal, 's01', 'p1', '2026-01-01T09:00:00+00:00')
    h.follows(journal, 'c1', 'c2', {'since': '2020-01-01', 'strength': 3}, klass='complete')
    h.rel(journal, 'VIEWED', 'Customer', 'c1', 'Store', 's01', 'views-store', {'seen_at': '2026-01-01T00:00:00+00:00', 'device': 'tv'})
    graph = h.FakeGraph()
    h.mirror(journal, oracle, graph)
    return journal, oracle, graph


def run(oracle, graph):
    return compare_all(oracle, graph)


def failing(reports):
    return {name: r for name, r in reports.items() if not r.ok}


def test_the_model_s_own_graph_passes(tmp_path):
    _, oracle, graph = healthy(tmp_path)
    reports = run(oracle, graph)
    assert not failing(reports)
    assert reports['PURCHASED:Customer->Product'].compared == 1 and reports['Customer'].compared == 2


def test_a_planted_missing_node_fails(tmp_path):
    _, oracle, graph = healthy(tmp_path)
    del graph.nodes_['Customer']['c2']
    report = failing(run(oracle, graph))['Customer']
    assert report.counts['missing'] == 1 and report.missing[0]['key'] == ['c2', '']


def test_a_planted_extra_node_fails(tmp_path):
    _, oracle, graph = healthy(tmp_path)
    graph.nodes_['Product']['p-extra'] = {'product_id': 'p-extra', 'rid': 'zz', 'title': 'x', 'price': 1.0}
    report = failing(run(oracle, graph))['Product']
    assert report.counts['extra'] == 1 and report.extra[0]['key'] == ['p-extra', '']


def test_a_planted_wrong_property_fails(tmp_path):
    _, oracle, graph = healthy(tmp_path)
    graph.nodes_['Customer']['c1']['tier'] = 'free'
    report = failing(run(oracle, graph))['Customer']
    assert report.counts['different'] == 1 and report.different[0]['observed']['tier'] == 'free'


def test_a_planted_extra_relationship_fails(tmp_path):
    _, oracle, graph = healthy(tmp_path)
    graph.rels[('STOCKS', 'Store', 'Product')].append(('s01', 'p-ghost', {'rid': 'g', 'quantity': 1, 'updated_at': '2026-01-01T00:00:00.000000Z'}))
    report = failing(run(oracle, graph))['STOCKS:Store->Product']
    assert report.counts['extra'] == 1 and report.extra[0]['key'] == ['s01', 'p-ghost']


def test_a_planted_duplicate_relationship_fails(tmp_path):
    _, oracle, graph = healthy(tmp_path)
    graph.rels[('PURCHASED', 'Customer', 'Product')].append(copy.deepcopy(graph.rels[('PURCHASED', 'Customer', 'Product')][0]))
    report = failing(run(oracle, graph))['PURCHASED:Customer->Product']
    assert report.counts['duplicates'] == 1


def test_a_planted_wrong_order_by_winner_fails(tmp_path):
    _, oracle, graph = healthy(tmp_path)
    # the graph kept the version-10 record although version 20 arrived on the other topic
    loser = next(r for r in oracle.journal.query("SELECT payload FROM records WHERE rid IN (SELECT rid FROM sends)")
                 if '"version":10' in r['payload'])
    import json
    props = json.loads(loser['payload'])['properties']
    graph.rels[('PURCHASED', 'Customer', 'Product')] = [('c1', 'p1', {**props, 'at': '2026-01-01T00:00:00.000000Z'})]
    report = failing(run(oracle, graph))['PURCHASED:Customer->Product']
    assert report.counts['different'] == 1


def test_a_complete_follows_pair_ending_at_an_earlier_complete_record_is_permitted_and_counted(tmp_path):
    journal, oracle, graph = healthy(tmp_path)
    h.follows(journal, 'c1', 'c2', {'since': '2021-02-02', 'strength': 4}, klass='complete')
    oracle.refresh()
    reports = run(oracle, graph)  # the graph still holds the first record: a permitted B5 outcome
    assert not failing(reports) and reports['FOLLOWS:Customer->Customer'].b5_differs_from_last == 1
    graph.rels[('FOLLOWS', 'Customer', 'Customer')] = [('c1', 'c2', {'rid': 'other', 'since': '1999-01-01', 'strength': 1})]
    assert failing(run(oracle, graph))['FOLLOWS:Customer->Customer'].counts['different'] == 1


def test_pending_and_inconclusive_identities_are_skipped_not_asserted(tmp_path):
    _, oracle, graph = healthy(tmp_path)
    ident = ('R', 'STOCKS', 'Store', 's01', 'Product', 'p1')
    graph.rels[('STOCKS', 'Store', 'Product')] = []
    assert 'STOCKS:Store->Product' in failing(run(oracle, graph))
    reports = compare_all(oracle, graph, pending={ident})
    assert not failing(reports) and reports['STOCKS:Store->Product'].pending_skipped == 1
    assert not failing(compare_all(oracle, graph, inconclusive={ident}))
