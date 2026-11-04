"""The generator: deterministic from a seed, a traffic mix that matches its configuration, caps that hold, records that fit the spec."""
import json

from soak import spec
from soak.generator import GenConfig, Generator
from soak.journal import Journal
from soak.tests import helpers as h


def make(tmp_path, name='j.db', **kw):
    journal = Journal(tmp_path / name)
    return journal, Generator(journal, h.TOKEN, GenConfig(**kw))


def dump(journal):
    return [tuple(row)[:-1] for row in journal.query('SELECT rid, kind, type, a_label, a_key, b_label, b_key, topic, msg_key, payload, '
                                                      'valid, window, klass, cohort, born FROM records ORDER BY rid')]


def drive(gen, steps, per_step, journal):
    out = []
    with journal.transaction():
        for i in range(steps):
            out += gen.step(float(i), per_step, window=1)
    return out


def test_the_same_seed_reproduces_the_same_journal(tmp_path):
    (ja, ga), (jb, gb) = make(tmp_path, 'a.db', seed=7), make(tmp_path, 'b.db', seed=7)
    for g in (ga, gb):
        g.bootstrap_stores()
    a, b = drive(ga, 300, 40, ja), drive(gb, 300, 40, jb)
    assert [r.rid for r in a] == [r.rid for r in b] and dump(ja) == dump(jb) and len(dump(ja)) > 5000
    other, go = make(tmp_path, 'c.db', seed=8)
    go.bootstrap_stores()
    drive(go, 300, 40, other)
    assert dump(other) != dump(ja)


def test_the_traffic_mix_matches_the_configuration_within_one_point_on_100000_records(tmp_path):
    journal, gen = make(tmp_path, seed=3)
    gen.bootstrap_stores()
    drive(gen, 1000, 100, journal)
    s = gen.stats
    effective = {k: v for k, v in s.items() if k.startswith('e_')}
    total = sum(effective.values())
    assert total == 100_000
    share = {k: 100 * v / total for k, v in effective.items()}
    for key, want in (('e_new_node', 35), ('e_node_update', 15), ('e_new_rel', 35), ('e_rel_update', 15)):
        assert abs(share[key] - want) < 1.0, (key, share)
    made = len(dump(journal))
    assert abs(100 * s['duplicate'] / made - 2.0) < 1.0
    assert abs(100 * s['malformed'] / made - 0.1) < 1.0
    assert abs(100 * s['older_after_newer'] / s['order_by_update'] - 5.0) < 1.0
    assert abs(100 * s['early_endpoint'] / s['e_new_rel'] - 10.0) < 1.0


def test_the_caps_hold_and_the_capped_share_becomes_updates(tmp_path):
    journal, gen = make(tmp_path, max_customers=40, max_products=15, max_relationships=120)
    gen.bootstrap_stores()
    drive(gen, 400, 50, journal)
    assert gen.customers <= 40 and gen.products <= 15 and gen.relationships <= 120
    assert journal.one('SELECT count(*) FROM pairs')[0] <= 120
    assert gen.stats['cap_new_node_to_update'] > 0 and gen.stats['cap_new_rel_to_update'] > 0


def test_every_valid_record_fits_the_spec_and_every_malformed_one_does_not(tmp_path):
    journal, gen = make(tmp_path, seed=5, malformed=0.02)
    gen.bootstrap_stores()
    drive(gen, 300, 50, journal)
    seen_invalid = 0
    for rec in (journal.record(row['rid']) for row in journal.query('SELECT rid FROM records')):
        if rec.kind == 'N':
            types, required = spec.NODES[rec.type].types, spec.NODES[rec.type].required
            props = rec.payload
        else:
            types, required = spec.RELS[rec.type].types, spec.RELS[rec.type].required
            props = rec.payload['properties']
            assert set(rec.payload) == {'source', 'target', 'properties'}
        fits = set(props) <= set(types) and all(props.get(k) is not None for k in required) and all(
            props[k] is None or _type_ok(types[k], props[k]) for k in props)
        assert fits == rec.valid, (rec.rid, rec.valid, props)
        seen_invalid += not rec.valid
    assert seen_invalid > 10


def _type_ok(kind, value):
    return {'string': isinstance(value, str), 'integer': isinstance(value, int) and not isinstance(value, bool),
            'float': isinstance(value, float), 'datetime': isinstance(value, str) and 'T' in value and not value.startswith('not'),
            'date': isinstance(value, str) and len(value) == 10 and value[0].isdigit()}[kind]


def test_order_by_values_are_fresh_per_rid_and_only_exact_duplicates_reuse_them(tmp_path):
    journal, gen = make(tmp_path, seed=9)
    gen.bootstrap_stores()
    drive(gen, 500, 50, journal)
    seen = {}
    for row in journal.query("SELECT rid, type, a_key, b_label, b_key, payload FROM records WHERE kind = 'R' AND valid = 1"):
        rspec = spec.RELS[row['type']]
        if not rspec.order_by:
            continue
        value = json.loads(row['payload'])['properties'][rspec.order_by]
        key = (row['type'], row['a_key'], row['b_label'], row['b_key'], value)
        assert seen.setdefault(key, row['rid']) == row['rid']


def test_an_early_relationship_is_followed_by_its_endpoint_after_one_to_one_hundred_twenty_seconds(tmp_path):
    journal, gen = make(tmp_path, early_endpoint=1.0, duplicate=0, malformed=0)
    gen.bootstrap_stores()
    with journal.transaction():
        for _ in range(30):                                  # a population to draw the non-early endpoint from
            gen._bump('cust'), gen._bump('prod')
        gen.step(0.0, 200, window=1)
    assert gen.deferred_count() > 20 and gen.stats['early_endpoint'] > 20
    assert not [r for r in gen.step(0.5, 0, window=1) if r.kind == 'N']      # nothing is due before one second
    due = gen.step(121.0, 0, window=1)
    assert len(due) > 20 and all(r.kind == 'N' for r in due) and gen.deferred_count() == 0


def test_null_patch_pairs_use_only_durable_customers_and_are_never_updated_later(tmp_path):
    journal, gen = make(tmp_path, seed=4)
    journal.set_meta('g_baseline_customers', 50)
    records = gen.null_patch_cohort(25, 'calm-1')
    assert len(records) == 50
    first, patch = records[0], records[1]
    assert first.payload['properties']['since'] == '2020-01-01' and patch.payload['properties'] == {'rid': patch.rid, 'since': None, 'strength': 2}
    assert (first.a_key, first.b_key) == (patch.a_key, patch.b_key) and first.klass == patch.klass == 'null_patch'
    assert all(int(r.a_key[1:]) <= 50 and int(r.b_key[1:]) <= 50 for r in records)
    pairs = {(r.a_key, r.b_key) for r in records}
    for _ in range(300):
        gen.step(0.0, 50, window=2)
    updated = {(r.a_key, r.b_key) for r in (journal.record(x['rid']) for x in journal.query(
        "SELECT rid FROM records WHERE type = 'FOLLOWS' AND window = 2")) if (r.a_key, r.b_key) in pairs}
    assert not updated


def test_a_cohort_withholds_its_customers_and_adds_no_later_updates(tmp_path):
    journal, gen = make(tmp_path, seed=6)
    for _ in range(40):
        gen._bump('prod')
    rels, nodes = gen.cohort(200, 'late-1')
    assert len(rels) == len(nodes) == 200 and {r.type for r in rels} == {'PURCHASED'} and {r.cohort for r in rels} == {'late-1'}
    assert [r.a_key for r in rels] == [n.a_key for n in nodes]
    assert len(gen.pending_cust) == 200
    gen.release_cohort(nodes)
    assert not gen.pending_cust


def test_the_baseline_sends_every_node_before_any_relationship(tmp_path):
    journal, gen = make(tmp_path, seed=2)
    records = list(gen.baseline(customers=300, products=80, relationships=1500))
    kinds = [r.kind for r in records]
    assert kinds.index('R') == kinds.count('N') and 'N' not in kinds[kinds.index('R'):]
    nodes = {(r.type, r.a_key) for r in records if r.kind == 'N'}
    assert len(nodes) == 20 + 300 + 80
    for r in (r for r in records if r.kind == 'R'):
        assert (r.a_label, r.a_key) in nodes and (r.b_label, r.b_key) in nodes
    assert len([r for r in records if r.kind == 'R']) == 1500


def test_concurrent_calls_from_the_traffic_thread_and_the_driver_never_share_a_rid(tmp_path):
    import threading
    journal, gen = make(tmp_path, seed=11)
    journal.set_meta('g_baseline_customers', 60)
    gen.bootstrap_stores()
    errors = []

    def traffic():
        try:
            for i in range(150):
                gen.step(float(i), 20, window=1)
        except Exception as error:
            errors.append(error)
    thread = threading.Thread(target=traffic)
    thread.start()
    try:
        for k in range(15):
            gen.null_patch_cohort(3, f'calm-{k}')
    except Exception as error:
        errors.append(error)
    thread.join()
    assert not errors, errors
    rids = [r['rid'] for r in journal.query('SELECT rid FROM records')]
    assert len(rids) == len(set(rids))


def test_the_default_sized_baseline_finishes_quickly(tmp_path):
    """50,000 customers, 10,000 products, 300,000 relationships: hot Zipf customers must not stall the search for distinct pairs."""
    import time
    journal, gen = make(tmp_path, seed=13)
    began = time.time()
    with journal.transaction():
        count = sum(1 for r in gen.baseline(50_000, 10_000, 300_000) if r.kind == 'R')
    assert count == 300_000 and time.time() - began < 120


def test_every_public_generator_call_that_makes_records_holds_the_lock(tmp_path):
    """The driver calls these while the traffic thread runs; a missing lock duplicated a rid in a live run."""
    import inspect
    journal, gen = make(tmp_path)
    for name in ('step', 'drain_deferred', 'bootstrap_stores', 'cohort', 'release_cohort', 'null_patch_cohort'):
        assert hasattr(getattr(type(gen), name), '__wrapped__'), name
