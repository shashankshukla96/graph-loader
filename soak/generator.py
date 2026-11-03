"""Seeded, deterministic traffic for the soak. The generator only creates journalled logical records; sending is the publisher's job.

Determinism: the same seed and the same sequence of ``step``/``baseline``/``cohort`` calls (same ``now`` values) produce the same
records. All state that must survive a harness restart (id counters, pair table, population sizes) lives in the journal.
"""
from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import heapq
import json
import functools
import random
import threading

from . import spec
from .journal import Journal, Record, node_record, rel_record

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)
TIERS = ('free', 'plus', 'pro', 'team')
CITIES = ('Oslo', 'Lyon', 'Pune', 'Lima', 'Kyiv', 'Cork', 'Bern', 'Riga')
CATEGORIES = ('tools', 'toys', 'books', 'food', 'garden')
DEVICES = ('ios', 'android', 'web', 'tv')


@dataclass
class GenConfig:
    seed: int = 1
    max_customers: int = 300_000
    max_products: int = 30_000
    stores: int = 20
    max_relationships: int = 3_000_000
    mix: tuple = (('new_node', 0.35), ('node_update', 0.15), ('new_rel', 0.35), ('rel_update', 0.15))
    new_node_weights: tuple = (('Customer', 0.8), ('Product', 0.2))
    new_rel_weights: tuple = (('PURCHASED', 0.40), ('FOLLOWS', 0.25), ('VIEWED', 0.20), ('STOCKS', 0.15))
    early_endpoint: float = 0.10       # a new relationship is sent before its endpoint; the endpoint follows 1-120 s later
    early_delay: tuple = (1.0, 120.0)
    duplicate: float = 0.02
    older_after_newer: float = 0.05    # of order_by updates
    malformed: float = 0.001
    null_optional: float = 0.30
    zipf: float = 1.1


def instant(slot: int) -> str:
    return (BASE + timedelta(seconds=10 * slot)).isoformat()


def cust_id(i):
    return f'c{i:07d}'


def prod_id(i):
    return f'p{i:06d}'


def store_id(i):
    return f's{i:02d}'


def locked(method):
    """Public generator calls come from the traffic thread and from the driver (cohorts, quiet points): one at a time."""
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        with self.lock:
            return method(self, *args, **kwargs)
    return wrapper


class Generator:
    def __init__(self, journal: Journal, token: str, config: GenConfig | None = None):
        self.j, self.token, self.cfg = journal, token, config or GenConfig()
        self.lock = threading.RLock()
        self.rng = random.Random(self.cfg.seed)
        self.deferred = []       # heap of (due, seq, Record) node records published after their relationships
        self._seq = 0
        self.pending_cust, self.pending_prod = set(), set()
        self.recent = deque(maxlen=2000)
        self.stats = Counter()
        self.window = 0
        self.topics = {t.suffix: spec.topic_name(token, t.suffix) for r in spec.RELS.values() for t in r.topics}
        self.topics.update({n.topic: spec.topic_name(token, n.topic) for n in spec.NODES.values()})

    # ---- counters (persistent) --------------------------------------------------------------------------------------
    def _get(self, name):
        return int(self.j.meta('g_' + name, 0))

    def _bump(self, name, by=1):
        value = self._get(name) + by
        self.j.set_meta('g_' + name, value)
        return value

    def rid(self):
        return f'r{self._bump("rid"):010d}'

    @property
    def customers(self):
        return self._get('cust')

    @property
    def products(self):
        return self._get('prod')

    @property
    def relationships(self):
        return self._get('rels')

    # ---- sampling ---------------------------------------------------------------------------------------------------
    def zipf(self, n):
        """A rank in [1, n], Zipf(s) by Devroye's rejection method, truncated at n."""
        s = self.cfg.zipf
        b = 2 ** (s - 1)
        while True:
            u, v = 1.0 - self.rng.random(), self.rng.random()
            x = int(u ** (-1.0 / (s - 1.0)))
            if x < 1 or x > n:
                continue
            t = (1.0 + 1.0 / x) ** (s - 1.0)
            if v * x * (t - 1.0) / (b - 1.0) <= t / b:
                return x

    def pick(self, weights):
        roll, acc = self.rng.random(), 0.0
        for item, weight in weights:
            acc += weight
            if roll < acc:
                return item
        return weights[-1][0]

    def existing_customer(self):
        n = self.customers
        if n <= 0:
            return None
        for _ in range(20):
            i = self.zipf(n)
            if i not in self.pending_cust:
                return cust_id(i)
        return None

    def existing_product(self):
        n = self.products
        if n <= 0:
            return None
        for _ in range(20):
            i = self.rng.randint(1, n)
            if i not in self.pending_prod:
                return prod_id(i)
        return None

    def store(self):
        return store_id(self.rng.randint(1, self.cfg.stores))

    # ---- node records -----------------------------------------------------------------------------------------------
    def _node_payload(self, label, key, *, update=False):
        r = self.rng
        rid = self.rid()
        nullable = lambda value: None if update and r.random() < self.cfg.null_optional else value  # noqa: E731
        if label == 'Customer':
            payload = {'customer_id': key, 'rid': rid, 'name': f'Customer {key[1:]}-{r.randint(0, 99)}'}
            for name, value in (('tier', r.choice(TIERS)), ('city', r.choice(CITIES)),
                                ('updated_at', (BASE + timedelta(seconds=r.randint(0, 10 ** 7))).isoformat())):
                value = nullable(value)
                if value is not None or r.random() < 0.5:
                    payload[name] = value
        elif label == 'Product':
            payload = {'product_id': key, 'rid': rid, 'title': f'Product {key[1:]}', 'price': round(r.uniform(1, 500), 2)}
            category = nullable(r.choice(CATEGORIES))
            if category is not None or r.random() < 0.5:
                payload['category'] = category
        else:
            payload = {'store_id': key, 'rid': rid, 'name': f'Store {key[1:]}'}
        return rid, payload

    def make_node(self, label, key, *, update=False, klass=None, cohort=None) -> Record:
        rid, payload = self._node_payload(label, key, update=update)
        rec = node_record(rid, label, key, self.topics[spec.NODES[label].topic], payload, window=self.window, klass=klass, cohort=cohort)
        self.j.add_record(rec)
        return rec

    def corrupt(self, rec: Record) -> Record:
        """A malformed copy: a required property missing or a wrong type. Its rid is fresh; it never changes the model."""
        payload = json.loads(json.dumps(rec.payload))
        target = payload['properties'] if rec.kind == 'R' else payload
        types = (spec.RELS[rec.type] if rec.kind == 'R' else spec.NODES[rec.type]).types
        keys = spec.RELS[rec.type].required if rec.kind == 'R' else spec.NODES[rec.type].required
        droppable = sorted(k for k in keys if k != 'rid' and k in target and not (rec.kind == 'N' and k == spec.NODES[rec.type].key))
        if droppable and self.rng.random() < 0.5:
            del target[self.rng.choice(droppable)]
        else:
            name = self.rng.choice(sorted(k for k in types if k not in ('rid',) and not (rec.kind == 'N' and k == spec.NODES[rec.type].key)))
            target[name] = {'string': ['bad'], 'integer': 'not-a-number', 'float': 'not-a-number', 'datetime': 'not-a-datetime', 'date': 'not-a-date'}[types[name]]
        rid = self.rid()
        target['rid'] = rid
        bad = Record(rid, rec.kind, rec.type, rec.a_label, rec.a_key, rec.b_label, rec.b_key, rec.topic, rec.msg_key, payload,
                     valid=False, window=self.window, klass=rec.klass if rec.kind == 'R' else None)
        self.j.add_record(bad)
        self.stats['malformed'] += 1
        return bad

    # ---- relationship records ---------------------------------------------------------------------------------------
    def _rel_topic(self, rel, source_label, target_label, *, other_than=None):
        topics = [t.suffix for t in spec.RELS[rel].pair_topics(source_label, target_label)]
        if other_than is not None and len(topics) > 1:
            topics = [t for t in topics if t != other_than]
        if len(topics) == 1:
            return topics[0]
        return topics[0] if self.rng.random() < 0.6 else topics[1]

    def _rel_props(self, rel, slot, rid, *, klass=None, nulls=False):
        r = self.rng
        if rel == 'PURCHASED':
            return {'rid': rid, 'qty': r.randint(1, 9), 'amount': round(r.uniform(1, 900), 2), 'version': slot * 10, 'at': instant(slot)}
        if rel == 'STOCKS':
            return {'rid': rid, 'quantity': r.randint(0, 500), 'updated_at': instant(slot)}
        if rel == 'VIEWED':
            return {'rid': rid, 'seen_at': instant(slot), 'device': r.choice(DEVICES)}
        props = {'rid': rid, 'since': (BASE + timedelta(days=r.randint(0, 3000))).date().isoformat(), 'strength': r.randint(1, 5)}
        return props

    def make_rel(self, rel, source, target, target_label, *, slot=0, topic=None, klass=None, props=None, cohort=None, source_label=None) -> Record:
        rid = self.rid()
        rspec = spec.RELS[rel]
        source_label = source_label or rspec.topics[0].source
        body = props if props is not None else self._rel_props(rel, slot, rid, klass=klass)
        body = {**body, 'rid': rid}
        payload = {'source': {spec.key_of(source_label): source}, 'target': {spec.key_of(target_label): target}, 'properties': body}
        topic = topic or self._rel_topic(rel, source_label, target_label)
        rec = rel_record(rid, rel, source_label, source, target_label, target, self.topics[topic], payload, window=self.window,
                         klass=klass, cohort=cohort)
        self.j.add_record(rec)
        return rec

    def _pair(self, rel, source, target_label, target):
        return self.j.one('SELECT rowid AS id, * FROM pairs WHERE type=? AND a_key=? AND b_label=? AND b_key=?',
                          (rel, source, target_label, target))

    def _register_pair(self, rel, source, target_label, target, klass):
        self.j.execute('INSERT OR IGNORE INTO pairs(type, a_key, b_label, b_key, klass, n) VALUES (?,?,?,?,?,0)',
                       (rel, source, target_label, target, klass))
        self._bump('rels')

    def _pair_slot(self, rel, source, target_label, target, advance):
        row = self._pair(rel, source, target_label, target)
        n = int(row['n']) if row else 0
        self.j.execute('UPDATE pairs SET n = ? WHERE type=? AND a_key=? AND b_label=? AND b_key=?',
                       (n + advance, rel, source, target_label, target))
        return n

    # ---- logical operations -----------------------------------------------------------------------------------------
    def _new_node(self, now) -> list:
        label = self.pick(self.cfg.new_node_weights)
        if label == 'Customer' and self.customers >= self.cfg.max_customers or label == 'Product' and self.products >= self.cfg.max_products:
            self.stats['cap_new_node_to_update'] += 1
            return self._node_update(now)
        index = self._bump('cust' if label == 'Customer' else 'prod')
        key = cust_id(index) if label == 'Customer' else prod_id(index)
        self.stats['e_new_node'] += 1
        return [self.make_node(label, key)]

    def _node_update(self, now) -> list:
        label = self.pick((('Customer', 0.7), ('Product', 0.25), ('Store', 0.05)))
        key = {'Customer': self.existing_customer, 'Product': self.existing_product, 'Store': self.store}[label]()
        if key is None:
            return self._new_node(now) if self.customers == 0 and self.products == 0 else []
        self.stats['e_node_update'] += 1
        return [self.make_node(label, key, update=True)]

    def _endpoint(self, label, *, early: bool, now):
        """(key, deferred node record or None). An early endpoint is a brand-new key whose node is published after 1-120 s."""
        if early and label in ('Customer', 'Product'):
            full = (self.customers >= self.cfg.max_customers) if label == 'Customer' else (self.products >= self.cfg.max_products)
            if not full:
                index = self._bump('cust' if label == 'Customer' else 'prod')
                key = cust_id(index) if label == 'Customer' else prod_id(index)
                (self.pending_cust if label == 'Customer' else self.pending_prod).add(index)
                node = self.make_node(label, key)
                due = now + self.rng.uniform(*self.cfg.early_delay)
                self._seq += 1
                heapq.heappush(self.deferred, (due, self._seq, node))
                self.stats['early_endpoint'] += 1
                return key
        key = {'Customer': self.existing_customer, 'Product': self.existing_product, 'Store': self.store}[label]()
        return key

    def _new_rel(self, now) -> list:
        if self.relationships >= self.cfg.max_relationships:
            self.stats['cap_new_rel_to_update'] += 1
            return self._rel_update(now)
        rel = self.pick(self.cfg.new_rel_weights)
        if rel == 'PURCHASED':
            source_label, target_label = 'Customer', 'Product'
        elif rel == 'STOCKS':
            source_label, target_label = 'Store', 'Product'
        elif rel == 'FOLLOWS':
            source_label, target_label = 'Customer', 'Customer'
        else:
            source_label, target_label = 'Customer', ('Product' if self.rng.random() < 0.7 else 'Store')
        # An early relationship misses exactly one endpoint: a brand-new customer or product published 1-120 s later.
        early_side = None
        if self.rng.random() < self.cfg.early_endpoint:
            sides = [side for side, label in (('source', source_label), ('target', target_label)) if label in ('Customer', 'Product')]
            early_side = self.rng.choice(sides)
        for _ in range(12):  # hot keys collide often: draw another pair rather than turn the share into an update
            source = self._endpoint(source_label, early=early_side == 'source', now=now)
            target = self._endpoint(target_label, early=early_side == 'target', now=now)
            if rel == 'FOLLOWS' and source == target and self.rng.random() >= 0.01:
                continue
            if source is None or target is None or self._pair(rel, source, target_label, target) is None:
                break
        else:
            return self._rel_update(now)
        if source is None or target is None:
            return self._new_node(now)
        self._register_pair(rel, source, target_label, target, 'complete' if rel == 'FOLLOWS' else None)
        if spec.RELS[rel].order_by:
            self._pair_slot(rel, source, target_label, target, 1)
        self.stats['e_new_rel'] += 1
        return [self.make_rel(rel, source, target, target_label, slot=1, source_label=source_label,
                              klass='complete' if rel == 'FOLLOWS' else None)]

    def _rel_update(self, now) -> list:
        top = self.j.one('SELECT max(rowid) AS m FROM pairs')['m']
        if not top:
            return self._new_node(now)
        for _ in range(12):
            row = self.j.one('SELECT * FROM pairs WHERE rowid = ?', (self.rng.randint(1, top),))
            if row is not None and row['klass'] in (None, 'complete'):
                break
        else:
            return []
        rel, source, target_label, target = row['type'], row['a_key'], row['b_label'], row['b_key']
        self.stats['e_rel_update'] += 1
        rspec = spec.RELS[rel]
        source_label = [t.source for t in rspec.topics if t.target == target_label][0]
        if not rspec.order_by:
            return [self.make_rel(rel, source, target, target_label, source_label=source_label, klass='complete')]
        n = self._pair_slot(rel, source, target_label, target, 1)
        self.stats['order_by_update'] += 1
        if self.rng.random() < self.cfg.older_after_newer:
            self._pair_slot(rel, source, target_label, target, 1)  # the older record takes slot n+1, the newer n+2
            newer = self.make_rel(rel, source, target, target_label, slot=n + 2, source_label=source_label)
            older = self.make_rel(rel, source, target, target_label, slot=n + 1, source_label=source_label,
                                  topic=self._other_topic(rel, source_label, target_label, newer.topic))
            self.stats['older_after_newer'] += 1
            return [newer, older]
        return [self.make_rel(rel, source, target, target_label, slot=n + 1, source_label=source_label)]

    def _other_topic(self, rel, source_label, target_label, topic):
        suffix = topic.split(f'{spec.NAMESPACE}-{self.token}-', 1)[1]
        return self._rel_topic(rel, source_label, target_label, other_than=suffix)

    # ---- driver-facing API ------------------------------------------------------------------------------------------
    @locked
    def step(self, now: float, count: int, window: int | None = None) -> list:
        """``count`` logical operations at logical time ``now`` (seconds): returns the records to send, in send order."""
        if window is not None:
            self.window = window
        out = []
        while self.deferred and self.deferred[0][0] <= now:
            _, _, node = heapq.heappop(self.deferred)
            index = int(node.a_key[1:])
            (self.pending_cust if node.type == 'Customer' else self.pending_prod).discard(index)
            out.append(node)
        for _ in range(count):
            op = self.pick(self.cfg.mix)
            made = {'new_node': self._new_node, 'node_update': self._node_update, 'new_rel': self._new_rel, 'rel_update': self._rel_update}[op](now)
            for rec in made:
                if self.rng.random() < self.cfg.malformed:
                    out.append(self.corrupt(rec))
                out.append(rec)
                self.recent.append(rec)
                if self.rng.random() < self.cfg.duplicate:
                    out.append(self.recent[self.rng.randrange(len(self.recent))])
                    self.stats['duplicate'] += 1
        self.stats['records'] += len(out)
        return out

    @locked
    def drain_deferred(self) -> list:
        """Every deferred endpoint now (quiet points and checkpoints send all endpoints first)."""
        out = []
        while self.deferred:
            _, _, node = heapq.heappop(self.deferred)
            (self.pending_cust if node.type == 'Customer' else self.pending_prod).discard(int(node.a_key[1:]))
            out.append(node)
        return out

    @locked
    def deferred_count(self) -> int:
        return len(self.deferred)

    @locked
    def bootstrap_stores(self) -> list:
        return [self.make_node('Store', store_id(i)) for i in range(1, self.cfg.stores + 1)]

    def baseline(self, customers=50_000, products=10_000, relationships=300_000):
        """Nodes first, then relationships, every endpoint existing: yields records in send order."""
        for rec in self.bootstrap_stores():
            yield rec
        for _ in range(customers):
            yield self.make_node('Customer', cust_id(self._bump('cust')))
        for _ in range(products):
            yield self.make_node('Product', prod_id(self._bump('prod')))
        self.j.set_meta('g_baseline_customers', customers)
        weights = (('PURCHASED', 0.45), ('FOLLOWS', 0.25), ('VIEWED', 0.20), ('STOCKS', 0.10))
        made, attempts = 0, 0
        while made < relationships and attempts < relationships * 4:
            attempts += 1
            # Zipf concentrates on a few customers, whose pairs run out: after a collision draw the customer uniformly instead.
            hot = (lambda: self.zipf(customers)) if self.rng.random() < 0.7 else (lambda: self.rng.randint(1, customers))
            rel = self.pick(weights)
            if rel == 'PURCHASED':
                source_label, target_label, source, target = 'Customer', 'Product', cust_id(hot()), prod_id(self.rng.randint(1, products))
            elif rel == 'STOCKS':
                source_label, target_label, source, target = 'Store', 'Product', self.store(), prod_id(self.rng.randint(1, products))
            elif rel == 'FOLLOWS':
                source_label, target_label, source, target = 'Customer', 'Customer', cust_id(hot()), cust_id(self.rng.randint(1, customers))
            else:
                target_label = 'Product' if self.rng.random() < 0.7 else 'Store'
                source_label, source = 'Customer', cust_id(hot())
                target = prod_id(self.rng.randint(1, products)) if target_label == 'Product' else self.store()
            if self._pair(rel, source, target_label, target) is not None:
                continue
            self._register_pair(rel, source, target_label, target, 'complete' if rel == 'FOLLOWS' else None)
            if spec.RELS[rel].order_by:
                self._pair_slot(rel, source, target_label, target, 1)
            made += 1
            yield self.make_rel(rel, source, target, target_label, slot=1, source_label=source_label, klass='complete' if rel == 'FOLLOWS' else None)
        if made < relationships:
            raise RuntimeError(f'baseline: only {made} of {relationships} distinct pairs found in {attempts} attempts')

    @locked
    def cohort(self, n: int, name: str):
        """``n`` new PURCHASED pairs over new customers (and existing products), no later updates: (relationships, withheld customer nodes)."""
        rels, nodes = [], []
        for _ in range(n):
            index = self._bump('cust')
            key = cust_id(index)
            self.pending_cust.add(index)
            product = self.existing_product()
            self._register_pair('PURCHASED', key, 'Product', product, 'cohort')
            self._pair_slot('PURCHASED', key, 'Product', product, 1)
            rels.append(self.make_rel('PURCHASED', key, product, 'Product', slot=1, source_label='Customer', cohort=name))
            nodes.append(self.make_node('Customer', key, cohort=name))
        return rels, nodes

    @locked
    def release_cohort(self, nodes):
        for node in nodes:
            self.pending_cust.discard(int(node.a_key[1:]))
        return nodes

    @locked
    def null_patch_cohort(self, n: int, name: str) -> list:
        """Pairs over durable customers: a non-null value, then a patch whose ``since`` is explicitly null. Records come in pair order."""
        durable = max(int(self.j.meta('g_baseline_customers', 0)), int(self.j.meta('durable_customers', 0)))
        out, attempts = [], 0
        if durable < 2:
            return out
        made = 0
        while made < n and attempts < n * 50:
            attempts += 1
            source, target = cust_id(self.rng.randint(1, durable)), cust_id(self.rng.randint(1, durable))
            if source == target or self._pair('FOLLOWS', source, 'Customer', target) is not None:
                continue
            self._register_pair('FOLLOWS', source, 'Customer', target, 'null_patch')
            first = self.make_rel('FOLLOWS', source, target, 'Customer', klass='null_patch', source_label='Customer', cohort=name,
                                  props={'since': '2020-01-01', 'strength': 1})
            patch = self.make_rel('FOLLOWS', source, target, 'Customer', klass='null_patch', source_label='Customer', cohort=name,
                                  props={'since': None, 'strength': 2}, topic='follows')
            out += [first, patch]
            made += 1
        return out
