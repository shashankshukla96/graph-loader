"""The expected graph, computed only from the journal (generator sends plus replay read-backs), never from Neo4j.

A parked receipt does not prove a record stayed unapplied, and a replay acknowledgement does not prove a record was applied,
so parking is never modelled as "absent": the checker excludes the identities of parked records (``pending``) until a
reconciliation checkpoint has replayed them and drained. What this module folds is every *acknowledged physical send*:

* nodes: per key, valid records in (topic, partition, offset) order; a non-null declared property overwrites, a null one never
  clears (the product drops nulls before ``SET n +=``);
* ``order_by`` types: per identity the record whose ``order_by`` value is the strictly greatest (instants for datetimes);
* types without ``order_by`` (``FOLLOWS``): null-patch pairs fold in offset order with null-never-clears and are exact; complete
  pairs (every declared property always non-null) may end at any complete record sent for the pair (documented B5 limit), and the
  distance from the last record is counted, not failed.
"""
from __future__ import annotations

import json

from . import spec
from .journal import Journal, JournalError


class AmbiguousJournal(JournalError):
    """Two different records of one identity share an ``order_by`` value: the expected winner is undefined."""


FOLD_SQL = ('SELECT s.send_id AS send_id, s.rid AS rid, s.origin AS origin, s.topic AS topic, s.part AS part, s.off AS off, '
            'r.payload AS payload, r.valid AS valid, r.klass AS klass, r.window AS window '
            # Drive from the identity index: left to itself SQLite scans every acknowledged send per identity (quadratic).
            'FROM records r INDEXED BY records_identity CROSS JOIN sends s INDEXED BY sends_rid ON s.rid = r.rid '
            "WHERE s.state = 'ack' AND r.kind = ? AND r.type = ? AND r.a_label = ? AND r.a_key = ? AND r.b_label = ? AND r.b_key = ? "
            'ORDER BY s.topic, s.part, s.off, s.send_id')


class Entry:
    """The model's verdict for one identity."""
    __slots__ = ('identity', 'mode', 'props', 'permitted', 'klass', 'windows', 'rids', 'winner')

    def __init__(self, identity, mode, props, permitted=None, klass=None, windows=(), rids=(), winner=None):
        self.identity, self.mode, self.props, self.permitted = identity, mode, props, permitted or []
        self.klass, self.windows, self.rids, self.winner = klass, list(windows), list(rids), winner


def _props(row):
    payload = json.loads(row['payload'])
    return payload if 'properties' not in payload or 'source' not in payload else payload['properties']


def fold_node(label, rows) -> Entry | None:
    node = spec.NODES[label]
    props, winner = {}, None
    for row in rows:
        if not row['valid']:
            continue
        for name, value in spec.normalise_props(node.types, _props(row)).items():
            props[name] = value
        winner = row['rid']
    return None if winner is None else Entry(('N', label, label, '', '', ''), 'exact', props, winner=winner)


def fold_order_by(rel, rows) -> Entry | None:
    rspec = spec.RELS[rel]
    best, seen = None, {}
    for row in rows:
        if not row['valid']:
            continue
        raw = _props(row)
        value = spec.order_key(rspec, raw)
        props = spec.normalise_props(rspec.types, raw)
        previous = seen.get(value)
        if previous is not None and previous != props:
            raise AmbiguousJournal(f'{rel}: order_by value {value!r} is reused with different properties ({previous} vs {props})')
        seen[value] = props
        if best is None or value > best[0]:
            best = (value, props, row['rid'])
    return None if best is None else Entry(('R', rel, '', '', '', ''), 'exact', best[1], winner=best[2])


def fold_unordered(rel, rows) -> Entry | None:
    """A type without ``order_by``: exact for null-patch pairs, a permitted set for complete pairs."""
    rspec = spec.RELS[rel]
    valid = [row for row in rows if row['valid']]
    if not valid:
        return None
    klass = valid[0]['klass'] or 'complete'
    props, permitted = {}, []
    for row in valid:
        update = spec.normalise_props(rspec.types, _props(row))
        props.update(update)
        if klass == 'complete' and update not in permitted:
            permitted.append(update)
    last = spec.normalise_props(rspec.types, _props(valid[-1]))
    if klass == 'complete':
        return Entry(('R', rel, '', '', '', ''), 'permitted', last, permitted=permitted, klass=klass, winner=valid[-1]['rid'])
    return Entry(('R', rel, '', '', '', ''), 'exact', props, klass=klass, winner=valid[-1]['rid'])


def fold_identity(identity, rows) -> Entry | None:
    kind, type_ = identity[0], identity[1]
    if kind == 'N':
        entry = fold_node(type_, rows)
    elif spec.RELS[type_].order_by:
        entry = fold_order_by(type_, rows)
    else:
        entry = fold_unordered(type_, rows)
    if entry is not None:
        entry.identity = identity
    return entry


class Oracle:
    def __init__(self, journal: Journal):
        self.journal = journal

    def refresh(self) -> int:
        """Re-fold every identity touched by an acknowledgement since the last refresh; returns how many were re-folded."""
        journal = self.journal
        folded = int(journal.meta('folded_seq', 0))
        top = int(journal.meta('ack_seq'))
        if top <= folded:
            return 0
        rows = journal.query('SELECT DISTINCT r.kind, r.type, r.a_label, r.a_key, r.b_label, r.b_key FROM sends s JOIN records r '
                             "ON r.rid = s.rid WHERE s.state = 'ack' AND s.ack_seq > ? AND s.ack_seq <= ?", (folded, top))
        with journal.transaction():
            for row in rows:
                self._refold(tuple(row))
            journal.set_meta('folded_seq', top)
        return len(rows)

    def _refold(self, identity):
        journal = self.journal
        rows = journal.query(FOLD_SQL, identity)
        entry = fold_identity(identity, rows)
        if entry is None:
            journal.execute('DELETE FROM expected WHERE kind=? AND type=? AND a_label=? AND a_key=? AND b_label=? AND b_key=?', identity)
            return
        meta = journal.query('SELECT rid, klass, window FROM records WHERE kind=? AND type=? AND a_label=? AND a_key=? '
                             'AND b_label=? AND b_key=? AND valid = 1', identity)
        klass = next((m['klass'] for m in meta if m['klass']), None) or entry.klass
        windows = sorted({m['window'] for m in meta})
        rids = [m['rid'] for m in meta] if klass == 'null_patch' else []
        journal.execute('INSERT OR REPLACE INTO expected VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
                        (*identity, entry.mode, json.dumps(entry.props, sort_keys=True), json.dumps(entry.permitted) if entry.permitted else None,
                         klass, json.dumps(windows), json.dumps(rids), entry.winner))

    # ---- reads for the checker --------------------------------------------------------------------------------------
    def entry(self, identity) -> Entry | None:
        row = self.journal.one('SELECT * FROM expected WHERE kind=? AND type=? AND a_label=? AND a_key=? AND b_label=? AND b_key=?', identity)
        return None if row is None else _entry(row)

    def stream(self, kind, type_, a_label, b_label='', batch=2000):
        """Entries of one (type, endpoint labels) in (a_key, b_key) order."""
        last = None
        while True:
            if last is None:
                rows = self.journal.query('SELECT * FROM expected WHERE kind=? AND type=? AND a_label=? AND b_label=? '
                                          'ORDER BY a_key, b_key LIMIT ?', (kind, type_, a_label, b_label, batch))
            else:
                rows = self.journal.query('SELECT * FROM expected WHERE kind=? AND type=? AND a_label=? AND b_label=? '
                                          'AND (a_key > ? OR (a_key = ? AND b_key > ?)) ORDER BY a_key, b_key LIMIT ?',
                                          (kind, type_, a_label, b_label, last[0], last[0], last[1], batch))
            if not rows:
                return
            for row in rows:
                yield _entry(row)
            last = (rows[-1]['a_key'], rows[-1]['b_key'])

    def counts(self) -> dict:
        return {f"{r['type']}:{r['a_label']}:{r['b_label']}": r['n'] for r in self.journal.query(
            'SELECT type, a_label, b_label, count(*) AS n FROM expected GROUP BY type, a_label, b_label')}

    def accounting(self) -> dict:
        """Every logical record in exactly one bucket: applied, superseded, rejected, never enqueued or still uncertain."""
        journal = self.journal
        buckets = {'applied': 0, 'superseded': 0, 'rejected': 0, 'never_enqueued': 0, 'uncertain': 0, 'unsent': 0}
        winners = {row[0] for row in journal.query('SELECT winner FROM expected')}
        for row in journal.query('SELECT r.rid AS rid, r.valid AS valid, group_concat(DISTINCT s.state) AS states FROM records r '
                                 'LEFT JOIN sends s ON s.rid = r.rid GROUP BY r.rid'):
            states = set((row['states'] or '').split(',')) - {''}
            if not row['valid']:
                buckets['rejected'] += 1
            elif 'ack' in states:
                buckets['applied' if row['rid'] in winners else 'superseded'] += 1
            elif states & {'uncertain', 'inflight'}:
                buckets['uncertain'] += 1
            elif states == {'never'}:
                buckets['never_enqueued'] += 1
            else:
                buckets['unsent'] += 1
        return buckets


def _entry(row) -> Entry:
    identity = (row['kind'], row['type'], row['a_label'], row['a_key'], row['b_label'], row['b_key'])
    return Entry(identity, row['mode'], json.loads(row['props']), json.loads(row['permitted']) if row['permitted'] else [],
                 row['klass'], json.loads(row['windows'] or '[]'), json.loads(row['rids'] or '[]'), row['winner'])
