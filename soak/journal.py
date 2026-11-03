"""On-disk journal of everything the generator sent, and nothing the product wrote.

Two kinds of row:

* a **logical record** (``records``): ``rid`` is the generator's unique record id, a declared property of every type. An exact
  duplicate re-sends the same ``rid`` with the identical payload.
* a **physical send** (``sends``): one enqueue, unique ``send_id`` (also the Kafka header ``e13-send``). Its state is
  ``inflight`` (the produce call returned, no outcome yet), ``never`` (the produce call raised: not in Kafka),
  ``ack`` (topic, partition, offset known) or ``uncertain`` (a delivery error or timeout: it may or may not be in Kafka).

A replay (``replay-dlq``) republishes a stored payload without headers; its physical sends are journalled with
``origin='replay'`` once found again by ``rid`` in the range written during the replay.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
import sqlite3
import threading
import time

Identity = tuple  # (kind, type, a_label, a_key, b_label, b_key); a node is ('N', label, label, key, '', '')

SCHEMA = '''
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS records (
  rid TEXT PRIMARY KEY, kind TEXT NOT NULL, type TEXT NOT NULL, a_label TEXT NOT NULL, a_key TEXT NOT NULL,
  b_label TEXT NOT NULL, b_key TEXT NOT NULL, topic TEXT NOT NULL, msg_key TEXT NOT NULL, payload TEXT NOT NULL,
  valid INTEGER NOT NULL, window INTEGER NOT NULL, klass TEXT, cohort TEXT, born REAL NOT NULL);
CREATE INDEX IF NOT EXISTS records_identity ON records(kind, type, a_label, a_key, b_label, b_key);
CREATE TABLE IF NOT EXISTS sends (
  send_id INTEGER PRIMARY KEY AUTOINCREMENT, rid TEXT NOT NULL, origin TEXT NOT NULL, state TEXT NOT NULL,
  topic TEXT, part INTEGER, off INTEGER, window INTEGER NOT NULL, at REAL NOT NULL, ack_seq INTEGER);
CREATE INDEX IF NOT EXISTS sends_rid ON sends(rid);
CREATE INDEX IF NOT EXISTS sends_state ON sends(state);
CREATE INDEX IF NOT EXISTS sends_ack ON sends(ack_seq);
CREATE UNIQUE INDEX IF NOT EXISTS sends_location ON sends(topic, part, off) WHERE state = 'ack' AND origin != 'replay';
CREATE TABLE IF NOT EXISTS windows (id INTEGER PRIMARY KEY, calm INTEGER NOT NULL, reason TEXT NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS pairs (
  type TEXT NOT NULL, a_key TEXT NOT NULL, b_key TEXT NOT NULL, b_label TEXT NOT NULL, last_order TEXT, klass TEXT,
  n INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (type, a_key, b_label, b_key));
CREATE TABLE IF NOT EXISTS dlq_seen (rid TEXT NOT NULL, cls TEXT NOT NULL, PRIMARY KEY (rid, cls));
CREATE TABLE IF NOT EXISTS parked (
  type TEXT NOT NULL, part INTEGER NOT NULL, off INTEGER NOT NULL, rid TEXT NOT NULL, reconciled INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (type, part, off));
CREATE TABLE IF NOT EXISTS expected (
  kind TEXT NOT NULL, type TEXT NOT NULL, a_label TEXT NOT NULL, a_key TEXT NOT NULL, b_label TEXT NOT NULL, b_key TEXT NOT NULL,
  mode TEXT NOT NULL, props TEXT NOT NULL, permitted TEXT, klass TEXT, windows TEXT, rids TEXT, winner TEXT,
  PRIMARY KEY (kind, type, a_label, a_key, b_label, b_key));
CREATE INDEX IF NOT EXISTS expected_pair ON expected(kind, type, a_label, b_label, a_key, b_key);
'''


class JournalError(RuntimeError):
    """The journal contradicts itself (for example a reused ``rid`` with a different payload)."""


@dataclass
class Record:
    rid: str
    kind: str          # 'N' node, 'R' relationship
    type: str          # label or relationship type
    a_label: str
    a_key: str
    b_label: str
    b_key: str
    topic: str
    msg_key: str
    payload: dict
    valid: bool = True
    window: int = 0
    klass: str | None = None
    cohort: str | None = None

    @property
    def identity(self) -> Identity:
        return (self.kind, self.type, self.a_label, self.a_key, self.b_label, self.b_key)

    @property
    def props(self) -> dict:
        return self.payload if self.kind == 'N' else self.payload.get('properties', {})


def node_record(rid, label, key, topic, payload, **kw) -> Record:
    return Record(rid, 'N', label, label, key, '', '', topic, key, payload, **kw)


def rel_record(rid, rel, source_label, source_key, target_label, target_key, topic, payload, **kw) -> Record:
    return Record(rid, 'R', rel, source_label, source_key, target_label, target_key, topic, source_key, payload, **kw)


class Journal:
    def __init__(self, path):
        self.path = str(path)
        self._lock = threading.RLock()
        self.db = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        for pragma in ('journal_mode=WAL', 'synchronous=NORMAL', 'temp_store=FILE', 'cache_size=-20000'):
            self.db.execute('PRAGMA ' + pragma)
        self.db.executescript(SCHEMA)
        self.db.execute("INSERT OR IGNORE INTO meta(k, v) VALUES ('ack_seq', '0')")
        self._tx = 0

    # ---- plumbing ---------------------------------------------------------------------------------------------------
    def execute(self, sql, params=()):
        with self._lock:
            return self.db.execute(sql, params)

    def query(self, sql, params=()):
        with self._lock:
            return self.db.execute(sql, params).fetchall()

    def one(self, sql, params=()):
        with self._lock:
            return self.db.execute(sql, params).fetchone()

    def transaction(self):
        return _Tx(self)

    def close(self):
        with self._lock:
            self.db.close()

    def meta(self, key, default=None):
        row = self.one('SELECT v FROM meta WHERE k = ?', (key,))
        return default if row is None else json.loads(row['v'])

    def set_meta(self, key, value):
        self.execute('INSERT INTO meta(k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v', (key, json.dumps(value)))

    # ---- logical records --------------------------------------------------------------------------------------------
    def add_record(self, rec: Record):
        payload = json.dumps(rec.payload, sort_keys=True, separators=(',', ':'))
        with self._lock:
            row = self.db.execute('SELECT topic, payload, valid FROM records WHERE rid = ?', (rec.rid,)).fetchone()
            if row is not None:
                if row['payload'] != payload or row['topic'] != rec.topic or bool(row['valid']) != rec.valid:
                    raise JournalError(f'rid {rec.rid} is reused with a different payload or topic: only an exact duplicate may reuse a rid')
                return False
            self.db.execute('INSERT INTO records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                            (rec.rid, rec.kind, rec.type, rec.a_label, rec.a_key, rec.b_label, rec.b_key, rec.topic, rec.msg_key,
                             payload, int(rec.valid), rec.window, rec.klass, rec.cohort, time.time()))
            return True

    def record(self, rid) -> Record | None:
        row = self.one('SELECT * FROM records WHERE rid = ?', (rid,))
        return None if row is None else _record(row)

    def identities_of(self, rids) -> set:
        found = set()
        rids = list(rids)
        for i in range(0, len(rids), 500):
            chunk = rids[i:i + 500]
            marks = ','.join('?' * len(chunk))
            for row in self.query(f'SELECT kind, type, a_label, a_key, b_label, b_key FROM records WHERE rid IN ({marks})', chunk):
                found.add(tuple(row))
        return found

    # ---- physical sends ---------------------------------------------------------------------------------------------
    def new_send(self, rid, *, origin='gen', window=0) -> int:
        with self._lock:
            cur = self.db.execute("INSERT INTO sends(rid, origin, state, window, at) VALUES (?, ?, 'inflight', ?, ?)",
                                  (rid, origin, window, time.time()))
            return cur.lastrowid

    def _next_ack_seq(self):
        seq = int(self.meta('ack_seq')) + 1
        self.set_meta('ack_seq', seq)
        return seq

    def mark_ack(self, send_id, topic, partition, offset):
        with self._lock:
            row = self.db.execute('SELECT state FROM sends WHERE send_id = ?', (send_id,)).fetchone()
            if row is None:
                raise JournalError(f'unknown send {send_id}')
            if row['state'] == 'ack':
                return
            self.db.execute("UPDATE sends SET state='ack', topic=?, part=?, off=?, ack_seq=? WHERE send_id=?",
                            (topic, partition, offset, self._next_ack_seq(), send_id))

    def mark_uncertain(self, send_id):
        self.execute("UPDATE sends SET state='uncertain' WHERE send_id = ? AND state = 'inflight'", (send_id,))

    def mark_never(self, send_id):
        self.execute("UPDATE sends SET state='never' WHERE send_id = ? AND state = 'inflight'", (send_id,))

    def settle_inflight(self):
        """Sends still without an outcome (the producer flush timed out, or the harness restarted) become uncertain."""
        return self.execute("UPDATE sends SET state='uncertain' WHERE state='inflight'").rowcount

    def add_replayed(self, rid, topic, partition, offset, window=0) -> int:
        """Journal one physical copy written by ``replay-dlq``, found by ``rid`` in the range read back."""
        with self._lock:
            dup = self.db.execute("SELECT send_id FROM sends WHERE origin='replay' AND topic=? AND part=? AND off=?",
                                  (topic, partition, offset)).fetchone()
            if dup is not None:
                return dup['send_id']
            cur = self.db.execute("INSERT INTO sends(rid, origin, state, topic, part, off, window, at, ack_seq) "
                                  "VALUES (?, 'replay', 'ack', ?, ?, ?, ?, ?, ?)",
                                  (rid, topic, partition, offset, window, time.time(), self._next_ack_seq()))
            return cur.lastrowid

    def uncertain(self):
        return self.query("SELECT s.send_id AS send_id, s.rid AS rid, r.topic AS topic FROM sends s JOIN records r ON r.rid = s.rid "
                          "WHERE s.state = 'uncertain' ORDER BY s.send_id")

    def uncertain_identities(self) -> set:
        rows = self.query("SELECT DISTINCT r.kind, r.type, r.a_label, r.a_key, r.b_label, r.b_key FROM sends s JOIN records r "
                          "ON r.rid = s.rid WHERE s.state IN ('uncertain', 'inflight')")
        return {tuple(row) for row in rows}

    def counts(self) -> dict:
        return {row['state']: row['n'] for row in self.query('SELECT state, count(*) AS n FROM sends GROUP BY state')}

    # ---- windows and the fault log ----------------------------------------------------------------------------------
    def open_window(self, window, calm=True, reason=''):
        self.execute('INSERT OR IGNORE INTO windows(id, calm, reason) VALUES (?, ?, ?)', (window, int(calm), reason))

    def flag_window(self, window, reason):
        """A fault, restart or replay happened in this window: it is no longer calm."""
        self.open_window(window)
        old = self.one('SELECT reason FROM windows WHERE id = ?', (window,))['reason']
        self.execute('UPDATE windows SET calm = 0, reason = ? WHERE id = ?', ((old + '; ' if old else '') + reason, window))

    def window_is_calm(self, window) -> bool:
        row = self.one('SELECT calm FROM windows WHERE id = ?', (window,))
        return True if row is None else bool(row['calm'])


class _Tx:
    def __init__(self, journal):
        self.journal = journal

    def __enter__(self):
        self.journal._lock.acquire()
        if not self.journal._tx:
            self.journal.db.execute('BEGIN')
        self.journal._tx += 1
        return self.journal

    def __exit__(self, kind, value, trace):
        try:
            self.journal._tx -= 1
            if not self.journal._tx:
                self.journal.db.execute('ROLLBACK' if kind else 'COMMIT')
        finally:
            self.journal._lock.release()
        return False


def _record(row) -> Record:
    return Record(row['rid'], row['kind'], row['type'], row['a_label'], row['a_key'], row['b_label'], row['b_key'], row['topic'],
                  row['msg_key'], json.loads(row['payload']), bool(row['valid']), row['window'], row['klass'], row['cohort'])


def ensure_dir(path) -> Path:
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path
