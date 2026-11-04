"""Quiet points and reconciliation checkpoints: drain, then compare the graph with the model.

Two comparison strengths:

* a **quiet point** excludes every identity touched by a record with an unreconciled parked copy (``pending``: parking is never
  modelled as "absent") and every identity an uncertain send touches or a null-patch pair that lost its preconditions
  (``inconclusive``: reported, never counted as a product write);
* a **reconciliation checkpoint** for a set of types T replays the parked records of T with the generator paused, drains, requires
  that no parked copy appeared after the boundary captured before the first replay, and only then compares T's identities with no
  pending exclusion. Identities of other types stay pending. The final checkpoint is over every type.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass, field
import hashlib
import json
import time

from . import spec
from .kafka_io import DlqFrontier, resolve_uncertain, source_lagging

INCONCLUSIVE_LIMIT = 0.001  # more than 0.1% inconclusive keys is a harness failure
SAMPLE = 20


# ---- comparison --------------------------------------------------------------------------------------------------------------

@dataclass
class TypeReport:
    name: str
    expected: int = 0
    observed: int = 0
    compared: int = 0
    pending_skipped: int = 0
    inconclusive_skipped: int = 0
    missing: list = field(default_factory=list)
    extra: list = field(default_factory=list)
    different: list = field(default_factory=list)
    duplicates: list = field(default_factory=list)
    b5_differs_from_last: int = 0
    null_patch_compared: int = 0
    digest_expected: str = ''
    digest_observed: str = ''
    counts: dict = field(default_factory=lambda: {'missing': 0, 'extra': 0, 'different': 0, 'duplicates': 0})

    @property
    def ok(self):
        return not any(self.counts.values()) and self.digest_expected == self.digest_observed

    def note(self, bucket, item):
        self.counts[bucket] += 1
        sample = getattr(self, bucket)
        if len(sample) < SAMPLE:
            sample.append(item)

    def to_json(self):
        return {'name': self.name, 'ok': self.ok, 'expected': self.expected, 'observed': self.observed, 'compared': self.compared,
                'pending_skipped': self.pending_skipped, 'inconclusive_skipped': self.inconclusive_skipped,
                'counts': dict(self.counts), 'missing': self.missing, 'extra': self.extra, 'different': self.different,
                'duplicates': self.duplicates, 'b5_differs_from_last': self.b5_differs_from_last,
                'null_patch_compared': self.null_patch_compared,
                'digest_equal': self.digest_expected == self.digest_observed}


def _digest(rows):
    h = hashlib.sha256()
    for key, props in rows:
        h.update(json.dumps([key, props], sort_keys=True, separators=(',', ':')).encode())
    return h.hexdigest()


def compare(name, expected, observed, *, identity, pending=frozenset(), inconclusive=frozenset()) -> TypeReport:
    """Merge-join two sorted streams.

    ``expected`` yields ``Entry`` objects sorted by (a_key, b_key); ``observed`` yields ``(sort key, props)`` where the sort key is
    ``(a_key, b_key)`` (a node's is ``(key, '')``). ``identity(sort_key)`` rebuilds the identity tuple for the exclusion sets.
    """
    report = TypeReport(name)
    exp_rows, obs_rows = [], []
    sentinel = object()
    exp, obs = iter(expected), iter(observed)
    e, o = next(exp, sentinel), next(obs, sentinel)
    previous = None

    def exp_key(entry):
        return entry.identity[3], entry.identity[5]

    while e is not sentinel or o is not sentinel:
        ek = exp_key(e) if e is not sentinel else None
        ok = o[0] if o is not sentinel else None
        if o is not sentinel and ok == previous:
            report.note('duplicates', {'key': list(ok)})
            o = next(obs, sentinel)
            continue
        if e is not sentinel and (o is sentinel or ek < ok):          # expected only
            report.expected += 1
            ident = e.identity
            if ident in pending:
                report.pending_skipped += 1
            elif ident in inconclusive:
                report.inconclusive_skipped += 1
            else:
                report.note('missing', {'key': list(ek), 'expected': e.props})
            e = next(exp, sentinel)
        elif e is sentinel or ok < ek:                                # observed only
            report.observed += 1
            previous = ok
            ident = identity(ok)
            if ident in pending:
                report.pending_skipped += 1
            elif ident in inconclusive:
                report.inconclusive_skipped += 1
            else:
                report.note('extra', {'key': list(ok), 'observed': o[1]})
            o = next(obs, sentinel)
        else:                                                         # both
            report.expected += 1
            report.observed += 1
            previous = ok
            ident = e.identity
            if ident in pending:
                report.pending_skipped += 1
            elif ident in inconclusive:
                report.inconclusive_skipped += 1
            else:
                report.compared += 1
                if e.klass == 'null_patch':
                    report.null_patch_compared += 1
                good = o[1] == e.props or (e.mode == 'permitted' and o[1] in e.permitted)
                if good and e.mode == 'permitted' and o[1] != e.props:
                    report.b5_differs_from_last += 1
                if good:
                    exp_rows.append((list(ek), e.props if e.mode == 'exact' else o[1]))
                    obs_rows.append((list(ok), o[1]))
                else:
                    report.note('different', {'key': list(ek), 'expected': e.props if e.mode == 'exact' else e.permitted, 'observed': o[1]})
            e, o = next(exp, sentinel), next(obs, sentinel)
    report.digest_expected, report.digest_observed = _digest(exp_rows), _digest(obs_rows)
    return report


def node_identity(label):
    return lambda key: ('N', label, label, key[0], '', '')


def rel_identity(rel, a_label, b_label):
    return lambda key: ('R', rel, a_label, key[0], b_label, key[1])


def compare_all(oracle, graph, *, pending=frozenset(), inconclusive=frozenset(), types=None) -> dict:
    """Compare every node label and relationship (type, label pair) — or only ``types`` — against the graph."""
    reports = {}
    for label in spec.NODES:
        if types is not None and label not in types:
            continue
        reports[label] = compare(label, oracle.stream('N', label, label), (((k, ''), p) for k, p in graph.nodes(label)),
                                 identity=node_identity(label), pending=pending, inconclusive=inconclusive)
    for rel, rspec in spec.RELS.items():
        if types is not None and rel not in types:
            continue
        for a_label, b_label in rspec.pairs:
            name = f'{rel}:{a_label}->{b_label}'
            reports[name] = compare(name, oracle.stream('R', rel, a_label, b_label),
                                    (((a, b), p) for a, b, p in graph.relationships(rel, a_label, b_label)),
                                    identity=rel_identity(rel, a_label, b_label), pending=pending, inconclusive=inconclusive)
    return reports


def extra_rels_without_model_nodes(reports, oracle):
    """An extra relationship whose endpoint the model lacks is reported separately (it is also an extra)."""
    notes = []
    for name, report in reports.items():
        if ':' not in name:
            continue
        rel, pair = name.split(':')
        a_label, b_label = pair.split('->')
        for item in report.extra:
            a, b = item['key']
            for label, key in ((a_label, a), (b_label, b)):
                if oracle.entry(('N', label, label, key, '', '')) is None:
                    notes.append({'rel': rel, 'missing_node': [label, key]})
    return notes


# ---- DLQ read-back -----------------------------------------------------------------------------------------------------------

def parse_envelope(value: bytes) -> dict | None:
    """The fields a soak check needs from a DLQ envelope: failure class, attempt, origin and the record's ``rid``."""
    try:
        obj = json.loads(value)
        raw = obj.get('value_bytes')
        rid = None
        if raw:
            inner = json.loads(base64.b64decode(raw))
            rid = (inner.get('properties') or {}).get('rid') if isinstance(inner, dict) else None
        return {'rid': rid, 'failure_class': obj.get('failure_class'), 'reason_code': obj.get('reason_code'),
                'attempt_count': obj.get('attempt_count'), 'original_topic': obj.get('original_topic'),
                'original_partition': obj.get('original_partition'), 'original_offset': obj.get('original_offset')}
    except Exception:
        return None


# ---- the checker -------------------------------------------------------------------------------------------------------------

class QuietPointFailed(RuntimeError):
    def __init__(self, reason, detail):
        super().__init__(f'{reason}: {detail}')
        self.reason, self.detail = reason, detail


class Checker:
    """Everything a quiet point or checkpoint needs, injected so the control flow is testable without Docker.

    ``replay(type) -> dict`` runs ``replay-dlq --parked`` for one type and returns ``{'exit': int, 'output': str}``.
    ``prepare() -> bool`` is called with the generator already paused: it sends every deferred endpoint, flushes the producer and
    returns True when every send has an outcome. ``rejected_locations() -> set`` of ``(topic, partition, offset)`` read from the node
    loaders' rejection files (a rejected node record is identified by where it was read, not by a ``rid``).
    """

    def __init__(self, *, token, group_id, journal, oracle, graph, kafka, prepare, replay=None, rejected_locations=None,
                 clock=time.monotonic, sleep=time.sleep, catchup_bound=900.0, dlq_gap=30.0, poll=2.0, inconclusive_limit=INCONCLUSIVE_LIMIT):
        self.token, self.group_id = token, group_id
        self.journal, self.oracle, self.graph, self.kafka = journal, oracle, graph, kafka
        self.prepare, self.replay, self.rejected_locations = prepare, replay, rejected_locations
        self.clock, self.sleep = clock, sleep
        self.catchup_bound, self.dlq_gap, self.poll = catchup_bound, dlq_gap, poll
        self.inconclusive_limit = inconclusive_limit
        self.sources = spec.source_topics(token)
        self.dlqs = spec.dlq_topics(token)

    # ---- groups -----------------------------------------------------------------------------------------------------
    def groups(self):
        """{group: [(topic, partition)]} for the stable stream groups only (bulk handover groups are run-scoped and excluded)."""
        out = {}
        for topic, (partitions, kind, name) in self.sources.items():
            out.setdefault(f'{self.group_id}-{name}', []).extend((topic, p) for p in range(partitions))
        return out

    def deadline(self):
        return 120.0 + spec.RETRY_WINDOW_S + self.catchup_bound

    # ---- steps ------------------------------------------------------------------------------------------------------
    def snapshot(self, only_groups=None):
        snap = {}
        for group, tps in self.groups().items():
            if only_groups is not None and group not in only_groups:
                continue
            for topic, partition in tps:
                snap[(topic, partition)] = tuple(self.kafka.watermarks(topic, partition))
        return snap

    def drain_sources(self, snapshot, end):
        """Wait until every stream group has committed up to the snapshot on every non-empty partition."""
        lagging = []
        while True:
            lagging = []
            for group, tps in self.groups().items():
                part = {tp: snapshot[tp] for tp in tps if tp in snapshot}
                if part:
                    committed = self.kafka.committed(group, list(part))
                    lagging += [(group, *row) for row in source_lagging(part, committed)]
            if not lagging:
                return []
            if self.clock() > end:
                return lagging
            self.sleep(self.poll)

    def dlq_read(self):
        """One bracketed read of every DLQ partition: (lows, ends_before, commits, ends_after, parked_ends)."""
        tps = [(dlq, p) for dlq, parked, partitions in self.dlqs.values() for p in range(partitions)]
        marks = {tp: self.kafka.watermarks(*tp) for tp in tps}
        lows = {tp: m[0] for tp, m in marks.items()}
        before = {tp: m[1] for tp, m in marks.items()}
        commits = self.kafka.committed(f'{self.group_id}-retry-consumer', tps)
        after = {tp: self.kafka.watermarks(*tp)[1] for tp in tps}
        parked_ends = {(parked, p): self.kafka.watermarks(parked, p)[1] for dlq, parked, partitions in self.dlqs.values()
                       for p in range(partitions)}
        return lows, before, commits, after, parked_ends

    def drain_dlq(self, end):
        frontier = DlqFrontier(self.dlq_gap)
        while True:
            lows, before, commits, after, parked_ends = self.dlq_read()
            if frontier.observe(self.clock(), lows, before, commits, after, parked_ends):
                return []
            if self.clock() > end:
                return frontier.last_bad or [('dlq', 'frontier not stable for two clean reads', None, None)]
            self.sleep(self.poll)

    def sync_dlq(self):
        """Read new DLQ and parked records into the journal (incremental, by cursor)."""
        cursors = self.journal.meta('dlq_cursor', {})
        for rel, (dlq, parked, partitions) in self.dlqs.items():
            for topic, is_parked in ((dlq, False), (parked, True)):
                for p in range(partitions):
                    low, high = self.kafka.watermarks(topic, p)
                    start = max(low, cursors.get(f'{topic}/{p}', 0))
                    for msg in self.kafka.read(topic, p, start, high):
                        info = parse_envelope(msg.value) if msg.value else None
                        if info is None or not info['rid']:
                            continue
                        if is_parked:
                            self.journal.execute('INSERT OR IGNORE INTO parked(type, part, off, rid) VALUES (?,?,?,?)',
                                                 (rel, p, msg.offset, info['rid']))
                        else:
                            self.journal.execute('INSERT OR IGNORE INTO dlq_seen(rid, cls) VALUES (?,?)', (info['rid'], info['failure_class']))
                    cursors[f'{topic}/{p}'] = high
        self.journal.set_meta('dlq_cursor', cursors)

    # ---- exclusion sets ---------------------------------------------------------------------------------------------
    def pending(self, except_types=()):
        rows = self.journal.query('SELECT DISTINCT rid FROM parked WHERE reconciled = 0' +
                                  (' AND type NOT IN (%s)' % ','.join('?' * len(except_types)) if except_types else ''), tuple(except_types))
        return self.journal.identities_of([r['rid'] for r in rows])

    def pending_causes(self):
        return {row['type']: row['n'] for row in self.journal.query(
            'SELECT type, count(DISTINCT rid) AS n FROM parked WHERE reconciled = 0 GROUP BY type')}

    def null_patch_status(self):
        """Decided from the journal, the fault log and the DLQ read-back, before the graph is read. Returns (inconclusive pairs, counts)."""
        dlq_rids = {row['rid'] for row in self.journal.query('SELECT DISTINCT rid FROM dlq_seen')}
        inconclusive, eligible = set(), 0
        for row in self.journal.query("SELECT * FROM expected WHERE klass = 'null_patch'"):
            eligible += 1
            windows = json.loads(row['windows'])
            rids = json.loads(row['rids'])
            ident = (row['kind'], row['type'], row['a_label'], row['a_key'], row['b_label'], row['b_key'])
            if not all(self.journal.window_is_calm(w) for w in windows) or any(r in dlq_rids for r in rids):
                inconclusive.add(ident)
        return inconclusive, {'eligible': eligible, 'inconclusive': len(inconclusive)}

    # ---- the two operations -----------------------------------------------------------------------------------------
    def quiet_point(self, name='quiet'):
        """Pause (the caller), prepare, resolve, drain, compare. Raises ``QuietPointFailed`` for a drain deadline."""
        started = self.clock()
        end = started + self.deadline()
        flushed = self.prepare()
        resolved = resolve_uncertain(self.journal, self.kafka, advance_floor=flushed)
        self.oracle.refresh()
        snapshot = self.snapshot()
        lagging = self.drain_sources(snapshot, end)
        if lagging:
            raise QuietPointFailed('source_not_drained', lagging[:10])
        stuck = self.drain_dlq(end)
        if stuck:
            raise QuietPointFailed('dlq_not_drained', stuck[:10])
        resolved_late = resolve_uncertain(self.journal, self.kafka, advance_floor=flushed)
        self.oracle.refresh()
        self.sync_dlq()
        return self._compare(name, started, snapshot, resolved, resolved_late, types=None, pending_except=())

    def _compare(self, name, started, snapshot, resolved, resolved_late, *, types, pending_except):
        pending = self.pending(pending_except)
        inconclusive_np, np_counts = self.null_patch_status()
        inconclusive = inconclusive_np | self.journal.uncertain_identities()
        reports = compare_all(self.oracle, self.graph, pending=pending, inconclusive=inconclusive, types=types)
        total = sum(r.expected for r in reports.values())
        skipped = len(inconclusive)
        result = {
            'name': name, 'seconds': round(self.clock() - started, 1), 'ok': all(r.ok for r in reports.values()),
            'reports': {k: v.to_json() for k, v in reports.items()},
            'pending': len(pending), 'pending_by_type': self.pending_causes(), 'inconclusive': len(inconclusive),
            'inconclusive_fraction': (skipped / total) if total else 0.0,
            'null_patch': {**np_counts, 'compared': sum(r.null_patch_compared for r in reports.values())},
            'uncertain_resolution': {'first': resolved, 'second': resolved_late},
            'extra_without_model_nodes': extra_rels_without_model_nodes(reports, self.oracle),
            'sends': self.journal.counts(), 'snapshot_partitions': len(snapshot),
        }
        result['harness_failures'] = []
        if result['inconclusive_fraction'] > self.inconclusive_limit:
            result['harness_failures'].append(f"inconclusive keys {skipped}/{total} exceed {self.inconclusive_limit:.1%}")
        result['ok'] = result['ok'] and not result['harness_failures'] and not result['extra_without_model_nodes']
        return result

    def checkpoint(self, types, name='checkpoint'):
        """A reconciliation checkpoint for the relationship types in ``types`` (all types: the final one)."""
        started = self.clock()
        end = started + self.deadline() * 2
        flushed = self.prepare()
        resolve_uncertain(self.journal, self.kafka, advance_floor=flushed)
        self.oracle.refresh()
        snapshot = self.snapshot()
        lagging = self.drain_sources(snapshot, end)
        if lagging:
            raise QuietPointFailed('source_not_drained_before_replay', lagging[:10])
        stuck = self.drain_dlq(end)
        if stuck:
            raise QuietPointFailed('dlq_not_drained_before_replay', stuck[:10])
        self.sync_dlq()
        rel_types = [t for t in types if t in spec.RELS]
        # The boundary is captured before the first replay invocation.
        boundary = {self.dlqs[t][1]: [self.kafka.watermarks(self.dlqs[t][1], p)[1] for p in range(self.dlqs[t][2])] for t in rel_types}
        source_before = {topic: {p: self.kafka.watermarks(topic, p)[1] for p in range(partitions)}
                         for topic, (partitions, kind, nm) in self.sources.items() if nm in rel_types}
        replays = {}
        for rel in rel_types:
            if self.replay is None:
                raise QuietPointFailed('no_replay_runner', rel)
            self.journal.flag_window(self._window(), f'replay {rel}')
            replays[rel] = self.replay(rel)
            if replays[rel].get('exit') != 0:
                raise QuietPointFailed('replay_failed', {rel: replays[rel]})
        journaled = self._journal_replayed(source_before)
        resolve_uncertain(self.journal, self.kafka, advance_floor=True)
        self.oracle.refresh()
        snapshot = self.snapshot()
        lagging = self.drain_sources(snapshot, end)
        if lagging:
            raise QuietPointFailed('source_not_drained_after_replay', lagging[:10])
        stuck = self.drain_dlq(end)
        if stuck:
            raise QuietPointFailed('dlq_not_drained_after_replay', stuck[:10])
        self.sync_dlq()
        after = {parked: [self.kafka.watermarks(parked, p)[1] for p in range(len(ends))] for parked, ends in boundary.items()}
        reparked = {t: [a - b for a, b in zip(after[b_topic], boundary[b_topic])] for t in rel_types
                    for b_topic in [self.dlqs[t][1]] if any(a != b for a, b in zip(after[b_topic], boundary[b_topic]))}
        if reparked:
            result = {'name': name, 'ok': False, 'reason': 'parked_after_boundary', 'reparked': reparked,
                      'boundary': boundary, 'replays': replays, 'journaled_replays': journaled}
            return result
        for rel in rel_types:
            self.journal.execute('UPDATE parked SET reconciled = 1 WHERE type = ?', (rel,))
        # Every label and type is compared; identities of the other types that still have an unreconciled parked copy stay pending.
        result = self._compare(name, started, snapshot, {}, {}, types=None, pending_except=())
        result['replays'] = replays
        result['journaled_replays'] = journaled
        result['boundary'] = boundary
        result['types'] = sorted(types)
        return result

    def _window(self):
        return int(self.journal.meta('current_window', 0))

    def _journal_replayed(self, source_before) -> dict:
        """Journal the copies a replay wrote: read the new range of each source topic and match by ``rid``."""
        counts = {}
        for topic, partitions in source_before.items():
            for p, start in partitions.items():
                low, high = self.kafka.watermarks(topic, p)
                for msg in self.kafka.read(topic, p, max(start, low), high):
                    payload = msg.json() or {}
                    rid = (payload.get('properties') or {}).get('rid')
                    if rid is None or self.journal.record(rid) is None:
                        raise QuietPointFailed('unknown_replayed_record', {'topic': topic, 'partition': p, 'offset': msg.offset})
                    self.journal.add_replayed(rid, topic, p, msg.offset, self._window())
                    counts[topic] = counts.get(topic, 0) + 1
        return counts

    # ---- terminal accounting ------------------------------------------------------------------------------------------
    def accounting(self):
        """The final-checkpoint accounting: buckets, malformed records terminal in the DLQ (relationships) or the rejection file (nodes)."""
        buckets = self.oracle.accounting()
        dlq_malformed = {row['rid'] for row in self.journal.query("SELECT rid FROM dlq_seen WHERE cls = 'malformed'")}
        missing_rel, missing_node = [], []
        for row in self.journal.query('SELECT rid, kind FROM records WHERE valid = 0'):
            if row['kind'] == 'R':
                if row['rid'] not in dlq_malformed:
                    missing_rel.append(row['rid'])
        rejected = self.rejected_locations() if self.rejected_locations else None
        if rejected is not None:
            for row in self.journal.query("SELECT r.rid AS rid, s.topic AS topic, s.part AS part, s.off AS off FROM records r "
                                          "JOIN sends s ON s.rid = r.rid WHERE r.valid = 0 AND r.kind = 'N' AND s.state = 'ack'"):
                if (row['topic'], row['part'], row['off']) not in rejected:
                    missing_node.append(row['rid'])
        parked_unreconciled = self.journal.one('SELECT count(*) AS n FROM parked WHERE reconciled = 0')['n']
        return {'buckets': buckets, 'malformed_relationships_not_in_dlq': missing_rel[:SAMPLE], 'malformed_nodes_not_rejected':
                missing_node[:SAMPLE] if rejected is not None else 'rejection files not read',
                'unaccounted': len(missing_rel) + len(missing_node), 'parked_unreconciled': parked_unreconciled,
                'ok': not missing_rel and not missing_node and not parked_unreconciled}
