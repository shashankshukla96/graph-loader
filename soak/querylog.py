"""Incremental reading of Neo4j's structured query log across rotation and restarts, for the zero-lock-wait criterion.

The E09 lane reads the whole non-rolling file at its marks. An 8-hour soak logs gigabytes, so the soak runs the same JSON layout
on a size-rotated appender and this reader follows it: each poll reads only new complete lines, per segment (tracked by inode),
in chronological order, in bounded chunks. Coverage is proved, not assumed:

* a segment that disappears (rotated away) before it was read to its last known size is a **coverage failure**;
* every query id must be seen: ids are allocated in order, so a hole that persists across polls is a **coverage failure**;
* a restart of Neo4j starts a new id epoch (``mark_restart``); queries still open at that moment are counted, not waited for;
* the sampler reports a polling gap over 60 s as a coverage failure too (``note_gap``).

A lock wait is a completion record with ``waiting`` > 0. Records are classified by their statement text; the positive control
(``tests/installed_service/lockwait.py``) must be detected in every fleet incarnation, which proves the reader saw the log.
"""
from __future__ import annotations

import hashlib
import re
import xml.etree.ElementTree as ElementTree

from tests.installed_service.query_log_config import TEMPLATE_SHA256
from tests.installed_service.structured_query_log import events

MAX_CHUNK = 4 * 1024 * 1024
HOLE_POLLS = 3          # an id still missing after this many polls is a coverage failure
GAP_LIMIT_S = 60.0
WRITE_WORDS = re.compile(r'\b(SET|MERGE|CREATE|DELETE|REMOVE)\b')


def make_rolling_config(template: bytes, marker: str, *, size='64 MB', keep=50) -> bytes:
    """The reviewed server-logs template with the query log as a synchronous JSON ``RollingFile`` (size-triggered, ``max`` index)."""
    if not isinstance(template, bytes) or not 0 < len(template) <= 256 * 1024 or b'<!DOCTYPE' in template.upper():
        raise ValueError('bounded XML template without a DTD is required')
    if hashlib.sha256(template).hexdigest() != TEMPLATE_SHA256:
        raise ValueError('query-log template differs from its reviewed hash')
    if re.fullmatch(r'(e09|e14)[0-9a-f]{32}', marker) is None:
        raise ValueError('unique marker required')
    root = ElementTree.fromstring(template)
    appenders, loggers = root.find('Appenders'), root.find('Loggers')
    query = [node for node in appenders if node.get('name') == 'QueryLog']
    logger = [node for node in loggers if node.tag == 'Logger' and node.get('name') == 'QueryLogger']
    if (len(query) != 1 or len(logger) != 1 or logger[0].get('additivity') != 'false' or len(logger[0]) != 1
            or logger[0][0].get('ref') != 'QueryLog'):
        raise ValueError('query logger must route only to its appender')
    root.set('monitorInterval', '0')
    index = list(appenders).index(query[0])
    appenders.remove(query[0])
    rolling = ElementTree.Element('RollingFile', {
        'name': 'QueryLog', 'fileName': '${config:server.directories.logs}/query.log',
        'filePattern': '${config:server.directories.logs}/query.log.%i', 'append': 'true', 'immediateFlush': 'true',
        'bufferedIO': 'false', 'ignoreExceptions': 'false'})
    layout = ElementTree.SubElement(rolling, 'JsonLayout', {'compact': 'true', 'eventEol': 'true', 'properties': 'false',
                                                            'includeStacktrace': 'false'})
    ElementTree.SubElement(layout, 'KeyValuePair', {'key': 'e09_marker', 'value': marker})
    policies = ElementTree.SubElement(rolling, 'Policies')
    ElementTree.SubElement(policies, 'SizeBasedTriggeringPolicy', {'size': size})
    ElementTree.SubElement(rolling, 'DefaultRolloverStrategy', {'fileIndex': 'max', 'max': str(keep)})
    appenders.insert(index, rolling)
    return ElementTree.tostring(root, encoding='utf-8', xml_declaration=True)


def classify(query: str) -> str:
    if 'LockProbe' in query:
        return 'control'
    if WRITE_WORDS.search(query):
        return 'loader_write'
    return 'other'


class QueryLogReader:
    """Follows ``/logs/query.log*`` through ``run(cmd) -> (exit_code, bytes)`` (``docker exec`` in the soak, a fake in tests)."""

    def __init__(self, run, marker, *, directory='/logs', clock=None):
        self.run, self.marker, self.directory = run, marker, directory
        self.offsets = {}        # inode -> bytes consumed
        self.sizes = {}          # inode -> last size seen
        self.max_id = 0
        self.holes = {}          # missing id -> polls it has been missing
        self.open = {}           # started, not completed: id -> (query head)
        self.epoch = 1
        self.records = 0
        self.waits = []          # {'id', 'waiting_ms', 'class', 'query', 'epoch'}
        self.controls = []       # waits classified as the positive control
        self.coverage = []       # coverage failure strings
        self.abandoned_open = 0
        self.restarts = 0
        self.bytes_read = 0
        self.polls = 0
        self.last_poll = None
        self.clock = clock

    # ---- shell plumbing -----------------------------------------------------------------------------------------------
    def listing(self):
        code, out = self.run(['sh', '-c', f'for f in {self.directory}/query.log*; do [ -e "$f" ] && stat -c "%i %s %n" "$f"; done; true'])
        if code != 0:
            raise RuntimeError('query-log listing failed')
        rows = []
        for line in out.decode().splitlines():
            inode, size, name = line.split(' ', 2)
            rows.append((int(inode), int(size), name))
        return rows

    def read(self, name, offset, limit):
        code, out = self.run(['sh', '-c', f'tail -c +{offset + 1} "{name}" | head -c {limit}'])
        if code != 0:
            raise RuntimeError('query-log read failed')
        return out

    # ---- one poll ------------------------------------------------------------------------------------------------------
    def poll(self, now=None):
        """Read everything new. Returns the number of records parsed."""
        self.polls += 1
        if now is not None:
            if self.last_poll is not None and now - self.last_poll > GAP_LIMIT_S:
                self.note_gap(now - self.last_poll)
            self.last_poll = now
        rows = self.listing()
        present = {inode for inode, _, _ in rows}
        for inode in list(self.offsets):
            if inode not in present:
                if self.offsets[inode] < self.sizes.get(inode, 0):
                    self.coverage.append(f'segment inode {inode} rotated away unread: {self.offsets[inode]} of {self.sizes[inode]} bytes')
                del self.offsets[inode], self.sizes[inode]
        active = f'{self.directory}/query.log'
        archives = sorted((r for r in rows if r[2] != active), key=lambda r: _archive_number(r[2]))
        ordered = archives + [r for r in rows if r[2] == active]
        parsed = 0
        for inode, size, name in ordered:
            self.sizes[inode] = size
            while self.offsets.get(inode, 0) < size:
                offset = self.offsets.get(inode, 0)
                data = self.read(name, offset, min(MAX_CHUNK, size - offset))
                end = data.rfind(b'\n') + 1
                if end == 0:
                    break  # only a partial line so far
                parsed += self._ingest(data[:end])
                self.offsets[inode] = offset + end
                self.bytes_read += end
        self._age_holes()
        return parsed

    def _ingest(self, chunk):
        try:
            batch = events(chunk, marker=self.marker, maximum=len(chunk) + 1)
        except (RuntimeError, ValueError) as error:
            self.coverage.append(f'unparseable query-log chunk of {len(chunk)} bytes: {error}')
            return 0
        for rec in batch:
            self.records += 1
            query_id, phase = rec['query_id'], rec['phase']
            if phase in ('started', 'rejected'):
                if phase == 'started' and query_id == 1 and self.max_id > 1 and query_id not in self.holes:
                    self._new_epoch()
                if query_id > self.max_id:
                    for missing in range(self.max_id + 1, query_id):
                        self.holes.setdefault(missing, 0)
                    self.max_id = query_id
                else:
                    self.holes.pop(query_id, None)
                if phase == 'started':
                    self.open[query_id] = rec['query'][:120]
            if phase in ('completed', 'failed', 'rejected'):
                self.open.pop(query_id, None)
                if rec['waiting_ms'] > 0 and not rec.get('duplicate'):
                    row = {'id': query_id, 'waiting_ms': rec['waiting_ms'], 'class': classify(rec['query']),
                           'query': rec['query'][:200], 'epoch': self.epoch}
                    self.waits.append(row)
                    if row['class'] == 'control':
                        self.controls.append(row)
        return len(batch)

    def _new_epoch(self):
        self.epoch += 1
        self.abandoned_open += len(self.open)
        self.open.clear()
        self.holes.clear()
        self.max_id = 0

    def mark_restart(self):
        """Neo4j was restarted by the harness: the next id 1 begins a new epoch (queries still open are counted, not waited for)."""
        self.restarts += 1

    def _age_holes(self):
        for missing in list(self.holes):
            self.holes[missing] += 1
            if self.holes[missing] >= HOLE_POLLS:
                self.coverage.append(f'query id {missing} (epoch {self.epoch}) never appeared in the log')
                del self.holes[missing]

    def note_gap(self, seconds):
        self.coverage.append(f'sampling gap of {seconds:.0f} s exceeds {GAP_LIMIT_S:.0f} s')

    # ---- results -------------------------------------------------------------------------------------------------------
    def control_seen(self, query: str) -> bool:
        return any(row['query'] == query[:200] for row in self.controls)

    def loader_waits(self):
        return [row for row in self.waits if row['class'] == 'loader_write']

    def other_waits(self):
        return [row for row in self.waits if row['class'] == 'other']

    def summary(self):
        return {'records': self.records, 'bytes': self.bytes_read, 'epochs': self.epoch, 'polls': self.polls,
                'loader_write_waits': len(self.loader_waits()), 'other_waits': len(self.other_waits()),
                'controls_detected': len(self.controls), 'coverage_failures': list(self.coverage),
                'abandoned_open': self.abandoned_open, 'open_now': len(self.open)}


def _archive_number(name):
    tail = name.rsplit('.', 1)[-1]
    return int(tail) if tail.isdigit() else 0
