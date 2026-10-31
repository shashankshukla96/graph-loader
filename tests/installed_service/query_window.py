"""Fail-closed Neo4j query-log windows for the owned scratch service.

Neo4j numbers every query it logs with one increasing per-server ID. With the
non-rolling structured appender from ``query_log_config`` a window is accepted
only when the same inode kept the earlier bytes as a prefix, both marks are
quiescent (no started-but-uncompleted query) and the IDs between the marks are
contiguous with a start and a completion record each. Any failed, malformed,
missing, truncated or rotated record raises. ID contiguity is an observed
property of Neo4j 5.26.32, not a proof that the server emitted every event.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
import time

from .structured_query_log import completed_for, events

# The scratch server's log accumulates over the whole service-harness session (its tests share one server).
MAX_LOG = 32 * 1024 * 1024


@dataclass(frozen=True)
class Capture:
    inode: int
    size: int
    content: bytes
    records: tuple
    max_id: int
    epochs: int = 1
    open_ids: tuple = ()


def read_log(container, maximum=MAX_LOG):
    script = ("set -eu; f=/logs/query.log; i=$(stat -c %i \"$f\"); n=$(stat -c %s \"$f\"); "
              f"[ \"$n\" -le {maximum} ]; printf '%s %s\\n' \"$i\" \"$n\"; head -c \"$n\" \"$f\"; "
              "j=$(stat -c %i \"$f\"); printf '\\n%s\\n' \"$j\"")
    result = container.exec_run(['sh', '-c', script])
    if result.exit_code != 0 or not isinstance(result.output, bytes):
        raise RuntimeError('owned Neo4j query log is unavailable or exceeds its bound')
    header, separator, rest = result.output.partition(b'\n')
    if not separator or re.fullmatch(rb'[0-9]+ [0-9]+', header) is None:
        raise RuntimeError('invalid query-log header')
    inode, size = (int(part) for part in header.split())
    content, trailer = rest[:size], rest[size:]
    if inode <= 0 or len(content) != size or trailer != b'\n' + str(inode).encode() + b'\n':
        raise RuntimeError('query log rotated or changed during capture')
    return inode, content


EPOCH_START = re.compile(rb'"message":\s*"Query started: id:1 -')


def parse(inode, content, marker, *, allow_open=False):
    """Parse a capture; a server restart starts a new query-ID epoch in the same appended file.

    ``allow_open`` accepts queries that were started and never ended in the newest epoch (a client killed mid-query
    can leave one); they are reported in ``open_ids`` and the caller must account for them.
    """
    chunks = [[]]
    for line in content.splitlines(keepends=True):
        if EPOCH_START.search(line) and chunks[-1]:
            chunks.append([])
        chunks[-1].append(line)
    epochs = [list(events(b''.join(chunk), marker=marker, maximum=MAX_LOG)) for chunk in chunks]
    for epoch in epochs[:-1]:
        _check_epoch(epoch, quiescent=False)
    max_id = _check_epoch(epochs[-1], quiescent=not allow_open)
    records = tuple(row for epoch in epochs for row in epoch)
    ended = {r['query_id'] for r in epochs[-1] if r['phase'] in ('completed', 'failed')}
    open_ids = tuple(sorted({r['query_id'] for r in epochs[-1] if r['phase'] == 'started'} - ended))
    return Capture(inode, len(content), content, records, max_id, len(epochs), open_ids)


def _check_epoch(epoch, *, quiescent):
    started = {r['query_id'] for r in epoch if r['phase'] == 'started'}
    completed = {r['query_id'] for r in epoch if r['phase'] in ('completed', 'failed')}
    if completed - started or (quiescent and started - completed):
        raise RuntimeError('query log is not quiescent: started and completed IDs differ')
    ids = sorted(started)
    if ids != list(range(1, len(ids) + 1)):
        raise RuntimeError('query IDs are not contiguous from the start of the log')
    return ids[-1] if ids else 0


def capture(container, marker, *, timeout=30, allow_open=False):
    """Wait for two identical quiescent reads, then return the parsed capture."""
    end = time.monotonic() + timeout
    previous = None
    while True:
        inode, content = read_log(container)
        try:
            current = parse(inode, content, marker, allow_open=allow_open)
        except RuntimeError as error:
            if 'not quiescent' not in str(error):
                raise
            current = None
        if current is not None and previous is not None and (current.inode, current.content) == (previous.inode, previous.content):
            return current
        previous = current
        if time.monotonic() > end:
            raise TimeoutError('query log did not become quiescent')
        time.sleep(0.5)


def window(before, after):
    """Return the records between two captures, or raise if coverage is not continuous."""
    if not isinstance(before, Capture) or not isinstance(after, Capture):
        raise ValueError('two query-log captures are required')
    if before.inode != after.inode:
        raise RuntimeError('query log rotated between marks')
    if before.epochs != after.epochs:
        raise RuntimeError('Neo4j restarted between marks; query IDs are not comparable')
    if len(after.content) < len(before.content) or not after.content.startswith(before.content):
        raise RuntimeError('query log was truncated or rewritten between marks')
    delta = after.records[len(before.records):]
    if after.records[:len(before.records)] != before.records:
        raise RuntimeError('query-log record prefix differs')
    expected = list(range(before.max_id + 1, after.max_id + 1))
    carried = set(before.open_ids)
    # A query left open at the earlier mark may end inside the window (one terminal record, no second start).
    late = [r for r in delta if r['query_id'] in carried]
    if any(r['phase'] == 'started' for r in late) or len({r['query_id'] for r in late}) != len(late):
        raise RuntimeError('query-log window has missing or unrelated records')
    if not set(after.open_ids) >= carried - {r['query_id'] for r in late}:
        raise RuntimeError('query-log window has missing or unrelated records')
    fresh = [r for r in delta if r['query_id'] not in carried and not r.get('duplicate')]
    still_open = set(after.open_ids) - carried
    if (sorted({r['query_id'] for r in delta if r['query_id'] not in carried}) != expected
            or len(fresh) != 2 * len(expected) - len(still_open)):
        raise RuntimeError('query-log window has missing or unrelated records')
    return list(delta)


def waits(records, *, query, database, user, expected):
    """Lock-wait milliseconds of exactly ``expected`` identified completions."""
    return [row['waiting_ms'] for row in completed_for(records, query=query, database=database, user=user, expected=expected)]
