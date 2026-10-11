"""External-memory building blocks of the edge planner (bounded memory, local spill files).

One planner worker keeps three kinds of data; each is bounded by a share of
the worker's memory budget and spills to an append-only file in the planner's
spill directory beyond it:

* :class:`KeyWindows` -- the worker's endpoint keys, interned per label into
  *windows* of at most ``cap`` keys.  A window stores each distinct key once
  (its canonical token, ``crc32(token)`` and bucket) with its exact degree
  inside the window; rows refer to their endpoints by window-local id.  A full
  window is *sealed*: its keys are written to the spill file and its degrees
  are added to a fixed-size counter indexed by ``crc32(token) & mask`` (an
  upper bound of every key's degree that never under-counts).
* :class:`GroupStore` -- the rows themselves, grouped by
  ``(source bucket, target bucket)`` cell in arrival order, as compact columns
  (record JSON with a trailing comma, record byte length, the two window-local
  endpoint ids, the Kafka partition for multi-partition tasks).  Buffers of
  every group are written out together when the buffered bytes exceed the
  flush threshold; each group then reads back as its segments in order.
* :func:`unit_chunks` -- cuts one unit's rows, streamed in order from any
  number of pieces, into exactly the chunks :func:`~src.loader.edge_plan.chunk_boundaries`
  returns (near-equal row counts, greedy byte cap).

Hub detection is exact: the summed counters of every worker bound each key's
total degree from above, so only keys in counter slots of at least ``H`` can
be hubs; their windows' entries are then summed exactly by token.  A counter
collision only costs extra exact work, never a wrong hub.
"""
from __future__ import annotations

from array import array
import errno
import os
from pathlib import Path
from typing import Callable, Iterable, Iterator, Sequence

import numpy as np

from src.loader.edge_plan import EdgePlanError, split_sizes


MIN_COUNTER_BITS, MAX_COUNTER_BITS = 12, 22
# Estimated bytes of one interned key while its window is open (dict slot,
# key and token objects, list and array entries).
WINDOW_ENTRY_BYTES = 200
_READ_LIMIT = 1 << 30


def counter_bits_for(budget_bytes: int) -> int:
    """Counter slots: 4-byte counters using at most 1/16 of the budget."""
    slots = max(1, budget_bytes // 64)
    return max(MIN_COUNTER_BITS, min(MAX_COUNTER_BITS, slots.bit_length() - 1))


class SpillFile:
    """An append-only local file of byte segments (created on first write)."""

    def __init__(self, path: str | os.PathLike) -> None:
        self.path = Path(path)
        self._fd: int | None = None
        self.size = 0
        self.segments = 0

    def append(self, parts: Iterable) -> int:
        """Write ``parts`` (bytes-like objects) contiguously; return their offset."""
        if self._fd is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._fd = os.open(self.path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o600)
        offset = self.size
        try:
            for part in parts:
                view = memoryview(part).cast("B")
                while len(view):
                    written = os.pwrite(self._fd, view, self.size)
                    self.size += written
                    view = view[written:]
        except OSError as exc:
            if exc.errno in (errno.ENOSPC, errno.EDQUOT):
                raise EdgePlanError(
                    f"planner spill directory {self.path.parent} is full ({exc.strerror}); "
                    "free disk space or point --plan-spill-dir at a larger disk"
                ) from exc
            raise
        self.segments += 1
        return offset

    def read(self, offset: int, length: int) -> bytes:
        if length == 0:
            return b""
        if self._fd is None:
            raise EdgePlanError(f"spill file {self.path} was never written")
        parts = []
        while length > 0:
            data = os.pread(self._fd, min(length, _READ_LIMIT), offset)
            if not data:
                raise EdgePlanError(f"spill file {self.path} is truncated")
            parts.append(data)
            offset += len(data)
            length -= len(data)
        return parts[0] if len(parts) == 1 else b"".join(parts)

    def close(self) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None

    def remove(self) -> None:
        self.close()
        self.path.unlink(missing_ok=True)


# -- endpoint keys ---------------------------------------------------------------------


class LabelWindow:
    """The open window of one label: key -> local id, with token, crc, bucket and degree."""

    __slots__ = ("ids", "tokens", "crcs", "buckets", "counts")

    def __init__(self) -> None:
        self.ids: dict[object, int] = {}
        self.tokens: list[bytes] = []
        self.crcs = array("I")
        self.buckets = array("I")
        self.counts = array("I")

    def clear(self) -> None:
        self.ids.clear()
        self.tokens.clear()
        del self.crcs[:], self.buckets[:], self.counts[:]


class KeyWindows:
    """Windowed per-label key interning of one worker (see the module docstring).

    ``windows[w][label]`` is ``(crcs, counts, lengths, tokens blob)`` kept in
    memory for the last window and ``("file", offset, count, blob bytes)`` for
    spilled windows.
    """

    def __init__(self, *, cap: int, counter_bits: int, spill: SpillFile) -> None:
        self.cap = max(1, cap)
        self.labels: dict[str, LabelWindow] = {}
        self.window = 0
        self.entries = 0
        self.windows: list[dict[str, tuple]] = []
        self.mask = (1 << counter_bits) - 1
        self.counter = np.zeros(1 << counter_bits, dtype=np.uint32)
        self.spill = spill
        self.total_entries = 0
        # Called with the window id before a window is sealed (rows mark their window).
        self.on_seal: Callable[[int], None] | None = None
        self._kept: dict[str, list[tuple[int, np.ndarray, list[bytes]]]] = {}

    def label(self, name: str) -> LabelWindow:
        keys = self.labels.get(name)
        if keys is None:
            keys = self.labels[name] = LabelWindow()
        return keys

    def seal(self, *, final: bool = False) -> None:
        """Close the open window (spilled unless ``final``); counters absorb its degrees."""
        if self.on_seal is not None:
            self.on_seal(self.window)
        sealed: dict[str, tuple] = {}
        for name, keys in self.labels.items():
            count = len(keys.tokens)
            if not count:
                continue
            crcs = np.array(keys.crcs, dtype=np.uint32)
            counts = np.array(keys.counts, dtype=np.uint32)
            np.add.at(self.counter, crcs & np.uint32(self.mask), counts)
            lengths = np.fromiter(map(len, keys.tokens), dtype=np.uint32, count=count)
            blob = b"".join(keys.tokens)
            if final:
                sealed[name] = (crcs, counts, lengths, blob)
            else:
                offset = self.spill.append((crcs, counts, lengths, blob))
                sealed[name] = ("file", offset, count, len(blob))
            keys.clear()
        self.windows.append(sealed)
        self.total_entries += self.entries
        self.entries = 0
        self.window += 1

    def window_arrays(self, window: int, label: str) -> tuple | None:
        item = self.windows[window].get(label)
        if item is None or not isinstance(item[0], str):
            return item
        _, offset, count, blob_bytes = item
        data = self.spill.read(offset, 12 * count + blob_bytes)
        columns = np.frombuffer(data, dtype=np.uint32, count=3 * count)
        return columns[:count], columns[count:2 * count], columns[2 * count:], memoryview(data)[12 * count:]

    def candidate_tokens(self, candidates: np.ndarray, progress: Callable[[], None] | None = None,
                         ) -> dict[str, dict[bytes, int]]:
        """Exact local degrees of every key whose counter slot is a candidate.

        The matching entries are kept (window, local id, token) so hub maps
        can be built later without reading the windows again.
        """
        result: dict[str, dict[bytes, int]] = {}
        self._kept = {}
        for window in range(len(self.windows)):
            for label in self.windows[window]:
                crcs, counts, lengths, blob = self.window_arrays(window, label)
                hits = np.flatnonzero(candidates[crcs & np.uint32(self.mask)])
                if hits.size:
                    ends = np.cumsum(lengths, dtype=np.int64)
                    starts = ends - lengths
                    tokens = [bytes(blob[start:end]) for start, end in zip(starts[hits].tolist(), ends[hits].tolist())]
                    totals = result.setdefault(label, {})
                    for token, count in zip(tokens, counts[hits].tolist()):
                        totals[token] = totals.get(token, 0) + count
                    self._kept.setdefault(label, []).append((window, hits.astype(np.uint32), tokens))
            if progress is not None:
                progress()
        return result

    def hub_maps(self, hubs: dict[str, dict[bytes, int]]) -> dict[tuple[str, int], tuple[np.ndarray, np.ndarray]]:
        """``(label, window) -> (sorted local ids, hub resources)`` of every hub occurrence."""
        maps = {}
        for label, kept in self._kept.items():
            resources = hubs.get(label)
            if not resources:
                continue
            for window, local_ids, tokens in kept:
                found = [(int(local), resources[token]) for local, token in zip(local_ids.tolist(), tokens)
                         if token in resources]
                if found:
                    found.sort()
                    maps[(label, window)] = (np.array([local for local, _ in found], dtype=np.uint32),
                                             np.array([resource for _, resource in found], dtype=np.int64))
        self._kept = {}
        return maps


# -- rows ----------------------------------------------------------------------------------


class Group:
    """One cell's rows of one task: in-memory buffers plus spilled segments.

    Buffered records are kept as the (immutable) bytes objects the reader
    produced -- appending is one list append per row; they are joined only
    when the group is flushed or loaded.
    """

    __slots__ = ("recs", "lids", "parts", "spilled_rows", "marked", "marks", "segments", "rec_bytes")

    def __init__(self, with_lids: bool, with_parts: bool) -> None:
        self.recs: list[bytes] = []
        self.lids = array("I") if with_lids else None
        self.parts = array("I") if with_parts else None
        self.spilled_rows = 0
        self.marked = 0            # rows already assigned to a key window
        self.marks = array("Q")    # (first row, window) pairs
        self.segments = array("Q")  # (offset, rows, record bytes) triples
        self.rec_bytes = 0         # comma-terminated record bytes over all segments

    @property
    def rows(self) -> int:
        return self.spilled_rows + len(self.recs)

    def mark(self, window: int) -> None:
        """The rows appended since the last mark belong to key window ``window``."""
        rows = self.spilled_rows + len(self.recs)
        if rows != self.marked:
            self.marks.extend((self.marked, window))
            self.marked = rows

    def release(self) -> None:
        self.recs = []
        if self.lids is not None:
            self.lids = array("I")
        if self.parts is not None:
            self.parts = array("I")


# Memory of one Group object that stays resident after its rows are spilled: the object, its
# list and its (empty) arrays, measured at about 544 bytes.  It is charged against the budget.
GROUP_OVERHEAD_BYTES = 544


class GroupStore:
    """Every group of one worker; buffers flush together to one spill file."""

    def __init__(self, spill: SpillFile, flush_bytes: int) -> None:
        self.spill = spill
        self.flush_bytes = max(1, flush_bytes)
        self.buffered = 0
        self.group_bytes = 0       # resident overhead of all groups (never spilled)
        self.groups: list[Group] = []
        self.spilled_bytes = 0
        self.flushes = 0
        self.peak_buffered = 0

    def new_group(self, with_lids: bool, with_parts: bool) -> Group:
        group = Group(with_lids, with_parts)
        self.groups.append(group)
        self.group_bytes += GROUP_OVERHEAD_BYTES
        return group

    @property
    def limit(self) -> int:
        """Buffered row bytes that trigger a flush.

        Group overhead up to a quarter of the row budget is free (single-topic loads, even at 64
        buckets and ten types, stay within it and flush exactly as before); the excess is taken out of
        the row budget, never below a quarter of it, so many topics cannot push memory past the budget.
        """
        allowance = self.flush_bytes // 4
        return max(self.flush_bytes - max(0, self.group_bytes - allowance), self.flush_bytes // 4)

    def flush(self) -> None:
        self.peak_buffered = max(self.peak_buffered, self.buffered)
        for group in self.groups:
            rows = len(group.recs)
            if not rows:
                continue
            blen = np.fromiter(map(len, group.recs), dtype=np.uint32, count=rows)
            blob = b",".join(group.recs)
            parts = [blen]
            if group.lids is not None:
                parts.append(group.lids)
            if group.parts is not None:
                parts.append(group.parts)
            parts.extend((blob, b","))
            offset = self.spill.append(parts)
            group.segments.extend((offset, rows, len(blob) + 1))
            group.rec_bytes += len(blob) + 1
            group.spilled_rows += rows
            self.spilled_bytes += len(blob) + 1 + 4 * rows * (
                1 + (2 if group.lids is not None else 0) + (1 if group.parts is not None else 0))
            group.release()
        self.buffered = 0
        self.flushes += 1

    @staticmethod
    def total_bytes(group: Group) -> int:
        """Comma-terminated record bytes of the whole group."""
        return group.rec_bytes + sum(map(len, group.recs)) + len(group.recs)

    def segments(self, group: Group) -> Iterator[tuple[int, np.ndarray, np.ndarray | None, np.ndarray | None, bytes]]:
        """Yield ``(first row, blen, lids (n, 2), parts, records)`` per segment in row order."""
        row = 0
        segments = group.segments
        for index in range(0, len(segments), 3):
            offset, rows, rec_bytes = segments[index], segments[index + 1], segments[index + 2]
            words = rows * (1 + (2 if group.lids is not None else 0) + (1 if group.parts is not None else 0))
            data = self.spill.read(offset, 4 * words + rec_bytes)
            columns = np.frombuffer(data, dtype=np.uint32, count=words)
            blen, rest = columns[:rows], columns[rows:]
            lids = parts = None
            if group.lids is not None:
                lids, rest = rest[:2 * rows].reshape(rows, 2), rest[2 * rows:]
            if group.parts is not None:
                parts = rest[:rows]
            yield row, blen, lids, parts, memoryview(data)[4 * words:]
            row += rows
        rows = len(group.recs)
        if rows:
            blen = np.fromiter(map(len, group.recs), dtype=np.uint32, count=rows)
            lids = None if group.lids is None else np.array(group.lids, dtype=np.uint32).reshape(rows, 2)
            parts = None if group.parts is None else np.array(group.parts, dtype=np.uint32)
            yield row, blen, lids, parts, b",".join(group.recs) + b","

    def load(self, group: Group) -> tuple[np.ndarray, np.ndarray | None, np.ndarray | None, list | bytearray]:
        """Read one whole group into memory: ``(blen, lids, parts, records)``.

        ``records`` is the list of buffered record objects for a group that
        was never spilled, else one buffer of comma-terminated records.
        """
        if not group.segments:
            rows = len(group.recs)
            blen = np.fromiter(map(len, group.recs), dtype=np.uint32, count=rows)
            lids = None if group.lids is None else np.array(group.lids, dtype=np.uint32).reshape(rows, 2)
            parts = None if group.parts is None else np.array(group.parts, dtype=np.uint32)
            return blen, lids, parts, group.recs
        total_rows = group.rows
        blen = np.empty(total_rows, dtype=np.uint32)
        lids = None if group.lids is None else np.empty((total_rows, 2), dtype=np.uint32)
        parts = None if group.parts is None else np.empty(total_rows, dtype=np.uint32)
        rec = bytearray(self.total_bytes(group))
        position = 0
        for first, piece_blen, piece_lids, piece_parts, piece_rec in self.segments(group):
            end = first + len(piece_blen)
            blen[first:end] = piece_blen
            if lids is not None:
                lids[first:end] = piece_lids
            if parts is not None:
                parts[first:end] = piece_parts
            rec[position:position + len(piece_rec)] = piece_rec
            position += len(piece_rec)
        return blen, lids, parts, rec

    def remove(self) -> None:
        self.groups = []
        self.spill.remove()




def window_slices(marks: array, first: int, end: int) -> list[tuple[int, int, int]]:
    """``(start, stop, window)`` row ranges of ``[first, end)`` relative to ``first``."""
    pairs = [(marks[index], marks[index + 1]) for index in range(0, len(marks), 2)]
    slices = []
    for index, (start, window) in enumerate(pairs):
        stop = pairs[index + 1][0] if index + 1 < len(pairs) else end
        start, stop = max(start, first), min(stop, end)
        if start < stop:
            slices.append((start - first, stop - first, window))
    return slices


def route(lids: np.ndarray | None, slices: Sequence[tuple[int, int, int]], label: str, default: int,
          maps: dict, rows: int) -> np.ndarray:
    """Each row's resource on one side: its hub resource, else ``default``."""
    resources = np.full(rows, default, dtype=np.int64)
    if lids is None or not maps:
        return resources
    for start, stop, window in slices:
        found = maps.get((label, window))
        if found is None:
            continue
        hub_ids, hub_resources = found
        ids = lids[start:stop]
        positions = np.searchsorted(hub_ids, ids)
        positions[positions >= len(hub_ids)] = 0
        hit = hub_ids[positions] == ids
        if hit.any():
            view = resources[start:stop]
            view[hit] = hub_resources[positions[hit]]
    return resources


# -- chunks -----------------------------------------------------------------------------------


Piece = tuple[np.ndarray, Callable[[int, int], list]]


def unit_chunks(total_rows: int, pieces: Iterable[Piece], max_rows: int, max_bytes: int,
                ) -> Iterator[tuple[int, list]]:
    """Yield ``(rows, record buffers)`` per chunk of one unit, in chain order.

    ``pieces`` stream the unit's rows in order as ``(byte sizes, body)``;
    ``body(a, b)`` returns buffers holding rows ``a..b-1`` of the piece, each
    record followed by one comma.  The chunks are exactly
    :func:`~src.loader.edge_plan.chunk_boundaries` of the whole unit.
    """
    if max_rows < 1 or max_bytes < 1:
        raise EdgePlanError("chunk limits must be positive")
    ranges = iter(split_sizes(total_rows, max_rows))
    remaining = next(ranges, 0)
    used = count = 0
    buffers: list = []
    seen = 0
    for sizes, body in pieces:
        n = len(sizes)
        if not n:
            continue
        seen += n
        cumulative = np.zeros(n + 1, dtype=np.int64)
        np.cumsum(sizes, dtype=np.int64, out=cumulative[1:])
        cumulative[1:] += np.arange(1, n + 1, dtype=np.int64)
        position = 0
        while position < n:
            if remaining <= 0:
                raise EdgePlanError("unit rows exceed the declared unit size")
            limit = min(n, position + remaining)
            end = int(np.searchsorted(cumulative, cumulative[position] + (max_bytes - used), side="right")) - 1
            if count == 0:
                end = max(end, position + 1)
            end = min(end, limit)
            if end > position:
                buffers.extend(body(position, end))
                count += end - position
                used += int(cumulative[end] - cumulative[position])
                remaining -= end - position
                position = end
            if remaining == 0:
                yield count, buffers
                buffers, used, count = [], 0, 0
                remaining = next(ranges, 0)
            elif position < n:
                yield count, buffers
                buffers, used, count = [], 0, 0
    if seen != total_rows or count:
        raise EdgePlanError(f"unit streamed {seen} rows; expected {total_rows}")


def gather_body(rec: memoryview, offsets: np.ndarray, rows: np.ndarray) -> Callable[[int, int], list]:
    """``body(a, b)`` over the rows ``rows[a:b]`` of one in-memory group."""
    def body(start: int, end: int) -> list:
        selected = rows[start:end]
        first, last = int(selected[0]), int(selected[-1])
        if last - first == end - start - 1 and (end - start < 2 or bool(np.all(np.diff(selected) == 1))):
            return [rec[int(offsets[first]):int(offsets[last + 1])]]
        return [rec[begin:stop] for begin, stop in zip(offsets[selected].tolist(), offsets[selected + 1].tolist())]
    return body


def list_body(records: list, rows: np.ndarray | None) -> Callable[[int, int], list]:
    """``body(a, b)`` over record objects ``records[rows[a:b]]`` (all records if ``rows`` is None)."""
    def body(start: int, end: int) -> list:
        if rows is None:
            return [b",".join(records[start:end]), b","]
        selected = rows[start:end]
        first, last = int(selected[0]), int(selected[-1])
        if last - first == end - start - 1 and (end - start < 2 or bool(np.all(np.diff(selected) == 1))):
            return [b",".join(records[first:last + 1]), b","]
        return [b",".join([records[index] for index in selected.tolist()]), b","]
    return body


def contiguous_body(rec: memoryview, offsets: np.ndarray) -> Callable[[int, int], list]:
    def body(start: int, end: int) -> list:
        return [rec[int(offsets[start]):int(offsets[end])]]
    return body


def record_offsets(blen: np.ndarray) -> np.ndarray:
    """Start offsets (plus the end) of comma-terminated records of byte lengths ``blen``."""
    offsets = np.zeros(len(blen) + 1, dtype=np.int64)
    np.cumsum(blen, dtype=np.int64, out=offsets[1:])
    offsets[1:] += np.arange(1, len(blen) + 1, dtype=np.int64)
    return offsets
