"""Private bounded worker frames; parent decoding never executes pickle callbacks.

Only the first parent->child task submission uses pickle (explicitly excluded
source serialization). All subsequent traffic uses the closed binary codec.
"""
from dataclasses import dataclass, fields
import math
import pickle
import selectors
import struct
import time

import numpy as np

from .edge_plan import ChunkSpec

HEADER = struct.Struct('!4sQ')
MAGIC = b'GLP1'
INITIAL_MAGIC = b'GLP0'
SLICE_SECONDS = 0.1


class FrameProtocolError(ConnectionError):
    """Invalid, unsupported, oversized or incomplete private worker message."""


class WaitCancelled(Exception):
    pass


class WaitExpired(TimeoutError):
    pass


class WaitControl:
    def __init__(self, *, deadline=None, cancel_event=None, clock=None):
        self.deadline, self.cancel_event = deadline, cancel_event
        self.clock = clock or time.monotonic

    def check(self):
        if self.cancel_event is not None and self.cancel_event.is_set():
            raise WaitCancelled('worker wait cancelled')
        if self.deadline is None:
            return SLICE_SECONDS
        remaining = self.deadline - self.clock()
        if remaining <= 0:
            raise WaitExpired('worker wait deadline expired')
        return min(SLICE_SECONDS, remaining)


@dataclass(frozen=True)
class CodecLimits:
    frame: int = 64 << 20
    scalar: int = 1 << 20
    array: int = 32 << 20
    allocation: int = 64 << 20
    nodes: int = 1_000_000
    depth: int = 64


_DTYPES = ('<u4', '<u8', '>u4', '>u8')
_SPEC_FIELDS = tuple(field.name for field in fields(ChunkSpec))


def encode(value, *, check=None, limits=CodecLimits()):
    check = check or WaitControl().check
    output = bytearray()
    nodes = allocated = 0

    def put(data):
        if len(output) + len(data) > limits.frame:
            raise FrameProtocolError('IPC frame exceeds byte limit')
        output.extend(data)

    def blob(tag, data, bound):
        nonlocal allocated
        allocated += len(data)
        if allocated > limits.allocation:
            raise FrameProtocolError("IPC cumulative allocation limit exceeded")
        if len(data) > bound:
            raise FrameProtocolError('IPC scalar/array exceeds byte limit')
        put(tag + struct.pack('!I', len(data)))
        put(data)

    def visit(item, depth):
        nonlocal nodes
        nodes += 1
        if nodes > limits.nodes or depth > limits.depth:
            raise FrameProtocolError('IPC node/depth limit exceeded')
        if not nodes % 1024:
            check()
        kind = type(item)
        if item is None:
            put(b'n')
        elif kind is bool:
            put(b't' if item else b'f')
        elif kind is int:
            length = max(1, (item.bit_length() + 8) // 8)
            if length > limits.scalar:
                raise FrameProtocolError("IPC integer exceeds scalar byte limit")
            blob(b'i', item.to_bytes(length, "big", signed=True), limits.scalar)
        elif kind is float:
            if not math.isfinite(item):
                raise FrameProtocolError('IPC nonfinite float')
            put(b'r' + struct.pack('!d', item))
        elif kind is str:
            if len(item) > limits.scalar:
                raise FrameProtocolError('IPC scalar exceeds byte limit')
            blob(b's', item.encode('utf-8'), limits.scalar)
        elif kind is bytes:
            blob(b'b', item, limits.scalar)
        elif kind in (list, tuple, dict):
            put({list: b'l', tuple: b'u', dict: b'd'}[kind] + struct.pack('!I', len(item)))
            if kind is dict:
                for key, val in item.items():
                    visit(key, depth + 1); visit(val, depth + 1)
            else:
                for val in item:
                    visit(val, depth + 1)
        elif kind is np.ndarray:
            if item.ndim != 1 or item.dtype.str not in _DTYPES or item.nbytes > limits.array:
                raise FrameProtocolError('IPC requires a bounded 1D uint32/uint64 array')
            put(b'a' + bytes([_DTYPES.index(item.dtype.str)]))
            blob(b'b', item.tobytes(order='C'), limits.array)
        elif kind is ChunkSpec:
            put(b'c')
            for name in _SPEC_FIELDS:
                visit(getattr(item, name), depth + 1)
        else:
            # Do not format/inspect/reduce an unapproved user object.
            raise FrameProtocolError('unsupported IPC value type')
    check()
    visit(value, 0)
    check()
    return bytes(output)


def decode(payload, *, check=None, limits=CodecLimits()):
    check = check or WaitControl().check
    if len(payload) > limits.frame:
        raise FrameProtocolError('IPC frame exceeds byte limit')
    view = memoryview(payload)
    position = nodes = allocated = 0

    def take(size):
        nonlocal position
        if size < 0 or position + size > len(view):
            raise FrameProtocolError('truncated IPC value')
        result = view[position:position + size]
        position += size
        return result

    def size():
        return struct.unpack('!I', take(4))[0]

    def blob(bound):
        nonlocal allocated
        length = size()
        if length > bound:
            raise FrameProtocolError('IPC scalar/array exceeds byte limit')
        allocated += length
        if allocated > limits.allocation:
            raise FrameProtocolError('IPC cumulative allocation limit exceeded')
        return take(length)

    def visit(depth):
        nonlocal nodes
        nodes += 1
        if nodes > limits.nodes or depth > limits.depth:
            raise FrameProtocolError('IPC node/depth limit exceeded')
        if not nodes % 1024:
            check()
        tag = bytes(take(1))
        if tag == b'n': return None
        if tag == b't': return True
        if tag == b'f': return False
        if tag == b'i':
            return int.from_bytes(blob(limits.scalar), 'big', signed=True)
        if tag == b'r':
            value = struct.unpack('!d', take(8))[0]
            if not math.isfinite(value): raise FrameProtocolError('IPC nonfinite float')
            return value
        if tag == b's': return bytes(blob(limits.scalar)).decode('utf-8')
        if tag == b'b': return bytes(blob(limits.scalar))
        if tag in (b'l', b'u', b'd'):
            count = size()
            if count * (2 if tag == b'd' else 1) > limits.nodes - nodes:
                raise FrameProtocolError('IPC collection exceeds node limit')
            if tag == b'd':
                result = {}
                for _ in range(count):
                    key = visit(depth + 1)
                    value = visit(depth + 1)
                    if type(key) not in (str, bytes, int, float, bool, tuple, type(None)):
                        raise FrameProtocolError('invalid IPC mapping key')
                    result[key] = value
                return result
            result = [visit(depth + 1) for _ in range(count)]
            return tuple(result) if tag == b'u' else result
        if tag == b'a':
            code = bytes(take(1))[0]
            if code >= len(_DTYPES) or bytes(take(1)) != b'b':
                raise FrameProtocolError('invalid IPC array dtype/tag')
            raw = blob(limits.array)
            dtype = np.dtype(_DTYPES[code])
            if len(raw) % dtype.itemsize: raise FrameProtocolError('invalid IPC array length')
            return np.frombuffer(raw, dtype=dtype).copy()
        if tag == b'c':
            return ChunkSpec(*(visit(depth + 1) for _ in _SPEC_FIELDS))
        raise FrameProtocolError('unknown IPC value tag')
    check()
    try:
        result = visit(0)
    except (UnicodeError, ValueError, TypeError, struct.error) as exc:
        raise FrameProtocolError('corrupt IPC value') from exc
    if position != len(view): raise FrameProtocolError('trailing IPC bytes')
    check()
    return result


class FrameStream:
    def __init__(self, sock, *, check=None, limits=CodecLimits()):
        self.socket = sock
        sock.setblocking(False)
        self.check = check or WaitControl().check
        self.limits = limits
        self._outbound_incomplete = False

    def _wait(self, event, check):
        timeout = check()
        with selectors.DefaultSelector() as selector:
            selector.register(self.socket, event)
            selector.select(timeout)
        check()

    def _write(self, content, check):
        view = memoryview(content)
        while view:
            check()
            try:
                count = self.socket.send(view[:65536])
            except (BlockingIOError, InterruptedError):
                self._wait(selectors.EVENT_WRITE, check)
                continue
            if not count: raise FrameProtocolError('EOF writing worker frame')
            view = view[count:]

    def _read(self, count, check):
        output = bytearray()
        while len(output) < count:
            check()
            try:
                data = self.socket.recv(min(65536, count - len(output)))
            except (BlockingIOError, InterruptedError):
                self._wait(selectors.EVENT_READ, check)
                continue
            if not data: raise FrameProtocolError(f'EOF/truncated worker frame ({len(output)}/{count} bytes)')
            output.extend(data)
        return bytes(output)

    def _send_payload(self, payload, magic, check):
        if len(payload) > self.limits.frame: raise FrameProtocolError('IPC frame exceeds byte limit')
        if self._outbound_incomplete:
            raise FrameProtocolError("cannot append a command after an interrupted frame")
        self._outbound_incomplete = True
        self._write(HEADER.pack(magic, len(payload)), check)
        self._write(payload, check)
        self._outbound_incomplete = False
        check()

    def send(self, value, *, check=None):
        check = check or self.check
        self._send_payload(encode(value, check=check, limits=self.limits), MAGIC, check)

    def send_initial(self, value):
        # Source reducers execute only on this explicitly excluded initial submission.
        self._send_payload(pickle.dumps(value, protocol=5), INITIAL_MAGIC, self.check)

    def recv(self, *, initial=False):
        magic, length = HEADER.unpack(self._read(HEADER.size, self.check))
        if magic != (INITIAL_MAGIC if initial else MAGIC): raise FrameProtocolError('invalid worker frame magic')
        if length > self.limits.frame: raise FrameProtocolError('IPC frame exceeds byte limit')
        payload = self._read(length, self.check)
        if initial:
            return pickle.loads(payload)  # child-only bootstrap; never parent reply decoding
        return decode(payload, check=self.check, limits=self.limits)

    def close(self):
        self.socket.close()
