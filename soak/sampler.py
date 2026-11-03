"""Continuous sampling (every 15 s, JSON lines) and the host guards.

The sampler is built from injected providers so that its gap accounting and the guard thresholds are testable without Docker:
``providers`` maps a name to a zero-argument callable returning a JSON-able value. A provider that raises is recorded as an
error for that tick (never as zero). A gap between ticks over 60 s is a **coverage failure**.
"""
from __future__ import annotations

import json
from pathlib import Path
import re
import threading
import time
import urllib.request

PERIOD_S = 15.0
GAP_LIMIT_S = 60.0
GUARD_PERIOD_S = 5.0
MEMORY_FLOOR = int(1.5 * 1024 ** 3)
DISK_REFUSE = 60 * 1024 ** 3
DISK_STOP = 30 * 1024 ** 3


# ---- readers -----------------------------------------------------------------------------------------------------------------

def read_mem_available(path='/proc/meminfo') -> int:
    for line in Path(path).read_text().splitlines():
        if line.startswith('MemAvailable:'):
            return int(line.split()[1]) * 1024
    raise RuntimeError('MemAvailable not found')


def fs_free(path) -> int:
    import os
    st = os.statvfs(path)
    return st.f_bavail * st.f_frsize


METRIC = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([-+0-9.eEnNaIf]+)\s*$')


def parse_metrics(text: str):
    """Prometheus text exposition to ``(name, {labels}, value)`` rows; comments and malformed lines are skipped."""
    rows = []
    for line in text.splitlines():
        if not line or line.startswith('#'):
            continue
        match = METRIC.match(line)
        if match is None:
            continue
        labels = dict(re.findall(r'(\w+)="((?:[^"\\]|\\.)*)"', match.group(2) or ''))
        try:
            rows.append((match.group(1), labels, float(match.group(3))))
        except ValueError:
            pass
    return rows


def scrape(port, timeout=3.0) -> list:
    with urllib.request.urlopen(f'http://127.0.0.1:{port}/metrics', timeout=timeout) as response:
        return parse_metrics(response.read().decode(errors='replace'))


def working_set(stats: dict):
    """(RSS-like working set bytes, cumulative CPU ns) from one ``docker stats`` sample (cgroup v1 or v2)."""
    memory = stats.get('memory_stats') or {}
    usage = memory.get('usage') or 0
    detail = memory.get('stats') or {}
    cache = detail.get('inactive_file', detail.get('total_inactive_file', detail.get('cache', 0)))
    return max(usage - cache, 0), (stats.get('cpu_stats') or {}).get('cpu_usage', {}).get('total_usage', 0)


# ---- sampler -----------------------------------------------------------------------------------------------------------------

class Sampler(threading.Thread):
    def __init__(self, path, providers, *, period=PERIOD_S, clock=time.monotonic, wall=time.time):
        super().__init__(name='sampler', daemon=True)
        self.path, self.providers, self.period, self.clock, self.wall = Path(path), providers, period, clock, wall
        self.stop_event = threading.Event()
        self.last = None
        self.gaps = []            # coverage failures
        self.ticks = 0
        self.errors = {}
        self.latest = {}
        self._lock = threading.Lock()

    def sample_once(self):
        now = self.clock()
        if self.last is not None and now - self.last > GAP_LIMIT_S:
            self.gaps.append({'at': self.wall(), 'seconds': round(now - self.last, 1)})
        self.last = now
        row = {'t': round(self.wall(), 1)}
        for name, provider in self.providers.items():
            try:
                row[name] = provider()
            except Exception as error:  # a failed provider is recorded, never read as zero
                row[name] = {'error': f'{type(error).__name__}: {error}'[:200]}
                self.errors[name] = self.errors.get(name, 0) + 1
        with self._lock:
            self.latest = row
            self.ticks += 1
        with self.path.open('a') as handle:
            handle.write(json.dumps(row, sort_keys=True, default=str) + '\n')
        return row

    def run(self):
        while not self.stop_event.is_set():
            started = self.clock()
            try:
                self.sample_once()
            except Exception as error:
                self.errors['sampler'] = self.errors.get('sampler', 0) + 1
                self.latest = {'error': str(error)}
            self.stop_event.wait(max(self.period - (self.clock() - started), 0.1))

    def stop(self):
        self.stop_event.set()
        self.join(timeout=30)

    def coverage_failures(self):
        return [f"sampling gap of {gap['seconds']} s" for gap in self.gaps]


class Guard(threading.Thread):
    """Samples host memory and Docker filesystem free space every 5 s; trips once, with a reason, and calls ``on_trip``."""

    def __init__(self, on_trip, *, mem=read_mem_available, disk=None, period=GUARD_PERIOD_S, memory_floor=MEMORY_FLOOR, disk_stop=DISK_STOP):
        super().__init__(name='guard', daemon=True)
        self.on_trip, self.mem, self.disk, self.period = on_trip, mem, disk, period
        self.memory_floor, self.disk_stop = memory_floor, disk_stop
        self.stop_event = threading.Event()
        self.tripped = None
        self.min_mem = None
        self.min_disk = None
        self.samples = 0

    def check(self):
        available = self.mem()
        self.min_mem = available if self.min_mem is None else min(self.min_mem, available)
        self.samples += 1
        if available < self.memory_floor:
            return 'host_memory', available
        if self.disk is not None:
            free = self.disk()
            self.min_disk = free if self.min_disk is None else min(self.min_disk, free)
            if free < self.disk_stop:
                return 'host_disk', free
        return None

    def run(self):
        while not self.stop_event.wait(self.period):
            try:
                verdict = self.check()
            except Exception as error:
                verdict = ('guard_error', str(error))
            if verdict and self.tripped is None:
                self.tripped = {'reason': verdict[0], 'value': verdict[1], 'at': time.time()}
                self.on_trip(self.tripped)
                return

    def stop(self):
        self.stop_event.set()
        self.join(timeout=15)
