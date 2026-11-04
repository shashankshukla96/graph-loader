"""Follow the logs of every application container while it lives (stream node containers are auto-removed, so their logs vanish).

Each followed container gets a capped log file and a counter per marker. The markers are the lines the docs promise for each fault:
``stage=write-outage`` / ``write-outage-recovered`` (B7), ``*-drain-abandoned`` (B7/B10), ``reason=neo4j_outage``, ``retry_stalled``,
``stale_resurrected_copy_skipped`` (B5), ``parked_pair_index_lagging`` and generic ERROR lines.
"""
from __future__ import annotations

from pathlib import Path
import threading
import time

MARKERS = ('stage=write-outage ', 'stage=write-outage-retry', 'stage=write-outage-recovered', 'drain-abandoned', 'reason=neo4j_outage',
           'reason=neo4j_fatal', 'retry_stalled', 'stale_resurrected_copy_skipped', 'parked_pair_index_lagging',
           'duplicate_or_unindexed_skipped', 'duplicate_skipped', 'stage=lease-server-restart', 'resume=stable-group', 'ERROR')
FILE_CAP = 32 * 1024 * 1024


class LogCollector(threading.Thread):
    def __init__(self, client, directory, fleet_provider, *, period=2.0):
        super().__init__(name='log-collector', daemon=True)
        self.client, self.directory, self.fleet_provider, self.period = client, Path(directory), fleet_provider, period
        self.directory.mkdir(parents=True, exist_ok=True)
        self.followers = {}
        self.counts = {}   # container name -> {marker: n}
        self.lines = {}    # container name -> {marker: [last few lines]}
        self.stop_event = threading.Event()
        self._lock = threading.Lock()

    def run(self):
        while not self.stop_event.is_set():
            fleet = self.fleet_provider()
            if fleet is not None:
                try:
                    fleet.record_new()
                except Exception:
                    pass
                for items in fleet.live().values():
                    for cid, name in items:
                        if cid not in self.followers:
                            thread = threading.Thread(target=self._follow, args=(cid, name), daemon=True, name=f'follow-{name[:24]}')
                            self.followers[cid] = thread
                            thread.start()
            self.stop_event.wait(self.period)

    def _follow(self, cid, name):
        path = self.directory / f'{name}-{cid[:12]}.log'
        written = 0
        try:
            stream = self.client.api.logs(cid, stream=True, follow=True, stdout=True, stderr=True, timestamps=False)
            with path.open('ab') as handle:
                buffer = b''
                for chunk in stream:
                    buffer += chunk
                    while b'\n' in buffer:
                        line, buffer = buffer.split(b'\n', 1)
                        text = line.decode(errors='replace')
                        if written < FILE_CAP:
                            handle.write(line + b'\n')
                            written += len(line) + 1
                        self._count(name, text)
        except Exception as error:  # the container ended or Docker dropped the stream
            with self._lock:
                self.counts.setdefault(name, {}).setdefault('_stream_error', 0)
                self.counts[name]['_stream_error'] += 1
                self.lines.setdefault(name, {})['_stream_error'] = [f'{type(error).__name__}']

    def _count(self, name, text):
        with self._lock:
            for marker in MARKERS:
                if marker in text:
                    self.counts.setdefault(name, {})[marker] = self.counts.get(name, {}).get(marker, 0) + 1
                    kept = self.lines.setdefault(name, {}).setdefault(marker, [])
                    kept.append(text[:300])
                    del kept[:-3]

    def total(self, marker, since=None):
        with self._lock:
            return sum(row.get(marker, 0) for row in self.counts.values())

    def snapshot(self):
        with self._lock:
            return {'counts': {k: dict(v) for k, v in self.counts.items()}, 'lines': {k: {m: list(v) for m, v in d.items()} for k, d in self.lines.items()}}

    def stop(self):
        self.stop_event.set()
        self.join(timeout=10)
