"""One installed ``graph-loader start --mode stream`` under an observer, for schema text supplied by the caller."""
from __future__ import annotations

import json
from pathlib import Path
import time

from . import query_window as qw
from .live_cli import AppObserver, RunningCli, cli_environment


class StreamRun:
    def __init__(self, stack, client, registry, config, run, name, schema_text, *, extra=(), deadline=900, track_queries=True):
        (run / 'logs').mkdir(parents=True, exist_ok=True)
        (run / f'{name}.yaml').write_text(schema_text)
        self.stack, self.client, self.run, self.name = stack, client, run, name
        self.track_queries = track_queries  # False when Neo4j is restarted during the run: query IDs restart, no window exists
        self.before = qw.capture(stack.neo, stack.marker) if track_queries else None
        self.observer = AppObserver(client, registry, stack.network.id, owner=stack.owner,
                                    candidate_images={row['id'] for row in config['images'].values()},
                                    barrier_tag=f'{run.name}-{name}-')
        self.observer.start()
        self.logs = {}
        self.env = cli_environment(stack, config['images'], config['docker_host'])
        self.executable = Path(config['venv_bin']) / 'graph-loader'
        self.schema_path = run / f'{name}.yaml'
        self.cli = RunningCli(self.executable,
                              ['start', '--mode', 'stream', '--config', str(self.schema_path), '--network', stack.network.name,
                               '--skip-image-build', '--run-dir', str(run / 'runs'), *extra],
                              env=self.env, cwd=run, log_dir=run / 'logs', name=name, deadline=deadline, redact=[stack.password])

    def components(self, name):
        """(container id, name) of the adopted application containers of one component that are not known to have ended."""
        with self.observer._lock:
            rows = [(cid, row.get('inspected') or {}) for cid, row in self.observer.containers.items() if cid not in self.observer.ended]
        return [(cid, info['name']) for cid, info in rows if info and info['labels'].get('component') == name]

    def poll_logs(self):
        with self.observer._lock:
            rows = list(self.observer.containers.items())
        for cid, row in rows:
            info = row.get('inspected') or {}
            cname = info.get('name') or row.get('event_name') or cid[:12]
            try:
                self.logs[cname] = self.client.api.logs(cid, tail=300, stdout=True, stderr=True).decode(errors='replace')
            except Exception as error:
                self.logs.setdefault(cname, f'<unreadable: {type(error).__name__}>')

    def wait(self, what, predicate, timeout):
        end = time.monotonic() + timeout
        began = time.monotonic()
        while True:
            assert self.cli.running(), f'the streaming CLI exited while waiting for {what}: {self.cli.text()[-1500:]}'
            value = predicate()
            if value:
                return time.monotonic() - began
            assert time.monotonic() < end, f'timed out waiting for {what}'
            self.poll_logs()
            time.sleep(0.5)

    def finish(self):
        code = self.cli.terminate(timeout=90)
        self.poll_logs()
        try:
            self.observer.barrier()
            self.observer.drain()
        finally:
            self.observer.stop()
        snapshot = self.observer.snapshot()
        leftovers = self.observer.reap()
        (self.run / 'logs' / f'{self.name}-observer.json').write_text(json.dumps(snapshot, indent=1, sort_keys=True, default=str) + '\n')
        for container, text in self.logs.items():
            (self.run / 'logs' / f'{self.name}-container-{container}.log').write_text(text)
        records = []
        if self.track_queries:
            after = qw.capture(self.stack.neo, self.stack.marker)
            (self.run / 'logs' / f'{self.name}-query-log-delta.log').write_bytes(after.content[len(self.before.content):])
            records = qw.window(self.before, after)
        return {'exit_code': code, 'stop_seconds': self.cli.stop_seconds, 'snapshot': snapshot, 'leftovers': leftovers,
                'records': records}
