"""The installed ``graph-loader start --mode stream`` fleet and its supervisor.

``Fleet`` is one incarnation (one CLI process and an ``AppObserver`` watching the containers it creates on the owned network).
``Supervisor`` runs incarnations the way production would: when the CLI exits it runs the **narrowed recovery** (only containers
this incarnation's observer adopted, re-verified as owned, removed by recorded ID — never the documented unnarrowed
``label=app=graph-loader`` filter), waits a backoff and starts the same command again. Every exit is recorded with its code and
reason line; an exit nobody injected is an **unexplained restart** and a failure.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import signal
import threading
import time

from tests.installed_service.live_cli import AppObserver, RunningCli, cli_environment

COMPONENTS = ('node-loader', 'edge-loader', 'retry-consumer', 'bucket-lease-server')
REASON = re.compile(r'(stage=\S+.*(failed|exited|stopping|abandon|lost).*|ERROR.*)', re.IGNORECASE)


def expected_counts(replicas: dict, node_labels, edge_types, retry_consumers=1) -> dict:
    return {'node-loader': sum(replicas[label] for label in node_labels),
            'edge-loader': sum(replicas[rel] for rel in edge_types),
            'retry-consumer': retry_consumers, 'bucket-lease-server': 1}


class Fleet:
    def __init__(self, ctx, incarnation: int, schema_path: Path, *, expected: dict, extra_args=()):
        self.ctx, self.incarnation, self.expected = ctx, incarnation, expected
        self.name = f'fleet{incarnation:03d}'
        self.schema_path = Path(schema_path)
        self.started_at = time.time()
        self.observer = AppObserver(ctx.client, ctx.registry, ctx.stack.network.id, owner=ctx.stack.owner,
                                    candidate_images={row['id'] for row in ctx.config['images'].values()},
                                    barrier_tag=f'{self.name}-')
        self.observer.start()
        self.env = cli_environment(ctx.stack, ctx.config['images'], ctx.config['docker_host'],
                                   extra={'GRAPH_LOADER_METRICS_ENABLED': '1', 'LOADER_CPUSET': ctx.cpuset,
                                          'GRAPH_LOADER_METRICS_PORT': str(ctx.cli_metrics_port)})
        ctx.log_dir.mkdir(parents=True, exist_ok=True)
        args = ['start', '--mode', 'stream', '--config', str(self.schema_path), '--network', ctx.stack.network.name,
                '--skip-image-build', '--run-dir', str(ctx.run_dir / 'runs'), '--run-id', ctx.run_id,
                '--stream-group-member-wait-seconds', str(ctx.member_wait), *extra_args]
        self.cli = RunningCli(Path(ctx.config['venv_bin']) / 'graph-loader', args, env=self.env, cwd=ctx.run_dir,
                              log_dir=ctx.log_dir, name=self.name, deadline=ctx.cli_deadline, redact=[ctx.stack.password])
        self.cpuset_seen = {}
        self.finished = None

    # ---- containers ----------------------------------------------------------------------------------------------------
    def live(self):
        """{component: [(container id, name)]} for adopted containers not known to have ended."""
        with self.observer._lock:
            rows = [(cid, row.get('inspected') or {}) for cid, row in self.observer.containers.items()
                    if cid not in self.observer.ended]
        out = {}
        for cid, info in rows:
            if info:
                out.setdefault(info['labels'].get('component'), []).append((cid, info['name']))
        return out

    def counts(self):
        return {k: len(v) for k, v in self.live().items()}

    def ready(self):
        counts = self.counts()
        return all(counts.get(k, 0) == n for k, n in self.expected.items())

    def wait_ready(self, timeout=240):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if not self.cli.running():
                raise RuntimeError('the fleet CLI exited before the fleet was ready: ' + self.cli.text()[-1500:])
            if self.ready() and self.all_running():
                return time.monotonic() - (end - timeout)
            time.sleep(1)
        raise TimeoutError(f'fleet not ready after {timeout}s: have {self.counts()}, want {self.expected}')

    def all_running(self):
        for components in self.live().values():
            for cid, _ in components:
                try:
                    if not self.ctx.client.api.inspect_container(cid)['State']['Running']:
                        return False
                except Exception:
                    return False
        return True

    def record_new(self):
        """Inspect containers not seen yet and remember their ``HostConfig`` (cpuset, memory): short-lived replacements included."""
        for items in self.live().values():
            for cid, name in items:
                if cid in self.cpuset_seen:
                    continue
                try:
                    attrs = self.ctx.client.api.inspect_container(cid)
                except Exception:
                    continue
                host = attrs.get('HostConfig', {})
                self.cpuset_seen[cid] = {'name': name, 'component': attrs['Config']['Labels'].get('component'),
                                         'cpuset': host.get('CpusetCpus', ''), 'memory_limit': host.get('Memory', 0),
                                         'incarnation': self.incarnation}

    def inspect_all(self):
        """Facts about every live container; ``HostConfig.CpusetCpus`` is recorded once per container (replacements included)."""
        rows = []
        for component, items in self.live().items():
            for cid, name in items:
                try:
                    attrs = self.ctx.client.api.inspect_container(cid)
                except Exception:
                    continue
                host = attrs.get('HostConfig', {})
                self.cpuset_seen[cid] = {'name': name, 'component': component, 'cpuset': host.get('CpusetCpus', ''),
                                         'memory_limit': host.get('Memory', 0), 'incarnation': self.incarnation}
                ports = attrs['NetworkSettings']['Ports'] or {}
                metrics = attrs['Config']['Labels'].get('graph_loader.metrics_port')
                bindings = ports.get(f'{metrics}/tcp') if metrics else None
                rows.append({'id': cid, 'name': name, 'component': component, 'state': attrs['State']['Status'],
                             'oom_killed': attrs['State']['OOMKilled'], 'exit_code': attrs['State']['ExitCode'],
                             'restarts': attrs.get('RestartCount', 0), 'started': attrs['State']['StartedAt'],
                             'metrics_port': int(bindings[0]['HostPort']) if bindings else None})
        return rows

    # ---- control -------------------------------------------------------------------------------------------------------
    def kill_container(self, cid):
        return self.observer.kill_observed(cid)

    def sigterm(self, timeout=90):
        return self.cli.terminate(timeout=timeout)

    def sigkill_cli(self):
        """``kill -9`` of the CLI process only: its containers keep running."""
        os.kill(self.cli.process.pid, signal.SIGKILL)
        self.cli.process.wait(timeout=10)
        self.cli._finish()

    def reason_lines(self, limit=6):
        lines = [line.strip() for line in self.cli.text().splitlines() if REASON.search(line)]
        return lines[-limit:]

    def finish(self):
        """Tear the incarnation down: the CLI is gone or stopped; remove what it left (narrowed to what the observer adopted)."""
        if self.finished is not None:
            return self.finished
        code = self.cli.process.poll()
        if code is None:
            code = self.cli.terminate(timeout=90)
        problems = []
        try:
            self.observer.barrier()
            self.observer.drain(timeout=60)
        except Exception as error:
            problems.append(f'{type(error).__name__}: {error}')
        finally:
            self.observer.stop()
        snapshot = self.observer.snapshot()
        leftovers = self.observer.reap()
        self.ctx.log_dir.mkdir(parents=True, exist_ok=True)
        (self.ctx.log_dir / f'{self.name}-observer.json').write_text(json.dumps(snapshot, indent=1, sort_keys=True, default=str) + '\n')
        self.finished = {'exit_code': code, 'snapshot_containers': len(snapshot['containers']), 'leftovers': leftovers,
                         'refused': snapshot['refused'], 'unobserved': self.observer.unobserved_candidates(),
                         'observer_errors': snapshot['errors'], 'closed_early': snapshot['closed_early'], 'problems': problems,
                         'created_from_candidates': len(snapshot['created_from_candidates'])}
        return self.finished


class Supervisor(threading.Thread):
    def __init__(self, ctx, make_fleet, *, backoff=5.0, on_event=None):
        super().__init__(name='supervisor', daemon=True)
        self.ctx, self.make_fleet, self.backoff, self.on_event = ctx, make_fleet, backoff, on_event
        self.fleet = None
        self.generation = 0
        self.events = []
        self.unexplained = []
        self.stop_requested = threading.Event()
        self.ready = threading.Event()
        self._expect = []
        self._windows = []
        self._lock = threading.Lock()
        self.failed = None

    def expect(self, cause, *, exit_codes=(0, 1, None)):
        """An injected fault is about to end the fleet: the next exit is explained by it."""
        with self._lock:
            self._expect.append({'cause': cause, 'exit_codes': tuple(exit_codes), 'at': time.time()})

    def explain_until(self, cause, deadline):
        """Any fleet exit before ``deadline`` (epoch seconds) is explained by this fault (for example a Kafka broker restart)."""
        with self._lock:
            self._windows.append({'cause': cause, 'until': deadline})

    def end_windows(self, cause):
        """The fault that opened an explanation window is over (the fleet is ready again): exits after this are not explained by it."""
        now = time.time()
        with self._lock:
            for window in self._windows:
                if window['cause'] == cause:
                    window['until'] = min(window['until'], now)

    def _take(self, code):
        with self._lock:
            if self._expect:
                return self._expect.pop(0)
            now = time.time()
            self._windows = [w for w in self._windows if w['until'] > now - 60]
            for window in self._windows:
                if now <= window['until']:
                    return {'cause': window['cause'], 'exit_codes': (code,), 'at': now}
        return None

    def run(self):
        incarnation = 0
        try:
            while not self.stop_requested.is_set():
                incarnation += 1
                self.ready.clear()
                fleet = self.make_fleet(incarnation)
                self.fleet = fleet
                self.generation = incarnation
                try:
                    fleet.wait_ready(self.ctx.ready_timeout)
                    self.ready.set()
                except Exception as error:
                    self._record(fleet, f'not ready: {error}')
                    self._recover(fleet)
                    if self.stop_requested.wait(self.backoff):
                        break
                    continue
                while fleet.cli.running() and not self.stop_requested.is_set():
                    time.sleep(0.5)
                if self.stop_requested.is_set() and fleet.cli.process.poll() is None:
                    break
                self._record(fleet)
                self._recover(fleet)
                if self.stop_requested.wait(self.backoff):
                    break
        except BaseException as error:  # a supervisor that dies must be visible
            self.failed = f'{type(error).__name__}: {error}'
            raise

    def _record(self, fleet, note=None):
        code = fleet.cli.process.returncode
        expectation = self._take(code)
        event = {'incarnation': fleet.incarnation, 'ran_s': round(time.time() - fleet.started_at, 1), 'exit_code': code,
                 'reasons': fleet.reason_lines(), 'cause': expectation['cause'] if expectation else None,
                 'injected': expectation is not None, 'note': note, 'at': time.time()}
        if expectation is None:
            self.unexplained.append(event)
        self.events.append(event)
        if self.on_event:
            self.on_event(event)

    def _recover(self, fleet):
        result = fleet.finish()
        if self.events:
            self.events[-1]['recovery'] = result

    def shutdown(self, timeout=120):
        """Stop supervising and stop the current fleet gracefully (SIGTERM). Returns the last incarnation's finish record."""
        self.stop_requested.set()
        fleet = self.fleet
        result = None
        if fleet is not None:
            fleet.sigterm(timeout=90)
            result = fleet.finish()
            self.events.append({'incarnation': fleet.incarnation, 'exit_code': fleet.cli.process.returncode, 'cause': 'harness shutdown',
                                'injected': True, 'recovery': result, 'at': time.time(), 'reasons': fleet.reason_lines()})
        self.join(timeout=timeout)
        return result
