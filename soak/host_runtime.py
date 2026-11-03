"""The soak on host processes: ``HostSoak`` (the ``Soak`` of runtime.py with a process or systemd-service fleet), its faults and its
log collector. E14.F3.S10; the container path of E13 is untouched.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import socket
import subprocess
import threading
import time

from tests.installed_service.live_cli import run_cli

from . import budget
from . import host
from .faults import Faults, judge_sigterm_outage
from .fleet import Supervisor
from .logs import MARKERS
from .runtime import Soak, SoakConfig, foreign_snapshot, diff_foreign  # noqa: F401
from .sampler import fs_free, read_mem_available, working_set

GiB = 1024 ** 3


@dataclass
class HostConfig(SoakConfig):
    shape: str = 'process'                 # 'process' or 'service'
    wheel: str = ''
    wheel_sha256: str = ''
    service_memory_max: str = '3G'
    rebalance_delay_ms: int = 3000      # Kafka's default group.initial.rebalance.delay.ms; E13's owned broker used 0
    run_root: str = '~/e14-soak'


def free_port():
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        return probe.getsockname()[1]


# ---- logs from files -------------------------------------------------------------------------------------------------------

class FileLogCollector(threading.Thread):
    """Count the documented log markers in every child's log file (``<attempt>/logs/<name>.log``) and in the command's own output.

    The command's output also relays every child line prefixed with ``[<name>]``; those lines are ignored there so that nothing is
    counted twice."""

    def __init__(self, soak, *, period=2.0):
        super().__init__(name='log-collector', daemon=True)
        self.soak, self.period = soak, period
        self._journal_at = 0.0
        self.offsets = {}
        self.counts = {}
        self.lines = {}
        self.stop_event = threading.Event()
        self._lock = threading.Lock()

    def sources(self):
        found = []
        for attempt in sorted(self.soak.claimed_attempts):
            for path in (Path(attempt) / 'logs').glob('*.log'):
                found.append((path.stem, path, False))
        for path in self.soak.log_dir.glob('fleet*.out'):
            found.append((path.stem + '-cli', path, True))
        for path in self.soak.log_dir.glob('fleet*.err'):
            found.append((path.stem + '-cli', path, True))
        if self.soak.unit is not None and self.soak.service_journal.exists():
            found.append(('unit-journal', self.soak.service_journal, True))     # relayed child lines start with '['; their files are counted
        return found

    def scan(self):
        for name, path, is_cli in self.sources():
            try:
                size = path.stat().st_size
                start = self.offsets.get(path, 0)
                if size <= start:
                    continue
                with path.open('rb') as handle:
                    handle.seek(start)
                    data = handle.read(size - start)
                # only whole lines; a partial last line is read again next time
                end = data.rfind(b'\n') + 1
                self.offsets[path] = start + end
                for raw in data[:end].splitlines():
                    text = raw.decode(errors='replace')
                    if is_cli and text.startswith('['):
                        continue
                    self._count(name, text)
            except OSError:
                continue

    def run(self):
        while not self.stop_event.is_set():
            fleet = self.soak.supervisor.fleet if self.soak.supervisor else None
            if fleet is not None:
                try:
                    fleet.record_new()
                except Exception:
                    pass
            if self.soak.unit is not None and time.monotonic() - self._journal_at > 10:
                self._journal_at = time.monotonic()
                try:
                    self.soak.unit.journal_append(self.soak.service_journal, self.soak.log_dir / 'unit-journal.cursor')
                except Exception:
                    pass
            self.scan()
            self.stop_event.wait(self.period)

    def _count(self, name, text):
        with self._lock:
            for marker in MARKERS:
                if marker in text:
                    self.counts.setdefault(name, {})[marker] = self.counts.get(name, {}).get(marker, 0) + 1
                    kept = self.lines.setdefault(name, {}).setdefault(marker, [])
                    kept.append(text[:300])
                    del kept[:-3]

    def total(self, marker, since=None):
        self.scan()
        with self._lock:
            return sum(row.get(marker, 0) for row in self.counts.values())

    def snapshot(self):
        with self._lock:
            return {'counts': {k: dict(v) for k, v in self.counts.items()}, 'lines': {k: {m: list(v) for m, v in d.items()} for k, d in self.lines.items()}}

    def stop(self):
        self.stop_event.set()
        self.join(timeout=10)
        self.scan()


# ---- the soak --------------------------------------------------------------------------------------------------------------

class HostSoak(Soak):
    def __init__(self, cfg: HostConfig, *, replicas=None):
        super().__init__(cfg, replicas=replicas)
        self.shape = cfg.shape
        self.claimed_attempts = set()
        self.last_main_pid = 0
        self.unit = None
        self.health_port = free_port()
        self.cli_metrics_port = free_port()
        self.service_journal = self.log_dir / 'unit-journal.log'
        self.stale_samples = 0
        self._stale_run = 0
        self.stale_events = []
        self.wheel_facts = None
        self.neo_internals = []

    # ---- configuration -------------------------------------------------------------------------------------------------
    def _config(self):
        return {'docker_host': self.cfg.docker_host, 'venv_bin': self.cfg.venv_bin, 'images': {},
                'wheel_sha256': self.cfg.wheel_sha256}

    def preflight(self):
        free = fs_free('/var/lib/docker')
        info = self.client.info()
        self.wheel_facts = host.verify_wheel(self.cfg.venv_bin, os.path.expanduser(self.cfg.wheel),
                                             expected_sha256=self.cfg.wheel_sha256 or None)
        facts = {'docker_root': info['DockerRootDir'], 'docker_server': info['ServerVersion'], 'docker_free_bytes': free,
                 'mem_available_bytes': read_mem_available(), 'ncpu': info['NCPU'], 'architecture': info['Architecture'],
                 'wheel': self.wheel_facts, 'shape': self.shape, 'product_docker_host': host.NO_DOCKER,
                 'broker_rebalance_delay_ms': self.cfg.rebalance_delay_ms}
        from .sampler import DISK_REFUSE
        if free < DISK_REFUSE:
            raise RuntimeError(f'host_disk: {free} bytes free is below the {DISK_REFUSE} start threshold')
        self.preflight_facts = facts
        return facts

    def check_budget(self):
        """The inequality with this shape's measured peaks (``process_peaks.json``), else the E13 container peaks as a floor."""
        path = Path(__file__).with_name('process_peaks.json')
        if not path.exists():
            path = Path(__file__).with_name('reference_peaks.json')
        ref = json.loads(path.read_text())
        peaks = {k: v for k, v in ref.items() if k not in ('cli-process', 'harness')}
        available = self.preflight_facts['mem_available_bytes']
        attempts = []
        for reduction in ({}, {'Customer': 1}, {'Customer': 1, 'PURCHASED': 1}):
            replicas = {**self.replicas, **reduction}
            after = {**replicas, 'PURCHASED': max(replicas['PURCHASED'], 3 if not reduction.get('PURCHASED') else 2)}
            fleet = budget.largest_fleet_peaks(peaks, replicas, after, cli_peak=ref['cli-process'])
            verdict = budget.evaluate(neo4j_limit=self.cfg.neo_memory, kafka_limit=self.cfg.kafka_memory, fleet=fleet,
                                      harness_peak=ref['harness'], mem_available=available)
            attempts.append({'reduction': reduction, 'ok': verdict['ok'], 'margin': verdict['margin'], 'required': verdict['required'],
                             'allowed': verdict['allowed'], 'peaks_file': path.name})
            if verdict['ok']:
                self.replicas = replicas
                return {'ok': True, 'chosen': reduction, 'attempts': attempts, 'replicas': dict(self.replicas)}
        return {'ok': False, 'attempts': attempts}

    def _new_stack(self):
        from .stack import SoakStack
        return SoakStack(self.client, self.registry, deadline=time.monotonic() + self.cfg.stack_deadline,
                         neo_memory=self.cfg.neo_memory, kafka_memory=self.cfg.kafka_memory, neo_heap=self.cfg.neo_heap,
                         neo_cache=self.cfg.neo_cache, kafka_heap=self.cfg.kafka_heap, pin_neo_port=True,
                         rebalance_delay_ms=self.cfg.rebalance_delay_ms)

    def _new_logs(self):
        return FileLogCollector(self)

    def start(self):
        super().start()
        (self.out_dir / 'jmx-probe.json').write_text(json.dumps(self.stack.jmx_probe(), indent=1, sort_keys=True, default=str))

    # ---- fleet ---------------------------------------------------------------------------------------------------------
    def make_fleet(self, incarnation):
        self.render_schema()
        if self.shape == 'service':
            if self.unit is None:
                self.unit = host.Unit(self)
            fleet = host.ServiceFleet(self.ctx, incarnation, self.schema_path, expected=self.expected())
        else:
            fleet = host.ProcessFleet(self.ctx, incarnation, self.schema_path, expected=self.expected())
        self.fleets.append(fleet)
        return fleet

    def _held_make_fleet(self, incarnation):
        while self.hold.is_set() and not (self.supervisor and self.supervisor.stop_requested.is_set()):
            time.sleep(0.5)
        return self.make_fleet(incarnation)

    def start_supervisor(self, backoff=5.0):
        self.hold = threading.Event()
        self.supervisor = Supervisor(self.ctx, self._held_make_fleet, backoff=backoff, on_event=self.on_supervisor_event)
        self.supervisor.start()
        return self.supervisor

    # ---- replay / oom ----------------------------------------------------------------------------------------------------
    def replay(self, rel):
        self.replays = getattr(self, 'replays', 0) + 1
        result = run_cli(Path(self.config['venv_bin']) / 'graph-loader',
                         ['replay-dlq', '--config', str(self.schema_path), '--edge-type', rel, '--parked'], env=host.child_env(self.stack),
                         cwd=self.run_dir, log_dir=self.log_dir, name=f'replay-{rel}-{self.replays}', timeout=900,
                         redact=[self.stack.password])
        return {'exit': result.exit_code, 'seconds': round(result.elapsed, 1), 'tail': result.tail[-400:], 'timed_out': result.timed_out}

    def oom_events(self):
        found = []
        for container in (self.stack.neo, self.stack.kafka):
            try:
                container.reload()
                if container.attrs['State']['OOMKilled']:
                    found.append(container.name)
            except Exception:
                pass
        out = subprocess.run(['journalctl', '-k', '--no-pager', '--since', f'@{int(self.started_at)}', '-g', 'Out of memory|oom-kill|oom_reaper'],
                             capture_output=True, text=True, timeout=60)
        found += [line.strip()[:200] for line in out.stdout.splitlines() if 'graph-loader' in line or 'python' in line or 'e14-' in line]
        if self.unit is not None:
            try:
                events = (Path('/sys/fs/cgroup') / self.unit.show('ControlGroup')['ControlGroup'].lstrip('/') / 'memory.events').read_text()
                oom = int(next((l.split()[1] for l in events.splitlines() if l.startswith('oom_kill')), 0))
                if oom:
                    found.append(f'unit cgroup oom_kill={oom}')
            except (OSError, RuntimeError, KeyError, ValueError):
                pass
        return found

    # ---- sampling ----------------------------------------------------------------------------------------------------------
    def providers(self):
        providers = super().providers()
        pool = self._pool

        def containers():
            rows = []
            fleet = self.supervisor.fleet if self.supervisor else None
            if fleet is not None and fleet.attempt is not None:
                for row in fleet.inspect_all():
                    row['rss'], row['cpu_ns'] = host.rss_bytes(row['pid']), host.cpu_ns(row['pid'])
                    self._peak(row)
                    rows.append(row)
                if fleet.cli.process.poll() is None:
                    row = {'component': 'cli-process', 'name': 'graph-loader', 'rss': host.rss_bytes(fleet.cli.process.pid),
                           'cpu_ns': host.cpu_ns(fleet.cli.process.pid)}
                    self._peak(row)
                    rows.append(row)
            if self.unit is not None:
                rows.append({'component': 'unit', 'name': 'unit-cgroup', 'memory_peak': self.unit.memory_peak(),
                             'processes': len(self.unit.cgroup_procs())})
            targets = [{'id': c.id, 'name': c.name, 'component': role, 'state': 'running', 'metrics_port': None}
                       for role, c in (('neo4j', self.stack.neo), ('kafka', self.stack.kafka))]

            def measure(row):
                try:
                    rss, cpu = working_set(self.client.api.stats(row['id'], stream=False))
                except Exception as error:
                    return {**row, 'error': type(error).__name__}
                return {**row, 'rss': rss, 'cpu_ns': cpu}
            for row in pool.map(measure, targets):
                if 'rss' in row:
                    self._peak(row)
                rows.append(row)
            return rows

        def fleet_integrity():
            """No second fleet: processes of an earlier attempt must be gone; the current attempt holds at most the expected count."""
            fleet = self.supervisor.fleet if self.supervisor else None
            current = str(fleet.attempt) if fleet is not None and fleet.attempt is not None else None
            stale = []
            for attempt in sorted(self.claimed_attempts):
                if attempt != current and Path(attempt, 'registry.jsonl').is_file():
                    stale += [s['name'] for s in host.alive_children(Path(attempt))]
            if stale and not (fleet is not None and fleet.cli.process.poll() is not None):
                self._stale_run += 1
            else:
                self._stale_run = 0
            self.stale_samples = max(self.stale_samples, self._stale_run)
            if self._stale_run >= 2:
                self.stale_events.append({'at': time.time(), 'stale': stale})
            return {'stale': stale, 'current_alive': sum(fleet.counts().values()) if fleet is not None and fleet.attempt is not None else 0}

        def neo4j_internals():
            row = self.stack.internals()
            self.neo_internals.append(row)
            del self.neo_internals[:-4000]
            return row

        providers['containers'] = containers
        providers['fleet_integrity'] = fleet_integrity
        providers['neo4j_internals'] = neo4j_internals
        return providers

    # ---- shutdown ----------------------------------------------------------------------------------------------------------
    def save_broker_log(self):
        """The Kafka broker's log (JoinGroup/rebalance timing) next to the run's output, capped; written before the stack goes away."""
        try:
            data = self.stack.kafka.logs(timestamps=True)[-24 * 1024 * 1024:]
            (self.out_dir / 'kafka-broker.log').write_bytes(data)
        except Exception as error:
            self.notes.append(f'broker log not saved: {type(error).__name__}')

    def close(self, keep_stack=False):
        self.save_broker_log()
        receipt = super().close(keep_stack=keep_stack)
        if self.unit is not None:
            try:
                receipt['unit'] = self.unit.cleanup()
                if receipt['unit']['cgroup_left'] or not receipt['unit']['unit_removed']:
                    receipt['ok'] = False
            except Exception as error:
                receipt.setdefault('errors', []).append(f'unit cleanup: {type(error).__name__}: {error}')
                receipt['ok'] = False
        return receipt


# ---- faults ----------------------------------------------------------------------------------------------------------------

class HostFaults(Faults):
    def sigterm_during_outage(self):
        soak = self.soak
        result = self._begin('sigterm-during-neo4j-outage')
        sup, logs = soak.supervisor, soak.logs
        generation, fleet = sup.generation, sup.fleet
        out0 = logs.total('stage=write-outage ')
        soak.stack.stop_neo4j(timeout=30)
        self._wait(lambda: logs.total('stage=write-outage ') > out0, 60, 'loaders to notice the outage')
        time.sleep(5)
        sup.expect('sigterm during a Neo4j outage', exit_codes=(0,))
        # A relaunch while Neo4j is down fails its preflight (exit 1); under systemd that would burn the start limit. The restart owner
        # (the harness, or the explicit systemctl start after an exit 0) waits until Neo4j is back.
        soak.hold.set()
        code = fleet.sigterm(timeout=180)
        receipts = fleet.exit_receipts()
        soak.stack.start_neo4j()
        soak.querylog.mark_restart()
        soak.hold.clear()
        restarted = self._restarted(generation)
        abandoned = logs.total('drain-abandoned')
        observed = {'cli_exit': code, 'child_exits': receipts, 'drain_abandoned_lines': abandoned, 'restarted': restarted,
                    'stop_seconds': fleet.cli.stop_seconds, 'restart_owner': 'systemd (explicit start after exit 0)' if soak.shape == 'service' else 'harness'}
        verdict = judge_sigterm_outage(code, {k: v for k, v in receipts.items() if v is not None})
        if not receipts:
            verdict = {'ok': False, 'problems': verdict['problems'] + ['no child exit receipt was recorded']}
        if not restarted:
            verdict = {'ok': False, 'problems': verdict['problems'] + ['the fleet did not restart']}
        return self._end(result, 'command exit 0; loaders log *-drain-abandoned reason=neo4j_outage and exit 0 (or 137 once the stop budget is spent); '
                                 'restart redelivers the held batches', observed, verdict)

    def kill9_cli(self):
        soak = self.soak
        result = self._begin('kill-9-cli')
        sup = soak.supervisor
        generation, fleet = sup.generation, sup.fleet
        before = [name for items in fleet.live().values() for _cid, name in items]
        sup.expect('kill -9 of the CLI', exit_codes=(-9, None))
        killed_at = time.monotonic()
        fleet.sigkill_cli()
        lifeline = fleet.lifeline_check(timeout=60)
        seconds = None if lifeline is None else round(time.monotonic() - killed_at - 0.0, 2)
        restarted = self._restarted(generation)
        new = sup.fleet
        recovery = next((e.get('recovery') for e in reversed(sup.events) if e['incarnation'] == fleet.incarnation), None) or {}
        observed = {'children_before': len(before), 'lifeline_seconds': seconds, 'orphans_removed_by_harness': recovery.get('leftovers', {}).get('removed'),
                    'restarted': restarted, 'restart_owner': 'systemd' if soak.shape == 'service' else 'harness',
                    'new_attempt_alive': new.counts() if restarted else None}
        problems = []
        if lifeline is None:
            problems.append('a child outlived the lifeline / the manager for 60 s')
        if recovery.get('leftovers', {}).get('removed'):
            problems.append('the harness had to remove orphans after the lifeline window')
        if not restarted:
            problems.append('the fleet did not restart')
        elif soak.shape == 'service':
            procs = len(soak.unit.cgroup_procs())
            observed['unit_cgroup_processes'] = procs
            if procs != 1 + sum(new.expected.values()):
                problems.append(f'{procs} processes in the unit; expected exactly one fleet ({1 + sum(new.expected.values())})')
        return self._end(result, 'children stop through the lifeline/manager; the restart owner starts one new fleet', observed,
                         {'ok': not problems, 'problems': problems})
