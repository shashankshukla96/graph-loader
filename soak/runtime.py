"""The running soak: owned stack, installed fleet, generator loop, sampler, guards, checker. Everything is created through here.

A ``Soak`` owns exactly the resources its registry names (``e13-<token>-`` containers, volumes, network, topics) and removes them by
recorded ID. Before it starts and after it ends it snapshots every foreign container, network, volume and image; any difference is
reported and stops the run.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import resource
import secrets
import shutil
import threading
import time

import docker

from tests.installed_service import lockwait
from tests.installed_service.live_cli import ENV_KEEP, cli_environment, run_cli
from tests.installed_service.live_stack import Registry

from . import schema as schema_mod
from . import spec
from .checker import Checker, QuietPointFailed
from .fleet import Fleet, Supervisor, expected_counts
from .generator import GenConfig, Generator
from .graph_read import Neo4jGraph
from .journal import Journal
from .kafka_io import Kafka, Publisher
from .logs import LogCollector
from .oracle import Oracle
from .querylog import QueryLogReader
from .sampler import DISK_REFUSE, DISK_STOP, Guard, Sampler, fs_free, read_mem_available, scrape, working_set
from .stack import GiB, SoakStack

IMAGE_ROLES = ('cli', 'node', 'edge', 'clock')


@dataclass
class SoakConfig:
    run_root: str = '~/e13-soak'
    manifest: str = ''
    venv_bin: str = ''
    docker_host: str = 'unix:///var/run/docker.sock'
    token: str = field(default_factory=lambda: secrets.token_hex(16))
    seed: int = 13
    cpuset: str = '4-11'
    member_wait: float = 60.0   # docs: the default session timeout is 45 s; a SIGKILLed loader's membership lasts that long
    ready_timeout: float = 300.0
    cli_deadline: float = 12 * 3600.0
    # The epic's 3 GiB / 1.5 GiB limits fail the memory-budget inequality on this host while the user's stack runs (evidence/E13.F1.S1.md);
    # these are the smallest limits with headroom over the measured peaks (Neo4j 1.6 GB, Kafka 0.5 GB at the smoke's data size).
    neo_memory: int = 1792 * 1024 ** 2
    kafka_memory: int = 1024 * 1024 ** 2
    neo_heap: str = '512m'
    neo_cache: str = '640m'
    kafka_heap: str = '512m'
    replicas: dict = field(default_factory=dict)
    gen: GenConfig = field(default_factory=GenConfig)
    catchup_bound: float = 900.0
    stack_deadline: float = 12 * 3600.0


class Ctx:
    """What ``Fleet`` and ``Supervisor`` read."""

    def __init__(self, soak):
        self.soak = soak

    def __getattr__(self, name):
        return getattr(self.soak, name)


def foreign_snapshot(client, token) -> dict:
    """Every container, network, volume and image that is not this run's: identity and state, for a before/after comparison."""
    def owned(labels, name=''):
        return (labels or {}).get('io.graph-loader.e09-owner') == token or f'{spec.NAMESPACE}-{token}' in name
    containers = {}
    for row in client.api.containers(all=True):
        if owned(row.get('Labels'), ' '.join(row.get('Names', []))):
            continue
        containers[row['Id']] = {'names': sorted(row.get('Names', [])), 'image': row['ImageID'], 'state': row['State'],
                                 'created': row['Created']}
    networks = {n['Id']: n['Name'] for n in client.api.networks() if not owned(n.get('Labels'), n['Name'])}
    volumes = {v['Name']: v.get('CreatedAt') for v in (client.api.volumes().get('Volumes') or []) if not owned(v.get('Labels'), v['Name'])}
    images = {i['Id']: sorted(i.get('RepoTags') or []) for i in client.api.images()}
    return {'containers': containers, 'networks': networks, 'volumes': volumes, 'images': images}


def diff_foreign(before: dict, after: dict) -> dict:
    changes = {}
    for kind in ('containers', 'networks', 'volumes', 'images'):
        b, a = before[kind], after[kind]
        added, removed = sorted(set(a) - set(b)), sorted(set(b) - set(a))
        changed = sorted(k for k in set(a) & set(b) if a[k] != b[k])
        if added or removed or changed:
            changes[kind] = {'added': added, 'removed': removed, 'changed': changed}
    return changes


class GeneratorLoop(threading.Thread):
    """Drives the generator at ``rate`` logical operations per second (times ``multiplier``) and publishes every record."""

    def __init__(self, soak, rate):
        super().__init__(name='generator', daemon=True)
        self.soak, self.rate, self.multiplier = soak, rate, 1.0
        self.running = threading.Event()
        self.idle = threading.Event()
        self.idle.set()
        self.stop_event = threading.Event()
        self.carry = 0.0
        self.sent = 0
        self.errors = 0
        self.t0 = time.monotonic()

    def resume(self):
        self.idle.clear()
        self.running.set()

    def pause(self, timeout=60):
        self.running.clear()
        if not self.idle.wait(timeout):
            raise TimeoutError('the generator did not pause')

    def run(self):
        soak = self.soak
        while not self.stop_event.is_set():
            if not self.running.is_set():
                self.idle.set()
                self.running.wait(0.2)
                continue
            self.idle.clear()
            started = time.monotonic()
            self.carry += self.rate * self.multiplier
            count, self.carry = int(self.carry), self.carry - int(self.carry)
            try:
                records = soak.gen.step(started - self.t0, count, soak.window)
                for rec in records:
                    soak.publisher.send(rec, window=soak.window)
                    self.sent += 1
                soak.publisher.poll(0)
            except Exception:
                self.errors += 1
            self.stop_event.wait(max(1.0 - (time.monotonic() - started), 0.0))
        self.idle.set()


class Soak:
    def __init__(self, cfg: SoakConfig, *, replicas=None):
        self.cfg = cfg
        self.token = cfg.token
        self.base = Path(os.path.expanduser(cfg.run_root)) / cfg.token
        self.run_dir = self.base / 'run'
        self.log_dir = self.base / 'logs'
        self.out_dir = self.base / 'out'
        for d in (self.run_dir, self.log_dir, self.out_dir):
            d.mkdir(parents=True, exist_ok=True)
        self.run_id = f'{spec.NAMESPACE}{cfg.token[:12]}'
        self.cpuset, self.member_wait, self.ready_timeout = cfg.cpuset, cfg.member_wait, cfg.ready_timeout
        self.cli_deadline = cfg.cli_deadline
        self.replicas = {**{n.label: n.replicas for n in spec.NODES.values()}, **{r.type: r.replicas for r in spec.RELS.values()},
                         **(replicas or cfg.replicas)}
        self.window = 0
        self.notes = []
        self.config = self._config()
        self.ctx = Ctx(self)
        self.client = docker.DockerClient(base_url=cfg.docker_host, timeout=60)
        self.registry = Registry(self.base / 'registry.jsonl', cfg.token)
        self.cli_metrics_port = None
        self.stack = None
        self.supervisor = None
        self.peaks = {}
        self.container_log = {}
        self.controls = []
        self.started_at = time.time()
        self.tripped = None
        self.harness_peak = 0
        self.fault_log = []
        self.fleets = []
        self.steps = []
        self.qp_log = []
        self.controlled = set()
        self.loop = None

    # ---- configuration --------------------------------------------------------------------------------------------------
    def _config(self):
        manifest = json.loads(Path(os.path.expanduser(self.cfg.manifest)).read_text())
        rows = manifest['images']['amd64']['rows']
        return {'docker_host': self.cfg.docker_host, 'venv_bin': self.cfg.venv_bin,
                'images': {role: {'id': rows[role]['id'], 'tag': rows[role]['local_tag']} for role in IMAGE_ROLES},
                'wheel_sha256': manifest['package']['artifacts']['neo4j_graph_loader-0.1.0a1-py3-none-any.whl']['sha256']}

    # ---- bring-up -------------------------------------------------------------------------------------------------------
    def preflight(self):
        free = fs_free('/var/lib/docker')
        info = self.client.info()
        facts = {'docker_root': info['DockerRootDir'], 'docker_server': info['ServerVersion'], 'docker_free_bytes': free,
                 'mem_available_bytes': read_mem_available(), 'ncpu': info['NCPU'], 'architecture': info['Architecture']}
        if free < DISK_REFUSE:
            raise RuntimeError(f'host_disk: {free} bytes free is below the {DISK_REFUSE} start threshold')
        for role, row in self.config['images'].items():
            image = self.client.images.get(row['id'])
            if image.id != row['id']:
                raise RuntimeError(f'image {role} id differs from the frozen manifest')
        facts['images'] = {role: row['id'] for role, row in self.config['images'].items()}
        self.preflight_facts = facts
        return facts

    def check_budget(self):
        """The memory-budget inequality for the largest fleet, from the S1-measured peaks and this host's MemAvailable right now.

        Evaluated before any load. If it fails, replicas are reduced (Customer first) and it is evaluated again; the outcome, and any
        reduction, is recorded. Returns the evaluation; ``ok`` False means no admissible plan."""
        from . import budget
        # reference_peaks.json = max(S1 smoke peaks, peaks measured at the calibrated base rate on the baseline graph after its re-read)
        s1 = json.loads(Path(__file__).with_name('reference_peaks.json').read_text())
        peaks = {k: v for k, v in s1.items() if k not in ('cli-process', 'harness')}
        available = self.preflight_facts['mem_available_bytes']
        attempts = []
        for reduction in ({}, {'Customer': 1}, {'Customer': 1, 'PURCHASED': 1}):
            replicas = {**self.replicas, **reduction}
            after = {**replicas, 'PURCHASED': max(replicas['PURCHASED'], 3 if not reduction.get('PURCHASED') else 2)}
            fleet = budget.largest_fleet_peaks(peaks, replicas, after, cli_peak=s1['cli-process'])
            verdict = budget.evaluate(neo4j_limit=self.cfg.neo_memory, kafka_limit=self.cfg.kafka_memory, fleet=fleet,
                                      harness_peak=s1['harness'], mem_available=available)
            attempts.append({'reduction': reduction, 'ok': verdict['ok'], 'margin': verdict['margin'], 'required': verdict['required'],
                             'allowed': verdict['allowed']})
            if verdict['ok']:
                self.replicas = replicas
                return {'ok': True, 'chosen': reduction, 'attempts': attempts, 'replicas': dict(self.replicas)}
        return {'ok': False, 'attempts': attempts}

    def start(self):
        self.foreign_before = foreign_snapshot(self.client, self.token)
        (self.out_dir / 'foreign-before.json').write_text(json.dumps(self.foreign_before, indent=1, sort_keys=True))
        self.guard = Guard(self.trip, disk=lambda: fs_free('/var/lib/docker'))
        self.guard.start()
        self.stack = self._new_stack()
        self.stack.start()
        self.facts = self.stack.facts()
        topics = {topic: partitions for topic, (partitions, _, _) in spec.source_topics(self.token).items()}
        self.stack.create_topics(topics)
        self.journal = Journal(self.base / 'journal.db')
        self.oracle = Oracle(self.journal)
        self.gen = Generator(self.journal, self.token, self.cfg.gen)
        self.kafka = Kafka(self.stack.bootstrap, probe_group=f'{spec.NAMESPACE}-{self.token}-probe')
        self.publisher = Publisher(self.stack.bootstrap, self.journal)
        self.graph = Neo4jGraph(lambda: self.stack.driver, self.stack.database)
        self.group_id = f'{spec.NAMESPACE}-{self.token}'
        self.querylog = QueryLogReader(self.stack.exec_log, self.stack.marker)
        self.checker = Checker(token=self.token, group_id=self.group_id, journal=self.journal, oracle=self.oracle, graph=self.graph,
                               kafka=self.kafka, prepare=self.prepare, replay=self.replay, rejected_locations=self.rejected_locations,
                               catchup_bound=self.cfg.catchup_bound)
        self.render_schema()
        self.new_window('start')
        self.logs = self._new_logs()
        self.logs.start()
        self.sampler = Sampler(self.out_dir / 'samples.jsonl', self.providers())
        self.sampler.start()

    def _new_stack(self):
        return SoakStack(self.client, self.registry, deadline=time.monotonic() + self.cfg.stack_deadline,
                         neo_memory=self.cfg.neo_memory, kafka_memory=self.cfg.kafka_memory, neo_heap=self.cfg.neo_heap,
                         neo_cache=self.cfg.neo_cache, kafka_heap=self.cfg.kafka_heap)

    def _new_logs(self):
        return LogCollector(self.client, self.log_dir / 'containers', lambda: self.supervisor.fleet if self.supervisor else None)

    def render_schema(self):
        self.schema_text = schema_mod.render(self.token, replicas=self.replicas)
        self.schema_path = self.run_dir / 'schema.yaml'
        self.schema_path.write_text(self.schema_text)
        return self.schema_path

    def expected(self):
        return expected_counts(self.replicas, list(spec.NODES), list(spec.RELS))

    def make_fleet(self, incarnation):
        self.render_schema()
        fleet = Fleet(self.ctx, incarnation, self.schema_path, expected=self.expected())
        self.fleets.append(fleet)
        return fleet

    def cpuset_all(self):
        merged = {}
        for fleet in self.fleets:
            merged.update(fleet.cpuset_seen)
        return merged

    def start_supervisor(self, backoff=5.0):
        import socket
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            self.cli_metrics_port = probe.getsockname()[1]
        self.supervisor = Supervisor(self.ctx, self.make_fleet, backoff=backoff, on_event=self.on_supervisor_event)
        self.supervisor.start()
        return self.supervisor

    def on_supervisor_event(self, event):
        self.journal.flag_window(self.window, f"fleet exit {event.get('exit_code')} ({event.get('cause') or 'unexplained'})")

    def wait_ready(self, generation_at_least=1, timeout=420):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if self.supervisor.failed:
                raise RuntimeError('supervisor failed: ' + self.supervisor.failed)
            if self.supervisor.ready.is_set() and self.supervisor.generation >= generation_at_least:
                return self.supervisor.fleet
            time.sleep(0.5)
        raise TimeoutError(f'fleet generation {generation_at_least} not ready in {timeout}s')

    # ---- windows, guards -------------------------------------------------------------------------------------------------
    def new_window(self, reason=''):
        self.window += 1
        self.journal.open_window(self.window, True, reason)
        self.journal.set_meta('current_window', self.window)
        if getattr(self, 'gen', None) is not None:
            self.gen.window = self.window   # records the generator makes outside step() (cohorts) carry the window they were made in
        return self.window

    def trip(self, verdict):
        self.tripped = verdict
        self.notes.append(f"guard tripped: {verdict}")
        try:
            if getattr(self, 'loop', None):
                self.loop.running.clear()
        except Exception:
            pass

    # ---- traffic ---------------------------------------------------------------------------------------------------------
    def make_loop(self, rate):
        """The generator thread, parked until ``resume_traffic``."""
        self.loop = GeneratorLoop(self, rate)
        self.loop.start()
        return self.loop

    def start_traffic(self, rate):
        self.make_loop(rate)
        self.loop.resume()
        return self.loop

    def ensure_control(self):
        """Run the positive control once per fleet incarnation (the detector must prove it saw the log in every one)."""
        generation = self.supervisor.generation
        if generation not in self.controlled:
            self.controlled.add(generation)
            return self.positive_control(generation)

    def prepare(self):
        """Called with the generator paused: send every deferred endpoint, then flush."""
        for rec in self.gen.drain_deferred():
            self.publisher.send(rec, window=self.window)
        return self.publisher.flush(90)

    def pause_traffic(self):
        self.loop.pause()

    def resume_traffic(self):
        self.loop.resume()

    # ---- replay ----------------------------------------------------------------------------------------------------------
    def replay(self, rel):
        self.replays = getattr(self, 'replays', 0) + 1
        env = cli_environment(self.stack, self.config['images'], self.config['docker_host'])
        result = run_cli(Path(self.config['venv_bin']) / 'graph-loader',
                         ['replay-dlq', '--config', str(self.schema_path), '--edge-type', rel, '--parked'], env=env, cwd=self.run_dir,
                         log_dir=self.log_dir, name=f'replay-{rel}-{self.replays}', timeout=900, redact=[self.stack.password])
        return {'exit': result.exit_code, 'seconds': round(result.elapsed, 1), 'tail': result.tail[-400:], 'timed_out': result.timed_out}

    def rejected_locations(self):
        """Where the node loaders rejected records: (topic, partition, offset) from every incarnation's rejection files."""
        found = set()
        for path in (self.run_dir / 'runs').glob('*/rejections/*.jsonl'):
            for line in path.read_text().splitlines():
                if line.strip():
                    row = json.loads(line)
                    found.add((row['topic'], row['partition'], row['offset']))
        return found

    # ---- positive control ------------------------------------------------------------------------------------------------
    def positive_control(self, incarnation, timeout=90):
        """A synchronised lock conflict; the query-log reader must report its wait. Proves coverage for this incarnation."""
        query = lockwait.lock_conflict(self.stack, 7000 + incarnation)
        began = time.monotonic()
        while time.monotonic() - began < timeout:
            self.querylog.poll(time.monotonic())
            if self.querylog.control_seen(query):
                row = {'incarnation': incarnation, 'detected': True, 'seconds': round(time.monotonic() - began, 1)}
                break
            time.sleep(1)
        else:
            row = {'incarnation': incarnation, 'detected': False, 'seconds': timeout}
            self.querylog.coverage.append(f'positive control of incarnation {incarnation} was not detected')
        self.controls.append(row)
        return row

    # ---- lag -------------------------------------------------------------------------------------------------------------
    def committed_all(self):
        out = {}
        for group, tps in self.checker.groups().items():
            for tp, value in self.kafka.committed(group, tps).items():
                out[(group, *tp)] = value
        return out

    def oom_events(self):
        """Owned containers (stack and fleets) the kernel OOM-killed, from the last inspection of each."""
        found = []
        for container in (self.stack.neo, self.stack.kafka):
            try:
                container.reload()
                if container.attrs['State']['OOMKilled']:
                    found.append(container.name)
            except Exception:
                pass
        for fleet in self.fleets:
            found += [event['id'][:12] for event in fleet.observer.snapshot()['events'] if event.get('action') == 'oom']
        return found

    def dlq_backlog(self):
        tps = [(dlq, p) for dlq, parked, n in self.checker.dlqs.values() for p in range(n)]
        committed = self.kafka.committed(f'{self.group_id}-retry-consumer', tps)
        out = {}
        for rel, (dlq, parked, partitions) in self.checker.dlqs.items():
            out[rel] = {'dlq': sum(max(self.kafka.watermarks(dlq, p)[1] - (committed[(dlq, p)] or 0), 0) for p in range(partitions)),
                        'parked': sum(self.kafka.watermarks(parked, p)[1] for p in range(partitions))}
        return out

    def total_lag(self):
        total = 0
        for group, tps in self.checker.groups().items():
            committed = self.kafka.committed(group, tps)
            for tp in tps:
                low, high = self.kafka.watermarks(*tp)
                total += max(high - (committed[tp] if committed[tp] is not None else low), 0)
        return total

    # ---- sampling providers ----------------------------------------------------------------------------------------------
    def providers(self):
        pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix='stats')
        self._pool = pool

        def host():
            return {'mem_available': read_mem_available(), 'docker_free': fs_free('/var/lib/docker'),
                    'harness_rss': self._rss(os.getpid())}

        def containers():
            rows = []
            fleet = self.supervisor.fleet if self.supervisor else None
            targets = []
            if fleet is not None:
                for row in fleet.inspect_all():
                    targets.append(row)
            for role, container in (('neo4j', self.stack.neo), ('kafka', self.stack.kafka)):
                targets.append({'id': container.id, 'name': container.name, 'component': role, 'state': 'running', 'metrics_port': None})

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
            if fleet is not None and fleet.cli.process.poll() is None:
                rows.append({'component': 'cli-process', 'name': 'graph-loader', 'rss': self._rss(fleet.cli.process.pid)})
                self._peak(rows[-1])
            return rows

        def kafka_state():
            lag, backlog = {}, {}
            for group, tps in self.checker.groups().items():
                committed = self.kafka.committed(group, tps)
                lag[group] = sum(max(self.kafka.watermarks(*tp)[1] - (committed[tp] or self.kafka.watermarks(*tp)[0]), 0) for tp in tps)
            tps = [(dlq, p) for dlq, parked, n in self.checker.dlqs.values() for p in range(n)]
            committed = self.kafka.committed(f'{self.group_id}-retry-consumer', tps)
            for rel, (dlq, parked, partitions) in self.checker.dlqs.items():
                backlog[rel] = {'dlq': sum(max(self.kafka.watermarks(dlq, p)[1] - (committed[(dlq, p)] or 0), 0) for p in range(partitions)),
                                'parked': sum(self.kafka.watermarks(parked, p)[1] for p in range(partitions))}
            return {'lag': lag, 'backlog': backlog}

        def outage():
            fleet = self.supervisor.fleet if self.supervisor else None
            gauges = {}
            if fleet is None:
                return gauges
            for row in fleet.inspect_all():
                if row['metrics_port'] and row['component'] in ('edge-loader', 'node-loader'):
                    try:
                        for name, labels, value in scrape(row['metrics_port']):
                            if name == 'graph_loader_write_outage_seconds':
                                gauges[row['name']] = max(gauges.get(row['name'], 0.0), value)
                    except Exception:
                        pass
            return {'max': max(gauges.values(), default=0.0), 'containers': len(gauges)}

        def querylog():
            self.querylog.poll(time.monotonic())
            return self.querylog.summary()

        def journal():
            return {'sends': self.journal.counts(), 'window': self.window, 'generated': dict(self.gen.stats)}

        return {'host': host, 'containers': containers, 'kafka': kafka_state, 'write_outage': outage, 'querylog': querylog, 'journal': journal}

    @staticmethod
    def _rss(pid):
        try:
            for line in Path(f'/proc/{pid}/status').read_text().splitlines():
                if line.startswith('VmRSS:'):
                    return int(line.split()[1]) * 1024
        except OSError:
            return 0
        return 0

    def _peak(self, row):
        key = row['component']
        entity = ''
        for name in [*spec.NODES, *spec.RELS]:
            if name in row.get('name', ''):
                entity = name
        if row['component'] in ('node-loader', 'edge-loader') and entity:
            key = f"{row['component']}:{entity}"
        self.peaks[key] = max(self.peaks.get(key, 0), row['rss'])
        record = self.container_log.setdefault(row.get('id') or row['name'], {'name': row['name'], 'key': key, 'peak': 0, 'first': time.time(),
                                                                              'series': []})
        record['peak'] = max(record['peak'], row['rss'])
        record['last'] = time.time()
        if not record['series'] or time.time() - record['series'][-1][0] >= 60:
            record['series'].append((round(time.time()), row['rss']))
        self.harness_peak = max(self.harness_peak, resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024)

    # ---- shutdown --------------------------------------------------------------------------------------------------------
    def close(self, keep_stack=False):
        receipt = {'errors': []}
        try:
            if getattr(self, 'loop', None):
                self.loop.stop_event.set()
                self.loop.join(timeout=30)
            if self.supervisor is not None:
                receipt['fleet'] = self.supervisor.shutdown()
            if getattr(self, 'publisher', None):
                self.publisher.flush(30)
        except Exception as error:
            receipt['errors'].append(f'fleet shutdown: {type(error).__name__}: {error}')
        for name in ('sampler', 'logs', 'guard'):
            thing = getattr(self, name, None)
            if thing is not None:
                try:
                    thing.stop()
                except Exception as error:
                    receipt['errors'].append(f'{name}: {type(error).__name__}')
        try:
            if getattr(self, 'kafka', None):
                self.kafka.close()
            if self.stack is not None and keep_stack:
                receipt['stack'] = 'kept for diagnosis: python -m soak.cleanup <run dir> --execute removes it by recorded ID'
            elif self.stack is not None:
                receipt['stack'] = self.stack.close()
                self.registry.write('close', **receipt['stack'])
        except Exception as error:
            receipt['errors'].append(f'stack close: {type(error).__name__}: {error}')
        after = foreign_snapshot(self.client, self.token)
        (self.out_dir / 'foreign-after.json').write_text(json.dumps(after, indent=1, sort_keys=True))
        receipt['foreign_changes'] = diff_foreign(self.foreign_before, after)
        owned_left = {
            'containers': [c['Id'] for c in self.client.api.containers(all=True, filters={'label': f'io.graph-loader.e09-owner={self.token}'})],
            'volumes': [v['Name'] for v in (self.client.api.volumes().get('Volumes') or []) if (v.get('Labels') or {}).get('io.graph-loader.e09-owner') == self.token],
            'networks': [n['Id'] for n in self.client.api.networks() if (n.get('Labels') or {}).get('io.graph-loader.e09-owner') == self.token],
        }
        receipt['owned_left'] = owned_left
        receipt['ok'] = (not any(owned_left.values()) or keep_stack) and not receipt['foreign_changes'] and not receipt['errors']
        if getattr(self, '_pool', None):
            self._pool.shutdown(wait=False)
        return receipt
