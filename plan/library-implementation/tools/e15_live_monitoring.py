#!/usr/bin/env python3
"""E15.F2.S12 cell B: an owned external Prometheus against live graph-loader runs (runs on the Docker host, after ``e15_stack.py up``).

Phases (``--phases a,b,...``; state is kept in WORKDIR/monitoring/):

``file``    file discovery, process runtime: a stream run (aggregated endpoint on 19600 as well) is scraped through the exported ``file_sd``
            job; a healthy run fires nothing; every sample carries run_id / execution_id / exporter / runtime.
``dup``     the same run is scraped BOTH ways (file_sd and the aggregated endpoint): ``GraphLoaderDuplicateScrape`` must fire.
``static``  only the aggregated endpoint (single-container shape): the duplicate resolves, ``graph_loader_exporter_up`` is 1 for every child.
``hang``    a child is stopped (SIGSTOP): its scrape through the aggregate fails -> ``GraphLoaderExporterScrapeFailed`` fires; SIGCONT resolves it.
``gone``    the run ends: the aggregate target is down -> ``GraphLoaderTargetDown`` fires.
``outage``  a second run with a custom low ``GraphLoaderWriteOutage`` threshold (from ``--values``) while Neo4j is stopped -> the alert fires and resolves;
            a malformed record is routed to the DLQ (``GraphLoaderDlqRoutingDetected``).
``docker``  Docker discovery: a Docker-runtime run scraped by a Prometheus on the owned network with the Docker socket mounted read-only and the
            server-side filters ``network=<owned network>`` and ``label=graph_loader.kind`` (the rendered config is saved).

Every Prometheus container is recorded in the stack registry (removed by ID by ``e15_stack.py down``), owned by the stack token, memory-capped, and
listens on 127.0.0.1 only. Nothing here prints a password.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path

import docker
import yaml

PROM_IMAGE = os.environ.get('E15_PROM_IMAGE', 'prom/prometheus:v2.55.1')
PROM_PORT, AGGREGATE_PORT = 19090, 19600
SOCKET = 'unix:///var/run/docker.sock'


def log(message):
    print(f'{time.strftime("%H:%M:%S")} {message}', flush=True)


class Env:
    def __init__(self, workdir: Path, tree: Path):
        self.workdir, self.tree = workdir, tree
        self.base = workdir / 'monitoring'
        self.base.mkdir(exist_ok=True)
        sys.path.insert(0, str(tree / 'plan' / 'library-implementation' / 'tools'))
        import e15_live_matrix as matrix
        self.matrix = matrix
        self.lab = matrix.Lab(workdir, tree)
        self.token = self.lab.token
        self.client = docker.DockerClient(base_url=SOCKET)
        self.results = self.base / 'results.jsonl'
        self.prom = None
        self.exe = str(Path(sys.executable).parent / 'graph-loader')

    # -- recording
    def record(self, phase, ok, **facts):
        with open(self.results, 'a') as handle:
            handle.write(json.dumps({'phase': phase, 'ok': ok, **facts}, sort_keys=True, default=str) + '\n')
        log(f"{'OK  ' if ok else 'FAIL'} {phase} {json.dumps(facts, default=str)[:400]}")

    def registry(self, resource, name, ident):
        with open(self.workdir / 'registry.jsonl', 'a') as handle:
            handle.write(json.dumps({'kind': 'created', 'owner': self.token, 'at': time.time(), 'resource': resource, 'name': name, 'id': ident}) + '\n')

    # -- Prometheus
    def start_prometheus(self, config_dir: Path, *, network=None, mounts=None, port=PROM_PORT):
        name = f'e15-{self.token}-prom'
        volumes = {str(config_dir): {'bind': '/etc/prometheus', 'mode': 'ro'}}
        volumes.update(mounts or {})
        command = ['--config.file=/etc/prometheus/prometheus.yml', f'--web.listen-address=127.0.0.1:{port}', '--storage.tsdb.path=/prometheus',
                   '--storage.tsdb.retention.time=2h', '--web.enable-lifecycle']
        kwargs = {'network_mode': 'host'} if network is None else {'network': network, 'ports': {f'{port}/tcp': ('127.0.0.1', port)}}
        if network is not None:
            command[1] = f'--web.listen-address=0.0.0.0:{port}'
        container = self.client.containers.create(
            PROM_IMAGE, name=name, command=command, labels={'io.graph-loader.e15-owner': self.token, 'role': 'prometheus'}, volumes=volumes,
            tmpfs={'/prometheus': f'rw,size=512m,uid={os.getuid()},gid={os.getgid()}'}, mem_limit='384m',
            user=f'{os.getuid()}:{os.getgid()}', **kwargs)       # the run directories are private (0700): same uid reads them
        self.registry('container', name, container.id)
        container.start()
        self.prom = container
        for _ in range(60):
            try:
                self.api('/-/ready', raw=True)
                return container
            except Exception:
                time.sleep(1)
        raise RuntimeError('Prometheus did not become ready')

    def attach_prometheus(self):
        """The Prometheus container of an earlier phase (same work directory), or None."""
        found = self.client.containers.list(all=True, filters={'name': f'e15-{self.token}-prom'})
        self.prom = found[0] if found else None
        return self.prom

    def ensure_prometheus(self, config_dir: Path, **kwargs):
        if self.attach_prometheus() is None:
            return self.start_prometheus(config_dir, **kwargs)
        return self.prom

    def stop_prometheus(self):
        if self.prom is not None:
            self.prom.remove(force=True, v=True)
            self.prom = None

    def api(self, path, raw=False, **params):
        url = f'http://127.0.0.1:{PROM_PORT}{path}' + ('?' + urllib.parse.urlencode(params) if params else '')
        with urllib.request.urlopen(url, timeout=10) as response:
            body = response.read().decode()
        return body if raw else json.loads(body)['data']

    def query(self, expression):
        return self.api('/api/v1/query', query=expression)['result']

    def reload(self):
        self.prom.kill(signal='HUP')
        time.sleep(3)

    def firing(self):
        return sorted({a['labels']['alertname'] for a in self.api('/api/v1/alerts')['alerts'] if a['state'] == 'firing'})

    def active(self):
        return sorted({(a['labels']['alertname'], a['state']) for a in self.api('/api/v1/alerts')['alerts']})

    def wait_for(self, what, predicate, timeout, step=5):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            try:
                value = predicate()
            except Exception:
                value = None
            if value:
                return value
            time.sleep(step)
        raise AssertionError(f'timed out waiting for {what}')

    # -- exports
    def export(self, name, *args):
        out = self.base / name
        if out.exists():
            subprocess.run(['rm', '-rf', str(out)], check=True)
        subprocess.run([self.exe, 'monitoring', 'export', '--out', str(out), *args], check=True, capture_output=True)
        return out

    def merged_config(self, jobs: list[tuple[Path, str]], target_dir: Path):
        """prometheus.yml with the scrape jobs of several exports and the rules of the first; alerts copied beside it."""
        first = yaml.safe_load((jobs[0][0] / 'prometheus' / 'prometheus.yml').read_text())
        scrape = []
        for export, _ in jobs:
            scrape += yaml.safe_load((export / 'prometheus' / 'scrape-config.yml').read_text())['scrape_configs']
        first['scrape_configs'] = scrape
        target_dir.mkdir(parents=True, exist_ok=True)
        (target_dir / 'alerts').mkdir(exist_ok=True)
        for f in (jobs[0][0] / 'prometheus' / 'alerts').glob('*.yml'):
            (target_dir / 'alerts' / f.name).write_text(f.read_text())
        text = yaml.safe_dump(first, sort_keys=False)
        (target_dir / 'prometheus.yml').write_text(text)
        os.chmod(target_dir, 0o755)
        for p in [target_dir / 'prometheus.yml', *(target_dir / 'alerts').glob('*.yml')]:
            os.chmod(p, 0o644)
        return text

    # -- runs
    def schema_text(self, prefix, topics, group):
        return f'''loading:
  consumer_group_id: {group}
  unwind_batch_size: 50
  flush_interval_ms: 100
nodes:
  - label: Worker
    topic: {topics['worker']}
    key_property: id
    properties:
      id: {{type: integer, required: true, constraint: {{type: uniqueness}}}}
      name: {{type: string, required: true}}
  - label: Employer
    topic: {topics['employer']}
    key_property: id
    properties:
      id: {{type: string, required: true, constraint: {{type: uniqueness}}}}
      name: {{type: string, required: true}}
edges:
  - type: EMPLOYED_BY
    topic: {topics['edge']}
    nodes: {{source: Worker, target: Employer}}
    source_key_property: id
    target_key_property: id
    retry: {{base_delay_seconds: 1.0, max_delay_seconds: 2.0}}
    dead_letter: {{enabled: true, max_retry_attempts: 3}}
    properties:
      role: {{type: string}}
      since: {{type: integer, required: true}}
'''

    def start_run(self, name, runtime, *, metrics_port=None, extra=()):
        directory = self.base / 'runs' / name
        directory.mkdir(parents=True)
        prefix = f'e15.{self.token}.mon.{name}.{int(time.time())}'       # unique per invocation: topics outlive a failed attempt
        topics = {'worker': f'{prefix}.worker', 'employer': f'{prefix}.employer', 'edge': f'{prefix}.edge'}
        self.lab.create_topics(list(topics.values()))
        self.lab.clear_graph()
        schema = directory / 'schema.yaml'
        schema.write_text(self.schema_text(prefix, topics, f'e15-{self.token}-mon-{name}-{int(time.time())}'))
        cell = self.matrix.Cell('stream', runtime, 'success', 'json')
        env = self.lab.environment(cell)
        args = ['--log-format', 'json', 'start', '--mode', 'stream', '--config', str(schema), '--run-dir', str(directory / 'runs'),
                '--runtime', runtime, *extra]
        if runtime == 'docker':
            args += ['--network', self.lab.facts['network'], '--skip-image-build']
        if metrics_port:
            args += ['--metrics-port', str(metrics_port)]
        out, err = open(directory / 'stdout.log', 'wb'), open(directory / 'stderr.log', 'wb')
        process = subprocess.Popen([self.exe, *args], cwd=directory, env=env, stdout=out, stderr=err, start_new_session=True)
        self.lab.produce_nodes(topics)
        return process, topics, directory

    def attempt_of(self, directory: Path):
        return self.matrix.attempt_dir(directory / 'runs')

    def loaded(self, relationships=None):
        nodes, rels = self.lab.counts()
        return nodes == {'Worker': self.matrix.WORKERS, 'Employer': self.matrix.EMPLOYERS} and (relationships is None or rels == relationships)

    def link_targets(self, attempt: Path, name: str):
        end = time.monotonic() + 60
        while not (attempt / 'metrics' / 'targets.json').exists() and time.monotonic() < end:
            time.sleep(0.5)
        link = self.base / 'targets' / f'{name}.json'
        if not link.is_symlink():
            link.symlink_to(attempt / 'metrics' / 'targets.json')

    def node_loader_pid(self, attempt: Path):
        from neo4j_graph_loader.orchestrator.process_service import registered_children
        from neo4j_graph_loader.orchestrator.run_registry import RunRegistry, fold
        names = {s['id'] for s in fold(RunRegistry.read(attempt / 'registry.jsonl')).values()
                 if not s.get('external') and (s.get('labels') or {}).get('component') == 'node-loader'}
        live = [s for s in registered_children(attempt / 'registry.jsonl') if s['id'] in names]
        return live[0] if live else None


class Feeder(threading.Thread):
    """A few records every two seconds on every topic of a running stream: new employers, workers and worker->employer relationships."""

    def __init__(self, env: Env, topics: dict):
        super().__init__(daemon=True)
        self.env, self.topics, self.done, self.rounds = env, topics, threading.Event(), 0

    def stop(self):
        self.done.set()
        self.join(timeout=30)

    def run(self):
        lab = self.env.lab
        base_worker = 1_000_000
        while not self.done.wait(2.0):
            n = self.rounds
            self.rounds += 1
            try:
                employer = f'f{n}'
                workers = [base_worker + n * 3 + i for i in range(3)]
                lab.produce(self.topics['employer'], [(employer, {'id': employer, 'name': f'feed-{n}'})])
                lab.produce(self.topics['worker'], [(w, {'id': w, 'name': f'feed-{w}'}) for w in workers])
                time.sleep(1.0)
                lab.produce(self.topics['edge'], [(f'{w}-{employer}', {'source': {'id': w}, 'target': {'id': employer}, 'properties': {'role': 'feed', 'since': 2000}})
                                                  for w in workers])
            except Exception:                                           # noqa: BLE001 - a feeding hiccup must not end the phase
                continue


def phase_file_dup_static_hang_gone(env: Env):
    base = env.base
    # Prometheus allows a wildcard only in the last path element, so each run's targets.json is linked into one flat directory
    (base / 'targets').mkdir(exist_ok=True)
    glob_path = str(base / 'targets' / '*.json')
    values = base / 'values.yaml'
    values.write_text('labels: {team: e15}\nalerts:\n  GraphLoaderWriteOutage: {threshold: 20, for: 30s}\n')
    file_export = env.export('export-file', '--discovery', 'file', '--targets-file', glob_path, '--scrape-interval', '5s',
                             '--values', str(values), '--job-name', 'graph-loader')
    static_export = env.export('export-static', '--discovery', 'static', '--metrics-port', str(AGGREGATE_PORT), '--scrape-interval', '5s',
                               '--values', str(values), '--job-name', 'graph-loader-aggregate')
    config_dir = base / 'prom-config'
    env.merged_config([(file_export, 'file')], config_dir)
    (base / 'runs').mkdir(exist_ok=True)
    env.start_prometheus(config_dir, mounts={str(base / 'runs'): {'bind': str(base / 'runs'), 'mode': 'ro'},
                                             str(base / 'targets'): {'bind': str(base / 'targets'), 'mode': 'ro'}})
    log('Prometheus up (file job only)')

    process, topics, directory = env.start_run('r1', 'process', metrics_port=AGGREGATE_PORT)
    attempt = env.attempt_of(directory)
    env.link_targets(attempt, 'r1')
    env.wait_for('the nodes to load', env.loaded, 240)
    env.lab.produce_edges(topics)
    env.wait_for('the relationships to load', lambda: env.loaded(2 * env.matrix.WORKERS), 240)
    env.wait_for('file_sd targets scraped', lambda: len([r for r in env.query('up{job="graph-loader"}') if r['value'][1] == '1']) >= 3, 120)

    # 1. healthy, file discovery. Approved change of PROCEDURE (not of the pass criterion): a healthy stream keeps receiving traffic, so a slow
    # producer feeds every topic (new employers, workers and their relationships) throughout the observation window.
    feeder = Feeder(env, topics)
    feeder.start()
    time.sleep(200)
    targets = env.query('up{job="graph-loader"}')
    process_up = env.query('graph_loader_process_up')
    identity = all(all(k in r['metric'] for k in ('run_id', 'execution_id', 'exporter', 'runtime')) for r in process_up)
    active = env.active()
    env.record('file-healthy', not active and identity and all(r['value'][1] == '1' for r in targets) and len(process_up) >= 3,
               targets=len(targets), exporters=sorted(r['metric']['exporter'] for r in process_up), identity_labels_on_samples=identity,
               alerts=active, extra_label=[r['metric'].get('team') for r in targets][:1])

    # 2. the same run scraped both ways -> DuplicateScrape
    env.merged_config([(file_export, 'file'), (static_export, 'static')], config_dir)
    env.reload()
    env.wait_for('the aggregate job to be scraped', lambda: env.query('up{job="graph-loader-aggregate"}'), 60)
    try:
        env.wait_for('DuplicateScrape', lambda: 'GraphLoaderDuplicateScrape' in env.firing(), 240)
        env.record('duplicate-scrape-fires', True, firing=env.firing())
    except AssertionError:
        env.record('duplicate-scrape-fires', False, active=env.active())

    # 3. aggregate only -> the duplicate resolves, exporter_up is 1 for every child. The timeline (series per job, the age of the newest
    # sample of the removed job's series) says whether the old series ended through staleness markers or through lookback expiry.
    env.merged_config([(static_export, 'static')], config_dir)
    env.reload()
    reloaded = time.monotonic()
    timeline, resolved_after = [], None
    while time.monotonic() - reloaded < 480:
        by_job = {r['metric']['job']: int(float(r['value'][1])) for r in env.query('count by (job) (graph_loader_process_up)')}
        old = env.query('time() - max(timestamp(graph_loader_process_up{job="graph-loader"}))')
        timeline.append({'t': round(time.monotonic() - reloaded), 'series_per_job': by_job, 'age_of_removed_job_series_s': round(float(old[0]['value'][1]), 1) if old else None,
                         'duplicate_firing': 'GraphLoaderDuplicateScrape' in env.firing()})
        if 'GraphLoaderDuplicateScrape' not in env.firing():
            resolved_after = round(time.monotonic() - reloaded)
            break
        time.sleep(15)
    rows = env.query('graph_loader_exporter_up')
    env.record('static-only', resolved_after is not None and all(r['value'][1] == '1' for r in rows) and len(rows) >= 3, resolved_after_s=resolved_after,
               exporter_up=[(r['metric']['exporter'], r['value'][1]) for r in rows], alerts=env.active(), timeline=timeline)

    # 4. a hung child: the aggregate cannot scrape it -> ExporterScrapeFailed
    child = env.node_loader_pid(attempt)
    os.kill(int(child['pid']), signal.SIGSTOP)
    try:
        env.wait_for('ExporterScrapeFailed', lambda: 'GraphLoaderExporterScrapeFailed' in env.firing(), 240)
        failed = [r['metric']['exporter'] for r in env.query('graph_loader_exporter_up == 0')]
        env.record('exporter-scrape-failed-fires', True, down=failed, target_up=[(r['metric']['instance'], r['value'][1]) for r in env.query('up{job="graph-loader-aggregate"}')])
    except AssertionError:
        env.record('exporter-scrape-failed-fires', False, active=env.active())
    os.kill(int(child['pid']), signal.SIGCONT)
    try:
        env.wait_for('ExporterScrapeFailed to resolve', lambda: 'GraphLoaderExporterScrapeFailed' not in env.firing(), 180)
        env.record('exporter-scrape-failed-resolves', True)
    except AssertionError:
        env.record('exporter-scrape-failed-resolves', False, active=env.active())

    # 5. the run ends: the aggregate target is gone -> TargetDown
    feeder.stop()
    process.send_signal(signal.SIGTERM)
    process.wait(timeout=180)
    try:
        env.wait_for('TargetDown', lambda: 'GraphLoaderTargetDown' in env.firing(), 240)
        env.record('target-down-fires', True, firing=env.firing())
    except AssertionError:
        env.record('target-down-fires', False, active=env.active())


def phase_outage(env: Env):
    base = env.base
    file_export = base / 'export-file'
    config_dir = base / 'prom-config'
    env.merged_config([(file_export, 'file')], config_dir)
    env.ensure_prometheus(config_dir, mounts={str(base / 'runs'): {'bind': str(base / 'runs'), 'mode': 'ro'},
                                              str(base / 'targets'): {'bind': str(base / 'targets'), 'mode': 'ro'}})
    env.reload()
    process, topics, directory = env.start_run('r2', 'process')
    env.link_targets(env.attempt_of(directory), 'r2')
    env.wait_for('the nodes to load', env.loaded, 240)
    f = env.lab.fixture
    rows = [(f'{w}-{e}', {'source': {'id': w}, 'target': {'id': e}, 'properties': {'role': r, 'since': s}}) for w, e, r, s in f.relationships(env.matrix.WORKERS, env.matrix.EMPLOYERS)]
    env.lab.produce(topics['edge'], rows[:2000])
    # a malformed record (no required "since") and a record with a missing endpoint: both are routed to the dead-letter topic
    env.lab.produce(topics['edge'], [('bad-1', {'source': {'id': 1}, 'target': {'id': 'e0'}, 'properties': {'role': 'x'}}),
                                      ('bad-2', {'source': {'id': 999999}, 'target': {'id': 'e0'}, 'properties': {'role': 'x', 'since': 1}})])
    env.wait_for('file_sd targets', lambda: len(env.query('up{job="graph-loader"}')) >= 3, 120)
    try:
        env.wait_for('DlqRoutingDetected', lambda: 'GraphLoaderDlqRoutingDetected' in env.firing(), 180)
        env.record('dlq-routing-fires', True, dlq=[(r['metric'].get('failure_class'), r['value'][1]) for r in env.query('graph_loader_dlq_routed_total')])
    except AssertionError:
        env.record('dlq-routing-fires', False, active=env.active(), dlq=[(r['metric'], r['value'][1]) for r in env.query('graph_loader_dlq_routed_total')])
    # Neo4j down while a load is in progress
    neo = env.client.containers.get(env.lab.facts['names']['neo'])
    env.lab.produce(topics['edge'], rows[2000:])
    neo.stop(timeout=10)
    try:
        env.wait_for('WriteOutage', lambda: 'GraphLoaderWriteOutage' in env.firing(), 240)
        env.record('write-outage-fires', True, seconds=[(r['metric']['name'], r['value'][1]) for r in env.query('graph_loader_write_outage_seconds > 0')])
    except AssertionError:
        env.record('write-outage-fires', False, active=env.active(), gauge=[(r['metric'].get('name'), r['value'][1]) for r in env.query('graph_loader_write_outage_seconds')])
    neo.start()
    for _ in range(120):
        try:
            env.lab.counts()
            break
        except Exception:
            time.sleep(2)
    try:
        env.wait_for('WriteOutage to resolve', lambda: 'GraphLoaderWriteOutage' not in env.firing(), 300)
        env.record('write-outage-resolves', True)
    except AssertionError:
        env.record('write-outage-resolves', False, active=env.active())
    process.send_signal(signal.SIGTERM)
    process.wait(timeout=180)


def phase_docker(env: Env):
    base = env.base
    env.stop_prometheus() if env.prom else None
    token = env.token
    export = env.export('export-docker', '--discovery', 'docker', '--docker-network', env.lab.facts['network'], '--scrape-interval', '5s', '--job-name', 'graph-loader')
    config_dir = base / 'prom-config-docker'
    env.merged_config([(export, 'docker')], config_dir)
    config = yaml.safe_load((config_dir / 'prometheus.yml').read_text())
    job = config['scrape_configs'][0]
    # server-side: only containers on the owned network that carry the graph_loader.kind label (the CLI's role containers cannot carry
    # the stack's owner label, so the owned network is the ownership filter); Prometheus never lists any other container
    job['docker_sd_configs'][0]['filters'] = [{'name': 'network', 'values': [env.lab.facts['network']]}, {'name': 'label', 'values': ['graph_loader.kind']}]
    job['docker_sd_configs'][0]['host'] = 'unix:///var/run/docker.sock'
    text = yaml.safe_dump(config, sort_keys=False)
    (config_dir / 'prometheus.yml').write_text(text)
    (base / 'rendered-docker-sd-config.yml').write_text(text)
    log('rendered Docker SD config saved')
    env.start_prometheus(config_dir, network=env.lab.facts['network'], mounts={'/var/run/docker.sock': {'bind': '/var/run/docker.sock', 'mode': 'ro'}})
    # the role containers carry the owner label? they are created by the CLI: label them through the network instead (see below)
    process, topics, directory = env.start_run('r3', 'docker')
    env.wait_for('the nodes to load', env.loaded, 240)
    env.lab.produce_edges(topics)
    env.wait_for('the relationships to load', lambda: env.loaded(2 * env.matrix.WORKERS), 240)
    time.sleep(40)
    targets = env.api('/api/v1/targets')['activeTargets']
    up = env.query('up{job="graph-loader"}')
    process_up = env.query('graph_loader_process_up')
    env.record('docker-discovery', bool(up) and all(r['value'][1] == '1' for r in up) and len(process_up) >= 3,
               targets=[(t['labels'].get('container_name') or t['labels'].get('instance'), t['health']) for t in targets],
               exporters=sorted(r['metric']['exporter'] for r in process_up), runtime={r['metric'].get('runtime') for r in process_up}, alerts=env.active())
    process.send_signal(signal.SIGTERM)
    process.wait(timeout=180)
    env.stop_prometheus()
    env.record('docker-prometheus-removed', env.client.containers.list(all=True, filters={'name': f'e15-{token}-prom'}) == [])


def promtool(env: Env, config_dir: Path, *arguments: str):
    """promtool from the Prometheus image with NO network and the configuration read-only: nothing here reaches a socket or a container."""
    try:
        output = env.client.containers.run(PROM_IMAGE, entrypoint='promtool', command=list(arguments), network_mode='none', remove=True, mem_limit='128m',
                                           volumes={str(config_dir): {'bind': '/etc/prometheus', 'mode': 'ro'}}, user=f'{os.getuid()}:{os.getgid()}')
        return 0, output.decode(errors='replace')[-600:]
    except docker.errors.ContainerError as exc:
        return exc.exit_status, (exc.stderr or b'').decode(errors='replace')[-600:]


def phase_docker_offline(env: Env):
    """Docker discovery WITHOUT a live Docker-SD Prometheus (the socket mount is not part of this cell): the exported config with the server-side
    filters is rendered and checked by promtool offline, and the exported rule tests (defaults + generated boundary tests) run."""
    base = env.base
    export = env.export('export-docker', '--discovery', 'docker', '--docker-network', env.lab.facts['network'], '--scrape-interval', '5s', '--job-name', 'graph-loader')
    config_dir = base / 'prom-config-docker'
    env.merged_config([(export, 'docker')], config_dir)
    config = yaml.safe_load((config_dir / 'prometheus.yml').read_text())
    job = config['scrape_configs'][0]
    job['docker_sd_configs'][0]['filters'] = [{'name': 'network', 'values': [env.lab.facts['network']]}, {'name': 'label', 'values': ['graph_loader.kind']}]
    text = yaml.safe_dump(config, sort_keys=False)
    (config_dir / 'prometheus.yml').write_text(text)
    (base / 'rendered-docker-sd-config.yml').write_text(text)
    code, output = promtool(env, config_dir, 'check', 'config', '/etc/prometheus/prometheus.yml')
    env.record('docker-sd-config-offline', code == 0 and 'SUCCESS' in output, exit_code=code, promtool=output.strip()[-300:],
               filters=job['docker_sd_configs'][0]['filters'], relabel_keeps_runtime_label=any('runtime' in json.dumps(r) for r in job.get('relabel_configs', [])),
               socket_mounted=False, container_started='promtool --network none only')
    tests_dir = base / 'export-docker' / 'prometheus'
    results = {}
    for test in sorted((tests_dir / 'alert_tests').glob('*_test.yml')):
        tcode, toutput = promtool(env, tests_dir, 'test', 'rules', f'/etc/prometheus/alert_tests/{test.name}')
        results[test.name] = tcode
    env.record('exported-rule-tests-offline', results and all(c == 0 for c in results.values()), results=results)


def phase_docker_logs(env: Env):
    """The role containers of a Docker-runtime stream run write JSON lines with the run's identity (no Prometheus involved)."""
    process, topics, directory = env.start_run('dl', 'docker')
    try:
        env.wait_for('the nodes to load', env.loaded, 240)
        attempt = env.attempt_of(directory)
        status = json.loads((attempt / 'status.json').read_text())
        containers = env.client.containers.list(filters={'network': env.lab.facts['network'], 'label': 'graph_loader.kind'})
        seen = {}
        for container in containers:
            lines = [json.loads(line) for line in container.logs(tail=300).decode(errors='replace').splitlines() if line.startswith('{')]
            rows = [r for r in lines if isinstance(r, dict) and 'level' in r]
            seen[container.name] = {'lines': len(rows), 'with_execution_id': sum(1 for r in rows if r.get('execution_id') == status['execution_id']),
                                    'other_execution_id': sum(1 for r in rows if r.get('execution_id') not in (None, status['execution_id'])),
                                    'with_kind': sum(1 for r in rows if r.get('kind'))}
        env.record('docker-role-container-logs-carry-identity', bool(seen) and all(v['lines'] and v['with_execution_id'] and not v['other_execution_id'] and v['with_kind']
                                                                                  for v in seen.values()), containers=seen)
    finally:
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=180)


def use_fast_scrape(env: Env, interval='2s'):
    """Procedure for short bulk runs: a 2 s scrape so a 15-20 s run shows increases (the default 5 s misses the first-sample window). Also makes sure
    the file job (and the static job for the aggregated live run, port 19601) exist."""
    base = env.base
    values = base / 'values.yaml'
    glob_path = str(base / 'targets' / '*.json')
    file_export = env.export('export-file-fast', '--discovery', 'file', '--targets-file', glob_path, '--scrape-interval', interval, '--values', str(values), '--job-name', 'graph-loader')
    static_export = env.export('export-static-live', '--discovery', 'static', '--metrics-port', '19601', '--scrape-interval', interval, '--values', str(values),
                               '--job-name', 'graph-loader-aggregate')
    config_dir = base / 'prom-config'
    env.merged_config([(file_export, 'file'), (static_export, 'static')], config_dir)
    env.ensure_prometheus(config_dir, mounts={str(base / 'runs'): {'bind': str(base / 'runs'), 'mode': 'ro'},
                                              str(base / 'targets'): {'bind': str(base / 'targets'), 'mode': 'ro'}})
    env.reload()


def phase_data(env: Env):
    """Bulk runs (planned, leases, classic) scraped through the file_sd job, so every dashboard panel has a source (plan progress, lease server,
    lag of the CLI, deadlocks...). The Prometheus of an earlier phase must still be running with the file job."""
    base = env.base
    (base / 'targets').mkdir(exist_ok=True)
    (base / 'runs').mkdir(exist_ok=True)
    for stale in (base / 'targets').glob('*.json'):               # the finished runs of the alert phases are not part of this one
        stale.unlink()
    use_fast_scrape(env)
    for name, mode in (('d1', 'planned'), ('d2', 'leases'), ('d3', 'classic')):
        directory = base / 'runs' / name
        directory.mkdir(parents=True)
        prefix = f'e15.{env.token}.mon.{name}.{int(time.time())}'
        topics = {'worker': f'{prefix}.worker', 'employer': f'{prefix}.employer', 'edge': f'{prefix}.edge'}
        env.lab.create_topics(list(topics.values()))
        env.lab.clear_graph()
        cell = env.matrix.Cell(mode, 'process', 'success', 'json')
        schema = directory / 'schema.yaml'
        schema.write_text(env.matrix.schema_text(env.lab, cell, topics, f'e15-{env.token}-mon-{name}-{int(time.time())}'))
        env.lab.produce_nodes(topics)
        env.lab.produce_edges(topics)
        out, err = open(directory / 'stdout.log', 'wb'), open(directory / 'stderr.log', 'wb')
        process = subprocess.Popen([env.exe, *env.matrix.arguments(env.lab, cell, schema, directory / 'runs')], cwd=directory, env=env.lab.environment(cell),
                                   stdout=out, stderr=err, start_new_session=True)
        attempt = env.attempt_of(directory)
        env.link_targets(attempt, name)
        code = process.wait(timeout=300)
        time.sleep(12)                                                  # a last scrape or two after the run
        env.record(f'data-{mode}', code == 0, exit_code=code, graph=env.lab.counts())


def bulk_run(env: Env, name: str, mode: str, runtime='process'):
    directory = env.base / 'runs' / name
    directory.mkdir(parents=True)
    prefix = f'e15.{env.token}.mon.{name}.{int(time.time())}'
    topics = {'worker': f'{prefix}.worker', 'employer': f'{prefix}.employer', 'edge': f'{prefix}.edge'}
    env.lab.create_topics(list(topics.values()))
    env.lab.clear_graph()
    cell = env.matrix.Cell(mode, runtime, 'success', 'json')
    schema = directory / 'schema.yaml'
    schema.write_text(env.matrix.schema_text(env.lab, cell, topics, f'e15-{env.token}-mon-{name}-{int(time.time())}'))
    env.lab.produce_nodes(topics)
    env.lab.produce_edges(topics)
    out, err = open(directory / 'stdout.log', 'wb'), open(directory / 'stderr.log', 'wb')
    process = subprocess.Popen([env.exe, *env.matrix.arguments(env.lab, cell, schema, directory / 'runs')], cwd=directory, env=env.lab.environment(cell),
                               stdout=out, stderr=err, start_new_session=True)
    attempt = env.attempt_of(directory)
    return process, attempt


def phase_normal(env: Env):
    """A NORMAL successful bulk finish under file_sd: the run's targets file is emptied and nothing alerts (no TargetDown)."""
    base = env.base
    (base / 'targets').mkdir(exist_ok=True)
    (base / 'runs').mkdir(exist_ok=True)
    for stale in (base / 'targets').glob('*.json'):
        stale.unlink()
    # Procedure (the criteria below are unchanged): a NEW Prometheus with the file job only, so the series of the earlier alert phases (their
    # firing alerts, their static aggregate targets that nothing listens on) are not part of this observation.
    env.attach_prometheus()
    env.stop_prometheus()
    file_export = env.export('export-file-normal', '--discovery', 'file', '--targets-file', str(base / 'targets' / '*.json'), '--scrape-interval', '2s',
                             '--values', str(base / 'values.yaml'), '--job-name', 'graph-loader')
    config_dir = base / 'prom-config-normal'
    env.merged_config([(file_export, 'file')], config_dir)
    env.start_prometheus(config_dir, mounts={str(base / 'runs'): {'bind': str(base / 'runs'), 'mode': 'ro'},
                                             str(base / 'targets'): {'bind': str(base / 'targets'), 'mode': 'ro'}})
    process, attempt = bulk_run(env, 'n2', 'planned')
    env.link_targets(attempt, 'n2')
    during = json.loads((attempt / 'metrics' / 'targets.json').read_text())
    env.wait_for('the CLI target to be scraped', lambda: any(r['metric'].get('component') == 'cli' and r['value'][1] == '1' for r in env.query('up{job="graph-loader"}')), 60)
    code = process.wait(timeout=300)
    after = json.loads((attempt / 'metrics' / 'targets.json').read_text())
    time.sleep(100)                                                     # longer than the rule's 1 m `for:` plus the file_sd refresh
    down = env.query('up == 0')
    state = env.active()
    env.record('normal-finish-no-target-down', code == 0 and after == [] and not down and not any(n == 'GraphLoaderTargetDown' for n, _ in state),
               exit_code=code, groups_during=len(during), file_after=after, up_zero=[r['metric'].get('instance') for r in down], alerts=state)


def phase_live(env: Env):
    """Cell C data: two live stream runs (file_sd on one, the aggregated endpoint 19601 on the other), then the Grafana cell on top of them."""
    import e15_live_grafana as grafana
    base = env.base
    (base / 'targets').mkdir(exist_ok=True)
    (base / 'runs').mkdir(exist_ok=True)
    for stale in (base / 'targets').glob('*.json'):
        stale.unlink()
    env.attach_prometheus()
    env.stop_prometheus()                      # a new Prometheus with the file job and the aggregate job (19601) of use_fast_scrape
    use_fast_scrape(env)
    stamp = int(time.time())                    # run directories are named per invocation, so a repeat never meets an earlier directory
    # a planned bulk run first (file_sd): the plan panels have a source in this Prometheus, which is new and holds only these runs
    process, attempt = bulk_run(env, f'l3-{stamp}', 'planned')
    env.link_targets(attempt, f'l3-{stamp}')
    code = process.wait(timeout=300)
    time.sleep(12)
    env.record('live-planned-bulk', code == 0, exit_code=code)
    runs = []
    feeders = []
    for name, port in ((f'l1-{stamp}', None), (f'l2-{stamp}', 19601)):
        process, topics, directory = env.start_run(name, 'process', metrics_port=port)
        attempt = env.attempt_of(directory)
        if port is None:
            env.link_targets(attempt, name)
        time.sleep(25)                                                    # the two runs share one graph, so exact counts are not checked here
        env.lab.produce_edges(topics)
        time.sleep(15)
        feeder = Feeder(env, topics)
        feeder.start()
        runs.append(process)
        feeders.append(feeder)
    time.sleep(45)
    try:
        lab = grafana.Lab(env.workdir)
        lab.base = env.workdir / 'grafana'
        lab.base.mkdir(exist_ok=True)
        for name in ('provision', 'api', 'postgres'):
            log(f'--- grafana {name}')
            try:
                grafana.PHASES[name](lab)
            except Exception as exc:                                       # noqa: BLE001
                import traceback
                lab.record(f'phase-{name}', False, error=f'{type(exc).__name__}: {exc}', trace=traceback.format_exc()[-700:])
    finally:
        for feeder in feeders:
            feeder.stop()
        for process in runs:
            process.send_signal(signal.SIGTERM)
            process.wait(timeout=180)


PHASES = {'file': phase_file_dup_static_hang_gone, 'outage': phase_outage, 'docker': phase_docker, 'docker-offline': phase_docker_offline, 'docker-logs': phase_docker_logs, 'data': phase_data, 'normal': phase_normal, 'live': phase_live}


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('workdir', type=Path)
    parser.add_argument('--tree', type=Path, default=Path.cwd())
    parser.add_argument('--phases', default='file,outage')
    args = parser.parse_args()
    env = Env(args.workdir.resolve(), args.tree.resolve())
    for name in args.phases.split(','):
        log(f'=== phase {name}')
        try:
            PHASES[name](env)
        except Exception as exc:                                   # noqa: BLE001
            import traceback
            env.record(f'phase-{name}', False, error=f'{type(exc).__name__}: {exc}', trace=traceback.format_exc()[-900:])
    if env.prom is not None and 'docker' not in args.phases:
        log('Prometheus left running for the Grafana cells (removed by down)')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
