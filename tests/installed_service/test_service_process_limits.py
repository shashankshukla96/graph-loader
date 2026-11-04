"""E14.F2.S6 live: the process runtime inside a memory limit, with no container and no image.

The CLI runs ``graph-loader start --runtime process`` inside a transient systemd **user** scope bounded by ``MemoryMax=`` (and
``MemorySwapMax=0``): ``systemd-run --user --scope``. The scope is the CLI's cgroup, so ``cgroup_budget`` reads a real finite
``memory.max`` on the leaf. The product uses no Docker at all (``DOCKER_HOST`` points at a missing socket); the only containers are
the owned Neo4j/Kafka test stack, reached over its published loopback ports, as in the S4 matrix. Cells:

* **binding budget**: a planned bulk load whose automatic planner budget is *binding* (concurrency reduced): exact oracle graph, the
  scope's peak stays below ``MemoryMax``, no OOM kill; computed budget and measured peaks recorded in ``limits.jsonl``;
* **undersized limit**: refused at launch (``stage=sizing``), exit 1, no role started, no OOM kill;
* **explicit settings that cannot fit** (control): refused up front;
* **formula check**: measured per-role peaks (``VmHWM``) are within the formula's reserved sizes, and the formula's total is within
  its stated margin of the measured peak.
"""
from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import threading
import time
from pathlib import Path

import pytest

from neo4j_graph_loader.orchestrator import cgroup_budget as cb
from neo4j_graph_loader.orchestrator.run_registry import RunRegistry, fold, signal_registered_cli
from tests.installed_service import bulk_fixture as fx
from tests.installed_service.observers import graph_observations

pytestmark = pytest.mark.integration

START_MIN_AVAILABLE_KB = 2_500_000
STOP_BELOW_AVAILABLE_KB = 1_500_000
CELL_TIMEOUT = 420


def available_kb():
    for line in Path('/proc/meminfo').read_text().splitlines():
        if line.startswith('MemAvailable:'):
            return int(line.split()[1])
    raise RuntimeError('no MemAvailable')


@pytest.fixture(autouse=True)
def memory_guard():
    assert available_kb() >= START_MIN_AVAILABLE_KB, 'host memory guard: not enough MemAvailable to start a cell'


class Sampler(threading.Thread):
    """Samples the scope's cgroup every 0.5 s: memory.current/peak/events, per-process VmHWM by module; SIGTERMs the CLI on a host breach."""

    def __init__(self, run, scope):
        super().__init__(daemon=True)
        self.run_dir, self.scope = Path(run), scope
        self.done = threading.Event()
        self.max_current = self.max_peak = self.oom_kill = 0
        self.role_hwm_kb = {}
        self.host_breach = False

    def read(self, name):
        try:
            return (self.scope / name).read_text()
        except OSError:
            return None

    def run(self):
        while not self.done.wait(0.5):
            if available_kb() < STOP_BELOW_AVAILABLE_KB and not self.host_breach:
                self.host_breach = True                      # stop the CLI through its normal stop path (SIGTERM, pidfd-verified)
                for registry in self.run_dir.glob('runs/*/registry.jsonl'):
                    try:
                        signal_registered_cli(registry, signal.SIGTERM)
                    except Exception:
                        pass
            current = self.read('memory.current')
            if current:
                self.max_current = max(self.max_current, int(current))
            peak = self.read('memory.peak')
            if peak:
                self.max_peak = max(self.max_peak, int(peak))
            events = self.read('memory.events') or ''
            match = re.search(r'oom_kill (\d+)', events)
            if match:
                self.oom_kill = max(self.oom_kill, int(match.group(1)))
            for pid in (self.read('cgroup.procs') or '').split():
                try:
                    status = Path(f'/proc/{pid}/status').read_text()
                    cmdline = Path(f'/proc/{pid}/cmdline').read_bytes().replace(b'\0', b' ').decode(errors='replace')
                except OSError:
                    continue
                match = re.search(r'^VmHWM:\s+(\d+) kB', status, re.M)
                if not match:
                    continue
                module = re.search(r'neo4j_graph_loader\.(?:loader|orchestrator)\.(\w+)', cmdline)
                if 'multiprocessing' in cmdline or 'spawn_main' in cmdline:
                    key = 'planner-worker'
                elif module:
                    key = module.group(1)
                elif 'graph-loader' in cmdline or 'neo4j_graph_loader.cli' in cmdline:
                    key = 'cli'
                else:
                    key = 'other'
                self.role_hwm_kb[key] = max(self.role_hwm_kb.get(key, 0), int(match.group(1)))


def user_scope_path(unit):
    uid = os.getuid()
    base = Path(f'/sys/fs/cgroup/user.slice/user-{uid}.slice/user@{uid}.service')
    for path in base.rglob(f'{unit}.scope'):
        return path
    return None


def run_cli_scope(stack, service_config, tag, args, *, mem_mb, timeout=CELL_TIMEOUT):
    """Run the installed CLI in a transient user scope bounded by ``MemoryMax`` (no swap); returns the observations."""
    run = Path(service_config['run_dir']) / f'p6-{tag}'
    unit = f'e14-{stack.owner[:8]}-{tag}'
    env = {key: os.environ[key] for key in ('PATH', 'HOME', 'LANG', 'LC_ALL', 'TMPDIR') if key in os.environ}
    env.update(XDG_RUNTIME_DIR=f'/run/user/{os.getuid()}', DOCKER_HOST='unix:///nonexistent/e14-no-docker.sock',
               NEO4J_URI=stack.neo_uri, NEO4J_USERNAME='neo4j', NEO4J_PASSWORD=stack.password, NEO4J_DATABASE=stack.database,
               KAFKA_BOOTSTRAP_SERVERS=stack.bootstrap, GRAPH_LOADER_METRICS_ENABLED='0')
    command = ['systemd-run', '--user', '--scope', '--quiet', f'--unit={unit}', '-p', f'MemoryMax={mem_mb}M', '-p', 'MemorySwapMax=0',
               '--', str(Path(service_config['venv_bin']) / 'graph-loader'), *args]
    out_path, err_path = run / 'cli.out', run / 'cli.err'
    out = {'run': run, 'unit': unit}
    with open(out_path, 'wb') as stdout, open(err_path, 'wb') as stderr:
        process = subprocess.Popen(command, cwd=run, env=env, stdout=stdout, stderr=stderr, start_new_session=True)
        began = time.monotonic()
        sampler = None
        scope = None
        try:
            while process.poll() is None and time.monotonic() - began < timeout:
                if sampler is None:
                    scope = user_scope_path(unit)
                    if scope is not None:
                        sampler = Sampler(run, scope)
                        sampler.start()
                time.sleep(0.2)
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(10)
        finally:
            if sampler is not None:
                sampler.done.set()
                sampler.join(5)
    text = out_path.read_text(errors='replace') + err_path.read_text(errors='replace')
    out.update(status=process.returncode, seconds=time.monotonic() - began, logs=text,
               oom_kill=sampler.oom_kill if sampler else None, oom_killed=bool(sampler and sampler.oom_kill),
               peak=(sampler.max_peak or sampler.max_current) if sampler else 0, sampled_peak=sampler.max_current if sampler else 0,
               role_hwm_kb=dict(sampler.role_hwm_kb) if sampler else {}, host_breach=bool(sampler and sampler.host_breach),
               scope_seen=scope is not None)
    return out


def bulk_args(config_path, extra=()):
    return ['start', '--mode', 'bulk', '--config', str(config_path), '--runtime', 'process', '--run-dir', str(config_path.parent / 'runs'),
            *fx.PLAN_ARGS, *extra]


def own_graph(stack):
    return fx.own_graph(graph_observations(stack.driver, owner=stack.owner, database=stack.database))


def clear(stack):
    with stack.driver.session(database=stack.database) as session:
        session.run('MATCH (n:Worker|Employer) DETACH DELETE n').consume()


def run_cell(stack, service_config, tag, mem_mb, *, extra=()):
    clear(stack)
    run = Path(service_config['run_dir']) / f'p6-{tag}'
    topics = {key: f'{stack.topic_prefix}{tag}-{key}' for key in ('worker', 'employer', 'edge')}
    stack.create_topics(list(topics.values()))
    fx.produce(stack, topics)
    group = f'{stack.topic_prefix}{tag}-group'
    run.mkdir(parents=True)
    (run / 'schema.yaml').write_text(fx.schema(stack.topic_prefix, topics, group))
    return run_cli_scope(stack, service_config, tag, bulk_args(run / 'schema.yaml', extra=extra), mem_mb=mem_mb)


def record(service_config, **row):
    with open(Path(service_config['run_dir']) / 'limits.jsonl', 'a') as out:
        out.write(json.dumps(row, default=str, sort_keys=True) + '\n')


def sizing_line(logs):
    match = re.search(r'stage=sizing planner_workers=(\d+) planner_memory_mb=(\d+) headroom_mb=(\d+) reserved_mb=(\d+) planner_mb=(\d+)'
                      r' roles=(\d+)', logs)
    assert match, 'no stage=sizing line in the CLI output:\n' + logs[-3000:]
    keys = ('planner_workers', 'planner_memory_mb', 'headroom_mb', 'reserved_mb', 'planner_mb', 'roles')
    return dict(zip(keys, map(int, match.groups())))


# The limit is chosen so the automatic planner budget binds: reserved = 200 (CLI) + 2 x 150 (nodes) + 120 (dispatcher) + 2 x 150
# (plan writers) = 920 MiB; planner = (1400 - 920) x 0.8 = 384 MiB -> one 256+ MiB worker instead of the default concurrency.
BINDING_LIMIT_MB = 1400
UNDERSIZED_LIMIT_MB = 900


def test_p6_binding_budget_keeps_the_run_below_the_limit(stack, service_config):
    out = run_cell(stack, service_config, 'bind', BINDING_LIMIT_MB)
    assert out['scope_seen'], 'the transient scope was never found under the user manager'
    assert out['status'] == 0, (out['status'], out['logs'][-3000:])
    assert out['oom_killed'] is False and not out['oom_kill'], 'the scope saw an OOM kill'
    sizing = sizing_line(out['logs'])
    assert sizing['headroom_mb'] <= BINDING_LIMIT_MB
    assert sizing['planner_workers'] == 1, 'the budget should have reduced the planner to one worker'
    assert 'reduced from' in out['logs']
    assert out['peak'] < BINDING_LIMIT_MB * (1 << 20), ('peak', out['peak'])
    assert own_graph(stack) == (fx.expected_nodes(), fx.expected_relationships()), 'the graph differs from the fixture oracle'
    record(service_config, cell='binding', limit_mb=BINDING_LIMIT_MB, sizing=sizing, peak_mb=out['peak'] >> 20,
           sampled_peak_mb=out['sampled_peak'] >> 20, role_hwm_mb={k: v >> 10 for k, v in out['role_hwm_kb'].items()}, seconds=out['seconds'])
    clear(stack)


def test_p6_undersized_limit_is_refused_at_launch_not_oom_killed(stack, service_config):
    out = run_cell(stack, service_config, 'under', UNDERSIZED_LIMIT_MB)
    assert out['status'] == 1, (out['status'], out['logs'][-3000:])
    assert out['oom_killed'] is False and not out['oom_kill']
    assert re.search(r'stage=sizing reason=.*one planner worker needs at least 256 MiB', out['logs']), out['logs'][-3000:]
    registry = RunRegistry.read(next((out['run'] / 'runs').glob('*/registry.jsonl')))
    assert not [s for s in fold(registry).values() if not s.get('external')], 'a role was launched before the sizing refusal'
    assert own_graph(stack) == ([], [])
    record(service_config, cell='undersized', limit_mb=UNDERSIZED_LIMIT_MB, peak_mb=out['peak'] >> 20)


def test_p6_explicit_planner_settings_that_exceed_the_limit_are_refused_not_oom_killed(stack, service_config):
    """Control: the settings the automatic budget avoids (8 workers x 1 GiB) would not fit; they are refused up front."""
    out = run_cell(stack, service_config, 'explicit', BINDING_LIMIT_MB, extra=('--plan-workers', '8', '--plan-memory-mb', '1024'))
    assert out['status'] == 1, (out['status'], out['logs'][-3000:])
    assert out['oom_killed'] is False and not out['oom_kill']
    assert re.search(r'stage=sizing reason=--plan-workers 8 x --plan-memory-mb 1024 needs 8192 MiB but only \d+ MiB fit', out['logs']), out['logs'][-3000:]
    registry = RunRegistry.read(next((out['run'] / 'runs').glob('*/registry.jsonl')))
    assert not [s for s in fold(registry).values() if not s.get('external')], 'a role was launched before the sizing refusal'
    record(service_config, cell='explicit-exceeds', limit_mb=BINDING_LIMIT_MB, peak_mb=out['peak'] >> 20)


def test_p6_formula_reproduces_the_measured_peaks_within_its_margin(stack, service_config):
    """Per-role peak RSS (VmHWM) measured in the binding cell vs the formula's reserved sizes."""
    rows = [json.loads(line) for line in (Path(service_config['run_dir']) / 'limits.jsonl').read_text().splitlines()]
    binding = next(row for row in rows if row['cell'] == 'binding')
    measured = binding['role_hwm_mb']
    reserved = {'node_loader': cb.ROLE_PEAK_MB['node-loader'], 'planned_dispatcher': cb.ROLE_PEAK_MB['planned-dispatcher'],
                'planned_writer': cb.ROLE_PEAK_MB['planned-edge-writer'], 'cli': cb.CLI_PEAK_MB}
    for role, limit in reserved.items():
        if role in measured:
            assert measured[role] <= limit, f'{role}: measured {measured[role]} MiB exceeds the reserved {limit} MiB'
    formula_total = binding['sizing']['reserved_mb'] + binding['sizing']['planner_mb']
    assert binding['peak_mb'] <= formula_total, 'the run used more than the formula reserved'
    margin = 3.0
    assert formula_total <= margin * max(binding['peak_mb'], 1) + 700, (
        f"the formula ({formula_total} MiB) is more than {margin}x the measured peak ({binding['peak_mb']} MiB) + 700 MiB slack")
