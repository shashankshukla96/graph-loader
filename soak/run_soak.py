"""Driver: ``python -m soak.run_soak {smoke,...}`` from the repository root, on the Linux host, with the frozen wheel's venv.

Every subcommand builds one ``Soak`` (owned stack, installed fleet), runs its plan with a hard wall-clock limit, always closes it by
recorded ID, and writes a JSON summary under ``<run_root>/<token>/out/``. A run is red (never silently green) on: a failed pass
criterion, a coverage failure, a tripped guard, an unexplained fleet restart, a foreign-snapshot change, or a cleanup failure.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback

from . import budget, spec
from .faults import Faults
from . import scenarios
from .runtime import Soak, SoakConfig


def write(soak, name, payload):
    path = soak.out_dir / name
    path.write_text(json.dumps(payload, indent=1, sort_keys=True, default=str) + '\n')
    return path


def s1_peaks():
    """The reference the 2x rule uses: the S1 smoke peaks, raised to the peaks measured at the calibrated rate on the baseline graph
    (the S1 smoke ran 10 ops/s on an empty graph, before any baseline re-read; the loaders' memory after a 300,000-record re-read is
    about twice that). Both tables are kept: s1_peaks.json and reference_peaks.json."""
    path = Path(__file__).with_name('reference_peaks.json')
    return json.loads(path.read_text()) if path.exists() else {}


def wheel_facts(venv_bin):
    python = Path(venv_bin) / 'python'
    out = subprocess.run([str(python), '-I', '-m', 'pip', 'show', '-f', 'neo4j-graph-loader'], capture_output=True, text=True, timeout=60).stdout
    location = next((line.split(': ', 1)[1] for line in out.splitlines() if line.startswith('Location: ')), '')
    version = next((line.split(': ', 1)[1] for line in out.splitlines() if line.startswith('Version: ')), '')
    editable = 'editable' in out.lower() or not location or 'site-packages' not in location
    cli = subprocess.run([str(Path(venv_bin) / 'graph-loader'), '--version'], capture_output=True, text=True, timeout=60).stdout.strip()
    return {'version': version, 'location': location, 'cli_version': cli, 'site_packages_install': not editable}


def host_reference():
    """Per-process peaks measured on the host (after the baseline re-read) for the 2x rule; empty until a host-smoke recorded them."""
    path = Path(__file__).with_name('process_peaks.json')
    return json.loads(path.read_text()) if path.exists() else {}


def judge(soak, extra=None):
    """The run-level red conditions that apply to every subcommand."""
    problems = list(extra or [])
    if soak.tripped:
        problems.append(f'guard tripped: {soak.tripped}')
    if soak.supervisor is not None and soak.supervisor.unexplained:
        problems.append(f'{len(soak.supervisor.unexplained)} unexplained fleet exit(s)')
    if soak.querylog.coverage:
        problems.append(f'{len(soak.querylog.coverage)} query-log coverage failure(s)')
    if soak.sampler.gaps:
        problems.append(f'{len(soak.sampler.gaps)} sampling gap(s)')
    if soak.querylog.loader_waits():
        problems.append(f'{len(soak.querylog.loader_waits())} Neo4j lock wait(s) on loader writes')
    s1 = s1_peaks() if not getattr(soak, 'shape', None) else host_reference()
    over = {k: (v, s1[k]) for k, v in soak.peaks.items() if k in s1 and k not in ('neo4j', 'kafka', 'cli-process', 'harness') and v > 2 * s1[k]}
    if over:
        problems.append(f'container(s) above 2x their S1 measured peak: {over}')
    oom = [r for r in soak.oom_events()]
    if oom:
        problems.append(f'OOM-killed container(s): {oom}')
    failed = [f['name'] for f in soak.fault_log if not f.get('ok')]
    if failed:
        problems.append(f'fault(s) without their expected outcome: {failed}')
    undetected = [c for c in soak.controls if not c['detected']]
    if undetected:
        problems.append('the positive control was not detected in every incarnation')
    return problems


def host_judge(soak):
    """Extra red conditions of the host shapes: a second fleet, kernel OOM evidence, harness-made orphan removal."""
    problems = []
    if getattr(soak, 'stale_samples', 0) >= 2:
        problems.append(f'processes of an earlier attempt were still alive in {soak.stale_samples} consecutive samples (a second fleet)')
    removed = [e.get('recovery', {}).get('leftovers', {}).get('removed') for e in (soak.supervisor.events if soak.supervisor else [])]
    if any(removed):
        problems.append(f'the harness had to remove orphan processes after a fleet exit: {[r for r in removed if r]}')
    return problems


def neo_summary(soak):
    """The numbers the memory story needs from Neo4j itself: heap, GC, page-cache hit ratio, store size."""
    rows = [r for r in soak.neo_internals if 'heap_used' in r]
    if not rows:
        return {'samples': 0}
    out = {'samples': len(soak.neo_internals), 'heap_used_max': max(r['heap_used'] for r in rows), 'heap_max': rows[-1].get('heap_max'),
           'heap_used_last': rows[-1]['heap_used']}
    first, last = rows[0].get('gc', {}), rows[-1].get('gc', {})
    out['gc'] = {name: {'count': v['count'] - first.get(name, {}).get('count', 0), 'ms': v['ms'] - first.get(name, {}).get('ms', 0)}
                 for name, v in last.items()}
    caches = [r['page_cache'] for r in soak.neo_internals if r.get('page_cache')]
    if caches:
        ratios = [c['dbms_page_cache_hit_ratio'] for c in caches if 'dbms_page_cache_hit_ratio' in c]
        usage = [c['dbms_page_cache_usage_ratio'] for c in caches if 'dbms_page_cache_usage_ratio' in c]
        keep = ('dbms_page_cache_hits_total', 'dbms_page_cache_page_faults_total', 'dbms_page_cache_evictions_total', 'dbms_page_cache_flushes_total')
        out['page_cache'] = {'hit_ratio_min': min(ratios) if ratios else None, 'hit_ratio_last': ratios[-1] if ratios else None,
                             'usage_ratio_max': max(usage) if usage else None, **{k: caches[-1].get(k) for k in keep}}
    stores = [r['store_bytes'] for r in soak.neo_internals if r.get('store_bytes')]
    out['store_bytes_last'] = stores[-1] if stores else None          # du of /data/databases/<db> and /data/transactions/<db>
    sizes = [r['data_size'] for r in soak.neo_internals if r.get('data_size')]
    out['store_metrics_last'] = {k: v for k, v in sizes[-1].items() if k.startswith('database_e14') and k.endswith(('store_size_total', 'store_size_database'))} if sizes else None
    out['errors'] = sorted({e for r in soak.neo_internals for k, e in r.items() if k.endswith('_error')})[:5]
    return out


def budget_report(soak):
    """The memory-budget inequality for the largest scheduled fleet, from this run's measured per-container peaks."""
    peaks = {k: v for k, v in soak.peaks.items() if k not in ('neo4j', 'kafka', 'cli-process')}
    after = {**soak.replicas, 'PURCHASED': max(soak.replicas['PURCHASED'], 3)}
    fleet = budget.largest_fleet_peaks(peaks, soak.replicas, after, cli_peak=soak.peaks.get('cli-process', 0))
    result = budget.evaluate(neo4j_limit=soak.cfg.neo_memory, kafka_limit=soak.cfg.kafka_memory, fleet=fleet,
                             harness_peak=soak.harness_peak, mem_available=soak.preflight_facts['mem_available_bytes'])
    result['fleet'] = fleet
    result['max_fleet_sum_allowed'] = budget.max_fleet_sum(soak.cfg.neo_memory, soak.cfg.kafka_memory, soak.harness_peak,
                                                           soak.preflight_facts['mem_available_bytes'])
    result['replicas_start'] = dict(soak.replicas)
    result['replicas_largest'] = after
    return result


def parse_replicas(text):
    return {k: int(v) for k, v in (item.split('=') for item in text.split(','))} if text else {}


def smoke(args):
    cfg = SoakConfig(**({'token': args.token} if args.token else {}), run_root=args.run_root, manifest=args.manifest, venv_bin=args.venv_bin, member_wait=args.member_wait,
                     cli_deadline=args.duration + 1800, stack_deadline=args.duration + 3600,
                     neo_memory=int(args.neo_mib * 1024 ** 2), kafka_memory=int(args.kafka_mib * 1024 ** 2),
                     neo_heap=args.neo_heap, neo_cache=args.neo_cache, kafka_heap=args.kafka_heap, replicas=parse_replicas(args.replicas))
    soak = Soak(cfg)
    summary = {'token': cfg.token, 'started': time.time(), 'kind': 'resource-smoke', 'rate_ops_per_s': args.rate, 'duration_s': args.duration}
    problems = []
    try:
        summary['wheel'] = wheel_facts(args.venv_bin)
        summary['preflight'] = soak.preflight()
        soak.start()
        summary['stack'] = soak.facts
        write(soak, 'stack-facts.json', soak.facts)
        soak.start_supervisor(backoff=3.0)
        fleet = soak.wait_ready(1)
        summary['fleet_ready_s'] = round(time.time() - fleet.started_at, 1)
        soak.positive_control(1)
        for rec in soak.gen.bootstrap_stores():
            soak.publisher.send(rec, window=soak.window)
        soak.publisher.flush(30)
        soak.start_traffic(args.rate)
        began = time.monotonic()
        faults = Faults(soak)
        killed = False
        while time.monotonic() - began < args.duration and not soak.tripped:
            if not killed and time.monotonic() - began > args.duration / 2:
                killed = True
                result = faults.kill('edge-loader', 'PURCHASED')
                summary['edge_kill'] = result
                soak.wait_ready(soak.supervisor.generation)
                soak.positive_control(soak.supervisor.generation)
                soak.new_window('after the edge kill')
            time.sleep(2)
        soak.pause_traffic()
        point = soak.checker.quiet_point('smoke-final')
        summary['quiet_point'] = {k: v for k, v in point.items() if k != 'reports'}
        summary['quiet_point_reports'] = point['reports']
        problems += [] if point['ok'] else ['the final quiet point failed']
        summary['containers'] = soak.container_log
        summary['peaks'] = soak.peaks
        summary['cpusets'] = soak.cpuset_all()
        bad_cpuset = {cid: row for cid, row in summary['cpusets'].items() if row['cpuset'] != soak.cpuset}
        if bad_cpuset or not summary['cpusets']:
            problems.append(f'{len(bad_cpuset)} container(s) without CpusetCpus={soak.cpuset}')
        summary['replacement_cpusets'] = {cid: row for cid, row in summary['cpusets'].items() if row.get('incarnation', 1) > 1}
        summary['budget'] = budget_report(soak)
        problems += [] if summary['budget']['ok'] else ['the memory-budget inequality fails for the largest fleet']
        summary['journal'] = soak.journal.counts()
        summary['generated'] = dict(soak.gen.stats)
        summary['accounting'] = soak.checker.accounting()
        summary['supervisor_events'] = soak.supervisor.events
        summary['log_markers'] = soak.logs.snapshot()['counts']
    except BaseException as error:
        problems.append(f'{type(error).__name__}: {error}')
        summary['traceback'] = traceback.format_exc()
    finally:
        if soak.stack is not None:
            summary['peaks'] = soak.peaks
            summary['harness_peak_rss'] = soak.harness_peak
            summary['querylog'] = soak.querylog.summary()
            summary['controls'] = soak.controls
            summary['sampler'] = {'ticks': soak.sampler.ticks, 'errors': soak.sampler.errors, 'gaps': soak.sampler.gaps}
            summary['guard'] = {'min_mem_available': soak.guard.min_mem, 'min_docker_free': soak.guard.min_disk, 'tripped': soak.tripped}
            problems += judge(soak)
        summary['cleanup'] = soak.close()
        problems += [] if summary['cleanup']['ok'] else ['cleanup receipt is not clean']
        summary['problems'] = problems
        summary['ok'] = not problems
        summary['finished'] = time.time()
        write(soak, 'smoke-summary.json', summary)
    print(json.dumps({'token': cfg.token, 'ok': summary['ok'], 'problems': problems, 'out': str(soak.out_dir)}))
    return 0 if summary['ok'] else 1


def guarded(argv, args):
    """Run the subcommand in a child process under a hard wall-clock limit, then always run the recorded-ID cleanup.

    A crash of the child (the librdkafka SIGSEGV that motivated this) must not leave the owned stack behind: the registry names every
    resource, and ``soak.cleanup`` removes exactly those. After a clean close it finds nothing and is a no-op."""
    import secrets
    from . import cleanup
    import docker
    token = args.token or secrets.token_hex(16)
    child_argv = [a for a in argv if a != '--child']
    if '--token' not in child_argv:
        child_argv += ['--token', token]
    wall = args.wall_limit or (args.duration + 3600)
    started = time.time()
    process = subprocess.Popen([sys.executable, '-X', 'faulthandler', '-m', 'soak.run_soak', *child_argv, '--child'], start_new_session=True)
    try:
        code = process.wait(timeout=wall)
        timed_out = False
    except subprocess.TimeoutExpired:
        timed_out = True
        os.killpg(process.pid, 9)
        code = process.wait(timeout=30)
    run_dir = Path(os.path.expanduser(args.run_root)) / token
    receipt = {'exit_code': code, 'timed_out': timed_out, 'wall_seconds': round(time.time() - started, 1)}
    kept = (run_dir / 'out' / 'KEEP').exists()
    receipt['kept_for_diagnosis'] = kept
    if (run_dir / 'registry.jsonl').exists() and not kept:
        client = docker.DockerClient(base_url='unix:///var/run/docker.sock', timeout=60)
        candidates = set()
        if args.manifest:
            manifest = json.loads(Path(os.path.expanduser(args.manifest)).read_text())
            candidates = {row['id'] for row in manifest['images']['amd64']['rows'].values()}
        found = cleanup.plan(client, run_dir, candidates)
        removed = cleanup.execute(client, found) if any(found[k] for k in ('containers', 'volumes', 'networks')) else {'removed': [], 'failed': []}
        receipt.update(found_after_exit=found, removed_after_exit=removed)
        (run_dir / 'out').mkdir(exist_ok=True)
        (run_dir / 'out' / 'wrapper-receipt.json').write_text(json.dumps(receipt, indent=1, default=str))
        if removed['failed']:
            code = code or 1
    print(json.dumps({'wrapper': receipt['exit_code'], 'timed_out': timed_out, 'cleaned_after_exit': bool(receipt.get('removed_after_exit', {}).get('removed'))}))
    return 1 if timed_out else code


def build(args, kind, duration):
    if getattr(args, 'shape', None):
        from .generator import GenConfig
        from .host_runtime import HostConfig, HostSoak
        cfg = HostConfig(**({'token': args.token} if args.token else {}), shape=args.shape, wheel=args.wheel, wheel_sha256=args.wheel_sha256,
                         service_memory_max=args.service_memory_max, rebalance_delay_ms=args.rebalance_delay_ms, run_root=args.run_root, venv_bin=args.venv_bin, cpuset=args.cpuset,
                         member_wait=args.member_wait, cli_deadline=duration + 3600, stack_deadline=duration + 7200,
                         neo_memory=int(args.neo_mib * 1024 ** 2), kafka_memory=int(args.kafka_mib * 1024 ** 2), neo_heap=args.neo_heap,
                         neo_cache=args.neo_cache, kafka_heap=args.kafka_heap, replicas=parse_replicas(args.replicas),
                         catchup_bound=args.catchup_bound,
                         gen=GenConfig(seed=13, max_customers=args.max_customers, max_products=args.max_products,
                                       max_relationships=args.max_relationships))
        return HostSoak(cfg)
    cfg = SoakConfig(**({'token': args.token} if args.token else {}), run_root=args.run_root, manifest=args.manifest,
                     venv_bin=args.venv_bin, member_wait=args.member_wait, cli_deadline=duration + 3600, stack_deadline=duration + 7200,
                     neo_memory=int(args.neo_mib * 1024 ** 2), kafka_memory=int(args.kafka_mib * 1024 ** 2), neo_heap=args.neo_heap,
                     neo_cache=args.neo_cache, kafka_heap=args.kafka_heap, replicas=parse_replicas(args.replicas),
                     catchup_bound=args.catchup_bound)
    return Soak(cfg)


def finish(soak, summary, problems, name, keep_on_red):
    red = bool(problems)
    keep = keep_on_red and red
    started = soak.stack is not None and hasattr(soak, 'querylog') and hasattr(soak, 'sampler')    # start() may have failed half way
    if soak.stack is not None and not started:
        summary['partial_start'] = True
    if started:
        summary['peaks'] = soak.peaks
        summary['harness_peak_rss'] = soak.harness_peak
        summary['querylog'] = soak.querylog.summary()
        summary['controls'] = soak.controls
        summary['sampler'] = {'ticks': soak.sampler.ticks, 'errors': soak.sampler.errors, 'gaps': soak.sampler.gaps}
        summary['guard'] = {'min_mem_available': soak.guard.min_mem, 'min_docker_free': soak.guard.min_disk, 'tripped': soak.tripped}
        summary['faults'] = soak.fault_log
        summary['steps'] = soak.steps
        summary['supervisor_events'] = soak.supervisor.events if soak.supervisor else []
        summary['unexplained_restarts'] = soak.supervisor.unexplained if soak.supervisor else []
        summary['cpusets'] = soak.cpuset_all()
        summary['log_markers'] = soak.logs.snapshot()
        summary['containers'] = soak.container_log
        problems += judge(soak)
        if getattr(soak, 'shape', None):
            summary['shape'] = soak.shape
            summary['neo4j'] = neo_summary(soak)
            summary['stale_samples'] = soak.stale_samples
            problems += host_judge(soak)
            summary['oom_evidence'] = soak.oom_events()
            if soak.unit is not None:
                summary['unit'] = soak.unit.state()
                summary['unit_memory_peak'] = soak.unit.memory_peak()
    summary['cleanup'] = soak.close(keep_stack=keep)
    if keep:
        (soak.out_dir / 'KEEP').write_text('stack kept for diagnosis; remove with: python -m soak.cleanup ' + str(soak.base) + ' --execute\n')
    problems += [] if summary['cleanup']['ok'] else ['cleanup receipt is not clean']
    summary['problems'] = problems
    summary['ok'] = not problems
    summary['finished'] = time.time()
    write(soak, name, summary)
    print(json.dumps({'token': soak.token, 'ok': summary['ok'], 'problems': problems, 'out': str(soak.out_dir), 'kept': keep}))
    return 0 if summary['ok'] else 1


def run_stream_scenario(args, kind, plan):
    """Shared shell of the S2 runs: stack, baseline + bulk handover, traffic loop, stream with re-read, then ``plan``."""
    duration = args.max_seconds
    soak = build(args, kind, duration)
    summary = {'token': soak.token, 'started': time.time(), 'kind': kind, 'args': {k: v for k, v in vars(args).items() if k not in ('child',)}}
    problems = []
    try:
        summary['wheel'] = wheel_facts(args.venv_bin)
        summary['preflight'] = soak.preflight()
        summary['budget_check'] = soak.check_budget()
        if not summary['budget_check']['ok']:
            raise scenarios.Red('the memory-budget inequality fails for the largest fleet even with reduced replicas: ' + str(summary['budget_check']['attempts']))
        soak.start()
        summary['stack'] = soak.facts
        write(soak, 'stack-facts.json', soak.facts)
        soak.make_loop(args.base_rate)
        summary['handover'] = scenarios.handover(soak, customers=args.customers, products=args.products, relationships=args.relationships)
        summary['reread_point'] = {k: v for k, v in scenarios.open_stream(soak).items() if k != 'reports'}
        write(soak, 'progress.json', {'steps': soak.steps})
        summary['plan'] = plan(soak, args)
    except scenarios.Red as error:
        problems.append(f'RED: {error}')
        summary['traceback'] = traceback.format_exc()
    except BaseException as error:
        problems.append(f'{type(error).__name__}: {error}')
        summary['traceback'] = traceback.format_exc()
    finally:
        try:
            if soak.stack is not None and soak.loop is not None and not problems:
                pass
            if soak.stack is not None:
                summary['accounting'] = soak.checker.accounting() if hasattr(soak, 'checker') else None
                summary['journal'] = soak.journal.counts()
                summary['generated'] = dict(soak.gen.stats)
                summary['quiet_points'] = [{k: v for k, v in q.items() if k != 'reports'} for q in soak.qp_log]
                summary['budget'] = budget_report(soak) if soak.peaks else None
        except Exception as error:
            problems.append(f'summary: {type(error).__name__}: {error}')
        return finish(soak, summary, problems, f'{kind}-summary.json', args.keep_on_red)


def plan_calibrate(soak, args):
    result = scenarios.calibrate(soak, start=args.start_rate, ceiling=args.ceiling, warmup=args.warmup, window=args.window)
    sustainable = result['sustainable_ops_per_s']
    base = max(sustainable // 2, 1)
    result.update(base_rate_ops_per_s=base, burst_multiplier=5, burst_seconds=180,
                  catchup_bound_s=max(2 * 180 + 300, 0))
    return result


def faults_for(soak):
    if getattr(soak, 'shape', None):
        from .host_runtime import HostFaults
        return HostFaults(soak)
    return Faults(soak)


def plan_host_smoke(soak, args):
    """Memory calibration on the host shape: base traffic after the re-read, one edge-loader kill, a quiet point; peaks are the output."""
    soak.loop.rate = args.base_rate
    soak.loop.multiplier = 1.0
    faults = faults_for(soak)
    scenarios.calm_window(soak, args.calm, name='calm')
    scenarios.quiet(soak, 'qp-calm')
    scenarios.calm_window(soak, 60, name='calm')
    result = faults.kill('edge-loader', 'PURCHASED')
    scenarios.record(soak, 'fault', **{k: result[k] for k in ('name', 'ok', 'expected', 'observed', 'problems')})
    if not result['ok']:
        raise scenarios.Red('fault kill-edge-loader: ' + '; '.join(result['problems']))
    soak.wait_ready(soak.supervisor.generation)
    scenarios.quiet(soak, 'qp-kill', resume=False)
    return {'peaks': dict(soak.peaks)}


def plan_compressed(soak, args):
    soak.loop.rate = args.base_rate
    soak.loop.multiplier = 1.0
    faults = faults_for(soak)
    return {'final': {k: v for k, v in scenarios.compressed(soak, faults, calm=args.calm, only=args.only.split(',') if args.only else None,
                                                       skip=set(args.skip.split(',')) if args.skip else set()).items()
                      if k != 'reports'}}


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(prog='python -m soak.run_soak')
    sub = parser.add_subparsers(dest='command', required=True)
    for name in ('smoke', 'calibrate', 'compressed', 'host-smoke', 'host-compressed'):
        p = sub.add_parser(name)
        p.add_argument('--manifest', required=not name.startswith('host-'), default='',
                       help='evidence/E11.F1.S3-manifest.json (frozen wheel hash and final image IDs); not used by the host shapes')
        p.add_argument('--venv-bin', required=True, help='bin directory of the venv holding the frozen wheel')
        p.add_argument('--run-root', default='~/e13-soak')
        p.add_argument('--token', default='', help='owner token (32 hex); generated when omitted')
        p.add_argument('--child', action='store_true', help=argparse.SUPPRESS)
        p.add_argument('--wall-limit', type=float, default=0.0, help='hard wall-clock limit in seconds (default: duration + 1 hour)')
        p.add_argument('--member-wait', type=float, default=60.0, help='seconds the CLI waits for a dead run\'s group members (session timeout 45 s + 5 s)')
        p.add_argument('--neo-mib', type=float, default=1792)
        p.add_argument('--neo-heap', default='512m')
        p.add_argument('--neo-cache', default='640m')
        p.add_argument('--kafka-mib', type=float, default=1024)
        p.add_argument('--kafka-heap', default='512m')
        p.add_argument('--replicas', default='', help='for example Customer=1,PURCHASED=2')
        p.add_argument('--rate', type=float, default=10.0, help='logical operations per second')
        p.add_argument('--duration', type=float, default=300.0)
        if name.startswith('host-'):
            p.add_argument('--shape', choices=('process', 'service'), required=True,
                           help='process: the harness starts and restarts the command; service: a systemd user unit does')
            p.add_argument('--wheel', required=True, help='the wheel the venv was installed from (its sha256 and RECORD are verified)')
            p.add_argument('--wheel-sha256', default='', help='expected sha256 of the wheel')
            p.add_argument('--service-memory-max', default='3G')
            p.add_argument('--cpuset', default='4-11')
            p.add_argument('--rebalance-delay-ms', type=int, default=3000,
                           help="owned Kafka broker group.initial.rebalance.delay.ms (Kafka's default 3000; E13's soak used 0)")
            p.add_argument('--max-customers', type=int, default=150_000)
            p.add_argument('--max-products', type=int, default=15_000)
            p.add_argument('--max-relationships', type=int, default=1_000_000)
        if name != 'smoke':
            p.add_argument('--base-rate', type=float, default=50.0, help='logical operations per second (50%% of the calibrated sustainable rate)')
            p.add_argument('--customers', type=int, default=50_000)
            p.add_argument('--products', type=int, default=10_000)
            p.add_argument('--relationships', type=int, default=300_000)
            p.add_argument('--catchup-bound', type=float, default=900.0)
            p.add_argument('--keep-on-red', action='store_true', help='leave the owned stack running when a criterion fails (python -m soak.cleanup removes it)')
            p.add_argument('--max-seconds', type=float, default=4 * 3600.0)
            p.add_argument('--start-rate', type=float, default=100.0)
            p.add_argument('--ceiling', type=float, default=1600.0)
            p.add_argument('--warmup', type=float, default=300.0)
            p.add_argument('--window', type=float, default=600.0)
            p.add_argument('--calm', type=float, default=90.0)
            p.add_argument('--only', default='', help='comma-separated fault names to run (debugging)')
            p.add_argument('--skip', default='', help='comma-separated of burst,cohort to skip (debugging only)')
    args = parser.parse_args(argv)
    if args.command != 'smoke':
        args.duration = args.max_seconds
    if not args.child:
        return guarded(argv, args)
    import faulthandler
    import signal
    faulthandler.register(signal.SIGUSR1, all_threads=True)   # kill -USR1 <pid> dumps every thread's stack to stderr
    if args.command == 'smoke':
        return smoke(args)
    return run_stream_scenario(args, args.command, {'calibrate': plan_calibrate, 'compressed': plan_compressed, 'host-smoke': plan_host_smoke,
                                                    'host-compressed': plan_compressed}[args.command])


if __name__ == '__main__':
    sys.exit(main())
