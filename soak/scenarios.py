"""The S2 scenarios built from the runtime: bulk handover, calm windows, quiet points, the late-endpoint cohort, calibration.

Every step records what it did in ``soak.steps`` (a list of dicts, in order) so the run summary tells the story without logs.
"""
from __future__ import annotations

import itertools
import time

from tests.installed_service.live_cli import AppObserver, cli_environment, run_cli
from pathlib import Path

from . import schema as schema_mod
from . import spec
from .checker import QuietPointFailed, compare_all


class Red(RuntimeError):
    """A pass criterion failed: the run stops (and, with --keep-on-red, leaves the stack for diagnosis)."""


def record(soak, kind, **values):
    row = {'kind': kind, 'at': time.time(), 'window': soak.window, **values}
    soak.steps.append(row)
    return row


def guard_check(soak):
    if soak.tripped:
        raise Red(f'guard tripped: {soak.tripped}')
    if soak.supervisor is not None and soak.supervisor.failed:
        raise Red('supervisor failed: ' + soak.supervisor.failed)


def sleep_checked(soak, seconds, step=1.0):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        guard_check(soak)
        time.sleep(min(step, max(end - time.monotonic(), 0)))


# ---- bulk handover ------------------------------------------------------------------------------------------------------------

def handover(soak, *, customers=50_000, products=10_000, relationships=300_000, timeout=3600):
    """Generate the baseline (all endpoints first), load it with ``graph-loader start --mode bulk``, check it exactly."""
    began = time.time()
    soak.window = 0
    soak.journal.open_window(0, True, 'baseline')
    sent = 0
    source = iter(soak.gen.baseline(customers=customers, products=products, relationships=relationships))
    while True:
        with soak.journal.transaction():          # generate a chunk in one transaction; sends commit on their own (the sampler needs the journal too)
            chunk = list(itertools.islice(source, 2000))
        if not chunk:
            break
        for rec in chunk:
            soak.publisher.send(rec, window=0)
        sent += len(chunk)
        soak.publisher.poll(0)
        guard_check(soak)
    flushed = soak.publisher.flush(300)
    publish_s = round(time.time() - began, 1)
    soak.oracle.refresh()
    expected = soak.oracle.counts()
    # The bulk run: the same schema and consumer_group_id; node groups commit under <id>-<Label> and the stream continues from them.
    path = soak.run_dir / 'schema-bulk.yaml'
    path.write_text(schema_mod.render(soak.token, replicas=soak.replicas, mode='bulk'))
    started = time.time()
    problems = []
    if getattr(soak, 'shape', None):
        # Host shapes: the bulk handover is a process-runtime run too (planned edges, bounded planner), no container image involved.
        from . import host
        env = host.child_env(soak.stack, {'GRAPH_LOADER_METRICS_ENABLED': '1'})
        result = run_cli('taskset', ['-c', soak.cpuset, str(Path(soak.config['venv_bin']) / 'graph-loader'), 'start', '--mode', 'bulk',
                                     '--config', str(path), '--runtime', 'process', '--run-dir', str(soak.run_dir / 'bulk-runs'),
                                     '--run-id', soak.run_id + 'bulk', '--plan-workers', '2', '--plan-memory-mb', '768'],
                         env=env, cwd=soak.run_dir, log_dir=soak.log_dir, name='bulk-handover', timeout=timeout, redact=[soak.stack.password])
        bulk_s = round(time.time() - started, 1)
        orphans = []
        for registry in (soak.run_dir / 'bulk-runs').glob('*/registry.jsonl'):
            for state in host.registry_states(registry.parent):
                if host.is_alive(state):
                    orphans.append(state['name'])
                    host.signal_state(state, 9)
        leftovers = {'removed': orphans, 'failed': []}
        if orphans:
            problems.append(f'the bulk run left processes behind: {orphans}')
    else:
        observer = AppObserver(soak.client, soak.registry, soak.stack.network.id, owner=soak.stack.owner,
                               candidate_images={row['id'] for row in soak.config['images'].values()}, barrier_tag='bulk-')
        observer.start()
        env = cli_environment(soak.stack, soak.config['images'], soak.config['docker_host'],
                              extra={'GRAPH_LOADER_METRICS_ENABLED': '1', 'LOADER_CPUSET': soak.cpuset})
        result = run_cli(Path(soak.config['venv_bin']) / 'graph-loader',
                         ['start', '--mode', 'bulk', '--config', str(path), '--network', soak.stack.network.name, '--skip-image-build',
                          '--run-dir', str(soak.run_dir / 'bulk-runs'), '--run-id', soak.run_id + 'bulk'],
                         env=env, cwd=soak.run_dir, log_dir=soak.log_dir, name='bulk-handover', timeout=timeout, redact=[soak.stack.password])
        bulk_s = round(time.time() - started, 1)
        try:
            observer.barrier()
            observer.drain(timeout=120)
        except Exception as error:
            problems.append(f'{type(error).__name__}: {error}')
        finally:
            observer.stop()
        leftovers = observer.reap()
    reports = compare_all(soak.oracle, soak.graph)
    exact = all(r.ok for r in reports.values())
    row = record(soak, 'handover', customers=customers, products=products, relationships=relationships, records_sent=sent,
                 flushed=flushed, publish_seconds=publish_s, bulk_exit=result.exit_code, bulk_seconds=bulk_s, bulk_timed_out=result.timed_out,
                 bulk_tail=result.tail[-600:], leftovers=leftovers, observer_problems=problems, expected=expected, exact=exact,
                 reports={k: v.to_json() for k, v in reports.items()})
    if result.exit_code != 0 or not exact or problems or leftovers['failed'] or not flushed:
        raise Red(f'handover failed: bulk exit {result.exit_code}, exact={exact}, problems={problems}')
    return row


def open_stream(soak, *, reread_timeout=3600):
    """Start the supervised stream on the same group id; accept the one-time re-read of the relationship topics; first quiet point exact."""
    began = time.time()
    soak.start_supervisor(backoff=5.0)
    soak.wait_ready(1)
    soak.ensure_control()
    # no new traffic yet: only the re-read of the baseline's relationship topics is in flight
    end = time.monotonic() + reread_timeout
    calm = 0
    lags = []
    while time.monotonic() < end:
        guard_check(soak)
        lag = soak.total_lag()
        lags.append(lag)
        calm = calm + 1 if lag == 0 else 0
        if calm >= 3:
            break
        time.sleep(10)
    else:
        raise Red('the re-read did not drain within its deadline')
    reread_s = round(time.time() - began, 1)
    soak.new_window('after the re-read')
    point = quiet(soak, 'after-reread', resume=False)
    record(soak, 'reread', seconds=reread_s, peak_lag=max(lags, default=0), exact=point['ok'])
    return point


# ---- windows and quiet points -------------------------------------------------------------------------------------------------

def calm_window(soak, seconds, *, null_patch=200, name='calm'):
    """A span without fault, restart or replay. It opens with its null-patch cohort and runs base traffic."""
    window = soak.new_window(name)
    records = soak.gen.null_patch_cohort(null_patch, f'{name}-{window}') if null_patch else []
    for rec in records:
        soak.publisher.send(rec, window=window)
    soak.publisher.poll(0)
    row = record(soak, 'calm-window', seconds=seconds, null_patch_pairs=len(records) // 2)
    soak.loop.resume()
    sleep_checked(soak, seconds)
    return row


def quiet(soak, name, *, resume=True):
    """Pause the generator, run a quiet point, record it; the next window is calm. Raises Red if it fails."""
    soak.pause_traffic()
    soak.ensure_control()
    try:
        point = soak.checker.quiet_point(name)
    except QuietPointFailed as error:
        record(soak, 'quiet-point', name=name, ok=False, failure=error.reason, detail=str(error.detail)[:800])
        raise Red(f'quiet point {name} failed: {error}')
    soak.qp_log.append(point)
    record(soak, 'quiet-point', name=name, ok=point['ok'], seconds=point['seconds'], pending=point['pending'],
           inconclusive=point['inconclusive'], null_patch=point['null_patch'],
           bad=[k for k, v in point['reports'].items() if not v['ok']], harness_failures=point['harness_failures'])
    soak.journal.set_meta('durable_customers', soak.gen.customers - len(soak.gen.pending_cust))
    soak.new_window(f'calm after {name}')
    if resume:
        soak.resume_traffic()
    if not point['ok']:
        raise Red(f'quiet point {name} is not exact: ' + ', '.join(k for k, v in point['reports'].items() if not v['ok'])
                  + (' | ' + '; '.join(point['harness_failures']) if point['harness_failures'] else ''))
    return point


# ---- the late-endpoint cohort --------------------------------------------------------------------------------------------------

def late_cohort(soak, *, n=200, tag='late-1'):
    """N new PURCHASED pairs over withheld customers: every rid parks, then the customers arrive and a PURCHASED checkpoint applies them."""
    window = soak.new_window(f'calm:{tag}')
    rels, nodes = soak.gen.cohort(n, tag)
    for rec in rels:
        soak.publisher.send(rec, window=window)
    soak.publisher.flush(60)
    soak.loop.resume()
    began = time.time()
    deadline = time.monotonic() + 3 * spec.RETRY_WINDOW_S
    parked = 0
    while time.monotonic() < deadline:
        guard_check(soak)
        soak.checker.sync_dlq()
        parked = soak.journal.one("SELECT count(DISTINCT p.rid) FROM parked p JOIN records r ON r.rid = p.rid WHERE r.cohort = ?", (tag,))[0]
        if parked >= n:
            break
        time.sleep(15)
    barrier_s = round(time.time() - began, 1)
    if parked < n:
        record(soak, 'cohort', tag=tag, n=n, parked=parked, ok=False)
        raise Red(f'only {parked}/{n} cohort records parked within {3 * spec.RETRY_WINDOW_S}s')
    for rec in soak.gen.release_cohort(nodes):
        soak.publisher.send(rec, window=window)
    soak.pause_traffic()
    result = soak.checker.checkpoint({'PURCHASED'}, name=f'checkpoint-{tag}')
    # every cohort identity must now hold its record
    missing = []
    for rel_rec in rels:
        entry = soak.oracle.entry(rel_rec.identity)
        if entry is None:
            missing.append(rel_rec.rid)
    soak.qp_log.append(result)
    row = record(soak, 'cohort', tag=tag, n=n, parked=parked, barrier_seconds=barrier_s, checkpoint_ok=result.get('ok'),
                 reason=result.get('reason'), reparked=result.get('reparked'), journaled_replays=result.get('journaled_replays'),
                 bad=[k for k, v in (result.get('reports') or {}).items() if not v['ok']], not_in_model=len(missing))
    soak.journal.set_meta('durable_customers', soak.gen.customers - len(soak.gen.pending_cust))
    soak.new_window(f'calm after {tag}')
    soak.resume_traffic()
    if not result.get('ok') or missing:
        raise Red(f'cohort checkpoint {tag} failed: {result.get("reason") or row["bad"]}')
    return row


# ---- calibration --------------------------------------------------------------------------------------------------------------

def slope(points):
    """Least-squares slope of (t, y) in y-units per second."""
    n = len(points)
    if n < 2:
        return 0.0
    mt = sum(t for t, _ in points) / n
    my = sum(y for _, y in points) / n
    den = sum((t - mt) ** 2 for t, _ in points)
    return sum((t - mt) * (y - my) for t, y in points) / den if den else 0.0


def judge_flat(points, records_per_s):
    """Flat = the lag trend is under 2% of the arrival rate and the final lag is under 10 s of traffic."""
    trend = slope(points)
    final = points[-1][1] if points else 0
    return {'slope_per_s': round(trend, 3), 'final_lag': final, 'limit_slope': round(0.02 * records_per_s, 3),
            'limit_final': round(10 * records_per_s, 1), 'flat': trend <= 0.02 * records_per_s and final <= 10 * records_per_s}


def measure_step(soak, rate, *, warmup=300, window=600, sample=15):
    soak.loop.rate = rate
    soak.loop.multiplier = 1.0
    soak.new_window(f'calibration:{rate}')
    soak.loop.resume()
    sleep_checked(soak, warmup)
    sent0, t0 = soak.loop.sent, time.monotonic()
    points = []
    while time.monotonic() - t0 < window:
        guard_check(soak)
        points.append((round(time.monotonic() - t0, 1), soak.total_lag()))
        time.sleep(sample)
    records_per_s = (soak.loop.sent - sent0) / (time.monotonic() - t0)
    verdict = judge_flat(points, records_per_s)
    row = record(soak, 'calibration-step', offered_ops_per_s=rate, records_per_s=round(records_per_s, 1), **verdict,
                 lag_series=points[::4], dlq=soak.dlq_backlog())
    return row


def calibrate(soak, *, start=100, ceiling=1600, warmup=300, window=600):
    """Double the offered rate until the lag is no longer flat; the sustainable rate is the highest flat one."""
    rate, best, steps = start, None, []
    while rate <= ceiling:
        row = measure_step(soak, rate, warmup=warmup, window=window)
        steps.append(row)
        if row['flat']:
            best = row
            rate *= 2
        else:
            if best is None and rate > 6:
                rate = rate / 2           # not even the first step is sustainable: go down
                continue
            break
        quiet(soak, f'calibration-{row["offered_ops_per_s"]}', resume=False)
    soak.loop.pause()
    if best is None:
        raise Red('no calibration step was flat')
    return {'steps': [{k: v for k, v in s.items() if k != 'lag_series'} for s in steps],
            'sustainable_ops_per_s': best['offered_ops_per_s'], 'sustainable_records_per_s': best['records_per_s']}


# ---- the compressed run -------------------------------------------------------------------------------------------------------

def fault_plan(faults):
    """(name, callable) in schedule order: every fault of the epic once, each followed by a quiet point."""
    return [
        ('neo4j-outage-30s', lambda: faults.neo4j_outage(30)),
        ('sigterm-during-neo4j-outage', faults.sigterm_during_outage),
        ('kafka-broker-restart', lambda: faults.kafka_restart(30)),
        ('kill-edge-loader', lambda: faults.kill('edge-loader', 'PURCHASED')),
        ('kill-retry-consumer', lambda: faults.kill('retry-consumer')),
        ('kill-node-loader', lambda: faults.kill('node-loader', 'Customer')),
        ('kill-lease-server', faults.kill_lease_server),
        ('graceful-stop-replica-change', lambda: faults.graceful_replica_change('PURCHASED', faults.soak.replicas['PURCHASED'] + 1)),
        ('kill-9-cli', faults.kill9_cli),
    ]


def compressed(soak, faults, *, calm=90, burst_seconds=180, only=None, skip=frozenset()):
    """Calm window and a quiet point, one burst, one late-endpoint cohort, then every fault with a quiet point after it, then the
    final all-type reconciliation checkpoint. Stops (Red) at the first failed criterion."""
    calm_window(soak, calm, name='calm')
    quiet(soak, 'qp-calm')
    if 'burst' not in skip:
        burst = faults.burst(5.0, burst_seconds)
        record(soak, 'burst', **{k: burst[k] for k in ('name', 'ok', 'observed', 'problems')})
        if not burst['ok']:
            raise Red('burst: ' + '; '.join(burst['problems']))
        quiet(soak, 'qp-burst')
    if 'cohort' not in skip:
        late_cohort(soak, n=200, tag='late-1')
        quiet(soak, 'qp-cohort')
    for name, action in fault_plan(faults):
        if only and name not in only:
            continue
        calm_window(soak, 60, name='calm')
        result = action()
        record(soak, 'fault', **{k: result[k] for k in ('name', 'ok', 'expected', 'observed', 'problems')})
        if not result['ok']:
            raise Red(f'fault {name}: ' + '; '.join(result['problems']))
        soak.wait_ready(soak.supervisor.generation)
        quiet(soak, f'qp-{name}')
    soak.pause_traffic()
    soak.ensure_control()
    final = soak.checker.checkpoint(set(spec.RELS), name='final-checkpoint')
    soak.qp_log.append(final)
    accounting = soak.checker.accounting()
    record(soak, 'final-checkpoint', ok=final.get('ok'), reason=final.get('reason'), pending=final.get('pending'),
           bad=[k for k, v in (final.get('reports') or {}).items() if not v['ok']], accounting=accounting,
           null_patch=final.get('null_patch'), replays=final.get('replays'))
    if not final.get('ok') or not accounting['ok']:
        raise Red('the final checkpoint failed: ' + str(final.get('reason') or accounting))
    return final
