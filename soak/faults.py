"""Fault injection on a schedule, each with the outcome the docs promise (INCREMENTAL_LOAD.md B7, B8, B10) and the observations that
decide it. A fault never decides the graph's correctness: the quiet point that follows does. Here the question is only whether the
*fleet* behaved as documented (exit codes, log lines, restarts, gauges).

Verdict helpers are pure so they can be tested without Docker.
"""
from __future__ import annotations

import time

from .sampler import scrape

KILL_MARKERS = {
    'edge-loader': 'exited before drain acknowledgement',
    'retry-consumer': 'retry consumer exited during stream supervision',
    'node-loader': 'node loader exited while the stream run was active',
}
DOCUMENTED_STOP_EXITS = (0, 137)  # B7: a drain abandon exits 0; a blocked connect outlasts Docker's 10 s grace and is killed (137)


# ---- pure verdicts -----------------------------------------------------------------------------------------------------------

def judge_kill(component: str, exit_code, reason_text: str, restarted: bool) -> dict:
    marker = KILL_MARKERS[component]
    problems = []
    if exit_code != 1:
        problems.append(f'fleet exit code {exit_code}, expected 1 (B8)')
    if marker not in reason_text:
        problems.append(f'reason line "{marker}" not found')
    if not restarted:
        problems.append('the supervisor did not restart the fleet')
    return {'ok': not problems, 'problems': problems}


def judge_outage(*, restarts: int, cli_running: bool, outage_lines: int, recovered_lines: int, gauge_peak: float, gauge_after: float) -> dict:
    problems = []
    if restarts:
        problems.append(f'{restarts} fleet restart(s) during an outage the fleet must ride out')
    if not cli_running:
        problems.append('the CLI is not running')
    if outage_lines <= 0:
        problems.append('no stage=write-outage line')
    if recovered_lines <= 0:
        problems.append('no stage=write-outage-recovered line')
    if gauge_peak <= 0:
        problems.append('graph_loader_write_outage_seconds never rose above 0')
    if gauge_after != 0:
        problems.append(f'the gauge is {gauge_after} after recovery, expected 0')
    return {'ok': not problems, 'problems': problems}


def judge_sigterm_outage(cli_exit, container_exits: dict) -> dict:
    problems = []
    if cli_exit != 0:
        problems.append(f'the CLI exited {cli_exit}, expected 0')
    bad = {name: code for name, code in container_exits.items() if code not in DOCUMENTED_STOP_EXITS}
    if bad:
        problems.append(f'containers exited with undocumented codes: {bad}')
    return {'ok': not problems, 'problems': problems}


def committed_not_behind(before: dict, after: dict) -> list:
    """Partitions whose committed offset went backwards (a re-read of committed records)."""
    behind = []
    for key, value in before.items():
        later = after.get(key)
        if value is not None and (later is None or later < value):
            behind.append((key, value, later))
    return behind


# ---- the faults --------------------------------------------------------------------------------------------------------------

class Faults:
    def __init__(self, soak):
        self.soak = soak

    # ---- helpers -----------------------------------------------------------------------------------------------------
    def _begin(self, name):
        window = self.soak.new_window(f'fault:{name}')
        self.soak.journal.flag_window(window, name)
        return {'name': name, 'window': window, 'started': time.time()}

    def _end(self, result, expected, observed, verdict, **extra):
        result.update(expected=expected, observed=observed, ok=verdict['ok'], problems=verdict['problems'], ended=time.time(), **extra)
        self.soak.fault_log.append(result)
        return result

    def _gauge(self, fleet):
        peak = 0.0
        for row in fleet.inspect_all():
            if row['metrics_port'] and row['component'] in ('edge-loader', 'node-loader'):
                try:
                    for name, _labels, value in scrape(row['metrics_port'], timeout=2):
                        if name == 'graph_loader_write_outage_seconds':
                            peak = max(peak, value)
                except Exception:
                    pass
        return peak

    def _wait(self, predicate, timeout, what):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if predicate():
                return True
            time.sleep(1)
        self.soak.notes.append(f'timed out waiting for {what}')
        return False

    def _restarted(self, generation, timeout=420):
        self._wait(lambda: self.soak.supervisor.generation > generation and self.soak.supervisor.ready.is_set(), timeout, 'fleet restart')
        return self.soak.supervisor.generation > generation and self.soak.supervisor.ready.is_set()

    def _pick(self, fleet, component, entity=None):
        for cid, name in fleet.live().get(component, []):
            if entity is None or entity in name:
                return cid, name
        raise RuntimeError(f'no live {component} {entity or ""}')

    # ---- Neo4j -------------------------------------------------------------------------------------------------------
    def neo4j_outage(self, seconds):
        soak = self.soak
        result = self._begin(f'neo4j-outage-{int(seconds)}s')
        sup, logs = soak.supervisor, soak.logs
        generation = sup.generation
        fleet = sup.fleet
        out0, rec0 = logs.total('stage=write-outage '), logs.total('stage=write-outage-recovered')
        soak.stack.stop_neo4j(timeout=30)
        peak, end = 0.0, time.monotonic() + seconds
        while time.monotonic() < end:
            peak = max(peak, self._gauge(fleet))
            time.sleep(5)
        soak.stack.start_neo4j()
        soak.querylog.mark_restart()
        self._wait(lambda: self._gauge(fleet) == 0 and logs.total('stage=write-outage-recovered') > rec0, 240, 'write-outage recovery')
        after = self._gauge(fleet)
        observed = {'restarts': sup.generation - generation, 'cli_running': fleet.cli.process.poll() is None,
                    'outage_lines': logs.total('stage=write-outage ') - out0, 'recovered_lines': logs.total('stage=write-outage-recovered') - rec0,
                    'gauge_peak': peak, 'gauge_after': after}
        return self._end(result, 'fleet stays up; stage=write-outage and recovery lines; gauge rises then returns to 0; no restart', observed,
                         judge_outage(**{k: observed[k] for k in ('restarts', 'cli_running', 'outage_lines', 'recovered_lines')},
                                      gauge_peak=peak, gauge_after=after))

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
        # The supervisor relaunches the same command after its backoff; until Neo4j is back that start fails its preflight (exit 1),
        # which the outage explains and the supervisor retries.
        sup.explain_until('restart attempted while the Neo4j outage lasts', time.time() + 180)
        code = fleet.sigterm(timeout=90)
        soak.stack.start_neo4j()
        soak.querylog.mark_restart()
        restarted = self._restarted(generation)
        events = fleet.observer.snapshot()['events']
        exits = {}
        for event in events:
            if event.get('type') == 'container' and event.get('action') == 'die':
                exits[event['id'][:12]] = int(event['exit_code']) if 'exit_code' in event else None
        abandoned = logs.total('drain-abandoned')
        observed = {'cli_exit': code, 'container_exits': exits, 'drain_abandoned_lines': abandoned, 'restarted': restarted,
                    'stop_seconds': fleet.cli.stop_seconds}
        verdict = judge_sigterm_outage(code, {k: v for k, v in exits.items() if v is not None})
        if not restarted:
            verdict = {'ok': False, 'problems': verdict['problems'] + ['the supervisor did not restart the fleet']}
        return self._end(result, 'CLI exit 0; loaders log *-drain-abandoned reason=neo4j_outage and exit 0 (or 137 after Docker\'s 10 s grace); '
                                 'restart redelivers the held batches', observed, verdict)

    # ---- Kafka -------------------------------------------------------------------------------------------------------
    def kafka_restart(self, down=30):
        """Not defined by the docs: record what happens. Pass = the fleet is back (restarts explained by this fault)."""
        soak = self.soak
        result = self._begin('kafka-broker-restart')
        sup = soak.supervisor
        generation = sup.generation
        sup.explain_until('kafka broker restart', time.time() + down + 400)
        uncertain0 = soak.journal.counts().get('uncertain', 0)
        soak.stack.stop_kafka(timeout=30)
        time.sleep(down)
        soak.stack.start_kafka()
        returned = time.monotonic()

        def settled():
            if not sup.ready.is_set():
                return False
            if sup.generation > generation:
                return True                                # the outage ended the fleet and the next one is ready
            return time.monotonic() - returned > 150       # or it rode the outage out: nothing more to wait for
        self._wait(settled, 420, 'fleet after the Kafka restart')
        sup.end_windows('kafka broker restart')       # a window that outlives the fault would absorb an unrelated exit (see E14.F3.S10)
        observed = {'fleet_exits': [e for e in sup.events if e['incarnation'] >= generation and e.get('cause') == 'kafka broker restart'],
                    'generation_delta': sup.generation - generation, 'new_uncertain_sends': soak.journal.counts().get('uncertain', 0) - uncertain0,
                    'ready': sup.ready.is_set()}
        verdict = {'ok': sup.ready.is_set(), 'problems': [] if sup.ready.is_set() else ['the fleet is not ready after the broker returned']}
        return self._end(result, 'not defined by the docs: record it; the model\'s checks must hold at the next quiet point', observed, verdict)

    # ---- kills -------------------------------------------------------------------------------------------------------
    def kill(self, component, entity=None):
        soak = self.soak
        result = self._begin(f'kill-{component}' + (f'-{entity}' if entity else ''))
        sup = soak.supervisor
        generation, fleet = sup.generation, sup.fleet
        cid, name = self._pick(fleet, component, entity)
        sup.expect(f'kill {component}', exit_codes=(1,))
        killed = fleet.kill_container(cid)
        restarted = self._restarted(generation)
        event = next((e for e in reversed(sup.events) if e['incarnation'] == fleet.incarnation), {})
        text = fleet.cli.text()
        verdict = judge_kill(component, event.get('exit_code'), text, restarted)
        observed = {'container': name, 'kill': killed, 'fleet_exit': event.get('exit_code'), 'reasons': event.get('reasons'),
                    'restarted': restarted}
        return self._end(result, 'fleet exits 1 (B8) naming the loss; the supervisor restarts it', observed, verdict)

    def kill_lease_server(self):
        soak = self.soak
        result = self._begin('kill-lease-server')
        sup = soak.supervisor
        generation, fleet = sup.generation, sup.fleet
        cid, name = self._pick(fleet, 'bucket-lease-server')
        killed = fleet.kill_container(cid)
        replaced = self._wait(lambda: [c for c, _ in fleet.live().get('bucket-lease-server', [])] not in ([], [cid])
                              and fleet.counts().get('bucket-lease-server') == 1, 120, 'a replacement lease server')
        time.sleep(3)
        observed = {'killed': killed, 'replaced_in_place': replaced, 'cli_running': fleet.cli.process.poll() is None,
                    'generation_delta': sup.generation - generation, 'restart_lines': fleet.cli.text().count('stage=lease-server-restart')}
        problems = []
        if not replaced:
            problems.append('no replacement lease server appeared')
        if not observed['cli_running'] or observed['generation_delta']:
            problems.append('the fleet exited; a lease server loss must be a supervised restart in place')
        return self._end(result, 'supervised restart in place, no fleet exit', observed, {'ok': not problems, 'problems': problems})

    def kill9_cli(self):
        soak = self.soak
        result = self._begin('kill-9-cli')
        sup = soak.supervisor
        generation, fleet = sup.generation, sup.fleet
        want = sum(fleet.expected.values())
        before = [cid for items in fleet.live().values() for cid, _ in items]
        sup.expect('kill -9 of the CLI', exit_codes=(-9, None))
        fleet.sigkill_cli()
        time.sleep(5)
        kept = 0
        for cid in before:
            try:
                kept += bool(soak.client.api.inspect_container(cid)['State']['Running'])
            except Exception:
                pass
        restarted = self._restarted(generation)
        recovery = next((e.get('recovery') for e in reversed(sup.events) if e['incarnation'] == fleet.incarnation), None) or {}
        gone = []
        for cid in before:
            try:
                soak.client.api.inspect_container(cid)
            except Exception:
                gone.append(cid)
        observed = {'containers_before': len(before), 'kept_running_after_kill': kept, 'recovery': recovery, 'removed_by_recorded_id': len(gone),
                    'restarted': restarted}
        problems = []
        if kept != want:
            problems.append(f'{kept}/{want} containers kept running after kill -9')
        if len(gone) != len(before):
            problems.append('the narrowed recovery did not remove every old container')
        if recovery.get('unobserved') or recovery.get('leftovers', {}).get('failed'):
            problems.append('recovery left or missed containers')
        if not restarted:
            problems.append('the fleet did not restart')
        return self._end(result, 'containers keep running; narrowed recovery by recorded ID; restart', observed, {'ok': not problems, 'problems': problems})

    def graceful_replica_change(self, entity='PURCHASED', replicas=3):
        soak = self.soak
        result = self._begin(f'graceful-stop-{entity}-replicas-{replicas}')
        sup = soak.supervisor
        generation, fleet = sup.generation, sup.fleet
        soak.replicas[entity] = replicas
        sup.expect('graceful SIGTERM with a replica change', exit_codes=(0,))
        code = fleet.sigterm(timeout=90)
        before = soak.committed_all()
        restarted = self._restarted(generation)
        after = soak.committed_all()
        behind = committed_not_behind(before, after)
        counts = sup.fleet.counts() if restarted else {}
        observed = {'cli_exit': code, 'stop_seconds': fleet.cli.stop_seconds, 'restarted': restarted, 'edge_loaders_after': counts.get('edge-loader'),
                    'partitions_committed_behind': behind[:5]}
        problems = []
        if code != 0:
            problems.append(f'the CLI exited {code}, expected 0')
        if behind:
            problems.append('committed offsets went backwards across the restart (records re-read)')
        if not restarted:
            problems.append('the fleet did not restart')
        return self._end(result, f'exit 0; resumes from committed offsets; {entity} runs {replicas} replicas', observed,
                         {'ok': not problems, 'problems': problems})

    # ---- burst -------------------------------------------------------------------------------------------------------
    def burst(self, multiplier=5.0, seconds=180):
        soak = self.soak
        result = self._begin(f'burst-{multiplier:g}x-{int(seconds)}s')
        loop = soak.loop
        peak, began = 0, time.monotonic()
        loop.multiplier = multiplier
        while time.monotonic() - began < seconds:
            peak = max(peak, soak.total_lag())
            time.sleep(5)
        loop.multiplier = 1.0
        bound = 2 * seconds + 300
        threshold = max(20, int(2 * loop.rate))
        calm = 0
        ended = time.monotonic()
        while time.monotonic() - ended < bound:
            lag = soak.total_lag()
            calm = calm + 1 if lag <= threshold else 0
            if calm >= 3:
                break
            time.sleep(5)
        drained = calm >= 3
        observed = {'lag_peak': peak, 'drain_seconds': round(time.monotonic() - ended, 1), 'bound_seconds': bound, 'lag_threshold': threshold}
        problems = [] if drained else [f'lag did not return within {bound}s']
        return self._end(result, f'lag rises and drains within {bound}s', observed, {'ok': drained, 'problems': problems})
