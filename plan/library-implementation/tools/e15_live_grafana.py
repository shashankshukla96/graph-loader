#!/usr/bin/env python3
"""E15.F2.S12 cell C: the exported dashboards in an owned Grafana whose datasources have uids other than ``prometheus`` (runs on the Docker host).

Needs the stack of ``e15_stack.py`` and the Prometheus of ``e15_live_monitoring.py`` (127.0.0.1:19090, holding data of earlier runs).
Grafana runs with the host network on 127.0.0.1:13000 (so it reaches the loopback Prometheus and PostgreSQL), memory-capped, tmpfs data, an
admin password generated here and never printed. Steps:

``provision``  ``monitoring export --datasource-uid e15-ds-x`` -> Grafana started with TWO Prometheus datasources (``e15-ds-x`` not default,
               ``e15-ds-other`` default and unreachable) + the exported ``grafana/dashboards``: the dashboard is provisioned, its datasource
               variable's ``current`` is ``e15-ds-x`` (read back from ``/api/dashboards/uid``), and every panel query run against THAT uid returns data.
               A second export without ``--datasource-uid`` shows the variable left unset (the default datasource would win).
``api``        the exported JSON imported through ``POST /api/dashboards/import`` with the datasource input set to ``e15-ds-x`` (another dashboard uid):
               the saved JSON has no ``${DS_PROMETHEUS}`` left; every panel query returns data.
``postgres``   a PostgreSQL datasource (uid ``e15-pg-y``) on the owned PostgreSQL; the run-history dashboard imported with its own input;
               every panel returns rows.

A panel that returns nothing is reported with its query; the cell does not guess an explanation.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import secrets
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import docker
import yaml

GRAFANA_IMAGE = os.environ.get('E15_GRAFANA_IMAGE', 'grafana/grafana-oss:11.3.1')
PORT = 13000
PROM_URL = 'http://127.0.0.1:19090'
PROM_UID, PG_UID = 'e15-ds-x', 'e15-pg-y'
OTHER_UID = 'e15-ds-other'          # a second Prometheus datasource, the DEFAULT one, pointing at nothing: a dashboard that follows it shows no data
OTHER_URL = 'http://127.0.0.1:19099'


def log(message):
    print(f'{time.strftime("%H:%M:%S")} {message}', flush=True)


class Lab:
    def __init__(self, workdir: Path):
        self.workdir = workdir
        self.facts = json.loads((workdir / 'stack.json').read_text())
        self.credentials = dict(line.split('=', 1) for line in (workdir / 'credentials.env').read_text().splitlines() if '=' in line)
        self.token = self.facts['token']
        self.client = docker.DockerClient(base_url='unix:///var/run/docker.sock')
        self.base = workdir / 'grafana'
        self.base.mkdir(exist_ok=True)
        self.password = secrets.token_urlsafe(16)
        self.results = self.base / 'results.jsonl'
        self.exe = str(Path(sys.executable).parent / 'graph-loader')

    def record(self, phase, ok, **facts):
        with open(self.results, 'a') as handle:
            handle.write(json.dumps({'phase': phase, 'ok': ok, **facts}, sort_keys=True, default=str) + '\n')
        log(f"{'OK  ' if ok else 'FAIL'} {phase} {json.dumps(facts, default=str)[:600]}")

    def registry(self, name, ident):
        with open(self.workdir / 'registry.jsonl', 'a') as handle:
            handle.write(json.dumps({'kind': 'created', 'owner': self.token, 'at': time.time(), 'resource': 'container', 'name': name, 'id': ident}) + '\n')

    # -- Grafana
    def start_grafana(self, provisioning: Path | None, dashboards: Path | None):
        name = f'e15-{self.token}-grafana'
        volumes = {}
        if provisioning is not None:
            volumes[str(provisioning)] = {'bind': '/etc/grafana/provisioning', 'mode': 'ro'}
        if dashboards is not None:
            volumes[str(dashboards)] = {'bind': '/etc/grafana/dashboards', 'mode': 'ro'}
        env = {'GF_SECURITY_ADMIN_USER': 'admin', 'GF_SECURITY_ADMIN_PASSWORD': self.password, 'GF_SERVER_HTTP_ADDR': '127.0.0.1', 'GF_SERVER_HTTP_PORT': str(PORT),
               'GF_USERS_ALLOW_SIGN_UP': 'false', 'GF_AUTH_ANONYMOUS_ENABLED': 'false', 'GF_ANALYTICS_REPORTING_ENABLED': 'false',
               'GF_ANALYTICS_CHECK_FOR_UPDATES': 'false'}
        container = self.client.containers.create(
            GRAFANA_IMAGE, name=name, network_mode='host', environment=env, labels={'io.graph-loader.e15-owner': self.token, 'role': 'grafana'},
            volumes=volumes, tmpfs={'/var/lib/grafana': 'rw,size=256m,uid=472,gid=0,mode=700'}, mem_limit='384m')
        self.registry(name, container.id)
        container.start()
        for _ in range(90):
            try:
                if self.get('/api/health', auth=False)['database'] == 'ok':
                    return container
            except Exception:
                time.sleep(1)
        raise RuntimeError('Grafana did not become ready')

    def request(self, method, path, body=None, auth=True):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(f'http://127.0.0.1:{PORT}{path}', data=data, method=method, headers={'Content-Type': 'application/json'})
        if auth:
            request.add_header('Authorization', 'Basic ' + base64.b64encode(f'admin:{self.password}'.encode()).decode())
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.loads(response.read().decode() or 'null')
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f'{method} {path} -> {exc.code} {exc.read().decode()[:300]}') from None

    def get(self, path, auth=True):
        return self.request('GET', path, auth=auth)

    # -- panel queries
    def prometheus_state(self):
        def q(expression):
            from urllib.parse import quote
            with urllib.request.urlopen(f'{PROM_URL}/api/v1/query?query={quote(expression)}', timeout=10) as r:
                return json.loads(r.read())['data']['result']
        return q

    def evaluate_panels(self, dashboard: dict, datasource_uid: str, *, executions: list[str], jobs: list[str]):
        """Run every panel target through /api/ds/query like the browser does (variables substituted as 'All'); returns (with_data, empty, errors).
        ``datasource_uid`` is the uid the dashboard's own datasource variable resolved to (callers read it from the saved dashboard)."""
        variables = {'$kind': '.*', '$name': '.*', '$edge_type': '.*', '$run_id': '.*', '$job': '.*', '$execution_id': '(' + '|'.join(executions) + ')' if executions else '.*'}
        now = int(time.time() * 1000)
        with_data, empty, errors, zoomed = [], [], [], []

        def walk(panels):
            for panel in panels:
                yield panel
                yield from walk(panel.get('panels') or [])

        def run(target, expression, datasource_uid, span_ms, interval_ms):
            body = {'queries': [{'refId': target.get('refId', 'A'), 'datasource': {'type': 'prometheus', 'uid': datasource_uid}, 'expr': expression,
                                 'instant': bool(target.get('instant')), 'range': not target.get('instant'), 'intervalMs': interval_ms, 'maxDataPoints': 200}],
                    'from': str(now - span_ms), 'to': str(now)}
            return self.request('POST', '/api/ds/query', body)['results'][target.get('refId', 'A')]

        def count(result):
            return sum(len(frame['data']['values'][0]) if frame['data']['values'] else 0 for frame in result.get('frames', []))
        for panel in walk(dashboard['panels']):
            if panel.get('type') == 'row':
                continue
            for target in panel.get('targets', []):
                expression = target['expr']
                for key, value in sorted(variables.items(), key=lambda kv: -len(kv[0])):
                    expression = expression.replace(key, value)
                expression = expression.replace('$__rate_interval', '1m')
                label = f"{panel['id']} {panel.get('title')} [{target.get('refId')}]"
                try:
                    result = run(target, expression, datasource_uid, 3600_000, 15000)
                except Exception as exc:                        # noqa: BLE001
                    errors.append((label, str(exc)[:200]))
                    continue
                if result.get('error'):
                    errors.append((label, result['error'][:200]))
                    continue
                if count(result):
                    with_data.append(label)
                    continue
                # Named condition: a role that lives for a few seconds (the planned dispatcher of this 3000-relationship fixture lives ~6 s) has fewer
                # samples than one 15 s step of a 1 h range, and a staleness marker follows its last sample. Zoomed to the run (the last 10 min, a 2 s
                # step: Grafana's own step for that range) the same query returns it.
                again = run(target, expression, datasource_uid, 600_000, 2000)
                (zoomed if count(again) else empty).append((label, expression[:140]) if not count(again) else label)
        return with_data, empty, errors, zoomed


def execution_ids(lab: Lab):
    q = lab.prometheus_state()
    return sorted({r['metric']['execution_id'] for r in q('last_over_time(graph_loader_process_up[1h])')})


def resolved_uid(dashboard: dict) -> str | None:
    """What the dashboard's datasource variable selects, read from the saved dashboard (``current.value``)."""
    variable = next(v for v in dashboard['templating']['list'] if v.get('type') == 'datasource')
    return (variable.get('current') or {}).get('value')


def phase_provision(lab: Lab):
    out = lab.base / 'export'
    if out.exists():
        subprocess.run(['rm', '-rf', str(out)], check=True)
    command = [lab.exe, 'monitoring', 'export', '--out', str(out), '--discovery', 'file', '--prometheus-url', PROM_URL, '--scrape-interval', '5s']
    subprocess.run(command + ['--datasource-uid', PROM_UID], check=True, capture_output=True)
    nouid = lab.base / 'export-nouid'
    if nouid.exists():
        subprocess.run(['rm', '-rf', str(nouid)], check=True)
    hint = subprocess.run(command[:4] + [str(nouid)] + command[5:], check=True, capture_output=True, text=True)
    # two Prometheus datasources: the exported one (not default) and an unreachable default one
    datasource_file = out / 'grafana' / 'provisioning' / 'datasources' / 'prometheus.yml'
    document = yaml.safe_load(datasource_file.read_text())
    document['datasources'][0]['isDefault'] = False
    document['datasources'].append({'name': 'Prometheus (other)', 'uid': OTHER_UID, 'type': 'prometheus', 'access': 'proxy', 'url': OTHER_URL,
                                    'isDefault': True, 'editable': False})
    datasource_file.write_text(yaml.safe_dump(document, sort_keys=False))
    for path in [out, *out.rglob('*')]:
        os.chmod(path, 0o755 if path.is_dir() else 0o644)
    provisioning, dashboards = out / 'grafana' / 'provisioning', out / 'grafana' / 'dashboards'
    lab.start_grafana(provisioning, dashboards)
    time.sleep(8)
    sources = lab.get('/api/datasources')
    dashboard = lab.get('/api/dashboards/uid/graph-loader-overview')
    variables = {v['name']: v for v in dashboard['dashboard']['templating']['list']}
    uid = resolved_uid(dashboard['dashboard'])
    ids = execution_ids(lab)
    with_data, empty, errors, zoomed = lab.evaluate_panels(dashboard['dashboard'], uid or PROM_UID, executions=ids, jobs=['graph-loader'])
    # control: the same panels against the default (other) datasource do not return data
    _, other_empty, other_errors, _ = lab.evaluate_panels(dashboard['dashboard'], OTHER_UID, executions=ids, jobs=['graph-loader'])
    nouid_dashboard = json.loads((nouid / 'grafana' / 'dashboards' / 'graph-loader-overview.json').read_text())
    default_uid = next(s['uid'] for s in sources if s['isDefault'])
    lab.record('provisioned-dashboard-two-datasources', uid == PROM_UID and default_uid == OTHER_UID and not errors and not empty,
               datasources=[(s['name'], s['uid'], s['isDefault']) for s in sources], provisioned=dashboard['meta'].get('provisioned'),
               dashboard_uses_uid=uid, default_datasource=default_uid, datasource_variable_current=variables['datasource'].get('current'),
               inputs_in_dashboard='__inputs' in dashboard['dashboard'], panels_with_data=len(with_data), panels_with_data_when_zoomed=zoomed, empty=empty, errors=errors, executions=len(ids),
               control_default_datasource_empty_or_error=len(other_empty) + len(other_errors), control_expected=len(with_data) + len(empty) + len(errors),
               export_without_uid_current=resolved_uid(nouid_dashboard), export_without_uid_hint=[line for line in hint.stderr.splitlines() if 'datasource-uid' in line][:1])
    (lab.base / 'provisioned-dashboard.json').write_text(json.dumps(dashboard['dashboard'], indent=1))


def phase_api(lab: Lab):
    export = lab.base / 'export' / 'grafana' / 'import' / 'graph-loader-overview.json'
    document = json.loads(export.read_text())
    document['uid'], document['title'] = 'graph-loader-overview-api', 'graph-loader overview (API import)'
    document.pop('id', None)
    result = lab.request('POST', '/api/dashboards/import', {'dashboard': document, 'overwrite': True, 'folderId': 0,
                                                              'inputs': [{'name': 'DS_PROMETHEUS', 'type': 'datasource', 'pluginId': 'prometheus', 'value': PROM_UID}]})
    saved = lab.get(f"/api/dashboards/uid/{result['uid']}")['dashboard']
    text = json.dumps(saved)
    ids = execution_ids(lab)
    uid = resolved_uid(saved)
    with_data, empty, errors, zoomed = lab.evaluate_panels(saved, uid or PROM_UID, executions=ids, jobs=['graph-loader'])
    variables = {v['name']: v for v in saved['templating']['list']}
    lab.record('api-import', '${DS_PROMETHEUS}' not in text and uid == PROM_UID and not errors and not empty,
               placeholder_left='${DS_PROMETHEUS}' in text, datasource_variable_current=variables['datasource'].get('current'), panels_with_data=len(with_data),
               panels_with_data_when_zoomed=zoomed, empty=empty, errors=errors)


def phase_postgres(lab: Lab):
    url = lab.credentials['GRAPH_LOADER_STATUS_DB_URL']
    match = re.match(r'postgresql\+psycopg://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)', url)
    user, password, host, port, database = match.groups()
    lab.request('POST', '/api/datasources', {
        'name': 'Run status (PostgreSQL)', 'uid': PG_UID, 'type': 'grafana-postgresql-datasource', 'access': 'proxy', 'url': f'{host}:{port}', 'user': user,
        'jsonData': {'database': database, 'sslmode': 'disable', 'postgresVersion': 1700, 'timescaledb': False}, 'secureJsonData': {'password': password}})
    export = lab.base / 'export' / 'grafana' / 'dashboards-sql' / 'graph-loader-run-history.json'
    document = json.loads(export.read_text())
    document.pop('id', None)
    result = lab.request('POST', '/api/dashboards/import', {'dashboard': document, 'overwrite': True, 'folderId': 0,
                                                              'inputs': [{'name': 'DS_POSTGRES', 'type': 'datasource', 'pluginId': 'grafana-postgresql-datasource', 'value': PG_UID}]})
    saved = lab.get(f"/api/dashboards/uid/{result['uid']}")['dashboard']
    text = json.dumps(saved)
    now = int(time.time() * 1000)
    # the execution variable's query first (like the browser), then every panel
    rows = {}
    with_data, empty, errors = [], [], []
    schema = 'graph_loader'

    def sql(raw, ref='A', fmt='table'):
        raw = raw.replace('$schema', schema)
        body = {'queries': [{'refId': ref, 'datasource': {'type': 'grafana-postgresql-datasource', 'uid': PG_UID}, 'rawSql': raw, 'format': fmt, 'rawQuery': True,
                             'editorMode': 'code'}], 'from': str(now - 7 * 86400_000), 'to': str(now)}
        return lab.request('POST', '/api/ds/query', body)['results'][ref]
    variable = next(v for v in saved['templating']['list'] if v['name'] == 'execution_id')
    first = sql(variable['query'])
    execution = first['frames'][0]['data']['values'][0][0] if first.get('frames') and first['frames'][0]['data']['values'] and first['frames'][0]['data']['values'][0] else None
    rows['variable execution_id'] = len(first['frames'][0]['data']['values'][0]) if first.get('frames') and first['frames'][0]['data']['values'] else 0
    for panel in saved['panels']:
        if panel.get('type') == 'row':
            continue
        for target in panel['targets']:
            label = f"{panel['id']} {panel['title']}"
            try:
                result = sql(target['rawSql'].replace('$execution_id', execution or ''), fmt=target.get('format', 'table'))
            except Exception as exc:                            # noqa: BLE001
                errors.append((label, str(exc)[:200]))
                continue
            if result.get('error'):
                errors.append((label, result['error'][:200]))
                continue
            count = sum(len(f['data']['values'][0]) if f['data']['values'] else 0 for f in result.get('frames', []))
            (with_data if count else empty).append(label)
            rows[label] = count
    lab.record('postgres-run-history', '${DS_POSTGRES}' not in text and PG_UID in text and not errors and not empty, placeholder_left='${DS_POSTGRES}' in text,
               rows=rows, empty=empty, errors=errors)


PHASES = {'provision': phase_provision, 'api': phase_api, 'postgres': phase_postgres}


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('workdir', type=Path)
    parser.add_argument('--phases', default='provision,api,postgres')
    args = parser.parse_args()
    lab = Lab(args.workdir.resolve())
    for name in args.phases.split(','):
        log(f'=== {name}')
        try:
            PHASES[name](lab)
        except Exception as exc:                                  # noqa: BLE001
            import traceback
            lab.record(f'phase-{name}', False, error=f'{type(exc).__name__}: {exc}', trace=traceback.format_exc()[-700:])
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
