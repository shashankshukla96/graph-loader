"""Installed public-main preflight/dispatch and real launcher argument contract.

Controlled Docker client records creates; no services/ingestion or worker is started.
"""
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
from types import SimpleNamespace


def forbidden(*args, **kwargs):
    raise AssertionError('unexpected service access')


socket.socket.connect = socket.socket.connect_ex = socket.socket.bind = forbidden
subprocess.Popen = forbidden
from neo4j_graph_loader import cli
from neo4j_graph_loader.orchestrator.docker_service import DockerService

records = []
roots = []
environment = dict(os.environ)


def dispatch(args):
    runtime = args.runtime
    roots.append(runtime.root)
    assert runtime.schema.is_file() and args.config == str(runtime.schema)
    assert runtime.root.stat().st_mode & 0o777 == 0o700
    assert args.plan_dir == str(runtime.plans) and args.plan_spill_dir == str(runtime.spill)
    class Client:
        def __init__(self):
            self.containers = self
            self.calls = []
        def run(self, **kwargs):
            self.calls.append(kwargs)
            return SimpleNamespace(id=str(len(self.calls)), name=kwargs['name'])
    client = Client()
    service = DockerService(client=client, runtime=runtime)
    plan = runtime.plans/'edge-plan.json'
    plan.write_text('{}')
    service.run_node_loader(node_label='P', topic='p', mode='bulk', config_path=args.config, run_id='owned')
    service.run_edge_loader(edge_type='E', topic='e', mode='bulk', config_path=args.config, run_id='owned')
    service.run_retry_consumer(run_id='owned', config_path=args.config, lease_address='lease:1', dlq_topics=['e.dlq'])
    service.run_global_batch_clock(run_id='owned', config_path=args.config, fleet_edge_types='E', coordination_topic='clock')
    service.run_bucket_lease_server(run_id='owned', bucket_count=4)
    service.run_planned_dispatcher(run_id='owned', plan_path=str(plan))
    service.run_planned_edge_writers(run_id='owned', plan_path=str(plan), config_path=args.config,
                                    dispatcher_address='dispatcher:1', writers=1)
    assert len(client.calls) == 7
    for call in client.calls:
        for source, target in call['volumes'].items():
            assert Path(source).is_absolute() and Path(source).is_relative_to(runtime.root), source
            assert target['mode'] == ('ro' if target['bind'] in ('/app/runtime_schema.yaml', '/app/edge-plan.json') else 'rw')
        assert call['volumes'][str(runtime.timings)] == {'bind': '/app/write-timings', 'mode': 'rw'}
        assert call['environment']['GRAPH_LOADER_METRICS_SNAPSHOT_PATH'].startswith('/app/write-timings/')
        if call['labels']['component'] in ('node-loader', 'edge-loader', 'retry-consumer'):
            assert call['volumes'][str(runtime.rejections)] == {'bind': '/app/rejections', 'mode': 'rw'}
            assert call['environment']['REJECTION_LOG_PATH'].startswith('/app/rejections/')
    for directory in (runtime.rejections, runtime.plans, runtime.spill, runtime.timings):
        (directory/'owned-proof').write_text('writable')
    records.append({'roles': 7, 'root': str(runtime.root), 'absolute_mounts': True})
    return 0


cli.handle_start = dispatch
with tempfile.TemporaryDirectory(prefix='graph-loader-runtime-proof-') as temporary:
    base = Path(temporary)
    cwd = base/'cwd'
    cwd.mkdir(); cwd.chmod(0o555)
    previous = Path.cwd()
    try:
        os.chdir(cwd)
        assert cli.main(['start', '--mode', 'bulk', '--run-id', 'same', '--run-dir', str(base/'runs')]) == 0
        assert cli.main(['start', '--mode', 'bulk', '--run-id', 'same']) == 0
        assert roots[0] != roots[1]
        assert list(cwd.iterdir()) == []
        # A file in a directory slot fails before the controlled dispatcher.
        blocker = base/'blocker'; blocker.write_text('retain')
        assert cli.main(['start', '--mode', 'bulk', '--run-dir', str(blocker)]) == 1
        assert blocker.read_text() == 'retain' and len(records) == 2
        assert cli.main(['start', '--mode', 'bulk', '--bulk-timeout-seconds', '0']) == 1
        assert len(records) == 2 and dict(os.environ) == environment
        if os.statvfs('/').f_flag & os.ST_RDONLY:
            assert cli.main(['start', '--mode', 'bulk', '--run-dir', '/']) == 1
            assert len(records) == 2
        # Native non-root cwd permissions; immutable rootfs checked above.
        if os.geteuid() != 0 or os.statvfs(cwd).f_flag & os.ST_RDONLY:
            assert cli.main(['start', '--mode', 'bulk', '--run-dir', str(cwd/'denied')]) == 1
            assert len(records) == 2
    finally:
        os.chdir(previous)
        cwd.chmod(0o700)
        for root in roots:
            # Only these paths from controlled dispatch, with no live owners, are cleaned.
            if not root.is_relative_to(base):
                shutil.rmtree(root)
print(json.dumps({'runs': records, 'environment_unchanged': True, 'preflight_failures': True}))
