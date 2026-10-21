"""Public CLI runtime ownership/preflight and every container-role path contract."""
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from neo4j_graph_loader import cli
from neo4j_graph_loader.orchestrator.docker_service import DockerService
from neo4j_graph_loader.orchestrator.planned_edges import start_kafka_edge_planner
from neo4j_graph_loader.utils import metrics
from neo4j_graph_loader.utils.schema_loader import SchemaLoadError, load_schema

ROOT=Path(__file__).resolve().parents[1]


def capture_start(monkeypatch, arguments):
    captured=[]
    monkeypatch.setattr(cli,'handle_start',lambda args: captured.append(args) or 0)
    assert cli.main(['start','--mode','bulk',*arguments]) == 0
    return captured[0]


def test_public_runs_isolate_same_identity_without_cwd_or_environment_mutation(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv('GRAPH_LOADER_TIMING_DIR',raising=False)
    before=dict(os.environ)
    base=tmp_path/'runs'
    a=capture_start(monkeypatch,['--run-dir',str(base),'--run-id','same'])
    b=capture_start(monkeypatch,['--run-dir',str(base),'--run-id','same'])
    assert a.runtime.root != b.runtime.root
    for args in (a,b):
        runtime=args.runtime
        assert runtime.root.parent == base
        assert runtime.root.stat().st_mode & 0o777 == 0o700
        assert runtime.schema.read_bytes() == (ROOT/'config/graph_schema.yaml').read_bytes()
        assert args.config == str(runtime.schema)
        assert args.plan_dir == str(runtime.plans)
        assert args.plan_spill_dir == str(runtime.spill)
        for path in (runtime.schema,runtime.rejections,runtime.plans,runtime.spill,runtime.timings):
            assert path.is_relative_to(runtime.root)
    assert set(tmp_path.iterdir()) == {base}
    assert dict(os.environ) == before


def test_explicit_paths_preserve_inputs_files_locks_modes_and_override_priority(tmp_path, monkeypatch):
    source=tmp_path/'caller.yaml';source.write_bytes((ROOT/'examples/social/graph_schema.yaml').read_bytes())
    source.chmod(0o444)
    plans=tmp_path/'explicit-plans';plans.mkdir(mode=0o750)
    lock=plans/'edge-plan.json.lock';lock.write_text('foreign lock')
    oldplan=plans/'edge-plan.json';oldplan.write_text('previous plan')
    spill=tmp_path/'explicit-spill';timings=tmp_path/'explicit-timings'
    monkeypatch.setenv('GRAPH_LOADER_TIMING_DIR',str(timings))
    args=capture_start(monkeypatch,['--config',str(source),'--run-dir',str(tmp_path/'runs'),
        '--plan-dir',str(plans),'--plan-spill-dir',str(spill)])
    assert args.runtime.plans == plans
    assert args.runtime.spill == spill
    assert args.runtime.timings == timings
    assert plans.stat().st_mode & 0o777 == 0o750
    assert lock.read_text() == 'foreign lock' and oldplan.read_text() == 'previous plan'
    assert source.stat().st_mode & 0o777 == 0o444
    assert args.runtime.schema.read_bytes() == source.read_bytes()


@pytest.mark.parametrize('argument', ['--run-dir','--plan-dir','--plan-spill-dir'])
def test_existing_file_in_directory_slot_fails_before_dispatch(tmp_path, monkeypatch, argument, caplog):
    blocker=tmp_path/'file';blocker.write_text('preserve')
    handler=MagicMock(side_effect=AssertionError('ingestion dispatched'))
    monkeypatch.setattr(cli,'handle_start',handler)
    with patch.object(cli,'DockerService',side_effect=AssertionError('docker connected')):
        assert cli.main(['start','--mode','bulk',argument,str(blocker)]) == 1
    handler.assert_not_called()
    assert blocker.read_text() == 'preserve'
    assert 'stage=runtime' in caplog.text


def test_permission_failure_before_services_and_persistent_attempt_evidence(tmp_path, monkeypatch, caplog):
    from neo4j_graph_loader import _runtime
    real_probe=_runtime._writable_directory
    def deny_rejections(path):
        if path.name == 'rejections': raise PermissionError('controlled read-only output')
        return real_probe(path)
    monkeypatch.setattr(_runtime,'_writable_directory',deny_rejections)
    handler=MagicMock();monkeypatch.setattr(cli,'handle_start',handler)
    assert cli.main(['start','--mode','bulk','--run-dir',str(tmp_path)]) == 1
    handler.assert_not_called()
    attempt=next(tmp_path.iterdir())
    assert attempt.joinpath('runtime_schema.yaml').exists()
    assert str(attempt) in caplog.text and 'controlled read-only output' in caplog.text


def test_readonly_timing_file_is_preflighted_without_truncation(tmp_path, monkeypatch):
    timings=tmp_path/'timings';timings.mkdir()
    output=timings/'metrics-cli.prom';output.write_text('retain');output.chmod(0o444)
    monkeypatch.setenv('GRAPH_LOADER_TIMING_DIR',str(timings))
    if os.geteuid() == 0:
        pytest.skip('native permission denial requires non-root; final readonly image proof covers root')
    handler=MagicMock();monkeypatch.setattr(cli,'handle_start',handler)
    try:
        assert cli.main(['start','--mode','bulk','--run-dir',str(tmp_path/'runs')]) == 1
        handler.assert_not_called()
        assert output.read_text() == 'retain'
    finally: output.chmod(0o600)


def test_missing_schema_and_invalid_numeric_flags_do_not_create_state(tmp_path, monkeypatch):
    handler=MagicMock();monkeypatch.setattr(cli,'handle_start',handler)
    assert cli.main(['start','--mode','bulk','--config',str(tmp_path/'missing'),'--run-dir',str(tmp_path/'runs')]) == 1
    with patch.object(cli,'load_schema',side_effect=AssertionError('schema accessed')):
        assert cli.main(['start','--mode','bulk','--bulk-timeout-seconds','0','--run-dir',str(tmp_path/'runs')]) == 1
    handler.assert_not_called()
    assert not (tmp_path/'runs').exists()


def test_external_plan_is_readonly_input_and_sibling_ledger_is_preflighted(tmp_path, monkeypatch):
    # External producers may create the plan later, so absence is deliberately supported.
    plan=tmp_path/'producer'/'future-plan.json'
    args=capture_start(monkeypatch,['--edge-plan',str(plan),'--run-dir',str(tmp_path/'runs')])
    assert args.edge_plan == str(plan)
    assert plan.parent.is_dir() and not plan.exists()
    assert args.runtime.plans.is_relative_to(args.runtime.root)


def test_main_threads_periodic_and_final_metrics_path_on_handler_failure(tmp_path, monkeypatch):
    monkeypatch.setenv("GRAPH_LOADER_METRICS_ENABLED","1")
    schema=load_schema(ROOT/'config/graph_schema.yaml')
    facade=metrics.GraphLoaderMetrics(registry=metrics.CollectorRegistry())
    monkeypatch.setattr(cli,'load_schema',MagicMock(side_effect=[schema,SchemaLoadError('controlled handler failure')]))
    monkeypatch.setattr(metrics,'process_metrics',lambda:facade)
    start=MagicMock(return_value=None);monkeypatch.setattr(metrics,'start_metrics_server',start)
    monkeypatch.setattr(metrics,'stop_metrics_server',MagicMock())
    assert cli.main(['start','--mode','bulk','--run-dir',str(tmp_path)]) == 1
    root=next(tmp_path.iterdir())
    snapshot=root/'timings/metrics-cli.prom'
    assert start.call_args.kwargs['snapshot_path'] == str(snapshot)
    assert snapshot.exists()


def test_metrics_periodic_explicit_path_beats_environment(tmp_path, monkeypatch):
    monkeypatch.setenv('GRAPH_LOADER_TIMING_DIR',str(tmp_path/'ambient'))
    monkeypatch.setenv('GRAPH_LOADER_METRICS_ENABLED','1')
    monkeypatch.setattr(metrics,'_start_metrics_server_impl',lambda *a,**k:None)
    periodic=MagicMock();monkeypatch.setattr(metrics,'start_periodic_snapshots',periodic)
    facade=metrics.GraphLoaderMetrics(registry=metrics.CollectorRegistry())
    path=str(tmp_path/'owned/metrics-cli.prom')
    metrics.start_metrics_server(facade,kind='cli',name='-',snapshot_path=path)
    periodic.assert_called_once_with(facade,path)


def test_planner_receives_owned_plan_spill_rejections(tmp_path, monkeypatch):
    args=capture_start(monkeypatch,['--run-dir',str(tmp_path)])
    factory=MagicMock()
    planner=start_kafka_edge_planner(load_schema(args.config),args,run_id='owned',writers=1,
        bootstrap_servers='forbidden:9092',planner_factory=factory)
    call=factory.call_args
    assert call.kwargs['plan_path'] == args.runtime.plans/'edge-plan.json'
    assert call.kwargs['rejection_dir'] == args.runtime.rejections
    settings=call.args[2](10,1)
    assert settings.spill_dir == str(args.runtime.spill)
    planner.run_in_background.assert_called_once_with(args.runtime.plans/'edge-plan.json')


def test_every_launch_role_has_absolute_correctly_owned_mounts(tmp_path, monkeypatch):
    args=capture_start(monkeypatch,['--run-dir',str(tmp_path)])
    runtime=args.runtime;client=MagicMock();service=DockerService(client=client,runtime=runtime)
    plan=runtime.plans/'edge-plan.json';plan.write_text('{}')
    service.run_node_loader(node_label='P',topic='p',mode='bulk',config_path=args.config,run_id='owned')
    service.run_edge_loader(edge_type='E',mode='bulk',config_path=args.config,topic='e',run_id='owned')
    service.run_retry_consumer(run_id='owned',config_path=args.config,lease_address='lease:1',dlq_topics=['e.dlq'])
    service.run_global_batch_clock(run_id='owned',config_path=args.config,fleet_edge_types='E',coordination_topic='clock')
    service.run_bucket_lease_server(run_id='owned',bucket_count=4)
    service.run_planned_dispatcher(run_id='owned',plan_path=str(plan))
    service.run_planned_edge_writers(run_id='owned',plan_path=str(plan),config_path=args.config,
                                     dispatcher_address='dispatcher:1',writers=1)
    launches=client.containers.run.call_args_list
    assert len(launches) == 7
    for launch in launches:
        data=launch.kwargs;mounts=data['volumes'];env=data['environment']
        assert mounts[str(runtime.timings)] == {'bind':'/app/write-timings','mode':'rw'}
        for source,target in mounts.items():
            assert Path(source).is_absolute() and Path(source).is_relative_to(runtime.root)
            if target['bind'] in ('/app/runtime_schema.yaml','/app/edge-plan.json'):
                assert target['mode'] == 'ro'
            else: assert target['mode'] == 'rw'
        assert env['GRAPH_LOADER_METRICS_SNAPSHOT_PATH'].startswith('/app/write-timings/')
        if data['labels']['component'] in ('node-loader','edge-loader','retry-consumer'):
            assert mounts[str(runtime.rejections)] == {'bind':'/app/rejections','mode':'rw'}
            assert env['REJECTION_LOG_PATH'].startswith('/app/rejections/')
    # Explicit internal launch arguments keep priority over the instance default.
    explicit=tmp_path/'caller-rejections'
    service.run_node_loader(node_label='P',topic='p',mode='stream',config_path=args.config,rejection_dir=str(explicit))
    assert str(explicit) in client.containers.run.call_args.kwargs['volumes']


@pytest.mark.parametrize('kind', ['directory','symlink','readonly'])
@pytest.mark.parametrize('target', ['external-ledger','auto-ledger','auto-progress'])
def test_hidden_output_siblings_fail_before_dispatch_without_touching_foreign_state(tmp_path, monkeypatch, kind, target):
    plans=tmp_path/'plans';plans.mkdir()
    if target == 'external-ledger':
        name='.caller-plan.json.ledger.json.tmp'
        extra=['--edge-plan',str(plans/'caller-plan.json')]
    else:
        name='.edge-plan.json.ledger.json.tmp' if target == 'auto-ledger' else '.edge-plan.json.progress.tmp'
        extra=['--plan-dir',str(plans)]
    blocker=plans/name
    sentinel=tmp_path/'foreign';sentinel.write_text('retain')
    if kind == 'directory': blocker.mkdir()
    elif kind == 'symlink': blocker.symlink_to(sentinel)
    else:
        blocker.write_text('retain');blocker.chmod(0o444)
    handler=MagicMock();monkeypatch.setattr(cli,'handle_start',handler)
    try:
        assert cli.main(['start','--mode','bulk','--run-dir',str(tmp_path/'runs'),*extra]) == 1
        handler.assert_not_called()
        assert sentinel.read_text() == 'retain'
        if kind == 'readonly': assert blocker.read_text() == 'retain'
        if kind == 'symlink': assert blocker.is_symlink()
    finally:
        if kind == 'readonly': blocker.chmod(0o600)


@pytest.mark.parametrize('arguments', [
    ['--mode','bulk','--edge-plan','--plan-writers','0'],
    ['--mode','bulk','--edge-plan','--plan-workers','0'],
    ['--mode','bulk','--plan-memory-mb','0'],
    ['--mode','stream','--edge-plan'],
    ['--mode','stream','--edge-leases'],
    ['--mode','bulk','--edge-leases','--bucket-lease-buckets','0'],
    ['--mode','stream','--bucket-lease-outer-batch-size','0'],
    ['--mode','stream','--bucket-lease-buckets','4097'],
])
def test_invalid_plan_flags_fail_before_schema_state_metrics_and_dispatch(tmp_path, monkeypatch, arguments):
    handler=MagicMock();monkeypatch.setattr(cli,'handle_start',handler)
    with patch.object(cli,'load_schema',side_effect=AssertionError('schema read')), \
         patch.object(cli,'prepare_run',side_effect=AssertionError('state created')), \
         patch.object(metrics,'start_metrics_server',side_effect=AssertionError('metrics started')):
        assert cli.main(['start',*arguments,'--run-dir',str(tmp_path/'runs')]) == 1
    handler.assert_not_called()
    assert not (tmp_path/'runs').exists()
