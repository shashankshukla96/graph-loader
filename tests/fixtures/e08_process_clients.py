"""Guarded external client: public planning API, importable sources, no services."""
from functools import partial
import hashlib
import json
import multiprocessing as mp
import os
from pathlib import Path
import sys
import threading
import time
from types import SimpleNamespace

from neo4j_graph_loader import planning, schema


SCHEMA = {
    'loading': {'mode': 'bulk'},
    'nodes': [{'label': 'Person', 'topic': 'people', 'key_property': 'id',
               'properties': {'id': {'type': 'integer', 'required': True}}}],
    'edges': [{'type': 'KNOWS', 'topic': 'knows', 'nodes': {'source': 'Person', 'target': 'Person'},
               'source_key_property': 'id', 'target_key_property': 'id',
               'properties': {'weight': {'type': 'integer'}}}],
}


class ExternalSource(planning.EdgeRecordSource):
    edge_type = 'KNOWS'

    def __init__(self, output, order, scenario='success'):
        self.output, self.order, self.scenario = str(output), order, scenario
        self.size_hint = order + 1

    def records(self, edge):
        identity = json.dumps({
            'pid': os.getpid(), 'method': mp.get_start_method(), 'order': self.order,
            'scenario': self.scenario,
            'source_module': type(self).__module__, 'source_file': __file__,
        }).encode()
        destination = Path(self.output, f'source-{self.order}.json')
        pending = destination.with_name(destination.name+f'.{os.getpid()}.pending')
        with pending.open('xb') as stream:
            stream.write(identity)
        os.replace(pending, destination)
        if self.scenario == 'source-error':
            raise LookupError('external source sentinel failure')
        if self.scenario == 'worker-exit':
            os._exit(37)
        for index in range(3):
            key = self.order * 3 + index
            yield {'source': {'id': key}, 'target': {'id': 100000 + key},
                   'properties': {'weight': self.order}}


class ObservedSpawnContext(mp.context.SpawnContext):
    """Retain real parent process handles without changing spawn or worker IO."""

    def __init__(self):
        self.processes = []

    def Process(self, *args, **kwargs):
        process = mp.get_context('spawn').Process(*args, **kwargs)
        self.processes.append(process)
        return process


class FileProducer:
    """Record actual published bytes and return stable local delivery metadata."""
    def __init__(self, config):
        self.output = Path(config['e08.output'])
        self.pending = []

    def produce(self, topic, *, value, partition, on_delivery):
        offset = int.from_bytes(hashlib.sha256(value).digest()[:8], 'big') & ((1 << 63) - 1)
        with (self.output / f'messages-{os.getpid()}.jsonl').open('a') as stream:
            stream.write(json.dumps({'partition': partition, 'offset': offset, 'value': value.decode()}) + '\n')
        message = SimpleNamespace(topic=lambda: topic, partition=lambda: partition, offset=lambda: offset)
        self.pending.append((on_delivery, message))

    def poll(self, timeout=0):
        pending, self.pending = self.pending, []
        for callback, message in pending:
            callback(None, message)

    def flush(self, timeout=0):
        self.poll()
        return 0


def run(context, workers, scenario, output):
    """Return observations; original public exceptions must reach the client."""
    output = Path(output)
    if not output.is_dir():
        raise ValueError('client output must be an existing owned directory')
    if context not in ('spawn', 'inline') or not 1 <= workers <= 64:
        raise ValueError('explicit supported client context and measured worker count required')
    if scenario not in ('success', 'nonpicklable', 'source-error', 'worker-exit'):
        raise ValueError('unknown external-client scenario')
    if context == 'inline' and scenario == 'worker-exit':
        raise ValueError('abrupt process exit requires process workers')
    # The caller owns this setting. The planner must use its private context.
    selected = 'forkserver' if 'forkserver' in mp.get_all_start_methods() else 'spawn'
    mp.set_start_method(selected)
    before = mp.get_start_method()
    sources = [ExternalSource(output, index, scenario if index == 0 else 'success') for index in range(64)]
    if scenario == 'nonpicklable':
        sources[0].unserializable = threading.Lock()
        sources[0].scenario = 'success'
    settings = planning.PlanSettings(topic='chunks', partitions=2, buckets=4, hub_degree=0,
                                      workers=workers, memory_mb=1, chunk_rows=2)
    path = output/'plan.json'
    sentinel = output/'caller-owned'
    sentinel.write_text('caller sentinel')
    producer_config = {'e08.output': str(output)}
    process_context = None if context == 'inline' else ObservedSpawnContext()
    planner = planning.producer_edge_planner(schema.GraphSchema.model_validate(SCHEMA), sources, settings, 'unused',
        context='inline' if context == 'inline' else process_context,
        producer_class=FileProducer, producer_factory=partial(FileProducer, producer_config),
        producer_config=producer_config, plan_path=path, plan_id='e08-public-process-client')
    expected_error = scenario != 'success' and not (scenario == 'nonpicklable' and context == 'inline')
    observed = {'context': context, 'workers': workers, 'scenario': scenario, 'caller_method': before}
    started = time.monotonic()
    try:
        try:
            result = planner.finish(path)
        except Exception as error:
            assert expected_error, repr(error)
            message = str(error)
            if scenario == 'nonpicklable':
                assert 'pickle' in message.lower() or 'serializ' in message.lower(), message
            elif scenario == 'source-error':
                assert 'external source sentinel failure' in message, message
                assert error.__cause__ is not None
                assert 'external source sentinel failure' in str(error.__cause__)
            else:
                assert 'exited unexpectedly' in message, message
                assert 'exit code 37' in message or 'exit code None' in message, message
                assert error.__cause__ is not None
                fault = json.loads((output/'source-0.json').read_text())
                assert fault['order'] == 0 and fault['scenario'] == 'worker-exit'
                assert fault['method'] == 'spawn' and fault['pid'] != os.getpid()
                fault_process = next(process for process in process_context.processes if process.pid == fault['pid'])
                assert not fault_process.is_alive() and fault_process.exitcode == 37, fault_process.exitcode
                observed.update(fault_source_identity=fault, observed_exit_code=fault_process.exitcode)
            assert not path.exists()
            marker = path.with_name(path.name+'.failed').read_text()
            assert message in marker
            observed.update(error=type(error).__name__, message=message,
                            cause=None if error.__cause__ is None else str(error.__cause__))
        else:
            assert not expected_error, 'expected source/serialization/process error was lost'
            assert isinstance(result, planning.PlanResult) and result.rows_read == result.plan.total_rows == 192
            assert result.digest == hashlib.sha256(path.read_bytes()).hexdigest()
            loaded = planning.EdgePlan.from_json(json.loads(path.read_bytes()))
            assert loaded.to_json() == result.plan.to_json()
            messages = {}
            for file in output.glob('messages-*.jsonl'):
                for line in file.read_text().splitlines():
                    row = json.loads(line)
                    key = row['partition'], row['offset']
                    assert key not in messages
                    messages[key] = row['value'].encode()
            rows = []
            for chunk in result.plan.chunks:
                value = messages.pop((chunk.partition, chunk.offset))
                assert len(value) == chunk.bytes
                message = json.loads(value)
                assert message['chunk'] == chunk.key and message['count'] == chunk.rows
                assert message['run'] == result.plan.plan_id and message['type'] == 'KNOWS'
                assert len(message['records']) == chunk.rows <= 2
                rows.extend(message['records'])
            assert not messages
            expected = [{'source': {'id': key}, 'target': {'id': 100000 + key},
                         'properties': {'weight': key // 3}} for key in range(192)]
            assert sorted(rows, key=lambda row: row['source']['id']) == expected
            observed.update(rows=192, chunks=len(result.plan.chunks), digest=result.digest,
                            planner_workers=result.plan.settings['planner_workers'])
    finally:
        cleanup_started = time.monotonic()
        planner.close(terminate=True)
        observed['explicit_close_seconds'] = time.monotonic() - cleanup_started
        observed['operation_cleanup_upper_seconds'] = time.monotonic() - planner._cleanup_started
    assert observed['explicit_close_seconds'] < 10
    assert observed['operation_cleanup_upper_seconds'] < 10
    assert not mp.active_children()
    assert not path.with_name(path.name+'.lock').exists()
    assert not planner.spill_dir.exists()
    assert not any(thread.name == 'edge-plan-heartbeat' for thread in threading.enumerate())
    assert sentinel.read_text() == 'caller sentinel'
    assert mp.get_start_method() == before
    identities = [json.loads(p.read_text()) for p in output.glob('source-*.json')]
    pending_identities = [{'path': str(p), 'size': p.stat().st_size,
                           'sha256': hashlib.sha256(p.read_bytes()).hexdigest()}
                          for p in output.glob('source-*.pending')]
    if not expected_error:
        assert not pending_identities
        assert len(identities) == 64
        if context == 'spawn':
            assert all(row['method'] == 'spawn' and row['pid'] != os.getpid() for row in identities)
            assert len({row['pid'] for row in identities}) == workers
        else:
            assert all(row['pid'] == os.getpid() and row['method'] == before for row in identities)
    process_exits = [] if process_context is None else [
        {'pid': process.pid, 'exitcode': process.exitcode, 'alive': process.is_alive()}
        for process in process_context.processes]
    assert all(not row['alive'] and row['exitcode'] is not None for row in process_exits)
    observed.update(status='observed', process_exits=process_exits, source_identities=identities, elapsed_seconds=time.monotonic()-started,
                    pending_source_identities=pending_identities,
                    caller_method_after=mp.get_start_method(), client_pid=os.getpid(),
                    client_file=__file__, client_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    application_origin=planning.__file__, schema_origin=schema.__file__)
    return observed


def reap_resource_tracker(timeout=5.):
    """Stop and wait for this client's multiprocessing resource tracker (same race and fix as tests/fixtures/e04_faults.py).

    Without it the tracker outlives the client by a few ms on CPython builds that never wait for it at exit (the host's 3.12.3), and the
    supervisor's group probe (run_fault.py, right after client.wait()) reports emergency_group_cleanup. Every worker is already reaped here.
    """
    from multiprocessing import resource_tracker
    tracker = resource_tracker._resource_tracker
    with tracker._lock:
        fd, pid = tracker._fd, tracker._pid
        if fd is None or pid is None:
            return
        os.close(fd)
        tracker._fd = None
        deadline = time.monotonic()+timeout
        while not os.waitpid(pid, os.WNOHANG)[0]:
            if time.monotonic() >= deadline:
                raise AssertionError('resource tracker did not exit after its pipe closed')
            time.sleep(.002)
        tracker._pid = None


if __name__ == '__main__':
    mode, count, scenario, destination = sys.argv[1:]
    result = run(mode, int(count), scenario, destination)
    Path(destination, 'client.json').write_text(json.dumps(result, indent=2)+'\n')
    reap_resource_tracker()
