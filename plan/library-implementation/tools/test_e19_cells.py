"""Tests of the S11 helpers: the scanner for secrets in `docker save` output, the schema and producer text, the count parser."""
from __future__ import annotations

import gzip
import io
import json
import os
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import e19_cells as C  # noqa: E402


def test_parse_count_takes_the_last_integer_line():
    assert C.parse_count('count(p)\n100\n') == 100
    assert C.parse_count('+------+\n 0\n') == 0
    with pytest.raises(Exception, match='no count'):
        C.parse_count('error: connection refused')


def test_the_cell_schema_is_a_valid_graph_loader_schema_with_distinct_topics(tmp_path):
    from neo4j_graph_loader.schema import load_schema
    topics = set()
    for cell in ('cell-a', 'cell-b', 'route1', 'route2'):
        path = tmp_path / f'{cell}.yaml'
        path.write_text(C.schema_text(cell))
        schema = load_schema(str(path))
        assert [n.label for n in schema.nodes] == ['Person', 'City'] and [e.type for e in schema.edges] == ['LIVES_IN']
        topics |= {n.topic for n in schema.nodes} | {e.topic for e in schema.edges}
    assert len(topics) == 12


def test_the_producer_and_the_stop_snippet_compile():
    compile(C.PRODUCER, 'producer', 'exec')
    compile(C.STOP_SNIPPET, 'stop', 'exec')
    compile(C.SCANNER, 'scanner', 'exec')


def fake_docker(tmp_path: Path, layers: dict[str, bytes], compress: bool = True, raw: bytes | None = None) -> Path:
    """A `docker` executable whose `save` prints an OCI-like tar: a config blob and gzip-compressed layer blobs holding the given file contents."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode='w') as outer:
        for name, content in layers.items():
            inner = io.BytesIO()
            with tarfile.open(fileobj=inner, mode='w') as layer:
                info = tarfile.TarInfo('app/file.txt')
                info.size = len(content)
                layer.addfile(info, io.BytesIO(content))
            blob = raw if raw is not None else (gzip.compress(inner.getvalue()) if compress else inner.getvalue())
            info = tarfile.TarInfo(f'blobs/sha256/{name}')
            info.size = len(blob)
            outer.addfile(info, io.BytesIO(blob))
        config = json.dumps({'config': {'Labels': {'a': 'b'}}}).encode()
        info = tarfile.TarInfo('blobs/sha256/config')
        info.size = len(config)
        outer.addfile(info, io.BytesIO(config))
    payload = tmp_path / 'saved.tar'
    payload.write_bytes(buffer.getvalue())
    script = tmp_path / 'bin' / 'docker'
    script.parent.mkdir(exist_ok=True)
    script.write_text(f'#!/bin/sh\ncat {payload}\n')
    script.chmod(0o755)
    return script.parent


def scan(tmp_path, needles, images=('img:1',), files=(), layers=None, compress=True, raw=None):
    path = fake_docker(tmp_path, layers or {'layer1': b'hello world'}, compress, raw)
    scanner = tmp_path / 'scanner.py'
    scanner.write_text(C.SCANNER)
    done = subprocess.run([sys.executable, str(scanner)], input=json.dumps({'needles': needles, 'images': list(images), 'files': [str(f) for f in files]}),
                          capture_output=True, text=True, env={**os.environ, 'PATH': f'{path}:{os.environ["PATH"]}'})
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout.strip().splitlines()[-1])


def test_the_scanner_finds_a_needle_inside_a_compressed_layer_and_prints_only_counts(tmp_path):
    result = scan(tmp_path, ['Zq7-sentinel'], layers={'layer1': b'config = Zq7-sentinel\n'})
    assert result['hits'] == [1] and result['gzip_members'] == 1 and result['unscanned'] == 0
    assert 'Zq7-sentinel' not in json.dumps(result)


def test_the_scanner_is_clean_on_a_clean_image_and_on_an_uncompressed_layer(tmp_path):
    assert scan(tmp_path, ['Zq7-sentinel'], layers={'layer1': b'nothing here'})['hits'] == [0]
    assert scan(tmp_path, ['Zq7-sentinel'], layers={'layer1': b'x Zq7-sentinel x'}, compress=False)['hits'] == [1]


def test_the_scanner_finds_a_needle_split_across_read_chunks(tmp_path):
    filler = b'a' * ((1 << 20) - 5)
    result = scan(tmp_path, ['Zq7-sentinel'], layers={'layer1': filler + b'Zq7-sentinel' + b'b' * 100})
    assert result['hits'] == [1]


def test_the_scanner_scans_files_and_counts_a_zstd_layer_as_unscanned(tmp_path):
    manifest = tmp_path / 'images.json'
    manifest.write_text('{"x": "Zq7-sentinel"}')
    assert scan(tmp_path, ['Zq7-sentinel'], images=(), files=[manifest])['hits'] == [1]
    # a layer blob in an encoding the scanner cannot read is reported as unscanned, so the assertion in the run fails instead of passing silently
    result = scan(tmp_path, ['Zq7-sentinel'], raw=b'\x28\xb5\x2f\xfd' + b'Zq7-sentinel')
    assert result['unscanned'] == 1 and result['zstd_members'] == 1


def test_the_start_command_differs_by_runtime():
    class H:  # minimal stand-in: start_cmd only uses constants
        pass
    cells = C.Cells.__new__(C.Cells)
    docker = cells.start_cmd('cell-a', 'bulk')
    assert '--network' in docker and '--plan-workers' in docker and docker[docker.index('--runtime') + 1] == 'docker'
    process = cells.start_cmd('route1', 'bulk', '/work/route1/venv/bin/graph-loader', 'process')
    assert '--network' not in process and '--plan-workers' in process and process[0].endswith('graph-loader')
    stream = cells.start_cmd('cell-a', 'stream')
    assert stream[stream.index('--mode') + 1] == 'stream' and '--plan-workers' not in stream
