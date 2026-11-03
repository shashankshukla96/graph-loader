"""soak/schema.yaml: consistent with the spec the harness checks against, and accepted by the installed CLI."""
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest
import yaml

from soak import schema, spec

SCHEMA = Path(__file__).parents[1] / 'schema.yaml'


def load():
    return yaml.safe_load(SCHEMA.read_text())


def test_the_template_carries_the_all_zero_token_and_a_run_substitutes_its_own():
    text = SCHEMA.read_text()
    assert spec.TEMPLATE_TOKEN in text
    rendered = schema.render('ab' * 16)
    assert spec.TEMPLATE_TOKEN not in rendered and f"consumer_group_id: e13-{'ab' * 16}" in rendered
    with pytest.raises(ValueError):
        schema.render('not-a-token')


def test_the_schema_matches_the_spec_the_checker_uses():
    doc = load()
    assert doc['loading']['consumer_group_id'] == f'e13-{spec.TEMPLATE_TOKEN}'
    nodes = {n['label']: n for n in doc['nodes']}
    assert set(nodes) == set(spec.NODES)
    for label, node in spec.NODES.items():
        entry = nodes[label]
        assert entry['topic'] == spec.topic_name(spec.TEMPLATE_TOKEN, node.topic) and entry['key_property'] == node.key
        assert entry['replicas'] == node.replicas
        assert {k: v['type'] for k, v in entry['properties'].items()} == node.types
        assert {k for k, v in entry['properties'].items() if v.get('required')} == set(node.required)
        assert 'rid' in entry['properties']
    edges = {e['type']: e for e in doc['edges']}
    assert set(edges) == set(spec.RELS)
    for name, rel in spec.RELS.items():
        entry = edges[name]
        assert entry.get('order_by') == rel.order_by and entry['replicas'] == rel.replicas
        assert {k: v['type'] for k, v in entry['properties'].items()} == rel.types
        assert {k for k, v in entry['properties'].items() if v.get('required')} == set(rel.required)
        topics = entry.get('topics') or [{'name': entry['topic'], 'nodes': entry['nodes']}]
        got = sorted((t['name'], t['nodes']['source'], t['nodes']['target']) for t in topics)
        want = sorted((spec.topic_name(spec.TEMPLATE_TOKEN, t.suffix), t.source, t.target) for t in rel.topics)
        assert got == want
        dead = entry['dead_letter']
        assert dead['enabled'] is True and dead['max_retry_attempts'] == spec.RETRY_ATTEMPTS
        assert dead['topic'] == spec.dlq_topic(spec.TEMPLATE_TOKEN, name)
        assert entry['retry'] == {'base_delay_seconds': 1.0, 'max_delay_seconds': 60.0}


def test_the_retry_window_is_about_five_minutes():
    base, cap, attempts = 1.0, 60.0, spec.RETRY_ATTEMPTS
    window = base + sum(min(base * 2 ** (k - 2), cap) for k in range(2, attempts + 1))
    assert window == spec.RETRY_WINDOW_S == 304


def test_replica_counts_give_eleven_application_containers_and_twelve_after_the_scaling_fault():
    assert schema.application_containers(schema.render('ab' * 16)) == 11
    assert schema.application_containers(schema.render('ab' * 16, replicas={'PURCHASED': 3})) == 12
    assert schema.replica_plan(schema.render('ab' * 16, replicas={'PURCHASED': 3}))['PURCHASED'] == 3


def installed_cli():
    explicit = os.environ.get('E13_GRAPH_LOADER')
    if explicit:
        return [explicit]
    found = shutil.which('graph-loader')
    return [found] if found else None


def test_the_schema_passes_validate_schema_with_the_installed_cli():
    cli = installed_cli()
    if cli is None:
        pytest.skip('no installed graph-loader: set E13_GRAPH_LOADER (the frozen wheel\'s executable)')
    for text in (SCHEMA.read_text(), schema.render('ab' * 16, replicas={'PURCHASED': 3})):
        path = Path(os.environ.get('TMPDIR', '/tmp')) / f'e13-validate-{os.getpid()}.yaml'
        path.write_text(text)
        try:
            run = subprocess.run([*cli, 'validate-schema', '--config', str(path)], capture_output=True, text=True, timeout=120)
        finally:
            path.unlink(missing_ok=True)
        assert run.returncode == 0, run.stdout + run.stderr
