"""Deterministic streaming-lease fixture for E09.F1.S2 (native shared Docker Engine substrate).

Batches of ``Citizen`` nodes (string ids) over six fixed ``Firm`` nodes with three relationship types: two that conflict on the
``Firm`` endpoint (``WORKS_FOR``, ``BUYS_FROM``) and one self-referencing (``KNOWS_OF``). Every value is a closed formula of the row
number, so the expected graph is computed here and never copied from a run.
"""
from __future__ import annotations

import json

LABELS = ('Citizen', 'Firm')
TYPES = ('WORKS_FOR', 'BUYS_FROM', 'KNOWS_OF')
FIRMS = 6
BATCHES = {'a': range(1, 25), 'b': range(25, 37), 'big': range(1, 2001), 'c': range(2001, 2013)}
EXTRA = {'a2': 'a'}  # more relationships between the citizens/firms of an existing batch (distinct pairs)


def firms():
    return [f'f{j}' for j in range(FIRMS)]


def citizens(batch):
    return [f'c{i}' for i in BATCHES[EXTRA.get(batch, batch)]]


def edges(batch):
    """(type, source, target, kind): one of each type per citizen; KNOWS_OF stays inside the batch."""
    people = citizens(batch)
    rows = []
    shift = 3 if batch in EXTRA else 0  # an extra batch uses other firms and neighbours, so every pair is new
    for position, person in enumerate(people):
        number = int(person[1:])
        rows.append(('WORKS_FOR', person, f'f{(number + shift) % FIRMS}', 'works'))
        rows.append(('BUYS_FROM', person, f'f{(number + 2 + shift) % FIRMS}', 'buys'))
        rows.append(('KNOWS_OF', person, people[(position + 1 + shift) % len(people)], 'knows'))
    return rows


def schema(prefix, topics, group, *, bulk=None):
    """Stream schema, or a bulk one (``bulk`` = classic tuning: {'worker_count': n, 'batch_size': n})."""
    tuning = '' if bulk is None else f'''    execution: {{mode: {bulk.get('mode', 'python_apoc')}, worker_count: {bulk['worker_count']}}}
    mix_and_batch: {{batch_size: {bulk['batch_size']}}}
'''
    mode = '' if bulk is None else '  mode: bulk\n'

    def edge(kind, target):
        return f'''  - type: {kind}
    topic: {topics[kind]}
    nodes: {{source: Citizen, target: {target}}}
    source_key_property: id
    target_key_property: id
    properties:
      kind: {{type: string, required: true}}
{tuning}'''
    return f'''loading:
{mode}  consumer_group_id: {group}
  flush_interval_ms: 25
  unwind_batch_size: 50
  bucket_lease:
    tx_timeout_ms: 6000
    restart_quiet_margin_ms: 2000
    expected_chunk_commit_ms: 100
nodes:
  - label: Citizen
    topic: {topics['citizen']}
    key_property: id
    properties:
      id: {{type: string, required: true}}
      name: {{type: string, required: true}}
  - label: Firm
    topic: {topics['firm']}
    key_property: id
    properties:
      id: {{type: string, required: true}}
      name: {{type: string, required: true}}
edges:
{edge('WORKS_FOR', 'Firm')}{edge('BUYS_FROM', 'Firm')}{edge('KNOWS_OF', 'Citizen')}'''


def produce_nodes(stack, topics, batch, *, with_firms):
    stack.produce(topics['citizen'], [(c, {'id': c, 'name': c}) for c in citizens(batch)])
    if with_firms:
        stack.produce(topics['firm'], [(f, {'id': f, 'name': f}) for f in firms()])


def produce_edges(stack, topics, batch):
    for kind in TYPES:
        stack.produce(topics[kind], [(f'{s}-{t}', {'source': {'id': s}, 'target': {'id': t}, 'properties': {'kind': k}})
                                     for typ, s, t, k in edges(batch) if typ == kind])


def expected(batches):
    base = list(dict.fromkeys(EXTRA.get(b, b) for b in batches))  # extra batches add relationships, not citizens
    nodes = sorted(json.dumps({'labels': [label], 'properties': {'id': i, 'name': i}}, sort_keys=True)
                   for label, ids in (('Citizen', [c for b in base for c in citizens(b)]), ('Firm', firms())) for i in ids)
    rels = []
    for batch in batches:
        for typ, source, target, kind in edges(batch):
            rels.append(json.dumps({'source_labels': ['Citizen'], 'source': {'id': source, 'name': source}, 'type': typ,
                                    'properties': {'kind': kind}, 'target_labels': ['Citizen' if typ == 'KNOWS_OF' else 'Firm'],
                                    'target': {'id': target, 'name': target}}, sort_keys=True))
    return nodes, sorted(rels)


def own_graph(observed):
    nodes = [n for n in observed['nodes'] if n['labels'][0] in LABELS]
    rels = [r for r in observed['relationships'] if r['type'] in TYPES]
    return (sorted(json.dumps(n, sort_keys=True) for n in nodes), sorted(json.dumps(r, sort_keys=True) for r in rels))
