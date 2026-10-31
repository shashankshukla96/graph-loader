"""Deterministic planned-bulk fixture for E09.F1.S1 (native shared Docker Engine substrate).

Sixty ``Worker`` nodes, six ``Employer`` nodes and one hundred and twenty
``EMPLOYED_BY`` relationships (two distinct employers per worker). Every value is
a closed formula of the row number, so the expected graph is computed here from
the fixture alone and never copied from a run.
"""
from __future__ import annotations

import json

WORKERS = 60
EMPLOYERS = 6
LABELS = ('Worker', 'Employer')
EDGE_TYPE = 'EMPLOYED_BY'
PLAN_ARGS = ('--plan-chunk-rows', '20', '--plan-writers', '2')


def workers(count=WORKERS):
    return [(i, f'worker-{i}') for i in range(1, count + 1)]


def employers(count=EMPLOYERS):
    return [(f'e{j}', f'employer-{j}') for j in range(count)]


def relationships(count=WORKERS, employers_count=EMPLOYERS):
    """(worker, employer, role, since): two distinct employers for every worker."""
    rows = []
    for i in range(1, count + 1):
        first = i % employers_count
        second = (first + 1 + (i // employers_count) % (employers_count - 1)) % employers_count
        assert first != second
        for slot, employer in enumerate((first, second)):
            rows.append((i, f'e{employer}', f'role-{(i + slot) % 7}', 1990 + (i * 3 + slot) % 30))
    assert len({(w, e) for w, e, _, _ in rows}) == 2 * count
    return rows


def schema(prefix, topics, group):
    return f'''loading:
  mode: bulk
  consumer_group_id: {group}
  unwind_batch_size: 500
  edge_unwind_batch_size: 2000
  rejection_log_path: var/rejections/{prefix}nodes.jsonl
nodes:
  - label: Worker
    topic: {topics['worker']}
    key_property: id
    properties:
      id: {{type: integer, required: true, constraint: {{type: uniqueness}}}}
      name: {{type: string, required: true}}
  - label: Employer
    topic: {topics['employer']}
    key_property: id
    properties:
      id: {{type: string, required: true, constraint: {{type: uniqueness}}}}
      name: {{type: string, required: true}}
edges:
  - type: {EDGE_TYPE}
    topic: {topics['edge']}
    nodes: {{source: Worker, target: Employer}}
    source_key_property: id
    target_key_property: id
    properties:
      role: {{type: string}}
      since: {{type: integer}}
'''


def produce(stack, topics, count=WORKERS, employers_count=EMPLOYERS):
    stack.produce(topics['worker'], [(i, {'id': i, 'name': n}) for i, n in workers(count)])
    stack.produce(topics['employer'], [(i, {'id': i, 'name': n}) for i, n in employers(employers_count)])
    stack.produce(topics['edge'], [(f'{w}-{e}', {'source': {'id': w}, 'target': {'id': e},
                                                   'properties': {'role': r, 'since': s}})
                                   for w, e, r, s in relationships(count, employers_count)])


def expected_nodes(count=WORKERS, employers_count=EMPLOYERS):
    return sorted(json.dumps({'labels': [label], 'properties': {'id': i, 'name': n}}, sort_keys=True)
                  for label, rows in (('Worker', workers(count)), ('Employer', employers(employers_count))) for i, n in rows)


def expected_relationships(count=WORKERS, employers_count=EMPLOYERS):
    names = dict(workers(count)), dict(employers(employers_count))
    return sorted(json.dumps({'source_labels': ['Worker'], 'source': {'id': w, 'name': names[0][w]}, 'type': EDGE_TYPE,
                              'properties': {'role': r, 'since': s}, 'target_labels': ['Employer'],
                              'target': {'id': e, 'name': names[1][e]}}, sort_keys=True)
                  for w, e, r, s in relationships(count, employers_count))


def own_graph(observed):
    """The fixture's own nodes and relationships out of a whole-database observation (earlier tests leave other labels)."""
    nodes = [n for n in observed['nodes'] if n['labels'][0] in LABELS]
    edges = [r for r in observed['relationships'] if r['type'] == EDGE_TYPE]
    return (sorted(json.dumps(n, sort_keys=True) for n in nodes),
            sorted(json.dumps(r, sort_keys=True) for r in edges))
