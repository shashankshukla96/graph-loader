#!/usr/bin/env python3
"""Validate and render the planning backlog. Does not execute implementation work."""
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[1]


def main():
    data = json.loads((ROOT / 'backlog.json').read_text())
    epics = data['epics']
    epic_by_id = {e['id']: e for e in epics}
    stories = {s['id']: s for e in epics for f in e['features'] for s in f['stories']}
    features = [f['id'] for e in epics for f in e['features']]
    all_ids = [e['id'] for e in epics] + features + [s['id'] for e in epics for f in e['features'] for s in f['stories']]
    assert len(set(all_ids)) == len(all_ids), 'Duplicate epic/feature/story ID'
    checklist = (REPO / 'docs/PYPI_LIBRARY_CHECKLIST.md').read_text()
    required = set(re.findall(r'\*\*([BSPADVUTR]\d+)\s+[—–]', checklist))
    coverage = {key: [] for key in required}
    for epic in epics:
        assert all(d in epic_by_id for d in epic['depends_on']), epic['id']
        assert epic['review_status'] in ('pending', 'changes requested', 'approved')
        for f in epic['features']:
            for s in f['stories']:
                assert 0.5 <= s['estimate_days'] <= 2, s['id']
                assert all(d in stories for d in s['depends_on']), s['id']
                assert all(s[k] for k in ('changes', 'acceptance', 'validation', 'evidence')), s['id']
                for item in s['checklist']:
                    assert item in required, (s['id'], item)
                    coverage[item].append(s['id'])
    assert required and all(coverage.values()), 'Missing checklist coverage: ' + str([k for k,v in coverage.items() if not v])

    def check_cycles(graph):
        active, done = set(), set()
        def visit(node):
            assert node not in active, f'Dependency cycle: {node}'
            if node in done:
                return
            active.add(node)
            for dependency in graph[node]:
                visit(dependency)
            active.remove(node)
            done.add(node)
        for key in graph:
            visit(key)
    check_cycles({e['id']: e['depends_on'] for e in epics})
    check_cycles({sid: s['depends_on'] for sid,s in stories.items()})
    # A story cannot reach forward into a later epic while that later epic
    # already depends (directly or transitively) on its own epic.
    def ancestors(eid):
        return set(epic_by_id[eid]['depends_on']).union(*(ancestors(d) for d in epic_by_id[eid]['depends_on']))
    for sid,s in stories.items():
        eid = sid.split('.')[0]
        for dep in s['depends_on']:
            deid = dep.split('.')[0]
            assert deid == eid or deid in ancestors(eid), (sid, 'undeclared epic prerequisite', dep)
    inventory = json.loads((ROOT / 'test-inventory.json').read_text())
    assert inventory['inventory_owner'] in stories
    for lane in inventory['lanes'].values():
        assert lane['owner'] in stories
        for relative in lane.get('current_test_files', []) + lane.get('reference_files', []):
            assert (REPO / relative).is_file(), ('Missing current test/reference', relative)
    for relative in inventory['initial_helper_config_fixture_closure']:
        assert (REPO / relative).is_file(), ('Missing current fixture/helper', relative)
    for e in epics:
        lines = [f"# {e['id']} — {e['title']}", '', f"Plan review: **{e['review_status']}**. Implementation: **{e['implementation_status']}**.", '',
                 '**Outcome.** ' + e['outcome'], '', '**Scope.** ' + e['scope'], '',
                 '**Prerequisites.** ' + (', '.join(e['depends_on']) or 'None; may begin with the current checkout.'), '',
                 '**Exit gate.** ' + e['exit_gate'], '',
                 'Epic prerequisites apply to every story, in addition to the story-specific dependencies below. Estimates are rough focused engineering effort, not elapsed-time commitments. Story completion requires its acceptance criteria, validation, and evidence; implementing a file alone is insufficient.', '']
        for f in e['features']:
            lines += [f"## Feature {f['id']} — {f['title']}", '']
            for s in f['stories']:
                lines += [f"### Story {s['id']} — {s['title']}", '',
                          f"- Status: {s['status']}; estimate: {s['estimate_days']} engineering day(s).",
                          '- Checklist: ' + ', '.join(s['checklist']) + '.',
                          '- Story dependencies: ' + (', '.join(s['depends_on']) or 'None beyond epic prerequisites.') + '.',
                          '- Change surface: ' + '; '.join(s['changes']) + '.',
                          f"- Evidence: `../{s['evidence']}` (create when executing, exclude secrets).", '',
                          '**Acceptance criteria**', '']
                lines += [f'{i}. {item}' for i,item in enumerate(s['acceptance'],1)]
                lines += ['', '**Validation**', ''] + ['- ' + item for item in s['validation']] + ['']
        (ROOT/'epics'/f"{e['id'].lower()}.md").write_text('\n'.join(lines))
    lines = ['# Checklist coverage', '', 'Generated from `backlog.json`. Multiple story references mean staged decision/implementation/validation, not duplicated work. Every checklist gate remains subject to its original evidence requirements.', '', '| Checklist item | Stories |', '| --- | --- |']
    prefix_order = 'BSPADVUTR'
    for item in sorted(required,key=lambda x:(prefix_order.index(x[0]),int(x[1:]))):
        links = ', '.join(f'[{sid}](epics/{sid.split(".")[0].lower()}.md)' for sid in coverage[item])
        lines.append(f'| {item} | {links} |')
    (ROOT/'traceability.md').write_text('\n'.join(lines)+'\n')
    print(f'Validated and rendered {len(epics)} epics, {len(features)} features, {len(stories)} stories; {len(required)} checklist items covered; dependency graphs acyclic.')


if __name__ == '__main__':
    main()
