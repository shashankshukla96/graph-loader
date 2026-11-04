#!/usr/bin/env python3
"""Freeze/check the explicit E07 test inventory; never import application sources."""
from __future__ import annotations

import argparse
import ast
from collections import Counter
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[1]
INVENTORY = ROOT / 'tests/inventory/test-inventory.json'
EXTRA_LANES = {
    'public-contracts': (6, 1, '''namespace_migration public_api public_lifecycle public_cancellation
        public_cleanup planner_transport planner_imports installed_cli cli_resources runtime_paths
        image_selection docker_topology neo4j_session distribution_foundation'''),
    'writer-runtime': (7, 1.5, '''node_loader edge_loader edge_execution edge_execution_writer
        planned_writer planned_dispatcher node_leases multi_pair_write_path mix_and_batch
        cypher_generator schema_initializer coordination clock_runner fleet_contract
        stop_log_levels metrics_overhead'''),
    'orchestration-runtime': (8, 1.5, '''bulk_default_mode bulk_e2e bulk_monitor relationship_bulk_monitor
        monitor_empty_partitions cli_dead_letter dlq_migration_guards multi_pair_evidence multi_pair_leases
        multi_pair_metrics multi_pair_orchestration multi_topic_metrics multi_topic_stream_bulk
        precedence_substages dependency_manager isolation rotation single_pair_multi_topic_compat
        docker_parity handle_conformance process_runtime process_service process_observability run_registry planner_supervision
        cgroup_budget cgroup_oom_report node_only_supervision
        status_model status_telemetry status_store status_command metric_identity status_live_findings'''),
    'example-contracts': (9, 1.5, 'social_example multi_topic_example star_topology_data'),
    'service-helper-unit': (10, .5, '''it_scratch_db lock_wait_helpers edge_before_node_dlq_probes
        planned_harness self_reference_isolation_e2e'''),
}
SERVICE_LANES = {
    'service-harness': ('tests/installed_service/test_inspection_controls.py', 'tests/installed_service/test_observer_controls.py', 'tests/installed_service/test_query_window_controls.py', 'tests/installed_service/test_service_harness.py',
                        'tests/installed_service/test_service_ingest_dlq.py',
                        'tests/installed_service/test_service_kill_and_outage.py',
                        'tests/installed_service/test_service_kill_bulk_modes.py',
                        'tests/installed_service/test_service_last_value_order.py',
                        'tests/installed_service/test_service_lease_stream.py',
                        'tests/installed_service/test_service_legacy_classic.py',
                        'tests/installed_service/test_service_load_bulk.py',
                        # Collection order is alphabetical. The destructive log-continuity control moves the query log away,
                        # so it must sort (and run) last: keep "load" before "log".
                        'tests/installed_service/test_service_log_continuity_live.py'),
}
SERVICE_EXTRA_FILES = {'service-harness': ('config/neo4j/server-logs.xml',)}
SERVICE_REASON = {
    'service-harness': 'Live harness check on owned Neo4j/Kafka (native shared Docker Engine substrate) or a synthetic fail-closed control of its query-log detector; proves the harness only, not any load mode.',
}
CHECKOUT = {
    'service_lane': 'Checkout outer-controller snapshot/reaper controls for service lanes; the live lane itself is the executed proof.',
    'service_observers': 'Checkout observer controls; observed installed application and scratch service evidence remain mandatory.',
    'installed_service': 'Checkout service-owner and runner tooling controls; real installed service scenarios remain mandatory.',
    'installed_service_log': 'Checkout query-log window and detector controls; real service detector proof remains mandatory.',
    'installed_service_log_config': 'Checkout scratch query-log configuration controls; runtime logging proof remains mandatory.',
    'installed_service_structured_log': 'Checkout structured-query-log parser controls; runtime log continuity remains mandatory.',
    'support_matrix': 'Checkout dependency-matrix tooling controls; retained checkout plus actual bare and installed platform/dependency cells.',
    'distribution_foundation': 'Packaging/dependency-generation developer checks; retained checkout suite plus mandatory strict artifact and two bare-install proof.',
    'candidate_images': 'Checkout image-builder input/ownership tests; retained checkout plus mandatory four rebuilt immutable image and read-only probes.',
    'dev_environment': 'Developer dotenv/settings helpers; synthetic unit fixtures retained. Actual smoke IDs remain service-only.',
    'dlq_proof_harness': 'Checkout proof-supervisor tooling; retained checkout and mandatory exact no-skip supervised B3.',
    'docs_yaml_snippets': 'Authoring documentation snippets outside the runtime artifact; retained checkout suite.',
    'makefile_env_precedence': 'Developer Makefile dotenv precedence using owned synthetic inputs; retained checkout suite.',
    'validate_monitoring': 'Authoring monitoring/deployment configuration outside runtime artifact; retained checkout suite.',
    'installed_runner': 'Developer installed-runner adversarial controls; retained checkout plus actual isolated positive/negative runner proof.',
    'installed_inventory': 'Developer inventory/freeze controls; retained checkout plus actual disappearance/deselection proof.',
    'runtime_boundary': 'E14 source-structure controls (the runtime seam imports no Docker names); they read the checkout source tree.',
    'dockerfile_single_container': 'E14 static controls on the CLI Dockerfile (tini PID 1, image contract unchanged); they read the checkout Dockerfiles.',
    'single_container_support': 'E14 offline controls of the single-container live helpers; retained checkout, the live cells are the executed proof.',
    'status_schema_files': 'E15 published status/event JSON Schema files under docs/ equal the Python source; they read the checkout docs tree.',
    'status_outbox': 'E15 local outbox controls include the SQL delivery cells (a module-level SQLAlchemy import), and the optional [sql] extra is not in the finite installed-lane dependency set; retained checkout plus the installed proofs of the extras.',
    'status_sql': 'E15 SQL status store controls need the optional [sql]/[postgres] extras, which the finite installed-lane dependency set excludes by design; retained checkout plus the installed proofs of the base, [sql] and [postgres] environments and the owned PostgreSQL cells.',
    'json_logs': 'E15 JSON-log controls include static drift guards over the checkout source tree (every logger call, every Kafka client factory) and a second Kafka client environment; retained checkout.',
    'monitoring_export': 'E15 monitoring bundle controls read the checkout monitoring/, Makefile, Compose and pyproject files; retained checkout plus the installed proof of `monitoring export` from the wheel.',
    'artifact_contract': 'E15 artifact-contract controls build the wheel and sdist from the checkout and mutate them; developer tooling, never part of the runtime artifact.',
    'image_locks': 'E19 dependency-lock controls read the checkout lock files, requirements.txt and pyproject.toml; the lock tool is developer tooling.',
    'image_install': 'E19 controls of the image install helper run it against local wheels and a local index; the helper itself ships in the wheel, the image proofs are the executed evidence.',
    'image_builder': 'E19 controls of `graph-loader images build` and the shared build core against a recording stand-in for the docker CLI; the builds themselves are the executed proof.',
    'image_build_files': 'E19 static controls on the four image Dockerfiles; they read the checkout Dockerfiles.',
    'neburyx_namespace': 'E19 identity controls read the checkout Dockerfiles, pyproject, docs and scripts; the installed package constant is covered by the image-selection tests.',
    'inventory_migration': 'E19 inventory migration and portability controls; developer tooling over the checkout inventory, never part of the runtime artifact.',
    'no_plan_dependency': 'E19 checks that no product file points at a maintainers\' planning document; they read the checkout tree.',
}
HISTORY = {
    'tests/test_multi_pair_leases.py::test_the_slice_does_not_touch_the_lease_server_or_client',
    'tests/test_multi_pair_orchestration.py::test_the_slice_does_not_touch_the_bulk_monitor_or_docker_service',
}
LIVE = {'integration', 'smoke', 'stress'}


def sha(value):
    return hashlib.sha256(value).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def source_version():
    """The version literal of the single file authority, read as text; application code is never imported."""
    tree = ast.parse((Path(__file__).resolve().parents[1] / 'src/neo4j_graph_loader/_version.py').read_text())
    return next(ast.literal_eval(n.value) for n in tree.body if isinstance(n, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == '__version__' for t in n.targets))


def collect(output, *, extra_args=()):
    """Source discovery is explicit and separate from the installed runner."""
    launch = '''import json,sys
from pathlib import Path
root=Path(sys.argv[1]);sys.path.insert(0,str(root))
import pytest
class Recorder:
 def pytest_collection_finish(self,session):
  rows=[{'id':i.nodeid,'file':str(i.path.relative_to(root)),'markers':sorted({m.name for m in i.iter_markers()}),'fixtures':sorted(i.fixturenames)} for i in session.items]
  Path(sys.argv[2]).write_text(json.dumps({'items':rows,'count':len(rows)},indent=2)+'\\n')
raise SystemExit(pytest.main(['--collect-only','-q','-o','addopts=','tests','examples/imdb/tests',*sys.argv[3:]],plugins=[Recorder()]))
'''
    env = {k: v for k, v in os.environ.items() if k not in {'PYTHONPATH', 'PYTHONHOME', 'PYTEST_ADDOPTS', 'PYTEST_PLUGINS'}
           and not k.startswith(('NEO4J_', 'KAFKA_', 'LOADER_', 'GRAPH_LOADER_'))}
    env['PYTEST_DISABLE_PLUGIN_AUTOLOAD'] = '1'
    command = [sys.executable, '-I', '-c', launch, str(ROOT), str(output), *extra_args]
    with output.with_suffix('.log').open('wb') as log:
        result = subprocess.run(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=120)
    if result.returncode:
        raise ValueError('source collection failed; see ' + str(output.with_suffix('.log')))
    return json.loads(output.read_text()), command


def file_policy(data):
    mapping = {}
    for name in ('core', 'safety', 'cli-runtime'):
        for path in data['lanes'][name]['current_test_files']:
            mapping[path] = name
    for name, (_, _, stems) in EXTRA_LANES.items():
        for stem in stems.split():
            mapping[f'tests/test_{stem}.py'] = name
    for path in sorted(ROOT.glob('examples/imdb/tests/test_*.py')):
        mapping[str(path.relative_to(ROOT))] = 'example-contracts'
    mapping['tests/integration/test_relationship_correctness_regressions.py'] = 'service-helper-unit'
    return mapping


def disposition(row, policy):
    path, nodeid = row['file'], row['id']
    if (path == 'tests/test_distribution_foundation.py'
            and '::test_checkout_metrics_report_package_version_or_explicit_override[' not in nodeid):
        return {'kind': 'checkout-development', 'reason': CHECKOUT['distribution_foundation']}
    if nodeid == 'tests/test_metrics_overhead.py::test_prometheus_client_call_cost_ceiling':
        return {'kind': 'checkout-development', 'reason': 'Wall-clock prometheus microbenchmark, no service/runtime guarantee; deterministic per-batch instrumentation tests remain installed.'}
    if set(row['markers']) & LIVE:
        owner = 'E14' if Path(path).name.startswith(('test_service_process_', 'test_service_single_container', 'test_service_systemd')) else 'E09'
        return {'kind': 'service', 'owner': owner, 'reason': f'Explicit live integration/smoke/stress check; retained checkout source, real supported-mode coverage owned by {owner}. No skip counts as installed success.'}
    if nodeid in HISTORY:
        return {'kind': 'checkout-development', 'reason': 'Historical Git slice diff only, not application behavior; all runtime assertions in this mixed file remain installed.'}
    if path in policy:
        return {'kind': 'installed', 'lane': policy[path], 'mapping': nodeid}
    stem = Path(path).stem.removeprefix('test_')
    if stem in CHECKOUT:
        return {'kind': 'checkout-development', 'reason': CHECKOUT[stem]}
    raise ValueError('unclassified runtime test: ' + nodeid)


def source_record(path):
    content = (ROOT / path).read_bytes()
    tree = ast.parse(content, filename=path)
    assertions = [ast.dump(n, include_attributes=False) for n in ast.walk(tree) if isinstance(n, ast.Assert)]
    return {'sha256': sha(content), 'size': len(content), 'assertions': assertions,
            'assertion_count': len(assertions), 'assertion_sha256': sha(canonical(assertions)),
            'test_functions': sum(isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name.startswith('test_') for n in ast.walk(tree))}


def closure(seeds):
    """Recursive local imports, ancestor config/packages and finite dynamic fixtures."""
    python_files = sorted([*ROOT.glob('tests/**/*.py'), *ROOT.glob('examples/**/*.py')])
    candidates = {str(p.relative_to(ROOT)) for p in python_files}
    basenames = {}
    for name in candidates:
        basenames.setdefault(Path(name).name, []).append(name)
    edges, visited = {}, set()
    queue = list(seeds) + ['tests/conftest.py']
    # Finite maintained schema/golden set: includes the complete source-driven
    # schema parametrization, rather than allowing a smaller staging glob to pass.
    data = sorted({str(p.relative_to(ROOT)) for pattern in (
        'config/*.yaml', 'examples/**/*.yaml', 'tests/golden/**/*.yaml', 'tests/golden/*.json')
        for p in ROOT.glob(pattern)})
    visited.update(data)
    while queue:
        path = queue.pop()
        if path in visited:
            continue
        source = ROOT / path
        if source.is_symlink() or not source.is_file():
            raise ValueError('missing/symlink closure file: ' + path)
        visited.add(path)
        dependencies = set()
        for parent in source.parents:
            if parent == ROOT:
                break
            for base in ('__init__.py', 'conftest.py'):
                candidate = str((parent / base).relative_to(ROOT))
                if candidate in candidates and candidate != path:
                    dependencies.add(candidate)
        tree = ast.parse(source.read_bytes(), filename=path)
        for node in ast.walk(tree):
            names = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                base = node.module or ''
                if node.level:
                    parents = Path(path).parent.parts
                    prefix = '.'.join(parents[:len(parents) - node.level + 1])
                    base = '.'.join(p for p in (prefix, base) if p)
                names = [base, *(base + '.' + a.name for a in node.names)]
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                value = node.value
                if value in candidates:
                    dependencies.add(value)
                if len(value) < 200 and value.endswith('.py'):
                    dependencies.update(p for p in basenames.get(Path(value).name, []) if p.startswith('tests/fixtures/'))
                if len(value) < 200 and all(part.isidentifier() for part in value.split('.')):
                    names = [value]
            for name in names:
                if name.startswith('neo4j_graph_loader'):
                    continue
                relative = name.replace('.', '/')
                for prefix in ('', 'tests/', str(Path(path).parent) + '/'):
                    for suffix in ('.py', '/__init__.py'):
                        candidate = prefix + relative + suffix
                        if candidate in candidates:
                            dependencies.add(candidate)
        dependencies.discard(path)
        edges[path] = sorted(dependencies)
        queue.extend(sorted(dependencies - visited))
    if any(p.startswith('examples/imdb/') and p.endswith('.py') for p in visited):
        visited.add('examples/imdb/README.md')
    return sorted(visited), dict(sorted(edges.items()))


def reviewed_renames(data, previous, collected):
    """Map each previously frozen ID that is no longer collected to its reviewed replacement.

    ``data['reviewed_id_renames']`` is ``{old id: {'new_id', 'reviewed', 'reason'}}``. A frozen ID may go
    missing only through such an entry whose replacement is collected, was not frozen before, and lives in the
    same file and the same test function (only the parametrization suffix differs). Any other removal is left
    for the caller's ordering check to refuse. The replacement later takes the old ID's position and disposition.
    """
    entries = data.get('reviewed_id_renames', {})
    renamed = {}
    for old, entry in entries.items():
        if old not in previous or old in collected:
            continue
        new = entry.get('new_id') if isinstance(entry, dict) else None
        if not isinstance(new, str) or not entry.get('reviewed') or not entry.get('reason'):
            raise ValueError('reviewed ID rename needs new_id, reviewed and reason: ' + old)
        if new not in collected or new in previous:
            raise ValueError('reviewed ID rename target must be a newly collected ID: ' + new)
        old_file, _, old_name = old.partition('::')
        new_file, _, new_name = new.partition('::')
        if old_file != new_file or old_name.split('[')[0] != new_name.split('[')[0]:
            raise ValueError('reviewed ID rename must stay in the same file and test function: ' + old)
        renamed[old] = new
    if len(set(renamed.values())) != len(renamed):
        raise ValueError('reviewed ID renames must not share a target')
    return renamed


def reviewed_file_removals(data, previous, collected):
    """Previously frozen IDs whose whole test file was deleted under a reviewed entry.

    ``data['reviewed_file_removals']`` is ``{test file: {'reviewed', 'reason'}}``. Only a whole file may go: it must
    be absent from disk and from the collection, and every frozen ID of that file is dropped. Single IDs still need
    a reviewed rename; any other removal is left for the caller's ordering check to refuse.
    """
    removed = set()
    for path, entry in data.get('reviewed_file_removals', {}).items():
        if not isinstance(entry, dict) or not entry.get('reviewed') or not entry.get('reason'):
            raise ValueError('reviewed file removal needs reviewed and reason: ' + path)
        if (ROOT / path).exists() or any(i.split('::')[0] == path for i in collected):
            raise ValueError('reviewed file removal names a test file that still exists: ' + path)
        removed |= {i for i in previous if i.split('::')[0] == path}
    return removed


def freeze(data, collection, wheel):
    items = collection['items']
    ids = [r['id'] for r in items]
    if not ids or len(ids) != len(set(ids)):
        raise ValueError('empty/duplicate source collection')
    discovered = {str(p.relative_to(ROOT)) for pattern in ('tests/**/test_*.py', 'examples/**/tests/test_*.py') for p in ROOT.glob(pattern)}
    collected = {r['file'] for r in items}
    if discovered != collected:
        raise ValueError('test file missing from full source collection: ' + repr(discovered ^ collected))
    policy = file_policy(data)
    result = json.loads(json.dumps(data))
    result.update(schema_version=1, status='executable inventory; original pre-port baseline kept in the private provenance; acceptance recorded in story reviews', package_version=source_version(),
                  test_requirements=['pytest==9.1.1', 'testcontainers[neo4j,kafka]>=4.8,<5', 'psutil>=6,<8'])
    with zipfile.ZipFile(wheel) as archive:
        result['package_payload'] = {n: sha(archive.read(n)) for n in archive.namelist() if n.startswith('neo4j_graph_loader/')}
        result['distribution_files'] = {n: sha(archive.read(n)) for n in archive.namelist() if '.dist-info/' in n}
    choices = {row['id']: disposition(row, policy) for row in items}
    if 'source_collection' in data:
        previous = data['source_collection']['test_ids']
        previous_set = set(previous)
        renamed = reviewed_renames(data, previous_set, set(ids))
        removed = reviewed_file_removals(data, previous_set, set(ids))
        carried = [renamed.get(i, i) for i in previous if i not in removed]
        carried_set = set(carried)
        if [i for i in ids if i in carried_set] != carried:
            raise ValueError('re-freeze cannot remove/reorder previously frozen IDs')
        # a reviewed rename moves an installed ID's ``mapping`` to the new ID; everything else must be identical
        if any(choices[renamed.get(i, i)] != ({**old, 'mapping': renamed[i]} if i in renamed and 'mapping' in old else old)
               for i, old in data['test_dispositions'].items() if i not in removed):
            raise ValueError('re-freeze cannot silently change a reviewed test disposition')
    result['test_dispositions'] = choices
    result['file_dispositions'] = {p: {'ids': [r['id'] for r in items if r['file'] == p],
        'categories': dict(Counter(choices[r['id']]['kind'] for r in items if r['file'] == p))} for p in sorted(collected)}
    baseline = result.setdefault('original_source_baseline', {})
    for path in sorted(collected):
        if path not in baseline:
            baseline[path] = source_record(path)
    result['source_collection'] = {'test_ids': ids, 'sha256': sha(canonical(ids)), 'files': len(collected), 'counts': dict(Counter(c['kind'] for c in choices.values()))}
    for name, (number, effort, _) in EXTRA_LANES.items():
        result['lanes'].setdefault(name, {'kind': 'installed unit regression', 'owner': f'E07.F1.S{number}', 'estimate_days': effort})
    for name, lane in result['lanes'].items():
        if lane['kind'] != 'installed unit regression':
            continue
        selected = [i for i in ids if choices[i].get('lane') == name]
        files = list(dict.fromkeys(i.split('::')[0] for i in selected))
        complete = [i for i in ids if i.split('::')[0] in files]
        staged, imports = closure(files)
        lane.update(current_test_files=files, test_ids=selected, collected_test_ids=complete,
            excluded_test_ids={i: choices[i]['reason'] for i in complete if i not in selected},
            stage_files={p: sha((ROOT / p).read_bytes()) for p in staged}, recursive_imports=imports,
            reference_files={p: 'Maintained example/reference helper or documentation fixture; never application source.'
                             for p in staged if not p.startswith(('tests/', 'config/'))},
            fixture_counts=dict(Counter(Path(p).suffix for p in staged)),
            extra_test_requirements=['pandas>=2.2,<3', 'python-dotenv>=1,<2'] if name == 'example-contracts' else [],
            target_status='frozen selection; acceptance recorded separately in story reviews')
        lane.setdefault('estimate_days', {'core': 1, 'safety': 1.5, 'cli-runtime': 1.5}.get(name, 1))
        lane['allowed_skips'] = ({'tests/test_schema_multi_pair.py::test_every_shipped_schema_stays_single_pair[examples/multi_topic/graph_schema_multi_pair.yaml]':
                                'the multi-pair example is multi-pair on purpose'} if name == 'core' else {})
        if name == 'orchestration-runtime':       # E15: three cells that need a dev tool or an optional extra the finite lane set excludes
            # the value must be a substring of the skip reason pytest reports (the runner refuses any other skip)
            lane['allowed_skips'] = {
                'tests/test_status_model.py::test_the_validator_agrees_with_jsonschema': "could not import 'jsonschema'",
                'tests/test_status_model.py::test_the_validator_agrees_with_jsonschema_on_the_published_schemas': "could not import 'jsonschema'",
                'tests/test_status_command.py::test_list_reads_the_sql_store_too': "could not import 'sqlalchemy'",
            }
    for name, files in SERVICE_LANES.items():
        selected = [i for i in ids if i.split('::')[0] in files]
        if not selected or {i.split('::')[0] for i in selected} != set(files):
            raise ValueError('service lane test files are missing from the full source collection: ' + name)
        staged, imports = closure(list(files))
        staged = sorted({*staged, *SERVICE_EXTRA_FILES[name]})
        lane = result['lanes'][name]
        lane.update(kind='installed service regression', current_test_files=list(files), test_ids=selected,
                    collected_test_ids=selected, excluded_test_ids={}, allowed_skips={}, extra_test_requirements=[],
                    stage_files={p: sha((ROOT / p).read_bytes()) for p in staged}, recursive_imports=imports,
                    reference_files={p: 'Maintained example/reference helper or documentation fixture; never application source.'
                                     for p in staged if not p.startswith(('tests/', 'config/'))},
                    fixture_counts=dict(Counter(Path(p).suffix for p in staged)),
                    service_dispositions={i: {'owner': lane['owner'], 'reason': SERVICE_REASON[name]} for i in selected},
                    target_status='frozen selection; acceptance recorded separately in story reviews')
    return result


PRIVATE_AMENDMENT_KEYS = ('previous_inventory', 'actual_unchanged_installed_reproduction')
CONCEALED = 'sha256:'
LAN_ADDRESS = re.compile(r'(?<![0-9.])192\.168\.[0-9]{1,3}\.[0-9]{1,3}')


def id_digest(identifier):
    return sha(identifier.encode())


def conceal_renames(data, old_ids):
    """Re-key the reviewed rename entries of ``old_ids`` by the sha256 of the old ID, so the public file does not carry the old string.

    The old ID of a rename exists to be removed from the public tree (it held a private address), so the entry keeps the new ID,
    the reviewer, the reason and the digest only. Returns ``{digest: old ID}`` for the private sidecar.
    """
    entries = data.get('reviewed_id_renames', {})
    hidden = {}
    for old in old_ids:
        entry = entries.pop(old, None)
        if entry is not None:
            digest = id_digest(old)
            entries[CONCEALED + digest] = {**entry, 'old_id_sha256': digest}
            hidden[digest] = old
    return hidden


def resolve_renames(entries, baseline_ids):
    """``{old ID: new ID}`` for the entries that rename an ID of ``baseline_ids`` (plain keys, or keys concealed by digest)."""
    known = set(baseline_ids)
    by_digest = {id_digest(i): i for i in known}
    found = {}
    for key, entry in entries.items():
        old = by_digest.get(key[len(CONCEALED):]) if key.startswith(CONCEALED) else key
        if old in known and isinstance(entry, dict) and isinstance(entry.get('new_id'), str):
            found[old] = entry['new_id']
    return found


def concealed_problems(data):
    """Malformed concealed rename entries, a concealed old ID that is still collected, and private LAN addresses anywhere in the file."""
    problems, ids = [], data['source_collection']['test_ids']
    digests = {id_digest(i) for i in ids}
    for key, entry in data.get('reviewed_id_renames', {}).items():
        if not key.startswith(CONCEALED):
            continue
        digest = key[len(CONCEALED):]
        if not (len(digest) == 64 and all(c in '0123456789abcdef' for c in digest)) or not isinstance(entry, dict):
            problems.append('malformed concealed rename: ' + key[:20])
        elif (entry.get('old_id_sha256') != digest or entry.get('new_id') not in ids or not entry.get('reviewed')
              or not entry.get('reason') or digest in digests):
            problems.append('inconsistent concealed rename: ' + key[:20])
    if LAN_ADDRESS.search(json.dumps(data)):
        problems.append('the inventory contains a private LAN address (192.168.x.x)')
    return problems


def portable_command(command):
    """The collection command with no machine path: interpreter, repository root and output become placeholders."""
    launch = command[3]
    return ['<python>', '-I', '-c', launch, '<repo-root>', '<collection-output>', *command[6:]]


def make_portable(data):
    """A copy of ``data`` that carries no machine, host or private-repository path, plus the private provenance removed from it.

    The public inventory holds repository-relative paths only. What was removed (the collection command with its absolute paths,
    the artifact paths and the private evidence paths of an amendment) is returned separately for a private sidecar.
    """
    result = json.loads(json.dumps(data))
    provenance = {}
    command = result.get('source_collection', {}).get('command')
    if command and command[0] != '<python>':
        provenance['source_collection.command'] = command
        result['source_collection']['command'] = portable_command(command)
    artifacts = result.get('artifact_contract', {}).get('artifacts', [])
    if any(Path(a['path']).is_absolute() for a in artifacts):
        provenance['artifact_contract.artifacts'] = artifacts
        result['artifact_contract']['artifacts'] = [{**a, 'path': Path(a['path']).name} for a in artifacts]
    baseline = result.pop('original_source_baseline', None)
    if baseline:
        provenance['original_source_baseline'] = baseline     # the assertion dumps of the tests at their first freeze: a reviewer aid, private
    for index, amendment in enumerate(result.get('disposition_amendments', [])):
        private = {k: amendment.pop(k) for k in PRIVATE_AMENDMENT_KEYS if k in amendment}
        if private:
            provenance[f'disposition_amendments[{index}]'] = private
    for lane in result.get('lanes', {}).values():
        if 'command' in lane:
            lane['command'] = re.sub(r'--inventory \S*test-inventory\.json', '--inventory tests/inventory/test-inventory.json', lane['command'])
    return result, provenance


def with_private_baseline(data, provenance_path):
    """``data`` plus the ``original_source_baseline`` kept in the private provenance file (the public inventory does not carry it).

    A path the public file still carries wins over the sidecar (it is the same original record); the sidecar fills in the rest, so a
    re-freeze keeps every original dump and records only the files that are new.
    """
    if provenance_path is None or not Path(provenance_path).exists():
        return data
    kept = json.loads(Path(provenance_path).read_text()).get('original_source_baseline')
    if not kept:
        return data
    return {**data, 'original_source_baseline': {**kept, **data.get('original_source_baseline', {})}}


def portability_problems(data):
    """Every metadata path of the inventory that is absolute, climbs out of the repository or names ``plan/`` or ``var/``."""
    values = list(data.get('source_collection', {}).get('command', [])[:3] + data.get('source_collection', {}).get('command', [])[4:])
    values += [a['path'] for a in data.get('artifact_contract', {}).get('artifacts', [])]
    for amendment in data.get('disposition_amendments', []):
        values += [v for v in amendment.values() if isinstance(v, str)]
    for lane in data.get('lanes', {}).values():
        values += [lane.get('command', '')]
        values += [*lane.get('stage_files', {}), *lane.get('reference_files', {}), *lane.get('recursive_imports', {})]
    problems = []
    for value in values:
        if value in ('<python>', '-I', '-c', '<repo-root>', '<collection-output>'):
            continue
        looks_private = (Path(value).is_absolute() or ('/' + 'home/') in value or ('/' + 'Users/') in value or value.startswith(('var/', 'plan/'))
                         or '/plan/' in value or ' plan/' in value or ' var/' in value or '..' in Path(value).parts)
        if looks_private:
            problems.append(value)
    return problems


def field_changes(old, new, path='', depth=0):
    """Readable list of the fields that differ between two inventories (maps are summarized, never dumped)."""
    if isinstance(old, dict) and isinstance(new, dict) and depth < 3 and len(old) + len(new) <= 400:
        found = []
        for key in dict.fromkeys([*old, *new]):
            where = f'{path}.{key}' if path else str(key)
            if key not in old:
                found.append(f'{where}: added')
            elif key not in new:
                found.append(f'{where}: removed')
            else:
                found.extend(field_changes(old[key], new[key], where, depth + 1))
        return found
    if old == new:
        return []
    if isinstance(old, dict) and isinstance(new, dict):
        keys = set(old) | set(new)
        changed = sum(1 for k in keys if k in old and k in new and old[k] != new[k])
        return [f'{path}: {len(keys - set(old))} added, {len(keys - set(new))} removed, {changed} changed (of {len(keys)})']
    if isinstance(old, list) and isinstance(new, list):
        return [f'{path}: list of {len(old)} -> {len(new)}']
    return [f'{path}: changed']


def compare_inventories(old, new, renames=None):
    """Compare a migrated inventory with its baseline; raise on any lost, reordered or re-dispositioned ID.

    Every baseline ID must survive in the same relative order (a reviewed rename maps it to its replacement, in the same position),
    keep its disposition (the ``mapping`` of an installed ID follows a rename), and stay listed in the same lane. New IDs may be
    added anywhere. Returns the report of added IDs, renames and changed fields.
    """
    baseline, migrated = old['source_collection']['test_ids'], new['source_collection']['test_ids']
    renames = dict(renames) if renames is not None else resolve_renames(new.get('reviewed_id_renames', {}), baseline)
    expected = [renames.get(i, i) for i in baseline]
    if len(set(migrated)) != len(migrated):
        raise ValueError('migrated inventory lists a duplicate ID')
    missing = [i for i in expected if i not in set(migrated)]
    if missing:
        raise ValueError(f'migration lost {len(missing)} IDs, first: {missing[0]}')
    kept = set(expected)
    if [i for i in migrated if i in kept] != expected:
        raise ValueError('migration reordered previously frozen IDs')
    changed = []
    for before in baseline:
        after = renames.get(before, before)
        was, now = old['test_dispositions'][before], new['test_dispositions'][after]
        was = {**was, 'mapping': after} if 'mapping' in was else was
        if was != now:
            changed.append(before)
    if changed:
        raise ValueError(f'migration changed {len(changed)} dispositions, first: {changed[0]}')
    for name, lane in old['lanes'].items():
        if lane.get('kind') in ('installed unit regression', 'installed service regression'):
            before = [renames.get(i, i) for i in lane['test_ids']]
            after = [i for i in new['lanes'][name]['test_ids'] if i in set(before)]
            if before != after:
                raise ValueError('migration changed the selection or order of lane ' + name)
    added = [i for i in migrated if i not in kept]
    return {'baseline_ids': len(baseline), 'migrated_ids': len(migrated), 'order_preserved': True, 'dispositions_compared': len(baseline),
            'dispositions_changed': 0, 'renamed': renames, 'added_ids': added, 'changed_fields': field_changes(old, new)}


def check(data, collection):
    actual = [r['id'] for r in collection['items']]
    if actual != data['source_collection']['test_ids']:
        raise ValueError('source IDs/order disappeared, changed or were deselected')
    found = {str(p.relative_to(ROOT)) for pattern in ('tests/**/test_*.py', 'examples/**/tests/test_*.py') for p in ROOT.glob(pattern)}
    if found != set(data['file_dispositions']):
        raise ValueError('source test files differ from frozen inventory')
    for lane in data['lanes'].values():
        if lane['kind'] in ('installed unit regression', 'installed service regression'):
            for path, digest in lane['stage_files'].items():
                if not (ROOT / path).is_file() or sha((ROOT / path).read_bytes()) != digest:
                    raise ValueError('staged file missing/changed: ' + path)
    if portability_problems(data):
        raise ValueError('inventory carries private or machine paths: ' + repr(portability_problems(data)[:3]))
    if concealed_problems(data):
        raise ValueError('inventory rename records are not clean: ' + repr(concealed_problems(data)[:3]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', type=Path, default=INVENTORY)
    parser.add_argument('--collection-output', type=Path)
    parser.add_argument('--freeze', action='store_true')
    parser.add_argument('--wheel', type=Path)
    parser.add_argument('--sdist', type=Path)
    parser.add_argument('--migrate-from', type=Path, help='baseline inventory to migrate: freeze from it, compare, and write --inventory')
    parser.add_argument('--rename', nargs=3, action='append', default=[], metavar=('OLD_ID', 'NEW_ID', 'REASON'),
                        help='record a reviewed ID rename in the migrated inventory (same file and test function only)')
    parser.add_argument('--reviewed-by', help='who reviewed the --rename entries, with the date (required with --rename)')
    parser.add_argument('--report', type=Path, help='with --migrate-from: write the comparison report here')
    parser.add_argument('--provenance', type=Path, help='write the private provenance removed from the public inventory here (merged if it exists)')
    parser.add_argument('--conceal-renames', nargs='+', metavar='OLD_ID',
                        help='re-key these reviewed renames of --inventory by the sha256 of the old ID (no collection; the old IDs go to --provenance)')
    args = parser.parse_args()
    if args.conceal_renames:
        data = json.loads(args.inventory.read_text())
        hidden = conceal_renames(data, args.conceal_renames)
        if not hidden:
            parser.error('none of the IDs has a reviewed rename entry in the inventory')
        if concealed_problems(data):
            parser.error('inventory rename records are not clean: ' + repr(concealed_problems(data)[:3]))
        if args.provenance:
            kept = json.loads(args.provenance.read_text()) if args.provenance.exists() else {}
            kept.setdefault('reviewed_id_renames.old_ids', {}).update(hidden)
            args.provenance.write_text(json.dumps(kept, indent=2) + '\n')
        args.inventory.write_text(json.dumps(data, indent=2) + '\n')
        print(json.dumps({'concealed': len(hidden)}))
        return
    if args.collection_output is None:
        parser.error('--collection-output is required')
    if args.collection_output.exists() or args.collection_output.with_suffix('.log').exists():
        parser.error('collection output must be new')
    if args.rename and not args.reviewed_by:
        parser.error('--rename needs --reviewed-by')
    if args.migrate_from:
        args.freeze = True
    collection, command = collect(args.collection_output.resolve())
    data = json.loads((args.migrate_from or args.inventory).read_text())
    if args.freeze:
        data = with_private_baseline(data, args.provenance)
        if args.wheel is None or args.sdist is None:
            parser.error('--freeze requires explicit wheel and sdist for strict validation')
        baseline = json.loads(json.dumps(data))
        renames = {}
        for old_id, new_id, reason in args.rename:
            data.setdefault('reviewed_id_renames', {})[old_id] = {'new_id': new_id, 'reviewed': args.reviewed_by, 'reason': reason}
            renames[old_id] = new_id
        spec = importlib.util.spec_from_file_location('strict_artifacts', ROOT/'scripts/verify_artifacts.py')
        strict = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(strict)
        contract = strict.inspect(args.wheel, args.sdist)
        data = freeze(data, collection, args.wheel)
        data['artifact_contract'] = contract
        data['source_collection']['command'] = command
        data, provenance = make_portable(data)
        if portability_problems(data):
            raise ValueError('inventory still carries private paths: ' + repr(portability_problems(data)[:3]))
        if args.migrate_from:
            report = compare_inventories(baseline, data, renames)
            provenance.setdefault('reviewed_id_renames.old_ids', {}).update(conceal_renames(data, list(renames)))
            if resolve_renames(data['reviewed_id_renames'], baseline['source_collection']['test_ids']) != renames:
                raise ValueError('concealed rename records do not reproduce the renames')
            compare_inventories(baseline, data)        # the comparison also works from the concealed records alone
            report['renamed'] = {id_digest(old): new for old, new in renames.items()}     # the report names no old ID
            if args.report:
                args.report.write_text(json.dumps(report, indent=2) + '\n')
        if concealed_problems(data):
            raise ValueError('inventory rename records are not clean: ' + repr(concealed_problems(data)[:3]))
        if args.provenance:
            kept = json.loads(args.provenance.read_text()) if args.provenance.exists() else {}
            args.provenance.write_text(json.dumps({**kept, **provenance}, indent=2) + '\n')
        args.inventory.parent.mkdir(parents=True, exist_ok=True)
        args.inventory.write_text(json.dumps(data, indent=2) + '\n')
    else:
        check(data, collection)
    print(json.dumps({'test_ids': len(collection['items']), 'files': len(data['file_dispositions']), 'status': 'frozen' if args.freeze else 'matched'}))


if __name__ == '__main__':
    main()
