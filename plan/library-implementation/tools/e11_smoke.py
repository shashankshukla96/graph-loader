"""Installed-CLI real-load smoke: a small planned bulk load against an owned Neo4j + Kafka stack.

    python e11_smoke.py STACK_DIR OVERRIDES_SH EVIDENCE_JSON

Run with the Python of a fresh virtualenv that has only the candidate wheel installed (plus nothing from the
checkout), from a directory outside the checkout. STACK_DIR holds stack.json and credentials.env written by
e11_stack.py (the password is read from that 0600 file into this process and the child environment, never
printed or placed in arguments). Publishes three node topics and three relationship topics (one self-referencing
type), runs `graph-loader start --mode bulk --skip-image-build` with the overrides, then checks: exit code, the
launch/completion stage lines, the exact node and relationship counts and sampled properties, committed node
consumer-group offsets at the end of every partition, the completion ledger, and that no loader container of the
run remains.
"""
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from neo4j import GraphDatabase

PERSONS, CITIES, COMPANIES = 300, 6, 12
SCHEMA = """loading:
  mode: bulk
  consumer_group_id: e11-smoke
nodes:
  - label: Person
    topic: e11-person
    key_property: person_id
    properties:
      person_id: {type: integer, required: true, constraint: {type: uniqueness}}
      name: {type: string, required: true}
      joined: {type: date}
  - label: City
    topic: e11-city
    key_property: name
    properties:
      name: {type: string, required: true, constraint: {type: uniqueness}}
  - label: Company
    topic: e11-company
    key_property: company_id
    properties:
      company_id: {type: string, required: true, constraint: {type: uniqueness}}
edges:
  - type: LIVES_IN
    topic: e11-lives-in
    nodes: {source: Person, target: City}
    source_key_property: person_id
    target_key_property: name
    properties:
      since: {type: date}
  - type: WORKS_AT
    topic: e11-works-at
    nodes: {source: Person, target: Company}
    source_key_property: person_id
    target_key_property: company_id
    properties:
      role: {type: string}
  - type: FRIEND_OF
    topic: e11-friend-of
    nodes: {source: Person, target: Person}
    source_key_property: person_id
    target_key_property: person_id
    properties:
      weight: {type: integer}
"""
PARTITIONS = 2
TOPICS = ['e11-person', 'e11-city', 'e11-company', 'e11-lives-in', 'e11-works-at', 'e11-friend-of']


def dated(day):
    return f'2024-{1 + day % 12:02d}-{1 + day % 28:02d}'


def main():
    stack_dir, overrides, evidence_path = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
    stack = json.loads((stack_dir / 'stack.json').read_text())
    creds = dict(line.split('=', 1) for line in (stack_dir / 'credentials.env').read_text().splitlines() if '=' in line)
    kafka, bolt = stack['kafka_bootstrap_host'], stack['neo4j_uri_host']
    evidence = {'stack_token': stack['token'], 'network': stack['network'], 'checks': {}}

    admin = AdminClient({'bootstrap.servers': kafka})
    for name, future in admin.create_topics([NewTopic(t, num_partitions=PARTITIONS, replication_factor=1) for t in TOPICS]).items():
        future.result(30)
    producer = Producer({'bootstrap.servers': kafka})
    expected = {'Person': PERSONS, 'City': CITIES, 'Company': COMPANIES}
    sent = {t: 0 for t in TOPICS}

    def send(topic, key, value):
        producer.produce(topic, key=str(key), value=json.dumps(value))
        sent[topic] += 1

    for p in range(PERSONS):
        send('e11-person', p, {'person_id': p, 'name': f'person-{p}', 'joined': dated(p)})
    for c in range(CITIES):
        send('e11-city', c, {'name': f'city-{c}'})
    for k in range(COMPANIES):
        send('e11-company', k, {'company_id': f'co-{k}'})
    lives = {p: p % CITIES for p in range(PERSONS)}
    works = {p: p % COMPANIES for p in range(PERSONS)}
    friends = {(p, (p + step) % PERSONS) for p in range(PERSONS) for step in (1, 7)}
    for p, c in lives.items():
        send('e11-lives-in', p, {'source': {'person_id': p}, 'target': {'name': f'city-{c}'}, 'properties': {'since': dated(p + 3)}})
    for p, k in works.items():
        send('e11-works-at', p, {'source': {'person_id': p}, 'target': {'company_id': f'co-{k}'}, 'properties': {'role': f'role-{p % 4}'}})
    for p, q in sorted(friends):
        send('e11-friend-of', p, {'source': {'person_id': p}, 'target': {'person_id': q}, 'properties': {'weight': (p + q) % 5}})
    producer.flush(60)
    evidence['published'] = sent

    schema = Path('e11-smoke-schema.yaml')
    schema.write_text(SCHEMA)
    env = {k: v for k, v in os.environ.items() if not k.startswith(('NEO4J_', 'KAFKA_', 'LOADER_', 'GRAPH_LOADER_'))}
    for line in overrides.read_text().splitlines():
        if line.startswith('export '):
            key, _, value = line[len('export '):].partition('=')
            env[key] = value
    env.update(NEO4J_URI=bolt, NEO4J_USERNAME='neo4j', NEO4J_PASSWORD=creds['NEO4J_PASSWORD'], NEO4J_DATABASE='neo4j',
               KAFKA_BOOTSTRAP_SERVERS=kafka, LOADER_NEO4J_URI=stack['neo4j_uri_network'],
               LOADER_KAFKA_BOOTSTRAP_SERVERS=stack['kafka_bootstrap_network'])
    started = time.time()
    run_dir = Path('runs').resolve()
    result = subprocess.run([str(Path(sys.executable).parent / 'graph-loader'), 'start', '--mode', 'bulk', '--config', str(schema),
                             '--network', stack['network'], '--skip-image-build', '--run-dir', str(run_dir)],
                            env=env, capture_output=True, text=True, timeout=1500)
    output = result.stdout + result.stderr
    Path('cli-output.log').write_text(output)
    evidence['cli'] = {'exit_code': result.returncode, 'seconds': round(time.time() - started, 1),
                       'password_in_output': creds['NEO4J_PASSWORD'] in output,
                       'launch_line': next((l for l in output.splitlines() if 'stage=launch edge_mode=' in l), None),
                       'node_ready_lines': sum('stage=node-label-ready' in l for l in output.splitlines()),
                       'planned_complete_line': next((l for l in output.splitlines() if 'stage=planned-edges-complete' in l), None)}

    driver = GraphDatabase.driver(bolt, auth=('neo4j', creds['NEO4J_PASSWORD']))
    with driver.session(database='neo4j') as session:
        nodes = {r['l']: r['n'] for r in session.run('MATCH (n) UNWIND labels(n) AS l RETURN l, count(*) AS n')}
        rels = {r['t']: r['n'] for r in session.run('MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS n')}
        dup = session.run('MATCH (a)-[r]->(b) WITH a, b, type(r) AS t, count(*) AS n WHERE n > 1 RETURN count(*) AS d').single()['d']
        sample = session.run('MATCH (p:Person {person_id: 17})-[l:LIVES_IN]->(c:City), (p)-[w:WORKS_AT]->(k:Company) '
                             'RETURN p.name AS name, toString(p.joined) AS joined, c.name AS city, toString(l.since) AS since, '
                             'k.company_id AS company, w.role AS role').single()
        self_loop = session.run('MATCH (p:Person {person_id: 5})-[f:FRIEND_OF]->(q:Person) RETURN collect(q.person_id) AS qs').single()['qs']
    driver.close()
    exp_rel = {'LIVES_IN': PERSONS, 'WORKS_AT': PERSONS, 'FRIEND_OF': len(friends)}
    evidence['graph'] = {'nodes': nodes, 'relationships': rels, 'duplicate_relationships': dup, 'sample': dict(sample),
                         'friends_of_5': sorted(self_loop)}
    evidence['checks']['exit_code_zero'] = result.returncode == 0
    evidence['checks']['planned_default'] = bool(evidence['cli']['launch_line']) and 'edge_mode=planned reason=default' in evidence['cli']['launch_line']
    evidence['checks']['node_counts'] = nodes == expected
    evidence['checks']['relationship_counts'] = rels == exp_rel
    evidence['checks']['no_duplicate_relationships'] = dup == 0
    evidence['checks']['sample'] = dict(sample) == {'name': 'person-17', 'joined': dated(17), 'city': f'city-{17 % CITIES}',
                                                    'since': dated(20), 'company': f'co-{17 % COMPANIES}', 'role': f'role-{17 % 4}'}
    evidence['checks']['self_reference'] = sorted(self_loop) == sorted(q for p, q in friends if p == 5)
    evidence['checks']['password_not_in_output'] = not evidence['cli']['password_in_output']

    # Node consumer-group offsets: committed == end offset on every partition (planned mode commits no edge group).
    offsets = {}
    for label, topic in (('Person', 'e11-person'), ('City', 'e11-city'), ('Company', 'e11-company')):
        group = f'e11-smoke-{label}'
        consumer = Consumer({'bootstrap.servers': kafka, 'group.id': group, 'enable.auto.commit': False})
        partitions = [TopicPartition(topic, p) for p in range(PARTITIONS)]
        committed = {tp.partition: tp.offset for tp in consumer.committed(partitions, timeout=30)}
        end = {p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=30)[1] for p in range(PARTITIONS)}
        consumer.close()
        offsets[label] = {'group': group, 'committed': committed, 'end': end}
    evidence['offsets'] = offsets
    evidence['checks']['node_offsets_at_end'] = all(v['committed'] == v['end'] for v in offsets.values())

    attempt = next((l.split('directory=', 1)[1].strip() for l in output.splitlines() if 'stage=runtime' in l and 'directory=' in l), None)
    ledgers = sorted(Path(attempt).glob('plans/*.ledger.json')) if attempt else []
    evidence['attempt_directory_logged'] = bool(attempt)
    if ledgers:
        ledger = json.loads(ledgers[0].read_text())
        evidence['ledger'] = {'file': ledgers[0].name, 'keys': sorted(ledger)[:12]}
    evidence['checks']['ledger_present'] = bool(ledgers)
    left = subprocess.run(['docker', 'ps', '-a', '-q', '--filter', f'network={stack["network"]}'], capture_output=True, text=True).stdout.split()
    owned = subprocess.run(['docker', 'ps', '-a', '--format', '{{.ID}} {{.Names}} {{.Image}}', '--filter', f'network={stack["network"]}'],
                           capture_output=True, text=True).stdout.splitlines()
    evidence['containers_on_network_after'] = owned
    evidence['checks']['only_stack_containers_remain'] = len(left) == 2
    ok = all(v is True for v in evidence['checks'].values())
    evidence['ok'] = ok
    evidence_path.write_text(json.dumps(evidence, indent=2) + '\n')
    print(json.dumps({'ok': ok, 'checks': evidence['checks']}))
    return 0 if ok else 1


sys.exit(main())
