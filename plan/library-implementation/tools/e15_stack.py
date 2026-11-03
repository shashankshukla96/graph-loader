"""Owned, small Neo4j + Kafka + PostgreSQL stack for the E15.F2.S12 live proof matrix (runs on the Docker host, key-auth ssh only).

Adapted from ``e11_stack.py``: ``e15-`` owner label and names, tmpfs data (no volumes, no bind-mounted data directories), PostgreSQL added,
fixed 127.0.0.1 ports (17687 bolt, 19092 kafka, 15432 postgres: the caller checks them first), memory caps on every container.

up:     writes an fsynced registry (intent before every create), creates the network and the three containers, waits until each answers,
        and writes the connection facts to ``stack.json`` and the passwords to ``credentials.env`` (0600). Nothing is printed that holds a password.
down:   removes ONLY the recorded containers and networks, after checking each carries this stack's owner label.
"""
import json
import os
import secrets
import socket
import sys
import time
from pathlib import Path

import docker
from confluent_kafka.admin import AdminClient
from neo4j import GraphDatabase

OWNER_LABEL = 'io.graph-loader.e15-owner'
FOREIGN_PORTS = {5432, 7474, 7687, 8080, 9092, 9101}
BOLT, KAFKA, POSTGRES = 17687, 19092, 15432
NEO4J_IMAGE = os.environ.get('E15_NEO4J_IMAGE', 'neo4j:5-enterprise')
KAFKA_IMAGE = os.environ.get('E15_KAFKA_IMAGE', 'confluentinc/cp-kafka:7.7.0')
POSTGRES_IMAGE = os.environ.get('E15_POSTGRES_IMAGE', 'postgres:latest')
SOCKET = os.environ.get('E15_DOCKER_HOST', 'unix:///var/run/docker.sock')


def port_is_free(port):
    with socket.socket() as s:
        return s.connect_ex(('127.0.0.1', port)) != 0


class Registry:
    def __init__(self, path, owner):
        self.path, self.owner = Path(path), owner
        os.close(os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600))

    def write(self, kind, **values):
        row = {'kind': kind, 'owner': self.owner, 'at': time.time(), **values}
        fd = os.open(self.path, os.O_WRONLY | os.O_APPEND)
        try:
            os.write(fd, (json.dumps(row, sort_keys=True) + '\n').encode())
            os.fsync(fd)
        finally:
            os.close(fd)


def up(workdir, token):
    workdir = Path(workdir)
    for port in (BOLT, KAFKA, POSTGRES):
        assert port not in FOREIGN_PORTS and port_is_free(port), f'port {port} is not free'
    client = docker.DockerClient(base_url=SOCKET, timeout=120)
    labels = {OWNER_LABEL: token}
    registry = Registry(workdir / 'registry.jsonl', token)
    images = {'neo4j': client.images.get(NEO4J_IMAGE).id, 'kafka': client.images.get(KAFKA_IMAGE).id,
              'postgres': client.images.get(POSTGRES_IMAGE).id}        # present locally: a missing image is a pull, which needs approval
    registry.write('plan', images=images)
    names = {k: f'e15-{token}-{k}' for k in ('neo', 'kafka', 'pg', 'net')}
    neo_password, pg_password = secrets.token_hex(16), secrets.token_hex(16)

    def make(resource, name, call):
        registry.write('intent', resource=resource, name=name)
        obj = call()
        registry.write('created', resource=resource, name=name, id=getattr(obj, 'id', None) or name)
        return obj

    make('network', names['net'], lambda: client.networks.create(names['net'], driver='bridge', labels=labels))
    neo = make('container', names['neo'], lambda: client.containers.create(
        images['neo4j'], name=names['neo'], labels={**labels, 'role': 'neo4j'}, network=names['net'],
        environment={'NEO4J_AUTH': 'neo4j/' + neo_password, 'NEO4J_ACCEPT_LICENSE_AGREEMENT': 'yes',
                     'NEO4J_PLUGINS': '["apoc"]', 'NEO4J_dbms_security_procedures_unrestricted': 'apoc.*',
                     'NEO4J_dbms_security_procedures_allowlist': 'apoc.*',
                     'NEO4J_server_memory_heap_initial__size': '512M', 'NEO4J_server_memory_heap_max__size': '512M',
                     'NEO4J_server_memory_pagecache_size': '256M'},
        ports={'7687/tcp': ('127.0.0.1', BOLT)}, tmpfs={'/data': 'rw,size=1g,uid=7474,gid=7474,mode=700', '/logs': 'rw,size=64m,uid=7474,gid=7474,mode=700'},
        mem_limit='2g', restart_policy={'Name': 'no'}))
    kafka = make('container', names['kafka'], lambda: client.containers.create(
        images['kafka'], name=names['kafka'], labels={**labels, 'role': 'kafka'}, network=names['net'],
        environment={'KAFKA_NODE_ID': '1', 'KAFKA_PROCESS_ROLES': 'broker,controller',
                     'KAFKA_LISTENERS': 'PLAINTEXT://0.0.0.0:9093,BROKER://0.0.0.0:9092,CONTROLLER://0.0.0.0:9094',
                     'KAFKA_ADVERTISED_LISTENERS': f'PLAINTEXT://127.0.0.1:{KAFKA},BROKER://{names["kafka"]}:9092',
                     'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'CONTROLLER:PLAINTEXT,BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
                     'KAFKA_INTER_BROKER_LISTENER_NAME': 'BROKER', 'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER',
                     'KAFKA_CONTROLLER_QUORUM_VOTERS': '1@localhost:9094', 'CLUSTER_ID': 'MkU3OEVBNTcwNTJENDM2Qk',
                     'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': '1', 'KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS': '1',
                     'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': '1', 'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': '1',
                     'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS': '0', 'KAFKA_HEAP_OPTS': '-Xms512m -Xmx768m'},
        ports={'9093/tcp': ('127.0.0.1', KAFKA)}, tmpfs={'/var/lib/kafka/data': 'rw,size=1g,uid=1000,gid=1000,mode=700'},
        mem_limit='1280m', restart_policy={'Name': 'no'}))
    pg = make('container', names['pg'], lambda: client.containers.create(
        images['postgres'], name=names['pg'], labels={**labels, 'role': 'postgres'}, network=names['net'],
        environment={'POSTGRES_PASSWORD': pg_password, 'POSTGRES_USER': 'glstatus', 'POSTGRES_DB': 'graphloader_status'},
        ports={'5432/tcp': ('127.0.0.1', POSTGRES)}, tmpfs={'/var/lib/postgresql/data': 'rw,size=512m,uid=999,gid=999,mode=700'},
        mem_limit='384m', restart_policy={'Name': 'no'}))
    for container in (neo, kafka, pg):
        container.start()
    for container in (neo, kafka, pg):
        container.reload()
        for binding in container.attrs['NetworkSettings']['Ports'].values():
            if binding and any(b['HostIp'] != '127.0.0.1' or int(b['HostPort']) in FOREIGN_PORTS for b in binding):
                raise RuntimeError('unsafe published port')
    driver = GraphDatabase.driver(f'bolt://127.0.0.1:{BOLT}', auth=('neo4j', neo_password), connection_timeout=2)
    for _ in range(180):
        try:
            driver.verify_connectivity()
            with driver.session(database='system') as session:
                session.run('SHOW DATABASES YIELD name RETURN name').consume()
            break
        except Exception:
            time.sleep(2)
    else:
        raise RuntimeError('Neo4j did not become ready')
    with driver.session(database='system') as session:                 # the live_stack preflight: classic edges need apoc.lock.nodes
        component = session.run('CALL dbms.components() YIELD name, versions, edition RETURN name, versions, edition').single()
        procedures = sorted(r['name'] for r in session.run(
            "SHOW PROCEDURES YIELD name WHERE name IN ['apoc.util.sleep','apoc.lock.nodes'] RETURN name"))
    if procedures != ['apoc.lock.nodes', 'apoc.util.sleep']:
        raise RuntimeError(f'APOC is not available: {procedures}')
    neo4j_facts = {'version': component['versions'][0], 'edition': component['edition'], 'apoc': procedures}
    driver.close()
    admin = AdminClient({'bootstrap.servers': f'127.0.0.1:{KAFKA}', 'socket.timeout.ms': 2000})
    for _ in range(120):
        try:
            if admin.list_topics(timeout=2).brokers:
                break
        except Exception:
            time.sleep(1)
    else:
        raise RuntimeError('Kafka did not become ready')
    for _ in range(90):
        code, _output = pg.exec_run(['pg_isready', '-U', 'glstatus', '-d', 'graphloader_status'])
        if code == 0:
            break
        time.sleep(1)
    else:
        raise RuntimeError('PostgreSQL did not become ready')
    version = pg.exec_run(['postgres', '--version'])[1].decode().strip()
    info = {'token': token, 'names': names, 'images': images, 'network': names['net'], 'postgres_version': version,
            'neo4j_uri_host': f'bolt://127.0.0.1:{BOLT}', 'neo4j_uri_network': f'bolt://{names["neo"]}:7687',
            'kafka_bootstrap_host': f'127.0.0.1:{KAFKA}', 'kafka_bootstrap_network': f'{names["kafka"]}:9092',
            'neo4j': neo4j_facts, 'postgres_port': POSTGRES, 'limits': {'neo4j': '2g', 'kafka': '1280m', 'postgres': '384m'}}
    fd = os.open(workdir / 'credentials.env', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as f:
        f.write(f'NEO4J_USERNAME=neo4j\nNEO4J_PASSWORD={neo_password}\nNEO4J_URI={info["neo4j_uri_host"]}\n'
                f'LOADER_NEO4J_URI={info["neo4j_uri_network"]}\nKAFKA_BOOTSTRAP_SERVERS={info["kafka_bootstrap_host"]}\n'
                f'LOADER_KAFKA_BOOTSTRAP_SERVERS={info["kafka_bootstrap_network"]}\n'
                f'GRAPH_LOADER_STATUS_DB_URL=postgresql+psycopg://glstatus:{pg_password}@127.0.0.1:{POSTGRES}/graphloader_status\n'
                f'E15_STACK_TOKEN={token}\n')
    (workdir / 'stack.json').write_text(json.dumps(info, indent=1, sort_keys=True) + '\n')
    registry.write('ready', bolt=BOLT, kafka=KAFKA, postgres=POSTGRES)
    print(json.dumps({key: value for key, value in info.items()}, indent=1, sort_keys=True))


def down(workdir):
    workdir = Path(workdir)
    rows = [json.loads(line) for line in (workdir / 'registry.jsonl').read_text().splitlines() if line.strip()]
    owners = {r['owner'] for r in rows}
    assert len(owners) == 1, owners
    token = owners.pop()
    client = docker.DockerClient(base_url=SOCKET, timeout=120)
    created = {}
    for r in rows:
        if r['kind'] == 'created':
            created.setdefault(r['resource'], []).append(r['id'])
    done = []
    for cid in created.get('container', []):
        try:
            c = client.containers.get(cid)
        except docker.errors.NotFound:
            continue
        assert c.labels.get(OWNER_LABEL) == token, f'{cid} is not owned by this stack'
        c.remove(force=True, v=True)
        done.append(('container', c.name, cid))
    for nid in created.get('network', []):
        try:
            n = client.networks.get(nid)
        except docker.errors.NotFound:
            continue
        assert n.attrs.get('Labels', {}).get(OWNER_LABEL) == token
        n.reload()
        for member in n.containers:                    # role containers the CLI created ON this private network: owned by construction
            if member.id not in created.get('container', []):
                member.remove(force=True, v=True)
                done.append(('role-container', member.name, member.id))
        n.remove()
        done.append(('network', n.name, nid))
    print(json.dumps(done))


if __name__ == '__main__':
    command = sys.argv[1]
    if command == 'up':
        up(sys.argv[2], sys.argv[3])
    else:
        down(sys.argv[2])
