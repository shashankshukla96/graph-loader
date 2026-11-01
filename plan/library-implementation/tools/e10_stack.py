"""Fresh, owned, small Neo4j 5 Enterprise + Kafka 7.7.0 stack for the E10 quick-start validation (runs on the Docker host, key-auth ssh only).

Adapted from the IMDb large-load stack script: own owner label, `e10-` names, small memory limits.

create: writes an fsynced registry (intent before every create), creates volumes, a user-defined bridge network, Neo4j and Kafka
        (127.0.0.1-only engine-chosen ports, memory/CPU limits, restart unless-stopped) and writes credentials to a 0600 file.
remove: removes ONLY the recorded containers, network and volumes, after checking each carries this stack's owner label.
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

OWNER_LABEL = 'io.graph-loader.e10-owner'
FOREIGN_PORTS = {5432, 7474, 7687, 8080, 9092, 9101}
NEO4J_IMAGE, KAFKA_IMAGE = 'neo4j:5-enterprise', 'confluentinc/cp-kafka:7.7.0'


def free_port():
    while True:
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]
        if port not in FOREIGN_PORTS:
            return port


class Registry:
    def __init__(self, path, owner):
        self.path, self.owner = Path(path), owner
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(fd)

    def write(self, kind, **values):
        row = {'kind': kind, 'owner': self.owner, 'at': time.time(), **values}
        fd = os.open(self.path, os.O_WRONLY | os.O_APPEND)
        try:
            os.write(fd, (json.dumps(row, sort_keys=True) + '\n').encode())
            os.fsync(fd)
        finally:
            os.close(fd)


def create(workdir):
    workdir = Path(workdir)
    token = secrets.token_hex(8)
    password = secrets.token_hex(16)
    client = docker.DockerClient(base_url='unix:///var/run/docker.sock', timeout=120)
    labels = {OWNER_LABEL: token}
    registry = Registry(workdir / 'registry.jsonl', token)
    registry.write('plan', neo4j_image=NEO4J_IMAGE, kafka_image=KAFKA_IMAGE)
    images = {'neo4j': client.images.get(NEO4J_IMAGE).id, 'kafka': client.images.get(KAFKA_IMAGE).id}

    def make(resource, name, call):
        registry.write('intent', resource=resource, name=name)
        obj = call()
        registry.write('created', resource=resource, name=name, id=getattr(obj, 'id', None) or name)
        return obj

    names = {k: f'e10-{token}-{k}' for k in ('neo', 'kafka', 'net', 'neo-data', 'neo-logs', 'kafka-data')}
    for key in ('neo-data', 'neo-logs', 'kafka-data'):
        make('volume', names[key], lambda key=key: client.volumes.create(name=names[key], labels=labels))
    network = make('network', names['net'], lambda: client.networks.create(names['net'], driver='bridge', labels=labels))
    kafka_port = free_port()
    neo = make('container', names['neo'], lambda: client.containers.create(
        images['neo4j'], name=names['neo'], labels={**labels, 'role': 'neo4j'}, network=names['net'],
        environment={'NEO4J_AUTH': 'neo4j/' + password, 'NEO4J_ACCEPT_LICENSE_AGREEMENT': 'yes',
                     'NEO4J_server_memory_heap_initial__size': '512M', 'NEO4J_server_memory_heap_max__size': '512M',
                     'NEO4J_server_memory_pagecache_size': '256M'},
        ports={'7687/tcp': ('127.0.0.1', None), '7474/tcp': ('127.0.0.1', None)},
        volumes={names['neo-data']: {'bind': '/data', 'mode': 'rw'}, names['neo-logs']: {'bind': '/logs', 'mode': 'rw'}},
        mem_limit='2g', nano_cpus=2_000_000_000, restart_policy={'Name': 'no'}))
    kafka = make('container', names['kafka'], lambda: client.containers.create(
        images['kafka'], name=names['kafka'], labels={**labels, 'role': 'kafka'}, network=names['net'],
        environment={'KAFKA_NODE_ID': '1', 'KAFKA_PROCESS_ROLES': 'broker,controller',
                     'KAFKA_LISTENERS': 'PLAINTEXT://0.0.0.0:9093,BROKER://0.0.0.0:9092,CONTROLLER://0.0.0.0:9094',
                     'KAFKA_ADVERTISED_LISTENERS': f'PLAINTEXT://127.0.0.1:{kafka_port},BROKER://{names["kafka"]}:9092',
                     'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'CONTROLLER:PLAINTEXT,BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
                     'KAFKA_INTER_BROKER_LISTENER_NAME': 'BROKER', 'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER',
                     'KAFKA_CONTROLLER_QUORUM_VOTERS': '1@localhost:9094', 'CLUSTER_ID': 'MkU3OEVBNTcwNTJENDM2Qk',
                     'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': '1', 'KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS': '1',
                     'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': '1', 'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': '1',
                     'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS': '0', 'KAFKA_HEAP_OPTS': '-Xms512m -Xmx768m'},
        ports={'9093/tcp': ('127.0.0.1', kafka_port)},
        volumes={names['kafka-data']: {'bind': '/var/lib/kafka/data', 'mode': 'rw'}},
        mem_limit='1536m', restart_policy={'Name': 'no'}))
    neo.start()
    kafka.start()
    neo.reload()
    kafka.reload()
    for container in (neo, kafka):
        for binding in container.attrs['NetworkSettings']['Ports'].values():
            if binding and any(b['HostIp'] != '127.0.0.1' or int(b['HostPort']) in FOREIGN_PORTS for b in binding):
                raise RuntimeError('unsafe published port')
    bolt = int(neo.attrs['NetworkSettings']['Ports']['7687/tcp'][0]['HostPort'])
    http = int(neo.attrs['NetworkSettings']['Ports']['7474/tcp'][0]['HostPort'])
    driver = GraphDatabase.driver(f'bolt://127.0.0.1:{bolt}', auth=('neo4j', password), connection_timeout=2)
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
    admin = AdminClient({'bootstrap.servers': f'127.0.0.1:{kafka_port}', 'socket.timeout.ms': 2000})
    for _ in range(120):
        try:
            if admin.list_topics(timeout=2).brokers:
                break
        except Exception:
            time.sleep(1)
    else:
        raise RuntimeError('Kafka did not become ready')
    with driver.session(database='system') as session:
        settings = {r['name']: r['value'] for r in session.run(
            "SHOW SETTINGS YIELD name, value WHERE name IN ['server.memory.heap.max_size','server.memory.heap.initial_size','server.memory.pagecache.size','db.logs.query.enabled'] RETURN name, value")}
    driver.close()
    info = {'token': token, 'names': names, 'images': images, 'neo4j_bolt_port': bolt, 'neo4j_http_port': http,
            'kafka_host_port': kafka_port, 'neo4j_uri_host': f'bolt://127.0.0.1:{bolt}',
            'neo4j_uri_network': f'bolt://{names["neo"]}:7687', 'kafka_bootstrap_host': f'127.0.0.1:{kafka_port}',
            'kafka_bootstrap_network': f'{names["kafka"]}:9092', 'network': names['net'], 'neo4j_settings': settings,
            'limits': {'neo4j': {'mem': '2g', 'cpus': 2, 'heap': '512M', 'pagecache': '256M'}, 'kafka': {'mem': '1536m', 'heap': '512m-768m'}}}
    env_path = workdir / 'credentials.env'
    fd = os.open(env_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as f:
        f.write(f'NEO4J_USERNAME=neo4j\nNEO4J_PASSWORD={password}\nNEO4J_URI={info["neo4j_uri_host"]}\n'
                f'LOADER_NEO4J_URI={info["neo4j_uri_network"]}\nKAFKA_BOOTSTRAP_SERVERS={info["kafka_bootstrap_host"]}\n'
                f'LOADER_KAFKA_BOOTSTRAP_SERVERS={info["kafka_bootstrap_network"]}\nE10_STACK_TOKEN={token}\n')
    (workdir / 'stack.json').write_text(json.dumps(info, indent=1, sort_keys=True) + '\n')
    registry.write('ready', bolt=bolt, kafka=kafka_port)
    print(json.dumps(info, indent=1, sort_keys=True))


def remove(workdir):
    workdir = Path(workdir)
    rows = [json.loads(line) for line in (workdir / 'registry.jsonl').read_text().splitlines() if line.strip()]
    owners = {r['owner'] for r in rows}
    assert len(owners) == 1, owners
    token = owners.pop()
    client = docker.DockerClient(base_url='unix:///var/run/docker.sock', timeout=120)
    created = {r['resource']: [] for r in rows if r['kind'] == 'created'}
    for r in rows:
        if r['kind'] == 'created':
            created[r['resource']].append(r['id'])
    done = []
    for cid in created.get('container', []):
        try:
            c = client.containers.get(cid)
        except docker.errors.NotFound:
            continue
        assert c.labels.get(OWNER_LABEL) == token, f'{cid} is not owned by this stack'
        c.remove(force=True, v=True)
        done.append(('container', c.name))
    for nid in created.get('network', []):
        try:
            n = client.networks.get(nid)
        except docker.errors.NotFound:
            continue
        assert n.attrs.get('Labels', {}).get(OWNER_LABEL) == token
        n.remove()
        done.append(('network', n.name))
    for name in created.get('volume', []):
        try:
            v = client.volumes.get(name)
        except docker.errors.NotFound:
            continue
        assert v.attrs.get('Labels', {}).get(OWNER_LABEL) == token
        v.remove(force=True)
        done.append(('volume', name))
    print(json.dumps(done))


if __name__ == '__main__':
    {'create': create, 'remove': remove}[sys.argv[1]](sys.argv[2])
