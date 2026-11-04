"""Owned throwaway Neo4j + Kafka on the shared native Docker Engine.

Substrate: the engine also runs the user's services and any member of the
``docker`` group can reach its socket. Isolation here is naming, labels, a
private bridge network and removal by recorded ID only; there is no fenced
daemon, broker or durable settlement of in-flight Docker calls.
"""
from __future__ import annotations

import io
import json
import os
from pathlib import Path
import re
import socket
import tarfile
import threading
import time
import uuid

import docker
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from neo4j import GraphDatabase

from .ownership import OWNER_LABEL, labelled
from .query_log_config import make_config

ROLE_LABEL = 'io.graph-loader.e09-role'
NEO4J_IMAGE = 'neo4j:5-enterprise'
KAFKA_IMAGE = 'confluentinc/cp-kafka:7.7.0'
FOREIGN_PORTS = frozenset({5432, 7474, 7687, 8080, 9092, 9101})
TOKEN_RE = re.compile(r'[0-9a-f]{32}\Z')


class Registry:
    """Append-only, fsynced JSON lines naming every resource this run may remove."""

    def __init__(self, path, owner):
        if TOKEN_RE.match(owner) is None:
            raise ValueError('owner token must be 32 lowercase hex characters')
        self.path = Path(path)
        self.owner = owner
        self.broken = False
        self._lock = threading.Lock()
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(fd)

    def write(self, kind, **values):
        row = {'kind': kind, 'owner': self.owner, 'at': time.time(), **values}
        raw = (json.dumps(row, sort_keys=True) + '\n').encode()
        with self._lock:
            fd = os.open(self.path, os.O_WRONLY | os.O_APPEND)
            try:
                if self.broken:
                    raise RuntimeError('the registry is unusable after an earlier failed write')
                size = os.fstat(fd).st_size
                try:
                    if os.write(fd, raw) != len(raw):
                        raise RuntimeError('short registry write')
                    os.fsync(fd)
                except BaseException:
                    # Never leave a partial line in front of later rows: cut back to the last whole row, or refuse all writes.
                    try:
                        os.ftruncate(fd, size)
                        os.fsync(fd)
                    except OSError:
                        self.broken = True
                    raise
            finally:
                os.close(fd)


def free_port():
    for _ in range(50):
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        if port not in FOREIGN_PORTS:
            return port
    raise RuntimeError('no free loopback port')


def owned_name(owner, role, prefix='e09'):
    return f'{prefix}-{owner}-{role}'


class LiveStack:
    """Network, Neo4j 5.26 Enterprise (APOC, structured query log) and Kafka 7.7.0."""

    def __init__(self, client, registry, *, deadline, prefix='e09', limits=None, expected_images=None, pin_neo_port=False):
        self.client = client
        self.prefix = prefix                      # E14 runs name their resources 'e14-<owner>-...'; E09 keeps 'e09'
        self.topic_prefix = f'{prefix}-{registry.owner}-'
        self.expected_images = dict(expected_images or {})   # {'neo4j': 'sha256:...', 'kafka': ...}: refuse any other image
        self.limits = dict(limits or {})          # optional memory bounds, see start(); empty = Docker's defaults
        self.registry = registry
        self.owner = registry.owner
        self.deadline = deadline
        self.labels = {OWNER_LABEL: self.owner}
        self.password = uuid.uuid4().hex
        self.marker = prefix + self.owner
        self.database = prefix + self.owner[:16]
        self.network = self.neo = self.kafka = self.driver = None
        self.volumes = {}
        self.images = {}
        self.kafka_port = None
        self.neo_port = None
        # Docker publishes Neo4j on a random loopback port that changes when the container is stopped and started again. Process-mode
        # children are handed NEO4J_URI once, so the outage cells publish it on a port that is chosen up front and survives restarts.
        self.neo_pin = free_port() if pin_neo_port else None

    def _mem_limit(self, key):
        return {'mem_limit': self.limits[key]} if self.limits.get(key) else {}

    def _neo4j_memory_env(self):
        env = {}
        if self.limits.get('neo4j_heap'):
            env['NEO4J_server_memory_heap_initial__size'] = env['NEO4J_server_memory_heap_max__size'] = self.limits['neo4j_heap']
        if self.limits.get('neo4j_pagecache'):
            env['NEO4J_server_memory_pagecache_size'] = self.limits['neo4j_pagecache']
        return env

    def _kafka_memory_env(self):
        return {'KAFKA_HEAP_OPTS': f"-Xms{self.limits['kafka_heap']} -Xmx{self.limits['kafka_heap']}"} if self.limits.get('kafka_heap') else {}

    def _check(self):
        if time.monotonic() > self.deadline:
            raise TimeoutError('stack deadline expired')

    @property
    def neo_name(self):
        return owned_name(self.owner, 'neo', self.prefix)

    @property
    def kafka_name(self):
        return owned_name(self.owner, 'kafka', self.prefix)

    @property
    def neo_uri(self):
        return f'bolt://127.0.0.1:{self.neo_port}'

    @property
    def container_neo_uri(self):
        return f'bolt://{self.neo_name}:7687'

    @property
    def bootstrap(self):
        return f'127.0.0.1:{self.kafka_port}'

    @property
    def container_bootstrap(self):
        return f'{self.kafka_name}:9092'

    def redact(self, text):
        return str(text).replace(self.password, '[redacted]')

    def _create(self, kind, name, make):
        self.registry.write('intent', resource=kind, name=name)
        obj = make()
        self.registry.write('created', resource=kind, name=name, id=obj.id)
        return obj

    def start(self):
        self.images = {'neo4j': self.client.images.get(NEO4J_IMAGE).id,
                       'kafka': self.client.images.get(KAFKA_IMAGE).id}
        for role, wanted in self.expected_images.items():
            if self.images[role] != wanted:
                raise RuntimeError(f'the {role} image on this host is {self.images[role]}, not the pinned {wanted}')
        self.volumes = {}
        for role, suffix, mount in (('neo', 'data', '/data'), ('neo', 'logs', '/logs'), ('kafka', 'data', '/var/lib/kafka/data')):
            volume_name = owned_name(self.owner, f'{role}-{suffix}', self.prefix)
            self.volumes[(role, mount)] = self._create('volume', volume_name, lambda n=volume_name: self.client.volumes.create(
                name=n, labels=self.labels))
        self.network = self._create('network', owned_name(self.owner, 'net', self.prefix), lambda: self.client.networks.create(
            owned_name(self.owner, 'net', self.prefix), driver='bridge', labels=self.labels))
        config = make_config((Path(__file__).parents[2] / 'config/neo4j/server-logs.xml').read_bytes(), self.marker)
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode='w') as tar:
            item = tarfile.TarInfo('server-logs.xml')
            item.size, item.mode = len(config), 0o444
            tar.addfile(item, io.BytesIO(config))
        self.neo = self._create('container', self.neo_name, lambda: self.client.containers.create(
            self.images['neo4j'], name=self.neo_name, labels={**self.labels, ROLE_LABEL: 'neo'}, network=self.network.name,
            environment={'NEO4J_AUTH': 'neo4j/' + self.password, 'NEO4J_ACCEPT_LICENSE_AGREEMENT': 'yes',
                         'NEO4J_PLUGINS': '["apoc"]', 'NEO4J_dbms_security_procedures_unrestricted': 'apoc.*',
                         'NEO4J_dbms_security_procedures_allowlist': 'apoc.*',
                         'NEO4J_db_logs_query_enabled': 'VERBOSE', 'NEO4J_db_logs_query_threshold': '0ms',
                         'NEO4J_db_logs_query_parameter__logging__enabled': 'false', **self._neo4j_memory_env()},
            ports={'7687/tcp': ('127.0.0.1', self.neo_pin)}, **self._mem_limit('neo4j_mem'),
            volumes={volume.name: {'bind': mount, 'mode': 'rw'} for (role, mount), volume in self.volumes.items() if role == 'neo'}))
        if not self.neo.put_archive('/var/lib/neo4j/conf', archive.getvalue()):
            raise RuntimeError('scratch query-log configuration transfer failed')
        self.kafka_port = free_port()
        name = self.kafka_name
        self.kafka = self._create('container', name, lambda: self.client.containers.create(
            self.images['kafka'], name=name, labels={**self.labels, ROLE_LABEL: 'kafka'}, network=self.network.name,
            environment={'KAFKA_NODE_ID': '1', 'KAFKA_PROCESS_ROLES': 'broker,controller',
                         'KAFKA_LISTENERS': 'PLAINTEXT://0.0.0.0:9093,BROKER://0.0.0.0:9092,CONTROLLER://0.0.0.0:9094',
                         'KAFKA_ADVERTISED_LISTENERS': f'PLAINTEXT://127.0.0.1:{self.kafka_port},BROKER://{name}:9092',
                         'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'CONTROLLER:PLAINTEXT,BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
                         'KAFKA_INTER_BROKER_LISTENER_NAME': 'BROKER', 'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER',
                         'KAFKA_CONTROLLER_QUORUM_VOTERS': '1@localhost:9094', 'CLUSTER_ID': 'MkU3OEVBNTcwNTJENDM2Qk',
                         'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': '1', 'KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS': '1',
                         'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': '1', 'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': '1',
                         'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS': '0', **self._kafka_memory_env()},
            ports={'9093/tcp': ('127.0.0.1', self.kafka_port)}, **self._mem_limit('kafka_mem'),
            volumes={volume.name: {'bind': mount, 'mode': 'rw'} for (role, mount), volume in self.volumes.items() if role == 'kafka'}))
        self.neo.start()
        self.kafka.start()
        self.neo.reload()
        self.kafka.reload()
        for container in (self.neo, self.kafka):
            for binding in container.attrs['NetworkSettings']['Ports'].values():
                if binding is not None and any(b['HostIp'] != '127.0.0.1' or int(b['HostPort']) in FOREIGN_PORTS
                                               for b in binding):
                    raise RuntimeError('unsafe published port')
        self.neo_port = int(self.neo.attrs['NetworkSettings']['Ports']['7687/tcp'][0]['HostPort'])
        self.driver = GraphDatabase.driver(self.neo_uri, auth=('neo4j', self.password), connection_timeout=2)
        self.wait_neo4j()
        with self.driver.session(database='system') as session:
            session.run('CREATE DATABASE ' + self.database + ' WAIT').consume()
        self.wait_kafka()

    def wait_neo4j(self):
        while True:
            self._check()
            try:
                self.driver.verify_connectivity()
                with self.driver.session(database='system') as session:
                    session.run('SHOW DATABASES YIELD name RETURN name').consume()
                return
            except Exception:
                self.neo.reload()
                if self.neo.status == 'exited':
                    raise RuntimeError('Neo4j exited before readiness')
                time.sleep(1)

    def wait_kafka(self):
        admin = AdminClient({'bootstrap.servers': self.bootstrap, 'socket.timeout.ms': 1000})
        while True:
            self._check()
            try:
                if admin.list_topics(timeout=2).brokers:
                    return
            except Exception:
                pass
            self.kafka.reload()
            if self.kafka.status == 'exited':
                raise RuntimeError('Kafka exited before readiness')
            time.sleep(1)

    def admin(self):
        return AdminClient({'bootstrap.servers': self.bootstrap, 'socket.timeout.ms': 2000})

    def create_topics(self, names, partitions=1):
        for name in names:
            if not name.startswith(self.topic_prefix):
                raise ValueError('scratch topic must carry the owner prefix')
        admin = self.admin()
        for name, future in admin.create_topics([NewTopic(n, num_partitions=partitions, replication_factor=1) for n in names]).items():
            future.result(timeout=20)
            self.registry.write('created', resource='topic', name=name)

    def produce(self, topic, rows, *, partition=None):
        """Publish ``(key, json-able value)`` rows and fail unless every delivery is acknowledged."""
        failures = []
        producer = Producer({'bootstrap.servers': self.bootstrap, 'enable.idempotence': True})
        for key, value in rows:
            kwargs = {} if partition is None else {'partition': partition}
            producer.produce(topic, key=str(key).encode(), value=json.dumps(value).encode(),
                             on_delivery=lambda error, message: failures.append(error) if error else None, **kwargs)
        if producer.flush(20) != 0 or failures:
            raise RuntimeError('scratch publish was not fully acknowledged')

    def end_offsets(self, topic, partitions=1):
        consumer = Consumer({'bootstrap.servers': self.bootstrap, 'group.id': f'{self.topic_prefix}probe-{topic[-8:]}',
                             'enable.auto.commit': False})
        try:
            return {p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in range(partitions)}
        finally:
            consumer.close()

    def facts(self):
        """Allowlisted versions and settings; never credentials."""
        with self.driver.session(database='system') as session:
            component = session.run('CALL dbms.components() YIELD name, versions, edition RETURN name, versions, edition').single()
            procedures = sorted(r['name'] for r in session.run(
                "SHOW PROCEDURES YIELD name WHERE name IN ['apoc.util.sleep','apoc.lock.nodes'] RETURN name"))
            settings = {r['name']: str(r['value']) for r in session.run(
                "SHOW SETTINGS YIELD name,value WHERE name IN ['db.logs.query.enabled','db.logs.query.threshold',"
                "'db.logs.query.parameter_logging_enabled'] RETURN name,value")}
        self.kafka.reload()
        return {'neo4j': {'version': component['versions'][0], 'edition': component['edition'],
                          'apoc': procedures, 'query_log': settings},
                'kafka_release': (self.client.images.get(self.images['kafka']).attrs.get('Config', {}).get('Labels') or {}).get('release'),
                'images': dict(self.images),
                'containers': {'neo4j': self.neo.id, 'kafka': self.kafka.id}, 'network': self.network.id}

    def container_reachable(self, source, target, port):
        result = source.exec_run(['/bin/bash', '-c', f'exec 3<>/dev/tcp/{target}/{port}'])
        return result.exit_code == 0

    def stop_neo4j(self, timeout=30):
        self.registry.write('control', action='stop', id=self.neo.id)
        self.driver.close()
        self.neo.stop(timeout=timeout)
        self.neo.reload()
        return self.neo.status

    def start_neo4j(self):
        self.registry.write('control', action='start', id=self.neo.id)
        self.neo.start()
        self.neo.reload()
        self.neo_port = int(self.neo.attrs['NetworkSettings']['Ports']['7687/tcp'][0]['HostPort'])
        self.driver = GraphDatabase.driver(self.neo_uri, auth=('neo4j', self.password), connection_timeout=2)
        self.wait_neo4j()

    def stop_kafka(self, timeout=30):
        self.registry.write('control', action='stop', id=self.kafka.id)
        self.kafka.stop(timeout=timeout)
        self.kafka.reload()
        return self.kafka.status

    def start_kafka(self):
        self.registry.write('control', action='start', id=self.kafka.id)
        self.kafka.start()
        self.kafka.reload()
        self.wait_kafka()

    def run_owned(self, role, image, command, *, network=True):
        """Create and start one owned helper container; the caller removes it with ``remove_owned``."""
        name = owned_name(self.owner, role, self.prefix)
        self.registry.write('intent', resource='container', name=name)
        container = self.client.containers.create(image, name=name, command=command, labels={**self.labels, ROLE_LABEL: role},
                                                  network=self.network.name if network else None,
                                                  network_mode=None if network else 'none')
        self.registry.write('created', resource='container', name=name, id=container.id)
        container.start()
        return container

    def _verified(self, obj, kind):
        """Fresh owner-label check immediately before a destructive action on an object."""
        obj.reload()
        labels = obj.attrs['Config'].get('Labels') if kind == 'container' else obj.attrs.get('Labels')
        return labelled(labels, self.owner)

    def kill_owned(self, container, *, timeout=30):
        """SIGKILL one owned helper container and read its terminal state back within a deadline."""
        if not self._verified(container, 'container'):
            raise RuntimeError('refusing to terminate a container without the run owner label')
        self.registry.write('control', action='kill', id=container.id, signal='SIGKILL', deadline=timeout)
        container.kill(signal='SIGKILL')
        exit_code = container.wait(timeout=timeout)['StatusCode']
        container.reload()
        return {'exit_code': exit_code, 'status': container.status, 'oom_killed': container.attrs['State']['OOMKilled']}

    def remove_owned(self, container):
        if not self._verified(container, 'container'):
            raise RuntimeError('refusing to remove a container without the run owner label')
        container.remove(force=True, v=True)
        self.registry.write('removed', resource='container', id=container.id)

    def close(self):
        """Remove only recorded IDs and report what was removed or failed."""
        removed, failed = [], []
        if self.driver is not None:
            try:
                self.driver.close()
            except Exception:
                pass
        for obj in (self.kafka, self.neo):
            if obj is None:
                continue
            try:
                if not self._verified(obj, 'container'):
                    failed.append((obj.id, 'container owner label differs'))
                    continue
                obj.remove(force=True, v=True)
                self.registry.write('removed', resource='container', id=obj.id)
                removed.append(obj.id)
            except docker.errors.NotFound:
                pass
            except Exception as error:
                failed.append((obj.id, type(error).__name__))
        for volume in self.volumes.values():
            try:
                if not self._verified(volume, 'volume'):
                    failed.append((volume.name, 'volume owner label differs'))
                    continue
                volume.remove(force=True)
                self.registry.write('removed', resource='volume', id=volume.name)
                removed.append(volume.name)
            except docker.errors.NotFound:
                pass
            except Exception as error:
                failed.append((volume.name, type(error).__name__))
        if self.network is not None:
            try:
                if not self._verified(self.network, 'network'):
                    failed.append((self.network.id, 'network owner label differs'))
                    return {'removed': removed, 'failed': failed}
                self.network.remove()
                self.registry.write('removed', resource='network', id=self.network.id)
                removed.append(self.network.id)
            except docker.errors.NotFound:
                pass
            except Exception as error:
                failed.append((self.network.id, type(error).__name__))
        return {'removed': removed, 'failed': failed}
