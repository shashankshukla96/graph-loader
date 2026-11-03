"""The soak's owned Neo4j + Kafka: ``tests/installed_service/live_stack.py`` under an ``e13`` namespace with memory limits.

``LiveStack`` hard-codes the ``e09-`` namespace in several places, and E09's call sites must not change, so the soak subclasses it:
owned names are ``e13-<token>-<role>``, Neo4j and Kafka get heap, page-cache and container memory limits, and the query log is the
rolling variant (``querylog.make_rolling_config``). The ownership label key is the shared one, so ``ownership``/``AppObserver``
work unchanged. The Neo4j database name keeps the ``e09<16 hex>`` form on purpose: the strict query-log parser recognises the
``CREATE DATABASE`` of exactly that name as the one admission command whose transaction id may change.
"""
from __future__ import annotations

import io
from pathlib import Path
import tarfile
import time
import uuid

from neo4j import GraphDatabase

from tests.installed_service.live_stack import FOREIGN_PORTS, KAFKA_IMAGE, NEO4J_IMAGE, ROLE_LABEL, LiveStack, free_port
from tests.installed_service.ownership import OWNER_LABEL

from .querylog import make_rolling_config
from .spec import NAMESPACE

MiB = 1024 * 1024
GiB = 1024 * MiB


def e13_name(owner: str, role: str) -> str:
    """Owned resource name ``<namespace>-<owner>-<role>`` (the namespace is e13 unless SOAK_NAMESPACE says otherwise)."""
    return f'{NAMESPACE}-{owner}-{role}'


class SoakStack(LiveStack):
    def __init__(self, client, registry, *, deadline, neo_memory=3 * GiB, kafka_memory=int(1.5 * GiB), neo_heap='1g', neo_cache='1g',
                 kafka_heap='1g', log_size='64 MB', log_keep=50, pin_neo_port=False, rebalance_delay_ms=0):
        # LiveStack derives the owned names, the topic prefix, the query-log marker and the database name from ``prefix``.
        super().__init__(client, registry, deadline=deadline, prefix=NAMESPACE, pin_neo_port=pin_neo_port)
        if NAMESPACE not in ('e09', 'e14'):
            # The strict query-log parser only tolerates the CREATE DATABASE of the e09/e14 form (see the module docstring).
            self.marker, self.database = 'e09' + self.owner, 'e09' + self.owner[:16]
        self.neo_memory, self.kafka_memory = neo_memory, kafka_memory
        # E13 ran the owned broker with 0 (the installed_service default); E14's host shapes use Kafka's own default, 3000 (see S10 evidence)
        self.rebalance_delay_ms = int(rebalance_delay_ms)
        self.neo_heap, self.neo_cache, self.kafka_heap = neo_heap, neo_cache, kafka_heap
        self.log_size, self.log_keep = log_size, log_keep
        self.labels = {OWNER_LABEL: self.owner, 'io.graph-loader.e13': '1'}
        self.metrics_port = free_port()      # Neo4j's own Prometheus endpoint, published on loopback on a port that survives restarts

    def start(self):
        self.images = {'neo4j': self.client.images.get(NEO4J_IMAGE).id, 'kafka': self.client.images.get(KAFKA_IMAGE).id}
        self.volumes = {}
        for role, suffix, mount in (('neo', 'data', '/data'), ('neo', 'logs', '/logs'), ('kafka', 'data', '/var/lib/kafka/data')):
            volume_name = e13_name(self.owner, f'{role}-{suffix}')
            self.volumes[(role, mount)] = self._create('volume', volume_name, lambda n=volume_name: self.client.volumes.create(
                name=n, labels=self.labels))
        self.network = self._create('network', e13_name(self.owner, 'net'), lambda: self.client.networks.create(
            e13_name(self.owner, 'net'), driver='bridge', labels=self.labels))
        template = (Path(__file__).parents[1] / 'config/neo4j/server-logs.xml').read_bytes()
        config = make_rolling_config(template, self.marker, size=self.log_size, keep=self.log_keep)
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode='w') as tar:
            item = tarfile.TarInfo('server-logs.xml')
            item.size, item.mode = len(config), 0o444
            tar.addfile(item, io.BytesIO(config))
        self.neo = self._create('container', self.neo_name, lambda: self.client.containers.create(
            self.images['neo4j'], name=self.neo_name, labels={**self.labels, ROLE_LABEL: 'neo'}, network=self.network.name,
            mem_limit=self.neo_memory, memswap_limit=self.neo_memory,
            environment={'NEO4J_AUTH': 'neo4j/' + self.password, 'NEO4J_ACCEPT_LICENSE_AGREEMENT': 'yes',
                         'NEO4J_PLUGINS': '["apoc"]', 'NEO4J_dbms_security_procedures_unrestricted': 'apoc.*',
                         'NEO4J_dbms_security_procedures_allowlist': 'apoc.*',
                         'NEO4J_server_memory_heap_initial__size': self.neo_heap, 'NEO4J_server_memory_heap_max__size': self.neo_heap,
                         'NEO4J_server_memory_pagecache_size': self.neo_cache,
                         'NEO4J_db_logs_query_enabled': 'VERBOSE', 'NEO4J_db_logs_query_threshold': '0ms',
                         'NEO4J_db_logs_query_parameter__logging__enabled': 'false',
                         'NEO4J_server_metrics_prometheus_enabled': 'true', 'NEO4J_server_metrics_prometheus_endpoint': '0.0.0.0:2004',
                         'NEO4J_server_metrics_filter': '*'},
            ports={'7687/tcp': ('127.0.0.1', self.neo_pin), '2004/tcp': ('127.0.0.1', self.metrics_port)},
            volumes={volume.name: {'bind': mount, 'mode': 'rw'} for (role, mount), volume in self.volumes.items() if role == 'neo'}))
        if not self.neo.put_archive('/var/lib/neo4j/conf', archive.getvalue()):
            raise RuntimeError('query-log configuration transfer failed')
        self.kafka_port = free_port()
        name = self.kafka_name
        self.kafka = self._create('container', name, lambda: self.client.containers.create(
            self.images['kafka'], name=name, labels={**self.labels, ROLE_LABEL: 'kafka'}, network=self.network.name,
            mem_limit=self.kafka_memory, memswap_limit=self.kafka_memory,
            environment={'KAFKA_NODE_ID': '1', 'KAFKA_PROCESS_ROLES': 'broker,controller',
                         'KAFKA_LISTENERS': 'PLAINTEXT://0.0.0.0:9093,BROKER://0.0.0.0:9092,CONTROLLER://0.0.0.0:9094',
                         'KAFKA_ADVERTISED_LISTENERS': f'PLAINTEXT://127.0.0.1:{self.kafka_port},BROKER://{name}:9092',
                         'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'CONTROLLER:PLAINTEXT,BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
                         'KAFKA_INTER_BROKER_LISTENER_NAME': 'BROKER', 'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER',
                         'KAFKA_CONTROLLER_QUORUM_VOTERS': '1@localhost:9094', 'CLUSTER_ID': 'MkU3OEVBNTcwNTJENDM2Qk',
                         'KAFKA_HEAP_OPTS': f'-Xms{self.kafka_heap} -Xmx{self.kafka_heap}',
                         'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': '1', 'KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS': '3',
                         'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': '1', 'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': '1',
                         'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS': str(self.rebalance_delay_ms)},
            ports={'9093/tcp': ('127.0.0.1', self.kafka_port)},
            volumes={volume.name: {'bind': mount, 'mode': 'rw'} for (role, mount), volume in self.volumes.items() if role == 'kafka'}))
        self.neo.start()
        self.kafka.start()
        self.neo.reload()
        self.kafka.reload()
        for container in (self.neo, self.kafka):
            for binding in container.attrs['NetworkSettings']['Ports'].values():
                if binding is not None and any(b['HostIp'] != '127.0.0.1' or int(b['HostPort']) in FOREIGN_PORTS for b in binding):
                    raise RuntimeError('unsafe published port')
        self.neo_port = int(self.neo.attrs['NetworkSettings']['Ports']['7687/tcp'][0]['HostPort'])
        self.driver = GraphDatabase.driver(self.neo_uri, auth=('neo4j', self.password), connection_timeout=2)
        self.wait_neo4j()
        with self.driver.session(database='system') as session:
            session.run('CREATE DATABASE ' + self.database + ' WAIT').consume()
        self.wait_kafka()

    def create_topics(self, topics, *, retention_ms=None):
        """``topics`` maps a topic name to its partition count. Only ``<namespace>-<token>-`` names are created."""
        from confluent_kafka.admin import NewTopic
        for name in topics:
            if not name.startswith(self.topic_prefix):
                raise ValueError('soak topic must carry the owner prefix')
        admin = self.admin()
        for name, future in admin.create_topics([NewTopic(n, num_partitions=p, replication_factor=1) for n, p in topics.items()]).items():
            future.result(timeout=30)
            self.registry.write('created', resource='topic', name=name)

    def exec_log(self, command):
        """``(exit code, output)`` of a command in the Neo4j container (the query-log reader's transport)."""
        result = self.neo.exec_run(command, demux=False)
        return result.exit_code, result.output or b''

    def memory_limits(self):
        return {'neo4j': self.neo_memory, 'kafka': self.kafka_memory}

    # ---- what the memory story needs (E14.F3.S10): heap, GC, page cache, store size --------------------------------------------
    def _jmx(self, query):
        with self.driver.session(database='system') as session:
            return [dict(r) for r in session.run('CALL dbms.queryJmx($q) YIELD name, attributes RETURN name, attributes', q=query)]

    def internals(self):
        """One row of Neo4j's own numbers: JVM heap, GC counts/times, page-cache hits/faults/hit ratio, store size on disk. Any part that
        cannot be read is recorded as an error, never as zero."""
        row = {}
        try:
            for item in self._jmx('java.lang:type=Memory'):
                heap = item['attributes']['HeapMemoryUsage']['value']['properties']
                row['heap_used'], row['heap_committed'], row['heap_max'] = (int(heap.get('used', 0)), int(heap.get('committed', 0)),
                                                                           int(heap.get('max', 0)))
            gc = {}
            for item in self._jmx('java.lang:type=GarbageCollector,name=*'):
                gc[item['name'].rpartition('name=')[2]] = {'count': int(item['attributes']['CollectionCount']['value']),
                                                           'ms': int(item['attributes']['CollectionTime']['value'])}
            row['gc'] = gc
        except Exception as error:
            row['jvm_error'] = f'{type(error).__name__}: {error}'[:200]
        try:
            from .sampler import scrape
            wanted = {}
            for name, labels, value in scrape(self.metrics_port, timeout=5):
                if 'page_cache' in name or name.endswith('data_size') or 'store_size' in name:
                    wanted[name.replace('neo4j_', '', 1)[:90]] = value
            row['page_cache'] = {k: v for k, v in wanted.items() if 'page_cache' in k}
            row['data_size'] = {k: v for k, v in wanted.items() if 'page_cache' not in k}
            if not row['page_cache']:
                row['page_cache_error'] = 'no page-cache metric in the Prometheus endpoint'
        except Exception as error:
            row['page_cache_error'] = f'{type(error).__name__}: {error}'[:200]
        try:
            result = self.neo.exec_run(['du', '-sk', f'/data/databases/{self.database}', f'/data/transactions/{self.database}'])
            sizes = {}
            for line in (result.output or b'').decode().splitlines():
                kb, _, path = line.partition('\t')
                sizes[path.rsplit('/', 2)[-2]] = int(kb) * 1024
            row['store_bytes'] = sizes
        except Exception as error:
            row['store_error'] = f'{type(error).__name__}: {error}'[:200]
        return row

    def jmx_probe(self):
        """What Neo4j's JMX offers here (bean names, the memory and page-cache attributes), recorded once per run as evidence."""
        out = {}
        try:
            out['beans'] = sorted({item['name'] for item in self._jmx('org.neo4j:*')})
            out['memory'] = {k: repr(v)[:300] for item in self._jmx('java.lang:type=Memory') for k, v in item['attributes'].items()
                             if k in ('HeapMemoryUsage', 'NonHeapMemoryUsage')}
            from .sampler import scrape
            out['prometheus_names'] = sorted({name for name, _l, _v in scrape(self.metrics_port, timeout=5)
                                              if 'page_cache' in name or 'vm_heap' in name or 'store' in name or 'data_size' in name})[:60]
        except Exception as error:
            out['error'] = f'{type(error).__name__}: {error}'[:300]
        return out
