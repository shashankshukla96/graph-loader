"""Private, bounded checks of the supported local Docker topology."""
from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path
import secrets

from docker.types import Mount

logger = logging.getLogger(__name__)


class DockerPreflightError(RuntimeError):
    pass


def check(service, network, *, plan_path=None):
    """Verify the daemon sees the same inputs and container service endpoints."""
    if service.client.api.base_url not in {'http+docker://localhost', 'http+docker://localnpipe'}:
        raise DockerPreflightError('unsupported Docker endpoint: use a local socket/Desktop daemon; remote host paths and coordinator ports are not translated')
    try:
        attrs = service.client.networks.get(network).attrs
        if attrs.get('Driver') != 'bridge' or attrs.get('Name') == 'bridge':
            raise DockerPreflightError('use an existing user-defined Docker bridge network with container DNS')
    except DockerPreflightError:
        raise
    except Exception as exc:
        raise DockerPreflightError('Docker network unavailable; create/select an existing user-defined bridge network') from exc
    runtime = service.runtime
    marker = '.graph-loader-preflight-' + secrets.token_hex(16)
    token = secrets.token_hex(32)
    directories = {name: Path(getattr(runtime, name)) for name in ('rejections', 'plans', 'spill', 'timings')}
    container_id = None
    created = []
    try:
        external_plan = Path(plan_path).resolve() if plan_path is not None else None
        if external_plan is not None:
            directories['plans'] = external_plan.parent
            if external_plan.exists() and not external_plan.is_file():
                raise DockerPreflightError('external edge plan must be a regular file when present')
        for name, directory in directories.items():
            path = directory / (marker + '-' + name)
            with path.open('x') as output:
                created.append(path)
                output.write(token)
        mounts = [Mount('/app/runtime_schema.yaml', str(runtime.schema), type='bind', read_only=True)]
        mounts += [Mount('/app/preflight/' + name, str(directory), type='bind',
                         read_only=name in {'plans', 'spill'}) for name, directory in directories.items()]
        env = {'NEO4J_URI': os.environ.get('LOADER_NEO4J_URI', 'bolt://neo4j:7687'),
               'NEO4J_USERNAME': os.environ.get('NEO4J_USERNAME', os.environ.get('NEO4J_USER', 'neo4j')),
               'NEO4J_PASSWORD': os.environ.get('NEO4J_PASSWORD', 'changeme'),
               'NEO4J_DATABASE': os.environ.get('NEO4J_DATABASE', ''),
               'KAFKA_BOOTSTRAP_SERVERS': os.environ.get('LOADER_KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')}
        if external_plan is not None and external_plan.is_file():
            mounts.append(Mount('/app/preflight/edge-plan.json', str(external_plan), type='bind', read_only=True))
            with external_plan.open('rb') as source:
                env['GRAPH_LOADER_PREFLIGHT_PLAN_SHA256'] = hashlib.file_digest(source, 'sha256').hexdigest()
        api = service.client.api
        host_config = api.create_host_config(network_mode=network, mounts=mounts, read_only=True,
            tmpfs={'/tmp': 'rw,nosuid,size=16m'}, init=True)
        response = api.create_container(
            image=service._image('node'), entrypoint=['python', '-m', 'neo4j_graph_loader._docker_preflight'],
            command=[hashlib.sha256(runtime.schema.read_bytes()).hexdigest(), marker, token],
            environment=env, host_config=host_config, detach=True, use_config_proxy=False,
            labels={'app': 'graph-loader', 'component': 'preflight', 'attempt': runtime.root.name})
        # Keep the daemon-returned ID before any further API request. High-level
        # create/run inspect internally and can hide this ID on inspect/start failure.
        container_id = response['Id']
        api.start(container_id)
        result = api.wait(container_id, timeout=40)
        # The probe prints only a closed set of phase identifiers. Do not surface
        # daemon output, driver messages, URIs, credentials or arbitrary logs.
        try:
            payload = json.loads(api.logs(container_id, stdout=True, stderr=False))
        except (ValueError, TypeError):
            payload = {}
        phases = {'mount', 'neo4j-auth', 'neo4j-database', 'neo4j-connect', 'kafka-bootstrap', 'kafka-metadata'}
        if result.get('StatusCode') != 0 or payload.get('ok') is not True:
            phase = payload.get('phase')
            phase = phase if phase in phases else 'probe'
            raise DockerPreflightError(f'container preflight failed ({phase}); check daemon-visible mounts, container service addresses, database and credentials')
        for name in ('rejections', 'timings'):
            if (directories[name] / (marker + '-' + name)).read_text() != token + '-container':
                raise DockerPreflightError('container preflight failed (mount): daemon and client do not share writable paths')
        logger.info('stage=docker-preflight mounts=verified container_services=reachable metadata=reachable')
    except DockerPreflightError:
        raise
    except Exception as exc:
        raise DockerPreflightError('container preflight unavailable; check Docker network, shared absolute bind paths and service reachability') from exc
    finally:
        cleanup_failed = False
        if container_id is not None:
            try:
                service.client.api.remove_container(container_id, force=True)
            except Exception:
                cleanup_failed = True
                logger.warning('stage=docker-preflight owned probe removal failed')
        for path in created:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                cleanup_failed = True
                logger.warning('stage=docker-preflight owned probe marker removal failed')
        if cleanup_failed:
            raise DockerPreflightError('owned Docker preflight cleanup failed; inspect the retained attempt before retrying')


def probe(schema_hash, marker, token):
    """Run inside a selected image; report only safe phase names."""
    phase = 'mount'
    driver = None
    try:
        assert hashlib.sha256(Path('/app/runtime_schema.yaml').read_bytes()).hexdigest() == schema_hash
        for name in ('rejections', 'plans', 'spill', 'timings'):
            path = Path('/app/preflight') / name / (marker + '-' + name)
            assert path.read_text() == token
            if name in {'rejections', 'timings'}:
                path.write_text(token + '-container')
        if os.environ.get('GRAPH_LOADER_PREFLIGHT_PLAN_SHA256'):
            with Path('/app/preflight/edge-plan.json').open('rb') as source:
                assert hashlib.file_digest(source, 'sha256').hexdigest() == os.environ['GRAPH_LOADER_PREFLIGHT_PLAN_SHA256']
        from neo4j import GraphDatabase
        from neo4j.exceptions import AuthError, ClientError
        from neo4j_graph_loader.utils.neo4j_session import open_session
        phase = 'neo4j-connect'
        driver = GraphDatabase.driver(os.environ['NEO4J_URI'],
            auth=(os.environ['NEO4J_USERNAME'], os.environ['NEO4J_PASSWORD']),
            connection_timeout=5, connection_acquisition_timeout=7, max_transaction_retry_time=0)
        try:
            driver.verify_connectivity()
            phase = 'neo4j-database'
            with open_session(driver) as session:
                session.run('RETURN 1').consume()
        except AuthError:
            phase = 'neo4j-auth'
            raise
        except ClientError:
            phase = 'neo4j-database'
            raise
        finally:
            driver.close()
            driver = None
        phase = 'kafka-bootstrap'
        import socket
        from urllib.parse import urlsplit
        reachable = False
        for address in os.environ['KAFKA_BOOTSTRAP_SERVERS'].split(','):
            try:
                endpoint = urlsplit('//' + address.strip())
                with socket.create_connection((endpoint.hostname, endpoint.port or 9092), timeout=2):
                    reachable = True
                break
            except (OSError, ValueError, TypeError):
                pass
        assert reachable
        phase = 'kafka-metadata'
        from confluent_kafka.admin import AdminClient
        metadata = AdminClient({'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],
                                'socket.timeout.ms': 7000, 'log_level': 0}).list_topics(timeout=8)
        assert metadata.brokers
        for broker in metadata.brokers.values():
            with socket.create_connection((broker.host, broker.port), timeout=2):
                pass
        print(json.dumps({'ok': True}))
        return 0
    except Exception:
        print(json.dumps({'ok': False, 'phase': phase}))
        return 1
    finally:
        if driver is not None:
            driver.close()


if __name__ == '__main__':
    import sys
    raise SystemExit(probe(*sys.argv[1:]))
