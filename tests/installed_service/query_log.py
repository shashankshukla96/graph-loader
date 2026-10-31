"""Fail-closed Neo4j query-log windows for owned scratch services."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
import re
from urllib.parse import urlsplit

from docker.transport.unixconn import UnixHTTPAdapter
from neo4j import BoltDriver


@dataclass(frozen=True)
class Snapshot:
    inode: int
    content: bytes


def _settings(driver, attrs):
    if not isinstance(driver, BoltDriver):
        raise ValueError('a direct Bolt driver is required for owned Neo4j settings')
    bindings = attrs.get('NetworkSettings', {}).get('Ports', {}).get('7687/tcp')
    if not isinstance(bindings, list) or len(bindings) != 1 or bindings[0].get('HostIp') != '127.0.0.1':
        raise ValueError('owned Neo4j must have one loopback-only Bolt endpoint')
    address = driver.get_server_info().address
    if address.host != '127.0.0.1' or address.port != int(bindings[0]['HostPort']):
        raise ValueError('query-log settings driver is not bound to the owned Neo4j endpoint')
    with driver.session(database='system') as session:
        rows = session.run("SHOW SETTINGS YIELD name,value WHERE name IN ['db.logs.query.enabled',"
                           "'db.logs.query.threshold','db.logs.query.parameter_logging_enabled'] RETURN name,value")
        settings = {row['name']: row['value'] for row in rows}
    if (settings.get('db.logs.query.parameter_logging_enabled') != 'false'
            or settings.get('db.logs.query.enabled') != 'VERBOSE'
            or str(settings.get('db.logs.query.threshold')).lower() not in {'0s', '0ms', '0'}):
        raise ValueError('live Neo4j query-log settings are unsafe or unavailable')
    return settings


def _bind_service(container, driver):
    """Compare the database server ID from host Bolt and inside the container."""
    with driver.session(database='system') as session:
        row = session.run('CALL dbms.info() YIELD id RETURN id').single()
    try:
        host_id = str(row['id']).lower()
        if re.fullmatch(r'[0-9a-f]{64}', host_id) is None:
            raise ValueError('noncanonical server identity')
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise ValueError('owned Neo4j server identity is invalid') from exc
    script = ('export NEO4J_USERNAME=neo4j; export NEO4J_PASSWORD="${NEO4J_AUTH#neo4j/}"; '
              'exec cypher-shell -a bolt://127.0.0.1:7687 -d system --format plain '
              '"CALL dbms.info() YIELD id RETURN id"')
    result = container.exec_run(['sh', '-c', script])
    if result.exit_code != 0 or not isinstance(result.output, bytes):
        raise ValueError('owned Neo4j container-side identity probe failed')
    try:
        ids = [line.strip().strip('"').lower() for line in result.output.decode('ascii').splitlines()
               if re.fullmatch(r'[0-9a-fA-F]{64}', line.strip().strip('"'))]
    except UnicodeDecodeError as exc:
        raise ValueError('owned Neo4j container-side identity output is invalid') from exc
    if ids != [host_id]:
        raise ValueError('host and container Neo4j server identities differ')


def snapshot(container, *, owner, container_id, created, image_id, daemon_id, docker_host, driver, maximum=8*1024*1024):
    """Capture a bounded prefix and verify the live query-log inode stayed put.

    The shell runs inside the explicitly owned Neo4j container.  A caller must
    wait for query completion and log flushing before taking the final mark.
    Use only a fresh owned service with synthetic queries: settings verified at
    capture time do not prove that older log entries contain no parameters.
    The caller must independently establish that the Unix socket is a trusted,
    non-forwarded local daemon and that the host endpoint is owned: host Bolt
    is contacted before the two server identities can be compared.
    """
    if type(maximum) is not int or not 1 <= maximum <= 64*1024*1024:
        raise ValueError('finite query-log size bound required')
    if (not isinstance(owner, str) or re.fullmatch(r'[0-9a-f]{32}', owner) is None
            or not isinstance(container_id, str) or re.fullmatch(r'[0-9a-f]{64}', container_id) is None
            or not isinstance(image_id, str) or re.fullmatch(r'sha256:[0-9a-f]{64}', image_id) is None
            or not isinstance(created, str) or not isinstance(daemon_id, str) or not daemon_id):
        raise ValueError('verified owned service identity is required')
    parsed = urlsplit(docker_host) if isinstance(docker_host, str) else None
    if (parsed is None or parsed.scheme != 'unix' or parsed.netloc or not parsed.path.startswith('/')
            or parsed.query or parsed.fragment or '%' in parsed.path or '\x00' in parsed.path
            or '..' in PurePosixPath(parsed.path).parts):
        raise ValueError('a local Docker Unix socket is required')
    api = container.client.api
    adapter = api.adapters.get('http+docker://')
    if (api.base_url != 'http+docker://localhost' or not isinstance(adapter, UnixHTTPAdapter)
            or adapter.socket_path != parsed.path):
        raise ValueError('Docker client is not bound to the verified local socket')
    container.reload()
    attrs = container.attrs
    labels = attrs.get('Config', {}).get('Labels') or {}
    if (container.client.info().get('ID') != daemon_id or attrs.get('Id') != container_id
            or attrs.get('Created') != created or attrs.get('Image') != image_id
            or labels.get('io.graph-loader.e09.owner') != owner
            or labels.get('io.graph-loader.e09.role') != 'neo'):
        raise ValueError('query-log container no longer matches its owned identity')
    parameter_env = [item.partition('=')[2] for item in attrs.get('Config', {}).get('Env', [])
                     if item.partition('=')[0] == 'NEO4J_db_logs_query_parameter__logging__enabled']
    if parameter_env != ['false']:
        raise ValueError('owned Neo4j was not started with parameter logging disabled')
    settings = _settings(driver, attrs)
    _bind_service(container, driver)
    script = ("set -eu; f=/logs/query.log; "
              "i=$(stat -c %i \"$f\"); n=$(stat -c %s \"$f\"); "
              f"[ \"$n\" -le {maximum} ]; "
              "printf '%s %s\\n' \"$i\" \"$n\"; head -c \"$n\" \"$f\"; "
              "j=$(stat -c %i \"$f\"); printf '\\n%s\\n' \"$j\"")
    result = container.exec_run(['sh', '-c', script])
    if result.exit_code != 0 or not isinstance(result.output, bytes):
        raise RuntimeError('owned Neo4j query log is unavailable or exceeds its bound')
    header, separator, rest = result.output.partition(b'\n')
    if not separator or re.fullmatch(rb'[0-9]+ [0-9]+', header) is None:
        raise RuntimeError('invalid query-log snapshot header')
    inode, size = (int(part) for part in header.split())
    if inode <= 0 or size > maximum or len(rest) < size:
        raise RuntimeError('invalid query-log snapshot identity or size')
    content, trailer = rest[:size], rest[size:]
    if trailer != b'\n'+str(inode).encode()+b'\n':
        raise RuntimeError('query log rotated or changed during snapshot')
    if content and not content.endswith(b'\n'):
        raise RuntimeError('query log ended with an incomplete record')
    if _settings(driver, attrs) != settings:
        raise RuntimeError('Neo4j query-log settings changed during capture')
    return Snapshot(inode, content)


def prefix_delta(before, after):
    """Check endpoint prefixes; this alone does not prove continuous coverage."""
    if not isinstance(before, Snapshot) or not isinstance(after, Snapshot):
        raise ValueError('two validated query-log snapshots are required')
    if before.inode != after.inode or not after.content.startswith(before.content):
        raise RuntimeError('query-log coverage is discontinuous')
    return after.content[len(before.content):]


def window(before, after):
    """Fail closed until a continuous capture mechanism is implemented."""
    prefix_delta(before, after)
    raise RuntimeError('continuous query-log coverage is not established by snapshots alone')


def _unambiguous_text(record):
    # Neo4j's text log is not a framed encoding. Restrict observational
    # parsing to records without comments or multiline quoted strings; a fake
    # timestamp/trailer embedded in such text must never become evidence.
    if any(marker in record for marker in (b'/*', b'*/', b'//')):
        raise RuntimeError('query-log record contains ambiguous comment text')
    quote = None
    index = 0
    while index < len(record):
        byte = record[index]
        if quote is not None:
            if byte == 10:
                raise RuntimeError('query-log record contains a multiline quoted string')
            if byte == 92:
                index += 2
                continue
            if byte == quote:
                if index+1 < len(record) and record[index+1] == quote:
                    index += 2
                    continue
                quote = None
        elif byte in (34, 39, 96):
            quote = byte
        index += 1
    if quote is not None:
        raise RuntimeError('query-log record contains an incomplete quoted string')


def waits(data, *, needle, minimum):
    """Extract candidate waits; ambiguous log text cannot prove zero coverage."""
    if not isinstance(data, bytes) or not isinstance(needle, bytes) or not needle or type(minimum) is not int or minimum < 1:
        raise ValueError('identified query and positive minimum are required')
    if data and not data.endswith(b'\n'):
        raise RuntimeError('query-log window has an incomplete record')
    timestamp = rb'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{4}|Z)?'
    header = re.compile(rb'(?m)^'+timestamp+rb'\s+(?:INFO|WARN|ERROR|DEBUG)\b')
    started = re.compile(rb'^'+timestamp+rb'\s+INFO\s+Query started:\s+id:\d+\s+-')
    completed = re.compile(rb'^'+timestamp+rb'\s+INFO\s+id:\d+\s+-\s+transaction id:\d+\s+-\s+'
                           rb'\d+\s+ms:\s+\(planning:\s*\d+,\s*waiting:\s*(\d+)\)\s+-')
    starts = list(header.finditer(data))
    if not starts or starts[0].start() != 0:
        raise RuntimeError('query-log window lacks a valid record header')
    records = [data[match.start():starts[index+1].start() if index+1<len(starts) else len(data)]
               for index, match in enumerate(starts)]
    # A timestamp-looking line inside query text must not silently become a
    # fresh record. Incomplete or ambiguous records fail closed. The final
    # map can contain transaction metadata even when parameters are disabled.
    if any(re.search(rb' - runtime=[A-Za-z0-9_-]+ - \{[^\n]*\}\n\Z', record) is None for record in records):
        raise RuntimeError('query-log record framing is incomplete or ambiguous')
    for record in records:
        _unambiguous_text(record)
    values = []
    for record in records:
        if needle not in record:
            continue
        first_line = record.split(b'\n', 1)[0]
        if started.match(first_line):
            continue
        match = completed.match(first_line)
        if match is None:
            raise RuntimeError('identified query has an invalid waiting field')
        values.append(int(match.group(1)))
    if len(values) < minimum:
        raise RuntimeError('identified workload query records are missing')
    return values
