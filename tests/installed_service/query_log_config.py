"""Build a scratch-only, non-rolling structured query-log experiment."""
from __future__ import annotations

import hashlib
import re
from xml.etree import ElementTree


TEMPLATE_SHA256 = 'd2375b2e25222138064c463d33293c6a05f01051c7af1dad42d16e21594735d6'


def make_config(template, marker):
    """Transform a bounded trusted template; this is not continuity proof."""
    if not isinstance(template, bytes) or not 0 < len(template) <= 256*1024 or b'<!DOCTYPE' in template.upper():
        raise ValueError('bounded XML template without a DTD is required')
    # The reviewed checked-in Neo4j template is the sole accepted source.
    # Mutations, alternate encodings, filters and async routes require review.
    if hashlib.sha256(template).hexdigest() != TEMPLATE_SHA256:
        raise ValueError('query-log template differs from its reviewed hash')
    if not isinstance(marker, str) or re.fullmatch(r'e09[0-9a-f]{32}', marker) is None:
        raise ValueError('unique E09 marker required')
    root = ElementTree.fromstring(template)
    if root.tag != 'Configuration':
        raise ValueError('unexpected Log4j configuration root')
    appenders = root.find('Appenders')
    loggers = root.find('Loggers')
    if appenders is None or loggers is None:
        raise ValueError('Log4j appenders and loggers are required')
    query = [node for node in appenders if node.get('name') == 'QueryLog']
    logger = [node for node in loggers if node.tag == 'Logger' and node.get('name') == 'QueryLogger']
    if (len(query) != 1 or len(logger) != 1 or logger[0].get('additivity') != 'false'
            or logger[0].get('level') != 'INFO'
            or len(logger[0]) != 1 or logger[0][0].tag != 'AppenderRef'
            or logger[0][0].get('ref') != 'QueryLog'):
        raise ValueError('query logger must route only to its synchronous appender')
    if query[0].get('fileName') != '${config:server.directories.logs}/query.log':
        raise ValueError('unexpected query-log path')
    root.set('monitorInterval', '0')
    index = list(appenders).index(query[0])
    appenders.remove(query[0])
    file_appender = ElementTree.Element('File', {
        'name': 'QueryLog', 'fileName': '${config:server.directories.logs}/query.log',
        'append': 'true', 'immediateFlush': 'true', 'bufferedIO': 'false', 'ignoreExceptions': 'false'})
    layout = ElementTree.SubElement(file_appender, 'JsonLayout', {
        'compact': 'true', 'eventEol': 'true', 'properties': 'false', 'includeStacktrace': 'false'})
    ElementTree.SubElement(layout, 'KeyValuePair', {'key': 'e09_marker', 'value': marker})
    appenders.insert(index, file_appender)
    return ElementTree.tostring(root, encoding='utf-8', xml_declaration=True)
