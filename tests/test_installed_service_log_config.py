"""Structural negative controls for the scratch-only query-log experiment."""
from pathlib import Path
from xml.etree import ElementTree

import pytest

from tests.installed_service.query_log_config import make_config


TEMPLATE = (Path(__file__).resolve().parents[1]/'config/neo4j/server-logs.xml').read_bytes()
MARKER = 'e09'+'a'*32


def test_scratch_config_uses_one_unbuffered_nonrolling_marked_query_appender():
    root = ElementTree.fromstring(make_config(TEMPLATE, MARKER))
    assert root.get('monitorInterval') == '0'
    matches = [node for node in root.find('Appenders') if node.get('name') == 'QueryLog']
    assert len(matches) == 1
    appender = matches[0]
    assert appender.tag == 'File'
    assert appender.get('append') == 'true' and appender.get('immediateFlush') == 'true'
    assert appender.get('bufferedIO') == 'false' and appender.get('ignoreExceptions') == 'false'
    assert appender.find('Policies') is None and appender.get('filePattern') is None
    layout = appender.find('JsonLayout')
    assert layout.get('compact') == 'true' and layout.get('eventEol') == 'true'
    assert layout.find('KeyValuePair').attrib == {'key': 'e09_marker', 'value': MARKER}


@pytest.mark.parametrize('marker', ['e09', 'e09'+'a'*31, 'e09'+'G'*32, 'e09'+'a'*32+'extra'])
def test_config_rejects_unsafe_marker(marker):
    with pytest.raises(ValueError):
        make_config(TEMPLATE, marker)


def test_config_rejects_dtd_and_nonexclusive_query_routing():
    with pytest.raises(ValueError):
        make_config(b'<!DOCTYPE x><Configuration/>', MARKER)
    modified = TEMPLATE.replace(b'<AppenderRef ref="QueryLog"/>', b'<AppenderRef ref="QueryLog"/><AppenderRef ref="DebugLog"/>')
    with pytest.raises(ValueError, match='template'):
        make_config(modified, MARKER)


@pytest.mark.parametrize('modified', [
    TEMPLATE.replace(b'<Appenders>', b'<Filters><ThresholdFilter level="OFF"/></Filters><Appenders>'),
    TEMPLATE.replace(b'<Logger name="QueryLogger"', b'<AsyncLogger name="QueryLogger"/><Logger name="QueryLogger"'),
    TEMPLATE.replace(b'<AppenderRef ref="QueryLog"/>', b'<AppenderRef ref="QueryLog" level="OFF"/>'),
    '<?xml version="1.0" encoding="UTF-16"?><!DOCTYPE x [<!ENTITY a "x">]><Configuration>&a;</Configuration>'.encode('utf-16'),
])
def test_only_the_reviewed_template_can_define_query_logging(modified):
    with pytest.raises(ValueError, match='template'):
        make_config(modified, MARKER)
