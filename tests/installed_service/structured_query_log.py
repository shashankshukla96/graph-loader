"""Strict parsing for the experimental scratch-only JSON query log.

Parsing events does not prove their completeness, source ownership, or log
continuity.  Those are separate acceptance gates.
"""
from __future__ import annotations

import json
import re


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate JSON key')
        result[key] = value
    return result


def _constant(value):
    raise ValueError('non-finite JSON number: '+value)


def events(data, *, marker, maximum=8*1024*1024):
    """Parse bounded complete JSON events with their server query IDs."""
    if (not isinstance(data, bytes) or type(maximum) is not int or maximum < 1
            or len(data) > maximum or not isinstance(marker, str)
            or re.fullmatch(r'e09[0-9a-f]{32}', marker) is None):
        raise ValueError('bounded data and a valid run marker are required')
    if not data or not data.endswith(b'\n'):
        raise RuntimeError('structured query log is empty or incomplete')
    started = re.compile(r'^Query started:\s+id:(\d+)\s+-\s+transaction id:(\d+)\s+-\s+\d+\s+ms:\s+'
                         r'\(planning:\s*\d+,\s*waiting:\s*(\d+)\)\s+-')
    completed = re.compile(r'^id:(\d+)\s+-\s+transaction id:(\d+)\s+-\s+\d+\s+ms:\s+'
                           r'\(planning:\s*\d+,\s*waiting:\s*(\d+)\)\s+-')
    query_text = re.compile(r'\s+-?\d+\s+B\s+-\s+\d+\s+page hits,\s+\d+\s+page faults\s+-\s+'
                            r'bolt-session\tbolt\t[^\t\r\n]*\t[^\t\r\n]*\tclient/[^\t\r\n]+\t'
                            r'server/[^\t\r\n>]+>\t(?P<database>[A-Za-z0-9._-]+) - '
                            r'(?P<user>[A-Za-z0-9._-]+) - (?P<query>.*) - '
                            r'runtime=(?P<runtime>[A-Za-z0-9_-]+) - \{[^\n]*\}$', re.DOTALL)
    failed_text = re.compile(query_text.pattern[:-len(r' - \{[^\n]*\}$')].replace(
        '(?P<database>[A-Za-z0-9._-]+)', '(?P<database>[A-Za-z0-9._-]+|<none>)')
        + r' - \{[^\n]*\} - (?P<failure>[^\n]{1,2048})$', re.DOTALL)
    # A statement rejected before it ran (for example a Cypher version probe) is logged once at ERROR, with no start record,
    # transaction id -1 and no database.
    failed_header = re.compile(r'^id:(\d+)\s+-\s+transaction id:(-1|\d+)\s+-\s+\d+\s+ms:\s+'
                               r'\(planning:\s*\d+,\s*waiting:\s*(\d+)\)\s+-')
    seen = set()
    identities = {}
    completions = {}
    result = []
    for line in data.splitlines():
        try:
            event = json.loads(line.decode('utf-8'), object_pairs_hook=_unique, parse_constant=_constant)
        except (UnicodeDecodeError, ValueError) as exc:
            raise RuntimeError('structured query event is malformed') from exc
        if (not isinstance(event, dict) or event.get('e09_marker') != marker
                or event.get('loggerName') != 'QueryLogger' or event.get('level') not in ('INFO', 'ERROR')
                or not isinstance(event.get('message'), str)):
            raise RuntimeError('structured query event identity differs')
        message = event['message']
        if event['level'] == 'ERROR':
            # A query that fails or is terminated is logged at ERROR in the completion format followed by the
            # failure text. Anything else at ERROR keeps failing closed.
            match, phase = failed_header.match(message), 'failed'
            if match is None or failed_text.match(message, match.end()) is None:
                raise RuntimeError('structured query event identity differs')
        else:
            match = started.match(message)
            phase = 'started'
            if match is None:
                match = completed.match(message)
                phase = 'completed'
        if match is None:
            raise RuntimeError('structured query event has an unsupported or failed format')
        if message.count(' - bolt-session\tbolt\t') != 1 or message.count('\tserver/') != 1:
            raise RuntimeError('structured query connection frame is ambiguous')
        text = (failed_text if event['level'] == 'ERROR' else query_text).match(message, match.end())
        if text is None:
            raise RuntimeError('structured query text cannot be identified')
        query_id, transaction_id, waiting = (int(value) for value in match.groups())
        if phase == 'failed' and (transaction_id == -1) != (text.group('database') == '<none>'):
            raise RuntimeError('structured query event identity differs')
        if phase == 'failed' and transaction_id == -1:
            phase = 'rejected'
        key = (query_id, 'completed' if phase in ('failed', 'rejected') else phase)  # a query ends once: completed or failed
        identity = (transaction_id, text.group('database'), text.group('user'), text.group('query'))
        previous = identities.get(query_id)
        # Neo4j 5.26 can change transaction IDs while executing this system
        # administration command.  Keep the exception limited to the exact
        # scratch database creation form observed in the live control.
        admin_transition = (previous is not None and previous[1:] == identity[1:]
                            and identity[1] == 'system'
                            and identity[3] == 'CREATE DATABASE '+marker[:19]+' WAIT')
        # Neo4j 5.26 logs a schema command's completion twice (observed live for
        # CREATE CONSTRAINT ... IF NOT EXISTS). Only an identical second completion
        # of such a command is tolerated; it is flagged and never counted. The
        # repeat must also carry the same waiting time and the same schema runtime
        # as the first completion, or it is a conflicting measurement.
        schema_repeat = (phase == 'completed' and key in seen and previous == identity
                         and text.group('runtime') == 'schema'
                         and completions.get(query_id) == (waiting, 'schema')
                         and re.match(r'(?:CREATE|DROP)\s+(?:CONSTRAINT|INDEX)\b', identity[3]) is not None
                         and (query_id, 'schema-repeat') not in seen)
        if (query_id < 1 or (transaction_id < 1 and phase != 'rejected') or (key in seen and not schema_repeat)
                or (previous is not None and previous != identity and not admin_transition)):
            raise RuntimeError('structured query ID is invalid or duplicated')
        seen.add(key)
        if schema_repeat:
            seen.add((query_id, 'schema-repeat'))
        identities[query_id] = identity
        if phase == 'completed' and not schema_repeat:
            completions[query_id] = (waiting, text.group('runtime'))
        result.append({'query_id': query_id, 'transaction_id': transaction_id,
                       'phase': phase, 'waiting_ms': waiting, 'query': text.group('query'),
                       'database': text.group('database'), 'user': text.group('user'),
                       **({'failure': text.group('failure')[:200]} if phase in ('failed', 'rejected') else {}),
                       **({'duplicate': True} if schema_repeat else {})})
    return result


def completed_for(records, *, query, database, user, expected):
    """Require exactly the requested number of matching completion records.

    The caller must independently establish that ``expected`` includes every
    server-side attempt, retry and failure; this function cannot infer that.
    """
    if (any(not isinstance(value, str) or not value for value in (query, database, user))
            or type(expected) is not int or expected < 1):
        raise ValueError('query identity and positive expected count are required')
    matches = [row for row in records if row['phase'] == 'completed' and not row.get('duplicate')
               and row['query'] == query and row['database'] == database and row['user'] == user]
    if len(matches) != expected:
        raise RuntimeError('identified query completion count differs')
    return matches
