"""Synchronised database lock conflict on the owned scratch Neo4j (positive control for the lock-wait detector)."""
from __future__ import annotations

import threading
import time


def lock_conflict(stack, value):
    """Hold a write lock, wait until the contender is reported blocked, then release."""
    database = stack.database
    with stack.driver.session(database=database) as session:
        session.run('MERGE (n:LockProbe {id: 1}) ON CREATE SET n.v = 0').consume()
    held = stack.driver.session(database=database)
    tx = held.begin_transaction(timeout=30)
    tx.run('MATCH (n:LockProbe {id: 1}) SET n.v = -1').consume()
    query = f'MATCH (n:LockProbe {{id: 1}}) SET n.v = {value} RETURN n.v AS v'
    result = {}

    def contend():
        with stack.driver.session(database=database) as session:
            result['v'] = session.run(query).single()['v']
    thread = threading.Thread(target=contend)
    thread.start()
    try:
        deadline = time.monotonic() + 20
        while True:
            with stack.driver.session(database='system') as session:
                rows = list(session.run('SHOW TRANSACTIONS YIELD database, currentQuery, status WHERE database = $d '
                                        'RETURN currentQuery, status', d=database))
            if any(r['currentQuery'] == query and r['status'].startswith('Blocked by') for r in rows):
                break
            assert time.monotonic() < deadline, 'contender was never reported as blocked'
            time.sleep(0.2)
        time.sleep(1)
    finally:
        tx.commit()
        held.close()
        thread.join(30)
    assert not thread.is_alive() and result == {'v': value}
    return query
