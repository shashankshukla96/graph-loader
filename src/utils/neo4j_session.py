"""Open Neo4j sessions in the configured database when one is specified."""
from __future__ import annotations

import os

from neo4j import Driver, Session


def open_session(driver: Driver) -> Session:
    database = os.environ.get("NEO4J_DATABASE", "").strip()
    if database:
        return driver.session(database=database)
    return driver.session()
