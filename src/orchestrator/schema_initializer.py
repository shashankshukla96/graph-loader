"""
src/orchestrator/schema_initializer.py
──────────────────────────────────────
Handles Neo4j connection initialization and automated schema
application (constraints and indexes) prior to data loading.
"""
import time
import logging
from neo4j import GraphDatabase, Driver
from neo4j.exceptions import ServiceUnavailable, AuthError

logger = logging.getLogger(__name__)


class SchemaInitializationError(Exception):
    """Raised when the schema initializer fails permanently."""
    pass


def get_neo4j_driver(
    uri: str,
    user: str,
    password: str,
    max_retries: int = 5,
    base_delay: float = 2.0
) -> Driver:
    """
    Creates and verifies a Neo4j driver connection with exponential backoff.

    Parameters
    ----------
    uri : str
        The Neo4j URI (e.g., 'bolt://localhost:7687').
    user : str
        The Neo4j username.
    password : str
        The Neo4j password.
    max_retries : int, default 5
        Maximum number of retry attempts for transient connection errors.
    base_delay : float, default 2.0
        Base delay in seconds for exponential backoff calculations.

    Returns
    -------
    neo4j.Driver
        An active, verified Neo4j driver instance.

    Raises
    ------
    SchemaInitializationError
        If authentication fails, retries are exhausted, or an unexpected
        connection error occurs.
    """
    for attempt in range(max_retries + 1):
        driver = None
        try:
            driver = GraphDatabase.driver(uri, auth=(user, password))
            driver.verify_connectivity()
            logger.info("Successfully connected to Neo4j.")
            return driver
        
        except AuthError as e:
            if driver is not None:
                driver.close()
            raise SchemaInitializationError(f"Authentication failed: {e}") from e
        
        except ServiceUnavailable as e:
            if driver is not None:
                driver.close()
                
            if attempt == max_retries:
                raise SchemaInitializationError(
                    f"Failed to connect to Neo4j after {max_retries} retries."
                ) from e
            
            delay = base_delay * (2 ** attempt)
            logger.warning(
                f"Neo4j service unavailable. Retrying in {delay} seconds "
                f"(Attempt {attempt + 1}/{max_retries})..."
            )
            time.sleep(delay)
            
        except Exception as e:
            if driver is not None:
                driver.close()
            raise SchemaInitializationError(f"Unexpected connection error: {e}") from e
            
    # Fallback (should be unreachable given the max_retries condition above)
    raise SchemaInitializationError("Failed to connect to Neo4j (Unknown state).")

from src.models.schema import GraphSchema
from src.utils.cypher_generator import generate_all_ddl
from neo4j.exceptions import Neo4jError

def apply_schema(driver: Driver, schema: GraphSchema, timeout_seconds: float = 120.0, poll_interval: float = 5.0) -> None:
    """
    Applies the schema DDL to Neo4j and waits for all indexes to become ONLINE.

    Parameters
    ----------
    driver : neo4j.Driver
        An active Neo4j driver.
    schema : GraphSchema
        The schema to apply.
    timeout_seconds : float, default 120.0
        Maximum time to wait for indexes to finish populating.
    poll_interval : float, default 5.0
        Seconds to sleep between polling attempts.

    Raises
    ------
    SchemaInitializationError
        If DDL execution fails, an index enters the FAILED state, or the timeout is exceeded.
    """
    statements = generate_all_ddl(schema)
    
    try:
        with driver.session() as session:
            for stmt in statements:
                session.run(stmt)
    except Neo4jError as e:
        raise SchemaInitializationError(f"Failed to execute DDL: {e}") from e

    start_time = time.monotonic()
    while True:
        with driver.session() as session:
            result = session.run("SHOW INDEXES YIELD name, state RETURN name, state")
            records = list(result)
            
            failed = [r["name"] for r in records if r["state"] == "FAILED"]
            if failed:
                raise SchemaInitializationError(f"Index creation failed for: {failed}")
            
            populating = [r["name"] for r in records if r["state"] == "POPULATING"]
            if not populating:
                logger.info("All constraints and indexes are ONLINE.")
                return
            
        if time.monotonic() - start_time > timeout_seconds:
            raise SchemaInitializationError(
                f"Timeout waiting for indexes to become ONLINE: {populating}"
            )
        
        logger.info(f"Waiting for indexes to populate: {populating}")
        time.sleep(poll_interval)
