import os
import pytest
from pathlib import Path

from src.utils.schema_loader import load_schema
from src.utils.cypher_generator import generate_all_ddl
from src.orchestrator.schema_initializer import get_neo4j_driver, apply_schema

_PROJECT_ROOT = Path(__file__).resolve().parents[1]

@pytest.fixture(scope="module")
def live_driver():
    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "changeme")
    
    try:
        driver = get_neo4j_driver(uri, user, password, max_retries=1, base_delay=0.1)
    except Exception as e:
        pytest.skip(f"Neo4j is not available for integration test: {e}")
        
    yield driver
    
    driver.close()

@pytest.mark.integration
def test_schema_initializer_against_live_neo4j(live_driver):
    schema = load_schema(_PROJECT_ROOT / "config" / "graph_schema.yaml")
    ddl_statements = generate_all_ddl(schema)
    
    # 1. Clean up existing constraints/indexes
    with live_driver.session() as session:
        for stmt in ddl_statements:
            parts = stmt.split()
            if "CONSTRAINT" in parts:
                name = parts[parts.index("CONSTRAINT") + 1]
                session.run(f"DROP CONSTRAINT {name} IF EXISTS")
            elif "INDEX" in parts:
                name = parts[parts.index("INDEX") + 1]
                session.run(f"DROP INDEX {name} IF EXISTS")
                
    # 2. Run the initializer (first pass)
    apply_schema(live_driver, schema, timeout_seconds=30.0)
    
    # 3. Verify they were created
    with live_driver.session() as session:
        res_c = session.run("SHOW CONSTRAINTS YIELD name RETURN name")
        constraints = [r["name"] for r in res_c]
        
        res_i = session.run("SHOW INDEXES YIELD name RETURN name")
        indexes = [r["name"] for r in res_i]
        
    assert "person_personid_unique" in constraints
    assert "person_name_not_null" in constraints
    assert "person_age_range" in indexes
    # The config doesn't have an edge index, but we can just check one that does exist:
    assert "company_foundedyear_range" in indexes
    
    # 4. Run the initializer again to prove idempotency (IF NOT EXISTS works)
    # Should not raise any Neo4j error
    apply_schema(live_driver, schema, timeout_seconds=30.0)
