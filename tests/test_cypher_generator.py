"""
tests/test_cypher_generator.py
───────────────────────────────
Tests for the Cypher DDL generator.
"""
from __future__ import annotations

from pathlib import Path
import pytest

from src.utils.schema_loader import load_schema
from src.utils.cypher_generator import generate_node_ddl

_PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def canonical_schema():
    return load_schema(_PROJECT_ROOT / "config" / "graph_schema.yaml")


class TestNodeDDLGenerator:
    def test_uniqueness_constraint_cypher(self, canonical_schema):
        ddl = generate_node_ddl(canonical_schema)
        # uniqueness on personId
        expected = "CREATE CONSTRAINT person_personid_unique IF NOT EXISTS FOR (n:Person) REQUIRE n.personId IS UNIQUE"
        assert expected in ddl

    def test_not_null_constraint_cypher(self, canonical_schema):
        ddl = generate_node_ddl(canonical_schema)
        # not_null on name
        expected = "CREATE CONSTRAINT person_name_not_null IF NOT EXISTS FOR (n:Person) REQUIRE n.name IS NOT NULL"
        assert expected in ddl

    def test_range_index_cypher(self, canonical_schema):
        ddl = generate_node_ddl(canonical_schema)
        # range index on age
        expected = "CREATE RANGE INDEX person_age_range IF NOT EXISTS FOR (n:Person) ON (n.age)"
        assert expected in ddl

    def test_text_index_cypher(self, canonical_schema):
        ddl = generate_node_ddl(canonical_schema)
        # text index on name
        expected = "CREATE TEXT INDEX person_name_text IF NOT EXISTS FOR (n:Person) ON (n.name)"
        assert expected in ddl

    def test_idempotency_and_lowercase(self, canonical_schema):
        ddl = generate_node_ddl(canonical_schema)
        
        for statement in ddl:
            # All statements must be idempotent
            assert "IF NOT EXISTS" in statement
            
            # Check naming convention: extract name after CONSTRAINT or INDEX
            tokens = statement.split()
            if "CONSTRAINT" in tokens:
                idx = tokens.index("CONSTRAINT")
                name = tokens[idx + 1]
            elif "INDEX" in tokens:
                idx = tokens.index("INDEX")
                name = tokens[idx + 1]
            else:
                pytest.fail(f"Unrecognized statement format: {statement}")
            
            assert name.islower(), f"Name '{name}' is not entirely lowercase"

from src.utils.cypher_generator import generate_edge_ddl

class TestEdgeDDLGenerator:
    def _make_schema_with_edge_prop(self, index_type=None, constraint_type=None):
        from src.models.schema import GraphSchema
        data = {
            "nodes": [{"label": "Person", "topic": "t1", "key_property": "id", "properties": {"id": {"type": "string", "required": True}}}],
            "edges": [{
                "type": "WORKS_AT",
                "topic": "t2",
                "nodes": {"source": "Person", "target": "Person"},
                "source_key_property": "id",
                "target_key_property": "id",
                "properties": {
                    "since": {
                        "type": "date",
                        "required": True,
                    }
                }
            }]
        }
        if index_type:
            data["edges"][0]["properties"]["since"]["index"] = {"type": index_type}
        if constraint_type:
            data["edges"][0]["properties"]["since"]["constraint"] = {"type": constraint_type}
            
        return GraphSchema.model_validate(data)

    def test_edge_range_index_cypher(self):
        schema = self._make_schema_with_edge_prop(index_type="range")
        ddl = generate_edge_ddl(schema)
        expected = "CREATE RANGE INDEX works_at_since_range IF NOT EXISTS FOR ()-[r:WORKS_AT]-() ON (r.since)"
        assert len(ddl) == 1
        assert expected in ddl

    def test_edge_text_index_cypher(self):
        schema = self._make_schema_with_edge_prop(index_type="text")
        ddl = generate_edge_ddl(schema)
        expected = "CREATE TEXT INDEX works_at_since_text IF NOT EXISTS FOR ()-[r:WORKS_AT]-() ON (r.since)"
        assert len(ddl) == 1
        assert expected in ddl

    def test_edge_constraint_ignored(self):
        schema = self._make_schema_with_edge_prop(constraint_type="not_null")
        ddl = generate_edge_ddl(schema)
        # Edge constraints should be skipped, so DDL list is empty
        assert len(ddl) == 0

from src.utils.cypher_generator import generate_all_ddl

def test_generate_all_ddl():
    from src.models.schema import GraphSchema
    # Construct a schema that has both node constraints/indexes and edge indexes
    data = {
        "nodes": [{
            "label": "Person", "topic": "t1", "key_property": "id",
            "properties": {"id": {"type": "string", "required": True, "constraint": {"type": "uniqueness"}}}
        }],
        "edges": [{
            "type": "WORKS_AT", "topic": "t2",
            "nodes": {"source": "Person", "target": "Person"},
            "source_key_property": "id", "target_key_property": "id",
            "properties": {"since": {"type": "date", "required": True, "index": {"type": "range"}}}
        }]
    }
    schema = GraphSchema.model_validate(data)
    
    node_ddl = generate_node_ddl(schema)
    edge_ddl = generate_edge_ddl(schema)
    all_ddl = generate_all_ddl(schema)
    
    # Verify we actually have both node and edge statements
    assert len(node_ddl) > 0
    assert len(edge_ddl) > 0
    
    assert len(all_ddl) == len(node_ddl) + len(edge_ddl)
    assert all_ddl == node_ddl + edge_ddl
