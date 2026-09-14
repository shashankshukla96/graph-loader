"""
src/utils/cypher_generator.py
──────────────────────────────
Generates Cypher DDL (CREATE CONSTRAINT and CREATE INDEX) statements
from a validated GraphSchema object.
"""
from __future__ import annotations

from src.models.schema import GraphSchema


def generate_node_ddl(schema: GraphSchema) -> list[str]:
    """
    Generate Cypher DDL for all nodes in the schema.

    Parameters
    ----------
    schema : GraphSchema
        The fully loaded and validated graph schema.

    Returns
    -------
    list[str]
        A list of idempotent Cypher CREATE statements.
    """
    statements = []

    for node in schema.nodes:
        label = node.label
        for prop_name, prop_config in node.properties.items():
            
            # Process constraints
            if prop_config.constraint:
                ctype = prop_config.constraint.type
                suffix = "unique" if ctype == "uniqueness" else "not_null"
                name = f"{label.lower()}_{prop_name.lower()}_{suffix}"
                
                if ctype == "uniqueness":
                    statements.append(
                        f"CREATE CONSTRAINT {name} IF NOT EXISTS "
                        f"FOR (n:{label}) REQUIRE n.{prop_name} IS UNIQUE"
                    )
                else:  # not_null
                    statements.append(
                        f"CREATE CONSTRAINT {name} IF NOT EXISTS "
                        f"FOR (n:{label}) REQUIRE n.{prop_name} IS NOT NULL"
                    )
            
            # Process indexes
            if prop_config.index:
                itype = prop_config.index.type
                name = f"{label.lower()}_{prop_name.lower()}_{itype}"
                
                if itype == "range":
                    statements.append(
                        f"CREATE RANGE INDEX {name} IF NOT EXISTS "
                        f"FOR (n:{label}) ON (n.{prop_name})"
                    )
                else:  # text
                    statements.append(
                        f"CREATE TEXT INDEX {name} IF NOT EXISTS "
                        f"FOR (n:{label}) ON (n.{prop_name})"
                    )

    return statements

def generate_edge_ddl(schema: GraphSchema) -> list[str]:
    """
    Generate Cypher DDL (indexes) for all edges in the schema.

    Note: Edge constraints are skipped as they are not supported
    in standard Neo4j configurations.

    Parameters
    ----------
    schema : GraphSchema
        The fully loaded and validated graph schema.

    Returns
    -------
    list[str]
        A list of idempotent Cypher CREATE INDEX statements.
    """
    statements = []

    for edge in schema.edges:
        etype = edge.type
        for prop_name, prop_config in edge.properties.items():
            
            # Process indexes only; skip constraints
            if prop_config.index:
                itype = prop_config.index.type
                name = f"{etype.lower()}_{prop_name.lower()}_{itype}"
                
                if itype == "range":
                    statements.append(
                        f"CREATE RANGE INDEX {name} IF NOT EXISTS "
                        f"FOR ()-[r:{etype}]-() ON (r.{prop_name})"
                    )
                else:  # text
                    statements.append(
                        f"CREATE TEXT INDEX {name} IF NOT EXISTS "
                        f"FOR ()-[r:{etype}]-() ON (r.{prop_name})"
                    )

    return statements

def generate_all_ddl(schema: GraphSchema) -> list[str]:
    """
    Generate all Cypher DDL (constraints and indexes) for the entire schema.

    Parameters
    ----------
    schema : GraphSchema
        The fully loaded and validated graph schema.

    Returns
    -------
    list[str]
        A combined list of idempotent Cypher statements for all nodes and edges.
    """
    return generate_node_ddl(schema) + generate_edge_ddl(schema)
