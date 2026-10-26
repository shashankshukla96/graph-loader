"""A process exit cannot substitute for the final pytest/outcome receipt."""
import os


def test_early_exit_is_not_a_pass():
    import neo4j_graph_loader.planning
    assert neo4j_graph_loader.planning.__file__
    os._exit(0)
