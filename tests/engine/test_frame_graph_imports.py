import subprocess  # ruff: ignore[suspicious-subprocess-import]
import sys
from textwrap import dedent

from graphon import dsl
from graphon.graph import Graph


def test_execution_creates_an_engine_frame_graph() -> None:
    engine = dsl.loads("""
kind: graph
graph:
  nodes:
    - {id: start, data: {type: start, variables: []}}
  edges: []
""")

    assert isinstance(engine.definition, Graph)
    assert type(engine.graph).__module__.startswith("graphon.engine.")
    assert engine.graph.root_node.id == "start"


def test_using_a_graph_definition_does_not_load_the_engine() -> None:
    script = dedent(
        """
        import sys

        from graphon import graph

        definition = graph.Graph(nodes=[graph.NodeDefinition("start", "start")])
        assert definition.node("start").node_type == "start"
        assert not any(
            name == "graphon.engine" or name.startswith("graphon.engine.")
            for name in sys.modules
        )
        """
    )
    subprocess.run([sys.executable, "-c", script], check=True)  # ruff: ignore[subprocess-without-shell-equals-true]
