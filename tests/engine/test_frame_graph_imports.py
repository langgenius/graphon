import subprocess  # ruff: ignore[suspicious-subprocess-import]
import sys
from importlib.util import find_spec
from pkgutil import resolve_name
from textwrap import dedent

import pytest

from graphon import dsl
from graphon.engine import FrameGraph
from graphon.graph import Graph


def test_loading_dsl_builds_an_engine_frame_graph() -> None:
    engine = dsl.loads("""
kind: graph
graph:
  nodes:
    - {id: start, data: {type: start, variables: []}}
  edges: []
""")

    assert isinstance(engine.definition, Graph)
    assert isinstance(engine.graph, FrameGraph)
    assert type(engine.graph).__module__.startswith("graphon.engine.")
    assert engine.graph.root_node.id == "start"


@pytest.mark.parametrize(
    "qualified_name",
    [
        "graphon.graph:Graph.init",
        "graphon.graph:Graph.new",
        "graphon.graph:Edge",
        "graphon.graph:GraphBuilder",
        "graphon.graph:NodeFactory",
        "graphon.graph.graph:Edge",
        "graphon.graph.graph:GraphBuilder",
        "graphon.graph.graph:NodeFactory",
    ],
)
def test_graph_package_omits_legacy_execution_apis(qualified_name: str) -> None:
    with pytest.raises(AttributeError):
        resolve_name(qualified_name)


def test_old_graph_edge_module_is_removed() -> None:
    assert find_spec("graphon.graph.edge") is None


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
