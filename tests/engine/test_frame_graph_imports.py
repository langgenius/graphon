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
def test_graph_package_omits_builder_and_execution_apis(qualified_name: str) -> None:
    with pytest.raises(AttributeError):
        resolve_name(qualified_name)


def test_old_graph_edge_module_is_removed() -> None:
    assert find_spec("graphon.graph.edge") is None


@pytest.mark.parametrize(
    "name",
    [
        "graphon.engine:FrameGraph.new",
        "graphon.engine:GraphBuilder",
        "graphon.engine.frame_graph:GraphBuilder",
    ],
)
def test_engine_omits_graph_authoring_apis(name: str) -> None:
    with pytest.raises(AttributeError):
        resolve_name(name)


def test_graph_and_builder_import_independently_of_dsl_and_execution() -> None:
    script = dedent(
        """
        import sys

        from graphon.graph import Graph, NodeDefinition

        start = NodeDefinition("start", "start")
        assert Graph(nodes=[start]).node("start") == start
        assert "graphon.graph_builder" not in sys.modules

        from graphon.graph_builder import GraphBuilder

        assert GraphBuilder.__module__.split(".")[:2] == ["graphon", "graph_builder"]
        builder = GraphBuilder()
        start_id = builder.add_node("start", "start")
        reply_id = builder.add_node("reply", "answer")
        builder.connect(start_id, reply_id)
        definition = builder.build()
        assert isinstance(definition, Graph)
        assert definition.node("reply").node_type == "answer"
        assert not any(
            name in {"graphon.dsl", "graphon.engine", "graphon.nodes"}
            or name.startswith(("graphon.dsl.", "graphon.engine.", "graphon.nodes."))
            for name in sys.modules
        )
        """
    )
    subprocess.run([sys.executable, "-c", script], check=True)  # ruff: ignore[subprocess-without-shell-equals-true]
