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
        "graphon.graph:Edge",
        "graphon.graph:NodeFactory",
        "graphon.graph.graph:Edge",
        "graphon.graph.graph:NodeFactory",
    ],
)
def test_graph_package_omits_legacy_execution_apis(qualified_name: str) -> None:
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


def test_authoring_a_graph_does_not_load_the_engine_or_executable_nodes() -> None:
    script = dedent(
        """
        import sys

        from graphon import graph

        definition = (
            graph.Graph.new()
            .add_root(graph.NodeDefinition("start", "start"))
            .add_node(graph.NodeDefinition("reply", "answer"))
            .add_root(graph.NodeDefinition("other", "start"))
            .connect(tail="other", head="reply")
            .build()
        )
        assert definition.node("reply").node_type == "answer"
        assert not any(
            name in {"graphon.engine", "graphon.nodes"}
            or name.startswith(("graphon.engine.", "graphon.nodes."))
            for name in sys.modules
        )
        """
    )
    subprocess.run([sys.executable, "-c", script], check=True)  # ruff: ignore[subprocess-without-shell-equals-true]
