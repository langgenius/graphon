from typing import TYPE_CHECKING

from . import _legacy_graph_types
from .graph import EdgeDefinition, Graph, GraphReference, NodeDefinition
from .graph_template import GraphTemplate

if TYPE_CHECKING:
    from graphon.nodes.factory import NodeFactory

    from ._legacy_graph_types import Edge, GraphBuilder

__getattr__ = _legacy_graph_types.get_public_type

__all__ = [
    "Edge",
    "EdgeDefinition",
    "Graph",
    "GraphBuilder",
    "GraphReference",
    "GraphTemplate",
    "NodeDefinition",
    "NodeFactory",
]
