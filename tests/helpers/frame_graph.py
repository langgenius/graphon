from itertools import pairwise

from graphon.engine import Edge, FrameGraph
from graphon.nodes.base.node import Node


def build_linear_frame_graph(root: Node, *following: Node) -> FrameGraph:
    """Connect test nodes in order with the edge IDs used by saved snapshots."""
    nodes = (root, *following)
    edges = [
        Edge(id=f"edge_{index}", tail=source.id, head=target.id)
        for index, (source, target) in enumerate(pairwise(nodes))
    ]
    return FrameGraph(
        root_node=root,
        nodes={node.id: node for node in nodes},
        edges={edge.id: edge for edge in edges},
        in_edges={edge.head: [edge.id] for edge in edges},
        out_edges={edge.tail: [edge.id] for edge in edges},
    )
