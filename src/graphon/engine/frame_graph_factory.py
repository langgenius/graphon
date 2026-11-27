"""Build frame graphs from shared definitions."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

from graphon.entities.graph_config import NodeConfigDict, NodeConfigDictAdapter
from graphon.enums import NodeExecutionType
from graphon.graph import Graph, GraphReference, NodeDefinition
from graphon.graph.validation import GraphValidationError, GraphValidationIssue
from graphon.nodes.factory import NodeFactory

from .frame_graph import FrameGraph

type GraphResolver = Callable[[GraphReference], Graph]


def _build_node_config(node: NodeDefinition, container_id: str) -> dict[str, Any]:
    data = node.copy_config()
    data.update(type=node.node_type, version=node.version, container_id=container_id)
    return {"id": node.id, "data": data}


def _build_edge_configs(graph: Graph) -> list[dict[str, Any]]:
    return [
        {
            "id": edge.id,
            "source": edge.source,
            "target": edge.target,
            "sourceHandle": edge.source_handle,
        }
        for edge in graph.edges.values()
    ]


def build_local_graph_config(graph: Graph, container_id: str) -> dict[str, Any]:
    """Build the node and edge configuration for a single graph."""
    return {
        "nodes": [
            _build_node_config(node, container_id) for node in graph.nodes.values()
        ],
        "edges": _build_edge_configs(graph),
    }


class FrameGraphFactory:
    """Create frame graphs for one run and resolve each graph reference once."""

    def __init__(self, resolver: GraphResolver | None) -> None:
        self._resolver = resolver
        self._resolved_graphs: dict[GraphReference, Graph] = {}

    def resolve_call_target(self, node: NodeDefinition) -> Graph:
        target = node.call_target
        if isinstance(target, Graph):
            return target
        if target is None:
            msg = f"Node {node.id!r} has no graph to call"
            raise ValueError(msg)
        if target not in self._resolved_graphs:
            if self._resolver is None:
                msg = f"Node {node.id!r} needs a resolver for graph {target.id!r}"
                raise ValueError(msg)
            graph = self._resolver(target)
            if not isinstance(graph, Graph):
                msg = f"Expected a Graph for reference {target.id!r}"
                raise TypeError(msg)
            self._resolved_graphs[target] = graph
        return self._resolved_graphs[target]

    def walk_graphs(
        self,
        graph: Graph,
        container_id: str = "",
        *,
        stop_at_node_type: str | None = None,
        ancestor_graph_ids: frozenset[int] = frozenset(),
    ) -> Iterator[tuple[Graph, str]]:
        yield graph, container_id
        ancestor_graph_ids |= {id(graph)}
        for node in graph.nodes.values():
            if node.call_target is not None and node.node_type != stop_at_node_type:
                child = self.resolve_call_target(node)
                # A recursive reference can open another frame when it is invoked.
                if id(child) not in ancestor_graph_ids:
                    yield from self.walk_graphs(
                        child,
                        node.id,
                        stop_at_node_type=stop_at_node_type,
                        ancestor_graph_ids=ancestor_graph_ids,
                    )

    def create(
        self,
        graph: Graph,
        node_factory: NodeFactory,
        root_node_id: str,
        container_id: str = "",
    ) -> FrameGraph:
        if root_node_id not in graph.entry_node_ids:
            raise GraphValidationError([
                GraphValidationIssue(
                    "INVALID_ROOT",
                    f"Node {root_node_id!r} is not an entry of this graph.",
                    root_node_id,
                )
            ])
        # Existing factories and response filters read the flat subtree config.
        # Node construction and edge lookup always use the frame's local IDs.
        graph_configs = [
            (scope, build_local_graph_config(scope, owner_id))
            for scope, owner_id in self.walk_graphs(graph, container_id)
        ]
        subtree_config = {
            "nodes": [
                node
                for _, scope_config in graph_configs
                for node in scope_config["nodes"]
            ],
            "edges": [
                edge
                for _, scope_config in graph_configs
                for edge in scope_config["edges"]
            ],
        }
        node_factory = node_factory.with_graph_config(subtree_config)
        local_node_configs: dict[str, NodeConfigDict] = {}
        for scope, scope_config in graph_configs:
            for raw_node_config in scope_config["nodes"]:
                node_config = NodeConfigDictAdapter.validate_python(raw_node_config)
                node = scope.node(node_config["id"])
                execution_type = node_factory.validate_node(node_config)
                if (
                    node.call_target is not None
                    and execution_type != NodeExecutionType.CONTAINER
                ):
                    msg = f"Node '{node.id}' owns child nodes but is not a container"
                    raise ValueError(msg)
                if scope is graph:
                    local_node_configs[node.id] = node_config
        frame_graph = FrameGraph.from_node_configs(
            node_configs_map=local_node_configs,
            direct_edge_configs=graph_configs[0][1]["edges"],
            scoped_graph_config=subtree_config,
            node_factory=node_factory,
            root_node_id=root_node_id,
            entry_node_ids=graph.entry_node_ids,
        )
        frame_graph.definition = graph
        frame_graph.frame_graph_factory = self
        return frame_graph
