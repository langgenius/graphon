from __future__ import annotations

import logging
from abc import abstractmethod
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any, Protocol, final

from pydantic import TypeAdapter

from graphon.entities.graph_config import NodeConfigDict
from graphon.enums import ErrorStrategy, NodeExecutionType, NodeState
from graphon.nodes.base.node import Node

from .edge import Edge
from .validation import get_graph_validator

logger = logging.getLogger(__name__)

_ListNodeConfigDict = TypeAdapter(list[NodeConfigDict])
_ListObjectDict = TypeAdapter(list[dict[str, Any]])


class NodeFactory(Protocol):
    """Protocol for creating Node instances from node data dictionaries.

    This protocol decouples the Graph class from specific node mapping implementations,
    allowing for different node creation strategies while maintaining type safety.
    """

    @abstractmethod
    def create_node(self, node_config: NodeConfigDict) -> Node:
        """Create a Node instance from node configuration data.

        :param node_config: node configuration dictionary containing type and other data
        :return: initialized Node instance
        :raises ValueError: if node type is unknown or no
            implementation exists for the resolved version
        :raises ValidationError: if node_config does not satisfy
            NodeConfigDict/BaseNodeData validation
        """
        ...


@final
class Graph:
    """Graph representation with nodes and edges for workflow execution."""

    nodes: dict[str, Node]
    edges: dict[str, Edge]
    in_edges: dict[str, list[str]]
    out_edges: dict[str, list[str]]
    root_node: Node

    def __init__(
        self,
        *,
        nodes: dict[str, Node] | None = None,
        edges: dict[str, Edge] | None = None,
        in_edges: dict[str, list[str]] | None = None,
        out_edges: dict[str, list[str]] | None = None,
        root_node: Node,
        graph_config: Mapping[str, Any] | None = None,
        node_factory: NodeFactory | None = None,
    ) -> None:
        """Initialize Graph instance.

        :param nodes: graph nodes mapping (node id: node object)
        :param edges: graph edges mapping (edge id: edge object)
        :param in_edges: incoming edges mapping (node id: list of edge ids)
        :param out_edges: outgoing edges mapping (node id: list of edge ids)
        :param root_node: root node object
        """
        self.nodes = nodes or {}
        self.edges = edges or {}
        self.in_edges = in_edges or {}
        self.out_edges = out_edges or {}
        self.root_node = root_node
        self.graph_config = graph_config
        self.node_factory = node_factory

    @classmethod
    def _parse_node_configs(
        cls,
        node_configs: list[NodeConfigDict],
    ) -> dict[str, NodeConfigDict]:
        """Parse node configurations and build a mapping of node IDs to configs.

        :param node_configs: list of node configuration dictionaries

        Returns:
            Mapping of node ID to node config.

        """
        node_configs_map: dict[str, NodeConfigDict] = {}

        for node_config in node_configs:
            node_configs_map[node_config["id"]] = node_config

        return node_configs_map

    @classmethod
    def _build_edges(
        cls,
        edge_configs: list[dict[str, Any]],
    ) -> tuple[dict[str, Edge], dict[str, list[str]], dict[str, list[str]]]:
        """Build edge objects and mappings from edge configurations.

        :param edge_configs: list of edge configurations

        Returns:
            Tuple of `edges`, `in_edges`, and `out_edges` mappings.

        """
        edges: dict[str, Edge] = {}
        in_edges: dict[str, list[str]] = defaultdict(list)
        out_edges: dict[str, list[str]] = defaultdict(list)

        edge_counter = 0
        for edge_config in edge_configs:
            source = edge_config.get("source")
            target = edge_config.get("target")

            if not isinstance(source, str) or not isinstance(target, str):
                continue

            # Create edge
            edge_id = f"edge_{edge_counter}"
            edge_counter += 1

            source_handle = edge_config.get("sourceHandle", "source")
            if not isinstance(source_handle, str):
                continue

            edge = Edge(
                id=edge_id,
                tail=source,
                head=target,
                source_handle=source_handle,
            )

            edges[edge_id] = edge
            out_edges[source].append(edge_id)
            in_edges[target].append(edge_id)

        return edges, dict(in_edges), dict(out_edges)

    @classmethod
    def _create_node_instances(
        cls,
        node_configs_map: dict[str, NodeConfigDict],
        node_factory: NodeFactory,
    ) -> dict[str, Node]:
        """Create node instances from configurations using the node factory.

        :param node_configs_map: mapping of node ID to node config
        :param node_factory: factory for creating node instances

        Returns:
            Mapping of node ID to node instance.

        """
        nodes: dict[str, Node] = {}

        for node_id, node_config in node_configs_map.items():
            try:
                node_instance = node_factory.create_node(node_config)
            except Exception:
                logger.exception(
                    "Failed to create node instance for node_id %s",
                    node_id,
                )
                raise
            nodes[node_id] = node_instance

        return nodes

    @classmethod
    def new(cls) -> GraphBuilder:
        """Create a fluent builder for assembling a graph programmatically."""
        return GraphBuilder(graph_cls=cls)

    @staticmethod
    def _filter_canvas_only_nodes(
        node_configs: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        """Remove editor-only nodes before `NodeConfigDict` validation.

        Persisted note widgets use a top-level `type == "custom-note"` but leave
        `data.type` empty because they are never executable graph nodes. Filter
        them while configs are still raw dicts so Pydantic does not validate
        their placeholder payloads against `BaseNodeData.type: NodeType`.

        Returns:
            Raw node configs with editor-only note widgets removed.

        """
        filtered_node_configs: list[dict[str, Any]] = []
        for node_config in node_configs:
            if node_config.get("type", "") == "custom-note":
                continue
            filtered_node_configs.append(dict(node_config))
        return filtered_node_configs

    @staticmethod
    def _node_container_id(node_config: Mapping[str, Any]) -> str:
        """Resolve the ID of the container that directly owns a node.

        ``data.container_id`` is authoritative. An empty or absent owner places
        the node in the root graph. During migration, one non-empty legacy
        ``iteration_id`` or ``loop_id`` is accepted; nested legacy ownership is
        ambiguous and must therefore be expressed with ``container_id``.

        :param node_config: raw node configuration from the workflow graph

        Returns:
            The direct container node ID, or ``""`` for a root node.

        Raises:
            TypeError: If an ownership field is not a string.
            ValueError: If legacy fields name different containers.

        """
        data = node_config.get("data")
        if not isinstance(data, Mapping):
            return ""

        if "container_id" in data:
            value = data["container_id"]
            if not isinstance(value, str):
                msg = "Node data.container_id must be a string"
                raise TypeError(msg)
            return value

        # Transitional support for existing single-level Loop/Iteration configs.
        legacy_ids: set[str] = set()
        for field in ("iteration_id", "loop_id"):
            value = data.get(field)
            if value is None:
                continue
            if not isinstance(value, str):
                msg = f"Node data.{field} must be a string"
                raise TypeError(msg)
            if not value:
                continue
            legacy_ids.add(value)
        if len(legacy_ids) > 1:
            msg = "Nested container nodes must set data.container_id"
            raise ValueError(msg)
        return next(iter(legacy_ids), "")

    @classmethod
    def _scope_graph_config(
        cls,
        *,
        graph_config: Mapping[str, Any],
        node_configs: list[dict[str, Any]],
        edge_configs: list[dict[str, Any]],
        container_id: str,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
        """Split a workflow graph into one frame's executable and visible scope.

        The direct node and edge lists contain only objects owned by
        ``container_id`` and are used to construct the frame's executable
        :class:`Graph`. The returned graph config also retains recursively nested
        containers so the frame can later construct its children, while excluding
        every parent and sibling scope. Edges between different direct owners are
        invalid because they would bypass the owning container node.

        :param graph_config: complete config visible to the parent frame
        :param node_configs: validated raw node dictionaries from that config
        :param edge_configs: validated raw edge dictionaries from that config
        :param container_id: direct owner to materialize, or ``""`` for root

        Returns:
            Direct nodes, direct edges, and the container subtree config.

        Raises:
            ValueError: If scopes are orphaned, cyclic, or joined by an edge.

        """
        container_ids = {
            node_id: cls._node_container_id(node_config)
            for node_config in node_configs
            if isinstance((node_id := node_config.get("id")), str)
        }
        direct_node_ids = {
            node_id
            for node_id, owning_container_id in container_ids.items()
            if owning_container_id == container_id
        }
        subtree_node_ids = set(direct_node_ids)
        while descendants := {
            node_id
            for node_id, owning_container_id in container_ids.items()
            if owning_container_id in subtree_node_ids
            and node_id not in subtree_node_ids
        }:
            subtree_node_ids.update(descendants)
        if not container_id and subtree_node_ids != set(container_ids):
            orphan_node_ids = sorted(set(container_ids) - subtree_node_ids)
            msg = f"Nodes reference unknown or cyclic containers: {orphan_node_ids}"
            raise ValueError(msg)

        direct_edge_configs: list[dict[str, Any]] = []
        subtree_edge_configs: list[dict[str, Any]] = []
        for edge_config in edge_configs:
            source = edge_config.get("source")
            target = edge_config.get("target")
            if not isinstance(source, str) or not isinstance(target, str):
                continue

            if (
                source in container_ids
                and target in container_ids
                and container_ids[source] != container_ids[target]
            ):
                msg = (
                    f"Edge '{source}->{target}' crosses container scopes "
                    f"'{container_ids[source]}' and '{container_ids[target]}'"
                )
                raise ValueError(msg)

            has_unknown_endpoint = (
                source not in container_ids or target not in container_ids
            )
            if (
                source in direct_node_ids
                or target in direct_node_ids
                or (not container_id and has_unknown_endpoint)
            ):
                direct_edge_configs.append(edge_config)
            if (
                source in subtree_node_ids
                or target in subtree_node_ids
                or (not container_id and has_unknown_endpoint)
            ):
                subtree_edge_configs.append(edge_config)

        scoped_graph_config = dict(graph_config)
        scoped_graph_config["nodes"] = [
            node_config
            for node_config in node_configs
            if node_config.get("id") in subtree_node_ids
        ]
        scoped_graph_config["edges"] = subtree_edge_configs
        direct_node_configs = [
            node_config
            for node_config in node_configs
            if node_config.get("id") in direct_node_ids
        ]
        return direct_node_configs, direct_edge_configs, scoped_graph_config

    @classmethod
    def _promote_fail_branch_nodes(cls, nodes: dict[str, Node]) -> None:
        """Promote nodes configured with FAIL_BRANCH error strategy
        to branch execution type.

        :param nodes: mapping of node ID to node instance
        """
        for node in nodes.values():
            if node.error_strategy == ErrorStrategy.FAIL_BRANCH:
                node.execution_type = NodeExecutionType.BRANCH

    @classmethod
    def mark_inactive_root_branches(
        cls,
        nodes: dict[str, Node],
        edges: dict[str, Edge],
        in_edges: dict[str, list[str]],
        out_edges: dict[str, list[str]],
        active_root_id: str,
    ) -> None:
        """Mark nodes and edges that belong to inactive root branches."""
        cls._mark_inactive_root_branches(
            nodes,
            edges,
            in_edges,
            out_edges,
            active_root_id,
        )

    @classmethod
    def _mark_inactive_root_branches(
        cls,
        nodes: dict[str, Node],
        edges: dict[str, Edge],
        in_edges: dict[str, list[str]],
        out_edges: dict[str, list[str]],
        active_root_id: str,
    ) -> None:
        """Mark nodes and edges from inactive root branches as skipped.

        Algorithm:
        1. Mark inactive root nodes as skipped
        2. For skipped nodes, mark all their outgoing edges as skipped
        3. For each edge marked as skipped, check its target node:
           - If ALL incoming edges are skipped, mark the node as skipped
           - Otherwise, leave the node state unchanged

        :param nodes: mapping of node ID to node instance
        :param edges: mapping of edge ID to edge instance
        :param in_edges: mapping of node ID to incoming edge IDs
        :param out_edges: mapping of node ID to outgoing edge IDs
        :param active_root_id: ID of the active root node
        """
        # Find all top-level root nodes
        # (nodes with ROOT execution type and no incoming edges)
        top_level_roots: list[str] = [
            node.id
            for node in nodes.values()
            if node.execution_type == NodeExecutionType.ROOT
        ]

        # If there's only one root or the active root is not a top-level
        # root, no marking is needed
        if len(top_level_roots) <= 1 or active_root_id not in top_level_roots:
            return

        # Mark inactive root nodes as skipped
        inactive_roots: list[str] = [
            root_id for root_id in top_level_roots if root_id != active_root_id
        ]
        for root_id in inactive_roots:
            if root_id in nodes:
                nodes[root_id].state = NodeState.SKIPPED

        # Recursively mark downstream nodes and edges
        def mark_downstream(node_id: str) -> None:
            """Recursively mark downstream nodes and edges as skipped."""
            if nodes[node_id].state != NodeState.SKIPPED:
                return
            # If this node is skipped, mark all its outgoing edges as skipped
            out_edge_ids = out_edges.get(node_id, [])
            for edge_id in out_edge_ids:
                edge = edges[edge_id]
                edge.state = NodeState.SKIPPED

                # Check the target node of this edge
                target_node = nodes[edge.head]
                in_edge_ids = in_edges.get(target_node.id, [])
                in_edge_states = [edges[eid].state for eid in in_edge_ids]

                # If all incoming edges are skipped, mark the node as skipped
                if all(state == NodeState.SKIPPED for state in in_edge_states):
                    target_node.state = NodeState.SKIPPED
                    # Recursively process downstream nodes
                    mark_downstream(target_node.id)

        # Process each inactive root and its downstream nodes
        for root_id in inactive_roots:
            mark_downstream(root_id)

    @classmethod
    def init(
        cls,
        *,
        graph_config: Mapping[str, Any],
        node_factory: NodeFactory,
        root_node_id: str,
        container_id: str = "",
        skip_validation: bool = False,
    ) -> Graph:
        """Initialize a graph with an explicit execution entry point.

        :param graph_config: graph config containing nodes and edges
        :param node_factory: factory for creating node instances from config data
        :param root_node_id: active root node id
        :param container_id: direct container scope to materialize; empty for root

        Returns:
            Initialized graph instance rooted at `root_node_id`.

        Raises:
            ValueError: If the graph has no nodes or `root_node_id` does not exist.

        """
        # Parse configs
        edge_configs = _ListObjectDict.validate_python(graph_config.get("edges", []))
        raw_node_configs = _ListObjectDict.validate_python(
            graph_config.get("nodes", []),
        )
        raw_node_configs = cls._filter_canvas_only_nodes(raw_node_configs)
        direct_node_configs, direct_edge_configs, scoped_graph_config = (
            cls._scope_graph_config(
                graph_config=graph_config,
                node_configs=raw_node_configs,
                edge_configs=edge_configs,
                container_id=container_id,
            )
        )
        node_configs = _ListNodeConfigDict.validate_python(direct_node_configs)

        if not node_configs:
            msg = "Graph must have at least one node"
            raise ValueError(msg)

        # Parse node configurations
        node_configs_map = cls._parse_node_configs(node_configs)

        if root_node_id not in node_configs_map:
            msg = f"Root node id {root_node_id} not found in the graph"
            raise ValueError(msg)

        # Build edges
        edges, in_edges, out_edges = cls._build_edges(direct_edge_configs)

        # Create node instances
        nodes = cls._create_node_instances(node_configs_map, node_factory)
        for node in nodes.values():
            node.graph_config = scoped_graph_config
            if isinstance(node, Node):
                node.bind_graph_config(scoped_graph_config)

        # Promote fail-branch nodes to branch execution type at graph level
        cls._promote_fail_branch_nodes(nodes)

        # Get root node instance
        root_node = nodes[root_node_id]

        # Mark inactive root branches as skipped
        cls._mark_inactive_root_branches(
            nodes,
            edges,
            in_edges,
            out_edges,
            root_node_id,
        )

        # Create and return the graph
        graph = cls(
            nodes=nodes,
            edges=edges,
            in_edges=in_edges,
            out_edges=out_edges,
            root_node=root_node,
            graph_config=scoped_graph_config,
            node_factory=node_factory,
        )

        if not skip_validation:
            # Validate the graph structure using built-in validators
            get_graph_validator().validate(graph)

        return graph

    @property
    def node_ids(self) -> list[str]:
        """Get list of node IDs (compatibility property for existing code)

        :return: list of node IDs
        """
        return list(self.nodes.keys())

    def get_outgoing_edges(self, node_id: str) -> list[Edge]:
        """Get all outgoing edges from a node (V2 method)

        :param node_id: node id

        Returns:
            All edges whose tail is `node_id`.

        """
        edge_ids = self.out_edges.get(node_id, [])
        return [self.edges[eid] for eid in edge_ids if eid in self.edges]

    def get_incoming_edges(self, node_id: str) -> list[Edge]:
        """Get all incoming edges to a node (V2 method)

        :param node_id: node id

        Returns:
            All edges whose head is `node_id`.

        """
        edge_ids = self.in_edges.get(node_id, [])
        return [self.edges[eid] for eid in edge_ids if eid in self.edges]


@final
class GraphBuilder:
    """Fluent helper for constructing simple graphs, primarily for tests."""

    def __init__(self, *, graph_cls: type[Graph]) -> None:
        self._graph_cls = graph_cls
        self._nodes: list[Node] = []
        self._nodes_by_id: dict[str, Node] = {}
        self._edges: list[Edge] = []
        self._edge_counter = 0

    def add_root(self, node: Node) -> GraphBuilder:
        """Register the root node. Must be called exactly once."""
        if self._nodes:
            msg = "Root node has already been added"
            raise ValueError(msg)
        self._register_node(node)
        self._nodes.append(node)
        return self

    def add_node(
        self,
        node: Node,
        *,
        from_node_id: str | None = None,
        source_handle: str = "source",
    ) -> GraphBuilder:
        """Append a node and connect it from the specified predecessor."""
        if not self._nodes:
            msg = "Root node must be added before adding other nodes"
            raise ValueError(msg)

        predecessor_id = from_node_id or self._nodes[-1].id
        if predecessor_id not in self._nodes_by_id:
            msg = f"Predecessor node '{predecessor_id}' not found"
            raise ValueError(msg)

        predecessor = self._nodes_by_id[predecessor_id]
        self._register_node(node)
        self._nodes.append(node)

        edge_id = f"edge_{self._edge_counter}"
        self._edge_counter += 1
        edge = Edge(
            id=edge_id,
            tail=predecessor.id,
            head=node.id,
            source_handle=source_handle,
        )
        self._edges.append(edge)

        return self

    def connect(
        self,
        *,
        tail: str,
        head: str,
        source_handle: str = "source",
    ) -> GraphBuilder:
        """Connect two existing nodes without adding a new node."""
        if tail not in self._nodes_by_id:
            msg = f"Tail node '{tail}' not found"
            raise ValueError(msg)
        if head not in self._nodes_by_id:
            msg = f"Head node '{head}' not found"
            raise ValueError(msg)

        edge_id = f"edge_{self._edge_counter}"
        self._edge_counter += 1
        edge = Edge(id=edge_id, tail=tail, head=head, source_handle=source_handle)
        self._edges.append(edge)

        return self

    def build(self) -> Graph:
        """Materialize the graph instance from the accumulated nodes and edges."""
        if not self._nodes:
            msg = "Cannot build an empty graph"
            raise ValueError(msg)

        nodes = {node.id: node for node in self._nodes}
        edges = {edge.id: edge for edge in self._edges}
        in_edges: dict[str, list[str]] = defaultdict(list)
        out_edges: dict[str, list[str]] = defaultdict(list)

        for edge in self._edges:
            out_edges[edge.tail].append(edge.id)
            in_edges[edge.head].append(edge.id)

        graph = self._graph_cls(
            nodes=nodes,
            edges=edges,
            in_edges=dict(in_edges),
            out_edges=dict(out_edges),
            root_node=self._nodes[0],
        )

        get_graph_validator().validate(graph)

        return graph

    def _register_node(self, node: Node) -> None:
        if not node.id:
            msg = "Node must have a non-empty id"
            raise ValueError(msg)
        if node.id in self._nodes_by_id:
            msg = f"Duplicate node id detected: {node.id}"
            raise ValueError(msg)
        self._nodes_by_id[node.id] = node
