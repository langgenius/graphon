from __future__ import annotations

import logging
import uuid
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, final

from pydantic import TypeAdapter

from graphon.entities.graph_config import NodeConfigDict
from graphon.enums import ErrorStrategy, NodeExecutionType, NodeState
from graphon.graph.config import normalize_nodes, prepare_edge_configs
from graphon.graph.validation import (
    GraphValidationError,
    get_edge_issues,
    get_graph_validator,
)
from graphon.nodes.base.node import Node
from graphon.nodes.factory import NodeFactory

if TYPE_CHECKING:
    from graphon.graph import Graph

    from .frame_graph_factory import FrameGraphFactory

logger = logging.getLogger(__name__)

_ListNodeConfigDict = TypeAdapter(list[NodeConfigDict])
_ListObjectDict = TypeAdapter(list[dict[str, Any]])


@dataclass
class Edge:
    """A connection and its traversal state in one execution frame."""

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    tail: str = ""  # tail node id (source)
    head: str = ""  # head node id (target)
    source_handle: str = "source"  # source handle for conditional branching
    state: NodeState = field(default=NodeState.UNKNOWN)  # edge execution state

    @property
    def source(self) -> str:
        return self.tail

    @property
    def target(self) -> str:
        return self.head


@final
class FrameGraph:
    """Executable nodes, edges, and the selected entry for one frame."""

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
        """Initialize FrameGraph instance.

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
        self.legacy_edge_ids: Mapping[str, str] | None = None
        self.definition: Graph | None = None
        self.frame_graph_factory: FrameGraphFactory | None = None

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

        Raises:
            ValueError: If two edges in this graph use the same public ID.

        """
        edges: dict[str, Edge] = {}
        edge_ids: set[str] = set()
        in_edges: dict[str, list[str]] = defaultdict(list)
        out_edges: dict[str, list[str]] = defaultdict(list)

        for edge_config in edge_configs:
            source = edge_config["source"]
            target = edge_config["target"]

            edge_id = edge_config["id"]
            if edge_id in edge_ids:
                msg = f"Duplicate graph edge ID: {edge_id}"
                raise ValueError(msg)
            edge_ids.add(edge_id)

            source_handle = edge_config.get("sourceHandle", "source")

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

    @classmethod
    def _scope_graph_config(
        cls,
        *,
        graph_config: Mapping[str, Any],
        node_configs: list[dict[str, Any]],
        edge_configs: list[dict[str, Any]],
        container_ids: Mapping[str, str],
        container_id: str,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
        """Split a graph configuration into one frame's executable and visible scope.

        The direct node and edge lists contain only objects owned by
        ``container_id`` and are used to construct the frame's executable
        :class:`FrameGraph`. The returned graph config also retains recursively nested
        containers so the frame can later construct its children, while excluding
        every parent and sibling scope. Edges between different direct owners are
        invalid because they would bypass the owning container node.

        :param graph_config: complete config visible to the parent frame
        :param node_configs: copied nodes carrying canonical ``data.container_id``
        :param edge_configs: validated raw edge dictionaries from that config
        :param container_ids: direct container ID already resolved for every node
        :param container_id: direct owner to materialize, or ``""`` for root

        Returns:
            Direct nodes, direct edges, and the container subtree config.

        Raises:
            ValueError: If scopes are orphaned, cyclic, or joined by an edge.

        """
        direct_node_ids = {
            node_id
            for node_id, node_container_id in container_ids.items()
            if node_container_id == container_id
        }
        subtree_node_ids = set(direct_node_ids)
        while descendants := {
            node_id
            for node_id, node_container_id in container_ids.items()
            if node_container_id in subtree_node_ids and node_id not in subtree_node_ids
        }:
            subtree_node_ids.update(descendants)
        if not container_id and subtree_node_ids != set(container_ids):
            orphan_node_ids = sorted(set(container_ids) - subtree_node_ids)
            msg = f"Nodes reference unknown or cyclic containers: {orphan_node_ids}"
            raise ValueError(msg)

        direct_edge_configs: list[dict[str, Any]] = []
        subtree_edge_configs: list[dict[str, Any]] = []
        for edge_config in edge_configs:
            source = edge_config["source"]
            target = edge_config["target"]

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
        entry_node_ids: frozenset[str] | None = None,
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
        if entry_node_ids is None:
            entry_node_ids = frozenset(
                node.id
                for node in nodes.values()
                if node.execution_type == NodeExecutionType.ROOT
            )

        # If there's only one root or the active root is not a top-level
        # root, no marking is needed
        if len(entry_node_ids) <= 1 or active_root_id not in entry_node_ids:
            return

        # Mark inactive root nodes as skipped
        inactive_roots = (entry_node_ids & nodes.keys()) - {active_root_id}
        for root_id in inactive_roots:
            nodes[root_id].state = NodeState.SKIPPED

        # Unchecked graphs can contain cycles or missing targets.
        visited_node_ids: set[str] = set()

        # Recursively mark downstream nodes and edges
        def mark_downstream(node_id: str) -> None:
            """Recursively mark downstream nodes and edges as skipped."""
            if node_id in visited_node_ids or nodes[node_id].state != NodeState.SKIPPED:
                return
            visited_node_ids.add(node_id)
            # If this node is skipped, mark all its outgoing edges as skipped
            out_edge_ids = out_edges.get(node_id, [])
            for edge_id in out_edge_ids:
                edge = edges[edge_id]
                edge.state = NodeState.SKIPPED

                # Check the target node of this edge
                target_node = nodes.get(edge.head)
                if target_node is None:
                    continue
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

    @staticmethod
    def _validate_subtree_edges(
        graph_config: Mapping[str, Any],
        container_ids: Mapping[str, str],
    ) -> None:
        """Validate retained subtree edges before any node is constructed."""
        issues = get_edge_issues(
            {
                node_config["id"]: container_ids[node_config["id"]]
                for node_config in graph_config["nodes"]
            },
            (
                Edge(
                    id=edge_config["id"],
                    tail=edge_config["source"],
                    head=edge_config["target"],
                    source_handle=edge_config.get("sourceHandle", "source"),
                )
                for edge_config in graph_config["edges"]
            ),
        )
        if issues:
            raise GraphValidationError(issues)

    @classmethod
    def init(
        cls,
        *,
        graph_config: Mapping[str, Any],
        node_factory: NodeFactory,
        root_node_id: str,
        container_id: str = "",
        skip_validation: bool = False,
    ) -> FrameGraph:
        """Initialize a graph with an explicit execution entry point.

        :param graph_config: graph config containing nodes and edges
        :param node_factory: factory for creating node instances from config data
        :param root_node_id: active root node id
        :param container_id: direct container scope to materialize; empty for root
        :param skip_validation: bypass endpoint existence, root type, and cycle
            checks; input fields, IDs, schemas, and ownership are still validated

        Returns:
            Initialized graph instance rooted at `root_node_id`.

        Raises:
            ValueError: If the graph has no nodes or `root_node_id` does not exist.

        """
        # Parse configs
        node_configs, container_ids = normalize_nodes(
            _ListObjectDict.validate_python(graph_config.get("nodes", [])),
        )

        direct_node_configs, direct_edge_configs, scoped_graph_config = (
            cls._scope_graph_config(
                graph_config=graph_config,
                node_configs=node_configs,
                edge_configs=prepare_edge_configs(
                    graph_config,
                    container_ids,
                ),
                container_ids=container_ids,
                container_id=container_id,
            )
        )
        node_factory = node_factory.with_graph_config(scoped_graph_config)
        scoped_node_configs = _ListNodeConfigDict.validate_python(
            scoped_graph_config["nodes"],
        )
        execution_types = {
            node_config["id"]: node_factory.validate_node(node_config)
            for node_config in scoped_node_configs
        }
        # Every descendant is prevalidated above, so container ownership can be
        # checked before direct-frame nodes run constructors or post-init hooks.
        for container_node_id in sorted(
            {
                container_ids[node_config["id"]]
                for node_config in scoped_node_configs
                if container_ids[node_config["id"]]
            }
            & execution_types.keys(),
        ):
            if execution_types[container_node_id] != NodeExecutionType.CONTAINER:
                msg = (
                    f"Node '{container_node_id}' owns child nodes "
                    "but is not a container"
                )
                raise ValueError(msg)
        node_configs_map = cls._parse_node_configs(
            _ListNodeConfigDict.validate_python(direct_node_configs),
        )

        if not node_configs_map:
            msg = "Graph must have at least one node"
            raise ValueError(msg)

        if root_node_id not in node_configs_map:
            msg = f"Root node id {root_node_id} not found in the graph"
            raise ValueError(msg)

        if not skip_validation:
            cls._validate_subtree_edges(
                scoped_graph_config,
                container_ids,
            )

        return cls.from_node_configs(
            node_configs_map=node_configs_map,
            direct_edge_configs=direct_edge_configs,
            scoped_graph_config=scoped_graph_config,
            node_factory=node_factory,
            root_node_id=root_node_id,
            skip_validation=skip_validation,
        )

    @classmethod
    def from_node_configs(
        cls,
        *,
        node_configs_map: dict[str, NodeConfigDict],
        direct_edge_configs: list[dict[str, Any]],
        scoped_graph_config: Mapping[str, Any],
        node_factory: NodeFactory,
        root_node_id: str,
        entry_node_ids: frozenset[str] | None = None,
        skip_validation: bool = False,
    ) -> FrameGraph:
        """Construct fresh executable nodes and edges from checked node configs."""
        # Build edges
        edges, in_edges, out_edges = cls._build_edges(direct_edge_configs)

        # Create node instances
        nodes = cls._create_node_instances(node_configs_map, node_factory)
        # Promote fail-branch nodes to branch execution type at graph level
        cls._promote_fail_branch_nodes(nodes)

        # Mark inactive root branches as skipped
        cls._mark_inactive_root_branches(
            nodes,
            edges,
            in_edges,
            out_edges,
            root_node_id,
            entry_node_ids,
        )

        # Create and return the graph
        graph = cls(
            nodes=nodes,
            edges=edges,
            in_edges=in_edges,
            out_edges=out_edges,
            root_node=nodes[root_node_id],
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

    def __init__(self, *, graph_cls: type[FrameGraph]) -> None:
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

    def build(self) -> FrameGraph:
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
        if not isinstance(node.id, str) or not node.id:
            msg = "Node must have a non-empty string id"
            raise ValueError(msg)
        if node.id in self._nodes_by_id:
            msg = f"Duplicate node id detected: {node.id}"
            raise ValueError(msg)
        self._nodes_by_id[node.id] = node
