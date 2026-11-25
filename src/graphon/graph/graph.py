"""The graph definition shared by the DSL and the engine."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, final

from graphon.enums import BuiltinNodeTypes

from .validation import GraphValidationError, GraphValidationIssue, get_edge_issues

__all__ = [
    "EdgeDefinition",
    "Graph",
    "GraphBuilder",
    "GraphReference",
    "NodeDefinition",
]


def _require_nonempty_string(value: str, label: str) -> None:
    if not isinstance(value, str) or not value:
        msg = f"{label} must be a non-empty string"
        raise ValueError(msg)


def _freeze_config(value: Any) -> Any:
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            msg = "Node configuration keys must be strings"
            raise TypeError(msg)
        return MappingProxyType({
            key: _freeze_config(item) for key, item in value.items()
        })
    if isinstance(value, list | tuple):
        return tuple(_freeze_config(item) for item in value)
    if value is None or isinstance(value, str | bool | int | float):
        return value
    msg = f"Unsupported node configuration value: {type(value).__name__}"
    raise TypeError(msg)


@dataclass(frozen=True, slots=True)
class GraphReference:
    """The host's key for another graph, optionally pinned to a version."""

    id: str
    version: str | None = None

    def __post_init__(self) -> None:
        _require_nonempty_string(self.id, "Graph reference")
        if self.version is not None:
            _require_nonempty_string(self.version, "Graph reference version")


@dataclass(frozen=True, slots=True)
class NodeDefinition:
    id: str
    node_type: str
    version: str = "1"
    config: Mapping[str, Any] = field(default_factory=dict, compare=False)
    call_target: Graph | GraphReference | None = None
    _config_json: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        _require_nonempty_string(self.id, "Node ID")
        _require_nonempty_string(self.node_type, "Node type")
        _require_nonempty_string(self.version, "Node version")
        if not isinstance(self.config, Mapping):
            msg = "Node configuration must be a mapping"
            raise TypeError(msg)
        if self.call_target is not None and not isinstance(
            self.call_target, Graph | GraphReference
        ):
            msg = "A call target must be a Graph or GraphReference"
            raise TypeError(msg)
        frozen_config = _freeze_config(self.config)
        # JSON also distinguishes true from 1 in node settings.
        config_json = json.dumps(
            frozen_config,
            default=dict,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        object.__setattr__(self, "config", frozen_config)
        object.__setattr__(self, "_config_json", config_json)

    def copy_config(self) -> dict[str, Any]:
        """Give a node factory its own mutable copy of these settings."""
        return json.loads(self._config_json)


@dataclass(frozen=True, slots=True)
class EdgeDefinition:
    id: str
    source: str
    target: str
    source_handle: str = "source"

    def __post_init__(self) -> None:
        _require_nonempty_string(self.id, "Edge ID")
        _require_nonempty_string(self.source, "Edge source")
        _require_nonempty_string(self.target, "Edge target")
        if not isinstance(self.source_handle, str):
            msg = "Edge source handle must be a string"
            raise TypeError(msg)


_BUILTIN_ENTRY_TYPES = frozenset((
    BuiltinNodeTypes.START,
    BuiltinNodeTypes.ITERATION_START,
    BuiltinNodeTypes.LOOP_START,
))


def _check_new_node(
    node: NodeDefinition, nodes_by_id: Mapping[str, NodeDefinition]
) -> None:
    if not isinstance(node, NodeDefinition):
        msg = "Graph nodes must be node definitions"
        raise TypeError(msg)
    if node.id in nodes_by_id:
        raise GraphValidationError([
            GraphValidationIssue(
                "DUPLICATE_NODE_ID", f"Duplicate node ID: {node.id}", node.id
            )
        ])


def _is_entry(node: NodeDefinition, entry_types: frozenset[tuple[str, str]]) -> bool:
    return (
        node.node_type in _BUILTIN_ENTRY_TYPES
        or (node.node_type, node.version) in entry_types
    )


@final
@dataclass(frozen=True, slots=True, init=False)
class Graph:
    """One immutable graph with local IDs and ordered outgoing edges.

    Hosts can declare additional entry node types by (type, version).
    Each execution chooses one of the resulting entry IDs.
    """

    nodes: Mapping[str, NodeDefinition]
    edges: Mapping[str, EdgeDefinition]
    entry_node_ids: frozenset[str]
    _incoming_edges: Mapping[str, tuple[EdgeDefinition, ...]] = field(
        repr=False, compare=False
    )
    _outgoing_edges: Mapping[str, tuple[EdgeDefinition, ...]] = field(repr=False)

    def __init__(
        self,
        *,
        nodes: Iterable[NodeDefinition],
        edges: Iterable[EdgeDefinition] = (),
        entry_types: Iterable[tuple[str, str]] = (),
    ) -> None:
        nodes_by_id: dict[str, NodeDefinition] = {}
        for node in nodes:
            _check_new_node(node, nodes_by_id)
            nodes_by_id[node.id] = node
        entry_types = frozenset(entry_types)
        entry_node_ids = frozenset(
            node.id for node in nodes_by_id.values() if _is_entry(node, entry_types)
        )
        if not entry_node_ids:
            raise GraphValidationError([
                GraphValidationIssue(
                    "INVALID_ROOT", "Graph must have at least one entry node."
                )
            ])

        edges_by_id: dict[str, EdgeDefinition] = {}
        incoming: defaultdict[str, list[EdgeDefinition]] = defaultdict(list)
        outgoing: defaultdict[str, list[EdgeDefinition]] = defaultdict(list)
        for edge in edges:
            if not isinstance(edge, EdgeDefinition):
                msg = "Graph edges must be edge definitions"
                raise TypeError(msg)
            if edge.id in edges_by_id:
                raise GraphValidationError([
                    GraphValidationIssue(
                        "DUPLICATE_EDGE_ID", f"Duplicate edge ID: {edge.id}"
                    )
                ])
            edges_by_id[edge.id] = edge
            incoming[edge.target].append(edge)
            outgoing[edge.source].append(edge)
        issues = get_edge_issues(dict.fromkeys(nodes_by_id, ""), edges_by_id.values())
        if issues:
            raise GraphValidationError(issues)
        object.__setattr__(self, "nodes", MappingProxyType(nodes_by_id))
        object.__setattr__(self, "edges", MappingProxyType(edges_by_id))
        object.__setattr__(self, "entry_node_ids", entry_node_ids)
        object.__setattr__(
            self,
            "_incoming_edges",
            MappingProxyType({key: tuple(items) for key, items in incoming.items()}),
        )
        object.__setattr__(
            self,
            "_outgoing_edges",
            MappingProxyType({key: tuple(items) for key, items in outgoing.items()}),
        )

    @staticmethod
    def new(*, entry_types: Iterable[tuple[str, str]] = ()) -> GraphBuilder:
        """Start a new graph definition in Python."""
        return GraphBuilder(entry_types=entry_types)

    def __hash__(self) -> int:
        return hash((
            frozenset(self.nodes.items()),
            frozenset(self._outgoing_edges.items()),
            self.entry_node_ids,
        ))

    def node(self, node_id: str) -> NodeDefinition:
        return self.nodes[node_id]

    def incoming(self, node_id: str) -> tuple[EdgeDefinition, ...]:
        return self._incoming_edges.get(node_id, ())

    def outgoing(self, node_id: str) -> tuple[EdgeDefinition, ...]:
        return self._outgoing_edges.get(node_id, ())


@final
class GraphBuilder:
    """Build immutable graphs by adding node definitions and connecting them."""

    def __init__(self, *, entry_types: Iterable[tuple[str, str]] = ()) -> None:
        self._entry_types = frozenset(entry_types)
        self._nodes_by_id: dict[str, NodeDefinition] = {}
        self._edges: list[EdgeDefinition] = []

    def add_root(self, node: NodeDefinition) -> GraphBuilder:
        """Start a path at an entry node. A graph can have several entries."""
        _check_new_node(node, self._nodes_by_id)
        if not _is_entry(node, self._entry_types):
            raise GraphValidationError([
                GraphValidationIssue(
                    "INVALID_ROOT", f"Node {node.id!r} is not an entry.", node.id
                )
            ])
        self._nodes_by_id[node.id] = node
        return self

    def add_node(
        self,
        node: NodeDefinition,
        *,
        from_node_id: str | None = None,
        source_handle: str = "source",
    ) -> GraphBuilder:
        """Connect a new node from the last node added or the named predecessor."""
        if not self._nodes_by_id:
            msg = "Add a root before adding other nodes"
            raise ValueError(msg)
        _check_new_node(node, self._nodes_by_id)
        source_id = (
            next(reversed(self._nodes_by_id)) if from_node_id is None else from_node_id
        )
        self._add_edge(source_id, node.id, source_handle)
        self._nodes_by_id[node.id] = node
        return self

    def connect(
        self, *, tail: str, head: str, source_handle: str = "source"
    ) -> GraphBuilder:
        """Connect two nodes already in this graph."""
        if head not in self._nodes_by_id:
            msg = f"Unknown target node: {head!r}"
            raise ValueError(msg)
        self._add_edge(tail, head, source_handle)
        return self

    def _add_edge(self, source_id: str, target_id: str, source_handle: str) -> None:
        if source_id not in self._nodes_by_id:
            msg = f"Unknown source node: {source_id!r}"
            raise ValueError(msg)
        self._edges.append(
            EdgeDefinition(
                f"edge_{len(self._edges)}", source_id, target_id, source_handle
            )
        )

    def build(self) -> Graph:
        """Return an immutable snapshot of the current nodes and edges."""
        return Graph(
            nodes=self._nodes_by_id.values(),
            edges=self._edges,
            entry_types=self._entry_types,
        )
