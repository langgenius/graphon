"""Build Graph definitions in Python."""

from collections.abc import Iterable, Mapping
from typing import Any, final

from graphon.graph import EdgeDefinition, Graph, GraphReference, NodeDefinition
from graphon.graph.validation import check_unique_node_id


@final
class GraphBuilder:
    """Register nodes and connect their local IDs in the desired edge order."""

    def __init__(self, *, entry_types: Iterable[tuple[str, str]] = ()) -> None:
        self._entry_types = frozenset(entry_types)
        self._nodes_by_id: dict[str, NodeDefinition] = {}
        self._edges: list[EdgeDefinition] = []

    def add_node(
        self,
        node_id: str,
        node_type: str,
        *,
        version: str = "1",
        config: Mapping[str, Any] | None = None,
        call_target: Graph | GraphReference | None = None,
    ) -> str:
        """Register a node and return the ID that connects it to other nodes."""
        node = NodeDefinition(
            node_id,
            node_type,
            version=version,
            config={} if config is None else config,
            call_target=call_target,
        )
        check_unique_node_id(node.id, self._nodes_by_id)
        self._nodes_by_id[node.id] = node
        return node.id

    def connect(
        self, source: str, target: str, *, source_handle: str = "source"
    ) -> None:
        """Add a directed edge between two registered nodes."""
        if source not in self._nodes_by_id:
            msg = f"Unknown source node: {source!r}"
            raise ValueError(msg)
        if target not in self._nodes_by_id:
            msg = f"Unknown target node: {target!r}"
            raise ValueError(msg)
        self._edges.append(
            EdgeDefinition(f"edge_{len(self._edges)}", source, target, source_handle)
        )

    def build(self) -> Graph:
        """Return an immutable snapshot of the current nodes and edges."""
        return Graph(
            nodes=self._nodes_by_id.values(),
            edges=self._edges,
            entry_types=self._entry_types,
        )
