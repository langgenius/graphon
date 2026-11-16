from collections import deque
from collections.abc import Set

from graphon.runtime.runtime_state import GraphProtocol


class _ResponseReachability:
    """Reachability through nonblocking edges and consumed root-edge events.

    Each node becomes reachable once, so shared suffixes never expand into
    separate complete paths. Remember consumed events even when their sources
    are not yet reachable. Live edge states are deliberately ignored: the engine
    may be ahead of the event consumer.
    """

    def __init__(
        self,
        graph: GraphProtocol,
        response_node_id: str,
        blocking_edge_ids: set[str],
        taken_edge_ids: Set[str],
    ) -> None:
        self._graph = graph
        self._response_node_id = response_node_id
        self._blocking_edge_ids = blocking_edge_ids
        self._taken_edge_ids = taken_edge_ids
        self._reachable_node_ids: set[str] = set()
        self._add_reachable_nodes(graph.root_node.id)

    def is_ready(self) -> bool:
        return self._response_node_id in self._reachable_node_ids

    def advance_from_edge(self, edge_id: str) -> bool:
        edge = self._graph.edges.get(edge_id)
        if edge is not None and edge.tail in self._reachable_node_ids:
            self._add_reachable_nodes(edge.head)
        return self.is_ready()

    def _add_reachable_nodes(self, node_id: str) -> None:
        if node_id in self._reachable_node_ids:
            return
        self._reachable_node_ids.add(node_id)
        pending_node_ids = deque([node_id])
        while pending_node_ids:
            current_node_id = pending_node_ids.popleft()
            if current_node_id == self._response_node_id:
                continue
            for edge in self._graph.get_outgoing_edges(current_node_id):
                if (
                    edge.id in self._blocking_edge_ids
                    and edge.id not in self._taken_edge_ids
                ):
                    continue
                if edge.head not in self._reachable_node_ids:
                    self._reachable_node_ids.add(edge.head)
                    pending_node_ids.append(edge.head)
