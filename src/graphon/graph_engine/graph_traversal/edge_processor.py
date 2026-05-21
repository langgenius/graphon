"""Edge processing logic for graph traversal."""

from collections.abc import Sequence
from typing import final

from graphon.enums import NodeExecutionType
from graphon.graph.edge import Edge
from graphon.graph.graph import Graph
from graphon.graph_events.traversal import GraphEdgeSkippedEvent, GraphEdgeTakenEvent

from ..graph_state_manager import GraphStateManager
from .skip_propagator import SkipPropagator

type GraphTraversalEvent = GraphEdgeTakenEvent | GraphEdgeSkippedEvent


@final
class EdgeProcessor:
    """Processes edges during graph execution.

    This handles marking edges as taken or skipped, emitting traversal events,
    triggering downstream node execution, and managing branch node logic.
    """

    def __init__(
        self,
        graph: Graph,
        state_manager: GraphStateManager,
        skip_propagator: SkipPropagator,
    ) -> None:
        """Initialize the edge processor.

        Args:
            graph: The workflow graph
            state_manager: Unified state manager
            skip_propagator: Propagator for skip states

        """
        self._graph = graph
        self._state_manager = state_manager
        self._skip_propagator = skip_propagator

    def process_node_success(
        self,
        node_id: str,
        selected_handle: str | None = None,
    ) -> tuple[Sequence[str], Sequence[GraphTraversalEvent]]:
        """Process edges after a node succeeds.

        Args:
            node_id: The ID of the succeeded node
            selected_handle: For branch nodes, the selected edge handle

        Returns:
            Tuple of (list of downstream node IDs that are now ready,
            list of traversal events)

        """
        node = self._graph.nodes[node_id]

        if node.execution_type == NodeExecutionType.BRANCH:
            return self.handle_branch_completion(node_id, selected_handle)
        return self._process_non_branch_node_edges(node_id)

    def _process_non_branch_node_edges(
        self,
        node_id: str,
    ) -> tuple[Sequence[str], Sequence[GraphTraversalEvent]]:
        """Process edges for non-branch nodes (mark all as TAKEN).

        Args:
            node_id: The ID of the succeeded node

        Returns:
            Tuple of (list of downstream nodes ready for execution,
            list of traversal events)

        """
        return self._process_taken_edges(self._graph.get_outgoing_edges(node_id))

    def _process_taken_edges(
        self,
        edges: Sequence[Edge],
    ) -> tuple[list[str], list[GraphEdgeTakenEvent]]:
        ready_nodes: list[str] = []
        traversal_events: list[GraphEdgeTakenEvent] = []
        for edge in edges:
            nodes, events = self._process_taken_edge(edge)
            ready_nodes.extend(nodes)
            traversal_events.extend(events)
        return ready_nodes, traversal_events

    def _process_taken_edge(
        self,
        edge: Edge,
    ) -> tuple[Sequence[str], Sequence[GraphEdgeTakenEvent]]:
        """Mark edge as taken and check downstream node.

        Args:
            edge: The edge to process

        Returns:
            Tuple of (
                list containing downstream node ID if it's ready,
                list of traversal events
            )

        """
        # Mark edge as taken
        self._state_manager.mark_edge_taken(edge.id)

        # Check if downstream node is ready
        ready_nodes: list[str] = []
        if self._state_manager.is_node_ready(edge.head):
            ready_nodes.append(edge.head)

        return ready_nodes, [self._build_taken_event(edge)]

    def handle_branch_completion(
        self,
        node_id: str,
        selected_handle: str | None,
    ) -> tuple[Sequence[str], Sequence[GraphTraversalEvent]]:
        """Handle completion of a branch node.

        Args:
            node_id: The ID of the branch node
            selected_handle: The handle of the selected branch

        Returns:
            Tuple of (list of downstream nodes ready for execution,
            list of traversal events)

        Raises:
            ValueError: If no branch was selected

        """
        if not selected_handle:
            msg = f"Branch node {node_id} completed without selecting a branch"
            raise ValueError(msg)

        selected_edges, unselected_edges = self._state_manager.categorize_branch_edges(
            node_id,
            selected_handle,
        )

        skipped_events = self._skip_propagator.skip_branch_paths(unselected_edges)

        ready_nodes, taken_events = self._process_taken_edges(selected_edges)
        return ready_nodes, [*skipped_events, *taken_events]

    def validate_branch_selection(self, node_id: str, selected_handle: str) -> bool:
        """Validate that a branch selection is valid.

        Args:
            node_id: The ID of the branch node
            selected_handle: The handle to validate

        Returns:
            True if the selection is valid

        """
        outgoing_edges = self._graph.get_outgoing_edges(node_id)
        valid_handles = {edge.source_handle for edge in outgoing_edges}
        return selected_handle in valid_handles

    @staticmethod
    def _build_taken_event(edge: Edge) -> GraphEdgeTakenEvent:
        return GraphEdgeTakenEvent(
            edge_id=edge.id,
            source_node_id=edge.tail,
            target_node_id=edge.head,
            source_handle=edge.source_handle,
        )
