"""Graph state manager that combines node, edge, and execution tracking."""

import threading
from collections.abc import Sequence
from typing import TypedDict, final

from graphon.enums import NodeState
from graphon.graph.edge import Edge
from graphon.graph.graph import Graph
from graphon.runtime.graph_runtime_state import GraphRuntimeState

from .ready_queue import ReadyTask, StartTask


class EdgeStateAnalysis(TypedDict):
    """Analysis result for edge states."""

    has_unknown: bool
    has_taken: bool
    all_skipped: bool


@final
class GraphStateManager:
    def __init__(
        self,
        graph: Graph,
        graph_runtime_state: GraphRuntimeState,
        frame_id: str,
    ) -> None:
        """Initialize the state manager.

        Args:
            graph: The workflow graph
            graph_runtime_state: Runtime state owning ready task queues
            frame_id: Execution frame managed by this instance

        """
        self._graph = graph
        self._graph_runtime_state = graph_runtime_state
        self._frame_id = frame_id
        self._lock = threading.Lock()

        self._unfinished_nodes: set[str] = set()

    # ============= Node State Operations =============

    def enqueue_node(self, node_id: str) -> None:
        """Mark a node as TAKEN and add its task to the ready queue.

        This combines the state transition and enqueueing operations
        that always occur together when preparing a node for execution.

        Args:
            node_id: The ID of the node to enqueue

        """
        with self._lock:
            self._graph.nodes[node_id].state = NodeState.TAKEN
            self._unfinished_nodes.add(node_id)
            self._graph_runtime_state.enqueue_ready_task(
                StartTask(frame_id=self._frame_id, node_id=node_id),
            )

    def mark_node_skipped(self, node_id: str) -> None:
        """Mark a node as SKIPPED.

        Args:
            node_id: The ID of the node to skip

        """
        with self._lock:
            self._graph.nodes[node_id].state = NodeState.SKIPPED

    def is_node_ready(self, node_id: str) -> bool:
        """Check if a node is ready to be executed.

        A node is ready when all its incoming edges from taken branches
        have been satisfied.

        Args:
            node_id: The ID of the node to check

        Returns:
            True if the node is ready for execution

        """
        with self._lock:
            # Get all incoming edges to this node
            incoming_edges = self._graph.get_incoming_edges(node_id)

            # If no incoming edges, node is always ready
            if not incoming_edges:
                return True

            # If any edge is UNKNOWN, node is not ready
            if any(edge.state == NodeState.UNKNOWN for edge in incoming_edges):
                return False

            # Node is ready if at least one edge is TAKEN
            return any(edge.state == NodeState.TAKEN for edge in incoming_edges)

    # ============= Edge State Operations =============

    def mark_edge_taken(self, edge_id: str) -> None:
        """Mark an edge as TAKEN.

        Args:
            edge_id: The ID of the edge to mark

        """
        with self._lock:
            self._graph.edges[edge_id].state = NodeState.TAKEN

    def mark_edge_skipped(self, edge_id: str) -> None:
        """Mark an edge as SKIPPED.

        Args:
            edge_id: The ID of the edge to mark

        """
        with self._lock:
            self._graph.edges[edge_id].state = NodeState.SKIPPED

    def analyze_edge_states(self, edges: list[Edge]) -> EdgeStateAnalysis:
        """Analyze the states of edges and return summary flags.

        Args:
            edges: List of edges to analyze

        Returns:
            Analysis result with state flags

        """
        with self._lock:
            states = {edge.state for edge in edges}

            return EdgeStateAnalysis(
                has_unknown=NodeState.UNKNOWN in states,
                has_taken=NodeState.TAKEN in states,
                all_skipped=(
                    states == frozenset((NodeState.SKIPPED,)) if states else True
                ),
            )

    def categorize_branch_edges(
        self,
        node_id: str,
        selected_handle: str,
    ) -> tuple[Sequence[Edge], Sequence[Edge]]:
        """Categorize branch edges into selected and unselected.

        Args:
            node_id: The ID of the branch node
            selected_handle: The handle of the selected edge

        Returns:
            A tuple of (selected_edges, unselected_edges)

        """
        with self._lock:
            outgoing_edges = self._graph.get_outgoing_edges(node_id)
            selected_edges: list[Edge] = []
            unselected_edges: list[Edge] = []

            for edge in outgoing_edges:
                if edge.source_handle == selected_handle:
                    selected_edges.append(edge)
                else:
                    unselected_edges.append(edge)

            return selected_edges, unselected_edges

    # ============= Execution Tracking Operations =============

    def track_unfinished(self, node_id: str) -> None:
        """Restore an unfinished node to this frame's execution tracking.

        Args:
            node_id: The ID of the unfinished node

        """
        with self._lock:
            self._unfinished_nodes.add(node_id)

    def finish_execution(self, node_id: str) -> None:
        """Mark a node as no longer pending or running.

        Args:
            node_id: The ID of the node finishing execution

        """
        with self._lock:
            self._unfinished_nodes.discard(node_id)

    # ============= Composite Operations =============

    def is_execution_complete(self) -> bool:
        """Check if this frame's execution is complete.

        Tasks are marked executing when they are enqueued, so this frame is
        complete when no task in this manager remains pending or running.

        Returns:
            True if execution is complete

        """
        with self._lock:
            return not self._unfinished_nodes

    def defer_ready_tasks(self, tasks: Sequence[ReadyTask]) -> None:
        """Move unclaimed tasks into deferred storage."""
        for task in tasks:
            self._graph_runtime_state.defer_ready_task(task)
