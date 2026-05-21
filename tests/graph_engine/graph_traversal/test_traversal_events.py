from collections.abc import Sequence
from typing import cast

from graphon.enums import BuiltinNodeTypes, NodeExecutionType, NodeState
from graphon.graph.edge import Edge
from graphon.graph.graph import Graph
from graphon.graph_engine.graph_state_manager import GraphStateManager
from graphon.graph_engine.graph_traversal.edge_processor import EdgeProcessor
from graphon.graph_engine.graph_traversal.skip_propagator import SkipPropagator
from graphon.graph_events.traversal import GraphEdgeSkippedEvent, GraphEdgeTakenEvent

type _TraversalEvent = GraphEdgeTakenEvent | GraphEdgeSkippedEvent


class _Node:
    node_type = BuiltinNodeTypes.CODE

    def __init__(self, node_id: str, execution_type: NodeExecutionType) -> None:
        self.id = node_id
        self.execution_type = execution_type
        self.state = NodeState.UNKNOWN


class _Graph:
    def __init__(self) -> None:
        self.nodes = {
            "branch": _Node("branch", NodeExecutionType.BRANCH),
            "selected": _Node("selected", NodeExecutionType.EXECUTABLE),
            "skipped": _Node("skipped", NodeExecutionType.EXECUTABLE),
            "skipped_child": _Node("skipped_child", NodeExecutionType.EXECUTABLE),
        }
        self.edges = {
            "edge-selected": Edge(
                id="edge-selected",
                tail="branch",
                head="selected",
                source_handle="yes",
            ),
            "edge-skipped": Edge(
                id="edge-skipped",
                tail="branch",
                head="skipped",
                source_handle="no",
            ),
            "edge-propagated": Edge(
                id="edge-propagated",
                tail="skipped",
                head="skipped_child",
                source_handle="success",
            ),
        }

    def get_outgoing_edges(self, node_id: str) -> list[Edge]:
        return [edge for edge in self.edges.values() if edge.tail == node_id]

    def get_incoming_edges(self, node_id: str) -> list[Edge]:
        return [edge for edge in self.edges.values() if edge.head == node_id]


class _StateManager:
    def __init__(self, graph: _Graph) -> None:
        self.graph = graph
        self.started_nodes: list[str] = []

    def categorize_branch_edges(
        self,
        node_id: str,
        selected_handle: str,
    ) -> tuple[list[Edge], list[Edge]]:
        edges = self.graph.get_outgoing_edges(node_id)
        return (
            [edge for edge in edges if edge.source_handle == selected_handle],
            [edge for edge in edges if edge.source_handle != selected_handle],
        )

    def mark_edge_taken(self, edge_id: str) -> None:
        self.graph.edges[edge_id].state = NodeState.TAKEN

    def mark_edge_skipped(self, edge_id: str) -> None:
        self.graph.edges[edge_id].state = NodeState.SKIPPED

    def mark_node_skipped(self, node_id: str) -> None:
        self.graph.nodes[node_id].state = NodeState.SKIPPED

    def is_node_ready(self, node_id: str) -> bool:
        return node_id == "selected"

    def analyze_edge_states(self, edges: list[Edge]) -> dict[str, bool]:
        states = [edge.state for edge in edges]
        return {
            "has_unknown": any(state == NodeState.UNKNOWN for state in states),
            "has_taken": any(state == NodeState.TAKEN for state in states),
            "all_skipped": bool(states)
            and all(state == NodeState.SKIPPED for state in states),
        }

    def enqueue_node(self, node_id: str) -> None:
        self.started_nodes.append(node_id)

    def start_execution(self, node_id: str) -> None:
        self.started_nodes.append(node_id)


def _branch_processor() -> EdgeProcessor:
    graph = _Graph()
    state_manager = _StateManager(graph)
    skip_propagator = SkipPropagator(
        graph=cast(Graph, graph),
        state_manager=cast(GraphStateManager, state_manager),
    )
    return EdgeProcessor(
        graph=cast(Graph, graph),
        state_manager=cast(GraphStateManager, state_manager),
        skip_propagator=skip_propagator,
    )


def _edge_payloads(
    events: Sequence[_TraversalEvent],
) -> list[tuple[str, str, str, str | None]]:
    return [
        (event.edge_id, event.source_node_id, event.target_node_id, event.source_handle)
        for event in events
    ]


def test_edge_processor_emits_taken_and_skipped_events_for_branch() -> None:
    processor = _branch_processor()

    ready_nodes, events = processor.handle_branch_completion("branch", "yes")

    assert ready_nodes == ["selected"]
    assert any(isinstance(event, GraphEdgeTakenEvent) for event in events)
    assert any(isinstance(event, GraphEdgeSkippedEvent) for event in events)
    assert _edge_payloads(events) == [
        ("edge-skipped", "branch", "skipped", "no"),
        ("edge-propagated", "skipped", "skipped_child", "success"),
        ("edge-selected", "branch", "selected", "yes"),
    ]


def test_process_node_success_emits_propagated_skip_events_for_branch() -> None:
    processor = _branch_processor()

    ready_nodes, events = processor.process_node_success("branch", "yes")

    assert ready_nodes == ["selected"]
    assert _edge_payloads(events) == [
        ("edge-skipped", "branch", "skipped", "no"),
        ("edge-propagated", "skipped", "skipped_child", "success"),
        ("edge-selected", "branch", "selected", "yes"),
    ]
