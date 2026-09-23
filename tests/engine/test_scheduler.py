from collections.abc import Sequence
from typing import cast

from graphon.engine.scheduler import Scheduler
from graphon.engine_events.traversal import GraphEdgeSkippedEvent, GraphEdgeTakenEvent
from graphon.enums import BuiltinNodeTypes, NodeExecutionType, NodeState
from graphon.graph.edge import Edge
from graphon.graph.graph import Graph
from graphon.runtime.runtime_state import RuntimeState
from graphon.runtime.variable_pool import VariablePool

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


def _branch_scheduler() -> Scheduler:
    graph = _Graph()
    return Scheduler(
        graph=cast(Graph, graph),
        state=RuntimeState(
            workflow_id="workflow", variable_pool=VariablePool(), start_at=0
        ),
        frame_id="branch-frame",
    )


def _edge_payloads(
    events: Sequence[_TraversalEvent],
) -> list[tuple[str, str, str, str | None]]:
    return [
        (event.edge_id, event.source_node_id, event.target_node_id, event.source_handle)
        for event in events
    ]


def test_scheduler_emits_taken_and_skipped_events_for_branch() -> None:
    scheduler = _branch_scheduler()

    ready_nodes, events = scheduler.handle_branch_completion("branch", "yes")

    assert ready_nodes == ["selected"]
    assert any(isinstance(event, GraphEdgeTakenEvent) for event in events)
    assert any(isinstance(event, GraphEdgeSkippedEvent) for event in events)
    assert {event.frame_id for event in events} == {"branch-frame"}
    assert _edge_payloads(events) == [
        ("edge-skipped", "branch", "skipped", "no"),
        ("edge-propagated", "skipped", "skipped_child", "success"),
        ("edge-selected", "branch", "selected", "yes"),
    ]


def test_process_node_success_emits_propagated_skip_events_for_branch() -> None:
    scheduler = _branch_scheduler()

    ready_nodes, events = scheduler.process_node_success("branch", "yes")

    assert ready_nodes == ["selected"]
    assert _edge_payloads(events) == [
        ("edge-skipped", "branch", "skipped", "no"),
        ("edge-propagated", "skipped", "skipped_child", "success"),
        ("edge-selected", "branch", "selected", "yes"),
    ]
