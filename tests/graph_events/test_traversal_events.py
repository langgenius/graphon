from graphon.graph_events import GraphEdgeSkippedEvent, GraphEdgeTakenEvent
from graphon.graph_events.traversal import (
    GraphEdgeSkippedEvent as TraversalGraphEdgeSkippedEvent,
)
from graphon.graph_events.traversal import (
    GraphEdgeTakenEvent as TraversalGraphEdgeTakenEvent,
)


def test_graph_edge_taken_event_exports_payload() -> None:
    event = GraphEdgeTakenEvent(
        edge_id="edge-1",
        source_node_id="source",
        target_node_id="target",
        source_handle="success",
    )

    assert GraphEdgeTakenEvent is TraversalGraphEdgeTakenEvent
    assert event.model_dump() == {
        "edge_id": "edge-1",
        "source_node_id": "source",
        "target_node_id": "target",
        "source_handle": "success",
    }


def test_graph_edge_skipped_event_exports_payload() -> None:
    event = GraphEdgeSkippedEvent(
        edge_id="edge-2",
        source_node_id="source",
        target_node_id="other",
    )

    assert GraphEdgeSkippedEvent is TraversalGraphEdgeSkippedEvent
    assert event.model_dump() == {
        "edge_id": "edge-2",
        "source_node_id": "source",
        "target_node_id": "other",
        "source_handle": None,
    }
