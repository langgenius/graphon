from graphon.engine_events import GraphEdgeSkippedEvent, GraphEdgeTakenEvent


def test_graph_edge_taken_event_exports_payload() -> None:
    event = GraphEdgeTakenEvent(
        frame_id="frame-1",
        edge_id="edge-1",
        source_node_id="source",
        target_node_id="target",
        source_handle="success",
    )

    assert event.model_dump() == {
        "frame_id": "frame-1",
        "edge_id": "edge-1",
        "source_node_id": "source",
        "target_node_id": "target",
        "source_handle": "success",
        "container_id": "",
    }


def test_graph_edge_skipped_event_exports_payload() -> None:
    event = GraphEdgeSkippedEvent(
        frame_id="frame-2",
        edge_id="edge-2",
        source_node_id="source",
        target_node_id="other",
    )

    assert event.model_dump() == {
        "frame_id": "frame-2",
        "edge_id": "edge-2",
        "source_node_id": "source",
        "target_node_id": "other",
        "source_handle": None,
        "container_id": "",
    }
