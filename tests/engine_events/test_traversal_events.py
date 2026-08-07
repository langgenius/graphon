from graphon.engine_events import (
    EngineEvent,
    GraphEdgeSkippedEvent,
    GraphEdgeTakenEvent,
)

_ENVELOPE_FIELDS = {*EngineEvent.model_fields, "event_type"}


def test_graph_edge_taken_event_exports_payload() -> None:
    event = GraphEdgeTakenEvent(
        edge_id="edge-1",
        source_node_id="source",
        target_node_id="target",
        source_handle="success",
    )

    assert event.model_dump(exclude=_ENVELOPE_FIELDS) == {
        "edge_id": "edge-1",
        "source_node_id": "source",
        "target_node_id": "target",
        "source_handle": "success",
        "container_id": "",
    }


def test_graph_edge_skipped_event_exports_payload() -> None:
    event = GraphEdgeSkippedEvent(
        edge_id="edge-2",
        source_node_id="source",
        target_node_id="other",
    )

    assert event.model_dump(exclude=_ENVELOPE_FIELDS) == {
        "edge_id": "edge-2",
        "source_node_id": "source",
        "target_node_id": "other",
        "source_handle": None,
        "container_id": "",
    }
