from pydantic import Field

from graphon.graph_events.base import BaseGraphEvent


class GraphEdgeTakenEvent(BaseGraphEvent):
    """Event emitted when graph traversal marks an edge as taken."""

    edge_id: str = Field(..., description="edge id")
    source_node_id: str = Field(..., description="source node id")
    target_node_id: str = Field(..., description="target node id")
    source_handle: str | None = Field(default=None, description="source handle")


class GraphEdgeSkippedEvent(BaseGraphEvent):
    """Event emitted when graph traversal marks an edge as skipped."""

    edge_id: str = Field(..., description="edge id")
    source_node_id: str = Field(..., description="source node id")
    target_node_id: str = Field(..., description="target node id")
    source_handle: str | None = Field(default=None, description="source handle")
