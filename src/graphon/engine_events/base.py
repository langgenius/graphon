from datetime import UTC, datetime
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, Field, computed_field

from graphon.enums import NodeType
from graphon.node_events.base import NodeRunResult


class EngineEvent(BaseModel):
    """Base model for events emitted by the engine."""

    id: str = Field(default_factory=lambda: str(uuid4()))
    graph_id: str = ""
    execution_id: str = ""
    schema_version: Literal["1.0"] = "1.0"
    sequence: int = Field(default=0, ge=0)
    emitted_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @computed_field
    @property
    def event_type(self) -> str:
        return type(self).__name__


class NodeEvent(EngineEvent):
    """Engine event associated with one node execution."""

    node_execution_id: str = Field(..., description="node execution id")
    node_id: str
    node_type: NodeType
    container_id: str = ""
    """ID of the container that directly owns the event's execution frame."""

    # The version of the node, or "1" if not specified.
    node_version: str = "1"
    node_run_result: NodeRunResult = Field(default_factory=NodeRunResult)
