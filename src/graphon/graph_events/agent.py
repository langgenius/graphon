from collections.abc import Mapping

from pydantic import Field

from .base import GraphAgentNodeEventBase


class NodeRunAgentLogEvent(GraphAgentNodeEventBase):
    message_id: str = Field(..., description="message id")
    label: str = Field(..., description="label")
    node_execution_id: str = Field(..., description="node execution id")
    parent_id: str | None = Field(..., description="parent id")
    error: str | None = Field(..., description="error")
    status: str = Field(..., description="status")
    data: Mapping[str, object] = Field(..., description="data")
    metadata: Mapping[str, object] = Field(default_factory=dict)
