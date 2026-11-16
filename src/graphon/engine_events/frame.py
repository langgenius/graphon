"""Frame observations for raw consumers and external engine event filters."""

from collections.abc import Mapping
from typing import Literal

from pydantic import Field

from graphon.entities.workflow_start_reason import WorkflowStartReason
from graphon.variables.variables import Variable

from .base import EngineEvent


class FrameStartedEvent(EngineEvent):
    """Detached values and reserved identities after setup, before body execution."""

    frame_id: str
    parent_frame_id: str | None = None
    invocation_id: str | None = None
    container_execution_id: str | None = None
    root_node_id: str | None = None
    index: int | None = None
    reason: WorkflowStartReason = WorkflowStartReason.INITIAL
    node_execution_ids: Mapping[str, str] = Field(default_factory=dict)
    variables: tuple[Variable, ...] = ()


class FrameFinishedEvent(EngineEvent):
    """The frame will publish no further execution or variable observations."""

    frame_id: str
    outcome: Literal["succeeded", "failed", "aborted"]
    error: str | None = None


class FrameVariablesUpdatedEvent(EngineEvent):
    """Detached committed replacements, including empty and None values."""

    frame_id: str
    variables: tuple[Variable, ...]
