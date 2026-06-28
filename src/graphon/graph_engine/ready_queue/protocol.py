"""Serialized state models for GraphEngine ready queue implementations."""

from collections.abc import Sequence
from typing import Final

from pydantic import BaseModel, ConfigDict, Field

ROOT_FRAME_ID: Final = "root"


class ReadyTask(BaseModel):
    """A concrete interpreter task scheduled for execution."""

    model_config = ConfigDict(frozen=True)

    frame_id: str = Field(description="Execution frame that owns the task")
    node_id: str = Field(description="Node to execute within the frame")


class ReadyQueueState(BaseModel):
    """Pydantic model for serialized ready queue state.

    This defines the structure of the data returned by dumps()
    and expected by loads() for ready queue serialization.
    """

    type: str = Field(
        description="Queue implementation type (e.g., 'InMemoryReadyQueue')",
    )
    version: str = Field(description="Serialization format version")
    items: Sequence[ReadyTask] = Field(
        default_factory=list,
        description="List of ready tasks in the queue",
    )
