"""Serialized state models for GraphEngine ready queue implementations."""

from collections.abc import Sequence
from typing import Annotated, Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from graphon.nodes.container_effects import ContainerExecutionResult

ROOT_FRAME_ID: Final = "root"


class StartTask(BaseModel):
    """Task that starts a node invocation inside an execution frame."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["start"] = "start"
    frame_id: str = Field(description="Execution frame that owns the task")
    node_id: str = Field(description="Node to execute within the frame")


class ResumeTask(BaseModel):
    """Task that resumes a suspended node invocation."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["resume"] = "resume"
    invocation_id: str = Field(description="Suspended invocation to resume")
    result: ContainerExecutionResult = Field(
        description="Container result to send into the suspended invocation",
    )


ReadyTask = Annotated[StartTask | ResumeTask, Field(discriminator="kind")]


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
