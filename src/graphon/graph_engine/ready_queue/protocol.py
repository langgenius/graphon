"""Serialized state models for GraphEngine ready queue implementations."""

from typing import Annotated, Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from graphon.nodes.container_effects import ContainerRunResult

ROOT_FRAME_ID: Final = "root"


class StartTask(BaseModel):
    """Task that starts a node invocation inside an execution frame."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["start"] = "start"
    frame_id: str
    node_id: str


class ResumeTask(BaseModel):
    """Task that resumes a suspended node invocation."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["resume"] = "resume"
    invocation_id: str
    result: ContainerRunResult


ReadyTask = Annotated[StartTask | ResumeTask, Field(discriminator="kind")]


class ReadyQueueState(BaseModel):
    """Pydantic model for serialized ready queue state.

    This defines the structure of the data returned by dumps()
    and expected by loads() for ready queue serialization.
    """

    version: Literal["1.0"]
    items: tuple[ReadyTask, ...]
