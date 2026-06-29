from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from graphon.node_events.base import NodeRunResult

if TYPE_CHECKING:
    from graphon.nodes.iteration.entities import ErrorHandleMode


@dataclass(frozen=True, slots=True)
class LoopFrameRequest:
    kind: Literal["loop"]
    started_at: datetime
    inputs: Mapping[str, object]
    loop_count: int
    root_node_id: str
    loop_variable_selectors: Mapping[str, Sequence[str]]
    loop_node_ids: frozenset[str]
    index: int


@dataclass(frozen=True, slots=True)
class IterationFrameRequest:
    kind: Literal["iteration"]
    started_at: datetime
    inputs: Mapping[str, object]
    items: tuple[object, ...]
    root_node_id: str
    indexes: tuple[int, ...]
    output_selector: Sequence[str]
    error_handle_mode: ErrorHandleMode
    flatten_output: bool
    parallel_nums: int


ContainerAwaitRequest = LoopFrameRequest | IterationFrameRequest


class LoopExecutionSucceeded(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    kind: Literal["loop_succeeded"] = "loop_succeeded"
    started_at: datetime
    inputs: Mapping[str, object] = Field(default_factory=dict)
    outputs: Mapping[str, object] = Field(default_factory=dict)
    metadata: Mapping[str, object] = Field(default_factory=dict)
    steps: int
    node_run_result: NodeRunResult


class LoopExecutionFailed(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    kind: Literal["loop_failed"] = "loop_failed"
    started_at: datetime
    inputs: Mapping[str, object] = Field(default_factory=dict)
    outputs: Mapping[str, object] = Field(default_factory=dict)
    metadata: Mapping[str, object] = Field(default_factory=dict)
    steps: int
    error: str
    node_run_result: NodeRunResult


class LoopFrameCompleted(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    kind: Literal["loop_frame_completed"] = "loop_frame_completed"
    next_index: int


class IterationExecutionSucceeded(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    kind: Literal["iteration_succeeded"] = "iteration_succeeded"
    started_at: datetime
    inputs: Mapping[str, object] = Field(default_factory=dict)
    outputs: Mapping[str, object] = Field(default_factory=dict)
    metadata: Mapping[str, object] = Field(default_factory=dict)
    steps: int
    node_run_result: NodeRunResult


class IterationExecutionFailed(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    kind: Literal["iteration_failed"] = "iteration_failed"
    started_at: datetime
    inputs: Mapping[str, object] = Field(default_factory=dict)
    outputs: Mapping[str, object] = Field(default_factory=dict)
    metadata: Mapping[str, object] = Field(default_factory=dict)
    steps: int
    error: str
    node_run_result: NodeRunResult


class IterationFramesRequested(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: Literal["iteration_frames_requested"] = "iteration_frames_requested"
    indexes: tuple[int, ...]


ContainerExecutionResult = Annotated[
    LoopExecutionSucceeded
    | LoopExecutionFailed
    | LoopFrameCompleted
    | IterationExecutionSucceeded
    | IterationExecutionFailed
    | IterationFramesRequested,
    Field(discriminator="kind"),
]


ContainerRunResult = (
    LoopExecutionSucceeded
    | LoopExecutionFailed
    | LoopFrameCompleted
    | IterationExecutionSucceeded
    | IterationExecutionFailed
    | IterationFramesRequested
)
