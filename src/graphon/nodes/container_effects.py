from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Literal, cast

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field

from graphon.enums import ErrorHandleMode
from graphon.node_events.base import NodeRunResult
from graphon.variables.factory import build_segment
from graphon.variables.segments import Segment, SerializableSegment


def _to_serializable_segment(value: object) -> object:
    if isinstance(value, Segment):
        return build_segment(value.to_object())
    if isinstance(value, Mapping) and "value_type" in value:
        return value
    return build_segment(value)


type ContainerValue = Annotated[
    SerializableSegment,
    BeforeValidator(_to_serializable_segment),
]


def build_container_value(value: object) -> ContainerValue:
    raw_value = value.to_object() if isinstance(value, Segment) else value
    return cast(ContainerValue, build_segment(raw_value))


class ContainerNodeRunResult(NodeRunResult):
    inputs: Mapping[str, ContainerValue] = Field(default_factory=dict)
    outputs: Mapping[str, ContainerValue] = Field(default_factory=dict)

    def to_node_run_result(self) -> NodeRunResult:
        return NodeRunResult(
            status=self.status,
            inputs={key: value.to_object() for key, value in self.inputs.items()},
            outputs={key: value.to_object() for key, value in self.outputs.items()},
            metadata=self.metadata,
            llm_usage=self.llm_usage,
            error=self.error,
            error_type=self.error_type,
        )


class LoopFrameRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: Literal["loop"] = "loop"
    inputs: Mapping[str, ContainerValue]
    outputs: Mapping[str, ContainerValue]
    loop_count: int
    root_node_id: str
    loop_variable_selectors: Mapping[str, tuple[str, ...]]
    loop_node_ids: frozenset[str]
    index: int


class IterationFrameRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: Literal["iteration"] = "iteration"
    items: tuple[ContainerValue, ...]
    root_node_id: str
    indexes: tuple[int, ...]
    output_selector: tuple[str, ...]
    error_handle_mode: ErrorHandleMode
    flatten_output: bool
    parallel_nums: int


ContainerAwaitRequest = LoopFrameRequest | IterationFrameRequest


class ContainerExecutionResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: Literal["result"] = "result"
    metadata: Mapping[str, object]
    steps: int
    node_run_result: ContainerNodeRunResult


ContainerRunResult = Annotated[
    ContainerExecutionResult | LoopFrameRequest | IterationFrameRequest,
    Field(discriminator="kind"),
]
