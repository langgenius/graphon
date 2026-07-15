from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from graphon.enums import ErrorHandleMode
from graphon.node_events.base import NodeRunResult


class LoopFrameRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: Literal["loop"] = "loop"
    inputs: Mapping[str, object]
    outputs: Mapping[str, object]
    loop_count: int
    root_node_id: str
    loop_variable_selectors: Mapping[str, Sequence[str]]
    loop_node_ids: frozenset[str]
    index: int


class IterationFrameRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: Literal["iteration"] = "iteration"
    items: tuple[object, ...]
    root_node_id: str
    indexes: tuple[int, ...]
    output_selector: Sequence[str]
    error_handle_mode: ErrorHandleMode
    flatten_output: bool
    parallel_nums: int


ContainerAwaitRequest = LoopFrameRequest | IterationFrameRequest


class ContainerExecutionResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: Literal["result"] = "result"
    metadata: Mapping[str, object]
    steps: int
    node_run_result: NodeRunResult


ContainerRunResult = Annotated[
    ContainerExecutionResult | LoopFrameRequest | IterationFrameRequest,
    Field(discriminator="kind"),
]
