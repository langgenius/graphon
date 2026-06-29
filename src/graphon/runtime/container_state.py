from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from graphon.enums import NodeState
from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.runtime.variable_pool import VariablePool


class FrameRuntimeData(BaseModel):
    """Serializable runtime data needed to rebuild one execution frame."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    variable_pool: VariablePool
    outputs: Mapping[str, object] = Field(default_factory=dict)
    llm_usage: LLMUsage = Field(default_factory=LLMUsage.empty_usage)
    node_run_steps: int = 0
    graph_node_states: Mapping[str, NodeState] = Field(default_factory=dict)
    graph_edge_states: Mapping[str, NodeState] = Field(default_factory=dict)


class ContainerRunState(BaseModel):
    """Runtime state for one container node invocation.

    A run belongs to the parent Loop/Iteration/WorkflowToolContainer node.
    It stores only the state needed to resume that parent node after child
    frames complete. It must not store Graph definitions, Node objects, or
    live generators.
    """

    model_config = ConfigDict(frozen=True)

    invocation_id: str
    kind: str
    frame_id: str
    node_id: str
    execution_id: str
    started_at: datetime
    phase_data: Mapping[str, object] = Field(default_factory=dict)


class ContainerFrameState(BaseModel):
    """Runtime state for one child frame created by a container run.

    A container run may own multiple child frames. This state maps a child
    frame back to its parent container run and handler kind so EventHandler can
    route frame completion without knowing concrete container types. It also
    stores the frame-local runtime snapshot needed to materialize the child
    frame again after resume.
    """

    model_config = ConfigDict(frozen=True)

    frame_id: str
    kind: str
    parent_invocation_id: str
    root_node_id: str
    phase_data: Mapping[str, object] = Field(default_factory=dict)
    runtime_data: FrameRuntimeData
