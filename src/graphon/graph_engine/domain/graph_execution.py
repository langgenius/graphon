"""GraphExecution aggregate root managing the overall graph execution state."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel

from graphon.entities.pause_reason import PauseReason

from .node_execution import NodeExecution


class NodeExecutionState(BaseModel):
    """Serializable representation of a node execution entity."""

    frame_id: str
    node_id: str
    retry_count: int
    execution_id: str


class GraphExecutionState(BaseModel):
    """Pydantic model describing serialized GraphExecution state."""

    version: Literal["1.0"]
    workflow_id: str
    started: bool
    completed: bool
    aborted: bool
    paused: bool
    pause_reasons: list[PauseReason]
    error: str | None
    exceptions_count: int
    node_executions: list[NodeExecutionState]


@dataclass
class GraphExecution:
    """Aggregate root for graph execution.

    This manages the overall execution state of a workflow graph,
    coordinating between multiple node executions.
    """

    workflow_id: str
    started: bool = False
    completed: bool = False
    aborted: bool = False
    paused: bool = False
    pause_reasons: list[PauseReason] = field(default_factory=list)
    error: Exception | None = None
    node_executions: dict[tuple[str, str], NodeExecution] = field(
        default_factory=dict[tuple[str, str], NodeExecution],
    )
    exceptions_count: int = 0

    def start(self) -> None:
        """Mark the graph execution as started."""
        if self.started:
            msg = "Graph execution already started"
            raise RuntimeError(msg)
        self.started = True

    def complete(self) -> None:
        """Mark the graph execution as completed."""
        if not self.started:
            msg = "Cannot complete execution that hasn't started"
            raise RuntimeError(msg)
        if self.completed:
            msg = "Graph execution already completed"
            raise RuntimeError(msg)
        self.completed = True

    def abort(self, reason: str) -> None:
        """Abort the graph execution."""
        self.aborted = True
        self.error = RuntimeError(f"Aborted: {reason}")

    def pause(self, reason: PauseReason) -> None:
        """Pause the graph execution without marking it complete."""
        if self.completed:
            msg = "Cannot pause execution that has completed"
            raise RuntimeError(msg)
        if self.aborted:
            msg = "Cannot pause execution that has been aborted"
            raise RuntimeError(msg)
        self.paused = True
        self.pause_reasons.append(reason)

    def fail(self, error: Exception) -> None:
        """Mark the graph execution as failed."""
        self.error = error
        self.completed = True

    def get_or_create_node_execution(
        self, *, frame_id: str, node_id: str
    ) -> NodeExecution:
        """Get or create a node execution entity."""
        key = (frame_id, node_id)
        if key not in self.node_executions:
            self.node_executions[key] = NodeExecution(
                execution_id=str(uuid4()),
            )
        return self.node_executions[key]

    def dumps(self) -> str:
        """Serialize the aggregate state into a JSON string."""
        node_states = [
            NodeExecutionState(
                frame_id=key[0],
                node_id=key[1],
                retry_count=node_execution.retry_count,
                execution_id=node_execution.execution_id,
            )
            for key, node_execution in sorted(self.node_executions.items())
        ]

        state = GraphExecutionState(
            version="1.0",
            workflow_id=self.workflow_id,
            started=self.started,
            completed=self.completed,
            aborted=self.aborted,
            paused=self.paused,
            pause_reasons=self.pause_reasons,
            error=None if self.error is None else str(self.error),
            exceptions_count=self.exceptions_count,
            node_executions=node_states,
        )

        return state.model_dump_json()

    def loads(self, data: str) -> None:
        """Restore aggregate state from a serialized JSON string."""
        state = GraphExecutionState.model_validate_json(data)

        if self.workflow_id != state.workflow_id:
            msg = "Serialized workflow_id does not match aggregate identity"
            raise ValueError(msg)

        self.started = state.started
        self.completed = state.completed
        self.aborted = state.aborted
        self.paused = state.paused
        self.pause_reasons = state.pause_reasons
        self.error = RuntimeError(state.error) if state.error is not None else None
        self.exceptions_count = state.exceptions_count
        self.node_executions = {
            (item.frame_id, item.node_id): NodeExecution(
                retry_count=item.retry_count,
                execution_id=item.execution_id,
            )
            for item in state.node_executions
        }

    def record_node_failure(self) -> None:
        """Increment the count of node failures encountered during execution."""
        self.exceptions_count += 1
