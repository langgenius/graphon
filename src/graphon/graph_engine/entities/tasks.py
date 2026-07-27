from __future__ import annotations

from dataclasses import dataclass

from graphon.graph_events.base import GraphNodeEventBase
from graphon.nodes.container_effects import ContainerAwaitRequest


@dataclass(frozen=True, slots=True)
class TaskEvent:
    frame_id: str
    event: GraphNodeEventBase


@dataclass(frozen=True, slots=True)
class ContainerAwaitTask:
    invocation_id: str
    request: ContainerAwaitRequest


type DispatchTask = TaskEvent | ContainerAwaitTask
