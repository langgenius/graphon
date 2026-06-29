from __future__ import annotations

from typing import Protocol

from graphon.graph_events.base import GraphNodeEventBase
from graphon.graph_events.node import NodeRunFailedEvent
from graphon.nodes.container_effects import ContainerAwaitRequest

from .frames import ExecutionFrame


class ContainerHandler(Protocol):
    kind: str

    def start_await(
        self,
        *,
        frame_id: str,
        node_id: str,
        invocation_id: str,
        request: ContainerAwaitRequest,
    ) -> None: ...

    def prepare_frame_event(
        self,
        *,
        frame: ExecutionFrame,
        event: GraphNodeEventBase,
    ) -> None: ...

    def should_collect(
        self,
        *,
        frame: ExecutionFrame,
        event: GraphNodeEventBase,
    ) -> bool: ...

    def record_frame_failure(
        self,
        *,
        frame: ExecutionFrame,
        event: NodeRunFailedEvent,
    ) -> bool: ...

    def complete_frame(self, frame: ExecutionFrame) -> bool: ...
