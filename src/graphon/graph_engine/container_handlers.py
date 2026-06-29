from __future__ import annotations

from abc import abstractmethod
from typing import Protocol

from graphon.graph_events.base import GraphNodeEventBase
from graphon.graph_events.node import NodeRunFailedEvent
from graphon.nodes.container_effects import ContainerAwaitRequest

from .frames import ExecutionFrame


class ContainerHandler(Protocol):
    kind: str

    @abstractmethod
    def start_await(
        self,
        *,
        frame_id: str,
        node_id: str,
        invocation_id: str,
        request: ContainerAwaitRequest,
    ) -> None: ...

    @abstractmethod
    def prepare_frame_event(
        self,
        *,
        frame: ExecutionFrame,
        event: GraphNodeEventBase,
    ) -> None: ...

    @abstractmethod
    def should_collect(
        self,
        *,
        frame: ExecutionFrame,
        event: GraphNodeEventBase,
    ) -> bool: ...

    @abstractmethod
    def record_frame_failure(
        self,
        *,
        frame: ExecutionFrame,
        event: NodeRunFailedEvent,
    ) -> bool: ...

    @abstractmethod
    def complete_frame(self, frame: ExecutionFrame) -> bool: ...
