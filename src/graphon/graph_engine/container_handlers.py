from __future__ import annotations

from abc import abstractmethod
from typing import Protocol

from graphon.graph_events.base import GraphNodeEventBase
from graphon.graph_events.node import NodeRunFailedEvent
from graphon.nodes.container_effects import ContainerAwaitRequest
from graphon.runtime.container_state import ContainerFrameState

from .frames import ExecutionFrame


class ContainerHandler(Protocol):
    @abstractmethod
    def restore_frame(self, frame_state: ContainerFrameState) -> None: ...

    @abstractmethod
    def start_await(
        self,
        *,
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
        event: GraphNodeEventBase,
    ) -> bool: ...

    @abstractmethod
    def record_frame_failure(
        self,
        *,
        frame: ExecutionFrame,
        event: NodeRunFailedEvent,
    ) -> None: ...

    @abstractmethod
    def complete_frame(self, frame: ExecutionFrame) -> None: ...
