from __future__ import annotations

import threading
from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime

from graphon.graph_events.base import GraphNodeEventBase
from graphon.nodes.base.node import Node
from graphon.nodes.container_effects import ContainerAwaitRequest, ContainerRunResult


@dataclass(frozen=True, slots=True)
class SuspendedInvocation:
    frame_id: str
    node_id: str
    node: Node
    events: Generator[
        GraphNodeEventBase | ContainerAwaitRequest, ContainerRunResult, None
    ]
    started_at: datetime


class SuspendedInvocationStore:
    def __init__(self) -> None:
        self._items: dict[str, SuspendedInvocation] = {}
        self._lock = threading.RLock()

    def store(
        self,
        invocation_id: str,
        invocation: SuspendedInvocation,
    ) -> None:
        with self._lock:
            self._items[invocation_id] = invocation

    def pop(self, invocation_id: str) -> SuspendedInvocation:
        with self._lock:
            return self._items.pop(invocation_id)

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._items)
