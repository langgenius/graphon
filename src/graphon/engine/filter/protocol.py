from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from graphon.engine_events.base import EngineEvent
from graphon.graph.graph import Graph
from graphon.runtime.graph_runtime_state_protocol import ReadOnlyGraphRuntimeState
from graphon.runtime.read_only_wrappers import ReadOnlyGraphRuntimeStateWrapper

if TYPE_CHECKING:
    from graphon.engine.engine import Engine


@dataclass(frozen=True)
class EngineEventFilterContext:
    """Run-scoped context available to engine event filters."""

    graph: Graph
    runtime_state: ReadOnlyGraphRuntimeState

    @classmethod
    def from_engine(cls, engine: Engine) -> EngineEventFilterContext:
        return cls(
            graph=engine.graph,
            runtime_state=ReadOnlyGraphRuntimeStateWrapper(
                engine.graph_runtime_state,
            ),
        )


class EngineEventFilter(Protocol):
    """Event-to-event transform used outside Engine execution."""

    def initialize(self, context: EngineEventFilterContext) -> None: ...

    def on_event(self, event: EngineEvent) -> Iterable[EngineEvent]: ...

    def flush(self) -> Iterable[EngineEvent]: ...
