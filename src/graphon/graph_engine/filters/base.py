from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from graphon.graph.graph import Graph
from graphon.graph_events.base import GraphEngineEvent
from graphon.runtime.graph_runtime_state_protocol import ReadOnlyGraphRuntimeState
from graphon.runtime.read_only_wrappers import ReadOnlyGraphRuntimeStateWrapper

if TYPE_CHECKING:
    from graphon.graph_engine.graph_engine import GraphEngine


@dataclass(frozen=True)
class GraphEventFilterContext:
    """Run-scoped context available to graph event filters."""

    graph: Graph
    runtime_state: ReadOnlyGraphRuntimeState

    @classmethod
    def from_engine(cls, engine: GraphEngine) -> GraphEventFilterContext:
        return cls(
            graph=engine.graph,
            runtime_state=ReadOnlyGraphRuntimeStateWrapper(
                engine.graph_runtime_state,
            ),
        )


class GraphEventFilter(Protocol):
    """Event-to-event transform used outside GraphEngine execution."""

    @property
    def filter_id(self) -> str:
        """Stable identifier for diagnostics and external state storage."""
        raise NotImplementedError

    def initialize(self, context: GraphEventFilterContext) -> None:
        """Bind run-scoped context before events are processed."""
        raise NotImplementedError

    def on_event(self, event: GraphEngineEvent) -> Iterable[GraphEngineEvent]:
        """Transform one input event into zero or more output events."""
        raise NotImplementedError

    def flush(self) -> Iterable[GraphEngineEvent]:
        """Emit buffered events after the upstream source is exhausted."""
        raise NotImplementedError


class ResumableGraphEventFilter(GraphEventFilter, Protocol):
    """Optional filter protocol for output-layer resume state."""

    def dumps(self) -> str:
        """Serialize this filter's private state."""
        raise NotImplementedError

    def loads(self, data: str) -> None:
        """Restore this filter's private state."""
        raise NotImplementedError
