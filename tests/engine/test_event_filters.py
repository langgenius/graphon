from collections.abc import Iterable
from typing import Any, cast

from graphon.engine.filter import (
    EngineEventFilterContext,
    filter_engine_events,
)
from graphon.engine_events.base import EngineEvent
from graphon.engine_events.graph import GraphRunStartedEvent
from graphon.engine_events.traversal import GraphEdgeTakenEvent


def _context() -> EngineEventFilterContext:
    return EngineEventFilterContext(
        graph=cast(Any, object()),
        runtime_state=cast(Any, object()),
    )


class _PassThroughFilter:
    def __init__(self) -> None:
        self.initialized = False

    def initialize(self, context: EngineEventFilterContext) -> None:
        self.initialized = context is not None

    def on_event(self, event: EngineEvent) -> Iterable[EngineEvent]:
        yield event

    def flush(self) -> Iterable[EngineEvent]:
        return ()


class _DropTraversalFilter:
    def initialize(self, context: EngineEventFilterContext) -> None:
        self.context = context

    def on_event(self, event: EngineEvent) -> Iterable[EngineEvent]:
        if isinstance(event, GraphEdgeTakenEvent):
            return ()
        return (event,)

    def flush(self) -> Iterable[EngineEvent]:
        return ()


class _SplitStartFilter:
    def initialize(self, context: EngineEventFilterContext) -> None:
        self.context = context

    def on_event(self, event: EngineEvent) -> Iterable[EngineEvent]:
        if isinstance(event, GraphRunStartedEvent):
            return (event, event.model_copy())
        return (event,)

    def flush(self) -> Iterable[EngineEvent]:
        return ()


class _FlushFilter:
    def initialize(self, context: EngineEventFilterContext) -> None:
        self.context = context

    def on_event(self, event: EngineEvent) -> Iterable[EngineEvent]:
        return (event,)

    def flush(self) -> Iterable[EngineEvent]:
        return (
            GraphEdgeTakenEvent(
                frame_id="root",
                edge_id="flush-edge",
                source_node_id="a",
                target_node_id="b",
            ),
        )


def test_filter_chain_passes_events_when_no_filters() -> None:
    event = GraphRunStartedEvent()
    output = list(
        filter_engine_events(
            [event],
            context=_context(),
            filters=[],
        )
    )

    assert output == [event]


def test_filter_chain_initializes_and_chains_drop_and_split() -> None:
    pass_through = _PassThroughFilter()
    edge = GraphEdgeTakenEvent(
        frame_id="root",
        edge_id="edge-1",
        source_node_id="start",
        target_node_id="answer",
    )
    start = GraphRunStartedEvent()

    output = list(
        filter_engine_events(
            [start, edge],
            context=_context(),
            filters=[pass_through, _SplitStartFilter(), _DropTraversalFilter()],
        )
    )

    assert pass_through.initialized is True
    assert output == [start, start]


def test_filter_chain_sends_flush_output_to_downstream_filters() -> None:
    output = list(
        filter_engine_events(
            [],
            context=_context(),
            filters=[_FlushFilter(), _DropTraversalFilter()],
        )
    )

    assert output == []
