from collections.abc import Iterable
from typing import Any, cast

from graphon.graph_engine.filters import (
    GraphEventFilter,
    GraphEventFilterContext,
    filter_graph_events,
)
from graphon.graph_events.base import GraphEngineEvent
from graphon.graph_events.graph import GraphRunStartedEvent
from graphon.graph_events.traversal import GraphEdgeTakenEvent


def _context() -> GraphEventFilterContext:
    return GraphEventFilterContext(
        graph=cast(Any, object()),
        runtime_state=cast(Any, object()),
    )


class _PassThroughFilter:
    filter_id = "pass-through"

    def __init__(self) -> None:
        self.initialized = False

    def initialize(self, context: GraphEventFilterContext) -> None:
        self.initialized = context is not None

    def on_event(self, event: GraphEngineEvent) -> Iterable[GraphEngineEvent]:
        yield event

    def flush(self) -> Iterable[GraphEngineEvent]:
        return ()


class _DropTraversalFilter:
    filter_id = "drop-traversal"

    def initialize(self, context: GraphEventFilterContext) -> None:
        self.context = context

    def on_event(self, event: GraphEngineEvent) -> Iterable[GraphEngineEvent]:
        if isinstance(event, GraphEdgeTakenEvent):
            return ()
        return (event,)

    def flush(self) -> Iterable[GraphEngineEvent]:
        return ()


class _SplitStartFilter:
    filter_id = "split-start"

    def initialize(self, context: GraphEventFilterContext) -> None:
        self.context = context

    def on_event(self, event: GraphEngineEvent) -> Iterable[GraphEngineEvent]:
        if isinstance(event, GraphRunStartedEvent):
            return (event, event.model_copy())
        return (event,)

    def flush(self) -> Iterable[GraphEngineEvent]:
        return ()


class _FlushFilter:
    filter_id = "flush"

    def initialize(self, context: GraphEventFilterContext) -> None:
        self.context = context

    def on_event(self, event: GraphEngineEvent) -> Iterable[GraphEngineEvent]:
        return (event,)

    def flush(self) -> Iterable[GraphEngineEvent]:
        return (
            GraphEdgeTakenEvent(
                edge_id="flush-edge",
                source_node_id="a",
                target_node_id="b",
            ),
        )


def test_filter_protocol_accepts_pass_through_filter() -> None:
    event_filter: GraphEventFilter = _PassThroughFilter()
    assert event_filter.filter_id == "pass-through"


def test_filter_chain_passes_events_when_no_filters() -> None:
    event = GraphRunStartedEvent()
    output = list(
        filter_graph_events(
            [event],
            context=_context(),
            filters=[],
        )
    )

    assert output == [event]


def test_filter_chain_initializes_and_chains_drop_and_split() -> None:
    pass_through = _PassThroughFilter()
    edge = GraphEdgeTakenEvent(
        edge_id="edge-1",
        source_node_id="start",
        target_node_id="answer",
    )
    start = GraphRunStartedEvent()

    output = list(
        filter_graph_events(
            [start, edge],
            context=_context(),
            filters=[pass_through, _SplitStartFilter(), _DropTraversalFilter()],
        )
    )

    assert pass_through.initialized is True
    assert output == [start, start]


def test_filter_chain_sends_flush_output_to_downstream_filters() -> None:
    output = list(
        filter_graph_events(
            [],
            context=_context(),
            filters=[_FlushFilter(), _DropTraversalFilter()],
        )
    )

    assert output == []
