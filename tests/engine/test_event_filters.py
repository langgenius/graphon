from collections.abc import Iterable
from types import SimpleNamespace
from typing import Any, cast

from graphon.engine.filter import (
    EngineEventFilterContext,
    filter_engine_events,
)
from graphon.engine_events.base import EngineEvent
from graphon.engine_events.graph import (
    GraphRunPausedEvent,
    GraphRunStartedEvent,
    GraphRunSucceededEvent,
)
from graphon.engine_events.traversal import GraphEdgeTakenEvent
from graphon.runtime.runtime_state import RuntimeState
from graphon.runtime.variable_pool import VariablePool


def _context(state: RuntimeState | None = None) -> EngineEventFilterContext:
    if state is not None:
        return EngineEventFilterContext.from_engine(
            cast(Any, SimpleNamespace(graph=object(), runtime_state=state))
        )
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
    event = GraphRunStartedEvent(sequence=1)
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
    start = GraphRunStartedEvent(sequence=1)

    output = list(
        filter_engine_events(
            [start, edge],
            context=_context(),
            filters=[pass_through, _SplitStartFilter(), _DropTraversalFilter()],
        )
    )

    assert pass_through.initialized is True
    assert output[0] is start
    assert [event.sequence for event in output] == [1, 2]


def test_filter_chain_sequences_expanded_and_flushed_output() -> None:
    start = GraphRunStartedEvent(sequence=1)
    terminal = GraphRunSucceededEvent(sequence=2)

    output = list(
        filter_engine_events(
            [start, terminal],
            context=_context(),
            filters=[_SplitStartFilter(), _FlushFilter()],
        )
    )

    assert [event.sequence for event in output] == [1, 2, 3, 4]
    assert [start.sequence, terminal.sequence] == [1, 2]


def test_filter_chain_restores_expanded_and_flushed_sequences() -> None:
    state = RuntimeState(
        workflow_id="workflow", variable_pool=VariablePool(), start_at=0
    )
    execution = state.graph_execution
    started = GraphRunStartedEvent(
        execution_id=execution.execution_id, sequence=execution.next_event_sequence()
    )
    paused = GraphRunPausedEvent(
        execution_id=execution.execution_id, sequence=execution.next_event_sequence()
    )
    before_pause = list(
        filter_engine_events(
            [started, paused],
            context=_context(state),
            filters=[_SplitStartFilter(), _FlushFilter()],
        )
    )

    restored_state = RuntimeState.from_snapshot(state.dumps())
    restored_execution = restored_state.graph_execution
    resumed = GraphRunStartedEvent(
        execution_id=restored_execution.execution_id,
        sequence=restored_execution.next_event_sequence(),
    )
    after_resume = list(
        filter_engine_events(
            [resumed],
            context=_context(restored_state),
            filters=[_SplitStartFilter(), _FlushFilter()],
        )
    )

    assert [event.sequence for event in before_pause + after_resume] == list(
        range(1, 8)
    )
    assert started.execution_id == resumed.execution_id
    assert [started.sequence, paused.sequence, resumed.sequence] == [1, 2, 3]
    assert restored_execution.last_event_sequence == 3
    assert restored_execution.last_filtered_event_sequence == 7


def test_filter_chain_sends_flush_output_to_downstream_filters() -> None:
    output = list(
        filter_engine_events(
            [],
            context=_context(),
            filters=[_FlushFilter(), _DropTraversalFilter()],
        )
    )

    assert output == []
