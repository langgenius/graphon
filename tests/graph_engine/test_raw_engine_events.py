import logging
from datetime import UTC, datetime
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from graphon import graph_events, node_events
from graphon.entities import GraphFailureSource
from graphon.enums import BuiltinNodeTypes, NodeExecutionType
from graphon.graph_engine.event_management.event_handlers import EventHandler
from graphon.graph_engine.graph_engine import _GraphRunLifecycle
from graphon.graph_events.node import (
    NodeRunReasoningChunkEvent,
    NodeRunStreamChunkEvent,
    NodeRunSucceededEvent,
)
from graphon.graph_events.traversal import GraphEdgeTakenEvent
from graphon.node_events.base import NodeRunResult


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def test_event_handler_collects_raw_stream_chunk_without_coordinator() -> None:
    event_collector = MagicMock()
    handler = EventHandler(
        graph=cast(Any, MagicMock()),
        graph_runtime_state=cast(Any, MagicMock()),
        graph_execution=cast(Any, MagicMock()),
        event_collector=cast(Any, event_collector),
        edge_processor=cast(Any, MagicMock()),
        state_manager=cast(Any, MagicMock()),
        error_handler=cast(Any, MagicMock()),
    )
    chunk = NodeRunStreamChunkEvent(
        id="run-1",
        node_id="node-1",
        node_type=BuiltinNodeTypes.CODE,
        selector=["node-1", "answer"],
        chunk="hello",
        is_final=False,
    )

    handler.dispatch(chunk)

    event_collector.collect.assert_called_once_with(chunk)


def test_event_handler_collects_reasoning_chunk_without_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Reasoning chunks must hit the registered collect-only group, not the
    # default fallback that warns once per chunk.
    event_collector = MagicMock()
    handler = EventHandler(
        graph=cast(Any, MagicMock()),
        graph_runtime_state=cast(Any, MagicMock()),
        graph_execution=cast(Any, MagicMock()),
        event_collector=cast(Any, event_collector),
        edge_processor=cast(Any, MagicMock()),
        state_manager=cast(Any, MagicMock()),
        error_handler=cast(Any, MagicMock()),
    )
    chunk = NodeRunReasoningChunkEvent(
        id="run-1",
        node_id="node-1",
        node_type=BuiltinNodeTypes.CODE,
        selector=["node-1", "reasoning_content"],
        chunk="thinking",
        is_final=False,
    )

    with caplog.at_level(logging.WARNING):
        handler.dispatch(chunk)

    event_collector.collect.assert_called_once_with(chunk)
    assert "Unhandled event type" not in caplog.text


def test_reasoning_events_are_exported_from_package_roots() -> None:
    assert graph_events.NodeRunReasoningChunkEvent is NodeRunReasoningChunkEvent
    assert "NodeRunReasoningChunkEvent" in graph_events.__all__
    assert "StreamReasoningEvent" in node_events.__all__


def test_event_handler_collects_traversal_events_before_node_success() -> None:
    graph = MagicMock()
    graph.nodes = {"node-1": MagicMock(execution_type=NodeExecutionType.EXECUTABLE)}
    runtime_state = MagicMock()
    runtime_state.variable_pool = MagicMock()
    graph_execution = MagicMock()
    graph_execution.get_or_create_node_execution.return_value = MagicMock()
    graph_execution.is_paused = False
    event_collector = MagicMock()
    edge_event = GraphEdgeTakenEvent(
        edge_id="edge-1",
        source_node_id="node-1",
        target_node_id="node-2",
        source_handle="success",
    )
    edge_processor = MagicMock()
    edge_processor.process_node_success.return_value = ([], [edge_event])
    state_manager = MagicMock()
    handler = EventHandler(
        graph=cast(Any, graph),
        graph_runtime_state=cast(Any, runtime_state),
        graph_execution=cast(Any, graph_execution),
        event_collector=cast(Any, event_collector),
        edge_processor=cast(Any, edge_processor),
        state_manager=cast(Any, state_manager),
        error_handler=cast(Any, MagicMock()),
    )
    success = NodeRunSucceededEvent(
        id="run-1",
        node_id="node-1",
        node_type=BuiltinNodeTypes.CODE,
        start_at=_now(),
        finished_at=_now(),
        node_run_result=NodeRunResult(outputs={"answer": "hello"}),
    )

    handler.dispatch(success)

    collected_events = [call.args[0] for call in event_collector.collect.call_args_list]
    assert collected_events == [edge_event, success]


def test_fatal_node_failure_records_exact_failure_source() -> None:
    graph_execution = MagicMock()
    graph_execution.get_or_create_node_execution.return_value = MagicMock()
    error_handler = MagicMock()
    error_handler.handle_node_failure.return_value = None
    event_collector = MagicMock()
    state_manager = MagicMock()
    handler = EventHandler(
        graph=cast(Any, MagicMock()),
        graph_runtime_state=cast(Any, MagicMock()),
        graph_execution=cast(Any, graph_execution),
        event_collector=cast(Any, event_collector),
        edge_processor=cast(Any, MagicMock()),
        state_manager=cast(Any, state_manager),
        error_handler=cast(Any, error_handler),
    )
    event = graph_events.NodeRunFailedEvent(
        id="execution-a",
        node_id="node-a",
        node_type=BuiltinNodeTypes.CODE,
        error="boom",
        start_at=_now(),
    )

    handler.dispatch(event)

    error = graph_execution.fail.call_args.args[0]
    assert str(error) == "boom"
    assert graph_execution.fail.call_args.kwargs == {
        "failure_source": GraphFailureSource(
            node_execution_id="execution-a",
            node_id="node-a",
        )
    }


def test_handled_node_failure_does_not_record_graph_failure_source() -> None:
    graph_execution = MagicMock()
    graph_execution.get_or_create_node_execution.return_value = MagicMock()
    retry_event = graph_events.NodeRunRetryEvent(
        id="execution-a",
        node_id="node-a",
        node_type=BuiltinNodeTypes.CODE,
        node_title="Code",
        start_at=_now(),
        error="retrying",
        retry_index=1,
        in_iteration_id="iteration-a",
    )
    error_handler = MagicMock()
    error_handler.handle_node_failure.return_value = retry_event
    handler = EventHandler(
        graph=cast(Any, MagicMock()),
        graph_runtime_state=cast(Any, MagicMock()),
        graph_execution=cast(Any, graph_execution),
        event_collector=cast(Any, MagicMock()),
        edge_processor=cast(Any, MagicMock()),
        state_manager=cast(Any, MagicMock()),
        error_handler=cast(Any, error_handler),
    )
    event = graph_events.NodeRunFailedEvent(
        id="execution-a",
        node_id="node-a",
        node_type=BuiltinNodeTypes.CODE,
        error="temporary failure",
        start_at=_now(),
    )

    handler.dispatch(event)

    graph_execution.fail.assert_not_called()


def test_graph_failed_event_publishes_recorded_failure_source() -> None:
    first = GraphFailureSource(
        node_execution_id="execution-a",
        node_id="node-a",
    )
    second = GraphFailureSource(
        node_execution_id="execution-b",
        node_id="node-b",
    )
    graph_execution = MagicMock(
        exceptions_count=2,
        failure_source=first,
        observed_failure_sources=[first, second],
    )
    event_manager = MagicMock()
    lifecycle = _GraphRunLifecycle(
        graph_execution=cast(Any, graph_execution),
        event_manager=cast(Any, event_manager),
        initialize_layers=MagicMock(),
        start_execution=MagicMock(),
        stop_execution=MagicMock(),
        emit_terminal_events=MagicMock(),
    )

    event = lifecycle._failed_event(RuntimeError("boom"))

    assert event.failure_source == first
    assert event.observed_failure_sources == [first, second]
    event_manager.notify_layers.assert_called_once_with(event)


def test_graph_failed_event_omits_unattributed_failure_source() -> None:
    graph_execution = MagicMock(
        exceptions_count=0,
        failure_source=None,
        observed_failure_sources=[],
    )
    lifecycle = _GraphRunLifecycle(
        graph_execution=cast(Any, graph_execution),
        event_manager=cast(Any, MagicMock()),
        initialize_layers=MagicMock(),
        start_execution=MagicMock(),
        stop_execution=MagicMock(),
        emit_terminal_events=MagicMock(),
    )

    event = lifecycle._failed_event(RuntimeError("infrastructure failed"))

    assert event.failure_source is None
