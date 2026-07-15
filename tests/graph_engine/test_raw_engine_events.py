import logging
from datetime import UTC, datetime
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from graphon import graph_events, node_events
from graphon.enums import BuiltinNodeTypes, NodeExecutionType
from graphon.graph_engine.entities.tasks import TaskEvent
from graphon.graph_engine.event_management.event_handlers import EventHandler
from graphon.graph_engine.frames import ExecutionFrame, FrameRegistry
from graphon.graph_events.node import (
    NodeRunReasoningChunkEvent,
    NodeRunStreamChunkEvent,
    NodeRunSucceededEvent,
)
from graphon.graph_events.traversal import GraphEdgeTakenEvent
from graphon.node_events.base import NodeRunResult


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _root_frame(
    *,
    graph: object,
    graph_runtime_state: object,
    state_manager: object,
    edge_processor: object,
    error_handler: object,
) -> FrameRegistry:
    if isinstance(graph_runtime_state, MagicMock):
        graph_runtime_state.has_container_frame.return_value = False
    frame_registry = FrameRegistry()
    frame_registry.register(
        ExecutionFrame(
            frame_id="root",
            graph=cast(Any, graph),
            graph_runtime_state=cast(Any, graph_runtime_state),
            state_manager=cast(Any, state_manager),
            edge_processor=cast(Any, edge_processor),
            error_handler=cast(Any, error_handler),
        ),
    )
    return frame_registry


def _event_handler(
    *,
    graph_execution: object,
    event_collector: object,
    frame_registry: FrameRegistry,
) -> EventHandler:
    return EventHandler(
        graph_execution=cast(Any, graph_execution),
        event_collector=cast(Any, event_collector),
        frame_registry=frame_registry,
        container_handlers={},
    )


def test_event_handler_collects_raw_stream_chunk_without_coordinator() -> None:
    event_collector = MagicMock()
    handler = _event_handler(
        graph_execution=cast(Any, MagicMock()),
        event_collector=cast(Any, event_collector),
        frame_registry=_root_frame(
            graph=MagicMock(),
            graph_runtime_state=MagicMock(),
            state_manager=MagicMock(),
            edge_processor=MagicMock(),
            error_handler=MagicMock(),
        ),
    )
    chunk = NodeRunStreamChunkEvent(
        id="run-1",
        node_id="node-1",
        node_type=BuiltinNodeTypes.CODE,
        selector=["node-1", "answer"],
        chunk="hello",
        is_final=False,
    )

    handler.dispatch(TaskEvent(frame_id="root", event=chunk))

    event_collector.collect.assert_called_once_with(chunk)


def test_event_handler_collects_reasoning_chunk_without_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Reasoning chunks must hit the registered collect-only group, not the
    # default fallback that warns once per chunk.
    event_collector = MagicMock()
    handler = _event_handler(
        graph_execution=cast(Any, MagicMock()),
        event_collector=cast(Any, event_collector),
        frame_registry=_root_frame(
            graph=MagicMock(),
            graph_runtime_state=MagicMock(),
            state_manager=MagicMock(),
            edge_processor=MagicMock(),
            error_handler=MagicMock(),
        ),
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
        handler.dispatch(TaskEvent(frame_id="root", event=chunk))

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
    handler = _event_handler(
        graph_execution=cast(Any, graph_execution),
        event_collector=cast(Any, event_collector),
        frame_registry=_root_frame(
            graph=graph,
            graph_runtime_state=runtime_state,
            state_manager=state_manager,
            edge_processor=edge_processor,
            error_handler=MagicMock(),
        ),
    )
    success = NodeRunSucceededEvent(
        id="run-1",
        node_id="node-1",
        node_type=BuiltinNodeTypes.CODE,
        start_at=_now(),
        finished_at=_now(),
        node_run_result=NodeRunResult(outputs={"answer": "hello"}),
    )

    handler.dispatch(TaskEvent(frame_id="root", event=success))

    collected_events = [call.args[0] for call in event_collector.collect.call_args_list]
    assert collected_events == [edge_event, success]
