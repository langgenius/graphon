import logging
from datetime import UTC, datetime
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from graphon import engine_events, node_events
from graphon.engine.event.processor import NodeEventProcessor
from graphon.engine.frame import ExecutionFrame, FrameRegistry
from graphon.engine.worker import NodeEventTask
from graphon.engine_events.node import (
    NodeRunReasoningChunkEvent,
    NodeRunStreamChunkEvent,
    NodeRunSucceededEvent,
)
from graphon.engine_events.traversal import GraphEdgeTakenEvent
from graphon.enums import BuiltinNodeTypes, NodeExecutionType
from graphon.node_events.base import NodeRunResult


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _root_frame(
    *,
    graph: object,
    state: object,
    scheduler: object,
    failure_handler: object,
) -> FrameRegistry:
    frame_registry = FrameRegistry()
    frame_registry.register(
        ExecutionFrame(
            frame_id="root",
            graph=cast(Any, graph),
            state=cast(Any, state),
            scheduler=cast(Any, scheduler),
            failure_handler=cast(Any, failure_handler),
        ),
    )
    return frame_registry


def _event_processor(
    *,
    graph_execution: object,
    event_stream: object,
    frame_registry: FrameRegistry,
) -> NodeEventProcessor:
    return NodeEventProcessor(
        graph_execution=cast(Any, graph_execution),
        event_stream=cast(Any, event_stream),
        frame_registry=frame_registry,
        container_handlers={},
    )


def test_event_processor_collects_raw_stream_chunk_without_coordinator() -> None:
    event_stream = MagicMock()
    handler = _event_processor(
        graph_execution=cast(Any, MagicMock()),
        event_stream=cast(Any, event_stream),
        frame_registry=_root_frame(
            graph=MagicMock(),
            state=MagicMock(),
            scheduler=MagicMock(),
            failure_handler=MagicMock(),
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

    handler.dispatch(NodeEventTask(frame_id="root", event=chunk))

    event_stream.collect.assert_called_once_with(chunk)


def test_event_processor_collects_reasoning_chunk_without_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Reasoning chunks must hit the registered collect-only group, not the
    # default fallback that warns once per chunk.
    event_stream = MagicMock()
    handler = _event_processor(
        graph_execution=cast(Any, MagicMock()),
        event_stream=cast(Any, event_stream),
        frame_registry=_root_frame(
            graph=MagicMock(),
            state=MagicMock(),
            scheduler=MagicMock(),
            failure_handler=MagicMock(),
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
        handler.dispatch(NodeEventTask(frame_id="root", event=chunk))

    event_stream.collect.assert_called_once_with(chunk)
    assert "Unhandled event type" not in caplog.text


def test_reasoning_events_are_exported_from_package_roots() -> None:
    assert engine_events.NodeRunReasoningChunkEvent is NodeRunReasoningChunkEvent
    assert "NodeRunReasoningChunkEvent" in engine_events.__all__
    assert "StreamReasoningEvent" in node_events.__all__


def test_event_processor_collects_traversal_events_before_node_success() -> None:
    graph = MagicMock()
    graph.nodes = {"node-1": MagicMock(execution_type=NodeExecutionType.EXECUTABLE)}
    runtime_state = MagicMock()
    runtime_state.variable_pool = MagicMock()
    graph_execution = MagicMock()
    graph_execution.get_or_create_node_execution.return_value = MagicMock()
    event_stream = MagicMock()
    edge_event = GraphEdgeTakenEvent(
        frame_id="root",
        edge_id="edge-1",
        source_node_id="node-1",
        target_node_id="node-2",
        source_handle="success",
    )
    scheduler = MagicMock()
    scheduler.process_node_success.return_value = ([], [edge_event])
    handler = _event_processor(
        graph_execution=cast(Any, graph_execution),
        event_stream=cast(Any, event_stream),
        frame_registry=_root_frame(
            graph=graph,
            state=runtime_state,
            scheduler=scheduler,
            failure_handler=MagicMock(),
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

    handler.dispatch(NodeEventTask(frame_id="root", event=success))

    collected_events = [call.args[0] for call in event_stream.collect.call_args_list]
    assert collected_events == [edge_event, success]
