from datetime import UTC, datetime
from typing import Any, cast
from unittest.mock import MagicMock

from graphon.enums import BuiltinNodeTypes, NodeExecutionType
from graphon.graph_engine.event_management.event_handlers import EventHandler
from graphon.graph_events.node import NodeRunStreamChunkEvent, NodeRunSucceededEvent
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
